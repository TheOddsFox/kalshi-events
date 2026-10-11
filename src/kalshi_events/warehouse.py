"""Idempotent dlt landing and packaged dbt certification of verified raw evidence."""

from __future__ import annotations

import itertools
import json
import os
import shutil
import subprocess
import sys
import uuid
from importlib.resources import files
from pathlib import Path
from typing import Any

import dlt
import duckdb

from kalshi_events.capture import capture_pages, complete_capture
from kalshi_events.common import (
    CatalogueError,
    Store,
    atomic_json,
    digest,
    file_digest,
    now,
    regular_bytes,
)
from kalshi_events.normalize import ENTITY_KEYS, observations

RELATIONS = {
    "series": "current_series",
    "events": "current_events",
    "markets": "current_markets",
    "relationships": "relationships",
    "combo_legs": "combo_legs",
    "observation_history": "observation_history",
}


def evidence_inventory(store: Store, capture_id: str) -> dict[str, str]:
    state = complete_capture(store, capture_id)
    result = {
        f"captures/{capture_id}/state.json": digest(
            regular_bytes(store.path(f"captures/{capture_id}/state.json"))
        )
    }
    for relative in state["pages"]:
        result[relative] = digest(regular_bytes(store.path(relative)))
        body = str(Path(relative).parent / "body.bin")
        result[body] = digest(regular_bytes(store.path(body), store.limits.max_response_bytes))
    return result


def load(store: Store, capture_id: str) -> dict[str, Any]:
    state = complete_capture(store, capture_id)
    observation_count = 0

    def rows():
        nonlocal observation_count
        for row in observations(capture_pages(store, state)):
            observation_count += 1
            kind = row["entity_kind"]
            yield {
                "observation_id": row["observation_id"],
                "capture_id": capture_id,
                "entity_kind": kind,
                "entity_ticker": row[ENTITY_KEYS[kind]],
                "received_at": row["received_at"],
                "provider_updated_at": row["provider_updated_at"],
                "source_endpoint": row["source_endpoint"],
                "source_kind": row["source_kind"],
                "payload_json": row["payload_json"],
                "event_ticker": row.get("event_ticker"),
                "series_ticker": row.get("series_ticker"),
            }

    estimated = (
        sum(
            store.path(str(Path(relative).parent / "body.bin")).stat().st_size
            for relative in state["pages"]
        )
        * 4
    )
    store.check_storage(estimated)
    stream = rows()
    first = next(stream, None)
    database = store.path("warehouse.duckdb")
    pipeline = dlt.pipeline(
        pipeline_name="kalshi_catalogue",
        pipelines_dir=str(store.path("landing")),
        destination=dlt.destinations.duckdb(str(database)),
        dataset_name="landing",
    )
    if first is not None:
        columns = {
            key: {
                "data_type": "text",
                "nullable": key in {"provider_updated_at", "event_ticker", "series_ticker"},
            }
            for key in first
        }
        pipeline.run(
            dlt.resource(
                itertools.chain([first], stream),
                name="observations",
                primary_key="observation_id",
                columns=columns,
                write_disposition="merge",
            ),
            loader_file_format="parquet",
        )
    else:
        with duckdb.connect(str(database)) as connection:
            connection.execute("create schema if not exists landing")
            connection.execute(
                "create table if not exists landing.observations (observation_id varchar, capture_id varchar, entity_kind varchar, entity_ticker varchar, received_at varchar, provider_updated_at varchar, source_endpoint varchar, source_kind varchar, payload_json varchar, event_ticker varchar, series_ticker varchar)"
            )
    store.check_storage()
    receipt = {
        "capture_id": capture_id,
        "loaded_at": now(),
        "observation_count": observation_count,
        "evidence": evidence_inventory(store, capture_id),
    }
    store.write(f"loaded/{capture_id}.json", receipt)
    return receipt


def dbt(
    store: Store, args: list[str], *, timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    runtime = store.path(f"tmp/dbt-{uuid.uuid4().hex}")
    runtime.mkdir(parents=True)
    project = runtime / "project"
    shutil.copytree(Path(str(files("kalshi_events").joinpath("dbt"))), project)
    profiles = {
        "kalshi_events": {
            "target": "local",
            "outputs": {
                "local": {
                    "type": "duckdb",
                    "path": str(store.path("warehouse.duckdb")),
                    "threads": 1,
                    "settings": {
                        "memory_limit": "512MB",
                        "temp_directory": str(runtime / "spill"),
                        "max_temp_directory_size": str(store.limits.max_temp_bytes) + "B",
                    },
                }
            },
        }
    }
    atomic_json(runtime / "profiles.yml", profiles)
    executable = str(Path(sys.executable).parent / "dbt")
    if not Path(executable).is_file():
        raise CatalogueError("dbt must be installed alongside the producer executable")
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith(("DBT_", "DLT_"))
    }
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    try:
        result = subprocess.run(
            [
                executable,
                *args,
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(runtime),
                "--target-path",
                str(runtime / "target"),
                "--log-path",
                str(runtime / "logs"),
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=min(timeout or store.limits.max_duration_seconds, store.remaining),
        )
        if result.returncode:
            raise CatalogueError(
                "dbt certification failed:\n" + result.stdout[-8000:] + result.stderr[-2000:]
            )
        if args[0] == "build":
            artifacts = {}
            for name in ("manifest.json", "run_results.json"):
                body = regular_bytes(runtime / "target" / name)
                artifacts[name] = {"sha256": digest(body), "content": json.loads(body)}
            store.write("build-artifacts.json", artifacts)
        store.check_storage()
        return result
    except subprocess.TimeoutExpired as exc:
        raise CatalogueError("dbt exceeded operation deadline") from exc
    finally:
        shutil.rmtree(runtime)


def build(store: Store, capture_id: str) -> dict[str, Any]:
    state = complete_capture(store, capture_id)
    load(store, capture_id)
    dbt(store, ["build"])
    inventory = {}
    with duckdb.connect(str(store.path("warehouse.duckdb")), read_only=True) as connection:
        represented = connection.execute(
            "select distinct capture_id from main.observation_history"
        ).fetchall()
    # A committed dlt load can survive a crash before its receipt is written.
    for (represented_id,) in represented:
        inventory.update(evidence_inventory(store, represented_id))
    inventory.update(evidence_inventory(store, capture_id))
    receipt = {
        "version": 1,
        "capture_id": capture_id,
        "built_at": now(),
        "scope": "targeted" if state["selected"] else "catalogue",
        "evidence": inventory,
        "warehouse_sha256": file_digest(
            store.path("warehouse.duckdb"), store.limits.max_database_bytes
        )[0],
        "dbt_artifacts_sha256": digest(regular_bytes(store.path("build-artifacts.json"))),
    }
    store.write("build.json", receipt)
    return receipt


def certified(store: Store) -> dict[str, Any]:
    receipt = json.loads(regular_bytes(store.path("build.json")))
    for relative, expected in receipt["evidence"].items():
        if digest(regular_bytes(store.path(relative))) != expected:
            raise CatalogueError("build evidence integrity mismatch")
    if (
        file_digest(store.path("warehouse.duckdb"), store.limits.max_database_bytes)[0]
        != receipt["warehouse_sha256"]
    ):
        raise CatalogueError("warehouse changed after certified build")
    if digest(regular_bytes(store.path("build-artifacts.json"))) != receipt["dbt_artifacts_sha256"]:
        raise CatalogueError("dbt certification artifacts changed")
    return receipt
