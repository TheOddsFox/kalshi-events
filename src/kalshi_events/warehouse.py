"""Latest-only dlt landing and packaged dbt certification of one catalogue capture.

A build always lands a fresh warehouse in staging, certifies it, and only then renames the staged
directory to `builds/<capture_id>`. Failures leave every previous build and release untouched.
"""

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

from kalshi_events.capture import capture_pages, complete_capture, page_receipts
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    atomic_json,
    decode,
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
DBT_MEMORY_LIMIT = "4GB"
DBT_THREADS = 4


def build_directory(capture_id: str) -> str:
    return f"builds/{capture_id}"


def catalogue_capture(store: Store, capture_id: str) -> dict[str, Any]:
    state = complete_capture(store, capture_id)
    if state["selected"] is not None:
        raise CatalogueError("targeted metadata captures never enter the catalogue warehouse")
    return state


def evidence_inventory(store: Store, capture_id: str) -> dict[str, str]:
    state = complete_capture(store, capture_id)
    base = f"captures/{capture_id}"
    result = {
        f"{base}/state.json": digest(regular_bytes(store.path(f"{base}/state.json"))),
        f"{base}/pages.jsonl": digest(regular_bytes(store.path(f"{base}/pages.jsonl"))),
    }
    for row in page_receipts(store, state):
        store.check_deadline()
        relative = row["relative"]
        result[relative] = file_digest(store.path(relative), store.limits.max_root_bytes)[0]
        body = str(Path(relative).parent / "body.bin")
        result[body] = file_digest(store.path(body), store.limits.max_root_bytes)[0]
    return result


def _as_budget(exc: BaseException) -> BudgetExceeded | None:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if isinstance(current, BudgetExceeded):
            return current
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def _land(store: Store, state: dict[str, Any], directory: Path) -> int:
    """Land one capture into a fresh warehouse under `directory`. Returns observation count."""
    capture_id = state["capture_id"]
    observation_count = 0
    store.check_storage()

    def rows():
        nonlocal observation_count
        for row in observations(capture_pages(store, state)):
            store.check_deadline()
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

    stream = rows()
    first = next(stream, None)
    database = directory / "warehouse.duckdb"
    try:
        pipeline = dlt.pipeline(
            pipeline_name="kalshi_catalogue",
            pipelines_dir=str(directory / "landing"),
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
    except Exception as exc:
        budget = _as_budget(exc)
        if budget is None:
            raise
        raise budget from exc
    store.sync(str(directory.relative_to(store.root)))
    store.check_storage()
    return observation_count


def load(store: Store, capture_id: str) -> dict[str, Any]:
    """Land a catalogue capture into a throwaway warehouse; reports the landing without publishing."""
    state = catalogue_capture(store, capture_id)
    relative = f"tmp/load-{uuid.uuid4().hex}"
    directory = store.path(relative)
    directory.mkdir(parents=True)
    try:
        observation_count = _land(store, state, directory)
    finally:
        shutil.rmtree(directory, ignore_errors=True)
        store.sync(relative)
    return {
        "capture_id": capture_id,
        "loaded_at": now(),
        "observation_count": observation_count,
        "evidence": evidence_inventory(store, capture_id),
    }


def dbt(
    store: Store,
    args: list[str],
    *,
    timeout: float | None = None,
    database: str = "warehouse.duckdb",
    artifacts: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the packaged dbt project against `database` (root-relative)."""
    runtime_relative = f"tmp/dbt-{uuid.uuid4().hex}"
    runtime = store.path(runtime_relative)
    runtime.mkdir(parents=True)
    try:
        project = runtime / "project"
        shutil.copytree(Path(str(files("kalshi_events").joinpath("dbt"))), project)
        profiles = {
            "kalshi_events": {
                "target": "local",
                "outputs": {
                    "local": {
                        "type": "duckdb",
                        "path": str(store.path(database)),
                        "threads": DBT_THREADS,
                        "settings": {
                            "memory_limit": DBT_MEMORY_LIMIT,
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
        if args[0] == "build" and artifacts is not None:
            values = {}
            for name in ("manifest.json", "run_results.json"):
                body = regular_bytes(runtime / "target" / name)
                values[name] = {"sha256": digest(body), "content": json.loads(body)}
            store.write(artifacts, values)
        store.check_storage()
        return result
    except subprocess.TimeoutExpired as exc:
        raise BudgetExceeded("dbt exceeded operation deadline") from exc
    finally:
        shutil.rmtree(runtime, ignore_errors=True)
        store.sync(runtime_relative)


def build(store: Store, capture_id: str) -> dict[str, Any]:
    """Build a fresh, certified warehouse for one complete catalogue capture."""
    state = catalogue_capture(store, capture_id)
    store.check_deadline()
    staging_relative = f"tmp/build-{uuid.uuid4().hex}"
    staging = store.path(staging_relative)
    staging.mkdir(parents=True)
    destination_relative = build_directory(capture_id)
    replaced_relative = f"tmp/replaced-{uuid.uuid4().hex}"
    replaced = store.path(replaced_relative)
    try:
        _land(store, state, staging)
        dbt(
            store,
            ["build"],
            database=f"{staging_relative}/warehouse.duckdb",
            artifacts=f"{staging_relative}/build-artifacts.json",
        )
        store.sync(staging_relative)
        receipt = {
            "version": 1,
            "capture_id": capture_id,
            "built_at": now(),
            "scope": "catalogue",
            "evidence": evidence_inventory(store, capture_id),
            "warehouse_sha256": file_digest(
                staging / "warehouse.duckdb", store.limits.max_database_bytes
            )[0],
            "dbt_artifacts_sha256": digest(regular_bytes(staging / "build-artifacts.json")),
        }
        store.write(f"{staging_relative}/build.json", receipt)
        destination = store.path(destination_relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        previous = destination.exists()
        if previous:
            os.rename(destination, replaced)
        try:
            os.rename(staging, destination)
        except BaseException:
            if previous:
                os.rename(replaced, destination)
            raise
        if previous:
            shutil.rmtree(replaced, ignore_errors=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        store.sync(staging_relative)
        store.sync(replaced_relative)
        store.sync(destination_relative)
    store.check_storage()
    return receipt


def certified(store: Store, capture_id: str) -> dict[str, Any]:
    directory = build_directory(capture_id)
    receipt = decode(regular_bytes(store.path(f"{directory}/build.json")))
    if receipt.get("capture_id") != capture_id or receipt.get("version") != 1:
        raise CatalogueError("build receipt does not match the requested capture")
    for relative, expected in receipt["evidence"].items():
        store.check_deadline()
        if file_digest(store.path(relative), store.limits.max_root_bytes)[0] != expected:
            raise CatalogueError("build evidence integrity mismatch")
    warehouse = store.path(f"{directory}/warehouse.duckdb")
    if file_digest(warehouse, store.limits.max_database_bytes)[0] != receipt["warehouse_sha256"]:
        raise CatalogueError("warehouse changed after certified build")
    artifacts = store.path(f"{directory}/build-artifacts.json")
    if digest(regular_bytes(artifacts)) != receipt["dbt_artifacts_sha256"]:
        raise CatalogueError("dbt certification artifacts changed")
    return receipt
