"""Real offline artifacts for sealed historical rehearsal tests."""

from __future__ import annotations

import collections
import hashlib
import json
import shutil
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write_census(
    operator: Path, control_root: Path, capture_id: str, root_id: str
) -> dict[str, Any]:
    from kalshi_events import capture, historical, publish
    from kalshi_events.common import Store, json_bytes

    state = capture.read_capture(Store(operator), capture_id)
    selection = historical.historical_selection(state, allow_legacy_incomplete=True)
    task_indices = historical.historical_task_indices(state, allow_legacy_incomplete=True)
    selected_pages = []
    observations: collections.Counter[str] = collections.Counter()
    nested_event_markets = 0
    row_collections = {
        "/series": "series",
        "/events": "events",
        "/historical/markets": "markets",
    }
    scopes = {int(index): scope for index, scope in state["task_scopes"].items()}
    excluded_scopes = [
        {
            "task_index": index,
            "scope": scope,
            "endpoint": state["tasks"][index]["endpoint"],
        }
        for index, scope in sorted(scopes.items())
        if scope in publish.HISTORICAL_OMITTED_SCOPES
    ]
    for order, row in enumerate(state.page_rows):
        if row["index"] not in task_indices:
            continue
        receipt = operator / row["relative"]
        manifest_path = receipt.parent / "manifest.json"
        body_path = receipt.parent / "body.bin"
        manifest = json.loads(manifest_path.read_bytes())
        selected_pages.append(
            {
                **row,
                "order": order,
                "endpoint": manifest["endpoint"],
                "generation": manifest.get(
                    "task_generation", manifest.get("request_generation", 0)
                ),
                "encoded_bytes": body_path.stat().st_size,
                "decoded_bytes": manifest.get("decoded_size", manifest["size"]),
                "status": manifest["status"],
                "manifest_bytes": manifest_path.stat().st_size,
            }
        )
    data_pages = [
        page
        for page in selected_pages
        if page["status"] == 200 and page["endpoint"] != "/historical/cutoff"
    ]
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = collections.defaultdict(list)
    for page in data_pages:
        grouped[(page["endpoint"], page["generation"])].append(page)
        body = json.loads((operator / page["relative"]).parent.joinpath("body.bin").read_bytes())
        rows = body[row_collections[page["endpoint"]]]
        observations[page["endpoint"]] += len(rows)
        if page["endpoint"] == "/events":
            nested_event_markets += sum(len(event.get("markets", [])) for event in rows)
    endpoint_counts = dict(sorted(observations.items()))
    data_strata = []
    for (endpoint, generation), pages in sorted(grouped.items()):
        decoded = sorted(page["decoded_bytes"] for page in pages)
        data_strata.append(
            {
                "endpoint": endpoint,
                "generation": generation,
                "candidate_pages": len(pages),
                "encoded_bytes": sum(page["encoded_bytes"] for page in pages),
                "decoded_bytes": sum(decoded),
                "median_decoded_page_bytes": decoded[(len(decoded) + 1) // 2 - 1],
                "max_decoded_page_bytes": max(decoded),
            }
        )
    state_sha256 = state.verified_snapshot_sha256
    index = {
        "capture_id": capture_id,
        "capture_state_sha256": state_sha256,
        "historical_selection": selection,
        "selected_task_indices": sorted(task_indices),
        "retired_scopes_excluded": excluded_scopes,
        "data_candidate_pages": len(data_pages),
        "manifest_metadata_bytes": sum(page["manifest_bytes"] for page in selected_pages),
        "pages": [
            {
                key: page[key]
                for key in (
                    "decoded_bytes",
                    "encoded_bytes",
                    "endpoint",
                    "generation",
                    "index",
                    "order",
                    "relative",
                    "request_id",
                    "status",
                )
            }
            for page in data_pages
        ],
    }
    index_raw = json_bytes(index)
    population = {
        "status": "complete",
        "capture_id": capture_id,
        "capture_state_sha256": state_sha256,
        "metadata_index_sha256": _sha256(index_raw),
        "candidate_pages": len(data_pages),
        "pages_processed": len(data_pages),
        "next_index": len(data_pages),
        "observation_counts": endpoint_counts | {"nested_event_markets": nested_event_markets},
        "candidate_population": {"data_strata": data_strata, "control_receipts": []},
        "encoded_bytes_verified": sum(page["encoded_bytes"] for page in data_pages),
        "decoded_bytes_verified": sum(page["decoded_bytes"] for page in data_pages),
        "page_counts": dict(
            sorted(collections.Counter(page["endpoint"] for page in data_pages).items())
        ),
    }
    index_path = control_root / "authorized-receipt-index.json"
    population_path = control_root / "authorized-population.json"
    index_path.write_bytes(index_raw)
    population_raw = json_bytes(population)
    population_path.write_bytes(population_raw)
    return {
        "population_report_path": str(population_path.resolve()),
        "population_report_sha256": _sha256(population_raw),
        "receipt_index_path": str(index_path.resolve()),
        "receipt_index_sha256": _sha256(index_raw),
        "capture_id": capture_id,
        "root_id": root_id,
        "capture_state_sha256": state_sha256,
        "candidate_pages": len(data_pages),
        "observations_by_endpoint": endpoint_counts,
        "nested_event_markets": nested_event_markets,
        "exact_observations": sum(endpoint_counts.values()),
    }


def _write_stage_and_spills(operator: Path, capture_id: str) -> tuple[str, Path, Path]:
    from kalshi_events import warehouse
    from kalshi_events.common import Store

    store = Store(operator)
    warehouse.build_historical(store, capture_id)
    stage_name = "build-historical-" + uuid.uuid4().hex
    stage = store.path(f"tmp/{stage_name}")
    stage.mkdir(parents=True)
    database = stage / "warehouse.duckdb"
    build = store.path(f"builds-historical/{capture_id}")
    shutil.copyfile(build / "warehouse.duckdb", database)
    shutil.rmtree(build)
    store.sync(f"builds-historical/{capture_id}")
    store.reconcile()

    # Leave a committed, recoverable DuckDB WAL as if the runner stopped before checkpoint.
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import duckdb, os, sys; db=duckdb.connect(sys.argv[1]); "
            "db.execute('create table rehearsal_wal_marker(value varchar)'); "
            "db.execute('insert into rehearsal_wal_marker values (?)', [sys.argv[2]]); "
            "os._exit(0)",
            str(database),
            capture_id,
        ],
        check=True,
    )
    if not database.with_name(database.name + ".wal").is_file():
        raise RuntimeError("fixture DuckDB did not retain the expected recovery WAL")

    owned_runtime_name = "dbt-" + uuid.uuid4().hex
    unrelated_runtime_name = "dbt-" + uuid.uuid4().hex
    owned_spill = store.path(f"tmp/{owned_runtime_name}/spill/owned-page.bin")
    unrelated_spill = store.path(f"tmp/{unrelated_runtime_name}/spill/unrelated-page.bin")
    owned_spill.parent.mkdir(parents=True)
    unrelated_spill.parent.mkdir(parents=True)
    # The cleanup proof itself is counted as temporary storage.
    owned_spill.write_bytes(b"s" * 1024**2)
    owned_spill.with_name("second-owned-page.bin").write_bytes(b"t" * 16 * 1024)
    unrelated_spill.write_bytes(b"unrelated dbt spill must remain")
    for runtime_name, spill, database_path in (
        (owned_runtime_name, owned_spill.parent, database),
        (
            unrelated_runtime_name,
            unrelated_spill.parent,
            store.path("tmp/unrelated-build/warehouse.duckdb"),
        ),
    ):
        store.write(
            f"tmp/{runtime_name}/profiles.yml",
            {
                "kalshi_events": {
                    "outputs": {
                        "local": {
                            "path": str(database_path),
                            "config_options": {"temp_directory": str(spill)},
                        }
                    }
                }
            },
        )
    runtime = store.path(f"tmp/{owned_runtime_name}")
    store.write(
        f"tmp/{owned_runtime_name}/target/manifest.json",
        {
            "metadata": {
                "project_name": "kalshi_events",
                "vars": {"current_capture_id": capture_id, "historical_compact": True},
            },
            "nodes": {
                f"model.kalshi_events._historical_current_{name}": {
                    "config": {"enabled": True},
                    "original_file_path": f"models/_historical_current_{name}.sql",
                    "compiled_code": f"select * from population p where p.capture_id = '{capture_id}'",
                }
                for name in ("events", "series")
            },
        },
    )
    compiled = runtime / "target/compiled/kalshi_events/models"
    compiled.mkdir(parents=True)
    for name in ("events", "series"):
        (compiled / f"_historical_current_{name}.sql").write_text(
            f"select * from population p\nwhere p.capture_id = '{capture_id}'\n",
            encoding="utf-8",
        )

    # Seed a real durable cleanup inventory and interrupt its deletion loop.
    remove = store.remove
    removed = []

    def interrupt_cleanup(relative):
        if removed:
            raise InterruptedError("fixture spill cleanup interrupted")
        remove(relative)
        removed.append(relative)

    with store.writer_guard(), patch.object(store, "remove", interrupt_cleanup):
        try:
            warehouse._cleanup_resumed_historical_dbt_spills(store, capture_id, f"tmp/{stage_name}")
        except InterruptedError as exc:
            if str(exc) != "fixture spill cleanup interrupted":
                raise
        else:
            raise RuntimeError("fixture spill cleanup did not leave a partial inventory")
    (surviving_spill,) = owned_spill.parent.iterdir()
    return stage_name, surviving_spill, unrelated_spill


def _write_stopped_report(
    directory: Path, control_root: Path, root_id: str, capture_id: str
) -> tuple[Path, dict[str, Any]]:
    import importlib.util

    script = Path(__file__).parents[1] / "scripts" / "production_run.py"
    spec = importlib.util.spec_from_file_location("historical_rehearsal_fixture_production", script)
    assert spec and spec.loader
    production = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(production)

    production_dir = Path(directory)
    operator = production_dir / "operator"
    source_root = control_root / "source"
    script_path = source_root / "scripts" / "production_run.py"
    if not script_path.is_file():
        raise FileNotFoundError(f"fixture source has no production runner: {script_path}")
    runtime = control_root
    stdout = runtime / "logs" / "worker.stdout.log"
    stdout.parent.mkdir(parents=True, exist_ok=True)
    stdout.write_bytes(b"")

    session_id = uuid.uuid4().hex
    report_path = production_dir / "runs" / session_id / production.REPORT_NAME
    report_path.parent.mkdir(parents=True)
    command = [
        sys.executable,
        str(script_path),
        "--invalid-fixture-only",
        "--directory",
        str(production_dir),
        "--historical",
        "--capture-id",
        capture_id,
    ]
    child = subprocess.Popen(
        command,
        cwd=production_dir,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    process_group = child.pid
    child.wait(timeout=15)
    if child.returncode not in {0, 2}:
        raise RuntimeError("offline fixture identity process exited unexpectedly")

    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    ).stdout.strip()
    started = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=1)
    invocation_id = uuid.uuid4().hex
    threshold = int(production._limits_for_profile("expanded")["storage_temporary_stop_bytes"])
    error = f"live temporary storage reached the {threshold // production.GIB} GiB stop threshold"
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": "full",
        "source_root": str(source_root),
        "revision": revision,
        "original_revision": revision,
        "capture_id": capture_id,
        "root_id": root_id,
        "boot_stamp": production._boot_stamp(),
        "phase": "stopped",
        "started_at": started.isoformat(),
        "active_process_group": process_group,
        "production_state": "not_attempted",
        "error": error,
        "network_activity_this_invocation": {
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
        "storage_supervision_invocation": invocation_id,
        "storage_supervision": {
            "current": {"operator_temporary_bytes": threshold + 1},
        },
        "process_supervision": {
            "primary_stop_reason": error,
            "termination_attempted": True,
            "leader_reaped": True,
            "group_absent": None,
            "uncertain": True,
            "invocation_token": invocation_id,
            "termination_errors": ["process-group probe: [Errno 1] Operation not permitted"],
        },
        "limits": dict(production.RUN_LIMITS),
        "resource_profile": "default",
        "system": production._system_info(production_dir),
    }
    production._bind_report_resource_profile(report, "expanded", transition=True)
    threshold = int(report["limits"]["storage_temporary_stop_bytes"])
    inclusive = bool(report["limits"]["storage_temporary_stop_inclusive"])
    report["error"] = error = (
        f"live temporary storage reached the {threshold // production.GIB} GiB stop threshold"
    )
    report["storage_supervision"]["current"]["operator_temporary_bytes"] = threshold + 1
    report["process_supervision"]["primary_stop_reason"] = error
    production._configure_offline_historical_window(report, capture_id, 86_400)
    report["storage_safety_stop"] = {
        "version": 1,
        "mode": "historical",
        "category": "temporary",
        "invocation_id": invocation_id,
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": revision,
        "revision_source": "session.revision",
        "observed_bytes": threshold + 1,
        "threshold_bytes": threshold,
        "threshold_inclusive": inclusive,
        "original_process_group": process_group,
    }
    production._atomic_report(report_path, report)
    launch = {
        "session": str(report_path),
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": revision,
        "pid": child.pid,
        "pgid": process_group,
        "state": "launched",
        "offline_only": True,
        "automatic_retry": False,
        "command": command,
        "process_identity": f"{child.pid} {process_group} fixture-exited",
        "launched_at_utc": started.isoformat(),
        "started_epoch": started.timestamp(),
        "started_monotonic": time.monotonic() - 1,
        "stdout": str(stdout),
    }
    launch_path = runtime / "latest-launch.json"
    launch_path.write_text(json.dumps(launch), encoding="utf-8")
    launch_path.chmod(0o600)
    (operator / "root.json").write_text(
        json.dumps({"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}),
        encoding="utf-8",
    )
    (operator / "writer.lock").write_bytes(b"")
    production._atomic_report(
        production_dir / production.OWNER_MARKER, {"version": 1, "root_id": root_id}
    )
    result = production._reconcile_stopped_worker(production_dir, capture_id)
    return report_path, result


def prepare_fixture(directory: Path, control_root: Path) -> dict[str, Any]:
    """Create a bounded, offline fixture with genuine capture, stop, and stage artifacts."""
    from conftest import FakeClient

    from kalshi_events import capture
    from kalshi_events.common import Limits, Store, json_bytes
    from kalshi_events.incremental import root_id as logical_root_id

    directory = Path(directory)
    control_root = Path(control_root)
    if directory.is_symlink() or control_root.is_symlink():
        raise ValueError("fixture roots must be real directories")
    directory.mkdir(parents=True, exist_ok=True)
    if not (control_root / "source").is_dir():
        raise FileNotFoundError(f"fixture source is missing: {control_root / 'source'}")
    operator = directory / "operator"
    operator.mkdir()
    (operator / "root.json").write_text(
        json.dumps({"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}),
        encoding="utf-8",
    )
    (operator / "writer.lock").write_bytes(b"")
    store = Store(operator, Limits(max_requests=64))
    with store.writer_guard():
        root_id = logical_root_id(store, create=True)

    series = {
        "ticker": "REHEARSAL-SERIES",
        "title": "Offline rehearsal series",
        "contract_url": "https://example.test/rehearsal-rules",
        "fee_multiplier": "1.25",
    }
    events = [
        {
            "event_ticker": "REHEARSAL-LIVE",
            "series_ticker": series["ticker"],
            "title": "Offline live event",
            "mutually_exclusive": False,
        },
        {
            "event_ticker": "REHEARSAL-HISTORY",
            "series_ticker": series["ticker"],
            "title": "Offline history event",
            "mutually_exclusive": False,
        },
    ]
    market = {
        "ticker": "REHEARSAL-MARKET",
        "event_ticker": "REHEARSAL-HISTORY",
        "status": "finalized",
        "rules_primary": "Resolve using the captured historical result",
        "rules_secondary": "Fixture market",
        "last_price_dollars": "0.00123",
        "volume_fp": "12.3456789",
        "open_interest_fp": "2.125",
        "price_level_structure": "deci_cent",
        "price_ranges": [{"start": "0", "end": "1", "step": "0.0001"}],
        "floor_strike": "3.123456789123456789",
        "result": "no",
        "settlement_ts": "2025-01-01T00:00:00Z",
        "settlement_value_dollars": "0.0000",
    }

    class Client(FakeClient):
        def response(self, endpoint, params):
            if endpoint == "/series":
                return 200, {"series": [series]}
            if endpoint == "/events":
                return 200, {"events": events, "cursor": ""}
            if endpoint == "/historical/markets":
                return 200, {"markets": [market], "cursor": ""}
            if endpoint in {"/events/multivariate", "/markets"}:
                cursor = params.get("cursor")
                return 200, {
                    "events" if endpoint == "/events/multivariate" else "markets": [],
                    "cursor": f"excluded-{cursor or 'start'}",
                }
            return super().response(endpoint, params)

    state = capture.capture(
        store,
        client_factory=Client,
        catalogue_profile="exchange",
    )
    capture_id = state["capture_id"]
    global_state = capture.read_capture(store, capture_id)
    if global_state["complete"] or not all(
        global_state["tasks"][index]["done"] for index in (1, 2, 5)
    ):
        raise RuntimeError(
            "fixture capture did not retain the selected historical scope: "
            + repr([(work["endpoint"], work["done"]) for work in global_state["tasks"]])
        )

    census = _write_census(operator, control_root, capture_id, root_id)
    expected = {
        "observations_by_endpoint": {
            "/series": 1,
            "/events": 2,
            "/historical/markets": 1,
        },
        "nested_event_markets": 0,
        "exact_observations": 4,
        "relation_keys": {
            "current_series": [series["ticker"]],
            "current_events": sorted(event["event_ticker"] for event in events),
            "current_markets": [market["ticker"]],
            "relationships": [
                ["event", event["event_ticker"], series["ticker"]] for event in events
            ]
            + [["market", market["ticker"], market["event_ticker"]]],
            "combo_legs": [],
            "observation_history": sorted(
                [["events", event["event_ticker"]] for event in events]
                + [["markets", market["ticker"]], ["series", series["ticker"]]]
            ),
        },
        "source": {"series": [series], "events": events, "markets": [market]},
    }

    stage_name, owned_spill, unrelated_spill = _write_stage_and_spills(operator, capture_id)
    report_path, outcome = _write_stopped_report(directory, control_root, root_id, capture_id)
    attempt_raw = json_bytes(outcome)
    launch_raw = (control_root / "latest-launch.json").read_bytes()
    audit_raw = (operator / "stopped-worker-reconciliation.jsonl").read_bytes()
    session_raw = report_path.read_bytes()
    prior_attempts = {
        "attempt_outcome_sha256": _sha256(attempt_raw),
        "failed_launch_sha256": _sha256(launch_raw),
        "audit_after_sha256": _sha256(audit_raw),
        "failed_session_sha256": _sha256(session_raw),
    }
    prior_attempts["bundle_sha256"] = _sha256(
        json_bytes(
            {
                key: {"path": path, "sha256": prior_attempts[key]}
                for key, path in (
                    ("attempt_outcome_sha256", "attempt.json"),
                    ("failed_launch_sha256", "launch.json"),
                    ("audit_after_sha256", "audit.jsonl"),
                    ("failed_session_sha256", "session.json"),
                )
            }
        )
    )
    return {
        "capture_id": capture_id,
        "root_id": root_id,
        "stage_name": stage_name,
        "report_path": report_path,
        "census": census,
        "owned_spill": owned_spill,
        "unrelated_spill": unrelated_spill,
        "prior_attempts": prior_attempts,
        "expected": expected,
    }
