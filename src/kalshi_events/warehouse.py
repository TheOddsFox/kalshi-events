"""Latest-only dlt landing and packaged dbt certification of one catalogue capture.

A build always lands a fresh warehouse in staging, certifies it, and only then renames the staged
directory to `builds/<capture_id>`. Failures leave every previous build and release untouched.
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import json
import os
import shutil
import subprocess
import sys
import threading
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any

import dlt
import duckdb
import pyarrow as pa
from dlt.common.data_types.type_helpers import coerce_value, py_type_to_sc_type

from kalshi_events import metrics
from kalshi_events.capture import (
    CURRENT_ACQUISITION_PROTOCOL,
    _acquisition_protocol,
    _complete_capture,
    capture_pages,
    page_receipts,
)
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    atomic_json,
    decode,
    digest,
    durable_mkdir,
    file_digest,
    fsync_directory,
    fsync_file,
    interrupt_at_deadline,
    json_bytes,
    now,
    regular_bytes,
    validate_series_scope,
)
from kalshi_events.normalize import ENTITY_KEYS, DiskParentIndex, ParentIndex, observations
from kalshi_events.transport import read_page

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
_LANDING_COLUMNS = (
    "observation_id",
    "capture_id",
    "entity_kind",
    "entity_ticker",
    "received_at",
    "provider_updated_at",
    "source_endpoint",
    "source_kind",
    "payload_json",
    "event_ticker",
    "series_ticker",
    "task_scope",
    "task_generation",
)
_LANDING_SCHEMA = pa.schema(
    pa.field(
        name,
        pa.string(),
        nullable=name in {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
    )
    for name in _LANDING_COLUMNS
)
_LANDING_BATCH_ROWS = 4096
_LANDING_BATCH_BYTES = 8 * 1024 * 1024


def _landing_arrow_batches(rows):
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    for row in rows:
        row_bytes = sum(len(value.encode("utf-8")) for value in row.values() if value is not None)
        if batch and (
            len(batch) >= _LANDING_BATCH_ROWS or batch_bytes + row_bytes > _LANDING_BATCH_BYTES
        ):
            yield pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)
            batch = []
            batch_bytes = 0
        batch.append(row)
        batch_bytes += row_bytes
        if len(batch) >= _LANDING_BATCH_ROWS or batch_bytes >= _LANDING_BATCH_BYTES:
            yield pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)
            batch = []
            batch_bytes = 0
    if batch:
        yield pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)


def _legacy_landing_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return None
    return coerce_value("text", py_type_to_sc_type(type(value)), value)


def build_directory(capture_id: str, catalogue_version: int = 1) -> str:
    if catalogue_version == 3:
        return f"builds-historical/{capture_id}"
    return f"builds-v2/{capture_id}" if catalogue_version == 2 else f"builds/{capture_id}"


def _dbt_executable(python_executable: str) -> Path:
    return Path(python_executable).parent / "dbt"


def _receipt_body_path(relative: str) -> str:
    return (PurePosixPath(relative).parent / "body.bin").as_posix()


@dataclass(frozen=True)
class _OperationEvidence:
    capture_id: str
    catalogue_version: int
    root: str
    writer_token: int
    writer_lock: Any
    state: dict[str, Any]
    page_rows: tuple[dict[str, Any], ...]
    checkpoints: dict[str, str]
    verified_pages: dict[str, str] | None
    evidence: dict[str, str]
    state_sha256: str
    binding_sha256: str


@dataclass
class _VerifiedCapture:
    proof: _OperationEvidence
    receipt: dict[str, Any]
    build_sha256: str
    warehouse_sha256: str
    artifacts_sha256: str
    cutoff_rows: tuple[str, ...]
    cutoff_records: tuple[dict[str, Any], ...] | None
    binding_sha256: str


@dataclass
class _OperationContext:
    root: str
    writer_lock: Any
    owner_thread: int
    captures: dict[tuple[str, int], _VerifiedCapture]
    closed: bool = False


@contextmanager
def operation_context(store: Store):
    """Share freshly verified evidence only inside one writer-lock operation."""
    if not store.writer_locked_by_current_thread:
        raise CatalogueError("operation verification requires the canonical writer lock")
    context = _OperationContext(
        str(store.root.resolve()), store._writer_lock, threading.get_ident(), {}
    )
    try:
        yield context
    finally:
        context.closed = True
        context.captures.clear()


def _check_operation_context(store: Store, context: _OperationContext) -> None:
    if (
        context.closed
        or context.root != str(store.root.resolve())
        or context.writer_lock is not store._writer_lock
        or context.owner_thread != threading.get_ident()
        or not store.writer_locked_by_current_thread
    ):
        raise CatalogueError("operation verification context is outside its writer lease")


def _operation_evidence_binding(
    capture_id: str,
    catalogue_version: int,
    root: str,
    writer_token: int,
    page_rows: tuple[dict[str, Any], ...],
    checkpoints: dict[str, str],
    verified_pages: dict[str, str] | None,
    evidence: dict[str, str],
    state_sha256: str,
) -> str:
    return digest(
        json_bytes(
            {
                "capture_id": capture_id,
                "catalogue_version": catalogue_version,
                "root": root,
                "writer_token": writer_token,
                "page_rows": list(page_rows),
                "checkpoints": checkpoints,
                "verified_pages": verified_pages,
                "evidence": evidence,
                "state_sha256": state_sha256,
            }
        )
    )


def _historical_profile(state: dict[str, Any]) -> tuple[dict[str, Any], set[int]]:
    from kalshi_events.historical import (
        historical_selection,
        historical_task_indices,
    )

    selection = historical_selection(state, allow_legacy_incomplete=True)
    indices = historical_task_indices(state, allow_legacy_incomplete=True)
    return selection, indices


def _profile_capture(
    store: Store,
    capture_id: str,
    catalogue_version: int,
    state_hint: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if catalogue_version != 3:
        state = _complete_capture(store, capture_id, _state=state_hint)
        return state, page_receipts(store, state)
    from kalshi_events.capture import read_capture

    state = read_capture(store, capture_id)
    if state.get("capture_id") != capture_id or (
        state_hint is not None
        and digest(json_bytes(dict(state_hint))) != digest(json_bytes(dict(state)))
    ):
        raise CatalogueError("historical capture state changed during the operation")
    _, indices = _historical_profile(state)
    rows = [row for row in page_receipts(store, state) if row["index"] in indices]
    return state, rows


def _operation_evidence(
    store: Store,
    state: dict[str, Any],
    catalogue_version: int,
    verified_pages: dict[str, str] | None,
) -> _OperationEvidence:
    capture_id = state.get("capture_id")
    if (
        not isinstance(capture_id, str)
        or catalogue_version not in {1, 2, 3}
        or not store.writer_locked_by_current_thread
    ):
        raise CatalogueError("operation evidence lacks a capture identity")
    state, rows = _profile_capture(store, capture_id, catalogue_version, state)
    rows = tuple(rows)
    expected: set[str] = set()
    for row in rows:
        relative = row.get("relative")
        if not isinstance(relative, str):
            raise CatalogueError("invalid operation-local capture receipt inventory")
        expected.update({relative, _receipt_body_path(relative)})
    if verified_pages is not None and set(verified_pages) != expected:
        raise CatalogueError("verified evidence inventory does not match the capture journal")
    checkpoints = _checkpoint_inventory(store, state)
    evidence = {**checkpoints, **(verified_pages or {})}
    state_sha256 = digest(json_bytes(dict(state)))
    root = str(store.root.resolve())
    writer_token = id(store._writer_lock)
    binding = _operation_evidence_binding(
        capture_id,
        catalogue_version,
        root,
        writer_token,
        rows,
        checkpoints,
        verified_pages,
        evidence,
        state_sha256,
    )
    return _OperationEvidence(
        capture_id,
        catalogue_version,
        root,
        writer_token,
        store._writer_lock,
        state,
        rows,
        checkpoints,
        None if verified_pages is None else dict(verified_pages),
        evidence,
        state_sha256,
        binding,
    )


def _validate_operation_evidence(
    store: Store, proof: _OperationEvidence, capture_id: str, catalogue_version: int
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    if (
        proof.capture_id != capture_id
        or proof.catalogue_version != catalogue_version
        or proof.root != str(store.root.resolve())
        or not store.writer_locked_by_current_thread
        or store._writer_lock is not proof.writer_lock
        or proof.writer_token != id(store._writer_lock)
        or proof.state.get("capture_id") != capture_id
        or digest(json_bytes(dict(proof.state))) != proof.state_sha256
    ):
        raise CatalogueError("operation evidence identity mismatch")
    if (
        _operation_evidence_binding(
            proof.capture_id,
            proof.catalogue_version,
            proof.root,
            proof.writer_token,
            proof.page_rows,
            proof.checkpoints,
            proof.verified_pages,
            proof.evidence,
            proof.state_sha256,
        )
        != proof.binding_sha256
    ):
        raise CatalogueError("operation evidence binding mismatch")
    state, rows = _profile_capture(store, capture_id, catalogue_version, proof.state)
    if tuple(rows) != proof.page_rows:
        raise CatalogueError("operation evidence receipts do not match the capture profile")
    checkpoints = _checkpoint_inventory(store, state)
    if checkpoints != proof.checkpoints:
        raise CatalogueError("capture checkpoint changed during the operation")
    expected: set[str] = set()
    for row in proof.page_rows:
        relative = row.get("relative")
        if not isinstance(relative, str):
            raise CatalogueError("invalid operation-local capture receipt inventory")
        expected.update({relative, _receipt_body_path(relative)})
    if proof.verified_pages is not None and set(proof.verified_pages) != expected:
        raise CatalogueError("operation evidence receipts do not match the capture journal")
    if proof.evidence != {**checkpoints, **(proof.verified_pages or {})}:
        raise CatalogueError("operation evidence inventory mismatch")
    return state, proof.page_rows


def catalogue_capture(store: Store, capture_id: str) -> dict[str, Any]:
    state = _complete_capture(store, capture_id)
    if state["selected"] is not None:
        raise CatalogueError("targeted metadata captures never enter the catalogue warehouse")
    return state


def _checkpoint_inventory(store: Store, state: dict[str, Any]) -> dict[str, str]:
    base = f"captures/{state['capture_id']}"
    result = {}
    for filename, attribute in (
        ("state.json", "verified_snapshot_sha256"),
        ("pages.jsonl", "verified_journal_sha256"),
    ):
        store.check_deadline()
        relative = f"{base}/{filename}"
        checksum = file_digest(store.path(relative), store.limits.max_root_bytes)[0]
        if state.get("journal_version") == 2 and checksum != getattr(state, attribute, None):
            raise CatalogueError("capture checkpoint changed after journal verification")
        result[relative] = checksum
    return result


def evidence_inventory(
    store: Store,
    capture_id: str,
    *,
    catalogue_version: int = 1,
    verified_pages: dict[str, str] | None = None,
    _state: dict[str, Any] | None = None,
    _page_rows: list[dict[str, Any]] | None = None,
    _proof: _OperationEvidence | None = None,
) -> dict[str, str]:
    if _proof is not None:
        state, proof_rows = _validate_operation_evidence(
            store, _proof, capture_id, _proof.catalogue_version
        )
        catalogue_version = _proof.catalogue_version
        if verified_pages != _proof.verified_pages:
            raise CatalogueError("operation evidence does not match verified landing pages")
        _state = state
        _page_rows = list(proof_rows)
    if catalogue_version not in {1, 2, 3}:
        raise CatalogueError("catalogue version must be 1, 2, or 3")
    if _page_rows is None:
        state, rows = _profile_capture(store, capture_id, catalogue_version, _state)
    else:
        state, expected_rows = _profile_capture(store, capture_id, catalogue_version, _state)
        rows = _page_rows
        if rows != expected_rows:
            raise CatalogueError("operation-local receipts do not match the capture profile")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise CatalogueError("invalid operation-local capture receipt inventory")
    result = _checkpoint_inventory(store, state)
    if verified_pages is not None:
        expected = set()
        for row in rows:
            relative = row["relative"]
            expected.update({relative, _receipt_body_path(relative)})
        if set(verified_pages) != expected:
            raise CatalogueError("verified evidence inventory does not match the capture journal")
        return {**result, **verified_pages}
    for row in rows:
        store.check_deadline()
        relative = row["relative"]
        result[relative] = file_digest(store.path(relative), store.limits.max_root_bytes)[0]
        body = _receipt_body_path(relative)
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


def _land(
    store: Store,
    state: dict[str, Any],
    directory: Path,
    *,
    verified_pages: dict[str, str] | None = None,
    parent_index: ParentIndex | DiskParentIndex | None = None,
    task_indices: set[int] | None = None,
) -> int:
    """Land one capture into a fresh warehouse under `directory`. Returns observation count."""
    with metrics.phase("landing"):
        return _land_rows(
            store,
            state,
            directory,
            verified_pages=verified_pages,
            parent_index=parent_index,
            task_indices=task_indices,
        )


def _land_rows(
    store: Store,
    state: dict[str, Any],
    directory: Path,
    *,
    verified_pages: dict[str, str] | None = None,
    parent_index: ParentIndex | DiskParentIndex | None = None,
    task_indices: set[int] | None = None,
) -> int:
    capture_id = state["capture_id"]
    observation_count = 0
    store.check_storage()

    def rows():
        nonlocal observation_count
        for manifest, payload in capture_pages(
            store, state, evidence=verified_pages, task_indices=task_indices
        ):
            manifest = _scope_manifest(state, manifest)
            if parent_index is not None:
                parent_index.add_page(manifest, payload)
            for row in observations(((manifest, payload),)):
                store.check_deadline()
                observation_count += 1
                kind = row["entity_kind"]
                values = {
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
                    "task_scope": manifest.get("task_scope"),
                    "task_generation": str(manifest.get("task_generation", "")),
                }
                yield {key: _legacy_landing_text(value) for key, value in values.items()}

    database = directory / "warehouse.duckdb"
    batches = iter(_landing_arrow_batches(rows()))
    first = next(batches, None)
    if first is None:
        with duckdb.connect(str(database)) as connection:
            connection.execute("create schema if not exists landing")
            connection.execute(
                "create table if not exists landing.observations (observation_id varchar, capture_id varchar, entity_kind varchar, entity_ticker varchar, received_at varchar, provider_updated_at varchar, source_endpoint varchar, source_kind varchar, payload_json varchar, event_ticker varchar, series_ticker varchar, task_scope varchar, task_generation varchar)"
            )
    else:
        try:
            with dlt.config.values(
                {
                    # dlt resolves the extractor writer inside these source/resource sections.
                    "kalshi_catalogue.sources.kalshi_catalogue.kalshi_catalogue.data_writer.buffer_max_items": 1,
                    "kalshi_catalogue.normalize.data_writer.buffer_max_items": 1,
                    "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_load_id": True,
                    "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_id": True,
                }
            ):
                pipeline = dlt.pipeline(
                    pipeline_name="kalshi_catalogue",
                    pipelines_dir=str(directory / "landing"),
                    destination=dlt.destinations.duckdb(str(database)),
                    dataset_name="landing",
                )
                pipeline.run(
                    dlt.resource(
                        itertools.chain([first], batches),
                        name="observations",
                        primary_key="observation_id",
                        columns={
                            field.name: {
                                "data_type": "text",
                                "nullable": field.nullable,
                            }
                            for field in _LANDING_SCHEMA
                        },
                        write_disposition="merge",
                    ),
                    loader_file_format="parquet",
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
    with store.writer_guard():
        return _load_locked(store, capture_id)


def _load_locked(store: Store, capture_id: str) -> dict[str, Any]:
    """Land a catalogue capture into a throwaway warehouse; reports the landing without publishing."""
    store.require_writable()
    from kalshi_events.incremental import finish_pending_retirement

    finish_pending_retirement(store)
    state = catalogue_capture(store, capture_id)
    relative = f"tmp/load-{uuid.uuid4().hex}"
    directory = store.path(relative)
    durable_mkdir(directory.parent)
    directory.mkdir()
    try:
        verified_pages: dict[str, str] = {}
        observation_count = _land(store, state, directory, verified_pages=verified_pages)
    finally:
        shutil.rmtree(directory, ignore_errors=True)
        store.sync(relative)
    return {
        "capture_id": capture_id,
        "loaded_at": now(),
        "observation_count": observation_count,
        "evidence": evidence_inventory(
            store, capture_id, verified_pages=verified_pages, _state=state
        ),
    }


def dbt(
    store: Store,
    args: list[str],
    *,
    timeout: float | None = None,
    database: str = "warehouse.duckdb",
    artifacts: str | None = None,
    variables: dict[str, Any] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the packaged dbt project against `database` (root-relative)."""
    store.require_writable()
    runtime_relative = f"tmp/dbt-{uuid.uuid4().hex}"
    runtime = store.path(runtime_relative)
    durable_mkdir(runtime.parent)
    runtime.mkdir()
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
        executable = str(_dbt_executable(sys.executable))
        if not Path(executable).is_file():
            raise CatalogueError("dbt must be installed alongside the producer executable")
        environment = {
            key: value for key, value in os.environ.items() if not key.startswith(("DBT_", "DLT_"))
        }
        environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        with metrics.phase("dbt"):
            command = [
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
            ]
            if variables:
                command.extend(["--vars", json.dumps(variables, separators=(",", ":"))])
            result = subprocess.run(
                command,
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


SCOPE_KIND = {
    "series": "series",
    "events_live": "events",
    "events_combo": "events",
    "markets_live": "markets",
    "markets_combo": "markets",
    "markets_history": "markets",
}


def _task_scopes(state: dict[str, Any]) -> dict[str, str]:
    """Return explicit or legacy-reconstructed list scopes by task index."""
    scopes = state.get("task_scopes")
    if isinstance(scopes, dict) and scopes:
        return {str(index): scope for index, scope in scopes.items()}
    result: dict[str, str] = {}
    for index, work in enumerate(state.get("tasks", [])):
        if not isinstance(work, dict) or work.get("singular"):
            continue
        if work.get("parent_batch") is not None or "tickers" in work.get("params", {}):
            continue
        endpoint = work.get("endpoint")
        kind = work.get("kind")
        if endpoint == "/series" and kind == "series":
            scope = "series"
        elif endpoint == "/events" and kind == "events":
            scope = "events_live"
        elif endpoint == "/events/multivariate" and kind == "events":
            scope = "events_combo"
        elif endpoint == "/markets" and kind == "markets":
            scope = (
                "markets_combo"
                if work.get("params", {}).get("mve_filter") == "only"
                else "markets_live"
            )
        elif endpoint == "/historical/markets" and kind == "markets":
            scope = "markets_history"
        else:
            continue
        result[str(index)] = scope
    return result


def _scope_manifest(state: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    """Attach list scope/generation to older page manifests from task identity."""
    if manifest.get("task_scope") is not None:
        return manifest
    task_index = manifest.get("task_index")
    if isinstance(task_index, int) and not isinstance(task_index, bool):
        scope = _task_scopes(state).get(str(task_index))
        if scope is not None:
            work = state["tasks"][task_index]
            return {
                **manifest,
                "task_scope": scope,
                "task_generation": work.get("restarts", 0),
            }
    return manifest


def _capture_membership(state: dict[str, Any], index: ParentIndex) -> dict[str, set[str]]:
    result = {scope: set() for scope in SCOPE_KIND}
    for task_index, scope in _task_scopes(state).items():
        work = state["tasks"][int(task_index)]
        generation = work["restarts"]
        result.setdefault(scope, set()).update(
            index.scope_membership.get(scope, {}).get(generation, set())
        )
        if scope == "markets_live" and not work.get("params", {}).get("mve_filter"):
            result["markets_combo"].update(
                index.scope_membership.get("markets_combo", {}).get(generation, set())
            )
    return result


def _write_parent_outcomes(
    connection: duckdb.DuckDBPyConnection,
    capture_id: str,
    index: ParentIndex | DiskParentIndex,
    state: dict[str, Any],
) -> None:
    connection.execute(
        """create table if not exists landing.parent_eligibility (
             capture_id varchar, entity_kind varchar, entity_ticker varchar,
             status integer, receipt varchar)"""
    )
    connection.execute("delete from landing.parent_eligibility where capture_id = ?", [capture_id])
    _, outcomes = index.effective_parent_evidence(state)
    rows = [
        (capture_id, kind, identity, status, page_id)
        for (kind, identity), (status, page_id, _) in sorted(outcomes.items())
    ]
    if rows:
        connection.executemany(
            "insert into landing.parent_eligibility values (?, ?, ?, ?, ?)", rows
        )


def _write_membership(
    connection: duckdb.DuckDBPyConnection, membership: dict[str, set[str]]
) -> None:
    connection.execute(
        """create table if not exists landing.membership (
             scope varchar, entity_kind varchar, entity_ticker varchar)"""
    )
    connection.execute("delete from landing.membership")
    rows = [
        (scope, SCOPE_KIND[scope], ticker)
        for scope, values in sorted(membership.items())
        for ticker in sorted(values)
        if scope in SCOPE_KIND
    ]
    if rows:
        connection.executemany("insert into landing.membership values (?, ?, ?)", rows)


def _ensure_affected(connection: duckdb.DuckDBPyConnection) -> None:
    for table, column in (
        ("affected_series", "series_ticker"),
        ("affected_events", "event_ticker"),
        ("affected_markets", "market_ticker"),
    ):
        connection.execute(f"create table if not exists landing.{table} ({column} varchar)")
        connection.execute(f"delete from landing.{table}")


def _insert_affected(connection: duckdb.DuckDBPyConnection, table: str, values: set[str]) -> None:
    if values:
        connection.executemany(
            f"insert into landing.{table} values (?)", [(value,) for value in sorted(values)]
        )


def _read_membership(connection: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    try:
        rows = connection.execute("select scope, entity_ticker from landing.membership").fetchall()
    except duckdb.CatalogException:
        return {scope: set() for scope in SCOPE_KIND}
    result = {scope: set() for scope in SCOPE_KIND}
    for scope, identity in rows:
        if scope in SCOPE_KIND:
            result[scope].add(identity)
    return result


def _copy_closed_database_impl(
    store: Store,
    source_relative: str,
    staging: Path,
    *,
    expected_sha256: str,
) -> None:
    source = store.path(source_relative)
    source_wal = store.path(source_relative + ".wal")
    try:
        staging_relative = str(staging.relative_to(store.root))
    except ValueError as exc:
        raise CatalogueError("database staging directory is outside the operator root") from exc
    staging = store.path(staging_relative)
    destination_relative = str(Path(staging_relative) / "warehouse.duckdb")
    destination = store.path(destination_relative)
    destination_wal = store.path(destination_relative + ".wal")
    if destination.exists() or destination_wal.exists():
        raise CatalogueError("staged warehouse already exists")
    if source_wal.exists():
        raise CatalogueError("baseline warehouse has an uncheckpointed WAL")

    def closed_digest(path: Path) -> tuple[str, int]:
        if path.is_symlink() or not path.is_file():
            raise CatalogueError("baseline warehouse is not a regular file")
        initial_size = path.stat().st_size
        if initial_size > store.limits.max_database_bytes:
            raise CatalogueError("baseline warehouse exceeds the configured database limit")
        checksum = hashlib.sha256()
        size = 0
        metrics.add("hashes")
        metrics.add("file_reads")
        with metrics.phase("hashing"), path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                store.check_deadline()
                size += len(chunk)
                if size > store.limits.max_database_bytes:
                    raise CatalogueError("baseline warehouse grew beyond the database limit")
                checksum.update(chunk)
                metrics.add("hash_bytes", len(chunk))
        if size != initial_size or path.stat().st_size != initial_size:
            raise CatalogueError("baseline warehouse changed while it was read")
        return checksum.hexdigest(), size

    def inventory(connection: duckdb.DuckDBPyConnection, database: str) -> dict[str, list[tuple]]:
        queries = {
            "schemas": (
                "select schema_name, parent_schema, comment, tags, sql from duckdb_schemas() "
                "where database_name = ? and not internal order by schema_name"
            ),
            "tables": (
                "select schema_name, table_name, comment, tags, sql from duckdb_tables() "
                "where database_name = ? and not internal and not temporary "
                "order by schema_name, table_name"
            ),
            "columns": (
                "select schema_name, table_name, column_name, column_index, comment, tags, "
                "column_default, is_nullable, data_type, data_type_id, "
                "character_maximum_length, numeric_precision, numeric_precision_radix, "
                "numeric_scale, is_generated, generation_expression "
                "from duckdb_columns() where database_name = ? and not internal "
                "order by schema_name, table_name, column_index"
            ),
            "views": (
                "select schema_name, view_name, comment, tags, sql from duckdb_views() "
                "where database_name = ? and not internal and not temporary "
                "order by schema_name, view_name"
            ),
            "indexes": (
                "select schema_name, index_name, table_name, comment, tags, is_unique, "
                "is_primary, expressions, sql from duckdb_indexes() where database_name = ? "
                "order by schema_name, index_name"
            ),
            "constraints": (
                "select schema_name, table_name, constraint_type, constraint_text, "
                "expression, constraint_column_indexes, "
                "constraint_column_names, constraint_name, referenced_table, "
                "referenced_column_names "
                "from duckdb_constraints() where database_name = ? "
                "order by schema_name, table_name, constraint_name, constraint_type, "
                "constraint_text, constraint_column_names"
            ),
            "sequences": (
                "select schema_name, sequence_name, comment, tags, start_value, min_value, "
                "max_value, increment_by, cycle, last_value, sql from duckdb_sequences() "
                "where database_name = ? order by schema_name, sequence_name"
            ),
            "types": (
                "select schema_name, type_name, comment, tags, logical_type, type_category, "
                "labels, parameters, parameter_types "
                "from duckdb_types() where database_name = ? and not internal "
                "order by schema_name, type_name"
            ),
            "macros": (
                "select schema_name, function_name, function_type, comment, tags, "
                "return_type, parameters, parameter_types, varargs, has_side_effects, "
                "macro_definition from duckdb_functions() "
                "where database_name = ? and not internal "
                "and function_type in ('macro', 'table_macro') and schema_name <> 'temp' "
                "order by schema_name, function_name, function_type, parameters"
            ),
        }
        return {
            name: connection.execute(query, [database]).fetchall()
            for name, query in queries.items()
        }

    def quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    source_hash, size = closed_digest(source)
    if source_hash != expected_sha256:
        raise CatalogueError("baseline warehouse differs from its certified identity")
    store.check_deadline()
    store.sync_file(source_relative)
    store.check_deadline()
    usage = store.usage()
    remaining_database = store.limits.max_database_bytes - usage["database_bytes"]
    remaining_temp = store.limits.max_temp_bytes - usage["temporary_bytes"]
    remaining_root = store.limits.max_root_bytes - usage["retained_bytes"]
    output_allowance = min(remaining_database, remaining_temp, remaining_root)
    if output_allowance < size:
        raise CatalogueError("baseline copy exceeds the remaining database storage allowance")

    native_version = "v2.0.0+"
    source_alias = "migration_source"
    destination_alias = destination.stem
    source_literal = "'" + str(source).replace("'", "''") + "'"

    # A v2.0 file is already native, so retain the certified byte-copy fast path.
    store.check_deadline()
    probe = duckdb.connect(config={"memory_limit": DBT_MEMORY_LIMIT, "threads": str(DBT_THREADS)})
    try:
        with interrupt_at_deadline(store, probe.interrupt):
            probe.execute(
                f"ATTACH {source_literal} AS {quote_identifier(source_alias)} (READ_ONLY)"
            )
            storage_version = probe.execute(
                "select tags['storage_version'] from duckdb_databases() where database_name = ?",
                [source_alias],
            ).fetchone()[0]
    except duckdb.InterruptException as exc:
        raise BudgetExceeded("operation deadline exhausted during source storage probe") from exc
    finally:
        probe.close()
    store.check_deadline()
    if storage_version != native_version and not (
        isinstance(storage_version, str)
        and storage_version.startswith("v1.")
        and storage_version.endswith("+")
    ):
        raise CatalogueError("baseline warehouse uses an unsupported DuckDB storage version")

    reservation = 0
    spill_directory: Path | None = None
    try:
        if storage_version == native_version:
            if usage["database_bytes"] + size > store.limits.max_database_bytes:
                raise CatalogueError("baseline copy exceeds the configured database storage limit")
            store.reserve_storage(size)
            reservation = size
            copied = 0
            checksum = hashlib.sha256()
            destination.parent.mkdir(parents=True, exist_ok=True)
            metrics.add("hashes")
            metrics.add("file_reads")
            with (
                metrics.phase("hashing"),
                source.open("rb") as input_stream,
                destination.open("xb") as output_stream,
            ):
                while chunk := input_stream.read(1024 * 1024):
                    store.check_deadline()
                    output_stream.write(chunk)
                    checksum.update(chunk)
                    copied += len(chunk)
                    metrics.add("hash_bytes", len(chunk))
            copied_hash = checksum.hexdigest()
            if copied != size or copied_hash != expected_sha256:
                raise CatalogueError(
                    "copied baseline warehouse differs from its certified identity"
                )
        else:
            spill_allowance = min(remaining_temp - size, remaining_root - size)
            if spill_allowance < 1:
                raise BudgetExceeded(
                    "no bounded temporary storage is available for database conversion"
                )
            requested_reservation = size + spill_allowance
            store.reserve_storage(requested_reservation)
            reservation = requested_reservation
            spill_relative = f"tmp/database-copy-{uuid.uuid4().hex}"
            spill_directory = store.path(spill_relative)
            durable_mkdir(spill_directory.parent)
            spill_directory.mkdir()
            destination.parent.mkdir(parents=True, exist_ok=True)
            target_literal = "'" + str(destination).replace("'", "''") + "'"
            with (
                duckdb.connect(
                    config={
                        "memory_limit": DBT_MEMORY_LIMIT,
                        "threads": str(DBT_THREADS),
                        "temp_directory": str(spill_directory),
                        "max_temp_directory_size": f"{spill_allowance}B",
                    }
                ) as connection,
                interrupt_at_deadline(store, connection.interrupt),
            ):
                try:
                    connection.execute(
                        f"ATTACH {source_literal} AS {quote_identifier(source_alias)} (READ_ONLY)"
                    )
                    connection.execute(
                        f"ATTACH {target_literal} AS {quote_identifier(destination_alias)}"
                    )
                    target_version = connection.execute(
                        "select tags['storage_version'] from duckdb_databases() "
                        "where database_name = ?",
                        [destination_alias],
                    ).fetchone()[0]
                    if target_version != native_version:
                        raise CatalogueError(
                            "database conversion did not create DuckDB 2.0 storage"
                        )
                    connection.execute(
                        "COPY FROM DATABASE "
                        f"{quote_identifier(source_alias)} TO {quote_identifier(destination_alias)}"
                    )
                    source_catalog = inventory(connection, source_alias)
                    target_catalog = inventory(connection, destination_alias)
                    if source_catalog != target_catalog:
                        raise CatalogueError("converted warehouse catalog differs from its source")
                    for table_row in source_catalog["tables"]:
                        schema, table = table_row[:2]
                        source_table = ".".join(
                            quote_identifier(part) for part in (source_alias, schema, table)
                        )
                        target_table = ".".join(
                            quote_identifier(part) for part in (destination_alias, schema, table)
                        )
                        for left, right in (
                            (source_table, target_table),
                            (target_table, source_table),
                        ):
                            store.check_deadline()
                            if connection.execute(
                                f"select 1 from ((select * from {left}) except all "
                                f"(select * from {right})) limit 1"
                            ).fetchone():
                                raise CatalogueError(
                                    "converted warehouse table data differs from its source"
                                )
                    connection.execute(f"DETACH {quote_identifier(source_alias)}")
                    for view_row in source_catalog["views"]:
                        schema, view = view_row[:2]
                        object_name = ".".join(
                            quote_identifier(part) for part in (destination_alias, schema, view)
                        )
                        try:
                            connection.execute(
                                f"EXPLAIN SELECT * FROM {object_name} LIMIT 0"
                            ).fetchall()
                        except duckdb.InterruptException:
                            raise
                        except duckdb.Error as exc:
                            raise CatalogueError(
                                "converted warehouse contains an unresolved view"
                            ) from exc
                    for macro_row in source_catalog["macros"]:
                        schema, macro, function_type = macro_row[:3]
                        parameters = macro_row[6]
                        object_name = ".".join(
                            quote_identifier(part) for part in (destination_alias, schema, macro)
                        )
                        arguments = ", ".join("NULL" for _ in parameters)
                        invocation = f"{object_name}({arguments})"
                        try:
                            if function_type == "table_macro":
                                connection.execute(
                                    f"EXPLAIN SELECT * FROM {invocation} LIMIT 0"
                                ).fetchall()
                            else:
                                connection.execute(
                                    f"EXPLAIN SELECT {invocation} LIMIT 0"
                                ).fetchall()
                        except duckdb.InterruptException:
                            raise
                        except duckdb.Error as exc:
                            raise CatalogueError(
                                "converted warehouse contains an unresolved macro"
                            ) from exc
                    connection.execute(f"CHECKPOINT {quote_identifier(destination_alias)}")
                except duckdb.InterruptException as exc:
                    raise BudgetExceeded(
                        "operation deadline exhausted during database conversion"
                    ) from exc
            store.check_deadline()
            if destination_wal.exists():
                raise CatalogueError("converted warehouse has an uncheckpointed WAL")

        if source_wal.exists():
            raise CatalogueError("baseline warehouse gained an uncheckpointed WAL during copy")
        after_hash, after_size = closed_digest(source)
        if after_hash != source_hash or after_size != size:
            raise CatalogueError("baseline warehouse changed while it was copied")
        if destination_wal.exists():
            raise CatalogueError("copied warehouse has an uncheckpointed WAL")
        if storage_version != native_version:
            store.check_deadline()
            verify = duckdb.connect(
                config={"memory_limit": DBT_MEMORY_LIMIT, "threads": str(DBT_THREADS)}
            )
            try:
                with interrupt_at_deadline(store, verify.interrupt):
                    target_literal = "'" + str(destination).replace("'", "''") + "'"
                    verify.execute(
                        f"ATTACH {target_literal} AS {quote_identifier(destination_alias)} "
                        "(READ_ONLY)"
                    )
                    target_version = verify.execute(
                        "select tags['storage_version'] from duckdb_databases() "
                        "where database_name = ?",
                        [destination_alias],
                    ).fetchone()[0]
            except duckdb.InterruptException as exc:
                raise BudgetExceeded(
                    "operation deadline exhausted during destination storage verification"
                ) from exc
            finally:
                verify.close()
            store.check_deadline()
            if target_version != native_version:
                raise CatalogueError("converted warehouse is not in DuckDB 2.0 storage")
        store.check_deadline()
        fsync_file(destination)
        store.check_deadline()
        fsync_directory(staging)
        store.check_deadline()
    except BaseException:
        destination.unlink(missing_ok=True)
        destination_wal.unlink(missing_ok=True)
        raise
    finally:
        if spill_directory is not None:
            shutil.rmtree(spill_directory, ignore_errors=True)
            try:
                store.check_deadline()
            finally:
                try:
                    store.sync("tmp")
                finally:
                    if reservation:
                        store.release_storage(reservation)
                        reservation = 0
            store.check_deadline()
        elif reservation:
            store.release_storage(reservation)

    staging_relative = str(staging.relative_to(store.root))
    try:
        store.check_deadline()
        store.sync(staging_relative)
        store.check_deadline()
        store.check_storage()
        store.check_deadline()
    except BaseException:
        destination.unlink(missing_ok=True)
        destination_wal.unlink(missing_ok=True)
        store.sync(staging_relative)
        raise


def _copy_closed_database(
    store: Store,
    source_relative: str,
    staging: Path,
    *,
    expected_sha256: str,
) -> None:
    with metrics.phase("warehouse_copy"):
        _copy_closed_database_impl(
            store,
            source_relative,
            staging,
            expected_sha256=expected_sha256,
        )


def _projection_membership(
    store: Store,
    state: dict[str, Any],
    index: ParentIndex | DiskParentIndex,
    connection: duckdb.DuckDBPyConnection,
    previous_membership: dict[str, set[str]] | None = None,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    if isinstance(index, DiskParentIndex):
        connection.execute(
            """create table if not exists landing.membership (
                 scope varchar, entity_kind varchar, entity_ticker varchar)"""
        )
        connection.execute("delete from landing.membership")
        batch = []
        for scope, identity in index.stream_membership(state):
            batch.append((scope, SCOPE_KIND[scope], identity))
            if len(batch) == 4096:
                connection.executemany("insert into landing.membership values (?,?,?)", batch)
                batch.clear()
                store.check_deadline()
        if batch:
            connection.executemany("insert into landing.membership values (?,?,?)", batch)
        connection.execute(
            """create table if not exists landing.parent_eligibility (
                 capture_id varchar, entity_kind varchar, entity_ticker varchar,
                 status integer, receipt varchar)"""
        )
        connection.execute(
            "delete from landing.parent_eligibility where capture_id = ?", [state["capture_id"]]
        )
        outcomes = []
        for kind, identity, status, page_id in index.stream_eligibility(state):
            outcomes.append((state["capture_id"], kind, identity, status, page_id))
            if len(outcomes) == 4096:
                connection.executemany(
                    "insert into landing.parent_eligibility values (?,?,?,?,?)", outcomes
                )
                outcomes.clear()
                store.check_deadline()
        if outcomes:
            connection.executemany(
                "insert into landing.parent_eligibility values (?,?,?,?,?)", outcomes
            )
        return {}, {}
    previous = previous_membership or _read_membership(connection)
    incoming = _capture_membership(state, index)
    if state.get("scope") is not None:
        membership = incoming
    elif state.get("incremental"):
        membership = {scope: set(values) for scope, values in previous.items()}
        for scope, values in incoming.items():
            if scope in {"events_live", "markets_live"}:
                membership.setdefault(scope, set()).update(values)
            else:
                membership[scope] = set(values)
    else:
        membership = incoming
    changes = {
        scope: previous.get(scope, set()) ^ membership.get(scope, set()) for scope in SCOPE_KIND
    }
    _write_membership(connection, membership)
    _write_parent_outcomes(connection, state["capture_id"], index, state)
    return membership, changes


def _affected_keys(
    connection: duckdb.DuckDBPyConnection,
    state: dict[str, Any],
    membership_changes: dict[str, set[str]],
) -> tuple[set[str], set[str], set[str]]:
    """Find the dependency closure that must be replaced after this delta.

    Membership changes and direct observations seed the projection. Parent
    availability is also a seed: a fresh 404 changes current eligibility even
    though it deliberately adds no observation to history.
    """
    capture_id = state["capture_id"]
    series = set(membership_changes.get("series", set()))
    events = set(membership_changes.get("events_live", set())) | set(
        membership_changes.get("events_combo", set())
    )
    markets = set().union(
        membership_changes.get("markets_live", set()),
        membership_changes.get("markets_combo", set()),
        membership_changes.get("markets_history", set()),
    )
    direct = {"series": set(), "events": set(), "markets": set()}
    for kind, target in direct.items():
        target.update(
            row[0]
            for row in connection.execute(
                """select distinct entity_ticker from landing.observations
                   where capture_id = ? and entity_kind = ? and source_kind = 'direct'""",
                [capture_id, kind],
            ).fetchall()
        )
    series.update(direct["series"])
    events.update(direct["events"])
    markets.update(direct["markets"])

    # Capture-local 200 and 404 outcomes are projection inputs even when a 404
    # adds no observation to history. Only eligibility transitions propagate
    # to dependent entities; routine full-scope refreshes must stay narrow.
    eligibility = connection.execute(
        "select entity_kind, entity_ticker, status from landing.parent_eligibility where capture_id = ?",
        [capture_id],
    ).fetchall()
    current_series = {
        row[0]
        for row in connection.execute("select series_ticker from main.current_series").fetchall()
    }
    current_events = {
        row[0]
        for row in connection.execute("select event_ticker from main.current_events").fetchall()
    }
    structural_series = set(membership_changes.get("series", set()))
    structural_events = set(membership_changes.get("events_live", set())) | set(
        membership_changes.get("events_combo", set())
    )
    structural_markets = set().union(
        membership_changes.get("markets_live", set()),
        membership_changes.get("markets_combo", set()),
        membership_changes.get("markets_history", set()),
    )
    for kind, identity, status in eligibility:
        if kind == "events":
            events.add(identity)
            if (identity in current_events) != (status == 200):
                structural_events.add(identity)
        elif kind == "series":
            series.add(identity)
            if (identity in current_series) != (status == 200):
                structural_series.add(identity)

    prior_event_series = {
        event: parent
        for event, parent in connection.execute(
            "select event_ticker, series_ticker from main.current_events"
        ).fetchall()
    }
    latest_event_series = {
        event: parent
        for event, parent in connection.execute(
            """select entity_ticker, series_ticker from landing.observations
               where entity_kind='events' and source_kind='direct'
               qualify row_number() over (
                 partition by entity_ticker order by received_at desc, observation_id desc
               ) = 1"""
        ).fetchall()
    }
    event_parents = dict(prior_event_series)
    event_parents.update(latest_event_series)
    # Event direct updates refresh their own rows. They propagate only when
    # current presence or the series relationship actually changes.
    for event in direct["events"]:
        old_parent = prior_event_series.get(event)
        new_parent = latest_event_series.get(event)
        if event not in current_events or old_parent != new_parent:
            structural_events.add(event)
            series.update(parent for parent in (old_parent, new_parent) if parent)
            structural_series.update(parent for parent in (old_parent, new_parent) if parent)

    # A series presence transition affects every event that points to it.
    structural_events.update(
        event for event, parent in event_parents.items() if parent in structural_series
    )
    events.update(structural_events)
    for event in structural_events:
        series.update(
            parent
            for parent in (prior_event_series.get(event), latest_event_series.get(event))
            if parent
        )

    market_membership = {
        row[0]
        for row in connection.execute(
            "select distinct entity_ticker from landing.membership where entity_kind='markets'"
        ).fetchall()
    }
    prior_market_parents = {
        market: event
        for market, event in connection.execute(
            "select market_ticker, event_ticker from main.current_markets"
        ).fetchall()
    }
    latest_market_parents = {
        market: event
        for market, event in connection.execute(
            """select entity_ticker, event_ticker from landing.observations
               where entity_kind='markets' and source_kind='direct'
               qualify row_number() over (
                 partition by entity_ticker order by received_at desc, observation_id desc
               ) = 1"""
        ).fetchall()
    }
    market_parents = dict(prior_market_parents)
    market_parents.update(latest_market_parents)
    affected_events = set(events)
    affected_markets = set(markets)

    for market in direct["markets"]:
        old_parent = prior_market_parents.get(market)
        new_parent = latest_market_parents.get(market)
        if market not in prior_market_parents or old_parent != new_parent:
            structural_markets.add(market)
            structural_events.update(parent for parent in (old_parent, new_parent) if parent)
            affected_events.update(parent for parent in (old_parent, new_parent) if parent)

    # Parent reassignment/removal on any changed market affects both its old
    # and new parent event, even when those parents came from lookup-only
    # closure and have no direct observation in this capture.
    for market in structural_markets:
        parents = {
            parent
            for parent in (prior_market_parents.get(market), latest_market_parents.get(market))
            if parent
        }
        affected_events.update(parents)
        structural_events.update(parents)

    # Close dependencies to a fixed point. Combo markets may depend on either a
    # market or native event, and their own event parent may then change too.
    changed = True
    while changed:
        before = (len(series), len(affected_events), len(affected_markets))
        structural_events.update(
            event for event, parent in event_parents.items() if parent in structural_series
        )
        affected_events.update(structural_events)
        for event in structural_events:
            series.update(
                parent
                for parent in (prior_event_series.get(event), latest_event_series.get(event))
                if parent
            )
        affected_markets.update(
            market
            for market in market_membership
            if market_parents.get(market) in structural_events
        )
        if affected_markets or affected_events:
            affected_markets.update(
                row[0]
                for row in connection.execute(
                    """select distinct m.market_ticker
                       from main.current_markets m,
                            lateral json_each(json_extract(m.payload_json, '$.mve_selected_legs')) leg
                       where json_extract_string(leg.value, '$.market_ticker') in (
                           select unnest(?)
                       ) or json_extract_string(leg.value, '$.event_ticker') in (
                           select unnest(?)
                       )""",
                    [sorted(affected_markets), sorted(affected_events)],
                ).fetchall()
            )
        for market in structural_markets | affected_markets:
            affected_events.update(
                parent
                for parent in (prior_market_parents.get(market), latest_market_parents.get(market))
                if parent
            )
        changed = before != (len(series), len(affected_events), len(affected_markets))
    _ensure_affected(connection)
    _insert_affected(connection, "affected_series", series)
    _insert_affected(connection, "affected_events", affected_events)
    _insert_affected(connection, "affected_markets", affected_markets)
    metrics.add("affected_series", len(series))
    metrics.add("affected_events", len(affected_events))
    metrics.add("affected_markets", len(affected_markets))
    return series, affected_events, affected_markets


def _delete_affected(connection: duckdb.DuckDBPyConnection) -> None:
    for table, key, source in (
        ("current_series", "series_ticker", "affected_series"),
        ("current_events", "event_ticker", "affected_events"),
        ("current_markets", "market_ticker", "affected_markets"),
    ):
        connection.execute(
            f"delete from main.{table} where {key} in (select {key} from landing.{source})"
        )
    connection.execute(
        """delete from main.relationships
           where (child_kind='events' and child_ticker in
                    (select event_ticker from landing.affected_events))
              or (child_kind='markets' and child_ticker in
                    (select market_ticker from landing.affected_markets))"""
    )
    connection.execute(
        "delete from main.combo_legs where combo_market_ticker in "
        "(select market_ticker from landing.affected_markets)"
    )


def membership_fingerprint(
    store: Store,
    capture_id: str,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> str:
    """Hash sorted certified scope membership without materializing the relation."""
    directory = build_directory(capture_id, 2)
    database = store.path(f"{directory}/warehouse.duckdb")
    if store.path(f"{directory}/warehouse.duckdb.wal").exists():
        raise CatalogueError("v2 warehouse has an uncheckpointed WAL")
    checksum = hashlib.sha256()
    owns_connection = connection is None
    spill_directory: Path | None = None
    spill_budget = 0
    if connection is None:
        usage = store.usage()
        spill_budget = min(
            store.limits.max_temp_bytes - usage["temporary_bytes"],
            store.limits.max_root_bytes - usage["retained_bytes"],
        )
        if spill_budget < 1:
            raise BudgetExceeded(
                "no bounded temporary storage is available for membership verification"
            )
        store.reserve_storage(spill_budget)
        spill_directory = store.path(f"tmp/membership-verify-{uuid.uuid4().hex}")
        spill_directory.mkdir(parents=True)
        connection = duckdb.connect(
            str(database),
            read_only=True,
            config={
                "memory_limit": "512MB",
                "temp_directory": str(spill_directory),
                "max_temp_directory_size": f"{spill_budget}B",
            },
        )
    try:
        deadline = (
            interrupt_at_deadline(store, connection.interrupt) if owns_connection else nullcontext()
        )
        with deadline:
            try:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "select table_name from information_schema.tables where table_schema='landing'"
                    ).fetchall()
                }
                if "membership" not in tables:
                    raise CatalogueError("v2 warehouse lacks certified scope membership")
                cursor = connection.execute(
                    "select scope, entity_kind, entity_ticker from landing.membership "
                    "order by scope, entity_kind, entity_ticker"
                )
                while rows := cursor.fetchmany(4096):
                    store.check_deadline()
                    for row in rows:
                        checksum.update(json_bytes(list(row)).rstrip(b"\n"))
                        checksum.update(b"\n")
                return checksum.hexdigest()
            except duckdb.InterruptException as exc:
                raise BudgetExceeded("operation deadline exhausted") from exc
    finally:
        if owns_connection:
            connection.close()
            if spill_directory is not None:
                shutil.rmtree(spill_directory, ignore_errors=True)
                store.sync("tmp")
                store.release_storage(spill_budget)


def build(store: Store, capture_id: str) -> dict[str, Any]:
    with store.writer_guard():
        return _build_locked(store, capture_id)


def _build_locked(store: Store, capture_id: str) -> dict[str, Any]:
    """Build a fresh, certified warehouse for one complete catalogue capture."""
    store.require_writable()
    if store.scope is not None:
        from kalshi_events.incremental import finish_pending_retirement

        finish_pending_retirement(store)
        state = catalogue_capture(store, capture_id)
        if state.get("incremental"):
            return _build_delta(store, state)
        return _full_v2_build(store, state, rebuild=False)
    from kalshi_events.incremental import finish_pending_retirement

    finish_pending_retirement(store)
    _recover_build(store, capture_id, 1)
    state = catalogue_capture(store, capture_id)
    if state.get("incremental"):
        return _build_delta(store, state)
    store.check_deadline()
    staging_relative = f"tmp/build-{uuid.uuid4().hex}"
    staging = store.path(staging_relative)
    durable_mkdir(staging.parent)
    staging.mkdir()
    try:
        verified_pages: dict[str, str] = {}
        _land(store, state, staging, verified_pages=verified_pages)
        dbt(
            store,
            ["build"],
            database=f"{staging_relative}/warehouse.duckdb",
            artifacts=f"{staging_relative}/build-artifacts.json",
        )
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, 1, verified_pages)
        receipt = {
            "version": 1,
            "capture_id": capture_id,
            "built_at": now(),
            "scope": "catalogue",
            "evidence": operation_evidence.evidence,
            "warehouse_sha256": file_digest(
                staging / "warehouse.duckdb", store.limits.max_database_bytes
            )[0],
            "dbt_artifacts_sha256": digest(regular_bytes(staging / "build-artifacts.json")),
        }
        store.write(f"{staging_relative}/build.json", receipt)
        fsync_directory(staging)
        receipt = _install_build(store, capture_id, 1, staging, operation_evidence)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        store.sync(staging_relative)
        store.sync(build_directory(capture_id, 1))
    store.check_storage()
    return receipt


def _recover_build(store: Store, capture_id: str, catalogue_version: int) -> None:
    if catalogue_version not in {1, 2, 3}:
        raise CatalogueError("catalogue version must be 1, 2, or 3")
    if not store.writer_locked_by_current_thread:
        raise CatalogueError("build recovery requires the canonical writer lock")
    destination = store.path(build_directory(capture_id, catalogue_version))
    backup_relative = f"tmp/replaced-{capture_id}-v{catalogue_version}"
    backup = store.path(backup_relative)
    if not backup.exists():
        return
    if destination.exists():
        try:
            certified(store, capture_id, catalogue_version=catalogue_version)
        except BudgetExceeded:
            raise
        except (CatalogueError, OSError):
            try:
                certified(
                    store,
                    capture_id,
                    catalogue_version=catalogue_version,
                    _directory=backup,
                )
            except BudgetExceeded:
                raise
            except (CatalogueError, OSError) as exc:
                raise CatalogueError(
                    "neither build nor its replacement backup is certified"
                ) from exc
            shutil.rmtree(destination)
            os.rename(backup, destination)
            fsync_directory(destination.parent)
            fsync_directory(backup.parent)
        else:
            shutil.rmtree(backup)
            fsync_directory(backup.parent)
    else:
        certified(store, capture_id, catalogue_version=catalogue_version, _directory=backup)
        os.rename(backup, destination)
        fsync_directory(destination.parent)
        fsync_directory(backup.parent)
    store.sync(build_directory(capture_id, catalogue_version))
    store.sync(backup_relative)


def _install_build(
    store: Store,
    capture_id: str,
    catalogue_version: int,
    staging: Path,
    proof: _OperationEvidence | None = None,
) -> dict[str, Any]:
    if catalogue_version not in {1, 2, 3}:
        raise CatalogueError("catalogue version must be 1, 2, or 3")
    destination_relative = build_directory(capture_id, catalogue_version)
    destination = store.path(destination_relative)
    durable_mkdir(destination.parent)
    backup_relative = f"tmp/replaced-{capture_id}-v{catalogue_version}"
    backup = store.path(backup_relative)
    if backup.exists():
        _recover_build(store, capture_id, catalogue_version)
    previous = destination.exists()
    if previous:
        os.rename(destination, backup)
        fsync_directory(destination.parent)
        fsync_directory(backup.parent)
    try:
        os.rename(staging, destination)
        fsync_directory(staging.parent)
        fsync_directory(destination.parent)
        store.sync(destination_relative)
        receipt = certified(
            store,
            capture_id,
            catalogue_version=catalogue_version,
            _proof=proof,
        )
    except BudgetExceeded:
        raise
    except BaseException:
        if previous and backup.exists():
            # Never discard the installed candidate until the rollback copy is known-good.
            # If validation is interrupted or the backup is corrupt, retain both directories;
            # the next writer can certify the destination and resolve the exact backup name.
            certified(store, capture_id, catalogue_version=catalogue_version, _directory=backup)
            if destination.exists():
                shutil.rmtree(destination)
            os.rename(backup, destination)
            fsync_directory(destination.parent)
            fsync_directory(backup.parent)
        elif destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        raise
    if backup.exists():
        shutil.rmtree(backup)
        fsync_directory(backup.parent)
    store.sync(destination_relative)
    store.sync(backup_relative)
    return receipt


def _projection_index(
    store: Store,
    state: dict[str, Any],
    *,
    verified_pages: dict[str, str] | None = None,
    index: ParentIndex | DiskParentIndex | None = None,
) -> ParentIndex | DiskParentIndex:
    index = index or ParentIndex()
    for manifest, payload in capture_pages(store, state, evidence=verified_pages):
        index.add_page(_scope_manifest(state, manifest), payload)
    return index


def _parent_closure_is_fresh(
    connection: duckdb.DuckDBPyConnection,
    state: dict[str, Any],
    index: ParentIndex | DiskParentIndex,
) -> None:
    if isinstance(index, DiskParentIndex):
        if (
            connection.execute(
                "select 1 from landing.membership where scope in ('markets_live','markets_combo','markets_history') limit 1"
            ).fetchone()
            is None
        ):
            return
        errors = connection.execute(
            """with market_winners as (
                 select entity_ticker,event_ticker from landing.observations
                 where entity_kind='markets' and source_kind='direct'
                   and entity_ticker in (select entity_ticker from landing.membership
                     where scope in ('markets_live','markets_combo','markets_history'))
                 qualify row_number() over(partition by entity_ticker
                   order by received_at desc,observation_id desc)=1
               )
               select 1 from market_winners m
               left join landing.membership e on e.entity_ticker=m.event_ticker
                 and e.scope in ('events_live','events_combo')
               left join landing.parent_eligibility p on p.capture_id=?
                 and p.entity_kind='events' and p.entity_ticker=m.event_ticker
               where m.event_ticker is not null and e.entity_ticker is null
                 and p.entity_ticker is null limit 1""",
            [state["capture_id"]],
        ).fetchone()
        if errors:
            raise CatalogueError(
                "full capture lacks fresh parent evidence for final membership; capture a new full baseline"
            )
        errors = connection.execute(
            """with active_events as (
                 select entity_ticker from landing.membership where scope in ('events_live','events_combo')
                 union select entity_ticker from landing.parent_eligibility
                   where capture_id=? and entity_kind='events' and status=200
               ), event_winners as (
                 select entity_ticker,series_ticker from landing.observations
                 where entity_kind='events' and source_kind='direct'
                   and entity_ticker in (select entity_ticker from active_events)
                 qualify row_number() over(partition by entity_ticker
                   order by received_at desc,observation_id desc)=1
               )
               select 1 from event_winners e
               left join landing.membership s on s.scope='series' and s.entity_ticker=e.series_ticker
               left join landing.parent_eligibility p on p.capture_id=?
                 and p.entity_kind='series' and p.entity_ticker=e.series_ticker
                 where e.series_ticker is not null and e.series_ticker<>'' and s.entity_ticker is null
                 and p.entity_ticker is null limit 1""",
            [state["capture_id"], state["capture_id"]],
        ).fetchone()
        if errors:
            raise CatalogueError(
                "full capture lacks fresh series evidence for final membership; capture a new full baseline"
            )
        return
    _, parent_lookups = index.effective_parent_evidence(state)
    listed_events = {
        identity
        for scope in ("events_live", "events_combo")
        for identity in _capture_membership(state, index).get(scope, set())
    }
    listed_series = _capture_membership(state, index).get("series", set())
    active_markets = {
        identity
        for scope in ("markets_live", "markets_combo", "markets_history")
        for identity in _capture_membership(state, index).get(scope, set())
    }
    if not active_markets:
        return
    rows = connection.execute(
        """select entity_ticker, event_ticker from landing.observations
           where entity_kind='markets' and source_kind='direct'
             and entity_ticker in (select unnest(?))
           qualify row_number() over (
             partition by entity_ticker order by received_at desc, observation_id desc
           ) = 1""",
        [sorted(active_markets)],
    ).fetchall()
    required_events = {parent for _, parent in rows if parent}
    scoped = state.get("scope") is not None
    for event in required_events - listed_events:
        outcome = parent_lookups.get(("events", event))
        if outcome is None or (scoped and outcome[0] != 200):
            raise CatalogueError(
                "full capture lacks fresh parent evidence for final membership; capture a new full baseline"
            )
    active_events = listed_events | {
        identity
        for (kind, identity), outcome in parent_lookups.items()
        if kind == "events" and outcome[0] == 200
    }
    if not active_events:
        return
    event_rows = connection.execute(
        """select entity_ticker, series_ticker from landing.observations
           where entity_kind='events' and source_kind='direct'
             and entity_ticker in (select unnest(?))
           qualify row_number() over (
             partition by entity_ticker order by received_at desc, observation_id desc
           ) = 1""",
        [sorted(active_events)],
    ).fetchall()
    for _, parent in event_rows:
        if parent and parent not in listed_series:
            outcome = parent_lookups.get(("series", parent))
            if outcome is None or (scoped and outcome[0] != 200):
                raise CatalogueError(
                    "full capture lacks fresh series evidence for final membership; capture a new full baseline"
                )


def _build_scope_fields(state: dict[str, Any]) -> dict[str, Any]:
    scope = state.get("scope")
    if scope is None:
        return {"scope": "catalogue"}
    validated = validate_series_scope(scope)
    root_identity = state.get("root_id")
    if (
        not isinstance(root_identity, str)
        or len(root_identity) != 32
        or any(char not in "0123456789abcdef" for char in root_identity)
    ):
        raise CatalogueError("scoped build lacks a logical operator root identity")
    return {"scope": "series", "series_scope": validated, "root_id": root_identity}


def _historical_receipt_fields(state: dict[str, Any]) -> dict[str, Any]:
    from kalshi_events.historical import HISTORICAL_CONTRACT, HISTORICAL_VERSION

    selection, _ = _historical_profile(state)
    return {
        "version": HISTORICAL_VERSION,
        "catalogue_version": HISTORICAL_VERSION,
        "scope": "historical",
        "historical_contract": HISTORICAL_CONTRACT,
        "historical_selection": selection,
        "source_capture_complete": state.get("complete") is True,
    }


def _write_historical_coverage(
    connection: duckdb.DuckDBPyConnection, state: dict[str, Any]
) -> None:
    from kalshi_events.historical import HISTORICAL_CONTRACT

    connection.execute(
        """create table main.catalogue_coverage (
             historical_contract varchar,
             scope varchar,
             omitted_scopes varchar[],
             source_capture_id varchar,
             source_capture_complete boolean
        )"""
    )
    connection.execute(
        "insert into main.catalogue_coverage values (?, 'historical', ?, ?, ?)",
        [
            HISTORICAL_CONTRACT,
            ["events_combo", "markets_live", "markets_combo"],
            state["capture_id"],
            state.get("complete") is True,
        ],
    )


def _check_historical_coverage(
    database: Path, capture_id: str, source_capture_complete: bool
) -> None:
    from kalshi_events.historical import HISTORICAL_CONTRACT

    try:
        with duckdb.connect(str(database), read_only=True) as connection:
            result = connection.execute("select * from main.catalogue_coverage")
            columns = tuple(column[0] for column in result.description or ())
            rows = result.fetchall()
    except duckdb.Error as exc:
        raise CatalogueError("historical catalogue coverage is missing or invalid") from exc
    if columns != (
        "historical_contract",
        "scope",
        "omitted_scopes",
        "source_capture_id",
        "source_capture_complete",
    ) or rows != [
        (
            HISTORICAL_CONTRACT,
            "historical",
            ["events_combo", "markets_live", "markets_combo"],
            capture_id,
            source_capture_complete,
        )
    ]:
        raise CatalogueError("historical catalogue coverage does not match its build receipt")


def _full_v2_build(
    store: Store, state: dict[str, Any], *, rebuild: bool, historical: bool = False
) -> dict[str, Any]:
    capture_id = state["capture_id"]
    if (
        not historical
        and not state.get("task_scopes")
        and any(int(work.get("restarts", 0)) > 0 for work in state.get("tasks", []))
    ):
        raise CatalogueError(
            "legacy full capture lacks final-generation membership after cursor reset; capture a new full baseline"
        )
    store.check_deadline()
    catalogue_version = 3 if historical else 2
    historical_indices: set[int] | None = None
    if historical:
        _, historical_indices = _historical_profile(state)
    _recover_build(store, capture_id, catalogue_version)
    staging_relative = (
        f"tmp/build-historical-{uuid.uuid4().hex}"
        if historical
        else f"tmp/build-v2-{uuid.uuid4().hex}"
    )
    staging = store.path(staging_relative)
    durable_mkdir(staging.parent)
    staging.mkdir()
    verified_pages: dict[str, str] = {}
    index: ParentIndex | DiskParentIndex = (
        DiskParentIndex(store)
        if store.scope is None and _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL
        else ParentIndex()
    )
    try:
        if historical:
            _land(
                store,
                state,
                staging,
                verified_pages=verified_pages,
                parent_index=index,
                task_indices=historical_indices,
            )
        else:
            source_relative = build_directory(capture_id, 1) + "/warehouse.duckdb"
            source_exists = store.scope is None and store.path(source_relative).exists()
            if source_exists and not rebuild:
                source_receipt = certified(store, capture_id, catalogue_version=1)
                _copy_closed_database(
                    store,
                    source_relative,
                    staging,
                    expected_sha256=source_receipt["warehouse_sha256"],
                )
                _projection_index(store, state, verified_pages=verified_pages, index=index)
            else:
                _land(
                    store,
                    state,
                    staging,
                    verified_pages=verified_pages,
                    parent_index=index,
                )
        with (
            duckdb.connect(str(staging / "warehouse.duckdb")) as connection,
            metrics.phase("current_projection"),
        ):
            _projection_membership(store, state, index, connection)
            if historical:
                _write_historical_coverage(connection, state)
            else:
                _parent_closure_is_fresh(connection, state, index)
        dbt(
            store,
            ["build"],
            database=f"{staging_relative}/warehouse.duckdb",
            artifacts=f"{staging_relative}/build-artifacts.json",
            variables={
                "catalogue_version": 2,
                "incremental_update": False,
                "current_capture_id": capture_id,
                "delta_capture_id": capture_id,
            },
        )
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, catalogue_version, verified_pages)
        database = staging / "warehouse.duckdb"
        fsync_file(database)
        fsync_directory(staging)
        receipt = {
            **(
                _historical_receipt_fields(state)
                if historical
                else {"version": 2, "catalogue_version": 2}
            ),
            "capture_id": capture_id,
            "built_at": now(),
            **({} if historical else _build_scope_fields(state)),
            "evidence": operation_evidence.evidence,
            "warehouse_sha256": file_digest(database, store.limits.max_database_bytes)[0],
            "dbt_artifacts_sha256": digest(regular_bytes(staging / "build-artifacts.json")),
        }
        store.write(f"{staging_relative}/build.json", receipt)
        fsync_directory(staging)
        promoted = _install_build(store, capture_id, catalogue_version, staging, operation_evidence)
        store.check_storage()
        return promoted
    finally:
        if isinstance(index, DiskParentIndex):
            index.close()
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
            fsync_directory(staging.parent)
        store.sync(staging_relative)


def build_v2_full(store: Store, capture_id: str, *, rebuild: bool = False) -> dict[str, Any]:
    with store.writer_guard():
        return _build_v2_full_locked(store, capture_id, rebuild=rebuild)


def build_historical(store: Store, capture_id: str, *, rebuild: bool = False) -> dict[str, Any]:
    with store.writer_guard():
        store.require_writable()
        if store.scope is not None:
            raise CatalogueError("historical catalogue builds require a catalogue root")
        from kalshi_events.incremental import finish_pending_retirement

        finish_pending_retirement(store)
        state, _ = _profile_capture(store, capture_id, 3)
        if state.get("incremental"):
            raise CatalogueError("incremental captures cannot be built as historical catalogues")
        return _full_v2_build(store, state, rebuild=rebuild, historical=True)


def _build_v2_full_locked(
    store: Store, capture_id: str, *, rebuild: bool = False
) -> dict[str, Any]:
    """Create a membership-aware v2 full baseline without modifying the v1 build."""
    store.require_writable()
    from kalshi_events.incremental import finish_pending_retirement

    finish_pending_retirement(store)
    state = catalogue_capture(store, capture_id)
    if state.get("incremental"):
        raise CatalogueError("a delta cannot be built as a full v2 rebase")
    return _full_v2_build(store, state, rebuild=rebuild)


def _build_delta(store: Store, state: dict[str, Any]) -> dict[str, Any]:
    from kalshi_events.incremental import baseline_membership, verify_pin

    capture_id = state["capture_id"]
    _recover_build(store, capture_id, 2)
    pin = state.get("baseline")
    if not isinstance(pin, dict):
        raise CatalogueError("incremental capture lacks a baseline pin")
    verify_pin(store, state)
    baseline_receipt = certified(
        store,
        pin["capture_id"],
        catalogue_version=pin["catalogue_version"],
    )
    staging_relative = f"tmp/build-v2-{uuid.uuid4().hex}"
    staging = store.path(staging_relative)
    durable_mkdir(staging.parent)
    staging.mkdir()
    verified_pages: dict[str, str] = {}
    try:
        previous_membership = baseline_membership(store, pin)
        source_relative = (
            build_directory(pin["capture_id"], pin["catalogue_version"]) + "/warehouse.duckdb"
        )
        _copy_closed_database(
            store,
            source_relative,
            staging,
            expected_sha256=baseline_receipt["warehouse_sha256"],
        )
        index = ParentIndex()
        _land(
            store,
            state,
            staging,
            verified_pages=verified_pages,
            parent_index=index,
        )
        with (
            duckdb.connect(str(staging / "warehouse.duckdb")) as connection,
            metrics.phase("current_projection"),
        ):
            _, changes = _projection_membership(
                store,
                state,
                index,
                connection,
                previous_membership=previous_membership,
            )
            if state.get("scope") is not None:
                _parent_closure_is_fresh(connection, state, index)
            _affected_keys(connection, state, changes)
            _delete_affected(connection)
        dbt(
            store,
            ["build"],
            database=f"{staging_relative}/warehouse.duckdb",
            artifacts=f"{staging_relative}/build-artifacts.json",
            variables={
                "catalogue_version": 2,
                "incremental_update": True,
                "current_capture_id": capture_id,
                "delta_capture_id": capture_id,
            },
        )
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, 2, verified_pages)
        database = staging / "warehouse.duckdb"
        fsync_file(database)
        fsync_directory(staging)
        receipt = {
            "version": 2,
            "catalogue_version": 2,
            "capture_id": capture_id,
            "built_at": now(),
            **_build_scope_fields(state),
            "evidence": operation_evidence.evidence,
            "warehouse_sha256": file_digest(database, store.limits.max_database_bytes)[0],
            "dbt_artifacts_sha256": digest(regular_bytes(staging / "build-artifacts.json")),
        }
        store.write(f"{staging_relative}/build.json", receipt)
        fsync_directory(staging)
        promoted = _install_build(store, capture_id, 2, staging, operation_evidence)
        store.check_storage()
        return promoted
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
            fsync_directory(staging.parent)
        store.sync(staging_relative)


def certified(
    store: Store,
    capture_id: str,
    *,
    cutoff_sink: list[dict[str, Any]] | None = None,
    catalogue_version: int = 1,
    _directory: Path | None = None,
    _state: dict[str, Any] | None = None,
    _page_rows: list[dict[str, Any]] | None = None,
    _proof: _OperationEvidence | None = None,
    _context: _OperationContext | None = None,
) -> dict[str, Any]:
    if catalogue_version not in {1, 2, 3}:
        raise CatalogueError("catalogue version must be 1, 2, or 3")
    if store.scope is not None and catalogue_version != 2:
        raise CatalogueError("series roots support only internal v2 warehouse builds")
    if _context is not None:
        _check_operation_context(store, _context)
        cached = _context.captures.get((capture_id, catalogue_version))
        if cached is not None:
            if _state is not None and digest(json_bytes(dict(_state))) != cached.proof.state_sha256:
                raise CatalogueError("operation verification state does not match its cached input")
            if _page_rows is not None and tuple(_page_rows) != cached.proof.page_rows:
                raise CatalogueError("operation verification receipts do not match cached input")
            return _reuse_certified(
                store,
                capture_id,
                catalogue_version,
                _context,
                cached,
                cutoff_sink,
            )
    directory = build_directory(capture_id, catalogue_version)
    directory_path = _directory or store.path(directory)
    receipt_body = regular_bytes(directory_path / "build.json")
    receipt = decode(receipt_body)
    if (
        receipt.get("capture_id") != capture_id
        or receipt.get("version") != catalogue_version
        or receipt.get("catalogue_version", catalogue_version) != catalogue_version
    ):
        raise CatalogueError("build receipt does not match the requested capture")
    proof_pages: dict[str, str] | None = None
    cutoff_rows: list[str] = []
    cutoff_start = len(cutoff_sink) if cutoff_sink is not None else 0
    if _proof is not None and cutoff_sink is None:
        state, proof_rows = _validate_operation_evidence(
            store, _proof, capture_id, catalogue_version
        )
        rows = list(proof_rows)
        proof_pages = _proof.verified_pages
    else:
        state, expected_rows = _profile_capture(store, capture_id, catalogue_version, _state)
        rows = expected_rows if _page_rows is None else _page_rows
        if (
            not isinstance(rows, list)
            or any(not isinstance(row, dict) for row in rows)
            or rows != expected_rows
        ):
            raise CatalogueError("invalid operation-local capture receipt inventory")
    if store.scope is not None:
        expected_scope = _build_scope_fields(state)
        if any(receipt.get(key) != value for key, value in expected_scope.items()):
            raise CatalogueError("build receipt scope does not match the operator root")
    elif receipt.get("scope") == "series":
        raise CatalogueError("series build receipt cannot be verified from a catalogue root")
    if catalogue_version == 3:
        expected_profile = _historical_receipt_fields(state)
        if any(receipt.get(key) != value for key, value in expected_profile.items()):
            raise CatalogueError("historical build receipt does not match its source capture")
    elif receipt.get("scope") == "historical":
        raise CatalogueError("historical build receipt requires catalogue version 3")
    base = f"captures/{capture_id}"
    required = {f"{base}/state.json", f"{base}/pages.jsonl"}
    for row in rows:
        relative = row["relative"]
        required.update({relative, _receipt_body_path(relative)})
    expected_files = receipt.get("evidence")
    if not isinstance(expected_files, dict) or set(expected_files) != required:
        raise CatalogueError("build evidence inventory does not match the capture journal")
    actual_files = (
        dict(_proof.checkpoints) if proof_pages is not None else _checkpoint_inventory(store, state)
    )
    if proof_pages is not None:
        expected_pages = {
            relative: checksum
            for relative, checksum in _proof.evidence.items()
            if relative not in actual_files
        }
        if expected_pages != proof_pages:
            raise CatalogueError("operation evidence receipt inventory mismatch")
        actual_files.update(proof_pages)
    else:
        for row in rows:
            store.check_deadline()
            relative = row["relative"]
            manifest, payload = read_page(
                store,
                relative,
                state["max_response_bytes"],
                state.get("max_decoded_response_bytes", state["max_response_bytes"]),
                decode_endpoints={"/historical/cutoff"} if cutoff_sink is not None else set(),
                inventory=actual_files,
            )
            if manifest.get("capture_id") != capture_id:
                raise CatalogueError("raw evidence belongs to another capture")
            if manifest.get("endpoint") == "/historical/cutoff" and manifest.get("status") == 200:
                cutoff_rows.append(relative)
            if (
                cutoff_sink is not None
                and manifest.get("endpoint") == "/historical/cutoff"
                and manifest.get("status") == 200
            ):
                cutoff_sink.append({"received_at": manifest["received_at"], "cutoff": payload})
    if actual_files != expected_files:
        raise CatalogueError("build evidence integrity mismatch")
    warehouse = directory_path / "warehouse.duckdb"
    if file_digest(warehouse, store.limits.max_database_bytes)[0] != receipt["warehouse_sha256"]:
        raise CatalogueError("warehouse changed after certified build")
    if catalogue_version == 3:
        _check_historical_coverage(warehouse, capture_id, state.get("complete") is True)
    artifacts = directory_path / "build-artifacts.json"
    if digest(regular_bytes(artifacts)) != receipt["dbt_artifacts_sha256"]:
        raise CatalogueError("dbt certification artifacts changed")
    if warehouse.with_name(warehouse.name + ".wal").exists():
        raise CatalogueError("warehouse has an uncheckpointed WAL")
    if _context is not None and _proof is None and _directory is None:
        checkpoint_paths = set(_checkpoint_inventory(store, state))
        verified_pages = {
            relative: checksum
            for relative, checksum in actual_files.items()
            if relative not in checkpoint_paths
        }
        proof = _operation_evidence(store, state, catalogue_version, verified_pages)
        _context.captures[(capture_id, catalogue_version)] = _VerifiedCapture(
            proof=proof,
            receipt=copy.deepcopy(receipt),
            build_sha256=digest(receipt_body),
            warehouse_sha256=receipt["warehouse_sha256"],
            artifacts_sha256=receipt["dbt_artifacts_sha256"],
            cutoff_rows=tuple(cutoff_rows),
            cutoff_records=(
                tuple(copy.deepcopy(cutoff_sink[cutoff_start:]))
                if cutoff_sink is not None
                else None
            ),
            binding_sha256=_verified_capture_binding(
                receipt,
                digest(receipt_body),
                receipt["warehouse_sha256"],
                receipt["dbt_artifacts_sha256"],
                tuple(cutoff_rows),
                tuple(copy.deepcopy(cutoff_sink[cutoff_start:]))
                if cutoff_sink is not None
                else None,
            ),
        )
    return receipt


def _reuse_certified(
    store: Store,
    capture_id: str,
    catalogue_version: int,
    context: _OperationContext,
    cached: _VerifiedCapture,
    cutoff_sink: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Reuse parsed evidence only after rehashing every bound file in this writer lease."""
    _check_operation_context(store, context)
    if (
        _verified_capture_binding(
            cached.receipt,
            cached.build_sha256,
            cached.warehouse_sha256,
            cached.artifacts_sha256,
            cached.cutoff_rows,
            cached.cutoff_records,
        )
        != cached.binding_sha256
    ):
        raise CatalogueError("operation verification cache binding mismatch")
    proof = cached.proof
    state, _ = _validate_operation_evidence(store, proof, capture_id, catalogue_version)
    expected_pages = proof.verified_pages or {}
    cutoff_paths = set(cached.cutoff_rows) if cutoff_sink is not None else set()
    cutoff_files = {
        path for relative in cutoff_paths for path in (relative, _receipt_body_path(relative))
    }
    cutoff_inventory: dict[str, str] = {}
    for relative, checksum in expected_pages.items():
        if relative in cutoff_files and cutoff_sink is not None and cached.cutoff_records is None:
            continue
        size_limit = (
            1024**2
            if relative.endswith("/manifest.json")
            else state["max_response_bytes"]
            if relative.endswith("/body.bin")
            else store.limits.max_root_bytes
        )
        if file_digest(store.path(relative), size_limit)[0] != checksum:
            raise CatalogueError("raw evidence changed during the operation")
    directory = store.path(build_directory(capture_id, catalogue_version))
    receipt_path = directory / "build.json"
    if file_digest(receipt_path, store.limits.max_root_bytes)[0] != cached.build_sha256:
        raise CatalogueError("build receipt changed during the operation")
    warehouse = directory / "warehouse.duckdb"
    if file_digest(warehouse, store.limits.max_database_bytes)[0] != cached.warehouse_sha256:
        raise CatalogueError("warehouse changed during the operation")
    artifacts = directory / "build-artifacts.json"
    if digest(regular_bytes(artifacts)) != cached.artifacts_sha256:
        raise CatalogueError("dbt certification artifacts changed during the operation")
    if warehouse.with_name(warehouse.name + ".wal").exists():
        raise CatalogueError("warehouse has an uncheckpointed WAL")
    if cutoff_sink is not None:
        if cached.cutoff_records is not None:
            cutoff_sink.extend(copy.deepcopy(cached.cutoff_records))
        else:
            for relative in cached.cutoff_rows:
                manifest, payload = read_page(
                    store,
                    relative,
                    state["max_response_bytes"],
                    state.get("max_decoded_response_bytes", state["max_response_bytes"]),
                    decode_endpoints={"/historical/cutoff"},
                    inventory=cutoff_inventory,
                )
                if manifest.get("capture_id") != capture_id:
                    raise CatalogueError("raw evidence belongs to another capture")
                cutoff_sink.append({"received_at": manifest["received_at"], "cutoff": payload})
            for relative in cutoff_files:
                if cutoff_inventory.get(relative) != expected_pages.get(relative):
                    raise CatalogueError("raw cutoff evidence changed during the operation")
    return copy.deepcopy(cached.receipt)


def _verified_capture_binding(
    receipt: dict[str, Any],
    build_sha256: str,
    warehouse_sha256: str,
    artifacts_sha256: str,
    cutoff_rows: tuple[str, ...],
    cutoff_records: tuple[dict[str, Any], ...] | None,
) -> str:
    return digest(
        json_bytes(
            {
                "receipt": receipt,
                "build_sha256": build_sha256,
                "warehouse_sha256": warehouse_sha256,
                "artifacts_sha256": artifacts_sha256,
                "cutoff_rows": list(cutoff_rows),
                "cutoff_records": cutoff_records,
            }
        )
    )
