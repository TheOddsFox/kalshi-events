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
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager, nullcontext, suppress
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as installed_version
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any

import dlt
import duckdb
import pyarrow as pa
from dlt.common.data_types.type_helpers import coerce_value, py_type_to_sc_type

from kalshi_events import historical_resume, metrics
from kalshi_events.capture import (
    CURRENT_ACQUISITION_PROTOCOL,
    _acquisition_protocol,
    _complete_capture,
    _iter_page_receipts,
    capture_pages,
    page_receipts,
    read_capture,
)
from kalshi_events.common import (
    MAX_BUNDLE_BYTES,
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
ORDERING_POLICY = "received-at-utc-microseconds-v1"
ORDERING_POLICY_FIELD = "warehouse_ordering_policy"
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


def _report_landing_progress(**counters: int) -> None:
    try:
        metrics.progress(**counters)
    except Exception:
        metrics.add("progress_write_failures")


def _landing_arrow_batches(rows):
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    observations_emitted = 0
    batches_yielded = 0
    for row in rows:
        row_bytes = sum(len(value.encode("utf-8")) for value in row.values() if value is not None)
        if batch and (
            len(batch) >= _LANDING_BATCH_ROWS or batch_bytes + row_bytes > _LANDING_BATCH_BYTES
        ):
            table = pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)
            observations_emitted += len(batch)
            batches_yielded += 1
            _report_landing_progress(
                observations_emitted=observations_emitted,
                batches_yielded=batches_yielded,
            )
            yield table
            batch = []
            batch_bytes = 0
        batch.append(row)
        batch_bytes += row_bytes
        if len(batch) >= _LANDING_BATCH_ROWS or batch_bytes >= _LANDING_BATCH_BYTES:
            table = pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)
            observations_emitted += len(batch)
            batches_yielded += 1
            _report_landing_progress(
                observations_emitted=observations_emitted,
                batches_yielded=batches_yielded,
            )
            yield table
            batch = []
            batch_bytes = 0
    if batch:
        table = pa.Table.from_pylist(batch, schema=_LANDING_SCHEMA)
        observations_emitted += len(batch)
        batches_yielded += 1
        _report_landing_progress(
            observations_emitted=observations_emitted,
            batches_yielded=batches_yielded,
        )
        yield table


def _legacy_landing_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return None
    return coerce_value("text", py_type_to_sc_type(type(value)), value)


@contextmanager
def _duckdb_work_config(store: Store, directory: Path):
    relative = directory.relative_to(store.root).as_posix()
    usage = store.usage()
    reserved = getattr(store, "_storage_reservations", 0)
    spill_budget = min(
        store.limits.max_temp_bytes - usage["temporary_bytes"] - reserved,
        store.limits.max_root_bytes - usage["retained_bytes"] - reserved,
    )
    if spill_budget < 1:
        raise BudgetExceeded("no bounded temporary storage is available for DuckDB work")
    # The iterator may still write to the same Store while DuckDB consumes its batches.
    spill_relative = f"{relative}/duckdb-spill"
    spill_directory = None
    try:
        spill_directory = store.path(spill_relative)
        durable_mkdir(spill_directory)
        yield {
            "memory_limit": DBT_MEMORY_LIMIT,
            "threads": str(DBT_THREADS),
            "temp_directory": str(spill_directory),
            "max_temp_directory_size": f"{spill_budget}B",
        }
    finally:
        if spill_directory is not None and spill_directory.exists():
            store.sync(spill_relative)


def build_directory(capture_id: str, catalogue_version: int = 1) -> str:
    if catalogue_version == 3:
        return f"builds-historical/{capture_id}"
    return f"builds-v2/{capture_id}" if catalogue_version == 2 else f"builds/{capture_id}"


def _dbt_executable(python_executable: str) -> Path:
    return Path(python_executable).parent / "dbt"


def _write_ordering_policy(store: Store, database: Path) -> None:
    with (
        _duckdb_work_config(store, database.parent) as config,
        duckdb.connect(str(database), config=config) as connection,
    ):
        store.check_deadline()
        try:
            with interrupt_at_deadline(store, connection.interrupt):
                connection.execute("create schema if not exists kalshi_private")
                connection.execute("drop table if exists kalshi_private.ordering_policy")
                connection.execute(
                    "create table kalshi_private.ordering_policy(policy varchar not null)"
                )
                connection.execute(
                    "insert into kalshi_private.ordering_policy values (?)", [ORDERING_POLICY]
                )
                connection.execute("checkpoint")
        except duckdb.Error as exc:
            try:
                store.check_deadline()
            except BudgetExceeded as budget:
                raise budget from exc
            raise
        store.check_deadline()


def _check_ordering_policy(store: Store, database: Path, receipt: dict[str, Any]) -> None:
    has_receipt_marker = ORDERING_POLICY_FIELD in receipt
    usage = store.usage()
    spill_budget = min(
        1024**2,
        store.limits.max_temp_bytes - usage["temporary_bytes"],
        store.limits.max_root_bytes - usage["retained_bytes"],
    )
    if spill_budget < 1:
        raise BudgetExceeded("no bounded temporary storage is available for policy verification")
    spill_relative = f"tmp/ordering-policy-{uuid.uuid4().hex}"
    spill_directory = store.path(spill_relative)
    store.reserve_storage(spill_budget)
    try:
        spill_directory.mkdir(parents=True)
        with (
            duckdb.connect(
                str(database),
                read_only=True,
                config={
                    "memory_limit": "64MB",
                    "temp_directory": str(spill_directory),
                    "max_temp_directory_size": f"{spill_budget}B",
                },
            ) as connection,
            interrupt_at_deadline(store, connection.interrupt),
        ):
            try:
                marker = connection.execute(
                    "select table_type from information_schema.tables "
                    "where table_schema='kalshi_private' and table_name='ordering_policy'"
                ).fetchone()
                if not has_receipt_marker:
                    if marker is not None:
                        raise CatalogueError(
                            "build receipt and warehouse ordering policy markers differ"
                        )
                    return
                if (
                    receipt[ORDERING_POLICY_FIELD] != ORDERING_POLICY
                    or marker is None
                    or marker[0] != "BASE TABLE"
                ):
                    raise CatalogueError(
                        "build receipt and warehouse ordering policy markers differ"
                    )
                rows = connection.execute(
                    "select policy from kalshi_private.ordering_policy limit 2"
                ).fetchall()
                if rows != [(ORDERING_POLICY,)]:
                    raise CatalogueError("warehouse ordering policy marker mismatch")
            except duckdb.Error as exc:
                try:
                    store.check_deadline()
                except BudgetExceeded as budget:
                    raise budget from exc
                raise
        store.check_deadline()
    except CatalogueError:
        raise
    except duckdb.Error as exc:
        raise CatalogueError("warehouse ordering policy marker is invalid") from exc
    finally:
        if spill_directory.exists():
            shutil.rmtree(spill_directory, ignore_errors=True)
            store.sync("tmp")
        store.release_storage(spill_budget)


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


def _invalidate_operation_certification(
    store: Store, capture_id: str, catalogue_version: int, context: _OperationContext
) -> None:
    """Forget proof for a build that this writer operation is about to replace."""
    _check_operation_context(store, context)
    context.captures.pop((capture_id, catalogue_version), None)


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
    state = read_capture(store, capture_id)
    if state.get("capture_id") != capture_id or (
        state_hint is not None
        and digest(json_bytes(dict(state_hint))) != digest(json_bytes(dict(state)))
    ):
        raise CatalogueError("historical capture state changed during the operation")
    _, indices = _historical_profile(state)
    rows = list(_iter_page_receipts(store, state, task_indices=indices))
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
        for receipts_verified, (manifest, payload) in enumerate(
            capture_pages(store, state, evidence=verified_pages, task_indices=task_indices), start=1
        ):
            _report_landing_progress(receipts_verified=receipts_verified)
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
        with (
            _duckdb_work_config(store, directory) as config,
            duckdb.connect(str(database), config=config) as connection,
        ):
            connection.execute("create schema if not exists landing")
            connection.execute(
                "create table if not exists landing.observations (observation_id varchar, capture_id varchar, entity_kind varchar, entity_ticker varchar, received_at varchar, provider_updated_at varchar, source_endpoint varchar, source_kind varchar, payload_json varchar, event_ticker varchar, series_ticker varchar, task_scope varchar, task_generation varchar)"
            )
    else:
        try:
            with (
                _duckdb_work_config(store, directory) as duckdb_config,
                dlt.config.values(
                    {
                        # dlt resolves the extractor writer inside these source/resource sections.
                        "kalshi_catalogue.sources.kalshi_catalogue.kalshi_catalogue.data_writer.buffer_max_items": 1,
                        "kalshi_catalogue.normalize.data_writer.buffer_max_items": 1,
                        "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_load_id": True,
                        "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_id": True,
                    }
                ),
            ):
                pipeline = dlt.pipeline(
                    pipeline_name="kalshi_catalogue",
                    pipelines_dir=str(directory / "landing"),
                    destination=dlt.destinations.duckdb(
                        credentials={
                            "database": str(database),
                            "global_config": duckdb_config,
                        }
                    ),
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


def _land_historical_rows(
    store: Store,
    state: dict[str, Any],
    directory: Path,
    *,
    verified_pages: dict[str, str],
    parent_index: ParentIndex | DiskParentIndex,
    task_indices: set[int],
) -> int:
    """Append selected historical observations directly into a fresh DuckDB table."""
    database = directory / "warehouse.duckdb"
    wal = database.with_name(database.name + ".wal")
    if any(path.exists() or path.is_symlink() for path in (database, wal)):
        raise CatalogueError("historical landing requires a fresh database without a WAL")

    store.check_deadline()
    store.check_storage()
    rows = _iter_historical_landing_rows(
        store,
        state,
        verified_pages=verified_pages,
        parent_index=parent_index,
        task_indices=task_indices,
    )

    database_relative = database.relative_to(store.root).as_posix()
    wal_relative = f"{database_relative}.wal"
    spill_relative = f"{directory.relative_to(store.root).as_posix()}/duckdb-spill"
    spill_directory = directory / "duckdb-spill"

    def sync_database() -> None:
        store.sync_file(database_relative)
        if wal.exists() or wal.is_symlink():
            store.sync_file(wal_relative)
        else:
            store.sync(wal_relative)
        if spill_directory.exists():
            store.sync(spill_relative)
        store.check_storage()

    def execute(connection: duckdb.DuckDBPyConnection, statement: str) -> None:
        try:
            with interrupt_at_deadline(store, connection.interrupt):
                connection.execute(statement)
        except duckdb.Error as exc:
            try:
                store.check_deadline()
            except BudgetExceeded as budget:
                raise budget from exc
            raise

    last_checkpoint = time.monotonic()
    checkpoint_bytes = 0
    checkpoint_after_bytes = 256 * 1024 * 1024
    with (
        _duckdb_work_config(store, directory) as config,
        duckdb.connect(str(database), config=config) as connection,
    ):
        execute(connection, "create schema landing")
        execute(
            connection,
            """create table landing.observations (
                 observation_id varchar not null,
                 capture_id varchar not null,
                 entity_kind varchar not null,
                 entity_ticker varchar not null,
                 received_at varchar not null,
                 provider_updated_at varchar,
                 source_endpoint varchar not null,
                 source_kind varchar not null,
                 payload_json varchar using compression zstd not null,
                 event_ticker varchar,
                 series_ticker varchar,
                 task_scope varchar,
                 task_generation varchar not null
             )""",
        )
        sync_database()
        for batch in _landing_arrow_batches(rows):
            store.check_deadline()
            registered = False
            try:
                connection.register("historical_landing_batch", batch)
                registered = True
                execute(
                    connection,
                    "insert into landing.observations "
                    "select observation_id, capture_id, entity_kind, entity_ticker, received_at, "
                    "provider_updated_at, source_endpoint, source_kind, payload_json, event_ticker, "
                    "series_ticker, task_scope, task_generation "
                    "from historical_landing_batch",
                )
            finally:
                if registered:
                    connection.unregister("historical_landing_batch")
            store.check_deadline()
            sync_database()
            checkpoint_bytes += sum(
                buffer.size
                for column in batch.columns
                for chunk in column.chunks
                if (buffer := chunk.buffers()[2]) is not None
            )
            checkpoint_due = (
                checkpoint_bytes >= checkpoint_after_bytes
                or time.monotonic() - last_checkpoint >= 60
            )
            if checkpoint_due:
                execute(connection, "checkpoint")
                sync_database()
                checkpoint_bytes = 0
                last_checkpoint = time.monotonic()
        store.check_deadline()
        execute(connection, "checkpoint")
        sync_database()
    sync_database()
    return rows.observation_count


class _HistoricalLandingRows:
    def __init__(self, rows, count: list[int]):
        self.rows = rows
        self._count = count

    @property
    def observation_count(self) -> int:
        return self._count[0]

    def __iter__(self):
        return self.rows


def _iter_historical_landing_rows(
    store: Store,
    state: dict[str, Any],
    *,
    verified_pages: dict[str, str],
    parent_index: ParentIndex | DiskParentIndex,
    task_indices: set[int],
    page_index: historical_resume.PageContextIndex | None = None,
    with_positions: bool = False,
) -> _HistoricalLandingRows:
    capture_id = state["capture_id"]
    seen_pages: dict[str, str] = {}
    count = [0]

    def rows():
        for receipts_verified, (manifest, payload) in enumerate(
            capture_pages(store, state, evidence=verified_pages, task_indices=task_indices),
            start=1,
        ):
            store.check_deadline()
            _report_landing_progress(receipts_verified=receipts_verified)
            manifest = _scope_manifest(state, manifest)
            page_id = manifest.get("page_id")
            if not isinstance(page_id, str) or not page_id:
                raise CatalogueError("historical receipt lacks a page identity")
            page_context = digest(json_bytes(manifest))
            if page_index is None:
                previous = seen_pages.get(page_id)
                if previous is not None:
                    if previous != page_context:
                        raise CatalogueError("historical page identity has contradictory context")
                    continue
                seen_pages[page_id] = page_context
            elif not page_index.add(page_id, page_context):
                continue
            parent_index.add_page(manifest, payload)
            for row_offset, row in enumerate(observations(((manifest, payload),))):
                store.check_deadline()
                count[0] += 1
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
                normalized = {key: _legacy_landing_text(value) for key, value in values.items()}
                if with_positions:
                    yield (
                        tuple(normalized[name] for name in _LANDING_COLUMNS),
                        {
                            "receipt_position": receipts_verified,
                            "page_id": page_id,
                            "page_context_sha256": page_context,
                            "row_offset": row_offset,
                        },
                    )
                else:
                    yield normalized

    return _HistoricalLandingRows(rows(), count)


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
        usage = store.usage()
        spill_budget = min(
            store.limits.max_temp_bytes - usage["temporary_bytes"],
            store.limits.max_root_bytes - usage["retained_bytes"],
        )
        if spill_budget < 1:
            raise BudgetExceeded("no bounded temporary storage is available for dbt work")
        profiles = {
            "kalshi_events": {
                "target": "local",
                "outputs": {
                    "local": {
                        "type": "duckdb",
                        "path": str(store.path(database)),
                        "threads": DBT_THREADS,
                        "config_options": {
                            "threads": str(DBT_THREADS),
                            "temp_directory": str(runtime / "spill"),
                            "max_temp_directory_size": f"{spill_budget}B",
                        },
                        "settings": {
                            "memory_limit": DBT_MEMORY_LIMIT,
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
        if args[0] in {"build", "compile", "run", "test"} and artifacts is not None:
            values = {}
            names = (
                ("manifest.json", "run_results.json")
                if args[0] != "compile"
                else ("manifest.json",)
            )
            for name in names:
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


def _read_dbt_artifacts(path: Path) -> dict[str, Any] | None:
    try:
        value = decode(regular_bytes(path, MAX_BUNDLE_BYTES))
    except (CatalogueError, OSError):
        return None
    manifest_record = value.get("manifest.json") if isinstance(value, dict) else None
    if not isinstance(manifest_record, dict) or not isinstance(
        manifest_record.get("content"), dict
    ):
        return None
    return value


def _historical_model_cache_valid(
    connection: duckdb.DuckDBPyConnection,
    current_manifest: dict[str, Any],
    model_name: str,
    store: Store,
) -> tuple[bool, int, str | None]:
    output = _historical_model_output_or_none(connection, current_manifest, model_name, store)
    if output is None:
        return False, 0, None
    count, checksum = output
    return True, count, checksum


def _historical_model_output(
    connection: duckdb.DuckDBPyConnection,
    current_manifest: dict[str, Any],
    model_name: str,
    store: Store,
) -> tuple[int, str]:
    output = _historical_model_output_or_none(connection, current_manifest, model_name, store)
    if output is None:
        raise CatalogueError(f"historical dbt model {model_name} differs from its current query")
    return output


def _historical_model_output_or_none(
    connection: duckdb.DuckDBPyConnection,
    current_manifest: dict[str, Any],
    model_name: str,
    store: Store,
) -> tuple[int, str] | None:
    current = current_manifest.get("nodes", {}).get(f"model.kalshi_events.{model_name}")
    if not isinstance(current, dict):
        raise CatalogueError(f"historical dbt manifest lacks model {model_name}")
    schema = current.get("schema")
    alias = current.get("alias")
    compiled = current.get("compiled_code")
    if not all(isinstance(value, str) and value for value in (schema, alias, compiled)):
        raise CatalogueError(f"historical dbt model {model_name} has no compiled relation")
    relation_type = _dbt_relation_type(connection, schema, alias)
    materialized = current.get("config", {}).get("materialized")
    expected_type = "VIEW" if materialized == "view" else "BASE TABLE"
    if relation_type != expected_type:
        return None

    def quote(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    relation = f"{quote(schema)}.{quote(alias)}"
    query = compiled.rstrip().removesuffix(";")
    from kalshi_events.historical_resume import compare_query_to_relation

    digest_source = "landing.combo_legs_canonical" if model_name == "combo_legs" else relation
    try:
        with interrupt_at_deadline(store, connection.interrupt):
            return compare_query_to_relation(
                connection,
                query,
                relation,
                check_deadline=store.check_deadline,
                digest_source=digest_source,
            )
    except duckdb.InterruptException as exc:
        raise BudgetExceeded(
            "operation deadline exhausted validating historical model output"
        ) from exc


def _merge_historical_dbt_artifacts(
    store: Store,
    destination: str,
    previous: dict[str, Any] | None,
    phase_paths: list[str],
) -> None:
    manifests = []
    run_metadata = None
    results: dict[str, Any] = {}
    if previous is not None:
        manifests.append(previous["manifest.json"]["content"])
        previous_results = previous.get("run_results.json", {}).get("content", {})
        run_metadata = (
            previous_results.get("metadata") if isinstance(previous_results, dict) else None
        )
        for result in (
            previous_results.get("results", []) if isinstance(previous_results, dict) else []
        ):
            if isinstance(result, dict) and isinstance(result.get("unique_id"), str):
                results[result["unique_id"]] = result
    for relative in phase_paths:
        phase = _read_dbt_artifacts(store.path(relative))
        if phase is None:
            continue
        manifests.append(phase["manifest.json"]["content"])
        phase_results = phase.get("run_results.json", {}).get("content", {})
        if isinstance(phase_results, dict):
            run_metadata = phase_results.get("metadata", run_metadata)
        for result in phase_results.get("results", []) if isinstance(phase_results, dict) else []:
            if isinstance(result, dict) and isinstance(result.get("unique_id"), str):
                results[result["unique_id"]] = result
    if not manifests:
        raise CatalogueError("historical dbt artifacts are missing after certification")
    manifest = manifests[-1]
    run_results = {"metadata": run_metadata or {}, "results": list(results.values())}
    manifest_body = json_bytes(manifest)
    results_body = json_bytes(run_results)
    store.write(
        destination,
        {
            "manifest.json": {
                "sha256": hashlib.sha256(manifest_body).hexdigest(),
                "content": manifest,
            },
            "run_results.json": {
                "sha256": hashlib.sha256(results_body).hexdigest(),
                "content": run_results,
            },
        },
    )


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
        membership_schema = pa.schema(
            [
                pa.field("scope", pa.string()),
                pa.field("entity_kind", pa.string()),
                pa.field("entity_ticker", pa.string()),
            ]
        )
        memberships = iter(index.stream_membership(state))
        while batch := list(itertools.islice(memberships, _LANDING_BATCH_ROWS)):
            table = pa.Table.from_arrays(
                [
                    pa.array([row[0] for row in batch], type=pa.string()),
                    pa.array([SCOPE_KIND[row[0]] for row in batch], type=pa.string()),
                    pa.array([row[1] for row in batch], type=pa.string()),
                ],
                schema=membership_schema,
            )
            registered = False
            try:
                connection.register("historical_membership_batch", table)
                registered = True
                connection.execute(
                    "insert into landing.membership "
                    "select scope, entity_kind, entity_ticker from historical_membership_batch"
                )
            finally:
                if registered:
                    connection.unregister("historical_membership_batch")
            store.check_deadline()
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
                 partition by entity_ticker order by cast(received_at as timestamptz) desc, observation_id desc
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
                 partition by entity_ticker order by cast(received_at as timestamptz) desc, observation_id desc
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
        database = staging / "warehouse.duckdb"
        _write_ordering_policy(store, database)
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, 1, verified_pages)
        receipt = {
            "version": 1,
            "capture_id": capture_id,
            "built_at": now(),
            "scope": "catalogue",
            ORDERING_POLICY_FIELD: ORDERING_POLICY,
            "evidence": operation_evidence.evidence,
            "warehouse_sha256": file_digest(database, store.limits.max_database_bytes)[0],
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


def _validate_historical_stage(path: Path, root_device: int) -> tuple[int, int]:
    root_stat = path.lstat()
    if not stat.S_ISDIR(root_stat.st_mode) or root_stat.st_dev != root_device:
        raise CatalogueError("abandoned historical stage has an unsafe root")
    pending = [path]
    while pending:
        directory = pending.pop()
        directory_stat = directory.lstat()
        if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_dev != root_device:
            raise CatalogueError("abandoned historical stage contains an unsafe directory")
        with os.scandir(directory) as entries:
            for entry in entries:
                entry_stat = entry.stat(follow_symlinks=False)
                if entry_stat.st_dev != root_device:
                    raise CatalogueError("abandoned historical stage crosses a filesystem")
                if stat.S_ISDIR(entry_stat.st_mode):
                    pending.append(Path(entry.path))
                elif stat.S_ISREG(entry_stat.st_mode):
                    if entry_stat.st_nlink != 1:
                        raise CatalogueError("abandoned historical stage has a hard-linked file")
                else:
                    raise CatalogueError("abandoned historical stage contains an unsafe file")
    return root_stat.st_dev, root_stat.st_ino


def _json_stage_reference(value: Any, targets: list[tuple[str, tuple[str, ...]]]) -> str | None:
    if isinstance(value, dict):
        for key, item in value.items():
            reference = _json_stage_reference(key, targets) or _json_stage_reference(item, targets)
            if reference is not None:
                return reference
        return None
    if isinstance(value, list):
        for item in value:
            reference = _json_stage_reference(item, targets)
            if reference is not None:
                return reference
        return None
    if isinstance(value, str):
        normalized = value.replace("\\", "/")
        for relative, stage_targets in targets:
            if any(target in normalized for target in stage_targets):
                return relative
    return None


def _check_historical_stage_references(
    store: Store, capture_id: str, stages: list[tuple[str, Path, tuple[int, int]]]
) -> None:
    targets = [
        (relative, (relative, relative.rsplit("/", 1)[-1], str(path)))
        for relative, path, _identity in stages
    ]

    def read_reference(path: Path, limit: int) -> Any | None:
        try:
            reference_stat = path.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(reference_stat.st_mode):
            raise CatalogueError("historical stage reference is unsafe")
        return decode(regular_bytes(path, limit))

    pointers = []
    for name in ("current.json", "current-v2.json", "current-historical.json"):
        value = read_reference(store.path(name), 1024**2)
        if value is not None:
            pointers.append(value)
    build_references = [
        value
        for name in (
            f"{build_directory(capture_id, 3)}/build.json",
            f"tmp/replaced-{capture_id}-v3/build.json",
        )
        if (value := read_reference(store.path(name), MAX_BUNDLE_BYTES)) is not None
    ]

    for value in (*pointers, *build_references):
        if _json_stage_reference(value, targets) is not None:
            raise CatalogueError("abandoned historical stage is referenced by a build or pointer")

    release_manifests = []
    for pointer in pointers:
        release_id = pointer.get("source_release_id") if isinstance(pointer, dict) else None
        if not isinstance(release_id, str) or re.fullmatch(r"[0-9a-f]{64}", release_id) is None:
            raise CatalogueError("current publication pointer has an invalid release identity")
        manifest = store.path(f"releases/{release_id}/manifest.json")
        try:
            manifest_stat = manifest.lstat()
        except FileNotFoundError as exc:
            raise CatalogueError("current publication pointer has no release manifest") from exc
        if not stat.S_ISREG(manifest_stat.st_mode):
            raise CatalogueError("current release manifest is unsafe")
        release_manifests.append(decode(regular_bytes(manifest, store.limits.max_root_bytes)))

    for manifest in release_manifests:
        if _json_stage_reference(manifest, targets) is not None:
            raise CatalogueError("abandoned historical stage is referenced by a published manifest")


def _historical_dbt_runtime_matches(
    store: Store, runtime: Path, stage_relative: str, capture_id: str
) -> tuple[Path, str, str] | None:
    profiles = runtime / "profiles.yml"
    try:
        profile_stat = profiles.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(profile_stat.st_mode) or profile_stat.st_nlink != 1:
        return None
    try:
        value = decode(regular_bytes(profiles, 1024**2))
        output = value["kalshi_events"]["outputs"]["local"]
        expected_databases = {
            str(store.path(f"{stage_relative}/warehouse.duckdb")),
            str(store.path(f"{stage_relative}/resume-candidate/warehouse.duckdb")),
        }
        spill = runtime / "spill"
        if (
            output.get("path") not in expected_databases
            or output.get("config_options", {}).get("temp_directory") != str(spill)
            or spill.is_symlink()
            or not spill.is_dir()
        ):
            return None
    except (KeyError, TypeError, ValueError, CatalogueError):
        return None

    manifest_path = runtime / "target" / "manifest.json"
    try:
        manifest_stat = manifest_path.lstat()
    except FileNotFoundError:
        manifest_stat = None
    if manifest_stat is not None:
        if not stat.S_ISREG(manifest_stat.st_mode) or manifest_stat.st_nlink != 1:
            return None
        try:
            manifest = decode(regular_bytes(manifest_path, store.limits.max_root_bytes))
            metadata = manifest.get("metadata", {})
            if not isinstance(metadata, dict) or metadata.get("project_name") != "kalshi_events":
                return None
            variables = metadata.get("vars")
            if isinstance(variables, dict) and (
                variables.get("current_capture_id") != capture_id
                or variables.get("historical_compact") is not True
            ):
                return None
            nodes = manifest.get("nodes", {})
            for name in ("events", "series"):
                model = f"_historical_current_{name}"
                node = nodes.get(f"model.kalshi_events.{model}")
                if not isinstance(node, dict) or node.get("config", {}).get("enabled") is not True:
                    return None
                compiled = node.get("compiled_code")
                if compiled is not None:
                    if not isinstance(compiled, str) or capture_id not in compiled:
                        return None
                    continue
                # An interrupted dbt build can leave a parse-time manifest.
                if node.get("original_file_path") != f"models/{model}.sql":
                    return None
                artifact = (
                    runtime / "target" / "compiled" / "kalshi_events" / "models" / f"{model}.sql"
                )
                details = artifact.lstat()
                if (
                    not stat.S_ISREG(details.st_mode)
                    or details.st_nlink != 1
                    or details.st_dev != store.root.stat().st_dev
                ):
                    return None
                predicate = f"where p.capture_id = '{capture_id}'".encode()
                if not any(
                    line.strip() == predicate
                    for line in regular_bytes(artifact, 1024**2).splitlines()
                ):
                    return None
        except (AttributeError, OSError, TypeError, ValueError, CatalogueError):
            return None
    return spill, digest(regular_bytes(profiles, 1024**2)), output["path"]


def _cleanup_resumed_historical_dbt_spills(
    store: Store, capture_id: str, stage_relative: str
) -> None:
    """Remove only spill files from abandoned dbt runtimes bound to this stage."""
    temporary = store.path("tmp")
    try:
        root_device = store.root.lstat().st_dev
        temporary_stat = temporary.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(temporary_stat.st_mode) or temporary_stat.st_dev != root_device:
        raise CatalogueError("temporary build namespace is unsafe")

    with os.scandir(temporary) as entries:
        runtimes = [
            Path(entry.path) for entry in entries if re.fullmatch(r"dbt-[0-9a-f]{32}", entry.name)
        ]
    for runtime in runtimes:
        runtime_stat = runtime.lstat()
        if not stat.S_ISDIR(runtime_stat.st_mode) or runtime_stat.st_dev != root_device:
            continue
        identity = _historical_dbt_runtime_matches(store, runtime, stage_relative, capture_id)
        if identity is None:
            continue
        spill, profiles_sha256, database_path = identity
        spill_stat = spill.lstat()
        if (
            not stat.S_ISDIR(runtime_stat.st_mode)
            or runtime_stat.st_dev != root_device
            or not stat.S_ISDIR(spill_stat.st_mode)
            or spill_stat.st_dev != root_device
        ):
            continue

        files: list[tuple[Path, tuple[int, int, int, int], str, int, str]] = []
        directories = [spill]
        safe = True
        while directories and safe:
            directory = directories.pop()
            directory_stat = directory.lstat()
            if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_dev != root_device:
                safe = False
                break
            with os.scandir(directory) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    item_stat = entry.stat(follow_symlinks=False)
                    if item_stat.st_dev != root_device:
                        safe = False
                        break
                    if stat.S_ISDIR(item_stat.st_mode):
                        directories.append(path)
                    elif stat.S_ISREG(item_stat.st_mode) and item_stat.st_nlink == 1:
                        relative_file = path.relative_to(store.root).as_posix()
                        sha256, size = file_digest(path, store.limits.max_root_bytes)
                        files.append(
                            (
                                path,
                                (
                                    item_stat.st_dev,
                                    item_stat.st_ino,
                                    item_stat.st_size,
                                    item_stat.st_mtime_ns,
                                ),
                                relative_file,
                                size,
                                sha256,
                            )
                        )
                    else:
                        safe = False
                        break
        if not safe:
            continue

        inventory = [
            {"path": relative, "bytes": size, "sha256": sha256}
            for _path, _identity, relative, size, sha256 in files
        ]
        ownership = {
            "root": str(store.root.resolve()),
            "capture_id": capture_id,
            "stage": stage_relative,
            "runtime": runtime.relative_to(store.root).as_posix(),
            "database": database_path,
            "spill": str(spill),
            "profiles_sha256": profiles_sha256,
        }
        proof = historical_resume.proof_record(
            "abandoned-dbt-spill-cleanup",
            ownership,
            {"files": inventory, "total_bytes": sum(item["bytes"] for item in inventory)},
        )
        proof_names = set()
        with os.scandir(runtime) as entries:
            for entry in entries:
                if not entry.name.startswith("historical-spill-cleanup"):
                    continue
                if (
                    re.fullmatch(r"historical-spill-cleanup(?:-[0-9a-f]{32})?\.json", entry.name)
                    is None
                ):
                    raise CatalogueError("historical spill-cleanup evidence name is invalid")
                proof_path = Path(entry.path)
                proof_stat = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(proof_stat.st_mode) or proof_stat.st_nlink != 1:
                    raise CatalogueError("historical spill-cleanup evidence path is unsafe")
                old_proof = decode(regular_bytes(proof_path, 1024**2))
                old_result = historical_resume.validate_proof(
                    old_proof, "abandoned-dbt-spill-cleanup", ownership
                )
                old_files = old_result.get("files")
                if not isinstance(old_files, list):
                    raise CatalogueError("historical spill-cleanup inventory is invalid")
                old_total = 0
                for item in old_files:
                    if (
                        not isinstance(item, dict)
                        or not isinstance(item.get("path"), str)
                        or not item["path"].startswith(
                            spill.relative_to(store.root).as_posix() + "/"
                        )
                        or isinstance(item.get("bytes"), bool)
                        or not isinstance(item.get("bytes"), int)
                        or item["bytes"] < 0
                        or not isinstance(item.get("sha256"), str)
                        or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
                    ):
                        raise CatalogueError("historical spill-cleanup inventory is invalid")
                    old_total += item["bytes"]
                if old_result.get("total_bytes") != old_total:
                    raise CatalogueError("historical spill-cleanup inventory total is invalid")
                proof_names.add(entry.name)
        proof_name = "historical-spill-cleanup.json"
        if proof_name in proof_names:
            proof_name = f"historical-spill-cleanup-{uuid.uuid4().hex}.json"
            while proof_name in proof_names:
                proof_name = f"historical-spill-cleanup-{uuid.uuid4().hex}.json"
        proof_path = runtime / proof_name
        store.write(proof_path.relative_to(store.root).as_posix(), proof)
        fsync_file(proof_path)
        fsync_directory(runtime)
        for path, file_identity, relative, _size, _sha256 in files:
            current = path.lstat()
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_nlink != 1
                or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
                != file_identity
            ):
                raise CatalogueError("abandoned historical dbt spill changed during cleanup")
            store.remove(relative)


def _historical_stage_has_uninstalled_adoption(
    store: Store,
    relative: str,
    path: Path,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> bool:
    stage_name = path.name
    adoption_path = path / "resume-adoption.json"
    cleanup_path = path / "resume-cleanup-intent.json"
    if not adoption_path.exists() and not adoption_path.is_symlink():
        if cleanup_path.exists() or cleanup_path.is_symlink():
            raise CatalogueError("historical cleanup intent has no adoption record")
        return False
    adoption = _load_self_proof(adoption_path, "historical-adoption")
    if adoption is None:
        raise CatalogueError("historical adoption record is missing")
    proof, result = adoption
    capture_id = proof["binding"].get("capture_id")
    if not isinstance(capture_id, str) or re.fullmatch(r"[0-9a-f]{32}", capture_id) is None:
        raise CatalogueError("historical adoption capture identity is invalid")
    installed = None
    with suppress(CatalogueError, OSError):
        installed = certified(store, capture_id, catalogue_version=3)
    if installed is not None and installed.get("historical_adoption", {}).get(
        "sha256"
    ) == proof.get("sha256"):
        return False
    state = read_capture(store, capture_id)
    if (
        state.get("capture_id") != capture_id
        or proof["binding"].get("root") != str(store.root.resolve())
        or proof["binding"].get("stage") != stage_name
        or proof["binding"].get("state_sha256") != digest(json_bytes(dict(state)))
    ):
        raise CatalogueError("historical adopted stage is bound to another capture")
    candidate = path / "resume-candidate"
    source_proof, source_result = _read_source_proof(store, state, stage_name, candidate)
    _validate_historical_adoption_origin(store, state, stage_name, source_proof, source_result)
    if _verified_mount is not None and _verified_mount.get("capture_id") != capture_id:
        if cleanup_path.exists() or cleanup_path.is_symlink():
            _validate_historical_cleanup_intent(
                store, state, stage_name, source_result, source_proof["sha256"], cleanup_path
            )
        elif not (path / "warehouse.duckdb").exists():
            raise CatalogueError(
                "unrelated adopted historical source is missing without cleanup intent"
            )
        return True
    _validate_historical_resume_source_presence(
        store,
        path / "warehouse.duckdb",
        path / "warehouse.duckdb.wal",
        source_result,
        adopted=True,
        cleanup_intent=cleanup_path,
        stage_name=stage_name,
        state=state,
        source_proof_sha256=source_proof["sha256"],
        _verified_mount=_verified_mount,
    )
    if result.get("candidate_sha256") != proof["result"].get("candidate_sha256"):
        raise CatalogueError("historical adoption candidate lineage is invalid")
    return True


def recover_and_cleanup_abandoned_historical_stages(
    store: Store,
    capture_id: str,
    *,
    preserve_stage: str | None = None,
    _verified_mount: dict[str, Any] | None = None,
) -> None:
    """Recover a same-capture replacement, then prune only safe historical build stages."""
    if not store.writer_locked_by_current_thread:
        raise CatalogueError("historical stage cleanup requires the canonical writer lock")
    if store.scope is not None:
        raise CatalogueError("historical stage cleanup requires an unscoped root")
    if (
        not isinstance(capture_id, str)
        or len(capture_id) != 32
        or any(character not in "0123456789abcdef" for character in capture_id)
    ):
        raise CatalogueError("invalid capture identity for historical stage cleanup")

    with metrics.phase("recovery_validation"):
        state = read_capture(store, capture_id)
        if state.get("capture_id") != capture_id:
            raise CatalogueError("historical stage cleanup source capture does not match")
        if _verified_mount is not None:
            historical_resume.validate_resume_mount_context(
                store=store, state=state, verified_mount=_verified_mount
            )
        _historical_profile(state)
        preserved: tuple[str, Path, tuple[int, int]] | None = None
        if preserve_stage is not None:
            from kalshi_events.historical_resume import stage_relative

            relative = stage_relative(preserve_stage)
            path = store.path(relative)
            try:
                identity = _validate_historical_stage(path, store.root.lstat().st_dev)
            except FileNotFoundError as exc:
                raise CatalogueError("requested historical resume stage is missing") from exc
            preserved = (relative, path, identity)
            _cleanup_resumed_historical_dbt_spills(store, capture_id, relative)
        _recover_build(store, capture_id, 3)
        temporary = store.path("tmp")
        try:
            temporary_stat = temporary.lstat()
        except FileNotFoundError:
            return
        root_stat = store.root.lstat()
        if not stat.S_ISDIR(temporary_stat.st_mode) or temporary_stat.st_dev != root_stat.st_dev:
            raise CatalogueError("temporary build namespace is unsafe")

        stages: list[tuple[str, Path, tuple[int, int]]] = []
        if preserved is not None:
            relative, path, identity = preserved
            try:
                current_stat = path.lstat()
            except FileNotFoundError as exc:
                raise CatalogueError("requested historical resume stage disappeared") from exc
            if (current_stat.st_dev, current_stat.st_ino) != identity:
                raise CatalogueError("requested historical resume stage changed during validation")
        with os.scandir(temporary) as entries:
            for entry in entries:
                if re.fullmatch(r"build-historical-[0-9a-f]{32}", entry.name) is None:
                    continue
                path = store.path(f"tmp/{entry.name}")
                if preserved is not None and entry.name == preserve_stage:
                    continue
                identity = _validate_historical_stage(path, root_stat.st_dev)
                if _historical_stage_has_uninstalled_adoption(
                    store, f"tmp/{entry.name}", path, _verified_mount=_verified_mount
                ):
                    continue
                stages.append((f"tmp/{entry.name}", path, identity))

        if not stages:
            return
        _check_historical_stage_references(store, capture_id, stages)

        current_tmp_stat = temporary.lstat()
        if (current_tmp_stat.st_dev, current_tmp_stat.st_ino) != (
            temporary_stat.st_dev,
            temporary_stat.st_ino,
        ):
            raise CatalogueError("temporary build namespace changed during cleanup validation")
        for _relative, path, identity in stages:
            current_stat = path.lstat()
            if (current_stat.st_dev, current_stat.st_ino) != identity:
                raise CatalogueError("historical build stage changed during cleanup validation")

        for relative, _path, _identity in stages:
            try:
                store.remove(relative)
            except BaseException:
                # A partial prune remains eligible for the same safe validation on retry.
                store.sync(relative)
                raise
            fsync_directory(temporary)


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
                   order by cast(received_at as timestamptz) desc,observation_id desc)=1
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
                   order by cast(received_at as timestamptz) desc,observation_id desc)=1
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
             partition by entity_ticker order by cast(received_at as timestamptz) desc, observation_id desc
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
             partition by entity_ticker order by cast(received_at as timestamptz) desc, observation_id desc
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


def _historical_resume_code_identity() -> str:
    package = Path(str(files("kalshi_events")))
    sources = []
    for path in sorted(package.rglob("*")):
        if path.is_file() and path.suffix in {".py", ".sql", ".yml", ".yaml"}:
            sources.append((path.relative_to(package).as_posix(), path.read_bytes()))
    for name in ("pyproject.toml", "uv.lock"):
        path = Path(__file__).parents[2] / name
        if path.is_file():
            sources.append((f"project/{name}", path.read_bytes()))
    sources.append(("runtime/python", sys.version.encode()))
    for distribution in ("duckdb", "dlt", "pyarrow", "dbt-core", "dbt-duckdb"):
        try:
            runtime = installed_version(distribution)
        except PackageNotFoundError:
            runtime = "unavailable"
        sources.append((f"runtime/{distribution}", runtime.encode()))
    return historical_resume.code_identity(sources)


def _resume_file_identity(path: Path) -> tuple[int, int, int, int]:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
        raise CatalogueError("historical resume database file is unsafe")
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _resume_file_digest(store: Store, path: Path, limit: int) -> tuple[str, int]:
    checksum = hashlib.sha256()
    size = 0
    store.check_deadline()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            store.check_deadline()
            size += len(chunk)
            if size > limit:
                raise CatalogueError("historical resume database exceeds its storage limit")
            checksum.update(chunk)
    store.check_deadline()
    return checksum.hexdigest(), size


def _copy_resume_file(store: Store, source: Path, destination: Path, limit: int) -> tuple[str, int]:
    checksum = hashlib.sha256()
    copied = 0
    store.check_deadline()
    with source.open("rb") as input_stream, destination.open("xb") as output_stream:
        while chunk := input_stream.read(1024 * 1024):
            store.check_deadline()
            copied += len(chunk)
            if copied > limit:
                raise CatalogueError("historical resume database exceeds its storage limit")
            checksum.update(chunk)
            output_stream.write(chunk)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    store.check_deadline()
    return checksum.hexdigest(), copied


def _recover_resume_candidate(store: Store, directory: Path) -> None:
    database = directory / "warehouse.duckdb"
    wal = database.with_name(database.name + ".wal")
    if not database.is_file() or database.is_symlink() or wal.is_symlink():
        raise CatalogueError("historical resume candidate is missing or unsafe")
    try:
        with (
            _duckdb_work_config(store, directory) as config,
            duckdb.connect(str(database), config=config) as connection,
        ):
            try:
                with interrupt_at_deadline(store, connection.interrupt):
                    connection.execute("checkpoint")
            except duckdb.InterruptException as exc:
                raise BudgetExceeded(
                    "operation deadline exhausted recovering historical stage"
                ) from exc
    except duckdb.Error as exc:
        raise CatalogueError("historical resume candidate database is invalid") from exc
    store.sync(directory.relative_to(store.root).as_posix())
    fsync_file(database)
    fsync_directory(directory)
    if wal.exists() or wal.is_symlink():
        raise CatalogueError("recovered historical candidate retained an uncheckpointed WAL")


def _prepare_historical_resume_candidate(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> tuple[str, Path, str, bool]:
    if _verified_mount is not None:
        historical_resume.validate_resume_mount_context(
            store=store, state=state, verified_mount=_verified_mount
        )
    stage_relative = historical_resume.stage_relative(stage_name)
    stage = store.path(stage_relative)
    candidate_relative = f"{stage_relative}/resume-candidate"
    candidate = store.path(candidate_relative)
    source = stage / "warehouse.duckdb"
    source_wal = source.with_name(source.name + ".wal")
    adoption_path = stage / "resume-adoption.json"
    binding = {
        "root": str(store.root.resolve()),
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "stage": stage_name,
    }
    marker = candidate / "resume-source.json"
    if (
        not candidate.exists()
        and not candidate.is_symlink()
        and (adoption_path.exists() or adoption_path.is_symlink())
    ):
        raise CatalogueError("adopted historical candidate is missing")
    if candidate.exists() or candidate.is_symlink():
        recovered_wal = False
        _validate_historical_stage(candidate, store.root.lstat().st_dev)
        if not marker.is_file() or marker.is_symlink():
            if adoption_path.exists() or adoption_path.is_symlink():
                raise CatalogueError("adopted historical candidate has no source proof")
            store.remove(candidate_relative)
        else:
            proof = decode(regular_bytes(marker, 1024**2))
            result = historical_resume.validate_proof(proof, "resume-source", binding)
            cleanup_path = stage / "resume-cleanup-intent.json"
            if adoption_path.exists() or adoption_path.is_symlink():
                _validate_historical_adoption_origin(store, state, stage_name, proof, result)
            _validate_historical_resume_source_presence(
                store,
                source,
                source_wal,
                result,
                adopted=adoption_path.is_file() and not adoption_path.is_symlink(),
                cleanup_intent=cleanup_path,
                stage_name=stage_name,
                state=state,
                source_proof_sha256=proof["sha256"],
                _verified_mount=_verified_mount,
            )
            candidate_wal = candidate / "warehouse.duckdb.wal"
            if candidate_wal.is_symlink():
                raise CatalogueError("historical resume candidate WAL is unsafe")
            if candidate_wal.exists():
                _recover_resume_candidate(store, candidate)
                recovered_wal = True
            else:
                _validate_resume_candidate_database(store, candidate)
            return candidate_relative, candidate, proof["sha256"], recovered_wal

    try:
        source_identity = _resume_file_identity(source)
        source_wal_identity = _resume_file_identity(source_wal) if source_wal.exists() else None
    except OSError as exc:
        raise CatalogueError("historical resume stage has no safe source database") from exc
    source_hash, source_size = _resume_file_digest(store, source, store.limits.max_database_bytes)
    wal_hash = None
    wal_size = 0
    if source_wal_identity is not None:
        wal_hash, wal_size = _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
    copy_size = source_size + wal_size
    if store.usage()["database_bytes"] + copy_size > store.limits.max_database_bytes:
        raise BudgetExceeded("historical resume copy exceeds the database storage allowance")
    store.reserve_storage(copy_size)
    try:
        candidate.mkdir()
        copied_hash, copied_size = _copy_resume_file(
            store, source, candidate / "warehouse.duckdb", store.limits.max_database_bytes
        )
        if (copied_hash, copied_size) != (source_hash, source_size):
            raise CatalogueError("historical resume database copy differs from its source")
        if source_wal_identity is not None:
            copied_wal_hash, copied_wal_size = _copy_resume_file(
                store,
                source_wal,
                candidate / "warehouse.duckdb.wal",
                store.limits.max_database_bytes,
            )
            if (copied_wal_hash, copied_wal_size) != (wal_hash, wal_size):
                raise CatalogueError("historical resume WAL copy differs from its source")
        store.sync(candidate_relative)
    finally:
        store.release_storage(copy_size)

    if _resume_file_identity(source) != source_identity or _resume_file_digest(
        store, source, store.limits.max_database_bytes
    ) != (source_hash, source_size):
        raise CatalogueError("historical resume source database changed while copied")
    current_wal_identity = _resume_file_identity(source_wal) if source_wal.exists() else None
    if current_wal_identity != source_wal_identity or (
        source_wal_identity is not None
        and _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
        != (wal_hash, wal_size)
    ):
        raise CatalogueError("historical resume source WAL changed while copied")

    candidate_wal = candidate / "warehouse.duckdb.wal"
    recovered_wal = candidate_wal.exists()
    if candidate_wal.exists():
        _recover_resume_candidate(store, candidate)
    else:
        _validate_resume_candidate_database(store, candidate)
    candidate_hash, candidate_size = _resume_file_digest(
        store, candidate / "warehouse.duckdb", store.limits.max_database_bytes
    )
    result = {
        "source_identity": list(source_identity),
        "source_sha256": source_hash,
        "source_size": source_size,
        "source_wal_identity": (
            list(source_wal_identity) if source_wal_identity is not None else None
        ),
        "source_wal_sha256": wal_hash,
        "source_wal_size": wal_size,
        "candidate_seed_sha256": candidate_hash,
        "candidate_seed_size": candidate_size,
    }
    proof = historical_resume.proof_record("resume-source", binding, result)
    store.write(f"{candidate_relative}/resume-source.json", proof)
    if decode(regular_bytes(marker, 1024**2)) != proof:
        raise CatalogueError("historical resume source proof failed readback")
    fsync_file(marker)
    fsync_directory(candidate)
    fsync_directory(stage)
    return candidate_relative, candidate, proof["sha256"], recovered_wal


def _validate_resume_candidate_database(store: Store, directory: Path) -> None:
    database = directory / "warehouse.duckdb"
    if not database.is_file() or database.is_symlink():
        raise CatalogueError("historical resume candidate is missing or unsafe")
    try:
        with (
            _duckdb_work_config(store, directory) as config,
            duckdb.connect(str(database), read_only=True, config=config) as connection,
        ):
            connection.execute("select 1").fetchone()
    except duckdb.Error as exc:
        raise CatalogueError("historical resume candidate database is invalid") from exc


def _validate_historical_resume_source_presence(
    store: Store,
    source: Path,
    source_wal: Path,
    source_result: dict[str, Any],
    *,
    adopted: bool,
    cleanup_intent: Path,
    stage_name: str,
    state: dict[str, Any],
    source_proof_sha256: str,
    _verified_mount: dict[str, Any] | None = None,
) -> None:
    source_exists = source.exists() or source.is_symlink()
    wal_exists = source_wal.exists() or source_wal.is_symlink()
    if not source_exists and not wal_exists:
        if not adopted:
            raise CatalogueError("historical resume source database disappeared before adoption")
        _validate_historical_cleanup_intent(
            store, state, stage_name, source_result, source_proof_sha256, cleanup_intent
        )
        return
    if not source_exists:
        if not adopted or not cleanup_intent.exists():
            raise CatalogueError("historical resume source database is missing")
        _validate_historical_cleanup_intent(
            store, state, stage_name, source_result, source_proof_sha256, cleanup_intent
        )
        if (
            source_result.get("source_wal_identity") is None
            or source_wal.is_symlink()
            or not historical_resume.resume_file_identity_matches(
                source_result.get("source_wal_identity"),
                _resume_file_identity(source_wal),
                store=store,
                state=state,
                verified_mount=_verified_mount,
            )
            or _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
            != (source_result.get("source_wal_sha256"), source_result.get("source_wal_size"))
        ):
            raise CatalogueError("historical resume source WAL changed during cleanup")
        return
    if (
        source.is_symlink()
        or not historical_resume.resume_file_identity_matches(
            source_result.get("source_identity"),
            _resume_file_identity(source),
            store=store,
            state=state,
            verified_mount=_verified_mount,
        )
        or _resume_file_digest(store, source, store.limits.max_database_bytes)
        != (source_result.get("source_sha256"), source_result.get("source_size"))
    ):
        raise CatalogueError("historical resume source database changed after staging")
    expected_wal = source_result.get("source_wal_identity")
    if expected_wal is None:
        if wal_exists:
            raise CatalogueError("historical resume source gained a WAL after staging")
    elif (
        not wal_exists
        or source_wal.is_symlink()
        or not historical_resume.resume_file_identity_matches(
            expected_wal,
            _resume_file_identity(source_wal),
            store=store,
            state=state,
            verified_mount=_verified_mount,
        )
        or _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
        != (source_result.get("source_wal_sha256"), source_result.get("source_wal_size"))
    ):
        if not (adopted and cleanup_intent.exists()):
            raise CatalogueError("historical resume source WAL changed after staging")
        _validate_historical_cleanup_intent(
            store, state, stage_name, source_result, source_proof_sha256, cleanup_intent
        )


def _historical_landing_schema_sha256() -> str:
    return digest(
        json_bytes(
            {
                "columns": list(_LANDING_COLUMNS),
                "nullable": ["event_ticker", "provider_updated_at", "series_ticker", "task_scope"],
                "types": ["VARCHAR"] * len(_LANDING_COLUMNS),
            }
        )
    )


def _historical_scope_sha256(
    state: dict[str, Any], selection: dict[str, Any], task_indices: set[int]
) -> str:
    return digest(
        json_bytes(
            {
                "historical_selection": selection,
                "task_indices": sorted(task_indices),
                "state_sha256": digest(json_bytes(dict(state))),
            }
        )
    )


def _historical_read_only_config() -> dict[str, str]:
    return {"memory_limit": DBT_MEMORY_LIMIT, "threads": str(DBT_THREADS)}


def _load_self_proof(path: Path, phase: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    if path.is_symlink():
        raise CatalogueError("historical stage proof is unsafe")
    if not path.exists():
        return None
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CatalogueError("historical stage proof is unsafe")
    proof = decode(regular_bytes(path, 1024**2))
    binding = proof.get("binding") if isinstance(proof, dict) else None
    if not isinstance(binding, dict):
        raise CatalogueError("historical stage proof binding is invalid")
    result = historical_resume.validate_proof(proof, phase, binding)
    return proof, result


def _historical_adoption_record(store: Store, stage_name: str) -> dict[str, Any] | None:
    relative = historical_resume.stage_relative(stage_name)
    loaded = _load_self_proof(store.path(f"{relative}/resume-adoption.json"), "historical-adoption")
    return loaded[0] if loaded is not None else None


def _candidate_phase_relative(
    candidate_relative: str,
    candidate_sha256: str,
    phase_name: str,
    adoption_sha256: str | None,
    landing_proof_sha256: str,
) -> str:
    if (
        phase_name not in {"landing-ready", "dbt-complete"}
        or re.fullmatch(r"[0-9a-f]{64}", landing_proof_sha256) is None
    ):
        raise CatalogueError("historical candidate phase name is invalid")
    adoption = adoption_sha256 or "origin"
    return (
        f"{candidate_relative}/resume-candidate-phase-{phase_name}-"
        f"{adoption}-{candidate_sha256}-{landing_proof_sha256}.json"
    )


def _historical_candidate_phase_binding(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    candidate_sha256: str,
    source_proof_sha256: str,
    landing_proof_sha256: str,
    scope_sha256: str,
) -> dict[str, Any]:
    adoption = _historical_adoption_record(store, stage_name)
    return {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "candidate_sha256": candidate_sha256,
        "source_proof_sha256": source_proof_sha256,
        "adoption_sha256": adoption["sha256"] if adoption is not None else None,
        "landing_proof_sha256": landing_proof_sha256,
        "scope_sha256": scope_sha256,
        "schema_sha256": _historical_landing_schema_sha256(),
        "code_sha256": _historical_resume_code_identity(),
    }


def _write_historical_candidate_phase(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    candidate_relative: str,
    candidate: Path,
    source_proof_sha256: str,
    selection: dict[str, Any],
    task_indices: set[int],
    landing_proof: dict[str, Any],
    phase_name: str,
) -> dict[str, Any]:
    database = candidate / "warehouse.duckdb"
    if database.with_name(database.name + ".wal").exists():
        raise CatalogueError("historical candidate phase cannot be sealed with a WAL")
    fsync_file(database)
    fsync_directory(candidate)
    candidate_sha256, candidate_size = _resume_file_digest(
        store, database, store.limits.max_database_bytes
    )
    scope_sha256 = _historical_scope_sha256(state, selection, task_indices)
    binding = _historical_candidate_phase_binding(
        store,
        state,
        stage_name,
        candidate_sha256,
        source_proof_sha256,
        landing_proof["sha256"],
        scope_sha256,
    )
    proof = historical_resume.proof_record(
        "historical-candidate-phase",
        binding,
        {
            "phase": phase_name,
            "candidate_size": candidate_size,
            "observation_count": landing_proof["result"]["observation_count"],
            "membership_count": landing_proof["result"]["membership_count"],
            "eligibility_count": landing_proof["result"]["eligibility_count"],
        },
    )
    adoption = _historical_adoption_record(store, stage_name)
    relative = _candidate_phase_relative(
        candidate_relative,
        candidate_sha256,
        phase_name,
        adoption["sha256"] if adoption is not None else None,
        landing_proof["sha256"],
    )
    path = store.path(relative)
    if path.exists() or path.is_symlink():
        current = decode(regular_bytes(path, 1024**2))
        if current != proof:
            raise CatalogueError("historical candidate phase seal conflicts with its origin")
    else:
        store.write(relative, proof)
    if decode(regular_bytes(path, 1024**2)) != proof:
        raise CatalogueError("historical candidate phase seal failed readback")
    fsync_file(path)
    fsync_directory(candidate)
    return proof


def _reuse_historical_candidate_phase(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    candidate_relative: str,
    candidate: Path,
    source_proof_sha256: str,
    selection: dict[str, Any],
    task_indices: set[int],
    verified_pages: dict[str, str],
) -> tuple[int, _OperationEvidence, dict[str, Any]] | None:
    database = candidate / "warehouse.duckdb"
    wal = database.with_name(database.name + ".wal")
    if wal.exists() or wal.is_symlink():
        return None
    candidate_sha256, _candidate_size = _resume_file_digest(
        store, database, store.limits.max_database_bytes
    )
    adoption = _historical_adoption_record(store, stage_name)
    adoption_sha256 = adoption["sha256"] if adoption is not None else None
    landing_alias = _load_self_proof(candidate / "resume-landing.json", "historical-landing")
    if landing_alias is None:
        return None
    landing_proof_sha256 = landing_alias[0]["sha256"]
    loaded = None
    for phase_name in ("dbt-complete", "landing-ready"):
        seal_path = store.path(
            _candidate_phase_relative(
                candidate_relative,
                candidate_sha256,
                phase_name,
                adoption_sha256,
                landing_proof_sha256,
            )
        )
        loaded = _load_self_proof(seal_path, "historical-candidate-phase")
        if loaded is not None:
            break
    if loaded is None:
        return None
    seal, seal_result = loaded
    if seal["binding"].get("landing_proof_sha256") != landing_proof_sha256:
        raise CatalogueError("historical candidate phase seal changed its landing proof")
    scope_sha256 = _historical_scope_sha256(state, selection, task_indices)
    expected_fields = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "candidate_sha256": candidate_sha256,
        "source_proof_sha256": source_proof_sha256,
        "adoption_sha256": adoption_sha256,
        "scope_sha256": scope_sha256,
        "schema_sha256": _historical_landing_schema_sha256(),
        "code_sha256": _historical_resume_code_identity(),
    }
    if any(seal["binding"].get(key) != value for key, value in expected_fields.items()):
        if seal["binding"].get("code_sha256") != expected_fields["code_sha256"]:
            return None
        raise CatalogueError("historical candidate phase seal does not match its scope")
    if (
        not isinstance(landing_proof_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", landing_proof_sha256) is None
    ):
        raise CatalogueError("historical candidate phase seal lacks a landing proof")
    landing_path = candidate / f"resume-landing-{landing_proof_sha256}.json"
    loaded_landing = _load_self_proof(landing_path, "historical-landing")
    if loaded_landing is None or loaded_landing[0].get("sha256") != landing_proof_sha256:
        raise CatalogueError("historical candidate phase seal lost its landing proof")
    landing_proof, landing_result = loaded_landing
    if any(
        seal_result.get(key) != landing_result.get(key)
        for key in ("observation_count", "membership_count", "eligibility_count")
    ):
        raise CatalogueError("historical candidate phase counts differ from its landing proof")

    for _manifest, _payload in capture_pages(
        store, state, evidence=verified_pages, task_indices=task_indices
    ):
        store.check_deadline()
    operation_evidence = _operation_evidence(store, state, 3, verified_pages)
    landing_binding = landing_proof["binding"]
    if (
        landing_binding.get("root") != str(store.root.resolve())
        or landing_binding.get("stage") != stage_name
        or landing_binding.get("capture_id") != state["capture_id"]
        or landing_binding.get("state_sha256") != digest(json_bytes(dict(state)))
        or landing_binding.get("source_proof_sha256") != source_proof_sha256
        or landing_binding.get("historical_selection") != selection
        or landing_binding.get("task_indices") != sorted(task_indices)
        or landing_binding.get("scope_sha256") != scope_sha256
        or landing_binding.get("schema_sha256") != _historical_landing_schema_sha256()
        or landing_binding.get("code_sha256") != _historical_resume_code_identity()
        or landing_binding.get("evidence_sha256") != digest(json_bytes(operation_evidence.evidence))
        or not isinstance(landing_binding.get("candidate_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", landing_binding["candidate_sha256"]) is None
    ):
        raise CatalogueError("historical landing proof differs from current receipt evidence")
    try:
        with (
            duckdb.connect(
                str(database), read_only=True, config=_historical_read_only_config()
            ) as connection,
        ):
            _check_historical_landing_schemas(connection)
            observation_count = connection.execute(
                "select count(*) from landing.observations"
            ).fetchone()[0]
            membership_count = connection.execute(
                "select count(*) from landing.membership"
            ).fetchone()[0]
            eligibility_count = connection.execute(
                "select count(*) from landing.parent_eligibility"
            ).fetchone()[0]
    except duckdb.Error as exc:
        raise CatalogueError("historical candidate phase output is invalid") from exc
    if (
        observation_count != landing_result.get("observation_count")
        or membership_count != landing_result.get("membership_count")
        or eligibility_count != landing_result.get("eligibility_count")
    ):
        raise CatalogueError("historical candidate phase counts changed after sealing")
    _check_historical_coverage(database, state["capture_id"], state.get("complete") is True)
    return observation_count, operation_evidence, landing_proof


def _historical_readiness_context(
    store: Store, capture_id: str, stage_name: str
) -> tuple[str, Path, dict[str, Any], dict[str, Any], set[int]]:
    if store.scope is not None:
        raise CatalogueError("historical readiness requires a catalogue root")
    if not isinstance(capture_id, str) or re.fullmatch(r"[0-9a-f]{32}", capture_id) is None:
        raise CatalogueError("historical readiness capture identity is invalid")
    stage_relative = historical_resume.stage_relative(stage_name)
    stage = store.path(stage_relative)
    _validate_historical_stage(stage, store.root.lstat().st_dev)
    state = read_capture(store, capture_id)
    if state.get("capture_id") != capture_id:
        raise CatalogueError("historical readiness capture does not match its stage")
    if state.get("incremental"):
        raise CatalogueError("incremental captures cannot use historical readiness")
    selection, task_indices = _historical_profile(state)
    return stage_relative, stage, state, selection, task_indices


def _read_source_proof(
    store: Store, state: dict[str, Any], stage_name: str, candidate: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = {
        "root": str(store.root.resolve()),
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "stage": stage_name,
    }
    loaded = _load_self_proof(candidate / "resume-source.json", "resume-source")
    if loaded is None:
        raise CatalogueError("historical resume source proof is missing")
    proof, result = loaded
    historical_resume.validate_proof(proof, "resume-source", binding)
    return proof, result


def _validate_historical_adoption_origin(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    source_proof: dict[str, Any],
    source_result: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    relative = historical_resume.stage_relative(stage_name)
    loaded = _load_self_proof(store.path(f"{relative}/resume-adoption.json"), "historical-adoption")
    if loaded is None:
        raise CatalogueError("historical adoption record is missing")
    proof, result = loaded
    binding = proof["binding"]
    if (
        binding.get("root") != str(store.root.resolve())
        or binding.get("stage") != stage_name
        or binding.get("capture_id") != state["capture_id"]
        or binding.get("state_sha256") != digest(json_bytes(dict(state)))
        or binding.get("source_proof_sha256") != source_proof.get("sha256")
        or binding.get("candidate_sha256") != result.get("candidate_sha256")
        or result.get("source_sha256") != source_result.get("source_sha256")
        or result.get("source_size") != source_result.get("source_size")
        or result.get("source_identity") != source_result.get("source_identity")
        or result.get("source_wal_sha256") != source_result.get("source_wal_sha256")
        or result.get("source_wal_size") != source_result.get("source_wal_size")
        or result.get("source_wal_identity") != source_result.get("source_wal_identity")
        or not isinstance(result.get("candidate_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", result["candidate_sha256"]) is None
        or not isinstance(binding.get("readiness_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", binding["readiness_sha256"]) is None
        or not isinstance(binding.get("verification_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", binding["verification_sha256"]) is None
        or any(
            isinstance(result.get(key), bool)
            or not isinstance(result.get(key), int)
            or result[key] < 0
            for key in ("observation_count", "membership_count", "eligibility_count")
        )
    ):
        raise CatalogueError("historical adoption origin does not match its source")
    return proof, result


def _validate_historical_cleanup_intent(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    source_result: dict[str, Any],
    source_proof_sha256: str,
    path: Path,
) -> dict[str, Any]:
    loaded = _load_self_proof(path, "historical-source-cleanup")
    if loaded is None:
        raise CatalogueError("historical source cleanup has no durable intent")
    proof, result = loaded
    adoption = _historical_adoption_record(store, stage_name)
    binding = proof["binding"]
    if (
        adoption is None
        or binding.get("root") != str(store.root.resolve())
        or binding.get("stage") != stage_name
        or binding.get("capture_id") != state["capture_id"]
        or binding.get("state_sha256") != digest(json_bytes(dict(state)))
        or binding.get("source_proof_sha256") != source_proof_sha256
        or binding.get("adoption_sha256") != adoption.get("sha256")
        or binding.get("retirement_policy") != "coexistence-capacity-relief-approved-v1"
        or result.get("source_sha256") != source_result.get("source_sha256")
        or result.get("source_size") != source_result.get("source_size")
        or result.get("source_identity") != source_result.get("source_identity")
        or result.get("source_wal_sha256") != source_result.get("source_wal_sha256")
        or result.get("source_wal_size") != source_result.get("source_wal_size")
        or result.get("source_wal_identity") != source_result.get("source_wal_identity")
        or result.get("retire_source") is not True
    ):
        raise CatalogueError("historical source cleanup intent does not match its adoption")
    return proof


def _historical_cleanup_retry_receipt_matches(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    source_proof: dict[str, Any],
    source_result: dict[str, Any],
    readiness: dict[str, Any],
    current_verification: dict[str, Any],
    receipt: Any,
    adoption: dict[str, Any],
    adoption_result: dict[str, Any],
    cleanup_path: Path,
) -> bool:
    if not (cleanup_path.exists() or cleanup_path.is_symlink()):
        return False
    try:
        _validate_historical_cleanup_intent(
            store,
            state,
            stage_name,
            source_result,
            source_proof["sha256"],
            cleanup_path,
        )
        if not isinstance(receipt, dict) or not isinstance(receipt.get("binding"), dict):
            return False
        origin_binding = receipt["binding"]
        origin_result = historical_resume.validate_proof(
            receipt, "historical-readiness-verification", origin_binding
        )
    except (CatalogueError, TypeError, ValueError):
        return False

    adoption_binding = adoption["binding"]
    current_binding = current_verification["binding"]
    expected_counts = {
        "observation_count": adoption_result["observation_count"],
        "membership_count": adoption_result["membership_count"],
        "eligibility_count": adoption_result["eligibility_count"],
    }
    return (
        receipt.get("sha256") == adoption_binding.get("verification_sha256")
        and origin_binding.get("root") == str(store.root.resolve())
        and origin_binding.get("stage") == stage_name
        and origin_binding.get("capture_id") == state["capture_id"]
        and origin_binding.get("state_sha256") == digest(json_bytes(dict(state)))
        and origin_binding.get("readiness_sha256") == adoption_binding.get("readiness_sha256")
        and origin_binding.get("candidate_sha256") == adoption_result.get("candidate_sha256")
        and origin_binding.get("code_sha256") == adoption_binding.get("code_sha256")
        and origin_binding.get("adoption_origin_sha256") is None
        and current_binding.get("root") == origin_binding.get("root")
        and current_binding.get("stage") == origin_binding.get("stage")
        and current_binding.get("capture_id") == origin_binding.get("capture_id")
        and current_binding.get("state_sha256") == origin_binding.get("state_sha256")
        and current_binding.get("candidate_sha256") == adoption_result.get("candidate_sha256")
        and current_binding.get("code_sha256") == origin_binding.get("code_sha256")
        and current_binding.get("evidence_sha256") == origin_binding.get("evidence_sha256")
        and current_binding.get("adoption_origin_sha256") == adoption.get("sha256")
        and readiness["binding"].get("adoption_origin_sha256") == adoption.get("sha256")
        and readiness["binding"].get("candidate_sha256") == adoption_result.get("candidate_sha256")
        and origin_result == expected_counts
        and current_verification["result"] == expected_counts
        and {
            key: readiness["result"].get(key)
            for key in ("observation_count", "membership_count", "eligibility_count")
        }
        == expected_counts
    )


def _historical_readiness_proof(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    candidate: Path,
    source_proof: dict[str, Any],
    landing_proof: dict[str, Any],
    selection: dict[str, Any],
    task_indices: set[int],
    *,
    adoption_origin_sha256: str | None = None,
) -> dict[str, Any]:
    candidate_sha256, candidate_size = _resume_file_digest(
        store, candidate / "warehouse.duckdb", store.limits.max_database_bytes
    )
    scope_sha256 = _historical_scope_sha256(state, selection, task_indices)
    binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "source_proof_sha256": source_proof["sha256"],
        "landing_proof_sha256": landing_proof["sha256"],
        "candidate_sha256": candidate_sha256,
        "scope_sha256": scope_sha256,
        "schema_sha256": _historical_landing_schema_sha256(),
        "code_sha256": _historical_resume_code_identity(),
        "adoption_origin_sha256": adoption_origin_sha256,
    }
    result = landing_proof["result"]
    source = source_proof["result"]
    return historical_resume.proof_record(
        "historical-readiness",
        binding,
        {
            "candidate_size": candidate_size,
            "source_sha256": source["source_sha256"],
            "source_size": source["source_size"],
            "source_wal_sha256": source["source_wal_sha256"],
            "source_wal_size": source["source_wal_size"],
            "observation_count": result["observation_count"],
            "membership_count": result["membership_count"],
            "eligibility_count": result["eligibility_count"],
            "prefix_replay_rows": result.get("prefix_replay_rows", 0),
            "prefix_replay_policy": result.get("prefix_replay_policy"),
        },
    )


def _persist_historical_readiness_proof(
    store: Store, stage_relative: str, proof: dict[str, Any]
) -> None:
    relative = f"{stage_relative}/resume-readiness.json"
    path = store.path(relative)
    store.write(relative, proof)
    if decode(regular_bytes(path, 1024**2)) != proof:
        raise CatalogueError("historical readiness seal failed readback")
    fsync_file(path)
    fsync_directory(path.parent)


def _verify_historical_readiness_locked(
    store: Store,
    capture_id: str,
    stage_name: str,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], Path, dict[str, Any]]:
    stage_relative, stage, state, selection, task_indices = _historical_readiness_context(
        store, capture_id, stage_name
    )
    if _verified_mount is not None:
        historical_resume.validate_resume_mount_context(
            store=store, state=state, verified_mount=_verified_mount
        )
    candidate_relative = f"{stage_relative}/resume-candidate"
    candidate = store.path(candidate_relative)
    database = candidate / "warehouse.duckdb"
    wal = database.with_name(database.name + ".wal")
    if wal.exists() or wal.is_symlink():
        raise CatalogueError("historical readiness candidate has an uncheckpointed WAL")
    source_proof, source_result = _read_source_proof(store, state, stage_name, candidate)
    adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
    cleanup_path = store.path(f"{stage_relative}/resume-cleanup-intent.json")
    adoption_origin_sha256 = None
    if adoption_path.exists() or adoption_path.is_symlink():
        adoption, _adoption_result = _validate_historical_adoption_origin(
            store, state, stage_name, source_proof, source_result
        )
        adoption_origin_sha256 = adoption["sha256"]
    _validate_historical_resume_source_presence(
        store,
        stage / "warehouse.duckdb",
        stage / "warehouse.duckdb.wal",
        source_result,
        adopted=adoption_path.is_file() and not adoption_path.is_symlink(),
        cleanup_intent=cleanup_path,
        stage_name=stage_name,
        state=state,
        source_proof_sha256=source_proof["sha256"],
        _verified_mount=_verified_mount,
    )
    candidate_sha256, _candidate_size = _resume_file_digest(
        store, database, store.limits.max_database_bytes
    )
    readiness_path = store.path(f"{stage_relative}/resume-readiness.json")
    loaded = _load_self_proof(readiness_path, "historical-readiness")
    if loaded is None:
        raise CatalogueError("historical readiness seal is missing")
    readiness_proof, readiness_result = loaded
    binding = readiness_proof["binding"]
    landing_sha256 = binding.get("landing_proof_sha256")
    expected_binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": capture_id,
        "state_sha256": digest(json_bytes(dict(state))),
        "source_proof_sha256": source_proof["sha256"],
        "landing_proof_sha256": landing_sha256,
        "candidate_sha256": candidate_sha256,
        "scope_sha256": _historical_scope_sha256(state, selection, task_indices),
        "schema_sha256": _historical_landing_schema_sha256(),
        "code_sha256": _historical_resume_code_identity(),
        "adoption_origin_sha256": adoption_origin_sha256,
    }
    if binding != expected_binding:
        raise CatalogueError("historical readiness proof does not match its candidate")
    if not isinstance(landing_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", landing_sha256) is None:
        raise CatalogueError("historical readiness landing proof identity is invalid")
    landing_path = candidate / f"resume-landing-{landing_sha256}.json"
    landing_loaded = _load_self_proof(landing_path, "historical-landing")
    if landing_loaded is None or landing_loaded[0].get("sha256") != binding.get(
        "landing_proof_sha256"
    ):
        raise CatalogueError("historical readiness landing proof is missing")
    landing_proof, landing_result = landing_loaded
    expected_landing_binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": capture_id,
        "state_sha256": digest(json_bytes(dict(state))),
        "historical_selection": selection,
        "task_indices": sorted(task_indices),
        "code_sha256": _historical_resume_code_identity(),
        "candidate_sha256": candidate_sha256,
        "source_proof_sha256": source_proof["sha256"],
        "scope_sha256": _historical_scope_sha256(state, selection, task_indices),
        "schema_sha256": _historical_landing_schema_sha256(),
    }
    if any(
        landing_proof["binding"].get(key) != value
        for key, value in expected_landing_binding.items()
    ):
        raise CatalogueError("historical readiness landing proof has a mismatched binding")
    if any(
        readiness_result.get(key) != landing_result.get(key)
        for key in ("observation_count", "membership_count", "eligibility_count")
    ):
        raise CatalogueError("historical readiness counts differ from landing proof")

    verified_pages: dict[str, str] = {}
    for _manifest, _payload in capture_pages(
        store, state, evidence=verified_pages, task_indices=task_indices
    ):
        store.check_deadline()
    operation_evidence = _operation_evidence(store, state, 3, verified_pages)
    if landing_proof["binding"].get("evidence_sha256") != digest(
        json_bytes(operation_evidence.evidence)
    ):
        raise CatalogueError("historical readiness evidence differs from its landing proof")
    try:
        with (
            duckdb.connect(
                str(database), read_only=True, config=_historical_read_only_config()
            ) as connection,
        ):
            _check_historical_landing_schemas(connection)
            counts = {
                "observation_count": connection.execute(
                    "select count(*) from landing.observations"
                ).fetchone()[0],
                "membership_count": connection.execute(
                    "select count(*) from landing.membership"
                ).fetchone()[0],
                "eligibility_count": connection.execute(
                    "select count(*) from landing.parent_eligibility"
                ).fetchone()[0],
            }
    except duckdb.Error as exc:
        raise CatalogueError("historical readiness candidate is invalid") from exc
    if any(counts[key] != readiness_result.get(key) for key in counts):
        raise CatalogueError("historical readiness candidate counts changed")
    _check_historical_coverage(database, capture_id, state.get("complete") is True)
    verification_binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": capture_id,
        "state_sha256": digest(json_bytes(dict(state))),
        "readiness_sha256": readiness_proof["sha256"],
        "candidate_sha256": candidate_sha256,
        "evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
        "code_sha256": _historical_resume_code_identity(),
        "adoption_origin_sha256": adoption_origin_sha256,
    }
    verification = historical_resume.proof_record(
        "historical-readiness-verification", verification_binding, counts
    )
    return verification, readiness_proof, candidate, source_result


def historical_readiness_snapshot(
    store: Store,
    capture_id: str,
    stage_name: str,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pin a historical stage's source files and receipt evidence without opening its database."""
    with store.writer_guard():
        stage_relative, stage, state, selection, task_indices = _historical_readiness_context(
            store, capture_id, stage_name
        )
        if _verified_mount is not None:
            historical_resume.validate_resume_mount_context(
                store=store, state=state, verified_mount=_verified_mount
            )
        source = stage / "warehouse.duckdb"
        source_wal = source.with_name(source.name + ".wal")
        try:
            source_identity = _resume_file_identity(source)
            source_sha256, source_size = _resume_file_digest(
                store, source, store.limits.max_database_bytes
            )
            source_wal_identity = _resume_file_identity(source_wal) if source_wal.exists() else None
            source_wal_sha256, source_wal_size = (
                _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
                if source_wal_identity is not None
                else (None, 0)
            )
        except OSError as exc:
            raise CatalogueError("historical source snapshot is missing or unsafe") from exc
        if _resume_file_identity(source) != source_identity or _resume_file_digest(
            store, source, store.limits.max_database_bytes
        ) != (source_sha256, source_size):
            raise CatalogueError("historical source database changed during snapshot")
        current_wal_identity = _resume_file_identity(source_wal) if source_wal.exists() else None
        if current_wal_identity != source_wal_identity or (
            source_wal_identity is not None
            and _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
            != (source_wal_sha256, source_wal_size)
        ):
            raise CatalogueError("historical source WAL changed during snapshot")

        verified_pages: dict[str, str] = {}
        receipt_count = 0
        for _manifest, _payload in capture_pages(
            store, state, evidence=verified_pages, task_indices=task_indices
        ):
            store.check_deadline()
            receipt_count += 1
        operation_evidence = _operation_evidence(store, state, 3, verified_pages)
        state_sha256 = digest(json_bytes(dict(state)))
        evidence_sha256 = digest(json_bytes(operation_evidence.evidence))
        if _resume_file_identity(source) != source_identity or _resume_file_digest(
            store, source, store.limits.max_database_bytes
        ) != (source_sha256, source_size):
            raise CatalogueError("historical source database changed during snapshot")
        current_wal_identity = _resume_file_identity(source_wal) if source_wal.exists() else None
        if current_wal_identity != source_wal_identity or (
            source_wal_identity is not None
            and _resume_file_digest(store, source_wal, store.limits.max_database_bytes)
            != (source_wal_sha256, source_wal_size)
        ):
            raise CatalogueError("historical source WAL changed during snapshot")
        scope_sha256 = _historical_scope_sha256(state, selection, task_indices)
        binding = {
            "root": str(store.root.resolve()),
            "stage": stage_name,
            "capture_id": capture_id,
            "capture_state_sha256": state_sha256,
            "scope_sha256": scope_sha256,
            "code_sha256": _historical_resume_code_identity(),
        }
        return historical_resume.proof_record(
            "historical-source-snapshot",
            binding,
            {
                "source_identity": list(source_identity),
                "source_sha256": source_sha256,
                "source_size": source_size,
                "source_wal_identity": (
                    list(source_wal_identity) if source_wal_identity is not None else None
                ),
                "source_wal_sha256": source_wal_sha256,
                "source_wal_size": source_wal_size,
                "receipt_count": receipt_count,
                "receipt_evidence_sha256": evidence_sha256,
            },
        )


def prepare_historical_readiness(
    store: Store,
    capture_id: str,
    stage_name: str,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Copy and fully prove a historical stage, leaving its source intact for independent review."""
    with store.writer_guard():
        store.require_writable()
        stage_relative, _stage, state, selection, task_indices = _historical_readiness_context(
            store, capture_id, stage_name
        )
        candidate_relative, candidate, source_proof_sha256, recovered_wal = (
            _prepare_historical_resume_candidate(
                store, state, stage_name, _verified_mount=_verified_mount
            )
        )
        source_proof, source_result = _read_source_proof(store, state, stage_name, candidate)
        index = DiskParentIndex(store, collect_winners=False)
        try:
            observation_count, verified_pages, operation_evidence, landing_proof = (
                _resume_historical_landing(
                    store,
                    state,
                    stage_name,
                    candidate_relative,
                    candidate,
                    source_proof_sha256,
                    index,
                    task_indices,
                    selection,
                    force_revalidate=recovered_wal,
                )
            )
        finally:
            index.close()
        adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
        if adoption_path.exists() or adoption_path.is_symlink():
            adoption, adoption_result = _validate_historical_adoption_origin(
                store, state, stage_name, source_proof, source_result
            )
            readiness_proof = _historical_readiness_proof(
                store,
                state,
                stage_name,
                candidate,
                source_proof,
                landing_proof,
                selection,
                task_indices,
                adoption_origin_sha256=adoption["sha256"],
            )
            _persist_historical_readiness_proof(store, stage_relative, readiness_proof)
            summary = {
                "ready": True,
                "adopted": True,
                "capture_id": capture_id,
                "stage": stage_name,
                "adoption_sha256": adoption["sha256"],
                "readiness_sha256": readiness_proof["sha256"],
                "capture_state_sha256": digest(json_bytes(dict(state))),
                "candidate_sha256": _resume_file_digest(
                    store, candidate / "warehouse.duckdb", store.limits.max_database_bytes
                )[0],
                "origin_candidate_sha256": adoption_result["candidate_sha256"],
                "source_sha256": source_result["source_sha256"],
                "source_wal_sha256": source_result["source_wal_sha256"],
                "observation_count": observation_count,
                "membership_count": landing_proof["result"]["membership_count"],
                "eligibility_count": landing_proof["result"]["eligibility_count"],
                "scope_sha256": _historical_scope_sha256(state, selection, task_indices),
                "code_sha256": _historical_resume_code_identity(),
                "prefix_replay_rows": landing_proof["result"].get("prefix_replay_rows", 0),
                "evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
                "receipt_evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
            }
            return summary
        readiness_proof = _historical_readiness_proof(
            store,
            state,
            stage_name,
            candidate,
            source_proof,
            landing_proof,
            selection,
            task_indices,
        )
        _persist_historical_readiness_proof(store, stage_relative, readiness_proof)
        summary = {
            "ready": True,
            "adopted": False,
            "capture_id": capture_id,
            "stage": stage_name,
            "readiness_sha256": readiness_proof["sha256"],
            "capture_state_sha256": digest(json_bytes(dict(state))),
            "source_sha256": source_result["source_sha256"],
            "source_wal_sha256": source_result["source_wal_sha256"],
            "candidate_sha256": readiness_proof["binding"]["candidate_sha256"],
            "observation_count": observation_count,
            "membership_count": landing_proof["result"]["membership_count"],
            "eligibility_count": landing_proof["result"]["eligibility_count"],
            "scope_sha256": readiness_proof["binding"]["scope_sha256"],
            "code_sha256": readiness_proof["binding"]["code_sha256"],
            "prefix_replay_rows": readiness_proof["result"]["prefix_replay_rows"],
            "evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
            "receipt_evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
        }
        return summary


def verify_historical_readiness(
    store: Store,
    capture_id: str,
    stage_name: str,
    *,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read back the closed candidate and readiness proof without changing stage files."""
    with store.writer_guard():
        store.require_writable()
        verification, _readiness, _candidate, _source = _verify_historical_readiness_locked(
            store, capture_id, stage_name, _verified_mount=_verified_mount
        )
        return verification


def adopt_historical_readiness(
    store: Store,
    capture_id: str,
    stage_name: str,
    verification_receipt: dict[str, Any],
    *,
    retire_source: bool = False,
    retirement_policy: str | None = None,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind a separate verification receipt to the candidate, optionally retiring its source."""
    if not isinstance(retire_source, bool):
        raise CatalogueError("historical source retirement flag must be boolean")
    if retire_source and retirement_policy != "coexistence-capacity-relief-approved-v1":
        raise CatalogueError("historical source retirement lacks its approved capacity policy")
    if not retire_source and retirement_policy is not None:
        raise CatalogueError("historical source retirement policy requires explicit retirement")
    with store.writer_guard():
        store.require_writable()
        verification, readiness, _candidate, source_result = _verify_historical_readiness_locked(
            store, capture_id, stage_name, _verified_mount=_verified_mount
        )
        stage_relative = historical_resume.stage_relative(stage_name)
        state = read_capture(store, capture_id)
        source_proof_path = store.path(f"{stage_relative}/resume-candidate/resume-source.json")
        source_proof, _source_result = _read_source_proof(
            store, state, stage_name, source_proof_path.parent
        )
        adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
        cleanup_path = store.path(f"{stage_relative}/resume-cleanup-intent.json")
        if adoption_path.exists() or adoption_path.is_symlink():
            adoption, adoption_result = _validate_historical_adoption_origin(
                store, state, stage_name, source_proof, source_result
            )
            if (
                verification_receipt != verification
                and not _historical_cleanup_retry_receipt_matches(
                    store,
                    state,
                    stage_name,
                    source_proof,
                    source_result,
                    readiness,
                    verification,
                    verification_receipt,
                    adoption,
                    adoption_result,
                    cleanup_path,
                )
            ):
                raise CatalogueError(
                    "historical readiness verification receipt does not match readback"
                )
        else:
            if verification_receipt != verification:
                raise CatalogueError(
                    "historical readiness verification receipt does not match readback"
                )
            adoption_binding = {
                "root": str(store.root.resolve()),
                "stage": stage_name,
                "capture_id": capture_id,
                "state_sha256": digest(json_bytes(dict(read_capture(store, capture_id)))),
                "source_proof_sha256": source_proof["sha256"],
                "readiness_sha256": readiness["sha256"],
                "verification_sha256": verification["sha256"],
                "candidate_sha256": readiness["binding"]["candidate_sha256"],
                "code_sha256": readiness["binding"]["code_sha256"],
            }
            adoption_proof = historical_resume.proof_record(
                "historical-adoption",
                adoption_binding,
                {
                    "source_identity": source_result["source_identity"],
                    "source_sha256": source_result["source_sha256"],
                    "source_size": source_result["source_size"],
                    "source_wal_identity": source_result["source_wal_identity"],
                    "source_wal_sha256": source_result["source_wal_sha256"],
                    "source_wal_size": source_result["source_wal_size"],
                    "candidate_sha256": readiness["binding"]["candidate_sha256"],
                    "observation_count": readiness["result"]["observation_count"],
                    "membership_count": readiness["result"]["membership_count"],
                    "eligibility_count": readiness["result"]["eligibility_count"],
                },
            )
            relative = f"{stage_relative}/resume-adoption.json"
            store.write(relative, adoption_proof)
            if decode(regular_bytes(adoption_path, 1024**2)) != adoption_proof:
                raise CatalogueError("historical adoption record failed readback")
            fsync_file(adoption_path)
            fsync_directory(adoption_path.parent)
            adoption, adoption_result = adoption_proof, adoption_proof["result"]

        stage_relative, _stage, state, selection, task_indices = _historical_readiness_context(
            store, capture_id, stage_name
        )
        candidate_relative = f"{stage_relative}/resume-candidate"
        landing_loaded = _load_self_proof(
            store.path(f"{candidate_relative}/resume-landing.json"), "historical-landing"
        )
        if landing_loaded is None:
            raise CatalogueError("adopted historical candidate lost its landing proof")
        current_readiness = _historical_readiness_proof(
            store,
            state,
            stage_name,
            store.path(candidate_relative),
            source_proof,
            landing_loaded[0],
            selection,
            task_indices,
            adoption_origin_sha256=adoption["sha256"],
        )
        _persist_historical_readiness_proof(store, stage_relative, current_readiness)
        _write_historical_candidate_phase(
            store,
            state,
            stage_name,
            candidate_relative,
            store.path(candidate_relative),
            source_proof["sha256"],
            selection,
            task_indices,
            landing_loaded[0],
            "landing-ready",
        )

        if retire_source:
            intent_binding = {
                "root": str(store.root.resolve()),
                "stage": stage_name,
                "capture_id": capture_id,
                "state_sha256": digest(json_bytes(dict(read_capture(store, capture_id)))),
                "source_proof_sha256": source_proof["sha256"],
                "adoption_sha256": adoption["sha256"],
                "retirement_policy": retirement_policy,
            }
            intent = historical_resume.proof_record(
                "historical-source-cleanup",
                intent_binding,
                {
                    "retire_source": True,
                    "source_identity": source_result["source_identity"],
                    "source_sha256": source_result["source_sha256"],
                    "source_size": source_result["source_size"],
                    "source_wal_identity": source_result["source_wal_identity"],
                    "source_wal_sha256": source_result["source_wal_sha256"],
                    "source_wal_size": source_result["source_wal_size"],
                },
            )
            intent_path = store.path(f"{stage_relative}/resume-cleanup-intent.json")
            if intent_path.exists() or intent_path.is_symlink():
                existing = decode(regular_bytes(intent_path, 1024**2))
                if existing != intent:
                    raise CatalogueError("historical source cleanup intent conflicts with adoption")
            else:
                store.write(f"{stage_relative}/resume-cleanup-intent.json", intent)
            if decode(regular_bytes(intent_path, 1024**2)) != intent:
                raise CatalogueError("historical source cleanup intent failed readback")
            fsync_file(intent_path)
            fsync_directory(intent_path.parent)
            source = store.path(f"{stage_relative}/warehouse.duckdb")
            source_wal = source.with_name(source.name + ".wal")
            _validate_historical_resume_source_presence(
                store,
                source,
                source_wal,
                source_result,
                adopted=True,
                cleanup_intent=intent_path,
                stage_name=stage_name,
                state=read_capture(store, capture_id),
                source_proof_sha256=source_proof["sha256"],
                _verified_mount=_verified_mount,
            )
            for path, expected_identity, expected_sha in (
                (source, source_result["source_identity"], source_result["source_sha256"]),
                (
                    source_wal,
                    source_result["source_wal_identity"],
                    source_result["source_wal_sha256"],
                ),
            ):
                if expected_identity is None:
                    continue
                if path.exists() or path.is_symlink():
                    if (
                        path.is_symlink()
                        or not historical_resume.resume_file_identity_matches(
                            expected_identity,
                            _resume_file_identity(path),
                            store=store,
                            state=state,
                            verified_mount=_verified_mount,
                        )
                        or _resume_file_digest(store, path, store.limits.max_database_bytes)[0]
                        != expected_sha
                    ):
                        raise CatalogueError("historical source changed during approved cleanup")
                    path.unlink()
            fsync_directory(store.path(stage_relative))
            store.sync(stage_relative)

        return {
            "adopted": True,
            "capture_id": capture_id,
            "stage": stage_name,
            "adoption_sha256": adoption["sha256"],
            "readiness_sha256": current_readiness["sha256"],
            "candidate_sha256": readiness["binding"]["candidate_sha256"],
            "origin_candidate_sha256": adoption_result["candidate_sha256"],
            "capture_state_sha256": digest(json_bytes(dict(read_capture(store, capture_id)))),
            "source_sha256": adoption_result["source_sha256"],
            "source_wal_sha256": adoption_result["source_wal_sha256"],
            "receipt_evidence_sha256": verification["binding"]["evidence_sha256"],
            "source_retired": retire_source,
            "cleanup_intent_sha256": (
                decode(
                    regular_bytes(
                        store.path(f"{stage_relative}/resume-cleanup-intent.json"), 1024**2
                    )
                )["sha256"]
                if retire_source
                else None
            ),
        }


def _compare_resumed_membership(
    connection: duckdb.DuckDBPyConnection,
    state: dict[str, Any],
    index: DiskParentIndex,
    store: Store,
) -> tuple[int, int]:
    from kalshi_events.historical_resume import compare_rows

    def check_schema(table: str, expected: list[tuple[str, str]]) -> None:
        description = connection.execute(f"describe landing.{table}").fetchall()
        if [(row[0], row[1].upper()) for row in description] != [
            (name, kind.upper()) for name, kind in expected
        ]:
            raise CatalogueError(f"resumed historical landing.{table} schema is invalid")

    membership_schema = [
        ("scope", "VARCHAR"),
        ("entity_kind", "VARCHAR"),
        ("entity_ticker", "VARCHAR"),
    ]
    check_schema("membership", membership_schema)
    membership_count = compare_rows(
        connection,
        "select scope, entity_kind, entity_ticker from landing.membership "
        "order by scope, entity_ticker, entity_kind",
        ((scope, SCOPE_KIND[scope], ticker) for scope, ticker in index.stream_membership(state)),
        membership_schema,
        check_deadline=store.check_deadline,
    )
    eligibility_schema = [
        ("capture_id", "VARCHAR"),
        ("entity_kind", "VARCHAR"),
        ("entity_ticker", "VARCHAR"),
        ("status", "INTEGER"),
        ("receipt", "VARCHAR"),
    ]
    check_schema("parent_eligibility", eligibility_schema)
    eligibility_count = compare_rows(
        connection,
        "select capture_id, entity_kind, entity_ticker, status, receipt "
        "from landing.parent_eligibility order by entity_kind, entity_ticker",
        (
            (state["capture_id"], kind, ticker, status, page_id)
            for kind, ticker, status, page_id in index.stream_eligibility(state)
        ),
        eligibility_schema,
        check_deadline=store.check_deadline,
    )
    return membership_count, eligibility_count


def _check_historical_landing_schemas(connection: duckdb.DuckDBPyConnection) -> None:
    historical_resume._logical_schema(
        connection,
        _LANDING_COLUMNS,
        {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
    )
    expected = {
        "membership": [
            ("scope", "VARCHAR"),
            ("entity_kind", "VARCHAR"),
            ("entity_ticker", "VARCHAR"),
        ],
        "parent_eligibility": [
            ("capture_id", "VARCHAR"),
            ("entity_kind", "VARCHAR"),
            ("entity_ticker", "VARCHAR"),
            ("status", "INTEGER"),
            ("receipt", "VARCHAR"),
        ],
    }
    for table, schema in expected.items():
        description = connection.execute(f"describe landing.{table}").fetchall()
        if [(row[0], row[1].upper()) for row in description] != schema:
            raise CatalogueError(f"resumed historical landing.{table} schema is invalid")


def _resume_historical_landing(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    candidate_relative: str,
    candidate: Path,
    source_proof_sha256: str,
    index: DiskParentIndex,
    task_indices: set[int],
    selection: dict[str, Any],
    *,
    force_revalidate: bool = False,
) -> tuple[int, dict[str, str], _OperationEvidence, dict[str, Any]]:
    from kalshi_events.historical_resume import (
        compare_landing_rows_resumable,
        resume_landing_cursor,
    )

    verified_pages: dict[str, str] = {}
    database = candidate / "warehouse.duckdb"
    if not force_revalidate:
        reused = _reuse_historical_candidate_phase(
            store,
            state,
            stage_name,
            candidate_relative,
            candidate,
            source_proof_sha256,
            selection,
            task_indices,
            verified_pages,
        )
        if reused is not None:
            observation_count, operation_evidence, proof = reused
            return observation_count, verified_pages, operation_evidence, proof
    candidate_sha256, _candidate_size = _resume_file_digest(
        store, database, store.limits.max_database_bytes
    )
    schema_sha256 = _historical_landing_schema_sha256()
    scope_sha256 = _historical_scope_sha256(state, selection, task_indices)
    code_sha256 = _historical_resume_code_identity()
    binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "candidate_sha256": candidate_sha256,
        "source_proof_sha256": source_proof_sha256,
        "historical_selection": selection,
        "task_indices": sorted(task_indices),
        "scope_sha256": scope_sha256,
        "schema_sha256": schema_sha256,
        "code_sha256": code_sha256,
        "normalization_policy": "historical-landing-v1",
        "deduplication_policy": "disk-page-id-context-v1",
    }
    cursor_relative = f"{candidate_relative}/resume-landing-cursor.json"
    cursor_path = store.path(cursor_relative)
    cursor = None
    if cursor_path.is_symlink():
        raise CatalogueError("historical landing cursor is unsafe")
    if cursor_path.exists() and not force_revalidate:
        try:
            cursor = decode(regular_bytes(cursor_path, 1024**2))
        except (CatalogueError, OSError):
            cursor = None
    selected_receipt_count = 0
    for _receipt in _iter_page_receipts(store, state, task_indices=task_indices):
        store.check_deadline()
        selected_receipt_count += 1
    # Check the candidate before reserving the disposable page index.
    try:
        with (
            _duckdb_work_config(store, candidate) as config,
            duckdb.connect(str(database), read_only=True, config=config) as count_connection,
            interrupt_at_deadline(store, count_connection.interrupt),
        ):
            store.check_deadline()
            candidate_rows = count_connection.execute(
                "select count(*) from landing.observations"
            ).fetchone()[0]
            store.check_deadline()
    except duckdb.InterruptException as exc:
        raise BudgetExceeded(
            "operation deadline exhausted counting historical candidate observations"
        ) from exc
    except duckdb.Error as exc:
        raise CatalogueError("historical resume candidate landing schema is invalid") from exc
    if isinstance(cursor, dict):
        result = cursor.get("result")
        if (
            isinstance(result, dict)
            and isinstance(result.get("observation_position"), int)
            and result["observation_position"] > candidate_rows
        ):
            cursor = None
    page_index = historical_resume.PageContextIndex(
        store,
        f"{candidate_relative}/resume-page-index.sqlite",
        max_entries=selected_receipt_count,
    )

    def expected_rows():
        page_index.reset()
        landing = _iter_historical_landing_rows(
            store,
            state,
            verified_pages=verified_pages,
            parent_index=index,
            task_indices=task_indices,
            page_index=page_index,
            with_positions=True,
        )
        yield from landing

    expected_stream = None
    try:
        (
            expected_stream,
            prefix_checksum,
            start_position,
            last_position,
            replayed_prefix_rows,
        ) = resume_landing_cursor(
            expected_rows,
            cursor,
            binding,
            check_deadline=store.check_deadline,
        )

        def save_cursor(proof: dict[str, Any]) -> None:
            store.write(cursor_relative, proof)
            fsync_file(cursor_path)
            fsync_directory(candidate)

        with (
            _duckdb_work_config(store, candidate) as config,
            duckdb.connect(str(database), read_only=True, config=config) as connection,
            interrupt_at_deadline(store, connection.interrupt),
            metrics.phase("recovery_validation"),
            metrics.phase("historical_resume_validation"),
        ):
            observation_count, cursor_result = compare_landing_rows_resumable(
                connection,
                expected_stream,
                _LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=store.check_deadline,
                binding=binding,
                save_cursor=save_cursor,
                start_position=start_position,
                prefix_checksum=prefix_checksum,
                last_position=last_position,
                replayed_prefix_rows=replayed_prefix_rows,
            )
            membership_count, eligibility_count = _compare_resumed_membership(
                connection, state, index, store
            )
    finally:
        close = getattr(expected_stream, "close", None)
        if close is not None:
            close()
        page_index.close()
    _check_historical_coverage(database, state["capture_id"], state.get("complete") is True)
    operation_evidence = _operation_evidence(store, state, 3, verified_pages)
    landing_binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "historical_selection": selection,
        "task_indices": sorted(task_indices),
        "code_sha256": code_sha256,
        "candidate_sha256": candidate_sha256,
        "source_proof_sha256": source_proof_sha256,
        "scope_sha256": scope_sha256,
        "schema_sha256": schema_sha256,
        "evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
    }
    proof = historical_resume.proof_record(
        "historical-landing",
        landing_binding,
        {
            "observation_count": observation_count,
            "membership_count": membership_count,
            "eligibility_count": eligibility_count,
            "cursor_sha256": digest(json_bytes(cursor_result)),
            "prefix_replay_rows": replayed_prefix_rows,
            "prefix_replay_policy": "normalize-deduplicate-and-digest-v1",
        },
    )
    proof_relative = f"{candidate_relative}/resume-landing-{proof['sha256']}.json"
    proof_path = store.path(proof_relative)
    if proof_path.exists() or proof_path.is_symlink():
        if decode(regular_bytes(proof_path, 1024**2)) != proof:
            raise CatalogueError("historical landing proof conflicts with its immutable identity")
    else:
        store.write(proof_relative, proof)
    alias_relative = f"{candidate_relative}/resume-landing.json"
    store.write(alias_relative, proof)
    fsync_file(proof_path)
    fsync_file(store.path(alias_relative))
    fsync_directory(candidate)
    _write_historical_candidate_phase(
        store,
        state,
        stage_name,
        candidate_relative,
        candidate,
        source_proof_sha256,
        selection,
        task_indices,
        proof,
        "landing-ready",
    )
    return observation_count, verified_pages, operation_evidence, proof


_HISTORICAL_COMPACT_MODELS = {
    "_historical_current_events": ("event_ticker",),
    "_historical_current_series": ("series_ticker",),
    "_historical_current_markets": ("market_ticker",),
}
_HISTORICAL_COMPACT_DEPENDENCIES = {
    "_historical_current_series": ("_historical_current_events",),
    "_historical_current_markets": ("_historical_current_events", "_historical_current_series"),
}
_HISTORICAL_COMBO_SCHEMA = [
    ("combo_market_ticker", "VARCHAR"),
    ("leg_index", "INTEGER"),
    ("event_ticker", "VARCHAR"),
    ("market_ticker", "VARCHAR"),
    ("side", "VARCHAR"),
    ("yes_settlement_value_dollars", "DECIMAL(38,18)"),
    ("resolved", "BOOLEAN"),
    ("payload_json", "JSON"),
]


def _dbt_relation_type(connection: duckdb.DuckDBPyConnection, schema: str, name: str) -> str | None:
    row = connection.execute(
        "select table_type from information_schema.tables where table_schema=? and table_name=?",
        [schema, name],
    ).fetchone()
    return row[0] if row is not None else None


def _drop_historical_combo_relation(store: Store, directory: Path) -> None:
    with (
        _duckdb_work_config(store, directory) as config,
        duckdb.connect(str(directory / "warehouse.duckdb"), config=config) as connection,
    ):
        relation_type = _dbt_relation_type(connection, "main", "combo_legs")
        if relation_type == "VIEW":
            connection.execute("drop view main.combo_legs")
        elif relation_type is not None:
            connection.execute("drop table main.combo_legs")
        connection.execute("checkpoint")
    store.sync(directory.relative_to(store.root).as_posix())


def _historical_combo_output(store: Store, database: Path) -> tuple[int, str] | None:
    from kalshi_events.historical_resume import digest_relation

    try:
        with (
            _duckdb_work_config(store, database.parent) as config,
            duckdb.connect(str(database), read_only=True, config=config) as connection,
        ):
            return digest_relation(
                connection,
                "landing.combo_legs_canonical",
                _HISTORICAL_COMBO_SCHEMA,
                check_deadline=store.check_deadline,
            )
    except BudgetExceeded:
        raise
    except (CatalogueError, duckdb.Error):
        return None


def _historical_dbt_workflow(
    store: Store,
    state: dict[str, Any],
    directory: Path,
    directory_relative: str,
    *,
    previous_artifacts: dict[str, Any] | None,
    landing_proof: dict[str, Any],
    source_proof_sha256: str | None,
    evidence: dict[str, str],
    stage_name: str,
    rebuild: bool,
) -> None:
    from kalshi_events import historical_combo
    from kalshi_events.historical_resume import validate_proof

    capture_id = state["capture_id"]
    database_relative = f"{directory_relative}/warehouse.duckdb"
    variables = {
        "catalogue_version": 2,
        "incremental_update": False,
        "current_capture_id": capture_id,
        "delta_capture_id": capture_id,
        "historical_compact": True,
        "historical_combo_prepared": True,
    }
    code_sha256 = _historical_resume_code_identity()
    compile_artifacts = f"{directory_relative}/resume-compile-artifacts.json"
    dbt(
        store,
        ["compile"],
        database=database_relative,
        artifacts=compile_artifacts,
        variables=variables,
    )
    compiled = _read_dbt_artifacts(store.path(compile_artifacts))
    if compiled is None:
        raise CatalogueError("historical dbt compile artifacts are invalid")
    current_manifest = compiled["manifest.json"]["content"]
    compiled_manifest = current_manifest
    if not isinstance(current_manifest.get("nodes"), dict):
        raise CatalogueError("historical dbt manifest has no model inventory")

    prepared_artifacts = f"{directory_relative}/resume-prepared-artifacts.json"
    compact_cached: dict[str, bool] = {}
    compact_output: dict[str, dict[str, Any]] = {}
    with (
        _duckdb_work_config(store, directory) as config,
        duckdb.connect(
            str(directory / "warehouse.duckdb"), read_only=True, config=config
        ) as connection,
    ):
        for model in _HISTORICAL_COMPACT_MODELS:
            dependencies_valid = all(
                compact_cached.get(dependency, False)
                for dependency in _HISTORICAL_COMPACT_DEPENDENCIES.get(model, ())
            )
            if rebuild or not dependencies_valid:
                valid, count, checksum = False, 0, None
            else:
                valid, count, checksum = _historical_model_cache_valid(
                    connection, current_manifest, model, store
                )
            compact_cached[model] = valid
            if valid:
                compact_output[model] = {"rows": count, "sha256": checksum}

    missing_compact = [model for model, valid in compact_cached.items() if not valid]
    prep_selectors = [
        "observation_history",
        "current_series",
        "current_events",
        "current_markets",
        *missing_compact,
    ]
    dbt(
        store,
        ["run", "--select", *prep_selectors],
        database=database_relative,
        artifacts=prepared_artifacts,
        variables=variables,
    )
    prepared = _read_dbt_artifacts(store.path(prepared_artifacts))
    if prepared is None:
        raise CatalogueError("historical compact model artifacts are invalid")

    with (
        _duckdb_work_config(store, directory) as config,
        duckdb.connect(
            str(directory / "warehouse.duckdb"), read_only=True, config=config
        ) as connection,
    ):
        for model in _HISTORICAL_COMPACT_MODELS:
            if model in compact_output:
                continue
            count, checksum = _historical_model_output(connection, compiled_manifest, model, store)
            compact_output[model] = {"rows": count, "sha256": checksum}
        relationships_valid, _relationship_count, _relationship_sha256 = (
            _historical_model_cache_valid(
                connection,
                compiled_manifest,
                "relationships",
                store,
            )
        )

    source_proof_sha256 = source_proof_sha256 or digest(
        json_bytes(
            {"root": str(store.root.resolve()), "stage": stage_name, "capture_id": capture_id}
        )
    )
    landing_record_sha256 = landing_proof["sha256"]
    combo_binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": capture_id,
        "state_sha256": digest(json_bytes(dict(state))),
        "source_proof_sha256": source_proof_sha256,
        "landing_proof_sha256": landing_record_sha256,
        "evidence_sha256": digest(json_bytes(evidence)),
        "code_sha256": code_sha256,
        "compact_outputs": compact_output,
    }
    combo_proof_path = directory / "resume-combo.json"
    combo_proof_valid = False
    if not rebuild and combo_proof_path.is_file() and not combo_proof_path.is_symlink():
        try:
            proof = decode(regular_bytes(combo_proof_path, 1024**2))
            result = validate_proof(proof, "historical-combo", combo_binding)
            current_output = _historical_combo_output(store, directory / "warehouse.duckdb")
            combo_proof_valid = current_output == (
                result.get("legs"),
                result.get("sha256"),
            )
        except BudgetExceeded:
            raise
        except (CatalogueError, OSError, duckdb.Error):
            combo_proof_valid = False

    if not combo_proof_valid:
        _drop_historical_combo_relation(store, directory)
        with metrics.phase("combo_expansion"), _duckdb_work_config(store, directory) as config:
            combo_stats = historical_combo.build_combo_legs(
                store, directory / "warehouse.duckdb", config=config
            )
        combo_output = _historical_combo_output(store, directory / "warehouse.duckdb")
        if combo_output is None or combo_output[0] != combo_stats["legs"]:
            raise CatalogueError("historical combo phase failed exact output validation")
        proof = historical_resume.proof_record(
            "historical-combo",
            combo_binding,
            {
                "markets": combo_stats["markets"],
                "legs": combo_stats["legs"],
                "batches": combo_stats["batches"],
                "sha256": combo_output[1],
            },
        )
        store.write(f"{directory_relative}/resume-combo.json", proof)
        fsync_file(combo_proof_path)
        fsync_directory(directory)

    with (
        _duckdb_work_config(store, directory) as config,
        duckdb.connect(
            str(directory / "warehouse.duckdb"), read_only=True, config=config
        ) as connection,
    ):
        combo_valid, _combo_count, _combo_sha256 = _historical_model_cache_valid(
            connection,
            compiled_manifest,
            "combo_legs",
            store,
        )

    final_models = []
    if not relationships_valid or rebuild:
        final_models.append("relationships")
    if not combo_valid or rebuild:
        final_models.append("combo_legs")
    final_artifacts = f"{directory_relative}/resume-final-artifacts.json"
    dbt(
        store,
        ["build", "--select", *final_models, "test_type:data"],
        database=database_relative,
        artifacts=final_artifacts,
        variables=variables,
    )
    _merge_historical_dbt_artifacts(
        store,
        f"{directory_relative}/build-artifacts.json",
        previous_artifacts,
        [compile_artifacts, prepared_artifacts, final_artifacts],
    )
    for relative in (compile_artifacts, prepared_artifacts, final_artifacts):
        store.remove(relative)


def _fresh_historical_landing_proof(
    store: Store,
    state: dict[str, Any],
    stage_name: str,
    staging_relative: str,
    observation_count: int,
    membership_count: int,
    eligibility_count: int,
    verified_pages: dict[str, str],
    selection: dict[str, Any],
    task_indices: set[int],
) -> tuple[_OperationEvidence, dict[str, Any]]:
    operation_evidence = _operation_evidence(store, state, 3, verified_pages)
    binding = {
        "root": str(store.root.resolve()),
        "stage": stage_name,
        "capture_id": state["capture_id"],
        "state_sha256": digest(json_bytes(dict(state))),
        "historical_selection": selection,
        "task_indices": sorted(task_indices),
        "code_sha256": _historical_resume_code_identity(),
        "evidence_sha256": digest(json_bytes(operation_evidence.evidence)),
    }
    proof = historical_resume.proof_record(
        "historical-landing",
        binding,
        {
            "observation_count": observation_count,
            "membership_count": membership_count,
            "eligibility_count": eligibility_count,
        },
    )
    relative = f"{staging_relative}/resume-landing.json"
    store.write(relative, proof)
    fsync_file(store.path(relative))
    fsync_directory(store.path(staging_relative))
    return operation_evidence, proof


def _full_historical_build(
    store: Store,
    state: dict[str, Any],
    *,
    rebuild: bool,
    resume_historical_stage: str | None,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    capture_id = state["capture_id"]
    selection, task_indices = _historical_profile(state)
    stage_name = resume_historical_stage
    source_proof_sha256: str | None = None
    candidate_recovered_wal = False
    resumed = stage_name is not None
    if resumed:
        stage_relative = historical_resume.stage_relative(stage_name)
        recover_and_cleanup_abandoned_historical_stages(
            store,
            capture_id,
            preserve_stage=stage_name,
            _verified_mount=_verified_mount,
        )
        (
            staging_relative,
            staging,
            source_proof_sha256,
            candidate_recovered_wal,
        ) = _prepare_historical_resume_candidate(
            store, state, stage_name, _verified_mount=_verified_mount
        )
        previous_path = staging / "build-artifacts.json"
        if not previous_path.exists():
            previous_path = store.path(f"{stage_relative}/build-artifacts.json")
        previous_artifacts = _read_dbt_artifacts(previous_path)
    else:
        _recover_build(store, capture_id, 3)
        staging_relative = f"tmp/build-historical-{uuid.uuid4().hex}"
        stage_name = Path(staging_relative).name
        staging = store.path(staging_relative)
        durable_mkdir(staging.parent)
        staging.mkdir()
        previous_artifacts = None

    verified_pages: dict[str, str] = {}
    index: ParentIndex | DiskParentIndex = DiskParentIndex(store, collect_winners=False)
    index_closed = False
    try:
        if resumed:
            assert stage_name is not None and source_proof_sha256 is not None
            observation_count, verified_pages, operation_evidence, landing_proof = (
                _resume_historical_landing(
                    store,
                    state,
                    stage_name,
                    staging_relative,
                    staging,
                    source_proof_sha256,
                    index,
                    task_indices,
                    selection,
                    force_revalidate=candidate_recovered_wal,
                )
            )
        else:
            with metrics.phase("landing"):
                observation_count = _land_historical_rows(
                    store,
                    state,
                    staging,
                    verified_pages=verified_pages,
                    parent_index=index,
                    task_indices=task_indices,
                )
            with (
                _duckdb_work_config(store, staging) as config,
                duckdb.connect(str(staging / "warehouse.duckdb"), config=config) as connection,
                metrics.phase("current_projection"),
            ):
                _projection_membership(store, state, index, connection)
                _write_historical_coverage(connection, state)
                membership_count = connection.execute(
                    "select count(*) from landing.membership"
                ).fetchone()[0]
                eligibility_count = connection.execute(
                    "select count(*) from landing.parent_eligibility"
                ).fetchone()[0]
            store.sync_file(f"{staging_relative}/warehouse.duckdb")
            operation_evidence, landing_proof = _fresh_historical_landing_proof(
                store,
                state,
                stage_name,
                staging_relative,
                observation_count,
                membership_count,
                eligibility_count,
                verified_pages,
                selection,
                task_indices,
            )

        if isinstance(index, DiskParentIndex):
            index.close()
            index_closed = True
        _historical_dbt_workflow(
            store,
            state,
            staging,
            staging_relative,
            previous_artifacts=previous_artifacts,
            landing_proof=landing_proof,
            source_proof_sha256=source_proof_sha256,
            evidence=operation_evidence.evidence,
            stage_name=stage_name,
            rebuild=rebuild,
        )
        database = staging / "warehouse.duckdb"
        _write_ordering_policy(store, database)
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, 3, verified_pages)
        fsync_file(database)
        fsync_directory(staging)
        if resumed:
            assert stage_name is not None and source_proof_sha256 is not None
            _write_historical_candidate_phase(
                store,
                state,
                stage_name,
                staging_relative,
                staging,
                source_proof_sha256,
                selection,
                task_indices,
                landing_proof,
                "dbt-complete",
            )
        historical_adoption = None
        if resumed:
            assert stage_name is not None
            adoption_proof = _historical_adoption_record(store, stage_name)
            if adoption_proof is not None:
                source_proof, source_result = _read_source_proof(store, state, stage_name, staging)
                _validate_historical_adoption_origin(
                    store, state, stage_name, source_proof, source_result
                )
                adoption_result = adoption_proof["result"]
                historical_adoption = {
                    "stage": stage_name,
                    "sha256": adoption_proof["sha256"],
                    "source_proof_sha256": source_proof["sha256"],
                    "candidate_sha256": adoption_result["candidate_sha256"],
                    "source_sha256": adoption_result["source_sha256"],
                    "source_wal_sha256": adoption_result["source_wal_sha256"],
                }
                adoption_relative = f"{staging_relative}/historical-adoption.json"
                adoption_path = store.path(adoption_relative)
                store.write(adoption_relative, adoption_proof)
                if decode(regular_bytes(adoption_path, 1024**2)) != adoption_proof:
                    raise CatalogueError("installed historical adoption lineage failed readback")
                fsync_file(adoption_path)
                fsync_directory(staging)
        receipt = {
            **_historical_receipt_fields(state),
            "capture_id": capture_id,
            "built_at": now(),
            ORDERING_POLICY_FIELD: ORDERING_POLICY,
            "evidence": operation_evidence.evidence,
            "observation_count": observation_count,
            "warehouse_sha256": file_digest(database, store.limits.max_database_bytes)[0],
            "dbt_artifacts_sha256": digest(regular_bytes(staging / "build-artifacts.json")),
            **({"historical_adoption": historical_adoption} if historical_adoption else {}),
        }
        store.write(f"{staging_relative}/build.json", receipt)
        fsync_directory(staging)
        promoted = _install_build(store, capture_id, 3, staging, operation_evidence)
        store.check_storage()
        installed_relative = build_directory(capture_id, 3)
        installed = store.path(installed_relative)
        with os.scandir(installed) as entries:
            resume_files = [
                f"{installed_relative}/{entry.name}"
                for entry in entries
                if entry.name.startswith("resume-")
            ]
        for relative in resume_files:
            store.remove(relative)
        fsync_directory(installed)
        if resumed:
            store.remove(historical_resume.stage_relative(stage_name))
            fsync_directory(store.path("tmp"))
        return promoted
    finally:
        if isinstance(index, DiskParentIndex) and not index_closed:
            index.close()
        if resumed:
            if staging.exists():
                _recover_resume_candidate(store, staging)
            store.sync(staging_relative)
            store.sync(historical_resume.stage_relative(stage_name))
        elif staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
            fsync_directory(staging.parent)
            store.sync(staging_relative)


def _full_v2_build(
    store: Store,
    state: dict[str, Any],
    *,
    rebuild: bool,
    historical: bool = False,
    resume_historical_stage: str | None = None,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    capture_id = state["capture_id"]
    if historical:
        return _full_historical_build(
            store,
            state,
            rebuild=rebuild,
            resume_historical_stage=resume_historical_stage,
            _verified_mount=_verified_mount,
        )
    if not state.get("task_scopes") and any(
        int(work.get("restarts", 0)) > 0 for work in state.get("tasks", [])
    ):
        raise CatalogueError(
            "legacy full capture lacks final-generation membership after cursor reset; capture a new full baseline"
        )
    store.check_deadline()
    catalogue_version = 2
    _recover_build(store, capture_id, catalogue_version)
    staging_relative = f"tmp/build-v2-{uuid.uuid4().hex}"
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
            nullcontext({}) as config,
            duckdb.connect(str(staging / "warehouse.duckdb"), config=config) as connection,
            metrics.phase("current_projection"),
        ):
            _projection_membership(store, state, index, connection)
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
        database = staging / "warehouse.duckdb"
        _write_ordering_policy(store, database)
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, catalogue_version, verified_pages)
        fsync_file(database)
        fsync_directory(staging)
        receipt = {
            "version": 2,
            "catalogue_version": 2,
            "capture_id": capture_id,
            "built_at": now(),
            **_build_scope_fields(state),
            ORDERING_POLICY_FIELD: ORDERING_POLICY,
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


def build_historical(
    store: Store,
    capture_id: str,
    *,
    rebuild: bool = False,
    resume_historical_stage: str | None = None,
    _verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with store.writer_guard():
        store.require_writable()
        if store.scope is not None:
            raise CatalogueError("historical catalogue builds require a catalogue root")
        if resume_historical_stage is not None:
            resume_relative = historical_resume.stage_relative(resume_historical_stage)
            try:
                _validate_historical_stage(store.path(resume_relative), store.root.lstat().st_dev)
            except FileNotFoundError as exc:
                raise CatalogueError("requested historical resume stage is missing") from exc
        if _verified_mount is not None:
            historical_resume.validate_resume_mount_context(
                store=store,
                state=read_capture(store, capture_id),
                verified_mount=_verified_mount,
            )
        from kalshi_events.incremental import finish_pending_retirement

        finish_pending_retirement(store)
        state = read_capture(store, capture_id)
        _historical_profile(state)
        if state.get("incremental"):
            raise CatalogueError("incremental captures cannot be built as historical catalogues")
        return _full_v2_build(
            store,
            state,
            rebuild=rebuild,
            historical=True,
            resume_historical_stage=resume_historical_stage,
            _verified_mount=_verified_mount,
        )


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
    reproject_all = baseline_receipt.get(ORDERING_POLICY_FIELD) != ORDERING_POLICY
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
            if not reproject_all:
                _affected_keys(connection, state, changes)
                _delete_affected(connection)
        dbt(
            store,
            ["build"],
            database=f"{staging_relative}/warehouse.duckdb",
            artifacts=f"{staging_relative}/build-artifacts.json",
            variables={
                "catalogue_version": 2,
                "incremental_update": not reproject_all,
                "current_capture_id": capture_id,
                "delta_capture_id": capture_id,
            },
        )
        database = staging / "warehouse.duckdb"
        _write_ordering_policy(store, database)
        store.sync(staging_relative)
        operation_evidence = _operation_evidence(store, state, 2, verified_pages)
        fsync_file(database)
        fsync_directory(staging)
        receipt = {
            "version": 2,
            "catalogue_version": 2,
            "capture_id": capture_id,
            "built_at": now(),
            **_build_scope_fields(state),
            ORDERING_POLICY_FIELD: ORDERING_POLICY,
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
        adoption_file = directory_path / "historical-adoption.json"
        adoption_summary = receipt.get("historical_adoption")
        if adoption_summary is None:
            if adoption_file.exists() or adoption_file.is_symlink():
                raise CatalogueError("historical build has unbound adoption lineage")
        else:
            if not isinstance(adoption_summary, dict) or set(adoption_summary) != {
                "stage",
                "sha256",
                "source_proof_sha256",
                "candidate_sha256",
                "source_sha256",
                "source_wal_sha256",
            }:
                raise CatalogueError("historical build adoption summary is invalid")
            historical_resume.stage_relative(adoption_summary.get("stage"))
            loaded_adoption = _load_self_proof(adoption_file, "historical-adoption")
            if loaded_adoption is None:
                raise CatalogueError("historical build adoption proof is missing")
            adoption_proof, adoption_result = loaded_adoption
            adoption_binding = adoption_proof["binding"]
            if (
                adoption_proof.get("sha256") != adoption_summary.get("sha256")
                or adoption_binding.get("root") != str(store.root.resolve())
                or adoption_binding.get("stage") != adoption_summary.get("stage")
                or adoption_binding.get("capture_id") != capture_id
                or adoption_binding.get("state_sha256") != digest(json_bytes(dict(state)))
                or adoption_binding.get("source_proof_sha256")
                != adoption_summary.get("source_proof_sha256")
                or adoption_result.get("candidate_sha256")
                != adoption_summary.get("candidate_sha256")
                or adoption_result.get("source_sha256") != adoption_summary.get("source_sha256")
                or adoption_result.get("source_wal_sha256")
                != adoption_summary.get("source_wal_sha256")
            ):
                raise CatalogueError("historical build adoption lineage does not match its receipt")
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
    _check_ordering_policy(store, warehouse, receipt)
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
