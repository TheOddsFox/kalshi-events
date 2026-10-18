"""Immutable verified catalogue releases with atomic last-good promotion.

Publication promotes a versioned pointer only after verification, then prunes evidence outside
both publication closures and in-progress captures.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_events import metrics
from kalshi_events.capture import (
    _complete_capture,
    latest_catalogue_capture,
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
    safe_path,
)
from kalshi_events.warehouse import RELATIONS, build_directory, certified

CONTRACT = "oddsfox.kalshi.catalogue.v1"
CONTRACT_V2 = "oddsfox.kalshi.catalogue.v2"
FILES = {name + ".parquet" for name in (*RELATIONS, "coverage")}
ORDER_KEYS = {
    "series": "series_ticker",
    "events": "event_ticker",
    "markets": "market_ticker",
    "observation_history": "observation_id",
    "combo_legs": "combo_market_ticker, leg_index",
    "relationships": "parent_kind, parent_ticker, child_kind, child_ticker",
}
V2_LINEAGE_FIELDS = (
    "root_id",
    "baseline_release_id",
    "baseline_manifest_sha256",
    "baseline_capture_id",
    "previous_watermark",
    "watermark_started_at",
    "delta_depth",
    "membership_sha256",
)
MAX_V2_COVERAGE_LINEAGE_BYTES = 1024**2


def inventory(directory: Path, names: set[str], limit: int) -> dict[str, dict[str, Any]]:
    result = {}
    for name in sorted(names):
        checksum, size = file_digest(directory / name, limit)
        result[name] = {"sha256": checksum, "size": size}
    return result


def verify_release(directory: Path, limit: int = 64 * 1024**3) -> dict[str, Any]:
    with metrics.phase("verification"):
        return _verify_release(directory, limit)


def _verify_release(directory: Path, limit: int = 64 * 1024**3) -> dict[str, Any]:
    directory = safe_path(directory)
    manifest = decode(regular_bytes(directory / "manifest.json", 1024**2))
    if (
        not isinstance(manifest, dict)
        or manifest.get("contract")
        not in (
            CONTRACT,
            CONTRACT_V2,
        )
        or not isinstance(manifest.get("files"), dict)
        or set(manifest["files"]) != FILES
    ):
        raise CatalogueError("invalid catalogue contract or relation inventory")
    if manifest["contract"] == CONTRACT_V2:
        _validate_v2_manifest(manifest)
    if {p.name for p in directory.iterdir()} != FILES | {"manifest.json"}:
        raise CatalogueError("release contains unexpected or missing files")
    actual = inventory(directory, FILES, limit)
    if actual != manifest["files"] or digest(json_bytes(actual).rstrip(b"\n")) != manifest.get(
        "source_release_id"
    ):
        raise CatalogueError("catalogue release integrity mismatch")
    for name in FILES:
        try:
            _ = pq.ParquetFile(directory / name).schema
        except Exception as exc:
            raise CatalogueError("invalid Parquet release relation") from exc
    if manifest["contract"] == CONTRACT_V2:
        _validate_v2_coverage(directory / "coverage.parquet", manifest)
    return manifest


def _validate_v2_manifest(manifest: dict[str, Any]) -> None:
    if any(name not in manifest for name in V2_LINEAGE_FIELDS):
        raise CatalogueError("invalid catalogue-v2 lineage fields")
    if not _hex_id(manifest.get("root_id"), 32) or not _hex_id(manifest.get("capture_id"), 32):
        raise CatalogueError("invalid catalogue-v2 root or capture identity")
    depth = manifest.get("delta_depth")
    if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= 8:
        raise CatalogueError("invalid catalogue-v2 delta depth")
    if not _hex_id(manifest.get("membership_sha256"), 64):
        raise CatalogueError("invalid catalogue-v2 membership fingerprint")
    if "warehouse_sha256" in manifest and not _hex_id(manifest["warehouse_sha256"], 64):
        raise CatalogueError("invalid catalogue-v2 warehouse checksum")
    baseline_id = manifest.get("baseline_release_id")
    baseline_sha = manifest.get("baseline_manifest_sha256")
    baseline_capture = manifest.get("baseline_capture_id")
    previous = manifest.get("previous_watermark")
    if depth == 0:
        if any(
            value is not None for value in (baseline_id, baseline_sha, baseline_capture, previous)
        ):
            raise CatalogueError("invalid full catalogue-v2 lineage")
    elif (
        not _hex_id(baseline_id, 64)
        or not _hex_id(baseline_sha, 64)
        or not _hex_id(baseline_capture, 32)
        or baseline_id == manifest.get("source_release_id")
        or baseline_capture == manifest.get("capture_id")
        or not _utc_timestamp(previous)
    ):
        raise CatalogueError("invalid delta catalogue-v2 lineage")
    if not _utc_timestamp(manifest.get("watermark_started_at")):
        raise CatalogueError("invalid catalogue-v2 watermark")
    if depth:
        from datetime import datetime

        if datetime.fromisoformat(previous.replace("Z", "+00:00")) > datetime.fromisoformat(
            manifest["watermark_started_at"].replace("Z", "+00:00")
        ):
            raise CatalogueError("catalogue-v2 watermark moves backwards")


def _validate_v2_coverage(path: Path, manifest: dict[str, Any]) -> None:
    fields = ("capture_id", *V2_LINEAGE_FIELDS)
    expected = {name: manifest.get(name) for name in fields}
    try:
        parquet = pq.ParquetFile(
            path,
            thrift_string_size_limit=MAX_V2_COVERAGE_LINEAGE_BYTES,
            thrift_container_size_limit=10_000,
        )
        metadata = parquet.metadata
        schema = parquet.schema_arrow
        if metadata.num_rows != 1 or metadata.num_row_groups != 1:
            raise CatalogueError("catalogue-v2 coverage must contain exactly one row")
        scalar_types = (
            pa.types.is_string,
            pa.types.is_large_string,
            pa.types.is_boolean,
            pa.types.is_int64,
            pa.types.is_null,
        )
        if metadata.num_columns != len(schema) or any(
            not any(is_type(field.type) for is_type in scalar_types) for field in schema
        ):
            raise CatalogueError("catalogue-v2 coverage must use flat primitive columns")
        indices = {name: schema.get_field_index(name) for name in fields}
        if any(index < 0 for index in indices.values()):
            raise CatalogueError("catalogue-v2 coverage is missing lineage columns")
        if any(
            metadata.row_group(0).column(index).path_in_schema != name
            for name, index in indices.items()
        ):
            raise CatalogueError("catalogue-v2 coverage lineage columns are not flat")
        selected_columns = [metadata.row_group(0).column(index) for index in indices.values()]
        compressed_sizes = [column.total_compressed_size for column in selected_columns]
        uncompressed_sizes = [column.total_uncompressed_size for column in selected_columns]
        if (
            any(
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or size > MAX_V2_COVERAGE_LINEAGE_BYTES
                for size in (*compressed_sizes, *uncompressed_sizes)
            )
            or sum(uncompressed_sizes) > MAX_V2_COVERAGE_LINEAGE_BYTES
        ):
            raise CatalogueError("catalogue-v2 coverage lineage columns exceed the size limit")
        for name, value in expected.items():
            field_type = schema.field(name).type
            if value is None:
                if not (pa.types.is_null(field_type) or pa.types.is_string(field_type)):
                    raise CatalogueError("catalogue-v2 coverage lineage column type mismatch")
            elif isinstance(value, str):
                if not (pa.types.is_string(field_type) or pa.types.is_large_string(field_type)):
                    raise CatalogueError("catalogue-v2 coverage lineage column type mismatch")
            elif isinstance(value, int) and not isinstance(value, bool):
                if not pa.types.is_int64(field_type):
                    raise CatalogueError("catalogue-v2 coverage lineage column type mismatch")
            else:
                raise CatalogueError("invalid catalogue-v2 manifest lineage value")
        table = parquet.read(columns=list(fields))
        if table.num_rows != 1:
            raise CatalogueError("catalogue-v2 coverage must contain exactly one row")
        row = table.to_pylist()[0]
    except CatalogueError:
        raise
    except Exception as exc:
        raise CatalogueError("invalid catalogue-v2 coverage lineage") from exc

    for name, value in expected.items():
        actual = row.get(name)
        if type(actual) is not type(value) or actual != value:
            raise CatalogueError(f"catalogue-v2 coverage lineage mismatch: {name}")


def _hex_id(value: Any, size: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == size
        and all(char in "0123456789abcdef" for char in value)
    )


def _utc_timestamp(value: Any) -> bool:
    from datetime import UTC, datetime, timedelta

    if not isinstance(value, str):
        return False
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return instant.utcoffset() == timedelta(0) and instant.astimezone(UTC).isoformat().endswith(
        "+00:00"
    )


def publish(
    store: Store, capture_id: str | None = None, *, catalogue_version: int = 1
) -> dict[str, Any]:
    if isinstance(catalogue_version, bool) or catalogue_version not in (1, 2):
        raise CatalogueError("catalogue version must be 1 or 2")
    with store.writer_guard():
        return _publish(store, capture_id, catalogue_version)


def _publish(
    store: Store,
    capture_id: str | None,
    catalogue_version: int,
    *,
    _context: Any = None,
) -> dict[str, Any]:
    from kalshi_events.warehouse import operation_context

    if not store.writer_locked_by_current_thread:
        with store.writer_guard(), operation_context(store) as context:
            return _publish(store, capture_id, catalogue_version, _context=context)
    if _context is None:
        with operation_context(store) as context:
            return _publish(store, capture_id, catalogue_version, _context=context)
    store.require_writable()
    from kalshi_events import incremental

    incremental.finish_pending_retirement(store)
    explicit_capture_id = bool(capture_id)
    capture_id = capture_id or latest_catalogue_capture(store)
    state = _complete_capture(store, capture_id)
    if catalogue_version == 1 and state.get("incremental") is True:
        raise CatalogueError("incremental captures can only publish as catalogue version 2")
    if catalogue_version == 2 and state.get("incremental") is True and not explicit_capture_id:
        raise CatalogueError("publishing an incremental capture requires an explicit capture ID")
    if state["selected"] is not None:
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    if catalogue_version == 2:
        from kalshi_events import incremental

        if state.get("incremental") is True:
            incremental.verify_pin(store, state)
            if not store.path(f"{build_directory(capture_id, 2)}/build.json").exists():
                from kalshi_events.warehouse import build

                build(store, capture_id)
        else:
            from kalshi_events.warehouse import build_v2_full

            if not store.path(f"{build_directory(capture_id, 2)}/build.json").exists():
                build_v2_full(store, capture_id)
    cutoffs: list[dict[str, Any]] = []
    rows = page_receipts(store, state)
    receipt = certified(
        store,
        capture_id,
        cutoff_sink=cutoffs,
        catalogue_version=catalogue_version,
        _state=state,
        _page_rows=rows,
        _context=_context,
    )
    if receipt["scope"] != "catalogue":
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    store.check_deadline()
    warehouse = store.path(f"{build_directory(capture_id, catalogue_version)}/warehouse.duckdb")
    temporary_relative = f"tmp/publish-{uuid.uuid4().hex}"
    temporary = store.path(temporary_relative)
    durable_mkdir(temporary.parent)
    temporary.mkdir()
    fsync_directory(temporary.parent)
    try:
        with (
            duckdb.connect(str(warehouse), read_only=True) as connection,
            interrupt_at_deadline(store, connection.interrupt),
            metrics.phase("export"),
        ):
            for name, table in RELATIONS.items():
                store.check_deadline()
                destination = temporary / (name + ".parquet")
                # DuckDB's path parameter keeps the operator path out of SQL syntax.
                try:
                    keys = ORDER_KEYS[name]
                    if connection.execute(
                        f"select 1 from main.{table} group by {keys} having count(*) > 1 limit 1"
                    ).fetchone():
                        raise CatalogueError(f"non-unique publication ordering key: {name}")
                    connection.execute(
                        f"copy (select * from main.{table} order by {keys}) to ? (format parquet)",
                        [str(destination)],
                    )
                except duckdb.InterruptException as exc:
                    raise BudgetExceeded("operation deadline exhausted") from exc
        coverage_row = {
            "capture_id": state["capture_id"],
            "coverage_scope": "catalogue",
            "started_at": state["started_at"],
            "completed_at": state["completed_at"],
            "atomic_snapshot": False,
            "traversals_json": json_bytes(state["tasks"]).decode().strip(),
            "cutoff_json": json_bytes(cutoffs).decode().strip(),
        }
        release_metadata = {}
        if catalogue_version == 2:
            from kalshi_events import incremental

            release_metadata = incremental.release_fields(store, state)
            coverage_row.update(release_metadata)
        coverage = [coverage_row]
        store.check_deadline()
        pq.write_table(pa.Table.from_pylist(coverage), temporary / "coverage.parquet")
        for name in FILES:
            fsync_file(temporary / name)
        fsync_directory(temporary)
        store.sync(temporary_relative)
        store.check_storage()
        evidence = inventory(temporary, FILES, store.limits.max_root_bytes)
        release_id = digest(json_bytes(evidence).rstrip(b"\n"))
        manifest = {
            "contract": CONTRACT_V2 if catalogue_version == 2 else CONTRACT,
            "source_release_id": release_id,
            "capture_id": state["capture_id"],
            "received_at": now(),
            "coverage_scope": "catalogue",
            "files": evidence,
        }
        manifest.update(release_metadata)
        if catalogue_version == 2:
            manifest["warehouse_sha256"] = receipt["warehouse_sha256"]
        atomic_json(temporary / "manifest.json", manifest)
        fsync_directory(temporary)
        verify_release(temporary, store.limits.max_root_bytes)
        destination = store.path(f"releases/{release_id}")
        durable_mkdir(destination.parent)
        fsync_directory(destination.parent.parent)
        if destination.exists():
            existing = verify_release(destination, store.limits.max_root_bytes)
            if (
                existing.get("contract") != manifest["contract"]
                or existing.get("capture_id") != capture_id
                or any(existing.get(name) != value for name, value in release_metadata.items())
            ):
                raise CatalogueError(
                    "release identity collides with different publication metadata"
                )
            for name in FILES | {"manifest.json"}:
                fsync_file(destination / name)
            fsync_directory(destination)
            if catalogue_version == 2:
                manifest = existing
        else:
            os.rename(temporary, destination)
        fsync_directory(temporary.parent)
        fsync_directory(destination.parent)
        store.sync(temporary_relative)
        store.sync(f"releases/{release_id}")
        store.check_deadline()
        pointer_name = "current-v2.json" if catalogue_version == 2 else "current.json"
        # A delta may be retried after pointer promotion and before pruning completed.
        already_current = False
        if catalogue_version == 2 and store.path(pointer_name).exists():
            existing_current = current(store, catalogue_version=2)
            already_current = (
                existing_current["source_release_id"] == release_id
                and existing_current["capture_id"] == capture_id
                and all(
                    existing_current.get(name) == value for name, value in release_metadata.items()
                )
            )
        if catalogue_version == 2 and not already_current and state.get("incremental") is True:
            from kalshi_events import incremental

            incremental.verify_predecessor(store, state)
        elif catalogue_version == 2 and state.get("incremental") is not True:
            from kalshi_events import incremental

            incremental.verify_full_rebase(store, state)
        with metrics.phase("promotion"):
            if not already_current:
                store.write(
                    pointer_name,
                    {
                        "source_release_id": release_id,
                        "manifest_sha256": digest(regular_bytes(destination / "manifest.json")),
                    },
                )
        # Pruning runs only after the promoted pointer re-verifies end to end.
        promoted = (
            current(store)
            if catalogue_version == 1
            else current(store, catalogue_version=catalogue_version)
        )
        if promoted["capture_id"] != capture_id:
            raise CatalogueError("promoted release does not belong to the published capture")
        with metrics.phase("pruning"):
            if catalogue_version == 1:
                pruned = prune_superseded(store, state, release_id, _context=_context)
            else:
                from kalshi_events import incremental

                pruned = incremental.prune(
                    store,
                    state,
                    release_id,
                    catalogue_version,
                    _context=_context,
                )
        return {"output": str(destination), **manifest, "pruned_captures": pruned}
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
        store.sync(temporary_relative)


def prune_superseded(
    store: Store, state: dict[str, Any], release_id: str, *, _context: Any = None
) -> list[str]:
    """Keep the evidence reachable from both publications and in-progress captures."""
    from kalshi_events import incremental

    return incremental.prune(store, state, release_id, 1, _context=_context)


def current(store: Store, *, catalogue_version: int = 1) -> dict[str, Any]:
    if isinstance(catalogue_version, bool) or catalogue_version not in (1, 2):
        raise CatalogueError("catalogue version must be 1 or 2")
    pointer_name = "current-v2.json" if catalogue_version == 2 else "current.json"
    expected_contract = CONTRACT_V2 if catalogue_version == 2 else CONTRACT
    pointer = decode(regular_bytes(store.path(pointer_name), 1024**2))
    if not isinstance(pointer, dict):
        raise CatalogueError("invalid current release pointer")
    release_id = pointer.get("source_release_id", "")
    if not _hex_id(release_id, 64):
        raise CatalogueError("invalid current release identity")
    directory = store.path(f"releases/{release_id}")
    manifest = verify_release(directory, store.limits.max_root_bytes)
    if manifest.get("contract") != expected_contract:
        raise CatalogueError("current pointer references a different catalogue contract")
    if digest(regular_bytes(directory / "manifest.json")) != pointer.get("manifest_sha256"):
        raise CatalogueError("current pointer manifest mismatch")
    if catalogue_version == 2:
        from kalshi_events.incremental import root_id

        if manifest["root_id"] != root_id(store):
            raise CatalogueError("v2 release belongs to a different logical operator root")
    return {"output": str(directory), **manifest}
