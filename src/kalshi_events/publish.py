"""Immutable verified catalogue releases with atomic last-good promotion.

Publication promotes a versioned pointer only after verification, then prunes evidence outside
both publication closures and in-progress captures.
"""

from __future__ import annotations

import os
import shutil
import uuid
from contextlib import contextmanager
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
from kalshi_events.warehouse import (
    DBT_MEMORY_LIMIT,
    DBT_THREADS,
    RELATIONS,
    build_directory,
    certified,
)

CONTRACT = "oddsfox.kalshi.catalogue.v1"
CONTRACT_V2 = "oddsfox.kalshi.catalogue.v2"
SERIES_CONTRACT = "oddsfox.kalshi.series.v1"
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
MAX_COVERAGE_BINDING_BYTES = 1024**2


@contextmanager
def _export_connection(store: Store, database: Path, temporary: Path) -> Any:
    relative_temporary = temporary.relative_to(store.root).as_posix()
    usage = store.usage()
    spill_budget = min(
        store.limits.max_temp_bytes - usage["temporary_bytes"],
        store.limits.max_root_bytes - usage["retained_bytes"],
    )
    if spill_budget < 1:
        raise BudgetExceeded("no bounded temporary storage is available for publication export")
    store.reserve_storage(spill_budget)

    spill_directory = temporary / "duckdb-spill"
    connection = None
    try:
        spill_directory.mkdir()
        store.check_deadline()
        connection = duckdb.connect(
            ":memory:",
            config={
                # DuckDB 2.x inference otherwise changes the public Parquet nullability schema.
                "disabled_optimizers": "statistics_propagation",
                "memory_limit": DBT_MEMORY_LIMIT,
                "threads": str(DBT_THREADS),
                "temp_directory": str(spill_directory),
                "max_temp_directory_size": f"{spill_budget}B",
            },
        )
        database_literal = str(database).replace("'", "''")
        try:
            with interrupt_at_deadline(store, connection.interrupt):
                connection.execute(f"ATTACH '{database_literal}' AS source (READ_ONLY)")
                connection.execute("USE source")
                store.check_deadline()
                yield connection
        except duckdb.InterruptException as exc:
            raise BudgetExceeded("operation deadline exhausted during publication export") from exc
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                if spill_directory.exists():
                    shutil.rmtree(spill_directory)
            finally:
                try:
                    store.sync(relative_temporary)
                finally:
                    store.release_storage(spill_budget)


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
            SERIES_CONTRACT,
        )
        or not isinstance(manifest.get("files"), dict)
        or set(manifest["files"]) != FILES
    ):
        raise CatalogueError("invalid catalogue contract or relation inventory")
    contract = manifest["contract"]
    series = contract == SERIES_CONTRACT
    catalogue_v2 = contract in {CONTRACT_V2, SERIES_CONTRACT}
    expected_scope = "series" if series else "catalogue"
    if not _hex_id(manifest.get("capture_id"), 32):
        raise CatalogueError("invalid catalogue capture identity")
    if manifest.get("coverage_scope") != expected_scope:
        raise CatalogueError("catalogue contract and coverage scope disagree")
    if catalogue_v2:
        _validate_v2_manifest(manifest, series=series)
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
    _validate_coverage(
        directory / "coverage.parquet",
        manifest,
        catalogue_v2=catalogue_v2,
        series=series,
    )
    return manifest


def _validate_v2_manifest(manifest: dict[str, Any], *, series: bool = False) -> None:
    if any(name not in manifest for name in V2_LINEAGE_FIELDS):
        raise CatalogueError("invalid catalogue-v2 lineage fields")
    if not _hex_id(manifest.get("root_id"), 32) or not _hex_id(manifest.get("capture_id"), 32):
        raise CatalogueError("invalid catalogue-v2 root or capture identity")
    depth = manifest.get("delta_depth")
    if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= 8:
        raise CatalogueError("invalid catalogue-v2 delta depth")
    if not _hex_id(manifest.get("membership_sha256"), 64):
        raise CatalogueError("invalid catalogue-v2 membership fingerprint")
    if series:
        from kalshi_events.common import validate_series_scope

        scope = validate_series_scope(manifest.get("series_scope"))
        if (
            manifest.get("coverage_scope") != "series"
            or manifest.get("include_multivariate") is not False
            or not isinstance(manifest.get("series_ticker"), str)
            or scope
            != validate_series_scope(
                {
                    "kind": "series",
                    "series_ticker": manifest.get("series_ticker"),
                    "include_multivariate": manifest.get("include_multivariate"),
                }
            )
        ):
            raise CatalogueError("invalid series release scope")
    elif manifest.get("coverage_scope") != "catalogue":
        raise CatalogueError("invalid catalogue-v2 coverage scope")
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


def _validate_coverage(
    path: Path,
    manifest: dict[str, Any],
    *,
    catalogue_v2: bool = False,
    series: bool = False,
) -> None:
    fields = ("capture_id", "coverage_scope")
    expected = {name: manifest.get(name) for name in fields}
    if catalogue_v2:
        fields = (*fields, *V2_LINEAGE_FIELDS)
        expected.update({name: manifest.get(name) for name in V2_LINEAGE_FIELDS})
    if series:
        fields = (*fields, "series_ticker", "include_multivariate")
        expected.update({name: manifest.get(name) for name in fields if name not in expected})
    try:
        parquet = pq.ParquetFile(
            path,
            thrift_string_size_limit=MAX_COVERAGE_BINDING_BYTES,
            thrift_container_size_limit=10_000,
        )
        metadata = parquet.metadata
        schema = parquet.schema_arrow
        if metadata.num_rows != 1 or metadata.num_row_groups != 1:
            raise CatalogueError("release coverage must contain exactly one row")
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
            raise CatalogueError("release coverage must use flat primitive columns")
        indices = {name: schema.get_field_index(name) for name in fields}
        if any(index < 0 for index in indices.values()):
            raise CatalogueError("release coverage is missing binding columns")
        if any(
            metadata.row_group(0).column(index).path_in_schema != name
            for name, index in indices.items()
        ):
            raise CatalogueError("release coverage binding columns are not flat")
        selected_columns = [metadata.row_group(0).column(index) for index in indices.values()]
        compressed_sizes = [column.total_compressed_size for column in selected_columns]
        uncompressed_sizes = [column.total_uncompressed_size for column in selected_columns]
        if (
            any(
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or size > MAX_COVERAGE_BINDING_BYTES
                for size in (*compressed_sizes, *uncompressed_sizes)
            )
            or sum(uncompressed_sizes) > MAX_COVERAGE_BINDING_BYTES
        ):
            raise CatalogueError("release coverage binding columns exceed the size limit")
        for name, value in expected.items():
            field_type = schema.field(name).type
            if value is None:
                if not (pa.types.is_null(field_type) or pa.types.is_string(field_type)):
                    raise CatalogueError("release coverage binding column type mismatch")
            elif isinstance(value, str):
                if not (pa.types.is_string(field_type) or pa.types.is_large_string(field_type)):
                    raise CatalogueError("release coverage binding column type mismatch")
            elif isinstance(value, int) and not isinstance(value, bool):
                if not pa.types.is_int64(field_type):
                    raise CatalogueError("release coverage binding column type mismatch")
            elif isinstance(value, bool):
                if not pa.types.is_boolean(field_type):
                    raise CatalogueError("release coverage binding column type mismatch")
            else:
                raise CatalogueError("invalid release manifest binding value")
        table = parquet.read(columns=list(fields))
        if table.num_rows != 1:
            raise CatalogueError("release coverage must contain exactly one row")
        row = table.to_pylist()[0]
    except CatalogueError:
        raise
    except Exception as exc:
        raise CatalogueError("invalid release coverage binding") from exc

    for name, value in expected.items():
        actual = row.get(name)
        if type(actual) is not type(value) or actual != value:
            raise CatalogueError(f"release coverage binding mismatch: {name}")


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
    _profile(store, catalogue_version)
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
    if store.scope is not None and not explicit_capture_id:
        raise CatalogueError("series publication requires an explicit capture ID")
    capture_id = capture_id or latest_catalogue_capture(store)
    state = _complete_capture(store, capture_id)
    if state.get("scope") != store.scope:
        raise CatalogueError("capture scope does not match the publication root")
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
    expected_build_scope = "series" if store.scope is not None else "catalogue"
    if receipt["scope"] != expected_build_scope:
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    store.check_deadline()
    warehouse = store.path(f"{build_directory(capture_id, catalogue_version)}/warehouse.duckdb")
    temporary_relative = f"tmp/publish-{uuid.uuid4().hex}"
    temporary = store.path(temporary_relative)
    durable_mkdir(temporary.parent)
    temporary.mkdir()
    fsync_directory(temporary.parent)
    try:
        with _export_connection(store, warehouse, temporary) as connection, metrics.phase("export"):
            for name, table in RELATIONS.items():
                store.check_deadline()
                destination = temporary / (name + ".parquet")
                # DuckDB's path parameter keeps the operator path out of SQL syntax.
                keys = ORDER_KEYS[name]
                if connection.execute(
                    f"select 1 from main.{table} group by {keys} having count(*) > 1 limit 1"
                ).fetchone():
                    raise CatalogueError(f"non-unique publication ordering key: {name}")
                connection.execute(
                    f"copy (select * from main.{table} order by {keys}) to ? (format parquet)",
                    [str(destination)],
                )
        coverage_row = {
            "capture_id": state["capture_id"],
            "coverage_scope": "series" if store.scope is not None else "catalogue",
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
            coverage_row.update(
                {key: value for key, value in release_metadata.items() if key != "series_scope"}
            )
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
            "contract": _profile(store, catalogue_version)[1],
            "source_release_id": release_id,
            "capture_id": state["capture_id"],
            "received_at": now(),
            "coverage_scope": "series" if store.scope is not None else "catalogue",
            "files": evidence,
        }
        manifest.update(release_metadata)
        if store.scope is not None:
            manifest["series_scope"] = dict(store.scope)
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
        pointer_name = _profile(store, catalogue_version)[0]
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
    pointer_name, expected_contract = _profile(store, catalogue_version)[:2]
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
        if store.scope is not None and manifest.get("series_scope") != store.scope:
            raise CatalogueError("series release belongs to a different operator scope")
    return {"output": str(directory), **manifest}


def _profile(store: Store, catalogue_version: int) -> tuple[str, str]:
    if store.scope is not None:
        if catalogue_version != 2:
            raise CatalogueError("series roots support only the series publication contract")
        return "current-series.json", SERIES_CONTRACT
    if catalogue_version == 1:
        return "current.json", CONTRACT
    if catalogue_version == 2:
        return "current-v2.json", CONTRACT_V2
    raise CatalogueError("catalogue version must be 1 or 2")
