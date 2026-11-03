"""Bounded historical relation sorting and resumable Parquet export."""

from __future__ import annotations

import contextlib
import heapq
import os
import re
import shutil
import stat
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from kalshi_events import metrics
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    decode,
    digest,
    durable_mkdir,
    file_digest,
    fsync_directory,
    fsync_file,
    interrupt_at_deadline,
    json_bytes,
    regular_bytes,
)

POLICY = "historical-arrow-external-merge-v1"
_INPUT_READER_STRATEGY = "owned-duckdb-scratch-byte-windows-v1"
_FRONTIER_STRATEGY = "owned-online-eight-way-frontier-v2"
COPY_OPTIONS = "compression zstd, compression_level 6, row_group_size_bytes 16777216"
_BATCH_ROWS = 4096
_BATCH_BYTES = 8 * 1024 * 1024
_MERGE_FAN_IN = 8
_MAX_SCHEMA_BYTES = 16 * 1024 * 1024
_INPUT_WRITE_RESERVE = 8 * 1024 * 1024
_INPUT_TABLE = "export_input"
_SPILL_OWNER = "duckdb-spill.owner.json"
_DUCKDB_SPILL_FILE = re.compile(
    r"duckdb_temp_[0-9]+_[0-9]+_(?:owner|storage_(?:DEFAULT|S[0-9]+K)-[0-9]+\.tmp)\Z"
)
_WRITE_TEMP = re.compile(r"\.write-[a-z0-9_]{8}\Z")
_RELATION = re.compile(r"[a-z][a-z0-9_]*\Z")
_SCALAR_SQL_TYPE = re.compile(
    r"(?:BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UHUGEINT|UTINYINT|USMALLINT|"
    r"UINTEGER|UBIGINT|FLOAT|REAL|DOUBLE|DECIMAL\(\s*\d+\s*,\s*\d+\s*\)|"
    r"NUMERIC\(\s*\d+\s*,\s*\d+\s*\)|DATE|TIME(?:_S|_MS|_US|_NS)?(?: WITH TIME ZONE)?|"
    r"TIMESTAMP(?:_S|_MS|_US|_NS)?(?: WITH TIME ZONE)?|VARCHAR|TEXT|STRING|JSON|BLOB|"
    r"UUID|INTERVAL)\Z",
    re.IGNORECASE,
)
_GENERATION = re.compile(r"gen-[0-9]{8}\Z")
_RUN = re.compile(r"run-[0-9]{8}\.parquet\Z")
_FRONTIER_RUN = re.compile(r"run-([0-9]{12})\.parquet\Z")
_FRONTIER_PARTIAL = re.compile(r"run-([0-9]{12})\.parquet\.partial\Z")
_OUTPUT = re.compile(r"[a-z][a-z0-9_]*\.parquet(?:\.partial)?\Z")


def binding(
    capture_id: str,
    warehouse_sha256: str,
    evidence: dict[str, str],
    release_metadata: dict[str, Any],
    coverage_row: dict[str, Any],
    order_keys: dict[str, str],
) -> tuple[str, str]:
    value = {
        "capture_id": capture_id,
        "warehouse_sha256": warehouse_sha256,
        "evidence_sha256": digest(json_bytes(evidence)),
        "release_metadata": release_metadata,
        "coverage": coverage_row,
        "order_keys": order_keys,
        "writer_policy": {
            "name": POLICY,
            "input_reader_strategy": _INPUT_READER_STRATEGY,
            "run_compaction_strategy": _FRONTIER_STRATEGY,
            "copy_options": COPY_OPTIONS,
            "run_compression": "zstd-6",
            "batch_rows": _BATCH_ROWS,
            "batch_bytes": _BATCH_BYTES,
            "merge_fan_in": _MERGE_FAN_IN,
            "threads": 1,
            "preserve_insertion_order": True,
            "disabled_statistics_propagation": True,
        },
    }
    checksum = digest(json_bytes(value))
    return checksum, f"tmp/publish-{checksum[:32]}"


def prepare(
    store: Store,
    relative: str,
    *,
    binding_sha256: str | None = None,
    tables: dict[str, str] | None = None,
) -> Path:
    if not store.writer_locked_by_current_thread:
        with store.writer_guard():
            return prepare(
                store,
                relative,
                binding_sha256=binding_sha256,
                tables=tables,
            )
    if not re.fullmatch(r"tmp/publish-[0-9a-f]{32}", relative):
        raise CatalogueError("invalid historical export work directory")
    if (binding_sha256 is None) != (tables is None):
        raise CatalogueError("historical export input cleanup identity is incomplete")
    if binding_sha256 is not None and (
        not _hex(binding_sha256, 64)
        or any(
            _RELATION.fullmatch(name) is None or _RELATION.fullmatch(table) is None
            for name, table in (tables or {}).items()
        )
    ):
        raise CatalogueError("historical export input cleanup identity is invalid")
    root = store.path(relative)
    if root.exists():
        _validate_work_tree(store, root)
        owner_path = root / _SPILL_OWNER
        if owner_path.exists() or owner_path.is_symlink():
            spill_owner = _validate_spill_owner_marker(store, root)
            if binding_sha256 is not None and (
                spill_owner["historical_export"] is not True
                or spill_owner["export_binding_sha256"] != binding_sha256
            ):
                raise CatalogueError("historical DuckDB spill owner binding changed")
        _discard_interrupted_marker_temps(store, root)
        if binding_sha256 is not None:
            _discard_stale_inputs(store, root, binding_sha256, tables or {})
            _validate_prepared_frontiers(store, root, binding_sha256, tables or {})
    else:
        durable_mkdir(root.parent)
        root.mkdir()
        fsync_directory(root.parent)
    for name in ("output", "runs", "checkpoints", "schemas", "inputs"):
        child = root / name
        if not child.exists():
            child.mkdir()
            fsync_directory(root)
    _assert_directory(store, root)
    store.sync(relative)
    return root


def export_relation(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    *,
    work_relative: str,
    binding_sha256: str,
    name: str,
    table: str,
    keys: tuple[str, ...],
) -> dict[str, Any]:
    if not store.writer_locked_by_current_thread:
        with store.writer_guard():
            return export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name=name,
                table=table,
                keys=keys,
            )
    return _export_frontier_relation(
        store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name=name,
        table=table,
        keys=keys,
    )


def _export_frontier_relation(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    *,
    work_relative: str,
    binding_sha256: str,
    name: str,
    table: str,
    keys: tuple[str, ...],
) -> dict[str, Any]:
    if _RELATION.fullmatch(name) is None or _RELATION.fullmatch(table) is None:
        raise CatalogueError("invalid historical export relation")
    if not keys or any(_RELATION.fullmatch(key) is None for key in keys):
        raise CatalogueError("invalid historical export ordering key")
    if not _hex(binding_sha256, 64):
        raise CatalogueError("historical export binding is invalid")

    root = store.path(work_relative)
    _assert_directory(store, root)
    _discard_stale_inputs(store, root, binding_sha256, {name: table}, relations={name})
    schema = _reference_schema(store, connection, root, name, table)
    key_indices, key_types = _ordering_columns(schema, keys)
    schema_sha256 = digest(schema.serialize().to_pybytes())
    _validate_prefetch_schema(schema)
    checkpoint_relative = f"{work_relative}/checkpoints/{name}.json"
    checkpoint_path = store.path(checkpoint_relative)
    output_relative = f"{work_relative}/output/{name}.parquet"
    output_path = store.path(output_relative)
    run_relative = f"{work_relative}/runs/{name}"
    run_directory = store.path(run_relative)
    durable_mkdir(run_directory)

    identity = {
        "version": 2,
        "binding_sha256": binding_sha256,
        "table": table,
        "keys": list(keys),
        "schema_sha256": schema_sha256,
    }
    checkpoint = _read_checkpoint(store, checkpoint_path)
    _ensure_frontier_owner(store, run_directory, name, identity, create=checkpoint is None)
    if checkpoint is None:
        checkpoint = _empty_frontier(identity)
        store.write(checkpoint_relative, checkpoint)
    elif not _checkpoint_matches(checkpoint, identity):
        raise CatalogueError("historical frontier checkpoint binding changed")

    _validate_frontier(store, run_directory, checkpoint, schema)
    checkpoint = _recover_pending_write(
        store, checkpoint_relative, run_directory, checkpoint, schema
    )
    _finish_frontier_deletes(store, checkpoint_relative, run_directory, checkpoint, schema)
    checkpoint = _read_checkpoint(store, checkpoint_path) or checkpoint
    _discard_unreferenced_frontier_runs(store, run_directory, checkpoint)

    if checkpoint.get("complete") is True and _valid_completed_export(
        store, output_path, checkpoint, schema
    ):
        _input_prefetch_metrics(checkpoint)
        return checkpoint

    input_directory = _input_directory(store, work_relative, name)
    input_marker = _validate_input_tree(
        store, input_directory, name, binding_sha256, table, schema_sha256
    )
    source_row_count = checkpoint.get("source_row_count")
    input_database = None
    if input_marker is not None and input_marker.get("state") == "sealed":
        _validate_sealed_input(store, input_directory, input_marker, schema)
        input_database = input_directory / "input.duckdb"
        if source_row_count is None:
            source_row_count = input_marker["row_count"]
            checkpoint = {
                **checkpoint,
                "source_row_count": source_row_count,
                "input_sha256": input_marker["input_sha256"],
                "input_size": input_marker["input_size"],
            }
            store.write(checkpoint_relative, checkpoint)
        elif (
            source_row_count != input_marker["row_count"]
            or checkpoint.get("input_sha256") != input_marker["input_sha256"]
            or checkpoint.get("input_size") != input_marker["input_size"]
        ):
            raise CatalogueError("sealed historical input does not match the frontier checkpoint")
    elif source_row_count is None and checkpoint["next_rowid"] == 0:
        input_database = _materialize_input(
            store,
            connection,
            work_relative,
            binding_sha256,
            name,
            table,
            schema,
            schema_sha256,
        )
        input_marker = _validate_input_tree(
            store, input_directory, name, binding_sha256, table, schema_sha256
        )
        if input_marker is None or input_marker.get("state") != "sealed":
            raise CatalogueError("historical input was not sealed")
        source_row_count = input_marker["row_count"]
        checkpoint = {
            **checkpoint,
            "source_row_count": source_row_count,
            "input_sha256": input_marker["input_sha256"],
            "input_size": input_marker["input_size"],
        }
        store.write(checkpoint_relative, checkpoint)
    elif source_row_count is None or checkpoint["next_rowid"] != source_row_count:
        if checkpoint["next_rowid"] > 0 or source_row_count is not None:
            raise CatalogueError("sealed historical input is missing for the committed prefix")
    if source_row_count is None:
        raise CatalogueError("historical input row count was not sealed")
    if checkpoint["next_rowid"] > source_row_count:
        raise CatalogueError("historical frontier cursor exceeds the sealed input")

    if checkpoint["next_rowid"] < source_row_count:
        if input_database is None:
            raise CatalogueError("sealed historical input is missing before the frontier cursor")
        try:
            input_connection = duckdb.connect(
                str(input_database),
                read_only=True,
                config={
                    "threads": "4",
                    "memory_limit": "4GB",
                    "disabled_optimizers": "statistics_propagation,build_side_probe_side",
                    "preserve_insertion_order": "false",
                },
            )
            try:
                with interrupt_at_deadline(store, input_connection.interrupt):
                    input_schema = _reference_schema(
                        store,
                        input_connection,
                        root,
                        name,
                        _INPUT_TABLE,
                        schema_name=f"{name}_input",
                    )
                    if not input_schema.equals(schema, check_metadata=True):
                        raise CatalogueError("historical input changed the reference schema")
                    cursor = checkpoint["next_rowid"]
                    for window_start, window_rows, estimated_bytes in _prefetch_windows(
                        input_connection,
                        _INPUT_TABLE,
                        schema,
                        start_rowid=cursor,
                        total_row_count=source_row_count,
                    ):
                        store.check_deadline()
                        if window_start != cursor:
                            raise CatalogueError("historical input cursor has a gap")
                        if window_rows == 1:
                            checkpoint["input_max_singleton_bytes"] = max(
                                checkpoint["input_max_singleton_bytes"], estimated_bytes
                            )
                            metrics.peak("historical_input_singleton_bytes", estimated_bytes)
                        reader = _read_input_window(
                            input_connection, _INPUT_TABLE, window_start, window_rows
                        )
                        fetched_rows = 0
                        try:
                            for batch in reader:
                                store.check_deadline()
                                fetched_rows += batch.num_rows
                                checkpoint["input_arrow_peak_bytes"] = max(
                                    checkpoint["input_arrow_peak_bytes"], batch.nbytes
                                )
                                checkpoint["input_arrow_peak_rows"] = max(
                                    checkpoint["input_arrow_peak_rows"], batch.num_rows
                                )
                                if batch.num_rows == 1:
                                    checkpoint["input_singleton_peak_bytes"] = max(
                                        checkpoint["input_singleton_peak_bytes"], batch.nbytes
                                    )
                                    metrics.peak(
                                        "historical_input_singleton_arrow_bytes", batch.nbytes
                                    )
                                if batch.num_rows > window_rows or (
                                    batch.num_rows > 1 and batch.nbytes > _BATCH_BYTES
                                ):
                                    raise CatalogueError(
                                        "historical input exceeded its Arrow window"
                                    )
                                metrics.peak("historical_input_arrow_bytes", batch.nbytes)
                                metrics.peak("historical_input_arrow_rows", batch.num_rows)
                                next_cursor = checkpoint["next_rowid"]
                                converted = pa.Table.from_batches([batch]).cast(schema)
                                for piece in _byte_bounded_batches(
                                    converted.to_batches(max_chunksize=_BATCH_ROWS)[0]
                                ):
                                    next_cursor += piece.num_rows
                                    checkpoint = _begin_frontier_write(
                                        store,
                                        checkpoint_relative,
                                        checkpoint,
                                        kind="leaf",
                                        cursor_end=next_cursor,
                                    )
                                    planned = checkpoint["pending_write"]["planned"][0]
                                    descriptor = _write_frontier_run(
                                        store,
                                        run_directory,
                                        int(_FRONTIER_RUN.fullmatch(planned["name"]).group(1)),
                                        planned["level"],
                                        piece,
                                        schema,
                                        key_indices,
                                    )
                                    checkpoint = _record_frontier_write(
                                        store, checkpoint_relative, checkpoint, descriptor
                                    )
                                    checkpoint = _commit_frontier_leaf(
                                        store,
                                        checkpoint_relative,
                                        run_directory,
                                        checkpoint,
                                        descriptor,
                                        next_cursor,
                                        schema,
                                        key_indices,
                                        key_types,
                                    )
                        finally:
                            reader.close()
                        if fetched_rows != window_rows:
                            raise CatalogueError("historical input payload window has a gap")
                        cursor = window_start + window_rows
                        if checkpoint["next_rowid"] != cursor:
                            raise CatalogueError(
                                "historical frontier cursor changed within a window"
                            )
                        store.sync(f"{work_relative}/inputs/{name}")
                        store.check_storage()
            finally:
                input_connection.close()
        except duckdb.InterruptException as exc:
            try:
                store.check_deadline()
            except BudgetExceeded as deadline:
                raise deadline from exc
            raise CatalogueError("historical input read was interrupted") from exc
        except duckdb.Error as exc:
            raise CatalogueError("could not read the owned historical input snapshot") from exc

    if source_row_count == 0 and checkpoint["run_count"] == 0:
        empty = pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )
        checkpoint = _begin_frontier_write(
            store,
            checkpoint_relative,
            checkpoint,
            kind="leaf",
            cursor_end=0,
        )
        planned = checkpoint["pending_write"]["planned"][0]
        descriptor = _write_frontier_run(
            store,
            run_directory,
            int(_FRONTIER_RUN.fullmatch(planned["name"]).group(1)),
            planned["level"],
            empty,
            schema,
            key_indices,
        )
        checkpoint = _record_frontier_write(store, checkpoint_relative, checkpoint, descriptor)
        checkpoint = _commit_frontier_leaf(
            store,
            checkpoint_relative,
            run_directory,
            checkpoint,
            descriptor,
            0,
            schema,
            key_indices,
            key_types,
        )

    if checkpoint["next_rowid"] != source_row_count:
        raise CatalogueError("historical input cursor did not reach the sealed row count")
    _discard_input(store, work_relative, name, binding_sha256, table, schema_sha256)
    checkpoint = _collapse_frontier(
        store,
        checkpoint_relative,
        run_directory,
        checkpoint,
        schema,
        key_indices,
        key_types,
    )
    if checkpoint["run_count"] > _MERGE_FAN_IN:
        raise CatalogueError("historical final frontier exceeds eight runs")

    _copy_merged(
        store,
        connection,
        run_directory,
        schema,
        key_indices,
        key_types,
        checkpoint,
        output_path,
        output_relative,
    )
    checksum, size = file_digest(output_path, store.limits.max_root_bytes)
    rows = pq.ParquetFile(output_path).metadata.num_rows
    result = {
        **checkpoint,
        **_input_prefetch_metrics(checkpoint),
        "complete": True,
        "output_sha256": checksum,
        "output_size": size,
        "row_count": rows,
    }
    if rows != checkpoint["row_count"]:
        _remove_owned_file(store, output_path)
        raise CatalogueError("historical export row count changed during final COPY")
    if not pq.ParquetFile(output_path).schema_arrow.equals(schema, check_metadata=True):
        _remove_owned_file(store, output_path)
        raise CatalogueError("historical export changed the DuckDB reference schema")
    store.write(checkpoint_relative, result)
    return result


def remove_work(
    store: Store,
    relative: str,
    *,
    binding_sha256: str | None = None,
) -> None:
    path = store.path(relative)
    if not path.exists():
        return
    _validate_work_tree(store, path)
    owner_path = path / _SPILL_OWNER
    spill_path = path / "duckdb-spill"
    if (
        owner_path.exists()
        or owner_path.is_symlink()
        or spill_path.exists()
        or spill_path.is_symlink()
    ):
        if not store.writer_locked_by_current_thread:
            raise CatalogueError(
                "historical DuckDB spill cleanup requires the canonical writer lock"
            )
        if not _hex(binding_sha256, 64):
            raise CatalogueError("historical DuckDB spill cleanup binding is missing")
        owner = _validate_spill_owner_marker(store, path)
        if (
            owner["historical_export"] is not True
            or owner["export_binding_sha256"] != binding_sha256
        ):
            raise CatalogueError("historical DuckDB spill owner binding changed")
        if spill_path.exists() or spill_path.is_symlink():
            _validate_safe_tree(store, spill_path)
            shutil.rmtree(spill_path)
            fsync_directory(path)
            store.sync(f"{relative}/duckdb-spill")
        _remove_owned_file(store, owner_path)
        _validate_work_tree(store, path)
    shutil.rmtree(path)
    fsync_directory(path.parent)
    store.sync(relative)


def _reference_schema(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    name: str,
    table: str,
    *,
    schema_name: str | None = None,
) -> pa.Schema:
    schema_name = name if schema_name is None else schema_name
    if _RELATION.fullmatch(schema_name) is None:
        raise CatalogueError("invalid historical schema artifact name")
    relative = f"{root.relative_to(store.root).as_posix()}/schemas/{schema_name}.parquet"
    path = store.path(relative)
    partial = store.path(relative + ".partial")
    _remove_owned_file(store, path)
    _remove_owned_file(store, partial)
    store.reserve_storage(_MAX_SCHEMA_BYTES)
    complete = False
    try:
        connection.execute(
            f"copy (select * from main.{table} limit 0) to ? (format parquet, {COPY_OPTIONS})",
            [str(partial)],
        ).fetchall()
        fsync_file(partial)
        os.replace(partial, path)
        fsync_directory(path.parent)
        store.sync_file(relative)
        schema = pq.read_schema(path)
        if len(schema) > 256 or len(schema.serialize().to_pybytes()) > _MAX_SCHEMA_BYTES:
            raise CatalogueError("historical relation schema exceeds its bounded limit")
        complete = True
        return schema
    except duckdb.Error as exc:
        raise CatalogueError("could not read historical relation schema") from exc
    finally:
        try:
            _remove_owned_file(store, partial)
            if not complete:
                _remove_owned_file(store, path)
        finally:
            store.release_storage(_MAX_SCHEMA_BYTES)


def _ordering_columns(
    schema: pa.Schema, keys: tuple[str, ...]
) -> tuple[tuple[int, ...], tuple[pa.DataType, ...]]:
    indices = []
    types = []
    for name in keys:
        index = schema.get_field_index(name)
        if index < 0:
            raise CatalogueError(f"historical ordering key is missing: {name}")
        data_type = schema.field(index).type
        storage = _storage_type(data_type)
        if not (
            pa.types.is_string(storage)
            or pa.types.is_large_string(storage)
            or pa.types.is_integer(storage)
        ):
            raise CatalogueError(f"unsupported historical ordering key type: {name}")
        indices.append(index)
        types.append(storage)
    return tuple(indices), tuple(types)


def _checkpoint_matches(checkpoint: dict[str, Any], expected: dict[str, Any]) -> bool:
    return all(checkpoint.get(key) == value for key, value in expected.items())


def _input_prefetch_metrics(checkpoint: dict[str, Any]) -> dict[str, int]:
    names = (
        "input_arrow_peak_bytes",
        "input_arrow_peak_rows",
        "input_max_singleton_bytes",
        "input_singleton_peak_bytes",
    )
    result = {name: checkpoint.get(name) for name in names}
    if any(type(value) is not int or value < 0 for value in result.values()):
        raise CatalogueError("historical input prefetch metrics are missing or invalid")
    return result


def _read_checkpoint(store: Store, path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    _assert_regular(store, path)
    body = regular_bytes(path, 1024 * 1024)
    value = decode(body)
    if not isinstance(value, dict):
        raise CatalogueError("invalid historical export checkpoint")
    return value


def _valid_completed_export(
    store: Store, path: Path, checkpoint: dict[str, Any], schema: pa.Schema
) -> bool:
    try:
        if (
            not _hex(checkpoint.get("output_sha256"), 64)
            or isinstance(checkpoint.get("output_size"), bool)
            or not isinstance(checkpoint.get("output_size"), int)
            or checkpoint["output_size"] < 0
            or isinstance(checkpoint.get("row_count"), bool)
            or not isinstance(checkpoint.get("row_count"), int)
            or checkpoint["row_count"] < 0
        ):
            return False
        _assert_regular(store, path)
        checksum, size = file_digest(path, store.limits.max_root_bytes)
        parquet = pq.ParquetFile(path)
        return (
            checksum == checkpoint.get("output_sha256")
            and size == checkpoint.get("output_size")
            and parquet.metadata.num_rows == checkpoint.get("row_count")
            and parquet.schema_arrow.equals(schema, check_metadata=True)
        )
    except (CatalogueError, OSError, pa.ArrowException, ValueError):
        return False


def _storage_type(data_type: pa.DataType) -> pa.DataType:
    return data_type.storage_type if isinstance(data_type, pa.BaseExtensionType) else data_type


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _validate_prefetch_schema(schema: pa.Schema) -> None:
    if (
        not schema.names
        or len(set(schema.names)) != len(schema.names)
        or any(name.lower() == "rowid" for name in schema.names)
    ):
        raise CatalogueError("historical relation has an unsupported Arrow schema")
    for field in schema:
        data_type = _storage_type(field.type)
        if (
            pa.types.is_string(data_type)
            or pa.types.is_large_string(data_type)
            or pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
            or pa.types.is_fixed_size_binary(data_type)
        ):
            continue
        try:
            _ = data_type.bit_width
        except (AttributeError, ValueError):
            raise CatalogueError(
                f"unsupported historical relation field type: {field.name}"
            ) from None


def _input_directory(store: Store, work_relative: str, relation: str) -> Path:
    return store.path(f"{work_relative}/inputs/{relation}")


def _input_marker(
    binding_sha256: str,
    relation: str,
    table: str,
    schema_sha256: str,
) -> dict[str, Any]:
    return {
        "version": 2,
        "state": "materializing",
        "binding_sha256": binding_sha256,
        "relation": relation,
        "table": table,
        "schema_sha256": schema_sha256,
    }


def _validate_input_tree(
    store: Store,
    path: Path,
    relation: str,
    binding_sha256: str | None = None,
    table: str | None = None,
    schema_sha256: str | None = None,
) -> dict[str, Any] | None:
    if _RELATION.fullmatch(relation) is None or path.name != relation:
        raise CatalogueError("historical input relation directory is invalid")
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    _assert_directory(store, path)
    entries = {child.name: child for child in path.iterdir()}
    if set(entries) - {
        "owner.json",
        "input.duckdb",
        "input.duckdb.wal",
    } and not _interrupted_owner_write(store, entries):
        raise CatalogueError("historical input contains an unexpected file")
    owner = entries.get("owner.json")
    if owner is None:
        if entries:
            if _interrupted_owner_write(store, entries):
                return None
            raise CatalogueError("historical input data has no valid owner marker")
        return None
    _assert_regular(store, owner)
    try:
        marker = decode(regular_bytes(owner, 1024))
    except Exception as exc:
        raise CatalogueError("historical input owner marker is invalid") from exc
    base_fields = {
        "version",
        "state",
        "binding_sha256",
        "relation",
        "table",
        "schema_sha256",
    }
    sealed_fields = base_fields | {"input_sha256", "input_size", "row_count"}
    if (
        not isinstance(marker, dict)
        or marker.get("state") not in {"materializing", "sealed"}
        or set(marker) != (sealed_fields if marker.get("state") == "sealed" else base_fields)
        or type(marker.get("version")) is not int
        or marker["version"] != 2
        or not _hex(marker.get("binding_sha256"), 64)
        or marker.get("relation") != relation
        or not isinstance(marker.get("table"), str)
        or _RELATION.fullmatch(marker["table"]) is None
        or not _hex(marker.get("schema_sha256"), 64)
        or marker.get("state") == "sealed"
        and (
            not _hex(marker.get("input_sha256"), 64)
            or isinstance(marker.get("input_size"), bool)
            or not isinstance(marker.get("input_size"), int)
            or marker["input_size"] < 0
            or isinstance(marker.get("row_count"), bool)
            or not isinstance(marker.get("row_count"), int)
            or marker["row_count"] < 0
        )
    ):
        raise CatalogueError("historical input owner marker is invalid")
    if any(
        expected is not None and marker[key] != expected
        for key, expected in (
            ("binding_sha256", binding_sha256),
            ("table", table),
            ("schema_sha256", schema_sha256),
        )
    ):
        raise CatalogueError("historical input owner binding changed")
    for name, child in entries.items():
        if name == "owner.json":
            _assert_regular(store, child)
        else:
            _assert_owned_input_data_file(store, child)
    return marker


def _interrupted_owner_write(store: Store, entries: dict[str, Path]) -> bool:
    if len(entries) != 1:
        return False
    name, path = next(iter(entries.items()))
    if _WRITE_TEMP.fullmatch(name) is None:
        return False
    _assert_regular(store, path)
    if path.stat().st_size > 1024:
        raise CatalogueError("interrupted historical input marker is oversized")
    return True


def _discard_interrupted_marker_temps(
    store: Store, root: Path, *, relations: set[str] | None = None
) -> None:
    inputs = root / "inputs"
    if not inputs.exists():
        return
    _assert_directory(store, inputs)
    changed = False
    for path in list(inputs.iterdir()):
        if relations is not None and path.name not in relations:
            continue
        _validate_input_tree(store, path, path.name)
        entries = list(path.iterdir())
        if not entries:
            path.rmdir()
            fsync_directory(inputs)
            changed = True
        elif _interrupted_owner_write(store, {entry.name: entry for entry in entries}):
            entries[0].unlink()
            fsync_directory(path)
            path.rmdir()
            fsync_directory(inputs)
            changed = True
    if changed:
        store.sync(f"{root.relative_to(store.root).as_posix()}/inputs")


def _durable_schema_digest(store: Store, root: Path, relation: str) -> str:
    path = store.path(f"{root.relative_to(store.root).as_posix()}/schemas/{relation}.parquet")
    _assert_regular(store, path)
    if path.stat().st_size > _MAX_SCHEMA_BYTES:
        raise CatalogueError("historical reference schema exceeds its bounded limit")
    try:
        schema = pq.read_schema(path)
    except (OSError, pa.ArrowException, ValueError) as exc:
        raise CatalogueError("historical reference schema is invalid") from exc
    serialized = schema.serialize().to_pybytes()
    if len(schema) > 256 or len(serialized) > _MAX_SCHEMA_BYTES:
        raise CatalogueError("historical reference schema exceeds its bounded limit")
    return digest(serialized)


def _discard_stale_inputs(
    store: Store,
    root: Path,
    binding_sha256: str,
    tables: dict[str, str],
    *,
    relations: set[str] | None = None,
) -> None:
    if not _hex(binding_sha256, 64):
        raise CatalogueError("historical input binding is invalid")
    _discard_interrupted_marker_temps(store, root, relations=relations)
    inputs = root / "inputs"
    if not inputs.exists():
        return
    _assert_directory(store, inputs)
    for path in list(inputs.iterdir()):
        relation = path.name
        if relations is not None and relation not in relations:
            continue
        marker = _validate_input_tree(store, path, relation)
        if marker is None:
            continue
        if relation not in tables:
            raise CatalogueError("historical input relation has no caller identity")
        table = tables[relation]
        if _RELATION.fullmatch(table) is None:
            raise CatalogueError("historical input table identity is invalid")
        schema_sha256 = _durable_schema_digest(store, root, relation)
        marker = _validate_input_tree(
            store,
            path,
            relation,
            binding_sha256,
            table,
            schema_sha256,
        )
        checkpoint = _read_checkpoint(store, root / "checkpoints" / f"{relation}.json")
        complete = (
            checkpoint is not None
            and checkpoint.get("complete") is True
            and checkpoint.get("binding_sha256") == binding_sha256
            and checkpoint.get("table") == table
            and checkpoint.get("schema_sha256") == schema_sha256
        )
        if marker["state"] == "materializing" or complete:
            _discard_input(
                store,
                root.relative_to(store.root).as_posix(),
                relation,
                binding_sha256,
                table,
                schema_sha256,
            )
        else:
            schema_path = store.path(
                f"{root.relative_to(store.root).as_posix()}/schemas/{relation}.parquet"
            )
            _validate_sealed_input(store, path, marker, pq.read_schema(schema_path))


def _validate_prepared_frontiers(
    store: Store,
    root: Path,
    binding_sha256: str,
    tables: dict[str, str],
) -> None:
    runs_root = root / "runs"
    checkpoints_root = root / "checkpoints"
    run_relations = {path.name for path in runs_root.iterdir()}
    checkpoint_relations = {
        path.stem for path in checkpoints_root.iterdir() if path.name != "publication.json"
    }
    if (run_relations | checkpoint_relations) - set(tables):
        raise CatalogueError("historical frontier inventory has an unknown relation")
    for relation, table in tables.items():
        run_directory = runs_root / relation
        checkpoint_path = checkpoints_root / f"{relation}.json"
        if not run_directory.exists() and not checkpoint_path.exists():
            continue
        schema_path = store.path(
            f"{root.relative_to(store.root).as_posix()}/schemas/{relation}.parquet"
        )
        schema_sha256 = _durable_schema_digest(store, root, relation)
        schema = pq.read_schema(schema_path)
        checkpoint = _read_checkpoint(store, checkpoint_path)
        identity = {
            "version": 2,
            "binding_sha256": binding_sha256,
            "table": table,
            "keys": checkpoint.get("keys") if checkpoint is not None else [],
            "schema_sha256": schema_sha256,
        }
        if checkpoint is None:
            if not run_directory.exists():
                raise CatalogueError("historical frontier checkpoint has no run directory")
            owner_path = run_directory / "owner.json"
            if owner_path.exists():
                _ensure_frontier_owner(store, run_directory, relation, identity, create=False)
            if any(path.name != "owner.json" for path in run_directory.iterdir()):
                raise CatalogueError("uncommitted historical runs have no frontier checkpoint")
            continue
        if (
            not isinstance(identity["keys"], list)
            or not identity["keys"]
            or any(
                not isinstance(key, str) or _RELATION.fullmatch(key) is None
                for key in identity["keys"]
            )
        ):
            raise CatalogueError("historical frontier ordering binding is invalid")
        _ordering_columns(schema, tuple(identity["keys"]))
        if not run_directory.exists():
            raise CatalogueError("historical frontier checkpoint has no run directory")
        _ensure_frontier_owner(store, run_directory, relation, identity, create=False)
        if not _checkpoint_matches(checkpoint, identity):
            raise CatalogueError("historical frontier checkpoint binding changed")
        _validate_frontier(store, run_directory, checkpoint, schema)
        checkpoint = _recover_pending_write(
            store,
            checkpoint_path.relative_to(store.root).as_posix(),
            run_directory,
            checkpoint,
            schema,
        )
        _finish_frontier_deletes(
            store,
            checkpoint_path.relative_to(store.root).as_posix(),
            run_directory,
            checkpoint,
            schema,
        )
        checkpoint = _read_checkpoint(store, checkpoint_path) or checkpoint
        _discard_unreferenced_frontier_runs(store, run_directory, checkpoint)


def _discard_input(
    store: Store,
    work_relative: str,
    relation: str,
    binding_sha256: str,
    table: str,
    schema_sha256: str,
) -> None:
    relative = f"{work_relative}/inputs/{relation}"
    path = _input_directory(store, work_relative, relation)
    marker = _validate_input_tree(store, path, relation, binding_sha256, table, schema_sha256)
    if marker is None and not path.exists():
        return
    for name in ("input.duckdb.wal", "input.duckdb"):
        _remove_owned_input_file(store, path / name)
    _remove_owned_file(store, path / "owner.json")
    path.rmdir()
    fsync_directory(path.parent)
    store.sync(relative)


def _input_snapshot_size(store: Store, directory: Path) -> int:
    for _attempt in range(3):
        try:
            with os.scandir(directory) as scan:
                names = {entry.name for entry in scan}
            if names - {"owner.json", "input.duckdb", "input.duckdb.wal"}:
                raise CatalogueError("historical input contains an unexpected file")
            total = 0
            for name in sorted(names & {"input.duckdb", "input.duckdb.wal"}):
                path = directory / name
                info = path.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_dev != store.root.stat().st_dev
                    or info.st_nlink != 1
                    or info.st_size > store.limits.max_root_bytes
                ):
                    raise CatalogueError("historical input file is unsafe or oversized")
                total += info.st_size
            with os.scandir(directory) as scan:
                current_names = {entry.name for entry in scan}
            if names == current_names:
                return total
        except FileNotFoundError:
            continue
    raise CatalogueError("historical input changed during storage sampling")


def _assert_owned_input_data_file(store: Store, path: Path) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise CatalogueError("historical input file is missing or unsafe") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_dev != store.root.stat().st_dev
        or info.st_nlink != 1
    ):
        raise CatalogueError("historical input file is unsafe")


def _remove_owned_input_file(store: Store, path: Path) -> None:
    if not path.exists():
        return
    _assert_owned_input_data_file(store, path)
    path.unlink()
    fsync_directory(path.parent)
    store.sync(path.relative_to(store.root).as_posix())


def _check_input_storage(store: Store, directory: Path) -> int:
    size = _input_snapshot_size(store, directory)
    usage = store.check_storage(size)
    if usage["database_bytes"] + size > store.limits.max_database_bytes:
        raise BudgetExceeded("historical input exceeds the database storage allowance")
    return size


def _materialize_input(
    store: Store,
    source_connection: duckdb.DuckDBPyConnection,
    work_relative: str,
    binding_sha256: str,
    name: str,
    table: str,
    schema: pa.Schema,
    schema_sha256: str,
) -> Path:
    relative = f"{work_relative}/inputs/{name}"
    directory = _input_directory(store, work_relative, name)
    durable_mkdir(directory)
    fsync_directory(directory.parent)
    marker = _input_marker(binding_sha256, name, table, schema_sha256)
    store.check_storage()
    store.write_bytes(f"{relative}/owner.json", json_bytes(marker))
    store.sync(relative)
    input_database = directory / "input.duckdb"

    description = source_connection.execute(f"describe select * from main.{table}").fetchall()
    if [row[0] for row in description] != schema.names:
        raise CatalogueError("historical input schema changed before materialization")
    columns = []
    for field, row in zip(schema, description, strict=True):
        sql_type = str(row[1])
        if _SCALAR_SQL_TYPE.fullmatch(sql_type) is None:
            raise CatalogueError(f"unsupported historical input type: {field.name}")
        compression = (
            " USING COMPRESSION 'zstd'"
            if sql_type.upper()
            in {
                "VARCHAR",
                "TEXT",
                "STRING",
                "JSON",
                "BLOB",
            }
            else ""
        )
        not_null = " NOT NULL" if row[2] == "NO" else ""
        columns.append(f"{_quote_identifier(field.name)} {sql_type}{compression}{not_null}")

    store.reserve_storage(_INPUT_WRITE_RESERVE)
    attached = False
    detached = False
    operation_error: BaseException | None = None
    sampler_error: list[BaseException] = []
    input_peak = [0]
    stop = threading.Event()
    try:
        usage = store.usage()
        if usage["database_bytes"] + _INPUT_WRITE_RESERVE > store.limits.max_database_bytes:
            raise BudgetExceeded("not enough database storage is available for historical input")
        store.check_deadline()
        input_literal = "'" + str(input_database).replace("'", "''") + "'"
        with interrupt_at_deadline(store, source_connection.interrupt):
            source_connection.execute(f"ATTACH {input_literal} AS historical_input")
            attached = True

            def monitor_storage() -> None:
                while not stop.wait(1.0):
                    try:
                        store.check_deadline()
                        input_peak[0] = max(input_peak[0], _check_input_storage(store, directory))
                    except Exception as exc:
                        sampler_error.append(exc)
                        with contextlib.suppress(duckdb.Error):
                            source_connection.interrupt()
                        return

            monitor = threading.Thread(target=monitor_storage, daemon=True)
            try:
                monitor.start()
                source_connection.execute(
                    f"CREATE TABLE historical_input.main.{_INPUT_TABLE} ({', '.join(columns)})"
                )
                source_connection.execute(
                    f"INSERT INTO historical_input.main.{_INPUT_TABLE} SELECT * FROM main.{table}"
                )
                input_peak[0] = max(input_peak[0], _check_input_storage(store, directory))
                source_connection.execute("CHECKPOINT historical_input")
                input_peak[0] = max(input_peak[0], _check_input_storage(store, directory))
            except BaseException as exc:
                operation_error = exc
            finally:
                stop.set()
                if monitor.ident is not None:
                    monitor.join()
            if operation_error is None:
                source_connection.execute("DETACH historical_input")
                attached = False
                detached = True
    except BaseException as exc:
        if operation_error is None:
            operation_error = exc
    finally:
        if attached:
            try:
                source_connection.execute("DETACH historical_input")
                attached = False
                detached = True
            except duckdb.Error:
                pass
        store.release_storage(_INPUT_WRITE_RESERVE)

    accounting_error: BaseException | None = None
    if detached:
        try:
            store.sync(relative)
            store.check_storage()
        except BaseException as exc:
            accounting_error = exc
    metrics.peak("historical_input_database_peak_bytes", input_peak[0])
    if sampler_error:
        raise sampler_error[0] from operation_error
    if operation_error is not None:
        if isinstance(operation_error, duckdb.InterruptException):
            try:
                store.check_deadline()
            except BudgetExceeded as exc:
                raise exc from operation_error
            if accounting_error is not None:
                raise accounting_error from operation_error
            raise CatalogueError(
                "historical input materialization was interrupted"
            ) from operation_error
        if isinstance(operation_error, duckdb.Error):
            raise CatalogueError("could not materialize historical input") from operation_error
        raise operation_error.with_traceback(operation_error.__traceback__)
    if accounting_error is not None:
        raise accounting_error
    if not detached:
        raise CatalogueError("historical input database was not detached")
    store.check_deadline()
    _seal_input(store, root=store.path(work_relative), directory=directory, schema=schema)
    return input_database


def _seal_input(store: Store, *, root: Path, directory: Path, schema: pa.Schema) -> None:
    database = directory / "input.duckdb"
    wal = directory / "input.duckdb.wal"
    if wal.exists():
        raise CatalogueError("historical input database still has a write-ahead log")
    _assert_owned_input_data_file(store, database)
    checksum, size = file_digest(database, store.limits.max_root_bytes)
    connection = duckdb.connect(str(database), read_only=True, config={"threads": "1"})
    try:
        input_schema = _reference_schema(
            store,
            connection,
            root,
            directory.name,
            _INPUT_TABLE,
            schema_name=f"{directory.name}_input",
        )
        if not input_schema.equals(schema, check_metadata=True):
            raise CatalogueError("historical input changed the reference schema")
        count, minimum, maximum = connection.execute(
            f"select count(*), min(rowid), max(rowid) from main.{_INPUT_TABLE}"
        ).fetchone()
        if count == 0:
            if minimum is not None or maximum is not None:
                raise CatalogueError("historical input row positions are invalid")
        elif minimum != 0 or maximum != count - 1:
            raise CatalogueError("historical input row positions are not dense")
    finally:
        connection.close()
    if wal.exists():
        raise CatalogueError("historical input database produced a write-ahead log")
    if file_digest(database, store.limits.max_root_bytes) != (checksum, size):
        raise CatalogueError("historical input database changed while sealing")
    owner = directory / "owner.json"
    marker = decode(regular_bytes(owner, 1024))
    marker.update(
        {
            "state": "sealed",
            "input_sha256": checksum,
            "input_size": size,
            "row_count": count,
        }
    )
    store.write(owner.relative_to(store.root).as_posix(), marker)
    store.sync(directory.relative_to(store.root).as_posix())
    sealed = _validate_input_tree(
        store,
        directory,
        directory.name,
        marker["binding_sha256"],
        marker["table"],
        marker["schema_sha256"],
    )
    if sealed is None:
        raise CatalogueError("historical input seal disappeared")
    _validate_sealed_input(store, directory, sealed, schema)


def _validate_sealed_input(
    store: Store,
    directory: Path,
    marker: dict[str, Any],
    schema: pa.Schema,
) -> None:
    database = directory / "input.duckdb"
    if (directory / "input.duckdb.wal").exists():
        raise CatalogueError("sealed historical input still has a write-ahead log")
    _assert_owned_input_data_file(store, database)
    if file_digest(database, store.limits.max_root_bytes) != (
        marker.get("input_sha256"),
        marker.get("input_size"),
    ):
        raise CatalogueError("sealed historical input database changed")
    connection = duckdb.connect(str(database), read_only=True, config={"threads": "1"})
    try:
        input_schema = _reference_schema(
            store,
            connection,
            store.path(directory.parent.parent.relative_to(store.root).as_posix()),
            directory.name,
            _INPUT_TABLE,
            schema_name=f"{directory.name}_input",
        )
        if not input_schema.equals(schema, check_metadata=True):
            raise CatalogueError("sealed historical input schema changed")
        count, minimum, maximum = connection.execute(
            f"select count(*), min(rowid), max(rowid) from main.{_INPUT_TABLE}"
        ).fetchone()
        if count != marker.get("row_count") or (
            (count == 0 and (minimum is not None or maximum is not None))
            or (count > 0 and (minimum != 0 or maximum != count - 1))
        ):
            raise CatalogueError("sealed historical input row positions changed")
    except duckdb.Error as exc:
        raise CatalogueError("could not validate the sealed historical input") from exc
    finally:
        connection.close()
    if (directory / "input.duckdb.wal").exists() or file_digest(
        database, store.limits.max_root_bytes
    ) != (marker.get("input_sha256"), marker.get("input_size")):
        raise CatalogueError("sealed historical input changed during validation")


def _prefetch_windows(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    schema: pa.Schema,
    *,
    start_rowid: int = 0,
    total_row_count: int | None = None,
) -> Iterator[tuple[int, int, int]]:
    query = _input_row_size_query(table, schema)
    if total_row_count is None:
        count, minimum, maximum = connection.execute(
            f"select count(*), min(rowid), max(rowid) from main.{table}"
        ).fetchone()
        if count == 0:
            if minimum is not None or maximum is not None:
                raise CatalogueError("historical input row positions are invalid")
            return
        if minimum != 0 or maximum != count - 1:
            raise CatalogueError("historical input row positions are not dense")
    else:
        count = total_row_count

    if (
        isinstance(start_rowid, bool)
        or not isinstance(start_rowid, int)
        or not 0 <= start_rowid <= count
    ):
        raise CatalogueError("historical input cursor is invalid")
    start = start_rowid
    while start < count:
        block_end = min(count, start + _BATCH_ROWS)
        metadata = connection.execute(query, [start, block_end]).fetchall()
        if len(metadata) != block_end - start:
            raise CatalogueError("historical input row sizing has a gap")
        window_start = start
        window_rows = 0
        window_bytes = 0
        for expected_rowid, (rowid, size) in enumerate(metadata, start=start):
            if (
                rowid != expected_rowid
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size < 1
            ):
                raise CatalogueError("historical input row sizing is invalid")
            if window_rows and (window_rows >= _BATCH_ROWS or window_bytes + size > _BATCH_BYTES):
                yield window_start, window_rows, window_bytes
                window_start = rowid
                window_rows = 0
                window_bytes = 0
            if not window_rows and size > _BATCH_BYTES:
                yield rowid, 1, size
                window_start = rowid + 1
                continue
            window_rows += 1
            window_bytes += size
        if window_rows:
            yield window_start, window_rows, window_bytes
        start = block_end


def _input_row_size_query(table: str, schema: pa.Schema) -> str:
    if _RELATION.fullmatch(table) is None:
        raise CatalogueError("invalid historical input table")
    terms = ["8"]
    for field in schema:
        data_type = _storage_type(field.type)
        identifier = _quote_identifier(field.name)
        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            terms.append(f"strlen(coalesce(cast({identifier} as varchar), '')) + 9")
        elif (
            pa.types.is_binary(data_type)
            or pa.types.is_large_binary(data_type)
            or pa.types.is_fixed_size_binary(data_type)
        ):
            terms.append(f"coalesce(octet_length({identifier}), 0) + 9")
        else:
            try:
                width = (data_type.bit_width + 7) // 8
            except (AttributeError, ValueError):
                raise CatalogueError(f"unsupported historical input type: {field.name}") from None
            terms.append(str(width + 1))
    row_bytes = " + ".join(f"({term})" for term in terms)
    return (
        f"select rowid, {row_bytes} as row_bytes from main.{table} "
        "where rowid >= ? and rowid < ? order by rowid"
    )


def _read_input_window(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    start_rowid: int,
    row_count: int,
) -> Any:
    return connection.sql(
        f"select * from main.{table} where rowid >= ? and rowid < ? order by rowid",
        params=[start_rowid, start_rowid + row_count],
    ).to_arrow_reader(row_count)


def _byte_bounded_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
    start = 0
    while start < batch.num_rows:
        stop = min(batch.num_rows, start + _BATCH_ROWS)
        while stop - start > 1 and batch.slice(start, stop - start).nbytes > _BATCH_BYTES:
            stop = start + (stop - start) // 2
        yield batch.slice(start, stop - start)
        start = stop


def _frontier_owner(identity: dict[str, Any], relation: str) -> dict[str, Any]:
    return {
        "version": 1,
        "binding_sha256": identity["binding_sha256"],
        "relation": relation,
        "table": identity["table"],
        "schema_sha256": identity["schema_sha256"],
    }


def _ensure_frontier_owner(
    store: Store,
    run_directory: Path,
    relation: str,
    identity: dict[str, Any],
    *,
    create: bool = True,
) -> None:
    _assert_directory(store, run_directory)
    path = run_directory / "owner.json"
    expected = _frontier_owner(identity, relation)
    if not path.exists():
        if not create:
            raise CatalogueError("historical frontier owner marker is missing")
        if any(run_directory.iterdir()):
            raise CatalogueError("historical frontier files have no owner marker")
        store.write(path.relative_to(store.root).as_posix(), expected)
        store.sync(run_directory.relative_to(store.root).as_posix())
        return
    _assert_regular(store, path)
    try:
        actual = decode(regular_bytes(path, 1024))
    except Exception as exc:
        raise CatalogueError("historical frontier owner marker is invalid") from exc
    if actual != expected:
        raise CatalogueError("historical frontier owner binding changed")


def _empty_frontier(identity: dict[str, Any]) -> dict[str, Any]:
    return {
        **identity,
        "complete": False,
        "next_rowid": 0,
        "source_row_count": None,
        "input_sha256": None,
        "input_size": None,
        "next_run_id": 0,
        "levels": [],
        "pending_write": None,
        "pending_delete": [],
        "run_count": 0,
        "row_count": 0,
        "run_bytes": 0,
        "input_arrow_peak_bytes": 0,
        "input_arrow_peak_rows": 0,
        "input_max_singleton_bytes": 0,
        "input_singleton_peak_bytes": 0,
    }


def _frontier_name(run_id: int) -> str:
    if isinstance(run_id, bool) or not isinstance(run_id, int) or not 0 <= run_id < 10**12:
        raise CatalogueError("historical frontier run identifier is invalid")
    return f"run-{run_id:012d}.parquet"


def _write_frontier_run(
    store: Store,
    run_directory: Path,
    run_id: int,
    level: int,
    batch: pa.RecordBatch,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
) -> dict[str, Any]:
    table = pa.Table.from_batches([batch]).cast(schema)
    key_columns = []
    key_names = []
    for index in key_indices:
        field = schema.field(index)
        column = table.column(index)
        data_type = _storage_type(field.type)
        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            column = pc.cast(column, pa.binary())
        key_columns.append(column)
        key_names.append(field.name)
    indices = pc.sort_indices(
        pa.Table.from_arrays(key_columns, names=key_names),
        sort_keys=[(name, "ascending") for name in key_names],
        null_placement="at_end",
    )
    sorted_batch = table.take(indices)
    name = _frontier_name(run_id)
    path = run_directory / name
    partial = run_directory / (name + ".partial")
    if path.exists() or partial.exists():
        raise CatalogueError("historical frontier run identifier is already present")
    estimated = max(1024 * 1024, sorted_batch.nbytes * 2)
    store.reserve_storage(estimated)
    sealed = False
    try:
        pq.write_table(
            sorted_batch,
            partial,
            compression="zstd",
            compression_level=6,
            row_group_size=_BATCH_ROWS,
        )
        fsync_file(partial)
        checksum, size = file_digest(partial, store.limits.max_root_bytes)
        rows = pq.ParquetFile(partial).metadata.num_rows
        if rows != sorted_batch.num_rows or not pq.ParquetFile(partial).schema_arrow.equals(
            schema, check_metadata=True
        ):
            raise CatalogueError("historical frontier run changed its input batch")
        os.replace(partial, path)
        fsync_directory(run_directory)
        store.sync_file(path.relative_to(store.root).as_posix())
        sealed = True
    finally:
        if not sealed:
            _remove_frontier_artifact(store, partial)
            _remove_frontier_artifact(store, path)
        store.release_storage(estimated)
    return {"name": name, "level": level, "sha256": checksum, "size": size, "rows": rows}


def _merge_frontier_group(
    store: Store,
    run_directory: Path,
    descriptors: list[dict[str, Any]],
    run_id: int,
    level: int,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
) -> dict[str, Any]:
    if len(descriptors) != _MERGE_FAN_IN:
        raise CatalogueError("historical frontier carry must merge exactly eight runs")
    paths = [run_directory / item["name"] for item in descriptors]
    name = _frontier_name(run_id)
    path = run_directory / name
    partial = run_directory / (name + ".partial")
    if path.exists() or partial.exists():
        raise CatalogueError("historical frontier run identifier is already present")
    projected = max(1024 * 1024, sum(item["size"] for item in descriptors) * 2)
    store.reserve_storage(projected)
    rows = 0
    sealed = False
    try:
        writer = pq.ParquetWriter(
            partial,
            schema,
            compression="zstd",
            compression_level=6,
        )
        try:
            for batch in _merge_batches(
                paths,
                schema,
                key_indices,
                key_types,
                store=store,
            ):
                writer.write_table(pa.Table.from_batches([batch]), row_group_size=batch.num_rows)
                rows += batch.num_rows
        finally:
            writer.close()
        fsync_file(partial)
        checksum, size = file_digest(partial, store.limits.max_root_bytes)
        parquet = pq.ParquetFile(partial)
        expected_rows = sum(item["rows"] for item in descriptors)
        if rows != expected_rows or parquet.metadata.num_rows != expected_rows:
            raise CatalogueError("historical frontier carry changed its row count")
        if not parquet.schema_arrow.equals(schema, check_metadata=True):
            raise CatalogueError("historical frontier carry changed its schema")
        os.replace(partial, path)
        fsync_directory(run_directory)
        store.sync_file(path.relative_to(store.root).as_posix())
        sealed = True
    finally:
        if not sealed:
            _remove_frontier_artifact(store, partial)
            _remove_frontier_artifact(store, path)
        store.release_storage(projected)
    return {"name": name, "level": level, "sha256": checksum, "size": size, "rows": rows}


def _frontier_totals(levels: list[list[dict[str, Any]]]) -> tuple[int, int, int]:
    descriptors = [item for level in levels for item in level]
    return (
        len(descriptors),
        sum(item["rows"] for item in descriptors),
        sum(item["size"] for item in descriptors),
    )


def _normalized_levels(levels: list[list[dict[str, Any]]]) -> list[list[dict[str, Any]]]:
    while levels and not levels[-1]:
        levels.pop()
    return levels


def _frontier_state_digest(checkpoint: dict[str, Any]) -> str:
    return digest(
        json_bytes(
            {
                key: checkpoint[key]
                for key in (
                    "binding_sha256",
                    "schema_sha256",
                    "input_sha256",
                    "next_rowid",
                    "next_run_id",
                    "levels",
                    "run_count",
                    "row_count",
                    "run_bytes",
                )
            }
        )
    )


def _frontier_leaf_plan(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    levels = [[item["name"] for item in level] for level in checkpoint["levels"]]
    if not levels:
        levels.append([])
    run_id = checkpoint["next_run_id"]
    planned = [{"name": _frontier_name(run_id), "level": 0, "parents": []}]
    levels[0].append(planned[0]["name"])
    run_id += 1
    level = 0
    while len(levels[level]) > _MERGE_FAN_IN - 1:
        parents = levels[level][:_MERGE_FAN_IN]
        del levels[level][:_MERGE_FAN_IN]
        item = {
            "name": _frontier_name(run_id),
            "level": level + 1,
            "parents": parents,
        }
        planned.append(item)
        run_id += 1
        while len(levels) <= level + 1:
            levels.append([])
        levels[level + 1].append(item["name"])
        level += 1
    return planned


def _frontier_collapse_group(
    levels: list[list[dict[str, Any]]],
) -> tuple[list[list[dict[str, Any]]], list[dict[str, Any]], int]:
    working = [list(level) for level in levels]
    group: list[dict[str, Any]] = []
    for level in working:
        take = min(_MERGE_FAN_IN - len(group), len(level))
        group.extend(level[:take])
        del level[:take]
        if len(group) == _MERGE_FAN_IN:
            break
    if len(group) != _MERGE_FAN_IN:
        raise CatalogueError("historical final frontier could not collapse eight runs")
    output_level = len(_normalized_levels(working))
    return working, group, output_level


def _frontier_collapse_plan(checkpoint: dict[str, Any]) -> list[dict[str, Any]]:
    _, group, level = _frontier_collapse_group(checkpoint["levels"])
    return [
        {
            "name": _frontier_name(checkpoint["next_run_id"]),
            "level": level,
            "parents": [item["name"] for item in group],
        }
    ]


def _begin_frontier_write(
    store: Store,
    checkpoint_relative: str,
    checkpoint: dict[str, Any],
    *,
    kind: str,
    cursor_end: int,
) -> dict[str, Any]:
    if checkpoint.get("pending_write") is not None or checkpoint["pending_delete"]:
        raise CatalogueError("historical frontier has unfinished file ownership work")
    if kind == "leaf":
        planned = _frontier_leaf_plan(checkpoint)
        source_count = checkpoint.get("source_row_count")
        if (
            source_count is None
            or isinstance(cursor_end, bool)
            or not isinstance(cursor_end, int)
            or cursor_end < checkpoint["next_rowid"]
            or cursor_end > source_count
            or cursor_end - checkpoint["next_rowid"] > _BATCH_ROWS
            or cursor_end == checkpoint["next_rowid"]
            and (source_count != 0 or checkpoint["run_count"] != 0)
        ):
            raise CatalogueError("historical frontier source batch cursor is invalid")
    elif kind == "collapse":
        if cursor_end != checkpoint["next_rowid"] or checkpoint["run_count"] <= _MERGE_FAN_IN:
            raise CatalogueError("historical frontier collapse cursor is invalid")
        planned = _frontier_collapse_plan(checkpoint)
    else:
        raise CatalogueError("historical frontier write intent kind is invalid")
    intent = {
        "kind": kind,
        "binding_sha256": checkpoint["binding_sha256"],
        "schema_sha256": checkpoint["schema_sha256"],
        "input_sha256": checkpoint["input_sha256"],
        "cursor_start": checkpoint["next_rowid"],
        "cursor_end": cursor_end,
        "frontier_sha256": _frontier_state_digest(checkpoint),
        "planned": planned,
        "completed": [],
    }
    updated = {**checkpoint, "pending_write": intent}
    store.write(checkpoint_relative, updated)
    return updated


def _record_frontier_write(
    store: Store,
    checkpoint_relative: str,
    checkpoint: dict[str, Any],
    descriptor: dict[str, Any],
) -> dict[str, Any]:
    intent = checkpoint.get("pending_write")
    if not isinstance(intent, dict):
        raise CatalogueError("historical frontier write has no durable intent")
    index = len(intent["completed"])
    if index >= len(intent["planned"]):
        raise CatalogueError("historical frontier write exceeded its durable intent")
    planned = intent["planned"][index]
    _validate_frontier_descriptor(descriptor, planned["level"])
    if descriptor["name"] != planned["name"]:
        raise CatalogueError("historical frontier output differs from its durable intent")
    updated_intent = {**intent, "completed": [*intent["completed"], descriptor]}
    updated = {**checkpoint, "pending_write": updated_intent}
    store.write(checkpoint_relative, updated)
    return updated


def _validate_frontier_write_intent(
    store: Store,
    run_directory: Path,
    checkpoint: dict[str, Any],
    intent: Any,
    schema: pa.Schema,
) -> None:
    fields = {
        "kind",
        "binding_sha256",
        "schema_sha256",
        "input_sha256",
        "cursor_start",
        "cursor_end",
        "frontier_sha256",
        "planned",
        "completed",
    }
    if (
        not isinstance(intent, dict)
        or set(intent) != fields
        or checkpoint["complete"]
        or checkpoint["pending_delete"]
        or intent.get("binding_sha256") != checkpoint["binding_sha256"]
        or intent.get("schema_sha256") != checkpoint["schema_sha256"]
        or intent.get("input_sha256") != checkpoint.get("input_sha256")
        or intent.get("cursor_start") != checkpoint["next_rowid"]
        or intent.get("frontier_sha256") != _frontier_state_digest(checkpoint)
        or not isinstance(intent.get("planned"), list)
        or not isinstance(intent.get("completed"), list)
    ):
        raise CatalogueError("historical frontier write intent is invalid")
    cursor_end = intent.get("cursor_end")
    source_count = checkpoint.get("source_row_count")
    if (
        source_count is None
        or isinstance(cursor_end, bool)
        or not isinstance(cursor_end, int)
        or cursor_end < checkpoint["next_rowid"]
        or cursor_end > source_count
    ):
        raise CatalogueError("historical frontier write intent cursor is invalid")
    if intent["kind"] == "leaf":
        if (
            cursor_end - checkpoint["next_rowid"] > _BATCH_ROWS
            or cursor_end == checkpoint["next_rowid"]
            and (source_count != 0 or checkpoint["run_count"] != 0)
        ):
            raise CatalogueError("historical frontier leaf intent is invalid")
        expected = _frontier_leaf_plan(checkpoint)
    elif intent["kind"] == "collapse":
        if cursor_end != checkpoint["next_rowid"] or checkpoint["run_count"] <= _MERGE_FAN_IN:
            raise CatalogueError("historical frontier collapse intent is invalid")
        expected = _frontier_collapse_plan(checkpoint)
    else:
        raise CatalogueError("historical frontier write intent kind is invalid")
    if intent["planned"] != expected or not expected:
        raise CatalogueError("historical frontier planned outputs changed")
    if len(intent["completed"]) > len(expected):
        raise CatalogueError("historical frontier completed too many planned outputs")
    for index, descriptor in enumerate(intent["completed"]):
        _validate_frontier_descriptor(descriptor, expected[index]["level"])
        if descriptor["name"] != expected[index]["name"]:
            raise CatalogueError("historical frontier completed output changed")
    if intent["kind"] == "collapse" and len(intent["completed"]) > 1:
        raise CatalogueError("historical collapse intent has extra outputs")
    for index, planned in enumerate(expected):
        final_path = run_directory / planned["name"]
        partial_path = run_directory / (planned["name"] + ".partial")
        if final_path.exists() and partial_path.exists():
            raise CatalogueError("historical frontier intent has duplicate output paths")
        if index < len(intent["completed"]):
            if partial_path.exists():
                raise CatalogueError("sealed frontier output has an unexpected partial file")
            if final_path.exists():
                _validate_frontier_run(store, run_directory, intent["completed"][index], schema)
        else:
            if final_path.exists():
                _assert_frontier_artifact(store, final_path)
            if partial_path.exists():
                _assert_frontier_artifact(store, partial_path)


def _recover_pending_write(
    store: Store,
    checkpoint_relative: str,
    run_directory: Path,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
) -> dict[str, Any]:
    intent = checkpoint.get("pending_write")
    if intent is None:
        return checkpoint
    _validate_frontier_write_intent(store, run_directory, checkpoint, intent, schema)
    for planned in intent["planned"]:
        final_path = run_directory / planned["name"]
        partial_path = run_directory / (planned["name"] + ".partial")
        _remove_frontier_artifact(store, final_path)
        _remove_frontier_artifact(store, partial_path)
    updated = {**checkpoint, "pending_write": None}
    store.write(checkpoint_relative, updated)
    return updated


def _commit_frontier(
    store: Store,
    checkpoint_relative: str,
    run_directory: Path,
    checkpoint: dict[str, Any],
    levels: list[list[dict[str, Any]]],
    next_rowid: int,
    next_run_id: int,
    created: list[dict[str, Any]],
    schema: pa.Schema,
) -> dict[str, Any]:
    intent = checkpoint.get("pending_write")
    if (
        not isinstance(intent, dict)
        or intent["completed"] != created
        or len(created) != len(intent["planned"])
        or next_run_id != checkpoint["next_run_id"] + len(created)
        or next_rowid != intent["cursor_end"]
    ):
        raise CatalogueError("historical frontier commit does not match its write intent")
    levels = _normalized_levels(levels)
    if any(len(level) > _MERGE_FAN_IN - 1 for level in levels):
        raise CatalogueError("historical frontier exceeded seven committed runs at a level")
    run_count, row_count, run_bytes = _frontier_totals(levels)
    if row_count != next_rowid:
        raise CatalogueError("historical frontier cursor and row count differ")
    committed_names = {item["name"] for level in levels for item in level}
    old = [item for level in checkpoint["levels"] for item in level]
    pending = [item for item in old + created if item["name"] not in committed_names]
    updated = {
        **checkpoint,
        "complete": False,
        "next_rowid": next_rowid,
        "next_run_id": next_run_id,
        "levels": levels,
        "pending_write": None,
        "pending_delete": pending,
        "run_count": run_count,
        "row_count": row_count,
        "run_bytes": run_bytes,
    }
    for key in ("output_sha256", "output_size"):
        updated.pop(key, None)
    store.write(checkpoint_relative, updated)
    _finish_frontier_deletes(store, checkpoint_relative, run_directory, updated, schema)
    result = _read_checkpoint(store, store.path(checkpoint_relative))
    if result is None:
        raise CatalogueError("historical frontier checkpoint disappeared")
    return result


def _commit_frontier_leaf(
    store: Store,
    checkpoint_relative: str,
    run_directory: Path,
    checkpoint: dict[str, Any],
    leaf: dict[str, Any],
    next_rowid: int,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
) -> dict[str, Any]:
    intent = checkpoint.get("pending_write")
    if not isinstance(intent, dict) or not intent["completed"] or intent["completed"][0] != leaf:
        raise CatalogueError("historical frontier leaf was not sealed under its write intent")
    levels = [list(level) for level in checkpoint["levels"]]
    if not levels:
        levels.append([])
    levels[0].append(leaf)
    created = list(intent["completed"])
    next_run_id = checkpoint["next_run_id"] + 1
    level = 0
    while len(levels[level]) > _MERGE_FAN_IN - 1:
        group = levels[level][:_MERGE_FAN_IN]
        del levels[level][:_MERGE_FAN_IN]
        planned = intent["planned"][len(created)]
        if planned["parents"] != [item["name"] for item in group]:
            raise CatalogueError("historical frontier carry parents changed from their intent")
        output = _merge_frontier_group(
            store,
            run_directory,
            group,
            int(_FRONTIER_RUN.fullmatch(planned["name"]).group(1)),
            planned["level"],
            schema,
            key_indices,
            key_types,
        )
        checkpoint = _record_frontier_write(store, checkpoint_relative, checkpoint, output)
        created.append(output)
        next_run_id += 1
        while len(levels) <= level + 1:
            levels.append([])
        levels[level + 1].append(output)
        level += 1
    if len(created) != len(intent["planned"]):
        raise CatalogueError("historical frontier carry did not produce every planned output")
    return _commit_frontier(
        store,
        checkpoint_relative,
        run_directory,
        checkpoint,
        levels,
        next_rowid,
        next_run_id,
        created,
        schema,
    )


def _collapse_frontier(
    store: Store,
    checkpoint_relative: str,
    run_directory: Path,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
) -> dict[str, Any]:
    while checkpoint["run_count"] > _MERGE_FAN_IN:
        levels, group, level = _frontier_collapse_group(checkpoint["levels"])
        checkpoint = _begin_frontier_write(
            store,
            checkpoint_relative,
            checkpoint,
            kind="collapse",
            cursor_end=checkpoint["next_rowid"],
        )
        planned = checkpoint["pending_write"]["planned"][0]
        if planned["parents"] != [item["name"] for item in group]:
            raise CatalogueError("historical collapse parents changed from their intent")
        output = _merge_frontier_group(
            store,
            run_directory,
            group,
            int(_FRONTIER_RUN.fullmatch(planned["name"]).group(1)),
            planned["level"],
            schema,
            key_indices,
            key_types,
        )
        checkpoint = _record_frontier_write(store, checkpoint_relative, checkpoint, output)
        while len(levels) <= level:
            levels.append([])
        levels[level].append(output)
        checkpoint = _commit_frontier(
            store,
            checkpoint_relative,
            run_directory,
            checkpoint,
            levels,
            checkpoint["next_rowid"],
            checkpoint["next_run_id"] + 1,
            [output],
            schema,
        )
    return checkpoint


def _validate_frontier_run(
    store: Store,
    run_directory: Path,
    descriptor: dict[str, Any],
    schema: pa.Schema,
) -> None:
    path = run_directory / descriptor["name"]
    _assert_regular(store, path)
    checksum, size = file_digest(path, store.limits.max_root_bytes)
    if checksum != descriptor["sha256"] or size != descriptor["size"]:
        raise CatalogueError("historical frontier run changed")
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != descriptor["rows"] or not parquet.schema_arrow.equals(
        schema, check_metadata=True
    ):
        raise CatalogueError("historical frontier run metadata changed")


def _assert_frontier_artifact(store: Store, path: Path) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise CatalogueError("historical frontier artifact is missing or unsafe") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_dev != store.root.stat().st_dev
        or info.st_nlink != 1
    ):
        raise CatalogueError("historical frontier artifact is unsafe")


def _remove_frontier_artifact(store: Store, path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    _assert_frontier_artifact(store, path)
    path.unlink()
    fsync_directory(path.parent)
    store.sync(path.parent.relative_to(store.root).as_posix())


def _validate_frontier(
    store: Store,
    run_directory: Path,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
) -> None:
    allowed = {
        "version",
        "binding_sha256",
        "table",
        "keys",
        "schema_sha256",
        "complete",
        "next_rowid",
        "source_row_count",
        "input_sha256",
        "input_size",
        "next_run_id",
        "levels",
        "pending_write",
        "pending_delete",
        "run_count",
        "row_count",
        "run_bytes",
        "input_arrow_peak_bytes",
        "input_arrow_peak_rows",
        "input_max_singleton_bytes",
        "input_singleton_peak_bytes",
        "output_sha256",
        "output_size",
    }
    if set(checkpoint) - allowed:
        raise CatalogueError("historical frontier checkpoint has unexpected fields")
    if (
        "pending_write" not in checkpoint
        or type(checkpoint.get("version")) is not int
        or checkpoint["version"] != 2
        or type(checkpoint.get("complete")) is not bool
        or not _hex(checkpoint.get("binding_sha256"), 64)
        or not _hex(checkpoint.get("schema_sha256"), 64)
        or not isinstance(checkpoint.get("table"), str)
        or not isinstance(checkpoint.get("keys"), list)
        or not isinstance(checkpoint.get("levels"), list)
        or not isinstance(checkpoint.get("pending_delete"), list)
    ):
        raise CatalogueError("historical frontier checkpoint is invalid")
    for field in ("next_rowid", "next_run_id", "run_count", "row_count", "run_bytes"):
        value = checkpoint.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CatalogueError("historical frontier checkpoint counters are invalid")
    source_count = checkpoint.get("source_row_count")
    if source_count is not None and (
        isinstance(source_count, bool)
        or not isinstance(source_count, int)
        or source_count < 0
        or checkpoint["next_rowid"] > source_count
    ):
        raise CatalogueError("historical frontier source count is invalid")
    if source_count is None and (
        checkpoint["next_rowid"] != 0
        or checkpoint["run_count"] != 0
        or checkpoint.get("input_sha256") is not None
        or checkpoint.get("input_size") is not None
    ):
        raise CatalogueError("historical frontier prefix has no sealed input binding")
    if (checkpoint.get("input_sha256") is None) != (source_count is None):
        raise CatalogueError("historical frontier input binding is incomplete")
    if source_count is not None and (
        not _hex(checkpoint.get("input_sha256"), 64)
        or isinstance(checkpoint.get("input_size"), bool)
        or not isinstance(checkpoint.get("input_size"), int)
        or checkpoint["input_size"] < 0
    ):
        raise CatalogueError("historical frontier input binding is invalid")
    for metric in (
        "input_arrow_peak_bytes",
        "input_arrow_peak_rows",
        "input_max_singleton_bytes",
        "input_singleton_peak_bytes",
    ):
        value = checkpoint.get(metric)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CatalogueError("historical frontier input metrics are invalid")
    if checkpoint["complete"]:
        if (
            source_count is None
            or checkpoint["next_rowid"] != source_count
            or checkpoint["run_count"] < 1
            or checkpoint["run_count"] > _MERGE_FAN_IN
            or checkpoint["pending_write"] is not None
            or checkpoint["pending_delete"]
            or not _hex(checkpoint.get("output_sha256"), 64)
        ):
            raise CatalogueError("historical completed export digest is invalid")
        size = checkpoint.get("output_size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise CatalogueError("historical completed export size is invalid")
    elif "output_sha256" in checkpoint or "output_size" in checkpoint:
        raise CatalogueError("incomplete historical frontier has output metadata")
    if checkpoint["pending_write"] is not None and checkpoint["pending_delete"]:
        raise CatalogueError("historical frontier has overlapping write and delete work")

    run_names: set[str] = set()
    run_ids: set[int] = set()
    total_rows = total_bytes = total_runs = 0
    for level_index, level in enumerate(checkpoint["levels"]):
        if not isinstance(level, list) or len(level) > _MERGE_FAN_IN - 1:
            raise CatalogueError("historical frontier level exceeds seven runs")
        for descriptor in level:
            _validate_frontier_descriptor(descriptor, level_index)
            match = _FRONTIER_RUN.fullmatch(descriptor["name"])
            assert match is not None
            run_id = int(match.group(1))
            if run_id >= checkpoint["next_run_id"] or run_id in run_ids:
                raise CatalogueError("historical frontier run identifier is invalid")
            if descriptor["name"] in run_names:
                raise CatalogueError("historical frontier repeats a run")
            run_ids.add(run_id)
            run_names.add(descriptor["name"])
            _validate_frontier_run(store, run_directory, descriptor, schema)
            total_rows += descriptor["rows"]
            total_bytes += descriptor["size"]
            total_runs += 1
    if (
        total_runs != checkpoint["run_count"]
        or total_rows != checkpoint["row_count"]
        or total_bytes != checkpoint["run_bytes"]
        or total_rows != checkpoint["next_rowid"]
    ):
        raise CatalogueError("historical frontier inventory does not match its checkpoint")

    if checkpoint["pending_write"] is not None:
        _validate_frontier_write_intent(
            store,
            run_directory,
            checkpoint,
            checkpoint["pending_write"],
            schema,
        )

    pending_names = set()
    pending_ids = set()
    for descriptor in checkpoint["pending_delete"]:
        _validate_frontier_descriptor(descriptor, descriptor.get("level"))
        name = descriptor["name"]
        if name in run_names or name in pending_names:
            raise CatalogueError("historical frontier pending-delete inventory overlaps")
        pending_id = int(_FRONTIER_RUN.fullmatch(name).group(1))
        if (
            pending_id >= checkpoint["next_run_id"]
            or pending_id in run_ids
            or pending_id in pending_ids
        ):
            raise CatalogueError("historical frontier pending-delete identifier is invalid")
        pending_ids.add(pending_id)
        pending_names.add(name)
        path = run_directory / name
        if path.exists():
            _validate_frontier_run(store, run_directory, descriptor, schema)


def _validate_frontier_descriptor(descriptor: Any, level: Any) -> None:
    if (
        not isinstance(descriptor, dict)
        or set(descriptor) != {"name", "level", "sha256", "size", "rows"}
        or descriptor.get("level") != level
        or isinstance(descriptor.get("level"), bool)
        or not isinstance(descriptor.get("level"), int)
        or descriptor["level"] < 0
        or not isinstance(descriptor.get("name"), str)
        or _FRONTIER_RUN.fullmatch(descriptor["name"]) is None
        or not _hex(descriptor.get("sha256"), 64)
        or isinstance(descriptor.get("size"), bool)
        or not isinstance(descriptor.get("size"), int)
        or descriptor["size"] < 0
        or isinstance(descriptor.get("rows"), bool)
        or not isinstance(descriptor.get("rows"), int)
        or descriptor["rows"] < 0
    ):
        raise CatalogueError("historical frontier run descriptor is invalid")


def _finish_frontier_deletes(
    store: Store,
    checkpoint_relative: str,
    run_directory: Path,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
) -> None:
    pending = checkpoint.get("pending_delete", [])
    if not pending:
        return
    for descriptor in pending:
        path = run_directory / descriptor["name"]
        if path.exists():
            _validate_frontier_run(store, run_directory, descriptor, schema)
            _remove_owned_file(store, path)
    updated = {**checkpoint, "pending_delete": []}
    store.write(checkpoint_relative, updated)


def _discard_unreferenced_frontier_runs(
    store: Store,
    run_directory: Path,
    checkpoint: dict[str, Any],
) -> None:
    if checkpoint.get("pending_write") is not None:
        raise CatalogueError("historical frontier write intent was not recovered")
    referenced = {item["name"] for level in checkpoint["levels"] for item in level} | {
        item["name"] for item in checkpoint["pending_delete"]
    }
    for path in list(run_directory.iterdir()):
        if path.name == "owner.json" or path.name in referenced:
            continue
        match = _FRONTIER_RUN.fullmatch(path.name) or _FRONTIER_PARTIAL.fullmatch(path.name)
        if match is None:
            if _GENERATION.fullmatch(path.name):
                raise CatalogueError("legacy run generation found in frontier work directory")
            raise CatalogueError("historical frontier contains a foreign file")
        raise CatalogueError("historical frontier contains an unowned run identifier")


def _copy_merged(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    run_directory: Path,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
    checkpoint: dict[str, Any],
    output_path: Path,
    output_relative: str,
) -> None:
    runs = [
        run_directory / descriptor["name"] for level in checkpoint["levels"] for descriptor in level
    ]
    projected = max(1024 * 1024, checkpoint["run_bytes"] * 2)
    partial = store.path(output_relative + ".partial")
    _remove_owned_file(store, partial)
    stats = {"rows": 0}

    def batches() -> Iterator[pa.RecordBatch]:
        for batch in _merge_batches(runs, schema, key_indices, key_types, store=store):
            stats["rows"] += batch.num_rows
            yield batch

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    table_name = "historical_sorted_export"
    registered = False
    threads = None
    insertion_order = None
    store.reserve_storage(projected)
    try:
        threads = connection.execute("select current_setting('threads')").fetchall()[0][0]
        insertion_order = connection.execute(
            "select current_setting('preserve_insertion_order')"
        ).fetchall()[0][0]
        connection.execute("set threads=1")
        connection.execute("set preserve_insertion_order=true")
        if (
            "statistics_propagation"
            not in connection.execute("select current_setting('disabled_optimizers')").fetchall()[
                0
            ][0]
        ):
            raise CatalogueError("historical export lost disabled statistics propagation")
        connection.register(table_name, reader)
        registered = True
        connection.execute(
            f"copy {table_name} to ? (format parquet, {COPY_OPTIONS})", [str(partial)]
        ).fetchall()
        store.check_deadline()
        if stats["rows"] != checkpoint["row_count"]:
            raise CatalogueError("historical merge row count changed during final COPY")
        fsync_file(partial)
        os.replace(partial, output_path)
        fsync_directory(output_path.parent)
        store.sync_file(output_relative)
    except BaseException as exc:
        _remove_owned_file(store, partial)
        if isinstance(exc, duckdb.Error):
            raise CatalogueError("historical Parquet COPY failed") from exc
        raise
    finally:
        try:
            if registered:
                with contextlib.suppress(duckdb.Error):
                    connection.unregister(table_name)
            reader.close()
            if threads is not None:
                connection.execute(f"set threads={threads}")
            if insertion_order is not None:
                connection.execute(
                    f"set preserve_insertion_order={'true' if insertion_order else 'false'}"
                )
        finally:
            store.release_storage(projected)


class _Stream:
    def __init__(self, path: Path, schema: pa.Schema):
        self.path = path
        self.parquet = pq.ParquetFile(path)
        if not self.parquet.schema_arrow.equals(schema, check_metadata=True):
            raise CatalogueError("historical sorted run schema changed")
        self.row_group = 0
        self.batches = iter(())
        self.batch: pa.RecordBatch | None = None
        self.batch_id = 0
        self.position = 0

    def next_batch(self) -> pa.RecordBatch | None:
        while True:
            try:
                self.batch = next(self.batches)
            except StopIteration:
                if self.row_group >= self.parquet.metadata.num_row_groups:
                    self.batch = None
                    return None
                self.batches = iter(
                    self.parquet.iter_batches(
                        batch_size=_BATCH_ROWS,
                        row_groups=[self.row_group],
                        use_threads=False,
                    )
                )
                self.row_group += 1
                continue
            self.batch_id += 1
            self.position = 0
            return self.batch


def _key(
    batch: pa.RecordBatch,
    position: int,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
) -> tuple[tuple[int, Any], ...]:
    values = []
    for index, data_type in zip(key_indices, key_types, strict=True):
        value = batch.column(index)[position].as_py()
        if value is None:
            values.append((1, b""))
        elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            values.append((0, value.encode("utf-8")))
        else:
            values.append((0, value))
    return tuple(values)


def _merge_batches(
    paths: list[Path],
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
    *,
    store: Store | None = None,
) -> Iterator[pa.RecordBatch]:
    if len(paths) > _MERGE_FAN_IN:
        raise CatalogueError("historical merge opened more than eight readers")
    streams = [_Stream(path, schema) for path in paths]
    heap: list[tuple[tuple[tuple[int, Any], ...], int, int, int]] = []
    source_batches: dict[tuple[int, int], pa.RecordBatch] = {}
    order: list[tuple[tuple[int, int], int]] = []
    selected: dict[tuple[int, int], list[int]] = {}
    last_key = None
    out_rows = 0
    out_bytes = 0
    validity_bytes_per_row = len(schema)

    def push(stream_index: int) -> None:
        stream = streams[stream_index]
        if stream.batch is None and stream.next_batch() is None:
            return
        assert stream.batch is not None
        source_id = (stream_index, stream.batch_id)
        source_batches[source_id] = stream.batch
        key = _key(stream.batch, stream.position, key_indices, key_types)
        heapq.heappush(heap, (key, stream_index, stream.position, stream.batch_id))

    for index in range(len(streams)):
        push(index)
    while heap:
        key, stream_index, position, batch_id = heap[0]
        source_id = (stream_index, batch_id)
        batch = source_batches[source_id]
        # Arrow take may allocate one validity byte per selected row and schema field.
        row_bytes = batch.slice(position, 1).nbytes + validity_bytes_per_row
        if out_rows and (out_rows >= _BATCH_ROWS or out_bytes + row_bytes > _BATCH_BYTES):
            if store is not None:
                store.check_deadline()
            yield _take_rows(schema, source_batches, selected, order)
            source_batches.clear()
            for _, active_stream, _, active_batch_id in heap:
                active = streams[active_stream].batch
                if active is not None:
                    source_batches[(active_stream, active_batch_id)] = active
            selected.clear()
            order.clear()
            out_rows = out_bytes = 0
            continue
        heapq.heappop(heap)
        if last_key is not None:
            if key == last_key:
                raise CatalogueError("historical publication ordering key is not unique")
            if key < last_key:
                raise CatalogueError("historical merge output is not monotonic")
        last_key = key
        picks = selected.setdefault(source_id, [])
        picks.append(position)
        order.append((source_id, len(picks) - 1))
        out_rows += 1
        out_bytes += row_bytes

        stream = streams[stream_index]
        stream.position += 1
        if stream.batch is None or stream.position >= stream.batch.num_rows:
            stream.batch = None
        push(stream_index)
    if out_rows:
        if store is not None:
            store.check_deadline()
        yield _take_rows(schema, source_batches, selected, order)


def _take_rows(
    schema: pa.Schema,
    source_batches: dict[tuple[int, int], pa.RecordBatch],
    selected: dict[tuple[int, int], list[int]],
    order: list[tuple[tuple[int, int], int]],
) -> pa.RecordBatch:
    tables = []
    offsets: dict[tuple[int, int], int] = {}
    count = 0
    for source_id, indices in selected.items():
        offsets[source_id] = count
        table = pa.Table.from_batches([source_batches[source_id]]).take(pa.array(indices))
        tables.append(table)
        count += len(indices)
    combined = pa.concat_tables(tables)
    take_indices = pa.array([offsets[source_id] + index for source_id, index in order])
    output = combined.take(take_indices)
    batches = output.to_batches(max_chunksize=_BATCH_ROWS)
    if len(batches) != 1:
        raise CatalogueError("historical merge produced an invalid Arrow batch")
    batch = batches[0]
    if batch.nbytes > _BATCH_BYTES and batch.num_rows > 1:
        raise CatalogueError("historical merge batch exceeds its byte bound")
    if not batch.schema.equals(schema, check_metadata=True):
        raise CatalogueError("historical merge changed relation schema")
    return batch


def _remove_owned_file(store: Store, path: Path) -> None:
    if not path.exists():
        return
    _assert_regular(store, path)
    path.unlink()
    fsync_directory(path.parent)
    store.sync(path.relative_to(store.root).as_posix())


def _assert_directory(store: Store, path: Path) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise CatalogueError("historical export directory is missing or unsafe") from exc
    root_device = store.root.stat().st_dev
    if not stat.S_ISDIR(info.st_mode) or info.st_dev != root_device:
        raise CatalogueError("historical export directory is unsafe")


def _assert_regular(store: Store, path: Path) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise CatalogueError("historical export file is missing or unsafe") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_dev != store.root.stat().st_dev
        or info.st_nlink != 1
        or info.st_size > store.limits.max_root_bytes
    ):
        raise CatalogueError("historical export file is unsafe or oversized")


def _validate_work_tree(store: Store, root: Path) -> None:
    _assert_directory(store, root)
    allowed = {"output", "runs", "checkpoints", "schemas", "inputs", "duckdb-spill"}
    spill_owner = False
    spill_directory = False
    for child in root.iterdir():
        if child.name == _SPILL_OWNER:
            _validate_spill_owner_marker(store, root)
            spill_owner = True
            continue
        if child.name not in allowed or child.is_symlink() or not child.is_dir():
            raise CatalogueError("historical export work tree contains an unexpected entry")
        if child.name == "output":
            for entry in child.iterdir():
                if _OUTPUT.fullmatch(entry.name) is None and entry.name != "manifest.json":
                    raise CatalogueError("historical export output contains an unexpected file")
                _assert_regular(store, entry)
        elif child.name == "checkpoints":
            for entry in child.iterdir():
                if _RELATION.fullmatch(entry.stem) is None or entry.suffix != ".json":
                    raise CatalogueError("historical export checkpoint inventory is unsafe")
                _assert_regular(store, entry)
        elif child.name == "schemas":
            for entry in child.iterdir():
                if re.fullmatch(r"[a-z][a-z0-9_]*\.parquet(?:\.partial)?", entry.name) is None:
                    raise CatalogueError("historical export schema inventory is unsafe")
                _assert_regular(store, entry)
        elif child.name == "runs":
            for relation in child.iterdir():
                if _RELATION.fullmatch(relation.name) is None or relation.is_symlink():
                    raise CatalogueError("historical export run inventory is unsafe")
                _assert_directory(store, relation)
                for entry in relation.iterdir():
                    if entry.name == "owner.json":
                        _assert_regular(store, entry)
                    elif _FRONTIER_RUN.fullmatch(entry.name) or _FRONTIER_PARTIAL.fullmatch(
                        entry.name
                    ):
                        _assert_frontier_artifact(store, entry)
                    elif _GENERATION.fullmatch(entry.name):
                        _assert_directory(store, entry)
                        for generation_entry in entry.iterdir():
                            if generation_entry.name not in {
                                "manifest.jsonl",
                                "manifest.jsonl.partial",
                            } and not (
                                _RUN.fullmatch(generation_entry.name)
                                or re.fullmatch(
                                    r"run-[0-9]{8}\.parquet\.partial",
                                    generation_entry.name,
                                )
                            ):
                                raise CatalogueError("historical run contains an unexpected file")
                            _assert_regular(store, generation_entry)
                    else:
                        raise CatalogueError("historical run inventory is unsafe")
        elif child.name == "inputs":
            for relation in child.iterdir():
                if _RELATION.fullmatch(relation.name) is None or relation.is_symlink():
                    raise CatalogueError("historical input inventory is unsafe")
                _validate_input_tree(store, relation, relation.name)
        else:
            spill_directory = True
            _validate_safe_tree(store, child)
    if spill_directory and not spill_owner:
        raise CatalogueError("historical DuckDB spill directory has no durable owner")


def _validate_spill_owner_marker(store: Store, root: Path) -> dict[str, Any]:
    path = root / _SPILL_OWNER
    _assert_regular(store, path)
    try:
        owner = decode(regular_bytes(path, 4096))
    except Exception as exc:
        raise CatalogueError("historical DuckDB spill owner marker is invalid") from exc
    expected_fields = {
        "version",
        "root",
        "temporary",
        "source_database",
        "spill",
        "historical_export",
        "export_binding_sha256",
    }
    relative = root.relative_to(store.root).as_posix()
    source_database = owner.get("source_database") if isinstance(owner, dict) else None
    if (
        not isinstance(owner, dict)
        or set(owner) != expected_fields
        or type(owner.get("version")) is not int
        or owner["version"] != 1
        or owner.get("root") != str(store.root.resolve())
        or owner.get("temporary") != relative
        or owner.get("spill") != f"{relative}/duckdb-spill"
        or type(owner.get("historical_export")) is not bool
        or not _hex(owner.get("export_binding_sha256"), 64)
        or not isinstance(source_database, str)
        or not source_database
        or Path(source_database).is_absolute()
        or ".." in Path(source_database).parts
        or Path(source_database).as_posix() != source_database
    ):
        raise CatalogueError("historical DuckDB spill owner binding is invalid")
    return owner


def _validate_safe_tree(store: Store, path: Path) -> None:
    _assert_directory(store, path)
    for child in path.iterdir():
        if _DUCKDB_SPILL_FILE.fullmatch(child.name) is None:
            raise CatalogueError("historical DuckDB spill inventory contains a foreign entry")
        try:
            _assert_regular(store, child)
        except CatalogueError as exc:
            raise CatalogueError(
                "historical DuckDB spill inventory contains an unsafe entry"
            ) from exc


def _hex(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )
