"""Bounded historical relation sorting and resumable Parquet export."""

from __future__ import annotations

import contextlib
import hashlib
import heapq
import itertools
import json
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
COPY_OPTIONS = "compression zstd, compression_level 6, row_group_size_bytes 16777216"
_BATCH_ROWS = 4096
_BATCH_BYTES = 8 * 1024 * 1024
_MERGE_FAN_IN = 8
_MAX_SCHEMA_BYTES = 16 * 1024 * 1024
_INPUT_WRITE_RESERVE = 8 * 1024 * 1024
_INPUT_TABLE = "export_input"
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
        _discard_interrupted_marker_temps(store, root)
        if binding_sha256 is not None:
            _discard_stale_inputs(store, root, binding_sha256, tables or {})
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
    if _RELATION.fullmatch(name) is None or _RELATION.fullmatch(table) is None:
        raise CatalogueError("invalid historical export relation")
    if not keys or any(_RELATION.fullmatch(key) is None for key in keys):
        raise CatalogueError("invalid historical export ordering key")

    root = store.path(work_relative)
    _assert_directory(store, root)
    _discard_stale_inputs(store, root, binding_sha256, {name: table})
    schema = _reference_schema(store, connection, root, name, table)
    key_indices, key_types = _ordering_columns(schema, keys)
    schema_sha256 = digest(schema.serialize().to_pybytes())
    _validate_prefetch_schema(schema)
    _discard_input(
        store,
        work_relative,
        name,
        binding_sha256,
        table,
        schema_sha256,
    )
    checkpoint_relative = f"{work_relative}/checkpoints/{name}.json"
    checkpoint_path = store.path(checkpoint_relative)
    output_relative = f"{work_relative}/output/{name}.parquet"
    output_path = store.path(output_relative)
    checkpoint = _read_checkpoint(store, checkpoint_path)

    if checkpoint is not None and checkpoint.get("complete") is True:
        if (
            checkpoint.get("binding_sha256") == binding_sha256
            and checkpoint.get("table") == table
            and checkpoint.get("keys") == list(keys)
            and checkpoint.get("schema_sha256") == schema_sha256
            and _valid_completed_export(store, output_path, checkpoint, schema)
        ):
            _input_prefetch_metrics(checkpoint)
            _remove_relation_runs(store, work_relative, name)
            return checkpoint
        _remove_owned_file(store, output_path)
        _remove_relation_runs(store, work_relative, name)
        store.remove(checkpoint_relative)
        checkpoint = None

    run_relative = f"{work_relative}/runs/{name}"
    run_directory = store.path(run_relative)
    durable_mkdir(run_directory)
    expected_state = {
        "version": 1,
        "binding_sha256": binding_sha256,
        "table": table,
        "keys": list(keys),
        "schema_sha256": schema_sha256,
        "complete": False,
    }

    generation = None
    if checkpoint is not None and _checkpoint_matches(checkpoint, expected_state):
        generation = checkpoint.get("generation")
        if (
            isinstance(generation, int)
            and not isinstance(generation, bool)
            and generation >= 0
            and _valid_generation(store, run_directory, generation, checkpoint, schema)
        ):
            _remove_unreferenced_generations(store, run_relative, generation)
        else:
            generation = None

    if generation is None:
        _remove_relation_runs(store, work_relative, name)
        run_directory.mkdir(exist_ok=True)
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
                        store.path(work_relative),
                        name,
                        _INPUT_TABLE,
                        schema_name=f"{name}_input",
                    )
                    if not input_schema.equals(schema, check_metadata=True):
                        raise CatalogueError("historical input changed the reference schema")
                    generation = _spool_runs(
                        store,
                        input_connection,
                        run_directory,
                        _INPUT_TABLE,
                        schema,
                        key_indices,
                        f"{work_relative}/inputs/{name}",
                    )
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
        checkpoint = {
            **expected_state,
            **generation,
            "generation": 0,
        }
        store.write(checkpoint_relative, checkpoint)
        _discard_input(
            store,
            work_relative,
            name,
            binding_sha256,
            table,
            schema_sha256,
        )

    while checkpoint["run_count"] > _MERGE_FAN_IN:
        next_generation = checkpoint["generation"] + 1
        checkpoint = _merge_generation(
            store,
            run_directory,
            schema,
            key_indices,
            key_types,
            checkpoint,
            next_generation,
        )
        store.write(checkpoint_relative, checkpoint)
        _remove_unreferenced_generations(store, run_relative, next_generation)

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
        **expected_state,
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
    _remove_relation_runs(store, work_relative, name)
    return result


def remove_work(store: Store, relative: str) -> None:
    path = store.path(relative)
    if not path.exists():
        return
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
        "version": 1,
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
    if (
        not isinstance(marker, dict)
        or set(marker) != {"version", "binding_sha256", "relation", "table", "schema_sha256"}
        or type(marker.get("version")) is not int
        or marker["version"] != 1
        or not _hex(marker.get("binding_sha256"), 64)
        or marker.get("relation") != relation
        or not isinstance(marker.get("table"), str)
        or _RELATION.fullmatch(marker["table"]) is None
        or not _hex(marker.get("schema_sha256"), 64)
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


def _discard_interrupted_marker_temps(store: Store, root: Path) -> None:
    inputs = root / "inputs"
    if not inputs.exists():
        return
    _assert_directory(store, inputs)
    changed = False
    for path in list(inputs.iterdir()):
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
) -> None:
    if not _hex(binding_sha256, 64):
        raise CatalogueError("historical input binding is invalid")
    _discard_interrupted_marker_temps(store, root)
    inputs = root / "inputs"
    if not inputs.exists():
        return
    _assert_directory(store, inputs)
    for path in list(inputs.iterdir()):
        relation = path.name
        marker = _validate_input_tree(store, path, relation)
        if marker is None:
            continue
        if relation not in tables:
            raise CatalogueError("historical input relation has no caller identity")
        table = tables[relation]
        if _RELATION.fullmatch(table) is None:
            raise CatalogueError("historical input table identity is invalid")
        schema_sha256 = _durable_schema_digest(store, root, relation)
        _discard_input(
            store,
            root.relative_to(store.root).as_posix(),
            relation,
            binding_sha256,
            table,
            schema_sha256,
        )


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
    return input_database


def _prefetch_windows(
    connection: duckdb.DuckDBPyConnection, table: str, schema: pa.Schema
) -> Iterator[tuple[int, int, int]]:
    query = _input_row_size_query(table, schema)
    count, minimum, maximum = connection.execute(
        f"select count(*), min(rowid), max(rowid) from main.{table}"
    ).fetchone()
    if count == 0:
        if minimum is not None or maximum is not None:
            raise CatalogueError("historical input row positions are invalid")
        return
    if minimum != 0 or maximum != count - 1:
        raise CatalogueError("historical input row positions are not dense")

    start = 0
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


def _spool_runs(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    run_directory: Path,
    table: str,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    input_relative: str,
) -> dict[str, Any]:
    generation = run_directory / "gen-00000000"
    if generation.exists():
        _remove_tree(store, generation)
    generation.mkdir()
    fsync_directory(run_directory)
    manifest_partial = generation / "manifest.jsonl.partial"
    manifest_path = generation / "manifest.jsonl"
    manifest_hash = hashlib.sha256()
    manifest_size = 0
    run_count = 0
    row_count = 0
    run_bytes = 0
    arrow_peak_bytes = 0
    arrow_peak_rows = 0
    max_singleton_bytes = 0
    singleton_peak_bytes = 0
    try:
        with manifest_partial.open("wb") as manifest:
            for window_start, window_rows, estimated_bytes in _prefetch_windows(
                connection, table, schema
            ):
                store.check_deadline()
                if window_rows == 1:
                    max_singleton_bytes = max(max_singleton_bytes, estimated_bytes)
                    metrics.peak("historical_input_singleton_bytes", estimated_bytes)
                reader = _read_input_window(connection, table, window_start, window_rows)
                fetched_rows = 0
                try:
                    for batch in reader:
                        store.check_deadline()
                        fetched_rows += batch.num_rows
                        arrow_peak_bytes = max(arrow_peak_bytes, batch.nbytes)
                        arrow_peak_rows = max(arrow_peak_rows, batch.num_rows)
                        if batch.num_rows == 1:
                            singleton_peak_bytes = max(singleton_peak_bytes, batch.nbytes)
                            metrics.peak("historical_input_singleton_arrow_bytes", batch.nbytes)
                        if batch.num_rows > window_rows or (
                            batch.num_rows > 1 and batch.nbytes > _BATCH_BYTES
                        ):
                            raise CatalogueError("historical input exceeded its Arrow window")
                        metrics.peak("historical_input_arrow_bytes", batch.nbytes)
                        metrics.peak("historical_input_arrow_rows", batch.num_rows)
                        converted = pa.Table.from_batches([batch]).cast(schema)
                        for piece in _byte_bounded_batches(
                            converted.to_batches(max_chunksize=_BATCH_ROWS)[0]
                        ):
                            table_piece = pa.Table.from_batches([piece])
                            sort_columns = []
                            sort_names = []
                            for index in key_indices:
                                field = schema.field(index)
                                column = table_piece.column(index)
                                data_type = _storage_type(field.type)
                                if pa.types.is_string(data_type) or pa.types.is_large_string(
                                    data_type
                                ):
                                    column = pc.cast(column, pa.binary())
                                sort_columns.append(column)
                                sort_names.append(field.name)
                            sort_table = pa.Table.from_arrays(sort_columns, names=sort_names)
                            indices = pc.sort_indices(
                                sort_table,
                                sort_keys=[(name, "ascending") for name in sort_names],
                                null_placement="at_end",
                            )
                            sorted_piece = table_piece.take(indices)
                            name = f"run-{run_count:08d}.parquet"
                            path = generation / (name + ".partial")
                            final_path = generation / name
                            estimate = max(1024 * 1024, sorted_piece.nbytes * 2)
                            store.reserve_storage(estimate)
                            try:
                                pq.write_table(
                                    sorted_piece,
                                    path,
                                    compression="zstd",
                                    compression_level=6,
                                    row_group_size=_BATCH_ROWS,
                                )
                                fsync_file(path)
                                os.replace(path, final_path)
                                fsync_directory(generation)
                                relative = final_path.relative_to(store.root).as_posix()
                                store.sync_file(relative)
                            finally:
                                store.release_storage(estimate)
                            checksum, size = file_digest(final_path, store.limits.max_root_bytes)
                            count = pq.ParquetFile(final_path).metadata.num_rows
                            entry = {
                                "name": name,
                                "sha256": checksum,
                                "size": size,
                                "rows": count,
                            }
                            encoded = json_bytes(entry)
                            store.reserve_storage(len(encoded))
                            try:
                                manifest.write(encoded)
                                manifest.flush()
                                manifest_hash.update(encoded)
                                manifest_size += len(encoded)
                                store.sync_file(manifest_partial.relative_to(store.root).as_posix())
                            finally:
                                store.release_storage(len(encoded))
                            run_count += 1
                            row_count += count
                            run_bytes += size
                finally:
                    reader.close()
                if fetched_rows != window_rows:
                    raise CatalogueError("historical input payload window has a gap")
                store.sync(input_relative)
                store.check_storage()
        fsync_file(manifest_partial)
        os.replace(manifest_partial, manifest_path)
        fsync_directory(generation)
        manifest_relative = manifest_path.relative_to(store.root).as_posix()
        store.sync(manifest_partial.relative_to(store.root).as_posix())
        store.sync_file(manifest_relative)
    except BaseException:
        raise
    if row_count == 0:
        # An empty relation still has one schema-bearing run for DuckDB COPY.
        empty = pa.Table.from_batches([], schema=schema)
        path = generation / "run-00000000.parquet"
        store.reserve_storage(1024 * 1024)
        try:
            pq.write_table(empty, path, compression="zstd", compression_level=6)
            fsync_file(path)
            fsync_directory(generation)
            store.sync_file(path.relative_to(store.root).as_posix())
        finally:
            store.release_storage(1024 * 1024)
        checksum, size = file_digest(path, store.limits.max_root_bytes)
        encoded = json_bytes({"name": path.name, "sha256": checksum, "size": size, "rows": 0})
        store.reserve_storage(len(encoded))
        try:
            with manifest_path.open("ab") as manifest:
                manifest.write(encoded)
                manifest.flush()
                os.fsync(manifest.fileno())
            store.sync_file(manifest_relative)
        finally:
            store.release_storage(len(encoded))
        fsync_directory(generation)
        manifest_hash.update(encoded)
        manifest_size += len(encoded)
        run_count = 1
        run_bytes = size
    return {
        "generation": 0,
        "manifest_sha256": manifest_hash.hexdigest(),
        "manifest_size": manifest_size,
        "run_count": run_count,
        "row_count": row_count,
        "run_bytes": run_bytes,
        "input_arrow_peak_bytes": arrow_peak_bytes,
        "input_arrow_peak_rows": arrow_peak_rows,
        "input_max_singleton_bytes": max_singleton_bytes,
        "input_singleton_peak_bytes": singleton_peak_bytes,
    }


def _byte_bounded_batches(batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
    start = 0
    while start < batch.num_rows:
        stop = min(batch.num_rows, start + _BATCH_ROWS)
        while stop - start > 1 and batch.slice(start, stop - start).nbytes > _BATCH_BYTES:
            stop = start + (stop - start) // 2
        yield batch.slice(start, stop - start)
        start = stop


def _merge_generation(
    store: Store,
    run_directory: Path,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
    key_types: tuple[pa.DataType, ...],
    checkpoint: dict[str, Any],
    next_generation: int,
) -> dict[str, Any]:
    input_directory = run_directory / f"gen-{checkpoint['generation']:08d}"
    output_directory = run_directory / f"gen-{next_generation:08d}"
    # The current generation remains on disk until the next one is sealed.
    # Preflight its full output allowance before starting the merge pass.
    store.check_storage(max(1024 * 1024, checkpoint["run_bytes"] * 2))
    if output_directory.exists():
        _remove_tree(store, output_directory)
    output_directory.mkdir()
    fsync_directory(run_directory)
    input_runs = _iter_runs(store, input_directory, checkpoint, schema)
    output_manifest_partial = output_directory / "manifest.jsonl.partial"
    output_manifest = output_directory / "manifest.jsonl"
    manifest_hash = hashlib.sha256()
    manifest_size = 0
    run_count = 0
    row_count = 0
    run_bytes = 0
    try:
        with output_manifest_partial.open("wb") as manifest:
            while group := list(itertools.islice(input_runs, _MERGE_FAN_IN)):
                store.check_deadline()
                projected = max(1024 * 1024, sum(item[1]["size"] for item in group) * 2)
                store.reserve_storage(projected)
                name = f"run-{run_count:08d}.parquet"
                path = output_directory / (name + ".partial")
                final_path = output_directory / name
                try:
                    writer = pq.ParquetWriter(
                        path,
                        schema,
                        compression="zstd",
                        compression_level=6,
                    )
                    rows = 0
                    try:
                        for batch in _merge_batches(
                            [item[0] for item in group],
                            schema,
                            key_indices,
                            key_types,
                            store=store,
                        ):
                            writer.write_table(
                                pa.Table.from_batches([batch]), row_group_size=batch.num_rows
                            )
                            rows += batch.num_rows
                    finally:
                        writer.close()
                    fsync_file(path)
                    os.replace(path, final_path)
                    fsync_directory(output_directory)
                    store.sync_file(final_path.relative_to(store.root).as_posix())
                    checksum, size = file_digest(final_path, store.limits.max_root_bytes)
                finally:
                    store.release_storage(projected)
                entry = {"name": name, "sha256": checksum, "size": size, "rows": rows}
                encoded = json_bytes(entry)
                store.reserve_storage(len(encoded))
                try:
                    manifest.write(encoded)
                    manifest.flush()
                    manifest_hash.update(encoded)
                    manifest_size += len(encoded)
                    store.sync_file(output_manifest_partial.relative_to(store.root).as_posix())
                finally:
                    store.release_storage(len(encoded))
                run_count += 1
                row_count += rows
                run_bytes += size
        fsync_file(output_manifest_partial)
        os.replace(output_manifest_partial, output_manifest)
        fsync_directory(output_directory)
        store.sync(output_manifest_partial.relative_to(store.root).as_posix())
        store.sync_file(output_manifest.relative_to(store.root).as_posix())
    except BaseException:
        raise
    if run_count == 0:
        raise CatalogueError("historical merge pass produced no runs")
    if row_count != checkpoint["row_count"]:
        raise CatalogueError("historical merge pass changed the row count")
    return {
        **{
            key: value
            for key, value in checkpoint.items()
            if key
            not in {
                "generation",
                "manifest_sha256",
                "manifest_size",
                "run_count",
                "run_bytes",
                "row_count",
            }
        },
        "generation": next_generation,
        "manifest_sha256": manifest_hash.hexdigest(),
        "manifest_size": manifest_size,
        "run_count": run_count,
        "row_count": row_count,
        "run_bytes": run_bytes,
    }


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
    generation = run_directory / f"gen-{checkpoint['generation']:08d}"
    runs = [path for path, _ in _iter_runs(store, generation, checkpoint, schema)]
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


def _iter_runs(
    store: Store,
    generation: Path,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
) -> Iterator[tuple[Path, dict[str, Any]]]:
    manifest = generation / "manifest.jsonl"
    _assert_regular(store, manifest)
    checksum, size = file_digest(manifest, store.limits.max_root_bytes)
    if (
        checksum != checkpoint.get("manifest_sha256")
        or size != checkpoint.get("manifest_size")
        or isinstance(checkpoint.get("manifest_size"), bool)
        or not isinstance(checkpoint.get("manifest_size"), int)
        or isinstance(checkpoint.get("row_count"), bool)
        or not isinstance(checkpoint.get("row_count"), int)
        or checkpoint["row_count"] < 0
        or isinstance(checkpoint.get("run_bytes"), bool)
        or not isinstance(checkpoint.get("run_bytes"), int)
        or checkpoint["run_bytes"] < 0
    ):
        raise CatalogueError("historical run manifest changed")
    expected = checkpoint.get("run_count")
    if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
        raise CatalogueError("invalid historical run count")
    seen = 0
    total_rows = 0
    total_bytes = 0
    with manifest.open("rb") as source:
        for line in source:
            store.check_deadline()
            try:
                record = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise CatalogueError("invalid historical run manifest") from exc
            if (
                not isinstance(record, dict)
                or record.get("name") != f"run-{seen:08d}.parquet"
                or not _hex(record.get("sha256"), 64)
                or isinstance(record.get("size"), bool)
                or not isinstance(record.get("size"), int)
                or record["size"] < 0
                or isinstance(record.get("rows"), bool)
                or not isinstance(record.get("rows"), int)
                or record["rows"] < 0
            ):
                raise CatalogueError("invalid historical run inventory")
            path = store.path(generation.relative_to(store.root).as_posix() + "/" + record["name"])
            _assert_regular(store, path)
            actual_hash, actual_size = file_digest(path, store.limits.max_root_bytes)
            if actual_hash != record["sha256"] or actual_size != record["size"]:
                raise CatalogueError("historical sorted run changed")
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows != record["rows"] or not parquet.schema_arrow.equals(
                schema, check_metadata=True
            ):
                raise CatalogueError("historical sorted run metadata changed")
            seen += 1
            total_rows += record["rows"]
            total_bytes += record["size"]
            yield path, record
    if (
        seen != expected
        or total_rows != checkpoint.get("row_count")
        or total_bytes != checkpoint.get("run_bytes")
    ):
        raise CatalogueError("historical sorted run inventory is incomplete")


def _valid_generation(
    store: Store,
    run_directory: Path,
    generation: int,
    checkpoint: dict[str, Any],
    schema: pa.Schema,
) -> bool:
    path = run_directory / f"gen-{generation:08d}"
    try:
        _assert_directory(store, path)
        entries = list(path.iterdir())
        if any(
            entry.name != "manifest.jsonl" for entry in entries if entry.name.startswith("manifest")
        ):
            return False
        if (
            isinstance(checkpoint.get("run_count"), bool)
            or not isinstance(checkpoint.get("run_count"), int)
            or checkpoint["run_count"] < 1
            or not _hex(checkpoint.get("manifest_sha256"), 64)
            or isinstance(checkpoint.get("generation"), bool)
            or checkpoint.get("generation") != generation
        ):
            return False
        if len(entries) != checkpoint.get("run_count", -1) + 1:
            return False
        for entry in entries:
            if entry.name != "manifest.jsonl" and _RUN.fullmatch(entry.name) is None:
                return False
            _assert_regular(store, entry)
        sum(1 for _ in _iter_runs(store, path, checkpoint, schema))
        return True
    except (CatalogueError, OSError, ValueError):
        return False


def _remove_unreferenced_generations(store: Store, relative: str, keep: int) -> None:
    directory = store.path(relative)
    _assert_directory(store, directory)
    for path in directory.iterdir():
        if _GENERATION.fullmatch(path.name) is None:
            raise CatalogueError("unexpected historical run directory")
        generation = int(path.name[-8:])
        if generation != keep:
            _remove_tree(store, path)


def _remove_relation_runs(store: Store, work_relative: str, name: str) -> None:
    relative = f"{work_relative}/runs/{name}"
    path = store.path(relative)
    if path.exists():
        _remove_tree(store, path)


def _remove_tree(store: Store, path: Path) -> None:
    _validate_owned_tree(store, path)
    shutil.rmtree(path)
    fsync_directory(path.parent)
    store.sync(path.relative_to(store.root).as_posix())


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


def _validate_owned_tree(store: Store, path: Path) -> None:
    _assert_directory(store, path)
    for child in path.iterdir():
        info = child.lstat()
        if stat.S_ISDIR(info.st_mode):
            if _GENERATION.fullmatch(child.name) is None:
                raise CatalogueError("historical export work tree contains an unexpected directory")
            _validate_owned_tree(store, child)
        elif (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or child.name not in {"manifest.jsonl", "manifest.jsonl.partial"}
            and _RUN.fullmatch(child.name) is None
            and re.fullmatch(r"run-[0-9]{8}\.parquet\.partial", child.name) is None
        ):
            raise CatalogueError("historical export work tree contains an unsafe entry")


def _validate_work_tree(store: Store, root: Path) -> None:
    _assert_directory(store, root)
    allowed = {"output", "runs", "checkpoints", "schemas", "inputs", "duckdb-spill"}
    for child in root.iterdir():
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
                for generation in relation.iterdir():
                    if _GENERATION.fullmatch(generation.name) is None:
                        raise CatalogueError("historical export generation inventory is unsafe")
                    _assert_directory(store, generation)
                    for entry in generation.iterdir():
                        if entry.name not in {"manifest.jsonl", "manifest.jsonl.partial"} and not (
                            _RUN.fullmatch(entry.name)
                            or re.fullmatch(r"run-[0-9]{8}\.parquet\.partial", entry.name)
                        ):
                            raise CatalogueError(
                                "historical export run contains an unexpected file"
                            )
                        _assert_regular(store, entry)
        elif child.name == "inputs":
            for relation in child.iterdir():
                if _RELATION.fullmatch(relation.name) is None or relation.is_symlink():
                    raise CatalogueError("historical input inventory is unsafe")
                _validate_input_tree(store, relation, relation.name)
        else:
            _validate_safe_tree(store, child)


def _validate_safe_tree(store: Store, path: Path) -> None:
    _assert_directory(store, path)
    for child in path.iterdir():
        info = child.lstat()
        if stat.S_ISDIR(info.st_mode):
            _validate_safe_tree(store, child)
        elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise CatalogueError("historical export work tree contains an unsafe entry")


def _hex(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )
