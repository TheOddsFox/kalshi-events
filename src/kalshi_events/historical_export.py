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
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from kalshi_events.common import (
    CatalogueError,
    Store,
    decode,
    digest,
    durable_mkdir,
    file_digest,
    fsync_directory,
    fsync_file,
    json_bytes,
    regular_bytes,
)

POLICY = "historical-arrow-external-merge-v1"
COPY_OPTIONS = "compression zstd, compression_level 6, row_group_size_bytes 16777216"
_BATCH_ROWS = 4096
_BATCH_BYTES = 8 * 1024 * 1024
_MERGE_FAN_IN = 8
_MAX_SCHEMA_BYTES = 16 * 1024 * 1024
_RELATION = re.compile(r"[a-z][a-z0-9_]*\Z")
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


def prepare(store: Store, relative: str) -> Path:
    if not re.fullmatch(r"tmp/publish-[0-9a-f]{32}", relative):
        raise CatalogueError("invalid historical export work directory")
    root = store.path(relative)
    if root.exists():
        _validate_work_tree(store, root)
    else:
        durable_mkdir(root.parent)
        root.mkdir()
        fsync_directory(root.parent)
    for name in ("output", "runs", "checkpoints", "schemas"):
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
    schema = _reference_schema(store, connection, root, name, table)
    key_indices, key_types = _ordering_columns(schema, keys)
    schema_sha256 = digest(schema.serialize().to_pybytes())
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
        generation = _spool_runs(
            store,
            connection,
            run_directory,
            table,
            schema,
            key_indices,
        )
        checkpoint = {
            **expected_state,
            **generation,
            "generation": 0,
        }
        store.write(checkpoint_relative, checkpoint)

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
) -> pa.Schema:
    relative = f"{root.relative_to(store.root).as_posix()}/schemas/{name}.parquet"
    path = store.path(relative)
    partial = store.path(relative + ".partial")
    _remove_owned_file(store, path)
    _remove_owned_file(store, partial)
    store.reserve_storage(_MAX_SCHEMA_BYTES)
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
        return schema
    except duckdb.Error as exc:
        raise CatalogueError("could not read historical relation schema") from exc
    finally:
        try:
            _remove_owned_file(store, partial)
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
        storage = data_type.storage_type if isinstance(data_type, pa.ExtensionType) else data_type
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


def _spool_runs(
    store: Store,
    connection: duckdb.DuckDBPyConnection,
    run_directory: Path,
    table: str,
    schema: pa.Schema,
    key_indices: tuple[int, ...],
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
    # shortcut: DuckDB caps this Arrow reader by rows, not bytes. Split each
    # fetched batch to 8 MiB runs immediately, but a wide 4096-row fetch (or a
    # single row) can exceed that target while it is being read. If production
    # row widths make this peak unsafe, replace the reader with byte-aware row
    # windows; the run and merge buffers are byte-bounded below.
    reader = connection.table(f"main.{table}").to_arrow_reader(_BATCH_ROWS)
    try:
        with manifest_partial.open("wb") as manifest:
            for batch in reader:
                store.check_deadline()
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
                        data_type = (
                            field.type.storage_type
                            if isinstance(field.type, pa.ExtensionType)
                            else field.type
                        )
                        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
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
                    entry = {"name": name, "sha256": checksum, "size": size, "rows": count}
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
        reader.close()
        fsync_file(manifest_partial)
        os.replace(manifest_partial, manifest_path)
        fsync_directory(generation)
        manifest_relative = manifest_path.relative_to(store.root).as_posix()
        store.sync(manifest_partial.relative_to(store.root).as_posix())
        store.sync_file(manifest_relative)
    except BaseException:
        reader.close()
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
        # Runs are written to the byte target, apart from a single oversized row.
        self.batches = iter(self.parquet.iter_batches(batch_size=_BATCH_ROWS, use_threads=False))
        self.batch: pa.RecordBatch | None = None
        self.batch_id = 0
        self.position = 0

    def next_batch(self) -> pa.RecordBatch | None:
        try:
            self.batch = next(self.batches)
        except StopIteration:
            self.batch = None
            return None
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
        row_bytes = batch.slice(position, 1).nbytes
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
    allowed = {"output", "runs", "checkpoints", "schemas", "duckdb-spill"}
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
        else:
            _validate_safe_tree(store, child)
    store.check_storage()


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
