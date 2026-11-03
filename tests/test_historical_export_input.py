from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from kalshi_events import historical_export
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store, digest


class _ReaderProbe:
    def __init__(self, reader, record_batch):
        self._reader = reader
        self._record_batch = record_batch

    def __iter__(self):
        return self

    def __next__(self):
        batch = next(self._reader)
        self._record_batch(batch)
        return batch

    def close(self):
        return self._reader.close()


class _RelationProbe:
    def __init__(self, relation, record_batch):
        self._relation = relation
        self._record_batch = record_batch

    def to_arrow_reader(self, *args, **kwargs):
        reader = self._relation.to_arrow_reader(*args, **kwargs)
        return _ReaderProbe(reader, self._record_batch)

    def fetch_record_batch(self, *args, **kwargs):
        reader = self._relation.fetch_record_batch(*args, **kwargs)
        return _ReaderProbe(reader, self._record_batch)

    def arrow(self, *args, **kwargs):
        reader = self._relation.arrow(*args, **kwargs)
        return _ReaderProbe(reader, self._record_batch)

    def fetch_arrow_table(self, *args, **kwargs):
        table = self._relation.fetch_arrow_table(*args, **kwargs)
        for batch in table.to_batches(max_chunksize=max(1, table.num_rows)):
            self._record_batch(batch)
        return table

    def __getattr__(self, name):
        return getattr(self._relation, name)


class _BatchRecorder:
    def __init__(self, schema):
        self.schema = schema
        self.payload_batches = []

    def record(self, batch):
        if batch.schema.names == self.schema.names:
            self.payload_batches.append(
                (
                    batch.num_rows,
                    batch.nbytes,
                    max(len(value.as_py() or "") for value in batch.column(2)),
                )
            )


class _ConnectionProbe:
    """Observe source and scratch-DB readers before exporter splitting/casting."""

    def __init__(self, connection, recorder):
        self._connection = connection
        self._recorder = recorder

    @property
    def payload_batches(self):
        return self._recorder.payload_batches

    def execute(self, *args, **kwargs):
        self._connection.execute(*args, **kwargs)
        return self

    def fetch_record_batch(self, *args, **kwargs):
        reader = self._connection.fetch_record_batch(*args, **kwargs)
        return _ReaderProbe(reader, self._recorder.record)

    def arrow(self, *args, **kwargs):
        reader = self._connection.arrow(*args, **kwargs)
        return _ReaderProbe(reader, self._recorder.record)

    def fetch_arrow_table(self, *args, **kwargs):
        table = self._connection.fetch_arrow_table(*args, **kwargs)
        for batch in table.to_batches(max_chunksize=max(1, table.num_rows)):
            self._recorder.record(batch)
        return table

    def table(self, *args, **kwargs):
        relation = self._connection.table(*args, **kwargs)
        return _RelationProbe(relation, self._recorder.record)

    def sql(self, *args, **kwargs):
        relation = self._connection.sql(*args, **kwargs)
        return _RelationProbe(relation, self._recorder.record)

    query = sql

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self._connection.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._connection, name)


def _export(tmp_path, rows, monkeypatch):
    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute("create table source_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.executemany("insert into source_rows values (?, ?, ?)", rows)
    reference = tmp_path / "reference.parquet"
    connection.execute(
        "copy (select * from main.source_rows limit 0) to ? "
        f"(format parquet, {historical_export.COPY_OPTIONS})",
        [str(reference)],
    ).fetchall()
    schema = pq.read_schema(reference)
    reference.unlink()

    store = Store(tmp_path / "root")
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    recorder = _BatchRecorder(schema)
    real_connect = duckdb.connect

    def recording_connect(*args, **kwargs):
        return _ConnectionProbe(real_connect(*args, **kwargs), recorder)

    monkeypatch.setattr(duckdb, "connect", recording_connect)
    probe = _ConnectionProbe(connection, recorder)
    try:
        result = historical_export.export_relation(
            store,
            probe,
            work_relative=work_relative,
            binding_sha256="c" * 64,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        output = store.path(f"{work_relative}/output/source_rows.parquet")
        return connection, store, work_relative, result, pq.read_table(output), probe
    except BaseException:
        connection.close()
        raise


def _assert_payload_prefetch_bound(probe, target_bytes):
    assert probe.payload_batches
    assert all(
        byte_count <= target_bytes or row_count == 1
        for row_count, byte_count, _ in probe.payload_batches
    ), probe.payload_batches


def _materialized_input(tmp_path, rows=None):
    rows = rows or [("second", 2, "value-2"), ("first", 1, "value-1")]
    connection = duckdb.connect(":memory:")
    connection.execute("create table source_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.executemany("insert into source_rows values (?, ?, ?)", rows)
    reference = tmp_path / "input-reference.parquet"
    connection.execute(
        "copy (select * from main.source_rows limit 0) to ? "
        f"(format parquet, {historical_export.COPY_OPTIONS})",
        [str(reference)],
    ).fetchall()
    schema = pq.read_schema(reference)
    reference.unlink()
    schema_sha256 = digest(schema.serialize().to_pybytes())
    store = Store(tmp_path / "input-root")
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    input_path = historical_export._materialize_input(
        store,
        connection,
        work_relative,
        binding_sha256,
        "source_rows",
        "source_rows",
        schema,
        schema_sha256,
    )
    return connection, store, work_relative, input_path, rows, schema, binding_sha256, schema_sha256


def _validate_input(store, input_path, binding_sha256, schema_sha256):
    return historical_export._validate_input_tree(
        store,
        input_path.parent,
        "source_rows",
        binding_sha256,
        "source_rows",
        schema_sha256,
    )


def test_wide_initial_reader_respects_byte_bound_at_prefetch_boundary(tmp_path, monkeypatch):
    rows = [
        (f"key-{index:04d}", index, "x" * 4096)
        for index in reversed(range(historical_export._BATCH_ROWS))
    ]
    connection, _, _, result, output, probe = _export(tmp_path, rows, monkeypatch)
    try:
        _assert_payload_prefetch_bound(probe, historical_export._BATCH_BYTES)
        assert result["row_count"] == len(rows)
        assert output["ticker"].to_pylist() == sorted(row[0] for row in rows)
        assert output["note"].to_pylist() == ["x" * 4096] * len(rows)
    finally:
        connection.close()


def test_narrow_initial_reader_keeps_oversized_row_as_singleton(tmp_path, monkeypatch):
    rows = [
        (f"key-{index:04d}", index, "small")
        for index in reversed(range(historical_export._BATCH_ROWS))
    ]
    oversized_index = historical_export._BATCH_ROWS // 2
    oversized = "z" * (historical_export._BATCH_BYTES + 1)
    rows[historical_export._BATCH_ROWS - 1 - oversized_index] = (
        f"key-{oversized_index:04d}",
        oversized_index,
        oversized,
    )
    connection, _, _, result, output, probe = _export(tmp_path, rows, monkeypatch)
    try:
        _assert_payload_prefetch_bound(probe, historical_export._BATCH_BYTES)
        assert any(
            row_count == 1 and max_payload_chars > historical_export._BATCH_BYTES
            for row_count, _, max_payload_chars in probe.payload_batches
        )
        assert result["row_count"] == len(rows)
        assert output["ticker"].to_pylist() == sorted(row[0] for row in rows)
        rows_by_ticker = dict(
            zip(output["ticker"].to_pylist(), output["note"].to_pylist(), strict=True)
        )
        assert rows_by_ticker[f"key-{oversized_index:04d}"] == oversized
    finally:
        connection.close()


def test_materialized_input_has_exact_owner_inventory_and_dense_rowids(tmp_path):
    connection, store, _, input_path, rows, _, binding_sha256, schema_sha256 = _materialized_input(
        tmp_path
    )
    try:
        owner = json.loads((input_path.parent / "owner.json").read_text())
        input_sha256, input_size = historical_export.file_digest(
            input_path, store.limits.max_root_bytes
        )
        assert owner == {
            "version": 2,
            "state": "sealed",
            "binding_sha256": binding_sha256,
            "relation": "source_rows",
            "table": "source_rows",
            "schema_sha256": schema_sha256,
            "input_sha256": input_sha256,
            "input_size": input_size,
            "row_count": len(rows),
        }
        assert {path.name for path in input_path.parent.iterdir()} == {
            "owner.json",
            "input.duckdb",
        }
        for path in input_path.parent.iterdir():
            assert not path.is_symlink()
            assert path.stat().st_nlink == 1
            assert path.stat().st_dev == store.root.stat().st_dev

        _validate_input(store, input_path, binding_sha256, schema_sha256)
        with duckdb.connect(str(input_path), read_only=True) as scratch:
            copied = scratch.execute(
                "select ticker, ordinal, note from export_input order by rowid"
            ).fetchall()
            rowids = scratch.execute("select rowid from export_input order by rowid").fetchall()
        assert copied == [tuple(row) for row in rows]
        assert rowids == [(index,) for index in range(len(rows))]
    finally:
        connection.close()


def test_prefetch_window_boundary_splits_at_exactly_4096_rows(tmp_path):
    rows = [(f"key-{index:05d}", index, "x") for index in range(4097)]
    connection, _, _, input_path, _, schema, _, _ = _materialized_input(tmp_path, rows)
    try:
        with duckdb.connect(str(input_path), read_only=True) as scratch:
            windows = list(historical_export._prefetch_windows(scratch, "export_input", schema))
        assert [count for _, count, _ in windows] == [4096, 1]
        assert windows[0][0] == 0
        assert windows[1][0] == 4096
        assert all(
            size <= historical_export._BATCH_BYTES for _, count, size in windows if count > 1
        )
    finally:
        connection.close()


def test_input_storage_interruption_after_database_growth_is_recoverable(tmp_path, monkeypatch):
    connection = duckdb.connect(":memory:")
    connection.execute("create table source_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.executemany(
        "insert into source_rows values (?, ?, ?)",
        [(f"key-{index:04d}", index, "x" * 4096) for index in range(256)],
    )
    reference = tmp_path / "input-reference.parquet"
    connection.execute(
        "copy (select * from main.source_rows limit 0) to ? "
        f"(format parquet, {historical_export.COPY_OPTIONS})",
        [str(reference)],
    ).fetchall()
    schema = pq.read_schema(reference)
    reference.unlink()
    schema_sha256 = digest(schema.serialize().to_pybytes())
    store = Store(tmp_path / "root")
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    observed = []
    original_check = historical_export._check_input_storage

    def interrupt_after_growth(current_store, directory):
        size = original_check(current_store, directory)
        names = {entry.name for entry in directory.iterdir()}
        if size:
            observed.append(
                (
                    size,
                    names,
                    sum(
                        (directory / name).stat().st_size
                        for name in ("input.duckdb", "input.duckdb.wal")
                        if (directory / name).exists()
                    ),
                )
            )
            raise BudgetExceeded("simulated storage interruption during input growth")
        return size

    monkeypatch.setattr(historical_export, "_check_input_storage", interrupt_after_growth)
    try:
        with pytest.raises(BudgetExceeded, match="storage interruption"):
            historical_export._materialize_input(
                store,
                connection,
                work_relative,
                binding_sha256,
                "source_rows",
                "source_rows",
                schema,
                schema_sha256,
            )
        assert observed
        assert observed[0][0] > 0
        assert "input.duckdb" in observed[0][1]
        assert observed[0][1] <= {
            "owner.json",
            "input.duckdb",
            "input.duckdb.wal",
        }
        assert observed[0][0] == observed[0][2]

        monkeypatch.setattr(historical_export, "_check_input_storage", original_check)
        historical_export._discard_input(
            store,
            work_relative,
            "source_rows",
            binding_sha256,
            "source_rows",
            schema_sha256,
        )
        assert not (store.path(f"{work_relative}/inputs/source_rows")).exists()
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("identity", "wrong_value"),
    [
        ("relation", "other_relation"),
        ("binding_sha256", "d" * 64),
        ("table", "other_table"),
        ("schema_sha256", "e" * 64),
    ],
)
def test_materialized_input_rejects_wrong_owner_binding_or_schema(tmp_path, identity, wrong_value):
    connection, store, _, input_path, _, _, binding_sha256, schema_sha256 = _materialized_input(
        tmp_path
    )
    try:
        owner_path = input_path.parent / "owner.json"
        owner = json.loads(owner_path.read_text())
        owner[identity] = wrong_value
        owner_path.write_text(json.dumps(owner))
        with pytest.raises(CatalogueError):
            _validate_input(store, input_path, binding_sha256, schema_sha256)
    finally:
        connection.close()


@pytest.mark.parametrize("tamper", ["symlink", "hardlink", "foreign_file"])
def test_materialized_input_rejects_unsafe_or_foreign_files(tmp_path, tamper):
    connection, store, _, input_path, _, _, binding_sha256, schema_sha256 = _materialized_input(
        tmp_path
    )
    try:
        if tamper == "symlink":
            replacement = tmp_path / "outside.duckdb"
            replacement.write_bytes(input_path.read_bytes())
            input_path.unlink()
            input_path.symlink_to(replacement)
        elif tamper == "hardlink":
            os.link(input_path, tmp_path / "outside.duckdb")
        else:
            (input_path.parent / "foreign.bin").write_bytes(b"unowned")

        with pytest.raises(CatalogueError):
            _validate_input(store, input_path, binding_sha256, schema_sha256)
    finally:
        connection.close()


def test_input_cleanup_removes_owner_marker_last_then_syncs_directory(tmp_path, monkeypatch):
    connection, store, work_relative, input_path, _, _, binding_sha256, schema_sha256 = (
        _materialized_input(tmp_path)
    )
    input_directory = input_path.parent
    existing = {path.name for path in input_directory.iterdir()}
    events = []
    original_unlink = type(input_path).unlink
    original_sync = historical_export.fsync_directory

    def record_unlink(path, *args, **kwargs):
        if path.parent == input_directory:
            events.append(("unlink", path.name))
        return original_unlink(path, *args, **kwargs)

    def record_sync(path):
        path = type(input_path)(path)
        if path == input_directory:
            events.append(("input_fsync", tuple(sorted(child.name for child in path.iterdir()))))
        elif path == input_directory.parent:
            events.append(("parent_fsync", ()))
        return original_sync(path)

    monkeypatch.setattr(type(input_path), "unlink", record_unlink)
    monkeypatch.setattr(historical_export, "fsync_directory", record_sync)
    try:
        historical_export._discard_input(
            store,
            work_relative,
            "source_rows",
            binding_sha256,
            "source_rows",
            schema_sha256,
        )
        expected_unlinks = [
            name for name in ("input.duckdb.wal", "input.duckdb", "owner.json") if name in existing
        ]
        assert [name for event, name in events if event == "unlink"] == expected_unlinks
        assert events.index(("input_fsync", ())) > events.index(("unlink", "owner.json"))
        assert events[-1] == ("parent_fsync", ())
        assert not input_directory.exists()
    finally:
        connection.close()


def test_partial_input_cleanup_retries_without_losing_sealed_export(tmp_path, monkeypatch):
    rows = [("key-2", 2, "value-2"), ("key-1", 1, "value-1")]
    connection, store, work_relative, result, _, _ = _export(tmp_path, rows, monkeypatch)
    output_path = store.path(f"{work_relative}/output/source_rows.parquet")
    output_digest = digest(output_path.read_bytes())
    checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
    schema = pq.read_schema(output_path)
    schema_sha256 = digest(schema.serialize().to_pybytes())
    binding_sha256 = "c" * 64
    input_path = historical_export._materialize_input(
        store,
        connection,
        work_relative,
        binding_sha256,
        "source_rows",
        "source_rows",
        schema,
        schema_sha256,
    )
    input_directory = input_path.parent
    original_rmdir = type(input_path).rmdir
    injected = False

    def fail_after_files_are_removed(path):
        nonlocal injected
        if path == input_directory and not injected:
            injected = True
            raise OSError("simulated interruption after input files were removed")
        return original_rmdir(path)

    monkeypatch.setattr(type(input_path), "rmdir", fail_after_files_are_removed)
    try:
        with pytest.raises(OSError, match="simulated interruption"):
            historical_export.export_relation(
                store,
                _ConnectionProbe(connection, _BatchRecorder(schema)),
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        assert injected
        assert checkpoint_path.is_file()
        assert input_directory.is_dir() and list(input_directory.iterdir()) == []
        assert digest(output_path.read_bytes()) == output_digest

        monkeypatch.setattr(type(input_path), "rmdir", original_rmdir)
        retry = historical_export.export_relation(
            store,
            _ConnectionProbe(connection, _BatchRecorder(schema)),
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert retry == result
        assert digest(output_path.read_bytes()) == output_digest
        assert not input_directory.exists()
    finally:
        connection.close()


def test_complete_frontier_survives_interruption_before_input_cleanup(tmp_path, monkeypatch):
    rows = [(f"key-{index:03d}", index, f"value-{index}") for index in range(6)]
    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute("create table source_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.executemany("insert into source_rows values (?, ?, ?)", rows)
    store = Store(tmp_path / "root")
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
    original_materialize = historical_export._materialize_input
    original_discard = historical_export._discard_input
    materializations = 0
    interrupted = False

    def count_materialization(*args, **kwargs):
        nonlocal materializations
        materializations += 1
        return original_materialize(*args, **kwargs)

    def interrupt_after_frontier_seal(*args, **kwargs):
        nonlocal interrupted
        if checkpoint_path.is_file() and not interrupted:
            checkpoint = json.loads(checkpoint_path.read_text())
            if (
                checkpoint.get("next_rowid") == len(rows)
                and checkpoint.get("complete") is False
                and checkpoint.get("pending_write") is None
            ):
                interrupted = True
                assert checkpoint["row_count"] == len(rows)
                raise RuntimeError("simulated stop after frontier seal")
        return original_discard(*args, **kwargs)

    monkeypatch.setattr(historical_export, "_materialize_input", count_materialization)
    monkeypatch.setattr(historical_export, "_discard_input", interrupt_after_frontier_seal)
    try:
        with pytest.raises(RuntimeError, match="frontier seal"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        assert interrupted and materializations == 1
        sealed_frontier = json.loads(checkpoint_path.read_text())
        assert sealed_frontier["complete"] is False
        assert sealed_frontier["next_rowid"] == len(rows)
        assert sealed_frontier["pending_write"] is None

        monkeypatch.setattr(historical_export, "_discard_input", original_discard)
        retry = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert materializations == 1
        assert retry["complete"] is True
        assert retry["row_count"] == len(rows)
        assert not store.path(f"{work_relative}/inputs/source_rows").exists()
    finally:
        connection.close()


@pytest.mark.timeout(60)
def test_native_kill_during_scratch_insert_recovers_without_losing_sealed_checkpoint(
    tmp_path, request
):
    source_database = tmp_path / "synthetic-source.duckdb"
    builder = duckdb.connect(str(source_database))
    hashes = " || ".join(f"md5(cast(i as varchar) || ':{index}')" for index in range(16))
    builder.execute(
        "create view main.source_rows as "
        "select printf('key-%08d', i) as ticker, i::bigint as ordinal, "
        f"{hashes} as note from range(100000) as source(i)"
    )
    builder.execute(
        "create view main.sealed_rows as "
        "select 'sealed-1'::varchar as ticker, 1::bigint as ordinal, 'one'::varchar as note "
        "union all "
        "select 'sealed-2'::varchar, 2::bigint, 'two'::varchar"
    )
    builder.close()

    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute(
        f"attach '{str(source_database).replace(chr(39), chr(39) * 2)}' as source (read_only)"
    )
    connection.execute("use source")
    request.addfinalizer(connection.close)
    root = tmp_path / "root"
    store = Store(root)
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32,
        "b" * 64,
        {},
        {},
        {},
        {"sealed_rows": "ticker, ordinal", "source_rows": "ticker, ordinal"},
    )
    historical_export.prepare(store, work_relative)
    sealed = historical_export.export_relation(
        store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="sealed_rows",
        table="sealed_rows",
        keys=("ticker", "ordinal"),
    )
    sealed_checkpoint_path = store.path(f"{work_relative}/checkpoints/sealed_rows.json")
    sealed_checkpoint = json.loads(sealed_checkpoint_path.read_text())
    sealed_output = store.path(f"{work_relative}/output/sealed_rows.parquet")
    sealed_output_sha256 = digest(sealed_output.read_bytes())

    schema = historical_export._reference_schema(
        store,
        connection,
        store.path(work_relative),
        "source_rows",
        "source_rows",
    )
    durable_schema_path = store.path(f"{work_relative}/schemas/source_rows.parquet")
    assert durable_schema_path.is_file()
    assert pq.read_schema(durable_schema_path).equals(schema, check_metadata=True)
    schema_path = tmp_path / "source-schema.arrow"
    schema_path.write_bytes(schema.serialize().to_pybytes())
    schema_sha256 = digest(schema.serialize().to_pybytes())
    start_marker = tmp_path / "insert-start.json"
    done_marker = tmp_path / "insert-done"
    input_directory = store.path(f"{work_relative}/inputs/source_rows")
    input_database = input_directory / "input.duckdb"
    input_wal = input_directory / "input.duckdb.wal"
    child_code = r"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))
import duckdb
import pyarrow as pa
from kalshi_events.common import Store
from kalshi_events.historical_export import _materialize_input

source_path, root_path, work_relative, binding, schema_path, schema_hash, start_path, done_path = sys.argv[1:]
schema = pa.ipc.read_schema(pa.BufferReader(Path(schema_path).read_bytes()))
source = duckdb.connect(
    ":memory:",
    config={"threads": "1", "disabled_optimizers": "statistics_propagation", "preserve_insertion_order": "false"},
)
source.execute(
    "attach '" + source_path.replace("'", "''") + "' as source (read_only)"
)
source.execute("use source")

class InsertSignal:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, *args, **kwargs):
        if sql.lstrip().upper().startswith("INSERT INTO HISTORICAL_INPUT.MAIN.EXPORT_INPUT"):
            input_directory = Path(root_path) / work_relative / "inputs" / "source_rows"
            names = ("input.duckdb", "input.duckdb.wal")
            baseline = sum(
                (input_directory / name).stat().st_size
                for name in names
                if (input_directory / name).exists()
            )
            marker_tmp = Path(start_path + ".partial")
            marker_tmp.write_text(json.dumps({"database_bytes_before_insert": baseline}))
            os.replace(marker_tmp, start_path)
            result = self.connection.execute(sql, *args, **kwargs)
            Path(done_path).write_text("done")
            return self
        self.connection.execute(sql, *args, **kwargs)
        return self

    def __getattr__(self, name):
        return getattr(self.connection, name)

store = Store(Path(root_path))
try:
    _materialize_input(
        store, InsertSignal(source), work_relative, binding,
        "source_rows", "source_rows", schema, schema_hash,
    )
finally:
    source.close()
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_code,
            str(source_database),
            str(root),
            work_relative,
            binding_sha256,
            str(schema_path),
            schema_sha256,
            str(start_marker),
            str(done_marker),
        ],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    killed_while_inserting = False
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=1)
                pytest.fail(f"scratch writer exited before interruption: {stdout}\n{stderr}")
            if start_marker.is_file():
                baseline = json.loads(start_marker.read_text())["database_bytes_before_insert"]
                current_bytes = 0
                for path in (input_database, input_wal):
                    try:
                        info = path.lstat()
                    except FileNotFoundError:
                        continue
                    assert path.is_file() and not path.is_symlink()
                    current_bytes += info.st_size
                if current_bytes >= baseline + 512 * 1024:
                    assert not done_marker.exists()
                    assert process.poll() is None
                    process.terminate()
                    process.wait(timeout=5)
                    killed_while_inserting = not done_marker.exists()
                    break
            time.sleep(0.01)
        assert start_marker.exists(), "child never reached the scratch-table INSERT"
        assert killed_while_inserting, "scratch insert completed before its database grew"
        assert process.returncode is not None and process.returncode < 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        process.communicate(timeout=5)

    store.reconcile()
    assert input_directory.is_dir()
    assert (input_directory / "owner.json").is_file()
    assert input_database.is_file() or input_wal.is_file()
    for path in (input_database, input_wal):
        if path.exists():
            info = path.lstat()
            assert path.is_file() and not path.is_symlink()
            assert info.st_nlink == 1
            assert info.st_dev == root.stat().st_dev
    retry = historical_export.export_relation(
        store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="source_rows",
        table="source_rows",
        keys=("ticker", "ordinal"),
    )
    assert retry["complete"] is True
    assert retry["row_count"] == 100000
    assert not input_directory.exists()
    exported_path = store.path(f"{work_relative}/output/source_rows.parquet")
    exported = pq.ParquetFile(exported_path)
    assert exported.schema_arrow.equals(schema, check_metadata=True)
    assert pq.read_table(exported_path, columns=["ordinal"])["ordinal"].to_pylist() == list(
        range(100000)
    )
    assert json.loads(sealed_checkpoint_path.read_text()) == sealed_checkpoint
    assert sealed["complete"] is True
    assert digest(sealed_output.read_bytes()) == sealed_output_sha256


@pytest.mark.timeout(20)
def test_marker_write_temp_is_reaped_after_native_kill_before_owner_rename(tmp_path, request):
    source_database = tmp_path / "marker-temp-source.duckdb"
    builder = duckdb.connect(str(source_database))
    builder.execute(
        "create view main.source_rows as "
        "select 'source-1'::varchar as ticker, 1::bigint as ordinal, 'one'::varchar as note "
        "union all "
        "select 'source-2'::varchar, 2::bigint, 'two'::varchar"
    )
    builder.execute(
        "create view main.sealed_rows as "
        "select 'sealed-1'::varchar as ticker, 1::bigint as ordinal, 'safe'::varchar as note"
    )
    builder.close()

    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute(
        f"attach '{str(source_database).replace(chr(39), chr(39) * 2)}' as source (read_only)"
    )
    connection.execute("use source")
    request.addfinalizer(connection.close)
    root = tmp_path / "root"
    store = Store(root)
    binding_sha256 = "c" * 64
    tables = {"safe_rows": "sealed_rows", "source_rows": "source_rows"}
    _, work_relative = historical_export.binding(
        "a" * 32,
        "b" * 64,
        {},
        {},
        {},
        {"safe_rows": "ticker, ordinal", "source_rows": "ticker, ordinal"},
    )
    historical_export.prepare(store, work_relative, binding_sha256=binding_sha256, tables=tables)
    sealed = historical_export.export_relation(
        store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="safe_rows",
        table="sealed_rows",
        keys=("ticker", "ordinal"),
    )
    sealed_checkpoint_path = store.path(f"{work_relative}/checkpoints/safe_rows.json")
    sealed_checkpoint = json.loads(sealed_checkpoint_path.read_text())
    sealed_output = store.path(f"{work_relative}/output/safe_rows.parquet")
    sealed_output_sha256 = digest(sealed_output.read_bytes())
    schema = pq.read_schema(sealed_output)
    schema_path = tmp_path / "marker-temp-schema.arrow"
    schema_path.write_bytes(schema.serialize().to_pybytes())
    schema_sha256 = digest(schema.serialize().to_pybytes())
    input_directory = store.path(f"{work_relative}/inputs/source_rows")
    signal_path = tmp_path / "owner-temp-ready.json"
    child_code = r"""
import json
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))
import duckdb
from kalshi_events import common
from kalshi_events.common import Store
from kalshi_events.historical_export import _materialize_input

source_path, root_path, work_relative, binding, schema_path, schema_hash, signal_path = sys.argv[1:]
schema = __import__("pyarrow").ipc.read_schema(
    __import__("pyarrow").BufferReader(Path(schema_path).read_bytes())
)
source = duckdb.connect(":memory:", config={"threads": "1"})
source.execute("attach '" + source_path.replace("'", "''") + "' as source (read_only)")
source.execute("use source")
store = Store(Path(root_path))
target_owner = Path(root_path) / work_relative / "inputs" / "source_rows" / "owner.json"
original_replace = common.os.replace

def pause_before_owner_rename(source_path, destination_path):
    if Path(destination_path) == target_owner:
        temp_path = Path(source_path)
        info = temp_path.lstat()
        signal_tmp = Path(signal_path + ".partial")
        signal_tmp.write_text(json.dumps({"temp_path": str(temp_path), "size": info.st_size}))
        original_replace(signal_tmp, signal_path)
        signal.pause()
    return original_replace(source_path, destination_path)

common.os.replace = pause_before_owner_rename
try:
    _materialize_input(
        store, source, work_relative, binding,
        "source_rows", "source_rows", schema, schema_hash,
    )
finally:
    source.close()
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_code,
            str(source_database),
            str(root),
            work_relative,
            binding_sha256,
            str(schema_path),
            schema_sha256,
            str(signal_path),
        ],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not signal_path.is_file():
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=1)
                pytest.fail(f"marker writer exited before tempfile pause: {stdout}\n{stderr}")
            time.sleep(0.01)
        assert signal_path.is_file(), "child never paused before owner-marker rename"
        marker = json.loads(signal_path.read_text())
        marker_temp = Path(marker["temp_path"])
        assert marker_temp.parent == input_directory
        assert re.fullmatch(r"\.write-[a-z0-9_]{8}", marker_temp.name)
        assert marker_temp.stat().st_size == marker["size"] > 0
        assert marker_temp.stat().st_nlink == 1
        assert marker_temp.stat().st_dev == root.stat().st_dev
        assert {path.name for path in input_directory.iterdir()} == {marker_temp.name}
        process.terminate()
        process.wait(timeout=5)
        assert process.returncode is not None and process.returncode < 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        process.communicate(timeout=5)

    fresh_store = Store(root)
    historical_export.prepare(
        fresh_store,
        work_relative,
        binding_sha256=binding_sha256,
        tables=tables,
    )
    assert not input_directory.exists()
    reused = historical_export.export_relation(
        fresh_store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="safe_rows",
        table="sealed_rows",
        keys=("ticker", "ordinal"),
    )
    assert reused == sealed
    assert json.loads(sealed_checkpoint_path.read_text()) == sealed_checkpoint
    assert digest(sealed_output.read_bytes()) == sealed_output_sha256
    retried = historical_export.export_relation(
        fresh_store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="source_rows",
        table="source_rows",
        keys=("ticker", "ordinal"),
    )
    assert retried["complete"] is True and retried["row_count"] == 2


@pytest.mark.parametrize("unowned_entry", ["foreign_file", "input_database", "symlink", "hardlink"])
def test_input_prepare_rejects_unsafe_non_temp_data_without_owner(tmp_path, unowned_entry):
    root = tmp_path / "root"
    store = Store(root)
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    input_directory = store.path(f"{work_relative}/inputs/source_rows")
    input_directory.mkdir()
    if unowned_entry == "foreign_file":
        (input_directory / "foreign.bin").write_bytes(b"unowned")
    elif unowned_entry == "input_database":
        (input_directory / "input.duckdb").write_bytes(b"unowned data")
    else:
        target = tmp_path / "outside.bin"
        target.write_bytes(b"unowned")
        entry = input_directory / "foreign.bin"
        if unowned_entry == "symlink":
            entry.symlink_to(target)
        else:
            os.link(target, entry)

    with pytest.raises(CatalogueError):
        fresh_store = Store(root)
        historical_export.prepare(
            fresh_store,
            work_relative,
            binding_sha256=binding_sha256,
            tables={"source_rows": "source_rows"},
        )


def test_overquota_owned_input_is_removed_before_export_and_remove_work_admission(
    tmp_path, monkeypatch
):
    rows = [("key-2", 2, "value-2"), ("key-1", 1, "value-1")]
    connection, store, work_relative, sealed, _, _ = _export(tmp_path, rows, monkeypatch)
    binding_sha256 = "c" * 64
    output_path = store.path(f"{work_relative}/output/source_rows.parquet")
    output_sha256 = digest(output_path.read_bytes())
    checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
    checkpoint = json.loads(checkpoint_path.read_text())
    schema = pq.read_schema(output_path)
    schema_sha256 = digest(schema.serialize().to_pybytes())
    max_database_bytes = 10 * 1024**2

    def create_overquota_input():
        input_path = historical_export._materialize_input(
            store,
            connection,
            work_relative,
            binding_sha256,
            "source_rows",
            "source_rows",
            schema,
            schema_sha256,
        )
        with duckdb.connect(str(input_path)) as scratch:
            scratch.execute("create table quota_padding (payload blob)")
            scratch.execute("insert into quota_padding values (?)", [os.urandom(12 * 1024**2)])
            scratch.execute("checkpoint")
        store.sync(f"{work_relative}/inputs/source_rows")
        assert input_path.stat().st_size > max_database_bytes
        return input_path

    input_path = create_overquota_input()
    constrained_store = Store(store.root, limits=Limits(max_database_bytes=max_database_bytes))
    historical_export.prepare(
        constrained_store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    assert not input_path.parent.exists()
    reused = historical_export.export_relation(
        constrained_store,
        _ConnectionProbe(connection, _BatchRecorder(schema)),
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="source_rows",
        table="source_rows",
        keys=("ticker", "ordinal"),
    )
    assert reused == sealed
    assert json.loads(checkpoint_path.read_text()) == checkpoint
    assert digest(output_path.read_bytes()) == output_sha256

    create_overquota_input()
    remove_store = Store(store.root, limits=Limits(max_database_bytes=max_database_bytes))
    historical_export.remove_work(remove_store, work_relative)
    assert not store.path(work_relative).exists()
    connection.close()


@pytest.mark.timeout(20)
def test_scratch_reader_deadline_interrupts_its_own_connection_and_preserves_seal(
    tmp_path, monkeypatch
):
    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute("create table source_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.execute("create table sealed_rows (ticker varchar, ordinal bigint, note varchar)")
    connection.executemany(
        "insert into source_rows values (?, ?, ?)",
        [(f"key-{index:03d}", index, f"value-{index}") for index in range(8)],
    )
    connection.execute("insert into sealed_rows values ('sealed', 1, 'ok')")
    request_store = Store(tmp_path / "root")
    binding_sha256 = "c" * 64
    _, work_relative = historical_export.binding(
        "a" * 32,
        "b" * 64,
        {},
        {},
        {},
        {"sealed_rows": "ticker, ordinal", "source_rows": "ticker, ordinal"},
    )
    historical_export.prepare(request_store, work_relative)
    sealed = historical_export.export_relation(
        request_store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="sealed_rows",
        table="sealed_rows",
        keys=("ticker", "ordinal"),
    )
    sealed_checkpoint_path = request_store.path(f"{work_relative}/checkpoints/sealed_rows.json")
    sealed_checkpoint = json.loads(sealed_checkpoint_path.read_text())
    sealed_output = request_store.path(f"{work_relative}/output/sealed_rows.parquet")
    sealed_output_sha256 = digest(sealed_output.read_bytes())
    original_deadline_context = historical_export.interrupt_at_deadline
    original_prefetch = historical_export._prefetch_windows
    scratch_contexts = []
    window_started = threading.Event()
    timer_fired = threading.Event()
    cancel_watch = threading.Event()

    @contextmanager
    def interrupt_scratch_reader(store, interrupt):
        connection_owner = interrupt.__self__
        database_list = connection_owner.execute("pragma database_list").fetchall()
        is_scratch = any(str(row[2]).endswith("/input.duckdb") for row in database_list)
        if not is_scratch:
            with original_deadline_context(store, interrupt):
                yield
            return

        scratch_contexts.append(connection_owner)
        timer_holder = []

        def interrupt_after_slow_scan_starts():
            if window_started.wait(5) and not cancel_watch.is_set():

                def interrupt_after_deadline():
                    timer_fired.set()
                    interrupt()

                timer = threading.Timer(0.05, interrupt_after_deadline)
                timer.daemon = True
                timer.start()
                timer_holder.append(timer)

        watcher = threading.Thread(target=interrupt_after_slow_scan_starts, daemon=True)
        watcher.start()
        try:
            yield
        finally:
            cancel_watch.set()
            watcher.join(timeout=5)
            for timer in timer_holder:
                timer.cancel()

    def slow_prefetch(connection, table, schema, **kwargs):
        assert scratch_contexts, "scratch reader has no deadline interrupt context"
        window_started.set()
        connection.execute(
            "select sum(length(md5(cast(i as varchar)))) from range(100000000) as scan(i)"
        ).fetchone()
        yield from original_prefetch(connection, table, schema, **kwargs)

    monkeypatch.setattr(historical_export, "interrupt_at_deadline", interrupt_scratch_reader)
    monkeypatch.setattr(historical_export, "_prefetch_windows", slow_prefetch)
    try:
        with pytest.raises(CatalogueError) as caught:
            historical_export.export_relation(
                request_store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        assert scratch_contexts
        assert window_started.is_set()
        assert timer_fired.is_set(), repr(caught.value.__cause__)
        assert json.loads(sealed_checkpoint_path.read_text()) == sealed_checkpoint
        assert digest(sealed_output.read_bytes()) == sealed_output_sha256
        assert sealed["complete"] is True
    finally:
        connection.close()


def test_payload_sizer_fails_closed_for_nested_arrow_types(tmp_path):
    connection = duckdb.connect(":memory:")
    connection.execute("create table source_rows (ticker varchar, values integer[])")
    connection.execute("insert into source_rows values ('key', [1, 2, 3])")
    store = Store(tmp_path / "root")
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker"}
    )
    historical_export.prepare(store, work_relative)

    try:
        with pytest.raises(CatalogueError, match="unsupported"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256="c" * 64,
                name="source_rows",
                table="source_rows",
                keys=("ticker",),
            )
        assert not store.path(f"{work_relative}/output/source_rows.parquet").exists()
    finally:
        connection.close()
