from __future__ import annotations

import hashlib
import time
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_export_ordering import catalogue as catalogue  # noqa: F401

from kalshi_events import publish
from kalshi_events.common import BudgetExceeded, Limits, Store


def _export_source(tmp_path: Path):
    store = Store(tmp_path / "root")
    relative = "builds/export/warehouse.duckdb"
    database = store.path(relative)
    database.parent.mkdir(parents=True)
    with duckdb.connect(str(database), config={}) as connection:
        connection.execute(
            """create table edge_cases (
                   order_key integer primary key,
                   ddl_not_null integer not null,
                   inferred_all_present integer,
                   mixed integer,
                   all_null integer,
                   amount decimal(38, 18),
                   payload varchar
               )"""
        )
        connection.executemany(
            "insert into edge_cases values (?, ?, ?, ?, ?, ?, ?)",
            [
                (3, 12, 102, 9, None, "0.100000000000000001", "wide界🌉"),
                (
                    1,
                    10,
                    100,
                    7,
                    None,
                    "12345678901234567890.123456789012345678",
                    "Unicode 🌉",
                ),
                (2, 11, 101, None, None, "-0.000000000000000001", 'café "quote"'),
            ],
        )
        connection.execute(
            """create table empty_cases (
                   order_key bigint not null,
                   amount decimal(38, 18) not null,
                   payload varchar not null
               )"""
        )
    store.sync(relative)
    temporary = store.path("tmp/export-test")
    temporary.mkdir(parents=True)
    return store, database, temporary


def _constraints(connection):
    return connection.execute(
        "select table_name, constraint_type, constraint_text, constraint_column_names "
        "from duckdb_constraints() where table_name in ('edge_cases', 'empty_cases') "
        "order by table_name, constraint_index"
    ).fetchall()


def _disabled_optimizers(connection):
    return connection.execute("select current_setting('disabled_optimizers')").fetchone()[0]


def _assert_spill_removed(temporary: Path) -> None:
    assert not (temporary / "duckdb-spill").exists()


def test_export_copy_keeps_all_parquet_fields_nullable_and_preserves_values(tmp_path):
    store, database, temporary = _export_source(tmp_path)
    output = tmp_path / "parquet"
    output.mkdir()

    with publish._export_connection(store, database, temporary) as connection:
        edge_file = output / "edge-cases.parquet"
        connection.execute(
            "copy (select * from main.edge_cases order by order_key) to ? (format parquet)",
            [str(edge_file)],
        )
        empty_file = output / "empty-cases.parquet"
        connection.execute(
            "copy (select * from main.empty_cases order by order_key) to ? (format parquet)",
            [str(empty_file)],
        )

    edge_schema = pq.read_schema(edge_file)
    assert edge_schema.names == [
        "order_key",
        "ddl_not_null",
        "inferred_all_present",
        "mixed",
        "all_null",
        "amount",
        "payload",
    ]
    assert all(field.nullable for field in edge_schema)
    assert edge_schema.field("order_key").type == pa.int32()
    assert edge_schema.field("amount").type == pa.decimal128(38, 18)
    assert edge_schema.field("payload").type == pa.string()
    assert pq.read_table(edge_file).to_pylist() == [
        {
            "order_key": 1,
            "ddl_not_null": 10,
            "inferred_all_present": 100,
            "mixed": 7,
            "all_null": None,
            "amount": Decimal("12345678901234567890.123456789012345678"),
            "payload": "Unicode 🌉",
        },
        {
            "order_key": 2,
            "ddl_not_null": 11,
            "inferred_all_present": 101,
            "mixed": None,
            "all_null": None,
            "amount": Decimal("-0.000000000000000001"),
            "payload": 'café "quote"',
        },
        {
            "order_key": 3,
            "ddl_not_null": 12,
            "inferred_all_present": 102,
            "mixed": 9,
            "all_null": None,
            "amount": Decimal("0.100000000000000001"),
            "payload": "wide界🌉",
        },
    ]

    empty_schema = pq.read_schema(empty_file)
    assert empty_schema.names == ["order_key", "amount", "payload"]
    assert all(field.nullable for field in empty_schema)
    assert empty_schema.field("order_key").type == pa.int64()
    assert empty_schema.field("amount").type == pa.decimal128(38, 18)
    assert empty_schema.field("payload").type == pa.string()
    assert pq.read_table(empty_file).num_rows == 0
    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


@pytest.mark.parametrize("fail", [False, True], ids=["success", "injected-failure"])
def test_export_keeps_existing_connections_and_source_unchanged(tmp_path, fail):
    store, database, temporary = _export_source(tmp_path)
    source_bytes = database.read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    source_mtime = database.stat().st_mtime_ns
    source_reader = duckdb.connect(str(database), read_only=True)
    default_connection = duckdb.connect()
    source_optimizers = _disabled_optimizers(source_reader)
    default_optimizers = _disabled_optimizers(default_connection)
    source_constraints = _constraints(source_reader)
    try:
        if fail:
            with (
                pytest.raises(RuntimeError, match="injected export failure"),
                publish._export_connection(store, database, temporary) as connection,
            ):
                assert connection.execute("select count(*) from main.edge_cases").fetchone() == (3,)
                assert _disabled_optimizers(source_reader) == source_optimizers
                assert _disabled_optimizers(default_connection) == default_optimizers
                raise RuntimeError("injected export failure")
        else:
            with publish._export_connection(store, database, temporary) as connection:
                assert connection.execute("select count(*) from main.edge_cases").fetchone() == (3,)
                assert _disabled_optimizers(source_reader) == source_optimizers
                assert _disabled_optimizers(default_connection) == default_optimizers

        assert source_reader.execute("select count(*) from edge_cases").fetchone() == (3,)
        assert default_connection.execute("select 1").fetchone() == (1,)
        assert _disabled_optimizers(source_reader) == source_optimizers
        assert _disabled_optimizers(default_connection) == default_optimizers
        assert _constraints(source_reader) == source_constraints
        assert database.read_bytes() == source_bytes
        assert hashlib.sha256(database.read_bytes()).hexdigest() == source_hash
        assert database.stat().st_mtime_ns == source_mtime
        assert not database.with_name(database.name + ".wal").exists()
        assert store._storage_reservations == 0
        _assert_spill_removed(temporary)
    finally:
        source_reader.close()
        default_connection.close()


def test_export_deadline_is_checked_before_attach(tmp_path):
    store, database, temporary = _export_source(tmp_path)
    store.limits = Limits(max_duration_seconds=5)
    store.started -= store.limits.max_duration_seconds + 1

    with (
        pytest.raises(BudgetExceeded, match="deadline"),
        publish._export_connection(store, database, temporary),
    ):
        pytest.fail("expired export opened a connection")

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_export_deadline_interrupts_copy_and_cleans_spill(tmp_path):
    store, database, temporary = _export_source(tmp_path)
    output = tmp_path / "slow-copy.parquet"
    store.limits = Limits(max_duration_seconds=0.25)
    store.started = time.monotonic()

    with (
        pytest.raises(BudgetExceeded, match="deadline"),
        publish._export_connection(store, database, temporary) as connection,
    ):
        connection.execute(
            "copy (select sum(sqrt(value)) as value from range(1000000000) numbers(value)) "
            "to ? (format parquet)",
            [str(output)],
        )

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_export_reservation_failure_preserves_existing_reservation(tmp_path, monkeypatch):
    store, database, temporary = _export_source(tmp_path)
    preexisting = 23
    store.reserve_storage(preexisting)
    reservations_before = store._storage_reservations

    def fail_reservation(_amount):
        raise BudgetExceeded("injected spill reservation failure")

    monkeypatch.setattr(store, "reserve_storage", fail_reservation)
    try:
        with (
            pytest.raises(BudgetExceeded, match="injected spill reservation failure"),
            publish._export_connection(store, database, temporary),
        ):
            pytest.fail("failed spill reservation opened a connection")
        assert store._storage_reservations == reservations_before
    finally:
        if store._storage_reservations == reservations_before:
            store.release_storage(preexisting)

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_export_mkdir_failure_releases_reservation(tmp_path, monkeypatch):
    store, database, temporary = _export_source(tmp_path)
    spill = temporary / "duckdb-spill"
    original_mkdir = Path.mkdir

    def fail_spill_mkdir(path, *args, **kwargs):
        if path == spill:
            raise OSError("injected spill mkdir failure")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_spill_mkdir)
    with (
        pytest.raises(OSError, match="spill mkdir failure"),
        publish._export_connection(store, database, temporary),
    ):
        pytest.fail("failed spill directory creation opened a connection")

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_export_connect_failure_cleans_spill_and_reservation(tmp_path, monkeypatch):
    store, database, temporary = _export_source(tmp_path)

    def fail_connect(*_args, **_kwargs):
        raise OSError("injected DuckDB connection failure")

    monkeypatch.setattr(publish.duckdb, "connect", fail_connect)
    with (
        pytest.raises(OSError, match="connection failure"),
        publish._export_connection(store, database, temporary),
    ):
        pytest.fail("failed DuckDB connection yielded")

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_export_sql_failure_cleans_spill_and_reservation(tmp_path):
    store, database, temporary = _export_source(tmp_path)

    with (
        pytest.raises(duckdb.CatalogException),
        publish._export_connection(store, database, temporary) as connection,
    ):
        connection.execute("select * from main.table_that_does_not_exist")

    assert store._storage_reservations == 0
    _assert_spill_removed(temporary)


def test_failed_publication_keeps_current_pointer(catalogue, monkeypatch):  # noqa: F811
    store, state, _release, _database, _market, _combo = catalogue
    pointer = store.path("current.json").read_bytes()

    def fail_export_sync(_path):
        raise OSError("injected export sync failure")

    monkeypatch.setattr(publish, "fsync_file", fail_export_sync)
    with pytest.raises(OSError, match="export sync failure"):
        publish.publish(store, state["capture_id"])

    assert store.path("current.json").read_bytes() == pointer
    assert not list(store.path("tmp").glob("publish-*"))
    assert store._storage_reservations == 0
