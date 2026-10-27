from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from kalshi_events import warehouse
from kalshi_events.common import CatalogueError, Store


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "store")


def _source_path(store: Store, relative: str, *, legacy: bool) -> Path:
    path = store.path(relative)
    path.parent.mkdir(parents=True)
    config = {"storage_compatibility_version": "v1.0.0"} if legacy else None
    connection_context = (
        duckdb.connect(str(path), config=config)
        if config is not None
        else duckdb.connect(str(path))
    )
    with connection_context as connection:
        connection.execute("create table payload (id integer, value varchar)")
        connection.execute("insert into payload values (1, 'native payload')")
        connection.execute("checkpoint")
    return path


def _copy(store: Store, source: Path, staging: Path) -> Path:
    staging.mkdir(parents=True)
    source_relative = str(source.relative_to(store.root))
    warehouse._copy_closed_database(
        store,
        source_relative,
        staging,
        expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    return staging / "warehouse.duckdb"


def _storage_version(path: Path) -> str:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(
            "select tags['storage_version'] from duckdb_databases() where not internal"
        ).fetchone()[0]


def test_native_database_keeps_certified_byte_copy_fast_path(tmp_path):
    store = _store(tmp_path)
    source = _source_path(store, "builds-v2/baseline's/warehouse.duckdb", legacy=False)
    original = source.read_bytes()

    copied = _copy(store, source, store.path("tmp/stage's"))

    assert copied.read_bytes() == original
    assert _storage_version(copied) == "v2.0.0+"
    assert not source.with_name(source.name + ".wal").exists()
    assert not copied.with_name(copied.name + ".wal").exists()


def _legacy_catalogue(path: Path) -> str:
    with duckdb.connect(
        str(path), config={"storage_compatibility_version": "v1.0.0"}
    ) as connection:
        database = connection.execute(
            "select database_name from duckdb_databases() where not internal"
        ).fetchone()[0]
        assert database == "warehouse"
        connection.execute('create schema "odd schema"')
        connection.execute("create type \"odd schema\".rating as enum ('low', 'high')")
        connection.execute("create sequence ids start 10")
        connection.execute(
            'create table "odd schema"."odd""table" '
            "(id integer primary key, label varchar unique, "
            'rating "odd schema".rating not null, '
            "seq_id integer default nextval('ids'), check (id > 0))"
        )
        connection.execute('create index items_label_idx on "odd schema"."odd""table" (label)')
        connection.execute(
            'comment on table "odd schema"."odd""table" is \'preserve the copied table comment\''
        )
        connection.execute(
            'comment on column "odd schema"."odd""table".label is '
            "'preserve the copied column comment'"
        )
        connection.execute(
            'insert into "odd schema"."odd""table" (id, label, rating) '
            "values (1, 'alpha', 'low'), (2, 'beta', 'high')"
        )
        connection.execute("create schema landing")
        connection.execute(
            'create table landing."_dlt_loads" '
            "(load_id varchar primary key, inserted_at timestamp, schema_name varchar)"
        )
        connection.execute(
            'insert into landing."_dlt_loads" values '
            "('load-1', timestamp '2026-10-27 12:00:00', 'landing')"
        )
        connection.execute(
            'create view "odd schema".self_view as '
            f'select id, label from {database}."odd schema"."odd""table"'
        )
        connection.execute(
            'create macro "odd schema".qualified_count(x) as '
            f'(select count(*) from {database}."odd schema"."odd""table") + x'
        )
        connection.execute(
            'create macro "odd schema".visible_items(min_id) as table '
            f'select id, label from {database}."odd schema"."odd""table" '
            "where id > min_id"
        )
        assert connection.execute("select nextval('ids')").fetchone() == (12,)
        connection.execute("checkpoint")
    return database


def test_legacy_database_converts_catalog_and_data_with_source_detached(tmp_path):
    store = _store(tmp_path)
    source = store.path("builds/old's/warehouse.duckdb")
    source.parent.mkdir(parents=True)
    _legacy_catalogue(source)
    before = hashlib.sha256(source.read_bytes()).hexdigest()

    copied = _copy(store, source, store.path("tmp/stage's"))

    assert _storage_version(copied) == "v2.0.0+"
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert not source.with_name(source.name + ".wal").exists()
    assert not copied.with_name(copied.name + ".wal").exists()
    with duckdb.connect(str(copied), read_only=True) as connection:
        assert connection.execute(
            'select * from "odd schema"."odd""table" order by id'
        ).fetchall() == [(1, "alpha", "low", 10), (2, "beta", "high", 11)]
        assert connection.execute(
            'select * from "odd schema".self_view order by id'
        ).fetchall() == [(1, "alpha"), (2, "beta")]
        assert connection.execute('select "odd schema".qualified_count(4)').fetchone() == (6,)
        assert connection.execute('select * from "odd schema".visible_items(1)').fetchall() == [
            (2, "beta")
        ]
        assert connection.execute('select * from landing."_dlt_loads"').fetchall() == [
            ("load-1", datetime(2026, 10, 27, 12, 0), "landing")
        ]
        assert connection.execute(
            "select count(*) from duckdb_indexes() where index_name = 'items_label_idx'"
        ).fetchone() == (1,)
        constraints = connection.execute(
            "select constraint_type from duckdb_constraints() "
            "where table_name = 'odd\"table' order by constraint_index"
        ).fetchall()
        assert {row[0] for row in constraints} >= {"PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK"}
        assert connection.execute(
            "select labels from duckdb_types() "
            "where schema_name = 'odd schema' and type_name = 'rating'"
        ).fetchone() == (["low", "high"],)
        assert connection.execute(
            "select comment from duckdb_tables() where table_name = 'odd\"table'"
        ).fetchone() == ("preserve the copied table comment",)
        assert connection.execute(
            "select comment from duckdb_columns() "
            "where table_name = 'odd\"table' and column_name = 'label'"
        ).fetchone() == ("preserve the copied column comment",)
        assert connection.execute(
            "select last_value from duckdb_sequences() where sequence_name = 'ids'"
        ).fetchone() == (12,)
    with duckdb.connect(str(copied)) as connection:
        assert connection.execute("select nextval('ids')").fetchone() == (13,)
        connection.execute("checkpoint")
    assert not copied.with_name(copied.name + ".wal").exists()


@pytest.mark.parametrize("object_type", ("view", "macro", "table_macro"))
def test_conversion_rejects_unresolved_external_catalog_references(tmp_path, object_type):
    store = _store(tmp_path)
    external = store.path("tmp/external.duckdb")
    external.parent.mkdir(parents=True)
    with duckdb.connect(str(external)) as connection:
        connection.execute("create table items (id integer)")
        connection.execute("insert into items values (1)")
        connection.execute("checkpoint")

    source = store.path("builds/warehouse.duckdb")
    source.parent.mkdir(parents=True)
    with duckdb.connect(
        str(source), config={"storage_compatibility_version": "v1.0.0"}
    ) as connection:
        connection.execute(f"attach '{external}' as external (read_only)")
        if object_type == "view":
            connection.execute("create view external_view as select id from external.main.items")
        elif object_type == "macro":
            connection.execute(
                "create macro external_count(x) as (select count(*) from external.main.items) + x"
            )
        else:
            connection.execute(
                "create macro external_items(min_id) as table "
                "select id from external.main.items where id > min_id"
            )
        connection.execute("detach external")
        connection.execute("checkpoint")
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    staging = store.path("tmp/failed-stage")
    staging.mkdir()

    with pytest.raises(CatalogueError):
        warehouse._copy_closed_database(
            store,
            "builds/warehouse.duckdb",
            staging,
            expected_sha256=before,
        )

    assert not (staging / "warehouse.duckdb").exists()
    assert not (staging / "warehouse.duckdb.wal").exists()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert not source.with_name(source.name + ".wal").exists()
