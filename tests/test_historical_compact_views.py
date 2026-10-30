from __future__ import annotations

import json
import shutil
from contextlib import contextmanager
from decimal import Decimal

import duckdb
import pyarrow.parquet as pq
from conftest import FakeClient

from kalshi_events import capture, publish, warehouse
from kalshi_events.common import Store

CAPTURE_ID = "c" * 32
_PUBLIC_RELATIONS = tuple(warehouse.RELATIONS.items())


def _market(title, event_ticker, *, last_price="0.1", legs=None):
    return {
        "ticker": title,
        "event_ticker": event_ticker,
        "status": "active",
        "result": "",
        "settlement_ts": None,
        "rules_primary": "primary rules",
        "rules_secondary": "secondary rules" if title != "MISSING" else None,
        "price_ranges": [{"start": "0", "end": "1", "step": "0.0001"}],
        "price_level_structure": "decimal",
        "last_price_dollars": last_price,
        "volume_fp": "0.000000000000000001",
        "open_interest_fp": "123.456789012345678901",
        "settlement_value_dollars": None,
        **({"mve_selected_legs": legs} if legs is not None else {}),
    }


def _landing_rows():
    legs = [
        {
            "event_ticker": "E1",
            "market_ticker": "M1",
            "side": "yes",
            "yes_settlement_value_dollars": "0.000000000000000001",
        },
        {
            "event_ticker": "NOEVENT",
            "market_ticker": "ABSENT",
            "side": "no",
            "yes_settlement_value_dollars": "0.000000000000000002",
        },
    ]
    records = [
        ("1" * 32, "series", "S1", {"ticker": "S1", "title": "listed"}, None, "S1", "series"),
        (
            "2" * 32,
            "series",
            "S2",
            {"ticker": "S2", "title": "required parent"},
            None,
            "S2",
            "series",
        ),
        (
            "3" * 32,
            "events",
            "E1",
            {"event_ticker": "E1", "series_ticker": "S1", "title": "listed event"},
            "E1",
            "S1",
            "events_live",
        ),
        (
            "4" * 32,
            "events",
            "E2",
            {"event_ticker": "E2", "series_ticker": "S2", "title": "required event"},
            "E2",
            "S2",
            "events_live",
        ),
        (
            "a" * 32,
            "markets",
            "M1",
            _market("M1", "E1", last_price="0.111111111111111111"),
            "E1",
            "S1",
            "markets_history",
        ),
        (
            "b" * 32,
            "markets",
            "M1",
            _market(
                "M1",
                "E1",
                last_price="12345678901234567890.123456789012345678",
            ),
            "E1",
            "S1",
            "markets_history",
        ),
        (
            "c" * 32,
            "markets",
            "M2",
            _market("M2", "E2", last_price="0.100000000000000001"),
            "E2",
            "S2",
            "markets_history",
        ),
        (
            "d" * 32,
            "markets",
            "MISSING",
            _market("MISSING", "NOEVENT"),
            "NOEVENT",
            None,
            "markets_history",
        ),
        (
            "e" * 32,
            "markets",
            "COMBO",
            _market("COMBO", "E1", legs=legs),
            "E1",
            "S1",
            "markets_history",
        ),
    ]
    rows = []
    for observation_id, kind, ticker, payload, event, series, scope in records:
        endpoint = {"series": "/series", "events": "/events", "markets": "/historical/markets"}[
            kind
        ]
        rows.append(
            (
                observation_id,
                CAPTURE_ID,
                kind,
                ticker,
                "2026-10-29T12:00:00Z",
                None,
                endpoint,
                "direct",
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                event,
                series,
                scope,
                "0",
            )
        )
    return rows


def _seed_warehouse(database):
    with duckdb.connect(str(database)) as connection:
        connection.execute("create schema landing")
        connection.execute(
            """create table landing.observations (
                   observation_id varchar, capture_id varchar, entity_kind varchar,
                   entity_ticker varchar, received_at varchar, provider_updated_at varchar,
                   source_endpoint varchar, source_kind varchar, payload_json varchar,
                   event_ticker varchar, series_ticker varchar, task_scope varchar,
                   task_generation varchar
               )"""
        )
        connection.executemany(
            "insert into landing.observations values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            _landing_rows(),
        )
        connection.execute(
            """create table landing.membership (
                   entity_kind varchar, entity_ticker varchar, scope varchar
               )"""
        )
        connection.executemany(
            "insert into landing.membership values (?, ?, ?)",
            [
                ("series", "S1", "series"),
                ("events", "E1", "events_live"),
                ("markets", "M1", "markets_history"),
                ("markets", "M2", "markets_history"),
                ("markets", "MISSING", "markets_history"),
                ("markets", "COMBO", "markets_history"),
            ],
        )
        connection.execute(
            """create table landing.parent_eligibility (
                   capture_id varchar, entity_kind varchar, entity_ticker varchar, status integer
               )"""
        )
        connection.executemany(
            "insert into landing.parent_eligibility values (?, ?, ?, ?)",
            [
                (CAPTURE_ID, "events", "E2", 200),
                (CAPTURE_ID, "series", "S2", 200),
                (CAPTURE_ID, "events", "NOEVENT", 404),
            ],
        )


def _schema(connection, table):
    return connection.execute(
        """select ordinal_position, column_name, data_type, is_nullable
           from information_schema.columns
           where table_schema = 'main' and table_name = ?
           order by ordinal_position""",
        [table],
    ).fetchall()


def _rows(connection, table, ordering):
    return connection.execute(f"select * from main.{table} order by {ordering}").fetchall()


def test_historical_compact_views_match_legacy_public_relations(tmp_path):
    store = Store(tmp_path / "root")
    databases = {}
    variables = {
        "catalogue_version": 2,
        "incremental_update": False,
        "current_capture_id": CAPTURE_ID,
        "delta_capture_id": CAPTURE_ID,
    }
    for mode, compact in (("legacy", False), ("compact", True)):
        relative = f"builds/{mode}/warehouse.duckdb"
        database = store.path(relative)
        database.parent.mkdir(parents=True)
        _seed_warehouse(database)
        warehouse.dbt(
            store,
            ["build"],
            database=relative,
            variables={**variables, "historical_compact": compact},
        )
        databases[mode] = database

    with duckdb.connect(str(databases["compact"]), read_only=True) as compact:
        for private in (
            "_historical_current_series",
            "_historical_current_events",
            "_historical_current_markets",
        ):
            columns = {
                row[1] for row in compact.execute(f"pragma table_info('{private}')").fetchall()
            }
            assert "observation_id" in columns
            assert "payload_json" not in columns
            assert not columns.intersection(
                {"rules_primary", "rules_secondary", "price_ranges", "last_price_dollars"}
            )
        assert compact.execute(
            "select typeof(payload_json) from landing.observations limit 1"
        ).fetchone() == ("VARCHAR",)

    shutil.rmtree(store.path("tmp"), ignore_errors=True)
    with (
        duckdb.connect(str(databases["legacy"]), read_only=True) as legacy,
        duckdb.connect(str(databases["compact"]), read_only=True) as compact,
    ):
        for name, table in _PUBLIC_RELATIONS:
            assert _schema(compact, table) == _schema(legacy, table), table
            assert _rows(compact, table, publish.ORDER_KEYS[name]) == _rows(
                legacy, table, publish.ORDER_KEYS[name]
            ), table

        market_schema = [column[1] for column in _schema(compact, "current_markets")]
        markets = {
            row[0]: dict(zip(market_schema, row, strict=True))
            for row in _rows(compact, "current_markets", "market_ticker")
        }
        assert markets["M1"]["observation_id"] == "b" * 32
        assert markets["M1"]["last_price_dollars"] == Decimal(
            "12345678901234567890.123456789012345678"
        )
        assert markets["MISSING"]["usable"] is False
        assert compact.execute(
            "select resolved from main.combo_legs order by leg_index"
        ).fetchall() == [(True,), (False,)]
        assert compact.execute(
            "select parent_ticker, child_ticker, resolved from main.relationships "
            "where child_kind = 'markets' order by child_ticker"
        ).fetchall() == [
            ("E1", "COMBO", True),
            ("E1", "M1", True),
            ("E2", "M2", True),
            ("NOEVENT", "MISSING", False),
        ]


def test_historical_publication_uses_zstd_level_six(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    state = capture.capture(store, client_factory=FakeClient)
    warehouse.build_historical(store, state["capture_id"])

    copied_queries = []
    original_export = publish._export_connection

    @contextmanager
    def record_export(*args, **kwargs):
        with original_export(*args, **kwargs) as connection:

            class RecordingConnection:
                def execute(self, query, parameters=None):
                    if query.lstrip().lower().startswith("copy "):
                        copied_queries.append(query)
                    return (
                        connection.execute(query, parameters)
                        if parameters is not None
                        else connection.execute(query)
                    )

            yield RecordingConnection()

    coverage_options = {}
    write_table = pq.write_table

    def record_coverage(table, path, **kwargs):
        if path.name == "coverage.parquet":
            coverage_options.update(kwargs)
        return write_table(table, path, **kwargs)

    monkeypatch.setattr(publish, "_export_connection", record_export)
    monkeypatch.setattr(pq, "write_table", record_coverage)
    release = publish.publish(store, state["capture_id"], catalogue_version=3)

    assert len(copied_queries) == len(warehouse.RELATIONS)
    assert all("compression zstd, compression_level 6" in query.lower() for query in copied_queries)
    assert coverage_options == {"compression": "zstd", "compression_level": 6}
    directory = store.path(f"releases/{release['source_release_id']}")
    for name in release["files"]:
        parquet = pq.ParquetFile(directory / name)
        assert all(
            parquet.metadata.row_group(row_group).column(column).compression == "ZSTD"
            for row_group in range(parquet.metadata.num_row_groups)
            for column in range(parquet.metadata.num_columns)
        )
