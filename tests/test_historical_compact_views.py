from __future__ import annotations

import json
import shutil
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import duckdb
import jinja2
import pyarrow.parquet as pq
import pytest
from conftest import FakeClient

from kalshi_events import capture, publish, warehouse
from kalshi_events.common import Store

CAPTURE_ID = "c" * 32
_PUBLIC_RELATIONS = tuple(warehouse.RELATIONS.items())
_MODELS = Path(__file__).parents[1] / "src/kalshi_events/dbt/models"


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
        {
            "event_ticker": "E1",
            "market_ticker": "M1",
            "side": "yes",
            "yes_settlement_value_dollars": "0.000000000000000001",
        },
    ]
    records = [
        (
            "1" * 32,
            "series",
            "S1",
            {"ticker": "S1", "title": "listéd 🌍"},
            None,
            "S1",
            "series",
        ),
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
            {"event_ticker": "E1", "series_ticker": "S1", "title": "listed café 🌍"},
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


def _historical_model_sql(name, *, compact=True):
    template = jinja2.Environment().from_string((_MODELS / f"{name}.sql").read_text())
    variables = {
        "catalogue_version": 2,
        "historical_compact": compact,
        "incremental_update": False,
        "current_capture_id": CAPTURE_ID,
    }
    return template.render(
        config=lambda **kwargs: "",
        ref=lambda relation: f"main.{relation}",
        source=lambda schema, relation: f"{schema}.{relation}",
        var=lambda key, default=None: variables.get(key, default),
    )


def _walk_plan(plan):
    if isinstance(plan, list):
        for node in plan:
            yield from _walk_plan(node)
        return
    if not isinstance(plan, dict):
        return
    yield plan
    for key in ("children", "operator"):
        yield from _walk_plan(plan.get(key, []))


def _scan_tables(plan):
    tables = []
    for node in _walk_plan(plan):
        if node.get("name", node.get("operator_type", node.get("type"))) not in {
            "SEQ_SCAN",
            "TABLE_SCAN",
        }:
            continue
        info = node.get("extra_info", node.get("extraInfo", {}))
        table = info.get("Table", info.get("table")) if isinstance(info, dict) else None
        if table is not None:
            tables.append(str(table))
    return tables


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

        relationship_plan = json.loads(
            compact.execute(
                f"explain (format json) {_historical_model_sql('relationships')}"
            ).fetchone()[1]
        )
        assert not any("landing.observations" in table for table in _scan_tables(relationship_plan))

        combo_plan = json.loads(
            compact.execute(
                f"explain (format json) {_historical_model_sql('combo_legs')}"
            ).fetchone()[1]
        )
        combo_scans = _scan_tables(combo_plan)
        assert sum("landing.observations" in table for table in combo_scans) == 1
        correlations = [
            str(node.get("extra_info", {}).get("Conditions", ""))
            for node in _walk_plan(combo_plan)
            if "IS NOT DISTINCT FROM"
            in str(node.get("extra_info", {}).get("Conditions", "")).upper()
        ]
        assert len(correlations) == 1
        assert "selected_legs" in correlations[0]
        assert "payload_json" not in correlations[0]

    shutil.rmtree(store.path("tmp"), ignore_errors=True)
    export_temporary = store.path("tmp/export-plan")
    export_temporary.mkdir(parents=True)
    with publish._export_connection(
        store, databases["compact"], export_temporary, historical_export=True
    ) as connection:
        plan = json.loads(
            connection.execute(
                "explain (format json) select * from main.current_markets order by market_ticker"
            ).fetchone()[1]
        )

    def walk(node):
        if isinstance(node, list):
            for child in node:
                yield from walk(child)
            return
        yield node
        for child in node.get("children", []):
            yield from walk(child)

    observation_join = [
        node
        for node in walk(plan)
        if node.get("name", "").upper() == "HASH_JOIN"
        and "observation_id = observation_id" in str(node.get("extra_info", {}))
    ]
    assert len(observation_join) == 1
    left, right = observation_join[0]["children"]
    assert "_historical_current_markets" not in json.dumps(left)
    assert "_historical_current_markets" in json.dumps(right)

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
        assert markets["MISSING"]["rules_secondary"] is None
        assert (
            "listed café 🌍"
            in compact.execute(
                "select payload_json from main.current_events where event_ticker = 'E1'"
            ).fetchone()[0]
        )
        assert compact.execute(
            "select resolved from main.combo_legs order by leg_index"
        ).fetchall() == [(True,), (False,), (True,)]
        assert compact.execute(
            "select combo_market_ticker, leg_index, event_ticker, market_ticker, side, "
            "yes_settlement_value_dollars, resolved from main.combo_legs order by leg_index"
        ).fetchall() == [
            ("COMBO", 0, "E1", "M1", "yes", Decimal("0.000000000000000001"), True),
            ("COMBO", 1, "NOEVENT", "ABSENT", "no", Decimal("0.000000000000000002"), False),
            ("COMBO", 2, "E1", "M1", "yes", Decimal("0.000000000000000001"), True),
        ]
        assert compact.execute(
            "select parent_ticker, child_ticker, resolved from main.relationships "
            "where child_kind = 'markets' order by child_ticker"
        ).fetchall() == [
            ("E1", "COMBO", True),
            ("E1", "M1", True),
            ("E2", "M2", True),
            ("NOEVENT", "MISSING", False),
        ]


def test_compact_combo_legs_match_legacy_for_json_shapes_and_preserve_key_errors():
    connection = duckdb.connect()
    connection.execute(
        "create table current_markets (market_ticker varchar, event_ticker varchar, payload_json varchar)"
    )
    connection.execute("create table current_events (event_ticker varchar)")
    connection.execute(
        "create table _historical_current_markets (market_ticker varchar, event_ticker varchar)"
    )
    connection.execute("create table _historical_current_events (event_ticker varchar)")
    valid_leg = {
        "event_ticker": "E1",
        "market_ticker": "M1",
        "side": "yes",
        "yes_settlement_value_dollars": "0.000000000000000001",
    }
    legs = {
        "COMBO": [valid_leg, {**valid_leg}, {**valid_leg}],
        "MISSING": {"__missing__": True},
        "EMPTY": [],
        "NULL": None,
        "SCALAR": 7,
        "OBJECT": {str(number): {**valid_leg} for number in range(3)},
        "MALFORMED": [None, {}, "text", 7, []],
    }
    rows = [
        (
            ticker,
            "E1",
            json.dumps(
                {"mve_selected_legs": selected} if selected != {"__missing__": True} else {},
                separators=(",", ":"),
            ),
        )
        for ticker, selected in legs.items()
    ]
    rows.append(("M1", "E1", json.dumps({"ticker": "M1"})))
    connection.executemany("insert into current_markets values (?, ?, ?)", rows)
    connection.execute("insert into _historical_current_markets values ('M1', 'E1')")
    connection.execute("insert into _historical_current_events values ('E1')")
    connection.execute("insert into current_events values ('E1')")

    def result(compact):
        query = _historical_model_sql("combo_legs", compact=compact)
        cursor = connection.execute(
            f"select * from ({query}) combo_legs order by combo_market_ticker, leg_index"
        )
        schema = [(column[0], column[1]) for column in cursor.description]
        return schema, cursor.fetchall()

    assert result(compact=True) == result(compact=False)
    assert len(result(compact=True)[1]) == 13

    connection.execute(
        "insert into current_markets values (?, ?, ?)",
        ["BAD_KEY", "E1", '{"mve_selected_legs":{"not-an-index":{}}}'],
    )
    for compact in (False, True):
        with pytest.raises(duckdb.ConversionException, match="not-an-index"):
            connection.execute(_historical_model_sql("combo_legs", compact=compact)).fetchall()
    connection.close()


def test_historical_publication_uses_zstd_level_six(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    state = capture.capture(store, client_factory=FakeClient)
    warehouse.build_historical(store, state["capture_id"])

    copied_queries = []
    historical_export_flags = []
    original_export = publish._export_connection

    @contextmanager
    def record_export(*args, **kwargs):
        historical_export_flags.append(kwargs.get("historical_export", False))
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

    assert historical_export_flags == [True]
    assert len(copied_queries) == len(warehouse.RELATIONS)
    assert all(publish.HISTORICAL_COPY_OPTIONS in query.lower() for query in copied_queries)
    assert coverage_options == {"compression": "zstd", "compression_level": 6}
    directory = store.path(f"releases/{release['source_release_id']}")
    for name in release["files"]:
        parquet = pq.ParquetFile(directory / name)
        assert all(
            parquet.metadata.row_group(row_group).column(column).compression == "ZSTD"
            for row_group in range(parquet.metadata.num_row_groups)
            for column in range(parquet.metadata.num_columns)
        )
