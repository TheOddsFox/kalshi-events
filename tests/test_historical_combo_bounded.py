from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from kalshi_events import historical_combo, warehouse
from kalshi_events.common import CatalogueError, Store


def _database(store: Store) -> tuple[str, str]:
    stage = store.path("tmp/combo-stage")
    stage.mkdir(parents=True)
    database = stage / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create schema landing")
        connection.execute(
            "create table main.current_markets (market_ticker varchar, payload_json varchar)"
        )
        connection.execute(
            "create table main._historical_current_markets "
            "(market_ticker varchar, event_ticker varchar)"
        )
        connection.execute("create table main._historical_current_events (event_ticker varchar)")
    store.sync("tmp/combo-stage")
    return str(database), "tmp/combo-stage"


def _seed(database: str, *, malformed: bool = False, duplicate_target: bool = False) -> None:
    source_rows = [
        (
            "COMBO-1",
            '{"mve_selected_legs":[ { "event_ticker" : "E1", '
            '"market_ticker" : "M1", "side" : "yes", '
            '"yes_settlement_value_dollars" : "0.123456789012345678" }, '
            '{ "event_ticker" : "E1", "market_ticker" : "M1", "side" : "yes" } ]}',
        ),
        (
            "COMBO-2",
            '{"mve_selected_legs":[{"event_ticker":"MISSING-EVENT",'
            '"market_ticker":"MISSING-MARKET","side":"no"}]}',
        ),
        (
            "COMBO-3",
            '{"mve_selected_legs":[{"event_ticker":"E2","market_ticker":"MNULL","side":"yes"}]}',
        ),
        (
            "COMBO-4",
            '{"mve_selected_legs":[{"event_ticker":"E3","market_ticker":"M3","side":"no"}]}',
        ),
    ]
    if malformed:
        source_rows.append(("COMBO-BROKEN", '{"mve_selected_legs":{"bad":{}}}'))
    with duckdb.connect(database) as connection:
        connection.executemany("insert into main.current_markets values (?, ?)", source_rows)
        targets = [("M1", "E1"), ("MNULL", None), ("M3", "OTHER-EVENT")]
        if duplicate_target:
            targets.append(("M1", "E1"))
        connection.executemany(
            "insert into main._historical_current_markets values (?, ?)", targets
        )
        connection.executemany(
            "insert into main._historical_current_events values (?)", [("E1",), ("E2",), ("E3",)]
        )


def _legacy_query(connection: duckdb.DuckDBPyConnection):
    return connection.execute(
        """with combo_sources as (
               select market_ticker,
                      json_extract(payload_json, '$.mve_selected_legs') as selected_legs
               from main.current_markets
               where json_extract(payload_json, '$.mve_selected_legs') is not null
           )
           select m.market_ticker as combo_market_ticker,
                  cast(leg.key as integer) as leg_index,
                  json_extract_string(leg.value, '$.event_ticker') as event_ticker,
                  json_extract_string(leg.value, '$.market_ticker') as market_ticker,
                  json_extract_string(leg.value, '$.side') as side,
                  cast(json_extract_string(leg.value, '$.yes_settlement_value_dollars')
                       as decimal(38,18)) as yes_settlement_value_dollars,
                  (target.market_ticker is not null
                   and target.event_ticker = json_extract_string(leg.value, '$.event_ticker')
                   and native_event.event_ticker is not null) as resolved,
                  leg.value as payload_json
           from combo_sources m,
                lateral json_each(m.selected_legs) leg
           left join main._historical_current_markets target
             on json_extract_string(leg.value, '$.market_ticker') = target.market_ticker
           left join main._historical_current_events native_event
             on json_extract_string(leg.value, '$.event_ticker') = native_event.event_ticker
           order by m.market_ticker, cast(leg.key as integer)"""
    )


def test_historical_combo_matches_legacy_projection_and_nullable_resolution(tmp_path):
    store = Store(tmp_path / "root")
    database, _ = _database(store)
    _seed(database)
    with duckdb.connect(database) as connection:
        expected = _legacy_query(connection).fetchall()
        expected_schema = _legacy_query(connection).description

    assert historical_combo.build_combo_legs(store, database) == {
        "markets": 4,
        "legs": 5,
        "batches": 1,
    }
    warehouse.dbt(
        store,
        ["run", "--select", "combo_legs"],
        database="tmp/combo-stage/warehouse.duckdb",
        variables={"historical_compact": True, "historical_combo_prepared": True},
    )

    with duckdb.connect(database, read_only=True) as connection:
        actual = connection.execute(
            "select combo_market_ticker, leg_index, event_ticker, market_ticker, side, "
            "yes_settlement_value_dollars, resolved, payload_json "
            "from main.combo_legs order by combo_market_ticker, leg_index"
        ).fetchall()
        actual_schema = connection.execute(
            "select combo_market_ticker, leg_index, event_ticker, market_ticker, side, "
            "yes_settlement_value_dollars, resolved, payload_json "
            "from main.combo_legs limit 0"
        ).description
        assert connection.execute(
            "select table_type from information_schema.tables "
            "where table_schema='main' and table_name='combo_legs'"
        ).fetchone() == ("VIEW",)

    assert actual == expected
    assert actual_schema == expected_schema
    assert [row[6] for row in actual] == [True, True, False, None, False]
    assert actual[0][1] == 0
    assert actual[1][1] == 1
    assert actual[0][5].as_tuple().exponent == -18


def test_historical_combo_fails_closed_on_malformed_keys_and_duplicate_targets(tmp_path):
    store = Store(tmp_path / "root")
    database, _ = _database(store)
    _seed(database, malformed=True)
    with pytest.raises(duckdb.Error):
        historical_combo.build_combo_legs(store, database)

    other = Store(tmp_path / "other-root")
    duplicate_database, _ = _database(other)
    _seed(duplicate_database, duplicate_target=True)
    with pytest.raises(CatalogueError, match="not unique"):
        historical_combo.build_combo_legs(other, duplicate_database)


def test_historical_combo_checkpoints_batches_with_bounded_wal(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    database, _ = _database(store)
    _seed(database)
    checkpoints = []
    original_checkpoint = historical_combo._checkpoint

    def record_checkpoint(connection, store, staging_relative):
        wal_path = Path(f"{database}.wal")
        before = wal_path.stat().st_size if wal_path.exists() else 0
        original_checkpoint(connection, store, staging_relative)
        after = wal_path.stat().st_size if wal_path.exists() else 0
        checkpoints.append((before, after))

    monkeypatch.setattr(historical_combo, "_checkpoint", record_checkpoint)
    monkeypatch.setattr(historical_combo, "_BATCH_ROWS", 2)
    monkeypatch.setattr(historical_combo, "_CHECKPOINT_BYTES", 1)
    monkeypatch.setattr(historical_combo, "_CHECKPOINT_SECONDS", 3600)

    assert historical_combo.build_combo_legs(store, database, config={"threads": "1"}) == {
        "markets": 4,
        "legs": 5,
        "batches": 2,
    }
    assert len(checkpoints) == 4  # table reset, two input batches, final resolution
    assert all(
        before < historical_combo._BATCH_BYTES and after == 0 for before, after in checkpoints
    )
    assert not store.path("tmp/combo-stage/combo-legs-input.parquet").exists()


class _Deadline:
    def check_deadline(self) -> None:
        pass


def test_historical_combo_arrow_batches_obey_row_and_logical_byte_bounds():
    rows = ((f"M{index}", "[]") for index in range(4097))
    tables = list(historical_combo._arrow_batches(rows, _Deadline()))
    assert [table.num_rows for table in tables] == [4096, 1]

    prefix, suffix = '[{"padding":"', '"}]'
    one_under = (
        prefix + "x" * (historical_combo._BATCH_BYTES - 1 - len(prefix) - len(suffix)) + suffix
    )
    one_over = (
        prefix + "y" * (historical_combo._BATCH_BYTES + 1 - len(prefix) - len(suffix)) + suffix
    )
    tables = list(
        historical_combo._arrow_batches(
            [("a", one_under), ("b", "[]"), ("c", one_over)], _Deadline()
        )
    )
    assert [table.num_rows for table in tables] == [1, 1, 1]
    assert tables[0].column("selected_legs")[0].as_py() == one_under
    assert tables[-1].column("selected_legs")[0].as_py() == one_over


def test_historical_combo_accepts_a_valid_oversized_singleton(tmp_path):
    store = Store(tmp_path / "root")
    database, _ = _database(store)
    padding = "x" * (historical_combo._BATCH_BYTES + 64)
    selected_legs = (
        '[{"event_ticker":"E1","market_ticker":"M1","side":"yes","padding":"' + padding + '"}]'
    )
    with duckdb.connect(database) as connection:
        connection.execute(
            "insert into main.current_markets values (?, ?)",
            [
                "COMBO-SMALL",
                '{"mve_selected_legs":[{"event_ticker":"E2","market_ticker":"M2","side":"no"}]}',
            ],
        )
        connection.execute(
            "insert into main.current_markets values (?, ?)",
            ["COMBO-LARGE", '{"mve_selected_legs":' + selected_legs + "}"],
        )
        connection.execute("insert into main._historical_current_markets values ('M1', 'E1')")
        connection.execute("insert into main._historical_current_markets values ('M2', 'E2')")
        connection.executemany(
            "insert into main._historical_current_events values (?)", [("E1",), ("E2",)]
        )

    assert historical_combo.build_combo_legs(store, database) == {
        "markets": 2,
        "legs": 2,
        "batches": 2,
    }
    with duckdb.connect(database, read_only=True) as connection:
        assert any(
            row[0]
            for row in connection.execute(
                "select length(payload_json) > ? from landing.combo_legs_canonical",
                [historical_combo._BATCH_BYTES],
            ).fetchall()
        )


def test_historical_combo_resolution_plan_does_not_read_payload_json(tmp_path):
    store = Store(tmp_path / "root")
    database, _ = _database(store)
    _seed(database)
    with duckdb.connect(database) as connection:
        connection.execute(
            "create table landing.combo_legs_canonical "
            "(combo_market_ticker varchar, leg_index integer, event_ticker varchar, "
            "market_ticker varchar, side varchar, yes_settlement_value_dollars decimal(38,18), "
            "resolved boolean, payload_json json)"
        )
        plan = "\n".join(
            row[1]
            for row in connection.execute(
                "explain " + historical_combo._RESOLUTION_UPDATE_SQL
            ).fetchall()
        )
    assert "payload_json" not in plan.lower()
