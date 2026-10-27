from __future__ import annotations

import hashlib
import json
import shutil
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from conftest import COMBO, EVENTS, MARKET, SERIES, FakeClient

from kalshi_events import capture, publish, warehouse
from kalshi_events.common import CatalogueError, Store


@pytest.fixture(scope="module")
def catalogue(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("export-ordering") / "root")
    market = {
        **MARKET,
        "rules_primary": 'Native rules: café, € and quoted "YES". ' * 64,
        "additional_metadata": {"native": ["α", None, {"unmodeled": True}]},
    }
    combo = {
        **COMBO,
        "mve_selected_legs": [
            COMBO["mve_selected_legs"][0],
            {**COMBO["mve_selected_legs"][0], "side": "no"},
            {**COMBO["mve_selected_legs"][0], "side": "YES"},
            {**COMBO["mve_selected_legs"][0], "side": "NO"},
        ],
    }
    responses = {
        "/series": (
            200,
            {"series": [{"ticker": "Z-SERIES"}, *SERIES, {"ticker": "A-SERIES"}]},
        ),
        "/events": (
            200,
            {
                "events": [
                    {"event_ticker": "ZETA", "series_ticker": "Z-SERIES"},
                    *EVENTS,
                    {"event_ticker": "ORPHAN", "series_ticker": None},
                    {"event_ticker": "ALPHA", "series_ticker": "A-SERIES"},
                ],
                "cursor": "",
            },
        ),
        "/markets": (200, {"markets": [market, combo], "cursor": ""}),
    }
    state = capture.capture(
        store, client_factory=lambda s, b, **_options: FakeClient(s, b, responses)
    )
    warehouse.build(store, state["capture_id"])
    release = publish.publish(store, state["capture_id"])
    database = store.path(f"builds/{state['capture_id']}/warehouse.duckdb")
    return store, state, release, database, market, combo


def test_every_narrow_ordered_export_matches_legacy_bytes_and_schema(catalogue, tmp_path):
    _, _, release, database, native_market, native_combo = catalogue
    connection = duckdb.connect(
        ":memory:",
        config={
            "disabled_optimizers": "statistics_propagation",
            "memory_limit": warehouse.DBT_MEMORY_LIMIT,
            "threads": str(warehouse.DBT_THREADS),
        },
    )
    try:
        database_literal = str(database).replace("'", "''")
        connection.execute(f"ATTACH '{database_literal}' AS source (READ_ONLY)")
        connection.execute("USE source")
        for name, relation in warehouse.RELATIONS.items():
            legacy = tmp_path / f"{name}.parquet"
            connection.execute(
                f"copy (select * from main.{relation} order by all) to ? (format parquet)",
                [str(legacy)],
            )
            exported = Path(release["output"]) / f"{name}.parquet"
            assert exported.read_bytes() == legacy.read_bytes(), name
            assert pq.read_schema(exported).equals(pq.read_schema(legacy), check_metadata=True)
            assert all(field.nullable for field in pq.read_schema(exported)), name
    finally:
        connection.close()

    markets = pq.read_table(Path(release["output"]) / "markets.parquet").to_pylist()
    market = next(row for row in markets if row["market_ticker"] == "MARKET")
    assert market["last_price_dollars"] == Decimal("0.00123")
    assert market["volume_fp"] == Decimal("12.3456789")
    assert market["open_interest_fp"] == Decimal("2.125")
    assert json.loads(market["payload_json"]) == native_market
    combo_market = next(row for row in markets if row["market_ticker"] == "COMBO-MARKET")
    assert json.loads(combo_market["payload_json"]) == native_combo
    legs = pq.read_table(Path(release["output"]) / "combo_legs.parquet").to_pylist()
    assert [row["leg_index"] for row in legs] == [0, 1, 2, 3]
    assert [row["side"] for row in legs] == ["yes", "no", "YES", "NO"]
    assert [json.loads(row["payload_json"])["side"] for row in legs] == [
        "yes",
        "no",
        "YES",
        "NO",
    ]
    assert all(row["yes_settlement_value_dollars"] == Decimal("0.00001") for row in legs)


def test_repeated_publication_preserves_every_file_and_release_identity(catalogue):
    store, state, release, _, _, _ = catalogue
    repeated = publish.publish(store, state["capture_id"])
    assert repeated["source_release_id"] == release["source_release_id"]
    assert repeated["files"] == release["files"]


@pytest.mark.parametrize(
    "relation,predicate",
    [
        ("combo_legs", "true"),
        ("relationships", "parent_ticker is not null"),
        ("relationships", "parent_ticker is null"),
    ],
)
def test_dbt_rejects_duplicate_composite_export_keys(catalogue, relation, predicate):
    store, _, _, source, _, _ = catalogue
    original_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    relative = (
        f"tmp/duplicate-{relation}-{'null' if predicate.endswith('is null') else 'normal'}.duckdb"
    )
    database = store.path(relative)
    database.parent.mkdir(exist_ok=True)
    shutil.copyfile(source, database)
    with duckdb.connect(str(database)) as connection:
        original_count = connection.execute(f"select count(*) from main.{relation}").fetchone()[0]
        replacement = (
            "case when side = 'yes' then 'no' else 'yes' end as side"
            if relation == "combo_legs"
            else "not resolved as resolved"
        )
        connection.execute(
            f"insert into main.{relation} select * replace ({replacement}) "
            f"from main.{relation} where {predicate} limit 1"
        )
        assert connection.execute(f"select count(*) from main.{relation}").fetchone()[0] == (
            original_count + 1
        )
    store.sync(relative)
    with pytest.raises(CatalogueError, match="dbt certification failed") as rejected:
        warehouse.dbt(store, ["test", "--select", relation], database=relative)
    assert "unique" in str(rejected.value).lower()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == original_digest


@pytest.mark.parametrize(
    "relation,replacement",
    [
        ("current_series", "cast('{\"changed\":true}' as json) as payload_json"),
        ("current_events", "cast('{\"changed\":true}' as json) as payload_json"),
        ("current_markets", "cast('{\"changed\":true}' as json) as payload_json"),
        ("observation_history", "cast('{\"changed\":true}' as json) as payload_json"),
        ("combo_legs", "not resolved as resolved"),
        ("relationships", "not resolved as resolved"),
    ],
)
def test_publication_rejects_legacy_duplicate_keys_before_pointer_change(
    catalogue, tmp_path, monkeypatch, relation, replacement
):
    source_store, state, _, _, _, _ = catalogue
    certified = warehouse.certified(source_store, state["capture_id"])
    root = tmp_path / "legacy-producer-root"
    shutil.copytree(source_store.root, root)
    store = Store(root)
    database = store.path(f"builds/{state['capture_id']}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            f"insert into main.{relation} select * replace ({replacement}) "
            f"from main.{relation} limit 1"
        )
    store.sync(f"builds/{state['capture_id']}")
    # Isolate publication's key proof from the older producer's certification checks.
    monkeypatch.setattr(publish, "certified", lambda *args, **kwargs: certified)
    pointer = store.path("current.json").read_bytes()
    with pytest.raises(CatalogueError, match="non-unique publication ordering key"):
        publish.publish(store, state["capture_id"])
    assert store.path("current.json").read_bytes() == pointer
    assert not list(store.path("tmp").glob("publish-*"))
