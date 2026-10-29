from __future__ import annotations

import json

import conftest
import duckdb
import pytest
from conftest import COMBO, HISTORICAL, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import warehouse
from kalshi_events.capture import page_receipts
from kalshi_events.common import CatalogueError, Limits, Store
from kalshi_events.historical import HISTORICAL_CONTRACT, historical_selection


def _factory(responses=None, calls=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            if calls is not None:
                calls.append(endpoint)
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: Client(store, budget, responses, **options)


def _incomplete_legacy_capture(store, responses=None):
    cursor = 0

    def incomplete_multivariate(_params):
        nonlocal cursor
        value = cursor
        cursor += 1
        return 200, {"events": [], "cursor": f"next-{value}"}

    bounded_store = Store(store.root, Limits(max_requests=14))
    responses = {"/events/multivariate": incomplete_multivariate, **(responses or {})}
    state = acquisition.capture(
        bounded_store,
        catalogue_profile="exchange",
        client_factory=_factory(responses),
    )
    assert state["complete"] is False
    selection = historical_selection(state, allow_legacy_incomplete=True)
    assert all(state["tasks"][item["task_index"]]["done"] for item in selection.values())
    return state


def test_historical_build_uses_selected_evidence_when_global_capture_is_incomplete(
    store,
):
    state = _incomplete_legacy_capture(store)
    capture_id = state["capture_id"]
    selection = historical_selection(state, allow_legacy_incomplete=True)
    selected_indices = {item["task_index"] for item in selection.values()} | {0}
    rows = page_receipts(store, state)
    excluded = next(row for row in rows if row["index"] not in selected_indices)
    journal = [
        json.loads(line)
        for line in store.path(f"captures/{capture_id}/pages.jsonl").read_text().splitlines()
    ]
    excluded_seq = next(
        record["seq"]
        for record in journal
        if record.get("op") == "page" and record["payload"]["relative"] == excluded["relative"]
    )
    snapshot = json.loads(store.path(f"captures/{capture_id}/state.json").read_text())
    assert excluded_seq <= snapshot["_journal"]["seq"]
    store.path(excluded["relative"]).parent.joinpath("body.bin").unlink()

    with pytest.raises(CatalogueError, match="incomplete"):
        warehouse.build(store, capture_id)

    receipt = warehouse.build_historical(store, capture_id)
    assert receipt["catalogue_version"] == 3
    assert receipt["scope"] == "historical"
    assert receipt["source_capture_complete"] is False
    assert receipt["historical_contract"] == HISTORICAL_CONTRACT
    assert receipt["evidence"][f"captures/{capture_id}/pages.jsonl"]
    assert excluded["relative"] not in receipt["evidence"]
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt

    historical_row = next(
        row for row in rows if row["index"] == selection["markets_history"]["task_index"]
    )
    body = store.path(historical_row["relative"]).parent / "body.bin"
    body.write_bytes(body.read_bytes() + b" ")
    with pytest.raises(CatalogueError, match="integrity|hash|evidence|changed"):
        warehouse.certified(store, capture_id, catalogue_version=3)


def test_historical_build_keeps_all_generations_combos_and_tie_winner(store, monkeypatch):
    timestamp = "2026-10-28T12:00:00Z"
    monkeypatch.setattr(acquisition, "now", lambda: timestamp)
    monkeypatch.setattr(conftest, "now", lambda: timestamp)
    old_market = {**HISTORICAL, "title": "older generation"}
    new_market = {**HISTORICAL, "title": "final generation"}
    resolved_combo = {
        **COMBO,
        "ticker": "COMBO-HISTORY-RESOLVED",
        "event_ticker": "HISTORY",
        "mve_selected_legs": [
            {
                "event_ticker": "HISTORY",
                "market_ticker": "HISTORICAL",
                "side": "yes",
                "yes_settlement_value_dollars": "0.00001",
            }
        ],
    }
    unresolved_combo = {
        **COMBO,
        "ticker": "COMBO-HISTORY-UNRESOLVED",
        "event_ticker": "MISSING-EVENT",
        "mve_selected_legs": [
            {
                "event_ticker": "MISSING-EVENT",
                "market_ticker": "MISSING-MARKET",
                "side": "no",
                "yes_settlement_value_dollars": "0.00002",
            }
        ],
    }
    first_page = True

    def historical_markets(params):
        nonlocal first_page
        if params.get("cursor") == "expired":
            return 400, {"message": "invalid cursor"}
        if first_page:
            first_page = False
            return 200, {"markets": [old_market], "cursor": "expired"}
        return 200, {
            "markets": [new_market, resolved_combo, unresolved_combo],
            "cursor": "",
        }

    calls = []
    state = acquisition.capture(
        store,
        client_factory=_factory({"/historical/markets": historical_markets}, calls),
    )
    capture_id = state["capture_id"]
    assert state["complete"]
    assert state["tasks"][3]["restarts"] == 1
    assert not any(endpoint in {"/markets", "/events/multivariate"} for endpoint in calls)

    legacy_build = store.path(f"builds/{capture_id}")
    legacy_build.mkdir(parents=True)
    (legacy_build / "keep").write_text("v1")
    v2_build = store.path(f"builds-v2/{capture_id}")
    v2_build.mkdir(parents=True)
    (v2_build / "keep").write_text("v2")

    receipt = warehouse.build_historical(store, capture_id)
    assert (legacy_build / "keep").read_text() == "v1"
    assert (v2_build / "keep").read_text() == "v2"
    assert warehouse.build_directory(capture_id, 3) == f"builds-historical/{capture_id}"

    with duckdb.connect(
        str(store.path(f"builds-historical/{capture_id}/warehouse.duckdb")), read_only=True
    ) as connection:
        history_rows = connection.execute(
            """select received_at, observation_id, payload_json
               from main.observation_history
               where entity_kind='markets' and entity_ticker='HISTORICAL'"""
        ).fetchall()
        current = connection.execute(
            """select observation_id, json_extract_string(payload_json, '$.title')
               from main.current_markets where market_ticker='HISTORICAL'"""
        ).fetchone()
        combo_status = connection.execute(
            """select combo_market_ticker, resolved from main.combo_legs
               order by combo_market_ticker"""
        ).fetchall()
        coverage = connection.execute("select * from main.catalogue_coverage").fetchall()

    assert len(history_rows) == 2
    assert {row[0] for row in history_rows} == {timestamp}
    winner = max(history_rows, key=lambda row: row[1])
    assert current == (winner[1], json.loads(winner[2])["title"])
    assert combo_status == [
        ("COMBO-HISTORY-RESOLVED", True),
        ("COMBO-HISTORY-UNRESOLVED", False),
    ]
    assert coverage == [
        (
            HISTORICAL_CONTRACT,
            "historical",
            ["events_combo", "markets_live", "markets_combo"],
            capture_id,
            True,
        )
    ]
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt


def test_historical_build_certifies_an_empty_selected_catalogue(store):
    empty = {
        "/series": (200, {"series": [], "cursor": ""}),
        "/events": (200, {"events": [], "cursor": ""}),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
    }
    state = acquisition.capture(store, client_factory=_factory(empty))
    receipt = warehouse.build_historical(store, state["capture_id"])
    with duckdb.connect(
        str(store.path(f"builds-historical/{state['capture_id']}/warehouse.duckdb")),
        read_only=True,
    ) as connection:
        assert connection.execute("select count(*) from main.observation_history").fetchone() == (
            0,
        )
        assert connection.execute("select count(*) from main.catalogue_coverage").fetchone() == (1,)
    assert warehouse.certified(store, state["capture_id"], catalogue_version=3) == receipt
