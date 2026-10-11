from __future__ import annotations

import pytest
from conftest import EVENTS, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.common import CatalogueError, Limits, Store
from kalshi_events.normalize import project_pages


def test_native_full_catalogue_keeps_history_and_relationships(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    assert state["complete"]
    assert state["requests"] == 6
    projected = project_pages(acquisition.capture_pages(store, state))
    assert {m["market_ticker"] for m in projected["markets"]} == {
        "MARKET",
        "COMBO-MARKET",
        "HISTORICAL",
    }
    assert projected["combo_legs"][0]["side"] == "yes"
    assert projected["combo_legs"][0]["resolved"]
    market = next(row for row in projected["markets"] if row["market_ticker"] == "MARKET")
    assert market["last_price_dollars"] == "0.00123"
    assert market["volume_fp"] == "12.3456789"
    assert market["rules_primary"] == MARKET["rules_primary"]
    archived = next(row for row in projected["markets"] if row["market_ticker"] == "HISTORICAL")
    assert archived["status"] == "finalized"
    assert archived["result"] == "no"


def test_budget_stop_resumes_without_publishing_incomplete(store, factory):
    bounded = Store(store.root, Limits(max_requests=2))
    state = acquisition.capture(bounded, client_factory=factory)
    assert not state["complete"]
    assert state["requests"] == 2
    with pytest.raises(CatalogueError, match="incomplete"):
        acquisition.complete_capture(store, state["capture_id"])
    resumed = acquisition.capture(store, resume=state["capture_id"], client_factory=factory)
    assert resumed["complete"]
    assert resumed["requests"] == 4


def test_raw_commit_crash_is_recoverable_without_reissuing_request(store, factory, monkeypatch):
    original = acquisition._consume

    def crash(*args):
        raise RuntimeError("simulated process death after raw commit")

    monkeypatch.setattr(acquisition, "_consume", crash)
    with pytest.raises(RuntimeError):
        acquisition.capture(store, client_factory=factory)
    capture_id = next(store.path("captures").iterdir()).name
    state = acquisition.read_capture(store, capture_id)
    assert state["pending"]
    assert len(list(acquisition.capture_pages(store, state))) == 1
    monkeypatch.setattr(acquisition, "_consume", original)
    resumed = acquisition.capture(store, resume=capture_id, client_factory=factory)
    assert resumed["complete"]
    assert resumed["requests"] == 5


def test_invalidated_cursor_restarts_and_retains_all_observations(store):
    invalidated = False

    def events(params):
        nonlocal invalidated
        if params.get("cursor") == "expired":
            if not invalidated:
                invalidated = True
                return 400, {"message": "invalid cursor"}
            return 200, {"events": [EVENTS[1]], "cursor": ""}
        return 200, {"events": [EVENTS[0]], "cursor": "expired"}

    state = acquisition.capture(
        store, client_factory=lambda s, b: FakeClient(s, b, {"/events": events})
    )
    assert state["complete"]
    assert state["tasks"][2]["restarts"] == 1
    projection = project_pages(acquisition.capture_pages(store, state))
    assert len(projection["events"]) == 3
    assert (
        len(
            [
                row
                for row in projection["observation_history"]
                if row["entity_kind"] == "events" and row["event_ticker"] == "EVENT"
            ]
        )
        == 2
    )


@pytest.mark.parametrize(
    "payload",
    [{"markets": []}, {"markets": None, "cursor": ""}, {"markets": [False], "cursor": ""}],
)
def test_malformed_pages_never_complete(store, payload):
    with pytest.raises(CatalogueError):
        acquisition.capture(
            store, client_factory=lambda s, b: FakeClient(s, b, {"/markets": (200, payload)})
        )
    assert not acquisition.read_capture(store, next(store.path("captures").iterdir()).name)[
        "complete"
    ]


def test_missing_parent_does_not_disappear_or_infer_ticker_prefix(store):
    native = {**MARKET, "ticker": "NOT-PARENT-MARKET", "event_ticker": "UNKNOWN"}
    state = acquisition.capture(
        store,
        selected=[native["ticker"]],
        client_factory=lambda s, b: FakeClient(
            s, b, {f"/markets/{native['ticker']}": (200, {"market": native})}
        ),
    )
    assert state["complete"]
    assert state["coverage"][native["ticker"]]["status"] == "unresolved"
    projection = project_pages(acquisition.capture_pages(store, state))
    assert projection["markets"][0]["usable"] is False
    assert projection["markets"][0]["series_ticker"] is None
    assert projection["relationships"][0]["resolved"] is False


def test_targeted_absence_checks_historical_and_never_reuses_previous(store, factory):
    first = acquisition.capture(store, selected=["MARKET"], client_factory=factory)
    second = acquisition.capture(
        store,
        selected=["MARKET"],
        client_factory=lambda s, b: FakeClient(s, b, {"/markets/MARKET": (404, {})}),
    )
    assert first["coverage"]["MARKET"]["status"] == "found"
    assert second["coverage"]["MARKET"]["status"] == "absent"
    assert second["requests"] == 3
