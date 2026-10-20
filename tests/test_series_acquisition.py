from __future__ import annotations

import copy

import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture
from kalshi_events.common import CatalogueError, Store
from kalshi_events.normalize import ParentIndex

SERIES = {"ticker": "KXCPI", "title": "Consumer Price Index"}
LIVE_EVENT = {
    "event_ticker": "KXCPI-26JAN",
    "series_ticker": "KXCPI",
    "title": "January 2026 CPI",
}
HISTORICAL_EVENT = {
    "event_ticker": "KXCPI-25JAN",
    "series_ticker": "KXCPI",
    "title": "January 2025 CPI",
}
LIVE_MARKET = {"ticker": "KXCPI-26JAN-CPI", "event_ticker": "KXCPI-26JAN", "status": "active"}
HISTORICAL_MARKET = {
    "ticker": "KXCPI-25JAN-CPI",
    "event_ticker": "KXCPI-25JAN",
    "status": "finalized",
    "result": "yes",
}


def _responses(*, empty: bool = False):
    return {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": [] if empty else [LIVE_EVENT], "cursor": ""}),
        "/markets": (200, {"markets": [] if empty else [LIVE_MARKET], "cursor": ""}),
        "/historical/markets": (
            200,
            {"markets": [] if empty else [HISTORICAL_MARKET], "cursor": ""},
        ),
        "/events/KXCPI-25JAN": (200, {"event": HISTORICAL_EVENT}),
    }


def _capture(store: Store, responses):
    clients = []

    def factory(client_store, budget, **kwargs):
        client = FakeClient(client_store, budget, responses=responses)
        clients.append(client)
        return client

    result = capture.capture(store, client_factory=factory)
    return result, clients


def test_scoped_capture_acquires_live_history_and_authoritative_parents(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    result, clients = _capture(store, _responses())

    assert result["complete"] is True
    assert result["acquisition_protocol"] == 4
    assert result["scope"] == {
        "kind": "series",
        "series_ticker": "KXCPI",
        "include_multivariate": False,
    }
    requests = [
        (manifest["endpoint"], manifest["params"])
        for manifest, _payload in capture.capture_pages(store, result)
    ]
    assert ("/series/KXCPI", {}) in requests
    assert (
        "/events",
        {"series_ticker": "KXCPI", "with_nested_markets": "false", "limit": 200},
    ) in requests
    assert (
        "/markets",
        {"series_ticker": "KXCPI", "mve_filter": "exclude", "limit": 1000},
    ) in requests
    assert (
        "/historical/markets",
        {"series_ticker": "KXCPI", "mve_filter": "exclude", "limit": 1000},
    ) in requests
    assert (
        "/events/KXCPI-25JAN",
        {"exclude_markets": "true"},
    ) in requests
    assert len(clients) == 1

    state = capture.read_capture(store, result["capture_id"])
    assert state["task_scopes"] == {
        "1": "series",
        "2": "events_live",
        "3": "markets_live",
        "4": "markets_history",
    }
    identities = ParentIndex()
    for manifest, payload in capture.capture_pages(store, state):
        identities.add_page(manifest, payload)
    assert identities.scope_membership == {
        "series": {0: {"KXCPI"}},
        "events_live": {0: {"KXCPI-26JAN"}},
        "markets_live": {0: {"KXCPI-26JAN-CPI"}},
        "markets_history": {0: {"KXCPI-25JAN-CPI"}},
    }


def test_verified_empty_series_scopes_are_complete(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    result, _ = _capture(store, _responses(empty=True))

    assert result["complete"] is True
    assert [work["done"] for work in result["tasks"]] == [True] * 5


def test_series_event_traversal_follows_every_cursor(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    responses = _responses()

    def event_pages(params):
        if params.get("cursor") == "event-next":
            return 200, {"events": [HISTORICAL_EVENT], "cursor": ""}
        return 200, {"events": [LIVE_EVENT], "cursor": "event-next"}

    responses["/events"] = event_pages
    result, _ = _capture(store, responses)

    assert result["complete"] is True
    event_receipts = [
        manifest
        for manifest, _payload in capture.capture_pages(store, result)
        if manifest["endpoint"] == "/events"
    ]
    assert [manifest["params"].get("cursor") for manifest in event_receipts] == [None, "event-next"]


def test_event_from_abandoned_cursor_generation_needs_a_fresh_parent_lookup(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    event_calls = 0
    responses = _responses(empty=True)

    def paginated_events(params):
        nonlocal event_calls
        event_calls += 1
        if event_calls == 1:
            return 200, {"events": [LIVE_EVENT], "cursor": "reset-me"}
        if params.get("cursor"):
            return 400, {}
        return 200, {"events": [], "cursor": ""}

    responses["/events"] = paginated_events
    responses["/markets"] = (200, {"markets": [LIVE_MARKET], "cursor": ""})
    responses["/events/KXCPI-26JAN"] = (404, {})
    capture_ids: list[str] = []

    def factory(client_store, budget, **kwargs):
        return FakeClient(client_store, budget, responses=responses)

    with pytest.raises(CatalogueError, match="authoritative in-scope event definition"):
        capture.capture(store, client_factory=factory, _on_capture_id=capture_ids.append)

    assert len(capture_ids) == 1
    state = capture.read_capture(store, capture_ids[0])
    assert state["complete"] is False
    assert state["tasks"][2]["restarts"] == 1
    assert any(
        work["endpoint"] == "/events/KXCPI-26JAN" and work["done"] for work in state["tasks"]
    )


def test_scoped_event_payload_rejects_nested_combo_markets(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    responses = _responses()
    nested_combo = {
        "ticker": "KXCPI-26JAN-COMBO",
        "event_ticker": LIVE_EVENT["event_ticker"],
        "mve_collection_ticker": "COLLECTION",
        "mve_selected_legs": [],
    }
    responses["/events"] = (
        200,
        {"events": [{**LIVE_EVENT, "markets": [nested_combo]}], "cursor": ""},
    )

    with pytest.raises(CatalogueError, match="nested combo market"):
        _capture(store, responses)


def test_protocol_four_rejects_mutated_task_plan_and_global_traversals(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    with store.lock():
        state = capture.new_capture(store, None)
    malformed = copy.deepcopy(state)
    malformed["tasks"][2]["params"].pop("series_ticker")
    with pytest.raises(CatalogueError, match="sealed series plan"):
        capture._validate_logical_state(malformed, malformed["capture_id"])

    malformed = copy.deepcopy(state)
    malformed["tasks"][1] = capture.task("/series", "series")
    with pytest.raises(CatalogueError, match="sealed series plan"):
        capture._validate_logical_state(malformed, malformed["capture_id"])

    malformed = copy.deepcopy(state)
    malformed["tasks"].append(capture.task("/events/multivariate", "events"))
    with pytest.raises(CatalogueError, match="non-parent traversal"):
        capture._validate_logical_state(malformed, malformed["capture_id"])
