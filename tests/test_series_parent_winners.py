"""Scope proof must cover incoming parents and the winners retained from all history."""

from __future__ import annotations

import copy
import json

import conftest
import pytest
from conftest import CUTOFF, MARKET, FakeClient
from test_incremental_catalogue import (
    controlled_receipt_client,
    keyed,
    lower_observation_page_id,
    relation,
)

from kalshi_events import capture, publish
from kalshi_events.common import CatalogueError, Store

SERIES = {"ticker": "KXCPI", "title": "Inflation"}
OLD_EVENT = {"event_ticker": "E-OLD", "series_ticker": "KXCPI"}
NEW_EVENT = {"event_ticker": "E-NEW", "series_ticker": "KXCPI"}
OLD_MARKET = {**MARKET, "ticker": "M", "event_ticker": "E-OLD"}
NEW_MARKET = {**MARKET, "ticker": "M", "event_ticker": "E-NEW"}


class Source:
    def __init__(self, events, markets, lookups=None):
        self.events = events
        self.markets = markets
        self.lookups = lookups or {}
        self.requests = []

    def response(self, endpoint, params):
        self.requests.append((endpoint, copy.deepcopy(params)))
        responses = {
            "/historical/cutoff": (200, CUTOFF),
            "/series/KXCPI": (200, {"series": SERIES}),
            "/events": (200, {"events": self.events, "cursor": ""}),
            "/markets": (200, {"markets": self.markets, "cursor": ""}),
            "/historical/markets": (200, {"markets": [], "cursor": ""}),
            **self.lookups,
        }
        assert endpoint in responses, f"unexpected request: {endpoint}"
        return responses[endpoint]

    def factory(self, store, budget):
        source = self

        class Client(FakeClient):
            def response(self, endpoint, params):
                return source.response(endpoint, params)

        return Client(store, budget)


def _capture_at(
    store, source, *, started_at, received_at, incremental=False, client=None, **kwargs
):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(capture, "now", lambda: started_at)
        patch.setattr(conftest, "now", lambda: received_at)
        return capture.capture(
            store,
            incremental=incremental,
            client_factory=client or source.factory,
            **kwargs,
        )


def test_series_hydrates_incoming_parent_even_when_equal_time_history_winner_has_another_parent(
    tmp_path,
):
    store = Store(tmp_path / "root", series_ticker="KXCPI")
    timestamp = "2026-10-20T12:00:00Z"
    original = Source([OLD_EVENT], [OLD_MARKET])
    baseline = _capture_at(
        store,
        original,
        started_at=timestamp,
        received_at=timestamp,
        client=controlled_receipt_client(original, "/markets", "f" * 32, timestamp),
    )
    publish.publish(store, baseline["capture_id"], catalogue_version=2)
    old_winner = keyed(store, baseline["capture_id"], "current_markets", "market_ticker")["M"]
    losing_page = lower_observation_page_id(store, old_winner["observation_id"], "/markets/0")
    updated = Source([OLD_EVENT], [NEW_MARKET], {"/events/E-NEW": (200, {"event": NEW_EVENT})})

    delta = _capture_at(
        store,
        updated,
        started_at=timestamp,
        received_at=timestamp,
        incremental=True,
        client=controlled_receipt_client(updated, "/markets", losing_page, timestamp),
    )

    assert delta["complete"] is True
    assert ("/events/E-NEW", {"exclude_markets": "true"}) in updated.requests
    publication = publish.publish(store, delta["capture_id"], catalogue_version=2)
    winner = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M"]
    assert winner["observation_id"] == old_winner["observation_id"]
    assert winner["event_ticker"] == "E-OLD"
    assert winner["series_ticker"] == "KXCPI"
    assert winner["usable"] is True
    history = relation(store, delta["capture_id"], "observation_history")
    assert any(
        row["entity_kind"] == "markets"
        and row["entity_ticker"] == "M"
        and json.loads(row["payload_json"])["event_ticker"] == "E-NEW"
        for row in history
    )
    assert (
        publish.verify_release(store.path(f"releases/{publication['source_release_id']}"))[
            "series_ticker"
        ]
        == "KXCPI"
    )


def test_series_rejects_missing_parent_of_history_winner_after_receipt_clock_steps_back(tmp_path):
    store = Store(tmp_path / "root", series_ticker="KXCPI")
    original = Source([OLD_EVENT], [OLD_MARKET])
    baseline = _capture_at(
        store,
        original,
        started_at="2026-10-20T12:00:00Z",
        received_at="2026-10-20T12:00:02Z",
    )
    publication = publish.publish(store, baseline["capture_id"], catalogue_version=2)
    pointer = store.path("current-series.json").read_bytes()
    updated = Source([NEW_EVENT], [NEW_MARKET], {"/events/E-OLD": (404, {})})
    allocated = []

    with pytest.raises(CatalogueError, match="authoritative.*parent"):
        _capture_at(
            store,
            updated,
            started_at="2026-10-20T12:00:01Z",
            received_at="2026-10-20T12:00:01Z",
            incremental=True,
            _on_capture_id=allocated.append,
        )

    assert ("/events/E-OLD", {"exclude_markets": "true"}) in updated.requests
    assert len(allocated) == 1
    assert capture.read_capture(store, allocated[0])["complete"] is False
    with pytest.raises(CatalogueError, match="incomplete"):
        publish.publish(store, allocated[0], catalogue_version=2)
    assert store.path("current-series.json").read_bytes() == pointer
    assert (
        publish.current(store, catalogue_version=2)["source_release_id"]
        == publication["source_release_id"]
    )
