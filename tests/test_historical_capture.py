from __future__ import annotations

from copy import deepcopy

import pytest
from conftest import COMBO, HISTORICAL, FakeClient

from kalshi_events import capture
from kalshi_events.common import CatalogueError, Limits, Store
from kalshi_events.historical import (
    HISTORICAL_CONTRACT,
    HISTORICAL_VERSION,
    historical_selection,
    historical_task_indices,
    is_historical_capture,
)
from kalshi_events.normalize import project_pages


def _recording_factory(calls, responses=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            calls.append((endpoint, dict(params or {})))
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: Client(store, budget, responses, **options)


def test_default_capture_is_sealed_historical_plan_and_keeps_unresolved_combos(store):
    combo_history = {
        **COMBO,
        "ticker": "COMBO-HISTORY",
        "event_ticker": "COMBO-PARENT-MISSING",
        "status": "finalized",
        "result": "yes",
    }
    calls = []
    state = capture.capture(
        store,
        client_factory=_recording_factory(
            calls,
            {
                "/historical/markets": (
                    200,
                    {"markets": [HISTORICAL, combo_history], "cursor": ""},
                )
            },
        ),
    )

    assert state["complete"] is True
    assert is_historical_capture(state)
    assert state["parent_policy"] == "historical-only-v1"
    assert [work["endpoint"] for work in state["tasks"]] == [
        "/historical/cutoff",
        "/series",
        "/events",
        "/historical/markets",
    ]
    assert not any(endpoint in {"/events/multivariate", "/markets"} for endpoint, _ in calls)

    selection = historical_selection(state)
    assert selection == {
        "series": {"task_index": 1, "final_generation": 0},
        "events_live": {"task_index": 2, "final_generation": 0},
        "markets_history": {"task_index": 3, "final_generation": 0},
    }
    assert historical_task_indices(state) == {0, 1, 2, 3}

    projected = project_pages(
        capture.capture_pages(store, state, task_indices=historical_task_indices(state))
    )
    markets = {row["market_ticker"]: row for row in projected["markets"]}
    assert {"HISTORICAL", "COMBO-HISTORY"}.issubset(markets)
    assert markets["COMBO-HISTORY"]["event_ticker"] == "COMBO-PARENT-MISSING"
    assert markets["COMBO-HISTORY"]["usable"] is False
    assert projected["combo_legs"][0]["resolved"] is False
    assert HISTORICAL_CONTRACT == "oddsfox.kalshi.historical.v1"
    assert HISTORICAL_VERSION == 3


def test_legacy_incomplete_capture_requires_explicit_offline_profile(store):
    state = capture.capture(store, catalogue_profile="exchange", client_factory=FakeClient)
    legacy = deepcopy(state)
    legacy["complete"] = False
    legacy["tasks"][3]["done"] = False  # Excluded multivariate scope remains incomplete.

    with pytest.raises(CatalogueError, match="not eligible"):
        historical_selection(legacy)
    assert historical_task_indices(legacy, allow_legacy_incomplete=True) == {0, 1, 2, 5}


def test_historical_profile_is_unchangeable_and_selected_capture_stays_legacy(store):
    with pytest.raises(CatalogueError, match="catalogue profile"):
        capture.new_capture(store, None, catalogue_profile=[])

    historical = capture.new_capture(store, None)
    tampered = deepcopy(historical)
    tampered["tasks"][2]["endpoint"] = "/events/multivariate"
    with pytest.raises(CatalogueError, match="scope does not match|sealed task plan"):
        capture._validate_logical_state(tampered, tampered["capture_id"])

    selected = capture.new_capture(store, ["MARKET"])
    assert selected["parent_policy"] == "batched-v1"
    assert selected["selected"] == ["MARKET"]


def test_retired_capture_resume_stops_before_client_creation(store):
    state = capture.capture(
        Store(store.root, Limits(max_requests=1)),
        client_factory=FakeClient,
    )
    store.write(f"captures/{state['capture_id']}/retired-scopes.json", {"version": 1})

    with pytest.raises(CatalogueError, match="scopes were retired"):
        capture.capture(store, resume=state["capture_id"], client_factory=lambda *_: None)


def test_capture_page_filter_validates_indices(store):
    state = capture.new_capture(store, None)
    with pytest.raises(CatalogueError, match="invalid task index"):
        list(capture.capture_pages(store, state, task_indices={True}))
