from __future__ import annotations

import copy

import pytest

from kalshi_events import capture
from kalshi_events.common import CatalogueError, Store, series_scope
from kalshi_events.incremental import root_id


def test_protocol_absence_and_catalogue_protocols_remain_compatible():
    assert capture._acquisition_protocol({}) == 2
    assert capture._acquisition_protocol({"acquisition_protocol": 3}) == 3

    for value in (True, False, 2, 5, 3.0, "4"):
        with pytest.raises(CatalogueError, match="unsupported acquisition protocol"):
            capture._acquisition_protocol({"acquisition_protocol": value})


def test_protocol_four_requires_an_exact_series_scope():
    scope = series_scope("KXCPI")
    assert capture._acquisition_protocol({"acquisition_protocol": 4, "scope": scope}) == 4

    for malformed in (
        None,
        {**scope, "include_multivariate": True},
        {**scope, "extra": "value"},
        {**scope, "series_ticker": "bad ticker"},
    ):
        with pytest.raises(CatalogueError):
            capture._acquisition_protocol({"acquisition_protocol": 4, "scope": malformed})

    with pytest.raises(CatalogueError, match="requires acquisition protocol 4"):
        capture._acquisition_protocol({"acquisition_protocol": 3, "scope": scope})
    with pytest.raises(CatalogueError, match="lacks its acquisition protocol"):
        capture._acquisition_protocol({"scope": scope})


def test_scoped_request_and_receipt_identity_is_bound_to_scope():
    state = {
        "capture_id": "a" * 32,
        "acquisition_protocol": 4,
        "scope": series_scope("KXCPI"),
        "tasks": [capture.task("/series/KXCPI", "series", singular=True)],
        "task_scopes": {"0": "series"},
    }
    context = capture._request_context(state, 0)
    assert context["series_scope"] == state["scope"]
    assert context["acquisition_protocol"] == 4
    assert context["task_scope"] == "series"

    manifest = copy.deepcopy(context)
    assert capture._receipt_matches_context(manifest, context)
    manifest["series_scope"] = series_scope("KXGDP")
    assert not capture._receipt_matches_context(manifest, context)


def test_scoped_capture_requires_same_root_identity(tmp_path):
    store = Store(tmp_path / "scoped", series_ticker="KXCPI")
    with store.lock():
        identity = root_id(store, create=True)
        state = {"scope": series_scope("KXCPI"), "root_id": identity}
        capture._validate_capture_root_scope(store, state)

        with pytest.raises(CatalogueError, match="different logical operator root"):
            capture._validate_capture_root_scope(
                store, {"scope": series_scope("KXCPI"), "root_id": "b" * 32}
            )


def test_scoped_capture_seals_protocol_scope_and_exact_series_tasks(tmp_path):
    store = Store(tmp_path / "scoped", series_ticker="KXCPI")
    with store.lock():
        state = capture.new_capture(store, None)

    assert state["acquisition_protocol"] == 4
    assert state["scope"] == series_scope("KXCPI")
    assert state["root_id"]
    assert [work["endpoint"] for work in state["tasks"]] == [
        "/historical/cutoff",
        "/series/KXCPI",
        "/events",
        "/markets",
        "/historical/markets",
    ]
    assert state["tasks"][2]["params"] == {
        "series_ticker": "KXCPI",
        "with_nested_markets": "false",
        "limit": capture.EVENT_PAGE_LIMIT,
    }
    assert state["tasks"][3]["params"] == {
        "series_ticker": "KXCPI",
        "mve_filter": "exclude",
        "limit": capture.MARKET_PAGE_LIMIT,
    }
    assert state["tasks"][4]["params"] == state["tasks"][3]["params"]
    assert state["task_scopes"] == {
        "1": "series",
        "2": "events_live",
        "3": "markets_live",
        "4": "markets_history",
    }
    with pytest.raises(CatalogueError, match="full series acquisition"):
        capture.new_capture(store, ["KXCPI-MARKET"])
