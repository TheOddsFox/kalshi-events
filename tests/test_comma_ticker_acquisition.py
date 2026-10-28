"""Comma-bearing native tickers stay whole across selected and parent lookups."""

from __future__ import annotations

import copy
import json
from collections import Counter

import pytest
from conftest import CUTOFF, MARKET, FakeClient

from kalshi_events import capture, metadata
from kalshi_events.common import (
    CatalogueError,
    Store,
    ticker_from_path_segment,
    ticker_path_segment,
)

COMMA_TICKER = "JOBLESS-22SEP10-C220,000"
COMMA_EVENT = "JOBLESS-22SEP10"
COMMA_PARENT = "EVENT,COMMA"
NORMAL_TICKER = "NORMAL"
NORMAL_EVENT = "NORMAL-EVENT"
TRAILING_NORMAL_TICKER = "TRAILING-NORMAL"
TRAILING_NORMAL_EVENT = "TRAILING-NORMAL-EVENT"


class PowerLoss(BaseException):
    pass


def _responses(*, live=(200, None), historical=(404, None), selected_normals=()):
    comma_market = {**MARKET, "ticker": COMMA_TICKER, "event_ticker": COMMA_EVENT}
    normal_markets = {
        NORMAL_TICKER: {**MARKET, "ticker": NORMAL_TICKER, "event_ticker": NORMAL_EVENT},
        TRAILING_NORMAL_TICKER: {
            **MARKET,
            "ticker": TRAILING_NORMAL_TICKER,
            "event_ticker": TRAILING_NORMAL_EVENT,
        },
    }
    return {
        "/historical/cutoff": (200, CUTOFF),
        f"/markets/{ticker_path_segment(COMMA_TICKER)}": (
            live[0],
            {"market": comma_market} if live[0] == 200 else {},
        ),
        f"/historical/markets/{ticker_path_segment(COMMA_TICKER)}": (
            historical[0],
            {"market": comma_market} if historical[0] == 200 else {},
        ),
        "/markets": lambda params: (
            200,
            {
                "markets": [
                    normal_markets[value]
                    for value in params.get("tickers", "").split(",")
                    if value in selected_normals and value in normal_markets
                ],
                "cursor": "",
            },
        ),
        f"/events/{COMMA_EVENT}": (
            200,
            {"event": {"event_ticker": COMMA_EVENT, "series_ticker": "SERIES"}},
        ),
        f"/events/{NORMAL_EVENT}": (
            200,
            {"event": {"event_ticker": NORMAL_EVENT, "series_ticker": "SERIES"}},
        ),
        f"/events/{TRAILING_NORMAL_EVENT}": (
            200,
            {"event": {"event_ticker": TRAILING_NORMAL_EVENT, "series_ticker": "SERIES"}},
        ),
        "/series/SERIES": (200, {"series": {"ticker": "SERIES", "title": "Series"}}),
    }


def _factory(responses, calls, *, after_receipt=None):
    class Client(FakeClient):
        def __init__(self, store, budget, **options):
            super().__init__(store, budget, responses=responses, **options)

        def get(self, endpoint, params=None, context=None):
            call = (endpoint, dict(params or {}), dict(context or {}))
            calls.append(call)
            relative = super().get(endpoint, params, context)
            if after_receipt is not None:
                after_receipt(call, relative)
            return relative

    return Client


def _comma_tasks(state):
    return [
        work
        for work in state["tasks"]
        if work["endpoint"].startswith("/markets/") and work.get("requested") == COMMA_TICKER
    ]


@pytest.mark.parametrize(
    "selected",
    [[COMMA_TICKER], [NORMAL_TICKER, COMMA_TICKER, TRAILING_NORMAL_TICKER]],
)
def test_initial_plan_partitions_comma_ids_from_csv_selection(tmp_path, selected):
    state = capture.new_capture(Store(tmp_path / "root"), selected=selected)

    assert capture._validate_logical_state(state, state["capture_id"])
    assert state["selected"] == selected
    assert len(_comma_tasks(state)) == 1
    comma_task = _comma_tasks(state)[0]
    assert comma_task["endpoint"] == f"/markets/{ticker_path_segment(COMMA_TICKER)}"
    assert comma_task["singular"] is True
    assert comma_task["requested"] == COMMA_TICKER
    assert comma_task["params"] == {}
    batches = [work for work in state["tasks"] if work["endpoint"] == "/markets"]
    normals = [value for value in selected if "," not in value]
    assert [work["params"]["tickers"] for work in batches] == (
        [",".join(normals)] if normals else []
    )
    market_tasks = [work for work in state["tasks"] if work["kind"] == "markets"]
    assert [work["endpoint"] for work in market_tasks] == (
        ["/markets", f"/markets/{ticker_path_segment(COMMA_TICKER)}"]
        if normals
        else [f"/markets/{ticker_path_segment(COMMA_TICKER)}"]
    )


@pytest.mark.parametrize(
    "mutation", ["missing", "duplicate", "requested", "path", "params", "native-binding"]
)
def test_initial_all_comma_plan_rejects_missing_or_malformed_lookup(tmp_path, mutation):
    state = capture.new_capture(Store(tmp_path / "root"), selected=[COMMA_TICKER])
    malformed = copy.deepcopy(state)
    index = next(
        i for i, work in enumerate(malformed["tasks"]) if work.get("requested") == COMMA_TICKER
    )
    work = malformed["tasks"][index]
    if mutation == "missing":
        malformed["tasks"].pop(index)
    elif mutation == "duplicate":
        malformed["tasks"].append(copy.deepcopy(work))
    elif mutation == "requested":
        work["requested"] = "OTHER"
    elif mutation == "path":
        work["endpoint"] = "/markets/JOBLESS-22SEP10-C220,000"
    elif mutation == "params":
        work["params"] = {"limit": 1000}
    else:
        work["endpoint"] = "/markets/OTHER%2CTICKER"
        work["requested"] = "OTHER,TICKER"

    with pytest.raises(CatalogueError):
        capture._validate_logical_state(malformed, malformed["capture_id"])


def test_comma_ticker_and_two_tickers_have_distinct_selected_plans(tmp_path):
    single = capture.new_capture(Store(tmp_path / "single"), selected=["A,B"])
    pair = capture.new_capture(Store(tmp_path / "pair"), selected=["A", "B"])

    assert single["selected"] == ["A,B"]
    assert [(work["endpoint"], work["requested"]) for work in _comma_tasks_for(single)] == [
        ("/markets/A%2CB", "A,B")
    ]
    pair_batch = next(work for work in pair["tasks"] if work["endpoint"] == "/markets")
    assert pair_batch["params"]["tickers"] == "A,B"
    assert not any(work.get("requested") for work in pair["tasks"])


def _comma_tasks_for(state):
    return [
        work
        for work in state["tasks"]
        if work["endpoint"].startswith("/markets/") and work.get("requested") is not None
    ]


def test_mixed_selection_preserves_operator_order_and_found_coverage(store, tmp_path):
    selected = [NORMAL_TICKER, COMMA_TICKER, TRAILING_NORMAL_TICKER]
    calls = []
    state = capture.capture(
        store,
        selected=selected,
        client_factory=_factory(
            _responses(selected_normals={NORMAL_TICKER, TRAILING_NORMAL_TICKER}), calls
        ),
    )

    assert state["complete"]
    assert state["selected"] == selected
    assert all(state["coverage"][value]["status"] == "found" for value in selected)
    assert (
        "/markets",
        {"limit": 1000, "tickers": f"{NORMAL_TICKER},{TRAILING_NORMAL_TICKER}"},
    ) in [(endpoint, params) for endpoint, params, _ in calls]
    assert (
        sum(endpoint == f"/markets/{ticker_path_segment(COMMA_TICKER)}" for endpoint, _, _ in calls)
        == 1
    )
    assert all(COMMA_TICKER not in params.get("tickers", "") for _, params, _ in calls)

    output = tmp_path / "metadata"
    metadata.export(Store(store.root), selected, output, capture_id=state["capture_id"])
    rows = json.loads((output / "coverage.json").read_bytes())
    assert [row["market_ticker"] for row in rows] == selected
    assert [row["status"] for row in rows] == ["found", "found", "found"]


@pytest.mark.parametrize(
    ("historical", "expected"), [((200, None), "found"), ((404, None), "absent")]
)
def test_comma_live_404_uses_historical_lookup_and_fresh_cutoff(store, historical, expected):
    calls = []
    state = capture.capture(
        store,
        selected=[COMMA_TICKER],
        client_factory=_factory(_responses(live=(404, None), historical=historical), calls),
    )

    assert state["complete"]
    assert state["cutoff"] == CUTOFF
    assert state["coverage"][COMMA_TICKER]["status"] == expected
    counts = Counter(endpoint for endpoint, _, _ in calls)
    assert counts[f"/markets/{ticker_path_segment(COMMA_TICKER)}"] == 1
    assert counts[f"/historical/markets/{ticker_path_segment(COMMA_TICKER)}"] == 1
    assert counts["/historical/cutoff"] == 2  # initial cutoff plus the fallback's fresh cutoff
    assert not any(endpoint == "/markets" and "tickers" in params for endpoint, params, _ in calls)


def test_resume_adopts_orphan_comma_market_receipt_without_repeating_request(store):
    calls = []
    allocated = []
    orphan = []
    crash_once = [True]
    path = f"/markets/{ticker_path_segment(COMMA_TICKER)}"

    def after_receipt(call, relative):
        if call[0] == path and crash_once[0]:
            crash_once[0] = False
            orphan.append((call[2]["request_id"], relative))
            raise PowerLoss("receipt is durable before its journal entry")

    factory = _factory(
        _responses(live=(404, None), historical=(404, None)),
        calls,
        after_receipt=after_receipt,
    )
    with pytest.raises(PowerLoss):
        capture.capture(
            store,
            selected=[COMMA_TICKER],
            client_factory=factory,
            _on_capture_id=allocated.append,
        )

    assert len(allocated) == 1 and orphan
    resumed = capture.capture(Store(store.root), resume=allocated[0], client_factory=factory)

    assert resumed["complete"]
    assert resumed["coverage"][COMMA_TICKER]["status"] == "absent"
    assert (
        Counter(context["request_id"] for endpoint, _, context in calls if endpoint == path)[
            orphan[0][0]
        ]
        == 1
    )
    assert sum(endpoint == path for endpoint, _, _ in calls) == 1


@pytest.mark.parametrize("planner", ["ordinary", "multivariate", "scope"])
def test_comma_event_parent_is_never_in_parent_csv_batch(tmp_path, planner):
    if planner == "scope":
        scoped_store = Store(tmp_path / "scope", series_ticker="SERIES")
        with scoped_store.lock():
            state = capture.new_capture(scoped_store, selected=None)
    else:
        state = capture.new_capture(Store(tmp_path / "root"), selected=None)
    parents = [f"EVENT-A-{index:02d}" for index in range(19)] + [
        COMMA_PARENT,
        *[f"EVENT-Z-{index:02d}" for index in range(19)],
    ]
    tasks = (
        capture.multivariate_parent_plan(state, parents)
        if planner == "multivariate"
        else capture.parent_lookup_plan(state, parents, [])
    )

    expected_batch = sorted(value for value in parents if "," not in value)
    expected_role = "multivariate" if planner == "multivariate" else "ordinary"
    expected_endpoint = "/events/multivariate" if planner == "multivariate" else "/events"
    batches = [work for work in tasks if work.get("parent_batch")]
    singles = [work for work in tasks if work.get("singular")]
    assert len(tasks) == len(batches) + len(singles) == 2
    assert len(batches) == len(singles) == 1
    assert batches[0]["parent_batch"] == expected_role
    assert batches[0]["endpoint"] == expected_endpoint
    assert batches[0]["params"] == {
        "tickers": ",".join(expected_batch),
        "limit": 200,
        "with_nested_markets": "false",
    }
    assert singles[0]["endpoint"] == f"/events/{ticker_path_segment(COMMA_PARENT)}"
    assert singles[0]["params"] == {"exclude_markets": "true"}
    assert [work["endpoint"] for work in tasks] == [
        expected_endpoint,
        f"/events/{ticker_path_segment(COMMA_PARENT)}",
    ]
    if planner == "scope":
        assert capture._is_scoped_capture(state)
        assert capture.multivariate_parent_plan(state, parents) == []
    else:
        assert not capture._is_scoped_capture(state)
    for work in tasks:
        context = capture._request_context_for_task(state, len(state["tasks"]), work)
        assert ("series_scope" in context) is (planner == "scope")
        if work.get("parent_batch"):
            assert context["parent_batch"] == expected_role
            assert context["parent_batch_tickers"] == ",".join(expected_batch)

    acquired = []
    for work in tasks:
        if work.get("parent_batch"):
            values = work["params"]["tickers"].split(",")
            assert all("," not in value for value in values)
            assert COMMA_PARENT not in values
            acquired.extend(values)
        else:
            assert work["singular"]
            acquired.append(ticker_from_path_segment(work["endpoint"].removeprefix("/events/")))
    assert sorted(acquired) == sorted(parents)
    assert any(work.get("parent_batch") for work in tasks)
