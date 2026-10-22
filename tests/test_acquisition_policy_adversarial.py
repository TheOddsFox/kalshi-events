"""Cutoff recovery and parent batches must not turn partial evidence into authority."""

from __future__ import annotations

import copy
import json
import shutil
from collections import Counter

import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, metadata
from kalshi_events.common import (
    Budget,
    BudgetExceeded,
    CatalogueError,
    Store,
    digest,
    json_bytes,
    series_scope,
)
from kalshi_events.normalize import ParentIndex, observations

PARENTS = [f"EVENT-{index:03d}" for index in range(20)]
SELECTED = [f"MARKET-{index:03d}" for index in range(20)]
EVENTS = [{"event_ticker": value, "series_ticker": "SERIES"} for value in PARENTS]
MARKETS = [
    {"ticker": market, "event_ticker": event}
    for market, event in zip(SELECTED, PARENTS, strict=True)
]


def _index(pages):
    result = ParentIndex()
    for manifest, payload in pages:
        result.add_page(manifest, payload)
    return result


def _parent_factory(response, calls):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            calls.append((endpoint, dict(params or {}), dict(context or {})))
            return super().get(endpoint, params, context)

        def response(self, endpoint, params):
            if endpoint == "/historical/cutoff":
                return 200, CUTOFF
            if endpoint == "/markets":
                return 200, {"markets": MARKETS, "cursor": ""}
            if endpoint == "/series/SERIES":
                return 200, {"series": {"ticker": "SERIES"}}
            return response(endpoint, params)

    return Client


def test_parent_batch_early_page_success_survives_empty_terminal_page(store):
    calls = []

    def response(endpoint, params):
        assert endpoint == "/events"
        if params.get("cursor"):
            return 200, {"events": [], "cursor": ""}
        return 200, {"events": EVENTS, "cursor": "LAST"}

    state = capture.capture(
        store, selected=SELECTED, client_factory=_parent_factory(response, calls)
    )
    assert state["complete"]
    pages = list(capture.capture_pages(store, state))
    index = _index(pages)
    first = next(
        manifest
        for manifest, payload in pages
        if manifest.get("parent_batch") == "ordinary" and payload.get("events")
    )
    assert not any(endpoint.startswith("/events/") for endpoint, _, _ in calls)
    assert {key for key in index.effective_parent_evidence(state)[1] if key[0] == "events"} == {
        ("events", event) for event in PARENTS
    }
    assert all(
        index.effective_parent_evidence(state)[1][("events", event)]
        == (200, first["page_id"], first["request_id"])
        for event in PARENTS
    )
    batch_pages = [manifest for manifest, _ in pages if manifest.get("parent_batch")]
    assert len(batch_pages) == 2
    assert all("task_scope" not in manifest for manifest in batch_pages)
    assert all(manifest["parent_batch_tickers"] == ",".join(PARENTS) for manifest in batch_pages)


def test_one_hundred_present_event_parents_use_one_ordinary_list_request(store):
    parents = [f"PARENT-{index:03d}" for index in range(100)]
    selected = [f"SELECTED-{index:03d}" for index in range(100)]
    calls = []

    class Client(FakeClient):
        def response(self, endpoint, params):
            calls.append((endpoint, dict(params)))
            if endpoint == "/markets":
                return 200, {
                    "markets": [
                        {"ticker": market, "event_ticker": event}
                        for market, event in zip(selected, parents, strict=True)
                    ],
                    "cursor": "",
                }
            if endpoint == "/events":
                assert params["tickers"] == ",".join(parents)
                return 200, {
                    "events": [
                        {"event_ticker": value, "series_ticker": "SERIES"} for value in parents
                    ],
                    "cursor": "",
                }
            if endpoint == "/historical/cutoff":
                return 200, CUTOFF
            assert endpoint == "/series/SERIES"
            return 200, {"series": {"ticker": "SERIES"}}

    state = capture.capture(store, selected=selected, client_factory=Client)
    assert state["complete"]
    assert Counter(endpoint for endpoint, _ in calls)["/events"] == 1
    assert not any(endpoint.startswith("/events/") for endpoint, _ in calls)
    lookups = _index(list(capture.capture_pages(store, state))).effective_parent_evidence(state)[1]
    assert all(lookups[("events", event)][0] == 200 for event in parents)


def test_reset_parent_observations_remain_history_but_do_not_override_final_404(store, tmp_path):
    calls = []
    first_generation = True

    def response(endpoint, params):
        nonlocal first_generation
        if endpoint == "/events":
            if params.get("cursor"):
                first_generation = False
                return 400, {}
            return 200, {
                "events": EVENTS if first_generation else [],
                "cursor": "BAD" if first_generation else "",
            }
        if endpoint == "/events/multivariate":
            return 200, {"events": [], "cursor": ""}
        assert endpoint.startswith("/events/")
        return 404, {}

    state = capture.capture(
        store, selected=SELECTED, client_factory=_parent_factory(response, calls)
    )
    assert state["complete"]
    pages = list(capture.capture_pages(store, state))
    historical_events = [row for row in observations(pages) if row["entity_kind"] == "events"]
    assert {row["event_ticker"] for row in historical_events} == set(PARENTS)
    index = _index(pages)
    assert all(index.parent_lookups[("events", event)][0] == 404 for event in PARENTS)
    assert Counter(
        endpoint for endpoint, _, _ in calls if endpoint.startswith("/events/")
    ) == Counter({"/events/multivariate": 1, **{f"/events/{event}": 1 for event in PARENTS}})
    batch_generations = {
        manifest["task_generation"]
        for manifest, _ in pages
        if manifest.get("parent_batch") == "ordinary"
    }
    assert batch_generations == {0, 1}
    output = tmp_path / "metadata"
    metadata.export(Store(store.root), SELECTED, output, capture_id=state["capture_id"])
    assert {row["status"] for row in json.loads((output / "coverage.json").read_bytes())} == {
        "unresolved"
    }
    assert json.loads((output / "markets.json").read_bytes()) == []


def test_parent_list_omission_requires_singular_authoritative_success(store):
    calls = []

    def response(endpoint, params):
        if endpoint in {"/events", "/events/multivariate"}:
            return 200, {"events": [], "cursor": ""}
        event = endpoint.removeprefix("/events/")
        assert event in PARENTS
        return 200, {"event": {"event_ticker": event, "series_ticker": "SERIES"}}

    state = capture.capture(
        store, selected=SELECTED, client_factory=_parent_factory(response, calls)
    )
    assert state["complete"]
    index = _index(list(capture.capture_pages(store, state)))
    assert all(index.parent_lookups[("events", event)][0] == 200 for event in PARENTS)
    assert Counter(endpoint for endpoint, _, _ in calls)["/events/multivariate"] == 1
    assert all(
        Counter(endpoint for endpoint, _, _ in calls)[f"/events/{event}"] == 1 for event in PARENTS
    )


@pytest.mark.parametrize("foreign", ["UNREQUESTED", "EVENT-020"])
def test_parent_batch_unrequested_identity_cannot_be_committed(store, foreign):
    calls = []
    allocated = []

    def response(endpoint, params):
        assert endpoint == "/events"
        return 200, {"events": [{"event_ticker": foreign, "series_ticker": "SERIES"}], "cursor": ""}

    with pytest.raises(CatalogueError, match="unrequested"):
        capture.capture(
            store,
            selected=SELECTED,
            client_factory=_parent_factory(response, calls),
            _on_capture_id=allocated.append,
        )
    assert len(allocated) == 1
    state = capture.read_capture(Store(store.root), allocated[0])
    assert not state["complete"]
    assert not any(
        manifest.get("parent_batch") for manifest, _ in capture.capture_pages(store, state)
    )
    assert any(
        json.loads(path.read_bytes()).get("parent_batch")
        for path in store.path("raw").glob("*/manifest.json")
    )


def _historical_factory(calls, *, terminal_stop=None, after_receipt=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            call = (endpoint, dict(params or {}), dict(context or {}))
            calls.append(call)
            if (
                terminal_stop is not None
                and context.get("cutoff_reason") == "terminal"
                and terminal_stop[0]
            ):
                terminal_stop[0] = False
                raise BudgetExceeded("stop at terminal cutoff")
            relative = super().get(endpoint, params, context)
            if after_receipt is not None:
                after_receipt(call, relative)
            return relative

        def response(self, endpoint, params):
            if endpoint == "/historical/cutoff":
                return 200, CUTOFF
            if endpoint == "/series":
                return 200, {"series": []}
            if endpoint in {"/events", "/events/multivariate"}:
                return 200, {"events": [], "cursor": ""}
            if endpoint == "/markets":
                return 200, {"markets": [], "cursor": ""}
            assert endpoint == "/historical/markets"
            return 200, {"markets": [], "cursor": "" if params.get("cursor") else "LAST"}

    return Client


def test_terminal_cutoff_failure_keeps_capture_incomplete_and_resume_reuses_data_pages(store):
    calls = []
    stop = [True]
    client = _historical_factory(calls, terminal_stop=stop)
    stopped = capture.capture(store, client_factory=client)
    assert not stopped["complete"]
    assert stopped["budget_exhausted"]
    first_data_requests = [
        context["request_id"] for endpoint, _, context in calls if endpoint == "/historical/markets"
    ]
    assert len(first_data_requests) == 2

    resumed = capture.capture(
        Store(store.root), resume=stopped["capture_id"], client_factory=client
    )
    assert resumed["complete"]
    assert [
        context["request_id"] for endpoint, _, context in calls if endpoint == "/historical/markets"
    ] == first_data_requests
    pages = list(capture.capture_pages(Store(store.root), resumed))
    checkpoints = [manifest for manifest, _ in pages if manifest.get("cutoff_reason")]
    assert {manifest["cutoff_reason"] for manifest in checkpoints} >= {"start", "terminal"}
    assert len({manifest["request_id"] for manifest in checkpoints}) == len(checkpoints)
    assert len({manifest["cutoff_ordinal"] for manifest in checkpoints}) == len(checkpoints)


class PowerLoss(BaseException):
    pass


def test_resume_adopts_orphan_start_cutoff_before_requesting_fresh_resume_cutoff(store):
    calls = []
    orphan = []
    allocated = []

    def interrupt(call, relative):
        if call[2].get("cutoff_reason") == "start" and not orphan:
            orphan.append((call[2]["request_id"], relative))
            raise PowerLoss("cutoff receipt durable before page journal")

    with pytest.raises(PowerLoss):
        capture.capture(
            store,
            client_factory=_historical_factory(calls, after_receipt=interrupt),
            _on_capture_id=allocated.append,
        )
    assert orphan and len(allocated) == 1
    before_resume = len(calls)
    resumed = capture.capture(
        Store(store.root), resume=allocated[0], client_factory=_historical_factory(calls)
    )
    assert resumed["complete"]
    assert Counter(context["request_id"] for _, _, context in calls)[orphan[0][0]] == 1
    subsequent = calls[before_resume:]
    data_position = next(
        index
        for index, (endpoint, _, _) in enumerate(subsequent)
        if endpoint == "/historical/markets"
    )
    assert any(
        context.get("cutoff_reason") == "resume" for _, _, context in subsequent[:data_position]
    )
    receipts = [manifest for manifest, _ in capture.capture_pages(Store(store.root), resumed)]
    assert Counter(manifest["request_id"] for manifest in receipts)[orphan[0][0]] == 1


def test_checkpoint_reason_and_ordinal_are_durably_recoverable_before_dispatch(store, monkeypatch):
    calls = []
    observed = []
    parent = _historical_factory(calls)

    class Client(parent):
        def get(self, endpoint, params=None, context=None):
            if context.get("cutoff_reason"):
                recovered = capture.read_capture(Store(self.store.root), context["capture_id"])
                work = recovered["tasks"][context["task_index"]]
                assert work["cutoff_reason"] == context["cutoff_reason"]
                assert work["cutoff_ordinal"] == context["cutoff_ordinal"]
                assert (
                    capture._request_context(recovered, context["task_index"])["request_id"]
                    == context["request_id"]
                )
                if context["cutoff_reason"] == "terminal":
                    assert work["data_done"] and not work["done"]
                observed.append(context["cutoff_reason"])
            return super().get(endpoint, params, context)

    # One worker makes this a durable-state assertion without a concurrent journal reader race.
    monkeypatch.setattr(capture, "MAX_CONCURRENT_REQUESTS", 1)
    state = capture.capture(store, client_factory=Client)
    assert state["complete"]
    assert observed == ["start", "terminal"]


def _policy_state(*, legacy=False, scoped=False):
    policies = {
        "rate_policy": "fixed",
        "parent_policy": "singular-v1" if legacy else "batched-v1",
        "cutoff_policy": "per-page-v1" if legacy else "checkpoint-v1",
    }
    state = capture._CaptureState(
        {
            "capture_id": "c" * 32,
            "complete": False,
            "selected": None,
            "tasks": [],
            "task_scopes": {},
            "coverage": {},
            "acquisition_protocol": 5,
            **policies,
            "acquisition_policy_sha256": digest(json_bytes(policies)),
        }
    )
    if scoped:
        state["scope"] = series_scope("SERIES")
    return state


def test_ten_thousand_historical_pages_use_at_most_102_total_cutoff_requests():
    state = _policy_state()
    work = capture._protocol_task(state, "/historical/markets", "markets", params={"limit": 1000})
    state["tasks"].append(work)
    seen = {}
    checkpoints = []

    def consume_cutoff():
        context = capture._request_context(state, 0)
        checkpoints.append(context)
        change = capture._derive_page_change(
            state, seen, 0, {"status": 200, "received_at": "2026-01-01T00:00:00Z"}, CUTOFF
        )
        capture._apply_page_change(state, seen, 0, change)

    consume_cutoff()
    for page in range(10_000):
        assert not work["cutoff_due"]
        change = capture._derive_page_change(
            state,
            seen,
            0,
            {"status": 200},
            {"markets": [], "cursor": str(page + 1) if page < 9_999 else ""},
        )
        capture._apply_page_change(state, seen, 0, change)
        if work["cutoff_due"]:
            if page == 9_999:
                assert work["data_done"] and not work["done"]
            consume_cutoff()
    assert work["done"]
    # One initial global cutoff is outside this traversal's start/periodic/terminal schedule.
    assert len(checkpoints) + 1 == 102
    assert 1 - (len(checkpoints) + 1) / 10_001 >= 0.98
    assert Counter(context["cutoff_reason"] for context in checkpoints) == {
        "start": 1,
        "pages": 99,
        "terminal": 1,
    }
    assert len({context["request_id"] for context in checkpoints}) == len(checkpoints)
    assert [context["cutoff_ordinal"] for context in checkpoints] == list(range(1, 102))


def test_elapsed_cutoff_trigger_uses_monotonic_time_at_exact_300_second_boundary(monkeypatch):
    state = _policy_state()
    state["tasks"].append(capture._protocol_task(state, "/historical/markets", "markets"))
    elapsed = [500.0]
    utc = ["2026-02-01T00:00:00Z"]
    monkeypatch.setattr(capture.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(capture, "now", lambda: utc[0])
    change = capture._derive_page_change(
        state, {}, 0, {"status": 200, "received_at": "2001-01-01T00:00:00Z"}, CUTOFF
    )
    capture._apply_page_change(state, {}, 0, change)
    assert not capture._interval_cutoff_due(state, 0)
    elapsed[0] = 799.999
    utc[0] = "2025-01-01T00:00:00Z"
    assert not capture._interval_cutoff_due(state, 0)
    elapsed[0] = 800.0
    assert capture._interval_cutoff_due(state, 0)


def test_post_append_cutoff_schedule_failure_latches_uncertainty_and_recovers(store, monkeypatch):
    calls = []
    client_factory = _historical_factory(calls)
    identities = ParentIndex()
    with store.lock():
        state = capture.new_capture(store, None, compression="identity", rate_policy="fixed")
        index = next(
            i for i, work in enumerate(state["tasks"]) if work["endpoint"] == "/historical/markets"
        )
        client = client_factory(store, Budget(store))
        for _ in range(2):
            context = capture._request_context(state, index)
            endpoint, params, _, _ = capture._plan(
                state["tasks"][index], capture._phase(state["tasks"][index])
            )
            relative = client.get(endpoint, params, context)
            capture._commit_page_v2(
                store, state, index, relative, context["request_id"], identities
            )
        first_request_ids = [context["request_id"] for _, _, context in calls]

        def stop_after_durable_control(*_args):
            raise PowerLoss("control appended before checkpoint application completed")

        with monkeypatch.context() as fault:
            fault.setattr(capture, "_after_transition", stop_after_durable_control)
            with pytest.raises(PowerLoss):
                capture._schedule_cutoff(store, state, index, "resume")
        assert state.journal_uncertain
        with pytest.raises(CatalogueError, match="uncertain"):
            capture._append_v2_record(store, state, "control", {})
        durable = [
            json.loads(line)
            for line in store.path(f"captures/{state['capture_id']}/pages.jsonl")
            .read_bytes()
            .splitlines()
        ]
        schedule = durable[-1]
        assert schedule["op"] == "control"
        assert schedule["payload"]["kind"] == "cutoff_schedule"
        assert schedule["payload"]["reason"] == "resume"

    recovered = capture.read_capture(Store(store.root), state["capture_id"])
    resumed_context = capture._request_context(recovered, index)
    assert resumed_context["cutoff_reason"] == "resume"
    assert resumed_context["cutoff_ordinal"] == 2
    resumed = capture.capture(
        Store(store.root), resume=state["capture_id"], client_factory=client_factory
    )
    assert resumed["complete"]
    requests = Counter(context["request_id"] for _, _, context in calls)
    assert all(requests[request_id] == 1 for request_id in first_request_ids)
    assert requests[resumed_context["request_id"]] == 1


class _NoIterationList(list):
    def __iter__(self):
        raise AssertionError("fresh receipt binding scanned unrelated capture tasks")

    def copy(self):
        raise AssertionError("fresh receipt binding copied unrelated capture tasks")


def test_fresh_receipt_binding_does_not_iterate_or_copy_one_hundred_thousand_tasks():
    state = _policy_state()
    unrelated = capture.task("/events/UNRELATED", "events", singular=True)
    state["tasks"] = _NoIterationList([unrelated] * 100_000)
    index = 37
    work = capture._protocol_task(
        state,
        "/events",
        "events",
        parent_batch="ordinary",
        params={"tickers": ",".join(PARENTS), "limit": 200, "with_nested_markets": "false"},
    )
    state["tasks"][index] = work
    context = capture._request_context(state, index)
    manifest = {**context, "endpoint": "/events", "params": dict(work["params"])}
    before = copy.deepcopy(work)
    tasks = state["tasks"]

    capture._validate_receipt_task_binding(
        manifest, state, {"index": index, "request_id": context["request_id"]}
    )

    assert state["tasks"] is tasks
    assert state["tasks"][index] == before


def test_final_batch_eligibility_keeps_all_history_winner_parent_during_clock_rollback():
    state = _policy_state()
    state["journal_version"] = capture.JOURNAL_VERSION
    work = capture._protocol_task(
        state,
        "/events",
        "events",
        parent_batch="ordinary",
        params={"tickers": ",".join(PARENTS), "limit": 200, "with_nested_markets": "false"},
    )
    state["tasks"].append(work)
    identities = ParentIndex()
    first = {
        **capture._request_context(state, 0),
        "status": 200,
        "page_id": "a" * 32,
        "received_at": "2026-02-02T00:00:00Z",
        "endpoint": "/events",
        "params": dict(work["params"]),
    }
    identities.add_page(
        first,
        {
            "events": [{"event_ticker": value, "series_ticker": "PARENT-A"} for value in PARENTS],
            "cursor": "INVALIDATED",
        },
    )
    work["restarts"] = 1
    last = {
        **capture._request_context(state, 0),
        "status": 200,
        "page_id": "b" * 32,
        "received_at": "2026-02-01T00:00:00Z",
        "endpoint": "/events",
        "params": dict(work["params"]),
    }
    identities.add_page(
        last,
        {
            "events": [{"event_ticker": value, "series_ticker": "PARENT-B"} for value in PARENTS],
            "cursor": "",
        },
    )
    work.update(done=True, batch_found=list(PARENTS))
    identities.markets = {
        market: ("2026-02-01T00:00:00Z", "m" * 64, parent)
        for market, parent in zip(SELECTED, PARENTS, strict=True)
    }

    events, eligible = identities.effective_parent_evidence(state)
    assert {winner[2] for winner in events.values()} == {"PARENT-A"}
    assert all(
        eligible[("events", event)] == (200, last["page_id"], last["request_id"])
        for event in PARENTS
    )
    assert capture._hydrate_pages(None, state, identities)
    series_tasks = [work["endpoint"] for work in state["tasks"] if work["kind"] == "series"]
    assert series_tasks == ["/series/PARENT-A"]


def test_scoped_parent_batches_reject_foreign_series_and_skip_multivariate():
    state = _policy_state(scoped=True)
    work = capture._protocol_task(
        state,
        "/events",
        "events",
        parent_batch="ordinary",
        params={"tickers": ",".join(PARENTS), "limit": 200, "with_nested_markets": "false"},
    )
    state["tasks"].append(work)
    with pytest.raises(CatalogueError, match="scope"):
        capture._derive_page_change(
            state,
            {},
            0,
            {"status": 200},
            {"events": [{"event_ticker": PARENTS[0], "series_ticker": "FOREIGN"}], "cursor": ""},
        )
    change = capture._derive_page_change(
        state, {}, 0, {"status": 200}, {"events": [], "cursor": ""}
    )
    assert len(change["tasks"]) == len(PARENTS)
    assert all(
        work["singular"] and work["endpoint"].startswith("/events/") for work in change["tasks"]
    )
    assert not any(work.get("parent_batch") for work in change["tasks"])


def test_deriving_parent_batch_page_does_not_mutate_nested_applied_task_state():
    state = _policy_state()
    work = capture._protocol_task(
        state,
        "/events",
        "events",
        parent_batch="ordinary",
        params={"tickers": ",".join(PARENTS), "limit": 200, "with_nested_markets": "false"},
    )
    work.update(cursor="PREVIOUS", batch_found=[PARENTS[0]])
    state["tasks"].append(work)
    before = copy.deepcopy(dict(state))
    applied_found = work["batch_found"]
    applied_params = work["params"]
    seen = {0: {digest(b"PREVIOUS")}}
    previous_cursors = copy.deepcopy(seen)

    change = capture._derive_page_change(
        state,
        seen,
        0,
        {"status": 200},
        {"events": [EVENTS[1]], "cursor": "NEXT"},
    )

    assert state == before
    assert seen == previous_cursors
    assert work["batch_found"] is applied_found
    assert work["params"] is applied_params
    assert change["task"]["batch_found"] == PARENTS[:2]
    assert change["task"]["batch_found"] is not applied_found


@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_batch", "multivariate"),
        ("parent_batch_id", "0" * 64),
        ("parent_batch_tickers", ",".join(reversed(PARENTS))),
        ("task_generation", True),
    ],
)
def test_parent_batch_receipt_context_cannot_change_role_selection_or_generation(field, value):
    state = _policy_state()
    work = capture._protocol_task(
        state,
        "/events",
        "events",
        parent_batch="ordinary",
        params={"tickers": ",".join(PARENTS), "limit": 200, "with_nested_markets": "false"},
    )
    work["restarts"] = 1
    state["tasks"].append(work)
    context = capture._request_context(state, 0)
    manifest = {**context, field: value}
    assert not capture._receipt_matches_context(manifest, context)


@pytest.mark.parametrize("field,value", [("cutoff_reason", "terminal"), ("cutoff_ordinal", True)])
def test_checkpoint_receipt_context_cannot_change_reason_or_ordinal(field, value):
    state = _policy_state()
    state["tasks"].append(capture._protocol_task(state, "/historical/markets", "markets"))
    context = capture._request_context(state, 0)
    assert not capture._receipt_matches_context({**context, field: value}, context)


def test_existing_protocol5_per_page_cutoffs_and_singular_parents_stay_sealed():
    state = _policy_state(legacy=True)
    work = capture._protocol_task(state, "/historical/markets", "markets")
    state["tasks"].append(work)
    assert "cutoff_reason" not in work
    for cursor in ("NEXT", ""):
        assert work["cutoff_due"]
        change = capture._derive_page_change(state, {}, 0, {"status": 200}, CUTOFF)
        capture._apply_page_change(state, {}, 0, change)
        change = capture._derive_page_change(
            state, {}, 0, {"status": 200}, {"markets": [], "cursor": cursor}
        )
        capture._apply_page_change(state, {}, 0, change)
    assert work["done"] and not work["cutoff_due"]
    assert "cutoff_ordinal" not in capture._request_context(state, 0)
    parents = capture.parent_lookup_plan(state, PARENTS, [])
    assert len(parents) == len(PARENTS)
    assert all(work["singular"] and not work.get("parent_batch") for work in parents)


@pytest.mark.parametrize(
    "count,batch_sizes,singular_count",
    [
        (19, [], 19),
        (20, [20], 0),
        (100, [100], 0),
        (101, [100], 1),
        (119, [100], 19),
        (120, [100, 20], 0),
    ],
)
def test_parent_batch_boundaries_preserve_every_sorted_identity(count, batch_sizes, singular_count):
    state = _policy_state()
    selected = [f"EVENT-{index:03d}" for index in reversed(range(count))]
    tasks = capture.parent_lookup_plan(state, selected, [])
    batches = [work for work in tasks if work.get("parent_batch")]
    singles = [work for work in tasks if work["singular"]]
    assert [len(work["params"]["tickers"].split(",")) for work in batches] == batch_sizes
    assert len(singles) == singular_count
    acquired = [value for work in batches for value in work["params"]["tickers"].split(",")]
    acquired.extend(work["endpoint"].rsplit("/", 1)[1] for work in singles)
    assert acquired == sorted(selected)


def test_url_limited_parent_batches_keep_small_tails_as_singular_lookups():
    selected = [f"E{index:03d}-" + "X" * 251 for index in range(100)]
    tasks = capture.parent_lookup_plan(_policy_state(), selected, [])
    acquired = []
    for work in tasks:
        if work.get("parent_batch"):
            values = work["params"]["tickers"].split(",")
            assert 20 <= len(values) <= 100
            assert capture._event_batch_url_size(values, multivariate=False) <= 6144
            acquired.extend(values)
        else:
            assert work["singular"]
            acquired.append(work["endpoint"].rsplit("/", 1)[1])
    assert acquired == selected
    assert any(work["singular"] for work in tasks)


@pytest.fixture(scope="module")
def anchored_policy_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("anchored-acquisition-policy") / "root"

    class Client(FakeClient):
        def response(self, endpoint, params):
            if endpoint == "/historical/cutoff":
                return 200, CUTOFF
            if endpoint == "/series":
                return 200, {"series": [{"ticker": "SERIES"}]}
            if endpoint == "/series/SERIES":
                return 200, {"series": {"ticker": "SERIES"}}
            if endpoint == "/events":
                return 200, {"events": EVENTS if params.get("tickers") else [], "cursor": ""}
            if endpoint == "/events/multivariate":
                return 200, {"events": [], "cursor": ""}
            if endpoint == "/markets":
                return 200, {"markets": MARKETS, "cursor": ""}
            assert endpoint == "/historical/markets"
            return 200, {"markets": [], "cursor": "" if params.get("cursor") else "LAST"}

    state = capture.capture(Store(root), client_factory=Client)
    assert state["complete"]
    snapshots = json.loads((root / "captures" / state["capture_id"] / "state.json").read_bytes())
    assert snapshots["state"]["complete"]
    return root, state["capture_id"]


@pytest.mark.parametrize(
    "family,field,value",
    [
        ("batch", "parent_batch", "multivariate"),
        ("batch", "parent_batch_id", "0" * 64),
        ("batch", "parent_batch_tickers", ",".join(reversed(PARENTS))),
        ("batch", "request_cursor", "FORGED"),
        ("batch", "request_generation", True),
        ("batch", "task_scope", "events_live"),
        ("cutoff", "cutoff_reason", "resume"),
        ("cutoff", "cutoff_ordinal", True),
        ("cutoff", "request_cursor", "FORGED"),
        ("cutoff", "request_generation", True),
    ],
)
def test_completed_snapshot_cannot_hide_changed_receipt_request_context(
    anchored_policy_root, tmp_path, family, field, value
):
    original, capture_id = anchored_policy_root
    root = tmp_path / "root"
    shutil.copytree(original, root)
    store = Store(root)
    state = capture.read_capture(store, capture_id)
    assert state["complete"]
    candidates = [
        path
        for path in store.path("raw").glob("*/manifest.json")
        if (
            json.loads(path.read_bytes()).get("parent_batch") == "ordinary"
            if family == "batch"
            else json.loads(path.read_bytes()).get("cutoff_reason") == "start"
        )
    ]
    assert len(candidates) == 1
    receipt = candidates[0]
    manifest = json.loads(receipt.read_bytes())
    receipt.write_bytes(json_bytes({**manifest, field: value}))

    with pytest.raises(CatalogueError):
        list(capture.capture_pages(store, state))


@pytest.mark.parametrize("protocol", [2, 3, 4, 5])
@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_batch", "ordinary"),
        ("parent_batch_id", "a" * 64),
        ("parent_batch_tickers", "EVENT"),
        ("cutoff_reason", "start"),
        ("cutoff_ordinal", 1),
        ("request_cursor", ""),
        ("request_generation", 0),
    ],
)
def test_legacy_receipts_reject_context_fields_not_enabled_by_their_sealed_policy(
    protocol, field, value
):
    if protocol == 5:
        state = _policy_state(legacy=True)
        manifest = {
            "acquisition_protocol": 5,
            "acquisition_policy_sha256": state["acquisition_policy_sha256"],
        }
    elif protocol == 2:
        state, manifest = {}, {}
    else:
        state = {"acquisition_protocol": protocol}
        manifest = {"acquisition_protocol": protocol}
        if protocol == 4:
            state["scope"] = series_scope("SERIES")
            manifest["series_scope"] = state["scope"]
    assert capture._receipt_matches_state_protocol(manifest, state)
    assert not capture._receipt_matches_state_protocol({**manifest, field: value}, state)


def test_anchored_legacy_event_receipt_rejects_injected_batch_membership_suppression(
    store, monkeypatch
):
    initialize = capture._initialize_v2

    def legacy_protocol3(actual_store, state):
        state["acquisition_protocol"] = 3
        for key in ("rate_policy", "parent_policy", "cutoff_policy", "acquisition_policy_sha256"):
            state.pop(key, None)
        state["tasks"] = [
            capture.task(
                work["endpoint"],
                work["kind"],
                singular=work["singular"],
                params=work["params"],
                requested=work["requested"],
            )
            for work in state["tasks"]
        ]
        return initialize(actual_store, state)

    with monkeypatch.context() as legacy:
        legacy.setattr(capture, "_initialize_v2", legacy_protocol3)
        state = capture.capture(
            store, client_factory=FakeClient, compression="identity", rate_policy="fixed"
        )
    assert state["complete"] and state["acquisition_protocol"] == 3
    state = capture.read_capture(store, state["capture_id"])
    pages = list(capture.capture_pages(store, state))
    assert _index(pages).scope_membership["events_live"][0]
    manifest, payload = next(
        (manifest, payload) for manifest, payload in pages if manifest["endpoint"] == "/events"
    )
    changed = {
        **manifest,
        "parent_batch": "ordinary",
        "parent_batch_id": "a" * 64,
        "parent_batch_tickers": ",".join(row["event_ticker"] for row in payload["events"]),
    }
    store.path(f"raw/{manifest['page_id']}/manifest.json").write_bytes(json_bytes(changed))

    with pytest.raises(CatalogueError):
        list(capture.capture_pages(Store(store.root), state))
