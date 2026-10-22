from __future__ import annotations

import copy
import json

import pytest
from conftest import MARKET, FakeClient
from test_journal_v2 import assert_chain_and_anchor

from kalshi_events import capture as acquisition
from kalshi_events.common import Budget, CatalogueError
from kalshi_events.normalize import ParentIndex


class PowerLoss(BaseException):
    pass


def _prepared_pages(store, *, invalid_native=False):
    issued = []
    native = (
        {
            **MARKET,
            "mve_selected_legs": [
                {"event_ticker": "EVENT", "market_ticker": "MARKET", "side": "invalid"}
            ],
        }
        if invalid_native
        else MARKET
    )

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            issued.append(context["request_id"])
            return super().get(endpoint, params, context)

    with store.lock():
        state = acquisition.new_capture(
            store, ["MARKET"], compression="identity", rate_policy="fixed"
        )
        budget = Budget(store)
        client = RecordingClient(
            store, budget, {"/markets": (200, {"markets": [native], "cursor": "NEXT"})}
        )
        index = next(i for i, work in enumerate(state["tasks"]) if work["endpoint"] == "/markets")
        context = acquisition._request_context(state, index)
        relative = client.get("/markets", state["tasks"][index]["params"], context)
        cutoff_context = acquisition._request_context(state, 0)
        cutoff = client.get("/historical/cutoff", {}, cutoff_context)
    return state, index, relative, context, cutoff, cutoff_context, issued, RecordingClient


def _page_records(store, capture_id):
    return [
        row
        for line in store.path(f"captures/{capture_id}/pages.jsonl").read_bytes().splitlines()
        if (row := json.loads(line))["op"] == "page"
    ]


@pytest.mark.parametrize("failure", ["native_validation", "append"])
def test_page_is_not_applied_before_its_durable_append(store, monkeypatch, failure):
    state, index, relative, context, *_ = _prepared_pages(
        store, invalid_native=failure == "native_validation"
    )
    before = copy.deepcopy(dict(state))
    seen_before = copy.deepcopy(state.seen_cursors)
    identities = ParentIndex()
    attempted = []

    def reject_append(_relative, encoded):
        assert json.loads(encoded)["op"] == "page"
        attempted.append(True)
        assert state == before
        assert state.seen_cursors == seen_before
        assert identities.markets == {}
        raise OSError("journal append did not complete")

    monkeypatch.setattr(store, "append", reject_append)
    expected = CatalogueError if failure == "native_validation" else OSError
    with store.lock(), pytest.raises(expected):
        acquisition._commit_page_v2(
            store, state, index, relative, context["request_id"], identities
        )

    assert state == before
    assert state.seen_cursors == seen_before
    assert identities.markets == {}
    assert attempted == ([True] if failure == "append" else [])
    assert state.journal_uncertain is (failure == "append")
    assert _page_records(store, state["capture_id"]) == []


@pytest.mark.parametrize("fault_point", ["_apply_page_change", "_merge_parent_index"])
@pytest.mark.parametrize("error", [OSError, RuntimeError, PowerLoss])
def test_post_append_failure_stops_commits_and_resume_reuses_evidence(
    store, monkeypatch, fault_point, error
):
    state, index, relative, context, cutoff, cutoff_context, issued, client = _prepared_pages(store)
    original = getattr(acquisition, fault_point)
    failed = False

    def fail_after_application(*args, **kwargs):
        nonlocal failed
        result = original(*args, **kwargs)
        if not failed:
            failed = True
            raise error("durable page was only partly applied")
        return result

    with monkeypatch.context() as fault:
        fault.setattr(acquisition, fault_point, fail_after_application)
        with store.lock():
            identities = ParentIndex()
            with pytest.raises(error):
                acquisition._commit_page_v2(
                    store, state, index, relative, context["request_id"], identities
                )
            assert state.journal_uncertain
            with pytest.raises(CatalogueError, match="uncertain"):
                acquisition._commit_page_v2(
                    store, state, 0, cutoff, cutoff_context["request_id"], identities
                )
    pages = _page_records(store, state["capture_id"])
    assert [row["payload"]["relative"] for row in pages] == [relative]

    resumed = acquisition.capture(store, resume=state["capture_id"], client_factory=client)
    assert resumed["complete"]
    assert len(issued) == len(set(issued)) == 5
    assert issued.count(context["request_id"]) == 1
    assert issued.count(cutoff_context["request_id"]) == 1
    records, snapshot = assert_chain_and_anchor(store, state["capture_id"])
    committed = [row["payload"]["relative"] for row in records if row["op"] == "page"]
    assert len(committed) == len(set(committed)) == 5
    assert {relative, cutoff} <= set(committed)
    assert acquisition.read_capture(store, state["capture_id"]) == snapshot["state"]
