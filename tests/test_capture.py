from __future__ import annotations

import json

import pytest
from conftest import EVENTS, HISTORICAL, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.cli import main
from kalshi_events.common import CatalogueError, Limits, Store
from kalshi_events.normalize import project_pages


def test_native_full_catalogue_keeps_history_and_relationships(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    assert state["complete"]
    assert state["requests"] == 7
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


def test_checkpoint_stays_small_while_pages_go_to_journal(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    checkpoint = json.loads(store.path(f"captures/{state['capture_id']}/state.json").read_text())
    assert "pages" not in checkpoint
    assert "seen_cursors" not in json.dumps(checkpoint["tasks"])
    journal = store.path(f"captures/{state['capture_id']}/pages.jsonl").read_text().splitlines()
    assert len(journal) == state["requests"]


def test_each_historical_read_records_its_own_cutoff(store):
    def markets(params):
        if params.get("cursor"):
            return 200, {"markets": [], "cursor": ""}
        return 200, {"markets": [HISTORICAL], "cursor": "page-2"}

    state = acquisition.capture(
        store, client_factory=lambda s, b: FakeClient(s, b, {"/historical/markets": markets})
    )
    assert state["complete"]
    cutoffs = [
        manifest
        for manifest, _ in acquisition.capture_pages(store, state)
        if manifest["endpoint"] == "/historical/cutoff"
    ]
    # One cutoff for the catalogue-start read plus one before each of the two historical pages.
    assert len(cutoffs) == 3
    assert all(manifest["status"] == 200 for manifest in cutoffs)


def test_budget_stop_resumes_without_publishing_incomplete(store, factory):
    bounded = Store(store.root, Limits(max_requests=2))
    state = acquisition.capture(bounded, client_factory=factory)
    assert not state["complete"]
    assert state["requests"] == 2
    with pytest.raises(CatalogueError, match="incomplete"):
        acquisition.complete_capture(store, state["capture_id"])
    resumed = acquisition.capture(store, resume=state["capture_id"], client_factory=factory)
    assert resumed["complete"]
    # The two receipts from the first run are applied from the journal, never reissued.
    assert resumed["requests"] == 5


def test_budget_exhaustion_exits_three_with_incomplete_json(store, factory, monkeypatch, capsys):
    monkeypatch.setattr(acquisition, "PublicClient", factory)
    code = main(["--root", str(store.root), "capture", "--max-requests", "2"])
    output = json.loads(capsys.readouterr().out)
    assert code == 3
    assert output["complete"] is False
    assert output["requests"] == 2


def test_fully_complete_capture_exits_zero(store, factory, monkeypatch, capsys):
    monkeypatch.setattr(acquisition, "PublicClient", factory)
    code = main(["--root", str(store.root), "capture"])
    output = json.loads(capsys.readouterr().out)
    assert code == 0
    assert output["complete"] is True


def test_raw_commit_crash_is_recoverable_without_reissuing_request(store, factory, monkeypatch):
    original = acquisition._consume

    def crash(*args):
        raise RuntimeError("simulated process death after raw commit")

    monkeypatch.setattr(acquisition, "_consume", crash)
    with pytest.raises(RuntimeError):
        acquisition.capture(store, client_factory=factory)
    capture_id = next(store.path("captures").iterdir()).name
    state = acquisition.read_capture(store, capture_id)
    assert [work for work in state["tasks"] if work["pending"]]
    assert len(list(acquisition.capture_pages(store, state))) == 1
    monkeypatch.setattr(acquisition, "_consume", original)
    resumed = acquisition.capture(store, resume=capture_id, client_factory=factory)
    assert resumed["complete"]
    # Every request issued before the crash committed its raw receipt; none is repeated.
    assert resumed["requests"] == 1
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 7


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
    # Cutoff, live lookup, historical cutoff, historical lookup.
    assert second["requests"] == 4


def test_sealed_response_ceiling_governs_later_reads(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    assert state["max_response_bytes"] == store.limits.max_response_bytes
    tighter = Store(store.root, Limits(max_response_bytes=1))
    assert len(list(acquisition.capture_pages(tighter, state))) == state["requests"]
    assert acquisition.complete_capture(tighter, state["capture_id"])["max_response_bytes"] == (
        store.limits.max_response_bytes
    )
