from __future__ import annotations

import json

import pytest
from conftest import EVENTS, HISTORICAL, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.cli import main
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store, digest, json_bytes
from kalshi_events.normalize import project_pages


def _legacy_checkpoint(store, state):
    """Convert a pristine v2 genesis into a v1 fixture for legacy resume tests."""
    logical = dict(state)
    logical.pop("journal_version")
    logical.pop("acquisition_protocol", None)
    logical.pop("rate_policy", None)
    logical.pop("parent_policy", None)
    logical.pop("cutoff_policy", None)
    logical.pop("acquisition_policy_sha256", None)
    if logical.get("selected") is not None:
        logical["tasks"] = [
            acquisition.task("/historical/cutoff", None, singular=True),
            *[
                acquisition.task(f"/markets/{value}", "markets", singular=True, requested=value)
                for value in logical["selected"]
            ],
        ]
    store.remove(f"captures/{state['capture_id']}/pages.jsonl")
    store.write(f"captures/{state['capture_id']}/state.json", logical)
    return logical


def test_native_full_catalogue_keeps_history_and_relationships(store, factory):
    state = acquisition.capture(store, client_factory=factory, catalogue_profile="exchange")
    assert state["complete"]
    assert state["requests"] == 8
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


def test_journal_control_size_does_not_scale_with_cursor_history(store):
    state = acquisition.new_capture(store, None)
    state.seen_cursors = {0: {f"{index:064x}" for index in range(10_000)}}

    acquisition._commit_control_delta(
        store, state, {"kind": "stop", "reason": "request allowance exhausted"}
    )

    journal = store.path(f"captures/{state['capture_id']}/pages.jsonl")
    record = json.loads(journal.read_bytes().splitlines()[-1])
    assert record["op"] == "control"
    assert "seen_cursors" not in record["payload"]
    assert len(json.dumps(record).encode()) < 2048


def test_acquisition_protocol_is_sealed_and_legacy_missing_protocol_keeps_request_identity(store):
    state = acquisition.new_capture(store, ["MARKET"])
    assert state["acquisition_protocol"] == 5
    context = acquisition._request_context(state, 0)
    assert context["acquisition_protocol"] == 5

    state.pop("acquisition_protocol")
    state.pop("rate_policy")
    state.pop("parent_policy")
    state.pop("cutoff_policy")
    state.pop("acquisition_policy_sha256")
    request_key = [state["capture_id"], 0, "page", "", 0]
    legacy = acquisition._request_context(state, 0)
    assert "acquisition_protocol" not in legacy
    assert legacy["request_id"] == digest(json_bytes(request_key))

    for invalid in (True, False, 2, 4, 6, 3.0, "3"):
        state["acquisition_protocol"] = invalid
        with pytest.raises(CatalogueError, match="unsupported acquisition protocol"):
            acquisition._request_context(state, 0)


def test_new_capture_seals_gzip_and_downward_rate_policy_by_default(store):
    state = acquisition.new_capture(store, None)

    assert state["compression"] == "gzip"
    assert state["rate_policy"] == "decrease-on-429"
    assert acquisition._protocol5_policies(state) == {
        "rate_policy": "decrease-on-429",
        "parent_policy": "historical-only-v1",
        "cutoff_policy": "checkpoint-v1",
    }


def test_capture_resume_preserves_legacy_sealed_compression_and_rate_policy(store, factory):
    state = acquisition.new_capture(store, None, compression="identity", rate_policy="fixed")

    resumed = acquisition.capture(
        store,
        resume=state["capture_id"],
        client_factory=factory,
    )

    assert resumed["compression"] == "identity"
    assert resumed["rate_policy"] == "fixed"


def test_completed_capture_reads_do_not_depend_on_current_source_origin(
    store, factory, monkeypatch
):
    state = acquisition.capture(store, selected=["MARKET"], client_factory=factory)
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "https://invalid.example/?unexpected=1")

    restored = acquisition.read_capture(store, state["capture_id"])
    assert restored["complete"]
    assert len(list(acquisition.capture_pages(store, restored))) == state["requests"]


def test_capture_pages_rejects_receipt_protocol_change_with_retained_snapshot(store, factory):
    state = acquisition.capture(store, selected=["MARKET"], client_factory=factory)
    rows = acquisition.page_receipts(store, state)
    relative = rows[0]["relative"]
    manifest_path = store.path(relative)
    manifest = json.loads(manifest_path.read_bytes())
    manifest["acquisition_protocol"] = 2
    manifest_path.write_bytes(json_bytes(manifest))

    replay_state = acquisition._CaptureState(state)
    replay_state.page_rows = rows
    with pytest.raises(CatalogueError, match="receipt protocol"):
        list(acquisition.capture_pages(store, replay_state))
    with pytest.raises(CatalogueError, match="traversal|protocol"):
        acquisition.complete_capture(store, state["capture_id"])


def test_legacy_capture_rejects_receipt_with_new_protocol_field():
    legacy = {"version": 1, "capture_id": "legacy-capture"}
    unexpected = {"capture_id": "legacy-capture", "acquisition_protocol": 3}
    assert acquisition._receipt_matches_state_protocol(unexpected, legacy) is False


def test_checkpoint_stays_small_while_pages_go_to_journal(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    checkpoint = json.loads(store.path(f"captures/{state['capture_id']}/state.json").read_text())
    assert "pages" not in checkpoint
    assert "seen_cursors" not in json.dumps(checkpoint["state"]["tasks"])
    journal = store.path(f"captures/{state['capture_id']}/pages.jsonl").read_text().splitlines()
    assert sum(json.loads(record)["op"] == "page" for record in journal) == state["requests"]


def test_each_historical_read_records_its_own_cutoff(store):
    def markets(params):
        if params.get("cursor"):
            return 200, {"markets": [], "cursor": ""}
        return 200, {"markets": [HISTORICAL], "cursor": "page-2"}

    state = acquisition.capture(
        store,
        client_factory=lambda s, b, **_options: FakeClient(s, b, {"/historical/markets": markets}),
    )
    assert state["complete"]
    cutoffs = [
        manifest
        for manifest, _ in acquisition.capture_pages(store, state)
        if manifest["endpoint"] == "/historical/cutoff"
    ]
    # The initial catalogue cutoff plus the historical list's start and terminal checkpoints.
    assert len(cutoffs) == 3
    assert all(manifest["status"] == 200 for manifest in cutoffs)


def test_budget_stop_resumes_without_publishing_incomplete(store, factory):
    bounded = Store(store.root, Limits(max_requests=2))
    state = acquisition.capture(bounded, client_factory=factory, catalogue_profile="exchange")
    assert not state["complete"]
    assert state["requests"] == 2
    with pytest.raises(CatalogueError, match="incomplete"):
        acquisition.complete_capture(store, state["capture_id"])
    resumed = acquisition.capture(store, resume=state["capture_id"], client_factory=factory)
    assert resumed["complete"]
    # The two receipts from the first run are applied from the journal, never reissued.
    # The additional request is the protocol-5 terminal cutoff checkpoint.
    assert resumed["requests"] == 6


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


def test_evidence_walk_checks_deadline_on_every_page(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    expired = Store(store.root, Limits(max_duration_seconds=1))
    expired.started -= 60
    with pytest.raises(BudgetExceeded, match="deadline"):
        list(acquisition.capture_pages(expired, state))


def test_pre_1_0_capture_is_refused_with_new_root_message(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    relative = f"captures/{state['capture_id']}/state.json"
    legacy = dict(state)
    legacy.pop("journal_version")
    legacy.pop("acquisition_protocol", None)
    legacy.pop("rate_policy", None)
    legacy.pop("parent_policy", None)
    legacy.pop("cutoff_policy", None)
    legacy.pop("acquisition_policy_sha256", None)
    del legacy["max_response_bytes"]
    store.write(relative, legacy)
    with pytest.raises(CatalogueError, match="pre-1.0 captures are not readable"):
        acquisition.read_capture(store, state["capture_id"])


def test_raw_commit_crash_is_recoverable_without_reissuing_request(store, factory, monkeypatch):
    initial = acquisition.new_capture(store, None, catalogue_profile="exchange")
    _legacy_checkpoint(store, initial)
    original = acquisition._consume

    def crash(*args):
        raise RuntimeError("simulated process death after raw commit")

    monkeypatch.setattr(acquisition, "_consume", crash)
    with pytest.raises(RuntimeError):
        acquisition.capture(store, resume=initial["capture_id"], client_factory=factory)
    capture_id = initial["capture_id"]
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
        store,
        catalogue_profile="exchange",
        client_factory=lambda s, b, **_options: FakeClient(s, b, {"/events": events}),
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
            store,
            catalogue_profile="exchange",
            client_factory=lambda s, b, **_options: FakeClient(s, b, {"/markets": (200, payload)}),
        )
    assert not acquisition.read_capture(store, next(store.path("captures").iterdir()).name)[
        "complete"
    ]


def test_missing_parent_does_not_disappear_or_infer_ticker_prefix(store):
    native = {**MARKET, "ticker": "NOT-PARENT-MARKET", "event_ticker": "UNKNOWN"}
    state = acquisition.capture(
        store,
        selected=[native["ticker"]],
        client_factory=lambda s, b, **_options: FakeClient(
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
        client_factory=lambda s, b, **_options: FakeClient(s, b, {"/markets/MARKET": (404, {})}),
    )
    assert first["coverage"]["MARKET"]["status"] == "found"
    assert second["coverage"]["MARKET"]["status"] == "absent"
    # Cutoff, batch page, live fallback, historical cutoff, historical lookup.
    assert second["requests"] == 5


def test_sealed_response_ceiling_governs_later_reads(store, factory):
    state = acquisition.capture(store, client_factory=factory)
    assert state["max_response_bytes"] == store.limits.max_response_bytes
    tighter = Store(store.root, Limits(max_response_bytes=1))
    assert len(list(acquisition.capture_pages(tighter, state))) == state["requests"]
    assert acquisition.complete_capture(tighter, state["capture_id"])["max_response_bytes"] == (
        store.limits.max_response_bytes
    )
