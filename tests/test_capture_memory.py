from __future__ import annotations

import hashlib
import json

import pytest
from conftest import FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.common import CatalogueError, Limits, Store


def _incomplete_v2_state(store):
    bounded = Store(store.root, Limits(max_requests=7))
    result = acquisition.capture(bounded, client_factory=FakeClient, catalogue_profile="exchange")
    assert result["complete"] is False

    state = acquisition.read_capture(store, result["capture_id"])
    assert isinstance(state, acquisition._CaptureState)
    assert state.page_rows
    assert state.processed == {row["relative"] for row in state.page_rows}
    return state


def test_page_receipts_return_independent_list_and_row_copies(store):
    state = _incomplete_v2_state(store)
    original = [dict(row) for row in state.page_rows]

    rows = acquisition.page_receipts(store, state)

    assert rows == original
    assert rows is not state.page_rows
    assert rows[0] is not state.page_rows[0]
    rows[0]["relative"] = "raw/changed/manifest.json"
    rows.append({"index": -1, "relative": "raw/extra/manifest.json"})
    assert state.page_rows == original


def test_v2_replay_preserves_processed_membership(store):
    state = _incomplete_v2_state(store)

    replayed = acquisition.read_capture(store, state["capture_id"])

    assert replayed.page_rows == state.page_rows
    assert replayed.processed == state.processed
    assert replayed.request_ids == state.request_ids


@pytest.mark.parametrize("duplicate", ["relative", "request_id"])
def test_v2_rejects_duplicate_page_across_snapshot_prefix_and_suffix(store, duplicate):
    state = _incomplete_v2_state(store)
    first = state.page_rows[0]
    snapshot = json.loads(store.path(f"captures/{state['capture_id']}/state.json").read_text())
    journal = [
        json.loads(line)
        for line in store.path(f"captures/{state['capture_id']}/pages.jsonl")
        .read_text()
        .splitlines()
    ]
    first_page = next(record for record in journal if record["op"] == "page")
    assert first_page["seq"] < snapshot["_journal"]["seq"]

    if duplicate == "relative":
        request_id = hashlib.sha256(b"different request").hexdigest()
        assert request_id not in state.request_ids
        relative = first["relative"]
    else:
        request_id = first["request_id"]
        relative = "raw/duplicate-request/manifest.json"

    acquisition._append_v2_record(
        store,
        state,
        "page",
        {"index": first["index"], "relative": relative, "request_id": request_id},
    )

    with pytest.raises(CatalogueError, match="duplicate page receipt"):
        acquisition.read_capture(store, state["capture_id"])


def test_recover_receipts_uses_validated_v2_membership_and_skips_logged_decode(store, monkeypatch):
    state = _incomplete_v2_state(store)
    processed = set(state.processed)

    def unexpected(*_args):
        pytest.fail("v2 recovery must reuse processed membership without cloning receipts")

    monkeypatch.setattr(acquisition, "page_receipts", unexpected)
    monkeypatch.setattr(acquisition, "decode", unexpected)

    assert acquisition._recover_receipts(store, state) == {}
    assert state.processed == processed


def test_recover_receipts_handles_empty_v2_state_without_public_projection(store, monkeypatch):
    state = acquisition.new_capture(store, None, catalogue_profile="exchange")
    assert not state.processed

    monkeypatch.setattr(
        acquisition,
        "page_receipts",
        lambda *_args: pytest.fail("empty v2 recovery must use its empty membership set"),
    )

    assert acquisition._recover_receipts(store, state) == {}
    assert not state.processed


def test_recover_receipts_keeps_page_receipt_fallback_for_plain_v2_dict(store, monkeypatch):
    state = _incomplete_v2_state(store)
    plain_state = dict(state)
    calls = []
    public_projection = acquisition.page_receipts

    def projected(_store, candidate):
        calls.append(candidate)
        return public_projection(_store, candidate)

    monkeypatch.setattr(acquisition, "page_receipts", projected)

    assert acquisition._recover_receipts(store, plain_state) == {}
    assert calls == [plain_state]


def test_recover_receipts_keeps_legacy_journal_fallback(store):
    relative = "raw/legacy/manifest.json"
    store.write_bytes(
        "captures/legacy-capture/pages.jsonl",
        json.dumps({"index": 0, "relative": relative}).encode() + b"\n",
    )
    store.write_bytes(relative, b"not-json")
    state = {"version": 1, "capture_id": "legacy-capture", "tasks": []}

    assert acquisition._recover_receipts(store, state) == {}


def test_recover_receipts_still_fails_on_unlogged_invalid_manifest(store):
    state = acquisition.new_capture(store, None, catalogue_profile="exchange")
    store.write_bytes("raw/unlogged/manifest.json", b"not-json")

    with pytest.raises(CatalogueError, match="invalid JSON"):
        acquisition._recover_receipts(store, state)


@pytest.mark.parametrize("mismatch", ["request_id", "protocol"])
def test_recover_receipts_preserves_unlogged_mismatch_behavior(store, mismatch):
    state = acquisition.new_capture(store, None, catalogue_profile="exchange")
    context = acquisition._request_context(state, 0)
    manifest = {
        **context,
        "accounting_complete": True,
        "status": 200,
    }
    if mismatch == "request_id":
        manifest["request_id"] = "0" * 64
        assert manifest["request_id"] != context["request_id"]
    else:
        manifest["acquisition_protocol"] += 1
    store.write("raw/unlogged/manifest.json", manifest)

    if mismatch == "request_id":
        assert acquisition._recover_receipts(store, state) == {}
    else:
        with pytest.raises(CatalogueError, match="contradicts acquisition protocol"):
            acquisition._recover_receipts(store, state)
