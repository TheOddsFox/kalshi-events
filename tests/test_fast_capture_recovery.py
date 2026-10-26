from __future__ import annotations

from collections import Counter

import pytest
from conftest import FakeClient
from recovery_fixtures import build_recovery_root

from kalshi_events import capture
from kalshi_events.common import Store


def test_resume_dispatches_before_prefix_replay_and_finishes_ordered_index(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=2, wide_bytes=4096)
    store = Store(root)
    before = capture.read_capture(store, capture_id)
    old_rows = capture.page_receipts(store, before)
    old_relatives = {row["relative"] for row in old_rows}
    old_request_ids = {row["request_id"] for row in old_rows}
    events: list[tuple[str, str]] = []
    issued: list[str] = []
    replayed: Counter[str] = Counter()

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            if endpoint != capture.CUTOFF_ENDPOINT:
                request_id = context["request_id"]
                issued.append(request_id)
                events.append(("request", request_id))
            return super().get(endpoint, params, context)

    original_read_page = capture.read_page
    original_consume = capture._consume
    original_hydrate = capture._hydrate

    def read_page(actual_store, relative, *args, **kwargs):
        if relative in old_relatives:
            replayed[relative] += 1
            assert any(kind == "committed_data" for kind, _ in events)
            assert any(kind == "request" for kind, _ in events)
            events.append(("replay", relative))
        return original_read_page(actual_store, relative, *args, **kwargs)

    def consume(actual_store, state, index, relative, seen, identities):
        result = original_consume(actual_store, state, index, relative, seen, identities)
        if (
            relative not in old_relatives
            and state["tasks"][index]["endpoint"] != capture.CUTOFF_ENDPOINT
        ):
            events.append(("committed_data", relative))
        return result

    def hydrate(actual_store, state, identities):
        assert identities.defer_page_merges is False
        assert identities.processed == {row["relative"] for row in state.page_rows}
        return original_hydrate(actual_store, state, identities)

    monkeypatch.setattr(capture, "read_page", read_page)
    monkeypatch.setattr(capture, "_consume", consume)
    monkeypatch.setattr(capture, "_hydrate", hydrate)

    resumed = capture.capture(
        store,
        resume=capture_id,
        client_factory=RecordingClient,
    )

    assert resumed["complete"] is True
    assert old_relatives.issubset(replayed)
    assert set(replayed.values()) == {1}
    assert issued
    assert old_request_ids.isdisjoint(issued)
    assert events.index(
        next(event for event in events if event[0] == "committed_data")
    ) < events.index(next(event for event in events if event[0] == "replay"))


def test_progress_write_failure_does_not_replace_request_failure(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    original_progress = capture.metrics.progress

    class FailingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            raise RuntimeError("original request failure")

    def failed_progress(phase=None, **counters):
        if phase == "first_new_request":
            raise OSError("progress destination failed")
        return original_progress(phase, **counters)

    monkeypatch.setattr(capture.metrics, "progress", failed_progress)
    with pytest.raises(RuntimeError, match="original request failure"):
        capture.capture(store, resume=capture_id, client_factory=FailingClient)
