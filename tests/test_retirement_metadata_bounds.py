"""Retirement metadata remains bounded without relaxing durable deletion rules."""

import hashlib

import pytest

from kalshi_events import incremental
from kalshi_events.common import CatalogueError, json_bytes


def test_progress_stream_keeps_committed_records_and_truncates_only_trailing_partial(store):
    body = b'{"index":0}\n{"index":1}\n'
    store.write_bytes("prune-progress.jsonl", body + b'{"index":')
    assert list(
        incremental._retirement_progress_lines(store, store.path("prune-progress.jsonl"))
    ) == [
        b'{"index":0}\n',
        b'{"index":1}\n',
    ]
    assert store.path("prune-progress.jsonl").read_bytes() == body


def test_progress_rejects_oversized_record_without_truncation(store, monkeypatch):
    monkeypatch.setattr(incremental, "MAX_RETIREMENT_PROGRESS_LINE_BYTES", 8)
    original = b"x" * 9 + b"\n"
    store.write_bytes("prune-progress.jsonl", original)
    with pytest.raises(CatalogueError, match="record exceeds"):
        list(incremental._retirement_progress_lines(store, store.path("prune-progress.jsonl")))
    assert store.path("prune-progress.jsonl").read_bytes() == original


def test_plan_limits_apply_before_a_durable_plan_is_written(store, monkeypatch):
    monkeypatch.setattr(incremental, "MAX_RETIREMENT_ENTRIES", 1)
    with pytest.raises(CatalogueError, match="entry bound"):
        incremental._write_retirement_plan(store, {"entries": [{}, {}]})
    assert not store.path("prune-plan.json").exists()
    monkeypatch.setattr(incremental, "MAX_RETIREMENT_PLAN_BYTES", 8)
    with pytest.raises(CatalogueError, match="metadata bound"):
        incremental._write_retirement_plan(store, {"entries": []})
    assert not store.path("prune-plan.json").exists()


def test_recovery_rejects_entry_overflow_before_inspecting_or_moving_targets(store, monkeypatch):
    monkeypatch.setattr(incremental, "MAX_RETIREMENT_ENTRIES", 0)
    plan = {"version": 1, "kind": "prune", "entries": [{"source": "raw/invalid", "target": "x"}]}
    store.write("prune-plan.json", plan)
    with store.writer_guard(), pytest.raises(CatalogueError, match="plan entries"):
        incremental.finish_pending_retirement(store)
    assert store.path("prune-plan.json").exists()


@pytest.mark.parametrize("raw_owner", [None, "", "not-a-capture"])
def test_bad_later_raw_owner_prevents_any_retirement(store, raw_owner):
    build_id, page_id = "a" * 32, "b" * 32
    store.write(f"builds/{build_id}/build.json", {"capture_id": build_id})
    store.write(f"raw/{page_id}/manifest.json", {"page_id": page_id, "capture_id": raw_owner})
    entries = [
        {"source": source, "target": f"prune-trash/{'c' * 32}-{source.replace('/', '-')}"}
        for source in (f"builds/{build_id}", f"raw/{page_id}")
    ]
    plan = {"version": 1, "kind": "prune", "entries": entries}
    store.write("prune-plan.json", plan)
    before = store.path("prune-plan.json").read_bytes()
    with store.writer_guard(), pytest.raises(CatalogueError, match="capture identity"):
        incremental.finish_pending_retirement(store)
    assert store.path(f"builds/{build_id}/build.json").exists()
    assert store.path(f"raw/{page_id}/manifest.json").exists()
    assert store.path("prune-plan.json").read_bytes() == before
    assert not store.path("prune-progress.jsonl").exists()


def test_raw_progress_owner_is_bound_before_earlier_artifacts_are_deleted(store):
    build_id, page_id, owner = "a" * 32, "b" * 32, "d" * 32
    store.write(f"raw/{page_id}/manifest.json", {"page_id": page_id, "capture_id": owner})
    store.write(f"builds/{build_id}/build.json", {"capture_id": build_id})
    sources = [f"raw/{page_id}", f"builds/{build_id}"]
    entries = [
        {"source": source, "target": f"prune-trash/{'c' * 32}-{source.replace('/', '-')}"}
        for source in sources
    ]
    plan = {"version": 1, "kind": "prune", "entries": entries}
    store.write("prune-plan.json", plan)
    record = {
        "version": 1,
        "plan_sha256": hashlib.sha256(store.path("prune-plan.json").read_bytes()).hexdigest(),
        "index": 0,
        **entries[0],
        "capture_id": "e" * 32,
    }
    store.append("prune-progress.jsonl", json_bytes(record))
    with store.writer_guard(), pytest.raises(CatalogueError, match="matching capture identity"):
        incremental.finish_pending_retirement(store)
    assert all(store.path(source).is_dir() for source in sources)
