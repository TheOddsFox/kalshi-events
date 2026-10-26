from __future__ import annotations

import json
from pathlib import Path

from recovery_fixtures import CURSOR, build_recovery_root

from kalshi_events import capture
from kalshi_events.common import Store


def _state(root: Path):
    store = Store(root)
    capture_id = next(path.name for path in (root / "captures").iterdir())
    return store, capture_id, capture.read_capture(store, capture_id)


def test_primary_fixture_is_deterministic_and_keeps_cursor_generations(tmp_path):
    roots = [tmp_path / "first", tmp_path / "second"]
    capture_ids = [
        build_recovery_root(root, "primary", repeats=2, wide_bytes=4096) for root in roots
    ]

    assert capture_ids == ["d" * 32] * 2
    journals = [(root / "captures" / capture_ids[0] / "pages.jsonl").read_bytes() for root in roots]
    assert journals[0] == journals[1]

    store, capture_id, state = _state(roots[0])
    primary = state["tasks"][2]
    receipts = capture.page_receipts(store, state)
    manifests = [json.loads(store.path(row["relative"]).read_text()) for row in receipts]
    assert state["acquisition_protocol"] == 5
    assert primary["done"] is False
    assert primary["cursor"] == CURSOR
    assert primary["restarts"] == 2
    snapshot = json.loads((roots[0] / "captures" / capture_id / "state.json").read_text())
    assert snapshot["_journal"]["transition_count"] == 5
    assert [row["task_generation"] for row in manifests] == [0, 0, 1, 1, 2]
    assert [row["status"] for row in manifests] == [200, 400, 200, 400, 200]
    assert (
        max((store.path(row["relative"]).parent / "body.bin").stat().st_size for row in receipts)
        > 4096
    )


def test_hydration_fixture_leaves_only_durable_hydration_tasks_unfinished(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "hydration", hydration_tasks=3)
    store, _, state = _state(root)
    initial = state["tasks"][:6]
    pending = state["tasks"][6:]
    journal = [
        json.loads(line)
        for line in (root / "captures" / capture_id / "pages.jsonl").read_text().splitlines()
    ]

    assert all(work["done"] for work in initial)
    assert [work["endpoint"] for work in pending] == [
        "/events/EVENT-MISSING-000000",
        "/events/EVENT-MISSING-000001",
        "/events/EVENT-MISSING-000002",
    ]
    assert all(not work["done"] for work in pending)
    snapshot = json.loads((root / "captures" / capture_id / "state.json").read_text())
    assert snapshot["_journal"]["transition_count"] == 9
    assert any(row["op"] == "control" and row["payload"]["kind"] == "hydrate" for row in journal)
    assert len(capture.page_receipts(store, state)) == 8
