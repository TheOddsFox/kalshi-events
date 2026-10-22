from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from kalshi_events import capture
from kalshi_events.metrics import collect


class _NoIterationList(list):
    def __iter__(self):
        raise AssertionError("page derivation scanned unrelated tasks")


def _event_task(requested: str | None = None) -> dict:
    return {
        "endpoint": "/events",
        "kind": "events",
        "singular": False,
        "params": {},
        "requested": requested,
        "cursor": "",
        "restarts": 0,
        "done": False,
        "pending": None,
        "cutoff_due": False,
    }


def test_page_change_is_pure_and_does_not_copy_long_cursor_or_task_history(monkeypatch):
    task = _event_task()
    unrelated = [_event_task(str(index)) for index in range(100_000)]
    tasks = [task, *unrelated]
    state = capture._CaptureState(
        {"tasks": tasks, "coverage": {}, "capture_id": "c" * 32, "complete": False}
    )
    state["tasks"] = _NoIterationList(tasks)
    cursor_history = {f"cursor-{index}" for index in range(100_000)}
    seen = {0: cursor_history}
    monkeypatch.setattr(
        capture.copy,
        "deepcopy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("whole-state copy")),
    )

    change = capture._derive_page_change(
        state,
        seen,
        0,
        {"status": 200},
        {"events": [{"event_ticker": "E-1", "series_ticker": "S-1"}], "cursor": "next"},
    )

    assert state["tasks"][0] is task
    assert state["tasks"][1] is unrelated[0]
    assert task["cursor"] == ""
    assert seen[0] is cursor_history
    assert len(cursor_history) == 100_000
    assert change["seen_add"] == capture.digest(b"next")
    capture._apply_page_change(state, seen, 0, change)
    assert task["cursor"] == "next"
    assert len(cursor_history) == 100_001


def test_cold_task_identity_cache_is_built_before_terminal_batch_transition(monkeypatch):
    selected = ["MARKET-1", "MARKET-2"]
    batch = capture.task(
        "/markets",
        "markets",
        params={"limit": 1000, "tickers": ",".join(selected)},
    )
    unrelated = [_event_task(f"EVENT-{index}") for index in range(99_998)]
    prior_fallback = capture.task(
        "/markets/MARKET-2", "markets", singular=True, requested="MARKET-2"
    )
    tasks = [batch, prior_fallback, *unrelated]
    state = capture._CaptureState(
        {
            "tasks": tasks,
            "coverage": {},
            "selected": selected,
            "capture_id": "c" * 32,
            "complete": False,
        }
    )
    state["tasks"] = _NoIterationList(tasks)
    identity_calls = 0
    original_identity = capture._task_identity

    def count_identity(work):
        nonlocal identity_calls
        identity_calls += 1
        return original_identity(work)

    monkeypatch.setattr(capture, "_task_identity", count_identity)

    change = capture._derive_page_change(
        state,
        {},
        0,
        {"status": 200},
        {"markets": [{"ticker": selected[0], "event_ticker": "E-1"}], "cursor": ""},
    )

    assert change["tasks"] == []
    assert identity_calls == 1


def test_terminal_page_preserves_empty_seen_cursor_entry():
    state = capture._CaptureState(
        {"tasks": [_event_task()], "coverage": {}, "capture_id": "c" * 32, "complete": False}
    )
    seen: dict[int, set[str]] = {}
    change = capture._derive_page_change(
        state,
        seen,
        0,
        {"status": 200},
        {"events": [], "cursor": ""},
    )

    assert change["seen_touch"] is True
    assert seen == {}
    capture._apply_page_change(state, seen, 0, change)
    assert seen == {0: set()}
    assert capture._seen_value(seen) == {"0": []}


def test_cutoff_transition_preserves_all_provider_fields():
    task = capture.task("/historical/cutoff", None)
    state = capture._CaptureState(
        {"tasks": [task], "coverage": {}, "capture_id": "c" * 32, "complete": False}
    )
    payload = {
        "market_settled_ts": "2025-01-01T00:00:00Z",
        "trades_created_ts": "2025-01-02T00:00:00Z",
        "extra_provider_field": {"preserve": [1, 2]},
    }
    change = capture._derive_page_change(state, {}, 0, {"status": 200}, payload)
    capture._apply_page_change(state, {}, 0, change)
    assert state["cutoff"] == payload
    assert task["done"] is True
    assert task["cutoff_due"] is False


def _patch_scheduler(monkeypatch, state, consume):
    monkeypatch.setattr(
        capture,
        "_request_context",
        lambda _state, index: {"request_id": f"request-{index}"},
    )
    monkeypatch.setattr(
        capture,
        "_plan",
        lambda work, _phase: ("/events", {"id": work["requested"]}, "events", False),
    )
    monkeypatch.setattr(capture, "_phase", lambda _work: "page")
    monkeypatch.setattr(capture, "_stage", lambda *_args: None)
    monkeypatch.setattr(capture, "_consume", consume)
    monkeypatch.setattr(capture, "_hydrate", lambda *_args: False)
    monkeypatch.setattr(capture, "save", lambda *_args: None)


class _Budget:
    @property
    def remaining(self):
        return 30


def test_ready_queue_requeues_a_page_after_other_ready_traversals(monkeypatch):
    state = {
        "tasks": [_event_task("0"), _event_task("1")],
        "complete": False,
        "journal_version": None,
    }
    requested = []
    counts = [0, 0]

    class Client:
        def get(self, endpoint, params, context=None):
            requested.append(params["id"])
            return f"raw/{params['id']}/{counts[int(params['id'])]}"

    def consume(_store, actual_state, index, _relative, _seen, _identities):
        counts[index] += 1
        if index != 0 or counts[index] == 2:
            actual_state["tasks"][index]["done"] = True

    _patch_scheduler(monkeypatch, state, consume)
    monkeypatch.setattr(capture, "MAX_CONCURRENT_REQUESTS", 1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        capture._run(None, state, Client(), pool, {}, {}, _Budget(), None)

    assert requested == ["0", "1", "0"]
    assert all(work["done"] for work in state["tasks"])


def test_scheduler_never_submits_more_than_eight_futures(monkeypatch):
    state = {
        "tasks": [_event_task(str(index)) for index in range(24)],
        "complete": False,
        "journal_version": None,
    }
    active = 0
    maximum_active = 0
    active_lock = threading.Lock()
    release = threading.Event()
    completed = 0

    class Client:
        def get(self, endpoint, params, context=None):
            nonlocal active, maximum_active, completed
            with active_lock:
                active += 1
                maximum_active = max(maximum_active, active)
                if active >= 8:
                    release.set()
            assert release.wait(5)
            time.sleep(0.001)
            with active_lock:
                active -= 1
                completed += 1
            return f"raw/{params['id']}"

    class TrackingPool(ThreadPoolExecutor):
        submitted = 0
        maximum_submitted = 0
        pending_lock = threading.Lock()

        def submit(self, *args, **kwargs):
            with self.pending_lock:
                self.submitted += 1
                self.maximum_submitted = max(self.maximum_submitted, self.submitted)
            future = super().submit(*args, **kwargs)

            def completed_callback(_future):
                with self.pending_lock:
                    self.submitted -= 1

            future.add_done_callback(completed_callback)
            return future

    def consume(_store, actual_state, index, _relative, _seen, _identities):
        actual_state["tasks"][index]["done"] = True

    _patch_scheduler(monkeypatch, state, consume)
    with TrackingPool(max_workers=8) as pool, collect() as measured:
        capture._run(None, state, Client(), pool, {}, {}, _Budget(), None)

    assert completed == 24
    assert maximum_active == 8
    assert pool.maximum_submitted <= 8
    assert measured.report()["peak_submitted_futures"] == 8
    assert all(work["done"] for work in state["tasks"])


def test_100k_completed_tasks_do_not_trigger_per_completion_identity_scans(monkeypatch):
    tasks = [_event_task(str(index)) for index in range(100_000)]
    for work in tasks:
        work["done"] = True
    tasks[-1]["done"] = False
    state = {"tasks": tasks, "complete": False, "journal_version": None}
    requested = []

    class Client:
        def get(self, endpoint, params, context=None):
            requested.append(params["id"])
            return "raw/last"

    def consume(_store, actual_state, index, _relative, _seen, _identities):
        actual_state["tasks"][index]["done"] = True

    _patch_scheduler(monkeypatch, state, consume)
    monkeypatch.setattr(
        capture,
        "_task_identity",
        lambda _work: (_ for _ in ()).throw(AssertionError("task identity scan")),
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        capture._run(None, state, Client(), pool, {}, {}, _Budget(), None)
    assert requested == ["99999"]
