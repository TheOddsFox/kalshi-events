from __future__ import annotations

import sqlite3
from collections import Counter

import pytest
from conftest import FakeClient
from recovery_fixtures import build_recovery_root

from kalshi_events import capture, incremental, recovery_index
from kalshi_events.common import CatalogueError, Store
from kalshi_events.incremental import discard, root_id
from kalshi_events.normalize import ParentIndex
from kalshi_events.recovery_index import RecoveryParentIndex, sealed_settings_sha256


def _seed_recovery_cache(root, capture_id):
    store = Store(root)
    with store.writer_guard():
        state = capture.read_capture(store, capture_id, _writer_resume=True)
        index = RecoveryParentIndex(
            store,
            capture_id,
            root_id(store),
            sealed_settings_sha256(state),
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )
        for row in state.page_rows:
            manifest, payload = capture.read_page(
                store,
                row["relative"],
                state["max_response_bytes"],
                state["max_decoded_response_bytes"],
            )
            page = ParentIndex()
            page.add_page(manifest, payload)
            index.merge_at_anchor(page, row["relative"], state.page_anchors[row["relative"]])
        index.close()
    return store, state, store.path(f"tmp/recovery-index/{capture_id}/index.sqlite")


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


def test_warm_cache_hydrates_done_primary_tasks_before_fresh_replay(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    original_commit_control = capture._commit_control
    original_hydrate = capture._hydrate

    def stop_before_completion(store, state, kind, candidate):
        if kind == "complete":
            raise RuntimeError("stop after clean cache checkpoint")
        return original_commit_control(store, state, kind, candidate)

    def leave_parent_unplanned(store, state, identities):
        return False

    class SeedClient(FakeClient):
        def __init__(self, store, budget, **options):
            super().__init__(
                store,
                budget,
                {
                    "/markets": (
                        200,
                        {
                            "markets": [
                                {
                                    "ticker": "WARM-MARKET",
                                    "event_ticker": "EVENT-WARM-MISSING",
                                    "status": "active",
                                }
                            ],
                            "cursor": "",
                        },
                    )
                },
                **options,
            )

    monkeypatch.setattr(capture, "_commit_control", stop_before_completion)
    monkeypatch.setattr(capture, "_hydrate", leave_parent_unplanned)
    with pytest.raises(RuntimeError, match="clean cache checkpoint"):
        capture.capture(store, resume=capture_id, client_factory=SeedClient)
    monkeypatch.setattr(capture, "_commit_control", original_commit_control)
    monkeypatch.setattr(capture, "_hydrate", original_hydrate)

    cache_path = store.path(f"tmp/recovery-index/{capture_id}/index.sqlite")
    assert cache_path.is_file()
    with store.writer_guard():
        state_before = capture.read_capture(store, capture_id, _writer_resume=True)
    assert all(work["done"] for work in state_before["tasks"])
    assert state_before["complete"] is False
    observations: list[tuple[int, int, bool]] = []
    reset_statements: list[str] = []
    original_hydrate_hint = capture._hydrate_from_recovery_hint
    original_reset = RecoveryParentIndex.reset
    old_relatives = {
        row["relative"]
        for row in state_before.page_rows
        if state_before.page_anchors[row["relative"]][0] <= state_before.snapshot_seq
    }
    events: list[str] = []
    requested: list[str] = []
    original_read_page = capture.read_page

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requested.append(endpoint)
            events.append("request")
            return super().get(endpoint, params, context)

    def observe_hydrate_hint(store, state, identities):
        observations.append(
            (
                identities.reused_pages,
                len(identities.processed),
                identities.defer_page_merges,
            )
        )
        return original_hydrate_hint(store, state, identities)

    def observe_old_page(store, relative, *args, **kwargs):
        if relative in old_relatives:
            assert requested
            events.append("old_page")
        return original_read_page(store, relative, *args, **kwargs)

    def replace_index_inode(identities):
        identities.db.set_trace_callback(reset_statements.append)
        return original_reset(identities)

    monkeypatch.setattr(capture, "_hydrate_from_recovery_hint", observe_hydrate_hint)
    monkeypatch.setattr(RecoveryParentIndex, "reset", replace_index_inode)
    monkeypatch.setattr(capture, "read_page", observe_old_page)
    resumed = capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    assert resumed["complete"] is True
    assert observations[0] == (len(state_before.page_rows), 0, True)
    assert requested[0] == "/events/EVENT-WARM-MISSING"
    assert events.index("request") < events.index("old_page")
    assert len(capture.read_capture(store, capture_id)["tasks"]) == len(state_before["tasks"]) + 1
    assert not any(statement.lower().startswith("delete from") for statement in reset_statements)
    assert capture.read_capture(store, capture_id)["complete"] is True


def test_warm_cache_skips_hint_hydration_when_known_tasks_are_pending(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "hydration", hydration_tasks=1)
    store, state, _ = _seed_recovery_cache(root, capture_id)
    assert any(not work["done"] for work in state["tasks"])
    hints: list[int] = []
    requests: list[str] = []
    original_hint = capture._hydrate_from_recovery_hint

    def observe_hint(store, current, identities):
        hints.append(len(current["tasks"]))
        return original_hint(store, current, identities)

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requests.append(endpoint)
            return super().get(endpoint, params, context)

    monkeypatch.setattr(capture, "_hydrate_from_recovery_hint", observe_hint)
    resumed = capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    assert resumed["complete"] is True
    assert hints == []
    assert requests[0] == "/events/EVENT-MISSING-000000"


@pytest.mark.parametrize("tamper", ["ahead", "settings", "schema", "corrupt"])
def test_invalid_derived_cache_is_discarded_without_changing_journal(tmp_path, tamper):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store, state, cache_path = _seed_recovery_cache(root, capture_id)
    journal_path = store.path(f"captures/{capture_id}/pages.jsonl")
    journal_before = journal_path.read_bytes()
    expected_settings = "0" * 64 if tamper == "settings" else sealed_settings_sha256(state)
    with store.writer_guard():
        if tamper == "ahead":
            with sqlite3.connect(cache_path) as db:
                db.execute(
                    "update recovery_meta set indexed_pages=? where id=1",
                    (len(state.page_rows) + 1,),
                )
        elif tamper == "schema":
            with sqlite3.connect(cache_path) as db:
                db.execute("create table unexpected(value text)")
        elif tamper == "corrupt":
            cache_path.write_bytes(b"not a sqlite database")
        index = RecoveryParentIndex(
            store,
            capture_id,
            root_id(store),
            expected_settings,
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )
        assert index.reused_pages == 0
        assert index.watermark_pages == 0
        index.close()
        with sqlite3.connect(cache_path) as db:
            assert db.execute("select indexed_pages from recovery_meta where id=1").fetchone() == (
                0,
            )
            assert db.execute(
                "select settings_sha256 from recovery_meta where id=1"
            ).fetchone() == (expected_settings,)
            assert (
                db.execute("select name from sqlite_schema where name='unexpected'").fetchone()
                is None
            )
    assert journal_path.read_bytes() == journal_before


def test_recovery_cache_rejects_hardlinked_database_before_connect(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store, state, cache_path = _seed_recovery_cache(root, capture_id)
    hardlink = store.path("cache-hardlink.sqlite")
    hardlink.hardlink_to(cache_path)

    with store.writer_guard(), pytest.raises(CatalogueError, match="unsafe"):
        RecoveryParentIndex(
            store,
            capture_id,
            root_id(store),
            sealed_settings_sha256(state),
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )

    assert hardlink.exists()
    assert cache_path.exists()


def test_discard_removes_only_the_retired_capture_cache(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store, _, cache_path = _seed_recovery_cache(root, capture_id)

    result = discard(store, capture_id)

    assert result["discarded"] is True
    assert not cache_path.parent.exists()


def test_retirement_plan_survives_crash_after_cache_cleanup(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store, _, cache_path = _seed_recovery_cache(root, capture_id)
    remove_cache = recovery_index.remove_capture_cache
    interrupted = False

    def remove_then_interrupt(actual_store, identity):
        nonlocal interrupted
        remove_cache(actual_store, identity)
        if identity == capture_id and not interrupted:
            interrupted = True
            raise OSError("simulated process stop after cache removal")

    monkeypatch.setattr(recovery_index, "remove_capture_cache", remove_then_interrupt)
    with pytest.raises(OSError, match="simulated process stop"):
        discard(store, capture_id)

    assert not cache_path.parent.exists()
    assert store.path("prune-plan.json").is_file()
    monkeypatch.setattr(recovery_index, "remove_capture_cache", remove_cache)
    incremental.finish_pending_retirement(store)
    assert not store.path("prune-plan.json").exists()


def test_prune_plan_derives_cache_owner_before_unlinking_plan(tmp_path, monkeypatch):
    root = tmp_path / "root"
    store = Store(root)
    with store.writer_guard():
        store.write("root-id.json", {"version": 1, "root_id": "a" * 32})
    result = capture.capture(store, client_factory=FakeClient)
    capture_id = result["capture_id"]
    cache_path = store.path(f"tmp/recovery-index/{capture_id}/index.sqlite")
    assert cache_path.is_file()
    store.write(
        "prune-plan.json",
        {
            "version": 1,
            "kind": "prune",
            "entries": [
                {
                    "source": f"captures/{capture_id}",
                    "target": f"prune-trash/{'e' * 32}-captures-{capture_id}",
                }
            ],
        },
    )
    remove_cache = recovery_index.remove_capture_cache
    interrupted = False

    def remove_then_interrupt(actual_store, identity):
        nonlocal interrupted
        remove_cache(actual_store, identity)
        if identity == capture_id and not interrupted:
            interrupted = True
            raise OSError("simulated process stop after cache removal")

    monkeypatch.setattr(recovery_index, "remove_capture_cache", remove_then_interrupt)
    with pytest.raises(OSError, match="simulated process stop"):
        incremental.finish_pending_retirement(store)

    assert not cache_path.parent.exists()
    assert store.path("prune-plan.json").is_file()
    monkeypatch.setattr(recovery_index, "remove_capture_cache", remove_cache)
    incremental.finish_pending_retirement(store)
    assert not store.path("prune-plan.json").exists()


def test_missing_logical_root_id_keeps_recovery_cache_ineligible(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    with store.writer_guard():
        store.remove("root-id.json")

    resumed = capture.capture(store, resume=capture_id, client_factory=FakeClient)

    assert resumed["complete"] is True
    assert not store.path("root-id.json").exists()
    assert not store.path(f"tmp/recovery-index/{capture_id}").exists()


def test_cache_page_limit_adds_free_space_to_existing_database(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store, state, _ = _seed_recovery_cache(root, capture_id)
    free_bytes = 8192
    monkeypatch.setattr(
        store,
        "usage",
        lambda: {
            "retained_bytes": store.limits.max_root_bytes - free_bytes,
            "temporary_bytes": store.limits.max_temp_bytes - free_bytes,
            "database_bytes": 0,
        },
    )

    with store.writer_guard():
        index = RecoveryParentIndex(
            store,
            capture_id,
            root_id(store),
            sealed_settings_sha256(state),
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )
        current_pages = index.db.execute("pragma page_count").fetchone()[0]
        page_size = index.db.execute("pragma page_size").fetchone()[0]
        page_limit = index.db.execute("pragma max_page_count").fetchone()[0]
        assert page_limit >= current_pages + free_bytes // page_size
        index.close(flush=False)


def test_late_cache_corruption_invalidates_and_replays_from_journal(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    original_merge = RecoveryParentIndex.merge_at_anchor
    failed = False

    def fail_once(index, page, relative, anchor):
        nonlocal failed
        if not failed:
            failed = True
            raise sqlite3.DatabaseError("database disk image is malformed")
        return original_merge(index, page, relative, anchor)

    monkeypatch.setattr(RecoveryParentIndex, "merge_at_anchor", fail_once)
    resumed = capture.capture(store, resume=capture_id, client_factory=FakeClient)

    assert failed is True
    assert resumed["complete"] is True
    state = capture.read_capture(store, capture_id)
    cache_path = store.path(f"tmp/recovery-index/{capture_id}/index.sqlite")
    with sqlite3.connect(cache_path) as db:
        assert db.execute("select indexed_pages from recovery_meta where id=1").fetchone() == (
            len(state.page_rows),
        )
