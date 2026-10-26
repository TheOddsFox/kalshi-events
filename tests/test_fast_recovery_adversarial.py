from __future__ import annotations

import json
import sqlite3

import pytest
from conftest import FakeClient
from recovery_fixtures import build_recovery_root

from kalshi_events import capture
from kalshi_events.common import CatalogueError, Store, json_bytes
from kalshi_events.incremental import root_id
from kalshi_events.recovery_index import RecoveryParentIndex, sealed_settings_sha256


def _all_done_incomplete_capture(root, monkeypatch):
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    original_commit_control = capture._commit_control

    def stop_before_completion(store, state, kind, candidate):
        if kind == "complete":
            raise RuntimeError("simulated loss before completion control")
        return original_commit_control(store, state, kind, candidate)

    with monkeypatch.context() as patch:
        patch.setattr(capture, "_commit_control", stop_before_completion)
        with pytest.raises(RuntimeError, match="before completion control"):
            capture.capture(store, resume=capture_id, client_factory=FakeClient)

    with store.writer_guard():
        state = capture.read_capture(store, capture_id, _writer_resume=True)
    assert state["complete"] is False
    assert all(work["done"] and work["pending"] is None for work in state["tasks"])
    return store, capture_id, state


def _seed_recovery_cache(store, capture_id, state):
    directory_relative = f"tmp/recovery-index/{capture_id}"
    for name in ("index.sqlite", "index.sqlite-journal", "index.sqlite-wal", "index.sqlite-shm"):
        path = store.path(f"{directory_relative}/{name}")
        if path.exists():
            store.remove(f"{directory_relative}/{name}")

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
        try:
            for row in state.page_rows:
                capture._index_receipt(store, state, row, index)
            index.flush(force=True)
        finally:
            index.close(flush=False)
    return store.path(f"{directory_relative}/index.sqlite")


def test_tampered_old_body_allows_new_data_but_blocks_completion(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    before = capture.read_capture(store, capture_id)
    old_rows = capture.page_receipts(store, before)
    old_body = store.path(old_rows[0]["relative"]).with_name("body.bin")
    old_body.write_bytes(b"tampered old response")
    issued: list[str] = []

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            if endpoint != capture.CUTOFF_ENDPOINT:
                issued.append(context["request_id"])
            return super().get(endpoint, params, context)

    with pytest.raises(CatalogueError, match="digest|checksum|raw"):
        capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    after = capture.read_capture(store, capture_id)
    assert issued
    assert len(capture.page_receipts(store, after)) > len(old_rows)
    assert after["complete"] is False
    assert not any(
        row["op"] == "control" and row.get("payload", {}).get("kind") == "complete"
        for row in (
            json.loads(line)
            for line in store.path(f"captures/{capture_id}/pages.jsonl").read_text().splitlines()
        )
    )
    with pytest.raises(CatalogueError, match="incomplete"):
        capture.complete_capture(store, capture_id)


@pytest.mark.parametrize("corruption", ["snapshot", "journal"])
def test_invalid_snapshot_or_journal_fails_before_any_request(tmp_path, corruption):
    root = tmp_path / corruption
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    if corruption == "snapshot":
        snapshot_path = store.path(f"captures/{capture_id}/state.json")
        envelope = json.loads(snapshot_path.read_bytes())
        envelope["snapshot_sha256"] = "0" * 64
        store.write(f"captures/{capture_id}/state.json", envelope)
    else:
        journal_path = store.path(f"captures/{capture_id}/pages.jsonl")
        lines = journal_path.read_bytes().splitlines()
        first = json.loads(lines[0])
        first["checksum"] = "0" * 64
        lines[0] = json_bytes(first)
        journal_path.write_bytes(b"\n".join(lines) + b"\n")
    requests: list[str] = []

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requests.append(context["request_id"])
            return super().get(endpoint, params, context)

    with pytest.raises(CatalogueError, match="snapshot|journal|checksum|sequence"):
        capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    assert requests == []


def test_hydration_only_resume_starts_with_known_pending_lookup(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "hydration", hydration_tasks=1)
    store = Store(root)
    requests: list[str] = []

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requests.append(endpoint)
            return super().get(endpoint, params, context)

    resumed = capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    assert requests[0] == "/events/EVENT-MISSING-000000"
    assert resumed["complete"] is True


def test_resume_adopts_orphan_receipt_without_reissuing_its_request(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "hydration", hydration_tasks=1)
    store = Store(root)
    before = capture.read_capture(store, capture_id)
    pending_index = next(index for index, work in enumerate(before["tasks"]) if not work["done"])
    orphan_request_id = capture._request_context(before, pending_index)["request_id"]
    first_run_requests: list[str] = []

    class FirstRunClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            first_run_requests.append(context["request_id"])
            return super().get(endpoint, params, context)

    original_stage = capture._stage
    stage_failed = False

    def fail_after_raw_receipt(store, state, index, relative, identities):
        nonlocal stage_failed
        if not stage_failed:
            stage_failed = True
            raise RuntimeError("simulated process loss after raw receipt")
        return original_stage(store, state, index, relative, identities)

    monkeypatch.setattr(capture, "_stage", fail_after_raw_receipt)
    with pytest.raises(RuntimeError, match="process loss"):
        capture.capture(store, resume=capture_id, client_factory=FirstRunClient)
    monkeypatch.setattr(capture, "_stage", original_stage)

    assert first_run_requests == [orphan_request_id]
    recovered_requests: list[str] = []

    class RecoveryClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            recovered_requests.append(context["request_id"])
            return super().get(endpoint, params, context)

    resumed = capture.capture(store, resume=capture_id, client_factory=RecoveryClient)

    assert resumed["complete"] is True
    assert orphan_request_id not in recovered_requests
    matching = [
        json.loads(path.read_bytes())
        for path in store.path("raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("request_id") == orphan_request_id
    ]
    assert len(matching) == 1


def test_poisoned_parent_cache_rows_are_rebuilt_before_completion(tmp_path, monkeypatch):
    store, capture_id, state = _all_done_incomplete_capture(tmp_path / "root", monkeypatch)
    cache_path = _seed_recovery_cache(store, capture_id, state)
    poison = (
        "CACHE-POISON-SERIES",
        "CACHE-POISON-EVENT",
        "CACHE-POISON-MARKET",
        "CACHE-POISON-LOOKUP-EVENT",
        "CACHE-POISON-MARKET-LOOKUP",
    )
    with store.writer_guard(), sqlite3.connect(cache_path) as db:
        db.executemany(
            "insert into winners values(?,?,?,?,?)",
            [
                ("series", poison[0], "2099-01-01T00:00:00Z", "poison-series", None),
                ("events", poison[1], "2099-01-01T00:00:00Z", "poison-event", poison[0]),
                ("markets", poison[2], "2099-01-01T00:00:00Z", "poison-market", poison[1]),
                (
                    "markets",
                    poison[4],
                    "2099-01-01T00:00:00Z",
                    "poison-market-lookup",
                    poison[3],
                ),
            ],
        )
        db.executemany(
            "insert into lookups values(?,?,?,?,?)",
            [
                ("events", poison[1], 200, "poison-event-page", "poison-event-request"),
                (
                    "events",
                    poison[3],
                    404,
                    "poison-lookup-page",
                    "poison-lookup-request",
                ),
            ],
        )
        db.executemany(
            "insert into membership values(?,?,?)",
            [
                ("series", 0, poison[0]),
                ("events_live", 0, poison[1]),
                ("markets_live", 0, poison[2]),
                ("markets_live", 0, poison[4]),
            ],
        )

    hints: list[tuple[int, tuple[bool, bool]]] = []
    requested: list[str] = []
    original_hint = capture._hydrate_from_recovery_hint

    def observe_hint(store, current, identities):
        result = original_hint(store, current, identities)
        hints.append((identities.reused_pages, result))
        return result

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requested.append(endpoint)
            return super().get(endpoint, params, context)

    monkeypatch.setattr(capture, "_hydrate_from_recovery_hint", observe_hint)
    resumed = capture.capture(store, resume=capture_id, client_factory=RecordingClient)

    assert resumed["complete"] is True
    assert hints == [(len(state.page_rows), (False, False))]
    assert not any(any(value in endpoint for value in poison) for endpoint in requested)
    with store.writer_guard():
        after = capture.read_capture(store, capture_id, _writer_resume=True)
    with sqlite3.connect(cache_path) as db:
        assert db.execute("select indexed_pages from recovery_meta where id=1").fetchone() == (
            len(after.page_rows),
        )
        assert not any(
            db.execute(
                f"select 1 from {table} where {column} like 'CACHE-POISON-%' limit 1"
            ).fetchone()
            for table, column in (
                ("winners", "ticker"),
                ("lookups", "ticker"),
                ("membership", "ticker"),
            )
        )
        assert db.execute(
            "select 1 from winners where kind='markets' and ticker='MARKET'"
        ).fetchone()


def test_partial_recovery_merge_rolls_back_without_reissuing_committed_page(tmp_path, monkeypatch):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=1)
    store = Store(root)
    with store.writer_guard():
        before = capture.read_capture(store, capture_id, _writer_resume=True)
    before_ids = {row["request_id"] for row in before.page_rows}
    first_requests: list[str] = []
    original_merge = RecoveryParentIndex._merge
    injected = False

    class FirstRunClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            first_requests.append(context["request_id"])
            return super().get(endpoint, params, context)

    def fail_after_partial_merge(index, page):
        nonlocal injected
        original_merge(index, page)
        if not injected:
            injected = True
            index.db.execute(
                "insert into winners values(?,?,?,?,?)",
                ("events", "CACHE-PARTIAL-POISON", "2099", "partial", "SERIES"),
            )
            raise RuntimeError("simulated power loss before cache anchor binding")

    monkeypatch.setattr(RecoveryParentIndex, "_merge", fail_after_partial_merge)
    with pytest.raises(RuntimeError, match="before cache anchor binding"):
        capture.capture(store, resume=capture_id, client_factory=FirstRunClient)
    monkeypatch.setattr(RecoveryParentIndex, "_merge", original_merge)

    with store.writer_guard():
        after_crash = capture.read_capture(store, capture_id, _writer_resume=True)
    committed_after_crash = {row["request_id"] for row in after_crash.page_rows} - before_ids
    assert injected
    assert committed_after_crash
    assert committed_after_crash.issubset(first_requests)
    assert after_crash["complete"] is False
    cache_path = store.path(f"tmp/recovery-index/{capture_id}/index.sqlite")
    with sqlite3.connect(cache_path) as db:
        assert db.execute("select indexed_pages from recovery_meta where id=1").fetchone() == (0,)
        assert (
            db.execute("select 1 from winners where ticker='CACHE-PARTIAL-POISON'").fetchone()
            is None
        )

    retry_requests: list[str] = []

    class RecoveryClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            retry_requests.append(context["request_id"])
            return super().get(endpoint, params, context)

    resumed = capture.capture(store, resume=capture_id, client_factory=RecoveryClient)

    assert resumed["complete"] is True
    assert committed_after_crash.isdisjoint(retry_requests)


@pytest.mark.parametrize("failure", ["flush", "fsync"])
def test_recovery_cache_flush_failure_keeps_capture_incomplete_and_resumable(
    tmp_path, monkeypatch, failure
):
    store, capture_id, state = _all_done_incomplete_capture(tmp_path / failure, monkeypatch)
    committed_ids = {row["request_id"] for row in state.page_rows}
    original_flush = RecoveryParentIndex.flush
    failed = False

    def fail_before_complete(index, *, force=False):
        nonlocal failed
        if force and not failed and len(index.processed) == len(index.page_rows):
            failed = True
            if failure == "flush":
                raise OSError("simulated cache flush failure")
            original_sync = index.store.sync
            sync_failed = False

            def fail_cache_sync(relative):
                nonlocal sync_failed
                if relative == index.directory_relative and not sync_failed:
                    sync_failed = True
                    raise OSError("simulated cache fsync failure")
                return original_sync(relative)

            index.store.sync = fail_cache_sync
            try:
                return original_flush(index, force=force)
            finally:
                index.store.sync = original_sync
        return original_flush(index, force=force)

    class NoRequestClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            pytest.fail(f"all requests were already committed: {endpoint}")

    monkeypatch.setattr(RecoveryParentIndex, "flush", fail_before_complete)
    with pytest.raises(OSError, match="simulated cache (flush|fsync) failure"):
        capture.capture(store, resume=capture_id, client_factory=NoRequestClient)
    monkeypatch.setattr(RecoveryParentIndex, "flush", original_flush)

    assert failed
    with store.writer_guard():
        after_failure = capture.read_capture(store, capture_id, _writer_resume=True)
    assert after_failure["complete"] is False
    assert all(work["done"] for work in after_failure["tasks"])
    with pytest.raises(CatalogueError, match="incomplete"):
        capture.complete_capture(store, capture_id)

    retried: list[str] = []

    class RetryClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            retried.append(context["request_id"])
            return super().get(endpoint, params, context)

    resumed = capture.capture(store, resume=capture_id, client_factory=RetryClient)

    assert resumed["complete"] is True
    assert committed_ids.isdisjoint(retried)
    assert retried == []
