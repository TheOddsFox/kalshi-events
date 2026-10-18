from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path

import httpx
import pytest

from kalshi_events import common
from kalshi_events.common import Budget, BudgetExceeded, Limits, Store
from kalshi_events.transport import PublicClient


def bounded_store(tmp_path, *, root_extra: int, temp: int) -> Store:
    store = Store(tmp_path / "root")
    usage = store.usage()["retained_bytes"]
    store.limits = Limits(max_root_bytes=usage + root_extra, max_temp_bytes=temp)
    return store


@pytest.mark.parametrize(
    ("old", "new"),
    [(b"12345678", b"abcdefgh"), (b"1234", b"abcdefgh"), (b"12345678", b"ab")],
)
def test_atomic_replacement_requires_room_for_full_temporary_file(tmp_path, old, new):
    store = Store(tmp_path / "root")
    store.write_bytes("item", old)
    before = store.usage()["retained_bytes"]
    store.limits = Limits(max_root_bytes=before + len(new) - 1, max_temp_bytes=len(new) - 1)

    with pytest.raises(BudgetExceeded):
        store.write_bytes("item", new)

    assert store.path("item").read_bytes() == old
    assert store.usage()["retained_bytes"] == before
    assert store.usage()["temporary_bytes"] == 0
    assert not list(store.root.glob(".write-*"))


def test_atomic_replacement_succeeds_when_peak_headroom_covers_temp_file(tmp_path):
    store = Store(tmp_path / "root")
    store.write_bytes("item", b"12345678")
    before = store.usage()["retained_bytes"]
    store.limits = Limits(max_root_bytes=before + 8, max_temp_bytes=8)

    store.write_bytes("item", b"abcdefgh")

    assert store.path("item").read_bytes() == b"abcdefgh"
    assert store.usage()["retained_bytes"] == before
    assert store.usage()["temporary_bytes"] == 0


def test_failed_atomic_write_preserves_file_and_releases_storage_reservation(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    store.write_bytes("item", b"old")
    before = store.usage()

    def fail_sync(_descriptor):
        raise OSError("synthetic file sync failure")

    monkeypatch.setattr(common.os, "fsync", fail_sync)
    with pytest.raises(OSError, match="synthetic file sync failure"):
        store.write_bytes("item", b"replacement")

    assert store.path("item").read_bytes() == b"old"
    assert store.usage() == before
    assert store._storage_reservations == 0
    assert not list(store.root.glob(".write-*"))


def test_concurrent_atomic_writes_cannot_spend_same_storage_headroom(tmp_path):
    store = bounded_store(tmp_path, root_extra=8, temp=8)
    barrier = threading.Barrier(2)
    original = common.atomic_bytes

    def pause_after_reservation(path, body):
        if path.name in ("one", "two"):
            with suppress(threading.BrokenBarrierError):
                barrier.wait(timeout=0.5)
        return original(path, body)

    common.atomic_bytes = pause_after_reservation

    def write(name: str) -> str:
        barrier.wait()
        try:
            store.write_bytes(name, b"12345678")
        except BudgetExceeded:
            return "limited"
        return "written"

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, ("one", "two")))
    finally:
        common.atomic_bytes = original

    assert sorted(results) == ["limited", "written"]
    assert sum(store.path(name).exists() for name in ("one", "two")) == 1
    assert store.usage()["retained_bytes"] <= store.limits.max_root_bytes


def test_concurrent_public_client_receipts_respect_root_limit(tmp_path):
    root = tmp_path / "root"
    store = Store(
        root,
        Limits(
            max_root_bytes=450,
            max_temp_bytes=1024**2,
            max_response_bytes=32,
            max_decoded_response_bytes=32,
            max_rate=100_000,
        ),
    )
    client = PublicClient(store, Budget(store))
    client.client.close()
    client.client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, stream=httpx.ByteStream(b'{"series":[]}'))
        )
    )
    try:
        with store.writer_guard(), ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda _: _get_result(client), range(2)))
    finally:
        client.client.close()

    actual = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    assert "operator root storage exhausted" in outcomes
    assert actual <= store.limits.max_root_bytes
    assert store.usage()["retained_bytes"] == actual


def _get_result(client: PublicClient) -> str:
    try:
        client.get("/series")
    except BudgetExceeded as exc:
        return str(exc)
    return "success"


def test_created_ancestor_entries_are_synced_before_file_commit(tmp_path, monkeypatch):
    events: list[tuple[str, str]] = []
    original_fsync_directory = common.fsync_directory
    original_fsync = common.os.fsync

    def record_directory(path):
        events.append(("directory", str(path)))
        return original_fsync_directory(path)

    def record_fsync(descriptor):
        # Directory and file descriptors can both be synchronized; classify via fstat.
        import stat

        kind = "directory" if stat.S_ISDIR(common.os.fstat(descriptor).st_mode) else "file"
        events.append((kind, str(tmp_path)))
        return original_fsync(descriptor)

    monkeypatch.setattr(common, "fsync_directory", record_directory)
    monkeypatch.setattr(common.os, "fsync", record_fsync)
    store = Store(tmp_path / "new-root" / "nested")
    events.clear()
    store.write_bytes("raw/page/body.bin", b"evidence")

    root_sync = events.index(("directory", str(store.root)))
    parent_sync = events.index(("directory", str(store.root / "raw")))
    file_sync = events.index(("file", str(tmp_path)))
    page_sync = events.index(("directory", str(store.root / "raw" / "page")))
    assert root_sync < parent_sync < file_sync < page_sync


def test_append_creation_syncs_new_ancestor_entries(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    events: list[str] = []
    original = common.fsync_directory

    def record(path):
        events.append(str(path))
        return original(path)

    monkeypatch.setattr(common, "fsync_directory", record)
    store.append("journal/capture/log.jsonl", b"{}\n")

    assert events == [
        str(store.root.parent),
        str(store.root),
        str(store.root / "journal"),
        str(store.root / "journal" / "capture"),
    ]


def test_directory_sync_failure_is_retried_when_directory_already_exists(tmp_path, monkeypatch):
    target = tmp_path / "new" / "nested"
    original = common.fsync_directory
    attempts: list[Path] = []

    def fail_once(path):
        attempts.append(path)
        if len(attempts) == 2:
            raise OSError("synthetic parent sync failure")
        return original(path)

    monkeypatch.setattr(common, "fsync_directory", fail_once)
    with pytest.raises(OSError, match="synthetic parent sync failure"):
        common.durable_mkdir(target)

    common.durable_mkdir(target)

    assert target.is_dir()
    assert attempts == [tmp_path.parent, tmp_path, tmp_path, target.parent]
