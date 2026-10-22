from __future__ import annotations

import json
import stat
import threading

import httpx
import pytest
from conftest import FakeClient

from kalshi_events import common
from kalshi_events.capture import capture
from kalshi_events.common import Budget, Limits, Store, json_bytes
from kalshi_events.transport import PublicClient, RateLimiter


@pytest.mark.parametrize("replacement", [b"x", b"a longer replacement"])
def test_failed_directory_sync_after_replace_accounts_installed_bytes(
    store, monkeypatch, replacement
):
    store.write_bytes("item", b"old bytes")
    original_sync = common.fsync_directory
    original_replace = common.os.replace
    installed = False

    def replace_and_record(source, destination):
        nonlocal installed
        original_replace(source, destination)
        installed = True

    def fail_directory_sync(path):
        if installed:
            raise OSError("replacement directory sync failed")
        return original_sync(path)

    with monkeypatch.context() as failure:
        failure.setattr(common.os, "replace", replace_and_record)
        failure.setattr(common, "fsync_directory", fail_directory_sync)
        with pytest.raises(OSError, match="replacement directory sync failed"):
            store.write_bytes("item", replacement)

    assert store.path("item").read_bytes() == replacement
    actual = sum(path.stat().st_size for path in store.root.rglob("*") if path.is_file())
    assert store.usage()["retained_bytes"] == actual
    assert store.usage()["temporary_bytes"] == 0
    assert not list(store.root.rglob(".write-*"))
    store.limits = Limits(max_root_bytes=actual + 4, max_temp_bytes=4)
    store.write_bytes("recovered", b"next")
    assert store.usage()["retained_bytes"] == actual + 4


def test_failed_append_sync_accounts_bytes_and_preserves_remaining_headroom(store, monkeypatch):
    store.append("journal", b"first\n")
    before = store.usage()["retained_bytes"]
    original_sync = common.os.fsync

    def fail_sync(descriptor):
        if store.path("journal").stat().st_size == len(b"first\nsecond\n"):
            raise OSError("journal sync failed")
        return original_sync(descriptor)

    with monkeypatch.context() as failure:
        failure.setattr(common.os, "fsync", fail_sync)
        with pytest.raises(OSError, match="journal sync failed"):
            store.append("journal", b"second\n")

    assert store.path("journal").read_bytes() == b"first\nsecond\n"
    assert store.usage()["retained_bytes"] == before + 7
    store.limits = Limits(max_root_bytes=before + 11, max_temp_bytes=4)
    store.write_bytes("recovered", b"next")
    assert store.usage()["retained_bytes"] == before + 11


def test_early_sleep_return_does_not_grant_request_before_its_start_slot(store, monkeypatch):
    clock = [0.0]
    waits = []

    def interrupted_sleep(delay):
        waits.append(delay)
        clock[0] += delay / 2 if len(waits) == 1 else delay

    store.started = 0.0
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("kalshi_events.transport.time.sleep", interrupted_sleep)
    limiter = RateLimiter(10)
    budget = Budget(store)
    limiter.acquire(budget)
    limiter.acquire(budget)

    assert clock[0] == pytest.approx(0.1)
    assert waits == pytest.approx([0.1, 0.05])


def test_capture_journals_only_receipts_with_durable_files_and_raw_directory_entry(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root", Limits(max_rate=1_000_000))
    original_sync = common.os.fsync
    original_directory_sync = common.fsync_directory
    original_append = Store.append
    durable_files = set()
    durable_pages = set()
    committed = []
    guard = threading.Lock()

    def sync_and_record(descriptor):
        original_sync(descriptor)
        descriptor_stat = common.os.fstat(descriptor)
        identity = (descriptor_stat.st_dev, descriptor_stat.st_ino)
        with guard:
            if stat.S_ISREG(descriptor_stat.st_mode):
                durable_files.add(identity)

    def sync_directory_and_record(path):
        result = original_directory_sync(path)
        raw = store.root / "raw"
        if path == raw:
            with guard:
                durable_pages.update(page for page in raw.iterdir() if page.is_dir())
        return result

    def append_after_evidence(actual_store, relative, body):
        record = json.loads(body)
        if record["op"] == "page":
            receipt = actual_store.path(record["payload"]["relative"])
            with guard:
                assert receipt.parent in durable_pages
                for path in (receipt, receipt.parent / "body.bin"):
                    file_stat = path.stat()
                    assert (file_stat.st_dev, file_stat.st_ino) in durable_files
                committed.append(receipt)
        return original_append(actual_store, relative, body)

    def client_factory(actual_store, budget, **options):
        source = FakeClient(actual_store, budget)

        def respond(request):
            endpoint = request.url.path.removeprefix("/trade-api/v2")
            status, payload = source.response(endpoint, dict(request.url.params))
            return httpx.Response(status, stream=httpx.ByteStream(json_bytes(payload)))

        client = PublicClient(actual_store, budget, **options)
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(respond), trust_env=False)
        return client

    monkeypatch.setattr(common.os, "fsync", sync_and_record)
    monkeypatch.setattr(common, "fsync_directory", sync_directory_and_record)
    monkeypatch.setattr(Store, "append", append_after_evidence)
    state = capture(store, client_factory=client_factory)

    assert state["complete"]
    assert committed
    assert len(committed) == len(list(store.path("raw").glob("*/manifest.json")))
