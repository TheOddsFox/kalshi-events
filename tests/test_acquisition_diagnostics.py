from __future__ import annotations

import gzip
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

import httpx
import pytest

from kalshi_events import capture, metrics
from kalshi_events.common import Budget, EvidenceSpool, Limits, Store
from kalshi_events.metrics import collect, endpoint_family, phase, thread_cpu_phase
from kalshi_events.transport import PublicClient


def _client(tmp_path, monkeypatch, body=b'{"markets":[]}'):
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1/trade-api/v2")
    store = Store(tmp_path / "root", limits=Limits(max_rate=1000))
    client = PublicClient(store, Budget(store))
    client.client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=httpx.ByteStream(body))),
        trust_env=False,
    )
    return client, store


def test_endpoint_family_is_fixed_cardinality_and_never_contains_ticker():
    assert endpoint_family("/markets/TICKER-123?cursor=secret") == "/markets/{ticker}"
    assert endpoint_family("/historical/markets/TICKER-123") == "/historical/markets/{ticker}"
    assert endpoint_family("/events/multivariate") == "/events/multivariate"
    assert endpoint_family("/unknown/arbitrary-secret") == "other"
    assert endpoint_family("/markets/not%20a%20ticker") == "other"

    with collect() as measured:
        for index in range(1000):
            with metrics.endpoint_attempt(f"/markets/TICKER-{index}", "identity"):
                pass
    report = measured.report()
    assert list(report["endpoint_families"]) == ["/markets/{ticker}"]
    assert report["endpoint_families"]["/markets/{ticker}"]["attempts"] == 1000


def test_transport_aggregates_attempt_status_encoding_bytes_and_latency(tmp_path, monkeypatch):
    client, store = _client(tmp_path, monkeypatch)
    try:
        with collect() as measured:
            receipt_path = client.get("/markets/TICKER-1")
        manifest = json.loads(store.path(receipt_path).read_text())
    finally:
        client.client.close()

    report = measured.report()
    family = report["endpoint_families"]["/markets/{ticker}"]
    assert family["attempts"] == 1
    assert family["response_statuses"] == {"200": 1}
    assert family["requested_encodings"] == {"identity": 1}
    assert family["observed_encodings"] == {"identity": 1}
    assert family["encoded_bytes"] == len(b'{"markets":[]}')
    assert family["decoded_bytes"] == len(b'{"markets":[]}')
    assert family["latency_count"] == 1
    assert family["latency_total_seconds"] >= 0
    assert family["latency_max_seconds"] >= 0
    assert "TICKER-1" not in json.dumps(report)
    assert manifest["endpoint"] == "/markets/TICKER-1"


def test_timers_close_on_failure_and_sync_error_still_propagates(tmp_path, monkeypatch):
    from kalshi_events import common

    path = tmp_path / "evidence"
    path.write_bytes(b"data")

    def fail(_fd):
        raise OSError("injected fsync failure")

    monkeypatch.setattr(common.os, "fsync", fail)
    with (
        collect() as measured,
        pytest.raises(OSError, match="injected fsync failure"),
        phase("synthetic_failure"),
    ):
        common.fsync_file(path)
    report = measured.report()
    assert report["phases_seconds"]["synthetic_failure"] >= 0
    assert report["phases_seconds"]["file_sync"] >= 0
    assert report["counters"]["fsync_calls"] == 1


def test_cpu_phase_and_worker_context_are_measured():
    with collect() as measured:
        with thread_cpu_phase("page_processing_cpu"):
            sum(range(1000))
        worker_context = copy_context()

        def worker_request():
            with metrics.http_request():
                pass

        worker = threading.Thread(target=lambda: worker_context.run(worker_request))
        worker.start()
        worker.join()
    assert measured.report()["phases_seconds"]["page_processing_cpu"] >= 0
    assert measured.report()["peak_http_concurrency"] == 1


def test_http_concurrency_gauge_tracks_active_requests_only_when_enabled():
    barrier = threading.Barrier(3)
    both_active = threading.Barrier(3)
    release = threading.Event()

    def request():
        barrier.wait()
        with metrics.http_request():
            both_active.wait()
            release.wait(timeout=2)

    with collect() as measured:
        threads = [threading.Thread(target=copy_context().run, args=(request,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        both_active.wait()
        release.set()
        for thread in threads:
            thread.join()
    assert measured.report()["peak_http_concurrency"] == 2


def test_nested_and_parallel_collectors_keep_concurrency_local():
    with collect() as outer:
        with metrics.http_request(), collect() as inner, metrics.http_request():
            pass
        assert inner.report()["peak_http_concurrency"] == 1
    assert outer.report()["peak_http_concurrency"] == 1

    barrier = threading.Barrier(2)
    reports = []

    def isolated():
        with collect() as measured:
            barrier.wait()
            with metrics.http_request():
                pass
            reports.append(measured.report())

    contexts = [copy_context() for _ in range(2)]
    threads = [threading.Thread(target=context.run, args=(isolated,)) for context in contexts]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert [report["peak_http_concurrency"] for report in reports] == [1, 1]


def test_http_waits_are_separate_from_response_processing(tmp_path, monkeypatch):
    class DelayedBody(httpx.SyncByteStream):
        def __iter__(self):
            time.sleep(0.01)
            yield b'{"markets":'
            time.sleep(0.01)
            yield b"[]}"

    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1/trade-api/v2")
    store = Store(tmp_path / "root", limits=Limits(max_rate=1000))
    client = PublicClient(store, Budget(store))

    def delayed_headers(_request):
        time.sleep(0.01)
        return httpx.Response(200, stream=DelayedBody())

    client.client = httpx.Client(transport=httpx.MockTransport(delayed_headers), trust_env=False)
    original_write = EvidenceSpool.write

    def slow_write(self, *args, **kwargs):
        actual_write = self.stream.write

        def delayed_write(chunk):
            time.sleep(0.03)
            return actual_write(chunk)

        self.stream.write = delayed_write
        return original_write(self, *args, **kwargs)

    monkeypatch.setattr("kalshi_events.common.EvidenceSpool.write", slow_write)
    try:
        with collect() as measured:
            client.get("/markets")
    finally:
        client.client.close()
    phases = measured.report()["phases_seconds"]
    assert phases["http_header_wait"] >= 0.008
    assert phases["response_iterator_wait"] >= 0.015
    assert phases["spool_write"] >= 0.025
    assert phases["request"] >= phases["spool_write"]


def test_gzip_expansion_failure_keeps_observed_decoded_bytes(tmp_path, monkeypatch):
    body = gzip.compress(b"decoded-before-trailing-data") + b"trailing"
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1/trade-api/v2")
    store = Store(tmp_path / "root", limits=Limits(max_rate=1000))
    client = PublicClient(store, Budget(store), compression="gzip")
    client.client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-encoding": "gzip"},
                stream=httpx.ByteStream(body),
            )
        ),
        trust_env=False,
    )
    try:
        with collect() as measured, pytest.raises(Exception, match="trailing data"):
            client.get("/markets")
    finally:
        client.client.close()
    family = measured.report()["endpoint_families"]["/markets"]
    assert family["encoded_bytes"] == len(body)
    assert family["decoded_bytes"] == len(b"decoded-before-trailing-data")


def test_retry_wait_is_excluded_from_endpoint_latency(tmp_path, monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1/trade-api/v2")
    store = Store(tmp_path / "root", limits=Limits(max_rate=1000))
    client = PublicClient(store, Budget(store))
    responses = [
        httpx.Response(503, stream=httpx.ByteStream(b"busy")),
        httpx.Response(200, stream=httpx.ByteStream(b"{}")),
    ]
    client.client = httpx.Client(
        transport=httpx.MockTransport(lambda _: responses.pop(0)), trust_env=False
    )
    client._pause = lambda _delay: time.sleep(0.03)
    try:
        started = time.perf_counter()
        with collect() as measured:
            client.get("/markets")
        wall = time.perf_counter() - started
    finally:
        client.client.close()
    family = measured.report()["endpoint_families"]["/markets"]
    assert family["attempts"] == 2
    assert family["response_statuses"] == {"200": 1, "503": 1}
    assert wall - family["latency_total_seconds"] >= 0.02


def test_recovered_page_count_requires_successful_commit_and_application(monkeypatch):
    class Deadline:
        @property
        def remaining(self):
            return 30

    def invoke(fail_consume):
        state = {"tasks": [{"done": False, "endpoint": "/markets"}]}
        recovered = {"request-id": "raw/evidence/manifest.json"}
        monkeypatch.setattr(capture, "_request_context", lambda *_: {"request_id": "request-id"})
        monkeypatch.setattr(capture, "_phase", lambda _work: "page")
        monkeypatch.setattr(capture, "_plan", lambda *_: ("/markets", {}, "markets", False))
        monkeypatch.setattr(capture, "_stage", lambda *_: None)

        def consume(*_):
            if fail_consume:
                raise capture.CatalogueError("receipt validation failed")
            state["tasks"][0]["done"] = True

        monkeypatch.setattr(capture, "_consume", consume)
        monkeypatch.setattr(capture, "_hydrate", lambda *_: False)
        monkeypatch.setattr(capture, "save", lambda *_: None)
        with ThreadPoolExecutor(max_workers=1) as pool, collect() as measured:
            try:
                capture._run(
                    None,
                    state,
                    None,
                    pool,
                    recovered,
                    {},
                    Deadline(),
                    None,
                )
            except capture.CatalogueError:
                assert fail_consume
        family = measured.report()["endpoint_families"].get("/markets", {})
        return family.get("recovered_pages", 0)

    assert invoke(True) == 0
    assert invoke(False) == 1


def test_legacy_page_is_not_counted_committed_before_receipt_validation(monkeypatch):
    state = {
        "tasks": [{"done": False, "pending": "raw/evidence/manifest.json"}],
        "max_response_bytes": 100,
    }
    monkeypatch.setattr(
        capture,
        "read_page",
        lambda *_args: (_ for _ in ()).throw(capture.CatalogueError("invalid body")),
    )
    with collect() as measured, pytest.raises(capture.CatalogueError, match="invalid body"):
        capture._consume(None, state, 0, "raw/evidence/manifest.json", {}, None)
    assert measured.report()["endpoint_families"] == {}


def test_legacy_cutoff_page_uses_validated_receipt_endpoint(monkeypatch):
    state = {
        "tasks": [{"done": False, "pending": "raw/cutoff/manifest.json", "cutoff_due": True}],
        "max_response_bytes": 100,
    }
    manifest = {
        "endpoint": "/historical/cutoff",
        "request_id": "cutoff-request",
        "status": 200,
    }
    payload = {
        "market_settled_ts": "2026-01-01T00:00:00Z",
        "trades_created_ts": "2026-01-01T00:00:00Z",
    }
    monkeypatch.setattr(capture, "_phase", lambda _work: "cutoff")
    monkeypatch.setattr(capture, "_request_context", lambda *_: {"request_id": "cutoff-request"})
    monkeypatch.setattr(capture, "_receipt_matches_context", lambda *_: True)
    monkeypatch.setattr(capture, "read_page", lambda *_: (manifest, payload))
    monkeypatch.setattr(capture, "save", lambda *_: None)

    class IdentityIndex:
        processed = set()

    with collect() as measured:
        capture._consume(None, state, 0, "raw/cutoff/manifest.json", {}, IdentityIndex())

    families = measured.report()["endpoint_families"]
    assert set(families) == {"/historical/cutoff"}
    assert families["/historical/cutoff"]["committed_pages"] == 1


def test_real_capture_records_v2_page_processing_cpu(store, factory):
    with collect() as measured:
        result = capture.capture(store, client_factory=factory)

    report = measured.report()
    assert result["complete"]
    assert report["phases_seconds"]["page_processing_cpu"] > 0


def test_metrics_disabled_does_not_change_receipt_bytes(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from kalshi_events import transport

    monkeypatch.setattr(transport, "now", lambda: "2026-01-01T00:00:00Z")
    monkeypatch.setattr(transport.uuid, "uuid4", lambda: SimpleNamespace(hex="frozen-receipt-id"))

    def acquire(path, enabled):
        client, store = _client(path, monkeypatch)
        try:
            if enabled:
                with collect():
                    receipt = client.get("/markets/SAME-TICKER")
            else:
                receipt = client.get("/markets/SAME-TICKER")
            return (
                store.path(receipt).read_bytes(),
                store.path(receipt).with_name("body.bin").read_bytes(),
            )
        finally:
            client.client.close()

    assert acquire(tmp_path / "measured", True) == acquire(tmp_path / "unmeasured", False)
