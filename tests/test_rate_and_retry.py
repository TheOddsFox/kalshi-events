from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime

import httpx
import pytest
from conftest import FakeClient

from kalshi_events.capture import capture
from kalshi_events.cli import main
from kalshi_events.common import Budget, CatalogueError, Limits, Store
from kalshi_events.transport import (
    MAX_ATTEMPTS,
    PublicClient,
    RateLimiter,
    backoff_seconds,
    retry_after_seconds,
)


def test_default_rate_is_ten_per_second_and_nonpositive_rates_are_rejected():
    assert Limits().max_rate == 10.0
    assert RateLimiter(10).interval == pytest.approx(0.1)
    for value in (0, -1, 0.0):
        with pytest.raises(CatalogueError, match="max_rate"):
            Limits(max_rate=value)


def test_cli_rejects_zero_rate_before_touching_the_root(tmp_path, capsys):
    assert main(["--root", str(tmp_path / "root"), "capture", "--max-rate", "0"]) == 1
    assert "max_rate" in capsys.readouterr().err
    assert not (tmp_path / "root").exists()


def test_configured_rate_sets_shared_limiter_spacing(store):
    slower = Store(store.root, Limits(max_rate=2))
    with PublicClient(slower, Budget(slower)) as client:
        assert client.limiter.interval == pytest.approx(0.5)


def test_one_limiter_paces_every_start_at_its_interval(store, monkeypatch):
    sleeps = []
    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleeps.append)
    limiter = RateLimiter(max_rate=10)
    budget = Budget(store)
    for _ in range(5):
        limiter.acquire(budget)
    # Each start waits for its slot: the k-th start is k intervals after the first.
    assert len(sleeps) == 4
    for slot, delay in enumerate(sleeps, start=1):
        assert 0.1 * slot - 0.05 < delay <= 0.1 * slot + 1e-9


def test_retry_after_delta_seconds_and_http_date_are_parsed():
    assert retry_after_seconds("7") == 7.0
    assert retry_after_seconds(" 2.5 ") == 2.5
    at = datetime(2026, 10, 17, 12, 0, 0, tzinfo=UTC)
    assert retry_after_seconds("Sat, 17 Oct 2026 12:01:30 GMT", at=at) == 90.0
    assert retry_after_seconds("Sat, 17 Oct 2026 11:00:00 GMT", at=at) == 0.0
    assert retry_after_seconds("soon") is None
    assert retry_after_seconds("nan") is None
    assert retry_after_seconds(None) is None


def test_backoff_is_jittered_exponential_capped_and_never_below_retry_after(monkeypatch):
    monkeypatch.setattr("kalshi_events.transport.random.uniform", lambda low, high: high)
    assert backoff_seconds(0, None) == 1.0
    assert backoff_seconds(2, None) == 4.0
    assert backoff_seconds(9, None) == 30.0
    assert backoff_seconds(0, 12.0) == 12.0
    monkeypatch.setattr("kalshi_events.transport.random.uniform", lambda low, high: low)
    assert backoff_seconds(3, None) == 4.0


def test_retry_after_is_honored_and_recorded_on_the_attempt_receipt(store, monkeypatch):
    sleeps = []
    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleeps.append)
    monkeypatch.setattr("kalshi_events.transport.random.uniform", lambda low, high: high)
    responses = [
        httpx.Response(
            429, headers={"Retry-After": "7"}, stream=httpx.ByteStream(b'{"rate":"limited"}')
        ),
        httpx.Response(200, stream=httpx.ByteStream(b'{"series":[]}')),
    ]
    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(lambda _: responses.pop(0)), trust_env=False
        )
        client.get("/series")
    assert any(delay >= 7 for delay in sleeps)
    receipts = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    throttled = next(row for row in receipts if row["status"] == 429)
    assert throttled["retry_after"] == "7"
    assert throttled["retry_delay_seconds"] == 7.0


def test_four_attempts_then_bounded_failure(store, monkeypatch):
    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(503, stream=httpx.ByteStream(b"busy"))

    budget = Budget(store)
    with PublicClient(store, budget) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
        with pytest.raises(CatalogueError, match="HTTP 503"):
            client.get("/series")
    assert MAX_ATTEMPTS == 4
    assert len(calls) == 4
    assert budget.requests == 4
    receipts = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    assert len(receipts) == 4
    assert sum("retry_delay_seconds" in row for row in receipts) == 3


def test_page_sizes_are_per_traversal(store):
    limits = {}

    class Recording(FakeClient):
        def response(self, endpoint, params):
            if "limit" in params:
                limits[endpoint] = params["limit"]
            return super().response(endpoint, params)

    state = capture(store, client_factory=Recording)
    assert state["complete"]
    assert limits == {
        "/events": 200,
        "/events/multivariate": 200,
        "/markets": 1000,
        "/historical/markets": 1000,
    }


def test_traversals_share_one_client_and_overlap(store):
    gate = threading.Lock()
    peak = {"active": 0, "peak": 0}

    class Overlapping(FakeClient):
        def response(self, endpoint, params):
            with gate:
                peak["active"] += 1
                peak["peak"] = max(peak["peak"], peak["active"])
            try:
                time.sleep(0.05)
                return super().response(endpoint, params)
            finally:
                with gate:
                    peak["active"] -= 1

    state = capture(store, client_factory=Overlapping)
    assert state["complete"]
    assert peak["peak"] >= 2
