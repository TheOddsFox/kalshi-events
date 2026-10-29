from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime

import httpx
import pytest
from conftest import FakeClient

from kalshi_events.capture import capture
from kalshi_events.cli import main, parser
from kalshi_events.common import Budget, BudgetExceeded, CatalogueError, Limits, Store
from kalshi_events.transport import (
    MAX_ATTEMPTS,
    PublicClient,
    RateLimiter,
    backoff_seconds,
    retry_after_seconds,
)


class SimulatedClock:
    def __init__(self):
        self.now = 0.0
        self.sleepers = 0
        self.condition = threading.Condition()

    def monotonic(self):
        with self.condition:
            return self.now

    def sleep(self, delay):
        with self.condition:
            deadline = self.now + delay
            self.sleepers += 1
            self.condition.notify_all()
            assert self.condition.wait_for(lambda: self.now >= deadline, timeout=2)

    def wait_for_sleepers(self, count):
        with self.condition:
            assert self.condition.wait_for(lambda: self.sleepers >= count, timeout=2)

    def record_start(self, starts):
        with self.condition:
            starts.append(self.now)
            self.condition.notify_all()

    def wait_for_starts(self, starts, count):
        with self.condition:
            assert self.condition.wait_for(lambda: len(starts) >= count, timeout=2)

    def advance(self, value):
        with self.condition:
            self.now = value
            self.condition.notify_all()


def test_default_rate_is_five_per_second_and_nonpositive_rates_are_rejected():
    assert Limits().max_rate == 5.0
    assert Limits().max_response_bytes == 384 * 1024**2
    assert RateLimiter(Limits().max_rate).interval == pytest.approx(0.2)
    assert RateLimiter(10).interval == pytest.approx(0.1)
    for value in (0, -1, 0.0):
        with pytest.raises(CatalogueError, match="max_rate"):
            Limits(max_rate=value)


@pytest.mark.parametrize(
    "command",
    [
        ["capture"],
        ["refresh", "--full"],
        ["refresh", "--capture-id", "existing-capture"],
        ["metadata", "refresh", "--market-ticker", "MARKET", "--output", "bundle"],
        ["series", "refresh", "--series-ticker", "KXCPI", "--full"],
    ],
)
@pytest.mark.parametrize("explicit_rate", [None, "10", "0.5"])
def test_acquisition_cli_defaults_to_five_and_preserves_explicit_rates(command, explicit_rate):
    arguments = command + (["--max-rate", explicit_rate] if explicit_rate is not None else [])
    args = parser().parse_args(arguments)
    assert args.max_rate == (5.0 if explicit_rate is None else float(explicit_rate))


def test_cli_rejects_zero_rate_before_touching_the_root(tmp_path, capsys):
    assert main(["--root", str(tmp_path / "root"), "capture", "--max-rate", "0"]) == 1
    assert "max_rate" in capsys.readouterr().err
    assert not (tmp_path / "root").exists()


def test_configured_rate_sets_shared_limiter_spacing(store):
    slower = Store(store.root, Limits(max_rate=2))
    with PublicClient(slower, Budget(slower)) as client:
        assert client.limiter.interval == pytest.approx(0.5)


def test_one_limiter_paces_every_start_at_its_interval(store, monkeypatch):
    clock = {"now": 0.0}
    store.started = 0.0

    def sleep(delay):
        clock["now"] += delay

    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleep)
    limiter = RateLimiter(max_rate=10)
    budget = Budget(store)
    starts = []
    for _ in range(5):
        limiter.acquire(budget)
        starts.append(clock["now"])
    # Each start waits for its slot: the k-th start is k intervals after the first.
    assert starts == pytest.approx([0.0, 0.1, 0.2, 0.3, 0.4])


def test_waiter_rechecks_shared_delay_added_while_sleeping(store, monkeypatch):
    clock = SimulatedClock()
    limiter = RateLimiter(max_rate=10)
    store.started = 0.0
    store.limits = Limits(max_duration_seconds=20)
    budget = Budget(store)

    monkeypatch.setattr("kalshi_events.transport.time.monotonic", clock.monotonic)
    monkeypatch.setattr("kalshi_events.transport.time.sleep", clock.sleep)
    limiter.acquire(budget)
    starts = []

    def acquire():
        limiter.acquire(budget)
        clock.record_start(starts)

    workers = [threading.Thread(target=acquire) for _ in range(2)]
    for worker in workers:
        worker.start()
    clock.wait_for_sleepers(2)
    limiter.defer(5)
    clock.advance(0.1)
    clock.wait_for_sleepers(4)
    clock.advance(5.0)
    clock.wait_for_sleepers(5)
    clock.wait_for_starts(starts, 1)
    clock.advance(5.1)
    for worker in workers:
        worker.join(timeout=2)
        assert not worker.is_alive()

    assert sorted(starts) == pytest.approx([5.0, 5.1])


def test_waiter_deadline_includes_a_delay_added_while_sleeping(store, monkeypatch):
    clock = SimulatedClock()
    limiter = RateLimiter(max_rate=10)
    store.started = 0.0
    store.limits = Limits(max_duration_seconds=2)
    budget = Budget(store)

    def sleep(delay):
        clock.advance(clock.monotonic() + delay)
        limiter.defer(5)

    monkeypatch.setattr("kalshi_events.transport.time.monotonic", clock.monotonic)
    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleep)
    limiter.acquire(budget)
    with pytest.raises(BudgetExceeded, match="deadline cannot accommodate request spacing"):
        limiter.acquire(budget)


def test_concurrent_waiters_start_at_spaced_slots(store, monkeypatch):
    clock = SimulatedClock()
    store.started = 0.0
    store.limits = Limits(max_duration_seconds=20)
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", clock.monotonic)
    monkeypatch.setattr("kalshi_events.transport.time.sleep", clock.sleep)
    limiter = RateLimiter(max_rate=10)
    budget = Budget(store)
    gate = threading.Barrier(4)
    starts = []
    limiter.acquire(budget)

    def acquire():
        gate.wait()
        limiter.acquire(budget)
        clock.record_start(starts)

    workers = [threading.Thread(target=acquire) for _ in range(3)]
    for worker in workers:
        worker.start()
    gate.wait()
    clock.wait_for_sleepers(3)
    clock.advance(0.1)
    clock.wait_for_starts(starts, 1)
    clock.wait_for_sleepers(5)
    clock.advance(0.2)
    clock.wait_for_starts(starts, 2)
    clock.wait_for_sleepers(6)
    clock.advance(0.31)
    for worker in workers:
        worker.join(timeout=2)
        assert not worker.is_alive()

    ordered = sorted(starts)
    assert len(ordered) == 3
    assert ordered == pytest.approx([0.1, 0.2, 0.31])


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
    clock = {"now": 0.0}
    sleeps = []
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock["now"])

    def sleep(delay):
        sleeps.append(delay)
        clock["now"] += delay

    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleep)
    store.started = 0.0
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


def test_attempt_observer_runs_after_rate_wait_and_cannot_block_http(store, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        "kalshi_events.transport.time.sleep",
        lambda delay: clock.__setitem__("now", clock["now"] + delay),
    )
    store.started = 0.0
    attempts = []

    def handler(request):
        attempts.append((str(request.url), clock["now"]))
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=httpx.ByteStream(b'{"series":[]}'),
        )

    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
        client.limiter.defer(5)
        observed = []
        client._set_attempt_observer(lambda endpoint: observed.append((endpoint, clock["now"])))
        client.get("/series")

        assert observed == [("/series", 5.0)]
        assert attempts[0][1] == 5.0

        def broken_observer(_endpoint):
            raise RuntimeError("observer failure")

        client._set_attempt_observer(broken_observer)
        client.get("/series")

    assert len(attempts) == 2


def test_four_attempts_then_bounded_failure(store, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        "kalshi_events.transport.time.sleep",
        lambda delay: clock.__setitem__("now", clock["now"] + delay),
    )
    store.started = 0.0
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

    state = capture(store, client_factory=Recording, catalogue_profile="exchange")
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

    state = capture(store, client_factory=Overlapping, catalogue_profile="exchange")
    assert state["complete"]
    assert peak["peak"] >= 2
