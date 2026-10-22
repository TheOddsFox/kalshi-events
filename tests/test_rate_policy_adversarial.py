from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from kalshi_events.capture import capture
from kalshi_events.common import Budget, BudgetExceeded, CatalogueError
from kalshi_events.transport import MAX_ATTEMPTS, PublicClient, RateLimiter


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


class Deadline:
    def __init__(self, clock, seconds=100):
        self.clock = clock
        self.seconds = seconds

    @property
    def remaining(self):
        remaining = self.seconds - self.clock.now
        if remaining <= 0:
            raise BudgetExceeded("deadline exhausted")
        return remaining


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock.now)
    monkeypatch.setattr("kalshi_events.transport.time.sleep", clock.sleep)
    return clock


def test_one_concurrent_response_wave_cannot_reduce_rate_once_per_worker(clock):
    limiter = RateLimiter(10, rate_policy="decrease-on-429")
    budget = Deadline(clock)
    generations = [limiter.acquire(budget) for _ in range(8)]
    barrier = threading.Barrier(8)

    def response(generation):
        barrier.wait(timeout=2)
        return limiter.on_429(generation)

    with ThreadPoolExecutor(max_workers=8) as pool:
        changed = list(pool.map(response, generations))

    assert sum(changed) == 1
    assert limiter.effective_rate == 5
    next_generation = limiter.acquire(budget)
    assert next_generation != generations[0]
    assert limiter.on_429(next_generation)
    assert limiter.effective_rate == 2.5
    assert not limiter.on_429(generations[0])
    assert limiter.effective_rate == 2.5


@pytest.mark.parametrize("initial_rate", [10, 0.5, 0.25, 0.1])
def test_repeated_new_generation_throttling_stops_at_operator_bounded_floor(clock, initial_rate):
    limiter = RateLimiter(initial_rate, rate_policy="decrease-on-429")
    budget = Deadline(clock, seconds=1000)
    observed = [initial_rate]
    for _ in range(12):
        generation = limiter.acquire(budget)
        limiter.on_429(generation)
        observed.append(limiter.effective_rate)

    assert observed == sorted(observed, reverse=True)
    assert observed[-1] == min(initial_rate, 0.25)
    assert all(min(initial_rate, 0.25) <= rate <= initial_rate for rate in observed)


def test_a_waiter_already_sleeping_rechecks_the_slower_interval(clock, monkeypatch):
    limiter = RateLimiter(10, rate_policy="decrease-on-429")
    budget = Deadline(clock)
    generation = limiter.acquire(budget)
    sleeping = threading.Event()
    resume = threading.Event()

    def sleep(delay):
        if not clock.sleeps:
            sleeping.set()
            assert resume.wait(timeout=2)
        clock.sleep(delay)

    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleep)
    with ThreadPoolExecutor(max_workers=1) as pool:
        waiter = pool.submit(limiter.acquire, budget)
        try:
            assert sleeping.wait(timeout=2)
            assert limiter.on_429(generation)
        finally:
            resume.set()
        assert waiter.result(timeout=2) != generation

    assert clock.now >= 0.2
    assert limiter.effective_rate == 5


def test_old_generation_retry_after_still_defers_all_starts(clock):
    limiter = RateLimiter(10, rate_policy="decrease-on-429")
    budget = Deadline(clock)
    generation = limiter.acquire(budget)
    assert limiter.on_429(generation)
    limiter.defer(3)
    clock.now = 1
    assert not limiter.on_429(generation)
    limiter.defer(7)
    limiter.acquire(budget)
    assert clock.now >= 8
    assert limiter.effective_rate == 5


def test_rate_reduction_respects_deadline_without_sleeping_past_it(clock):
    limiter = RateLimiter(1, rate_policy="decrease-on-429")
    budget = Deadline(clock, seconds=1.5)
    generation = limiter.acquire(budget)
    assert limiter.on_429(generation)
    with pytest.raises(BudgetExceeded, match="deadline"):
        limiter.acquire(budget)
    assert clock.now < 1.5


def test_fixed_policy_never_changes_spacing_after_throttling(clock):
    limiter = RateLimiter(10, rate_policy="fixed")
    budget = Deadline(clock)
    generation = limiter.acquire(budget)
    assert not limiter.on_429(generation)
    limiter.defer(7)
    limiter.acquire(budget)
    first = clock.now
    limiter.acquire(budget)
    assert clock.now - first == pytest.approx(0.1)
    assert limiter.effective_rate == 10


@pytest.mark.parametrize("rate_policy", [None, True, 0, "adaptive", "DECREASE-ON-429"])
def test_invalid_policy_is_rejected_instead_of_silently_falling_back(rate_policy):
    with pytest.raises(CatalogueError, match="policy"):
        RateLimiter(10, rate_policy=rate_policy)


@pytest.mark.parametrize("rate_policy", ["fixed", "decrease-on-429"])
def test_throttled_transport_keeps_four_attempt_bound_and_every_retry_after(
    store, monkeypatch, clock, rate_policy
):
    store.started = 0
    monkeypatch.setattr("kalshi_events.transport.random.uniform", lambda low, high: low)
    starts = []

    def handler(request):
        starts.append(clock.now)
        return httpx.Response(
            429,
            headers={"Retry-After": "7"},
            stream=httpx.ByteStream(b'{"error":"throttled"}'),
        )

    budget = Budget(store)
    with PublicClient(store, budget, rate_policy=rate_policy) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
        with pytest.raises(CatalogueError, match="HTTP 429"):
            client.get("/series")
        assert client.limiter.effective_rate == pytest.approx(
            10 if rate_policy == "fixed" else 0.625
        )

    assert len(starts) == MAX_ATTEMPTS == 4
    assert all(later - earlier >= 7 for earlier, later in zip(starts, starts[1:], strict=False))
    assert budget.requests == 4
    receipts = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    assert len(receipts) == 4
    assert all(receipt["status"] == 429 and receipt["accounting_complete"] for receipt in receipts)
    assert sum(receipt.get("retry_delay_seconds") == 7 for receipt in receipts) == 3


@pytest.mark.parametrize(
    ("operator_options", "expected_options", "sealed_compression", "sealed_rate_policy"),
    [
        ({}, {"compression": "gzip", "rate_policy": "decrease-on-429"}, "gzip", "decrease-on-429"),
        ({"compression": "gzip", "rate_policy": "fixed"}, {"compression": "gzip"}, "gzip", "fixed"),
        ({"compression": "identity", "rate_policy": "fixed"}, {}, "identity", "fixed"),
    ],
)
def test_custom_factory_receives_sealed_options_when_resume_flags_are_omitted(
    store, operator_options, expected_options, sealed_compression, sealed_rate_policy
):
    observed = []

    def stop_before_network(store, budget, **options):
        observed.append(options)
        raise BudgetExceeded("fixture stops before network")

    first = capture(store, client_factory=stop_before_network, **operator_options)
    assert not first["complete"]
    assert first["compression"] == sealed_compression
    assert first["rate_policy"] == sealed_rate_policy

    resumed = capture(store, resume=first["capture_id"], client_factory=stop_before_network)

    assert not resumed["complete"]
    assert resumed["capture_id"] == first["capture_id"]
    assert resumed["compression"] == sealed_compression
    assert resumed["rate_policy"] == sealed_rate_policy
    assert observed == [expected_options, expected_options]
    assert not list(store.path("raw").glob("*/manifest.json"))
