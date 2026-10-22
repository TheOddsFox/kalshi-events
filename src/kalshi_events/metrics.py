"""Opt-in, out-of-band pipeline measurements."""

from __future__ import annotations

import contextvars
import json
import os
import re
import resource
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, TextIO

_active: contextvars.ContextVar[Metrics | None] = contextvars.ContextVar(
    "kalshi_events_metrics", default=None
)

# Keep endpoint labels deliberately finite. Anything added upstream is reported as
# ``other`` until explicitly reviewed here; never emit a ticker or query string.
_ENDPOINT_FAMILIES = frozenset(
    {
        "/series",
        "/series/{ticker}",
        "/events",
        "/events/{ticker}",
        "/events/multivariate",
        "/multivariate_event_collections",
        "/multivariate_event_collections/{ticker}",
        "/markets",
        "/markets/{ticker}",
        "/historical/cutoff",
        "/historical/markets",
        "/historical/markets/{ticker}",
    }
)
_TICKER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")


def endpoint_family(endpoint: str) -> str:
    """Return a fixed-cardinality family label without preserving request identifiers."""
    path = endpoint.split("?", 1)[0].split("#", 1)[0]
    if path in _ENDPOINT_FAMILIES:
        return path
    parts = path.split("/")
    if len(parts) == 3 and parts[1] in {"series", "events", "markets"}:
        candidate = f"/{parts[1]}/{{ticker}}"
        if candidate in _ENDPOINT_FAMILIES and _TICKER.fullmatch(parts[2]):
            return candidate
    if len(parts) == 4 and parts[1:3] == ["historical", "markets"] and _TICKER.fullmatch(parts[3]):
        return "/historical/markets/{ticker}"
    if (
        len(parts) == 3
        and parts[1] == "multivariate_event_collections"
        and _TICKER.fullmatch(parts[2])
    ):
        return "/multivariate_event_collections/{ticker}"
    return "other"


class EndpointAttempt:
    """Bounded details for one HTTP attempt, attached to an endpoint family."""

    def __init__(self, metrics: Metrics | None, family: str, requested_encoding: str):
        self._metrics = metrics
        self._family = family
        self._requested_encoding = requested_encoding
        self._started = 0.0
        self._finished = False

    def __enter__(self) -> EndpointAttempt:
        if self._metrics is not None:
            self._started = time.perf_counter()
            with self._metrics._lock:
                row = self._metrics.endpoint_families.setdefault(self._family, _new_endpoint_row())
                row["attempts"] += 1
                _increment(row["requested_encodings"], self._requested_encoding)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.finish()

    def finish(self) -> None:
        if self._finished:
            return
        self._finished = True
        if self._metrics is not None:
            elapsed = time.perf_counter() - self._started
            with self._metrics._lock:
                row = self._metrics.endpoint_families[self._family]
                row["latency_count"] += 1
                row["latency_total_seconds"] += elapsed
                row["latency_max_seconds"] = max(row["latency_max_seconds"], elapsed)

    def response(self, status: int, observed_encoding: str) -> None:
        if self._metrics is None:
            return
        encoding = observed_encoding if observed_encoding in {"identity", "gzip"} else "other"
        with self._metrics._lock:
            row = self._metrics.endpoint_families[self._family]
            _increment(row["response_statuses"], str(status))
            _increment(row["observed_encodings"], encoding)

    def add_bytes(self, *, encoded: int = 0, decoded: int = 0) -> None:
        if self._metrics is not None:
            with self._metrics._lock:
                row = self._metrics.endpoint_families[self._family]
                row["encoded_bytes"] += encoded
                row["decoded_bytes"] += decoded


def _new_endpoint_row() -> dict[str, Any]:
    return {
        "attempts": 0,
        "response_statuses": {},
        "encoded_bytes": 0,
        "decoded_bytes": 0,
        "requested_encodings": {},
        "observed_encodings": {},
        "latency_count": 0,
        "latency_total_seconds": 0.0,
        "latency_max_seconds": 0.0,
        "committed_pages": 0,
        "recovered_pages": 0,
    }


def _increment(values: dict[str, int], key: str) -> None:
    values[key] = values.get(key, 0) + 1


class Metrics:
    def __init__(self) -> None:
        self.started = time.perf_counter()
        self._lock = threading.Lock()
        self.timings: dict[str, float] = defaultdict(float)
        self.counters: dict[str, int] = defaultdict(int)
        self.high_water: dict[str, int] = defaultdict(int)
        self.peak_http_concurrency = 0
        self.active_http_requests = 0
        self.endpoint_families: dict[str, dict[str, Any]] = {}

    def add(self, name: str, value: int = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def peak(self, name: str, value: int) -> None:
        with self._lock:
            self.high_water[name] = max(self.high_water[name], value)

    def report(self) -> dict[str, Any]:
        def peak_rss(who: int) -> int:
            value = resource.getrusage(who).ru_maxrss
            return int(value if sys.platform == "darwin" else value * 1024)

        peak_rss_bytes = {
            "self": peak_rss(resource.RUSAGE_SELF),
            "children": peak_rss(resource.RUSAGE_CHILDREN),
        }

        return {
            "version": 1,
            "elapsed_seconds": round(time.perf_counter() - self.started, 6),
            "phases_seconds": {key: round(value, 6) for key, value in sorted(self.timings.items())},
            "counters": dict(sorted(self.counters.items())),
            "store_accounted_high_water_bytes": dict(sorted(self.high_water.items())),
            "peak_http_concurrency": self.peak_http_concurrency,
            "peak_rss_bytes": peak_rss_bytes,
            "endpoint_families": {
                family: _endpoint_report(row)
                for family, row in sorted(self.endpoint_families.items())
            },
        }


def _endpoint_report(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "response_statuses": dict(sorted(row["response_statuses"].items())),
        "requested_encodings": dict(sorted(row["requested_encodings"].items())),
        "observed_encodings": dict(sorted(row["observed_encodings"].items())),
        "latency_total_seconds": round(row["latency_total_seconds"], 6),
        "latency_max_seconds": round(row["latency_max_seconds"], 6),
    }


@contextmanager
def collect() -> Iterator[Metrics]:
    """Enable measurements only inside this context; inactive hooks are a no-op."""
    metrics = Metrics()
    token = _active.set(metrics)
    try:
        yield metrics
    finally:
        _active.reset(token)


@contextmanager
def phase(name: str) -> Iterator[None]:
    metrics = _active.get()
    if metrics is None:
        yield
        return
    started = time.perf_counter()
    try:
        yield
    finally:
        with metrics._lock:
            metrics.timings[name] += time.perf_counter() - started


@contextmanager
def thread_cpu_phase(name: str) -> Iterator[None]:
    """Measure main-thread CPU time without including time spent blocked on I/O."""
    metrics = _active.get()
    if metrics is None:
        yield
        return
    started = time.thread_time()
    try:
        yield
    finally:
        with metrics._lock:
            metrics.timings[name] += time.thread_time() - started


@contextmanager
def endpoint_attempt(endpoint: str, requested_encoding: str) -> Iterator[EndpointAttempt]:
    """Collect bounded attempt telemetry; inactive operation performs no clock reads."""
    current = _active.get()
    family = endpoint_family(endpoint) if current is not None else "other"
    attempt = EndpointAttempt(current, family, requested_encoding)
    with attempt:
        yield attempt


def endpoint_page(endpoint: str, *, recovered: bool = False) -> None:
    """Count one durable page transition or receipt recovered into the journal."""
    current = _active.get()
    if current is None:
        return
    family = endpoint_family(endpoint)
    with current._lock:
        row = current.endpoint_families.setdefault(family, _new_endpoint_row())
        key = "recovered_pages" if recovered else "committed_pages"
        row[key] += 1


@contextmanager
def http_request() -> Iterator[None]:
    """Record actual active HTTP work, excluding requests waiting for the rate limiter."""
    current = _active.get()
    if current is None:
        yield
        return
    with current._lock:
        current.active_http_requests += 1
        active = current.active_http_requests
        current.peak_http_concurrency = max(current.peak_http_concurrency, active)
    try:
        yield
    finally:
        with current._lock:
            current.active_http_requests -= 1


def add(name: str, value: int = 1) -> None:
    metrics = _active.get()
    if metrics is not None:
        metrics.add(name, value)


def peak(name: str, value: int) -> None:
    metrics = _active.get()
    if metrics is not None:
        metrics.peak(name, value)


def reserve_report(path: str | os.PathLike[str]) -> TextIO:
    """Validate and exclusively reserve the out-of-band report before pipeline work starts."""
    from pathlib import Path

    from kalshi_events.common import CatalogueError, safe_path

    try:
        destination = safe_path(Path(path))
    except CatalogueError as exc:
        raise OSError("metrics report path cannot contain symlinks") from exc
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination.open("x", encoding="utf-8")


def write_report(
    path: str | os.PathLike[str], metrics: Metrics, stream: TextIO | None = None
) -> None:
    """Write metrics separately from command JSON and operator evidence."""
    reserved_here = stream is None
    stream = stream or reserve_report(path)
    try:
        stream.seek(0)
        stream.truncate()
        json.dump(metrics.report(), stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
    finally:
        if reserved_here:
            stream.close()
