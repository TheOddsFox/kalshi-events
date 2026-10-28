"""Opt-in, out-of-band pipeline measurements."""

from __future__ import annotations

import contextvars
import json
import os
import re
import resource
import sys
import tempfile
import threading
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
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
_TICKER = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._()\-/ ]{0,254}[A-Za-z0-9._()\-])?\Z")
_PROGRESS_INTERVAL_SECONDS = 15.0
_PROGRESS_PHASES = frozenset(
    {
        "preflight",
        "recovery_validation",
        "first_new_request",
        "index_replay",
        "closure",
        "complete",
        "stopped",
    }
)
_PROGRESS_INTEGER_COUNTERS = frozenset(
    {
        "journal_records",
        "total_pages",
        "indexed_pages",
        "backlog_pages",
        "reused_pages",
        "verified_pages",
    }
)
_PROGRESS_ELAPSED_COUNTERS = frozenset(
    {"first_data_request_elapsed_seconds", "first_data_page_elapsed_seconds"}
)


def endpoint_family(endpoint: str) -> str:
    """Return a fixed-cardinality family label without preserving request identifiers."""
    path = endpoint.split("?", 1)[0].split("#", 1)[0]
    if path in _ENDPOINT_FAMILIES:
        return path
    parts = path.split("/")
    if (
        len(parts) >= 3
        and parts[1] in {"series", "events", "markets"}
        and _TICKER.fullmatch("/".join(parts[2:]).replace("%2F", "/"))
    ):
        candidate = f"/{parts[1]}/{{ticker}}"
        if candidate in _ENDPOINT_FAMILIES:
            return candidate
    if (
        len(parts) >= 4
        and parts[1:3] == ["historical", "markets"]
        and _TICKER.fullmatch("/".join(parts[3:]).replace("%2F", "/"))
    ):
        return "/historical/markets/{ticker}"
    if (
        len(parts) >= 3
        and parts[1] == "multivariate_event_collections"
        and _TICKER.fullmatch("/".join(parts[2:]).replace("%2F", "/"))
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
    def __init__(self, progress_path: str | os.PathLike[str] | None = None) -> None:
        self.started = time.perf_counter()
        self._lock = threading.Lock()
        self.timings: dict[str, float] = defaultdict(float)
        self.counters: dict[str, int] = defaultdict(int)
        self.high_water: dict[str, int] = defaultdict(int)
        self.peak_submitted_futures = 0
        self.peak_http_concurrency = 0
        self.active_http_requests = 0
        self.endpoint_families: dict[str, dict[str, Any]] = {}
        self.rate_policy: dict[str, Any] | None = None
        self._progress_path = (
            _checked_progress_path(progress_path) if progress_path is not None else None
        )
        self._progress_started = time.monotonic() if self._progress_path is not None else 0.0
        self._progress_phase = "preflight"
        self._progress_counters: dict[str, int | float | None] = {
            **dict.fromkeys(_PROGRESS_INTEGER_COUNTERS, 0),
            **dict.fromkeys(_PROGRESS_ELAPSED_COUNTERS, None),
        }
        self._progress_last_write: float | None = None
        self._progress_written_phase: str | None = None
        self._progress_identity: tuple[int, int] | None = None
        self._progress_lock = threading.Lock()

    def add(self, name: str, value: int = 1) -> None:
        with self._lock:
            self.counters[name] += value

    def peak(self, name: str, value: int) -> None:
        with self._lock:
            self.high_water[name] = max(self.high_water[name], value)

    def progress(self, phase: str | None = None, **counters: int | float) -> None:
        """Write only fixed-phase, aggregate recovery progress when it changes or is due."""
        if self._progress_path is None:
            return
        if phase is not None and phase not in _PROGRESS_PHASES:
            raise ValueError("unknown recovery progress phase")
        for name, value in counters.items():
            if name in _PROGRESS_INTEGER_COUNTERS:
                if type(value) is not int or not 0 <= value <= 2**63 - 1:
                    raise ValueError(f"invalid recovery progress counter: {name}")
            elif name in _PROGRESS_ELAPSED_COUNTERS:
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not 0 <= value <= 2**31
                ):
                    raise ValueError(f"invalid recovery progress counter: {name}")
                counters[name] = round(float(value), 6)
            else:
                raise ValueError(f"unknown recovery progress counter: {name}")

        with self._progress_lock:
            if phase is not None:
                self._progress_phase = phase
            self._progress_counters.update(counters)
            now = time.monotonic()
            phase_changed = self._progress_phase != self._progress_written_phase
            if self._progress_path is None or (
                not phase_changed
                and self._progress_last_write is not None
                and now - self._progress_last_write < _PROGRESS_INTERVAL_SECONDS
            ):
                return
            self._write_progress(now)

    def _finish_progress(self, failed: bool) -> None:
        if self._progress_path is None:
            return
        with self._progress_lock:
            if failed:
                self._progress_phase = "stopped"
            elif self._progress_phase != "stopped":
                self._progress_phase = "complete"
            self._write_progress(time.monotonic())

    def _write_progress(self, now: float) -> None:
        from kalshi_events.common import safe_path

        path = safe_path(self._progress_path)
        if not path.parent.is_dir():
            raise OSError(f"metrics progress directory is missing: {path.parent}")
        identity = None
        if path.exists() or path.is_symlink():
            if path.is_symlink() or not path.is_file():
                raise OSError("metrics progress path is not a regular file")
            info = path.stat()
            identity = (info.st_dev, info.st_ino)
            if self._progress_identity is None or identity != self._progress_identity:
                raise FileExistsError(
                    f"metrics progress path is not owned by this collection: {path}"
                )

        report = {
            "version": 1,
            "phase": self._progress_phase,
            "elapsed_seconds": round(max(0.0, now - self._progress_started), 6),
            "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "counters": dict(self._progress_counters),
        }
        temporary: Path | None = None
        primary_error: BaseException | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".metrics-progress-", suffix=".tmp", dir=path.parent, delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write((json.dumps(report, sort_keys=True, allow_nan=False) + "\n").encode())
                stream.flush()
                os.fsync(stream.fileno())
            if self._progress_identity is None:
                os.link(temporary, path, follow_symlinks=False)
            else:
                if identity != self._progress_identity:
                    raise FileExistsError(
                        f"metrics progress path changed during collection: {path}"
                    )
                os.replace(temporary, path)
            info = path.stat()
            self._progress_identity = (info.st_dev, info.st_ino)
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._progress_last_write = now
            self._progress_written_phase = self._progress_phase
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            if temporary is not None:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    if primary_error is None:
                        raise

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
            "peak_submitted_futures": self.peak_submitted_futures,
            "peak_http_concurrency": self.peak_http_concurrency,
            "peak_rss_bytes": peak_rss_bytes,
            "rate_policy": dict(self.rate_policy) if self.rate_policy is not None else None,
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
def collect(progress_path: str | os.PathLike[str] | None = None) -> Iterator[Metrics]:
    """Enable measurements only inside this context; inactive hooks are a no-op."""
    metrics = Metrics(progress_path)
    token = _active.set(metrics)
    operation_failed = False
    try:
        if progress_path is not None:
            metrics._write_progress(time.monotonic())
        yield metrics
    except BaseException:
        operation_failed = True
        raise
    finally:
        try:
            metrics._finish_progress(operation_failed)
        except BaseException:
            if not operation_failed:
                raise
        finally:
            _active.reset(token)


def _checked_progress_path(path: str | os.PathLike[str]) -> Path:
    from kalshi_events.common import CatalogueError, safe_path

    try:
        destination = safe_path(Path(path))
    except CatalogueError as exc:
        raise OSError("metrics progress path cannot contain symlinks") from exc
    if destination.exists():
        raise FileExistsError(f"metrics progress path already exists: {destination}")
    return destination


def check_progress_path(path: str | os.PathLike[str]) -> Path:
    """Reject an unsafe or pre-existing progress companion before reserving its report."""
    return _checked_progress_path(path)


def progress(phase: str | None = None, **counters: int | float) -> None:
    """Update optional aggregate recovery progress for the active collection."""
    current = _active.get()
    if current is not None:
        current.progress(phase, **counters)


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


def rate_policy_update(
    policy: str,
    initial_rate: float,
    minimum_rate: float,
    final_rate: float,
    changes: int,
    generation: int,
) -> None:
    """Keep only the bounded invocation-level rate summary in the opt-in report."""
    current = _active.get()
    if current is None:
        return
    with current._lock:
        current.rate_policy = {
            "policy": policy,
            "initial_rate": initial_rate,
            "minimum_rate": minimum_rate,
            "final_rate": final_rate,
            "change_count": changes,
            "generation": generation,
        }


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


def submitted_futures(value: int) -> None:
    metrics = _active.get()
    if metrics is not None:
        with metrics._lock:
            metrics.peak_submitted_futures = max(metrics.peak_submitted_futures, value)


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
