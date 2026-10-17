"""Opt-in, out-of-band pipeline measurements."""

from __future__ import annotations

import contextvars
import json
import os
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


class Metrics:
    def __init__(self) -> None:
        self.started = time.perf_counter()
        self._lock = threading.Lock()
        self.timings: dict[str, float] = defaultdict(float)
        self.counters: dict[str, int] = defaultdict(int)
        self.high_water: dict[str, int] = defaultdict(int)

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

        return {
            "version": 1,
            "elapsed_seconds": round(time.perf_counter() - self.started, 6),
            "phases_seconds": {key: round(value, 6) for key, value in sorted(self.timings.items())},
            "counters": dict(sorted(self.counters.items())),
            "store_accounted_high_water_bytes": dict(sorted(self.high_water.items())),
            "peak_rss_bytes": {
                "self": peak_rss(resource.RUSAGE_SELF),
                "children": peak_rss(resource.RUSAGE_CHILDREN),
            },
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

    destination = Path(path).absolute()
    if any(part.is_symlink() for part in (destination, *destination.parents)):
        raise OSError("metrics report path cannot contain symlinks")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.resolve()
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
