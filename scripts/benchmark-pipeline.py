#!/usr/bin/env python3
"""Repeatable offline capture/build/publish benchmark; reports stay outside the repository."""

from __future__ import annotations

import argparse
import contextlib
import contextvars
import functools
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs

FIXED_TIME = "2026-01-01T00:00:00Z"
FIXED_ROOT_ID = "b" * 32
PAGE_SIZE = 200
HISTORY_STRESS_COUNT = 250_000
HISTORY_STRESS_IDENTITIES = 10_000
HISTORY_STRESS_PASSES = HISTORY_STRESS_COUNT // HISTORY_STRESS_IDENTITIES
SELECTED_PROFILES = {"selected-stress", "selected-single", "selected-fallback"}
TARGETED_PROFILES = {"targeted", *SELECTED_PROFILES}
COMPOSED_REFRESH_PROFILES = {"no-change", "sparse-change", "all-change"}
FIXTURE_VARIANTS = {"frozen", "combo-heavy"}
COMBO_HEAVY_FIXTURE_LABEL = "combo-heavy"
BENCHMARK_CHILD_TIMEOUT_SECONDS = 180
BENCHMARK_TERMINATION_RESERVE_SECONDS = 5
_request_id = contextvars.ContextVar("fixture_request_id")
_page_request = contextvars.ContextVar("benchmark_page_request", default=None)
_inside_page_read = contextvars.ContextVar("benchmark_inside_page_read", default=False)


class _UnreapedWorkerGroup(RuntimeError):
    """A supervised worker left descendants alive; its disposable root must be preserved."""


def _cleanup_on_sigterm(function):
    """Turn SIGTERM into a BaseException so active isolated worker groups are reaped."""

    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        if threading.current_thread() is not threading.main_thread():
            return function(*args, **kwargs)
        previous = signal.getsignal(signal.SIGTERM)

        def terminate(signum, _frame):
            signal.signal(signum, signal.SIG_IGN)
            raise SystemExit(128 + signum)

        signal.signal(signal.SIGTERM, terminate)
        try:
            return function(*args, **kwargs)
        finally:
            signal.signal(signal.SIGTERM, previous)

    return wrapped


def _worker_group_alive(process: subprocess.Popen[Any]) -> bool:
    if os.name != "posix":
        return process.poll() is None
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_worker_group(
    process: subprocess.Popen[Any], reserve_seconds: float = BENCHMARK_TERMINATION_RESERVE_SECONDS
) -> bool:
    """TERM then KILL a whole worker session, returning whether it is fully gone."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, 15)
        elif process.poll() is None:
            process.terminate()
    except ProcessLookupError:
        pass
    except PermissionError:
        return False
    first_wait = min(3.0, max(0.0, reserve_seconds - 2.0))
    deadline = time.monotonic() + first_wait
    while time.monotonic() < deadline:
        process.poll()  # Reap the direct worker before checking for surviving descendants.
        if not _worker_group_alive(process):
            break
        time.sleep(max(0.0, min(0.05, deadline - time.monotonic())))
    if _worker_group_alive(process):
        try:
            if os.name == "posix":
                os.killpg(process.pid, 9)
            elif process.poll() is None:
                process.kill()
        except ProcessLookupError:
            pass
        except PermissionError:
            return False
    remaining = max(0.0, reserve_seconds - first_wait)
    deadline = time.monotonic() + remaining
    while time.monotonic() < deadline:
        process.poll()
        if not _worker_group_alive(process):
            break
        time.sleep(max(0.0, min(0.05, deadline - time.monotonic())))
    if process.poll() is None:
        try:
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            return False
    return not _worker_group_alive(process)


@_cleanup_on_sigterm
def _run_supervised(
    command: list[str],
    *,
    timeout: float,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    text: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[Any]:
    """Run a worker and its descendants within a deadline and termination reserve."""
    stdout = subprocess.PIPE if capture_output else None
    stderr = subprocess.PIPE if capture_output else None
    process = None
    output = error_output = None
    termination_reserve = min(BENCHMARK_TERMINATION_RESERVE_SECONDS, timeout)
    active_seconds = max(0.0, timeout - termination_reserve)
    try:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=stdout,
            stderr=stderr,
            text=text,
            start_new_session=(os.name == "posix"),
        )
        output, error_output = process.communicate(timeout=active_seconds)
    except subprocess.TimeoutExpired as exc:
        assert process is not None
        if not _terminate_worker_group(process, termination_reserve):
            raise _UnreapedWorkerGroup(
                f"benchmark worker descendants remain after timeout: {command!r}"
            ) from exc
        raise subprocess.TimeoutExpired(
            command, timeout, output=output, stderr=error_output
        ) from exc
    except BaseException as exc:
        if (
            process is not None
            and _worker_group_alive(process)
            and not _terminate_worker_group(process, termination_reserve)
        ):
            raise _UnreapedWorkerGroup(
                f"benchmark worker descendants remain after interruption: {command!r}"
            ) from exc
        raise
    assert process is not None
    if _worker_group_alive(process):
        if not _terminate_worker_group(process, termination_reserve):
            raise _UnreapedWorkerGroup(
                f"benchmark worker exited but descendants remain: {command!r}"
            )
        raise RuntimeError(
            f"benchmark worker exited while descendants were still running: {command!r}"
        )
    completed = subprocess.CompletedProcess(command, process.returncode, output, error_output)
    if check and completed.returncode:
        raise subprocess.CalledProcessError(
            completed.returncode, command, output=output, stderr=error_output
        )
    return completed


def _baseline_metrics(source_root: Path) -> Any:
    """Load current recorder standalone so archived source gets comparable read/decode counts."""
    candidates = [
        source_root / "src/kalshi_events/metrics.py",
        Path.cwd() / "src/kalshi_events/metrics.py",
    ]
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        raise RuntimeError("benchmark needs an installed metrics.py from the candidate checkout")
    spec = importlib.util.spec_from_file_location("benchmark_metrics", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _instrument_baseline(metrics: Any, modules: list[Any]) -> None:
    """Benchmark-only observers let pre-instrumentation source use the same counters."""
    expanded = list(modules)
    for module in modules:
        for name in ("common", "transport", "capture", "warehouse", "publish"):
            target = getattr(module, name, None)
            if target is not None and target not in expanded:
                expanded.append(target)
    for module in expanded:
        for name in ("decode", "regular_bytes", "digest", "file_digest", "read_page"):
            original = getattr(module, name, None)
            if not callable(original) or getattr(original, "_bench_wrapped", False):
                continue

            def observe(*args, __name=name, __original=original, __module=module, **kwargs):
                if __name == "decode":
                    body = args[0]
                    metrics.add("json_decodes")
                    metrics.add("json_decoded_bytes", len(body))
                    with metrics.phase("json_parsing"):
                        return __original(*args, **kwargs)
                elif __name == "regular_bytes":
                    body = __original(*args, **kwargs)
                    metrics.add("file_reads")
                    metrics.add("file_read_bytes", len(body))
                    return body
                elif __name == "digest":
                    metrics.add("hashes")
                    metrics.add("hash_bytes", len(args[0]))
                    phase = (
                        "encoded_hashing"
                        if _inside_page_read.get() or hasattr(__module, "PublicClient")
                        else "hashing"
                    )
                    with metrics.phase(phase):
                        return __original(*args, **kwargs)
                elif __name == "file_digest":
                    metrics.add("hashes")
                    metrics.add("file_reads")
                    with metrics.phase("hashing"):
                        result = __original(*args, **kwargs)
                    metrics.add("hash_bytes", result[1])
                    path = Path(args[0])
                    if path.name == "body.bin" and path.parent.parent.name == "raw":
                        metrics.add("raw_body_reads")
                        metrics.add("raw_body_read_bytes", result[1])
                    return result
                if __name == "read_page":
                    token = _inside_page_read.set(True)
                    try:
                        result = __original(*args, **kwargs)
                    finally:
                        _inside_page_read.reset(token)
                else:
                    result = __original(*args, **kwargs)
                if __name == "read_page":
                    store, relative = args[:2]
                    body_path = store.path((Path(relative).parent / "body.bin").as_posix())
                    metrics.add("raw_body_reads")
                    metrics.add("raw_body_read_bytes", body_path.stat().st_size)
                    if result[0].get("status") == 200:
                        metrics.add("raw_body_json_decodes")
                return result

            observe._bench_wrapped = True
            setattr(module, name, observe)
    store_type = next(module.Store for module in expanded if hasattr(module, "Store"))
    original_sync_prefix = store_type._sync_prefix

    def sync_prefix(store, *args, **kwargs):
        metrics.add("metadata_scans")
        with metrics.phase("metadata_scan"):
            return original_sync_prefix(store, *args, **kwargs)

    store_type._sync_prefix = sync_prefix
    original_record = store_type._record

    def record(store, path, size):
        original_record(store, path, size)
        metrics.peak("retained", store._usage["retained_bytes"])
        metrics.peak("temporary", store._usage["temporary_bytes"])

    store_type._record = record
    store_type.write_bytes = _timed_write(store_type.write_bytes, metrics)
    original_write = store_type.write

    def write(store, relative, value):
        if relative == "current.json":
            with metrics.phase("promotion"):
                return original_write(store, relative, value)
        return original_write(store, relative, value)

    store_type.write = write
    original_atomic_write = next(
        module.atomic_bytes for module in expanded if hasattr(module, "atomic_bytes")
    )

    def atomic_write(*args, **kwargs):
        result = original_atomic_write(*args, **kwargs)
        metrics.add("fsync_calls", 2)
        return result

    for module in expanded:
        if hasattr(module, "atomic_bytes"):
            module.atomic_bytes = atomic_write
    original_append = store_type.append

    def append(store, relative, body):
        created = not store.path(relative).exists()
        result = original_append(store, relative, body)
        metrics.add("fsync_calls", 2 if created else 1)
        return result

    store_type.append = append
    for module, function, phase_name in (
        (next((m for m in expanded if hasattr(m, "_hydrate")), None), "_hydrate", "hydration"),
        (next((m for m in expanded if hasattr(m, "_land")), None), "_land", "landing"),
        (next((m for m in expanded if hasattr(m, "dbt")), None), "dbt", "dbt"),
        (
            next((m for m in expanded if hasattr(m, "verify_release")), None),
            "verify_release",
            "verification",
        ),
        (
            next((m for m in expanded if hasattr(m, "prune_superseded")), None),
            "prune_superseded",
            "pruning",
        ),
    ):
        if module is not None:
            setattr(
                module, function, _timed_function(getattr(module, function), metrics, phase_name)
            )
    transport = next((m for m in expanded if hasattr(m, "RateLimiter")), None)
    if transport is not None:
        transport.RateLimiter.acquire = _timed_function(
            transport.RateLimiter.acquire, metrics, "rate_wait"
        )


class _JsonLoadsObserver:
    """A warehouse-local json proxy that counts its direct first-party artifact parses."""

    def __init__(self, json_module: Any, metrics: Any):
        self._json_module = json_module
        self._metrics = metrics

    def loads(self, body: Any, *args: Any, **kwargs: Any) -> Any:
        size = len(body) if isinstance(body, (str, bytes, bytearray)) else 0
        self._metrics.add("json_decodes")
        self._metrics.add("json_decoded_bytes", size)
        self._metrics.add("first_party_artifact_json_decodes")
        self._metrics.add("first_party_artifact_json_decoded_bytes", size)
        with self._metrics.phase("json_parsing"):
            return self._json_module.loads(body, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._json_module, name)


def _instrument_warehouse_json_loads(warehouse: Any, metrics: Any) -> None:
    """Count warehouse's two direct dbt-artifact parses without touching global json.loads."""
    warehouse.json = _JsonLoadsObserver(warehouse.json, metrics)


def _instrument_dlt_trace(warehouse: Any, timings: dict[str, float]) -> None:
    """Collect dlt extract/normalize/load spans without changing pipeline arguments or results."""
    original_factory = warehouse.dlt.pipeline

    def pipeline_factory(*args: Any, **kwargs: Any) -> Any:
        pipeline = original_factory(*args, **kwargs)
        original_run = pipeline.run

        def run(*run_args: Any, **run_kwargs: Any) -> Any:
            try:
                return original_run(*run_args, **run_kwargs)
            finally:
                trace = getattr(pipeline, "last_trace", None)
                for span in getattr(trace, "steps", ()) or ():
                    step = getattr(span, "step", None)
                    if step not in {"extract", "normalize", "load"}:
                        continue
                    started = getattr(span, "started_at", None)
                    finished = getattr(span, "finished_at", None)
                    if started is None or finished is None:
                        continue
                    try:
                        elapsed = (finished - started).total_seconds()
                    except (AttributeError, TypeError):
                        continue
                    if elapsed >= 0:
                        timings[step] = timings.get(step, 0.0) + elapsed

        pipeline.run = run
        return pipeline

    warehouse.dlt.pipeline = pipeline_factory


def _instrument_current_raw_file_reads(metrics: Any, modules: list[Any]) -> None:
    """Classify hash-only raw body reads without changing native recorder behavior."""
    expanded = list(modules)
    for module in modules:
        for name in ("common", "warehouse", "publish"):
            target = getattr(module, name, None)
            if target is not None and target not in expanded:
                expanded.append(target)
    for module in expanded:
        original = getattr(module, "file_digest", None)
        if not callable(original):
            continue

        def observe(path, *args, __original=original, **kwargs):
            result = __original(path, *args, **kwargs)
            path = Path(path)
            if path.name == "body.bin" and path.parent.parent.name == "raw":
                metrics.add("raw_body_reads")
                metrics.add("raw_body_read_bytes", result[1])
            return result

        module.file_digest = observe
    transport = next((module for module in expanded if hasattr(module, "PublicClient")), None)
    if transport is not None:
        transport.PublicClient._pause = _timed_function(
            transport.PublicClient._pause, metrics, "retry_wait"
        )


def _timed_function(function: Any, metrics: Any, name: str) -> Any:
    def wrapped(*args, **kwargs):
        with metrics.phase(name):
            return function(*args, **kwargs)

    return wrapped


def _timed_write(function: Any, metrics: Any) -> Any:
    def wrapped(store, relative, body):
        if relative.startswith("raw/"):
            with metrics.phase("raw_write"):
                return function(store, relative, body)
        return function(store, relative, body)

    return wrapped


def _freeze_legacy_acquisition(
    profile: str, scenario: str, capture: Any, fixture_variant: str = "frozen"
) -> bool:
    """Keep pre-existing byte-identity profiles on request protocol 2."""
    if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL:
        return False
    frozen = profile in {
        "full",
        "targeted",
        "resume",
        "baseline-setup",
        "baseline-v2-setup",
        "no-change",
        "sparse-change",
        "all-change",
        "history-sparse",
    }
    if frozen:
        initialize = capture._initialize_v2

        def initialize_legacy(store, state):
            selected = state.get("selected")
            if selected is not None:
                tasks = []
                for work in state["tasks"]:
                    if work["endpoint"] == "/markets" and isinstance(
                        work["params"].get("tickers"), str
                    ):
                        tasks.extend(
                            capture.task(
                                f"/markets/{capture.ticker(value)}",
                                "markets",
                                singular=True,
                                requested=value,
                            )
                            for value in selected
                        )
                    else:
                        tasks.append(work)
                state["tasks"] = tasks
                state["task_scopes"] = {}
            # New protocol-5 constructors seed historical-list checkpoint fields.
            # Frozen protocol-2 comparisons must retain their original task shape.
            for work in state["tasks"]:
                for key in getattr(capture, "_CHECKPOINT_TASK_FIELDS", ()):
                    work.pop(key, None)
            state.pop("acquisition_protocol", None)
            state.pop("rate_policy", None)
            state.pop("parent_policy", None)
            state.pop("cutoff_policy", None)
            state.pop("acquisition_policy_sha256", None)
            state["compression"] = "identity"
            return initialize(store, state)

        capture._initialize_v2 = initialize_legacy
    return frozen


class FixtureClient:
    """Deterministic full and metadata-update snapshots for offline profiles."""

    def __init__(self, markets: int, scenario: str = "full", stage: str = "full") -> None:
        self.markets = markets
        self.scenario = scenario
        self.stage = stage

    def response(self, endpoint: str, params: dict[str, Any]) -> tuple[int, Any]:
        if endpoint == "/historical/cutoff":
            return 200, {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            }
        if endpoint == "/series":
            return 200, {"series": [{"ticker": "SERIES", "title": "Fixture contracts"}]}
        if endpoint == "/events":
            rows = [self._event()]
            if "min_updated_ts" in params:
                rows = rows if self.stage == "refresh" and self.scenario != "no-change" else []
            return 200, {"events": rows, "cursor": ""}
        if endpoint == "/events/multivariate":
            return 200, {
                "events": [{"event_ticker": "COMBOEVENT", "series_ticker": "SERIES"}],
                "cursor": "",
            }
        if endpoint == "/markets":
            if "tickers" in params:
                requested = params["tickers"].split(",")
                indexes = [
                    int(ticker.removeprefix("MKT-"))
                    for ticker in requested
                    if ticker.startswith("MKT-")
                    and ticker.removeprefix("MKT-").isdigit()
                    and int(ticker.removeprefix("MKT-")) < self.markets
                ]
                start = int(params.get("cursor", "0"))
                page = indexes[start : start + PAGE_SIZE]
                stop = start + len(page)
                return 200, {
                    "markets": [self._market(index) for index in page],
                    "cursor": str(stop) if stop < len(indexes) else "",
                }
            if (
                self.scenario == "history-sparse"
                and self.stage == "baseline"
                and "mve_filter" not in params
                and "min_updated_ts" not in params
            ):
                start = int(params.get("cursor", "0"))
                stop = min(start + PAGE_SIZE, HISTORY_STRESS_COUNT)
                rows = [self._history_stress_market(index) for index in range(start, stop)]
                return 200, {
                    "markets": rows,
                    "cursor": str(stop) if stop < HISTORY_STRESS_COUNT else "",
                }
            if params.get("mve_filter") == "only":
                indexes = [i for i in range(self.markets) if self._is_combo(i)]
            elif params.get("mve_filter") == "exclude":
                indexes = [i for i in range(self.markets) if not self._is_combo(i)]
                if "min_updated_ts" in params:
                    if self.stage != "refresh" or self.scenario == "no-change":
                        indexes = []
                    elif self.scenario == "sparse-change":
                        indexes = [i for i in indexes if i == 0]
                    elif self.scenario == "history-sparse":
                        indexes = [i for i in indexes if i % 100 == 0]
            else:
                indexes = range(self.markets)
            start = int(params.get("cursor", "0"))
            page = indexes[start : start + PAGE_SIZE]
            stop = start + len(page)
            rows = [self._market(index) for index in page]
            return 200, {
                "markets": rows,
                "cursor": str(stop) if stop < len(indexes) else "",
            }
        if endpoint == "/historical/markets":
            rows = []
            if self.scenario == "history-sparse":
                rows = [self._historical_market(1)]
            elif self.scenario != "full":
                rows = [
                    {
                        "ticker": "HIST-000001",
                        "event_ticker": "EVENT",
                        "status": "finalized",
                        "result": "yes",
                        "settlement_ts": "2025-12-01T00:00:00Z",
                        "settlement_value_dollars": "1.000000000000000001",
                        "last_price_dollars": "1.000",
                        "volume_fp": "12.3456789",
                        "open_interest_fp": "2.125",
                    }
                ]
            return 200, {"markets": rows, "cursor": ""}
        if endpoint == "/events/EVENT":
            return 200, {"event": self._event()}
        if endpoint == "/events/COMBOEVENT":
            return 200, {"event": {"event_ticker": "COMBOEVENT", "series_ticker": "SERIES"}}
        if endpoint == "/series/SERIES":
            return 200, {"series": {"ticker": "SERIES", "title": "Fixture contracts"}}
        if endpoint.startswith("/markets/"):
            ticker = endpoint.removeprefix("/markets/")
            try:
                index = int(ticker.removeprefix("MKT-"))
            except ValueError:
                return 404, {}
            if index < self.markets:
                return 200, {"market": self._market(index)}
        return 404, {}

    def _event(self) -> dict[str, Any]:
        event = {"event_ticker": "EVENT", "series_ticker": "SERIES"}
        if self.scenario != "full":
            event["title"] = (
                "Sparse update"
                if self.stage == "refresh" and self.scenario != "no-change"
                else "Fixture event"
            )
        return event

    @staticmethod
    def _is_combo(index: int) -> bool:
        return index > 1 and index % 1000 == 1

    @staticmethod
    def _historical_market(index: int) -> dict[str, Any]:
        return {
            "ticker": f"HIST-{index:06d}",
            "event_ticker": "EVENT",
            "status": "finalized",
            "result": "yes" if index % 2 == 0 else "no",
            "settlement_ts": (
                "2025-12-01T00:00:00Z" if index % 2 == 0 else "2025-12-01T01:00:00+01:00"
            ),
            "settlement_value_dollars": "1.000000000000000001" if index % 2 == 0 else "0.000",
        }

    def _history_stress_market(self, index: int) -> dict[str, Any]:
        """Repeat 10k ordinary native identities to stress retained observation history."""
        return self._market(index % min(self.markets, HISTORY_STRESS_IDENTITIES))

    def _market(self, index: int) -> dict[str, Any]:
        ticker = f"MKT-{index:06d}"
        combo = self._is_combo(index)
        changed = (
            self.stage == "refresh"
            and self.scenario != "no-change"
            and (
                self.scenario == "all-change"
                or index == 0
                or (self.scenario == "history-sparse" and index % 100 == 0)
            )
        )
        row = {
            "ticker": ticker,
            "event_ticker": "COMBOEVENT" if combo else "EVENT",
            "status": "active",
            "rules_primary": "Resolve when the stated event condition is met.",
            "rules_secondary": "Fixture settlement details.",
            "last_price_dollars": f"{index % 1000 / 1000:.3f}",
            "volume_fp": "12.3456789",
            "open_interest_fp": "2.125",
            "floor_strike": "3.123456789123456789",
            "price_ranges": [{"start": "0", "end": "1", "step": "0.0001"}],
            "mve_selected_legs": (
                [{"event_ticker": "EVENT", "market_ticker": "MKT-000000", "side": "yes"}]
                if combo
                else []
            ),
        }
        if self.scenario != "full":
            row["title"] = "Updated contract" if changed else f"Fixture contract {index}"
        return row


class ComboHeavyFixtureClient(FixtureClient):
    """Separate combo-heavy payload seam; the frozen production fixtures stay untouched."""

    LEG_CARDINALITIES = (1, 2, 4, 8, 32)

    def __init__(self, markets: int, scenario: str = "full", stage: str = "full") -> None:
        super().__init__(markets, scenario, stage)

    def response(self, endpoint: str, params: dict[str, Any]) -> tuple[int, Any]:
        if endpoint == "/historical/markets":
            start = int(params.get("cursor", "0"))
            indexes = range(self.markets)
            selected = indexes[start : start + PAGE_SIZE]
            stop = start + len(selected)
            rows = []
            for index in selected:
                row = self._market(index)
                row.update(
                    ticker=f"HIST-{index:06d}",
                    status="finalized",
                    result="yes" if index % 2 == 0 else "no",
                    settlement_ts="2025-12-01T00:00:00Z",
                    settlement_value_dollars=(
                        "1.000000000000000001" if index % 2 == 0 else "0.000000000000000001"
                    ),
                )
                rows.append(row)
            return 200, {
                "markets": rows,
                "cursor": str(stop) if stop < len(indexes) else "",
            }
        return super().response(endpoint, params)

    @staticmethod
    def _is_combo(index: int) -> bool:
        # 95% combo density approximates the retained 91.2%/97.5% live/history tiers.
        return index % 20 != 0

    def _market(self, index: int) -> dict[str, Any]:
        row = super()._market(index)
        if not self._is_combo(index):
            return row
        if self.stage == "refresh" and self.scenario == "sparse-change" and index == 1:
            row["title"] = "Updated combo contract"
        ordinal = index - index // 20
        cardinality = 200 if ordinal % 200 == 0 else self.LEG_CARDINALITIES[ordinal % 5]
        row["mve_selected_legs"] = [
            {
                "event_ticker": (
                    f"MISSING-EVENT-{ordinal:06d}"
                    if leg_index == 0 and ordinal % 17 == 0
                    else "EVENT"
                ),
                "market_ticker": (
                    f"MISSING-MARKET-{ordinal:06d}"
                    if leg_index == 0 and ordinal % 19 == 0
                    else f"MKT-{leg_index:06d}"
                ),
                "side": "yes" if (ordinal + leg_index) % 2 == 0 else "no",
            }
            for leg_index in range(cardinality)
        ]
        row["rules_primary"] += " " + ("Combo settlement detail. " * 64)
        row["floor_strike"] = f"{ordinal}.000000000000000001"
        return row


def parent_hydration_fixture(count: int) -> list[str]:
    """Deterministic distinct parent identities for scheduler-scale fixtures."""
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("parent workload count must be a positive integer")
    return [f"EVENT-{index:06d}" for index in range(count)]


def selected_stress_tickers() -> list[str]:
    """A stable selection set with shared parents and lookup edge cases."""
    return [*(f"S{i:03d}" for i in range(96)), "HIST", "MISS", "ORPHAN", "MKT-" + "X" * 252]


def _selected_profile_tickers(profile: str) -> list[str]:
    if profile == "selected-single":
        return ["S000"]
    if profile == "selected-fallback":
        return ["HIST", "MISS"]
    return selected_stress_tickers()


def _benchmark_page_uuid(
    profile: str,
    endpoint: str,
    params: dict[str, Any] | None,
    capture_id: str,
    context_request_id: str,
) -> str:
    """Stabilize new selected fixture receipts without changing the journal request identity."""
    if profile in SELECTED_PROFILES and endpoint == "/historical/cutoff":
        return context_request_id
    if profile not in (SELECTED_PROFILES | COMPOSED_REFRESH_PROFILES):
        return context_request_id
    if profile in COMPOSED_REFRESH_PROFILES:
        # These legacy composed profiles can issue the same endpoint/params more
        # than once in a capture. Preserve request-level uniqueness while using
        # a transport-valid UUID width; the journal keeps the full context ID.
        return context_request_id[:32]
    identity = json.dumps(
        [capture_id, endpoint, params or {}], sort_keys=True, separators=(",", ":")
    ).encode()
    digest_value = hashlib.sha256(identity).hexdigest()
    return digest_value


def _receipt_uuid_format(profile: str) -> str:
    if profile in SELECTED_PROFILES:
        return "sha256-canonical-request-64-hex"
    if profile in COMPOSED_REFRESH_PROFILES:
        return "context-request-id-32-hex"
    return "context-request-id-64-hex"


def _selected_stress_fixture_sha256(tickers: list[str] | None = None) -> str:
    fixture = SelectedStressFixtureClient()
    tickers = selected_stress_tickers() if tickers is None else tickers
    digest = hashlib.sha256()
    for ticker in tickers:
        for endpoint in (f"/markets/{ticker}", f"/historical/markets/{ticker}"):
            status, payload = fixture.response(endpoint, {})
            digest.update(
                json.dumps(
                    [endpoint, status, payload], sort_keys=True, separators=(",", ":")
                ).encode()
            )
    for endpoint in (
        "/historical/cutoff",
        "/events/EVENT",
        "/events/HIST-EVENT",
        "/events/ORPHAN-EVENT",
        "/series/SERIES",
    ):
        status, payload = fixture.response(endpoint, {})
        digest.update(
            json.dumps([endpoint, status, payload], sort_keys=True, separators=(",", ":")).encode()
        )
    for endpoint, params in (
        ("/markets", {"tickers": ",".join(tickers), "limit": "1000"}),
        ("/events/EVENT", {"exclude_markets": "true"}),
    ):
        status, payload = fixture.response(endpoint, params)
        digest.update(
            json.dumps(
                [endpoint, params, status, payload], sort_keys=True, separators=(",", ":")
            ).encode()
        )
    return digest.hexdigest()


class SelectedStressFixtureClient:
    """Targeted acquisition fixture; deliberately excludes unrelated child rows from output."""

    def __init__(self) -> None:
        self.tickers = selected_stress_tickers()
        self.children = [{"ticker": f"NOISE-{i:05d}", "event_ticker": "EVENT"} for i in range(2000)]
        self.fallback_request_count = 0

    @staticmethod
    def _market(ticker: str, event: str = "EVENT") -> dict[str, Any]:
        return {
            "ticker": ticker,
            "event_ticker": event,
            "status": "active",
            "title": f"Fixture {ticker}",
            "created_time": "2025-01-01T00:00:00Z",
            "updated_time": "2025-01-02T01:00:00+01:00",
            "close_time": None,
            "expiration_time": "2026-01-01T00:00:00.000Z",
            "last_price_dollars": "0.125",
        }

    def response(self, endpoint: str, params: dict[str, Any]) -> tuple[int, Any]:
        if endpoint in {
            "/markets/HIST",
            "/historical/markets/HIST",
            "/markets/MISS",
            "/historical/markets/MISS",
        }:
            self.fallback_request_count += 1
        if endpoint == "/historical/cutoff":
            return 200, {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            }
        if endpoint == "/markets/HIST":
            return 404, {}
        if endpoint == "/historical/markets/HIST":
            return 200, {
                "market": {
                    **self._market("HIST", "HIST-EVENT"),
                    "status": "finalized",
                    "result": "yes",
                    "settlement_ts": "2025-12-01T00:00:00-05:00",
                    "settlement_value_dollars": "1.000000000000000001",
                }
            }
        if endpoint in {"/markets/MISS", "/historical/markets/MISS"}:
            return 404, {}
        if endpoint == "/events/EVENT":
            event = {
                "event_ticker": "EVENT",
                "series_ticker": "SERIES",
                "markets": self.children,
            }
            if params.get("exclude_markets") == "true":
                event.pop("markets")
            return 200, {"event": event}
        if endpoint == "/events/HIST-EVENT":
            return 200, {"event": {"event_ticker": "HIST-EVENT", "series_ticker": "SERIES"}}
        if endpoint == "/events/ORPHAN-EVENT":
            return 404, {}
        if endpoint == "/series/SERIES":
            return 200, {"series": {"ticker": "SERIES", "title": "Fixture contracts"}}
        if endpoint == "/markets" and "tickers" in params:
            requested = params["tickers"].split(",")
            selected = [
                ticker
                for ticker in requested
                if ticker in self.tickers and ticker not in {"HIST", "MISS"}
            ]
            start = int(params.get("cursor", "0"))
            page = selected[start : start + PAGE_SIZE]
            stop = start + len(page)
            return 200, {
                "markets": [
                    self._market(ticker, "ORPHAN-EVENT" if ticker == "ORPHAN" else "EVENT")
                    for ticker in page
                ],
                "cursor": str(stop) if stop < len(selected) else "",
            }
        if endpoint.startswith("/markets/"):
            ticker = endpoint.removeprefix("/markets/")
            if ticker == "ORPHAN":
                return 200, {"market": self._market(ticker, "ORPHAN-EVENT")}
            if ticker in self.tickers and ticker not in {"HIST", "MISS"}:
                return 200, {"market": self._market(ticker)}
        return 404, {}


_SOURCE_PROVENANCE_FIELDS = {
    "capture_id",
    "page_id",
    "received_at",
    "source_endpoint",
    "payload_sha256",
    "source_kind",
    "observation_id",
}


def _selected_relation_semantics(output: Path) -> dict[str, Any]:
    """Hash native fields, excluding only capture provenance and nested event children."""
    result = {}
    for relation in ("markets", "events", "series"):
        rows = json.loads((output / f"{relation}.json").read_text(encoding="utf-8"))
        native = []
        for row in rows:
            fields = {
                key: value for key, value in row.items() if key not in _SOURCE_PROVENANCE_FIELDS
            }
            if relation == "events":
                fields.pop("markets", None)
                payload = fields.get("payload_json")
                if isinstance(payload, str):
                    decoded = json.loads(payload)
                    if isinstance(decoded, dict):
                        decoded.pop("markets", None)
                        fields["payload_json"] = json.dumps(
                            decoded, sort_keys=True, separators=(",", ":")
                        )
            native.append(fields)
        canonical = sorted(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in native)
        result[relation] = {
            "rows": len(native),
            "sha256": hashlib.sha256("\n".join(canonical).encode()).hexdigest(),
        }
    return result


def _selected_provenance_is_valid(
    store: Any, state: dict[str, Any], output: Path, selected: list[str]
) -> bool:
    """Verify exported row lineage and request scope against hash-verified raw receipts."""
    from kalshi_events import capture, normalize

    pages = list(capture.capture_pages(store, state))
    raw_observations = list(normalize.observations(pages))
    raw_by_identity: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    requested: set[str] = set()
    for manifest, _payload in pages:
        endpoint = manifest.get("endpoint", "")
        params = manifest.get("params", {})
        if endpoint == "/markets" and isinstance(params.get("tickers"), str):
            requested.update(params["tickers"].split(","))
        elif endpoint.startswith("/markets/"):
            requested.add(endpoint.removeprefix("/markets/"))
        for obs in raw_observations:
            if obs.get("page_id") != manifest.get("page_id"):
                continue
            identity = obs.get("market_ticker", obs.get("event_ticker", obs.get("series_ticker")))
            if identity is not None:
                key = (obs["entity_kind"], identity, obs["payload_json"])
                raw_by_identity.setdefault(key, []).append(obs)
        # read_page, reached through capture_pages, checks the raw JSON pointer's source hash.
        if not isinstance(manifest.get("sha256"), str) or len(manifest["sha256"]) != 64:
            return False
    if state.get("selected") != selected or requested != set(selected):
        return False
    for relation, kind, identity_key in (
        ("markets", "markets", "market_ticker"),
        ("events", "events", "event_ticker"),
        ("series", "series", "series_ticker"),
    ):
        rows = json.loads((output / f"{relation}.json").read_text(encoding="utf-8"))
        for row in rows:
            key = (kind, row.get(identity_key), row.get("payload_json"))
            candidates = raw_by_identity.get(key, [])
            if not any(
                all(row.get(field) == source.get(field) for field in _SOURCE_PROVENANCE_FIELDS)
                and row.get("observation_id") == source.get("observation_id")
                for source in candidates
            ):
                return False
    coverage = _selected_coverage(output)
    if [row.get("market_ticker") for row in coverage] != selected:
        return False
    market_rows = json.loads((output / "markets.json").read_text(encoding="utf-8"))
    return {row.get("market_ticker") for row in market_rows} == {
        row.get("market_ticker") for row in coverage if row.get("status") == "found"
    }


def _selected_coverage(output: Path) -> list[dict[str, Any]]:
    return json.loads((output / "coverage.json").read_text(encoding="utf-8"))


def _direct_v2_setup_build(warehouse: Any, store: Any, capture_id: str) -> Any:
    """Create a v2 baseline directly; this profile measures v2 setup only."""
    return warehouse.build_v2_full(store, capture_id)


def _selected_comparison(reference: dict[str, Any], candidate: dict[str, Any]) -> dict[str, bool]:
    def valid_semantics(value: Any) -> bool:
        if not isinstance(value, dict) or set(value) != {"markets", "events", "series"}:
            return False
        return all(
            isinstance(relation, dict)
            and isinstance(relation.get("rows"), int)
            and isinstance(relation.get("sha256"), str)
            and len(relation["sha256"]) == 64
            and all(char in "0123456789abcdef" for char in relation["sha256"])
            for relation in value.values()
        )

    def valid_coverage(value: Any, tickers: Any) -> bool:
        return (
            isinstance(value, list)
            and isinstance(tickers, list)
            and len(value) == len(tickers)
            and all(
                isinstance(row, dict)
                and row.get("market_ticker") == ticker
                and row.get("status") in {"found", "absent", "unresolved"}
                for row, ticker in zip(value, tickers, strict=True)
            )
        )

    reference_tickers = reference.get("selected_tickers")
    candidate_tickers = candidate.get("selected_tickers")
    reference_semantics = reference.get("selected_semantics")
    candidate_semantics = candidate.get("selected_semantics")
    reference_coverage = reference.get("selected_coverage")
    candidate_coverage = candidate.get("selected_coverage")
    shapes_valid = (
        valid_semantics(reference_semantics)
        and valid_semantics(candidate_semantics)
        and valid_coverage(reference_coverage, reference_tickers)
        and valid_coverage(candidate_coverage, candidate_tickers)
    )
    return {
        "selection_match": (
            isinstance(reference_tickers, list)
            and isinstance(candidate_tickers, list)
            and bool(reference_tickers)
            and reference_tickers == candidate_tickers
        ),
        "semantics_match": shapes_valid and reference_semantics == candidate_semantics,
        "coverage_match": shapes_valid and reference_coverage == candidate_coverage,
        "provenance_match": reference.get("selected_provenance_valid") is True
        and candidate.get("selected_provenance_valid") is True,
    }


def _worker(
    source_root: Path,
    root: Path,
    markets: int,
    result_path: Path,
    profile: str = "full",
    scenario: str = "full",
    refresh_mode: str = "full",
    baseline_capture_id: str | None = None,
    fixture_variant: str = "frozen",
    capture_only: bool = False,
) -> None:
    os.environ["DLT_TELEMETRY"] = "0"
    os.environ["DLT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    sys.path.insert(0, str(source_root / "src"))
    from kalshi_events import capture, common, metadata, normalize, publish, transport, warehouse
    from kalshi_events.common import Limits, Store

    frozen_acquisition = _freeze_legacy_acquisition(profile, scenario, capture, fixture_variant)

    legacy_metrics = False
    try:
        from kalshi_events import metrics
    except ImportError:
        metrics = _baseline_metrics(source_root)
        legacy_metrics = True
        _instrument_baseline(
            metrics, [common, capture, metadata, normalize, transport, warehouse, publish]
        )
    if not legacy_metrics:
        _instrument_current_raw_file_reads(
            metrics, [common, capture, metadata, normalize, transport, warehouse, publish]
        )
    _instrument_warehouse_json_loads(warehouse, metrics)
    dlt_phase_seconds: dict[str, float] = {}
    _instrument_dlt_trace(warehouse, dlt_phase_seconds)
    capture.capture = _timed_function(capture.capture, metrics, "capture")
    if capture_only and (profile != "full" or fixture_variant != COMBO_HEAVY_FIXTURE_LABEL):
        raise ValueError("capture-only mode requires the full combo-heavy fixture")

    # Each independent process gets deterministic, noncolliding capture identities.
    seed = (
        1 if profile in {"full", "targeted", "resume", "baseline-setup", "baseline-v2-setup"} else 2
    )
    sequence = {"value": seed - 1}

    def next_capture_id():
        sequence["value"] += 1
        return SimpleNamespace(hex=f"{sequence['value']:032x}")

    capture.uuid = SimpleNamespace(uuid4=next_capture_id)
    os.environ["KALSHI_EVENTS_ALLOW_LOOPBACK"] = "1"
    os.environ["KALSHI_EVENTS_API_URL"] = "http://127.0.0.1/trade-api/v2"
    import httpx

    receipt_profile = scenario if profile == "baseline-setup" else profile

    def page_uuid():
        request = _page_request.get()
        if request is None:
            return SimpleNamespace(hex=_request_id.get())
        endpoint, params, capture_id = request
        return SimpleNamespace(
            hex=_benchmark_page_uuid(
                receipt_profile, endpoint, params, capture_id, _request_id.get()
            )
        )

    transport.uuid = SimpleNamespace(uuid4=page_uuid)
    capture_time = (
        "2026-01-01T00:00:00Z"
        if profile in {"baseline-setup", "baseline-v2-setup"}
        else "2026-01-02T00:00:00Z"
        if profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
        else FIXED_TIME
    )
    capture.now = lambda: capture_time
    transport.now = lambda: capture_time
    common.now = lambda: capture_time
    warehouse.now = lambda: capture_time
    publish.now = lambda: capture_time
    fixture_stage = "baseline" if profile in {"baseline-setup", "baseline-v2-setup"} else "refresh"
    fixture_scenario = (
        scenario
        if profile
        in {
            "baseline-setup",
            "baseline-v2-setup",
            "no-change",
            "sparse-change",
            "all-change",
            "history-sparse",
        }
        else "full"
    )
    fixture = (
        SelectedStressFixtureClient()
        if profile in SELECTED_PROFILES
        else ComboHeavyFixtureClient(markets, fixture_scenario, fixture_stage)
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL
        else FixtureClient(markets, fixture_scenario, fixture_stage)
    )

    def handler(request):
        params = {key: values[-1] for key, values in parse_qs(request.url.query.decode()).items()}
        status, payload = fixture.response(request.url.path.removeprefix("/trade-api/v2"), params)
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        if legacy_metrics:
            metrics.add("requests")
            metrics.add("encoded_bytes", len(body))
            metrics.add("decoded_bytes", len(body))
        return httpx.Response(
            status,
            headers={"content-type": "application/json"},
            stream=httpx.ByteStream(body),
        )

    def client_factory(root_store, budget, **options):
        client = transport.PublicClient(root_store, budget, **options)
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(handler))
        original_get = client.get

        def get(endpoint, params=None, context=None):
            token = _request_id.set(context["request_id"])
            page_token = _page_request.set((endpoint, dict(params or {}), context["capture_id"]))
            recorder_token = metrics._active.set(measured) if legacy_metrics else None
            try:
                return original_get(endpoint, params, context)
            finally:
                if legacy_metrics:
                    metrics._active.reset(recorder_token)
                _page_request.reset(page_token)
                _request_id.reset(token)

        client.get = get
        return client

    download_limit = (
        4 * 1024**3
        if profile
        in {
            "baseline-setup",
            "baseline-v2-setup",
            "no-change",
            "sparse-change",
            "all-change",
            "history-sparse",
            *SELECTED_PROFILES,
        }
        else 2 * 1024**3
    )
    limits = Limits(max_rate=1000, max_requests=10000, max_download_bytes=download_limit)
    profile_metrics = metrics.collect() if metrics else contextlib.nullcontext(None)
    with profile_metrics as measured:
        if profile == "baseline-setup":
            store = Store(root, limits)
            if refresh_mode == "incremental":
                _seed_incremental_root(store)
            state = capture.capture(store, client_factory=client_factory)
            _require_complete(state, profile)
            warehouse.build(store, state["capture_id"])
            release = publish.publish(Store(root, limits), state["capture_id"])
            if refresh_mode == "incremental":
                release = publish.publish(
                    Store(root, limits), state["capture_id"], catalogue_version=2
                )
            output = Path(release["output"])
        elif profile == "baseline-v2-setup":
            store = Store(root, limits)
            _seed_incremental_root(store)
            state = capture.capture(store, client_factory=client_factory)
            _require_complete(state, profile)
            _direct_v2_setup_build(warehouse, store, state["capture_id"])
            release = publish.publish(Store(root, limits), state["capture_id"], catalogue_version=2)
            output = Path(release["output"])
        elif profile in {"no-change", "sparse-change", "all-change", "history-sparse"}:
            store = Store(root, limits)
            if refresh_mode == "incremental":
                state = capture.capture(store, client_factory=client_factory, incremental=True)
            else:
                state = capture.capture(store, client_factory=client_factory)
            _require_complete(state, profile)
            warehouse.build(store, state["capture_id"])
            if refresh_mode == "incremental":
                release = publish.publish(
                    Store(root, limits),
                    state["capture_id"],
                    catalogue_version=2,
                )
            else:
                release = publish.publish(Store(root, limits), state["capture_id"])
            output = Path(release["output"])
        elif profile == "resume":
            initial_limits = Limits(max_rate=1000, max_requests=4, max_download_bytes=2 * 1024**3)
            partial = capture.capture(Store(root, initial_limits), client_factory=client_factory)
            state = capture.capture(
                Store(root, limits), resume=partial["capture_id"], client_factory=client_factory
            )
            _require_complete(state, profile)
            warehouse.build(Store(root, limits), state["capture_id"])
            release = publish.publish(Store(root, limits), state["capture_id"])
            output = Path(release["output"])
        else:
            selected = (
                _selected_profile_tickers(profile)
                if profile in SELECTED_PROFILES
                else (["MKT-000000"] if profile == "targeted" else None)
            )
            store = Store(root, limits)
            if capture_only:
                _seed_incremental_root(store)
            state = capture.capture(store, client_factory=client_factory, selected=selected)
            if profile in TARGETED_PROFILES:
                from kalshi_events import metadata

                output = result_path.with_suffix(".metadata")
                release = metadata.export(
                    Store(root, limits),
                    selected,
                    output,
                    capture_id=state["capture_id"],
                )
            else:
                _require_complete(state, profile)
                if capture_only:
                    release = {"source_release_id": None, "output": None}
                    output = None
                else:
                    warehouse.build(Store(root, limits), state["capture_id"])
                    release = publish.publish(Store(root, limits), state["capture_id"])
                    output = Path(release["output"])

        result = measured.report() if measured else {"version": 0}
        result["dlt_phase_seconds"] = dlt_phase_seconds
        result["pipeline_elapsed_seconds"] = result.pop("elapsed_seconds", None)
        result["worker_inclusive_seconds"] = result["pipeline_elapsed_seconds"]
        result["timing_semantics"] = (
            "Phase timings are inclusive and may be nested; do not sum them. "
            "pipeline_elapsed_seconds comes from the worker's native collector and covers the profile operations. "
            "worker_wall_seconds includes subprocess startup, standalone verification, output checksums, and post-run diagnostics. "
            "Refresh pipeline time excludes separately prepared baseline setup."
        )
        counters = result.setdefault("counters", {})
        counters["metadata_json_decodes"] = max(
            0, counters.get("json_decodes", 0) - counters.get("raw_body_json_decodes", 0)
        )
        result.update(
            capture_id=state["capture_id"],
            baseline_capture_id=baseline_capture_id,
            observations=(
                len(_selected_profile_tickers(profile))
                if profile in SELECTED_PROFILES
                else 3
                if profile == "targeted"
                else _fixture_observation_count(markets, profile, fixture_variant)
            ),
            profile=profile,
            fixture_variant=fixture_variant,
            receipt_uuid_format=_receipt_uuid_format(receipt_profile),
            acquisition_protocol="legacy-frozen-v2" if frozen_acquisition else "current",
            refresh_mode=refresh_mode
            if profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
            else None,
            release_id=release.get("source_release_id"),
        )
        if legacy_metrics:
            result["unsupported_phases"] = ["request", "decompression", "export"]
            result["unsupported_phase_notes"] = {
                "request": "legacy observer does not time HTTP calls independently of waits and writes",
                "decompression": "offline fixtures use identity encoding; no decompression is timed",
                "export": "legacy observer does not isolate publication export timing",
            }

    files = (
        {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.iterdir())
            if path.is_file()
        }
        if profile in TARGETED_PROFILES
        else {}
        if output is None
        else {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.glob("*.parquet"))
        }
    )
    result["artifact_sha256"] = files
    result["consumer_output_bytes"] = (
        sum(path.stat().st_size for path in output.iterdir() if path.is_file())
        if profile in TARGETED_PROFILES
        else None
    )
    if profile in TARGETED_PROFILES:
        metadata_manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        metadata_inventory = metadata_manifest["files"]
        assert set(metadata_inventory) == {
            "coverage.json",
            "events.json",
            "markets.json",
            "series.json",
        }
        result["metadata_source_release_id"] = metadata_manifest["source_release_id"]
        result["metadata_inventory"] = metadata_inventory
        if profile in SELECTED_PROFILES:
            metadata.verify(output)
            coverage = json.loads((output / "coverage.json").read_text(encoding="utf-8"))
            selected_tickers = _selected_profile_tickers(profile)
            expected = {
                "selected-stress": (100, 98, {"found", "absent", "unresolved"}),
                "selected-single": (1, 1, {"found"}),
                "selected-fallback": (2, 1, {"found", "absent"}),
            }[profile]
            coverage_count, found_count, statuses = expected
            assert len(coverage) == coverage_count
            assert {row["status"] for row in coverage} == statuses
            assert sum(row["status"] == "found" for row in coverage) == found_count
            assert (
                len(json.loads((output / "markets.json").read_text(encoding="utf-8")))
                == found_count
            )
            result["selected_semantics"] = _selected_relation_semantics(output)
            result["selected_coverage"] = _selected_coverage(output)
            result["selected_provenance_valid"] = _selected_provenance_is_valid(
                Store(root, limits), state, output, selected_tickers
            )
            assert result["selected_provenance_valid"], "selected metadata provenance is invalid"
            result["selected_tickers"] = selected_tickers
            result["selected_fallback_request_count"] = fixture.fallback_request_count
    if profile == "baseline-v2-setup":
        result["lineage"] = _assert_lineage_manifest(
            output, None, "full", root, state["capture_id"]
        )
        result["lineage_valid"] = True
    manifest_path = output / "manifest.json" if output is not None else None
    result["release_manifest_sha256"] = (
        hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        if manifest_path is not None and manifest_path.is_file()
        else None
    )
    pointer_name = _publication_pointer_name(profile, refresh_mode)
    result["publication_pointer_sha256"] = (
        _publication_pointer_sha256(
            root,
            pointer_name,
            result.get("release_id"),
            result.get("release_manifest_sha256"),
        )
        if output is not None and profile not in TARGETED_PROFILES
        else None
    )
    result["fixture_timestamps_frozen"] = {
        "capture": capture_time,
        "transport": capture_time,
        "common": capture_time,
        "warehouse": capture_time,
        "publish": capture_time,
    }
    composed_profile = profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
    if (
        composed_profile
        or (fixture_variant == COMBO_HEAVY_FIXTURE_LABEL and profile == "full")
        and not capture_only
    ):
        catalogue_version = (
            2
            if profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
            and refresh_mode == "incremental"
            else 1
        )
        result["semantic_relations"] = _semantic_relation_hashes(
            root, state["capture_id"], catalogue_version=catalogue_version
        )
        if composed_profile and refresh_mode == "incremental":
            result["lineage"] = _assert_lineage_manifest(
                output, baseline_capture_id, refresh_mode, root, state["capture_id"]
            )
            result["lineage_valid"] = True
            result["observation_counts"] = _assert_history_composition(
                root, output, profile, markets, fixture_variant
            )
        if composed_profile:
            result["provenance_assertions"] = _assert_incremental_provenance(
                root,
                state["capture_id"],
                baseline_capture_id,
                profile,
                refresh_mode,
                markets,
                fixture_variant,
            )
        if legacy_metrics:
            result["unsupported_phases"] = ["request", "decompression", "export"]
            result["unsupported_phase_notes"] = {
                "request": "legacy observer does not time HTTP calls independently of waits and writes",
                "decompression": "offline fixtures use identity encoding; no decompression is timed",
                "export": "legacy observer does not isolate publication export timing",
            }

    if profile != "baseline-setup" and not capture_only:
        verification_context = metrics.collect() if metrics else contextlib.nullcontext(None)
        with verification_context as verified:
            verify_store = Store(root, limits)
            capture.complete_capture(verify_store, state["capture_id"])
            if profile in TARGETED_PROFILES:
                from kalshi_events import metadata

                metadata.verify(output)
            else:
                publish.verify_release(output)
            verification = verified.report() if verified else {"version": 0}
        result["standalone_verification"] = verification
        verification_counters = verification.setdefault("counters", {})
        verification_counters["metadata_json_decodes"] = max(
            0,
            verification_counters.get("json_decodes", 0)
            - verification_counters.get("raw_body_json_decodes", 0),
        )
    result["provenance"] = _environment(source_root, limits, root)
    result["capture_only"] = capture_only
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")


def _parquet_schemas(output: Path) -> dict[str, str]:
    import pyarrow.parquet as parquet

    return {
        path.name: hashlib.sha256(parquet.read_schema(path).serialize().to_pybytes()).hexdigest()
        for path in sorted(output.glob("*.parquet"))
    }


def _frozen_v2_build_worker(
    source_root: Path, frozen_root: Path, root: Path, capture_id: str, result_path: Path
) -> None:
    """Run one v2 full-baseline build against an already captured immutable root."""
    import shutil

    frozen_root = frozen_root.expanduser()
    if frozen_root.is_symlink():
        raise ValueError("frozen capture root must be an existing non-symlink directory")
    frozen_root = frozen_root.resolve()
    if not frozen_root.is_dir():
        raise ValueError("frozen capture root must be an existing non-symlink directory")
    if any(path.is_symlink() for path in frozen_root.rglob("*")):
        raise ValueError("frozen capture root must not contain symlinks")
    _validate_worker_destinations(root, result_path, source_root)
    for destination in (root.resolve(), result_path.resolve()):
        if destination == frozen_root or destination.is_relative_to(frozen_root):
            raise ValueError("worker outputs must be outside the frozen capture root")
    if frozen_root.is_relative_to(root.resolve()):
        raise ValueError("worker clone must not contain the frozen capture root")

    clone_started = time.perf_counter()
    shutil.copytree(frozen_root, root)
    clone_seconds = round(time.perf_counter() - clone_started, 6)
    os.environ["DLT_TELEMETRY"] = "0"
    os.environ["DLT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    sys.path.insert(0, str(source_root / "src"))
    from kalshi_events import capture, common, metadata, normalize, publish, transport, warehouse
    from kalshi_events.common import Limits, Store

    try:
        from kalshi_events import metrics
    except ImportError:
        metrics = _baseline_metrics(source_root)
        _instrument_baseline(
            metrics, [common, capture, metadata, normalize, transport, warehouse, publish]
        )
    else:
        _instrument_current_raw_file_reads(
            metrics, [common, capture, metadata, normalize, transport, warehouse, publish]
        )
    _instrument_warehouse_json_loads(warehouse, metrics)
    dlt_phase_seconds: dict[str, float] = {}
    _instrument_dlt_trace(warehouse, dlt_phase_seconds)
    for module in (capture, common, metadata, normalize, transport, warehouse, publish):
        if hasattr(module, "now"):
            module.now = lambda: FIXED_TIME
    store = Store(root, Limits(max_rate=1000, max_requests=10000, max_download_bytes=4 * 1024**3))
    if store.storage_version != 5:
        raise ValueError("frozen-v2-build comparisons require storage-version-5 roots")
    state = capture.read_capture(store, capture_id)
    if state.get("complete") is not True or state.get("incremental") is True:
        raise ValueError("frozen v2 build requires a complete full capture")
    if (
        store.path(f"builds-v2/{capture_id}").exists()
        or store.path(f"builds/{capture_id}").exists()
    ):
        raise ValueError("frozen capture already has a build; prepare an unbuilt capture root")
    for manifest_path in store.path("releases").glob("*/manifest.json"):
        manifest = json.loads(manifest_path.read_bytes())
        if manifest.get("capture_id") == capture_id:
            raise ValueError("frozen capture already has a release; prepare an unpublished root")

    started = time.perf_counter()
    with metrics.collect() as build_measured:
        warehouse.build_v2_full(Store(root, store.limits), capture_id)
    build_metrics = build_measured.report()
    with metrics.collect() as publish_measured:
        release = publish.publish(Store(root, store.limits), capture_id, catalogue_version=2)
    publish_metrics = publish_measured.report()
    pipeline_elapsed = {
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "build": build_metrics,
        "publish": publish_metrics,
    }
    output = Path(release["output"])
    verification_started = time.perf_counter()
    with metrics.collect() as verified:
        capture.complete_capture(Store(root, store.limits), capture_id)
        publish.verify_release(output)
    verification = verified.report()
    verification["elapsed_seconds"] = round(time.perf_counter() - verification_started, 6)
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    logical_manifest = {key: value for key, value in manifest.items() if key != "warehouse_sha256"}
    result = {
        **pipeline_elapsed,
        "pipeline_elapsed_seconds": pipeline_elapsed.get("elapsed_seconds"),
        "clone_seconds": clone_seconds,
        "dlt_phase_seconds": dlt_phase_seconds,
        "verification": verification,
        "capture_id": capture_id,
        "storage_version": store.storage_version,
        "release_id": release["source_release_id"],
        "artifact_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.glob("*.parquet"))
        },
        "parquet_schema_sha256": _parquet_schemas(output),
        "semantic_relations": _semantic_relation_hashes(root, capture_id, catalogue_version=2),
        "release_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "logical_release_manifest_sha256": hashlib.sha256(
            json.dumps(logical_manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "warehouse_sha256": manifest["warehouse_sha256"],
        "publication_pointer_sha256": _publication_pointer_sha256(
            root,
            "current-v2.json",
            release["source_release_id"],
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        ),
        "lineage": _assert_lineage_manifest(output, None, "full", root, capture_id),
        "lineage_valid": True,
        "output_bytes": sum(path.stat().st_size for path in output.glob("*.parquet")),
        "source_revision": _revision(source_root),
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")


def _frozen_build_worker_command(
    source_root: Path, frozen_root: Path, root: Path, capture_id: str, result_path: Path
) -> list[str]:
    return [
        sys.executable,
        __file__,
        "--worker-frozen-v2-build",
        "--source-root",
        str(source_root),
        "--frozen-root",
        str(frozen_root),
        "--root",
        str(root),
        "--capture-id",
        capture_id,
        "--result",
        str(result_path),
    ]


def _root_inventory(root: Path) -> dict[str, tuple[int, bool, str | None]]:
    inventory = {}
    for path in sorted(root.rglob("*")):
        if not (path.is_file() or path.is_symlink()):
            continue
        is_symlink = path.is_symlink()
        size = path.stat(follow_symlinks=False).st_size
        checksum = None
        if not is_symlink:
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            checksum = digest.hexdigest()
        inventory[path.relative_to(root).as_posix()] = (size, is_symlink, checksum)
    return inventory


def _run_frozen_v2_build(
    source_root: Path,
    report_dir: Path,
    label: str,
    pair_label: str,
    frozen_root: Path,
    capture_id: str,
) -> dict[str, Any]:
    clone = report_dir / f"frozen-v2-root-{label}-{pair_label}"
    result_path = report_dir / f"frozen-v2-result-{label}-{pair_label}.json"
    if clone.exists():
        raise ValueError(f"benchmark clone path already exists: {clone}")
    worker_started = time.perf_counter()
    cleanup_safe = True
    try:
        _run_supervised(
            _frozen_build_worker_command(source_root, frozen_root, clone, capture_id, result_path),
            check=True,
            env=os.environ.copy(),
            timeout=BENCHMARK_CHILD_TIMEOUT_SECONDS,
        )
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["worker_wall_seconds"] = round(time.perf_counter() - worker_started, 6)
        result["role"] = label
        return result
    except _UnreapedWorkerGroup:
        cleanup_safe = False
        raise
    finally:
        import shutil

        if cleanup_safe:
            shutil.rmtree(clone, ignore_errors=True)


def _frozen_v2_gate(
    comparisons: list[dict[str, Any]], identity_stability: dict[str, bool]
) -> dict[str, Any]:
    required = (
        "same_capture_id",
        "semantic_relations_match",
        "parquet_schema_match",
        "artifact_bytes_match",
        "release_id_match",
        "logical_release_manifest_match",
        "lineage_match",
    )
    failed_pairs = [
        pair["pair"] for pair in comparisons if not all(pair.get(key) is True for key in required)
    ]
    return {
        "passed": not failed_pairs and all(identity_stability.values()),
        "failed_pairs": failed_pairs,
        "identity_stability": identity_stability,
        "basis": "same frozen receipts and v2 full-baseline operation; native rows, schemas, Parquet bytes, release identity, manifest, and logical lineage",
    }


def _run_frozen_v2_comparison(
    source_root: Path,
    compare_root: Path,
    frozen_root: Path,
    capture_id: str,
    report_dir: Path,
    warmups: int,
    runs: int,
) -> dict[str, Any]:
    inventory_started = time.perf_counter()
    before = _root_inventory(frozen_root)
    inventory_before_seconds = round(time.perf_counter() - inventory_started, 6)
    if any(is_symlink for _, is_symlink, _ in before.values()):
        raise ValueError("frozen capture root must not contain symlinks")
    roots = {"reference": source_root, "candidate": compare_root}
    results: dict[str, list[dict[str, Any]]] = {label: [] for label in roots}
    for warmup in range(warmups):
        for label, root in roots.items():
            _run_frozen_v2_build(
                root, report_dir, label, f"warmup-{warmup}", frozen_root, capture_id
            )
    comparisons = []
    for pair in range(runs):
        ordered = list(roots.items()) if pair % 2 == 0 else list(reversed(list(roots.items())))
        paired: dict[str, dict[str, Any]] = {}
        for label, root in ordered:
            result = _run_frozen_v2_build(
                root, report_dir, label, f"pair-{pair}", frozen_root, capture_id
            )
            results[label].append(result)
            paired[label] = result
        reference, candidate = paired["reference"], paired["candidate"]
        comparisons.append(
            {
                "pair": pair,
                "build_elapsed_ratio": round(
                    candidate["pipeline_elapsed_seconds"] / reference["pipeline_elapsed_seconds"],
                    6,
                ),
                "same_capture_id": reference["capture_id"] == candidate["capture_id"],
                "semantic_relations_match": reference["semantic_relations"]
                == candidate["semantic_relations"],
                "parquet_schema_match": reference["parquet_schema_sha256"]
                == candidate["parquet_schema_sha256"],
                "artifact_bytes_match": reference["artifact_sha256"]
                == candidate["artifact_sha256"],
                "release_id_match": reference["release_id"] == candidate["release_id"],
                "release_manifest_match": reference["release_manifest_sha256"]
                == candidate["release_manifest_sha256"],
                "logical_release_manifest_match": reference["logical_release_manifest_sha256"]
                == candidate["logical_release_manifest_sha256"],
                "warehouse_sha256_match": reference["warehouse_sha256"]
                == candidate["warehouse_sha256"],
                "lineage_match": _logical_lineage_match(reference, candidate),
                "reference": reference,
                "candidate": candidate,
            }
        )
    identity_stability = {}
    for label, samples in results.items():
        keys = (
            "semantic_relations",
            "parquet_schema_sha256",
            "artifact_sha256",
            "release_id",
            "logical_release_manifest_sha256",
        )
        identity_stability[label] = all(
            all(sample.get(key) == samples[0].get(key) for key in keys)
            and _logical_lineage_signature(sample) == _logical_lineage_signature(samples[0])
            for sample in samples[1:]
        )
    inventory_started = time.perf_counter()
    unchanged = _root_inventory(frozen_root) == before
    inventory_after_seconds = round(time.perf_counter() - inventory_started, 6)
    gate = _frozen_v2_gate(comparisons, identity_stability)
    gate["frozen_input_unchanged"] = unchanged
    gate["passed"] = gate["passed"] and unchanged
    return {
        "profile": "frozen-v2-build",
        "comparison_contract": "same complete capture receipts and same full v2 build/publish/verify operation",
        "source_revisions": {
            "reference": _revision(source_root),
            "candidate": _revision(compare_root),
        },
        "frozen_input": {"root": str(frozen_root), "capture_id": capture_id},
        "storage_compatibility": {
            "seed_storage_version": 5,
            "both_implementations_must_support_storage_version_5": True,
            "pre_v5_runtime_comparison_supported": False,
        },
        "warmups": warmups,
        "runs": runs,
        "clone_time_is_separate": True,
        "frozen_inventory_hash_seconds": {
            "before": inventory_before_seconds,
            "after": inventory_after_seconds,
        },
        "clone_times_seconds": {
            label: [sample["clone_seconds"] for sample in samples]
            for label, samples in results.items()
        },
        "paired_comparisons": comparisons,
        "results": results,
        "correctness_gate": gate,
    }


def _require_complete(state: dict[str, Any], profile: str) -> None:
    if state.get("complete") is not True:
        raise RuntimeError(
            f"{profile} capture {state.get('capture_id')} is incomplete: {state.get('error')}"
        )


def _seed_incremental_root(store: Any) -> None:
    """Give benchmark v2 lineage a fixed root identity under the normal writer lock."""
    with store.writer_guard():
        store.write("root-id.json", {"version": 1, "root_id": FIXED_ROOT_ID})


def _publication_pointer_name(profile: str, refresh_mode: str) -> str:
    if profile == "baseline-v2-setup":
        return "current-v2.json"
    incremental_publication = profile in {
        "baseline-setup",
        "no-change",
        "sparse-change",
        "all-change",
        "history-sparse",
    }
    return (
        "current-v2.json"
        if incremental_publication and refresh_mode == "incremental"
        else "current.json"
    )


def _publication_pointer_sha256(
    root: Path, name: str, release_id: str | None, manifest_sha256: str | None
) -> str:
    pointer_path = root / name
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert pointer.get("source_release_id") == release_id, (
        "publication pointer selects another release"
    )
    assert pointer.get("manifest_sha256") == manifest_sha256, (
        "publication pointer does not certify the published manifest"
    )
    return hashlib.sha256(pointer_path.read_bytes()).hexdigest()


def _semantic_relation_hashes(
    root: Path, capture_id: str, *, catalogue_version: int
) -> dict[str, Any]:
    import duckdb

    from kalshi_events.warehouse import RELATIONS

    build_dir = "builds-v2" if catalogue_version == 2 else "builds"
    database = root / f"{build_dir}/{capture_id}/warehouse.duckdb"
    drop = {"observation_id", "capture_id", "received_at"}
    result = {}
    with duckdb.connect(str(database), read_only=True) as connection:
        for _relation, table in RELATIONS.items():
            if table == "observation_history":
                continue
            cursor = connection.execute(f"select * from main.{table}")
            names = [column[0] for column in cursor.description]
            kept = [name for name in names if name not in drop]
            indexes = [names.index(name) for name in kept]
            rows = [
                {name: values[index] for name, index in zip(kept, indexes, strict=True)}
                for values in cursor.fetchall()
            ]
            encoded = sorted(
                json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) for row in rows
            )
            result[table] = {
                "rows": len(rows),
                "sha256": hashlib.sha256("\n".join(encoded).encode()).hexdigest(),
            }
    return result


def _assert_lineage_manifest(
    output: Path,
    baseline_capture_id: str | None,
    refresh_mode: str,
    root: Path,
    capture_id: str,
) -> dict[str, Any]:
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    lineage_fields = {
        "root_id",
        "baseline_release_id",
        "baseline_manifest_sha256",
        "baseline_capture_id",
        "previous_watermark",
        "watermark_started_at",
        "delta_depth",
        "membership_sha256",
        "warehouse_sha256",
    }
    assert lineage_fields.issubset(manifest), "v2 manifest is missing lineage fields"
    membership = manifest["membership_sha256"]
    assert len(membership) == 64 and all(c in "0123456789abcdef" for c in membership)
    root_id = manifest["root_id"]
    assert len(root_id) == 32 and all(c in "0123456789abcdef" for c in root_id)
    assert root_id == FIXED_ROOT_ID, "v2 manifest root identity differs from the seeded root"
    warehouse_sha256 = manifest["warehouse_sha256"]
    assert len(warehouse_sha256) == 64 and all(c in "0123456789abcdef" for c in warehouse_sha256)
    build_receipt = json.loads(
        (root / "builds-v2" / capture_id / "build.json").read_text(encoding="utf-8")
    )
    assert warehouse_sha256 == build_receipt.get("warehouse_sha256"), (
        "v2 manifest warehouse checksum differs from the certified build receipt"
    )
    if refresh_mode == "incremental":
        assert manifest["delta_depth"] == 1
        assert manifest["baseline_capture_id"] == baseline_capture_id
        for field in ("baseline_release_id", "baseline_manifest_sha256"):
            value = manifest[field]
            assert isinstance(value, str) and len(value) == 64
            assert all(c in "0123456789abcdef" for c in value)
    else:
        assert manifest["delta_depth"] == 0
        assert manifest["baseline_capture_id"] is None
        assert manifest["baseline_release_id"] is None
        assert manifest["baseline_manifest_sha256"] is None
    return {
        "lineage_field_count": len(lineage_fields),
        "membership_sha256": membership,
        "delta_depth": manifest["delta_depth"],
        "baseline_capture_id": manifest["baseline_capture_id"],
        "root_id": root_id,
        "baseline_release_id": manifest["baseline_release_id"],
        "baseline_manifest_sha256": manifest["baseline_manifest_sha256"],
        "previous_watermark": manifest["previous_watermark"],
        "watermark_started_at": manifest["watermark_started_at"],
        "warehouse_sha256": warehouse_sha256,
    }


def _logical_lineage_match(reference: dict[str, Any], candidate: dict[str, Any]) -> bool:
    fields = (
        "root_id",
        "baseline_release_id",
        "baseline_manifest_sha256",
        "baseline_capture_id",
        "previous_watermark",
        "watermark_started_at",
        "delta_depth",
        "membership_sha256",
    )
    left = reference.get("lineage")
    right = candidate.get("lineage")
    return (
        isinstance(left, dict)
        and isinstance(right, dict)
        and set(fields).issubset(left)
        and set(fields).issubset(right)
        and isinstance(left.get("warehouse_sha256"), str)
        and len(left["warehouse_sha256"]) == 64
        and isinstance(right.get("warehouse_sha256"), str)
        and len(right["warehouse_sha256"]) == 64
        and all(left.get(field) == right.get(field) for field in fields)
        and reference.get("lineage_valid") is True
        and candidate.get("lineage_valid") is True
    )


def _logical_lineage_signature(result: dict[str, Any]) -> tuple[Any, ...]:
    lineage = result.get("lineage", {})
    return tuple(
        lineage.get(field)
        for field in (
            "root_id",
            "baseline_release_id",
            "baseline_manifest_sha256",
            "baseline_capture_id",
            "previous_watermark",
            "watermark_started_at",
            "delta_depth",
            "membership_sha256",
        )
    )


def _upgrade_baseline_clone(source_root: Path, root: Path) -> tuple[float, dict[str, Any]]:
    """Upgrade a copied v2 baseline using the candidate checkout, outside timed refresh work."""
    code = """
import json, sys
from pathlib import Path
from kalshi_events.common import Store
store = Store(Path(sys.argv[1]))
before = store.storage_version
identity = json.loads((store.root / "root-id.json").read_text(encoding="utf-8"))
with store.lock(allow_legacy=True) as lock:
    store.upgrade_root(lock)
after_identity = json.loads((store.root / "root-id.json").read_text(encoding="utf-8"))
print(json.dumps({"storage_version_before": before, "storage_version_after": store.storage_version, "root_id_before": identity["root_id"], "root_id_after": after_identity["root_id"]}))
"""
    environment = os.environ.copy()
    source_path = str(source_root / "src")
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_path + os.pathsep + existing_pythonpath if existing_pythonpath else source_path
    )
    started = time.perf_counter()
    completed = _run_supervised(
        [sys.executable, "-c", code, str(root)],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        timeout=BENCHMARK_CHILD_TIMEOUT_SECONDS,
    )
    elapsed = round(time.perf_counter() - started, 6)
    upgrade = json.loads(completed.stdout)
    assert upgrade == {
        "storage_version_before": 2,
        "storage_version_after": 5,
        "root_id_before": FIXED_ROOT_ID,
        "root_id_after": FIXED_ROOT_ID,
    }, f"candidate baseline clone did not upgrade v2 to v3: {upgrade}"
    return elapsed, upgrade


def _assert_history_composition(
    root: Path, output: Path, profile: str, markets: int, fixture_variant: str = "frozen"
) -> dict[str, Any]:
    import duckdb

    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    baseline_id = manifest.get("baseline_release_id")
    assert isinstance(baseline_id, str)
    baseline_file = root / f"releases/{baseline_id}/observation_history.parquet"
    current_file = output / "observation_history.parquet"
    connection = duckdb.connect()
    try:
        missing = connection.execute(
            "select count(*) from (select * from read_parquet(?) "
            "except all select * from read_parquet(?))",
            [str(baseline_file), str(current_file)],
        ).fetchone()[0]
        baseline_rows = connection.execute(
            "select count(*) from read_parquet(?)", [str(baseline_file)]
        ).fetchone()[0]
        affected_market_count = connection.execute(
            "select count(distinct entity_ticker) from read_parquet(?) "
            "where capture_id = ? and entity_kind = 'markets'",
            [str(current_file), manifest["capture_id"]],
        ).fetchone()[0]
        total_rows = connection.execute(
            "select count(*) from read_parquet(?)", [str(current_file)]
        ).fetchone()[0]
        new_rows = connection.execute(
            "select count(*) from read_parquet(?) where capture_id = ?",
            [
                str(current_file),
                json.loads((output / "manifest.json").read_text(encoding="utf-8"))["capture_id"],
            ],
        ).fetchone()[0]
    finally:
        connection.close()
    assert missing == 0, "composed history dropped or altered baseline observations"
    affected_projection_count = None
    if profile == "history-sparse":
        assert baseline_rows >= HISTORY_STRESS_COUNT, (
            f"expected at least {HISTORY_STRESS_COUNT} retained baseline observations, "
            f"found {baseline_rows}"
        )
        database = root / "builds-v2" / manifest["capture_id"] / "warehouse.duckdb"
        with duckdb.connect(str(database), read_only=True) as warehouse_connection:
            affected_projection_count = warehouse_connection.execute(
                "select count(*) from landing.affected_markets"
            ).fetchone()[0]
        assert affected_projection_count / markets <= 0.02, (
            "history-sparse refresh must affect no more than 2% of market identities, "
            f"found {affected_projection_count}/{markets} in landing.affected_markets"
        )
    combo_count, _ = _fixture_combo_counts(markets, fixture_variant)
    event_updates = 0 if profile == "no-change" else 1
    market_updates = (
        0
        if profile == "no-change"
        else 1
        if profile == "sparse-change"
        else sum(
            not FixtureClient._is_combo(index) and index % 100 == 0 for index in range(markets)
        )
        if profile == "history-sparse"
        else markets - combo_count
    )
    history_updates = 1 if profile == "history-sparse" else 0
    baseline_relations = 2 if profile == "history-sparse" else 3
    expected_new = (
        _fixture_observation_count(markets, profile, fixture_variant)
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL
        else baseline_relations + combo_count + event_updates + market_updates + history_updates
    )
    assert new_rows == expected_new, f"expected {expected_new} T1 observations, found {new_rows}"
    assert total_rows == baseline_rows + new_rows
    return {
        "baseline_observations": baseline_rows,
        "new_t1_observations": new_rows,
        "total_observations": total_rows,
        "affected_market_identities": affected_market_count,
        "affected_market_ratio": affected_market_count / markets,
        "affected_projection_markets": affected_projection_count,
        "affected_projection_ratio": (
            affected_projection_count / markets if affected_projection_count is not None else None
        ),
    }


def _assert_incremental_provenance(
    root: Path,
    capture_id: str,
    baseline_capture_id: str | None,
    profile: str,
    refresh_mode: str,
    markets: int,
    fixture_variant: str = "frozen",
) -> dict[str, Any]:
    import duckdb

    build_dir = "builds-v2" if refresh_mode == "incremental" else "builds"
    database = root / f"{build_dir}/{capture_id}/warehouse.duckdb"
    carried_index = (
        2
        if profile == "history-sparse"
        else 20
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL and markets > 20
        else 1
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL
        else 1
    )
    carried_ticker = f"MKT-{carried_index:06d}"
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = {
            row[0]: {"capture_id": row[1], "received_at": str(row[2])}
            for row in connection.execute(
                "select market_ticker, capture_id, received_at from main.current_markets "
                "where market_ticker in ('MKT-000000', 'MKT-000001', 'MKT-000002', ?)",
                [carried_ticker],
            ).fetchall()
        }
    assertions = {}
    carried_is_combo = _fixture_class(fixture_variant)._is_combo(carried_index)
    with duckdb.connect(str(database), read_only=True) as connection:
        event_row = connection.execute(
            "select capture_id, received_at from main.current_events where event_ticker='EVENT'"
        ).fetchone()
    if event_row is not None:
        if refresh_mode == "incremental" and profile == "no-change":
            expected_event_id, expected_event_time = baseline_capture_id, "2026-01-01"
        else:
            expected_event_id, expected_event_time = capture_id, "2026-01-02"
        assert event_row[0] == expected_event_id and str(event_row[1]).startswith(
            expected_event_time
        )
        assertions["event_source"] = {
            "capture_id": event_row[0],
            "received_at": str(event_row[1]),
        }
    if markets > 2 and carried_index < markets:
        carried = rows[carried_ticker]
        if profile == "all-change" or (
            fixture_variant == COMBO_HEAVY_FIXTURE_LABEL and carried_is_combo
        ):
            expected_id = capture_id
            expected_prefix = "2026-01-02"
        else:
            expected_id = baseline_capture_id if refresh_mode == "incremental" else capture_id
            expected_prefix = "2026-01-01" if refresh_mode == "incremental" else "2026-01-02"
        assert carried["capture_id"] == expected_id and carried["received_at"].startswith(
            expected_prefix
        )
        assertions["unchanged_market_source"] = {
            "market_ticker": carried_ticker,
            **carried,
        }
    if profile != "no-change" and refresh_mode == "incremental":
        refreshed = rows["MKT-000000"]
        assert refreshed["capture_id"] == capture_id
        assert refreshed["received_at"].startswith("2026-01-02")
        assertions["refreshed_market_source"] = refreshed
    return assertions


def _worker_command(
    source_root: Path,
    root: Path,
    result_path: Path,
    markets: int,
    profile: str,
    scenario: str = "full",
    refresh_mode: str = "full",
    baseline_capture_id: str | None = None,
    fixture_variant: str = "frozen",
    capture_only: bool = False,
) -> list[str]:
    command = [
        sys.executable,
        __file__,
        "--worker",
        "--source-root",
        str(source_root),
        "--root",
        str(root),
        "--markets",
        str(markets),
        "--result",
        str(result_path),
        "--worker-profile",
        profile,
        "--scenario",
        scenario,
        "--refresh-mode",
        refresh_mode,
        "--fixture",
        fixture_variant,
    ]
    if baseline_capture_id:
        command += ["--baseline-capture-id", baseline_capture_id]
    if capture_only:
        command.append("--capture-only")
    return command


def _prepare_incremental_baseline(
    source_root: Path,
    report_dir: Path,
    label: str,
    pair: int,
    markets: int,
    scenario: str,
    role: str,
    fixture_variant: str = "frozen",
) -> tuple[Path, dict[str, Any], float]:
    root = report_dir / f"baseline-{label}-{pair}"
    result_path = report_dir / f"baseline-{label}-{pair}.json"
    started = time.perf_counter()
    _run_supervised(
        _worker_command(
            source_root,
            root,
            result_path,
            markets,
            "baseline-setup",
            scenario,
            _paired_refresh_mode(scenario, role),
            fixture_variant=fixture_variant,
        ),
        check=True,
        env=os.environ.copy(),
        timeout=BENCHMARK_CHILD_TIMEOUT_SECONDS,
    )
    setup_seconds = round(time.perf_counter() - started, 6)
    return root, json.loads(result_path.read_text(encoding="utf-8")), setup_seconds


def _paired_refresh_mode(profile: str, role: str) -> str:
    return "incremental" if role == "candidate" or profile == "history-sparse" else "full"


def _run_one(
    source_root: Path,
    report_dir: Path,
    label: str,
    pair: int,
    markets: int,
    profile: str = "full",
    role: str = "reference",
    prepared_baseline: tuple[Path, dict[str, Any], float] | None = None,
    fixture_variant: str = "frozen",
) -> dict[str, Any]:
    import shutil

    root = report_dir / f"root-{label}-{pair}"
    result_path = report_dir / f"run-{label}-{pair}.json"
    setup_result = None
    setup_seconds = None
    baseline_upgrade_seconds = None
    baseline_upgrade = None
    scenario = (
        profile
        if profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
        else "full"
    )
    refresh_mode = _paired_refresh_mode(profile, role)
    if scenario != "full":
        if prepared_baseline is None:
            baseline_root, setup_result, setup_seconds = _prepare_incremental_baseline(
                source_root, report_dir, label, pair, markets, scenario, role, fixture_variant
            )
            remove_baseline = True
        else:
            baseline_root, setup_result, setup_seconds = prepared_baseline
            remove_baseline = False
        clone_started = time.perf_counter()
        shutil.copytree(baseline_root, root)
        baseline_clone_seconds = round(time.perf_counter() - clone_started, 6)
        if profile == "history-sparse" and role == "candidate":
            baseline_upgrade_seconds, baseline_upgrade = _upgrade_baseline_clone(source_root, root)
        baseline_capture_id = setup_result["capture_id"]
    else:
        baseline_root = None
        baseline_capture_id = None
        baseline_clone_seconds = None

    started = time.perf_counter()
    _run_supervised(
        _worker_command(
            source_root,
            root,
            result_path,
            markets,
            profile,
            scenario,
            refresh_mode,
            baseline_capture_id,
            fixture_variant,
        ),
        check=True,
        env=os.environ.copy(),
        timeout=BENCHMARK_CHILD_TIMEOUT_SECONDS,
    )
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result["worker_wall_seconds"] = round(time.perf_counter() - started, 6)
    result["role"] = role
    if setup_result is not None:
        result["baseline_setup_seconds"] = setup_seconds
        result["baseline_clone_seconds"] = baseline_clone_seconds
        result["baseline_setup_metrics"] = setup_result
    if profile == "history-sparse":
        result["baseline_upgrade_seconds"] = baseline_upgrade_seconds
        result["baseline_upgrade"] = baseline_upgrade
    shutil.rmtree(root)
    if baseline_root is not None and (scenario == "full" or remove_baseline):
        shutil.rmtree(baseline_root)
    if result.get("profile") in TARGETED_PROFILES:
        shutil.rmtree(result_path.with_suffix(".metadata"), ignore_errors=True)
    return result


@_cleanup_on_sigterm
def _sampled_storage_run(
    source_root: Path,
    report_dir: Path,
    label: str,
    markets: int,
    profile: str,
    fixture_variant: str = "frozen",
) -> dict[str, Any]:
    """Run an untimed duplicate while sampling actual on-disk high-water usage."""
    root = report_dir / f"storage-root-{label}"
    result_path = report_dir / f"storage-run-{label}.json"
    process = None
    cleanup_safe = True
    retained_peak = temporary_peak = samples = 0
    interval = 0.05
    deadline = time.monotonic() + max(
        0, BENCHMARK_CHILD_TIMEOUT_SECONDS - BENCHMARK_TERMINATION_RESERVE_SECONDS
    )
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                __file__,
                "--worker",
                "--source-root",
                str(source_root),
                "--root",
                str(root),
                "--markets",
                str(markets),
                "--result",
                str(result_path),
                "--worker-profile",
                profile,
                "--fixture",
                fixture_variant,
            ],
            env=os.environ.copy(),
            start_new_session=(os.name == "posix"),
        )
        while process.poll() is None:
            if time.monotonic() >= deadline:
                if not _terminate_worker_group(process):
                    cleanup_safe = False
                    raise _UnreapedWorkerGroup("storage sample worker descendants remain alive")
                raise subprocess.TimeoutExpired(process.args, BENCHMARK_CHILD_TIMEOUT_SECONDS)
            retained = temporary = 0
            if root.exists():
                for directory, _, filenames in os.walk(root, followlinks=False):
                    for filename in filenames:
                        path = Path(directory) / filename
                        try:
                            size = path.stat(follow_symlinks=False).st_size
                        except FileNotFoundError:
                            continue
                        if path.is_file() and not path.is_symlink():
                            relative = path.relative_to(root)
                            is_temporary = "tmp" in relative.parts or filename.startswith(".write-")
                            retained += size
                            if is_temporary:
                                temporary += size
            retained_peak = max(retained_peak, retained)
            temporary_peak = max(temporary_peak, temporary)
            samples += 1
            time.sleep(interval)
        if process.wait() != 0:
            worker_error = subprocess.CalledProcessError(process.returncode, process.args)
        else:
            worker_error = None
        if _worker_group_alive(process):
            if not _terminate_worker_group(process):
                cleanup_safe = False
                raise _UnreapedWorkerGroup("storage sample worker exited with live descendants")
            raise RuntimeError("storage sample worker exited while descendants were still running")
        if worker_error is not None:
            raise worker_error
    except _UnreapedWorkerGroup:
        cleanup_safe = False
        raise
    except BaseException:
        if (
            process is not None
            and _worker_group_alive(process)
            and not _terminate_worker_group(process)
        ):
            cleanup_safe = False
            raise _UnreapedWorkerGroup(
                "storage sample worker descendants remain after interruption"
            ) from None
        raise
    finally:
        import shutil

        if cleanup_safe:
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(result_path.with_suffix(".metadata"), ignore_errors=True)
    return {
        "source": str(source_root),
        "sample_interval_seconds": interval,
        "samples": samples,
        "sampled_actual_high_water_bytes_lower_bound": {
            "retained": retained_peak,
            "temporary": temporary_peak,
        },
        "timing_included": False,
    }


def _revision(root: Path) -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _environment(source_root: Path, limits: Any, data_root: Path | None = None) -> dict[str, Any]:
    packages = {}
    for package in ("dlt", "duckdb", "httpx", "pyarrow", "dbt-core", "dbt-duckdb", "filelock"):
        try:
            packages[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            packages[package] = None
    try:
        filesystem_device = (data_root or source_root).stat().st_dev
    except OSError:
        filesystem_device = None
    source_files = {
        path.relative_to(source_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((source_root / "src/kalshi_events").rglob("*"))
        if path.is_file() and path.suffix in {".py", ".sql", ".yml"}
    }
    return {
        "platform": platform.platform(),
        "python": sys.version,
        "python_executable": sys.executable,
        "packages": packages,
        "filesystem_device": filesystem_device,
        "durability_requirement": "POSIX-file-and-directory-fsync",
        "child_peak_rss_available": True,
        "runtime_source_sha256": source_files,
        "limits": vars(limits) if limits is not None else None,
    }


def _fixture_class(fixture_variant: str) -> type[FixtureClient]:
    if fixture_variant == "frozen":
        return FixtureClient
    if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL:
        return ComboHeavyFixtureClient
    raise ValueError(f"unsupported fixture variant: {fixture_variant}")


def _fixture_combo_counts(markets: int, fixture_variant: str) -> tuple[int, int]:
    fixture_type = _fixture_class(fixture_variant)
    combo_markets = 0
    combo_legs = 0
    for index in range(markets):
        if not fixture_type._is_combo(index):
            continue
        combo_markets += 1
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL:
            ordinal = index - index // 20
            combo_legs += (
                200
                if ordinal % 200 == 0
                else ComboHeavyFixtureClient.LEG_CARDINALITIES[ordinal % 5]
            )
        else:
            combo_legs += 1
    return combo_markets, combo_legs


def _fixture_combo_summary(markets: int, profile: str, fixture_variant: str) -> dict[str, int]:
    live_markets, live_legs = _fixture_combo_counts(markets, fixture_variant)
    historical_markets = (
        live_markets
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL
        and profile in {"full", "no-change", "sparse-change", "all-change"}
        else 0
    )
    historical_legs = (
        live_legs
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL
        and profile in {"full", "no-change", "sparse-change", "all-change"}
        else 0
    )
    return {
        "combo_markets": live_markets + historical_markets,
        "combo_legs_expected": live_legs + historical_legs,
        "live_combo_markets": live_markets,
        "live_combo_legs_expected": live_legs,
        "historical_combo_markets": historical_markets,
        "historical_combo_legs_expected": historical_legs,
    }


def _fixture_observation_count(markets: int, profile: str, fixture_variant: str) -> int:
    """Return fixture observations expected from the selected acquisition scenario."""
    if fixture_variant != COMBO_HEAVY_FIXTURE_LABEL:
        return markets + (
            4 if profile in COMPOSED_REFRESH_PROFILES | {"history-sparse", "baseline-setup"} else 3
        )
    combos, _ = _fixture_combo_counts(markets, fixture_variant)
    if profile in {"full", "baseline-setup", "baseline-v2-setup"}:
        # Full live membership plus a complete historical-market reconciliation.
        return 2 * markets + 3
    if profile == "no-change":
        # Historical membership and combo markets reconcile fully even with no metadata updates.
        return combos + markets + 2
    if profile == "sparse-change":
        return combos + markets + 4  # full history plus one live/event delta
    if profile == "all-change":
        return 2 * markets + 3
    raise ValueError(f"unsupported combo-heavy profile: {profile}")


def _fixture_historical_market_count(
    markets: int, profile: str, fixture_variant: str
) -> int | None:
    if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL:
        return markets if profile in {"full", "no-change", "sparse-change", "all-change"} else None
    if profile in {"all-change", "no-change", "sparse-change", "history-sparse"}:
        return 1
    if profile in {"full", "baseline-setup", "baseline-v2-setup"}:
        return 0
    return None


def _fixture_refresh_history_count(markets: int, profile: str, fixture_variant: str) -> int:
    if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL and profile in {
        "no-change",
        "sparse-change",
        "all-change",
    }:
        return markets
    return 1 if profile == "history-sparse" else 0


def _fixture_full_scope_market_pages(
    markets: int, fixture_variant: str
) -> tuple[int, dict[str, int]]:
    live_pages = (markets + PAGE_SIZE - 1) // PAGE_SIZE
    if fixture_variant == "frozen":
        return live_pages, {"combined": live_pages}
    history_pages = (markets + PAGE_SIZE - 1) // PAGE_SIZE
    return live_pages + history_pages, {
        "live_combined": live_pages,
        "historical": history_pages,
    }


def _fixture_sha256(markets: int, profile: str = "full", fixture_variant: str = "frozen") -> str:
    digest = hashlib.sha256()
    scenario = (
        profile
        if profile in {"no-change", "sparse-change", "all-change", "history-sparse"}
        else "full"
    )
    fixture_type = _fixture_class(fixture_variant)
    fixture_identity = {
        "baseline_time": "2026-01-01T00:00:00Z",
        "refresh_time": "2026-01-02T00:00:00Z",
        "page_size": PAGE_SIZE,
        "scenario": scenario,
    }
    if fixture_variant != "frozen":
        fixture_identity["fixture_variant"] = fixture_variant
    digest.update(json.dumps(fixture_identity, sort_keys=True).encode())
    for stage in ("baseline", "refresh"):
        fixture = fixture_type(markets, scenario, stage)
        digest.update(json.dumps(fixture._event(), sort_keys=True, separators=(",", ":")).encode())
        source_requests = [
            ("/historical/cutoff", {}),
            ("/series", {}),
            ("/events/multivariate", {}),
        ]
        if scenario != "history-sparse" and fixture_variant == "frozen":
            source_requests.append(("/historical/markets", {}))
        for endpoint, params in source_requests:
            digest.update(
                json.dumps(
                    [endpoint, fixture.response(endpoint, params)],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
        if fixture_variant == COMBO_HEAVY_FIXTURE_LABEL:
            cursor = ""
            while True:
                params = {"cursor": cursor} if cursor else {}
                payload = fixture.response("/historical/markets", params)
                digest.update(
                    json.dumps(
                        ["/historical/markets", params, payload],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                cursor = payload[1].get("cursor", "")
                if not cursor:
                    break
        if scenario == "history-sparse":
            cursor = ""
            while True:
                params = {"cursor": cursor} if cursor else {}
                endpoint = "/historical/markets"
                payload = fixture.response(endpoint, params)
                digest.update(
                    json.dumps(
                        [endpoint, params, payload], sort_keys=True, separators=(",", ":")
                    ).encode()
                )
                next_cursor = payload[1].get("cursor", "")
                if not next_cursor:
                    break
                cursor = next_cursor
        if scenario == "history-sparse" and stage == "baseline":
            cursor = ""
            while True:
                params = {"limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                payload = fixture.response("/markets", params)
                digest.update(
                    json.dumps(
                        ["/markets", params, payload],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                cursor = payload[1].get("cursor", "")
                if not cursor:
                    break
        else:
            for index in range(markets):
                digest.update(
                    json.dumps(
                        fixture._market(index), sort_keys=True, separators=(",", ":")
                    ).encode()
                )
    return digest.hexdigest()


def _script_checkout() -> Path:
    script_path = Path(__file__).resolve()
    return script_path.parents[1] if script_path.parent.name == "scripts" else script_path.parent


def _validate_worker_destinations(
    root: Path,
    result_path: Path,
    source_root: Path,
    compare_root: Path | None = None,
) -> None:
    protected = [_script_checkout(), source_root.resolve()]
    if compare_root is not None:
        protected.append(compare_root.resolve())
    for destination in (root.resolve(), result_path.resolve()):
        if any(
            destination == checkout or destination.is_relative_to(checkout)
            for checkout in protected
        ):
            raise ValueError(
                f"worker destination must be outside every source checkout: {destination}"
            )
        if any((ancestor / ".git").exists() for ancestor in (destination, *destination.parents)):
            raise ValueError(f"worker destination cannot be inside a Git checkout: {destination}")


def _correctness_gate(
    comparisons: list[dict[str, Any]],
    *,
    incremental: bool,
    identity_stability: dict[str, bool],
    selected_stress: bool = False,
    v2_output: bool = False,
) -> dict[str, Any]:
    required = (
        ("selection_match", "semantics_match", "coverage_match", "provenance_match")
        if selected_stress
        else (
            ("semantics_match", "artifact_bytes_match", "release_id_match", "lineage_match")
            if incremental
            else ("artifact_bytes_match", "release_id_match", "lineage_match")
        )
        if v2_output
        else ("semantics_match",)
        if incremental
        else (
            "artifact_bytes_match",
            "release_id_match",
            "release_manifest_match",
            "publication_pointer_match",
        )
    )
    failed_pairs = [
        pair["pair"] for pair in comparisons if not all(pair.get(key) is True for key in required)
    ]
    return {
        "passed": not failed_pairs and all(identity_stability.values()),
        "failed_pairs": failed_pairs,
        "identity_stability": identity_stability,
        "basis": (
            "selected native relations, coverage, and provenance; incidental observation history excluded"
            if selected_stress
            else "public outputs and logical v2 lineage; physical warehouse checksum hints are validated per build"
            if v2_output
            else "semantic current relations"
            if incremental
            else "artifact file checksums"
        ),
    }


def _native_acquisition_gate(
    comparisons: list[dict[str, Any]], identity_stability: dict[str, bool]
) -> dict[str, Any]:
    """Compare changed acquisition protocols by native rows/membership, not receipt identity."""
    failed_pairs = [
        pair["pair"]
        for pair in comparisons
        if pair.get("native_values_and_membership_match") is not True
    ]
    return {
        "passed": not failed_pairs and all(identity_stability.values()),
        "failed_pairs": failed_pairs,
        "identity_stability": identity_stability,
        "basis": "native values and current membership; source receipt, observation, Parquet, and release identities may change with acquisition protocol",
    }


def _identity_stability(
    results: dict[str, list[dict[str, Any]]],
    *,
    incremental: bool,
    v2_physical_hint: bool = False,
) -> dict[str, bool]:
    stability: dict[str, bool] = {}
    for label, runs in results.items():
        candidate_incremental = incremental and label == "compare"
        use_v2_identity = v2_physical_hint or candidate_incremental
        fields = (
            ("release_id", "artifact_sha256")
            if use_v2_identity
            else ("release_id", "release_manifest_sha256", "publication_pointer_sha256")
        )
        stable = all(
            all(run.get(field) == runs[0].get(field) for run in runs[1:]) for field in fields
        )
        if use_v2_identity and runs:
            stable = stable and all(
                all(
                    run.get("lineage", {}).get(field) == runs[0].get("lineage", {}).get(field)
                    for field in (
                        "root_id",
                        "baseline_release_id",
                        "baseline_manifest_sha256",
                        "baseline_capture_id",
                        "previous_watermark",
                        "watermark_started_at",
                        "delta_depth",
                        "membership_sha256",
                    )
                )
                and len(run.get("lineage", {}).get("warehouse_sha256", "")) == 64
                and (
                    run.get("lineage_valid") is True
                    if v2_physical_hint
                    else run.get("lineage_valid", True) is True
                )
                for run in runs
            )
        stability[label] = stable
    return stability


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--compare-root", type=Path)
    parser.add_argument("--source-revision", default=None)
    parser.add_argument("--compare-revision", default=None)
    parser.add_argument("--fixture", choices=tuple(sorted(FIXTURE_VARIANTS)), default="frozen")
    parser.add_argument(
        "--prepare-frozen-capture",
        type=Path,
        help="create a fresh external combo-heavy full capture root for a same-receipt build comparison",
    )
    parser.add_argument(
        "--frozen-root",
        type=Path,
        help="completed, unpublished capture-only root used by frozen-v2-build",
    )
    parser.add_argument("--capture-id", help="capture to build in frozen-v2-build mode")
    parser.add_argument(
        "--profile",
        choices=(
            "full",
            "targeted",
            "selected-stress",
            "selected-single",
            "selected-fallback",
            "resume",
            "baseline-v2-setup",
            "no-change",
            "sparse-change",
            "all-change",
            "history-sparse",
            "frozen-v2-build",
        ),
        default="full",
    )
    parser.add_argument("--sample-storage", action="store_true")
    parser.add_argument(
        "--report-dir", type=Path, default=Path(tempfile.gettempdir()) / "kalshi-events-benchmark"
    )
    parser.add_argument(
        "--scale", action="store_true", help="benchmark approximately 250,000 observations"
    )
    parser.add_argument("--markets", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-frozen-v2-build", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--capture-only", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--worker-profile",
        choices=(
            "full",
            "targeted",
            "selected-stress",
            "selected-single",
            "selected-fallback",
            "resume",
            "baseline-setup",
            "baseline-v2-setup",
            "no-change",
            "sparse-change",
            "all-change",
            "history-sparse",
        ),
        default="full",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--scenario",
        choices=("full", "no-change", "sparse-change", "all-change", "history-sparse"),
        default="full",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--refresh-mode", choices=("full", "incremental"), default="full", help=argparse.SUPPRESS
    )
    parser.add_argument("--baseline-capture-id", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker_frozen_v2_build:
        if (
            args.frozen_root is None
            or args.capture_id is None
            or args.result is None
            or args.root is None
        ):
            parser.error(
                "frozen build worker requires --frozen-root, --root, --capture-id, and --result"
            )
        _frozen_v2_build_worker(
            args.source_root.resolve(),
            args.frozen_root,
            args.root,
            args.capture_id,
            args.result,
        )
        return 0
    if args.worker:
        if args.markets is None or args.markets < 1:
            parser.error("worker --markets must be positive")
        if args.root is None or args.result is None:
            parser.error("worker mode requires --root and --result")
        try:
            _validate_worker_destinations(
                args.root, args.result, args.source_root, args.compare_root
            )
        except ValueError as exc:
            parser.error(str(exc))
        _worker(
            args.source_root.resolve(),
            args.root,
            args.markets,
            args.result,
            args.worker_profile,
            args.scenario,
            args.refresh_mode,
            args.baseline_capture_id,
            args.fixture,
            args.capture_only,
        )
        return 0
    if args.runs < 1 or args.warmups < 0:
        parser.error("--runs must be positive and --warmups nonnegative")
    if args.prepare_frozen_capture is not None:
        if args.fixture != COMBO_HEAVY_FIXTURE_LABEL or args.profile != "full":
            parser.error("--prepare-frozen-capture requires --fixture combo-heavy --profile full")
        if args.compare_root or args.frozen_root or args.capture_id:
            parser.error("capture preparation cannot be combined with comparison inputs")
        root = args.prepare_frozen_capture.expanduser().absolute()
        if args.markets is not None and args.markets < 1:
            parser.error("--markets must be positive")
        result_path = root.with_name(root.name + "-capture-result.json")
        if root.exists() or root.is_symlink() or result_path.exists() or result_path.is_symlink():
            parser.error("capture root and adjacent result file must not already exist")
        try:
            _validate_worker_destinations(root, result_path, args.source_root, args.compare_root)
        except ValueError as exc:
            parser.error(str(exc))
        _run_supervised(
            _worker_command(
                args.source_root.resolve(),
                root,
                result_path,
                args.markets or 197,
                "full",
                fixture_variant=COMBO_HEAVY_FIXTURE_LABEL,
                capture_only=True,
            ),
            check=True,
            env=os.environ.copy(),
            timeout=BENCHMARK_CHILD_TIMEOUT_SECONDS,
        )
        result = json.loads(result_path.read_text(encoding="utf-8"))
        print(
            json.dumps(
                {"root": str(root), "capture_id": result["capture_id"], "result": str(result_path)}
            )
        )
        return 0
    if args.profile == "frozen-v2-build":
        if not args.compare_root or not args.frozen_root or not args.capture_id:
            parser.error("frozen-v2-build requires --compare-root, --frozen-root, and --capture-id")
        if args.sample_storage or args.fixture != "frozen":
            parser.error("frozen-v2-build does not accept storage sampling or fixture variants")
        frozen_root = args.frozen_root.expanduser().resolve()
        if args.frozen_root.expanduser().is_symlink():
            parser.error("--frozen-root must not be a symlink")
        if not frozen_root.is_dir():
            parser.error("--frozen-root must name an existing capture root")
        if args.runs < 1 or args.warmups < 0:
            parser.error("--runs must be positive and --warmups nonnegative")
        source_root = args.source_root.resolve()
        compare_root = args.compare_root.resolve()
        report_dir = args.report_dir.resolve()
        if report_dir.exists():
            parser.error("--report-dir must name a new directory outside the source roots")
        protected_roots = (_script_checkout(), source_root, compare_root, frozen_root)
        if any(
            report_dir == protected
            or report_dir.is_relative_to(protected)
            or protected.is_relative_to(report_dir)
            for protected in protected_roots
        ):
            parser.error("--report-dir and frozen root must be disjoint from source checkouts")
        if any(
            frozen_root == source
            or frozen_root.is_relative_to(source)
            or source.is_relative_to(frozen_root)
            for source in (source_root, compare_root, _script_checkout())
        ):
            parser.error("--frozen-root must be outside every source checkout")
        report_dir.mkdir(parents=True, exist_ok=False)
        summary = _run_frozen_v2_comparison(
            source_root,
            compare_root,
            frozen_root,
            args.capture_id,
            report_dir,
            args.warmups,
            args.runs,
        )
        report = report_dir / "benchmark-summary.json"
        report.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(report)
        if not summary["correctness_gate"]["passed"]:
            print("frozen v2 correctness gate failed; see benchmark-summary.json", file=sys.stderr)
            return 1
        return 0
    markets = (
        args.markets
        if args.markets is not None
        else (
            HISTORY_STRESS_IDENTITIES
            if args.profile == "history-sparse"
            else 249997
            if args.scale
            else 197
        )
    )
    if markets < 1:
        parser.error("--markets must be positive")
    incremental_profile = args.profile in {
        "no-change",
        "sparse-change",
        "all-change",
        "history-sparse",
    }
    if incremental_profile and not args.compare_root:
        parser.error(
            "incremental refresh profiles require --compare-root for reference/candidate comparison"
        )
    if incremental_profile and args.sample_storage:
        parser.error("--sample-storage currently supports full, targeted, or resume profiles only")
    if args.fixture == COMBO_HEAVY_FIXTURE_LABEL and args.profile not in {
        "full",
        "no-change",
        "sparse-change",
        "all-change",
    }:
        parser.error(
            "--fixture combo-heavy supports full, no-change, sparse-change, and all-change"
        )
    report_dir = args.report_dir.resolve()
    if report_dir.exists():
        parser.error("--report-dir must name a new directory outside the repository")
    for _, source_root in [
        ("script", _script_checkout()),
        ("source", args.source_root.resolve()),
        *([("compare", args.compare_root.resolve())] if args.compare_root else []),
    ]:
        if report_dir.is_relative_to(source_root):
            parser.error("--report-dir must be outside every source checkout")
    report_dir.mkdir(parents=True, exist_ok=True)
    roots = [("source", args.source_root.resolve())]
    if args.compare_root:
        roots.append(("compare", args.compare_root.resolve()))
    prepared_baselines: dict[str, tuple[Path, dict[str, Any], float]] = {}
    if incremental_profile:
        scenario = args.profile
        if args.profile == "history-sparse":
            baseline = _prepare_incremental_baseline(
                args.source_root.resolve(),
                report_dir,
                "shared-reference",
                0,
                markets,
                scenario,
                "reference",
                args.fixture,
            )
            prepared_baselines = {label: baseline for label, _ in roots}
        else:
            for label, root in roots:
                role = "candidate" if label == "compare" else "reference"
                prepared_baselines[label] = _prepare_incremental_baseline(
                    root,
                    report_dir,
                    f"{label}-shared",
                    0,
                    markets,
                    scenario,
                    role,
                    args.fixture,
                )
    for label, root in roots:
        for warmup in range(args.warmups):
            role = "candidate" if label == "compare" else "reference"
            _run_one(
                root,
                report_dir,
                f"{label}-warmup",
                warmup,
                markets,
                args.profile,
                role,
                prepared_baselines.get(label),
                args.fixture,
            )
    results: dict[str, list[dict[str, Any]]] = {label: [] for label, _ in roots}
    for pair in range(args.runs):
        ordered = roots if pair % 2 == 0 else list(reversed(roots))
        for label, root in ordered:
            role = "candidate" if label == "compare" else "reference"
            results[label].append(
                _run_one(
                    root,
                    report_dir,
                    label,
                    pair,
                    markets,
                    args.profile,
                    role,
                    prepared_baselines.get(label),
                    args.fixture,
                )
            )
    combo_summary = _fixture_combo_summary(markets, args.profile, args.fixture)
    live_combo_markets = combo_summary["live_combo_markets"]
    live_combo_legs = combo_summary["live_combo_legs_expected"]
    combo_markets = combo_summary["combo_markets"]
    combo_legs = combo_summary["combo_legs_expected"]
    full_scope_market_pages, full_scope_market_pages_by_tier = _fixture_full_scope_market_pages(
        markets, args.fixture
    )
    summary = {
        "fixture": {
            "markets": markets,
            "fixture_variant": args.fixture,
            "receipt_uuid_format": _receipt_uuid_format(args.profile),
            "observations_expected": (
                len(_selected_profile_tickers(args.profile))
                if args.profile in SELECTED_PROFILES
                else 3
                if args.profile == "targeted"
                else _fixture_observation_count(markets, args.profile, args.fixture)
            ),
            "selected_tickers": _selected_profile_tickers(args.profile)
            if args.profile in SELECTED_PROFILES
            else None,
            "retained_baseline_history_observations": (
                HISTORY_STRESS_COUNT if args.profile == "history-sparse" else 0
            ),
            "refresh_history_observations": _fixture_refresh_history_count(
                markets, args.profile, args.fixture
            ),
            "history_passes_per_market": (
                HISTORY_STRESS_PASSES if args.profile == "history-sparse" else 0
            ),
            "history_stress_shape": (
                "25 observations per ordinary market identity at baseline; refresh changes about 1% of keys"
                if args.profile == "history-sparse"
                else None
            ),
            "page_size": PAGE_SIZE,
            "full_scope_market_pages": full_scope_market_pages,
            "full_scope_market_pages_by_tier": full_scope_market_pages_by_tier,
            "combo_markets": combo_markets,
            "combo_legs_expected": combo_legs,
            "live_combo_markets": live_combo_markets,
            "live_combo_legs_expected": live_combo_legs,
            "historical_combo_markets": combo_summary["historical_combo_markets"],
            "historical_combo_legs_expected": combo_summary["historical_combo_legs_expected"],
            "live_market_observations_expected": (
                markets
                if args.profile in {"full", "all-change", "baseline-setup", "baseline-v2-setup"}
                else live_combo_markets + 1
                if args.profile == "sparse-change"
                else live_combo_markets
                if args.profile == "no-change"
                else None
            ),
            "historical_market_observations_expected": _fixture_historical_market_count(
                markets, args.profile, args.fixture
            ),
            "incremental_updated_market_pages": (
                (
                    0
                    if args.profile == "no-change"
                    else (
                        1
                        if args.profile == "sparse-change"
                        else (
                            sum(
                                not FixtureClient._is_combo(i) and i % 100 == 0
                                for i in range(markets)
                            )
                            + PAGE_SIZE
                            - 1
                        )
                        // PAGE_SIZE
                        if args.profile == "history-sparse"
                        else (markets - live_combo_markets + PAGE_SIZE - 1) // PAGE_SIZE
                    )
                )
                if incremental_profile
                else None
            ),
            "fixture_sha256": (
                _selected_stress_fixture_sha256(_selected_profile_tickers(args.profile))
                if args.profile in SELECTED_PROFILES
                else _fixture_sha256(markets, args.profile, args.fixture)
            ),
        },
        "environment": _environment(args.source_root.resolve(), None),
        "source_revisions": {
            "source": args.source_revision or _revision(args.source_root.resolve()),
            "compare": (
                args.compare_revision or _revision(args.compare_root.resolve())
                if args.compare_root
                else None
            ),
        },
        "runs": args.runs,
        "warmups": args.warmups,
        "profile": args.profile,
        "counter_legend": {
            "raw_body_reads": "A body-file read, including hash-only file_digest passes; not just JSON reads.",
            "raw_body_json_decodes": "JSON parses of committed raw bodies; metadata JSON parsing is counted separately.",
            "json_decodes": "Counted common.decode calls plus direct first-party warehouse artifact parses; excludes third-party subprocess/internal JSON parsing.",
            "first_party_artifact_json_decodes": "Direct warehouse.json.loads parses of dbt manifest.json and run_results.json; subset of json_decodes, not additive.",
            "hashes": "Instrumented common.digest/file_digest calls, including normalize/metadata aliases, plus explicitly counted transport/journal hashing; excludes uninstrumented lineage/membership and third-party/internal hashing.",
            "hash_bytes": "Bytes counted by instrumented common.digest/file_digest and explicit transport/journal hashing; excludes uninstrumented lineage/membership and third-party/internal hash bytes.",
            "fsync_calls": "Python-level explicit fsync calls observed or inferred by the legacy observer; excludes DuckDB/C++ internal syncs.",
            "encoded_hashing": "Hash time for encoded HTTP entity bodies; legacy observer also classifies transport.digest calls here.",
            "metadata_scans": "Filesystem inventory walk/reconcile calls; metadata JSON parsing is counted separately.",
            "metadata_scan": "Elapsed time in filesystem inventory walk/reconcile operations, excluding metadata JSON decoding.",
        },
        "results": results,
        "baseline_preparations": {
            label: {
                "setup_seconds": setup_seconds,
                "setup_metrics": setup_result,
                "shared_across_warmups_and_pairs": True,
            }
            for label, (_, setup_result, setup_seconds) in (
                {"shared_reference": next(iter(prepared_baselines.values()))}.items()
                if args.profile == "history-sparse"
                else prepared_baselines.items()
            )
        },
    }
    if len(results) == 2:
        reference, candidate = results.values()
        v2_physical_hint = args.profile in {"baseline-v2-setup", "history-sparse"}
        summary["comparison_roles"] = {
            "reference": str(args.source_root.resolve()),
            "candidate": str(args.compare_root.resolve()),
            "elapsed_ratio": "candidate/reference; below 1 is faster",
            "comparison_contract": (
                "changed acquisition: compare native values and current membership; receipt, observation, Parquet, and release identities may differ"
                if args.fixture == COMBO_HEAVY_FIXTURE_LABEL
                else "frozen legacy fixture: compare existing artifact and release identities"
            ),
        }
        summary["paired_comparisons"] = [
            {
                "pair": index,
                "refresh_elapsed_ratio": round(
                    candidate[index]["pipeline_elapsed_seconds"]
                    / reference[index]["pipeline_elapsed_seconds"],
                    4,
                ),
                "native_values_and_membership_match": (
                    reference[index].get("semantic_relations") is not None
                    and reference[index].get("semantic_relations")
                    == candidate[index].get("semantic_relations")
                    if args.fixture == COMBO_HEAVY_FIXTURE_LABEL
                    else None
                ),
                "capture_id_match": (
                    reference[index].get("capture_id") == candidate[index].get("capture_id")
                    if args.fixture == COMBO_HEAVY_FIXTURE_LABEL
                    else None
                ),
                **(
                    _selected_comparison(reference[index], candidate[index])
                    if args.profile in SELECTED_PROFILES
                    else {
                        "semantics_match": (
                            reference[index].get("semantic_relations")
                            == candidate[index].get("semantic_relations")
                            if incremental_profile
                            else None
                        ),
                        "native_match": None,
                        "coverage_match": None,
                        "provenance_match": None,
                    }
                ),
                "selected_fallback_request_counts": (
                    {
                        "reference": reference[index].get("selected_fallback_request_count"),
                        "candidate": candidate[index].get("selected_fallback_request_count"),
                    }
                    if args.profile in SELECTED_PROFILES
                    else None
                ),
                "lineage_match": (
                    _logical_lineage_match(reference[index], candidate[index])
                    if v2_physical_hint
                    else None
                ),
                "artifact_bytes_match": (
                    reference[index].get("artifact_sha256")
                    == candidate[index].get("artifact_sha256")
                    if not incremental_profile or v2_physical_hint
                    else None
                ),
                "release_id_match": (
                    reference[index].get("release_id") == candidate[index].get("release_id")
                    if not incremental_profile or v2_physical_hint
                    else None
                ),
                "release_manifest_match": (
                    reference[index].get("release_manifest_sha256")
                    == candidate[index].get("release_manifest_sha256")
                    if not incremental_profile and not v2_physical_hint
                    else None
                ),
                "publication_pointer_match": (
                    reference[index].get("publication_pointer_sha256")
                    == candidate[index].get("publication_pointer_sha256")
                    if not incremental_profile and not v2_physical_hint
                    else None
                ),
                "timing_scope": (
                    "native worker pipeline timing: both roles perform the same baseline-plus-delta T1 refresh; the candidate uses a copied v2 baseline upgraded to storage v3 before the timed interval; baseline setup, copy/upgrade, worker startup, verification, and diagnostics excluded"
                    if args.profile == "history-sparse"
                    else "native worker pipeline timing: reference full T1 acquisition/build/publication vs candidate baseline-plus-delta T1 refresh; baseline setup, worker startup, standalone verification, and post-run diagnostics excluded"
                    if incremental_profile
                    else "native worker pipeline timing; worker startup, standalone verification, and output checksums excluded"
                ),
            }
            for index in range(args.runs)
        ]
        identity_stability = _identity_stability(
            results,
            incremental=incremental_profile,
            v2_physical_hint=args.profile in {"baseline-v2-setup", "history-sparse"},
        )
        if args.fixture == COMBO_HEAVY_FIXTURE_LABEL:
            summary["correctness_gate"] = _native_acquisition_gate(
                summary["paired_comparisons"], identity_stability
            )
        else:
            summary["correctness_gate"] = _correctness_gate(
                summary["paired_comparisons"],
                incremental=incremental_profile,
                identity_stability=identity_stability,
                selected_stress=args.profile in SELECTED_PROFILES,
                v2_output=args.profile in {"baseline-v2-setup", "history-sparse"},
            )
    if args.sample_storage:
        summary["untimed_storage_samples"] = {
            label: _sampled_storage_run(
                root, report_dir, label, markets, args.profile, args.fixture
            )
            for label, root in roots
        }
    report = report_dir / "benchmark-summary.json"
    report.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    if summary.get("correctness_gate", {}).get("passed"):
        import shutil

        for baseline_root, _, _ in prepared_baselines.values():
            shutil.rmtree(baseline_root, ignore_errors=True)
    print(report)
    if summary.get("correctness_gate", {}).get("passed") is False:
        print("paired correctness gate failed; see benchmark-summary.json", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
