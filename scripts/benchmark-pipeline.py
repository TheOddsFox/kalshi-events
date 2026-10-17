#!/usr/bin/env python3
"""Repeatable offline capture/build/publish benchmark; reports stay outside the repository."""

from __future__ import annotations

import argparse
import contextlib
import contextvars
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs

FIXED_TIME = "2026-01-01T00:00:00Z"
PAGE_SIZE = 200
_request_id = contextvars.ContextVar("fixture_request_id")


def _baseline_metrics() -> Any:
    """Load current recorder standalone so archived source gets comparable read/decode counts."""
    path = Path(__file__).resolve().parents[1] / "src/kalshi_events/metrics.py"
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

            def observe(*args, __name=name, __original=original, **kwargs):
                if __name == "decode":
                    body = args[0]
                    metrics.add("json_decodes")
                    metrics.add("json_decoded_bytes", len(body))
                elif __name == "regular_bytes":
                    body = __original(*args, **kwargs)
                    metrics.add("file_reads")
                    metrics.add("file_read_bytes", len(body))
                    return body
                elif __name == "digest":
                    metrics.add("hashes")
                    metrics.add("hash_bytes", len(args[0]))
                    with metrics.phase("hashing"):
                        return __original(*args, **kwargs)
                elif __name == "file_digest":
                    metrics.add("hashes")
                    metrics.add("file_reads")
                    with metrics.phase("hashing"):
                        result = __original(*args, **kwargs)
                    metrics.add("hash_bytes", result[1])
                    return result
                result = __original(*args, **kwargs)
                if __name == "read_page":
                    store, relative = args[:2]
                    body_path = store.path(str(Path(relative).parent / "body.bin"))
                    metrics.add("raw_body_reads")
                    metrics.add("raw_body_read_bytes", body_path.stat().st_size)
                    if result[0].get("status") == 200:
                        metrics.add("raw_body_json_decodes")
                return result

            observe._bench_wrapped = True
            setattr(module, name, observe)
    store_type = next(module.Store for module in expanded if hasattr(module, "Store"))
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


class FixtureClient:
    """Deterministic capture client with stable receipt IDs and response completion order."""

    def __init__(self, markets: int) -> None:
        self.markets = markets

    def response(self, endpoint: str, params: dict[str, Any]) -> tuple[int, Any]:
        if endpoint == "/historical/cutoff":
            return 200, {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            }
        if endpoint == "/series":
            return 200, {"series": [{"ticker": "SERIES", "title": "Fixture contracts"}]}
        if endpoint == "/events":
            return 200, {
                "events": [{"event_ticker": "EVENT", "series_ticker": "SERIES"}],
                "cursor": "",
            }
        if endpoint == "/events/multivariate":
            return 200, {
                "events": [{"event_ticker": "COMBOEVENT", "series_ticker": "SERIES"}],
                "cursor": "",
            }
        if endpoint == "/markets":
            start = int(params.get("cursor", "0"))
            stop = min(self.markets, start + PAGE_SIZE)
            rows = [self._market(index) for index in range(start, stop)]
            return 200, {"markets": rows, "cursor": str(stop) if stop < self.markets else ""}
        if endpoint == "/historical/markets":
            return 200, {"markets": [], "cursor": ""}
        if endpoint == "/events/EVENT":
            return 200, {"event": {"event_ticker": "EVENT", "series_ticker": "SERIES"}}
        if endpoint == "/events/COMBOEVENT":
            return 200, {"event": {"event_ticker": "COMBOEVENT", "series_ticker": "SERIES"}}
        if endpoint == "/series/SERIES":
            return 200, {"series": {"ticker": "SERIES", "title": "Fixture contracts"}}
        return 404, {}

    @staticmethod
    def _market(index: int) -> dict[str, Any]:
        ticker = f"MKT-{index:06d}"
        combo = index > 1 and index % 1000 == 1
        return {
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


def _worker(source_root: Path, root: Path, markets: int, result_path: Path) -> None:
    os.environ["DLT_TELEMETRY"] = "0"
    os.environ["DLT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    sys.path.insert(0, str(source_root / "src"))
    from kalshi_events import capture, common, publish, transport, warehouse
    from kalshi_events.common import Limits, Store

    legacy_metrics = False
    try:
        from kalshi_events import metrics
    except ImportError:  # Older baseline source has no instrumentation hooks.
        metrics = _baseline_metrics()
        legacy_metrics = True
        _instrument_baseline(metrics, [common, capture, transport, warehouse, publish])

    measured_context = metrics.collect() if metrics else contextlib.nullcontext(None)
    with measured_context as measured:
        # Freeze source capture identities and times; changing them is scoped to this worker.
        capture.uuid = SimpleNamespace(uuid4=lambda: SimpleNamespace(hex="0" * 31 + "1"))
        capture.now = lambda: FIXED_TIME
        os.environ["KALSHI_EVENTS_ALLOW_LOOPBACK"] = "1"
        os.environ["KALSHI_EVENTS_API_URL"] = "http://127.0.0.1/trade-api/v2"
        import httpx

        transport.now = lambda: FIXED_TIME
        transport.uuid = SimpleNamespace(uuid4=lambda: SimpleNamespace(hex=_request_id.get()))
        fixture = FixtureClient(markets)

        def handler(request):
            params = {
                key: values[-1] for key, values in parse_qs(request.url.query.decode()).items()
            }
            status, payload = fixture.response(
                request.url.path.removeprefix("/trade-api/v2"), params
            )
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

        def client_factory(root_store, budget):
            client = transport.PublicClient(root_store, budget)
            client.client.close()
            client.client = httpx.Client(transport=httpx.MockTransport(handler))
            original_get = client.get

            def get(endpoint, params=None, context=None):
                token = _request_id.set(context["request_id"])
                recorder_token = metrics._active.set(measured) if legacy_metrics else None
                try:
                    return original_get(endpoint, params, context)
                finally:
                    if legacy_metrics:
                        metrics._active.reset(recorder_token)
                    _request_id.reset(token)

            client.get = get
            return client

        from contextlib import nullcontext

        limits = Limits(max_rate=1000, max_requests=10000, max_download_bytes=2 * 1024**3)
        store = Store(root, limits)
        capture_phase = metrics.phase("capture") if metrics else nullcontext()
        with capture_phase:
            state = capture.capture(
                store,
                client_factory=client_factory,
            )
        store.reconcile()
        store = Store(root, limits)
        warehouse.build(store, state["capture_id"])
        store.reconcile()
        store = Store(root, limits)
        release = publish.publish(store, state["capture_id"])
        store.reconcile()
        output = Path(release["output"])
        files = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.glob("*.parquet"))
        }
        result = measured.report() if measured else {"version": 0}
        result.update(
            capture_id=state["capture_id"],
            observations=markets + 3,
            release_id=release["source_release_id"],
            parquet_sha256=files,
        )
        if legacy_metrics:
            result["unsupported_phases"] = ["request", "export"]
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


def _run_one(
    source_root: Path, report_dir: Path, label: str, pair: int, markets: int
) -> dict[str, Any]:
    root = report_dir / f"root-{label}-{pair}"
    result_path = report_dir / f"run-{label}-{pair}.json"
    started = time.perf_counter()
    subprocess.run(
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
        ],
        check=True,
        env=os.environ.copy(),
    )
    result = json.loads(result_path.read_text())
    result["elapsed_seconds"] = round(time.perf_counter() - started, 6)
    import shutil

    shutil.rmtree(root)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--compare-root", type=Path)
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
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        _worker(args.source_root.resolve(), args.root, args.markets, args.result)
        return 0
    if args.runs < 1 or args.warmups < 0:
        parser.error("--runs must be positive and --warmups nonnegative")
    markets = args.markets or (249997 if args.scale else 197)
    if markets < 1:
        parser.error("--markets must be positive")
    report_dir = args.report_dir.resolve()
    if report_dir.exists():
        parser.error("--report-dir must name a new directory outside the repository")
    for _, source_root in [
        ("script", Path(__file__).resolve().parents[1]),
        ("source", args.source_root.resolve()),
        *([("compare", args.compare_root.resolve())] if args.compare_root else []),
    ]:
        if report_dir.is_relative_to(source_root):
            parser.error("--report-dir must be outside every source checkout")
    report_dir.mkdir(parents=True, exist_ok=True)
    roots = [("source", args.source_root.resolve())]
    if args.compare_root:
        roots.append(("compare", args.compare_root.resolve()))
    for label, root in roots:
        for warmup in range(args.warmups):
            _run_one(root, report_dir, f"{label}-warmup", warmup, markets)
    results: dict[str, list[dict[str, Any]]] = {label: [] for label, _ in roots}
    for pair in range(args.runs):
        for label, root in roots:
            results[label].append(_run_one(root, report_dir, label, pair, markets))
    summary = {
        "fixture": {
            "markets": markets,
            "observations_expected": markets + 3,
            "page_size": PAGE_SIZE,
        },
        "runs": args.runs,
        "warmups": args.warmups,
        "results": results,
    }
    if len(results) == 2:
        reference, candidate = results.values()
        summary["comparison_roles"] = {
            "reference": str(args.source_root.resolve()),
            "candidate": str(args.compare_root.resolve()),
            "elapsed_ratio": "candidate/reference; below 1 is faster",
        }
        summary["paired_comparisons"] = [
            {
                "pair": index,
                "elapsed_ratio": round(
                    candidate[index]["elapsed_seconds"] / reference[index]["elapsed_seconds"], 4
                ),
                "artifacts_match": reference[index].get("parquet_sha256")
                == candidate[index].get("parquet_sha256"),
            }
            for index in range(args.runs)
        ]
    report = report_dir / "benchmark-summary.json"
    report.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
