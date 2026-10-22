"""Throughput-profile orchestration for the opt-in acquisition probe."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

from kalshi_events.common import CatalogueError, safe_path, ticker

MIB = 1024**2
GIB = 1024**3
TOTAL = {
    "attempts": 2_000,
    "seconds": 900.0,
    "downloaded_bytes": 4 * GIB,
    "retained_bytes": 8 * GIB,
    "temporary_bytes": 2 * GIB,
}
GLOBAL = {
    "attempts": 250,
    "downloaded_bytes": 512 * MIB,
    "seconds": 60.0,
    "acquisition_seconds": 55.0,
}
RESPONSE = {"encoded_bytes": 32 * MIB, "decoded_bytes": 64 * MIB}
MAX_REPORT_BYTES = 1024**2
TICKER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")


def parse_throughput_rates(values: list[str] | None) -> list[float]:
    if values is None:
        return [10.0]
    if not 1 <= len(values) <= 3:
        raise ValueError("throughput profile accepts one to three request rates")
    rates: list[float] = []
    for value in values:
        try:
            rate = float(value)
        except ValueError as exc:
            raise ValueError(f"invalid request rate: {value}") from exc
        if not math.isfinite(rate) or not 0 < rate <= 20:
            raise ValueError("request rates must be finite, positive, and at most 20")
        if rate in rates:
            raise ValueError("request rates must be distinct")
        rates.append(rate)
    return sorted(rates)


def validate_throughput_selection(
    series_tickers: list[str] | None, collection_tickers: list[str] | None
) -> list[dict[str, str]]:
    """Validate explicit, bounded partitions without discovering provider catalogues."""
    partitions = [
        {"kind": kind, "ticker": value}
        for kind, values in (("series", series_tickers), ("collection", collection_tickers))
        for value in (values or [])
    ]
    if not partitions:
        return []
    if not 2 <= len(partitions) <= 8:
        raise ValueError("select between two and eight distinct event partitions")
    identities = []
    for partition in partitions:
        try:
            value = ticker(partition["ticker"])
        except CatalogueError as exc:
            raise ValueError("partition selectors must be valid native tickers") from exc
        if not TICKER_PATTERN.fullmatch(value):
            raise ValueError("partition selectors must use native ticker syntax")
        identities.append((partition["kind"], value))
    if len(set(identities)) != len(identities):
        raise ValueError("partition selectors must be distinct")
    return partitions


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def _load_report(path: Path) -> dict[str, Any] | None:
    try:
        if path.is_symlink() or path.stat().st_size > MAX_REPORT_BYTES:
            return None
        value = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
            parse_float=lambda raw: _finite_float(raw),
        )
    except (OSError, UnicodeError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _finite_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    return value


def _nonnegative(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _valid_worker_report(value: dict[str, Any] | None) -> bool:
    if value is None or set(value) != {
        "version",
        "command",
        "returncode",
        "capture",
        "accounting",
        "receipts",
        "metrics",
        "timings",
        "error",
    }:
        return False
    if (
        isinstance(value["version"], bool)
        or value["version"] != 1
        or value["command"] != "capture"
        or not _nonnegative(value["returncode"])
    ):
        return False
    accounting = value["accounting"]
    if not isinstance(accounting, dict) or set(accounting) != {
        "attempts",
        "downloaded_or_reserved_bytes",
        "accounting_complete",
        "retained_bytes",
        "temporary_bytes",
    }:
        return False
    if any(
        not _nonnegative(accounting.get(key))
        for key in ("attempts", "downloaded_or_reserved_bytes", "retained_bytes", "temporary_bytes")
    ) or not isinstance(accounting.get("accounting_complete"), bool):
        return False
    capture = value["capture"]
    if capture is not None and (
        not isinstance(capture, dict)
        or not isinstance(capture.get("complete"), bool)
        or (
            capture.get("capture_id") is not None and not isinstance(capture.get("capture_id"), str)
        )
    ):
        return False
    receipts = value["receipts"]
    if not isinstance(receipts, dict) or any(
        not _nonnegative(receipts.get(key))
        for key in (
            "data_pages",
            "control_pages",
            "control_responses",
            "non_data_responses",
            "attempts",
            "retry_attempts",
            "encoded_bytes",
        )
    ):
        return False
    if (
        not isinstance(receipts.get("http_429"), bool)
        or not isinstance(receipts.get("observed_encodings"), dict)
        or not isinstance(receipts.get("response_statuses"), dict)
    ):
        return False
    if any(
        not isinstance(key, str)
        or (key != "unknown" and not key.isdigit())
        or not _nonnegative(count)
        for key, count in receipts.get("response_statuses", {}).items()
    ):
        return False
    if any(
        key not in {"identity", "gzip", "other"} or not _nonnegative(count)
        for key, count in receipts["observed_encodings"].items()
    ):
        return False
    metrics = value["metrics"]
    if metrics is not None and (
        not isinstance(metrics, dict)
        or not isinstance(metrics.get("counters"), dict)
        or not isinstance(metrics.get("phases_seconds"), dict)
        or not isinstance(metrics.get("endpoint_families"), dict)
    ):
        return False
    if metrics is not None:
        if any(
            not isinstance(key, str) or not _nonnegative(count)
            for key, count in metrics["counters"].items()
        ):
            return False
        if any(
            not isinstance(key, str)
            or isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds < 0
            for key, seconds in metrics["phases_seconds"].items()
        ):
            return False
        high_water = metrics.get("store_accounted_high_water_bytes")
        families = metrics["endpoint_families"]
        if (
            not isinstance(high_water, dict)
            or any(
                not isinstance(key, str) or not _nonnegative(amount)
                for key, amount in high_water.items()
            )
            or len(families) > 32
            or any(not isinstance(key, str) or len(key) > 64 for key in families)
        ):
            return False
        concurrency = metrics.get("peak_http_concurrency")
        if not _nonnegative(concurrency):
            return False
        rss = metrics.get("peak_rss_bytes")
        if not isinstance(rss, dict) or any(
            key not in {"self", "children"} or (amount is not None and not _nonnegative(amount))
            for key, amount in rss.items()
        ):
            return False
    timings = value["timings"]
    if not isinstance(timings, dict) or any(
        isinstance(timings.get(key), bool)
        or not isinstance(timings.get(key), (int, float))
        or not math.isfinite(timings[key])
        or timings[key] < 0
        for key in ("accounting_seconds", "worker_seconds")
    ):
        return False
    acquisition_seconds = timings.get("acquisition_seconds")
    if acquisition_seconds is not None and (
        isinstance(acquisition_seconds, bool)
        or not isinstance(acquisition_seconds, (int, float))
        or not math.isfinite(acquisition_seconds)
        or acquisition_seconds < 0
    ):
        return False
    return value["error"] is None or isinstance(value["error"], str)


def _capture_command(
    root: Path,
    metrics_path: Path,
    rate: float,
    compression: str,
    attempts: int,
    seconds: float,
    download: int,
    retained: int,
    temporary: int,
) -> list[str]:
    source_root = Path(__file__).resolve().parents[1]
    worker = source_root / "scripts" / "probe-acquisition-worker.py"
    result = metrics_path.with_suffix(".worker.json")
    cli = [
        "--root",
        str(root),
        "--metrics-file",
        str(metrics_path),
        "capture",
        "--max-rate",
        str(rate),
        "--max-requests",
        str(attempts),
        "--max-download-bytes",
        str(download),
        "--max-response-bytes",
        str(min(RESPONSE["encoded_bytes"], download)),
        "--max-decoded-response-bytes",
        str(RESPONSE["decoded_bytes"]),
        "--max-duration-seconds",
        str(seconds),
        "--max-root-bytes",
        str(retained),
        "--max-temp-bytes",
        str(temporary),
        "--compression",
        compression,
    ]
    return [sys.executable, str(worker), "--result", str(result), "--", *cli]


def _save_initial(destination: Path, report: dict[str, Any]) -> None:
    _atomic_json(destination / "reports" / "summary.json", report)


def run_throughput(
    output: Path,
    rates: list[float],
    *,
    partitions: list[dict[str, str]] | None = None,
    invocation_started: float | None = None,
) -> dict[str, Any]:
    started = time.monotonic() if invocation_started is None else invocation_started
    source_root = Path(__file__).resolve().parents[1]
    destination = output.expanduser().absolute()
    try:
        destination = safe_path(destination)
    except Exception as exc:
        raise ValueError("output path cannot contain symlinks or reparse points") from exc
    if destination == source_root or source_root in destination.parents:
        raise ValueError("output directory must be outside the repository")
    if destination.exists() or not destination.parent.is_dir():
        raise ValueError("output must be a new directory under an existing parent")
    destination.mkdir()
    paths = {name: destination / name for name in ("roots", "metrics", "logs", "reports")}
    for path in paths.values():
        path.mkdir()
    used: dict[str, int | None] = {
        "attempts": 0,
        "downloaded_bytes": 0,
        "retained_bytes": 0,
        "temporary_bytes": 0,
        "temporary_high_water_bytes": 0,
    }
    report: dict[str, Any] = {
        "version": 2,
        "profile": "throughput",
        "started_at_monotonic": started,
        "limits": {"total": dict(TOTAL), "global_sample": dict(GLOBAL), "response": dict(RESPONSE)},
        "rates": rates,
        "partitions": partitions or [],
        "samples": [],
        "accounting": dict(used),
        "complete": False,
        "stop_reason": None,
        "notes": [
            "Samples are short-window public observations; they do not establish sustained API capacity.",
            "Worker phase durations overlap and are not additive; HTTP waits are client-observed.",
        ],
    }
    _save_initial(destination, report)
    if partitions:
        report["stop_reason"] = "partition pilot is not available in this implementation stage"
        _save_initial(destination, report)
        return report

    deadline = started + TOTAL["seconds"]
    scenarios = [(rate, encoding) for rate in rates for encoding in ("identity", "gzip")]
    for index, (rate, encoding) in enumerate(scenarios, 1):
        sample_started = time.monotonic()
        remaining_session = deadline - sample_started
        remaining_attempts = TOTAL["attempts"] - int(used["attempts"] or 0)
        remaining_download = TOTAL["downloaded_bytes"] - int(used["downloaded_bytes"] or 0)
        remaining_retained = TOTAL["retained_bytes"] - int(used["retained_bytes"] or 0)
        remaining_temporary = TOTAL["temporary_bytes"] - int(used["temporary_bytes"] or 0)
        if (
            min(
                remaining_session,
                remaining_attempts,
                remaining_download,
                remaining_retained,
                remaining_temporary,
            )
            <= 0
        ):
            report["stop_reason"] = "aggregate allowance exhausted"
            break
        sample_limit = min(GLOBAL["seconds"], remaining_session)
        acquisition_limit = min(GLOBAL["acquisition_seconds"], max(0.0, sample_limit - 5.0))
        if acquisition_limit <= 0:
            report["stop_reason"] = "insufficient time remains to launch another sample"
            break
        root = paths["roots"] / f"global-{index:02d}-rate-{rate:g}-{encoding}"
        try:
            root.mkdir()
        except OSError as exc:
            report["samples"].append(
                {
                    "rate": rate,
                    "compression": encoding,
                    "status": "failed",
                    "accounting": "unknown",
                    "error": f"could not create sample root: {exc}",
                }
            )
            report["stop_reason"] = "sample storage setup failed; accounting is unknown"
            for key in used:
                used[key] = None
            report["accounting"] = dict(used)
            _save_initial(destination, report)
            break
        metrics_path = paths["metrics"] / f"global-{index:02d}.json"
        result_path = metrics_path.with_suffix(".worker.json")
        stdout_path = paths["logs"] / f"global-{index:02d}.stdout.log"
        stderr_path = paths["logs"] / f"global-{index:02d}.stderr.log"
        per_attempts = min(GLOBAL["attempts"], remaining_attempts)
        per_download = min(GLOBAL["downloaded_bytes"], remaining_download)
        report["active_scenario"] = {
            "index": index,
            "kind": "global",
            "rate": rate,
            "compression": encoding,
            "root": str(root),
            "limits": {
                "attempts": per_attempts,
                "downloaded_bytes": per_download,
                "acquisition_seconds": acquisition_limit,
                "retained_bytes": remaining_retained,
                "temporary_bytes": remaining_temporary,
            },
        }
        report["accounting"] = dict(used)
        _save_initial(destination, report)
        command = _capture_command(
            root,
            metrics_path,
            rate,
            encoding,
            per_attempts,
            acquisition_limit,
            per_download,
            remaining_retained,
            remaining_temporary,
        )
        environment = os.environ.copy()
        source = str(source_root / "src")
        environment["PYTHONPATH"] = source + (
            os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else ""
        )
        process = None
        timed_out = False
        returncode: int | None = None
        child_started = time.monotonic()
        try:
            with (
                stdout_path.open("x", encoding="utf-8") as stdout,
                stderr_path.open("x", encoding="utf-8") as stderr,
            ):
                process = subprocess.Popen(
                    command, stdout=stdout, stderr=stderr, env=environment, shell=False
                )
                child_allowance = min(
                    sample_limit - (time.monotonic() - sample_started), deadline - time.monotonic()
                )
                try:
                    if child_allowance <= 0:
                        raise subprocess.TimeoutExpired(command, child_allowance)
                    returncode = process.wait(timeout=child_allowance)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    with suppress(ProcessLookupError):
                        process.kill()
                    try:
                        returncode = process.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        returncode = None
        except OSError as exc:
            report["samples"].append(
                {
                    "rate": rate,
                    "compression": encoding,
                    "status": "failed",
                    "accounting": "unknown",
                    "error": str(exc),
                }
            )
            report["stop_reason"] = "worker could not start; accounting is unknown"
            for key in used:
                used[key] = None
            report["accounting"] = dict(used)
            report.pop("active_scenario", None)
            break
        elapsed = time.monotonic() - child_started
        worker_report = _load_report(result_path)
        if timed_out or returncode != 0 or not _valid_worker_report(worker_report):
            sample = {
                "rate": rate,
                "compression": encoding,
                "requested_encoding": encoding,
                "status": "timed_out" if timed_out else "invalid_worker_report",
                "supervisor_returncode": returncode,
                "subprocess_seconds": round(elapsed, 3),
                "accounting": "unknown",
                "root": str(root),
                "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
            }
            report["samples"].append(sample)
            report["stop_reason"] = "worker did not exit cleanly or returned untrusted accounting"
            for key in used:
                used[key] = None
            report.pop("active_scenario", None)
            break
        assert worker_report is not None
        account = worker_report["accounting"]
        receipts = worker_report["receipts"]
        metrics = worker_report["metrics"]
        capture = worker_report["capture"]
        command_returncode = worker_report["returncode"]
        complete_accounting = account["accounting_complete"]
        sample: dict[str, Any] = {
            "rate": rate,
            "requested_rate": rate,
            "compression": encoding,
            "requested_encoding": encoding,
            "status": "timed_out"
            if timed_out
            else "completed"
            if command_returncode == 0
            else "partial"
            if command_returncode == 3
            else "failed",
            "supervisor_returncode": returncode,
            "command_returncode": command_returncode,
            "subprocess_seconds": round(elapsed, 3),
            "acquisition_seconds": worker_report["timings"]["acquisition_seconds"],
            "reporting_overhead_seconds": round(
                max(0.0, time.monotonic() - (child_started + elapsed)), 6
            ),
            "accounting": "verified" if complete_accounting else "unknown",
            "attempts": account["attempts"],
            "downloaded_or_reserved_bytes": account["downloaded_or_reserved_bytes"],
            "retained_bytes": account["retained_bytes"],
            "temporary_bytes": account["temporary_bytes"],
            "temporary_high_water_bytes": (metrics or {})
            .get("store_accounted_high_water_bytes", {})
            .get("temporary"),
            "receipts": receipts,
            "metrics": metrics,
            "encoded_bytes": receipts["encoded_bytes"],
            "decoded_bytes": (metrics or {}).get("counters", {}).get("decoded_bytes"),
            "response_statuses": receipts["response_statuses"],
            "observed_encodings": receipts["observed_encodings"],
            "validated_data_pages": receipts["data_pages"],
            "capture": capture,
            "root": str(root),
            "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
            "encoding_fallback": (
                set(receipts["observed_encodings"]) == {"identity"}
                if encoding == "gzip" and receipts["observed_encodings"]
                else None
            ),
            "attempts_per_second": _per_second(
                account["attempts"], worker_report["timings"]["acquisition_seconds"]
            ),
            "achieved_attempts_per_second": _per_second(
                account["attempts"], worker_report["timings"]["acquisition_seconds"]
            ),
            "data_pages_per_second": _per_second(
                receipts["data_pages"], worker_report["timings"]["acquisition_seconds"]
            ),
            "validated_data_pages_per_second": _per_second(
                receipts["data_pages"], worker_report["timings"]["acquisition_seconds"]
            ),
            "throughput_conclusion": "unknown"
            if worker_report["timings"]["acquisition_seconds"] is None
            else "insufficient"
            if receipts["data_pages"] < 100 or worker_report["timings"]["acquisition_seconds"] < 10
            else "short_window_only",
        }
        report["samples"].append(sample)
        if complete_accounting:
            prior_temporary_bytes = int(used["temporary_bytes"] or 0)
            used["attempts"] = int(used["attempts"] or 0) + account["attempts"]
            used["downloaded_bytes"] = (
                int(used["downloaded_bytes"] or 0) + account["downloaded_or_reserved_bytes"]
            )
            used["retained_bytes"] = int(used["retained_bytes"] or 0) + account["retained_bytes"]
            used["temporary_bytes"] = int(used["temporary_bytes"] or 0) + account["temporary_bytes"]
            high_water = sample["temporary_high_water_bytes"]
            if isinstance(high_water, int) and not isinstance(high_water, bool):
                aggregate_high_water = prior_temporary_bytes + high_water
                sample["aggregate_temporary_high_water_bytes"] = aggregate_high_water
                used["temporary_high_water_bytes"] = max(
                    int(used["temporary_high_water_bytes"] or 0), aggregate_high_water
                )
            else:
                sample["accounting"] = "unknown"
                used["temporary_high_water_bytes"] = None
                report["stop_reason"] = "temporary-storage high-water accounting is unknown"
        else:
            for key in used:
                used[key] = None
            report["stop_reason"] = "incomplete receipt accounting"
        if report["stop_reason"] is None and (
            account["attempts"] > per_attempts
            or account["downloaded_or_reserved_bytes"] > per_download
            or account["retained_bytes"] > remaining_retained
            or account["temporary_bytes"] > remaining_temporary
            or sample["temporary_high_water_bytes"] > remaining_temporary
        ):
            for key in used:
                used[key] = None
            sample["accounting"] = "unknown"
            report["stop_reason"] = "worker accounting exceeded its assigned allowance"
        if report["stop_reason"] is None and receipts["http_429"]:
            report["stop_reason"] = "HTTP 429 observed; remaining scenarios stopped"
        elif report["stop_reason"] is None and timed_out:
            report["stop_reason"] = "sample deadline exceeded; worker terminated"
        elif (
            report["stop_reason"] is None
            and command_returncode == 3
            and not _expected_acquisition_budget(worker_report)
        ):
            report["stop_reason"] = "command stopped for a non-request/download budget condition"
        elif report["stop_reason"] is None and command_returncode not in {0, 3}:
            report["stop_reason"] = "unexpected capture command failure"
        elif (
            report["stop_reason"] is None
            and command_returncode == 3
            and (capture is None or capture.get("complete") is not False)
        ):
            report["stop_reason"] = "budget exit did not report an incomplete capture"
        elif report["stop_reason"] is None and any(
            (
                (used.get("attempts") or 0) > TOTAL["attempts"],
                (used.get("downloaded_bytes") or 0) > TOTAL["downloaded_bytes"],
                (used.get("retained_bytes") or 0) > TOTAL["retained_bytes"],
                (used.get("temporary_bytes") or 0) > TOTAL["temporary_bytes"],
            )
        ):
            report["stop_reason"] = "aggregate allowance exceeded"
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        if report["stop_reason"]:
            break
        # A trustworthy budget stop is a valid partial sample; continue only if it left room.
        if command_returncode == 3 and (
            used["attempts"] == TOTAL["attempts"]
            or used["downloaded_bytes"] >= TOTAL["downloaded_bytes"]
        ):
            report["stop_reason"] = "aggregate acquisition allowance exhausted"
            break
        _save_initial(destination, report)
    else:
        report["complete"] = True
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    report["accounting"] = dict(used)
    _save_initial(destination, report)
    return report


def _per_second(count: int, duration: float | None) -> float | None:
    return round(count / duration, 4) if duration is not None and duration > 0 else None


def _expected_acquisition_budget(worker_report: dict[str, Any]) -> bool:
    capture = worker_report["capture"]
    error = capture.get("error") if isinstance(capture, dict) else None
    return (
        isinstance(capture, dict)
        and capture.get("complete") is False
        and error in {"request allowance exhausted", "download allowance exhausted"}
    )


def worker_main(arguments: list[str]) -> int:
    """Run capture, accounting and bounded inspection inside one supervised process."""
    import contextlib
    import io

    if "--" not in arguments:
        return 2
    separator = arguments.index("--")
    prefix, cli_arguments = arguments[:separator], arguments[separator + 1 :]
    if len(prefix) != 2 or prefix[0] != "--result":
        return 2
    result_path = Path(prefix[1])
    started = time.monotonic()
    output = io.StringIO()
    error: str | None = None
    try:
        from kalshi_events import cli

        with contextlib.redirect_stdout(output):
            returncode = cli.main(cli_arguments)
        try:
            capture_result = json.loads(
                output.getvalue(),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
            )
            if not isinstance(capture_result, dict):
                capture_result = None
        except ValueError:
            capture_result = None
        args = cli.parser().parse_args(cli_arguments)
        root = safe_path(args.root)
        from kalshi_events import capture
        from kalshi_events.common import Limits, Store
        from kalshi_events.transport import accounting

        limits = Limits(**{field: getattr(args, field) for field in Limits.__dataclass_fields__})
        store = Store(root, limits)
        accounting_started = time.monotonic()
        accounted = accounting(store)
        usage = store.usage()
        receipts = _inspect_capture_receipts(store, capture, capture_result)
        accounting_seconds = time.monotonic() - accounting_started
        metrics_path = safe_path(args.metrics_file)
        metrics_value = json.loads(metrics_path.read_text(encoding="utf-8"))
        metrics_summary = {
            key: metrics_value.get(key)
            for key in (
                "counters",
                "phases_seconds",
                "store_accounted_high_water_bytes",
                "endpoint_families",
                "elapsed_seconds",
                "peak_http_concurrency",
                "peak_rss_bytes",
            )
        }
        capture_seconds = metrics_value.get("phases_seconds", {}).get("capture")
        if (
            isinstance(capture_seconds, bool)
            or not isinstance(capture_seconds, (int, float))
            or not math.isfinite(capture_seconds)
            or capture_seconds < 0
        ):
            capture_seconds = None
        result = {
            "version": 1,
            "command": "capture",
            "returncode": returncode,
            "capture": capture_result,
            "accounting": {
                **accounted,
                "retained_bytes": usage["retained_bytes"],
                "temporary_bytes": usage["temporary_bytes"],
            },
            "receipts": receipts,
            "metrics": metrics_summary,
            "timings": {
                "acquisition_seconds": capture_seconds,
                "accounting_seconds": accounting_seconds,
                "worker_seconds": time.monotonic() - started,
            },
            "error": error,
        }
    except Exception as exc:
        result = {
            "version": 1,
            "command": "capture",
            "returncode": 1,
            "capture": None,
            "accounting": {
                "attempts": 0,
                "downloaded_or_reserved_bytes": 0,
                "accounting_complete": False,
                "retained_bytes": 0,
                "temporary_bytes": 0,
            },
            "receipts": {
                "data_pages": 0,
                "control_pages": 0,
                "control_responses": 0,
                "non_data_responses": 0,
                "attempts": 0,
                "retry_attempts": 0,
                "encoded_bytes": 0,
                "http_429": False,
                "observed_encodings": {},
            },
            "metrics": None,
            "timings": {
                "acquisition_seconds": None,
                "accounting_seconds": 0.0,
                "worker_seconds": time.monotonic() - started,
            },
            "error": f"worker accounting failed: {type(exc).__name__}",
        }
    try:
        if result_path.is_symlink() or result_path.exists():
            return 2
        _atomic_json(result_path, result)
    except OSError:
        return 2
    return 0


def _inspect_capture_receipts(
    store: Any, capture_module: Any, capture_result: Any
) -> dict[str, Any]:
    from kalshi_events.common import decode, regular_bytes

    attempts = retry_attempts = encoded_bytes = control_responses = non_data_responses = 0
    statuses: dict[str, int] = {}
    encodings: dict[str, int] = {}
    http_429 = False
    raw = store.path("raw")
    for path in raw.glob("*/manifest.json"):
        receipt = decode(regular_bytes(path, 1024**2))
        if not isinstance(receipt, dict):
            raise CatalogueError("invalid raw receipt during throughput accounting")
        attempts += 1
        if receipt.get("endpoint") == "/historical/cutoff":
            control_responses += 1
        status = receipt.get("status")
        if isinstance(status, int) and not isinstance(status, bool):
            statuses[str(status)] = statuses.get(str(status), 0) + 1
            http_429 = http_429 or status == 429
            if status != 200:
                non_data_responses += 1
        else:
            statuses["unknown"] = statuses.get("unknown", 0) + 1
        valid_observation = (
            receipt.get("accounting_complete") is True
            and not receipt.get("error")
            and isinstance(status, int)
            and not isinstance(status, bool)
            and isinstance(receipt.get("size"), int)
            and not isinstance(receipt.get("size"), bool)
            and isinstance(receipt.get("sha256"), str)
            and len(receipt["sha256"]) == 64
            and all(character in "0123456789abcdef" for character in receipt["sha256"])
        )
        encoding = receipt.get("content_encoding")
        if encoding is None and receipt.get("version") == 1:
            encoding = "identity"
        if valid_observation and encoding in {"identity", "gzip"}:
            encodings[encoding] = encodings.get(encoding, 0) + 1
        size = receipt.get("downloaded_bytes", receipt.get("size", 0))
        if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
            encoded_bytes += size
        if receipt.get("retry_delay_seconds") is not None:
            retry_attempts += 1
    data_pages = control_pages = 0
    capture_id = capture_result.get("capture_id") if isinstance(capture_result, dict) else None
    if isinstance(capture_id, str):
        state = capture_module.read_capture(store, capture_id)
        for row in capture_module.page_receipts(store, state):
            manifest = decode(regular_bytes(store.path(row["relative"]), 1024**2))
            if manifest.get("endpoint") == "/historical/cutoff" or manifest.get("status") != 200:
                control_pages += 1
            else:
                data_pages += 1
    return {
        "data_pages": data_pages,
        "control_pages": control_pages,
        "control_responses": control_responses,
        "non_data_responses": non_data_responses,
        "attempts": attempts,
        "retry_attempts": retry_attempts,
        "encoded_bytes": encoded_bytes,
        "http_429": http_429,
        "response_statuses": statuses,
        "observed_encodings": encodings,
    }
