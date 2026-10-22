#!/usr/bin/env python3
"""Run bounded, opt-in public API capture probes in disposable operator roots."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(SOURCE_ROOT / "src"))

MIB = 1024**2
GIB = 1024**3
PER_SAMPLE = {
    "attempts": 12,
    "seconds": 30.0,
    "downloaded_bytes": 64 * MIB,
    "response_bytes": 32 * MIB,
    "decoded_response_bytes": 64 * MIB,
}
TOTAL = {
    "attempts": 72,
    "seconds": 180.0,
    "downloaded_bytes": 256 * MIB,
    "retained_bytes": 2 * GIB,
    "temporary_bytes": GIB,
}


def parse_rates(values: list[str] | None) -> list[float]:
    if values is None:
        return [10.0]
    if not 1 <= len(values) <= 3:
        raise ValueError("provide between one and three request rates")
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
    return rates


def validate_output(path: Path, repository: Path) -> Path:
    """Return the canonical destination, requiring a new external directory path."""
    from kalshi_events.common import safe_path

    expanded = path.expanduser().absolute()
    try:
        destination = safe_path(expanded)
    except Exception as exc:
        raise ValueError("output path cannot contain symlinks") from exc
    repo = repository.resolve()
    if destination == repo or repo in destination.parents:
        raise ValueError("output directory must be outside the repository")
    if destination.exists():
        raise ValueError("output directory must not already exist")
    if not destination.parent.is_dir():
        raise ValueError("output parent directory must already exist")
    return destination


def _command(
    root: Path,
    metrics: Path,
    rate: float,
    compression: str,
    attempts: int,
    seconds: float,
    download: int,
    retained: int,
    temporary: int,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "kalshi_events.cli",
        "--root",
        str(root),
        "--metrics-file",
        str(metrics),
        "capture",
        "--max-rate",
        str(rate),
        "--max-requests",
        str(attempts),
        "--max-download-bytes",
        str(download),
        "--max-response-bytes",
        str(min(PER_SAMPLE["response_bytes"], download)),
        "--max-decoded-response-bytes",
        str(PER_SAMPLE["decoded_response_bytes"]),
        "--max-duration-seconds",
        str(seconds),
        "--max-root-bytes",
        str(retained),
        "--max-temp-bytes",
        str(temporary),
        "--compression",
        compression,
    ]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (OSError, UnicodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _finite_json_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _account(root: Path) -> dict[str, Any] | None:
    """Use the producer's receipt and Store accounting, never successful-page counts."""
    try:
        from kalshi_events.common import Limits, Store, safe_path
        from kalshi_events.transport import accounting

        marker = safe_path(root / "root.json")
        if not root.is_dir() or not marker.is_file():
            return None
        store = Store(
            root,
            Limits(
                max_rate=20,
                max_requests=TOTAL["attempts"],
                max_download_bytes=TOTAL["downloaded_bytes"],
                max_response_bytes=PER_SAMPLE["response_bytes"],
                max_decoded_response_bytes=PER_SAMPLE["decoded_response_bytes"],
                max_duration_seconds=TOTAL["seconds"],
                max_root_bytes=TOTAL["retained_bytes"],
                max_temp_bytes=TOTAL["temporary_bytes"],
            ),
        )
        return {**accounting(store), **store.usage()}
    except Exception:
        return None


def _metrics_summary(metrics_path: Path) -> dict[str, Any] | None:
    raw = _read_json(metrics_path)
    if raw is None:
        return None
    counters = raw.get("counters", {})
    phases = raw.get("phases_seconds", {})
    high_water = raw.get("store_accounted_high_water_bytes", {})
    return {
        "counters": counters if isinstance(counters, dict) else {},
        "phases_seconds": phases if isinstance(phases, dict) else {},
        "store_accounted_high_water_bytes": (high_water if isinstance(high_water, dict) else {}),
        "endpoint_families": raw.get("endpoint_families", {}),
        "elapsed_seconds": raw.get("elapsed_seconds"),
    }


def _receipt_summary(root: Path, capture_id: str | None) -> dict[str, Any]:
    """Summarize durable request evidence, including failed/retried attempts."""
    statuses: Counter[str] = Counter()
    encodings: Counter[str] = Counter()
    attempts = retries = encoded_bytes = 0
    valid = True
    raw = root / "raw"
    for manifest_path in raw.glob("*/manifest.json"):
        try:
            receipt = json.loads(
                manifest_path.read_text(encoding="utf-8"),
                parse_constant=_reject_json_constant,
                parse_float=_finite_json_float,
            )
        except (OSError, UnicodeError, ValueError):
            valid = False
            break
        if not isinstance(receipt, dict):
            valid = False
            break
        status = receipt.get("status")
        accounting_complete = receipt.get("accounting_complete")
        downloaded = receipt.get("downloaded_bytes", receipt.get("size", 0))
        retry_delay = receipt.get("retry_delay_seconds")
        if (
            (status is not None and not _nonnegative_int(status))
            or not isinstance(accounting_complete, bool)
            or not _nonnegative_int(downloaded)
            or (
                retry_delay is not None
                and (
                    isinstance(retry_delay, bool)
                    or not isinstance(retry_delay, (int, float))
                    or not math.isfinite(retry_delay)
                    or retry_delay < 0
                )
            )
        ):
            valid = False
            break
        attempts += 1
        statuses[str(status if status is not None else "unknown")] += 1
        checksum = receipt.get("sha256")
        valid_checksum = (
            isinstance(checksum, str)
            and len(checksum) == 64
            and all(character in "0123456789abcdef" for character in checksum)
        )
        if (
            accounting_complete is True
            and not receipt.get("error")
            and _nonnegative_int(receipt.get("size"))
            and valid_checksum
            and status
            in {
                200,
                400,
                404,
                410,
            }
        ):
            encoding = receipt.get("content_encoding", "identity")
            if isinstance(encoding, str) and encoding in {"identity", "gzip"}:
                encodings[encoding] += 1
        encoded_bytes += downloaded
        if retry_delay is not None:
            retries += 1
    committed_pages = None
    if capture_id:
        try:
            from kalshi_events import capture
            from kalshi_events.common import Limits, Store

            if not (root / "root.json").is_file():
                raise OSError("capture root marker is missing")
            store = Store(root, Limits())
            state = capture.read_capture(store, capture_id)
            committed_pages = len(capture.page_receipts(store, state))
        except Exception:
            committed_pages = None
    return {
        "attempts": attempts,
        "retry_attempts": retries,
        "response_statuses": dict(sorted(statuses.items())),
        "observed_encodings": dict(sorted(encodings.items())),
        "encoded_receipt_bytes": encoded_bytes,
        "committed_pages": committed_pages,
        "valid": valid,
    }


def _sample(
    rate: float,
    compression: str,
    index: int,
    paths: dict[str, Path],
    started: float,
    deadline: float,
    used: dict[str, int | None],
) -> dict[str, Any]:
    root = paths["roots"] / f"sample-{index:02d}-rate-{rate:g}-{compression}"
    root.mkdir()
    metrics_path = paths["metrics"] / f"sample-{index:02d}.json"
    stdout_path = paths["logs"] / f"sample-{index:02d}.stdout.json"
    stderr_path = paths["logs"] / f"sample-{index:02d}.stderr.log"
    remaining_seconds = min(PER_SAMPLE["seconds"], deadline - time.monotonic())
    remaining_attempts = min(PER_SAMPLE["attempts"], TOTAL["attempts"] - used["attempts"])
    remaining_download = min(
        PER_SAMPLE["downloaded_bytes"], TOTAL["downloaded_bytes"] - used["downloaded_bytes"]
    )
    remaining_retained = TOTAL["retained_bytes"] - used["retained_bytes"]
    remaining_temporary = TOTAL["temporary_bytes"] - used["temporary_bytes"]
    if (
        min(
            remaining_seconds,
            remaining_attempts,
            remaining_download,
            remaining_retained,
            remaining_temporary,
        )
        <= 0
    ):
        return {
            "rate": rate,
            "compression": compression,
            "root": str(root),
            "status": "not_started",
            "stop_reason": "aggregate budget exhausted",
        }

    cmd = _command(
        root,
        metrics_path,
        rate,
        compression,
        remaining_attempts,
        remaining_seconds,
        remaining_download,
        remaining_retained,
        remaining_temporary,
    )
    env = os.environ.copy()
    source = str(SOURCE_ROOT / "src")
    env["PYTHONPATH"] = source + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    child_started = time.monotonic()
    timed_out = False
    try:
        with (
            stdout_path.open("x", encoding="utf-8") as stdout,
            stderr_path.open("x", encoding="utf-8") as stderr,
        ):
            process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, env=env, shell=False)
            try:
                child_timeout = min(
                    PER_SAMPLE["seconds"] - (time.monotonic() - child_started),
                    deadline - time.monotonic(),
                )
                if child_timeout <= 0:
                    raise subprocess.TimeoutExpired(cmd, child_timeout)
                returncode = process.wait(timeout=child_timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.kill()
                try:
                    returncode = process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    returncode = None
    except OSError as exc:
        return {
            "rate": rate,
            "compression": compression,
            "root": str(root),
            "status": "failed",
            "stop_reason": f"could not run capture: {exc}",
            "accounting": "unknown",
        }

    elapsed = time.monotonic() - child_started
    result = _read_json(stdout_path)
    accounting_result = None if timed_out and returncode is None else _account(root)
    metrics = _metrics_summary(metrics_path)
    receipt_summary = _receipt_summary(root, result.get("capture_id") if result else None)
    if not receipt_summary["valid"]:
        accounting_result = None
    if accounting_result is None:
        accounting_label = "unknown"
    elif accounting_result["accounting_complete"]:
        accounting_label = "verified"
    else:
        accounting_label = "reserved_upper_bound"

    actual_attempts = accounting_result.get("attempts") if accounting_result else None
    actual_downloaded = (
        accounting_result.get("downloaded_or_reserved_bytes") if accounting_result else None
    )
    if accounting_result is not None and any(
        not _nonnegative_int(accounting_result.get(key))
        for key in (
            "attempts",
            "downloaded_or_reserved_bytes",
            "retained_bytes",
            "temporary_bytes",
        )
    ):
        accounting_result = None
        accounting_label = "unknown"
        actual_attempts = actual_downloaded = None
    sample: dict[str, Any] = {
        "rate": rate,
        "compression": compression,
        "requested_encoding": compression,
        "root": str(root),
        "status": "timed_out" if timed_out else "completed" if returncode == 0 else "partial",
        "returncode": returncode,
        "elapsed_seconds": round(elapsed, 3),
        "capture": result,
        "accounting": accounting_label,
        "attempts": actual_attempts,
        "downloaded_or_reserved_bytes": actual_downloaded,
        "retained_bytes": accounting_result.get("retained_bytes") if accounting_result else None,
        "temporary_bytes": accounting_result.get("temporary_bytes") if accounting_result else None,
        "metrics": metrics,
        "receipts": receipt_summary,
        "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
    }
    if result is not None:
        sample["complete"] = result.get("complete") is True
        sample["capture_id"] = result.get("capture_id")
        sample["pages"] = receipt_summary["committed_pages"]
        sample["error"] = result.get("error")
    observed = receipt_summary["observed_encodings"]
    sample["encoding_fallback"] = (
        compression == "gzip" and bool(observed) and set(observed) == {"identity"}
    )
    if accounting_result is not None:
        used["attempts"] += accounting_result["attempts"]
        used["downloaded_bytes"] += accounting_result["downloaded_or_reserved_bytes"]
        used["retained_bytes"] += accounting_result["retained_bytes"]
        if metrics is None:
            sample["temporary_bytes"] = accounting_result["temporary_bytes"]
            sample["temporary_high_water_bytes"] = None
            sample["accounting"] = "unknown"
            used["temporary_high_water_bytes"] = None
            used["temporary_bytes"] += sample["temporary_bytes"]
        else:
            sample["temporary_bytes"] = accounting_result["temporary_bytes"]
            child_high_water = metrics["store_accounted_high_water_bytes"].get("temporary")
            if not _nonnegative_int(child_high_water):
                sample["temporary_high_water_bytes"] = None
                accounting_label = sample["accounting"] = "unknown"
                used["temporary_high_water_bytes"] = None
            else:
                sample["temporary_high_water_bytes"] = used["temporary_bytes"] + child_high_water
                if used["temporary_high_water_bytes"] is not None:
                    used["temporary_high_water_bytes"] = max(
                        used["temporary_high_water_bytes"],
                        sample["temporary_high_water_bytes"],
                    )
            used["temporary_bytes"] += sample["temporary_bytes"]
    else:
        for key in used:
            used[key] = None

    stop_reason = None
    if timed_out:
        stop_reason = "sample deadline exceeded; child terminated"
    elif accounting_result is None or sample["accounting"] != "verified":
        stop_reason = (
            "child could not be reaped within the termination grace"
            if timed_out and returncode is None
            else "accounting or storage usage is unknown"
        )
    elif (
        used["attempts"] > TOTAL["attempts"] or used["downloaded_bytes"] > TOTAL["downloaded_bytes"]
    ):
        stop_reason = "aggregate acquisition allowance exceeded"
    elif (
        used["retained_bytes"] > TOTAL["retained_bytes"]
        or used["temporary_bytes"] > TOTAL["temporary_bytes"]
        or used["temporary_high_water_bytes"] > TOTAL["temporary_bytes"]
    ):
        stop_reason = "aggregate storage allowance exceeded"
    elif time.monotonic() >= deadline:
        stop_reason = "aggregate deadline exhausted"
    if stop_reason:
        sample["stop_reason"] = stop_reason
    return sample


def run(
    output: Path, rates: list[float], *, invocation_started: float | None = None
) -> dict[str, Any]:
    started = time.monotonic() if invocation_started is None else invocation_started
    repository = Path(__file__).resolve().parents[1]
    destination = validate_output(output, repository)
    destination.mkdir()
    paths = {name: destination / name for name in ("roots", "metrics", "logs", "reports")}
    for path in paths.values():
        path.mkdir()
    deadline = started + TOTAL["seconds"]
    used: dict[str, int | None] = {
        "attempts": 0,
        "downloaded_bytes": 0,
        "retained_bytes": 0,
        "temporary_bytes": 0,
        "temporary_high_water_bytes": 0,
    }
    report: dict[str, Any] = {
        "version": 1,
        "started_at_monotonic": started,
        "limits": {"total": dict(TOTAL), "per_sample": dict(PER_SAMPLE)},
        "notes": [
            "The 32 MiB response ceiling and 64 MiB download allowance may exhaust two concurrent reservations before 12 attempts; this is an expected diagnostic stop."
        ],
        "rates": rates,
        "samples": [],
        "accounting": used,
        "complete": False,
        "stop_reason": None,
    }

    def save() -> None:
        report["accounting"] = dict(used)
        summary = paths["reports"] / "summary.json"
        temporary = summary.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(summary)

    index = 0
    for rate in rates:
        for compression in ("identity", "gzip"):
            index += 1
            sample = _sample(rate, compression, index, paths, started, deadline, used)
            report["samples"].append(sample)
            if sample.get("stop_reason"):
                report["stop_reason"] = sample["stop_reason"]
                save()
                return report
            save()
    report["complete"] = True
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    save()
    return report


def main(argv: list[str] | None = None) -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new external directory for disposable roots and reports",
    )
    parser.add_argument("--profile", choices=("screening", "throughput"), default="screening")
    parser.add_argument("--rates", nargs="+", help="one to three distinct rates, each <= 20")
    parser.add_argument(
        "--series-ticker", action="append", help="ordinary event partition (throughput only)"
    )
    parser.add_argument(
        "--collection-ticker",
        action="append",
        help="multivariate event partition (throughput only)",
    )
    args = parser.parse_args(argv)
    try:
        if args.profile == "throughput":
            from acquisition_probe import (
                parse_throughput_rates,
                run_throughput,
                validate_throughput_selection,
            )

            rates = parse_throughput_rates(args.rates)
            partitions = validate_throughput_selection(args.series_ticker, args.collection_ticker)
            report = run_throughput(
                args.output, rates, partitions=partitions, invocation_started=started
            )
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if report["complete"] else 1
        if args.series_ticker or args.collection_ticker:
            raise ValueError("partition selectors require --profile throughput")
        rates = parse_rates(args.rates)
        report = run(args.output, rates, invocation_started=started)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
