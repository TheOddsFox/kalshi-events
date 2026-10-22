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
PREFLIGHT = {
    "attempts": 100,
    "downloaded_bytes": 64 * MIB,
    "seconds": 30.0,
    "acquisition_seconds": 25.0,
}
PARTITION = {
    "attempts": 200,
    "downloaded_bytes": 512 * MIB,
    "seconds": 60.0,
    "acquisition_seconds": 55.0,
}
MAX_PILOT_REPORT_BYTES = 32 * MIB
GLOBAL = {
    "attempts": 250,
    "downloaded_bytes": 512 * MIB,
    "seconds": 60.0,
    "acquisition_seconds": 55.0,
}
RESPONSE = {"encoded_bytes": 32 * MIB, "decoded_bytes": 64 * MIB}
MAX_REPORT_BYTES = 1024**2
MAX_REPORT_INTEGER = 2**63 - 1
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
        path = safe_path(path)
        if path.is_symlink() or path.stat().st_size > MAX_REPORT_BYTES:
            return None
        value = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
            parse_float=lambda raw: _finite_float(raw),
        )
    except (CatalogueError, OSError, UnicodeError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _load_pilot_report(path: Path) -> dict[str, Any] | None:
    try:
        path = safe_path(path)
        if path.is_symlink() or path.stat().st_size > MAX_PILOT_REPORT_BYTES:
            return None
        value = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
            parse_float=lambda raw: _finite_float(raw),
        )
    except (CatalogueError, OSError, UnicodeError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _valid_pilot_report(
    value: dict[str, Any] | None,
    operation: str,
    *,
    partitions: list[dict[str, str]] | None = None,
    workers: int | None = None,
) -> bool:
    if value is None or set(value) != {
        "version",
        "command",
        "returncode",
        "result",
        "accounting",
        "receipts",
        "metrics",
        "timings",
        "error",
    }:
        return False
    if (
        value["version"] != 1
        or isinstance(value["version"], bool)
        or value["command"] != "partition"
    ):
        return False
    if not _nonnegative(value["returncode"]):
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
        not _nonnegative(accounting.get(k))
        for k in ("attempts", "downloaded_or_reserved_bytes", "retained_bytes", "temporary_bytes")
    ) or not isinstance(accounting.get("accounting_complete"), bool):
        return False
    attempt_limit = PREFLIGHT["attempts"] if operation == "preflight" else PARTITION["attempts"]
    download_limit = (
        PREFLIGHT["downloaded_bytes"] if operation == "preflight" else PARTITION["downloaded_bytes"]
    )
    if (
        accounting["attempts"] > attempt_limit
        or accounting["downloaded_or_reserved_bytes"] > download_limit
        or accounting["retained_bytes"] > TOTAL["retained_bytes"]
        or accounting["temporary_bytes"] > TOTAL["temporary_bytes"]
    ):
        return False
    receipts = value["receipts"]
    if not isinstance(receipts, dict) or set(receipts) != {
        "response_statuses",
        "observed_encodings",
        "retry_attempts",
        "encoded_bytes",
        "http_429",
    }:
        return False
    if (
        not isinstance(receipts["response_statuses"], dict)
        or not isinstance(receipts["observed_encodings"], dict)
        or any(
            not isinstance(k, str) or (k != "unknown" and not k.isdigit()) or not _nonnegative(x)
            for k, x in receipts["response_statuses"].items()
        )
        or any(
            k not in {"identity", "gzip"} or not _nonnegative(x)
            for k, x in receipts["observed_encodings"].items()
        )
        or any(not _nonnegative(receipts[k]) for k in ("retry_attempts", "encoded_bytes"))
        or not isinstance(receipts["http_429"], bool)
    ):
        return False
    if (receipts["response_statuses"].get("429", 0) > 0) != receipts["http_429"] or sum(
        receipts["response_statuses"].values()
    ) > accounting["attempts"]:
        return False
    if (
        sum(receipts["response_statuses"].values()) != accounting["attempts"]
        or receipts["retry_attempts"] > accounting["attempts"]
        or receipts["encoded_bytes"] > accounting["downloaded_or_reserved_bytes"]
        or sum(receipts["observed_encodings"].values()) > accounting["attempts"]
    ):
        return False
    if not _valid_pilot_measurements(value):
        return False
    result = value["result"]
    if not isinstance(result, dict):
        return False
    if operation == "preflight":
        if (
            set(result) != {"collection_series", "completed"}
            or not isinstance(result.get("completed"), bool)
            or not isinstance(result.get("collection_series"), dict)
        ):
            return False
        expected = {row["ticker"] for row in (partitions or []) if row.get("kind") == "collection"}
        bindings = result["collection_series"]
        if value["returncode"] == 0 and (
            result["completed"] is not True or set(bindings) != expected
        ):
            return False
        # Failed preflight reports are diagnostic only. Without an explicit
        # binding of the failure to the requested selection, do not trust any
        # partial collection metadata as an identity for this run.
        if value["returncode"] == 1:
            return False
        if not set(bindings) <= expected:
            return False
        try:
            valid_bindings = all(
                ticker(key) == key and ticker(series) == series for key, series in bindings.items()
            )
        except CatalogueError:
            return False
        return valid_bindings and value["returncode"] in {0, 1}
    # A failed traversal must be bound to the exact selectors and worker
    # assignment before its partial result can be trusted. Current workers do
    # not persist that identity in their failure envelope, so fail closed and
    # surface the bounded error string separately in the parent report.
    if value["returncode"] == 1:
        return False
    if set(result) != {
        "workers",
        "completed",
        "budget_stop",
        "pages",
        "observations",
        "overlap_unique_identities",
        "partitions",
    }:
        return False
    if (
        isinstance(result.get("workers"), bool)
        or not _nonnegative(result.get("workers"))
        or result.get("workers") not in range(1, 9)
        or (workers is not None and result.get("workers") != workers)
        or not isinstance(result.get("completed"), bool)
        or (
            result.get("budget_stop") is not None
            and (
                not isinstance(result.get("budget_stop"), str)
                or result.get("budget_stop")
                not in {"request allowance exhausted", "download allowance exhausted"}
            )
        )
        or any(
            not _nonnegative(result.get(k))
            for k in ("pages", "observations", "overlap_unique_identities")
        )
        or not isinstance(result.get("partitions"), list)
        or len(result["partitions"]) > 8
        or result["pages"] > PARTITION["attempts"]
        or result["observations"] > PARTITION["attempts"] * 200
        or result["overlap_unique_identities"] > PARTITION["attempts"] * 200
    ):
        return False
    total_rows = 0
    total_pages = 0
    for row in result["partitions"]:
        if not isinstance(row, dict) or set(row) != {
            "kind",
            "ticker",
            "pages",
            "observations",
            "unique_identities",
            "identity_digests",
            "inventory_sha256",
            "inventory_rows",
        }:
            return False
        if (
            not isinstance(row.get("kind"), str)
            or row.get("kind") not in {"series", "collection"}
            or not isinstance(row.get("ticker"), str)
            or any(
                not _nonnegative(row.get(k))
                for k in ("pages", "observations", "unique_identities", "inventory_rows")
            )
            or row["pages"] > PARTITION["attempts"]
            or row["observations"] > row["pages"] * 200
            or not isinstance(row.get("identity_digests"), list)
            or len(row["identity_digests"]) != row["unique_identities"]
            or row["unique_identities"] > row["observations"]
            or any(not _is_sha256(value) for value in row["identity_digests"])
            or row["identity_digests"] != sorted(set(row["identity_digests"]))
            or row["unique_identities"] > 40_000
            or any(
                not isinstance(x, str)
                or len(x) != 64
                or any(c not in "0123456789abcdef" for c in x)
                for x in row["identity_digests"]
            )
            or not _is_sha256(row.get("inventory_sha256"))
            or row["inventory_rows"] != row["observations"]
        ):
            return False
        total_rows += row["observations"]
        total_pages += row["pages"]
    expected_partitions = [(row["kind"], row["ticker"]) for row in (partitions or [])]
    actual_partitions = [(row["kind"], row["ticker"]) for row in result["partitions"]]
    if actual_partitions != expected_partitions:
        return False
    if value["metrics"]["counters"].get("diagnostic_verified_pages", 0) != result["pages"]:
        return False
    return (
        total_rows == result["observations"]
        and total_pages == result["pages"]
        and value["returncode"] in {0, 1, 3}
        and (
            (value["returncode"] == 0 and result["completed"] is True)
            or (value["returncode"] == 3 and result["budget_stop"] is not None)
            or value["returncode"] == 1
        )
    )


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
    )


def _valid_pilot_measurements(value: dict[str, Any]) -> bool:
    metrics_value = value["metrics"]
    if not isinstance(metrics_value, dict) or not {
        "counters",
        "phases_seconds",
        "store_accounted_high_water_bytes",
        "peak_http_concurrency",
        "peak_rss_bytes",
        "endpoint_families",
    } <= set(metrics_value):
        return False
    families = metrics_value["endpoint_families"]
    if (
        not isinstance(metrics_value["counters"], dict)
        or any(
            not isinstance(k, str) or not _nonnegative(v)
            for k, v in metrics_value["counters"].items()
        )
        or not isinstance(metrics_value["phases_seconds"], dict)
        or any(
            not isinstance(k, str)
            or isinstance(v, bool)
            or not isinstance(v, (int, float))
            or not _finite_measurement(v)
            or v < 0
            for k, v in metrics_value["phases_seconds"].items()
        )
        or not isinstance(metrics_value["store_accounted_high_water_bytes"], dict)
        or any(
            not isinstance(k, str) or not _nonnegative(v)
            for k, v in metrics_value["store_accounted_high_water_bytes"].items()
        )
        or not _nonnegative(metrics_value["peak_http_concurrency"])
        or not isinstance(metrics_value["peak_rss_bytes"], dict)
        or not isinstance(families, dict)
        or len(families) > 32
        or any(
            not isinstance(k, str) or len(k) > 64 or not isinstance(v, dict)
            for k, v in families.items()
        )
    ):
        return False
    allowed_families = {
        "/events",
        "/events/multivariate",
        "/multivariate_event_collections/{ticker}",
    }
    for family, row in families.items():
        if family not in allowed_families:
            return False
        if not _nonnegative(row.get("attempts")) or not _nonnegative(row.get("committed_pages")):
            return False
        if row["committed_pages"] != 0:
            return False
    if value["error"] is not None and (
        not isinstance(value["error"], str) or len(value["error"]) > 600
    ):
        return False
    if value["returncode"] in {0, 3} and value["error"] is not None:
        return False
    timings = value["timings"]
    if not isinstance(timings, dict) or set(timings) != {
        "acquisition_seconds",
        "accounting_seconds",
        "worker_seconds",
    }:
        return False
    return all(
        (seconds is None and value["returncode"] == 1)
        or (
            not isinstance(seconds, bool)
            and isinstance(seconds, (int, float))
            and _finite_measurement(seconds)
            and seconds >= 0
        )
        for seconds in timings.values()
    )


def _finite_float(raw: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    return value


def _nonnegative(value: Any) -> bool:
    return (
        isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_REPORT_INTEGER
    )


def _finite_measurement(value: int | float) -> bool:
    # math.isfinite(int) can overflow while converting untrusted huge JSON integers.
    return _nonnegative(value) if isinstance(value, int) else math.isfinite(value)


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
    if (
        accounting["attempts"] > GLOBAL["attempts"]
        or accounting["downloaded_or_reserved_bytes"] > GLOBAL["downloaded_bytes"]
        or accounting["retained_bytes"] > TOTAL["retained_bytes"]
        or accounting["temporary_bytes"] > TOTAL["temporary_bytes"]
    ):
        return False
    capture = value["capture"]
    if capture is not None and (
        not isinstance(capture, dict)
        or not isinstance(capture.get("complete"), bool)
        or (
            capture.get("capture_id") is not None and not isinstance(capture.get("capture_id"), str)
        )
        or (
            capture.get("error") is not None
            and (not isinstance(capture.get("error"), str) or len(capture["error"]) > 600)
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
        receipts["attempts"] > GLOBAL["attempts"]
        or receipts["encoded_bytes"] > GLOBAL["downloaded_bytes"]
        or not isinstance(receipts.get("http_429"), bool)
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
    if (receipts["response_statuses"].get("429", 0) > 0) != receipts["http_429"] or sum(
        receipts["response_statuses"].values()
    ) > accounting["attempts"]:
        return False
    if (
        receipts["attempts"] != accounting["attempts"]
        or receipts["data_pages"] + receipts["control_pages"] > receipts["attempts"]
        or receipts["control_responses"] > receipts["attempts"]
        or receipts["non_data_responses"] > receipts["attempts"]
        or receipts["retry_attempts"] > receipts["attempts"]
        or receipts["encoded_bytes"] > accounting["downloaded_or_reserved_bytes"]
        or sum(receipts["observed_encodings"].values()) > receipts["attempts"]
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
            or not _finite_measurement(seconds)
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
        or not _finite_measurement(timings[key])
        or timings[key] < 0
        for key in ("accounting_seconds", "worker_seconds")
    ):
        return False
    acquisition_seconds = timings.get("acquisition_seconds")
    if acquisition_seconds is not None and (
        isinstance(acquisition_seconds, bool)
        or not isinstance(acquisition_seconds, (int, float))
        or not _finite_measurement(acquisition_seconds)
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


def _pilot_command(config_path: Path, result_path: Path) -> list[str]:
    worker = Path(__file__).resolve().parents[1] / "scripts" / "probe-acquisition-worker.py"
    return [
        sys.executable,
        str(worker),
        "--partition-worker",
        "--result",
        str(result_path),
        "--config",
        str(config_path),
    ]


def _pilot_limits(
    *, rate: float, attempts: int, download: int, seconds: float, retained: int, temporary: int
) -> dict[str, Any]:
    from dataclasses import asdict

    from kalshi_events.common import Limits

    return asdict(
        Limits(
            max_rate=rate,
            max_requests=attempts,
            max_download_bytes=download,
            max_response_bytes=min(RESPONSE["encoded_bytes"], download),
            max_decoded_response_bytes=RESPONSE["decoded_bytes"],
            max_duration_seconds=seconds,
            max_root_bytes=retained,
            max_temp_bytes=temporary,
            max_database_bytes=TOTAL["retained_bytes"],
        )
    )


def _run_pilot_child(
    *,
    destination: Path,
    paths: dict[str, Path],
    used: dict[str, int | None],
    report: dict[str, Any],
    deadline: float,
    index: int,
    operation: str,
    partitions: list[dict[str, str]],
    collection_series: dict[str, str],
    workers: int,
    rate: float,
    compression: str,
) -> tuple[dict[str, str] | None, str | None]:
    now = time.monotonic()
    remaining_session = deadline - now
    remaining_attempts = TOTAL["attempts"] - int(used["attempts"] or 0)
    remaining_download = TOTAL["downloaded_bytes"] - int(used["downloaded_bytes"] or 0)
    remaining_retained = TOTAL["retained_bytes"] - int(used["retained_bytes"] or 0)
    remaining_temporary = TOTAL["temporary_bytes"] - int(used["temporary_bytes"] or 0)
    limits = PREFLIGHT if operation == "preflight" else PARTITION
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
        return None, "aggregate allowance exhausted before partition stage"
    stage_seconds = min(limits["seconds"], remaining_session)
    acquisition_seconds = min(limits["acquisition_seconds"], max(0.0, stage_seconds - 5.0))
    if acquisition_seconds <= 0:
        return None, "insufficient time remains for partition stage"
    attempts = min(limits["attempts"], remaining_attempts)
    download = min(limits["downloaded_bytes"], remaining_download)
    root = paths["roots"] / f"partition-{index:02d}-{operation}-{workers}w-{compression}"
    try:
        root.mkdir()
    except OSError as exc:
        for key in used:
            used[key] = None
        report["accounting"] = dict(used)
        return None, f"partition root setup failed: {exc}"
    config_path = paths["reports"] / f"partition-{index:02d}.config.json"
    result_path = paths["reports"] / f"partition-{index:02d}.worker.json"
    metrics_path = paths["metrics"] / f"partition-{index:02d}.json"
    stdout_path = paths["logs"] / f"partition-{index:02d}.stdout.log"
    stderr_path = paths["logs"] / f"partition-{index:02d}.stderr.log"
    config = {
        "operation": operation,
        "root": str(root),
        "limits": _pilot_limits(
            rate=rate,
            attempts=attempts,
            download=download,
            seconds=acquisition_seconds,
            retained=remaining_retained,
            temporary=remaining_temporary,
        ),
        "compression": compression,
        "partitions": partitions,
        "collection_series": collection_series,
        "workers": workers,
    }
    _atomic_json(config_path, config)
    report["active_scenario"] = {
        "index": index,
        "kind": operation,
        "workers": workers,
        "rate": rate,
        "compression": compression,
        "root": str(root),
        "limits": {
            "attempts": attempts,
            "downloaded_bytes": download,
            "acquisition_seconds": acquisition_seconds,
            "retained_bytes": remaining_retained,
            "temporary_bytes": remaining_temporary,
        },
    }
    report["accounting"] = dict(used)
    _save_initial(destination, report)
    command = _pilot_command(config_path, result_path)
    environment = os.environ.copy()
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = source + (
        os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else ""
    )
    process = None
    child_started = time.monotonic()
    timed_out = False
    returncode = None
    try:
        with (
            stdout_path.open("x", encoding="utf-8") as stdout,
            stderr_path.open("x", encoding="utf-8") as stderr,
        ):
            process = subprocess.Popen(
                command, stdout=stdout, stderr=stderr, env=environment, shell=False
            )
            allowance = min(stage_seconds - (time.monotonic() - now), deadline - time.monotonic())
            try:
                if allowance <= 0:
                    raise subprocess.TimeoutExpired(command, allowance)
                returncode = process.wait(timeout=allowance)
            except subprocess.TimeoutExpired:
                timed_out = True
                with suppress(ProcessLookupError):
                    process.kill()
                try:
                    returncode = process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    returncode = None
    except OSError as exc:
        for key in used:
            used[key] = None
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        return None, f"partition worker could not start: {exc}"
    elapsed = time.monotonic() - child_started
    worker_report = _load_pilot_report(result_path)
    if (
        timed_out
        or returncode != 0
        or not _valid_pilot_report(worker_report, operation, partitions=partitions, workers=workers)
    ):
        for key in used:
            used[key] = None
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        report["samples"].append(
            {
                "kind": operation,
                "workers": workers,
                "rate": rate,
                "requested_encoding": compression,
                "status": "timed_out" if timed_out else "invalid_worker_report",
                "supervisor_returncode": returncode,
                "accounting": "unknown",
                "subprocess_seconds": round(elapsed, 3),
                "root": str(root),
                "worker_error": worker_report.get("error")
                if isinstance(worker_report, dict)
                else None,
                "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
            }
        )
        return None, "partition worker timed out or accounting could not be verified"
    assert worker_report is not None
    accounting_value = worker_report["accounting"]
    receipts = worker_report["receipts"]
    metrics_value = worker_report["metrics"]
    sample = {
        "kind": operation,
        "workers": workers,
        "rate": rate,
        "requested_encoding": compression,
        "observed_encodings": receipts["observed_encodings"],
        "encoding_fallback": compression == "gzip"
        and set(receipts["observed_encodings"]) == {"identity"},
        "status": "completed"
        if worker_report["returncode"] == 0
        else "partial"
        if worker_report["returncode"] == 3
        else "failed",
        "supervisor_returncode": returncode,
        "command_returncode": worker_report["returncode"],
        "subprocess_seconds": round(elapsed, 3),
        "acquisition_seconds": worker_report["timings"]["acquisition_seconds"],
        "accounting": "verified" if accounting_value["accounting_complete"] else "unknown",
        "attempts": accounting_value["attempts"],
        "encoded_bytes": receipts["encoded_bytes"],
        "downloaded_or_reserved_bytes": accounting_value["downloaded_or_reserved_bytes"],
        "retained_bytes": accounting_value["retained_bytes"],
        "temporary_bytes": accounting_value["temporary_bytes"],
        "temporary_high_water_bytes": (metrics_value or {})
        .get("store_accounted_high_water_bytes", {})
        .get("temporary"),
        "response_statuses": receipts["response_statuses"],
        "retry_attempts": receipts["retry_attempts"],
        "http_429": receipts["http_429"],
        "diagnostic_verified_pages": worker_report["result"].get("pages", 0),
        "diagnostic_verified_pages_per_second": _per_second(
            worker_report["result"].get("pages", 0),
            worker_report["timings"]["acquisition_seconds"],
        ),
        "decoded_bytes": (metrics_value or {}).get("counters", {}).get("decoded_bytes"),
        "attempts_per_second": _per_second(
            accounting_value["attempts"], worker_report["timings"]["acquisition_seconds"]
        ),
        "reporting_overhead_seconds": 0.0,
        "metrics": metrics_value,
        "result": worker_report["result"],
        "root": str(root),
        "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
    }
    if operation == "preflight":
        sample["throughput_conclusion"] = "not_applicable"
    else:
        acquisition_seconds = worker_report["timings"]["acquisition_seconds"]
        data_pages = worker_report["result"].get("pages", 0)
        pages_per_second = _per_second(data_pages, acquisition_seconds)
        sample["achieved_attempts_per_second"] = sample["attempts_per_second"]
        sample["data_pages_per_second"] = pages_per_second
        sample["validated_data_pages_per_second"] = pages_per_second
        sample["throughput_conclusion"] = (
            "unknown"
            if acquisition_seconds is None
            else "insufficient"
            if data_pages < 100 or acquisition_seconds < 10
            else "short_window_only"
        )
    report["samples"].append(sample)
    if not accounting_value["accounting_complete"]:
        for key in used:
            used[key] = None
        sample["accounting"] = "unknown"
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        return None, "partition receipt accounting is incomplete"
    prior_temp = int(used["temporary_bytes"] or 0)
    used["attempts"] = int(used["attempts"] or 0) + accounting_value["attempts"]
    used["downloaded_bytes"] = (
        int(used["downloaded_bytes"] or 0) + accounting_value["downloaded_or_reserved_bytes"]
    )
    used["retained_bytes"] = int(used["retained_bytes"] or 0) + accounting_value["retained_bytes"]
    used["temporary_bytes"] = prior_temp + accounting_value["temporary_bytes"]
    high_water = sample["temporary_high_water_bytes"]
    if not _nonnegative(high_water):
        used["temporary_high_water_bytes"] = None
        for key in ("attempts", "downloaded_bytes", "retained_bytes", "temporary_bytes"):
            used[key] = None
        sample["accounting"] = "unknown"
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        return None, "partition temporary high-water accounting is unavailable"
    aggregate_high_water = prior_temp + high_water
    sample["aggregate_temporary_high_water_bytes"] = aggregate_high_water
    used["temporary_high_water_bytes"] = max(
        int(used["temporary_high_water_bytes"] or 0), aggregate_high_water
    )
    if (
        accounting_value["attempts"] > attempts
        or accounting_value["downloaded_or_reserved_bytes"] > download
        or accounting_value["retained_bytes"] > remaining_retained
        or accounting_value["temporary_bytes"] > remaining_temporary
        or high_water > remaining_temporary
    ):
        for key in used:
            used[key] = None
        sample["accounting"] = "unknown"
        report["accounting"] = dict(used)
        report.pop("active_scenario", None)
        return None, "partition worker exceeded its assigned allowance"
    report["accounting"] = dict(used)
    report.pop("active_scenario", None)
    if metrics_value is not None:
        try:
            _atomic_json(metrics_path, metrics_value)
        except OSError:
            sample["metrics_write_error"] = True
            return None, "partition metrics report could not be written"
    sample["reporting_overhead_seconds"] = round(
        max(0.0, time.monotonic() - (child_started + elapsed)), 6
    )
    if receipts["http_429"]:
        return None, "HTTP 429 observed; remaining scenarios stopped"
    if worker_report["returncode"] == 3:
        return (
            ({}, None)
            if operation == "partitions"
            else (
                None,
                "collection preflight exhausted its request or download allowance",
            )
        )
    if worker_report["returncode"] != 0:
        return None, "partition validation or acquisition failed"
    if operation == "preflight":
        return worker_report["result"].get("collection_series", {}), None
    return {}, None


def _save_initial(destination: Path, report: dict[str, Any]) -> None:
    _atomic_json(destination / "reports" / "summary.json", report)


def _partition_comparison(samples: list[dict[str, Any]]) -> dict[str, Any] | None:
    serial = next(
        (
            sample
            for sample in samples
            if sample.get("kind") == "partitions" and sample.get("workers") == 1
        ),
        None,
    )
    parallel = next(
        (
            sample
            for sample in samples
            if sample.get("kind") == "partitions" and sample.get("workers") == 8
        ),
        None,
    )
    if serial is None or parallel is None:
        return None
    serial_result = serial.get("result")
    parallel_result = parallel.get("result")
    if not isinstance(serial_result, dict) or not isinstance(parallel_result, dict):
        return {"status": "unavailable", "reason": "one traversal has no verified inventory"}
    serial_rows = {(row["kind"], row["ticker"]): row for row in serial_result.get("partitions", [])}
    parallel_rows = {
        (row["kind"], row["ticker"]): row for row in parallel_result.get("partitions", [])
    }
    if set(serial_rows) != set(parallel_rows):
        return {
            "status": "unavailable",
            "reason": "partition inventories do not have matching scope",
        }
    per_partition = []
    all_equal = serial_result.get("completed") is True and parallel_result.get("completed") is True
    for kind, selected in serial_rows:
        left = serial_rows[(kind, selected)]
        right = parallel_rows[(kind, selected)]
        left_ids = set(left["identity_digests"])
        right_ids = set(right["identity_digests"])
        inventory_equal = (
            left["inventory_sha256"] == right["inventory_sha256"]
            and left["observations"] == right["observations"]
        )
        all_equal = all_equal and inventory_equal and left_ids == right_ids
        per_partition.append(
            {
                "kind": kind,
                "ticker": selected,
                "serial_unique_identities": len(left_ids),
                "parallel_unique_identities": len(right_ids),
                "identity_intersection": len(left_ids & right_ids),
                "serial_only_identities": len(left_ids - right_ids),
                "parallel_only_identities": len(right_ids - left_ids),
                "serial_observations": left["observations"],
                "parallel_observations": right["observations"],
                "observation_inventory": "identical" if inventory_equal else "different",
            }
        )
    return {
        "status": "identical" if all_equal else "different_or_incomplete",
        "interpretation": "live source samples are not frozen; differences do not establish worker loss",
        "serial_observations": serial_result.get("observations"),
        "parallel_observations": parallel_result.get("observations"),
        "partitions": per_partition,
    }


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
        raise ValueError("output path cannot contain symlinks") from exc
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
        "limits": {
            "total": dict(TOTAL),
            "global_sample": dict(GLOBAL),
            "preflight": dict(PREFLIGHT),
            "partition_sample": dict(PARTITION),
            "response": dict(RESPONSE),
        },
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
    deadline = started + TOTAL["seconds"]
    collection_series: dict[str, str] = {}
    scenario_index = 0
    if partitions and any(row["kind"] == "collection" for row in partitions):
        scenario_index += 1
        collection_series_value, stop_reason = _run_pilot_child(
            destination=destination,
            paths=paths,
            used=used,
            report=report,
            deadline=deadline,
            index=scenario_index,
            operation="preflight",
            partitions=partitions,
            collection_series={},
            workers=1,
            rate=rates[0],
            compression="gzip",
        )
        if stop_reason is not None or collection_series_value is None:
            report["stop_reason"] = stop_reason or "collection preflight failed"
            report["accounting"] = dict(used)
            report["elapsed_seconds"] = round(time.monotonic() - started, 3)
            _save_initial(destination, report)
            return report
        collection_series = collection_series_value
        report["collection_membership_basis"] = (
            "provider-filter assertion; selected collection definitions bind declared series"
        )
        report["accounting"] = dict(used)
        _save_initial(destination, report)

    scenarios: list[tuple[str, float, str, int]] = [
        ("global", rates[0], encoding, 0) for encoding in ("identity", "gzip")
    ]
    if partitions:
        scenarios.extend([("pilot", rates[0], "gzip", 1), ("pilot", rates[0], "gzip", 8)])
    scenarios.extend(
        ("global", rate, encoding, 0) for rate in rates[1:] for encoding in ("identity", "gzip")
    )
    for _, (scenario_kind, rate, encoding, worker_count) in enumerate(scenarios, 1):
        scenario_index += 1
        index = scenario_index
        if scenario_kind == "pilot":
            _, stop_reason = _run_pilot_child(
                destination=destination,
                paths=paths,
                used=used,
                report=report,
                deadline=deadline,
                index=scenario_index,
                operation="partitions",
                partitions=partitions or [],
                collection_series=collection_series,
                workers=worker_count,
                rate=rate,
                compression=encoding,
            )
            report["accounting"] = dict(used)
            if stop_reason is None and worker_count == 8:
                comparison = _partition_comparison(report["samples"])
                if comparison is not None:
                    report["partition_comparison"] = comparison
                    for sample in report["samples"]:
                        result_value = sample.get("result")
                        if isinstance(result_value, dict):
                            for row in result_value.get("partitions", []):
                                row.pop("identity_digests", None)
            _save_initial(destination, report)
            if stop_reason is not None:
                report["stop_reason"] = stop_reason
                break
            continue
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
    if partitions and "partition_comparison" not in report:
        report["partition_comparison"] = _partition_comparison(report["samples"])
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    report["accounting"] = dict(used)
    _save_initial(destination, report)
    return report


def _per_second(count: int, duration: float | None) -> float | None:
    if duration is None or duration <= 0:
        return None
    try:
        rate = count / duration
    except OverflowError:
        return None
    return round(rate, 4) if math.isfinite(rate) else None


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
    if arguments and arguments[0] == "--partition-worker":
        return partition_worker_main(arguments[1:])
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


def partition_worker_main(arguments: list[str]) -> int:
    """Run one raw-only diagnostic traversal, never a production capture command."""
    if len(arguments) != 4 or arguments[0] != "--result" or arguments[2] != "--config":
        return 2
    result_path = Path(arguments[1])
    config_path = Path(arguments[3])
    started = time.monotonic()
    result: dict[str, Any] = {
        "version": 1,
        "command": "partition",
        "returncode": 1,
        "result": None,
        "accounting": {
            "attempts": 0,
            "downloaded_or_reserved_bytes": 0,
            "accounting_complete": False,
            "retained_bytes": 0,
            "temporary_bytes": 0,
        },
        "receipts": {
            "response_statuses": {},
            "observed_encodings": {},
            "retry_attempts": 0,
            "encoded_bytes": 0,
            "http_429": False,
        },
        "metrics": None,
        "timings": {
            "acquisition_seconds": None,
            "accounting_seconds": None,
            "worker_seconds": None,
        },
        "error": None,
    }
    store = None
    metrics_context = None
    operation = None
    worker_count = None
    try:
        if config_path.is_symlink() or config_path.stat().st_size > 1024**2:
            raise ValueError("invalid pilot configuration path")
        config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(config, dict) or set(config) != {
            "operation",
            "root",
            "limits",
            "compression",
            "partitions",
            "collection_series",
            "workers",
        }:
            raise ValueError("invalid pilot configuration")
        from kalshi_events import capture
        from kalshi_events.common import Budget, Limits, Store
        from kalshi_events.metrics import collect
        from kalshi_events.partition_probe import acquire_partitions, preflight_collections
        from kalshi_events.transport import PublicClient, accounting

        if not isinstance(config["limits"], dict):
            raise ValueError("invalid pilot limits")
        limits = Limits(**config["limits"])
        store = Store(Path(config["root"]), limits)
        operation = config["operation"]
        worker_count = config["workers"]
        if operation not in {"preflight", "partitions"}:
            raise ValueError("invalid pilot operation")
        with collect() as metrics_context:
            acquisition_started = time.monotonic()
            with (
                store.writer_guard(),
                PublicClient(
                    store,
                    Budget(
                        store,
                        max_response_bytes=limits.max_response_bytes,
                        max_decoded_response_bytes=limits.max_decoded_response_bytes,
                    ),
                    compression=config["compression"],
                ) as client,
            ):
                budget = client.budget
                if operation == "preflight":
                    values = preflight_collections(store, client, budget, config["partitions"])
                    result["result"] = {"collection_series": values, "completed": True}
                else:
                    result["result"] = acquire_partitions(
                        store,
                        client,
                        budget,
                        config["partitions"],
                        collection_series=config["collection_series"],
                        workers=config["workers"],
                    )
                    result["returncode"] = 3 if result["result"].get("budget_stop") else 0
                if operation == "preflight":
                    result["returncode"] = 0
            result["timings"]["acquisition_seconds"] = time.monotonic() - acquisition_started
        accounting_started = time.monotonic()
        accounted = accounting(store)
        usage = store.usage()
        inspected = _inspect_capture_receipts(store, capture, None)
        result["accounting"] = {
            **accounted,
            "retained_bytes": usage["retained_bytes"],
            "temporary_bytes": usage["temporary_bytes"],
        }
        result["receipts"] = {
            key: inspected[key]
            for key in (
                "response_statuses",
                "observed_encodings",
                "retry_attempts",
                "encoded_bytes",
                "http_429",
            )
        }
        result["timings"]["accounting_seconds"] = time.monotonic() - accounting_started
    except Exception as exc:
        message = str(exc).replace("\n", " ")[:512]
        result["error"] = f"{type(exc).__name__}: {message}" if message else type(exc).__name__
        if operation == "preflight":
            result["result"] = {"collection_series": {}, "completed": False}
        elif (
            operation == "partitions"
            and isinstance(worker_count, int)
            and not isinstance(worker_count, bool)
        ):
            counters = metrics_context.counters if metrics_context is not None else {}
            result["result"] = {
                "workers": worker_count,
                "completed": False,
                "failure": result["error"] or type(exc).__name__,
                "pages": int(counters.get("diagnostic_verified_pages", 0)),
                "observations": int(counters.get("diagnostic_observations", 0)),
            }
        if store is not None:
            try:
                from kalshi_events import capture
                from kalshi_events.transport import accounting

                accounted = accounting(store)
                usage = store.usage()
                result["accounting"] = {
                    **accounted,
                    "retained_bytes": usage["retained_bytes"],
                    "temporary_bytes": usage["temporary_bytes"],
                }
                inspected = _inspect_capture_receipts(store, capture, None)
                result["receipts"] = {
                    key: inspected[key]
                    for key in (
                        "response_statuses",
                        "observed_encodings",
                        "retry_attempts",
                        "encoded_bytes",
                        "http_429",
                    )
                }
            except Exception:
                pass
    finally:
        if metrics_context is not None:
            result["metrics"] = metrics_context.report()
        result["timings"]["worker_seconds"] = time.monotonic() - started
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
        status = receipt.get("status")
        accounting_complete = receipt.get("accounting_complete")
        downloaded = receipt.get("downloaded_bytes", receipt.get("size", 0))
        retry_delay = receipt.get("retry_delay_seconds")
        if (
            (status is not None and not _nonnegative(status))
            or not isinstance(accounting_complete, bool)
            or not _nonnegative(downloaded)
            or (
                retry_delay is not None
                and (
                    isinstance(retry_delay, bool)
                    or not isinstance(retry_delay, (int, float))
                    or not _finite_measurement(retry_delay)
                    or retry_delay < 0
                )
            )
        ):
            raise CatalogueError("malformed raw receipt accounting during throughput inspection")
        attempts += 1
        if receipt.get("endpoint") == "/historical/cutoff":
            control_responses += 1
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
        encoded_bytes += downloaded
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
