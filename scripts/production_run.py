"""Bounded manual production refresh and verified DuckDB activation."""

from __future__ import annotations

import argparse
import contextlib
import contextvars
import ctypes
import errno
import fcntl
import hashlib
import json
import math
import os
import platform
import plistlib
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(SOURCE_ROOT / "src"))

GIB = 1024**3
MIB = 1024**2
HTTPX_MAX_RAW_CHUNK_BYTES = 64 * 1024
LEGACY_RECONCILIATION_UV_LOCK_SHA256 = (
    "9a802ab62302a6ec0d8db5a3c602552892c9370c33c4edc098a0501a192d1d17"
)
RECONCILIATION_UV_LOCK_SHA256 = "79ceef4ec550fcb4f83aa53d3eb140175df78374c21356243831c725ea65c421"
# Saved raw-response implementations accepted for the 64 KiB reconciliation allowance.
PREVIOUS_RECONCILIATION_TRANSPORT_SHA256 = (
    "5c4727b4653c4d201302407b25fc0b914f54ad561961209d3f51524524e91849"
)
PRE_PAYLOAD_RECONCILIATION_TRANSPORT_SHA256 = (
    "c05b2e1c8492d14ae437e11b9b408f82684fba95fdb77b0d66c44084dcab1a55"
)
RECONCILIATION_TRANSPORT_SHA256 = "c61ca11eb57160f93edc2554dbaffe43f2143af423357c863b90acbdea84c8e5"
LEGACY_RECONCILIATION_RUNTIME_VERSIONS = {
    "duckdb": "1.5.6",
    "dlt": "1.31.0",
    "dbt-core": "1.12.5",
    "dbt-duckdb": "1.11.0",
    "pyarrow": "26.0.0",
    "httpx": "0.28.1",
    "httpcore": "1.0.9",
}
_RECONCILIATION_LOCK_TRANSITIONS = {
    (LEGACY_RECONCILIATION_UV_LOCK_SHA256, RECONCILIATION_UV_LOCK_SHA256),
    (RECONCILIATION_UV_LOCK_SHA256, RECONCILIATION_UV_LOCK_SHA256),
}
_LEGACY_MISSING_RUNTIME_VERSIONS = frozenset({"dbt-duckdb", "httpcore"})
SUPPORTED_RUNTIME_VERSIONS = {
    "duckdb": "2.0.0",
    "dlt": "1.31.0",
    "dbt-core": "1.12.5",
    "dbt-duckdb": "1.11.1",
    "pyarrow": "26.0.0",
    "httpx": "0.28.1",
    "httpcore": "1.0.9",
}
RUN_LIMITS: dict[str, int | float | str | bool] = {
    "session_seconds": 48 * 3600,
    "workflow_seconds": 46 * 3600,
    "active_cutoff_seconds": 47 * 3600 + 50 * 60,
    "attempts": 850_000,
    "downloaded_bytes": 256 * GIB,
    "aggregate_bytes": 400 * GIB,
    "root_bytes": 350 * GIB,
    "temporary_bytes": 64 * GIB,
    "database_bytes": 64 * GIB,
    "response_bytes": 384 * MIB,
    "decoded_response_bytes": 768 * MIB,
    "minimum_free_bytes": 150 * GIB,
    "storage_sample_seconds": 1,
    "storage_database_admission_bytes": 56 * GIB,
    "storage_temporary_admission_bytes": 56 * GIB,
    "storage_database_stop_bytes": 56 * GIB,
    "storage_temporary_stop_bytes": 56 * GIB,
    "storage_database_stop_inclusive": False,
    "storage_temporary_stop_inclusive": False,
    "storage_inventory_entries": 65536,
    "storage_release_directories": 4096,
    "peak_rss_bytes": 24 * GIB,
    "peak_rss_seconds": 60,
    "memory_pressure_warn_seconds": 60,
    "memory_pressure_max_gap_seconds": 22.5,
    "sample_seconds": 15,
    "terminate_seconds": 5,
    "rate": 5,
    "compression": "gzip",
    "rate_policy": "decrease-on-429",
}
RESOURCE_PROFILES = {
    "default": {},
    "expanded": {
        "database_bytes": 128 * GIB,
        "temporary_bytes": 192 * GIB,
        "storage_database_admission_bytes": 96 * GIB,
        "storage_temporary_admission_bytes": 144 * GIB,
        "storage_database_stop_bytes": 112 * GIB,
        "storage_temporary_stop_bytes": 168 * GIB,
        "storage_database_stop_inclusive": True,
        "storage_temporary_stop_inclusive": True,
    },
}
_ACTIVE_LIMITS: contextvars.ContextVar[dict[str, int | float | str | bool] | None] = (
    contextvars.ContextVar("production_run_limits", default=None)
)
MAX_LEGACY_OFFLINE_HISTORICAL_SECONDS = 4 * 3600
MAX_OFFLINE_HISTORICAL_SECONDS = 24 * 3600
CAPTURE_ID = re.compile(r"[0-9a-f]{32}\Z")
REVISION = re.compile(r"[0-9a-f]{40}\Z")
REPORT_NAME = "session.json"
OWNER_MARKER = ".production-root.json"
RESOURCE_SAMPLE_LIMIT = 256
RESOURCE_SAMPLE_PEAK_FIELDS = (
    ("peak_child_process_group_rss_bytes", "child_process_group_rss_bytes"),
    (
        "peak_child_process_group_physical_footprint_bytes",
        "child_process_group_physical_footprint_bytes",
    ),
    ("peak_supervisor_physical_footprint_bytes", "supervisor_physical_footprint_bytes"),
    ("peak_system_compressed_bytes", "system_compressed_bytes"),
    ("peak_system_compressor_occupied_bytes", "system_compressor_occupied_bytes"),
)
RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS = (
    ("invocation_peak_child_process_group_rss_bytes", "child_process_group_rss_bytes"),
    (
        "invocation_peak_child_process_group_physical_footprint_bytes",
        "child_process_group_physical_footprint_bytes",
    ),
    (
        "invocation_peak_supervisor_physical_footprint_bytes",
        "supervisor_physical_footprint_bytes",
    ),
)
DEADLINE_STOP_REASONS = frozenset(
    {
        "session or workflow deadline reached termination reserve",
        "insufficient time remains to start and reap a child",
        "insufficient wall-clock time remains to start and reap a child",
    }
)
NO_CHILD_START_REASONS = frozenset(
    {
        "insufficient time remains to start and reap a child",
        "insufficient wall-clock time remains to start and reap a child",
        "memory metrics unavailable before supervised work",
        "memory pressure was not normal before supervised work",
    }
)
RECOVERABLE_ACCOUNTING_STOP_REASONS = frozenset(
    {
        "refresh failed with unknown child accounting",
        "resumed child did not report exact invocation accounting",
        "insufficient time remains to start and reap a child",
        "insufficient wall-clock time remains to start and reap a child",
        "resource accounting failed: Command '['ps', '-axo', 'pgid=,rss=']' timed out after 5.0 seconds",
        "resource accounting failed: process-group RSS sampling timed out",
        "process-group RSS exceeded 24 GiB for 60 seconds",
        "system compressed memory reached the conservative stop threshold",
        "system memory pressure reached critical",
        "system memory pressure remained warning for 60 seconds",
        "resource accounting failed: memory snapshot unavailable",
        "memory metrics unavailable before supervised work",
        "memory pressure was not normal before supervised work",
        "SSD free space fell below 150 GiB",
    }
)
_STOP_IN_PROGRESS = False
_SPAWNING_CHILD = False
_PENDING_STOP_SIGNAL: int | None = None
_TERMINATION_ATTEMPTED: set[int] = set()
_ACTIVE_OFFLINE_HISTORICAL_WINDOW: dict[str, Any] | None = None


class RunnerError(RuntimeError):
    pass


class BudgetStop(RunnerError):
    """An approved bounded allowance is exhausted without an invariant failure."""


class RunnerInterrupted(RunnerError):
    """The operator requested a clean stop while a supervised stage was running."""


def _effective_limits() -> dict[str, int | float | str | bool]:
    return _ACTIVE_LIMITS.get() or RUN_LIMITS


def _limits_for_profile(profile: str) -> dict[str, int | float | str | bool]:
    if not isinstance(profile, str) or profile not in RESOURCE_PROFILES:
        raise RunnerError("resource profile is invalid")
    return {**RUN_LIMITS, **RESOURCE_PROFILES[profile]}


def _same_typed_json(value: Any, expected: Any) -> bool:
    if type(value) is not type(expected):
        return False
    if isinstance(expected, dict):
        return value.keys() == expected.keys() and all(
            _same_typed_json(value[key], expected[key]) for key in expected
        )
    if isinstance(expected, list):
        return len(value) == len(expected) and all(
            _same_typed_json(item, expected_item)
            for item, expected_item in zip(value, expected, strict=True)
        )
    return value == expected


def _sealed_profile_limits(profile: str, saved: Any) -> dict[str, Any]:
    expected = _limits_for_profile(profile)
    if not _same_typed_json(saved, expected):
        raise RunnerError("session resource limits do not match their sealed profile")
    return expected


def _limits_from_report(
    report: dict[str, Any], profile: str
) -> dict[str, int | float | str | bool]:
    if report.get("resource_profile_history") is None:
        if profile != "default":
            raise RunnerError("sealed resource profile history is missing")
        # Legacy reports did not select limits; retain the runner's fixed policy.
        return _limits_for_profile("default")
    _bind_report_resource_profile(report, profile)
    return _sealed_profile_limits(profile, report.get("limits"))


def _resource_policy_record(profile: str, limits: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": 1,
        "profile": profile,
        "enforcement": "one-second monitoring with cooperative stop; no hard quota",
        "overshoot_acknowledged": True,
        "database_bytes": {
            "limit": int(limits["database_bytes"]),
            "admission": int(limits["storage_database_admission_bytes"]),
            "stop": int(limits["storage_database_stop_bytes"]),
            "stop_inclusive": bool(limits["storage_database_stop_inclusive"]),
        },
        "temporary_bytes": {
            "limit": int(limits["temporary_bytes"]),
            "admission": int(limits["storage_temporary_admission_bytes"]),
            "stop": int(limits["storage_temporary_stop_bytes"]),
            "stop_inclusive": bool(limits["storage_temporary_stop_inclusive"]),
        },
    }


def _resource_profile_scope(function: Any) -> Any:
    """Keep selected limits local to one production invocation."""

    @wraps(function)
    def scoped(*args: Any, **kwargs: Any) -> Any:
        token = _ACTIVE_LIMITS.set(_limits_for_profile(kwargs.get("resource_profile") or "default"))
        try:
            return function(*args, **kwargs)
        finally:
            _ACTIVE_LIMITS.reset(token)

    return scoped


def validate_mode(
    full: bool,
    capture_id: str | None,
    activate_current: bool,
    historical: bool = False,
    offline_duration_seconds: float | None = None,
    renew_offline_window: bool = False,
    supersede_offline_window: bool = False,
    resource_profile: str | None = None,
) -> str:
    duration = _validate_offline_duration_seconds(offline_duration_seconds)
    if sum((full, capture_id is not None, activate_current)) != 1:
        raise RunnerError("choose exactly one of --full, --capture-id or --activate-current")
    if historical and (full or (capture_id is None and not activate_current)):
        raise RunnerError("--historical requires --capture-id or --activate-current")
    if resource_profile is not None and (
        not isinstance(resource_profile, str) or resource_profile not in RESOURCE_PROFILES
    ):
        raise RunnerError("--historical-resource-profile must be default or expanded")
    if resource_profile is not None and (not historical or capture_id is None):
        raise RunnerError("--historical-resource-profile requires --historical --capture-id")
    if capture_id is not None and (
        not isinstance(capture_id, str) or not CAPTURE_ID.fullmatch(capture_id)
    ):
        raise RunnerError("--capture-id must be a 32-character lowercase hex ID")
    if historical:
        mode = "historical" if capture_id is not None else "activate-historical"
    else:
        mode = "full" if full else "resume" if capture_id else "activate-current"
    if duration is not None and mode != "historical":
        raise RunnerError("--offline-duration-seconds requires --historical --capture-id")
    if renew_offline_window and (mode != "historical" or duration is None):
        raise RunnerError(
            "--renew-offline-window requires --historical --capture-id and "
            "--offline-duration-seconds"
        )
    if supersede_offline_window and (mode != "historical" or duration is None):
        raise RunnerError(
            "--supersede-offline-window requires --historical --capture-id and "
            "--offline-duration-seconds"
        )
    return mode


def _bind_report_resource_profile(
    report: dict[str, Any], profile: str, *, transition: bool = False
) -> None:
    history = report.get("resource_profile_history")
    legacy_history = history is None
    if history is None:
        if report.get("resource_profile", "default") != "default":
            raise RunnerError("session resource profile history is missing")
        if not transition:
            if profile != "default":
                raise RunnerError("legacy session requires an explicit resource-profile transition")
            report["resource_profile"] = "default"
            return
        prior_limits = _limits_from_report(report, "default")
        history = [
            {
                "profile": "default",
                "limits": prior_limits,
                "policy": _resource_policy_record("default", prior_limits),
                "selected_at": report.get("started_at"),
                "reason": "legacy session policy",
            }
        ]
    if not isinstance(history, list):
        raise RunnerError("session resource profile history is invalid")
    for entry in history:
        if (
            not isinstance(entry, dict)
            or set(entry) != {"profile", "limits", "policy", "selected_at", "reason"}
            or (entry.get("selected_at") is not None and not isinstance(entry["selected_at"], str))
            or not isinstance(entry.get("reason"), str)
        ):
            raise RunnerError("session resource profile history is invalid")
        entry_profile = entry.get("profile")
        if not isinstance(entry_profile, str) or entry_profile not in RESOURCE_PROFILES:
            raise RunnerError("session resource profile history is invalid")
        entry_limits = _sealed_profile_limits(entry_profile, entry.get("limits"))
        expected_policy = _resource_policy_record(entry_profile, entry_limits)
        if not _same_typed_json(entry.get("policy"), expected_policy):
            raise RunnerError("session resource profile history is invalid")
    current_profile = report.get("resource_profile", "default")
    if (
        not isinstance(current_profile, str)
        or current_profile not in RESOURCE_PROFILES
        or not history
        or (not legacy_history and "resource_profile" not in report)
        or history[-1]["profile"] != current_profile
    ):
        raise RunnerError("session resource profile history does not match its sealed profile")
    if not legacy_history:
        sealed_limits = _sealed_profile_limits(current_profile, report.get("limits"))
        if not _same_typed_json(history[-1]["limits"], sealed_limits):
            raise RunnerError("session resource limits differ from their sealed history")
        expected_policy = _resource_policy_record(current_profile, sealed_limits)
        if not _same_typed_json(report.get("effective_resource_policy"), expected_policy):
            raise RunnerError("session effective resource policy differs from its sealed profile")
    if transition:
        limits = _limits_for_profile(profile)
        history.append(
            {
                "profile": profile,
                "limits": limits,
                "policy": _resource_policy_record(profile, limits),
                "selected_at": _utc(),
                "reason": "explicit offline historical resource-profile transition",
            }
        )
    elif not legacy_history:
        return
    report["resource_profile"] = profile
    report["resource_profile_history"] = history
    report["limits"] = limits = _limits_for_profile(profile)
    report["effective_resource_policy"] = _resource_policy_record(profile, limits)


def _session_resource_profile(
    report_path: Path,
    expected_profile: str | None = None,
    capture_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    report = _strict_json(report_path)
    if report is None or (capture_id is not None and report.get("capture_id") != capture_id):
        raise RunnerError("hidden worker session report does not match its capture")
    profile = report.get("resource_profile", "default")
    if (
        not isinstance(profile, str)
        or profile not in RESOURCE_PROFILES
        or (expected_profile is not None and expected_profile != profile)
    ):
        raise RunnerError("hidden worker resource profile does not match its session report")
    history = report.get("resource_profile_history")
    if history is None and profile == "default":
        return profile, report
    if not isinstance(history, list) or not history:
        raise RunnerError("hidden worker session resource profile history is invalid")
    try:
        _bind_report_resource_profile(report, profile)
    except RunnerError as exc:
        raise RunnerError("hidden worker session resource limits do not match its profile") from exc
    return profile, report


def _validate_offline_duration_seconds(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerError("--offline-duration-seconds must be a finite positive number")
    try:
        duration = float(value)
    except OverflowError as exc:
        raise RunnerError("--offline-duration-seconds must be a finite positive number") from exc
    if not math.isfinite(duration) or duration <= 0:
        raise RunnerError("--offline-duration-seconds must be a finite positive number")
    if duration > MAX_OFFLINE_HISTORICAL_SECONDS:
        raise RunnerError("--offline-duration-seconds cannot exceed 24 hours")
    return duration


def remaining_allowances(
    accounting: dict[str, Any],
    started_at: float,
    now: float | None = None,
    *,
    external_reserved_bytes: int = 0,
    started_epoch: float | None = None,
    wall_now: float | None = None,
) -> dict[str, int | float | str]:
    """Calculate a new child's ceilings from verified receipts and session time."""
    bounded_recovery = (
        accounting.get("accounting_basis") == "reserved-upper-bound"
        and accounting.get("accounting_bound_verified") is True
        and accounting.get("accounting_complete") is False
    )
    if accounting.get("accounting_complete") is not True and not bounded_recovery:
        raise RunnerError("prior acquisition accounting is incomplete; networking is blocked")
    byte_field = "downloaded_bytes_upper_bound" if bounded_recovery else "downloaded_bytes"
    for name in ("attempts", byte_field):
        value = accounting.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RunnerError("prior acquisition accounting is invalid")
    if (
        isinstance(external_reserved_bytes, bool)
        or not isinstance(external_reserved_bytes, int)
        or external_reserved_bytes < 0
    ):
        raise RunnerError("external storage accounting is invalid")
    elapsed = (time.monotonic() if now is None else now) - started_at
    wall_elapsed = (
        max(0.0, (time.time() if wall_now is None else wall_now) - started_epoch)
        if started_epoch is not None
        else elapsed
    )
    workflow_remaining = min(
        float(_effective_limits()["workflow_seconds"]) - elapsed,
        float(_effective_limits()["active_cutoff_seconds"]) - elapsed,
        float(_effective_limits()["session_seconds"]) - elapsed,
        float(_effective_limits()["workflow_seconds"]) - wall_elapsed,
        float(_effective_limits()["active_cutoff_seconds"]) - wall_elapsed,
        float(_effective_limits()["session_seconds"]) - wall_elapsed,
    )
    max_root = min(
        int(_effective_limits()["root_bytes"]),
        int(_effective_limits()["aggregate_bytes"]) - external_reserved_bytes,
    )
    attempts = int(_effective_limits()["attempts"]) - accounting["attempts"]
    downloads = int(_effective_limits()["downloaded_bytes"]) - accounting[byte_field]
    if min(workflow_remaining, attempts, downloads, max_root) <= 0:
        raise BudgetStop("approved session allowance is exhausted")
    return {
        "max-requests": attempts,
        "max-download-bytes": downloads,
        "max-root-bytes": max_root,
        "max-temp-bytes": int(_effective_limits()["temporary_bytes"]),
        "max-database-bytes": int(_effective_limits()["database_bytes"]),
        "max-response-bytes": min(int(_effective_limits()["response_bytes"]), downloads),
        "max-decoded-response-bytes": int(_effective_limits()["decoded_response_bytes"]),
        "max-duration-seconds": workflow_remaining,
        "max-rate": float(_effective_limits()["rate"]),
        "compression": str(_effective_limits()["compression"]),
        "rate-policy": str(_effective_limits()["rate_policy"]),
    }


def _time_budget_started(report: dict[str, Any], clock: str) -> float:
    key = f"time_budget_started_{clock}"
    value = report.get(key, report.get(f"started_{clock}"))
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerError("session time budget start is invalid")
    try:
        value = float(value)
    except OverflowError as exc:
        raise RunnerError("session time budget start is invalid") from exc
    if not math.isfinite(value):
        raise RunnerError("session time budget start is invalid")
    return value


def _time_budget_deadline(report: dict[str, Any], clock: str) -> float:
    start = _time_budget_started(report, clock)
    key = f"deadline_{clock}"
    value = report.get(key, start + float(_effective_limits()["session_seconds"]))
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerError("session time budget deadline is invalid")
    try:
        value = float(value)
    except OverflowError as exc:
        raise RunnerError("session time budget deadline is invalid") from exc
    if not math.isfinite(value):
        raise RunnerError("session time budget deadline is invalid")
    derived = start + float(_effective_limits()["session_seconds"])
    if not math.isfinite(derived):
        raise RunnerError("session time budget deadline is invalid")
    return min(value, derived)


def rss_limit_exceeded(
    samples: list[tuple[float, int]],
    limit: int | None = None,
    sustained: float | None = None,
) -> bool:
    """Detect uninterrupted above-limit RSS over promptly sampled timestamps."""
    limit = int(_effective_limits()["peak_rss_bytes"]) if limit is None else limit
    sustained = float(_effective_limits()["peak_rss_seconds"]) if sustained is None else sustained
    above_since: float | None = None
    previous: float | None = None
    for instant, rss in samples:
        if previous is not None and instant - previous > 1.5 * float(
            _effective_limits()["sample_seconds"]
        ):
            above_since = None
        if rss > limit:
            if above_since is None:
                above_since = instant
            if instant - above_since >= sustained:
                return True
        else:
            above_since = None
        previous = instant
    return False


def _utc() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@contextlib.contextmanager
def _graceful_stop_handlers():
    """Convert public supervisor signals into exceptions that reap its child group."""
    signals = [signal.SIGTERM, signal.SIGHUP, signal.SIGINT]
    global _PENDING_STOP_SIGNAL, _SPAWNING_CHILD, _STOP_IN_PROGRESS
    previous = {item: signal.getsignal(item) for item in signals}
    _STOP_IN_PROGRESS = False
    _SPAWNING_CHILD = False
    _PENDING_STOP_SIGNAL = None

    def stop(signum: int, _frame: Any) -> None:
        global _PENDING_STOP_SIGNAL, _STOP_IN_PROGRESS
        if _STOP_IN_PROGRESS:
            return
        if _SPAWNING_CHILD:
            _PENDING_STOP_SIGNAL = signum
            _STOP_IN_PROGRESS = True
            return
        _STOP_IN_PROGRESS = True
        raise RunnerInterrupted(f"received {signal.Signals(signum).name}; stopping supervised work")

    try:
        for item in signals:
            signal.signal(item, stop)
        yield
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)
        _STOP_IN_PROGRESS = False
        _SPAWNING_CHILD = False
        _PENDING_STOP_SIGNAL = None


def _strict_json(path: Path, limit: int = 16 * MIB) -> dict[str, Any] | None:
    from kalshi_events.common import CatalogueError, regular_bytes

    def reject(value: str) -> None:
        raise ValueError(value)

    try:
        raw = regular_bytes(path, limit)
        value = json.loads(raw.decode("utf-8"), parse_constant=reject)
    except (CatalogueError, OSError, UnicodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _atomic_report(path: Path, report: dict[str, Any]) -> None:
    from kalshi_events.common import fsync_directory, safe_path

    path = safe_path(path)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    data = (json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    with temporary.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fsync_directory(path.parent)


def _effective_memory_policy() -> dict[str, Any]:
    """Describe the current closed memory policy without rewriting legacy limits."""
    return {
        "version": 2,
        "sample_interval_seconds": float(_effective_limits()["sample_seconds"]),
        "sample_timeout_seconds": 5,
        "process_group_rss_limit_bytes": int(_effective_limits()["peak_rss_bytes"]),
        "process_group_rss_continuous_seconds": int(_effective_limits()["peak_rss_seconds"]),
        "process_group_rss_limit_is_strict": True,
        "warning_pressure_continuous_seconds": int(
            _effective_limits()["memory_pressure_warn_seconds"]
        ),
        "pressure_sample_max_gap_seconds": float(
            _effective_limits()["memory_pressure_max_gap_seconds"]
        ),
        "preflight_pressure": "normal",
        "warn_pressure_resets_on_normal_or_sample_gap": True,
        "critical_pressure_stops_on_first_sample": True,
        "physical_footprint_is_telemetry_only": True,
    }


def _resource_sample_archive_path(report_path: Path) -> Path:
    return report_path.with_name(f"{report_path.stem}.before-resource-ring{report_path.suffix}")


def _validate_resource_archive(archive: dict[str, Any], report: dict[str, Any]) -> None:
    resource_samples = archive.get("resource_samples")
    preflight_samples = archive.get("memory_preflight_samples")
    if not (
        isinstance(resource_samples, list)
        and len(resource_samples) >= RESOURCE_SAMPLE_LIMIT
        or isinstance(preflight_samples, list)
        and len(preflight_samples) >= RESOURCE_SAMPLE_LIMIT
    ):
        raise RunnerError("resource sample archive is invalid")
    for key in ("session_id", "capture_id"):
        expected = report.get(key)
        if expected is not None and archive.get(key) != expected:
            raise RunnerError("resource sample archive belongs to another session")


def _archive_resource_samples_once(report_path: Path, report: dict[str, Any]) -> None:
    archive_path = _resource_sample_archive_path(report_path)
    archive_name = archive_path.name
    recorded = report.get("resource_samples_archive")
    if recorded is not None:
        if recorded != archive_name:
            raise RunnerError("resource sample archive path is invalid")
        if archive_path.is_symlink() or not archive_path.is_file():
            raise RunnerError("recorded resource sample archive is missing or unsafe")
        return
    if archive_path.exists():
        archive = _strict_json(archive_path, limit=64 * MIB)
        if archive is None:
            raise RunnerError("existing resource sample archive is unreadable")
        _validate_resource_archive(archive, report)
    else:
        _atomic_report(archive_path, report)
    report["resource_samples_archive"] = archive_name


def _resource_sample_state(
    report: dict[str, Any], report_path: Path
) -> tuple[list[dict[str, Any]], int, int]:
    samples = report.setdefault("resource_samples", [])
    if not isinstance(samples, list) or any(not isinstance(row, dict) for row in samples):
        raise RunnerError("resource sample history is invalid")
    total = report.get("resource_samples_total")
    dropped = report.get("resource_samples_dropped")
    if total is None and dropped is None:
        total, dropped = len(samples), 0
    elif (
        isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
        or isinstance(dropped, bool)
        or not isinstance(dropped, int)
        or dropped < 0
        or total - dropped != len(samples)
    ):
        raise RunnerError("resource sample counters are invalid")
    for peak_field, sample_field in RESOURCE_SAMPLE_PEAK_FIELDS:
        peak = report.get(peak_field)
        if peak is None:
            peak = 0
        if isinstance(peak, bool) or not isinstance(peak, int) or peak < 0:
            raise RunnerError(f"{peak_field} is invalid")
        for row in samples:
            value = row.get(sample_field)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RunnerError(f"resource sample field {sample_field} is invalid")
            peak = max(peak, value)
        report[peak_field] = peak
    if len(samples) > RESOURCE_SAMPLE_LIMIT:
        report["resource_samples_total"] = total
        report["resource_samples_dropped"] = dropped
        _archive_resource_samples_once(report_path, report)
        trim = len(samples) - RESOURCE_SAMPLE_LIMIT
        del samples[:trim]
        dropped += trim
    for peak_field, _sample_field in RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS:
        peak = report.get(peak_field, 0)
        if isinstance(peak, bool) or not isinstance(peak, int) or peak < 0:
            raise RunnerError(f"{peak_field} is invalid")
        report[peak_field] = peak
    report["resource_samples_total"] = total
    report["resource_samples_dropped"] = dropped
    return samples, total, dropped


def _append_resource_sample(
    report: dict[str, Any], report_path: Path, sample: dict[str, Any]
) -> None:
    samples, total, dropped = _resource_sample_state(report, report_path)
    if len(samples) >= RESOURCE_SAMPLE_LIMIT:
        _archive_resource_samples_once(report_path, report)
    for peak_field, sample_field in RESOURCE_SAMPLE_PEAK_FIELDS:
        value = sample.get(sample_field)
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RunnerError(f"resource sample field {sample_field} is invalid")
            report[peak_field] = max(report[peak_field], value)
    for peak_field, sample_field in RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS:
        value = sample.get(sample_field)
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RunnerError(f"resource sample field {sample_field} is invalid")
            report[peak_field] = max(report[peak_field], value)
    sample["memory_policy_version"] = 2
    samples.append(sample)
    total += 1
    if len(samples) > RESOURCE_SAMPLE_LIMIT:
        trim = len(samples) - RESOURCE_SAMPLE_LIMIT
        del samples[:trim]
        dropped += trim
    report["resource_samples_total"] = total
    report["resource_samples_dropped"] = dropped


def _memory_preflight_sample_state(
    report: dict[str, Any], report_path: Path
) -> tuple[list[dict[str, Any]], int, int]:
    samples = report.setdefault("memory_preflight_samples", [])
    if not isinstance(samples, list) or any(not isinstance(row, dict) for row in samples):
        raise RunnerError("memory preflight sample history is invalid")
    total = report.get("memory_preflight_samples_total")
    dropped = report.get("memory_preflight_samples_dropped")
    if total is None and dropped is None:
        total, dropped = len(samples), 0
    elif (
        isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
        or isinstance(dropped, bool)
        or not isinstance(dropped, int)
        or dropped < 0
        or total - dropped != len(samples)
    ):
        raise RunnerError("memory preflight sample counters are invalid")
    if len(samples) > RESOURCE_SAMPLE_LIMIT:
        report["memory_preflight_samples_total"] = total
        report["memory_preflight_samples_dropped"] = dropped
        _archive_resource_samples_once(report_path, report)
        trim = len(samples) - RESOURCE_SAMPLE_LIMIT
        del samples[:trim]
        dropped += trim
    report["memory_preflight_samples_total"] = total
    report["memory_preflight_samples_dropped"] = dropped
    return samples, total, dropped


def _append_memory_preflight_sample(
    report: dict[str, Any], report_path: Path, sample: dict[str, Any]
) -> None:
    samples, total, dropped = _memory_preflight_sample_state(report, report_path)
    if len(samples) >= RESOURCE_SAMPLE_LIMIT:
        _archive_resource_samples_once(report_path, report)
    samples.append({"at": _utc(), **sample, "memory_policy_version": 2})
    total += 1
    if len(samples) > RESOURCE_SAMPLE_LIMIT:
        trim = len(samples) - RESOURCE_SAMPLE_LIMIT
        del samples[:trim]
        dropped += trim
    report["memory_preflight_samples_total"] = total
    report["memory_preflight_samples_dropped"] = dropped


def _external_path(path: Path) -> Path:
    from kalshi_events.common import CatalogueError
    from kalshi_events.paths import PathPolicyError, persistent_path, project_root

    try:
        checkout = project_root(code_root=SOURCE_ROOT)
        result = persistent_path(path, checkout=checkout, label="production directory")
    except (CatalogueError, PathPolicyError) as exc:
        raise RunnerError(str(exc)) from exc
    return result


def prepare_directory(directory: Path, mode: str) -> Path:
    directory = _external_path(directory)
    root = directory / "operator"
    if mode == "full":
        if directory.exists() and any(directory.iterdir()):
            raise RunnerError("--full requires a new, empty production directory")
        if not directory.exists():
            directory.mkdir(parents=True)
        if root.exists() or (directory / "production.duckdb").exists():
            raise RunnerError("--full cannot overwrite an existing operator root or production DB")
        runs = directory / "runs"
        if runs.is_symlink() or (runs.exists() and not runs.is_dir()):
            raise RunnerError("production run history path is unsafe")
        runs.mkdir(exist_ok=True)
    elif not root.is_dir() or not (root / "root.json").is_file():
        raise RunnerError("resume and activation require an existing production operator root")
    if root.is_symlink() or root.resolve() != root.absolute():
        raise RunnerError("operator root path is unsafe")
    runs = directory / "runs"
    if mode != "full" and (runs.is_symlink() or not runs.is_dir()):
        raise RunnerError("production run history directory is unsafe or missing")
    if (directory / "production.duckdb").is_symlink():
        raise RunnerError("production database path cannot be a symlink")
    marker = directory / OWNER_MARKER
    if mode == "full":
        if marker.exists() or marker.is_symlink():
            raise RunnerError("--full requires a new, unmanaged production directory")
    elif marker.is_symlink() or not marker.is_file():
        raise RunnerError("resume and activation require a managed production directory")
    return directory


def _owner_root_id(directory: Path) -> str:
    marker_path = directory / OWNER_MARKER
    if marker_path.is_symlink():
        raise RunnerError("production ownership marker cannot be a symlink")
    marker = _strict_json(marker_path)
    if (
        marker is None
        or set(marker) != {"version", "root_id"}
        or isinstance(marker.get("version"), bool)
        or marker.get("version") != 1
        or not isinstance(marker.get("root_id"), str)
        or not CAPTURE_ID.fullmatch(marker["root_id"])
    ):
        raise RunnerError("production ownership marker is invalid")
    return marker["root_id"]


def _bind_owner(directory: Path, root_id: str, mode: str) -> None:
    marker_path = directory / OWNER_MARKER
    if marker_path.exists():
        if _owner_root_id(directory) != root_id:
            raise RunnerError("operator logical root differs from production ownership marker")
        return
    if mode != "full" or marker_path.is_symlink():
        raise RunnerError("production ownership marker is missing")
    temporary = directory / f".{OWNER_MARKER}.{uuid.uuid4().hex}.tmp"
    _atomic_report(temporary, {"version": 1, "root_id": root_id})
    try:
        os.link(temporary, marker_path, follow_symlinks=False)
        from kalshi_events.common import fsync_directory

        fsync_directory(directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _revision() -> str:
    supplied = os.environ.get("KALSHI_EVENTS_REVISION")
    if supplied:
        if not REVISION.fullmatch(supplied):
            raise RunnerError("KALSHI_EVENTS_REVISION must be a full lowercase commit ID")
        return supplied
    try:
        result = subprocess.run(
            ["git", "-C", str(SOURCE_ROOT), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        revision = result.stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(SOURCE_ROOT), "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError(
            "set KALSHI_EVENTS_REVISION when running from a frozen source archive"
        ) from exc
    if not REVISION.fullmatch(revision) or dirty:
        raise RunnerError("production runner requires a clean frozen revision")
    return revision


def _package_versions() -> dict[str, str | None]:
    from importlib.metadata import PackageNotFoundError, version

    result = {}
    for package in (
        "oddsfox-kalshi-events",
        "duckdb",
        "dlt",
        "dbt-core",
        "dbt-duckdb",
        "pyarrow",
        "httpx",
        "httpcore",
    ):
        try:
            result[package] = version(package)
        except PackageNotFoundError:
            result[package] = None
    return result


def _validate_runtime_versions(versions: dict[str, str | None]) -> None:
    unsupported = [
        f"{package}={versions.get(package)!r} (requires {expected})"
        for package, expected in SUPPORTED_RUNTIME_VERSIONS.items()
        if versions.get(package) != expected
    ]
    if unsupported:
        raise RunnerError("unsupported installed package stack: " + ", ".join(unsupported))


def _validate_reconciliation_versions(versions: Any, lock_sha256: str, *, saved: bool) -> bool:
    expected = (
        LEGACY_RECONCILIATION_RUNTIME_VERSIONS
        if lock_sha256 == LEGACY_RECONCILIATION_UV_LOCK_SHA256
        else SUPPORTED_RUNTIME_VERSIONS
        if lock_sha256 == RECONCILIATION_UV_LOCK_SHA256
        else None
    )
    if not isinstance(versions, dict) or expected is None:
        return False
    for package, version in expected.items():
        if (
            saved
            and lock_sha256 == LEGACY_RECONCILIATION_UV_LOCK_SHA256
            and package in _LEGACY_MISSING_RUNTIME_VERSIONS
            and package not in versions
        ):
            continue
        if versions.get(package) != version:
            return False
    return True


def _supported_reconciliation_lock_transition(saved_lock: str, current_lock: str) -> bool:
    return (saved_lock, current_lock) in _RECONCILIATION_LOCK_TRANSITIONS


def _transport_source_sha256() -> str:
    try:
        from kalshi_events import transport

        return hashlib.sha256(Path(transport.__file__).resolve().read_bytes()).hexdigest()
    except (ImportError, OSError, AttributeError, TypeError) as exc:
        raise RunnerError("current HTTP transport source cannot be verified") from exc


def _system_info(directory: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(directory)
    stat = directory.stat()
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "filesystem_device": stat.st_dev,
        "filesystem_free_bytes": usage.free,
        "filesystem_total_bytes": usage.total,
        "python": sys.version,
        "package_versions": _package_versions(),
        "package_origin": str(Path(sys.modules["kalshi_events"].__file__).resolve()),
        "uv_lock_sha256": hashlib.sha256((SOURCE_ROOT / "uv.lock").read_bytes()).hexdigest(),
        "transport_sha256": _transport_source_sha256(),
    }


def _boot_stamp() -> str:
    try:
        result = subprocess.run(
            ["sysctl", "-n", "kern.boottime"], check=True, capture_output=True, text=True, timeout=3
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError("cannot verify Mac boot identity; refusing a resumable session") from exc
    return result.stdout.strip()


def _volume_uuid(path: Path) -> str:
    resolved = path.resolve(strict=True)
    mount = next(
        (parent for parent in (resolved, *resolved.parents) if os.path.ismount(parent)), None
    )
    if mount is None:
        raise RunnerError("production path is not on a mounted filesystem")
    try:
        result = subprocess.run(
            ["diskutil", "info", "-plist", str(mount)],
            check=True,
            capture_output=True,
            timeout=5,
        )
        if len(result.stdout) > 1024 * 1024:
            raise RunnerError("filesystem volume information is oversized")
        info = plistlib.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, plistlib.InvalidFileException) as exc:
        raise RunnerError("cannot verify external SSD volume identity") from exc
    volume_uuid = info.get("VolumeUUID") if isinstance(info, dict) else None
    mountpoint = info.get("MountPoint") if isinstance(info, dict) else None
    if not isinstance(volume_uuid, str) or not re.fullmatch(
        r"[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}", volume_uuid
    ):
        raise RunnerError("filesystem volume has no valid UUID")
    if not isinstance(mountpoint, str):
        raise RunnerError("filesystem volume has no valid mount point")
    mounted = Path(mountpoint).resolve(strict=True)
    if mounted != mount:
        raise RunnerError("diskutil returned a different filesystem mount point")
    try:
        resolved.relative_to(mounted)
    except ValueError as exc:
        raise RunnerError("production directory is outside the selected volume mount") from exc
    if not resolved.is_dir() or resolved.stat().st_dev != mounted.stat().st_dev:
        raise RunnerError("production path and volume mount are on different filesystems")
    return volume_uuid.upper()


def _process_group_rss(group: int, timeout: float = 5.0) -> int:
    command = ["ps", "-g", str(group), "-o", "pgid=,rss="]
    deadline = time.monotonic() + timeout
    try:
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout / 2,
            )
        except subprocess.TimeoutExpired as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RunnerError("process-group RSS sampling timed out") from exc
            try:
                result = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=remaining,
                )
            except subprocess.TimeoutExpired as retry_exc:
                raise RunnerError("process-group RSS sampling timed out") from retry_exc
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 1 and not _process_group_exists(group):
            return 0
        raise
    if not isinstance(result.stdout, str):
        raise RunnerError("malformed process-group RSS output")
    lines = result.stdout.splitlines()
    if not lines and not _process_group_exists(group):
        return 0
    total = 0
    for line in lines:
        fields = line.split()
        if len(fields) != 2 or any(not field.isascii() or not field.isdigit() for field in fields):
            raise RunnerError("malformed process-group RSS output")
        if int(fields[0]) != group:
            raise RunnerError("ps returned a process outside the requested group")
        total += int(fields[1]) * 1024
    if not lines:
        raise RunnerError("ps returned no processes for a live process group")
    return total


def _process_group_exists(group: int) -> bool:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        raise
    return True


def _terminate(process: subprocess.Popen[Any]) -> bool:
    global _STOP_IN_PROGRESS
    previous = getattr(process, "_production_termination_outcome", None)
    prior_errors = previous.get("termination_errors", []) if isinstance(previous, dict) else []
    errors: list[str] = list(prior_errors)

    def observe(*, attempted: bool) -> tuple[bool, bool | None]:
        leader_reaped = process.poll() is not None
        try:
            group_absent: bool | None = not _process_group_exists(process.pid)
        except OSError as exc:
            errors.append(f"process-group probe: {exc}")
            group_absent = None
        with contextlib.suppress(AttributeError):
            process._production_termination_outcome = {
                "leader_reaped": leader_reaped,
                "group_absent": group_absent,
                "uncertain": not leader_reaped or group_absent is None,
                "termination_attempted": attempted,
                "termination_errors": list(errors),
            }
        return leader_reaped, group_absent

    if process.pid in _TERMINATION_ATTEMPTED:
        leader_reaped, group_absent = observe(attempted=False)
        reaped = leader_reaped and group_absent is True
        if reaped:
            _TERMINATION_ATTEMPTED.discard(process.pid)
        return reaped
    was_stopping = _STOP_IN_PROGRESS
    _STOP_IN_PROGRESS = True
    try:
        grace_deadline = time.monotonic() + float(_effective_limits()["terminate_seconds"])
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except OSError as exc:
            errors.append(f"SIGTERM: {exc}")
        term_deadline = min(grace_deadline, time.monotonic() + 2)
        while time.monotonic() < term_deadline:
            try:
                group_exists = _process_group_exists(process.pid)
            except OSError as exc:
                errors.append(f"process-group probe: {exc}")
                group_exists = True
            if not group_exists:
                break
            process.poll()
            time.sleep(min(0.05, max(0.0, term_deadline - time.monotonic())))
        try:
            group_exists = _process_group_exists(process.pid)
        except OSError as exc:
            errors.append(f"process-group probe: {exc}")
            group_exists = True
        if group_exists:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError as exc:
                errors.append(f"SIGKILL: {exc}")
        while time.monotonic() < grace_deadline:
            try:
                group_exists = _process_group_exists(process.pid)
            except OSError as exc:
                errors.append(f"process-group probe: {exc}")
                group_exists = True
            if not group_exists:
                break
            process.poll()
            time.sleep(min(0.05, max(0.0, grace_deadline - time.monotonic())))
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, grace_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                _TERMINATION_ATTEMPTED.add(process.pid)
                observe(attempted=True)
                return False
        leader_reaped, group_absent = observe(attempted=True)
        reaped = leader_reaped and group_absent is True
        if not reaped:
            _TERMINATION_ATTEMPTED.add(process.pid)
        else:
            _TERMINATION_ATTEMPTED.discard(process.pid)
        return reaped
    finally:
        _STOP_IN_PROGRESS = was_stopping


def _monitor(
    process: subprocess.Popen[Any],
    directory: Path,
    operator: Path,
    report: dict[str, Any],
    report_path: Path,
    deadline: float,
    *,
    capture_id: str | None = None,
    original_device: int | None = None,
    capture_notification: Path | None = None,
    wall_deadline_epoch: float | None = None,
    storage_state: dict[str, Any] | None = None,
    storage_progress_path: Path | None = None,
    storage_external_paths: dict[str, Path] | None = None,
) -> tuple[int | None, str | None, bool]:
    _resource_sample_state(report, report_path)
    samples: list[tuple[float, int]] = []
    warning_started: float | None = None
    previous_pressure_sample: float | None = None
    next_sample = time.monotonic()
    next_storage_sample = time.monotonic()
    stop_reason = None
    reaped = True
    terminated = False
    termination_status: dict[str, Any] = {}

    def terminate() -> bool:
        nonlocal reaped, terminated
        reaped = _terminate(process)
        saved_status = getattr(process, "_production_termination_outcome", None)
        if isinstance(saved_status, dict):
            termination_status.update(saved_status)
        terminated = True
        return reaped

    while process.poll() is None:
        now = time.monotonic()
        remaining = _child_time_remaining(deadline, wall_deadline_epoch)
        if remaining <= float(_effective_limits()["terminate_seconds"]):
            stop_reason = "session or workflow deadline reached termination reserve"
            terminate()
            break
        if (
            storage_state is not None
            and storage_progress_path is not None
            and storage_external_paths is not None
            and now >= next_storage_sample
        ):
            sample_started = time.monotonic()
            try:
                stop_reason = _sample_live_storage(
                    directory,
                    operator,
                    report,
                    storage_state,
                    storage_progress_path,
                    storage_external_paths,
                )
            except (OSError, RunnerError) as exc:
                stop_reason = f"resource accounting failed: {exc}"
            next_storage_sample = sample_started + float(
                _effective_limits()["storage_sample_seconds"]
            )
            if stop_reason:
                terminate()
                break
        if now >= next_sample:
            try:
                _merge_capture_notification(report, capture_notification)
                free = shutil.disk_usage(directory).free
                if original_device is not None and directory.stat().st_dev != original_device:
                    raise RunnerError("production filesystem mount changed during the run")
                remaining = _child_time_remaining(deadline, wall_deadline_epoch)
                if remaining <= float(_effective_limits()["terminate_seconds"]):
                    stop_reason = "session or workflow deadline reached termination reserve"
                    terminate()
                    break
                timeout = min(
                    5.0,
                    remaining - float(_effective_limits()["terminate_seconds"]),
                )
                if timeout <= 0:
                    stop_reason = "session or workflow deadline reached termination reserve"
                    terminate()
                    break
                memory = _sample_memory(process.pid, timeout=timeout)
                if _child_time_remaining(deadline, wall_deadline_epoch) <= float(
                    _effective_limits()["terminate_seconds"]
                ):
                    stop_reason = "session or workflow deadline reached termination reserve"
                    terminate()
                    break
            except RunnerInterrupted:
                raise
            except (OSError, subprocess.SubprocessError, RunnerError) as exc:
                if _child_time_remaining(deadline, wall_deadline_epoch) <= float(
                    _effective_limits()["terminate_seconds"]
                ):
                    stop_reason = "session or workflow deadline reached termination reserve"
                elif isinstance(exc, RunnerError) and str(exc).startswith("bounded memory sample"):
                    stop_reason = "resource accounting failed: memory snapshot unavailable"
                else:
                    stop_reason = f"resource accounting failed: {exc}"
                terminate()
                break
            observed_at = memory["system_memory_pressure_observed_monotonic"]
            rss = memory["child_process_group_rss_bytes"]
            pressure = memory["system_memory_pressure"]
            samples.append((observed_at, rss))
            samples = samples[-8:]
            if (
                previous_pressure_sample is not None
                and observed_at - previous_pressure_sample
                > float(_effective_limits()["memory_pressure_max_gap_seconds"])
            ):
                warning_started = None
            previous_pressure_sample = observed_at
            if pressure == "normal":
                warning_started = None
            elif pressure == "warn":
                warning_started = observed_at if warning_started is None else warning_started
            _append_resource_sample(
                report,
                report_path,
                {
                    "at": _utc(),
                    "ssd_free_bytes": free,
                    "child_process_group_rss_bytes": rss,
                    "system_compressed_bytes": memory["system_compressed_bytes"],
                    "system_compressor_occupied_bytes": memory["system_compressor_occupied_bytes"],
                    "system_memory_pressure": pressure,
                    "system_memory_pressure_observed_monotonic": observed_at,
                    "child_process_group_physical_footprint_bytes": memory[
                        "child_process_group_physical_footprint_bytes"
                    ],
                    "supervisor_physical_footprint_bytes": memory[
                        "supervisor_physical_footprint_bytes"
                    ],
                },
            )
            report["last_observed_epoch"] = time.time()
            if free < int(_effective_limits()["minimum_free_bytes"]):
                stop_reason = "SSD free space fell below 150 GiB"
            elif rss_limit_exceeded(samples):
                stop_reason = "process-group RSS exceeded 24 GiB for 60 seconds"
            elif pressure == "critical":
                stop_reason = "system memory pressure reached critical"
            elif (
                pressure == "warn"
                and warning_started is not None
                and observed_at - warning_started
                >= float(_effective_limits()["memory_pressure_warn_seconds"])
            ):
                stop_reason = "system memory pressure remained warning for 60 seconds"
            if stop_reason:
                terminate()
                break
            report["last_observed_at"] = _utc()
            _atomic_report(report_path, report)
            next_sample = now + float(_effective_limits()["sample_seconds"])
        remaining = _child_time_remaining(deadline, wall_deadline_epoch)
        time.sleep(min(1.0, max(0.0, remaining - float(_effective_limits()["terminate_seconds"]))))
    if (
        storage_state is not None
        and storage_progress_path is not None
        and storage_external_paths is not None
    ):
        try:
            storage_reason = _sample_live_storage(
                directory,
                operator,
                report,
                storage_state,
                storage_progress_path,
                storage_external_paths,
            )
            if storage_reason:
                stop_reason = stop_reason or storage_reason
        except (OSError, RunnerError) as exc:
            stop_reason = stop_reason or f"resource accounting failed: {exc}"
    try:
        group_absent_before_cleanup: bool | None = not _process_group_exists(process.pid)
    except OSError as exc:
        termination_status.setdefault("termination_errors", []).append(
            f"process-group probe: {exc}"
        )
        group_absent_before_cleanup = None
    if process.poll() is None and not terminated:
        terminate()
    elif group_absent_before_cleanup is False and not terminated:
        stop_reason = stop_reason or "child left a descendant process running"
        terminate()
    elif group_absent_before_cleanup is False:
        stop_reason = stop_reason or "child process group remains after termination grace"
        reaped = False
    elif group_absent_before_cleanup is None:
        stop_reason = stop_reason or "child process-group absence could not be verified"
        reaped = False
    try:
        returncode = process.wait(timeout=0.1 if process.poll() is not None else 0)
    except subprocess.TimeoutExpired:
        returncode = None
        reaped = False
    try:
        group_absent: bool | None = not _process_group_exists(process.pid)
    except OSError as exc:
        termination_status.setdefault("termination_errors", []).append(
            f"process-group probe: {exc}"
        )
        group_absent = None
    termination_status.update(
        {
            "leader_reaped": returncode is not None,
            "group_absent": group_absent,
            "uncertain": returncode is None or group_absent is None,
            "primary_stop_reason": stop_reason,
            "termination_errors": termination_status.get("termination_errors", []),
        }
    )
    if isinstance(report.get("storage_supervision_invocation"), str):
        termination_status["invocation_token"] = report["storage_supervision_invocation"]
    reaped = returncode is not None and group_absent is True
    report["process_supervision"] = termination_status
    report["process_supervision"]["primary_stop_reason"] = stop_reason
    return returncode, stop_reason, reaped


def _system_compressed_bytes(timeout: float = 5.0) -> int:
    """Return logical compressor-stored bytes for legacy report compatibility."""
    result = subprocess.run(
        ["vm_stat"], check=True, capture_output=True, text=True, timeout=timeout
    )
    page_size = re.search(r"page size of (\d+) bytes", result.stdout)
    compressed_pages = re.search(r"Pages stored in compressor:\s*(\d+)", result.stdout)
    if page_size is None or compressed_pages is None or int(page_size.group(1)) <= 0:
        raise RunnerError("macOS compressed-memory accounting is unavailable")
    return int(page_size.group(1)) * int(compressed_pages.group(1))


def _memory_pressure_name(flags: int) -> str:
    # Apple exposes this private sysctl as USERSPACE flags, not the kernel's internal enum.
    states = {1: "normal", 2: "warn", 4: "critical"}
    if type(flags) is not int or flags not in states:
        raise RunnerError("macOS memory-pressure flags are unknown")
    return states[flags]


def _vm_stat_compressor_metrics(text: str) -> tuple[int, int]:
    page_size = re.findall(
        r"^Mach Virtual Memory Statistics: \(page size of (\d+) bytes\)\s*$", text, re.M
    )
    stored_pages = re.findall(r"^Pages stored in compressor:\s*(\d+)\.\s*$", text, re.M)
    occupied_pages = re.findall(r"^Pages occupied by compressor:\s*(\d+)\.\s*$", text, re.M)
    if (
        len(page_size) != 1
        or len(stored_pages) != 1
        or len(occupied_pages) != 1
        or int(page_size[0]) <= 0
    ):
        raise RunnerError("macOS compressor metrics are unavailable")
    size = int(page_size[0])
    return int(stored_pages[0]) * size, int(occupied_pages[0]) * size


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_in_group(pid: int, group: int) -> bool:
    try:
        process_group = os.getpgid(pid)
    except ProcessLookupError:
        if not _pid_exists(pid):
            return False
        raise RunnerError("owned process group could not be verified") from None
    if process_group != group:
        if not _pid_exists(pid):
            return False
        raise RunnerError("process escaped its supervised process group")
    return True


def _proc_pid_footprint(proc_pid_rusage: Any, pid: int) -> int | None:
    identities = []
    footprints = []
    for _ in range(2):
        buffer = ctypes.create_string_buffer(96)
        ctypes.set_errno(0)
        if proc_pid_rusage(pid, 0, ctypes.byref(buffer)) != 0:
            error = ctypes.get_errno()
            if error == errno.ESRCH and not _pid_exists(pid):
                return None
            raise OSError(error, os.strerror(error), f"proc_pid_rusage({pid})")
        # rusage_info_v0 is 96 bytes; ri_phys_footprint and ri_proc_start_abstime are at 72 and 80.
        raw = buffer.raw
        footprints.append(int.from_bytes(raw[72:80], sys.byteorder))
        identities.append(int.from_bytes(raw[80:88], sys.byteorder))
    if identities[0] == 0 or identities[0] != identities[1]:
        raise RunnerError("process identity changed during physical-footprint sampling")
    return footprints[1]


def _memory_sample_worker(group: int | None) -> dict[str, Any]:
    if group is not None and (type(group) is not int or group <= 0):
        raise RunnerError("memory sampler process group is invalid")
    vm = subprocess.run(["vm_stat"], check=True, capture_output=True, text=True, timeout=1.0)
    stored_bytes, occupied_bytes = _vm_stat_compressor_metrics(vm.stdout)
    pressure_result = subprocess.run(
        ["sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
        check=True,
        capture_output=True,
        text=True,
        timeout=1.0,
    )
    pressure_text = pressure_result.stdout.strip()
    if not re.fullmatch(r"\d+", pressure_text):
        raise RunnerError("macOS memory-pressure measurement is malformed")
    pressure = _memory_pressure_name(int(pressure_text))
    pressure_observed = time.monotonic()
    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError as exc:
        raise RunnerError("macOS process physical-footprint accounting is unavailable") from exc
    proc_pid_rusage = libproc.proc_pid_rusage
    proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    proc_pid_rusage.restype = ctypes.c_int
    supervisor_footprint = _proc_pid_footprint(proc_pid_rusage, os.getppid())
    if supervisor_footprint is None:
        raise RunnerError("production supervisor exited during memory sampling")

    rss_bytes = footprint_bytes = None
    if group is not None:
        result = subprocess.run(
            ["ps", "-g", str(group), "-o", "pgid=,pid=,rss="],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
        if result.returncode not in {0, 1}:
            raise RunnerError("process-group memory sampling failed")
        rows = []
        for line in result.stdout.splitlines():
            fields = line.split()
            if (
                len(fields) != 3
                or any(not field.isascii() or not field.isdigit() for field in fields)
                or int(fields[0]) != group
                or int(fields[1]) <= 0
            ):
                raise RunnerError("process-group memory output is malformed or unscoped")
            rows.append((int(fields[1]), int(fields[2]) * 1024))
        if not rows and _process_group_exists(group):
            raise RunnerError("process-group memory sampling found no live processes")
        rss_bytes = sum(rss for _pid, rss in rows)
        footprint_bytes = 0
        for pid, _rss in rows:
            if not _pid_in_group(pid, group):
                continue
            footprint = _proc_pid_footprint(proc_pid_rusage, pid)
            if footprint is None:
                continue
            if not _pid_in_group(pid, group):
                continue
            footprint_bytes += footprint
    return {
        "child_process_group_rss_bytes": rss_bytes,
        "system_compressed_bytes": stored_bytes,
        "system_compressor_occupied_bytes": occupied_bytes,
        "system_memory_pressure": pressure,
        "system_memory_pressure_observed_monotonic": pressure_observed,
        "child_process_group_physical_footprint_bytes": footprint_bytes,
        "supervisor_physical_footprint_bytes": supervisor_footprint,
    }


def _sample_memory(group: int | None, *, timeout: float = 5.0) -> dict[str, Any]:
    if isinstance(group, bool) or (
        group is not None and (not isinstance(group, int) or group <= 0)
    ):
        raise RunnerError("memory sampler process group is invalid")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise RunnerError("memory sampler timeout is invalid")
    sample_started = time.monotonic()
    sample_deadline = sample_started + min(5.0, timeout)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_memory-sample",
        "-" if group is None else str(group),
    ]
    try:
        worker = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        raise RunnerError("bounded memory sample worker failed to start") from exc
    try:
        cleanup_reserve = min(0.5, (sample_deadline - time.monotonic()) / 4)
        work_timeout = sample_deadline - time.monotonic() - cleanup_reserve
        if work_timeout <= 0:
            raise subprocess.TimeoutExpired(command, min(5.0, timeout))
        stdout, _stderr = worker.communicate(timeout=work_timeout)
    except subprocess.TimeoutExpired as exc:
        _stop_memory_sample_worker(worker, sample_deadline)
        raise RunnerError("bounded memory sample worker timed out") from exc
    except BaseException:
        _stop_memory_sample_worker(worker, sample_deadline)
        raise
    sample_completed = time.monotonic()
    try:
        value = json.loads(stdout)
    except (TypeError, ValueError) as exc:
        raise RunnerError("bounded memory sample returned malformed JSON") from exc
    if worker.returncode != 0 or not isinstance(value, dict):
        message = value.get("error") if isinstance(value, dict) else None
        raise RunnerError(
            f"bounded memory sample failed: {message}"
            if isinstance(message, str)
            else "bounded memory sample failed"
        )
    expected = {
        "child_process_group_rss_bytes",
        "system_compressed_bytes",
        "system_compressor_occupied_bytes",
        "system_memory_pressure",
        "system_memory_pressure_observed_monotonic",
        "child_process_group_physical_footprint_bytes",
        "supervisor_physical_footprint_bytes",
    }
    if set(value) != expected:
        raise RunnerError("bounded memory sample is incomplete")
    pressure = value["system_memory_pressure"]
    if pressure not in {"normal", "warn", "critical"}:
        raise RunnerError("bounded memory sample has unknown pressure state")
    pressure_observed = value["system_memory_pressure_observed_monotonic"]
    if (
        isinstance(pressure_observed, bool)
        or not isinstance(pressure_observed, (int, float))
        or not math.isfinite(pressure_observed)
        or not sample_started <= pressure_observed <= sample_completed
    ):
        raise RunnerError("bounded memory sample has invalid pressure timestamp")
    for name in (
        "system_compressed_bytes",
        "system_compressor_occupied_bytes",
        "supervisor_physical_footprint_bytes",
    ):
        if type(value[name]) is not int or value[name] < 0:
            raise RunnerError("bounded memory sample has invalid system metrics")
    group_rss = value["child_process_group_rss_bytes"]
    group_footprint = value["child_process_group_physical_footprint_bytes"]
    if group is None:
        if group_rss is not None or group_footprint is not None:
            raise RunnerError("preflight memory sample unexpectedly contains a child group")
    elif any(type(metric) is not int or metric < 0 for metric in (group_rss, group_footprint)):
        raise RunnerError("bounded memory sample has invalid child metrics")
    if time.monotonic() > sample_deadline:
        raise RunnerError("bounded memory sample exceeded its time budget")
    return value


def _stop_memory_sample_worker(worker: subprocess.Popen[Any], deadline: float) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(worker.pid, signal.SIGKILL)
    try:
        worker.communicate(timeout=max(0.0, deadline - time.monotonic()))
    except subprocess.TimeoutExpired as exc:
        raise RunnerError("memory sample worker could not be reaped") from exc


def _memory_sample_worker_main(group: int | None) -> int:
    try:
        print(json.dumps(_memory_sample_worker(group), sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, sort_keys=True))
        return 1


def _invoke(
    command: list[str],
    *,
    directory: Path,
    operator: Path,
    report: dict[str, Any],
    report_path: Path,
    deadline: float,
    stdout_path: Path,
    stderr_path: Path,
    capture_id: str | None = None,
    capture_notification: Path | None = None,
    workflow_deadline: bool = False,
    phase_on_start: str | None = None,
    storage_supervision: bool = False,
    storage_invocation_token: str | None = None,
    metrics_progress_path: Path | None = None,
    storage_progress_path: Path | None = None,
) -> tuple[int | None, str | None, bool]:
    from kalshi_events.common import safe_path
    from kalshi_events.paths import child_environment, project_root

    env = os.environ.copy()
    env["PYTHONPATH"] = str(SOURCE_ROOT / "src")
    env = child_environment(env, project_root(code_root=SOURCE_ROOT))
    directory = _external_path(directory)
    original_device = directory.stat().st_dev
    recorded_device = report.get("system", {}).get("filesystem_device")
    if recorded_device != original_device:
        raise RunnerError("production filesystem changed since session preflight")
    stdout_path = safe_path(stdout_path)
    stderr_path = safe_path(stderr_path)
    if time.monotonic() >= deadline - float(_effective_limits()["terminate_seconds"]):
        return None, "insufficient time remains to start and reap a child", True
    wall_deadline_epoch = _child_wall_deadline_epoch(
        report, deadline, workflow_deadline=workflow_deadline
    )
    wall_now = time.time()
    if wall_deadline_epoch is not None and wall_now >= wall_deadline_epoch - float(
        _effective_limits()["terminate_seconds"]
    ):
        return None, "insufficient wall-clock time remains to start and reap a child", True
    remaining = _child_time_remaining(deadline, wall_deadline_epoch)
    if remaining <= float(_effective_limits()["terminate_seconds"]):
        reason = (
            "insufficient wall-clock time remains to start and reap a child"
            if wall_deadline_epoch is not None
            and time.time() >= wall_deadline_epoch - float(_effective_limits()["terminate_seconds"])
            else "insufficient time remains to start and reap a child"
        )
        return None, reason, True
    try:
        memory_preflight = _sample_memory(
            None,
            timeout=min(5.0, remaining - float(_effective_limits()["terminate_seconds"])),
        )
    except RunnerInterrupted:
        raise
    except (OSError, subprocess.SubprocessError, RunnerError) as exc:
        report["memory_preflight_error"] = str(exc)
        _atomic_report(report_path, report)
        return None, "memory metrics unavailable before supervised work", True
    report.pop("memory_preflight_error", None)
    _append_memory_preflight_sample(report, report_path, memory_preflight)
    if memory_preflight["system_memory_pressure"] != "normal":
        _atomic_report(report_path, report)
        return None, "memory pressure was not normal before supervised work", True
    remaining = _child_time_remaining(deadline, wall_deadline_epoch)
    if remaining <= float(_effective_limits()["terminate_seconds"]):
        reason = (
            "insufficient wall-clock time remains to start and reap a child"
            if wall_deadline_epoch is not None
            and time.time() >= wall_deadline_epoch - float(_effective_limits()["terminate_seconds"])
            else "insufficient time remains to start and reap a child"
        )
        return None, reason, True
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process: subprocess.Popen[Any] | None = None
        storage_state = None
        storage_external_paths = None
        try:
            global _PENDING_STOP_SIGNAL, _SPAWNING_CHILD
            wall_now = time.time()
            if time.monotonic() >= deadline - float(_effective_limits()["terminate_seconds"]) or (
                wall_deadline_epoch is not None
                and wall_now
                >= wall_deadline_epoch - float(_effective_limits()["terminate_seconds"])
            ):
                reason = (
                    "insufficient wall-clock time remains to start and reap a child"
                    if wall_deadline_epoch is not None
                    and wall_now
                    >= wall_deadline_epoch - float(_effective_limits()["terminate_seconds"])
                    else "insufficient time remains to start and reap a child"
                )
                return None, reason, True
            if storage_supervision:
                if storage_progress_path is None:
                    raise RunnerError("historical storage progress path is unavailable")
                storage_external_paths = {
                    "session_report": report_path,
                    "stdout": stdout_path,
                    "stderr": stderr_path,
                    "storage_progress": storage_progress_path,
                }
                if capture_notification is not None:
                    storage_external_paths["capture_notification"] = capture_notification
                if metrics_progress_path is not None:
                    storage_external_paths["metrics_progress"] = metrics_progress_path
                    if metrics_progress_path.name.endswith(".progress.json"):
                        storage_external_paths["metrics"] = metrics_progress_path.with_name(
                            metrics_progress_path.name.removesuffix(".progress.json")
                        )
                storage_state = _begin_storage_supervision(
                    directory,
                    operator,
                    report,
                    storage_progress_path,
                    storage_external_paths,
                )
            _SPAWNING_CHILD = True
            try:
                process = subprocess.Popen(
                    command,
                    stdout=stdout,
                    stderr=stderr,
                    cwd=SOURCE_ROOT,
                    env=env,
                    shell=False,
                    start_new_session=True,
                )
                if phase_on_start is not None:
                    report["phase"] = phase_on_start
                if storage_supervision:
                    report["storage_supervision_invocation"] = (
                        storage_invocation_token or uuid.uuid4().hex
                    )
                report["active_process_group"] = process.pid
                _atomic_report(report_path, report)
            finally:
                _SPAWNING_CHILD = False
            if _PENDING_STOP_SIGNAL is not None:
                signum = _PENDING_STOP_SIGNAL
                _PENDING_STOP_SIGNAL = None
                raise RunnerInterrupted(
                    f"received {signal.Signals(signum).name}; stopping supervised work"
                )
            result = _monitor(
                process,
                directory,
                operator,
                report,
                report_path,
                deadline,
                capture_id=capture_id,
                original_device=original_device,
                capture_notification=capture_notification,
                wall_deadline_epoch=wall_deadline_epoch,
                storage_state=storage_state,
                storage_progress_path=storage_progress_path if storage_state is not None else None,
                storage_external_paths=storage_external_paths,
            )
        except BaseException as exc:
            _merge_capture_notification(report, capture_notification)
            reaped = process is None or _terminate(process)
            if process is not None:
                outcome = getattr(process, "_production_termination_outcome", {})
                if isinstance(report.get("storage_supervision_invocation"), str):
                    outcome = {
                        **(outcome if isinstance(outcome, dict) else {}),
                        "invocation_token": report["storage_supervision_invocation"],
                    }
                report["process_supervision"] = {
                    **(outcome if isinstance(outcome, dict) else {}),
                    "primary_stop_reason": str(exc),
                }
            if process is not None and not reaped:
                outcome = getattr(process, "_production_termination_outcome", {})
                errors = "; ".join(
                    outcome.get("termination_errors", []) if isinstance(outcome, dict) else []
                )
                detail = f"; termination errors: {errors}" if errors else ""
                raise RunnerError(
                    f"{exc}; child could not be confirmed reaped within five seconds{detail}"
                ) from exc
            if process is not None:
                report["active_process_group"] = None
            if _PENDING_STOP_SIGNAL is not None:
                signum = _PENDING_STOP_SIGNAL
                _PENDING_STOP_SIGNAL = None
                raise RunnerInterrupted(
                    f"received {signal.Signals(signum).name}; stopping supervised work"
                ) from exc
            raise
        _merge_capture_notification(report, capture_notification)
    if result[2]:
        report["active_process_group"] = None
        _TERMINATION_ATTEMPTED.discard(process.pid)
    return result


def _child_accounting(
    operator: Path,
    deadline: float,
    paths: dict[str, Path],
    report: dict[str, Any],
    *,
    require_complete: bool = True,
    workflow_deadline: bool = False,
    capture_id: str | None = None,
    cleanup_historical_stages: bool = False,
    expected_root_id: str | None = None,
) -> dict[str, Any]:
    command = [sys.executable, str(Path(__file__).resolve())]
    if cleanup_historical_stages:
        if (
            capture_id is None
            or expected_root_id is None
            or report.get("active_process_group") is not None
        ):
            raise RunnerError("initial historical accounting cleanup is not session-bound")
        command += [
            "--_account-historical-initial",
            str(operator),
            str(paths["directory"]),
            str(paths["report"]),
            capture_id,
            expected_root_id,
            str(report.get("revision", "")),
            str(report.get("resource_profile", "default")),
        ]
    elif capture_id is not None:
        command += [
            "--_account-recovery",
            str(operator),
            capture_id,
            str(paths["report"]),
            str(report.get("resource_profile", "default")),
        ]
    else:
        command += ["--_account-root", str(operator)]
    run_id = uuid.uuid4().hex
    stdout = paths["logs"] / f"accounting-{run_id}.stdout.json"
    stderr = paths["logs"] / f"accounting-{run_id}.stderr.log"
    returncode, reason, reaped = _invoke(
        command,
        directory=paths["directory"],
        operator=operator,
        report=report,
        report_path=paths["report"],
        deadline=deadline,
        stdout_path=stdout,
        stderr_path=stderr,
        workflow_deadline=workflow_deadline,
        phase_on_start="accounting" if cleanup_historical_stages else None,
    )
    value = _strict_json(stdout)
    if reason in DEADLINE_STOP_REASONS:
        raise BudgetStop(reason)
    if (
        returncode != 0
        or reason
        or not reaped
        or value is None
        or (require_complete and value.get("accounting_complete") is not True)
        or not isinstance(value.get("storage"), dict)
    ):
        raise RunnerError("durable operator accounting or storage could not be confirmed")
    return value


def _root_summary(
    operator: Path, report: dict[str, Any], paths: dict[str, Path], deadline: float
) -> dict[str, Any]:
    stdout, stderr = (
        paths["logs"] / "root-summary.stdout.json",
        paths["logs"] / "root-summary.stderr.log",
    )
    status = _invoke(
        [sys.executable, str(Path(__file__).resolve()), "--_root-summary", str(operator)],
        directory=paths["directory"],
        operator=operator,
        report=report,
        report_path=paths["report"],
        deadline=deadline,
        stdout_path=stdout,
        stderr_path=stderr,
    )
    code, reason, reaped = status
    value = _strict_json(stdout)
    if reason in DEADLINE_STOP_REASONS:
        raise BudgetStop(reason)
    if code != 0 or reason or not reaped or value is None:
        raise RunnerError("could not safely inspect current production publication")
    return value


def _release_verify(
    release: Path,
    directory: Path,
    operator: Path,
    paths: dict[str, Path],
    report: dict[str, Any],
    deadline: float,
    *,
    storage_supervision: bool = False,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "kalshi_events.cli",
        "verify",
        "--release",
        str(release),
        "--max-root-bytes",
        str(int(_effective_limits()["root_bytes"])),
    ]
    stdout = paths["logs"] / "release-verify.stdout.json"
    stderr = paths["logs"] / "release-verify.stderr.log"
    storage_progress = _stage_storage_progress_path(paths, "release-verify")
    storage_token = uuid.uuid4().hex if storage_supervision else None
    started = time.monotonic()
    try:
        returncode, reason, reaped = _invoke(
            command,
            directory=directory,
            operator=operator,
            report=report,
            report_path=paths["report"],
            deadline=deadline,
            stdout_path=stdout,
            stderr_path=stderr,
            storage_supervision=storage_supervision,
            storage_invocation_token=storage_token,
            metrics_progress_path=paths.get("metrics_progress"),
            storage_progress_path=storage_progress if storage_supervision else None,
        )
        if storage_supervision and report.get("storage_supervision_invocation") == storage_token:
            if not storage_progress.exists() and not storage_progress.is_symlink():
                raise RunnerError("live verification storage report disappeared after child start")
            storage_result = _strict_json(storage_progress)
            if storage_result is None or storage_result.get("version") != 1:
                raise RunnerError("live verification storage report is missing or malformed")
            report["storage_supervision_file"] = str(storage_progress)
            report["storage_supervision"] = {
                key: value for key, value in storage_result.items() if key != "baseline"
            }
        value = _strict_json(stdout)
        if reason in DEADLINE_STOP_REASONS:
            raise BudgetStop(reason)
        if returncode != 0 or reason or not reaped or value is None:
            raise RunnerError("standalone release verification failed or has unknown status")
        return value
    finally:
        phases = report.setdefault("phase_seconds", {})
        phases["standalone-verification"] = phases.get("standalone-verification", 0) + (
            time.monotonic() - started
        )


def _validate_initial_historical_accounting_session(
    root: Path,
    directory: Path,
    report_path: Path,
    capture_id: str,
    expected_root_id: str,
    expected_revision: str,
) -> dict[str, Any]:
    from kalshi_events.common import safe_path

    root = safe_path(root)
    directory = safe_path(directory)
    report_path = safe_path(report_path)
    expected_report_parent = directory / "runs"
    session_id = report_path.parent.name
    if (
        root != directory / "operator"
        or report_path.parent.parent != expected_report_parent
        or report_path.name != REPORT_NAME
        or not CAPTURE_ID.fullmatch(session_id)
        or not isinstance(expected_root_id, str)
        or not CAPTURE_ID.fullmatch(expected_root_id)
        or not isinstance(expected_revision, str)
        or not REVISION.fullmatch(expected_revision)
    ):
        raise RunnerError("initial historical accounting paths or identity are invalid")
    report = _strict_json(report_path)
    if (
        report is None
        or report.get("version") != 1
        or report.get("mode") not in {"full", "historical"}
        or report.get("session_id") != session_id
        or report.get("capture_id") != capture_id
        or report.get("root_id") != expected_root_id
        or report.get("revision") != expected_revision
        or report.get("active_process_group") != os.getpgrp()
        or os.getpgrp() != os.getpid()
    ):
        raise RunnerError("initial historical accounting session binding changed")
    if "offline_historical_window" in report:
        _validate_offline_historical_window(
            report,
            capture_id,
            allow_expired=True,
            allow_revision_change=True,
        )
    elif (
        "offline_historical_window_history" in report
        or "offline_historical_window_supersessions" in report
    ):
        history = report.get("offline_historical_window_history")
        supersessions = report.get("offline_historical_window_supersessions", [])
        if (
            not isinstance(history, list)
            or history
            or not isinstance(supersessions, list)
            or supersessions
        ):
            raise RunnerError("offline historical window history has no current window")
    lock_path = root / "writer.lock"
    try:
        lock_details = os.stat(lock_path, follow_symlinks=False)
    except OSError as exc:
        raise RunnerError("canonical writer lock changed during historical cleanup") from exc
    _validate_offline_renewal_root(
        directory,
        root,
        expected_root_id,
        capture_id,
        writer_lock_identity=(lock_details.st_dev, lock_details.st_ino),
    )
    return report


def _read_accounting_worker(
    root: Path,
    expected_capture_id: str | None = None,
    *,
    cleanup_historical_stages: bool = False,
    expected_directory: Path | None = None,
    report_path: Path | None = None,
    expected_root_id: str | None = None,
    expected_revision: str | None = None,
) -> dict[str, Any]:
    from kalshi_events.common import Limits, Store, decode, regular_bytes

    if cleanup_historical_stages:
        if (
            expected_capture_id is None
            or expected_directory is None
            or report_path is None
            or expected_root_id is None
            or expected_revision is None
        ):
            raise RunnerError("initial historical accounting cleanup identity is incomplete")
    elif any(
        value is not None
        for value in (expected_directory, report_path, expected_root_id, expected_revision)
    ):
        raise RunnerError("historical cleanup identity was supplied without its closed flag")

    store = Store(
        root,
        Limits(
            max_rate=float(_effective_limits()["rate"]),
            max_requests=int(_effective_limits()["attempts"]),
            max_download_bytes=int(_effective_limits()["downloaded_bytes"]),
            max_response_bytes=int(_effective_limits()["response_bytes"]),
            max_decoded_response_bytes=int(_effective_limits()["decoded_response_bytes"]),
            max_duration_seconds=float(_effective_limits()["session_seconds"]),
            max_root_bytes=int(_effective_limits()["root_bytes"]),
            max_temp_bytes=int(_effective_limits()["temporary_bytes"]),
            max_database_bytes=int(_effective_limits()["database_bytes"]),
        ),
    )
    with store.lock():
        capture_state = None
        if expected_capture_id is not None:
            from kalshi_events.capture import read_capture

            capture_state = read_capture(store, expected_capture_id)
            if (
                capture_state.get("capture_id") != expected_capture_id
                or capture_state.get("selected") is not None
                or capture_state.get("incremental") is True
                or capture_state.get("acquisition_protocol") != 5
                or capture_state.get("max_response_bytes")
                != int(_effective_limits()["response_bytes"])
            ):
                raise RunnerError(
                    "recovery capture does not match the session's sealed full-scope policy"
                )
        if cleanup_historical_stages:
            assert expected_capture_id is not None
            assert expected_directory is not None
            assert report_path is not None
            assert expected_root_id is not None
            assert expected_revision is not None
            _validate_initial_historical_accounting_session(
                root,
                expected_directory,
                report_path,
                expected_capture_id,
                expected_root_id,
                expected_revision,
            )
            from kalshi_events import incremental, warehouse

            incremental.finish_pending_retirement(store)

            cleanup = getattr(warehouse, "recover_and_cleanup_abandoned_historical_stages", None)
            if not callable(cleanup):
                raise RunnerError("historical stage recovery helper is unavailable")
            cleanup(store, expected_capture_id)
        attempts = downloaded_or_reserved = 0
        accounting_complete = True
        unfinished = []
        for path in store.path("raw").glob("*/manifest.json"):
            receipt = decode(regular_bytes(path, 1024**2))
            if not isinstance(receipt, dict):
                raise RunnerError("durable receipt manifest is not an object")
            attempts += 1
            complete = receipt.get("accounting_complete")
            reservation = receipt.get("reserved_bytes")
            page_id = receipt.get("page_id")
            if (
                isinstance(receipt.get("version"), bool)
                or receipt.get("version") not in {1, 2}
                or not isinstance(page_id, str)
                or not CAPTURE_ID.fullmatch(page_id)
                or page_id != path.parent.name
            ):
                raise RunnerError("durable receipt has invalid identity metadata")
            if complete is True:
                if "reserved_bytes" in receipt and (
                    isinstance(reservation, bool)
                    or not isinstance(reservation, int)
                    or not 0 < reservation <= int(_effective_limits()["response_bytes"])
                ):
                    raise RunnerError("finalized receipt has invalid reservation metadata")
                amount = receipt.get("downloaded_bytes", receipt.get("size"))
                if (
                    isinstance(amount, bool)
                    or not isinstance(amount, int)
                    or not 0
                    <= amount
                    <= int(_effective_limits()["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
                ):
                    raise RunnerError("finalized receipt has invalid downloaded-byte accounting")
                status = receipt.get("status")
                if status is not None and (
                    isinstance(status, bool)
                    or not isinstance(status, int)
                    or not 100 <= status <= 599
                ):
                    raise RunnerError("finalized receipt has invalid HTTP status")
                downloaded_or_reserved += amount
                continue
            if complete is not False:
                raise RunnerError("durable receipt accounting state is malformed")
            accounting_complete = False
            downloaded_or_reserved += (
                int(_effective_limits()["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
            )
            if (
                isinstance(reservation, bool)
                or not isinstance(reservation, int)
                or not 0 < reservation <= int(_effective_limits()["response_bytes"])
            ):
                raise RunnerError("unfinished receipt has invalid reservation metadata")
            if receipt.get("status") is not None:
                raise RunnerError("unfinished receipt has a contradictory HTTP status")
            if (
                receipt.get("version") not in {1, 2}
                or receipt.get("acquisition_protocol") != 5
                or not isinstance(receipt.get("capture_id"), str)
                or not CAPTURE_ID.fullmatch(receipt["capture_id"])
                or not isinstance(receipt.get("request_id"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", receipt["request_id"])
            ):
                raise RunnerError("unfinished receipt lacks verifiable acquisition identity")
            if capture_state is not None:
                from kalshi_events.capture import _validate_receipt_task_binding

                _validate_receipt_task_binding(
                    receipt,
                    capture_state,
                    {
                        "relative": path.relative_to(store.root).as_posix(),
                        "index": receipt.get("task_index"),
                        "request_id": receipt["request_id"],
                    },
                )
            if complete is False:
                unfinished.append(
                    {
                        "capture_id": receipt.get("capture_id"),
                        "request_id": receipt.get("request_id"),
                        "reserved_bytes": receipt.get("reserved_bytes"),
                        "status": receipt.get("status"),
                        "accounting_complete": False,
                        "acquisition_protocol": receipt.get("acquisition_protocol"),
                        "version": receipt.get("version"),
                    }
                )
        totals = {
            "attempts": attempts,
            "downloaded_or_reserved_bytes": downloaded_or_reserved,
            "accounting_complete": accounting_complete,
        }
        storage = store.usage()
        logical_root = _root_id(store)
    return {
        **totals,
        "unfinished_reservations": unfinished,
        "storage": storage,
        "root_id": logical_root,
    }


def _read_root_summary(root: Path) -> dict[str, Any]:
    from kalshi_events import publish
    from kalshi_events.common import Limits, Store

    store = Store(root, Limits(max_root_bytes=int(_effective_limits()["root_bytes"])))
    result: dict[str, Any] = {
        "root_id": _root_id(store),
        "current_v2": None,
        "current_historical": None,
    }
    if store.path("current-v2.json").exists():
        try:
            result["current_v2"] = publish.current(store, catalogue_version=2)
        except Exception:
            result["current_v2"] = None
    if store.path("current-historical.json").exists():
        try:
            result["current_historical"] = publish.current(store, catalogue_version=3)
        except Exception:
            result["current_historical"] = None
    return result


def _root_id(store: Any) -> str | None:
    from kalshi_events.common import decode, regular_bytes

    identity = store.path("root-id.json")
    if not identity.exists():
        return None
    value = decode(regular_bytes(identity, 1024**2))
    return value.get("root_id") if isinstance(value, dict) else None


def activate_current(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str | None = None,
    expected_capture_id: str | None = None,
    standalone_verified: dict[str, Any] | None = None,
    catalogue_version: int = 2,
    max_database_bytes: int = 64 * GIB,
    max_temp_bytes: int = 64 * GIB,
    link_fn: Any = None,
    replace_fn: Any = None,
    sync_fn: Any = None,
) -> dict[str, Any]:
    """Public runner helper, kept importable for offline activation tests."""
    from production_activation import activate_current as activate

    return activate(
        operator_root,
        production_directory,
        expected_release_id=expected_release_id,
        expected_capture_id=expected_capture_id,
        standalone_verified=standalone_verified,
        catalogue_version=catalogue_version,
        max_database_bytes=max_database_bytes,
        max_temp_bytes=max_temp_bytes,
        link_fn=link_fn,
        replace_fn=replace_fn,
        sync_fn=sync_fn,
    )


def _external_bytes(directory: Path) -> int:
    total = 0
    operator = directory / "operator"
    seen: set[tuple[int, int]] = set()
    for builds in (operator / "builds-v2", operator / "builds-historical"):
        if builds.is_symlink():
            raise RunnerError("operator build directory is a symlink")
        if builds.is_dir():
            for candidate in builds.glob("*/warehouse.duckdb"):
                if candidate.is_symlink() or not candidate.is_file():
                    raise RunnerError(
                        "retained warehouse path is unsafe during external accounting"
                    )
                info = candidate.stat()
                seen.add((info.st_dev, info.st_ino))
    for path in (directory / "production.duckdb", directory / "runs"):
        if path.is_symlink():
            raise RunnerError(f"external production accounting path is a symlink: {path.name}")
        if path.is_file() and not path.is_symlink():
            info = path.stat()
            key = (info.st_dev, info.st_ino)
            if key not in seen:
                total += info.st_size
                seen.add(key)
        elif path.is_dir():
            for child in path.rglob("*"):
                if child.is_symlink():
                    raise RunnerError(f"external run artifact is a symlink: {child}")
                if child.is_file() and not child.is_symlink():
                    info = child.stat()
                    key = (info.st_dev, info.st_ino)
                    if key not in seen:
                        total += info.st_size
                        seen.add(key)
    return total


_LIVE_TMP_NAMES = (
    re.compile(r"build-historical-[0-9a-f]{32}\Z"),
    re.compile(r"parent-index-[1-9][0-9]*-[0-9a-f]{12}\Z"),
    re.compile(r"membership-verify-[0-9a-f]{32}\Z"),
    re.compile(r"publish-[0-9a-f]{32}\Z"),
    re.compile(r"dbt-[0-9a-f]{32}\Z"),
    re.compile(r"load-[0-9a-f]{32}\Z"),
    re.compile(r"baseline-verify-[0-9a-f]{32}\Z"),
    re.compile(r"database-copy-[0-9a-f]{32}\Z"),
    re.compile(r"build-[0-9a-f]{32}\Z"),
    re.compile(r"build-v2-[0-9a-f]{32}\Z"),
    re.compile(r"replaced-[0-9a-f]{32}-v3\Z"),
    re.compile(r"\.write-[A-Za-z0-9_]{8}\Z"),
)
_ATOMIC_TEMP_NAME = re.compile(r"\.write-[A-Za-z0-9_]{8}\Z")
_ACTIVATION_TEMP_NAME = re.compile(r"\.production\.duckdb\.[0-9a-f]{32}\.tmp\Z")


def _bounded_tree_bytes(
    path: Path,
    device: int,
    count: list[int],
    allocated_inodes: set[tuple[int, int]],
) -> dict[str, int]:
    """Measure one declared mutable tree without following links or walking the root."""
    import stat

    totals = {"logical_bytes": 0, "allocated_bytes": 0, "database_bytes": 0}
    try:
        root = path.lstat()
    except FileNotFoundError:
        return totals
    if not stat.S_ISDIR(root.st_mode) or root.st_dev != device:
        raise RunnerError(f"live storage workspace is unsafe: {path.name}")

    def walk(directory: Path) -> None:
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    count[0] += 1
                    if count[0] > int(_effective_limits()["storage_inventory_entries"]):
                        raise RunnerError("live storage inventory exceeded its entry bound")
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise RunnerError("live storage entry could not be inspected") from exc
                    if info.st_dev != device:
                        raise RunnerError("live storage workspace crossed filesystems")
                    if stat.S_ISDIR(info.st_mode):
                        walk(Path(entry.path))
                    elif stat.S_ISREG(info.st_mode):
                        totals["logical_bytes"] += info.st_size
                        inode = (info.st_dev, info.st_ino)
                        if inode not in allocated_inodes:
                            totals["allocated_bytes"] += info.st_blocks * 512
                            allocated_inodes.add(inode)
                        if entry.name.endswith((".duckdb", ".wal")):
                            totals["database_bytes"] += info.st_size
                    else:
                        raise RunnerError("live storage workspace contains a non-regular entry")
        except OSError as exc:
            raise RunnerError("live storage workspace could not be scanned") from exc

    walk(path)
    return totals


def _live_workspace_snapshot(
    operator: Path, directory: Path, capture_id: str, expected_root_id: str
) -> dict[str, Any]:
    """Account only declared mutable workspaces and promotion destinations."""
    import stat

    root_info = operator.lstat()
    if (
        not stat.S_ISDIR(root_info.st_mode)
        or operator.resolve() != operator.absolute()
        or root_info.st_dev != directory.stat().st_dev
    ):
        raise RunnerError("operator root changed during live storage supervision")
    root_identity = _strict_json(operator / "root-id.json", MIB)
    if (
        not isinstance(expected_root_id, str)
        or _owner_root_id(directory) != expected_root_id
        or not isinstance(root_identity, dict)
        or root_identity.get("version") != 1
        or root_identity.get("root_id") != expected_root_id
    ):
        raise RunnerError("operator root identity changed during live storage supervision")
    count = [0]
    allocated_inodes: set[tuple[int, int]] = set()
    logical = allocated = database = temporary = 0
    try:
        with os.scandir(operator) as entries:
            operator_entries = {}
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
                ):
                    raise RunnerError("operator root contains an unsafe live entry")
                operator_entries[entry.name] = [
                    info.st_dev,
                    info.st_ino,
                    info.st_mode,
                    info.st_size,
                    info.st_mtime_ns,
                    info.st_blocks,
                ]
    except OSError as exc:
        raise RunnerError("operator root could not be inventoried") from exc
    if len(operator_entries) > int(_effective_limits()["storage_inventory_entries"]):
        raise RunnerError("operator root exceeded its direct-entry bound")
    pointer_entry = operator_entries.get("current-historical.json")
    if pointer_entry is not None:
        logical += pointer_entry[3]
    for name, entry in operator_entries.items():
        if not _ATOMIC_TEMP_NAME.fullmatch(name):
            continue
        if not stat.S_ISREG(entry[2]) or entry[0] != root_info.st_dev:
            raise RunnerError("operator root atomic-write temporary is unsafe")
        logical += entry[3]
        temporary += entry[3]
        inode = (entry[0], entry[1])
        if inode not in allocated_inodes:
            allocated += entry[5] * 512
            allocated_inodes.add(inode)
    tmp = operator / "tmp"
    try:
        tmp_info = tmp.lstat()
    except FileNotFoundError:
        tmp_info = None
    if tmp_info is not None:
        if not stat.S_ISDIR(tmp_info.st_mode) or tmp_info.st_dev != root_info.st_dev:
            raise RunnerError("operator temporary namespace is unsafe")
        try:
            with os.scandir(tmp) as entries:
                names = list(entries)
        except OSError as exc:
            raise RunnerError("operator temporary namespace could not be listed") from exc
        if len(names) > int(_effective_limits()["storage_inventory_entries"]):
            raise RunnerError("operator temporary namespace exceeded its entry bound")
        for entry in names:
            if entry.name == "recovery-index":
                valid = entry.is_dir(follow_symlinks=False)
            else:
                valid = any(pattern.fullmatch(entry.name) for pattern in _LIVE_TMP_NAMES)
            if not valid:
                raise RunnerError(f"unknown live temporary workspace: {entry.name}")
            info = entry.stat(follow_symlinks=False)
            if info.st_dev != root_info.st_dev:
                raise RunnerError("operator temporary workspace crossed filesystems")
            if stat.S_ISDIR(info.st_mode):
                totals = _bounded_tree_bytes(
                    Path(entry.path), root_info.st_dev, count, allocated_inodes
                )
            elif stat.S_ISREG(info.st_mode) and entry.name.startswith(".write-"):
                totals = {
                    "logical_bytes": info.st_size,
                    "allocated_bytes": info.st_blocks * 512,
                    "database_bytes": 0,
                }
            else:
                raise RunnerError("operator temporary workspace contains an unsafe entry")
            logical += totals["logical_bytes"]
            allocated += totals["allocated_bytes"]
            database += totals["database_bytes"]
            temporary += totals["logical_bytes"]

    def directory_names(parent: Path, pattern: re.Pattern[str]) -> tuple[set[str], Path | None]:
        try:
            parent_info = parent.lstat()
        except FileNotFoundError:
            return set(), None
        if not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_dev != root_info.st_dev:
            raise RunnerError(f"live storage directory is unsafe: {parent.name}")
        try:
            with os.scandir(parent) as entries:
                rows = list(entries)
        except OSError as exc:
            raise RunnerError(f"live storage directory could not be listed: {parent.name}") from exc
        if len(rows) > int(_effective_limits()["storage_release_directories"]):
            raise RunnerError(f"live storage directory exceeded its entry bound: {parent.name}")
        names = set()
        for entry in rows:
            if not pattern.fullmatch(entry.name) or not entry.is_dir(follow_symlinks=False):
                raise RunnerError(f"unknown live storage destination: {entry.name}")
            names.add(entry.name)
        return names, parent

    build_names, _ = directory_names(operator / "builds-historical", re.compile(r"[0-9a-f]{32}\Z"))
    build_path = operator / "builds-historical" / capture_id
    try:
        build_info = build_path.lstat()
    except FileNotFoundError:
        build_info = None
    if build_info is not None:
        totals = _bounded_tree_bytes(build_path, root_info.st_dev, count, allocated_inodes)
        logical += totals["logical_bytes"]
        allocated += totals["allocated_bytes"]
        database += totals["database_bytes"]

    release_names, _ = directory_names(operator / "releases", re.compile(r"[0-9a-f]{64}\Z"))
    pointer_path = operator / "current-historical.json"
    try:
        pointer_info = pointer_path.lstat()
    except FileNotFoundError:
        pointer_info = None
    if pointer_info is not None and not stat.S_ISREG(pointer_info.st_mode):
        raise RunnerError("historical release pointer is missing or unsafe")
    pointer = _strict_json(pointer_path, MIB) if pointer_info is not None else None
    if pointer_info is not None and pointer is None:
        raise RunnerError("historical release pointer is missing or unsafe")
    current_release = pointer.get("source_release_id") if pointer is not None else None
    if current_release is not None and (
        not isinstance(current_release, str)
        or not re.fullmatch(r"[0-9a-f]{64}", current_release)
        or current_release not in release_names
    ):
        raise RunnerError("historical release pointer has no safe release directory")
    return_value = {
        "logical_bytes": logical,
        "allocated_bytes": allocated,
        "database_bytes": database,
        "temporary_bytes": temporary,
        "tmp_names": sorted(entry.name for entry in names) if tmp_info is not None else [],
        "build_names": sorted(build_names),
        "release_names": sorted(release_names),
        "operator_entries": operator_entries,
        "allocated_inodes": allocated_inodes,
    }
    return {
        **return_value,
    }


def _live_external_paths(paths: dict[str, Path], directory: Path) -> dict[str, Path]:
    import stat

    report_path = paths.get("session_report")
    if report_path is None:
        raise RunnerError("live external inventory has no session report path")
    result = dict(paths)
    for parent, pattern, label in (
        (report_path.parent, re.compile(r"\.session\.json\.[0-9a-f]{32}\.tmp\Z"), "report"),
        (directory, _ACTIVATION_TEMP_NAME, "activation"),
    ):
        try:
            with os.scandir(parent) as entries:
                rows = list(entries)
        except OSError as exc:
            raise RunnerError("live external artifact directory could not be listed") from exc
        if len(rows) > int(_effective_limits()["storage_inventory_entries"]):
            raise RunnerError("live external artifact inventory exceeded its entry bound")
        for entry in rows:
            if pattern.fullmatch(entry.name):
                result[f"{label}:{entry.name}"] = Path(entry.path)
    try:
        with os.scandir(directory) as entries:
            root_rows = list(entries)
    except OSError as exc:
        raise RunnerError("production directory could not be inventoried") from exc
    if len(root_rows) > int(_effective_limits()["storage_inventory_entries"]):
        raise RunnerError("production directory exceeded its direct-entry bound")
    fixed_root_entries = {
        OWNER_MARKER: "file",
        ".production-run.lock": "file",
        "operator": "directory",
        "runs": "directory",
        "production.duckdb": "file",
    }
    for entry in root_rows:
        if _ACTIVATION_TEMP_NAME.fullmatch(entry.name):
            continue
        expected_type = fixed_root_entries.get(entry.name)
        if expected_type is None:
            raise RunnerError(f"unknown production-root workspace: {entry.name}")
        info = entry.stat(follow_symlinks=False)
        if expected_type == "directory" and not stat.S_ISDIR(info.st_mode):
            raise RunnerError(f"production-root directory is unsafe: {entry.name}")
        if expected_type == "file" and not stat.S_ISREG(info.st_mode):
            raise RunnerError(f"production-root file is unsafe: {entry.name}")
    return result


def _live_external_files(
    paths: dict[str, Path],
    *,
    directory: Path,
    seen: set[tuple[int, int]] | None = None,
) -> dict[str, int]:
    import stat

    result: dict[str, int] = {}
    seen = set() if seen is None else set(seen)
    for name, path in _live_external_paths(paths, directory).items():
        try:
            info = path.lstat()
        except FileNotFoundError:
            result[name] = 0
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink < 1:
            raise RunnerError(f"live external artifact is unsafe: {path.name}")
        identity = (info.st_dev, info.st_ino)
        result[name] = info.st_size if identity not in seen else 0
        seen.add(identity)
    return result


def _live_external_allocated_bytes(
    paths: dict[str, Path],
    *,
    directory: Path,
    seen: set[tuple[int, int]] | None = None,
) -> int:
    seen = set() if seen is None else set(seen)
    total = 0
    for path in _live_external_paths(paths, directory).values():
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        identity = (info.st_dev, info.st_ino)
        if identity not in seen:
            total += info.st_blocks * 512
            seen.add(identity)
    return total


def _storage_stop_reached(observed: int, category: str) -> bool:
    limits = _effective_limits()
    threshold = int(limits[f"storage_{category}_stop_bytes"])
    if bool(limits[f"storage_{category}_stop_inclusive"]):
        return observed >= threshold
    return observed > threshold


def _begin_storage_supervision(
    directory: Path,
    operator: Path,
    report: dict[str, Any],
    progress_path: Path,
    external_paths: dict[str, Path],
) -> dict[str, Any]:
    saved = report.get("storage")
    fields = (
        "operator_retained_bytes",
        "operator_temporary_bytes",
        "operator_database_bytes",
        "external_bytes",
        "aggregate_estimate_bytes",
    )
    if not isinstance(saved, dict) or any(
        type(saved.get(field)) is not int or saved[field] < 0 for field in fields
    ):
        raise RunnerError("live storage supervision has no verified baseline")
    if not isinstance(report.get("capture_id"), str) or not CAPTURE_ID.fullmatch(
        report["capture_id"]
    ):
        raise RunnerError("live storage supervision has no exact capture identity")
    if progress_path.exists() or progress_path.is_symlink():
        raise RunnerError("live storage progress path already exists or is unsafe")
    workspace = _live_workspace_snapshot(
        operator,
        directory,
        str(report["capture_id"]),
        str(report.get("root_id", "")),
    )
    if any(_ATOMIC_TEMP_NAME.fullmatch(name) for name in workspace["operator_entries"]):
        raise RunnerError("operator root contains a stale atomic-write temporary")
    if _storage_stop_reached(saved["operator_database_bytes"], "database"):
        threshold = int(_effective_limits()["storage_database_stop_bytes"])
        raise RunnerError(
            f"verified database storage baseline reaches the {threshold // GIB} GiB stop threshold"
        )
    if _storage_stop_reached(saved["operator_temporary_bytes"], "temporary"):
        threshold = int(_effective_limits()["storage_temporary_stop_bytes"])
        raise RunnerError(
            f"verified temporary storage baseline reaches the {threshold // GIB} GiB stop threshold"
        )
    if saved["operator_retained_bytes"] > int(_effective_limits()["root_bytes"]) or saved[
        "aggregate_estimate_bytes"
    ] > int(_effective_limits()["aggregate_bytes"]):
        raise RunnerError("verified storage baseline exceeds a live admission threshold")
    external = _live_external_files(
        external_paths,
        directory=directory,
        seen=workspace["allocated_inodes"],
    )
    if any(name.startswith("activation:") for name in external):
        raise RunnerError("stale production activation temporary exists before supervision")
    current = {
        "operator_retained_bytes": saved["operator_retained_bytes"],
        "operator_temporary_bytes": saved["operator_temporary_bytes"],
        "operator_database_bytes": saved["operator_database_bytes"],
        "external_bytes": saved["external_bytes"],
        "aggregate_estimate_bytes": saved["aggregate_estimate_bytes"],
        "operator_live_workspace_allocated_bytes": workspace["allocated_bytes"],
    }
    state = {
        "version": 1,
        "target_interval_seconds": int(_effective_limits()["storage_sample_seconds"]),
        "sample_count": 0,
        "last_sample_monotonic": None,
        "max_interval_seconds": 0.0,
        "baseline": {
            "workspace": workspace,
            "external_files": external,
            "external_allocated_bytes": _live_external_allocated_bytes(
                external_paths,
                directory=directory,
                seen=workspace["allocated_inodes"],
            ),
            "storage": saved,
        },
        "high_water": dict(current),
        "current": current,
    }
    _write_storage_progress(progress_path, state, create=True)
    return state


def _write_storage_progress(path: Path, state: dict[str, Any], *, create: bool = False) -> None:
    import stat

    if create and (path.exists() or path.is_symlink()):
        raise RunnerError("live storage progress path already exists")
    if not create:
        try:
            info = path.lstat()
        except OSError as exc:
            raise RunnerError("live storage progress report is missing") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RunnerError("live storage progress path became unsafe")
    if path.is_symlink():
        raise RunnerError("live storage progress path became a symlink")
    value = dict(state)
    baseline = dict(state["baseline"])
    workspace = baseline["workspace"]
    baseline["workspace"] = {
        key: workspace[key]
        for key in (
            "logical_bytes",
            "allocated_bytes",
            "database_bytes",
            "temporary_bytes",
        )
    }
    value["baseline"] = baseline
    _atomic_report(path, value)


def _sample_live_storage(
    directory: Path,
    operator: Path,
    report: dict[str, Any],
    state: dict[str, Any],
    progress_path: Path,
    external_paths: dict[str, Path],
) -> str | None:
    baseline = state["baseline"]
    base_workspace = baseline["workspace"]
    current_workspace = _live_workspace_snapshot(
        operator,
        directory,
        str(report.get("capture_id", "")),
        str(report.get("root_id", "")),
    )
    base_builds = set(base_workspace["build_names"])
    if set(current_workspace["build_names"]) - base_builds - {str(report.get("capture_id"))}:
        raise RunnerError("live historical build destination was not registered")
    base_releases = set(base_workspace["release_names"])
    added_releases = set(current_workspace["release_names"]) - base_releases
    if len(added_releases) > 1:
        raise RunnerError("multiple live historical release destinations are ambiguous")
    if added_releases:
        release_id = next(iter(added_releases))
        release = _bounded_tree_bytes(
            operator / "releases" / release_id,
            operator.stat().st_dev,
            [0],
            current_workspace["allocated_inodes"],
        )
        current_workspace["logical_bytes"] += release["logical_bytes"]
        current_workspace["allocated_bytes"] += release["allocated_bytes"]
        current_workspace["database_bytes"] += release["database_bytes"]
    baseline_entries = base_workspace["operator_entries"]
    current_entries = current_workspace["operator_entries"]
    mutable_top_level = {"tmp", "builds-historical", "releases", "current-historical.json"}
    missing = set(baseline_entries) - set(current_entries)
    if missing - {name for name in missing if _ATOMIC_TEMP_NAME.fullmatch(name)}:
        raise RunnerError("operator root entry disappeared during live storage supervision")
    new_entries = set(current_entries) - set(baseline_entries) - mutable_top_level
    if new_entries - {name for name in new_entries if _ATOMIC_TEMP_NAME.fullmatch(name)}:
        raise RunnerError("unknown operator-root workspace appeared during live supervision")
    mutable_entries = mutable_top_level | {
        name
        for name in set(baseline_entries) | set(current_entries)
        if _ATOMIC_TEMP_NAME.fullmatch(name)
    }
    for name in set(baseline_entries) & set(current_entries) - mutable_entries:
        if baseline_entries[name] != current_entries[name]:
            raise RunnerError(f"non-workspace operator entry changed during live work: {name}")
    current_external = _live_external_files(
        external_paths,
        directory=directory,
        seen=current_workspace["allocated_inodes"],
    )
    external_delta = sum(current_external.values()) - sum(baseline["external_files"].values())
    external_allocated = _live_external_allocated_bytes(
        external_paths,
        directory=directory,
        seen=current_workspace["allocated_inodes"],
    )
    free = shutil.disk_usage(directory).free
    workspace_delta = current_workspace["logical_bytes"] - base_workspace["logical_bytes"]
    database_delta = current_workspace["database_bytes"] - base_workspace["database_bytes"]
    temporary_delta = current_workspace["temporary_bytes"] - base_workspace["temporary_bytes"]
    saved = baseline["storage"]
    current = {
        "operator_retained_bytes": saved["operator_retained_bytes"] + workspace_delta,
        "operator_temporary_bytes": saved["operator_temporary_bytes"] + temporary_delta,
        "operator_database_bytes": saved["operator_database_bytes"] + database_delta,
        "external_bytes": saved["external_bytes"] + external_delta,
        "aggregate_estimate_bytes": saved["aggregate_estimate_bytes"]
        + workspace_delta
        + external_delta,
        "operator_live_workspace_allocated_bytes": current_workspace["allocated_bytes"],
        "external_live_artifact_allocated_bytes": external_allocated,
        "external_live_artifact_logical_bytes": sum(current_external.values()),
        "ssd_free_bytes": free,
    }
    state["sample_count"] += 1
    now = time.monotonic()
    previous = state.get("last_sample_monotonic")
    interval = None if previous is None else max(0.0, now - previous)
    if interval is not None:
        state["max_interval_seconds"] = max(float(state["max_interval_seconds"]), interval)
    state["last_sample_monotonic"] = now
    state["last_interval_seconds"] = interval
    state["last_observed_at"] = _utc()
    state["current"] = current
    for name, value in current.items():
        if name.endswith("allocated_bytes") or name.endswith("logical_bytes"):
            continue
        state["high_water"][name] = max(state["high_water"].get(name, 0), value)
    report["storage_supervision"] = {
        key: value for key, value in state.items() if key != "baseline"
    }
    _write_storage_progress(progress_path, state)
    database_stop = int(_effective_limits()["storage_database_stop_bytes"])
    if _storage_stop_reached(current["operator_database_bytes"], "database"):
        return f"live database storage reached the {database_stop // GIB} GiB stop threshold"
    temporary_stop = int(_effective_limits()["storage_temporary_stop_bytes"])
    if _storage_stop_reached(current["operator_temporary_bytes"], "temporary"):
        return f"live temporary storage reached the {temporary_stop // GIB} GiB stop threshold"
    if current["operator_retained_bytes"] > int(_effective_limits()["root_bytes"]):
        return "live operator root storage allowance was exceeded"
    if current["aggregate_estimate_bytes"] > int(_effective_limits()["aggregate_bytes"]):
        return "live aggregate storage allowance was exceeded"
    if free < int(_effective_limits()["minimum_free_bytes"]):
        return "SSD free space fell below 150 GiB"
    return None


def _prepare_run_paths(directory: Path, report_path: Path) -> dict[str, Path]:
    run_root = report_path.parent
    logs = run_root / "logs" / f"invocation-{uuid.uuid4().hex}"
    logs.mkdir(parents=True)
    metrics_path = run_root / f"metrics-{uuid.uuid4().hex}.json"
    return {
        "directory": directory,
        "report": report_path,
        "logs": logs,
        "metrics": metrics_path,
        "metrics_progress": metrics_path.with_name(f"{metrics_path.name}.progress.json"),
        "storage_progress": run_root / f"storage-{uuid.uuid4().hex}.json",
        "capture_notification": run_root / "capture-notification.json",
    }


def _stage_storage_progress_path(paths: dict[str, Path], label: str) -> Path:
    candidate = paths.get("storage_progress")
    if candidate is not None and not candidate.exists() and not candidate.is_symlink():
        return candidate
    return paths["logs"] / f"{label}.storage.json"


@contextlib.contextmanager
def _run_lock(directory: Path):
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    lock = os.fdopen(os.open(directory / ".production-run.lock", flags, 0o600), "r+b")
    try:
        import stat

        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise RunnerError("production runner lock is not a regular file")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunnerError("another production runner holds the session lock") from exc
        yield lock
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


@contextlib.contextmanager
def _operator_writer_lock(operator: Path):
    """Read operator identity under its canonical lock without creating or truncating it."""
    path = operator / "writer.lock"
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RunnerError("operator root writer lock is missing or unsafe") from exc
    locked = False
    try:
        import stat

        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
            raise RunnerError("operator root writer lock is not a private regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RunnerError("another writer holds the canonical operator root lock") from exc
        locked = True
        try:
            current = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise RunnerError("canonical operator writer lock changed during acquisition") from exc
        if (current.st_dev, current.st_ino) != (details.st_dev, details.st_ino):
            raise RunnerError("canonical operator writer lock changed during acquisition")
        yield details.st_dev, details.st_ino
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _stopped_worker_evidence(
    directory: Path, report_path: Path, report: dict[str, Any], capture_id: str
) -> tuple[dict[str, Any], Path, bytes]:
    from kalshi_events.common import regular_bytes, safe_path

    profile, sealed_report = _session_resource_profile(report_path, capture_id=capture_id)
    if sealed_report != report:
        raise RunnerError("stopped-worker session report changed during profile validation")
    database_limit = int(_limits_from_report(sealed_report, profile)["database_bytes"])
    source_value = report.get("source_root")
    if not isinstance(source_value, str):
        raise RunnerError("stopped-worker reconciliation has no saved source root")
    try:
        source_root = safe_path(Path(source_value))
    except Exception as exc:
        raise RunnerError("saved source root is unsafe") from exc
    runtime_root = (SOURCE_ROOT / "data" / "runtime").resolve()
    if (
        not source_root.is_dir()
        or source_root.name != "source"
        or source_root.parent.parent != runtime_root
    ):
        raise RunnerError("saved source root is outside the managed runtime layout")
    launch_path = source_root.parent / "latest-launch.json"
    try:
        launch_stat = launch_path.lstat()
    except OSError as exc:
        raise RunnerError("saved source runtime has no latest-launch record") from exc
    import stat

    if not stat.S_ISREG(launch_stat.st_mode) or launch_stat.st_nlink != 1:
        raise RunnerError("latest-launch record is unsafe")
    launch_raw = regular_bytes(launch_path, MIB)
    try:
        launch = json.loads(launch_raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("latest-launch record is malformed") from exc
    if not isinstance(launch, dict):
        raise RunnerError("latest-launch record is malformed")
    launch_capture = launch.get("capture_id")
    launch_revision = launch.get("revision")
    launch_pid = launch.get("pid")
    launch_pgid = launch.get("pgid")
    command = launch.get("command")
    if (
        launch.get("session") != str(report_path)
        or launch_capture != capture_id
        or launch_revision != report.get("revision")
        or not isinstance(launch_revision, str)
        or not REVISION.fullmatch(launch_revision)
        or isinstance(launch_pid, bool)
        or not isinstance(launch_pid, int)
        or launch_pid <= 1
        or isinstance(launch_pgid, bool)
        or not isinstance(launch_pgid, int)
        or launch_pgid != launch_pid
        or launch.get("offline_only") is not True
        or launch.get("automatic_retry") is not False
        or not isinstance(command, list)
        or any(not isinstance(item, str) for item in command)
        or "--historical" not in command
    ):
        raise RunnerError("latest-launch identity does not match the stopped capture")
    try:
        capture_index = command.index("--capture-id")
        directory_index = command.index("--directory")
        command_capture = command[capture_index + 1]
        command_directory = Path(command[directory_index + 1]).absolute()
        command_script = safe_path(Path(command[1]))
        latest_session = safe_path(Path(launch["session"]))
    except (ValueError, IndexError, KeyError, OSError, TypeError) as exc:
        raise RunnerError("latest-launch command is incomplete or unsafe") from exc
    if (
        command_capture != capture_id
        or command_directory != directory.absolute()
        or not command_script.is_file()
        or not command_script.is_relative_to(source_root)
        or latest_session != report_path
    ):
        raise RunnerError("latest-launch command does not match this session")
    process_identity = launch.get("process_identity")
    if not isinstance(process_identity, str) or process_identity.split(maxsplit=2)[:2] != [
        str(launch_pid),
        str(launch_pgid),
    ]:
        raise RunnerError("latest-launch process identity is ambiguous")
    stdout_path = launch.get("stdout")
    if not isinstance(stdout_path, str):
        raise RunnerError("latest-launch stdout path is missing")
    try:
        stdout = safe_path(Path(stdout_path))
        stdout_info = stdout.lstat()
    except OSError as exc:
        raise RunnerError("latest-launch stdout path is unsafe") from exc
    if not stat.S_ISREG(stdout_info.st_mode) or stdout.parent.parent != source_root.parent:
        raise RunnerError("latest-launch stdout path is outside its managed run")
    evidence_path = stdout.parent / "storage-safety-stop.json"
    try:
        evidence_info = evidence_path.lstat()
    except OSError as exc:
        raise RunnerError("stopped-worker evidence is missing") from exc
    if not stat.S_ISREG(evidence_info.st_mode) or evidence_info.st_nlink != 1:
        raise RunnerError("stopped-worker evidence is unsafe")
    evidence_raw = regular_bytes(evidence_path, MIB)
    try:
        evidence = json.loads(evidence_raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("stopped-worker evidence is malformed") from exc
    if not isinstance(evidence, dict):
        raise RunnerError("stopped-worker evidence is malformed")
    started_at = launch.get("started_at")
    process_started_at = evidence.get("supervisor_process_started_utc")
    try:
        launch_started = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
        evidence_started = datetime.fromisoformat(str(process_started_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RunnerError("stopped-worker process start identity is invalid") from exc
    original_group = report.get("active_process_group")
    activity = evidence.get("network_activity_this_invocation")
    if (
        launch_started.tzinfo is None
        or evidence_started.tzinfo is None
        or int(launch_started.timestamp()) != int(evidence_started.timestamp())
        or evidence.get("capture_id") != capture_id
        or evidence.get("revision") != report.get("revision")
        or evidence.get("old_active_child_group") != original_group
        or evidence.get("supervisor_pid") != launch_pid
        or evidence.get("signal") != "SIGTERM to exact original supervisor only"
        or evidence.get("original_pids_absent") is not True
        or evidence.get("owned_group_members") != []
        or evidence.get("production_exists") is not False
        or evidence.get("automatic_retry") is not False
        or evidence.get("session_phase") != "stopped"
        or evidence.get("session_error") != report.get("error")
        or evidence.get("database_limit_bytes") != database_limit
        or type(evidence.get("observed_database_bytes")) is not int
        or evidence["observed_database_bytes"] <= database_limit
        or not isinstance(activity, dict)
        or activity.get("requests") != 0
        or activity.get("downloaded_bytes") != 0
        or activity.get("accounting_complete") is not True
        or not isinstance(evidence.get("session_before_sha256"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", evidence["session_before_sha256"])
    ):
        raise RunnerError("stopped-worker evidence identity or stop condition is invalid")
    return evidence, evidence_path, evidence_raw


def _append_stopped_worker_audit(operator: Path, record: dict[str, Any]) -> dict[str, Any]:
    from kalshi_events.common import fsync_directory

    path = operator / "stopped-worker-reconciliation.jsonl"
    flags = os.O_CREAT | os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        import stat

        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_dev != operator.stat().st_dev
        ):
            raise RunnerError("stopped-worker audit log is unsafe")
        if info.st_size > MIB:
            raise RunnerError("stopped-worker audit log exceeded its byte bound")
        os.lseek(descriptor, 0, os.SEEK_SET)
        previous = os.read(descriptor, MIB + 1)
        if len(previous) > MIB:
            raise RunnerError("stopped-worker audit log exceeded its byte bound")
        for line in previous.splitlines():
            try:
                entry = json.loads(line.decode("utf-8"))
            except (UnicodeError, ValueError) as exc:
                raise RunnerError("stopped-worker audit log is malformed") from exc
            if not isinstance(entry, dict) or entry.get("version") != 1:
                raise RunnerError("stopped-worker audit log is malformed")
            if entry.get("event_id") == record["event_id"]:
                if {key: value for key, value in entry.items() if key != "reconciled_at"} != {
                    key: value for key, value in record.items() if key != "reconciled_at"
                }:
                    raise RunnerError("stopped-worker audit event conflicts with its prior record")
                return entry
            if entry.get("session_id") == record["session_id"]:
                raise RunnerError(
                    "this stopped session already has a different reconciliation audit"
                )
        body = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
        remaining = memoryview(body)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise RunnerError("stopped-worker audit append was incomplete")
            remaining = remaining[written:]
        os.fsync(descriptor)
        if info.st_size == 0:
            fsync_directory(operator)
        return record
    finally:
        os.close(descriptor)


def _reconcile_stopped_worker(directory: Path, capture_id: str) -> dict[str, Any]:
    from kalshi_events.common import regular_bytes, safe_path

    directory = prepare_directory(directory, "historical")
    operator = directory / "operator"
    with _run_lock(directory):
        run_root = directory / "runs"
        matches = []
        for candidate in run_root.glob(f"*/{REPORT_NAME}"):
            try:
                candidate = safe_path(candidate)
            except Exception as exc:
                raise RunnerError("session report path is unsafe") from exc
            report = _strict_json(candidate)
            if report is not None and report.get("capture_id") == capture_id:
                matches.append((candidate, report))
        if len(matches) != 1:
            raise RunnerError("stopped-worker reconciliation requires one exact capture report")
        report_path, report = matches[0]
        group = report.get("active_process_group")
        if (
            not isinstance(report.get("session_id"), str)
            or not CAPTURE_ID.fullmatch(report["session_id"])
            or report["session_id"] != report_path.parent.name
            or report_path.parent.parent != run_root
            or type(group) is not int
            or group <= 1
            or report.get("phase") != "stopped"
            or report.get("production_state") == "verified"
            or not isinstance(report.get("error"), str)
            or report.get("boot_stamp") != _boot_stamp()
        ):
            raise RunnerError("session is not an exact stopped worker on the saved boot")
        root_id = report.get("root_id")
        if not isinstance(root_id, str) or not CAPTURE_ID.fullmatch(root_id):
            raise RunnerError("stopped session root identity is invalid")
        window = _validate_offline_historical_window(
            report,
            capture_id,
            allow_expired=True,
            allow_revision_change=True,
        )
        if window.get("root_id") != root_id or _owner_root_id(directory) != root_id:
            raise RunnerError("stopped session root identity does not match its owner marker")
        evidence, evidence_path, evidence_raw = _stopped_worker_evidence(
            directory, report_path, report, capture_id
        )
        supervisor_pid = evidence["supervisor_pid"]
        if (
            type(supervisor_pid) is not int
            or supervisor_pid <= 1
            or _pid_exists(group)
            or _process_group_exists(group)
            or _pid_exists(supervisor_pid)
            or _process_group_exists(supervisor_pid)
        ):
            raise RunnerError("stopped worker or original supervisor is still alive or ambiguous")
        raw_report = regular_bytes(report_path, 16 * MIB)
        report_hash = hashlib.sha256(raw_report).hexdigest()
        evidence_hash = hashlib.sha256(evidence_raw).hexdigest()
        event_identity = {
            "capture_id": capture_id,
            "session_id": report.get("session_id"),
            "root_id": root_id,
            "stale_process_group": group,
            "supervisor_pid": supervisor_pid,
            "boot_stamp": report["boot_stamp"],
            "revision": report.get("revision"),
            "report_sha256": report_hash,
            "evidence_path": str(evidence_path),
            "evidence_sha256": evidence_hash,
            "evidence_report_preimage_sha256": evidence["session_before_sha256"],
            "primary_stop_reason": report.get("error"),
        }
        event_id = hashlib.sha256(
            json.dumps(event_identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        audit = {
            "version": 1,
            "event_id": event_id,
            "reconciled_at": _utc(),
            **event_identity,
        }
        with _operator_writer_lock(operator) as writer_identity:
            _validate_offline_renewal_root(
                directory,
                operator,
                root_id,
                capture_id,
                writer_lock_identity=writer_identity,
            )
            if (
                _pid_exists(group)
                or _process_group_exists(group)
                or _pid_exists(supervisor_pid)
                or _process_group_exists(supervisor_pid)
            ):
                raise RunnerError("stopped worker or original supervisor reappeared")
            audit = _append_stopped_worker_audit(operator, audit)
            report["active_process_group"] = None
            report["stopped_worker_reconciliation"] = {
                "event_id": event_id,
                "audit_file": "stopped-worker-reconciliation.jsonl",
                "reconciled_at": audit["reconciled_at"],
                "stale_process_group": group,
                "supervisor_pid": supervisor_pid,
                "primary_stop_reason": report.get("error"),
                "termination_evidence_sha256": evidence_hash,
                "session_before_sha256": evidence["session_before_sha256"],
            }
            _atomic_report(report_path, report)
        return report


def _validate_offline_renewal_root(
    directory: Path,
    operator: Path,
    expected_root_id: str,
    capture_id: str,
    *,
    writer_lock_identity: tuple[int, int] | None = None,
) -> tuple[int, int]:

    def validate(lock_identity: tuple[int, int]) -> tuple[int, int]:
        import stat

        from kalshi_events.common import json_bytes, safe_path

        operator_details = os.stat(operator, follow_symlinks=False)
        if (
            not stat.S_ISDIR(operator_details.st_mode)
            or operator.resolve() != operator.absolute()
            or operator_details.st_dev != directory.stat().st_dev
        ):
            raise RunnerError("offline historical renewal requires the original operator root")
        marker = _strict_json(operator / "root.json")
        if marker != {"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}:
            raise RunnerError(
                "offline historical renewal requires a current unscoped operator root"
            )
        identity = _strict_json(operator / "root-id.json", MIB)
        root_id = identity.get("root_id") if isinstance(identity, dict) else None
        if (
            not isinstance(identity, dict)
            or set(identity) != {"version", "root_id"}
            or type(identity.get("version")) is not int
            or identity.get("version") != 1
            or not isinstance(root_id, str)
            or not CAPTURE_ID.fullmatch(root_id)
            or root_id != expected_root_id
            or _owner_root_id(directory) != expected_root_id
        ):
            raise RunnerError("offline historical renewal root identity does not match the session")
        capture_root = safe_path(operator / "captures" / capture_id)
        if not capture_root.is_dir() or not capture_root.is_relative_to(operator):
            raise RunnerError(
                "offline historical capture path does not exist under the operator root"
            )
        journal_path = safe_path(capture_root / "pages.jsonl")
        try:
            with journal_path.open("rb") as stream:
                genesis_bytes = stream.readline(16 * MIB + 1)
            if len(genesis_bytes) > 16 * MIB or not genesis_bytes.endswith(b"\n"):
                raise ValueError("missing or oversized journal genesis")

            def reject_constant(value: str) -> None:
                raise ValueError(value)

            genesis = json.loads(genesis_bytes, parse_constant=reject_constant)
        except (OSError, UnicodeError, ValueError) as exc:
            raise RunnerError("offline historical capture genesis is missing or invalid") from exc
        payload = genesis.get("payload") if isinstance(genesis, dict) else None
        state = payload.get("state") if isinstance(payload, dict) else None
        if (
            not isinstance(genesis, dict)
            or set(genesis) != {"version", "capture_id", "seq", "prev", "op", "payload", "sha256"}
            or type(genesis.get("version")) is not int
            or genesis.get("version") != 2
            or genesis.get("capture_id") != capture_id
            or type(genesis.get("seq")) is not int
            or genesis.get("seq") != 1
            or genesis.get("prev") != "0" * 64
            or genesis.get("op") != "start"
            or not isinstance(payload, dict)
            or set(payload) != {"state", "seen_cursors", "state_sha256"}
            or not isinstance(payload.get("seen_cursors"), dict)
            or not isinstance(payload.get("state_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", payload["state_sha256"])
            or not isinstance(state, dict)
            or type(state.get("version")) is not int
            or state.get("version") != 1
            or state.get("capture_id") != capture_id
            or ("root_id" in state and state.get("root_id") != expected_root_id)
        ):
            raise RunnerError(
                "offline historical capture genesis identity does not match the session"
            )
        checksum = genesis["sha256"]
        if (
            not isinstance(checksum, str)
            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
            or hashlib.sha256(
                json_bytes({key: value for key, value in genesis.items() if key != "sha256"})
            ).hexdigest()
            != checksum
        ):
            raise RunnerError("offline historical capture genesis checksum is invalid")
        try:
            operator_after = os.stat(operator, follow_symlinks=False)
            lock_after = os.stat(operator / "writer.lock", follow_symlinks=False)
        except OSError as exc:
            raise RunnerError(
                "operator root or writer lock changed during renewal validation"
            ) from exc
        if (
            (operator_after.st_dev, operator_after.st_ino)
            != (operator_details.st_dev, operator_details.st_ino)
            or (lock_after.st_dev, lock_after.st_ino) != lock_identity
            or not stat.S_ISREG(lock_after.st_mode)
            or lock_after.st_nlink != 1
        ):
            raise RunnerError("operator root or writer lock changed during renewal validation")
        return operator_details.st_dev, operator_details.st_ino

    if writer_lock_identity is not None:
        return validate(writer_lock_identity)
    else:
        with _operator_writer_lock(operator) as lock_identity:
            return validate(lock_identity)


def _resource_guard(
    directory: Path,
    operator: Path,
    *,
    report: dict[str, Any],
    deadline: float,
    paths: dict[str, Path],
    require_complete_accounting: bool = True,
    workflow_deadline: bool = False,
    capture_id: str | None = None,
    cleanup_historical_stages: bool = False,
) -> dict[str, Any]:
    """Run Store's only full accounting walk in a supervised boundary child."""
    if cleanup_historical_stages and (
        capture_id is None
        or not isinstance(report.get("root_id"), str)
        or report.get("active_process_group") is not None
    ):
        raise RunnerError("initial historical accounting cleanup is not eligible")
    started = time.monotonic()
    try:
        snapshot = _child_accounting(
            operator,
            deadline,
            paths,
            report,
            require_complete=require_complete_accounting,
            workflow_deadline=workflow_deadline,
            capture_id=capture_id,
            cleanup_historical_stages=cleanup_historical_stages,
            expected_root_id=(report.get("root_id") if cleanup_historical_stages else None),
        )
    finally:
        phases = report.setdefault("phase_seconds", {})
        phases["accounting"] = phases.get("accounting", 0) + time.monotonic() - started
    external = _external_bytes(directory)
    storage = snapshot.get("storage")
    if not isinstance(storage, dict):
        raise RunnerError("operator storage accounting is unknown")
    retained = storage.get("retained_bytes")
    temporary = storage.get("temporary_bytes")
    database = storage.get("database_bytes")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (retained, temporary, database)
    ):
        raise RunnerError("operator storage accounting is invalid")
    physical_estimate = retained + external
    if (
        retained > int(_effective_limits()["root_bytes"])
        or temporary > int(_effective_limits()["temporary_bytes"])
        or database > int(_effective_limits()["database_bytes"])
        or physical_estimate > int(_effective_limits()["aggregate_bytes"])
    ):
        raise BudgetStop("production storage allowance was exceeded")
    free = shutil.disk_usage(directory).free
    if free < int(_effective_limits()["minimum_free_bytes"]):
        raise BudgetStop("SSD free space fell below 150 GiB")
    report["storage"] = {
        "operator_retained_bytes": retained,
        "operator_temporary_bytes": temporary,
        "operator_database_bytes": database,
        "external_bytes": external,
        "aggregate_estimate_bytes": physical_estimate,
        "ssd_free_bytes": free,
    }
    return snapshot


def _current_warehouse_path(operator: Path) -> Path | None:
    pointer = _strict_json(operator / "current-v2.json")
    if not isinstance(pointer, dict):
        return None
    release_id = pointer.get("source_release_id")
    if not isinstance(release_id, str) or not re.fullmatch(r"[0-9a-f]{64}", release_id):
        return None
    manifest = _strict_json(operator / "releases" / release_id / "manifest.json")
    capture_id = manifest.get("capture_id") if isinstance(manifest, dict) else None
    if not isinstance(capture_id, str) or not CAPTURE_ID.fullmatch(capture_id):
        return None
    candidate = operator / "builds-v2" / capture_id / "warehouse.duckdb"
    return candidate if candidate.is_file() and not candidate.is_symlink() else None


def _check_accounting_match(snapshot: dict[str, Any], expected: dict[str, Any]) -> None:
    expected_bytes = expected.get(
        "downloaded_bytes_upper_bound"
        if expected.get("accounting_basis") == "reserved-upper-bound"
        else "downloaded_bytes"
    )
    if (
        snapshot.get("accounting_complete") is not expected.get("accounting_complete", True)
        or snapshot.get("attempts") != expected.get("attempts", expected.get("requests"))
        or snapshot.get("downloaded_or_reserved_bytes") != expected_bytes
    ):
        raise RunnerError(
            "durable receipt accounting is unknown or differs from the invocation ledger"
        )


def _monitor_deadline(report: dict[str, Any], *, workflow: bool) -> float:
    ceiling = float(
        _effective_limits()["workflow_seconds"]
        if workflow
        else _effective_limits()["active_cutoff_seconds"]
    )
    if _ACTIVE_OFFLINE_HISTORICAL_WINDOW is not None:
        window = _ACTIVE_OFFLINE_HISTORICAL_WINDOW
        wall_remaining = float(window["deadline_epoch"]) - time.time()
        reserve = _offline_workflow_reserve(window) if workflow else 0.0
        return min(
            float(window["deadline_monotonic"]) - reserve,
            float(window["started_monotonic"]) + ceiling,
            time.monotonic() + max(0.0, wall_remaining - reserve),
        )
    monotonic_deadline = min(
        _time_budget_started(report, "monotonic") + ceiling,
        _time_budget_deadline(report, "monotonic"),
    )
    wall_deadline = min(
        _time_budget_started(report, "epoch") + ceiling,
        _time_budget_deadline(report, "epoch"),
    )
    wall_remaining = wall_deadline - time.time()
    return min(monotonic_deadline, time.monotonic() + max(0.0, wall_remaining))


def _child_wall_deadline_epoch(
    report: dict[str, Any], monotonic_deadline: float, *, workflow_deadline: bool = False
) -> float | None:
    """Seal a child wall deadline from the effective session and active-stage caps."""
    if _ACTIVE_OFFLINE_HISTORICAL_WINDOW is not None:
        window = _ACTIVE_OFFLINE_HISTORICAL_WINDOW
        stage_seconds = float(
            _effective_limits()["workflow_seconds"]
            if workflow_deadline
            else _effective_limits()["active_cutoff_seconds"]
        )
        reserve = _offline_workflow_reserve(window) if workflow_deadline else 0.0
        wall_now = time.time()
        return min(
            float(window["deadline_epoch"]) - reserve,
            float(window["started_epoch"]) + stage_seconds,
            wall_now + max(0.0, monotonic_deadline - time.monotonic()),
        )
    if "time_budget_started_epoch" in report:
        started = _time_budget_started(report, "epoch")
    else:
        value = report.get("started_epoch")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        if not math.isfinite(value):
            raise RunnerError("session time budget start is invalid")
        started = float(value)
    stage_seconds = float(
        _effective_limits()["workflow_seconds"]
        if workflow_deadline
        else _effective_limits()["active_cutoff_seconds"]
    )
    wall_now = time.time()
    deadlines = [
        started + stage_seconds,
        started + float(_effective_limits()["active_cutoff_seconds"]),
        wall_now + max(0.0, monotonic_deadline - time.monotonic()),
    ]
    deadlines.append(_time_budget_deadline(report, "epoch"))
    return min(deadlines)


def _offline_workflow_reserve(window: dict[str, Any]) -> float:
    """Keep the final twelfth, up to two hours, for offline verification and activation."""
    duration = min(
        float(window["deadline_epoch"]) - float(window["started_epoch"]),
        float(window["deadline_monotonic"]) - float(window["started_monotonic"]),
    )
    if not math.isfinite(duration) or duration <= 0:
        raise RunnerError("offline historical window duration is invalid")
    return min(2 * 3600.0, duration / 12.0)


def _offline_historical_window_bounds(
    report: dict[str, Any],
    capture_id: str,
    window: Any,
    *,
    require_current_revision: bool,
) -> tuple[float, float, float, float]:
    if not isinstance(window, dict):
        raise RunnerError("offline historical window is invalid")
    expected = {
        "version",
        "capture_id",
        "root_id",
        "revision",
        "started_at",
        "started_epoch",
        "started_monotonic",
        "deadline_epoch",
        "deadline_monotonic",
    }
    if (
        set(window) != expected
        or type(window.get("version")) is not int
        or window["version"] not in {1, 2}
    ):
        raise RunnerError("offline historical window is invalid")
    root_id = report.get("root_id")
    revision = window.get("revision")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        raise RunnerError("offline historical window revision is invalid")
    if (
        window.get("capture_id") != capture_id
        or window.get("root_id") != root_id
        or not isinstance(root_id, str)
        or not CAPTURE_ID.fullmatch(root_id)
        or (require_current_revision and revision != _revision())
        or not isinstance(window.get("started_at"), str)
    ):
        raise RunnerError("offline historical window identity does not match this session")
    try:
        started_at = datetime.fromisoformat(window["started_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise RunnerError("offline historical window start time is invalid") from exc
    if started_at.tzinfo is None or started_at.utcoffset() != timedelta(0):
        raise RunnerError("offline historical window start time is invalid")
    for field in (
        "started_epoch",
        "started_monotonic",
        "deadline_epoch",
        "deadline_monotonic",
    ):
        value = window.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise RunnerError("offline historical window deadline is invalid")
    started_epoch = float(window["started_epoch"])
    started_monotonic = float(window["started_monotonic"])
    deadline_epoch = float(window["deadline_epoch"])
    deadline_monotonic = float(window["deadline_monotonic"])
    epoch_duration = deadline_epoch - started_epoch
    monotonic_duration = deadline_monotonic - started_monotonic
    max_duration = (
        MAX_LEGACY_OFFLINE_HISTORICAL_SECONDS
        if window["version"] == 1
        else MAX_OFFLINE_HISTORICAL_SECONDS
    )
    if (
        epoch_duration <= 0
        or deadline_epoch > started_epoch + max_duration
        or monotonic_duration <= 0
        or deadline_monotonic > started_monotonic + max_duration
        or abs(epoch_duration - monotonic_duration) > 1.0
    ):
        raise RunnerError("offline historical window duration is invalid")
    return started_epoch, deadline_epoch, started_monotonic, deadline_monotonic


def _validate_offline_historical_window_history(
    report: dict[str, Any], capture_id: str, *, allow_old_current_revision: bool = False
) -> list[dict[str, Any]]:
    history = report.get("offline_historical_window_history", [])
    current = report.get("offline_historical_window")
    if not isinstance(history, list) or not isinstance(current, dict):
        raise RunnerError("offline historical window history is invalid")
    windows = [*history, current]
    bounds = [
        _offline_historical_window_bounds(
            report,
            capture_id,
            item,
            require_current_revision=(item is current and not allow_old_current_revision),
        )
        for item in windows
    ]
    supersessions = report.get("offline_historical_window_supersessions", [])
    if not isinstance(supersessions, list):
        raise RunnerError("offline historical window supersession history is invalid")
    superseded_indices: set[int] = set()
    fields = {
        "history_index",
        "window_started_epoch",
        "window_started_monotonic",
        "window_deadline_epoch",
        "window_deadline_monotonic",
        "successor_started_epoch",
        "successor_started_monotonic",
    }
    for transition in supersessions:
        if (
            not isinstance(transition, dict)
            or set(transition) != fields
            or type(transition.get("history_index")) is not int
        ):
            raise RunnerError("offline historical window supersession history is invalid")
        index = transition["history_index"]
        if index < 0 or index >= len(history) or index in superseded_indices:
            raise RunnerError("offline historical window supersession history is invalid")
        values = [transition[field] for field in fields - {"history_index"}]
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in values
        ):
            raise RunnerError("offline historical window supersession history is invalid")
        previous, successor = bounds[index], bounds[index + 1]
        expected = {
            "window_started_epoch": previous[0],
            "window_deadline_epoch": previous[1],
            "window_started_monotonic": previous[2],
            "window_deadline_monotonic": previous[3],
            "successor_started_epoch": successor[0],
            "successor_started_monotonic": successor[2],
        }
        if any(transition[field] != value for field, value in expected.items()):
            raise RunnerError("offline historical window supersession history is invalid")
        superseded_indices.add(index)

    for index, (previous, successor) in enumerate(zip(bounds[:-1], bounds[1:], strict=True)):
        if successor[0] < previous[0] or successor[2] < previous[2]:
            raise RunnerError("offline historical window history is out of order")
        overlaps = successor[0] < previous[1] or successor[2] < previous[3]
        if overlaps != (index in superseded_indices):
            raise RunnerError("offline historical window history overlaps or is out of order")
    return history


def _validate_offline_historical_window(
    report: dict[str, Any],
    capture_id: str,
    *,
    allow_expired: bool = False,
    allow_revision_change: bool = False,
) -> dict[str, Any]:
    window = report.get("offline_historical_window")
    if not isinstance(window, dict):
        raise RunnerError("offline historical window is invalid")
    bounds = _offline_historical_window_bounds(
        report,
        capture_id,
        window,
        require_current_revision=not allow_revision_change,
    )
    _validate_offline_historical_window_history(
        report, capture_id, allow_old_current_revision=allow_revision_change
    )
    remaining = min(
        bounds[1] - time.time(),
        bounds[3] - time.monotonic(),
    )
    if remaining <= 0 and not allow_expired:
        raise RunnerError("offline historical window expired")
    return window


def _configure_offline_historical_window(
    report: dict[str, Any],
    capture_id: str,
    duration_seconds: float | None,
    *,
    renew: bool = False,
    supersede: bool = False,
) -> dict[str, Any] | None:
    duration = _validate_offline_duration_seconds(duration_seconds)
    if supersede and not renew:
        raise RunnerError("offline historical window supersession requires renewal")
    existing = report.get("offline_historical_window")
    superseded_window = None
    early_supersede = False
    if renew:
        if duration is None or not isinstance(existing, dict):
            raise RunnerError("offline historical window renewal requires an existing window")
        _validate_offline_historical_window(
            report,
            capture_id,
            allow_expired=True,
            allow_revision_change=True,
        )
        expired = time.time() >= float(existing["deadline_epoch"]) and time.monotonic() >= float(
            existing["deadline_monotonic"]
        )
        if not expired and not supersede:
            raise RunnerError("offline historical window renewal requires both clocks to expire")
        early_supersede = not expired
        superseded_window = dict(existing)
        history = list(report.get("offline_historical_window_history", []))
        history.append(superseded_window)
        report["offline_historical_window_history"] = history
        report["offline_historical_window"] = None
        existing = None
    if existing is not None:
        return _validate_offline_historical_window(report, capture_id)
    if duration is None:
        return None
    root_id = report.get("root_id")
    if not isinstance(root_id, str) or not CAPTURE_ID.fullmatch(root_id):
        raise RunnerError("offline historical window requires a verified session root")
    revision = _revision()
    if not REVISION.fullmatch(revision):
        raise RunnerError("offline historical window requires a verified source revision")
    started_epoch = time.time()
    started_monotonic = time.monotonic()
    report["offline_historical_window"] = {
        "version": 2,
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": revision,
        "started_at": _utc(),
        "started_epoch": started_epoch,
        "started_monotonic": started_monotonic,
        "deadline_epoch": started_epoch + duration,
        "deadline_monotonic": started_monotonic + duration,
    }
    if early_supersede:
        history_index = len(report["offline_historical_window_history"]) - 1
        supersessions = list(report.get("offline_historical_window_supersessions", []))
        supersessions.append(
            {
                "history_index": history_index,
                "window_started_epoch": superseded_window["started_epoch"],
                "window_started_monotonic": superseded_window["started_monotonic"],
                "window_deadline_epoch": superseded_window["deadline_epoch"],
                "window_deadline_monotonic": superseded_window["deadline_monotonic"],
                "successor_started_epoch": started_epoch,
                "successor_started_monotonic": started_monotonic,
            }
        )
        report["offline_historical_window_supersessions"] = supersessions
    return _validate_offline_historical_window(report, capture_id)


def _child_time_remaining(
    monotonic_deadline: float,
    wall_deadline_epoch: float | None,
    wall_now: float | None = None,
) -> float:
    remaining = monotonic_deadline - time.monotonic()
    if wall_deadline_epoch is not None:
        remaining = min(
            remaining,
            wall_deadline_epoch - (time.time() if wall_now is None else wall_now),
        )
    return remaining


def _effective_deadline_expired(report: dict[str, Any]) -> bool:
    if _ACTIVE_OFFLINE_HISTORICAL_WINDOW is not None:
        window = _ACTIVE_OFFLINE_HISTORICAL_WINDOW
        return (
            min(
                float(window["deadline_epoch"]) - time.time(),
                float(window["deadline_monotonic"]) - time.monotonic(),
            )
            <= 0
        )
    return time.time() >= _time_budget_deadline(report, "epoch")


def _run_external_child(
    command: list[str],
    *,
    directory: Path,
    operator: Path,
    paths: dict[str, Path],
    report: dict[str, Any],
    deadline: float,
    label: str,
    capture_id: str | None = None,
    preserve_phase_until_start: bool = False,
    storage_supervision: bool = False,
) -> tuple[int | None, str | None, bool, Path, Path]:
    stdout = paths["logs"] / f"{label}.stdout.json"
    stderr = paths["logs"] / f"{label}.stderr.log"
    notification = paths.get("capture_notification") if label == "refresh" else None
    if notification is not None:
        with contextlib.suppress(FileNotFoundError):
            notification.unlink()
    if not preserve_phase_until_start:
        report["phase"] = label
        _atomic_report(paths["report"], report)
    started = time.monotonic()
    storage_progress = _stage_storage_progress_path(paths, label)
    storage_token = uuid.uuid4().hex if storage_supervision else None
    status = _invoke(
        command,
        directory=directory,
        operator=operator,
        report=report,
        report_path=paths["report"],
        deadline=deadline,
        stdout_path=stdout,
        stderr_path=stderr,
        capture_id=capture_id,
        capture_notification=notification,
        workflow_deadline=label == "refresh",
        phase_on_start=label if preserve_phase_until_start else None,
        storage_supervision=storage_supervision,
        storage_invocation_token=storage_token,
        metrics_progress_path=paths.get("metrics_progress"),
        storage_progress_path=storage_progress if storage_supervision else None,
    )
    if storage_supervision and report.get("storage_supervision_invocation") == storage_token:
        if not storage_progress.exists() and not storage_progress.is_symlink():
            raise RunnerError("live storage report disappeared after child start")
        storage_result = _strict_json(storage_progress)
        if storage_result is None or storage_result.get("version") != 1:
            raise RunnerError("live storage supervision report is missing or malformed")
        report["storage_supervision_file"] = str(storage_progress)
        report["storage_supervision"] = {
            key: value for key, value in storage_result.items() if key != "baseline"
        }
    report["phase_seconds"][label] = report["phase_seconds"].get(label, 0) + (
        time.monotonic() - started
    )
    report["last_observed_at"] = _utc()
    report["last_observed_epoch"] = time.time()
    _atomic_report(paths["report"], report)
    return (*status, stdout, stderr)


def _kill_awake_guard(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=3)


@_resource_profile_scope
def run_production(
    directory: Path,
    *,
    full: bool = False,
    capture_id: str | None = None,
    activate_only: bool = False,
    historical: bool = False,
    offline_duration_seconds: float | None = None,
    renew_offline_window: bool = False,
    supersede_offline_window: bool = False,
    reconcile_accounting: bool = False,
    extend_session: bool = False,
    recover_reboot: bool = False,
    recovery_volume_uuid: str | None = None,
    reconcile_stopped_worker: bool = False,
    resource_profile: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Run one bounded catalogue refresh/resume or verify-and-activate a publication."""
    preflight_started = time.monotonic()
    mode = validate_mode(
        full,
        capture_id,
        activate_only,
        historical,
        offline_duration_seconds=offline_duration_seconds,
        renew_offline_window=renew_offline_window,
        supersede_offline_window=supersede_offline_window,
        resource_profile=resource_profile,
    )
    if reconcile_stopped_worker:
        if (
            mode != "historical"
            or resource_profile is not None
            or offline_duration_seconds is not None
            or renew_offline_window
            or supersede_offline_window
            or reconcile_accounting
            or extend_session
            or recover_reboot
            or recovery_volume_uuid is not None
        ):
            raise RunnerError("--reconcile-stopped-worker requires only --historical --capture-id")
        return _reconcile_stopped_worker(directory, str(capture_id)), 0
    _validate_runtime_versions(_package_versions())
    if reconcile_accounting and mode != "resume":
        raise RunnerError("accounting reconciliation requires --capture-id resume")
    if extend_session and (mode != "resume" or not reconcile_accounting):
        raise RunnerError("session extension requires --capture-id and --reconcile-accounting")
    if recover_reboot and (
        mode != "resume"
        or not reconcile_accounting
        or extend_session
        or recovery_volume_uuid is None
    ):
        raise RunnerError(
            "reboot recovery requires --capture-id, --reconcile-accounting and "
            "--recovery-volume-uuid, and cannot extend the session"
        )
    if recovery_volume_uuid is not None and not recover_reboot:
        raise RunnerError("--recovery-volume-uuid requires --recover-reboot")
    directory = prepare_directory(directory, mode)
    operator = directory / "operator"
    if mode == "full":
        run_root = directory / "runs"
        if not run_root.exists():
            run_root.mkdir()
    lock_context = _run_lock(directory)
    with lock_context:
        if mode in {"resume", "historical"}:
            report_path, report, paths = _load_resume(
                directory,
                str(capture_id),
                historical=mode == "historical",
                offline_duration_seconds=(
                    offline_duration_seconds if mode == "historical" else None
                ),
                renew_offline_window=renew_offline_window,
                supersede_offline_window=supersede_offline_window,
                reconcile_accounting=reconcile_accounting,
                extend_session=extend_session,
                recover_reboot=recover_reboot,
                recovery_volume_uuid=recovery_volume_uuid,
                resource_profile=resource_profile,
            )
            profile = report.get("resource_profile", "default")
            _ACTIVE_LIMITS.set(_limits_from_report(report, profile))
            if reconcile_accounting:
                saved_system = report["system"]
                report.setdefault("original_revision", report.get("revision"))
                report.setdefault("original_source_root", report.get("source_root"))
                report.setdefault("original_system", saved_system)
                report["recovery_revision"] = _revision()
                report["recovery_source_root"] = str(SOURCE_ROOT)
                report["accounting_reconciliation"] = {
                    "explicit": True,
                    "started_at": _utc(),
                    "prior_exactness_preserved": False,
                    "prior_error": (
                        report.get("previous_stop", {}).get("error")
                        if isinstance(report.get("previous_stop"), dict)
                        else report.get("error")
                    ),
                    "prior_recovery_instructions": (
                        report.get("previous_stop", {}).get("recovery")
                        if isinstance(report.get("previous_stop"), dict)
                        else report.get("recovery")
                    ),
                    "runtime_transition": {
                        "from_uv_lock_sha256": saved_system["uv_lock_sha256"],
                    },
                }
            paths = _prepare_run_paths(directory, report_path)
            report["metrics_file"] = str(paths["metrics"])
            report["metrics_progress_file"] = str(paths["metrics_progress"])
            report["storage_progress_file"] = str(paths["storage_progress"])
            if report.get("root_id") is None:
                raise RunnerError("resume root identity was not durably recorded")
        else:
            report_path, report, paths = _session_report(directory, mode)
            profile = report.get("resource_profile", "default")
            _ACTIVE_LIMITS.set(_limits_from_report(report, profile))
            paths = _prepare_run_paths(directory, report_path)
            report["metrics_file"] = str(paths["metrics"])
            report["metrics_progress_file"] = str(paths["metrics_progress"])
            report["storage_progress_file"] = str(paths["storage_progress"])
        phases = report.setdefault("phase_seconds", {})
        phases["preflight"] = phases.get("preflight", 0) + time.monotonic() - preflight_started
        result: dict[str, Any] = report
        if mode in {"activate-current", "activate-historical"}:
            report["requests"] = None
            report["downloaded_bytes"] = None
            report["accounting_complete"] = False
            report["network_activity_this_invocation"] = {
                "requests": 0,
                "downloaded_bytes": 0,
                "accounting_complete": True,
            }
        report["revision"] = _revision()
        report["source_root"] = str(SOURCE_ROOT)
        prior_volume_uuid = report.get("system", {}).get("filesystem_volume_uuid")
        report["system"] = _system_info(directory)
        if recovery_volume_uuid is not None and recover_reboot:
            report["system"]["filesystem_volume_uuid"] = recovery_volume_uuid.upper()
        elif isinstance(prior_volume_uuid, str):
            report["system"]["filesystem_volume_uuid"] = prior_volume_uuid
        if reconcile_accounting:
            report["accounting_reconciliation"]["runtime_transition"].update(
                {
                    "to_uv_lock_sha256": hashlib.sha256(
                        (SOURCE_ROOT / "uv.lock").read_bytes()
                    ).hexdigest(),
                    "transport_sha256": _transport_source_sha256(),
                }
            )
        if mode in {"historical", "activate-historical"}:
            report["network_activity_this_invocation"] = {
                "requests": 0,
                "downloaded_bytes": 0,
                "accounting_complete": True,
            }
        else:
            from kalshi_events.transport import BASE_URL, source_url

            report["api_origin"] = source_url()
            if mode != "activate-current" and report["api_origin"] != BASE_URL:
                raise RunnerError("production acquisition must use the public Kalshi API origin")
        report["last_observed_epoch"] = time.time()
        report["last_observed_at"] = _utc()
        _atomic_report(report_path, report)
        if shutil.disk_usage(directory).free < int(_effective_limits()["minimum_free_bytes"]):
            report["error"] = "SSD free space is below 150 GiB before work"
            report["phase"] = "stopped"
            _atomic_report(report_path, report)
            return result, 3
        offline_window = None
        if mode == "historical":
            if renew_offline_window or supersede_offline_window:
                with _operator_writer_lock(operator) as writer_lock_identity:
                    operator_identity = _validate_offline_renewal_root(
                        directory,
                        operator,
                        str(report["root_id"]),
                        str(capture_id),
                        writer_lock_identity=writer_lock_identity,
                    )
                    offline_window = _configure_offline_historical_window(
                        report,
                        str(capture_id),
                        offline_duration_seconds,
                        renew=True,
                        supersede=supersede_offline_window,
                    )
                    try:
                        operator_after = os.stat(operator, follow_symlinks=False)
                        lock_after = os.stat(operator / "writer.lock", follow_symlinks=False)
                    except OSError as exc:
                        raise RunnerError(
                            "operator root or writer lock changed before the renewal grant"
                        ) from exc
                    if (
                        (operator_after.st_dev, operator_after.st_ino) != operator_identity
                        or (lock_after.st_dev, lock_after.st_ino) != writer_lock_identity
                        or lock_after.st_nlink != 1
                    ):
                        raise RunnerError(
                            "operator root or writer lock changed before the renewal grant"
                        )
                    _atomic_report(report_path, report)
            else:
                offline_window = _configure_offline_historical_window(
                    report, str(capture_id), offline_duration_seconds
                )
                if offline_window is not None:
                    _atomic_report(report_path, report)
        if offline_window is not None:
            reserve = _offline_workflow_reserve(offline_window)
            report["offline_workflow_reserve_seconds"] = reserve
            report["offline_workflow_deadline_epoch"] = (
                float(offline_window["deadline_epoch"]) - reserve
            )
            report["offline_workflow_deadline_monotonic"] = (
                float(offline_window["deadline_monotonic"]) - reserve
            )
            _atomic_report(report_path, report)
        awake = subprocess.Popen(
            ["caffeinate", "-is", "-w", str(os.getpid())],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        global _ACTIVE_OFFLINE_HISTORICAL_WINDOW
        _ACTIVE_OFFLINE_HISTORICAL_WINDOW = offline_window
        try:
            if mode not in {"activate-current", "activate-historical"} and _monitor_deadline(
                report, workflow=True
            ) <= time.monotonic() + float(_effective_limits()["terminate_seconds"]):
                raise BudgetStop("workflow deadline reached before supervised work could start")
            session_deadline = _monitor_deadline(report, workflow=False)
            standalone_result: dict[str, Any] | None = None
            if mode not in {"activate-current", "activate-historical"}:
                snapshot = None
                if mode == "historical":
                    snapshot = _resource_guard(
                        directory,
                        operator,
                        report=report,
                        deadline=session_deadline,
                        paths=paths,
                        require_complete_accounting=False,
                        workflow_deadline=True,
                        capture_id=capture_id,
                        cleanup_historical_stages=True,
                    )
                    if snapshot.get("root_id") != report.get("root_id"):
                        raise RunnerError("operator root identity changed since this session began")
                    if report.get("accounting_complete") is True or (
                        report.get("accounting_complete") is False
                        and report.get("accounting_basis") == "reserved-upper-bound"
                        and isinstance(report.get("requests"), int)
                        and isinstance(report.get("downloaded_bytes_upper_bound"), int)
                    ):
                        _check_accounting_match(snapshot, report)
                    prior = _reconciliation_accounting(snapshot, str(capture_id))
                    _check_accounting_match(snapshot, prior)
                    report.update(
                        requests=prior["attempts"],
                        downloaded_bytes=prior.get("downloaded_bytes"),
                        downloaded_bytes_upper_bound=prior.get("downloaded_bytes_upper_bound"),
                        accounting_complete=prior["accounting_complete"],
                        accounting_basis=prior["accounting_basis"],
                        accounting_bound_verified=prior["accounting_bound_verified"],
                        unfinished_reservation_ids=prior.get("unfinished_reservation_ids", []),
                        consumption_state=prior["accounting_basis"],
                    )
                elif mode == "resume":
                    if reconcile_accounting:
                        snapshot = _resource_guard(
                            directory,
                            operator,
                            report=report,
                            deadline=session_deadline,
                            paths=paths,
                            require_complete_accounting=False,
                            workflow_deadline=True,
                            capture_id=capture_id,
                        )
                        if snapshot.get("root_id") != report.get("root_id"):
                            raise RunnerError(
                                "operator root identity changed since this session began"
                            )
                        prior = _reconciliation_accounting(snapshot, str(capture_id))
                        _check_accounting_match(snapshot, prior)
                        report["accounting_basis"] = prior["accounting_basis"]
                        report["accounting_bound_verified"] = prior["accounting_bound_verified"]
                        report["unfinished_reservation_ids"] = prior["unfinished_reservation_ids"]
                        report["requests"] = prior["attempts"]
                        report["downloaded_bytes"] = prior.get("downloaded_bytes")
                        report["downloaded_bytes_upper_bound"] = prior.get(
                            "downloaded_bytes_upper_bound"
                        )
                        report["accounting_complete"] = prior["accounting_complete"]
                        report["consumption_state"] = prior["accounting_basis"]
                        report["accounting_reconciliation"].update(
                            {
                                "prior_exactness_preserved": prior["accounting_complete"],
                                "basis": prior["accounting_basis"],
                            }
                        )
                    else:
                        prior = {
                            "attempts": report.get("requests"),
                            "downloaded_bytes": report.get("downloaded_bytes"),
                            "accounting_complete": report.get("accounting_complete"),
                        }
                        remaining_allowances(
                            prior,
                            started_at=_time_budget_started(report, "monotonic"),
                            started_epoch=_time_budget_started(report, "epoch"),
                            wall_now=time.time(),
                            external_reserved_bytes=_external_bytes(directory),
                        )
                if mode == "resume" and snapshot is None:
                    snapshot = _resource_guard(
                        directory,
                        operator,
                        report=report,
                        deadline=session_deadline,
                        paths=paths,
                        workflow_deadline=True,
                    )
                if snapshot is not None:
                    if not reconcile_accounting:
                        _check_accounting_match(snapshot, report)
                    if snapshot.get("root_id") != report.get("root_id"):
                        raise RunnerError("operator root identity changed since this session began")
                if not reconcile_accounting and mode != "historical":
                    prior = (
                        _accounting_shape(snapshot)
                        if snapshot is not None
                        else {
                            "attempts": 0,
                            "downloaded_bytes": 0,
                            "accounting_complete": True,
                        }
                    )
                if mode == "historical":
                    workflow_deadline = _monitor_deadline(report, workflow=True)
                    refresh_deadline = min(workflow_deadline, session_deadline)
                    duration = max(
                        0.0,
                        refresh_deadline - time.monotonic(),
                    )
                    allowances = {
                        "max-requests": int(_effective_limits()["attempts"]),
                        "max-download-bytes": int(_effective_limits()["downloaded_bytes"]),
                        "max-root-bytes": int(_effective_limits()["root_bytes"]),
                        "max-temp-bytes": int(_effective_limits()["temporary_bytes"]),
                        "max-database-bytes": int(_effective_limits()["database_bytes"]),
                        "max-response-bytes": int(_effective_limits()["response_bytes"]),
                        "max-decoded-response-bytes": int(
                            _effective_limits()["decoded_response_bytes"]
                        ),
                        "max-duration-seconds": duration,
                        "max-rate": float(_effective_limits()["rate"]),
                        "compression": str(_effective_limits()["compression"]),
                        "rate-policy": str(_effective_limits()["rate_policy"]),
                    }
                else:
                    refresh_deadline = None
                    allowances = remaining_allowances(
                        prior,
                        started_at=_time_budget_started(report, "monotonic"),
                        external_reserved_bytes=_external_bytes(directory),
                        started_epoch=_time_budget_started(report, "epoch"),
                        wall_now=time.time(),
                    )
                    duration = min(
                        float(allowances["max-duration-seconds"]),
                        max(0.0, session_deadline - time.monotonic()),
                    )
                    allowances["max-duration-seconds"] = duration
                command = _refresh_command(
                    operator,
                    paths,
                    mode,
                    capture_id,
                    allowances,
                    str(report.get("resource_profile", "default")),
                )
                if reconcile_accounting:
                    report["error"] = None
                    report["recovery"] = None
                report["previous_accounting"] = {
                    "requests": prior["attempts"],
                    "downloaded_bytes": prior.get("downloaded_bytes"),
                    "downloaded_bytes_upper_bound": prior.get("downloaded_bytes_upper_bound"),
                    "accounting_complete": prior.get("accounting_complete", True),
                    "accounting_basis": prior.get("accounting_basis", "exact"),
                }
                if mode != "historical":
                    report["accounting_complete"] = False
                    report["requests"] = None
                    report["downloaded_bytes"] = None
                    report["consumption_state"] = "in_progress"
                code, stop_reason, reaped, stdout_path, stderr_path = _run_external_child(
                    command,
                    directory=directory,
                    operator=operator,
                    paths=paths,
                    report=report,
                    deadline=(
                        refresh_deadline
                        if refresh_deadline is not None
                        else time.monotonic() + duration
                    ),
                    label="refresh",
                    capture_id=capture_id,
                    preserve_phase_until_start=(mode == "historical"),
                    storage_supervision=(mode == "historical"),
                )
                cli_result = _read_cli_result(stdout_path)
                observed_capture = (cli_result or {}).get("capture_id") or report.get("capture_id")
                if observed_capture:
                    report["capture_id"] = observed_capture
                report["refresh_result"] = cli_result
                report["refresh_logs"] = {"stdout": str(stdout_path), "stderr": str(stderr_path)}
                report["acquisition_metrics"] = _strict_json(paths["metrics"])
                if code is None and reaped and stop_reason in NO_CHILD_START_REASONS:
                    report.update(
                        requests=prior["attempts"],
                        downloaded_bytes=prior.get("downloaded_bytes"),
                        downloaded_bytes_upper_bound=prior.get("downloaded_bytes_upper_bound"),
                        accounting_complete=prior["accounting_complete"],
                        accounting_basis=prior.get("accounting_basis", "exact"),
                        accounting_bound_verified=prior.get("accounting_bound_verified", False),
                        unfinished_reservation_ids=prior.get("unfinished_reservation_ids", []),
                        consumption_state=prior.get("accounting_basis", "verified"),
                        network_activity_this_invocation={
                            "requests": 0,
                            "downloaded_bytes": 0,
                            "accounting_complete": True,
                        },
                        error=stop_reason,
                        phase="stopped",
                    )
                    _atomic_report(report_path, report)
                    return result, 3
                if not reaped and not stop_reason:
                    stop_reason = "refresh process group could not be confirmed reaped"
                if stop_reason or cli_result is None or code not in {0, 1, 3}:
                    if mode != "historical":
                        report["accounting_complete"] = False
                        report["requests"] = None
                        report["downloaded_bytes"] = None
                        report["capture_complete"] = None
                    report["publication_state"] = "unknown"
                    report["scope_complete"] = False
                    report["error"] = stop_reason or (
                        "offline historical build did not return a complete report"
                        if mode == "historical"
                        else "refresh failed with unknown child accounting"
                    )
                    if mode != "historical":
                        report["resumable"] = None
                    report["phase"] = "stopped"
                    report["recovery"] = (
                        (
                            f"After investigating the stop, retry only this saved capture offline: "
                            f"--historical --capture-id {capture_id}, within its existing session deadline."
                        )
                        if mode == "historical"
                        else f"Preserve {operator}; do not resume until accounting is independently reconciled."
                    )
                    _atomic_report(report_path, report)
                    return result, 3 if stop_reason else 1
                source_capture_complete = cli_result.get(
                    "source_capture_complete", cli_result.get("capture_complete")
                )
                if not isinstance(source_capture_complete, bool):
                    raise RunnerError("refresh did not report source capture completeness")
                report["capture_complete"] = source_capture_complete
                report["source_capture_complete"] = source_capture_complete
                report["coverage_scope"] = cli_result.get("coverage_scope", "catalogue")
                report["scope_complete"] = (
                    cli_result.get(
                        "scope_complete",
                        cli_result.get("complete") is True and source_capture_complete,
                    )
                    is True
                )
                report["publication_state"] = cli_result.get("publication_state", "unknown")
                historical_profile = report["coverage_scope"] == "historical"
                historical_required = mode in {"full", "historical"}
                if code == 0 and historical_required and not historical_profile:
                    report["error"] = "historical scope is incomplete or has the wrong contract"
                    report["phase"] = "stopped"
                    _atomic_report(report_path, report)
                    return result, 1
                if (
                    code == 0
                    and historical_profile
                    and cli_result.get("scope_complete") is not True
                ):
                    report["error"] = "historical scope is incomplete or has the wrong contract"
                    report["phase"] = "stopped"
                    _atomic_report(report_path, report)
                    return result, 1
                if (
                    mode != "historical"
                    and cli_result.get("accounting_complete") is not True
                    and not reconcile_accounting
                ):
                    report["accounting_complete"] = False
                    report["error"] = "refresh reported incomplete acquisition accounting"
                    report["resumable"] = None
                    report["phase"] = "stopped"
                    _atomic_report(report_path, report)
                    return result, 1
                report["resumable"] = cli_result.get("resumable") is True
                if mode != "historical" and report["resumable"] is not True:
                    report["accounting_complete"] = False
                    report["error"] = "capture initialization or recovery state is uncertain"
                    report["phase"] = "stopped"
                    _atomic_report(report_path, report)
                    return result, 1
                # A short supervised Store/receipt walk confirms the durable ledger and actual storage.
                snapshot = _resource_guard(
                    directory,
                    operator,
                    report=report,
                    deadline=session_deadline,
                    paths=paths,
                    require_complete_accounting=(
                        mode != "historical"
                        and (not reconcile_accounting or prior.get("accounting_complete") is True)
                    ),
                    capture_id=capture_id if reconcile_accounting or mode == "historical" else None,
                )
                if mode == "historical":
                    after = _reconciliation_accounting(snapshot, str(capture_id))
                    if after != prior:
                        report.update(
                            accounting_complete=False,
                            accounting_basis="offline-historical-accounting-changed",
                            accounting_bound_verified=False,
                            consumption_state="unknown",
                        )
                        raise RunnerError("offline historical work changed acquisition accounting")
                    _check_accounting_match(snapshot, after)
                    if snapshot.get("root_id") != report.get("root_id"):
                        raise RunnerError("operator root identity changed during historical work")
                    report["network_activity_this_invocation"] = {
                        "requests": 0,
                        "downloaded_bytes": 0,
                        "accounting_complete": True,
                    }
                prior_charged_bytes = prior.get("downloaded_bytes_upper_bound")
                if prior_charged_bytes is None:
                    prior_charged_bytes = prior.get("downloaded_bytes", 0)
                if (
                    snapshot["attempts"] < prior["attempts"]
                    or snapshot["downloaded_or_reserved_bytes"] < prior_charged_bytes
                ):
                    raise RunnerError("durable receipt accounting decreased during the invocation")
                if reconcile_accounting:
                    if snapshot.get("root_id") != report.get("root_id"):
                        raise RunnerError("operator root identity changed since this session began")
                    if (
                        cli_result.get("requests") is None
                        or cli_result.get("downloaded_bytes") is None
                    ):
                        durable = _reconciliation_accounting(snapshot, str(capture_id))
                        report["requests"] = durable["attempts"]
                        report["downloaded_bytes"] = durable.get("downloaded_bytes")
                        report["downloaded_bytes_upper_bound"] = durable.get(
                            "downloaded_bytes_upper_bound"
                        )
                        report["accounting_complete"] = durable["accounting_complete"]
                        report["accounting_basis"] = durable["accounting_basis"]
                        report["accounting_bound_verified"] = durable["accounting_bound_verified"]
                        report["unfinished_reservation_ids"] = durable["unfinished_reservation_ids"]
                        report["consumption_state"] = durable["accounting_basis"]
                        raise RunnerError(
                            "resumed child did not report exact invocation accounting"
                        )
                    if prior["accounting_complete"]:
                        expected_after = {
                            "attempts": prior["attempts"] + int(cli_result["requests"]),
                            "downloaded_bytes": prior["downloaded_bytes"]
                            + int(cli_result["downloaded_bytes"]),
                            "accounting_complete": True,
                        }
                        _check_accounting_match(snapshot, expected_after)
                        report["requests"] = snapshot["attempts"]
                        report["downloaded_bytes"] = snapshot["downloaded_or_reserved_bytes"]
                        report["downloaded_bytes_upper_bound"] = None
                        report["accounting_basis"] = "exact"
                        report["accounting_bound_verified"] = False
                        report["unfinished_reservation_ids"] = []
                        report["accounting_complete"] = True
                        report["consumption_state"] = "verified"
                    else:
                        expected_after = {
                            "attempts": prior["attempts"] + int(cli_result["requests"]),
                            "downloaded_bytes_upper_bound": prior["downloaded_bytes_upper_bound"]
                            + int(cli_result["downloaded_bytes"]),
                            "accounting_complete": False,
                            "accounting_basis": "reserved-upper-bound",
                        }
                        bound = _reserved_upper_bound(snapshot, str(report["capture_id"]))
                        if (
                            bound["reservation_inventory_sha256"]
                            != prior["reservation_inventory_sha256"]
                        ):
                            raise RunnerError(
                                "unfinished reservation inventory changed during the resumed invocation"
                            )
                        _check_accounting_match(snapshot, expected_after)
                        report["requests"] = snapshot["attempts"]
                        report["downloaded_bytes"] = None
                        report["downloaded_bytes_upper_bound"] = bound[
                            "downloaded_bytes_upper_bound"
                        ]
                        report["accounting_basis"] = bound["accounting_basis"]
                        report["accounting_bound_verified"] = True
                        report["unfinished_reservation_ids"] = bound["unfinished_reservation_ids"]
                        report["accounting_complete"] = False
                        report["consumption_state"] = "reserved-upper-bound"
                elif mode == "historical":
                    cumulative = prior
                elif (
                    cli_result.get("requests") is None or cli_result.get("downloaded_bytes") is None
                ):
                    cumulative = {
                        "attempts": snapshot["attempts"],
                        "downloaded_bytes": snapshot["downloaded_or_reserved_bytes"],
                        "accounting_complete": True,
                    }
                else:
                    cumulative = {
                        "attempts": prior["attempts"] + int(cli_result["requests"]),
                        "downloaded_bytes": prior["downloaded_bytes"]
                        + int(cli_result["downloaded_bytes"]),
                        "accounting_complete": True,
                    }
                if not reconcile_accounting and mode != "historical":
                    _check_accounting_match(snapshot, cumulative)
                    report["requests"] = snapshot["attempts"]
                    report["downloaded_bytes"] = snapshot["downloaded_or_reserved_bytes"]
                    report["downloaded_bytes_upper_bound"] = None
                    report["accounting_complete"] = True
                    report["consumption_state"] = "verified"
                report["root_id"] = snapshot.get("root_id")
                if (
                    code != 0
                    or cli_result.get("complete") is not True
                    or not report["scope_complete"]
                    or (mode != "historical" and not report["capture_complete"])
                    or report["publication_state"] != "verified"
                ):
                    report["error"] = (
                        cli_result.get("error") or "capture or publication remains incomplete"
                    )
                    report["phase"] = "stopped"
                    report["recovery"] = (
                        f"After investigating the stop, retry only this saved capture offline: --historical --capture-id {report['capture_id']}, within its existing session deadline."
                        if mode == "historical"
                        else f"After investigating the stop, resume only this capture: --capture-id {report['capture_id']}."
                        if report.get("capture_id") and report["accounting_complete"]
                        else None
                    )
                    _atomic_report(report_path, report)
                    return result, code
                release_path = Path(str(cli_result.get("output", "")))
                release_id = cli_result.get("source_release_id")
                if not release_path.is_absolute() or not isinstance(release_id, str):
                    raise RunnerError("refresh did not report a verified release")
                report["phase"] = "standalone-verification"
                _atomic_report(report_path, report)
                standalone_result = _release_verify(
                    release_path,
                    directory,
                    operator,
                    paths,
                    report,
                    _monitor_deadline(report, workflow=False),
                    storage_supervision=(mode == "historical"),
                )
                if standalone_result.get("source_release_id") != release_id:
                    raise RunnerError("standalone verification returned a different release")
                if historical_profile and standalone_result.get("coverage_scope") != "historical":
                    raise RunnerError("standalone verification did not confirm historical coverage")
            else:
                activation_accounting = _resource_guard(
                    directory,
                    operator,
                    report=report,
                    deadline=session_deadline,
                    paths=paths,
                    require_complete_accounting=False,
                )
                _record_operator_accounting(report, activation_accounting)
                summary = _root_summary(operator, report, paths, session_deadline)
                if summary.get("root_id") != _owner_root_id(directory):
                    raise RunnerError(
                        "operator root does not match the managed production directory"
                    )
                current = summary.get(
                    "current_historical" if mode == "activate-historical" else "current_v2"
                )
                if not isinstance(current, dict):
                    raise RunnerError(
                        "no verified current historical release is available"
                        if mode == "activate-historical"
                        else "no verified current v2 release is available"
                    )
                if mode == "activate-historical":
                    current_capture = current.get("capture_id")
                    if not isinstance(current_capture, str) or not CAPTURE_ID.fullmatch(
                        current_capture
                    ):
                        raise RunnerError(
                            "current historical release has no exact capture identity"
                        )
                    report["capture_id"] = current_capture
                    report["root_id"] = summary.get("root_id")
                release = Path(str(current["output"]))
                standalone_result = _release_verify(
                    release,
                    directory,
                    operator,
                    paths,
                    report,
                    _monitor_deadline(report, workflow=False),
                    storage_supervision=(mode == "activate-historical"),
                )
                if mode == "activate-historical":
                    source_capture_complete = standalone_result.get("source_capture_complete")
                    if not isinstance(source_capture_complete, bool):
                        raise RunnerError(
                            "historical release verification omitted source capture completeness"
                        )
                    report["coverage_scope"] = "historical"
                    report["source_capture_complete"] = source_capture_complete
                    report["capture_complete"] = source_capture_complete
                    report["scope_complete"] = True
            expected_release = (
                standalone_result.get("source_release_id") if standalone_result else None
            )
            if not expected_release:
                raise RunnerError("no verified current catalogue release is available")
            catalogue_version = 3 if standalone_result.get("coverage_scope") == "historical" else 2
            if mode == "activate-historical" and catalogue_version != 3:
                raise RunnerError("current release does not use the historical contract")
            report["phase"] = "activation"
            _atomic_report(report_path, report)
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--_activate-current",
                str(operator),
                str(directory),
                f"historical:{expected_release}"
                if catalogue_version == 3
                else str(expected_release),
                str(paths["report"]),
                str(report.get("resource_profile", "default")),
            ]
            code, stop_reason, reaped, activation_stdout, activation_stderr = _run_external_child(
                command,
                directory=directory,
                operator=operator,
                paths=paths,
                report=report,
                deadline=_monitor_deadline(report, workflow=False),
                label="activation",
                storage_supervision=(catalogue_version == 3),
            )
            activated = _strict_json(activation_stdout)
            if (
                not reaped
                or stop_reason
                or code != 0
                or activated is None
                or activated.get("state") != "verified"
            ):
                report["production_state"] = "unknown"
                report["error"] = (
                    stop_reason or "production activation did not finish with verified readback"
                )
                report["recovery"] = (
                    "Run --activate-current --historical to reverify the historical publication and hard-link state."
                    if catalogue_version == 3
                    else "Run --activate-current to reverify the actual publication and hard-link state."
                )
                report["phase"] = "stopped"
                _atomic_report(report_path, report)
                return result, 3 if stop_reason else 1
            final_accounting = _resource_guard(
                directory,
                operator,
                report=report,
                deadline=_monitor_deadline(report, workflow=False),
                paths=paths,
                require_complete_accounting=False,
            )
            if mode in {"activate-current", "activate-historical"}:
                _record_operator_accounting(report, final_accounting)
            report["error"] = None
            report["recovery"] = None
            report["production_state"] = "verified"
            report["activation"] = activated
            if activated.get("coverage_scope") is not None:
                report["coverage_scope"] = activated["coverage_scope"]
                report["capture_id"] = activated.get("capture_id")
                report["source_capture_complete"] = activated.get("source_capture_complete")
                report["capture_complete"] = activated.get("source_capture_complete") is True
                report["scope_complete"] = activated.get("scope_complete") is True
            report["phase"] = "complete"
            report["completed_at"] = _utc()
            report["last_observed_epoch"] = time.time()
            _atomic_report(report_path, report)
            return result, 0
        except Exception as exc:
            failed_phase = report.get("phase")
            report["error"] = str(exc)
            report["phase"] = "stopped"
            report["last_observed_at"] = _utc()
            report["last_observed_epoch"] = time.time()
            if report.get("production_state") not in {"verified"}:
                report["production_state"] = (
                    "unknown"
                    if failed_phase == "activation"
                    else report.get("production_state", "not_attempted")
                )
            with contextlib.suppress(Exception):
                _atomic_report(report_path, report)
            return result, (
                3
                if isinstance(exc, (BudgetStop, RunnerInterrupted))
                or _effective_deadline_expired(report)
                else 1
            )
        finally:
            _ACTIVE_OFFLINE_HISTORICAL_WINDOW = None
            _kill_awake_guard(awake)


def _activation_worker(
    operator: Path,
    directory: Path,
    expected: str,
    report_path: Path | None = None,
    expected_profile: str | None = None,
) -> int:
    try:
        directory = _external_path(directory)
        operator = _external_path(operator)
        if report_path is not None:
            report_path = _external_path(report_path)
        if operator != directory / "operator":
            raise RunnerError("activation operator root must belong to its production directory")
        profile = "default"
        session_report = None
        if report_path is not None:
            profile, session_report = _session_resource_profile(report_path, expected_profile)
        elif expected_profile not in {None, "default"}:
            raise RunnerError("expanded activation requires its bound session report")
        catalogue_version = 3 if expected.startswith("historical:") else 2
        release_id = expected.removeprefix("historical:")
        if not re.fullmatch(r"[0-9a-f]{64}", release_id):
            raise RunnerError("activation release ID is invalid")
        if profile == "expanded" and catalogue_version != 3:
            raise RunnerError("expanded resource profile only supports historical activation")
        limits = (
            _limits_from_report(session_report, profile)
            if session_report is not None
            else _limits_for_profile(profile)
        )
        if catalogue_version == 3:
            current = _strict_json(operator / "current-historical.json")
            if current is None or current.get("source_release_id") != release_id:
                raise RunnerError(
                    "historical activation pointer does not match the expected release"
                )
            manifest_path = operator / "releases" / release_id / "manifest.json"
            manifest = _strict_json(manifest_path)
            from kalshi_events.common import digest, regular_bytes

            if (
                manifest is None
                or current.get("manifest_sha256") != digest(regular_bytes(manifest_path, 1024**2))
                or manifest.get("source_release_id") != release_id
                or manifest.get("contract") != "oddsfox.kalshi.historical.v1"
                or not isinstance(manifest.get("capture_id"), str)
                or not CAPTURE_ID.fullmatch(manifest["capture_id"])
            ):
                raise RunnerError(
                    "historical activation pointer does not bind a valid release manifest"
                )
            current_capture = manifest["capture_id"]
            if profile == "expanded" and (
                session_report is None
                or session_report.get("mode") not in {"full", "resume", "historical"}
                or session_report.get("capture_id") != current_capture
            ):
                raise RunnerError(
                    "expanded activation release does not match its stopped session capture"
                )
            if profile == "default":
                source = operator / "builds-historical" / current_capture / "warehouse.duckdb"
                try:
                    source_info = source.lstat()
                except OSError:
                    source_info = None
                if (
                    source_info is not None
                    and source.is_file()
                    and not source.is_symlink()
                    and source_info.st_size > int(limits["database_bytes"])
                ):
                    raise RunnerError(
                        "historical activation exceeds the default database limit; "
                        "resume the exact capture with --historical-resource-profile expanded"
                    )
        result = activate_current(
            operator,
            directory,
            expected_release_id=release_id,
            expected_capture_id=(
                str(session_report["capture_id"])
                if profile == "expanded" and session_report is not None
                else None
            ),
            catalogue_version=catalogue_version,
            max_database_bytes=int(limits["database_bytes"]),
            max_temp_bytes=int(limits["temporary_bytes"]),
        )
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    hidden = _main_hidden(argv)
    if hidden is not None:
        return hidden
    parser = argparse.ArgumentParser(
        description="Create or resume one verified production catalogue."
    )
    parser.add_argument("--directory", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--full", action="store_true")
    modes.add_argument("--capture-id")
    modes.add_argument("--activate-current", action="store_true")
    parser.add_argument(
        "--historical",
        action="store_true",
        help="finish saved evidence offline or activate the historical publication",
    )
    parser.add_argument(
        "--historical-resource-profile",
        choices=tuple(RESOURCE_PROFILES),
        help="select the sealed storage policy for offline historical completion",
    )
    parser.add_argument(
        "--offline-duration-seconds",
        type=float,
        help="start or reuse a bounded offline historical window (maximum 24 hours)",
    )
    parser.add_argument(
        "--renew-offline-window",
        action="store_true",
        help="explicitly grant a new offline window after the saved one expired",
    )
    parser.add_argument(
        "--supersede-offline-window",
        action="store_true",
        help="explicitly replace an existing offline window before it expires",
    )
    parser.add_argument(
        "--reconcile-accounting",
        action="store_true",
        help="resume only after charging unfinished requests at their full durable reservation",
    )
    parser.add_argument(
        "--reconcile-stopped-worker",
        action="store_true",
        help="clear one evidenced stale historical child group without starting work",
    )
    parser.add_argument(
        "--extend-session",
        action="store_true",
        help="consume one new 48-hour window for an exact reconciled capture",
    )
    parser.add_argument(
        "--recover-reboot",
        action="store_true",
        help="reconcile accounting and resume an exact stopped capture after a Mac reboot",
    )
    parser.add_argument(
        "--recovery-volume-uuid",
        help="explicit APFS volume UUID to verify when recovering after reboot",
    )
    args = parser.parse_args(argv)
    try:
        validate_mode(
            args.full,
            args.capture_id,
            args.activate_current,
            args.historical,
            offline_duration_seconds=args.offline_duration_seconds,
            renew_offline_window=args.renew_offline_window,
            supersede_offline_window=args.supersede_offline_window,
            resource_profile=args.historical_resource_profile,
        )
    except RunnerError as exc:
        parser.error(str(exc))
    if args.reconcile_accounting and (not args.capture_id or args.historical):
        parser.error("--reconcile-accounting requires a network --capture-id resume")
    if args.reconcile_stopped_worker and (
        not args.historical
        or not args.capture_id
        or args.historical_resource_profile is not None
        or args.offline_duration_seconds is not None
        or args.renew_offline_window
        or args.supersede_offline_window
        or args.reconcile_accounting
        or args.extend_session
        or args.recover_reboot
        or args.recovery_volume_uuid
    ):
        parser.error("--reconcile-stopped-worker requires only --historical --capture-id")
    if args.extend_session and (not args.capture_id or not args.reconcile_accounting):
        parser.error("--extend-session requires --capture-id and --reconcile-accounting")
    if args.recover_reboot and (
        not args.capture_id
        or not args.reconcile_accounting
        or args.extend_session
        or not args.recovery_volume_uuid
    ):
        parser.error(
            "--recover-reboot requires --capture-id, --reconcile-accounting and "
            "--recovery-volume-uuid; "
            "it cannot be combined with --extend-session"
        )
    if args.recovery_volume_uuid and not args.recover_reboot:
        parser.error("--recovery-volume-uuid requires --recover-reboot")
    try:
        with _graceful_stop_handlers():
            result, code = run_production(
                args.directory,
                full=args.full,
                capture_id=args.capture_id,
                activate_only=args.activate_current,
                historical=args.historical,
                offline_duration_seconds=args.offline_duration_seconds,
                renew_offline_window=args.renew_offline_window,
                supersede_offline_window=args.supersede_offline_window,
                reconcile_accounting=args.reconcile_accounting,
                extend_session=args.extend_session,
                recover_reboot=args.recover_reboot,
                recovery_volume_uuid=args.recovery_volume_uuid,
                reconcile_stopped_worker=args.reconcile_stopped_worker,
                resource_profile=args.historical_resource_profile,
            )
    except (BudgetStop, RunnerInterrupted) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 3
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return code


def _session_report(
    directory: Path, mode: str, capture_id: str | None = None
) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    run_id = uuid.uuid4().hex
    run_root = directory / "runs" / run_id
    (run_root / "logs").mkdir(parents=True)
    paths = {
        "directory": directory,
        "report": run_root / REPORT_NAME,
        "logs": run_root / "logs",
        "metrics": run_root / "metrics.json",
        "capture_notification": run_root / "capture-notification.json",
    }
    now = time.time()
    mono = time.monotonic()
    report: dict[str, Any] = {
        "version": 1,
        "session_id": run_id,
        "mode": mode,
        "started_at": _utc(),
        "started_epoch": now,
        "deadline_epoch": now + float(_effective_limits()["session_seconds"]),
        "started_monotonic": mono,
        "deadline_monotonic": mono + float(_effective_limits()["session_seconds"]),
        "time_budget_started_epoch": now,
        "time_budget_started_monotonic": mono,
        "last_observed_at": _utc(),
        "boot_stamp": _boot_stamp(),
        "revision": _revision(),
        "source_root": str(SOURCE_ROOT),
        "system": _system_info(directory),
        "capture_id": capture_id,
        "root_id": None,
        "capture_complete": False,
        "publication_state": "not_attempted",
        "production_state": "not_attempted",
        "accounting_complete": True,
        "requests": 0,
        "downloaded_bytes": 0,
        "phase": "preflight",
        "phase_seconds": {},
        "metrics_file": str(paths["metrics"]),
        "limits": dict(_effective_limits()),
        "resource_profile": "default",
        "effective_resource_policy": _resource_policy_record("default", _effective_limits()),
        "resource_profile_history": [
            {
                "profile": "default",
                "limits": dict(_effective_limits()),
                "policy": _resource_policy_record("default", _effective_limits()),
                "selected_at": _utc(),
                "reason": "initial session policy",
            }
        ],
        "resource_samples": [],
        "resource_samples_total": 0,
        "resource_samples_dropped": 0,
        "memory_preflight_samples": [],
        "memory_preflight_samples_total": 0,
        "memory_preflight_samples_dropped": 0,
        "peak_child_process_group_rss_bytes": 0,
        "peak_child_process_group_physical_footprint_bytes": 0,
        "peak_supervisor_physical_footprint_bytes": 0,
        "peak_system_compressed_bytes": 0,
        "peak_system_compressor_occupied_bytes": 0,
        "invocation_peak_child_process_group_rss_bytes": 0,
        "invocation_peak_child_process_group_physical_footprint_bytes": 0,
        "invocation_peak_supervisor_physical_footprint_bytes": 0,
        "effective_memory_policy": _effective_memory_policy(),
        "error": None,
        "recovery": None,
    }
    _bind_report_resource_profile(report, "default")
    _atomic_report(paths["report"], report)
    return paths["report"], report, paths


def _load_resume(
    directory: Path,
    capture_id: str,
    *,
    historical: bool = False,
    offline_duration_seconds: float | None = None,
    renew_offline_window: bool = False,
    supersede_offline_window: bool = False,
    reconcile_accounting: bool = False,
    extend_session: bool = False,
    recover_reboot: bool = False,
    recovery_volume_uuid: str | None = None,
    resource_profile: str | None = None,
) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    duration = _validate_offline_duration_seconds(offline_duration_seconds)
    renew_requested = renew_offline_window or supersede_offline_window
    if duration is not None and not historical:
        raise RunnerError("--offline-duration-seconds requires offline historical completion")
    if renew_requested and (not historical or duration is None):
        raise RunnerError(
            "--renew-offline-window requires offline historical completion and an explicit duration"
        )
    if historical and (reconcile_accounting or extend_session or recover_reboot):
        raise RunnerError("offline historical completion cannot reconcile or extend acquisition")
    if extend_session and not reconcile_accounting:
        raise RunnerError("session extension requires --reconcile-accounting")
    if recover_reboot and (
        not reconcile_accounting or extend_session or recovery_volume_uuid is None
    ):
        raise RunnerError(
            "reboot recovery requires accounting reconciliation and a verified volume UUID"
        )
    matches = []
    run_root = directory / "runs"
    if run_root.is_dir():
        for path in run_root.glob(f"*/{REPORT_NAME}"):
            from kalshi_events.common import safe_path

            path = safe_path(path)
            report = _strict_json(path)
            if report and report.get("capture_id") == capture_id:
                matches.append((path, report))
    if len(matches) != 1:
        raise RunnerError("resume requires one durable session report for this capture ID")
    report_path, report = matches[0]
    saved_profile = report.get("resource_profile", "default")
    if not isinstance(saved_profile, str) or saved_profile not in RESOURCE_PROFILES:
        raise RunnerError("session resource profile is invalid")
    selected_profile = resource_profile or saved_profile
    profile_transition = selected_profile != saved_profile
    if not historical and selected_profile != "default":
        raise RunnerError(
            "expanded resource profile is restricted to offline historical completion"
        )
    reconciliation = report.get("stopped_worker_reconciliation")
    reconciled_stop = (
        isinstance(reconciliation, dict)
        and re.fullmatch(r"[0-9a-f]{64}", str(reconciliation.get("event_id", ""))) is not None
        and reconciliation.get("audit_file") == "stopped-worker-reconciliation.jsonl"
        and reconciliation.get("primary_stop_reason") == report.get("error")
        and type(reconciliation.get("stale_process_group")) is int
        and reconciliation["stale_process_group"] > 1
        and type(reconciliation.get("supervisor_pid")) is int
        and reconciliation["supervisor_pid"] > 1
        and isinstance(reconciliation.get("termination_evidence_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", reconciliation["termination_evidence_sha256"]) is not None
        and isinstance(reconciliation.get("session_before_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", reconciliation["session_before_sha256"]) is not None
    )
    zero_network_stop = report.get("network_activity_this_invocation") == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }
    if profile_transition and not (
        saved_profile == "default"
        and selected_profile == "expanded"
        and historical
        and report.get("mode") in {"full", "resume", "historical"}
        and report.get("phase") == "stopped"
        and report.get("active_process_group") is None
        and (zero_network_stop or reconciled_stop)
        and report.get("production_state") != "verified"
    ):
        raise RunnerError(
            "conflicting historical resource profile requires an eligible stopped capture"
        )
    _bind_report_resource_profile(report, selected_profile, transition=profile_transition)
    recoverable_accounting_stop = _is_recoverable_accounting_stop(
        report, capture_id, reconcile_accounting
    )
    if (
        not historical
        and report.get("accounting_complete") is not True
        and not recoverable_accounting_stop
    ) or report.get("production_state") == "verified":
        raise RunnerError("resume accounting is unknown or production activation already succeeded")
    if reconcile_accounting and not historical:
        saved_system = report.get("system")
        saved_versions = (
            saved_system.get("package_versions") if isinstance(saved_system, dict) else None
        )
        saved_lock_sha256 = (
            saved_system.get("uv_lock_sha256") if isinstance(saved_system, dict) else None
        )
        if not isinstance(saved_lock_sha256, str) or not _validate_reconciliation_versions(
            saved_versions, saved_lock_sha256, saved=True
        ):
            raise RunnerError(
                "the stopped session's HTTP chunk bound or locked package stack cannot be verified"
            )
        try:
            current_lock_sha256 = hashlib.sha256((SOURCE_ROOT / "uv.lock").read_bytes()).hexdigest()
        except OSError as exc:
            raise RunnerError(
                "current HTTP runtime and source uv.lock must match "
                "an approved reconciliation stack"
            ) from exc
        current_versions = _package_versions()
        if not _supported_reconciliation_lock_transition(
            saved_lock_sha256, current_lock_sha256
        ) or not _validate_reconciliation_versions(
            current_versions, current_lock_sha256, saved=False
        ):
            raise RunnerError(
                "current HTTP runtime and source uv.lock must match "
                "an approved reconciliation stack"
            )
        saved_transport_sha256 = saved_system.get("transport_sha256")
        if (
            HTTPX_MAX_RAW_CHUNK_BYTES != 64 * 1024
            or _transport_source_sha256() != RECONCILIATION_TRANSPORT_SHA256
            or (
                "transport_sha256" not in saved_system
                and saved_lock_sha256 != LEGACY_RECONCILIATION_UV_LOCK_SHA256
            )
            or (
                "transport_sha256" in saved_system
                and (
                    not isinstance(saved_transport_sha256, str)
                    or saved_transport_sha256
                    not in (
                        PREVIOUS_RECONCILIATION_TRANSPORT_SHA256,
                        PRE_PAYLOAD_RECONCILIATION_TRANSPORT_SHA256,
                        RECONCILIATION_TRANSPORT_SHA256,
                    )
                )
            )
        ):
            raise RunnerError(
                "current HTTP transport source and 64 KiB raw chunk bound cannot be verified"
            )
    if report.get("active_process_group") is not None:
        raise RunnerError(
            "session records an unreconciled child process group; explicit recovery is required"
        )
    now = time.time()
    if now < float(report.get("last_observed_epoch", report["started_epoch"])):
        raise RunnerError("wall clock moved backward since the last durable session report")
    current_boot = _boot_stamp()
    rebooted = report.get("boot_stamp") != current_boot
    if rebooted and not recover_reboot:
        raise RunnerError("Mac restarted; explicit reconciled reboot recovery is required")
    if rebooted and report.get("phase") != "stopped":
        raise RunnerError("reboot recovery requires a durably stopped session")
    if not isinstance(report.get("root_id"), str) or _owner_root_id(directory) != report["root_id"]:
        raise RunnerError("session logical root differs from production ownership marker")
    saved_system = report.get("system", {})
    saved_device = saved_system.get("filesystem_device") if isinstance(saved_system, dict) else None
    current_device = directory.stat().st_dev
    saved_volume_uuid = (
        saved_system.get("filesystem_volume_uuid") if isinstance(saved_system, dict) else None
    )
    current_volume_uuid = None
    if recover_reboot:
        if not re.fullmatch(
            r"[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}",
            str(recovery_volume_uuid),
        ):
            raise RunnerError("recovery volume UUID must be a canonical UUID")
        current_volume_uuid = _volume_uuid(directory)
        if current_volume_uuid != str(recovery_volume_uuid).upper():
            raise RunnerError("mounted production volume does not match --recovery-volume-uuid")
        if saved_volume_uuid is not None and current_volume_uuid != saved_volume_uuid:
            raise RunnerError("production volume UUID differs from the saved session")
        operator_path = directory / "operator"
        if operator_path.exists() and operator_path.stat().st_dev != current_device:
            raise RunnerError("production operator directory is on another filesystem")
    if not rebooted and saved_device != current_device:
        raise RunnerError("production filesystem changed since the original session")
    existing_offline_window = "offline_historical_window" in report
    if historical and existing_offline_window:
        _validate_offline_historical_window(
            report,
            capture_id,
            allow_expired=renew_requested,
            allow_revision_change=renew_requested,
        )
    elif historical and (
        "offline_historical_window_history" in report
        or "offline_historical_window_supersessions" in report
    ):
        history = report.get("offline_historical_window_history")
        supersessions = report.get("offline_historical_window_supersessions", [])
        if (
            not isinstance(history, list)
            or history
            or not isinstance(supersessions, list)
            or supersessions
        ):
            raise RunnerError("offline historical window history has no current window")
    if renew_requested:
        window = report.get("offline_historical_window")
        if not isinstance(window, dict):
            raise RunnerError("offline historical window renewal requires an existing window")
        if report.get("phase") != "stopped":
            raise RunnerError("offline historical window renewal requires a stopped session")
        if report.get("production_state") == "verified":
            raise RunnerError("offline historical window renewal is prohibited after activation")
        session_id = report.get("session_id")
        if (
            not isinstance(session_id, str)
            or not CAPTURE_ID.fullmatch(session_id)
            or session_id != report_path.parent.name
            or report_path.name != REPORT_NAME
            or report_path.parent.parent != run_root
        ):
            raise RunnerError("offline historical renewal report is outside its capture path")
        expired = time.time() >= float(window["deadline_epoch"]) and time.monotonic() >= float(
            window["deadline_monotonic"]
        )
        if not expired and not supersede_offline_window:
            raise RunnerError("offline historical window renewal requires both clocks to expire")
        if (
            not isinstance(saved_volume_uuid, str)
            or not re.fullmatch(
                r"[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}",
                saved_volume_uuid,
            )
            or _volume_uuid(directory) != saved_volume_uuid.upper()
        ):
            raise RunnerError("production volume UUID differs from the saved session")
        _validate_offline_renewal_root(
            directory,
            directory / "operator",
            str(report["root_id"]),
            capture_id,
        )
    offline_historical_allowed = historical and (duration is not None or existing_offline_window)
    if (
        not offline_historical_allowed
        and not rebooted
        and time.monotonic() >= _time_budget_deadline(report, "monotonic")
        and not extend_session
    ):
        raise RunnerError("48-hour session expired; networking resume is prohibited")
    if (
        not offline_historical_allowed
        and now >= _time_budget_deadline(report, "epoch")
        and not extend_session
    ):
        raise RunnerError("48-hour wall-clock session expired; networking resume is prohibited")
    if rebooted:
        old_deadlines = {
            "started_monotonic": report.get("started_monotonic"),
            "time_budget_started_monotonic": _time_budget_started(report, "monotonic"),
            "deadline_monotonic": _time_budget_deadline(report, "monotonic"),
        }
        # Anchor monotonic time to the existing wall clock; never grant new time.
        mono_sample = time.monotonic()
        wall_sample = time.time()
        offset = mono_sample - wall_sample
        report["started_monotonic"] = offset + float(report["started_epoch"])
        report["time_budget_started_monotonic"] = offset + _time_budget_started(report, "epoch")
        report["deadline_monotonic"] = offset + _time_budget_deadline(report, "epoch")
        history = report.setdefault("reboot_recovery_history", [])
        if not isinstance(history, list):
            raise RunnerError("reboot recovery history is invalid")
        history.append(
            {
                "recovered_at": _utc(),
                "previous_boot_stamp": report.get("boot_stamp"),
                "boot_stamp": current_boot,
                "previous_filesystem_device": saved_device,
                "filesystem_device": current_device,
                "volume_uuid": current_volume_uuid,
                "prior_volume_uuid_available": saved_volume_uuid is not None,
                "previous_monotonic": old_deadlines,
                "wall_deadlines_preserved": {
                    "started_epoch": report.get("started_epoch"),
                    "time_budget_started_epoch": _time_budget_started(report, "epoch"),
                    "deadline_epoch": _time_budget_deadline(report, "epoch"),
                },
                "accounting_reconciliation_required": True,
            }
        )
        report.setdefault("original_system", dict(saved_system))
        saved_system["filesystem_device"] = current_device
        saved_system["filesystem_volume_uuid"] = current_volume_uuid
        report["boot_stamp"] = current_boot
        _atomic_report(report_path, report)
    run_root = report_path.parent
    paths = {
        "directory": directory,
        "report": report_path,
        "logs": run_root / "logs",
        "metrics": run_root / f"metrics-resume-{uuid.uuid4().hex}.json",
        "capture_notification": run_root / "capture-notification.json",
    }
    if extend_session:
        if report.get("phase") != "stopped":
            raise RunnerError("session extension requires a stopped capture")
        history = report.get("session_extension_history", [])
        if not isinstance(history, list):
            raise RunnerError("session extension history is invalid")
        if history:
            raise RunnerError("the one-time session extension has already been used")
        old_deadline_epoch = _time_budget_deadline(report, "epoch")
        old_deadline_monotonic = _time_budget_deadline(report, "monotonic")
        now = time.time()
        mono = time.monotonic()
        deadline_epoch = now + float(_effective_limits()["session_seconds"])
        deadline_monotonic = mono + float(_effective_limits()["session_seconds"])
        granting_revision = _revision()
        report["time_budget_started_epoch"] = now
        report["time_budget_started_monotonic"] = mono
        report["deadline_epoch"] = deadline_epoch
        report["deadline_monotonic"] = deadline_monotonic
        report["session_extension_history"] = [
            {
                "action": "--extend-session",
                "capture_id": capture_id,
                "granted_at": _utc(),
                "source_revision": granting_revision,
                "prior_deadline_epoch": old_deadline_epoch,
                "prior_deadline_monotonic": old_deadline_monotonic,
                "deadline_epoch": deadline_epoch,
                "deadline_monotonic": deadline_monotonic,
            }
        ]
        _atomic_report(report_path, report)
    _memory_preflight_sample_state(report, report_path)
    previous_error = report.get("error")
    previous_recovery = report.get("recovery")
    previous_phase = report.get("phase")
    if previous_phase == "stopped" or previous_error is not None or previous_recovery is not None:
        prior_resource_peaks = {
            key: report.get(key)
            for key, _sample_field in (
                *RESOURCE_SAMPLE_PEAK_FIELDS,
                *RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS,
            )
        }
        previous_stop = {
            "phase": previous_phase,
            "error": previous_error,
            "recovery": previous_recovery,
            "last_observed_at": report.get("last_observed_at"),
            "last_observed_epoch": report.get("last_observed_epoch"),
            "resource_peaks": prior_resource_peaks,
        }
        history = report.get("stop_history", [])
        if not isinstance(history, list) or any(not isinstance(item, dict) for item in history):
            raise RunnerError("session stop history is invalid")
        history.append(previous_stop)
        report["stop_history"] = history[-64:]
        report["previous_stop"] = previous_stop
    report["error"] = None
    report["recovery"] = None
    # Keep an offline grant in the durable stopped state until the first processing child
    # actually starts. A crash between grant persistence and Popen must remain renewable.
    report["phase"] = "stopped" if historical else "preflight"
    for peak_field, _sample_field in RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS:
        report[peak_field] = 0
    report["effective_memory_policy"] = _effective_memory_policy()
    return report_path, report, paths


def _is_recoverable_accounting_stop(
    report: dict[str, Any], capture_id: str, reconcile_accounting: bool
) -> bool:
    """Allow only known resource stops through explicit, exact-capture recovery."""
    return (
        reconcile_accounting
        and report.get("capture_id") == capture_id
        and report.get("accounting_complete") is False
        and report.get("phase") == "stopped"
        and report.get("production_state") != "verified"
        and report.get("error") in RECOVERABLE_ACCOUNTING_STOP_REASONS
    )


def _accounting_shape(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempts": snapshot["attempts"],
        "downloaded_bytes": snapshot["downloaded_or_reserved_bytes"],
        "accounting_complete": snapshot["accounting_complete"],
    }


def _reserved_upper_bound(snapshot: dict[str, Any], capture_id: str) -> dict[str, Any]:
    """Validate incomplete durable receipts and charge their full pre-request reservations."""
    attempts = snapshot.get("attempts")
    amount = snapshot.get("downloaded_or_reserved_bytes")
    pending = snapshot.get("unfinished_reservations")
    if (
        snapshot.get("accounting_complete") is not False
        or isinstance(attempts, bool)
        or not isinstance(attempts, int)
        or attempts < 1
        or isinstance(amount, bool)
        or not isinstance(amount, int)
        or amount < 0
        or not isinstance(pending, list)
        or not pending
    ):
        raise RunnerError("durable incomplete accounting cannot be bounded")
    ids: set[str] = set()
    inventory: list[tuple[str, str, int]] = []
    charges = 0
    for receipt in pending:
        if not isinstance(receipt, dict):
            raise RunnerError("unfinished receipt inventory is malformed")
        request_id = receipt.get("request_id")
        reservation = receipt.get("reserved_bytes")
        receipt_version = receipt.get("version")
        if (
            receipt.get("capture_id") != capture_id
            or receipt.get("acquisition_protocol") != 5
            or isinstance(receipt_version, bool)
            or receipt_version not in {1, 2}
            or receipt.get("accounting_complete") is not False
            or receipt.get("status") is not None
            or not isinstance(request_id, str)
            or not re.fullmatch(r"[0-9a-f]{64}", request_id)
            or request_id in ids
            or isinstance(reservation, bool)
            or not isinstance(reservation, int)
            or not 0 < reservation <= int(_effective_limits()["response_bytes"])
        ):
            raise RunnerError("unfinished receipt cannot be safely charged")
        ids.add(request_id)
        inventory.append((request_id, receipt["capture_id"], reservation))
        charges += int(_effective_limits()["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
    if amount < charges or attempts < len(pending):
        raise RunnerError("durable reservation totals are contradictory")
    return {
        "attempts": attempts,
        "downloaded_bytes_upper_bound": amount,
        "accounting_complete": False,
        "accounting_basis": "reserved-upper-bound",
        "accounting_bound_verified": True,
        "unfinished_reservations": len(pending),
        "unfinished_response_ceiling_bytes": len(pending)
        * int(_effective_limits()["response_bytes"]),
        "unfinished_chunk_slack_bytes": len(pending) * HTTPX_MAX_RAW_CHUNK_BYTES,
        "unfinished_reservation_ids": sorted(ids),
        "reservation_inventory_sha256": hashlib.sha256(
            json.dumps(sorted(inventory), separators=(",", ":")).encode()
        ).hexdigest(),
    }


def _reconciliation_accounting(snapshot: dict[str, Any], capture_id: str) -> dict[str, Any]:
    """Recover exact accounting when every durable receipt is closed, otherwise bound it."""
    if snapshot.get("accounting_complete") is not True:
        return _reserved_upper_bound(snapshot, capture_id)
    attempts = snapshot.get("attempts")
    amount = snapshot.get("downloaded_or_reserved_bytes")
    pending = snapshot.get("unfinished_reservations")
    if (
        isinstance(attempts, bool)
        or not isinstance(attempts, int)
        or attempts < 0
        or isinstance(amount, bool)
        or not isinstance(amount, int)
        or amount < 0
        or pending != []
    ):
        raise RunnerError("durable exact accounting is malformed")
    return {
        "attempts": attempts,
        "downloaded_bytes": amount,
        "downloaded_bytes_upper_bound": None,
        "accounting_complete": True,
        "accounting_basis": "exact",
        "accounting_bound_verified": False,
        "unfinished_reservations": 0,
        "unfinished_reservation_ids": [],
    }


def _record_operator_accounting(report: dict[str, Any], snapshot: dict[str, Any]) -> None:
    if snapshot.get("accounting_complete") is True:
        report["operator_accounting"] = {
            "state": "verified",
            "requests": snapshot.get("attempts"),
            "downloaded_bytes": snapshot.get("downloaded_or_reserved_bytes"),
        }
    else:
        report["operator_accounting"] = {"state": "incomplete"}


def _refresh_command(
    operator: Path,
    paths: dict[str, Path],
    mode: str,
    capture_id: str | None,
    limits: dict[str, Any],
    resource_profile: str = "default",
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_refresh-worker",
        str(operator),
        str(paths["report"]),
        str(paths["metrics"]),
        str(paths["capture_notification"]),
        mode,
        capture_id or "-",
        json.dumps(limits, sort_keys=True, allow_nan=False),
        resource_profile,
    ]


def _read_cli_result(path: Path) -> dict[str, Any] | None:
    result = _strict_json(path)
    return result if result and isinstance(result.get("complete"), bool) else None


def _accounting_worker_main(
    root: Path,
    capture_id: str | None = None,
    *,
    cleanup_historical_stages: bool = False,
    expected_directory: Path | None = None,
    report_path: Path | None = None,
    expected_root_id: str | None = None,
    expected_revision: str | None = None,
    resource_profile: str | None = None,
) -> int:
    try:
        root = _existing_operator_root(root)
        profile = "default"
        if report_path is not None:
            profile, _report = _session_resource_profile(report_path, resource_profile, capture_id)
        elif resource_profile not in {None, "default"}:
            raise RunnerError("expanded accounting requires its bound session report")
        token = _ACTIVE_LIMITS.set(
            _limits_from_report(_report, profile)
            if report_path is not None
            else _limits_for_profile(profile)
        )
        try:
            value = _read_accounting_worker(
                root,
                capture_id,
                cleanup_historical_stages=cleanup_historical_stages,
                expected_directory=expected_directory,
                report_path=(report_path if cleanup_historical_stages else None),
                expected_root_id=expected_root_id,
                expected_revision=expected_revision,
            )
        finally:
            _ACTIVE_LIMITS.reset(token)
        print(json.dumps(value, sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


def _root_summary_worker_main(root: Path) -> int:
    try:
        root = _existing_operator_root(root)
        print(json.dumps(_read_root_summary(root), sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


def _existing_operator_root(root: Path) -> Path:
    from kalshi_events.common import CatalogueError
    from kalshi_events.paths import PathPolicyError

    try:
        root = _external_path(root)
        details = (root / "root.json").lstat()
    except (CatalogueError, PathPolicyError, OSError) as exc:
        raise RunnerError("read worker requires an existing operator root") from exc
    import stat

    if root.is_symlink() or not root.is_dir() or not stat.S_ISREG(details.st_mode):
        raise RunnerError("read worker requires an existing operator root")
    return root


def _refresh_worker_main(
    operator: Path,
    report_path: Path,
    metrics_path: Path,
    notification_path: Path,
    mode: str,
    capture_id: str | None,
    raw_limits: str,
    expected_profile: str | None = None,
) -> int:
    from kalshi_events import metrics, refresh
    from kalshi_events.common import Limits, Store

    try:
        operator = _external_path(operator)
        if operator != operator.parent / "operator":
            raise RunnerError(
                "refresh worker operator root must belong to its production directory"
            )
        report_path = _external_path(report_path)
        metrics_path = _external_path(metrics_path)
        notification_path = _external_path(notification_path)
    except Exception as exc:
        result = refresh.result_template()
        result["complete"] = False
        result["error"] = str(exc)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 1
    report = _strict_json(report_path)
    result = refresh.result_template()
    if report is None:
        print(json.dumps({**result, "error": "session report is missing"}))
        return 1
    token = None
    try:
        profile, _ = _session_resource_profile(
            report_path, expected_profile, capture_id if mode != "full" else None
        )
        if profile == "expanded" and mode != "historical":
            raise RunnerError(
                "expanded resource profile is restricted to offline historical refresh"
            )
        if mode != "historical":
            from kalshi_events.transport import BASE_URL, source_url

            actual_source = source_url()
            if actual_source != BASE_URL:
                raise RunnerError("production acquisition must use the public Kalshi API origin")
        values = json.loads(raw_limits)
        limits = Limits(
            **{
                key.replace("-", "_"): value
                for key, value in values.items()
                if key not in {"compression", "rate-policy"}
            }
        )
        sealed_limits = _limits_from_report(report, profile)
        if (
            not isinstance(sealed_limits, dict)
            or limits.max_temp_bytes != sealed_limits.get("temporary_bytes")
            or limits.max_database_bytes != sealed_limits.get("database_bytes")
            or profile not in RESOURCE_PROFILES
        ):
            raise RunnerError("refresh worker limits do not match its session resource profile")
        token = _ACTIVE_LIMITS.set(_limits_from_report(report, profile))
        compression = values["compression"]
        rate_policy = values["rate-policy"]
        progress_path = metrics_path.with_name(f"{metrics_path.name}.progress.json")
        metrics.check_progress_path(progress_path)
        stream = metrics.reserve_report(metrics_path)
        exit_code = 1
        with metrics.collect(progress_path=progress_path) as measured:
            store = None
            try:
                store = Store(operator, limits)
                with store.lock():
                    from kalshi_events.incremental import root_id

                    logical_root_id = root_id(store, create=True)
                    _bind_owner(operator.parent, logical_root_id, mode)
                    if mode != "historical":
                        _write_capture_notification(notification_path, None, logical_root_id)
                    result, exit_code = refresh.run(
                        store,
                        full=mode == "full",
                        capture_id=capture_id,
                        historical=mode == "historical",
                        compression=compression,
                        rate_policy=rate_policy,
                        on_capture_id=(
                            None
                            if mode == "historical"
                            else lambda value: _write_capture_notification(
                                notification_path, value, logical_root_id
                            )
                        ),
                    )
            except Exception as exc:
                result["error"] = str(exc)
            finally:
                if store is not None:
                    try:
                        store.reconcile()
                    except Exception as exc:
                        result["complete"] = False
                        result["error"] = (
                            result.get("error") or f"final storage accounting failed: {exc}"
                        )
                        result["accounting_complete"] = False
                        exit_code = 3
                try:
                    measured.progress(
                        "complete"
                        if exit_code == 0 and result.get("complete") is True
                        else "stopped"
                    )
                    metrics.write_report(metrics_path, measured, stream)
                finally:
                    stream.close()
        _ACTIVE_LIMITS.reset(token)
        token = None
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return exit_code
    except Exception as exc:
        if token is not None:
            _ACTIVE_LIMITS.reset(token)
        result["complete"] = False
        result["error"] = result.get("error") or str(exc)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 1


def _write_capture_notification(path: Path, capture_id: str | None, root_id: str) -> None:
    from kalshi_events.common import safe_path

    _atomic_report(
        safe_path(path),
        {
            "version": 1,
            "capture_id": capture_id,
            "root_id": root_id,
            "allocated_at": _utc(),
            "resumable": None,
        },
    )


def _merge_capture_notification(report: dict[str, Any], path: Path | None) -> None:
    if path is None:
        return
    value = _strict_json(path)
    if value is None or value.get("version") != 1:
        return
    capture_id = value.get("capture_id")
    root_id = value.get("root_id")
    if capture_id is not None and isinstance(capture_id, str) and CAPTURE_ID.fullmatch(capture_id):
        report["capture_id"] = capture_id
        report["resumable"] = None
        report["accounting_complete"] = False
    if isinstance(root_id, str) and CAPTURE_ID.fullmatch(root_id):
        report["root_id"] = root_id


def _main_hidden(argv: list[str]) -> int | None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--_account-root", type=Path)
    parser.add_argument("--_account-recovery", nargs="+")
    parser.add_argument("--_account-historical-initial", nargs=7)
    parser.add_argument("--_root-summary", type=Path)
    parser.add_argument("--_memory-sample", nargs=1)
    parser.add_argument("--_refresh-worker", nargs=8)
    parser.add_argument("--_activate-current", nargs="+")
    args, rest = parser.parse_known_args(argv)
    selected_workers = (
        args._account_root,
        args._account_recovery,
        args._account_historical_initial,
        args._root_summary,
        args._memory_sample,
        args._refresh_worker,
        args._activate_current,
    )
    if sum(value is not None for value in selected_workers) > 1:
        print(json.dumps({"error": "conflicting hidden worker modes"}))
        return 1
    if args._account_root is not None and not rest:
        return _accounting_worker_main(args._account_root)
    if args._account_recovery is not None and not rest:
        if len(args._account_recovery) == 2:
            return _accounting_worker_main(
                Path(args._account_recovery[0]), args._account_recovery[1]
            )
        if len(args._account_recovery) != 4:
            print(json.dumps({"error": "recovery accounting worker identity is incomplete"}))
            return 1
        operator, capture_id, report_path, profile = args._account_recovery
        return _accounting_worker_main(
            Path(operator), capture_id, report_path=Path(report_path), resource_profile=profile
        )
    if args._account_historical_initial is not None and not rest:
        operator, directory, report, capture_id, root_id, revision, profile = (
            args._account_historical_initial
        )
        return _accounting_worker_main(
            Path(operator),
            capture_id,
            cleanup_historical_stages=True,
            expected_directory=Path(directory),
            report_path=Path(report),
            expected_root_id=root_id,
            expected_revision=revision,
            resource_profile=profile,
        )
    if args._root_summary is not None and not rest:
        return _root_summary_worker_main(args._root_summary)
    if args._memory_sample is not None and not rest:
        group = args._memory_sample[0]
        if group == "-":
            return _memory_sample_worker_main(None)
        if not re.fullmatch(r"[1-9]\d*", group):
            return _memory_sample_worker_main(0)
        return _memory_sample_worker_main(int(group))
    if args._refresh_worker is not None and not rest:
        operator, report, metrics_path, notification_path, mode, capture_id, limits, profile = (
            args._refresh_worker
        )
        return _refresh_worker_main(
            Path(operator),
            Path(report),
            Path(metrics_path),
            Path(notification_path),
            mode,
            None if capture_id == "-" else capture_id,
            limits,
            profile,
        )
    if args._activate_current is not None and not rest:
        if len(args._activate_current) == 3:
            return _activation_worker(
                Path(args._activate_current[0]),
                Path(args._activate_current[1]),
                args._activate_current[2],
            )
        if len(args._activate_current) != 5:
            print(json.dumps({"error": "activation worker session identity is incomplete"}))
            return 1
        return _activation_worker(
            Path(args._activate_current[0]),
            Path(args._activate_current[1]),
            args._activate_current[2],
            Path(args._activate_current[3]),
            args._activate_current[4],
        )
    return None


if __name__ == "__main__":
    raise SystemExit(main())
