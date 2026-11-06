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
import stat
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
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
HISTORICAL_STAGE = re.compile(r"build-historical-[0-9a-f]{32}\Z")
HISTORICAL_CONTROL_DIRECTORY = re.compile(r"kalshi-historical-completion-[0-9]{8}\Z")
HISTORICAL_HOST_PYTHON = Path("/opt/homebrew/bin/python3")
PROCESS_IDENTITY_UTC = re.compile(
    r"(\d+)\s+(\d+)\s+(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\d{4})\s+(.+)"
)
REPORT_NAME = "session.json"
OWNER_MARKER = ".production-root.json"
HISTORICAL_SUPERVISOR_FD_ENV = "KALSHI_EVENTS_HISTORICAL_SUPERVISOR_FD"
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


def _validate_historical_stage(value: Any) -> str:
    if not isinstance(value, str) or not HISTORICAL_STAGE.fullmatch(value):
        raise RunnerError("historical stage must be a full build-historical stage basename")
    return value


def _record_historical_invocation(
    report: dict[str, Any], stage: str | None, *, revision: str
) -> None:
    report["resume_historical_stage"] = stage
    if stage is not None:
        _validate_historical_stage(stage)
    history = report.setdefault("historical_invocations", [])
    if (
        not isinstance(history, list)
        or len(history) >= 64
        or any(
            not isinstance(item, dict)
            or set(item)
            != {
                "invocation_id",
                "started_at",
                "mode",
                "capture_id",
                "root_id",
                "revision",
                "boot_stamp",
                "supervisor_pid",
                "supervisor_pgid",
                "resume_historical_stage",
            }
            or not isinstance(item.get("invocation_id"), str)
            or not CAPTURE_ID.fullmatch(item["invocation_id"])
            or not isinstance(item.get("started_at"), str)
            or item.get("mode") != "historical"
            or item.get("capture_id") != report.get("capture_id")
            or item.get("root_id") != report.get("root_id")
            or not isinstance(item.get("revision"), str)
            or not REVISION.fullmatch(item["revision"])
            or item.get("boot_stamp") != report.get("boot_stamp")
            or type(item.get("supervisor_pid")) is not int
            or item["supervisor_pid"] <= 1
            or type(item.get("supervisor_pgid")) is not int
            or item["supervisor_pgid"] <= 1
            or item.get("resume_historical_stage") is not None
            and (
                not isinstance(item["resume_historical_stage"], str)
                or not HISTORICAL_STAGE.fullmatch(item["resume_historical_stage"])
            )
            for item in history
        )
    ):
        raise RunnerError("historical invocation history is invalid or full")
    invocation_id = uuid.uuid4().hex
    history.append(
        {
            "invocation_id": invocation_id,
            "started_at": _utc(),
            "mode": "historical",
            "capture_id": report.get("capture_id"),
            "root_id": report.get("root_id"),
            "revision": revision,
            "boot_stamp": report.get("boot_stamp"),
            "supervisor_pid": os.getpid(),
            "supervisor_pgid": os.getpgrp(),
            "resume_historical_stage": stage,
        }
    )
    report["active_invocation_id"] = invocation_id


def _session_historical_stage(report: dict[str, Any], mode: str) -> str | None:
    stage = report.get("resume_historical_stage")
    if stage is None:
        return None
    _validate_historical_stage(stage)
    history = report.get("historical_invocations")
    latest = history[-1] if isinstance(history, list) and history else None
    if (
        mode != "historical"
        or not isinstance(history, list)
        or len(history) > 64
        or not isinstance(latest, dict)
        or set(latest)
        != {
            "invocation_id",
            "started_at",
            "mode",
            "capture_id",
            "root_id",
            "revision",
            "boot_stamp",
            "supervisor_pid",
            "supervisor_pgid",
            "resume_historical_stage",
        }
        or not isinstance(latest.get("invocation_id"), str)
        or not CAPTURE_ID.fullmatch(latest["invocation_id"])
        or report.get("active_invocation_id") != latest["invocation_id"]
        or not isinstance(latest.get("started_at"), str)
        or latest.get("mode") != "historical"
        or latest.get("capture_id") != report.get("capture_id")
        or latest.get("root_id") != report.get("root_id")
        or latest.get("revision") != report.get("revision")
        or latest.get("boot_stamp") != report.get("boot_stamp")
        or type(latest.get("supervisor_pid")) is not int
        or latest["supervisor_pid"] <= 1
        or type(latest.get("supervisor_pgid")) is not int
        or latest["supervisor_pgid"] <= 1
        or latest.get("resume_historical_stage") != stage
    ):
        raise RunnerError("historical stage selection is not sealed to this invocation")
    return stage


def _validate_launch_process_identity(
    launch: dict[str, Any], command: list[str], pid: int, pgid: int
) -> str:
    identity = launch.get("process_identity_utc")
    identity_match = (
        PROCESS_IDENTITY_UTC.fullmatch(identity.strip())
        if isinstance(identity, str) and len(identity) <= 8192
        else None
    )
    launched_at_value = launch.get("launched_at_utc")
    try:
        launched_at = datetime.fromisoformat(launched_at_value)
        identity_started = datetime(
            int(identity_match.group(7)),
            {
                month: index
                for index, month in enumerate(
                    (
                        "Jan",
                        "Feb",
                        "Mar",
                        "Apr",
                        "May",
                        "Jun",
                        "Jul",
                        "Aug",
                        "Sep",
                        "Oct",
                        "Nov",
                        "Dec",
                    ),
                    start=1,
                )
            }[identity_match.group(4)],
            int(identity_match.group(5)),
            *map(int, identity_match.group(6).split(":")),
            tzinfo=UTC,
        )
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise RunnerError("latest-launch UTC process identity is malformed") from exc
    if (
        identity_match is None
        or identity_match.group(1) != str(pid)
        or identity_match.group(2) != str(pgid)
        or identity_match.group(3)
        != ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[identity_started.weekday()]
        or identity_match.group(8).strip() != " ".join(command)
        or not isinstance(launched_at_value, str)
        or launched_at.tzinfo is None
        or launched_at.utcoffset() != timedelta(0)
        or int(identity_started.timestamp()) != int(launched_at.timestamp())
    ):
        raise RunnerError("latest-launch UTC process identity does not match its command")
    return identity


def _validate_current_processing_launch(
    launch_path: Path,
    report_path: Path,
    report: dict[str, Any],
    launch: dict[str, Any],
    command: list[str],
    capture_id: str,
    pid: int,
    pgid: int,
    *,
    expected_report_boot_stamp: str | None = None,
) -> tuple[Path, bytes, str]:
    from kalshi_events.common import regular_bytes

    current_revision = _revision()
    expected_boot_stamp = (
        _boot_stamp() if expected_report_boot_stamp is None else expected_report_boot_stamp
    )
    processing_revision = launch.get("processing_revision")
    reconciliation_source = launch.get("reconciliation_code_source")
    if (
        not isinstance(processing_revision, str)
        or not REVISION.fullmatch(processing_revision)
        or processing_revision != report.get("revision")
        or not isinstance(launch.get("revision"), str)
        or not REVISION.fullmatch(launch["revision"])
        or not isinstance(reconciliation_source, dict)
        or reconciliation_source.get("revision") != launch.get("revision")
        or not isinstance(expected_boot_stamp, str)
        or not expected_boot_stamp
        or report.get("boot_stamp") != expected_boot_stamp
        or launch.get("session") != str(report_path)
        or launch.get("capture_id") != capture_id
        or launch.get("root_id") != report.get("root_id")
        or launch.get("pid") != pid
        or launch.get("pgid") != pgid
        or launch.get("state") != "launched"
        or launch.get("offline_only") is not True
        or launch.get("automatic_retry") is not False
        or command.count("--historical") != 1
        or command.count("--capture-id") != 1
        or command.count("--directory") != 1
    ):
        raise RunnerError("current processing revision is not bound to this stopped session")
    history = report.get("historical_invocations")
    if history is not None:
        latest = history[-1] if isinstance(history, list) and history else None
        if (
            not isinstance(history, list)
            or len(history) > 64
            or not isinstance(latest, dict)
            or latest.get("invocation_id") != report.get("active_invocation_id")
            or latest.get("mode") != "historical"
            or latest.get("capture_id") != capture_id
            or latest.get("root_id") != report.get("root_id")
            or latest.get("revision") != processing_revision
            or latest.get("boot_stamp") != report.get("boot_stamp")
            or latest.get("supervisor_pid") != pid
            or latest.get("supervisor_pgid") != pgid
        ):
            raise RunnerError(
                "current processing launch differs from its immutable invocation record"
            )

    _validate_launch_process_identity(launch, command, pid, pgid)
    checks_path = launch_path.with_name("launch-checks.json")
    try:
        checks_stat = checks_path.lstat()
    except OSError as exc:
        raise RunnerError("current launch checks are missing") from exc
    import stat

    if not stat.S_ISREG(checks_stat.st_mode) or checks_stat.st_nlink != 1:
        raise RunnerError("current launch checks are unsafe")
    checks_raw = regular_bytes(checks_path, MIB)
    try:
        checks = json.loads(checks_raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("current launch checks are malformed") from exc
    if (
        not isinstance(checks, dict)
        or set(checks)
        != {
            "checked_at_utc",
            "fresh_processing_cutoff_and_active_end_enforced",
            "original_grant_and_reconciliation_history_preserved",
            "new_window",
        }
        or checks.get("fresh_processing_cutoff_and_active_end_enforced") is not True
        or checks.get("original_grant_and_reconciliation_history_preserved") is not True
        or checks.get("new_window") != report.get("offline_historical_window")
    ):
        raise RunnerError("current launch checks do not bind the saved offline window")
    try:
        checked_at_value = checks["checked_at_utc"]
        checked_at = datetime.fromisoformat(
            checked_at_value.replace("Z", "+00:00") if isinstance(checked_at_value, str) else ""
        )
        launched_at = datetime.fromisoformat(launch.get("launched_at_utc", ""))
    except (TypeError, ValueError) as exc:
        raise RunnerError("current launch-check timestamp is invalid") from exc
    window = checks["new_window"]
    epoch_fields = (
        "started_epoch",
        "deadline_epoch",
    )
    monotonic_fields = ("started_monotonic", "deadline_monotonic")
    for field in (*epoch_fields, *monotonic_fields):
        value = window.get(field) if isinstance(window, dict) else None
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise RunnerError("current launch window has invalid clock values")
    launch_clocks = (
        "started_epoch",
        "processing_deadline_epoch",
        "active_end_epoch",
        "outer_deadline_epoch",
        "launched_monotonic",
        "processing_deadline_monotonic",
        "active_end_monotonic",
        "outer_deadline_monotonic",
    )
    for field in launch_clocks:
        value = launch.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise RunnerError("current launch has invalid processing clock values")
    checked_epoch = checked_at.timestamp()
    if (
        checked_at.tzinfo is None
        or checked_at.utcoffset() != timedelta(0)
        or launched_at.tzinfo is None
        or launched_at.utcoffset() != timedelta(0)
        or checked_epoch < launched_at.timestamp()
        or checked_epoch < float(window["started_epoch"])
        or checked_epoch > float(window["deadline_epoch"])
        or time.time() < checked_epoch
        or launch["processing_deadline_epoch"] > window["deadline_epoch"]
        or launch["active_end_epoch"] < window["deadline_epoch"]
        or launch["outer_deadline_epoch"] < launch["active_end_epoch"]
        or launch["processing_deadline_monotonic"] > window["deadline_monotonic"]
        or launch["active_end_monotonic"] < window["deadline_monotonic"]
        or launch["outer_deadline_monotonic"] < launch["active_end_monotonic"]
    ):
        raise RunnerError("current launch clocks do not preserve the checked offline deadline")
    return checks_path, checks_raw, current_revision


def _validate_inherited_launch_stdout(
    *,
    launch_path: Path,
    launch: dict[str, Any],
    report: dict[str, Any],
    capture_id: str,
    processing_revision: str,
    stdout: Path,
    stdout_info: os.stat_result,
    runtime_root: Path,
) -> None:
    import stat

    from kalshi_events.common import regular_bytes, safe_path

    runtime_root = safe_path(runtime_root)
    current_runtime = launch_path.parent
    authorization = launch.get("continuation_authorization")
    if not isinstance(authorization, dict):
        raise RunnerError("inherited stdout has no continuation archive binding")
    approval_id = authorization.get("approval_id")
    if not isinstance(approval_id, str) or not re.fullmatch(r"[0-9a-f]{32}", approval_id):
        raise RunnerError("inherited stdout continuation archive identity is invalid")

    archive_root = current_runtime / "continuation-archives"
    archive_directory = archive_root / approval_id
    failed_path = archive_directory / "failed-latest-launch.json"
    approval_path = archive_directory / "approval.json"
    if authorization.get("failed_latch_archive") != str(failed_path) or authorization.get(
        "approval_archive"
    ) != str(approval_path):
        raise RunnerError("inherited stdout archive paths are outside the current runtime")

    try:
        runtime_info = runtime_root.lstat()
        current_info = current_runtime.lstat()
        archive_root_info = archive_root.lstat()
        archive_info = archive_directory.lstat()
    except OSError as exc:
        raise RunnerError("inherited stdout continuation archive is missing or unsafe") from exc
    if (
        not stat.S_ISDIR(runtime_info.st_mode)
        or not stat.S_ISDIR(current_info.st_mode)
        or not stat.S_ISDIR(archive_root_info.st_mode)
        or not stat.S_ISDIR(archive_info.st_mode)
        or current_info.st_dev != runtime_info.st_dev
        or archive_root_info.st_dev != runtime_info.st_dev
        or archive_info.st_dev != runtime_info.st_dev
        or current_info.st_uid != os.geteuid()
        or archive_root_info.st_uid != os.geteuid()
        or archive_info.st_uid != os.geteuid()
        or stat.S_IMODE(archive_root_info.st_mode) & 0o077
        or stat.S_IMODE(archive_info.st_mode) & 0o077
    ):
        raise RunnerError("inherited stdout continuation archive directory is unsafe")

    def read_archive(path: Path, digest_value: Any) -> tuple[dict[str, Any], str]:
        if not isinstance(digest_value, str) or not re.fullmatch(r"[0-9a-f]{64}", digest_value):
            raise RunnerError("inherited stdout archive checksum is malformed")
        try:
            info = path.lstat()
        except OSError as exc:
            raise RunnerError("inherited stdout archive file is missing or unsafe") from exc
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_dev != runtime_info.st_dev
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            raise RunnerError("inherited stdout archive file is unsafe")
        raw = regular_bytes(path, MIB)
        digest = hashlib.sha256(raw).hexdigest()
        if digest != digest_value:
            raise RunnerError("inherited stdout archive checksum does not match")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeError, ValueError) as exc:
            raise RunnerError("inherited stdout archive JSON is malformed") from exc
        if not isinstance(value, dict):
            raise RunnerError("inherited stdout archive JSON is malformed")
        return value, digest

    failed, failed_digest = read_archive(failed_path, authorization.get("failed_latch_sha256"))
    approval, _ = read_archive(approval_path, authorization.get("approval_sha256"))
    old_revision = launch.get("revision")
    if (
        not isinstance(old_revision, str)
        or not REVISION.fullmatch(old_revision)
        or failed.get("state") != "preflight_or_launch_failed"
        or failed.get("session") != launch.get("session")
        or failed.get("capture_id") != capture_id
        or failed.get("root_id") != report.get("root_id")
        or failed.get("revision") != old_revision
        or failed.get("stdout") != launch.get("stdout")
        or launch.get("stdout") != str(stdout)
        or failed.get("offline_only") is not True
        or failed.get("automatic_retry") is not False
        or approval.get("approval_id") != approval_id
        or approval.get("action") != "continue_reconciled_cleanup_once"
        or approval.get("capture_id") != capture_id
        or approval.get("root_id") != report.get("root_id")
        or approval.get("revision") != processing_revision
        or approval.get("failed_launch_sha256") != failed_digest
    ):
        raise RunnerError("inherited stdout does not match its approved failed launch")

    try:
        relative_stdout = stdout.relative_to(runtime_root)
        runtime_entry = runtime_root / relative_stdout.parts[0]
        run_directory = runtime_entry / relative_stdout.parts[1]
        runtime_entry_info = runtime_entry.lstat()
        run_info = run_directory.lstat()
    except (OSError, ValueError, IndexError) as exc:
        raise RunnerError("inherited stdout is outside managed runtime logs") from exc
    if (
        len(relative_stdout.parts) != 3
        or relative_stdout.name != "stdout.log"
        or not relative_stdout.parts[1].startswith("run-")
        or stdout.parent != run_directory
        or stdout.parent.parent != runtime_entry
        or runtime_entry.parent != runtime_root
        or stdout.parent.parent == current_runtime
        or not stat.S_ISDIR(runtime_entry_info.st_mode)
        or not stat.S_ISDIR(run_info.st_mode)
        or runtime_entry_info.st_dev != runtime_info.st_dev
        or run_info.st_dev != runtime_info.st_dev
        or stdout_info.st_dev != runtime_info.st_dev
        or stdout_info.st_nlink != 1
    ):
        raise RunnerError("inherited stdout is not a same-device managed runtime log")


def _temporary_storage_stop_evidence(
    report: dict[str, Any],
    capture_id: str,
    original_group: Any,
    *,
    revision_source: str,
    derive: bool,
) -> dict[str, Any]:
    profile = report.get("resource_profile", "default")
    limits = _limits_from_report(report, profile)
    current = report.get("storage_supervision", {}).get("current")
    threshold = int(limits["storage_temporary_stop_bytes"])
    inclusive = bool(limits["storage_temporary_stop_inclusive"])
    observed = current.get("operator_temporary_bytes") if isinstance(current, dict) else None
    if type(original_group) is not int or original_group <= 1 or type(observed) is not int:
        raise RunnerError("temporary storage stop evidence has invalid group or observation")
    reached = observed >= threshold if inclusive else observed > threshold
    if not reached or report.get("error") != (
        f"live temporary storage reached the {threshold // GIB} GiB stop threshold"
    ):
        raise RunnerError("temporary storage stop does not match the sealed stop threshold")
    invocation_id = report.get("active_invocation_id")
    if invocation_id is None:
        invocation_id = report.get("storage_supervision_invocation")
    if not isinstance(invocation_id, str) or not CAPTURE_ID.fullmatch(invocation_id):
        raise RunnerError("temporary storage stop has no exact invocation identity")
    evidence = report.get("storage_safety_stop")
    if derive:
        evidence = {
            "version": 1,
            "mode": "historical",
            "category": "temporary",
            "invocation_id": invocation_id,
            "capture_id": capture_id,
            "root_id": report.get("root_id"),
            "revision": report.get("revision"),
            "revision_source": revision_source,
            "observed_bytes": observed,
            "threshold_bytes": threshold,
            "threshold_inclusive": inclusive,
            "original_process_group": original_group,
        }
    process = report.get("process_supervision")
    errors = process.get("termination_errors") if isinstance(process, dict) else None
    if (
        not isinstance(evidence, dict)
        or set(evidence)
        != {
            "version",
            "mode",
            "category",
            "invocation_id",
            "capture_id",
            "root_id",
            "revision",
            "revision_source",
            "observed_bytes",
            "threshold_bytes",
            "threshold_inclusive",
            "original_process_group",
        }
        or type(evidence.get("version")) is not int
        or evidence["version"] != 1
        or evidence.get("mode") != "historical"
        or evidence.get("category") != "temporary"
        or evidence.get("invocation_id") != invocation_id
        or evidence.get("capture_id") != capture_id
        or evidence.get("root_id") != report.get("root_id")
        or evidence.get("revision") != report.get("revision")
        or evidence.get("revision_source") not in {"session.revision", "processing_revision"}
        or evidence.get("observed_bytes") != observed
        or evidence.get("threshold_bytes") != threshold
        or evidence.get("threshold_inclusive") is not inclusive
        or evidence.get("original_process_group") != original_group
        or not isinstance(process, dict)
        or process.get("primary_stop_reason") != report.get("error")
        or process.get("termination_attempted") is not True
        or process.get("leader_reaped") is not True
        or process.get("invocation_token") != report.get("storage_supervision_invocation")
        or process.get("group_absent") is not None
        and type(process.get("group_absent")) is not bool
        or type(process.get("uncertain")) is not bool
        or process.get("uncertain") != (process.get("group_absent") is None)
        or not isinstance(errors, list)
        or len(errors) > 256
        or any(not isinstance(item, str) or len(item) > 512 for item in errors)
        or report.get("network_activity_this_invocation")
        != {"requests": 0, "downloaded_bytes": 0, "accounting_complete": True}
    ):
        raise RunnerError("temporary storage stop evidence is invalid")
    return dict(evidence)


def _original_supervisor_absent(pid: int, process_identity: str) -> bool:
    if not _pid_exists(pid):
        return True
    command = ["ps", "-p", str(pid), "-o", "pid=,pgid=,lstart=,command="]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError("original supervisor identity could not be verified") from exc
    rows = [line for line in result.stdout.splitlines() if line.strip()]
    if result.returncode not in {0, 1} or len(rows) > 1 or (result.returncode == 0) != bool(rows):
        raise RunnerError("original supervisor identity query is malformed")
    if not rows:
        if _pid_exists(pid):
            raise RunnerError(
                "saved original supervisor PID is present but identity is inaccessible"
            )
        return True
    expected = PROCESS_IDENTITY_UTC.fullmatch(process_identity.strip())
    actual = PROCESS_IDENTITY_UTC.fullmatch(rows[0].strip())
    if expected is None or actual is None or actual.group(1) != str(pid):
        raise RunnerError("original supervisor process identity is malformed")
    return actual.group(1, 4, 5, 6, 7) != expected.group(1, 4, 5, 6, 7)


def _require_stopped_storage_processes_absent(
    worker_group: int,
    supervisor_pid: int,
    supervisor_pgid: int,
    process_identity: str,
    *,
    allow_pid_reuse: bool,
) -> None:
    try:
        worker_group_exists = _process_group_exists(worker_group)
    except OSError as exc:
        raise RunnerError("stopped worker process-group absence could not be verified") from exc
    if worker_group_exists:
        raise RunnerError("stopped worker process group is still live or ambiguous")
    if _pid_exists(worker_group):
        if not allow_pid_reuse:
            raise RunnerError("stopped worker PID is still live or ambiguous")
        try:
            current_group = os.getpgid(worker_group)
        except ProcessLookupError:
            if _pid_exists(worker_group):
                raise RunnerError("stopped worker PID reuse could not be verified") from None
        except OSError as exc:
            raise RunnerError("stopped worker PID reuse could not be verified") from exc
        else:
            if current_group == worker_group:
                raise RunnerError("stopped worker process group reappeared")
    try:
        supervisor_group_exists = _process_group_exists(supervisor_pgid)
    except OSError as exc:
        raise RunnerError(
            "original supervisor process-group absence could not be verified"
        ) from exc
    if supervisor_group_exists:
        raise RunnerError("original supervisor process group is still live or ambiguous")
    if not _original_supervisor_absent(supervisor_pid, process_identity):
        raise RunnerError("original supervisor process is still live or ambiguous")
    try:
        group_reappeared = _process_group_exists(worker_group) or _process_group_exists(
            supervisor_pgid
        )
    except OSError as exc:
        raise RunnerError(
            "process-group absence could not be confirmed under the writer lock"
        ) from exc
    if group_reappeared:
        raise RunnerError("stopped worker or original supervisor reappeared during reconciliation")


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


def _historical_session_report_bytes(path: Path, *, max_bytes: int = 16 * MIB) -> bytes:
    """Read a regular session report safely without requiring control-file mode bits."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        before = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RunnerError("historical session report is missing or unsafe") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_size > max_bytes
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RunnerError("historical session report is unsafe or oversized")
        chunks = []
        size = 0
        while chunk := os.read(descriptor, min(65536, max_bytes + 1 - size)):
            chunks.append(chunk)
            size += len(chunk)
            if size > max_bytes:
                raise RunnerError("historical session report is oversized")
        after = os.fstat(descriptor)
        final = path.lstat()

        def signature(info: os.stat_result) -> tuple[int, ...]:
            return (
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_nlink,
                info.st_size,
                info.st_mtime_ns,
                info.st_ctime_ns,
            )

        if signature(opened) != signature(after) or signature(opened) != signature(final):
            raise RunnerError("historical session report changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _historical_path_device(path: Path) -> int:
    try:
        info = path.lstat()
    except OSError as exc:
        raise RunnerError("historical storage path is unavailable") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise RunnerError("historical storage path is not a real directory")
    return info.st_dev


_RESERVATION_BOUND_FIELDS = (
    "accounting_basis",
    "accounting_complete",
    "accounting_bound_verified",
    "attempts",
    "downloaded_bytes",
    "downloaded_bytes_upper_bound",
    "unfinished_reservation_ids",
    "reservation_inventory_sha256",
)


def _same_audited_reservation_bound(current: dict[str, Any], audited: dict[str, Any]) -> bool:
    return (
        audited.get("accounting_basis") == "reserved-upper-bound"
        and audited.get("accounting_complete") is False
        and audited.get("accounting_bound_verified") is True
        and audited.get("downloaded_bytes") is None
        and bool(audited.get("unfinished_reservation_ids"))
        and re.fullmatch(r"[0-9a-f]{64}", str(audited.get("reservation_inventory_sha256")))
        is not None
        and all(current.get(key) == audited.get(key) for key in _RESERVATION_BOUND_FIELDS)
    )


def _private_reboot_preserves_bound(event: dict[str, Any], audited: dict[str, Any]) -> bool:
    return (
        re.fullmatch(r"[0-9a-f]{64}", str(event.get("private_worker_reconciliation_sha256")))
        is not None
        and event.get("accounting_sha256") == audited.get("accounting_sha256")
        and _same_audited_reservation_bound(event, audited)
    )


def _historical_reboot_mount_proof(
    report: dict[str, Any],
    identity: dict[str, Any],
    freeze: dict[str, Any],
    *,
    final_device: int,
    final_volume_uuid: str,
    final_boot_stamp: str,
) -> dict[str, Any] | None:
    history = report.get("reboot_recovery_history")
    reconciliation = report.get("historical_reboot_reconciliation")
    has_history = history is not None
    has_reconciliation = reconciliation is not None
    if not has_history and not has_reconciliation:
        if final_device != freeze.get("root_device"):
            raise RunnerError("changed historical mount has no completed reboot proof")
        return None
    if (
        not isinstance(history, list)
        or not history
        or len(history) > 64
        or not isinstance(reconciliation, dict)
    ):
        raise RunnerError("historical reboot mount history is missing or malformed")
    origin_device = freeze.get("root_device")
    expected_volume = freeze.get("root_volume_uuid")
    if type(origin_device) is not int or origin_device < 0:
        raise RunnerError("frozen historical root device is invalid")
    if not isinstance(expected_volume, str) or not expected_volume:
        raise RunnerError("frozen historical volume UUID is invalid")
    expected_volume = expected_volume.upper()
    prior_device = origin_device
    prior_boot: str | None = None
    deadlines = {
        "started_epoch": report.get("started_epoch"),
        "time_budget_started_epoch": _time_budget_started(report, "epoch"),
        "deadline_epoch": _time_budget_deadline(report, "epoch"),
    }
    if not isinstance(history[-1], dict):
        raise RunnerError("historical reboot mount history contains a malformed event")
    recovery_indices = [
        index
        for index, event in enumerate(history)
        if isinstance(event, dict) and event.get("recovery_kind") == "prior_boot_temporary_stop"
    ]
    if len(recovery_indices) > 1:
        raise RunnerError("historical reboot history contains multiple legacy stop recoveries")
    prior_boot_temporary_stop = bool(recovery_indices)
    prior_device = origin_device
    prior_boot: str | None = None
    recovery_index = recovery_indices[0] if recovery_indices else None
    if recovery_index is not None:
        recovery = history[recovery_index]
        prefix = history[:recovery_index]
        original_system = report.get("original_system")
        if (
            not isinstance(original_system, dict)
            or type(original_system.get("filesystem_device")) is not int
            or original_system.get("filesystem_device") < 0
            or original_system.get("filesystem_volume_uuid") not in {None, expected_volume}
        ):
            raise RunnerError("legacy reboot origin is not bound to its original session")
        prior_device = original_system["filesystem_device"]
        legacy_fields = {
            "recovered_at",
            "previous_boot_stamp",
            "boot_stamp",
            "previous_filesystem_device",
            "filesystem_device",
            "volume_uuid",
            "prior_volume_uuid_available",
            "previous_monotonic",
            "wall_deadlines_preserved",
            "accounting_reconciliation_required",
        }
        for event in prefix:
            if not isinstance(event, dict):
                raise RunnerError("pre-freeze reboot history is not a valid legacy prefix")
            previous_monotonic = event.get("previous_monotonic")
            if (
                set(event) != legacy_fields
                or not isinstance(event.get("recovered_at"), str)
                or not isinstance(event.get("previous_boot_stamp"), str)
                or not event["previous_boot_stamp"]
                or not isinstance(event.get("boot_stamp"), str)
                or not event["boot_stamp"]
                or event["previous_boot_stamp"] == event["boot_stamp"]
                or type(event.get("previous_filesystem_device")) is not int
                or event["previous_filesystem_device"] != prior_device
                or type(event.get("filesystem_device")) is not int
                or event["filesystem_device"] < 0
                or event.get("volume_uuid") != expected_volume
                or type(event.get("prior_volume_uuid_available")) is not bool
                or not isinstance(previous_monotonic, dict)
                or set(previous_monotonic)
                != {"started_monotonic", "time_budget_started_monotonic", "deadline_monotonic"}
                or any(
                    type(value) not in {int, float} or not math.isfinite(value)
                    for value in previous_monotonic.values()
                )
                or event.get("wall_deadlines_preserved") != deadlines
                or event.get("accounting_reconciliation_required") is not True
                or prior_boot is not None
                and event["previous_boot_stamp"] != prior_boot
            ):
                raise RunnerError("pre-freeze reboot history is not a valid legacy prefix")
            prior_device = event["filesystem_device"]
            prior_boot = event["boot_stamp"]
        if prefix:
            if prior_device != origin_device or prior_boot != recovery.get("previous_boot_stamp"):
                raise RunnerError("legacy reboot history does not reach its frozen session origin")
        elif prior_device != origin_device:
            raise RunnerError("frozen session origin has no history binding")
        if (
            recovery.get("previous_filesystem_device") != origin_device
            or prior_boot is not None
            and recovery.get("previous_boot_stamp") != prior_boot
            or not isinstance(recovery.get("pre_recovery_history_sha256"), str)
            or recovery["pre_recovery_history_sha256"]
            != hashlib.sha256(
                json.dumps(prefix, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest()
        ):
            raise RunnerError("first post-freeze reboot event is not bound to its legacy prefix")
        if (
            not isinstance(identity.get("prior_session_sha256"), str)
            or recovery.get("session_before_sha256") != identity["prior_session_sha256"]
        ):
            raise RunnerError(
                "temporary-stop reboot proof is not bound to the frozen prior session"
            )
        if recovery_index == len(history) - 1:
            stop_audit = report.get("stopped_worker_reconciliation")
            stop_evidence = (
                stop_audit.get("storage_stop_evidence") if isinstance(stop_audit, dict) else None
            )
            if (
                not isinstance(stop_audit, dict)
                or stop_audit.get("recovery_kind") != "prior_boot_temporary_stop"
                or recovery.get("previous_boot_stamp") != stop_audit.get("boot_stamp")
                or recovery.get("previous_active_process_group")
                != stop_audit.get("stale_process_group")
                or recovery.get("stopped_worker_event_id") != stop_audit.get("event_id")
                or recovery.get("stopped_worker_audit_sha256")
                != stop_audit.get("recovery_audit_sha256")
                or recovery.get("stopped_worker_session_before_sha256")
                != stop_audit.get("session_before_sha256")
                or recovery.get("stopped_worker_reconciled_boot_stamp") != final_boot_stamp
                or stop_audit.get("recovered_boot_stamp") != final_boot_stamp
                or stop_audit.get("reconciliation_kind") != "temporary_storage_stop"
                or not isinstance(stop_evidence, dict)
                or stop_evidence.get("category") != "temporary"
                or reconciliation.get("recovery_kind") != "prior_boot_temporary_stop"
                or reconciliation.get("stopped_worker_event_id") != stop_audit.get("event_id")
                or reconciliation.get("stopped_worker_audit_sha256")
                != stop_audit.get("recovery_audit_sha256")
                or reconciliation.get("stopped_worker_session_before_sha256")
                != stop_audit.get("session_before_sha256")
            ):
                raise RunnerError("prior-boot temporary-stop recovery is not bound to its audit")
        events_to_validate = history[recovery_index:]
    else:
        events_to_validate = history

    for index, event in enumerate(events_to_validate):
        if not isinstance(event, dict):
            raise RunnerError("historical reboot mount history contains a malformed event")
        previous_boot = event.get("previous_boot_stamp")
        boot_stamp = event.get("boot_stamp")
        previous_device = event.get("previous_filesystem_device")
        filesystem_device = event.get("filesystem_device")
        previous_group = event.get("previous_active_process_group")
        if (
            not isinstance(previous_boot, str)
            or not previous_boot
            or not isinstance(boot_stamp, str)
            or not boot_stamp
            or previous_boot == boot_stamp
            or type(previous_device) is not int
            or previous_device != prior_device
            or type(filesystem_device) is not int
            or filesystem_device < 0
            or event.get("volume_uuid") != expected_volume
            or event.get("accounting_basis") != "exact"
            and not (
                prior_boot_temporary_stop
                and (
                    index == 0
                    and _same_audited_reservation_bound(event, recovery)
                    or index > 0
                    and _private_reboot_preserves_bound(event, recovery)
                )
            )
            or event.get("accounting_basis") == "exact"
            and event.get("accounting_complete") is not True
            or event.get("processing_worker_started") is not False
            or event.get("grant_started") is not False
            or type(previous_group) not in {int, type(None)}
            or isinstance(previous_group, int)
            and previous_group <= 1
            or event.get("prior_process_group_ignored_after_boot_change")
            is not (previous_group is not None)
            or event.get("wall_deadlines_preserved") != deadlines
            or not isinstance(event.get("accounting_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", event["accounting_sha256"]) is None
            or not isinstance(event.get("session_before_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", event["session_before_sha256"]) is None
        ):
            raise RunnerError("historical reboot mount history failed its transition proof")
        if index and previous_boot != prior_boot:
            raise RunnerError("historical reboot mount history has a stale boot transition")
        prior_boot = boot_stamp
        prior_device = filesystem_device
    latest = history[-1]
    bounded = latest.get("accounting_basis") == "reserved-upper-bound"
    accounting_proof_invalid = (
        reconciliation.get("accounting_basis") != latest.get("accounting_basis")
        or reconciliation.get("processing_worker_started") is not False
        or reconciliation.get("grant_started") is not False
    )
    if bounded:
        accounting_proof_invalid = accounting_proof_invalid or (
            latest.get("accounting_complete") is not False
            or latest.get("accounting_bound_verified") is not True
            or reconciliation.get("accounting_complete") is not False
            or reconciliation.get("accounting_bound_verified") is not True
            or report.get("accounting_complete") is not False
            or report.get("accounting_basis") != "reserved-upper-bound"
            or report.get("accounting_bound_verified") is not True
            or report.get("unfinished_reservation_ids") != latest.get("unfinished_reservation_ids")
            or report.get("consumption_state") != "reserved-upper-bound"
            or report.get("downloaded_bytes") is not None
            or report.get("downloaded_bytes_upper_bound")
            != latest.get("downloaded_bytes_upper_bound")
            or report.get("requests") != latest.get("attempts")
            or not latest.get("unfinished_reservation_ids")
        )
    else:
        accounting_proof_invalid = accounting_proof_invalid or (
            latest.get("accounting_basis") != "exact"
            or latest.get("accounting_complete") is not True
            or report.get("accounting_complete") is not True
            or report.get("unfinished_reservation_ids") != []
            or report.get("consumption_state") != "verified"
            or prior_boot_temporary_stop
            and recovery_index == len(history) - 1
            and (
                reconciliation.get("accounting_complete") is not True
                or reconciliation.get("accounting_bound_verified") is not False
                or report.get("accounting_basis") != "exact"
                or report.get("accounting_bound_verified") is not False
                or report.get("requests") != latest.get("attempts")
                or report.get("downloaded_bytes") != latest.get("downloaded_bytes")
            )
        )
    if (
        latest.get("boot_stamp") != final_boot_stamp
        or latest.get("filesystem_device") != final_device
        or latest.get("volume_uuid") != final_volume_uuid
        or report.get("boot_stamp") != final_boot_stamp
        or not isinstance(reconciliation, dict)
        or reconciliation.get("state") != "complete"
        or reconciliation.get("capture_id") != identity.get("capture_id")
        or reconciliation.get("root_id") != identity.get("root_id")
        or reconciliation.get("boot_stamp") != final_boot_stamp
        or accounting_proof_invalid
        or reconciliation.get("private_worker_reconciliation_sha256")
        != latest.get("private_worker_reconciliation_sha256")
        or report.get("root_id") != identity.get("root_id")
        or report.get("capture_id") != identity.get("capture_id")
    ):
        raise RunnerError("historical reboot reconciliation does not bind the current mount")
    original_system = report.get("original_system")
    if (
        not prior_boot_temporary_stop
        and isinstance(original_system, dict)
        and (
            original_system.get("filesystem_device") != origin_device
            or original_system.get("filesystem_volume_uuid") not in {None, expected_volume}
        )
    ):
        raise RunnerError("historical reboot origin differs from its frozen root")
    return latest


def _historical_read_bounded_private_reboot(
    control_root: Path, identity: dict[str, Any], event: dict[str, Any]
) -> None:
    import historical_grants as grants

    worker = event.get("private_worker_reconciliation")
    grant_id = worker.get("grant_id") if isinstance(worker, dict) else None
    if not isinstance(grant_id, str) or re.fullmatch(r"[0-9a-f]{32}", grant_id) is None:
        raise RunnerError("bounded private reboot has no exact grant binding")
    grants_root = control_root / "grants"
    grant_root = grants_root / grant_id
    try:
        grants._private_directory(grants_root, "historical grants directory")
        grants._private_directory(grant_root, "historical grant directory")
        record = grants.read_json(grant_root / "private-reconciliation.json")
    except grants.ControlError as exc:
        raise RunnerError("bounded private reboot record is missing or unsafe") from exc
    body = {key: value for key, value in record.items() if key != "sha256"}
    if (
        record.get("sha256") != grants.sha256_json(body)
        or record.get("sha256") != event.get("private_worker_reconciliation_sha256")
        or record.get("kind") != "changed_boot_worker_ignored"
        or record.get("identity") != identity
        or record.get("grant_id") != grant_id
        or record.get("launch_sha256") != worker.get("launch_sha256")
        or record.get("session_before_sha256") != event.get("session_before_sha256")
        or record.get("from_boot_stamp") != event.get("previous_boot_stamp")
        or record.get("to_boot_stamp") != event.get("boot_stamp")
        or record.get("previous_active_process_group") != event.get("previous_active_process_group")
        or record.get("accounting_sha256") != event.get("accounting_sha256")
        or record.get("process_group_interaction") != "not_queried_after_boot_change"
        or record.get("reconciliation_worker_started") is not False
        or record.get("new_grant_started") is not False
    ):
        raise RunnerError("bounded private reboot record differs from its transition")


def _historical_current_mount(
    directory: Path,
    operator: Path,
    control_root: Path,
    report: dict[str, Any],
    identity: dict[str, Any],
    freeze: dict[str, Any],
    *,
    current_boot: str | None = None,
    session_root_device: int | None = None,
    allow_unreconciled_boot_change: bool = False,
    expected_current_device: int | None = None,
    expected_current_volume_uuid: str | None = None,
) -> dict[str, Any]:
    """Validate the live mount while retaining the original frozen device as provenance."""
    control_root = _historical_control_root(control_root)
    current_boot = _boot_stamp() if current_boot is None else current_boot
    system = report.get("system")
    if not isinstance(system, dict):
        raise RunnerError("historical report has no filesystem identity")
    try:
        root_device = _historical_path_device(directory)
        operator_device = _historical_path_device(operator)
        control_device = _historical_path_device(control_root)
        root_volume = _volume_uuid(directory).upper()
        operator_volume = _volume_uuid(operator).upper()
        control_volume = _volume_uuid(control_root).upper()
    except OSError as exc:
        raise RunnerError("historical current mount identity is unavailable") from exc
    expected_volume = freeze.get("root_volume_uuid")
    saved_device = system.get("filesystem_device")
    if (
        not isinstance(expected_volume, str)
        or not expected_volume
        or type(saved_device) is not int
        or saved_device < 0
        or root_device != operator_device
        or root_device != control_device
        or (root_volume, operator_volume, control_volume) != (expected_volume.upper(),) * 3
        or system.get("filesystem_volume_uuid") not in {None, expected_volume.upper()}
        or _owner_root_id(directory) != identity.get("root_id")
        or report.get("root_id") != identity.get("root_id")
        or report.get("capture_id") != identity.get("capture_id")
        or session_root_device is not None
        and session_root_device != saved_device
        or expected_current_device is not None
        and expected_current_device != root_device
        or expected_current_volume_uuid is not None
        and expected_current_volume_uuid.upper() != root_volume
    ):
        raise RunnerError("historical root, operator or controls mount identity changed")
    if allow_unreconciled_boot_change:
        if report.get("boot_stamp") == current_boot:
            raise RunnerError("private reboot reconciliation has no changed-boot proof")
        if system.get("filesystem_volume_uuid") is None and saved_device != freeze.get(
            "root_device"
        ):
            raise RunnerError("changed historical mount has no saved volume identity")
        _historical_reboot_mount_proof(
            report,
            identity,
            freeze,
            final_device=saved_device,
            final_volume_uuid=expected_volume.upper(),
            final_boot_stamp=str(report.get("boot_stamp")),
        )
    else:
        if report.get("boot_stamp") != current_boot or saved_device != root_device:
            raise RunnerError("historical report does not match the current boot and mount")
        if system.get("filesystem_volume_uuid") is None and (
            root_device != freeze.get("root_device")
            or report.get("reboot_recovery_history") is not None
            or report.get("historical_reboot_reconciliation") is not None
        ):
            raise RunnerError("reconciled historical report has no current volume UUID")
        _historical_reboot_mount_proof(
            report,
            identity,
            freeze,
            final_device=root_device,
            final_volume_uuid=root_volume,
            final_boot_stamp=current_boot,
        )
    device_lineage = [freeze["root_device"]]
    for event in report.get("reboot_recovery_history") or []:
        if (
            event.get("accounting_basis") == "reserved-upper-bound"
            and event.get("private_worker_reconciliation_sha256") is not None
        ):
            _historical_read_bounded_private_reboot(control_root, identity, event)
        device = event.get("filesystem_device")
        if type(device) is int and device not in device_lineage:
            device_lineage.append(device)
    if not allow_unreconciled_boot_change and root_device not in device_lineage:
        raise RunnerError("current historical device is outside its completed reboot lineage")
    return {
        "device": root_device,
        "volume_uuid": root_volume,
        "saved_report_device": saved_device,
        "boot_stamp": current_boot,
        "device_lineage": device_lineage,
    }


def _historical_resume_source_identity_equal(
    persisted: Any,
    other: Any,
    *,
    store: Any,
    state: dict[str, Any],
    verified_mount: dict[str, Any] | None,
    require_live: bool = False,
) -> bool:
    from kalshi_events import historical_resume

    if persisted is None or other is None:
        return persisted is None and other is None
    normalized_persisted = historical_resume.normalize_resume_file_identity(
        persisted,
        store=store,
        state=state,
        verified_mount=verified_mount,
    )
    normalized_other = historical_resume.normalize_resume_file_identity(
        other,
        store=store,
        state=state,
        verified_mount=verified_mount,
    )
    if require_live and not historical_resume.resume_file_identity_matches(
        persisted,
        other,
        store=store,
        state=state,
        verified_mount=verified_mount,
    ):
        return False
    return normalized_persisted == normalized_other


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
                try:
                    memory = _sample_memory(process.pid, timeout=timeout)
                except (OSError, subprocess.SubprocessError, RunnerError) as exc:
                    report["memory_sample_error"] = _memory_sample_error(exc)
                    raise
                report.pop("memory_sample_error", None)
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


def _bounded_memory_diagnostic(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    elif not isinstance(value, str):
        value = str(value)
    value = re.sub(r"'(/[^']*)'", "<path>", value)
    value = re.sub(r'"(/[^\"]*)"', "<path>", value)
    value = re.sub(r"(?<![\w:/'\"])/(?!/)[\s\S]*", "<path>", value)
    value = "".join(char for char in value if char.isprintable() or char.isspace())
    return " ".join(value.split())[:512]


def _memory_worker_error(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        command = exc.cmd
        executable = command[0] if isinstance(command, (list, tuple)) and command else command
        family = Path(str(executable)).name
        if family not in {"vm_stat", "sysctl", "ps"}:
            family = "unknown"
        message = f"{family} probe exited with status {exc.returncode}"
        stderr = _bounded_memory_diagnostic(exc.stderr)
        if stderr:
            message += f"; stderr: {stderr}"
        return _bounded_memory_diagnostic(message)
    return _bounded_memory_diagnostic(str(exc)) or type(exc).__name__


def _memory_sample_error(exc: BaseException) -> str:
    message = _bounded_memory_diagnostic(str(exc)) or type(exc).__name__
    causes = []
    cause = exc.__cause__
    while cause is not None and len(causes) < 3:
        causes.append(type(cause).__name__)
        cause = cause.__cause__
    if causes:
        suffix = " (caused by " + " -> ".join(causes) + ")"
        message = message[: max(0, 512 - len(suffix))] + suffix
    return _bounded_memory_diagnostic(message)


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
        command = ["ps", "-g", str(group), "-o", "pgid=,pid=,rss="]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
        if result.returncode not in {0, 1}:
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
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
        stdout, stderr = worker.communicate(timeout=work_timeout)
    except subprocess.TimeoutExpired as exc:
        _stop_memory_sample_worker(worker, sample_deadline)
        raise RunnerError("bounded memory sample worker timed out") from exc
    except BaseException:
        _stop_memory_sample_worker(worker, sample_deadline)
        raise
    sample_completed = time.monotonic()
    stderr_detail = _bounded_memory_diagnostic(stderr)
    try:
        value = json.loads(stdout)
    except (TypeError, ValueError) as exc:
        detail = "bounded memory sample returned malformed JSON"
        if stderr_detail:
            detail += f"; worker stderr: {stderr_detail}"
        raise RunnerError(_bounded_memory_diagnostic(detail)) from exc
    if worker.returncode != 0 or not isinstance(value, dict):
        message = value.get("error") if isinstance(value, dict) else None
        detail = (
            f"bounded memory sample failed: {_bounded_memory_diagnostic(message)}"
            if isinstance(message, str)
            else "bounded memory sample failed"
        )
        if stderr_detail:
            detail += f"; worker stderr: {stderr_detail}"
        raise RunnerError(_bounded_memory_diagnostic(detail))
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
        print(json.dumps({"error": _memory_worker_error(exc)}, sort_keys=True))
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
    wall_deadline_epoch_override: float | None = None,
    phase_on_start: str | None = None,
    storage_supervision: bool = False,
    storage_invocation_token: str | None = None,
    metrics_progress_path: Path | None = None,
    storage_progress_path: Path | None = None,
    child_source_root: Path | None = None,
    storage_maintenance: bool = False,
    supervisor_handoff: dict[str, Any] | None = None,
) -> tuple[int | None, str | None, bool]:
    from kalshi_events.common import safe_path
    from kalshi_events.paths import child_environment, project_root

    child_source_root = (
        SOURCE_ROOT if child_source_root is None else _external_path(child_source_root)
    )
    if not (child_source_root / "src").is_dir() or not (child_source_root / "scripts").is_dir():
        raise RunnerError("supervised child source root is incomplete")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(child_source_root / "src")
    env = child_environment(env, project_root(code_root=child_source_root))
    if supervisor_handoff is not None and not storage_supervision:
        raise RunnerError("private supervisor handoff requires live storage supervision")
    directory = _external_path(directory)
    original_device = directory.stat().st_dev
    recorded_device = report.get("system", {}).get("filesystem_device")
    if recorded_device != original_device:
        raise RunnerError("production filesystem changed since session preflight")
    stdout_path = safe_path(stdout_path)
    stderr_path = safe_path(stderr_path)
    if time.monotonic() >= deadline - float(_effective_limits()["terminate_seconds"]):
        return None, "insufficient time remains to start and reap a child", True
    wall_deadline_epoch = (
        wall_deadline_epoch_override
        if wall_deadline_epoch_override is not None
        else _child_wall_deadline_epoch(report, deadline, workflow_deadline=workflow_deadline)
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
    report.pop("memory_sample_error", None)
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
        storage_token = None
        handoff_read_fd: int | None = None
        handoff_write_fd: int | None = None
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
                if storage_progress_path is None or not isinstance(storage_invocation_token, str):
                    raise RunnerError("historical storage progress path is unavailable")
                if re.fullmatch(r"[0-9a-f]{32}", storage_invocation_token) is None:
                    raise RunnerError("historical storage invocation token is invalid")
                storage_token = storage_invocation_token
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
                    invocation_token=storage_token,
                    allow_temporary_cleanup=storage_maintenance,
                )
            if supervisor_handoff is not None:
                expected_handoff_fields = {
                    "directory",
                    "control_root",
                    "grant_id",
                    "launch_sha256",
                    "phase",
                    "substep",
                    "storage_invocation_token",
                    "storage_progress_path",
                }
                if (
                    set(supervisor_handoff) != expected_handoff_fields
                    or supervisor_handoff.get("directory") != str(directory)
                    or supervisor_handoff.get("storage_invocation_token") != storage_token
                    or supervisor_handoff.get("storage_progress_path") != str(storage_progress_path)
                ):
                    raise RunnerError(
                        "private supervisor handoff fields do not match the live child"
                    )
                handoff_read_fd, handoff_write_fd = os.pipe()
                os.set_inheritable(handoff_read_fd, True)
                env[HISTORICAL_SUPERVISOR_FD_ENV] = str(handoff_read_fd)
            _SPAWNING_CHILD = True
            try:
                process = subprocess.Popen(
                    command,
                    stdout=stdout,
                    stderr=stderr,
                    cwd=child_source_root,
                    env=env,
                    shell=False,
                    start_new_session=True,
                    pass_fds=(handoff_read_fd,) if handoff_read_fd is not None else (),
                )
                if handoff_read_fd is not None:
                    os.close(handoff_read_fd)
                    handoff_read_fd = None
                if handoff_write_fd is not None:
                    handoff = {
                        "version": 1,
                        "nonce": uuid.uuid4().hex,
                        **supervisor_handoff,
                        "supervisor_pid": os.getpid(),
                        "supervisor_pgid": os.getpgrp(),
                        "worker_pid": process.pid,
                        "worker_pgid": process.pid,
                    }
                    payload = json.dumps(
                        handoff, sort_keys=True, separators=(",", ":"), allow_nan=False
                    ).encode("utf-8")
                    pending = memoryview(payload)
                    while pending:
                        written = os.write(handoff_write_fd, pending)
                        if written <= 0:
                            raise RunnerError("private supervisor handoff pipe closed early")
                        pending = pending[written:]
                    os.close(handoff_write_fd)
                    handoff_write_fd = None
                if phase_on_start is not None:
                    report["phase"] = phase_on_start
                if storage_supervision:
                    report["storage_supervision_invocation"] = storage_token
                    report["storage_supervision_file"] = str(storage_progress_path)
                report["active_process_group"] = process.pid
                private_grant = report.get("private_historical_grant")
                if supervisor_handoff is not None and isinstance(private_grant, dict):
                    private_grant["worker_pid"] = process.pid
                    private_grant["worker_pgid"] = process.pid
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
            for descriptor in (handoff_read_fd, handoff_write_fd):
                if descriptor is not None:
                    with contextlib.suppress(OSError):
                        os.close(descriptor)
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
    wall_deadline_epoch_override: float | None = None,
    preserve_session: bool = False,
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
    if preserve_session:
        # The enclosing private worker owns the active group and its report.
        # Keep the verifier in that group so it shares the same live supervision.
        if storage_supervision:
            raise RunnerError("private verification cannot create a second storage supervisor")
        timeout = _child_time_remaining(deadline, wall_deadline_epoch_override) - float(
            _effective_limits()["terminate_seconds"]
        )
        if timeout <= 0:
            raise BudgetStop("insufficient time remains for private release verification")
        env = {**os.environ, "PYTHONPATH": str(SOURCE_ROOT / "src")}
        try:
            with stdout.open("xb") as out, stderr.open("xb") as err:
                result = subprocess.run(
                    command, env=env, stdout=out, stderr=err, timeout=timeout, check=False
                )
        except subprocess.TimeoutExpired as exc:
            raise BudgetStop("private release verification exceeded its deadline") from exc
        if _child_time_remaining(deadline, wall_deadline_epoch_override) <= 0:
            raise BudgetStop("private release verification exceeded its deadline")
        value = _strict_json(stdout)
        if result.returncode != 0 or value is None:
            raise RunnerError("standalone release verification failed or has unknown status")
        return value
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
            wall_deadline_epoch_override=wall_deadline_epoch_override,
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
        or report.get("mode") not in {"full", "resume", "historical"}
        or report.get("session_id") != session_id
        or report.get("capture_id") != capture_id
        or report.get("root_id") != expected_root_id
        or report.get("revision") != expected_revision
        or report.get("active_process_group") != os.getpgrp()
        or os.getpgrp() != os.getpid()
    ):
        raise RunnerError("initial historical accounting session binding changed")
    _session_historical_stage(report, "historical")
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


def _accounting_store(root: Path) -> Any:
    from kalshi_events.common import Limits, Store

    return Store(
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


def _read_accounting_worker(
    root: Path,
    expected_capture_id: str | None = None,
    *,
    locked_store: Any | None = None,
    cleanup_historical_stages: bool = False,
    expected_directory: Path | None = None,
    report_path: Path | None = None,
    expected_root_id: str | None = None,
    expected_revision: str | None = None,
) -> dict[str, Any]:
    from kalshi_events.common import Store, decode, regular_bytes

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

    if locked_store is not None and (
        cleanup_historical_stages
        or not isinstance(locked_store, Store)
        or locked_store.root != Path(root).resolve()
        or not locked_store.writer_locked_by_current_thread
    ):
        raise RunnerError("accounting scan requires the validated locked operator store")
    store = locked_store if locked_store is not None else _accounting_store(root)
    if cleanup_historical_stages and expected_directory is not None:
        from production_activation import (
            _activation_temporary_names,
            recover_activation_temporaries,
        )

        if _activation_temporary_names(expected_directory):
            assert report_path is not None
            assert expected_capture_id is not None
            assert expected_root_id is not None
            assert expected_revision is not None
            with store.lock():
                _validate_initial_historical_accounting_session(
                    root,
                    expected_directory,
                    report_path,
                    expected_capture_id,
                    expected_root_id,
                    expected_revision,
                )
                pointer = _strict_json(root / "current-historical.json")
                release_id = pointer.get("source_release_id") if pointer else None
                if not isinstance(release_id, str) or not re.fullmatch(r"[0-9a-f]{64}", release_id):
                    raise RunnerError("stale activation links have no current historical release")
            recover_activation_temporaries(
                root,
                expected_directory,
                expected_release_id=release_id,
                expected_capture_id=expected_capture_id,
                catalogue_version=3,
                max_database_bytes=store.limits.max_database_bytes,
                max_temp_bytes=store.limits.max_temp_bytes,
                max_duration_seconds=store.remaining,
            )
    with store.lock() if locked_store is None else contextlib.nullcontext():
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
            session = _validate_initial_historical_accounting_session(
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
            preserve_stage = session.get("resume_historical_stage") if session else None
            if preserve_stage is not None:
                _validate_historical_stage(preserve_stage)
                cleanup(store, expected_capture_id, preserve_stage=preserve_stage)
            else:
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
    max_duration_seconds: float | None = None,
    expected_observations: int | None = None,
    activation_audit: dict[str, Any] | None = None,
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
        max_duration_seconds=max_duration_seconds,
        expected_observations=expected_observations,
        activation_audit=activation_audit,
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
    re.compile(r"activation-readback-[0-9a-f]{32}\Z"),
    re.compile(r"ordering-policy-[0-9a-f]{32}\Z"),
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
    count[0] += 1
    if count[0] > int(_effective_limits()["storage_inventory_entries"]):
        raise RunnerError("live storage inventory exceeded its entry bound")

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
    for attempt in range(3):
        try:
            return _live_workspace_snapshot_once(operator, directory, capture_id, expected_root_id)
        except FileNotFoundError:
            if attempt == 2:
                raise
        except RunnerError as exc:
            cause = exc.__cause__
            seen = {id(exc)}
            while cause is not None and id(cause) not in seen:
                if isinstance(cause, FileNotFoundError):
                    break
                seen.add(id(cause))
                cause = cause.__cause__
            else:
                raise
            if attempt == 2:
                raise


def _live_workspace_snapshot_once(
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
                if entry.name == "writer.lock" and (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_dev != root_info.st_dev
                    or info.st_nlink != 1
                ):
                    raise RunnerError("operator root writer lock is missing or unsafe")
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
    # Count direct entries once; raw/capture descendants are intentionally exempt.
    count = [len(operator_entries)]
    writer_lock = operator_entries.get("writer.lock")
    if writer_lock is None:
        raise RunnerError("operator root writer lock is missing or unsafe")
    logical += writer_lock[3]
    lock_inode = (writer_lock[0], writer_lock[1])
    if lock_inode not in allocated_inodes:
        allocated += writer_lock[5] * 512
        allocated_inodes.add(lock_inode)
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
                count[0] += 1
                if count[0] > int(_effective_limits()["storage_inventory_entries"]):
                    raise RunnerError("live storage inventory exceeded its entry bound")
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
        "inventory_entries": count[0],
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


def _record_temporary_storage_stop(report: dict[str, Any], observed: int) -> None:
    invocation_id = report.get("active_invocation_id")
    group = report.get("active_process_group")
    if invocation_id is None:
        return
    if (
        not isinstance(invocation_id, str)
        or not CAPTURE_ID.fullmatch(invocation_id)
        or type(group) is not int
        or group <= 1
        or not isinstance(report.get("revision"), str)
        or not REVISION.fullmatch(report["revision"])
    ):
        raise RunnerError("temporary storage stop lacks its sealed invocation identity")
    limits = _effective_limits()
    report["storage_safety_stop"] = {
        "version": 1,
        "mode": "historical",
        "category": "temporary",
        "invocation_id": invocation_id,
        "capture_id": report.get("capture_id"),
        "root_id": report.get("root_id"),
        "revision": report["revision"],
        "revision_source": "session.revision",
        "observed_bytes": observed,
        "threshold_bytes": int(limits["storage_temporary_stop_bytes"]),
        "threshold_inclusive": bool(limits["storage_temporary_stop_inclusive"]),
        "original_process_group": group,
    }


def _begin_storage_supervision(
    directory: Path,
    operator: Path,
    report: dict[str, Any],
    progress_path: Path,
    external_paths: dict[str, Path],
    *,
    invocation_token: str | None = None,
    allow_temporary_cleanup: bool = False,
) -> dict[str, Any]:
    invocation_token = invocation_token or uuid.uuid4().hex
    if re.fullmatch(r"[0-9a-f]{32}", invocation_token) is None:
        raise RunnerError("live storage supervision invocation token is invalid")
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
    temporary_stop = _storage_stop_reached(saved["operator_temporary_bytes"], "temporary")
    if temporary_stop:
        threshold = int(_effective_limits()["storage_temporary_stop_bytes"])
        hard_ceiling = int(_effective_limits()["temporary_bytes"])
        if not allow_temporary_cleanup or saved["operator_temporary_bytes"] > hard_ceiling:
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
        "invocation_token": invocation_token,
        "maintenance_temporary_baseline_bytes": (
            saved["operator_temporary_bytes"]
            if allow_temporary_cleanup and temporary_stop
            else None
        ),
        "maintenance_temporary_growth_allowance_bytes": (
            64 * MIB if allow_temporary_cleanup and temporary_stop else 0
        ),
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
        if name == "writer.lock":
            previous = baseline_entries[name]
            current = current_entries[name]
            unchanged_size = current[3] == previous[3] and current[5] == previous[5]
            filelock_truncated = current[3] == 0 and current[5] <= previous[5]
            if current[:3] != previous[:3] or not (unchanged_size or filelock_truncated):
                raise RunnerError("canonical writer lock changed unsafely during live work")
            continue
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
    maintenance_baseline = state.get("maintenance_temporary_baseline_bytes")
    maintenance_allowance = state.get("maintenance_temporary_growth_allowance_bytes", 0)
    if type(maintenance_baseline) is int and (
        type(maintenance_allowance) is not int
        or maintenance_allowance < 0
        or current["operator_temporary_bytes"] > maintenance_baseline + maintenance_allowance
    ):
        return "startup cleanup exceeded its fixed temporary metadata allowance"
    if _storage_stop_reached(current["operator_temporary_bytes"], "temporary") and not (
        type(maintenance_baseline) is int
        and current["operator_temporary_bytes"] <= maintenance_baseline + maintenance_allowance
    ):
        _record_temporary_storage_stop(report, current["operator_temporary_bytes"])
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
    directory: Path,
    report_path: Path,
    report: dict[str, Any],
    capture_id: str,
    *,
    expected_saved_boot_stamp: str | None = None,
) -> tuple[dict[str, Any], Path, bytes, Path, bytes, Path | None, bytes | None]:
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
    launch_checks_path: Path | None = None
    launch_checks_raw: bytes | None = None
    reconciliation_runtime_revision: str | None = None
    if launch_revision == report.get("revision"):
        revision_source = "session.revision"
    else:
        (
            launch_checks_path,
            launch_checks_raw,
            reconciliation_runtime_revision,
        ) = _validate_current_processing_launch(
            launch_path,
            report_path,
            report,
            launch,
            command,
            capture_id,
            launch_pid,
            launch_pgid,
            expected_report_boot_stamp=expected_saved_boot_stamp,
        )
        revision_source = "processing_revision"
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
    process_identity_utc = launch.get("process_identity_utc")
    if process_identity_utc is not None:
        _validate_launch_process_identity(launch, command, launch_pid, launch_pgid)
    else:
        process_identity = launch.get("process_identity")
        if (
            revision_source == "processing_revision"
            or not isinstance(process_identity, str)
            or process_identity.split(maxsplit=2)[:2] != [str(launch_pid), str(launch_pgid)]
        ):
            raise RunnerError("latest-launch process identity is ambiguous")
    stdout_path = launch.get("stdout")
    if not isinstance(stdout_path, str):
        raise RunnerError("latest-launch stdout path is missing")
    try:
        stdout = safe_path(Path(stdout_path))
        stdout_info = stdout.lstat()
    except OSError as exc:
        raise RunnerError("latest-launch stdout path is unsafe") from exc
    if not stat.S_ISREG(stdout_info.st_mode):
        raise RunnerError("latest-launch stdout is unsafe")
    if stdout.parent.parent != source_root.parent:
        if revision_source != "processing_revision":
            raise RunnerError("latest-launch stdout path is outside its managed run")
        _validate_inherited_launch_stdout(
            launch_path=launch_path,
            launch=launch,
            report=report,
            capture_id=capture_id,
            processing_revision=launch.get("processing_revision"),
            stdout=stdout,
            stdout_info=stdout_info,
            runtime_root=runtime_root,
        )
    original_group = report.get("active_process_group")
    typed_stop = report.get("storage_safety_stop")
    derived_temporary_stop = typed_stop is None and revision_source == "processing_revision"
    if typed_stop is not None or derived_temporary_stop:
        evidence = _temporary_storage_stop_evidence(
            report,
            capture_id,
            original_group,
            revision_source=revision_source,
            derive=derived_temporary_stop,
        )
        evidence_path = report_path
        evidence_raw = (
            json.dumps(evidence, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode()
    else:
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
            evidence_started = datetime.fromisoformat(
                str(process_started_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise RunnerError("stopped-worker process start identity is invalid") from exc
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
    evidence["launch_revision_source"] = revision_source
    evidence["processing_revision"] = (
        launch.get("processing_revision") if revision_source == "processing_revision" else None
    )
    evidence["supervisor_pid"] = launch_pid
    evidence["supervisor_pgid"] = launch_pgid
    evidence["original_process_group"] = original_group
    evidence["process_identity_utc"] = process_identity_utc
    evidence["reconciliation_runtime_revision"] = reconciliation_runtime_revision
    raw_launch_after = regular_bytes(launch_path, MIB)
    if raw_launch_after != launch_raw:
        raise RunnerError("latest-launch changed during stopped-worker validation")
    return (
        evidence,
        evidence_path,
        evidence_raw,
        launch_path,
        launch_raw,
        launch_checks_path,
        launch_checks_raw,
    )


def _stopped_memory_worker_evidence(
    directory: Path, report_path: Path, report: dict[str, Any], capture_id: str
) -> tuple[dict[str, Any], Path, bytes, bytes, str]:
    from kalshi_events.common import regular_bytes, safe_path

    stop_reason = "resource accounting failed: memory snapshot unavailable"
    profile, sealed_report = _session_resource_profile(report_path, capture_id=capture_id)
    if sealed_report != report:
        raise RunnerError("stopped-worker session report changed during validation")
    raw_report = regular_bytes(report_path, 16 * MIB)
    try:
        if json.loads(raw_report.decode("utf-8")) != report:
            raise RunnerError("stopped-worker session report changed during validation")
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("stopped-worker session report is malformed") from exc

    supervision = report.get("process_supervision")
    group = report.get("active_process_group")
    token = supervision.get("invocation_token") if isinstance(supervision, dict) else None
    errors = supervision.get("termination_errors") if isinstance(supervision, dict) else None
    group_absent = supervision.get("group_absent") if isinstance(supervision, dict) else None
    leader_reaped = supervision.get("leader_reaped") if isinstance(supervision, dict) else None
    if (
        report.get("error") != stop_reason
        or not isinstance(supervision, dict)
        or supervision.get("primary_stop_reason") != stop_reason
        or type(group) is not int
        or group <= 1
        or type(leader_reaped) is not bool
        or leader_reaped is not True
        or group_absent is not None
        and type(group_absent) is not bool
        or type(supervision.get("uncertain")) is not bool
        or supervision["uncertain"] != (not leader_reaped or group_absent is None)
        or supervision.get("termination_attempted") is not True
        or not isinstance(errors, list)
        or len(errors) > 256
        or any(not isinstance(item, str) or len(item) > 512 for item in errors)
        or group_absent is None
        and not errors
        or not isinstance(token, str)
        or not re.fullmatch(r"[0-9a-f]{32}", token)
        or report.get("storage_supervision_invocation") != token
        or type(report.get("network_activity_this_invocation")) is not dict
        or set(report["network_activity_this_invocation"])
        != {"requests", "downloaded_bytes", "accounting_complete"}
        or type(report["network_activity_this_invocation"].get("requests")) is not int
        or report["network_activity_this_invocation"]["requests"] != 0
        or type(report["network_activity_this_invocation"].get("downloaded_bytes")) is not int
        or report["network_activity_this_invocation"]["downloaded_bytes"] != 0
        or report["network_activity_this_invocation"].get("accounting_complete") is not True
    ):
        raise RunnerError("memory-stop termination evidence is invalid")

    source_value = report.get("source_root")
    if not isinstance(source_value, str):
        raise RunnerError("memory-stop reconciliation has no saved source root")
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

    launch_pid = launch.get("pid")
    launch_pgid = launch.get("pgid")
    command = launch.get("command")
    launch_revision = launch.get("revision")
    if (
        launch.get("session") != str(report_path)
        or launch.get("capture_id") != capture_id
        or launch.get("root_id") != report.get("root_id")
        or launch_revision != report.get("revision")
        or not isinstance(launch_revision, str)
        or not REVISION.fullmatch(launch_revision)
        or type(launch_pid) is not int
        or launch_pid <= 1
        or type(launch_pgid) is not int
        or launch_pgid != launch_pid
        or launch.get("offline_only") is not True
        or launch.get("automatic_retry") is not False
        or not isinstance(command, list)
        or len(command) < 2
        or any(not isinstance(item, str) for item in command)
        or command.count("--historical") != 1
        or command.count("--capture-id") != 1
        or command.count("--directory") != 1
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

    identity = launch.get("process_identity_utc")
    identity_match = (
        PROCESS_IDENTITY_UTC.fullmatch(identity.strip())
        if isinstance(identity, str) and len(identity) <= 8192
        else None
    )
    launched_at_value = launch.get("launched_at_utc")
    try:
        launched_at = datetime.fromisoformat(launched_at_value)
        if not isinstance(launched_at_value, str):
            raise ValueError("launch timestamp is not text")
        identity_started = datetime(
            int(identity_match.group(7)),
            {
                month: index
                for index, month in enumerate(
                    (
                        "Jan",
                        "Feb",
                        "Mar",
                        "Apr",
                        "May",
                        "Jun",
                        "Jul",
                        "Aug",
                        "Sep",
                        "Oct",
                        "Nov",
                        "Dec",
                    ),
                    start=1,
                )
            }[identity_match.group(4)],
            int(identity_match.group(5)),
            *map(int, identity_match.group(6).split(":")),
            tzinfo=UTC,
        )
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise RunnerError("latest-launch UTC process identity is malformed") from exc
    if (
        identity_match is None
        or identity_match.group(1) != str(launch_pid)
        or identity_match.group(2) != str(launch_pgid)
        or identity_match.group(3)
        != ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[identity_started.weekday()]
        or identity_match.group(8) != " ".join(command)
        or launched_at.tzinfo is None
        or launched_at.utcoffset() != timedelta(0)
        or int(identity_started.timestamp()) != int(launched_at.timestamp())
    ):
        raise RunnerError("latest-launch UTC process identity does not match its command")

    if "process_identity" in launch:
        legacy_identity = launch["process_identity"]
        if not isinstance(legacy_identity, str) or legacy_identity.split(maxsplit=2)[:2] != [
            str(launch_pid),
            str(launch_pgid),
        ]:
            raise RunnerError("legacy latest-launch process identity conflicts with UTC identity")
    if "started_at" in launch:
        try:
            legacy_started = datetime.fromisoformat(launch["started_at"])
        except (TypeError, ValueError) as exc:
            raise RunnerError(
                "legacy latest-launch start time conflicts with UTC identity"
            ) from exc
        if (
            legacy_started.tzinfo is None
            or legacy_started.utcoffset() != timedelta(0)
            or int(legacy_started.timestamp()) != int(launched_at.timestamp())
        ):
            raise RunnerError("legacy latest-launch start time conflicts with UTC identity")
    return launch, launch_path, launch_raw, raw_report, token


def _append_stopped_worker_audit(
    operator: Path, record: dict[str, Any], *, allow_distinct_attempts: bool = False
) -> dict[str, Any]:
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
                if allow_distinct_attempts and (
                    type(entry.get("supervisor_pid")) is not int
                    or type(entry.get("stale_process_group")) is not int
                ):
                    raise RunnerError("stopped-worker audit launch identity is malformed")
                same_attempt = (
                    entry.get("supervisor_pid") == record.get("supervisor_pid")
                    and entry.get("stale_process_group") == record.get("stale_process_group")
                ) or (
                    isinstance(record.get("invocation_token"), str)
                    and entry.get("invocation_token") == record["invocation_token"]
                )
                if not allow_distinct_attempts or same_attempt:
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


def _reconcile_private_historical_worker(
    directory: Path,
    operator: Path,
    report_path: Path,
    report: dict[str, Any],
    raw_report: bytes,
) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events.common import regular_bytes

    proof = _historical_private_uncertain_authority(
        directory,
        report_path,
        report,
        current_boot=_boot_stamp(),
        changed_boot=False,
    )
    _historical_private_processes_absent(proof)
    marker = proof["marker"]
    with _operator_writer_lock(operator):
        if (
            regular_bytes(report_path, 16 * MIB) != raw_report
            or _strict_json(report_path) != report
        ):
            raise RunnerError("private uncertain session changed during reconciliation")
        _historical_private_processes_absent(proof)
        storage_state = _historical_validate_storage_handoff(
            directory,
            report_path,
            Path(marker["storage_progress_path"]),
            invocation_token=marker["storage_invocation_token"],
            allow_stale=True,
        )
        if grants.sha256_json(storage_state) != proof["storage_sha256"]:
            raise RunnerError("private uncertain storage handoff changed during reconciliation")
        body = {
            "version": 1,
            "kind": "same_boot_worker_absence",
            "identity": proof["identity"],
            "grant_id": marker["grant_id"],
            "launch_id": marker["launch_id"],
            "launch_sha256": proof["launch_sha256"],
            "session_before_sha256": hashlib.sha256(raw_report).hexdigest(),
            "boot_stamp": report["boot_stamp"],
            "controller_pid": marker["controller_pid"],
            "controller_pgid": marker["controller_pgid"],
            "worker_pid": marker["worker_pid"],
            "worker_pgid": marker["worker_pgid"],
            "storage_invocation_token": marker["storage_invocation_token"],
            "storage_progress_path": marker["storage_progress_path"],
            "storage_progress_sha256": proof["storage_sha256"],
            "processes_proven_absent": True,
            "reconciliation_worker_started": False,
            "new_grant_started": False,
        }
        _record, record_sha256 = _historical_write_private_reconciliation(
            proof["control_root"], marker["grant_id"], body
        )
        current = _strict_json(report_path)
        if current != report:
            raise RunnerError("private uncertain session changed before reconciliation readback")
        current["phase"] = "stopped"
        current["active_process_group"] = None
        current["private_historical_grant"]["state"] = "reconciled"
        current["private_historical_grant"]["reconciliation_sha256"] = record_sha256
        current["private_historical_reconciliation"] = {
            "state": "complete",
            "kind": "same_boot_worker_absence",
            "grant_id": marker["grant_id"],
            "launch_id": marker["launch_id"],
            "launch_sha256": proof["launch_sha256"],
            "record_sha256": record_sha256,
            "boot_stamp": report["boot_stamp"],
            "reconciliation_worker_started": False,
            "new_grant_started": False,
        }
        _atomic_report(report_path, current)
    return current


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
        if "private_historical_grant" in report or report.get("phase") == "uncertain":
            raw_report = regular_bytes(report_path, 16 * MIB)
            if _strict_json(report_path) != report:
                raise RunnerError("private uncertain session changed before reconciliation")
            return _reconcile_private_historical_worker(
                directory, operator, report_path, report, raw_report
            )
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
        memory_stop = (
            report.get("error") == "resource accounting failed: memory snapshot unavailable"
        )
        if memory_stop:
            launch, launch_path, evidence_raw, raw_report, token = _stopped_memory_worker_evidence(
                directory, report_path, report, capture_id
            )
            supervisor_pid = launch["pid"]
            supervisor_pgid = launch["pgid"]
            worker_pid = worker_pgid = group

            def require_memory_processes_absent():
                for pid, pgid in ((supervisor_pid, supervisor_pgid), (worker_pid, worker_pgid)):
                    try:
                        if _pid_exists(pid) or _process_group_exists(pgid):
                            raise RunnerError(
                                "stopped worker or original supervisor is still alive or ambiguous"
                            )
                    except OSError as exc:
                        raise RunnerError(
                            "stopped worker or original supervisor identity could not be verified"
                        ) from exc

            require_memory_processes_absent()
            launch_hash = hashlib.sha256(evidence_raw).hexdigest()
            report_hash = hashlib.sha256(raw_report).hexdigest()
            evidence_path = launch_path
            evidence_hash = launch_hash
            session_before_hash = report_hash
            event_identity = {
                "capture_id": capture_id,
                "session_id": report.get("session_id"),
                "root_id": root_id,
                "stale_process_group": group,
                "worker_pid": worker_pid,
                "worker_pgid": worker_pgid,
                "supervisor_pid": supervisor_pid,
                "supervisor_pgid": supervisor_pgid,
                "invocation_token": token,
                "boot_stamp": report["boot_stamp"],
                "revision": report.get("revision"),
                "report_sha256": report_hash,
                "launch_path": str(launch_path),
                "launch_sha256": launch_hash,
                "primary_stop_reason": report.get("error"),
            }
        else:
            (
                evidence,
                evidence_path,
                evidence_raw,
                launch_path,
                launch_raw,
                launch_checks_path,
                launch_checks_raw,
            ) = _stopped_worker_evidence(directory, report_path, report, capture_id)
            supervisor_pid = evidence["supervisor_pid"]
            supervisor_pgid = evidence["supervisor_pgid"]
            process_identity = evidence.get("process_identity_utc")
            typed_temporary_stop = evidence.get("category") == "temporary"
            if (
                type(supervisor_pid) is not int
                or supervisor_pid <= 1
                or type(supervisor_pgid) is not int
                or supervisor_pgid <= 1
            ):
                raise RunnerError("stopped-worker launch identity is invalid")
            if typed_temporary_stop and report.get("storage_safety_stop") is None:
                report["storage_safety_stop"] = {
                    key: evidence[key]
                    for key in (
                        "version",
                        "mode",
                        "category",
                        "invocation_id",
                        "capture_id",
                        "root_id",
                        "revision",
                        "revision_source",
                        "observed_bytes",
                        "threshold_bytes",
                        "threshold_inclusive",
                        "original_process_group",
                    )
                }
                _atomic_report(report_path, report)

            def require_storage_processes_absent():
                if report.get("boot_stamp") != _boot_stamp():
                    raise RunnerError("saved original boot identity changed during reconciliation")
                if typed_temporary_stop and isinstance(process_identity, str):
                    _require_stopped_storage_processes_absent(
                        group,
                        supervisor_pid,
                        supervisor_pgid,
                        process_identity,
                        allow_pid_reuse=True,
                    )
                elif (
                    _pid_exists(group)
                    or _process_group_exists(group)
                    or _pid_exists(supervisor_pid)
                    or _process_group_exists(supervisor_pgid)
                ):
                    raise RunnerError(
                        "stopped worker or original supervisor is still alive or ambiguous"
                    )

            require_storage_processes_absent()
            raw_report = regular_bytes(report_path, 16 * MIB)
            report_hash = hashlib.sha256(raw_report).hexdigest()
            evidence_hash = hashlib.sha256(evidence_raw).hexdigest()
            session_before_hash = evidence.get("session_before_sha256", report_hash)
            event_identity = {
                "capture_id": capture_id,
                "session_id": report.get("session_id"),
                "root_id": root_id,
                "stale_process_group": group,
                "supervisor_pid": supervisor_pid,
                "supervisor_pgid": supervisor_pgid,
                "boot_stamp": report["boot_stamp"],
                "revision": report.get("revision"),
                "launch_revision_source": evidence["launch_revision_source"],
                "processing_revision": evidence.get("processing_revision"),
                "reconciliation_runtime_revision": evidence.get("reconciliation_runtime_revision"),
                "report_sha256": report_hash,
                "evidence_path": str(evidence_path),
                "evidence_sha256": evidence_hash,
                "launch_path": str(launch_path),
                "launch_sha256": hashlib.sha256(launch_raw).hexdigest(),
                "launch_checks_path": (
                    str(launch_checks_path) if launch_checks_path is not None else None
                ),
                "launch_checks_sha256": (
                    hashlib.sha256(launch_checks_raw).hexdigest()
                    if launch_checks_raw is not None
                    else None
                ),
                "primary_stop_reason": report.get("error"),
            }
            if typed_temporary_stop:
                event_identity["reconciliation_kind"] = "temporary_storage_stop"
                event_identity["storage_stop_evidence"] = {
                    key: evidence[key]
                    for key in (
                        "mode",
                        "category",
                        "revision_source",
                        "threshold_bytes",
                        "threshold_inclusive",
                        "observed_bytes",
                        "original_process_group",
                    )
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
            if memory_stop:
                current_launch, _current_launch_path, current_launch_raw, current_report_raw, _ = (
                    _stopped_memory_worker_evidence(directory, report_path, report, capture_id)
                )
                if current_launch_raw != evidence_raw or current_report_raw != raw_report:
                    raise RunnerError(
                        "stopped-worker launch or report changed during reconciliation"
                    )
                if (current_launch["pid"], current_launch["pgid"]) != (
                    supervisor_pid,
                    supervisor_pgid,
                ):
                    raise RunnerError(
                        "stopped-worker launch identity changed during reconciliation"
                    )
                require_memory_processes_absent()
            else:
                (
                    current_evidence,
                    current_evidence_path,
                    current_evidence_raw,
                    current_launch_path,
                    current_launch_raw,
                    current_checks_path,
                    current_checks_raw,
                ) = _stopped_worker_evidence(directory, report_path, report, capture_id)
                current_report_raw = regular_bytes(report_path, 16 * MIB)
                if (
                    current_evidence_path != evidence_path
                    or current_evidence_raw != evidence_raw
                    or current_launch_path != launch_path
                    or current_launch_raw != launch_raw
                    or current_checks_path != launch_checks_path
                    or current_checks_raw != launch_checks_raw
                    or current_report_raw != raw_report
                    or current_evidence.get("supervisor_pid") != supervisor_pid
                    or current_evidence.get("supervisor_pgid") != supervisor_pgid
                    or current_evidence.get("reconciliation_runtime_revision")
                    != evidence.get("reconciliation_runtime_revision")
                ):
                    raise RunnerError("stopped-worker inputs changed during reconciliation")
                require_storage_processes_absent()
            audit = _append_stopped_worker_audit(
                operator, audit, allow_distinct_attempts=memory_stop or typed_temporary_stop
            )
            report["active_process_group"] = None
            report["stopped_worker_reconciliation"] = {
                "event_id": event_id,
                "audit_file": "stopped-worker-reconciliation.jsonl",
                "reconciled_at": audit["reconciled_at"],
                "boot_stamp": report["boot_stamp"],
                "stale_process_group": group,
                "supervisor_pid": supervisor_pid,
                "supervisor_pgid": supervisor_pgid,
                "primary_stop_reason": report.get("error"),
                "termination_evidence_sha256": evidence_hash,
                "session_before_sha256": session_before_hash,
            }
            if not memory_stop:
                report["stopped_worker_reconciliation"].update(
                    {
                        "launch_revision_source": evidence.get("launch_revision_source"),
                        "processing_revision": evidence.get("processing_revision"),
                        "reconciliation_runtime_revision": evidence.get(
                            "reconciliation_runtime_revision"
                        ),
                        "launch_sha256": event_identity["launch_sha256"],
                    }
                )
                if launch_checks_path is not None:
                    report["stopped_worker_reconciliation"]["launch_checks_sha256"] = (
                        event_identity["launch_checks_sha256"]
                    )
                if typed_temporary_stop:
                    report["stopped_worker_reconciliation"].update(
                        {
                            "reconciliation_kind": "temporary_storage_stop",
                            "storage_stop_evidence": event_identity["storage_stop_evidence"],
                        }
                    )
            if memory_stop:
                report["stopped_worker_reconciliation"].update(
                    {
                        "reconciliation_kind": "memory_snapshot_unavailable",
                        "invocation_token": token,
                        "launch_sha256": launch_hash,
                        "report_sha256": report_hash,
                    }
                )
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
        if workflow and "historical_publication_hold" in report:
            reserve = max(reserve, 2 * 3600)
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
        if workflow_deadline and "historical_publication_hold" in report:
            reserve = max(reserve, 2 * 3600)
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


def _offline_window_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _offline_window_boot_boundary(
    report: dict[str, Any], windows: list[dict[str, Any]]
) -> tuple[int, bool] | None:
    transition = report.get("offline_historical_window_boot_transition")
    if transition is None:
        return None
    if not isinstance(transition, dict) or set(transition) != {
        "index",
        "reboot_sha256",
        "successor_sha256",
    }:
        raise RunnerError("invalid offline historical boot transition")
    index = transition["index"]
    history = report.get("reboot_recovery_history")
    if type(index) is not int or not 0 <= index < len(windows) or not isinstance(history, list):
        raise RunnerError("invalid offline historical boot transition index")
    matches = [
        event
        for event in history
        if isinstance(event, dict) and _offline_window_digest(event) == transition["reboot_sha256"]
    ]
    if len(matches) != 1:
        raise RunnerError("offline historical boot transition lacks its reconciliation")
    event = matches[0]
    pending = transition["successor_sha256"] is None
    reconciled_epoch = event.get("offline_window_reconciled_epoch")
    reconciliation = report.get("historical_reboot_reconciliation")
    if (
        type(reconciled_epoch) not in (int, float)
        or not math.isfinite(reconciled_epoch)
        or reconciled_epoch < windows[index]["started_epoch"]
        or event.get("offline_window_before_sha256") != _offline_window_digest(windows[index])
        or event.get("offline_window_capture_id") != report.get("capture_id")
        or event.get("offline_window_root_id") != report.get("root_id")
        or event.get("offline_window_history_index") != index
        or not isinstance(event.get("previous_boot_stamp"), str)
        or not isinstance(event.get("boot_stamp"), str)
        or event["previous_boot_stamp"] == event["boot_stamp"]
        or event.get("processing_worker_started") is not False
        or event.get("grant_started") is not False
        or (
            pending
            and (
                index != len(windows) - 1
                or not isinstance(reconciliation, dict)
                or reconciliation.get("state") != "complete"
                or reconciliation.get("boot_stamp") != event["boot_stamp"]
                or event["boot_stamp"] != report.get("boot_stamp")
                or reconciliation.get("capture_id") != report.get("capture_id")
                or reconciliation.get("root_id") != report.get("root_id")
                or reconciliation.get("processing_worker_started") is not False
                or reconciliation.get("grant_started") is not False
            )
        )
        or (
            not pending
            and (
                index + 1 >= len(windows)
                or transition["successor_sha256"] != _offline_window_digest(windows[index + 1])
                or windows[index + 1]["started_epoch"] < reconciled_epoch
            )
        )
    ):
        raise RunnerError("offline historical boot transition changed its window or reconciliation")
    return index, pending


def _offline_window_pending_boot(report: dict[str, Any]) -> bool:
    transition = report.get("offline_historical_window_boot_transition")
    return isinstance(transition, dict) and transition.get("successor_sha256") is None


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
    boot_boundary = _offline_window_boot_boundary(report, windows)
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
        same_boot = boot_boundary != (index, False)
        if successor[0] < previous[0] or (same_boot and successor[2] < previous[2]):
            raise RunnerError("offline historical window history is out of order")
        overlaps = successor[0] < previous[1] or (same_boot and successor[2] < previous[3])
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
    if not allow_expired and (remaining <= 0 or _offline_window_pending_boot(report)):
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
        expired = time.time() >= float(existing["deadline_epoch"]) and (
            _offline_window_pending_boot(report)
            or time.monotonic() >= float(existing["deadline_monotonic"])
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
    if renew and _offline_window_pending_boot(report):
        report["offline_historical_window_boot_transition"]["successor_sha256"] = (
            _offline_window_digest(report["offline_historical_window"])
        )
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
    wall_deadline_epoch_override: float | None = None,
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
        wall_deadline_epoch_override=wall_deadline_epoch_override,
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


def _historical_hold_release(
    operator: Path, report: dict[str, Any], release_id: str, verification: dict[str, Any]
) -> str:
    """Resolve the exact current release and check its manifest binding."""
    from kalshi_events.common import regular_bytes, safe_path

    if not isinstance(release_id, str) or not re.fullmatch(r"[0-9a-f]{64}", release_id):
        raise RunnerError("held historical release ID is invalid")
    pointer = _strict_json(safe_path(operator / "current-historical.json"))
    if not pointer or pointer.get("source_release_id") != release_id:
        raise RunnerError("current historical pointer differs from the held release")
    manifest_path = safe_path(operator / "releases" / release_id / "manifest.json")
    raw = regular_bytes(manifest_path, MIB)
    manifest = json.loads(raw)
    manifest_sha = hashlib.sha256(raw).hexdigest()
    if (
        not isinstance(manifest, dict)
        or manifest.get("source_release_id") != release_id
        or manifest.get("capture_id") != report.get("capture_id")
        or manifest.get("root_id") != report.get("root_id")
        or manifest.get("contract") != "oddsfox.kalshi.historical.v1"
        or pointer.get("manifest_sha256") != manifest_sha
        or manifest != verification
    ):
        raise RunnerError("historical release manifest differs from the held capture")
    return manifest_sha


def _held_release_id(operator: Path, report: dict[str, Any]) -> str:
    hold = report.get("historical_publication_hold")
    if (
        not isinstance(hold, dict)
        or hold.get("state") != "verified"
        or report.get("publication_state") != "verified"
        or any(hold.get(key) != report.get(key) for key in ("session_id", "capture_id", "root_id"))
    ):
        raise RunnerError("activation requires this session's verified historical publication hold")
    release_id = hold.get("release_id")
    verification = hold.get("standalone_verification")
    if (
        not isinstance(verification, dict)
        or verification.get("source_release_id") != release_id
        or verification.get("coverage_scope") != "historical"
    ):
        raise RunnerError("held historical release lacks standalone verification evidence")
    if _historical_hold_release(operator, report, release_id, verification) != hold.get(
        "manifest_sha256"
    ):
        raise RunnerError("historical manifest changed after the publication hold")
    return release_id


def _historical_hold_intent(directory: Path, report: dict[str, Any]) -> None:
    import historical_grants as grants

    from kalshi_events.common import fsync_directory, safe_path

    registry = safe_path(directory / "runs" / ".historical-holds")
    try:
        registry.mkdir(mode=0o700)
    except FileExistsError:
        pass
    else:
        fsync_directory(registry.parent)
    grants._private_directory(registry, "historical hold directory")
    capture_id = report["capture_id"]
    intent = {key: report[key] for key in ("session_id", "capture_id", "root_id")}
    marker = registry / f"{capture_id}.json"
    if marker.exists() or marker.is_symlink():
        if grants.read_json(marker) != intent:
            raise RunnerError("historical hold is already bound to a different session")
    else:
        grants.write_once_json(marker, intent)


def _guard_historical_activation_hold(
    directory: Path,
    capture_id: str,
    report_path: Path | None = None,
) -> None:
    import historical_grants as grants

    from kalshi_events.common import safe_path

    registry = safe_path(directory / "runs" / ".historical-holds")
    marker = registry / f"{capture_id}.json"
    if marker.exists() or marker.is_symlink():
        grants._private_directory(registry, "historical hold directory")
        intent = grants.read_json(marker)
        session_id = intent.get("session_id")
        if (
            not isinstance(session_id, str)
            or not CAPTURE_ID.fullmatch(session_id)
            or report_path != directory / "runs" / session_id / REPORT_NAME
        ):
            raise RunnerError("historical release is held for explicit audited activation")
        report = _strict_json(safe_path(report_path))
        if (
            report is None
            or any(
                intent.get(key) != report.get(key)
                for key in ("capture_id", "root_id", "session_id")
            )
            or report.get("phase") != "activation"
            or not isinstance(report.get("historical_publication_hold"), dict)
            or not isinstance(report.get("activation_audit"), dict)
        ):
            raise RunnerError("historical hold lost its audited activation session")
        return

    for path in safe_path(directory / "runs").glob(f"*/{REPORT_NAME}"):
        report = _strict_json(safe_path(path))
        if report is None:
            raise RunnerError("historical activation session report is unavailable")
        if (
            report.get("capture_id") == capture_id
            and "historical_publication_hold" in report
            and (
                path != report_path
                or report.get("phase") != "activation"
                or not isinstance(report.get("activation_audit"), dict)
            )
        ):
            raise RunnerError("historical release is held for explicit audited activation")


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
    resume_historical_stage: str | None = None,
    publish_only: bool = False,
    activate_held_historical: str | None = None,
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
    if publish_only or activate_held_historical is not None:
        if (
            mode != "historical"
            or reconcile_stopped_worker
            or reconcile_accounting
            or recover_reboot
        ):
            raise RunnerError("historical hold options require an offline historical capture run")
        if activate_held_historical is not None and (
            publish_only
            or resume_historical_stage is not None
            or not isinstance(activate_held_historical, str)
            or not re.fullmatch(r"[0-9a-f]{64}", activate_held_historical)
        ):
            raise RunnerError("held activation requires only the exact capture and audit digest")
    if resume_historical_stage is not None:
        _validate_historical_stage(resume_historical_stage)
        if mode != "historical":
            raise RunnerError("--resume-historical-stage requires --historical --capture-id")
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
    historical_reboot_reconciliation = (
        mode == "historical" and reconcile_accounting and recover_reboot
    )
    if reconcile_accounting and mode != "resume" and not historical_reboot_reconciliation:
        raise RunnerError("accounting reconciliation requires --capture-id resume")
    if extend_session and (mode != "resume" or not reconcile_accounting):
        raise RunnerError("session extension requires --capture-id and --reconcile-accounting")
    if recover_reboot and (
        mode not in {"resume", "historical"}
        or not reconcile_accounting
        or extend_session
        or recovery_volume_uuid is None
        or mode == "historical"
        and (
            offline_duration_seconds is not None
            or renew_offline_window
            or supersede_offline_window
            or reconcile_stopped_worker
            or resource_profile is not None
        )
    ):
        raise RunnerError(
            "reboot recovery requires accounting reconciliation and a verified volume UUID; "
            "offline historical recovery accepts no other grant or resource options"
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
            if historical_reboot_reconciliation:
                profile = report.get("resource_profile", "default")
                token = _ACTIVE_LIMITS.set(_limits_from_report(report, profile))
                try:
                    return _finish_historical_reboot_reconciliation(
                        directory,
                        report_path,
                        report,
                        str(capture_id),
                    ), 0
                finally:
                    _ACTIVE_LIMITS.reset(token)
            hold = report.get("historical_publication_hold")
            if activate_held_historical is not None:
                if report.get("phase") != "stopped":
                    raise RunnerError("held activation requires a stopped historical session")
                held_release = _held_release_id(operator, report)
                previous_activation_audit = report.get("activation_audit")
                report["activation_audit"] = {
                    "relative_path": f"activation-audit-{activate_held_historical}.json",
                    "sha256": activate_held_historical,
                }
            elif "historical_publication_hold" in report and (
                not publish_only or not isinstance(hold, dict) or hold.get("state") != "pending"
            ):
                raise RunnerError(
                    "historical publication hold requires explicit audited activation"
                )
            elif publish_only and hold is None:
                if (
                    offline_duration_seconds is None
                    and report.get("offline_historical_window") is None
                ):
                    raise RunnerError("publication hold requires a bounded offline window")
                report["historical_publication_hold"] = {
                    "state": "pending",
                    **{key: report[key] for key in ("session_id", "capture_id", "root_id")},
                }
            if publish_only:
                _historical_hold_intent(directory, report)
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
        if mode == "historical":
            _record_historical_invocation(
                report,
                resume_historical_stage,
                revision=report["revision"],
            )
        else:
            report["resume_historical_stage"] = None
            report.pop("active_invocation_id", None)
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
        if activate_held_historical is not None:
            from production_activation import _activation_audit_payload

            try:
                _activation_audit_payload(
                    directory, report["activation_audit"], expected_session_id=report["session_id"]
                )
            except Exception:
                if previous_activation_audit is None:
                    report.pop("activation_audit")
                else:
                    report["activation_audit"] = previous_activation_audit
                _atomic_report(report_path, report)
                raise
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
            if publish_only:
                reserve = max(reserve, 2 * 3600)
            report["offline_workflow_reserve_seconds"] = reserve
            report["offline_workflow_deadline_epoch"] = (
                float(offline_window["deadline_epoch"]) - reserve
            )
            report["offline_workflow_deadline_monotonic"] = (
                float(offline_window["deadline_monotonic"]) - reserve
            )
            _atomic_report(report_path, report)
        if (publish_only or activate_held_historical is not None) and offline_window is None:
            raise RunnerError("historical hold options require a bounded offline window")
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
            if (
                activate_held_historical is None
                and mode not in {"activate-current", "activate-historical"}
                and _monitor_deadline(report, workflow=True)
                <= time.monotonic() + float(_effective_limits()["terminate_seconds"])
            ):
                raise BudgetStop("workflow deadline reached before supervised work could start")
            session_deadline = _monitor_deadline(report, workflow=False)
            standalone_result: dict[str, Any] | None = None
            if activate_held_historical is not None:
                activation_accounting = _resource_guard(
                    directory,
                    operator,
                    report=report,
                    deadline=session_deadline,
                    paths=paths,
                    require_complete_accounting=False,
                    capture_id=capture_id,
                )
                if activation_accounting.get("root_id") != report.get("root_id"):
                    raise RunnerError("held activation accounting belongs to another root")
                _check_accounting_match(activation_accounting, report)
                _record_operator_accounting(report, activation_accounting)
                standalone_result = {
                    "source_release_id": held_release,
                    "coverage_scope": "historical",
                }
            elif mode not in {"activate-current", "activate-historical"}:
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
                if publish_only:
                    manifest_sha = _historical_hold_release(
                        operator, report, release_id, standalone_result
                    )
                    report["historical_publication_hold"].update(
                        state="verified",
                        release_id=release_id,
                        manifest_sha256=manifest_sha,
                        standalone_verification=standalone_result,
                    )
                    report["phase"] = "stopped"
                    report["error"] = None
                    report["recovery"] = (
                        "Complete the independent audit, then activate this held release."
                    )
                    _atomic_report(report_path, report)
                    return result, 0
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
                    _guard_historical_activation_hold(directory, current_capture)
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
                    "Reverify the held release and use --activate-held-historical with its audit."
                    if activate_held_historical is not None
                    else "Run --activate-current --historical to reverify the historical publication and hard-link state."
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
    grant_deadlines: tuple[float, float] | None = None,
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
        remaining = None
        if session_report is not None:
            if grant_deadlines is not None:
                remaining = min(
                    float(grant_deadlines[0]) - time.time(),
                    float(grant_deadlines[1]) - time.monotonic(),
                )
            elif "offline_historical_window" in session_report:
                window = _validate_offline_historical_window(
                    session_report, str(session_report.get("capture_id"))
                )
                remaining = min(
                    float(window["deadline_epoch"]) - time.time(),
                    float(window["deadline_monotonic"]) - time.monotonic(),
                    float(window["started_monotonic"])
                    + float(limits["active_cutoff_seconds"])
                    - time.monotonic(),
                )
            else:
                remaining = _monitor_deadline(session_report, workflow=False) - time.monotonic()
            remaining -= float(limits["terminate_seconds"])
            if remaining <= 0:
                raise BudgetStop("no activation time remains in the approved session")
        expected_observations = None
        activation_audit = None
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
            _guard_historical_activation_hold(directory, current_capture, report_path)
            if (
                session_report is not None
                and "historical_publication_hold" in session_report
                and _held_release_id(operator, session_report) != release_id
            ):
                raise RunnerError("activation release differs from the audited publication hold")
            if session_report is not None and "activation_audit" in session_report:
                if report_path is None:
                    raise RunnerError("activation audit requires its session report path")
                session_id = session_report.get("session_id")
                if (
                    not isinstance(session_id, str)
                    or not CAPTURE_ID.fullmatch(session_id)
                    or report_path != directory / "runs" / session_id / REPORT_NAME
                ):
                    raise RunnerError("activation audit session path is invalid")
                from production_activation import _activation_audit_payload

                activation_audit = _activation_audit_payload(
                    directory,
                    session_report["activation_audit"],
                    expected_session_id=session_id,
                )
                report_root_id = session_report.get("root_id")
                if (
                    session_report.get("capture_id") != current_capture
                    or not isinstance(report_root_id, str)
                    or not CAPTURE_ID.fullmatch(report_root_id)
                    or activation_audit.get("capture_id") != current_capture
                    or activation_audit.get("root_id") != report_root_id
                    or activation_audit.get("root_id") != manifest.get("root_id")
                ):
                    raise RunnerError(
                        "activation audit does not match the current session and release"
                    )
            if profile == "expanded" and (
                session_report is None
                or session_report.get("mode") not in {"full", "resume", "historical"}
                or session_report.get("capture_id") != current_capture
            ):
                raise RunnerError(
                    "expanded activation release does not match its stopped session capture"
                )
            build = _strict_json(operator / "builds-historical" / current_capture / "build.json")
            if build is not None and "observation_count" in build:
                expected_observations = build["observation_count"]
                if type(expected_observations) is not int or expected_observations < 0:
                    raise RunnerError("historical build observation count is invalid")
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
                if (profile == "expanded" or activation_audit is not None)
                and session_report is not None
                else None
            ),
            catalogue_version=catalogue_version,
            max_database_bytes=int(limits["database_bytes"]),
            max_temp_bytes=int(limits["temporary_bytes"]),
            max_duration_seconds=remaining,
            expected_observations=expected_observations,
            activation_audit=activation_audit,
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
        "--resume-historical-stage",
        help="resume the exact saved build-historical stage for this capture",
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
    parser.add_argument(
        "--publish-only", action="store_true", help="hold verified historical publication for audit"
    )
    parser.add_argument(
        "--activate-held-historical",
        metavar="AUDIT_SHA256",
        help="activate only the exact held historical release after independent audit",
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
    if args.resume_historical_stage is not None:
        try:
            _validate_historical_stage(args.resume_historical_stage)
        except RunnerError as exc:
            parser.error(str(exc))
        if not args.historical or not args.capture_id:
            parser.error("--resume-historical-stage requires --historical --capture-id")
    if args.reconcile_accounting and (
        not args.capture_id or args.historical and not args.recover_reboot
    ):
        parser.error(
            "--reconcile-accounting requires network resume or explicit offline historical reboot recovery"
        )
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
                resume_historical_stage=args.resume_historical_stage,
                publish_only=args.publish_only,
                activate_held_historical=args.activate_held_historical,
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
    historical_reboot_reconciliation = historical and reconcile_accounting and recover_reboot
    if (
        historical
        and (reconcile_accounting or extend_session or recover_reboot)
        and not (historical_reboot_reconciliation)
    ):
        raise RunnerError("offline historical completion cannot reconcile or extend acquisition")
    if historical_reboot_reconciliation and (
        duration is not None or renew_requested or extend_session
    ):
        raise RunnerError("offline historical reboot reconciliation accepts no grant options")
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
    now = time.time()
    if now < float(report.get("last_observed_epoch", report["started_epoch"])):
        raise RunnerError("wall clock moved backward since the last durable session report")
    current_boot = _boot_stamp()
    rebooted = report.get("boot_stamp") != current_boot
    private_reboot_uncertain = (
        historical_reboot_reconciliation
        and rebooted
        and isinstance(report.get("private_historical_grant"), dict)
        and type(report.get("active_process_group")) is int
        and (
            report.get("phase") == "uncertain"
            and report["private_historical_grant"].get("state") == "stopped"
            or report["private_historical_grant"].get("state") == "supervised-child-pending"
            and report.get("phase")
            == "historical-{}-{}".format(
                report["private_historical_grant"].get("phase"),
                report["private_historical_grant"].get("substep"),
            )
        )
    )
    if rebooted and not recover_reboot:
        raise RunnerError("Mac restarted; explicit reconciled reboot recovery is required")
    if historical_reboot_reconciliation and not rebooted:
        raise RunnerError("--recover-reboot requires a verified boot change")
    if rebooted and report.get("phase") != "stopped" and not private_reboot_uncertain:
        raise RunnerError("reboot recovery requires a durably stopped session")
    active_group = report.get("active_process_group")
    if active_group is not None:
        if not (historical_reboot_reconciliation and rebooted):
            raise RunnerError(
                "session records an unreconciled child process group that is live or ambiguous; "
                "same-boot reconciliation is required"
            )
        if type(active_group) is not int or active_group <= 1:
            raise RunnerError("prior-boot process group identity is malformed")
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
    private_worker_reconciliation = None
    if private_reboot_uncertain:
        private_worker_reconciliation = _historical_private_uncertain_authority(
            directory,
            report_path,
            report,
            current_boot=current_boot,
            changed_boot=True,
        )
    if not rebooted and saved_device != current_device:
        raise RunnerError("production filesystem changed since the original session")
    existing_offline_window = "offline_historical_window" in report
    if historical and existing_offline_window:
        _validate_offline_historical_window(
            report,
            capture_id,
            allow_expired=renew_requested or historical_reboot_reconciliation,
            allow_revision_change=renew_requested or historical_reboot_reconciliation,
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
        expired = time.time() >= float(window["deadline_epoch"]) and (
            _offline_window_pending_boot(report)
            or time.monotonic() >= float(window["deadline_monotonic"])
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
        if historical_reboot_reconciliation:
            report["_pending_historical_reboot_reconciliation"] = {
                "previous_boot_stamp": report.get("boot_stamp"),
                "boot_stamp": current_boot,
                "previous_filesystem_device": saved_device,
                "filesystem_device": current_device,
                "volume_uuid": current_volume_uuid,
                "prior_volume_uuid_available": saved_volume_uuid is not None,
                "previous_active_process_group": active_group,
                "private_worker_reconciliation": (
                    {
                        "control_root": str(private_worker_reconciliation["control_root"]),
                        "identity": private_worker_reconciliation["identity"],
                        "freeze": private_worker_reconciliation["freeze"],
                        "session_root_device": private_worker_reconciliation["session_binding"][
                            "root_device"
                        ],
                        "grant_id": private_worker_reconciliation["marker"]["grant_id"],
                        "launch_id": private_worker_reconciliation["marker"]["launch_id"],
                        "launch_sha256": private_worker_reconciliation["launch_sha256"],
                        "controller_pid": private_worker_reconciliation["marker"]["controller_pid"],
                        "controller_pgid": private_worker_reconciliation["marker"][
                            "controller_pgid"
                        ],
                        "worker_pid": private_worker_reconciliation["marker"]["worker_pid"],
                        "worker_pgid": private_worker_reconciliation["marker"]["worker_pgid"],
                        "storage_sha256": private_worker_reconciliation["storage_sha256"],
                        "report_sha256": private_worker_reconciliation["report_sha256"],
                    }
                    if private_worker_reconciliation is not None
                    else None
                ),
                "previous_monotonic": old_deadlines,
                "wall_deadlines_preserved": {
                    "started_epoch": report.get("started_epoch"),
                    "time_budget_started_epoch": _time_budget_started(report, "epoch"),
                    "deadline_epoch": _time_budget_deadline(report, "epoch"),
                },
            }
        else:
            # Anchor only the expired session clocks after the explicit reboot proof.
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
    if historical_reboot_reconciliation:
        return report_path, report, paths
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
        if isinstance(report.get("storage_safety_stop"), dict):
            previous_stop["storage_safety_stop"] = report["storage_safety_stop"]
        if isinstance(report.get("memory_sample_error"), str):
            previous_stop["memory_sample_error"] = report["memory_sample_error"]
        history = report.get("stop_history", [])
        if not isinstance(history, list) or any(not isinstance(item, dict) for item in history):
            raise RunnerError("session stop history is invalid")
        history.append(previous_stop)
        report["stop_history"] = history[-64:]
        report["previous_stop"] = previous_stop
    report.pop("memory_sample_error", None)
    report.pop("storage_safety_stop", None)
    report["error"] = None
    report["recovery"] = None
    # Keep an offline grant in the durable stopped state until the first processing child
    # actually starts. A crash between grant persistence and Popen must remain renewable.
    report["phase"] = "stopped" if historical else "preflight"
    for peak_field, _sample_field in RESOURCE_SAMPLE_INVOCATION_PEAK_FIELDS:
        report[peak_field] = 0
    report["effective_memory_policy"] = _effective_memory_policy()
    return report_path, report, paths


def _finish_historical_reboot_reconciliation(
    directory: Path,
    report_path: Path,
    report: dict[str, Any],
    capture_id: str,
    *,
    locked_store: Any | None = None,
) -> dict[str, Any]:
    """Read exact stopped-session accounting before recording the confirmed new boot."""

    from kalshi_events.common import regular_bytes

    pending_proof = report.get("_pending_historical_reboot_reconciliation")
    if (
        locked_store is None
        and isinstance(pending_proof, dict)
        and isinstance(pending_proof.get("private_worker_reconciliation"), dict)
        and report.get("accounting_basis") == "reserved-upper-bound"
    ):
        store = _accounting_store(directory / "operator")
        with store.writer_guard():
            return _finish_historical_reboot_reconciliation(
                directory, report_path, report, capture_id, locked_store=store
            )

    pending = report.pop("_pending_historical_reboot_reconciliation", None)
    if not isinstance(pending, dict):
        raise RunnerError("offline historical reboot reconciliation has no changed-boot proof")
    raw = regular_bytes(report_path, 16 * MIB)
    saved = _strict_json(report_path)
    if saved is None or saved != report:
        raise RunnerError("session report changed before reboot accounting reconciliation")
    if (
        report.get("offline_historical_window") is not None
        and report.get("offline_historical_window_boot_transition") is not None
        and not _offline_window_pending_boot(report)
    ):
        raise RunnerError("a second offline historical boot transition requires separate recovery")
    private_worker = pending.get("private_worker_reconciliation")
    private_uncertain = isinstance(private_worker, dict)
    private_phase = report.get("private_historical_grant", {})
    private_pending_phase = (
        private_uncertain
        and private_phase.get("state") == "supervised-child-pending"
        and report.get("phase")
        == f"historical-{private_phase.get('phase')}-{private_phase.get('substep')}"
    )
    if (
        (
            report.get("phase") != "stopped"
            and not (
                private_uncertain and (report.get("phase") == "uncertain" or private_pending_phase)
            )
        )
        or report.get("production_state") == "verified"
        or report.get("capture_id") != capture_id
        or report.get("boot_stamp") == pending.get("boot_stamp")
        or pending.get("boot_stamp") != _boot_stamp()
        or not isinstance(report.get("system"), dict)
        or pending.get("previous_boot_stamp") != report.get("boot_stamp")
        or pending.get("previous_filesystem_device") != report["system"].get("filesystem_device")
        or report.get("active_process_group") != pending.get("previous_active_process_group")
        or private_uncertain
        and (
            report.get("private_historical_grant", {}).get("grant_id")
            != private_worker.get("grant_id")
            or report.get("private_historical_grant", {}).get("launch_sha256")
            != private_worker.get("launch_sha256")
            or report.get("private_historical_grant", {}).get("worker_pgid")
            != pending.get("previous_active_process_group")
        )
    ):
        raise RunnerError("offline historical reboot session identity changed")

    if private_uncertain:
        _historical_current_mount(
            directory,
            directory / "operator",
            Path(private_worker["control_root"]),
            report,
            private_worker["identity"],
            private_worker["freeze"],
            current_boot=pending["boot_stamp"],
            session_root_device=private_worker["session_root_device"],
            allow_unreconciled_boot_change=True,
            expected_current_device=pending["filesystem_device"],
            expected_current_volume_uuid=pending["volume_uuid"],
        )

    history = report.get("reboot_recovery_history", [])
    if (
        not isinstance(history, list)
        or len(history) >= 64
        or any(not isinstance(item, dict) for item in history)
    ):
        raise RunnerError("reboot recovery history is invalid or full")
    previous_system = report.get("system")
    if not isinstance(previous_system, dict):
        raise RunnerError("reboot recovery has no saved filesystem identity")
    pre_recovery_history_sha256 = hashlib.sha256(
        json.dumps(history, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()

    operator = directory / "operator"
    raw_sha256 = hashlib.sha256(raw).hexdigest()
    audit_binding: dict[str, Any] | None = None

    def read_accounting(locked_store: Any | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        current_snapshot = (
            _read_accounting_worker(operator, capture_id)
            if locked_store is None
            else _read_accounting_worker(operator, capture_id, locked_store=locked_store)
        )
        if current_snapshot.get("root_id") != report.get("root_id"):
            raise RunnerError("reboot recovery accounting belongs to another operator root")
        if current_snapshot.get("accounting_complete") is not True and (
            not private_uncertain
            and (
                not isinstance(report.get("storage_safety_stop"), dict)
                or report.get("phase") != "stopped"
            )
        ):
            raise RunnerError("reboot recovery requires complete exact receipt accounting")
        current_accounting = _reconciliation_accounting(current_snapshot, capture_id)
        if current_accounting.get("accounting_complete") is True:
            current_accounting.setdefault("downloaded_bytes_upper_bound", None)
            current_accounting.setdefault("accounting_basis", "exact")
            current_accounting.setdefault("accounting_bound_verified", False)
            current_accounting.setdefault("unfinished_reservation_ids", [])
        previous_attempts = report.get("requests")
        if previous_attempts is not None and (
            isinstance(previous_attempts, bool)
            or not isinstance(previous_attempts, int)
            or current_accounting.get("attempts", -1) < previous_attempts
        ):
            raise RunnerError("reboot receipt accounting regressed its saved request count")
        previous_downloaded = report.get("downloaded_bytes")
        current_downloaded = current_accounting.get("downloaded_bytes")
        if (
            report.get("accounting_complete") is True
            and current_accounting.get("accounting_complete") is True
            and previous_downloaded is not None
            and (
                isinstance(previous_downloaded, bool)
                or not isinstance(previous_downloaded, int)
                or not isinstance(current_downloaded, int)
                or current_downloaded < previous_downloaded
            )
        ):
            raise RunnerError("reboot exact receipt accounting regressed its saved byte count")
        _check_accounting_match(current_snapshot, current_accounting)
        return current_snapshot, current_accounting

    # Keep the scan and audit under the same writer lock. Exclude storage totals
    # from the audit digest so a retry after the append keeps the same event ID.
    if (
        not private_uncertain
        and isinstance(report.get("storage_safety_stop"), dict)
        and report.get("phase") == "stopped"
        and report.get("error") is not None
        and type(report.get("active_process_group")) is int
        and report["active_process_group"] > 1
    ):
        store = _accounting_store(operator)
        with store.writer_guard():
            if not store.writer_locked_by_current_thread:
                raise RunnerError("reboot recovery failed to acquire the canonical writer guard")
            snapshot, accounting = read_accounting(locked_store=store)
            accounting_sha256 = _accounting_snapshot_sha256(snapshot)
            if regular_bytes(report_path, 16 * MIB) != raw or _strict_json(report_path) != report:
                raise RunnerError("session report changed before reboot audit append")
            stop = _stopped_worker_evidence(
                directory,
                report_path,
                report,
                capture_id,
                expected_saved_boot_stamp=pending["previous_boot_stamp"],
            )
            (
                evidence,
                evidence_path,
                evidence_raw,
                launch_path,
                launch_raw,
                checks_path,
                checks_raw,
            ) = stop
            if evidence.get("category") == "temporary":
                if (
                    evidence.get("original_process_group")
                    != pending.get("previous_active_process_group")
                    or evidence.get("mode") != "historical"
                    or report.get("boot_stamp") != pending.get("previous_boot_stamp")
                ):
                    raise RunnerError("prior-boot temporary stop evidence changed")
                event_identity = {
                    "capture_id": capture_id,
                    "session_id": report.get("session_id"),
                    "root_id": report.get("root_id"),
                    "stale_process_group": evidence["original_process_group"],
                    "supervisor_pid": evidence["supervisor_pid"],
                    "supervisor_pgid": evidence["supervisor_pgid"],
                    "boot_stamp": report["boot_stamp"],
                    "revision": report.get("revision"),
                    "launch_revision_source": evidence["launch_revision_source"],
                    "processing_revision": evidence.get("processing_revision"),
                    "reconciliation_runtime_revision": evidence.get(
                        "reconciliation_runtime_revision"
                    ),
                    "report_sha256": raw_sha256,
                    "evidence_path": str(evidence_path),
                    "evidence_sha256": hashlib.sha256(evidence_raw).hexdigest(),
                    "launch_path": str(launch_path),
                    "launch_sha256": hashlib.sha256(launch_raw).hexdigest(),
                    "launch_checks_path": str(checks_path) if checks_path is not None else None,
                    "launch_checks_sha256": (
                        hashlib.sha256(checks_raw).hexdigest() if checks_raw is not None else None
                    ),
                    "primary_stop_reason": report.get("error"),
                    "reconciliation_kind": "temporary_storage_stop",
                    "storage_stop_evidence": {
                        key: evidence[key]
                        for key in (
                            "mode",
                            "category",
                            "revision_source",
                            "threshold_bytes",
                            "threshold_inclusive",
                            "observed_bytes",
                            "original_process_group",
                        )
                    },
                    "recovery_kind": "prior_boot_temporary_stop",
                    "recovered_boot_stamp": pending["boot_stamp"],
                    "accounting_sha256": accounting_sha256,
                    "pre_recovery_history_sha256": pre_recovery_history_sha256,
                    "accounting_basis": accounting["accounting_basis"],
                    "accounting_complete": accounting["accounting_complete"],
                    "accounting_bound_verified": accounting["accounting_bound_verified"],
                    "attempts": accounting["attempts"],
                    "downloaded_bytes": accounting.get("downloaded_bytes"),
                    "downloaded_bytes_upper_bound": accounting.get("downloaded_bytes_upper_bound"),
                    "unfinished_reservation_ids": accounting["unfinished_reservation_ids"],
                    "reservation_inventory_sha256": accounting.get("reservation_inventory_sha256"),
                    "previous_filesystem_device": pending["previous_filesystem_device"],
                    "filesystem_device": pending["filesystem_device"],
                    "volume_uuid": pending["volume_uuid"],
                    "prior_process_group_ignored_after_boot_change": True,
                }
                event_id = hashlib.sha256(
                    json.dumps(event_identity, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                audit = _append_stopped_worker_audit(
                    operator,
                    {
                        "version": 1,
                        "event_id": event_id,
                        "reconciled_at": _utc(),
                        **event_identity,
                    },
                    allow_distinct_attempts=True,
                )
                import historical_grants as grants

                record_sha256 = grants.sha256_json(audit)
                audit_binding = {
                    "event_id": event_id,
                    "record_sha256": record_sha256,
                    "audit_file": "stopped-worker-reconciliation.jsonl",
                    "boot_stamp": report["boot_stamp"],
                    "primary_stop_reason": report.get("error"),
                    "supervisor_pid": evidence["supervisor_pid"],
                    "supervisor_pgid": evidence["supervisor_pgid"],
                    "reconciliation_kind": "temporary_storage_stop",
                    "recovery_kind": "prior_boot_temporary_stop",
                    "storage_stop_evidence": event_identity["storage_stop_evidence"],
                    "stale_process_group": evidence["original_process_group"],
                    "termination_evidence_sha256": event_identity["evidence_sha256"],
                    "session_before_sha256": raw_sha256,
                    "pre_recovery_history_sha256": pre_recovery_history_sha256,
                    "recovered_boot_stamp": pending["boot_stamp"],
                    "launch_revision_source": event_identity["launch_revision_source"],
                    "processing_revision": event_identity["processing_revision"],
                    "reconciliation_runtime_revision": event_identity[
                        "reconciliation_runtime_revision"
                    ],
                    "launch_sha256": event_identity["launch_sha256"],
                    "launch_checks_sha256": event_identity["launch_checks_sha256"],
                    "recovery_audit_sha256": record_sha256,
                }
            elif accounting.get("accounting_complete") is not True:
                raise RunnerError("reboot recovery requires a typed temporary storage stop")
            if regular_bytes(report_path, 16 * MIB) != raw:
                raise RunnerError("session report changed during reboot audit append")
    else:
        snapshot, accounting = read_accounting(locked_store=locked_store)

    accounting_sha256 = (
        _accounting_snapshot_sha256(snapshot)
        if audit_binding is not None
        else _full_accounting_snapshot_sha256(snapshot)
    )

    private_reconciliation_sha256 = None
    if private_uncertain:
        if (
            private_worker.get("identity", {}).get("capture_id") != capture_id
            or private_worker.get("identity", {}).get("root_id") != report.get("root_id")
            or private_worker.get("report_sha256") != raw_sha256
            or private_worker.get("worker_pid") != private_worker.get("worker_pgid")
            or private_worker.get("worker_pgid") != pending.get("previous_active_process_group")
        ):
            raise RunnerError("private reboot reconciliation binding changed")
        if accounting["accounting_complete"] is False:
            import historical_grants as grants

            audit = report.get("stopped_worker_reconciliation")
            if (
                not isinstance(audit, dict)
                or audit.get("recovery_kind") != "prior_boot_temporary_stop"
            ):
                raise RunnerError("private reboot requires an audited reservation bound")
            record = _read_stopped_worker_audit(operator, audit.get("event_id"))
            if (
                locked_store is None
                or grants.sha256_json(record) != audit.get("recovery_audit_sha256")
                or record.get("recovery_kind") != "prior_boot_temporary_stop"
                or record.get("capture_id") != capture_id
                or record.get("session_id") != report.get("session_id")
                or record.get("root_id") != report.get("root_id")
                or record.get("report_sha256") != private_worker["identity"]["prior_session_sha256"]
                or record.get("accounting_sha256") != _accounting_snapshot_sha256(snapshot)
                or not _same_audited_reservation_bound(accounting, record)
            ):
                raise RunnerError(
                    "private reboot receipt accounting differs from its audited bound"
                )
            accounting_sha256 = record["accounting_sha256"]
        private_record, private_reconciliation_sha256 = _historical_write_private_reconciliation(
            Path(private_worker["control_root"]),
            private_worker["grant_id"],
            {
                "version": 1,
                "kind": "changed_boot_worker_ignored",
                "identity": private_worker["identity"],
                "grant_id": private_worker["grant_id"],
                "launch_id": private_worker["launch_id"],
                "launch_sha256": private_worker["launch_sha256"],
                "from_boot_stamp": report["boot_stamp"],
                "to_boot_stamp": pending["boot_stamp"],
                "previous_active_process_group": pending["previous_active_process_group"],
                "controller_pid": private_worker["controller_pid"],
                "controller_pgid": private_worker["controller_pgid"],
                "worker_pid": private_worker["worker_pid"],
                "worker_pgid": private_worker["worker_pgid"],
                "prior_process_group_ignored_after_boot_change": True,
                "process_group_interaction": "not_queried_after_boot_change",
                "session_before_sha256": private_worker["report_sha256"],
                "storage_progress_sha256": private_worker["storage_sha256"],
                "accounting_sha256": accounting_sha256,
                "reconciliation_worker_started": False,
                "new_grant_started": False,
            },
        )
        if private_record.get("sha256") != private_reconciliation_sha256:
            raise RunnerError("private reboot reconciliation record failed readback")

    wall_now = time.time()
    mono_now = time.monotonic()
    offset = mono_now - wall_now
    report["started_monotonic"] = offset + float(report["started_epoch"])
    report["time_budget_started_monotonic"] = offset + _time_budget_started(report, "epoch")
    report["deadline_monotonic"] = offset + _time_budget_deadline(report, "epoch")
    report.setdefault("original_system", dict(previous_system))
    previous_system["filesystem_device"] = pending["filesystem_device"]
    previous_system["filesystem_volume_uuid"] = pending["volume_uuid"]
    report["boot_stamp"] = pending["boot_stamp"]
    report["active_process_group"] = None
    report.update(
        requests=accounting["attempts"],
        downloaded_bytes=accounting.get("downloaded_bytes"),
        downloaded_bytes_upper_bound=accounting.get("downloaded_bytes_upper_bound"),
        accounting_complete=accounting["accounting_complete"],
        accounting_basis=accounting["accounting_basis"],
        accounting_bound_verified=accounting["accounting_bound_verified"],
        unfinished_reservation_ids=accounting["unfinished_reservation_ids"],
        consumption_state=(
            accounting["accounting_basis"]
            if accounting["accounting_complete"] is False
            else "verified"
        ),
        network_activity_this_invocation={
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
    )
    _record_operator_accounting(report, snapshot)
    if audit_binding is not None:
        report["active_process_group"] = None
        report["stopped_worker_reconciliation"] = audit_binding
    reboot_event = {
        "recovered_at": _utc(),
        **pending,
        "session_before_sha256": hashlib.sha256(raw).hexdigest(),
        "accounting_sha256": accounting_sha256,
        "pre_recovery_history_sha256": pre_recovery_history_sha256,
        "accounting_basis": accounting["accounting_basis"],
        "accounting_complete": accounting["accounting_complete"],
        "accounting_bound_verified": accounting["accounting_bound_verified"],
        "attempts": accounting["attempts"],
        "downloaded_bytes": accounting.get("downloaded_bytes"),
        "downloaded_bytes_upper_bound": accounting.get("downloaded_bytes_upper_bound"),
        "unfinished_reservation_ids": accounting["unfinished_reservation_ids"],
        "reservation_inventory_sha256": accounting.get("reservation_inventory_sha256"),
        "prior_process_group_ignored_after_boot_change": pending["previous_active_process_group"]
        is not None,
        "processing_worker_started": False,
        "grant_started": False,
        "wall_deadlines_preserved": pending["wall_deadlines_preserved"],
    }
    if audit_binding is not None:
        reboot_event.update(
            recovery_kind="prior_boot_temporary_stop",
            recovered_boot_stamp=pending["boot_stamp"],
            stopped_worker_event_id=audit_binding["event_id"],
            stopped_worker_audit_sha256=audit_binding["recovery_audit_sha256"],
            stopped_worker_session_before_sha256=audit_binding["session_before_sha256"],
            stopped_worker_reconciled_boot_stamp=audit_binding["recovered_boot_stamp"],
            pre_recovery_history_sha256=pre_recovery_history_sha256,
        )
    if private_uncertain:
        reboot_event["private_worker_reconciliation_sha256"] = private_reconciliation_sha256
        report["private_historical_grant"]["state"] = "reconciled-after-boot-change"
        report["private_historical_grant"]["reconciliation_sha256"] = private_reconciliation_sha256
    offline_window = report.get("offline_historical_window")
    if isinstance(offline_window, dict) and (
        report.get("offline_historical_window_boot_transition") is None
        or _offline_window_pending_boot(report)
    ):
        index = len(report.get("offline_historical_window_history", []))
        reboot_event.update(
            offline_window_before_sha256=_offline_window_digest(offline_window),
            offline_window_capture_id=capture_id,
            offline_window_root_id=report["root_id"],
            offline_window_history_index=index,
            offline_window_reconciled_epoch=wall_now,
        )
        report["offline_historical_window_boot_transition"] = {
            "index": index,
            "reboot_sha256": _offline_window_digest(reboot_event),
            "successor_sha256": None,
        }
    report["reboot_recovery_history"] = [*history, reboot_event]
    report["historical_reboot_reconciliation"] = {
        "state": "complete",
        "capture_id": capture_id,
        "root_id": report["root_id"],
        "boot_stamp": pending["boot_stamp"],
        "accounting_basis": accounting["accounting_basis"],
        "accounting_complete": accounting["accounting_complete"],
        "accounting_bound_verified": accounting["accounting_bound_verified"],
        "processing_worker_started": False,
        "grant_started": False,
        "private_worker_reconciliation_sha256": private_reconciliation_sha256,
    }
    if audit_binding is not None:
        report["historical_reboot_reconciliation"].update(
            recovery_kind="prior_boot_temporary_stop",
            stopped_worker_event_id=audit_binding["event_id"],
            stopped_worker_audit_sha256=audit_binding["recovery_audit_sha256"],
            stopped_worker_session_before_sha256=audit_binding["session_before_sha256"],
        )
    report["phase"] = "stopped"
    report["error"] = None
    report["recovery"] = (
        "No historical worker or grant was started. After explicit authorization, use the "
        "approved offline historical continuation for this capture and its saved checkpoint."
    )
    report["last_observed_at"] = _utc()
    report["last_observed_epoch"] = wall_now
    _atomic_report(report_path, report)
    return report


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


def _accounting_snapshot_sha256(snapshot: dict[str, Any]) -> str:
    """Hash receipt-derived facts without storage usage that the audit append changes."""
    facts = {
        key: snapshot.get(key)
        for key in (
            "root_id",
            "attempts",
            "downloaded_or_reserved_bytes",
            "accounting_complete",
            "unfinished_reservations",
        )
    }
    unfinished = facts["unfinished_reservations"]
    if isinstance(unfinished, list):
        facts["unfinished_reservations"] = sorted(
            unfinished,
            key=lambda item: item.get("request_id", "") if isinstance(item, dict) else "",
        )
    return hashlib.sha256(
        json.dumps(facts, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _full_accounting_snapshot_sha256(snapshot: dict[str, Any]) -> str:
    """Keep the established digest for exact and private reboot reconciliation records."""
    return hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


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


def _historical_control_root(path: Path) -> Path:
    from kalshi_events.common import safe_path

    try:
        root = safe_path(path)
        details = root.lstat()
    except OSError as exc:
        raise RunnerError("private historical control root is missing or unsafe") from exc
    try:
        checkout_runtime = (SOURCE_ROOT / "data" / "runtime").resolve(strict=True)
    except OSError:
        checkout_runtime = None
    frozen_sibling_layout = (
        SOURCE_ROOT.name == "source"
        and root == SOURCE_ROOT.parent.resolve()
        and root.parent.name == "runtime"
    )
    checkout_layout = checkout_runtime is not None and root.parent == checkout_runtime
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.geteuid()
        or stat.S_IMODE(details.st_mode) & 0o077
        or not (checkout_layout or frozen_sibling_layout)
        or HISTORICAL_CONTROL_DIRECTORY.fullmatch(root.name) is None
    ):
        raise RunnerError("private historical control root is outside the managed runtime layout")
    return root


def _historical_source_root(control_root: Path) -> Path:
    """Resolve the frozen source tree from its managed control-root layout."""
    root = _historical_control_root(control_root)
    frozen_source = root / "source"
    if frozen_source.is_dir() and not frozen_source.is_symlink():
        source_root = frozen_source
    else:
        # In a checkout the control root is <project>/data/runtime/<controls>.
        source_root = root.parent.parent.parent
        if source_root / "data" / "runtime" != root.parent:
            raise RunnerError("historical source root is not bound to the managed controls")
    try:
        script = (source_root / "scripts" / "production_run.py").resolve(strict=True)
        package = (source_root / "src" / "kalshi_events").resolve(strict=True)
        executing_script = Path(__file__).resolve(strict=True)
    except OSError as exc:
        raise RunnerError("frozen historical source tree is unavailable") from exc
    if (
        script != executing_script
        or not package.is_dir()
        or not (source_root / "scripts" / "historical_grants.py").is_file()
    ):
        raise RunnerError("running historical controller is outside its bound source tree")
    return source_root


def _historical_venv_root(control_root: Path) -> Path:
    root = _historical_control_root(control_root)
    source_root = _historical_source_root(root)
    if source_root == root / "source":
        return root / ".venv"
    return source_root / ".venv"


def _historical_assert_pinned_python(control_root: Path) -> None:
    """Require grant-consuming controllers and workers to use the frozen venv."""
    venv_root = _historical_venv_root(control_root).resolve(strict=True)
    expected_python = (venv_root / "bin" / "python").resolve(strict=True)
    try:
        active_prefix = Path(sys.prefix).resolve(strict=True)
        active_executable = Path(sys.executable).resolve(strict=True)
        base_prefix = Path(sys.base_prefix).resolve(strict=True)
    except OSError as exc:
        raise RunnerError("historical Python environment identity is unavailable") from exc
    if (
        active_prefix != venv_root
        or active_executable != expected_python
        or active_prefix == base_prefix
    ):
        raise RunnerError(
            "historical controller or worker is outside the pinned virtual environment"
        )


def _historical_file_record(
    path: Path, *, max_bytes: int = 256 * 1024 * 1024, linked_source: Path | None = None
) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        before = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RunnerError(f"frozen runtime input is missing or unsafe: {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != (1 if linked_source is None else 2)
            or opened.st_size > max_bytes
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RunnerError(f"frozen runtime input is unsafe or oversized: {path}")
        checksum = hashlib.sha256()
        size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            checksum.update(chunk)
            size += len(chunk)
            if size > max_bytes:
                raise RunnerError(f"frozen runtime input is oversized: {path}")
        after = os.fstat(descriptor)
        final = path.lstat()

        def signature(item: os.stat_result) -> tuple[int, ...]:
            return (
                item.st_dev,
                item.st_ino,
                item.st_mode,
                item.st_nlink,
                item.st_size,
                item.st_mtime_ns,
                item.st_ctime_ns,
            )

        if signature(opened) != signature(after) or signature(opened) != signature(final):
            raise RunnerError(f"frozen runtime input changed while being hashed: {path}")
        if linked_source is not None and signature(opened) != signature(linked_source.lstat()):
            raise RunnerError("activated database is not a hard link to the certified warehouse")
        return {
            "sha256": checksum.hexdigest(),
            "size": size,
            "mode": oct(stat.S_IFMT(opened.st_mode) | stat.S_IMODE(opened.st_mode)),
        }
    finally:
        os.close(descriptor)


def _historical_operational_source_sha256(control_root: Path) -> str:
    from historical_grants import sha256_json

    source_root = _historical_source_root(control_root)
    roots = [source_root / "src" / "kalshi_events"]
    paths = [
        source_root / "pyproject.toml",
        source_root / "uv.lock",
        source_root / "scripts" / "production_run.py",
        source_root / "scripts" / "historical_grants.py",
        source_root / "scripts" / "historical_admission.py",
        source_root / "scripts" / "production_activation.py",
        control_root / "controls" / "runtime_manifest.py",
    ]
    for package_root in roots:
        try:
            root_info = package_root.lstat()
        except OSError as exc:
            raise RunnerError("historical package source tree is missing") from exc
        if not stat.S_ISDIR(root_info.st_mode):
            raise RunnerError("historical package source tree is unsafe")
        for candidate in package_root.rglob("*"):
            details = candidate.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise RunnerError("historical package source tree contains a symlink")
            if stat.S_ISREG(details.st_mode):
                if candidate.name.endswith(".pyc") or candidate.parent.name == "__pycache__":
                    raise RunnerError("historical source contains unfingerprinted Python cache")
                paths.append(candidate)
    records = []

    def label(path: Path) -> str:
        if path == control_root / "controls" / "runtime_manifest.py":
            return "control-helper/runtime_manifest.py"
        try:
            return path.relative_to(source_root).as_posix()
        except ValueError as exc:
            raise RunnerError("operational source file is outside its bound source root") from exc

    for path in sorted(set(paths), key=label):
        record = _historical_file_record(path)
        records.append({"path": label(path), **record})
    try:
        host = HISTORICAL_HOST_PYTHON.resolve(strict=True)
    except OSError as exc:
        raise RunnerError("trusted host Python interpreter is missing") from exc
    host_record = _historical_file_record(host, max_bytes=1024 * 1024 * 1024)
    return sha256_json({"source_files": records, "host_python": host_record})


def _historical_verify_runtime(
    root: Path,
    freeze: dict[str, Any],
    *,
    deadline_epoch: float | None = None,
    deadline_monotonic: float | None = None,
) -> str:
    """Use the trusted host interpreter to verify the frozen venv before workers start."""
    helper = root / "controls" / "runtime_manifest.py"
    helper_record = _historical_file_record(helper, max_bytes=2 * 1024 * 1024)
    if helper_record["mode"] != oct(stat.S_IFREG | 0o400):
        raise RunnerError("trusted runtime inventory helper must be immutable and owner-readable")
    host = HISTORICAL_HOST_PYTHON
    commands = (
        [
            str(host),
            "-I",
            "-S",
            "-B",
            str(helper),
            "--check-source-root",
            str(_historical_source_root(root)),
        ],
        [
            str(host),
            "-I",
            "-S",
            "-B",
            str(helper),
            "--verify",
            "--root",
            str(_historical_venv_root(root)),
            "--manifest",
            str(root / "runtime-manifest.json"),
            "--expected-sha256",
            str(freeze["runtime_manifest_sha256"]),
        ],
    )
    env = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL") if key in os.environ}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    for command in commands:
        timeout = 30 * 60.0
        if deadline_epoch is not None or deadline_monotonic is not None:
            if deadline_epoch is None or deadline_monotonic is None:
                raise RunnerError("runtime precheck requires both grant deadlines")
            remaining = min(
                deadline_epoch - time.time(),
                deadline_monotonic - time.monotonic(),
            ) - float(_effective_limits()["terminate_seconds"])
            if remaining <= 0:
                raise RunnerError("no grant time remains for trusted runtime verification")
            timeout = min(timeout, remaining)
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RunnerError("trusted host runtime precheck could not complete") from exc
        if result.returncode != 0:
            raise RunnerError("trusted host runtime precheck rejected the frozen inputs")
    return result.stdout.strip()


def _historical_store(operator: Path, *, max_duration_seconds: float) -> Any:
    from kalshi_events.common import Limits, Store

    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * GIB,
        max_response_bytes=384 * MIB,
        max_decoded_response_bytes=768 * MIB,
        max_duration_seconds=max_duration_seconds,
        max_root_bytes=350 * GIB,
        max_temp_bytes=192 * GIB,
        max_database_bytes=128 * GIB,
    )
    return Store(operator, limits)


def _historical_authorized_census(
    authority: dict[str, Any], *, full_revalidation: bool
) -> dict[str, Any]:
    """Bind the target's immutable population report/index; optionally rewalk receipts."""
    import collections
    import json
    import math
    import time

    import historical_admission

    from kalshi_events import capture, historical, publish
    from kalshi_events.common import CatalogueError, _root_scope

    control = authority["control"].get("authorized_census")
    if not isinstance(control, dict):
        raise RunnerError("private controls have no authorized population census")
    population_path = Path(control["population_report_path"])
    index_path = Path(control["receipt_index_path"])
    for path, expected_sha, label in (
        (population_path, control["population_report_sha256"], "population report"),
        (index_path, control["receipt_index_sha256"], "receipt index"),
    ):
        record = _historical_file_record(path, max_bytes=256 * MIB)
        if record["sha256"] != expected_sha:
            raise RunnerError(f"authorized {label} changed after the controls were frozen")
        if path.stat().st_dev != authority["directory"].stat().st_dev:
            raise RunnerError(f"authorized {label} is not on the frozen production volume")

    def strict_json(raw: bytes) -> dict[str, Any]:
        def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            value: dict[str, Any] = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate JSON key")
                value[key] = item
            return value

        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
        if not isinstance(value, dict):
            raise ValueError("expected object")
        return value

    def read_regular(path: Path, limit: int) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            before = path.lstat()
            fd = os.open(path, flags)
        except OSError as exc:
            raise CatalogueError("authorized census input is unavailable") from exc
        try:
            opened = os.fstat(fd)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_size > limit
                or opened.st_dev != authority["directory"].stat().st_dev
                or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            ):
                raise CatalogueError("authorized census input is unsafe or oversized")
            chunks: list[bytes] = []
            total = 0
            while chunk := os.read(fd, min(1024 * 1024, limit + 1 - total)):
                chunks.append(chunk)
                total += len(chunk)
                if total > limit:
                    raise CatalogueError("authorized census input is oversized")
            after = os.fstat(fd)
            final = path.lstat()

            def signature(item: os.stat_result) -> tuple[int, ...]:
                return (
                    item.st_dev,
                    item.st_ino,
                    item.st_mode,
                    item.st_nlink,
                    item.st_size,
                    item.st_mtime_ns,
                    item.st_ctime_ns,
                )

            if signature(opened) != signature(after) or signature(opened) != signature(final):
                raise CatalogueError("authorized census input changed while reading")
            return b"".join(chunks)
        finally:
            os.close(fd)

    population_raw = read_regular(population_path, MIB)
    index_raw = read_regular(index_path, 256 * MIB)
    try:
        population = strict_json(population_raw)
        index = strict_json(index_raw)
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("authorized population report or receipt index is malformed") from exc
    expected_population = {
        "capture_id": control["capture_id"],
        "root_id": control["root_id"],
        "capture_state_sha256": control["capture_state_sha256"],
        "candidate_pages": control["candidate_pages"],
        "observations_by_endpoint": control["observations_by_endpoint"],
        "nested_event_markets": control["nested_event_markets"],
        "exact_observations": control["exact_observations"],
    }
    report_counts = population.get("observation_counts")
    actual_population = {
        "capture_id": population.get("capture_id"),
        "root_id": control["root_id"],
        "capture_state_sha256": population.get("capture_state_sha256"),
        "candidate_pages": population.get("candidate_pages"),
        "observations_by_endpoint": (
            {key: value for key, value in report_counts.items() if key.startswith("/")}
            if isinstance(report_counts, dict)
            else None
        ),
        "nested_event_markets": (
            report_counts.get("nested_event_markets") if isinstance(report_counts, dict) else None
        ),
        "exact_observations": (
            sum(value for key, value in report_counts.items() if key.startswith("/"))
            if isinstance(report_counts, dict)
            else None
        ),
    }
    if (
        population.get("status") != "complete"
        or population.get("metadata_index_sha256") != control["receipt_index_sha256"]
        or index.get("capture_id") != control["capture_id"]
        or index.get("capture_state_sha256") != control["capture_state_sha256"]
        or actual_population != expected_population
    ):
        raise RunnerError(
            "authorized report and receipt index do not match the frozen exact census"
        )

    result = {
        "population_report_sha256": control["population_report_sha256"],
        "receipt_index_sha256": control["receipt_index_sha256"],
        "candidate_pages": control["candidate_pages"],
        "observations_by_endpoint": control["observations_by_endpoint"],
        "nested_event_markets": control["nested_event_markets"],
        "exact_observations": control["exact_observations"],
    }
    if not full_revalidation:
        return result

    class ReadOnlyStore:
        def __init__(self, root: Path, deadline: float):
            self.root = root.resolve(strict=True)
            self.deadline = deadline
            self.limits = type("ReadOnlyLimits", (), {"max_root_bytes": 350 * GIB})()
            self.scope = _root_scope(strict_json(read_regular(self.path("root.json"), MIB)))

        def path(self, relative: str) -> Path:
            candidate = Path(relative)
            if candidate.is_absolute() or any(part in {"", ".", ".."} for part in candidate.parts):
                raise CatalogueError("unsafe read-only census path")
            path = self.root.joinpath(*candidate.parts)
            if not path.resolve(strict=False).is_relative_to(self.root):
                raise CatalogueError("read-only census path escaped the operator root")
            current = self.root
            for part in candidate.parts:
                current = current / part
                try:
                    item = current.lstat()
                except OSError as exc:
                    raise CatalogueError("read-only census path is missing") from exc
                if stat.S_ISLNK(item.st_mode) or item.st_dev != self.root.stat().st_dev:
                    raise CatalogueError("read-only census path crossed a device or symlink")
            return path

        def check_deadline(self) -> None:
            if time.monotonic() >= self.deadline:
                raise TimeoutError(
                    "authorized census receipt revalidation reached its grant cutoff"
                )

    class CensusFacade:
        @staticmethod
        def _read_regular_bytes(path: Path, limit: int) -> bytes:
            return read_regular(path, limit)

        @staticmethod
        def _read_json(path: Path, limit: int) -> dict[str, Any]:
            return strict_json(read_regular(path, limit))

        @staticmethod
        def _historical_task_plan(state: Any):
            selection = historical.historical_selection(state, allow_legacy_incomplete=True)
            task_indices = historical.historical_task_indices(state, allow_legacy_incomplete=True)
            expected_scopes = {"series", "events_live", "markets_history"}
            if set(selection) != expected_scopes:
                raise CatalogueError(
                    "authorized census selection differs from publication contract"
                )
            task_scopes = {int(key): value for key, value in state["task_scopes"].items()}
            excluded = [
                {
                    "task_index": index,
                    "scope": scope,
                    "endpoint": state["tasks"][index]["endpoint"],
                }
                for index, scope in sorted(task_scopes.items())
                if scope in publish.HISTORICAL_OMITTED_SCOPES
            ]
            if any(item["task_index"] in task_indices for item in excluded):
                raise CatalogueError("excluded scopes entered authorized census tasks")
            cutoff = {
                index
                for index, task in enumerate(state["tasks"])
                if task.get("endpoint") == "/historical/cutoff" and task.get("singular") is True
            }
            expected_indices = {*(item["task_index"] for item in selection.values()), *cutoff}
            if len(cutoff) != 1 or task_indices != expected_indices:
                raise CatalogueError("authorized census task indices differ from selection")
            return selection, task_indices, excluded

        @staticmethod
        def _page_metadata(source: Any, row: dict[str, Any], order: int, state: Any = None):
            from pathlib import PurePosixPath

            relative = row.get("relative")
            if not isinstance(relative, str) or not relative.startswith("raw/"):
                raise CatalogueError("capture journal contains an invalid census receipt path")
            receipt_directory = source.path(PurePosixPath(relative).parent.as_posix())
            manifest_path = receipt_directory / "manifest.json"
            manifest = strict_json(read_regular(manifest_path, MIB))
            if state is not None and (
                manifest.get("capture_id") != state.get("capture_id")
                or not capture._receipt_matches_state_protocol(manifest, state)
            ):
                raise CatalogueError("census receipt is not bound to current capture protocol")
            if (
                state is not None
                and capture._acquisition_protocol(state) == capture.CURRENT_ACQUISITION_PROTOCOL
            ):
                capture._validate_receipt_task_binding(manifest, state, row)
            body_path = receipt_directory / "body.bin"
            body_info = body_path.lstat()
            if not stat.S_ISREG(body_info.st_mode) or stat.S_ISLNK(body_info.st_mode):
                raise CatalogueError("census receipt body is missing or unsafe")
            decoded = manifest.get("decoded_size", manifest.get("size"))
            generation = manifest.get("task_generation", manifest.get("request_generation", 0))
            endpoint = manifest.get("endpoint")
            if (
                type(decoded) is not int
                or decoded < 0
                or type(generation) is not int
                or generation < 0
                or not isinstance(endpoint, str)
            ):
                raise CatalogueError("census receipt metadata is malformed")
            return {
                **row,
                "order": order,
                "endpoint": endpoint,
                "generation": generation,
                "encoded_bytes": body_info.st_size,
                "decoded_bytes": decoded,
                "status": manifest.get("status"),
                "manifest_bytes": manifest_path.stat().st_size,
            }

        @staticmethod
        def _is_control_page(page: dict[str, Any]) -> bool:
            return page["status"] != 200 or page["endpoint"] == "/historical/cutoff"

        @staticmethod
        def _quantile(values: list[float], fraction: float) -> float:
            if not values:
                return 0.0
            ordered = sorted(values)
            return ordered[min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)]

        @classmethod
        def _receipt_population_metadata(cls, pages: list[dict[str, Any]]):
            groups: dict[tuple[str, int], list[dict[str, Any]]] = collections.defaultdict(list)
            for page in pages:
                if not cls._is_control_page(page):
                    groups[(page["endpoint"], page["generation"])].append(page)
            return {
                "data_strata": [
                    {
                        "endpoint": endpoint,
                        "generation": generation,
                        "candidate_pages": len(rows),
                        "encoded_bytes": sum(row["encoded_bytes"] for row in rows),
                        "decoded_bytes": sum(row["decoded_bytes"] for row in rows),
                        "median_decoded_page_bytes": cls._quantile(
                            [row["decoded_bytes"] for row in rows], 0.5
                        ),
                        "max_decoded_page_bytes": max(row["decoded_bytes"] for row in rows),
                    }
                    for (endpoint, generation), rows in sorted(groups.items())
                ],
                "control_receipts": [],
            }

        @classmethod
        def _control_receipt_counts(cls, pages: list[dict[str, Any]]):
            counts = collections.Counter(
                (page["endpoint"], page["generation"], page["status"])
                for page in pages
                if cls._is_control_page(page)
            )
            return [
                {
                    "endpoint": endpoint,
                    "generation": generation,
                    "status": status,
                    "receipts": count,
                }
                for (endpoint, generation, status), count in sorted(
                    counts.items(), key=lambda item: str(item[0])
                )
            ]

    store = ReadOnlyStore(
        authority["operator"], float(authority["grant"]["workflow_deadline_monotonic"])
    )
    state = capture.read_capture(store, control["capture_id"])
    try:
        binding = historical_admission._admission_census_binding(
            store,
            state,
            population_path,
            index_path,
            CensusFacade,
            deadline=float(authority["grant"]["workflow_deadline_monotonic"]),
        )
        approved = historical_admission._check_authorized_census(binding)
    except Exception as exc:
        raise RunnerError("read-only authorized capture census revalidation failed") from exc
    if (
        approved["exact_observations"] != control["exact_observations"]
        or approved["observations_by_endpoint"] != control["observations_by_endpoint"]
        or approved["receipt_index_sha256"] != control["receipt_index_sha256"]
        or binding["fingerprints"]["population_report_sha256"]
        != control["population_report_sha256"]
    ):
        raise RunnerError("read-only capture census differs from identity-specific controls")
    return result


def _historical_census_proof(
    authority: dict[str, Any], *, full_revalidation_if_missing: bool
) -> dict[str, Any]:
    import historical_grants as grants

    path = authority["control_root"] / "source" / "authorized-census.json"
    if path.exists() or path.is_symlink():
        proof, _ = _historical_read_artifact(path, 1024**2)
        body = {
            key: proof.get(key)
            for key in ("version", "identity", "source_snapshot_sha256", "census")
        }
        if (
            set(proof) != set(body) | {"sha256"}
            or proof.get("version") != 1
            or proof.get("identity") != authority["identity"]
            or proof.get("source_snapshot_sha256") != authority["source_snapshot"]["sha256"]
            or proof.get("sha256") != grants.sha256_json(body)
        ):
            raise RunnerError("saved authorized population census proof is invalid")
        census = _historical_authorized_census(authority, full_revalidation=False)
        if proof.get("census") != census:
            raise RunnerError("saved authorized census differs from its pinned report and index")
        return census
    if not full_revalidation_if_missing:
        raise RunnerError("authorized population census proof is missing after source phase")
    census = _historical_authorized_census(authority, full_revalidation=True)
    body = {
        "version": 1,
        "identity": authority["identity"],
        "source_snapshot_sha256": authority["source_snapshot"]["sha256"],
        "census": census,
    }
    grants.write_once_json(path, body | {"sha256": grants.sha256_json(body)})
    return census


def _historical_validate_storage_handoff(
    directory: Path,
    report_path: Path,
    progress_path: Path,
    *,
    invocation_token: str,
    allow_stale: bool = False,
) -> dict[str, Any]:
    """Prove the live parent storage monitor is installed before the worker opens Store."""
    import stat

    if re.fullmatch(r"[0-9a-f]{32}", invocation_token) is None:
        raise RunnerError("historical storage handoff token is invalid")
    expected_parent = report_path.parent / "logs"
    try:
        relative = progress_path.relative_to(expected_parent)
        device = directory.stat().st_dev
        parents = (expected_parent, progress_path.parent)
        parent_info = tuple(path.lstat() for path in parents)
    except (OSError, ValueError) as exc:
        raise RunnerError("historical storage observer progress is unavailable") from exc
    if (
        len(relative.parts) != 2
        or re.fullmatch(r"invocation-[0-9a-f]{32}", relative.parts[0]) is None
        or re.fullmatch(r"[a-z0-9-]+\.storage\.json", relative.parts[1]) is None
        or any(not stat.S_ISDIR(info.st_mode) or info.st_dev != device for info in parent_info)
    ):
        raise RunnerError("historical storage observer progress path is unsafe")

    def identity(info: os.stat_result) -> tuple[int, int, int]:
        return info.st_dev, info.st_ino, info.st_mode

    def payload_metadata(info: os.stat_result) -> tuple[int, int, int, int]:
        return info.st_size, info.st_mtime_ns, info.st_uid, info.st_gid

    def safe_file(info: os.stat_result) -> bool:
        return stat.S_ISREG(info.st_mode) and info.st_dev == device and info.st_size <= MIB

    parent_identity = tuple(identity(info) for info in parent_info)
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    # The supervisor replaces the sample atomically. Discard raced generations.
    for _ in range(3):
        try:
            before = progress_path.lstat()
            if not safe_file(before) or before.st_nlink != 1:
                raise RunnerError("historical storage observer progress path is unsafe")
            descriptor = os.open(progress_path, flags)
            try:
                opened = os.fstat(descriptor)
                if not safe_file(opened) or opened.st_nlink not in {0, 1}:
                    raise RunnerError("historical storage observer progress path is unsafe")
                raw_parts = []
                total = 0
                if identity(before) == identity(opened) and opened.st_nlink == 1:
                    while chunk := os.read(descriptor, min(64 * 1024, MIB + 1 - total)):
                        raw_parts.append(chunk)
                        total += len(chunk)
                        if total > MIB:
                            raise RunnerError("historical storage observer progress is oversized")
                after = os.fstat(descriptor)
                final = progress_path.lstat()
                if final.st_ino != opened.st_ino and after.st_nlink == 1:
                    after = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            if (
                not safe_file(final)
                or final.st_nlink != 1
                or after.st_nlink not in {0, 1}
                or tuple(identity(path.lstat()) for path in parents) != parent_identity
            ):
                raise RunnerError("historical storage observer progress path is unsafe")
        except OSError as exc:
            raise RunnerError("historical storage observer progress is unavailable") from exc

        if (
            identity(opened) != identity(after)
            or payload_metadata(opened) != payload_metadata(after)
            or (after.st_nlink == opened.st_nlink == 1 and after.st_ctime_ns != opened.st_ctime_ns)
        ):
            raise RunnerError("historical storage observer progress changed during read")
        if before.st_ino != opened.st_ino and opened.st_nlink == 1:
            continue
        if identity(before) == identity(opened) and payload_metadata(before) == payload_metadata(
            opened
        ):
            if final.st_ino != opened.st_ino and after.st_nlink == 0:
                continue
            if (
                identity(final) == identity(opened)
                and payload_metadata(final) == payload_metadata(opened)
                and before.st_ctime_ns
                == opened.st_ctime_ns
                == after.st_ctime_ns
                == final.st_ctime_ns
                and opened.st_nlink == after.st_nlink == 1
                and total == opened.st_size
            ):
                break
        raise RunnerError("historical storage observer progress changed during read")
    else:
        raise RunnerError("historical storage observer progress kept changing during read")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    try:
        value = json.loads(
            b"".join(raw_parts).decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("historical storage observer progress is malformed") from exc
    if (
        not isinstance(value, dict)
        or value.get("version") != 1
        or value.get("invocation_token") != invocation_token
        or type(value.get("sample_count")) is not int
        or value["sample_count"] < 0
        or type(value.get("target_interval_seconds")) is not int
        or value["target_interval_seconds"] != int(_effective_limits()["storage_sample_seconds"])
    ):
        raise RunnerError("historical storage observer progress is not bound to this worker")
    now = time.time()
    modified = final.st_mtime
    if not allow_stale and (
        modified > now + 2 or now - modified > max(20.0, value["target_interval_seconds"] * 4.0)
    ):
        raise RunnerError("historical storage observer heartbeat is stale")
    return value


def _historical_private_uncertain_authority(
    directory: Path,
    report_path: Path,
    report: dict[str, Any],
    *,
    current_boot: str,
    changed_boot: bool,
) -> dict[str, Any]:
    """Revalidate a private interrupted child without touching its saved PID."""
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    import historical_grants as grants

    marker = report.get("private_historical_grant")
    marker_fields = {
        "version",
        "grant_id",
        "launch_sha256",
        "launch_id",
        "control_root",
        "phases",
        "phase",
        "substep",
        "controller_pid",
        "controller_pgid",
        "worker_pid",
        "worker_pgid",
        "storage_invocation_token",
        "storage_progress_path",
        "state",
    }
    if isinstance(marker, dict) and marker.get("state") == "stopped":
        marker_fields.add("reason")
    phase = marker.get("phase") if isinstance(marker, dict) else None
    substep = marker.get("substep") if isinstance(marker, dict) else None
    stopped_uncertain = (
        isinstance(marker, dict)
        and marker.get("state") == "stopped"
        and report.get("phase") == "uncertain"
        and isinstance(marker.get("reason"), str)
        and bool(marker["reason"])
    )
    abruptly_interrupted = (
        isinstance(marker, dict)
        and marker.get("state") == "supervised-child-pending"
        and phase in _HISTORICAL_PHASE_SUBSTEPS
        and substep in _HISTORICAL_PHASE_SUBSTEPS[phase]
        and report.get("phase") == f"historical-{phase}-{substep}"
    )
    if (
        not isinstance(marker, dict)
        or set(marker) != marker_fields
        or marker.get("version") != 1
        or not (stopped_uncertain or abruptly_interrupted)
        or report.get("production_state") == "verified"
        or report.get("mode") not in {"full", "resume", "historical"}
        or (
            report.get("boot_stamp") == current_boot
            if changed_boot
            else report.get("boot_stamp") != current_boot
        )
    ):
        raise RunnerError("session is not an exact private uncertain historical attempt")

    control_root_value = marker.get("control_root")
    if not isinstance(control_root_value, str) or not Path(control_root_value).is_absolute():
        raise RunnerError("private uncertain attempt has no frozen control-root binding")
    control_root = _historical_control_root(Path(control_root_value))
    if str(control_root) != control_root_value:
        raise RunnerError("private uncertain control-root path is not canonical")
    grant_id = marker.get("grant_id")
    if not isinstance(grant_id, str) or re.fullmatch(r"[0-9a-f]{32}", grant_id) is None:
        raise RunnerError("private uncertain attempt grant ID is invalid")
    if (
        type(marker.get("controller_pid")) is not int
        or marker["controller_pid"] <= 1
        or type(marker.get("controller_pgid")) is not int
        or marker["controller_pgid"] <= 1
        or type(marker.get("worker_pid")) is not int
        or marker["worker_pid"] <= 1
        or type(marker.get("worker_pgid")) is not int
        or marker["worker_pgid"] <= 1
        or type(report.get("active_process_group")) is not int
        or report.get("active_process_group") != marker["worker_pgid"]
        or marker["worker_pid"] != marker["worker_pgid"]
    ):
        raise RunnerError("private uncertain attempt process identities are invalid")
    if (
        phase not in _HISTORICAL_PHASE_SUBSTEPS
        or substep not in _HISTORICAL_PHASE_SUBSTEPS[phase]
        or not isinstance(marker.get("phases"), list)
        or phase not in marker["phases"]
    ):
        raise RunnerError("private uncertain attempt phase binding is invalid")

    bundle = grants.read_control_bundle(control_root, grant_id)
    identity = bundle["identity"]
    approval, latch, launch = bundle["approval"], bundle["latch"], bundle["launch"]
    session_binding = grants.validate_session_binding(
        bundle["session_binding"], approval=approval, expected_identity=identity
    )
    launch_path = control_root / "grants" / grant_id / "launch.json"
    launch_raw = grants._private_regular_file(launch_path, max_bytes=1024**2)
    launch_sha256 = grants.sha256_bytes(launch_raw)
    if (
        launch_sha256 != marker.get("launch_sha256")
        or launch.get("launch_id") != marker.get("launch_id")
        or marker.get("phases") != approval.get("phases")
        or phase not in approval.get("phases", [])
        or launch.get("boot_stamp") != report.get("boot_stamp")
        or session_binding.get("boot_stamp") != report.get("boot_stamp")
        or session_binding.get("report_path") != str(report_path)
        or session_binding.get("root_volume_uuid") != bundle["freeze"].get("root_volume_uuid")
        or session_binding.get("root_device") != report.get("system", {}).get("filesystem_device")
        or report.get("system", {}).get("filesystem_volume_uuid")
        not in {None, bundle["freeze"].get("root_volume_uuid")}
        or grants.sha256_json(session_binding) != launch.get("session_binding_sha256")
        or any(report.get(key) != identity[key] for key in ("capture_id", "session_id", "root_id"))
        or report_path.parent.name != identity["session_id"]
        or report.get("root_id") != identity["root_id"]
    ):
        raise RunnerError("private uncertain attempt does not match its frozen grant")
    try:
        grant = grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=float(launch["started_epoch"]),
            now_monotonic=float(launch["started_monotonic"]),
            boot_stamp=str(launch["boot_stamp"]),
            dbt_memory_limit="4GB",
            dbt_threads=4,
        )
        if grant["phase"] != approval["phase"]:
            raise RunnerError("private uncertain phase grant is inconsistent")
    except grants.ControlError as exc:
        raise RunnerError("private uncertain grant or launch failed its immutable checks") from exc
    _historical_measurement_artifact(
        control_root,
        grant_id,
        approval,
        identity,
        bundle["freeze"],
        session_report_sha256=session_binding["report_sha256"],
        session_root_device=session_binding["root_device"],
        baseline=None,
        require_fresh=False,
        allow_initial_maintenance_overage=(phase == "source" and substep == "maintenance"),
    )

    _historical_assert_pinned_python(control_root)
    operational_source_sha = _historical_operational_source_sha256(control_root)
    freeze = bundle["freeze"]
    runtime_sha = grants.sha256_json(
        {
            "runtime_manifest_sha256": freeze["runtime_manifest_sha256"],
            "operational_source_sha256": operational_source_sha,
        }
    )
    if (
        operational_source_sha != freeze["operational_source_sha256"]
        or runtime_sha != identity["runtime_sha256"]
    ):
        raise RunnerError("private uncertain attempt no longer matches frozen source/runtime")
    _historical_verify_runtime(control_root, freeze)

    operator = directory / "operator"
    current_mount = _historical_current_mount(
        directory,
        operator,
        control_root,
        report,
        identity,
        freeze,
        current_boot=current_boot,
        session_root_device=session_binding["root_device"],
        allow_unreconciled_boot_change=changed_boot,
    )

    token = marker.get("storage_invocation_token")
    progress_path = Path(marker.get("storage_progress_path", ""))
    if (
        not isinstance(token, str)
        or re.fullmatch(r"[0-9a-f]{32}", token) is None
        or not progress_path.is_absolute()
        or report.get("storage_supervision_invocation") != token
        or report.get("storage_supervision_file") != str(progress_path)
        or marker.get("controller_pid") <= 1
        or marker.get("controller_pgid") <= 1
    ):
        raise RunnerError("private uncertain storage handoff binding is invalid")
    storage_state = _historical_validate_storage_handoff(
        directory,
        report_path,
        progress_path,
        invocation_token=token,
        allow_stale=True,
    )
    if storage_state.get("sample_count", 0) < 0:
        raise RunnerError("private uncertain storage handoff has no valid inventory")

    return {
        "control_root": control_root,
        "identity": identity,
        "freeze": freeze,
        "grant": grant,
        "launch": launch,
        "launch_sha256": launch_sha256,
        "session_binding": session_binding,
        "current_mount": current_mount,
        "marker": marker,
        "storage_state": storage_state,
        "storage_sha256": grants.sha256_json(storage_state),
        "report_sha256": hashlib.sha256(
            _historical_session_report_bytes(report_path, max_bytes=16 * MIB)
        ).hexdigest(),
        "current_boot": current_boot,
        "changed_boot": changed_boot,
    }


def _historical_private_processes_absent(proof: dict[str, Any]) -> None:
    marker = proof["marker"]
    try:
        if any(
            (
                _pid_exists(marker["controller_pid"]),
                _process_group_exists(marker["controller_pgid"]),
                _pid_exists(marker["worker_pid"]),
                _process_group_exists(marker["worker_pgid"]),
            )
        ):
            raise RunnerError("private historical controller or worker is live or ambiguous")
    except OSError as exc:
        raise RunnerError("private historical controller or worker identity is ambiguous") from exc


def _historical_write_private_reconciliation(
    control_root: Path, grant_id: str, body: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    import historical_grants as grants

    path = control_root / "grants" / grant_id / "private-reconciliation.json"
    existing = None
    if path.exists() or path.is_symlink():
        existing = grants.read_json(path)
        saved_body = {key: value for key, value in existing.items() if key not in {"sha256"}}
        expected_body = {key: value for key, value in body.items() if key != "completed_at_epoch"}
        compared_body = {
            key: value for key, value in saved_body.items() if key != "completed_at_epoch"
        }
        if (
            existing.get("sha256") != grants.sha256_json(saved_body)
            or compared_body != expected_body
        ):
            raise RunnerError(
                "private historical reconciliation record conflicts with this attempt"
            )
        return existing, existing["sha256"]
    body = dict(body)
    body["completed_at_epoch"] = time.time()
    record = body | {"sha256": grants.sha256_json(body)}
    grants.write_once_json(path, record)
    return record, record["sha256"]


class _HistoricalStorageHeartbeat:
    """Stop native work if the parent storage supervisor stops producing samples."""

    def __init__(
        self,
        directory: Path,
        report_path: Path,
        progress_path: Path,
        invocation_token: str,
    ) -> None:
        self.directory = directory
        self.report_path = report_path
        self.progress_path = progress_path
        self.invocation_token = invocation_token
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: str | None = None
        self._interval = float(_effective_limits()["storage_sample_seconds"])

    def start(self) -> None:
        if self._thread is not None:
            raise RunnerError("historical storage heartbeat was started twice")
        _historical_validate_storage_handoff(
            self.directory,
            self.report_path,
            self.progress_path,
            invocation_token=self.invocation_token,
        )
        context = contextvars.copy_context()
        self._thread = threading.Thread(
            target=context.run,
            args=(self._watch,),
            name="historical-storage-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def _watch(self) -> None:
        grace = max(20.0, self._interval * 4.0)
        while not self._stop.wait(min(1.0, max(0.25, self._interval / 2.0))):
            try:
                state = _historical_validate_storage_handoff(
                    self.directory,
                    self.report_path,
                    self.progress_path,
                    invocation_token=self.invocation_token,
                )
                if state.get("target_interval_seconds", 0) * 4.0 > grace:
                    grace = float(state["target_interval_seconds"] * 4)
            except Exception as exc:
                self._failure = f"historical live storage supervisor failed: {exc}"
                print(self._failure, file=sys.stderr, flush=True)
                if os.getpid() > 1 and os.getpgrp() == os.getpid():
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(os.getpgrp(), signal.SIGTERM)
                return

    def check(self) -> None:
        if self._failure is not None:
            raise RunnerError(self._failure)

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
            if thread.is_alive():
                raise RunnerError("historical storage heartbeat did not stop cleanly")
        self.check()


def _historical_source_snapshot(
    store: Any,
    control_root: Path,
    identity: dict[str, Any],
    freeze: dict[str, Any],
    *,
    grant_id: str,
    approval: dict[str, Any],
    control: dict[str, Any],
    verified_mount: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from historical_grants import ControlError, read_json

    from kalshi_events import historical_resume, warehouse
    from kalshi_events.common import decode, digest, json_bytes, regular_bytes

    state = warehouse.read_capture(store, identity["capture_id"])
    state_sha = digest(json_bytes(dict(state)))
    selection, task_indices = warehouse._historical_profile(state)
    scope_sha = warehouse._historical_scope_sha256(state, selection, task_indices)
    code_sha = warehouse._historical_resume_code_identity()
    stage_relative = historical_resume.stage_relative(identity["stage_name"])
    source_path = store.path(f"{stage_relative}/warehouse.duckdb")
    cleanup_path = store.path(f"{stage_relative}/resume-cleanup-intent.json")
    cleanup_pending = cleanup_path.exists() or cleanup_path.is_symlink()
    saved_path = control_root / "source" / "source-snapshot.json"
    try:
        frozen_snapshot = read_json(saved_path)
    except ControlError as exc:
        raise RunnerError("frozen historical source snapshot is missing or unsafe") from exc
    frozen_body = {
        key: frozen_snapshot.get(key) for key in ("version", "phase", "binding", "result")
    }
    if frozen_snapshot.get("sha256") != digest(json_bytes(frozen_body)):
        raise RunnerError("saved historical source snapshot failed its self-check")
    if (source_path.exists() or source_path.is_symlink()) and not cleanup_pending:
        if source_path.is_symlink() or not source_path.is_file():
            raise RunnerError("historical resume source path is unsafe")
        current_snapshot = warehouse.historical_readiness_snapshot(
            store,
            identity["capture_id"],
            identity["stage_name"],
            _verified_mount=verified_mount,
        )
        current_body = {
            key: current_snapshot.get(key) for key in ("version", "phase", "binding", "result")
        }
        if current_snapshot.get("sha256") != digest(json_bytes(current_body)):
            raise RunnerError("current historical source snapshot failed its self-check")
        frozen_result = frozen_snapshot.get("result")
        current_result = current_snapshot.get("result")
        if not isinstance(frozen_result, dict) or not isinstance(current_result, dict):
            raise RunnerError("historical source snapshot result is malformed")
        for identity_key in ("source_identity", "source_wal_identity"):
            try:
                matches = _historical_resume_source_identity_equal(
                    frozen_result.get(identity_key),
                    current_result.get(identity_key),
                    store=store,
                    state=state,
                    verified_mount=verified_mount,
                    require_live=True,
                )
            except Exception as exc:
                raise RunnerError("historical source snapshot device lineage is invalid") from exc
            if not matches:
                raise RunnerError("historical source snapshot identity differs from frozen source")
        normalized_frozen = dict(frozen_body)
        normalized_result = dict(frozen_result)
        for identity_key in ("source_identity", "source_wal_identity"):
            persisted_identity = frozen_result.get(identity_key)
            if persisted_identity is not None:
                try:
                    normalized_result[identity_key] = list(
                        historical_resume.normalize_resume_file_identity(
                            persisted_identity,
                            store=store,
                            state=state,
                            verified_mount=verified_mount,
                        )
                    )
                except Exception as exc:
                    raise RunnerError(
                        "frozen historical source identity is outside mount lineage"
                    ) from exc
        normalized_frozen["result"] = normalized_result
        if normalized_frozen != current_body:
            raise RunnerError("current historical source proof differs from its frozen snapshot")
        snapshot = frozen_snapshot
    else:
        snapshot = frozen_snapshot
        candidate_path = store.path(f"{stage_relative}/resume-candidate/warehouse.duckdb")
        if candidate_path.is_file() and not candidate_path.is_symlink():
            # The closed candidate may already contain valid derived model
            # changes, or may need supervised readiness revalidation after an
            # interrupted model attempt. Check immutable source/adoption
            # provenance here; let the next supervised prepare decide whether
            # the current candidate is sound. Never make source preflight depend
            # on re-verifying a mutable candidate.
            state = warehouse.read_capture(store, identity["capture_id"])
            source_proof, source_result = warehouse._read_source_proof(
                store, state, identity["stage_name"], candidate_path.parent
            )
            adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
            adoption_loaded = warehouse._load_self_proof(adoption_path, "historical-adoption")
            if adoption_loaded is None:
                raise RunnerError("retired historical source has no adoption origin")
            adoption_proof, adoption_result = warehouse._validate_historical_adoption_origin(
                store, state, identity["stage_name"], source_proof, source_result
            )
            try:
                source_identities_match = all(
                    _historical_resume_source_identity_equal(
                        source_result.get(key),
                        snapshot.get("result", {}).get(key),
                        store=store,
                        state=state,
                        verified_mount=verified_mount,
                    )
                    for key in ("source_identity", "source_wal_identity")
                )
            except Exception as exc:
                raise RunnerError("retired source proof device lineage is invalid") from exc
            if (
                source_result.get("source_sha256")
                != snapshot.get("result", {}).get("source_sha256")
                or source_result.get("source_wal_sha256")
                != snapshot.get("result", {}).get("source_wal_sha256")
                or not source_identities_match
                or adoption_proof.get("binding", {}).get("source_proof_sha256")
                != source_proof.get("sha256")
                or adoption_result.get("source_sha256") != source_result.get("source_sha256")
                or adoption_result.get("candidate_sha256")
                != adoption_proof.get("binding", {}).get("candidate_sha256")
            ):
                raise RunnerError("retired historical source provenance differs from its snapshot")
            if cleanup_pending:
                warehouse._validate_historical_cleanup_intent(
                    store,
                    state,
                    identity["stage_name"],
                    source_result,
                    source_proof["sha256"],
                    cleanup_path,
                )
                if not _historical_cleanup_retirement_authorized(
                    control_root,
                    identity,
                    freeze,
                    grant_id,
                    approval,
                    adoption_proof,
                    cleanup_path,
                    store=store,
                    source_snapshot=snapshot,
                    control=control,
                ):
                    raise RunnerError("historical cleanup intent has no approved retirement grant")
                warehouse._validate_historical_resume_source_presence(
                    store,
                    source_path,
                    source_path.with_name(source_path.name + ".wal"),
                    source_result,
                    adopted=True,
                    cleanup_intent=cleanup_path,
                    stage_name=identity["stage_name"],
                    state=state,
                    source_proof_sha256=source_proof["sha256"],
                    _verified_mount=verified_mount,
                )
            elif not source_path.is_file() or source_path.is_symlink():
                raise RunnerError("retired historical source has no durable cleanup intent")
        else:
            build_path = store.path(f"builds-historical/{identity['capture_id']}/build.json")
            if not build_path.is_file() or build_path.is_symlink():
                raise RunnerError("retired historical source has no installed build lineage")
            build = decode(regular_bytes(build_path, 1024**2))
            adoption = build.get("historical_adoption") if isinstance(build, dict) else None
            adoption_loaded = warehouse._load_self_proof(
                store.path(f"builds-historical/{identity['capture_id']}/historical-adoption.json"),
                "historical-adoption",
            )
            if adoption_loaded is None:
                raise RunnerError("installed historical build lost its immutable adoption proof")
            adoption_proof, adoption_result = adoption_loaded
            if (
                not isinstance(adoption, dict)
                or adoption.get("stage") != identity["stage_name"]
                or adoption.get("source_sha256") != snapshot.get("result", {}).get("source_sha256")
                or adoption.get("source_wal_sha256")
                != snapshot.get("result", {}).get("source_wal_sha256")
                or adoption.get("sha256") != adoption_proof.get("sha256")
                or adoption.get("candidate_sha256") != adoption_result.get("candidate_sha256")
                or adoption_result.get("source_sha256")
                != snapshot.get("result", {}).get("source_sha256")
            ):
                raise RunnerError(
                    "installed historical build does not bind the frozen source proof"
                )
            warehouse.certified(store, identity["capture_id"], catalogue_version=3)
    binding = snapshot.get("binding") if isinstance(snapshot, dict) else None
    result = snapshot.get("result") if isinstance(snapshot, dict) else None
    snapshot_body = {key: snapshot.get(key) for key in ("version", "phase", "binding", "result")}
    if (
        not isinstance(binding, dict)
        or not isinstance(result, dict)
        or snapshot.get("version") != 1
        or snapshot.get("phase") != "historical-source-snapshot"
        or snapshot.get("sha256") != digest(json_bytes(snapshot_body))
        or binding.get("root") != str(store.root.resolve())
        or binding.get("stage") != identity["stage_name"]
        or binding.get("capture_id") != identity["capture_id"]
        or binding.get("capture_state_sha256") != state_sha
        or binding.get("scope_sha256") != scope_sha
        or binding.get("code_sha256") != code_sha
        or state_sha != freeze["state_sha256"]
        or result.get("source_sha256") != freeze["source_sha256"]
        or result.get("receipt_evidence_sha256") != freeze["receipt_evidence_sha256"]
        or binding.get("scope_sha256") != freeze["scope_sha256"]
        or binding.get("code_sha256") != freeze["code_sha256"]
        or snapshot.get("sha256") != freeze["source_snapshot_sha256"]
    ):
        raise RunnerError("current historical capture/source no longer matches the frozen controls")
    return snapshot


def _historical_resource_baseline(
    directory: Path, store: Any, report: dict[str, Any]
) -> dict[str, int]:
    storage = store.usage()
    retained = storage.get("retained_bytes")
    temporary = storage.get("temporary_bytes")
    database = storage.get("database_bytes")
    if any(type(value) is not int or value < 0 for value in (retained, temporary, database)):
        raise RunnerError("historical measured storage inventory is incomplete")

    capture_id = report.get("capture_id")
    session_id = report.get("session_id")
    root_id = report.get("root_id")
    if (
        not isinstance(capture_id, str)
        or CAPTURE_ID.fullmatch(capture_id) is None
        or not isinstance(session_id, str)
        or CAPTURE_ID.fullmatch(session_id) is None
        or not isinstance(root_id, str)
        or CAPTURE_ID.fullmatch(root_id) is None
    ):
        raise RunnerError("historical inventory has no exact session, capture or root identity")
    session_report = directory / "runs" / session_id / REPORT_NAME
    _live_external_paths({"session_report": session_report}, directory)
    workspace = _live_workspace_snapshot(store.root, directory, capture_id, root_id)
    count = [workspace["inventory_entries"]]
    inventory_limit = int(_effective_limits()["storage_inventory_entries"])
    if type(count[0]) is not int or count[0] < 0 or count[0] > inventory_limit:
        raise RunnerError("historical mutable workspace exceeded its entry bound")
    device = directory.stat(follow_symlinks=False).st_dev
    runs_path = directory / "runs"
    if runs_path.exists() or runs_path.is_symlink():
        _bounded_tree_bytes(runs_path, device, count, set())
    pending = [directory]
    while pending:
        parent = pending.pop()
        try:
            with os.scandir(parent) as children:
                for child in children:
                    store.check_deadline()
                    item = child.stat(follow_symlinks=False)
                    if item.st_dev != device or stat.S_ISLNK(item.st_mode):
                        raise RunnerError("historical resource inventory contains an unsafe entry")
                    if stat.S_ISDIR(item.st_mode):
                        pending.append(Path(child.path))
                    elif not stat.S_ISREG(item.st_mode):
                        raise RunnerError("historical resource inventory contains a special file")
                    if parent == directory and child.name != "runs":
                        count[0] += 1
                        if count[0] > inventory_limit:
                            raise RunnerError(
                                "historical mutable workspace exceeded its entry bound"
                            )
        except OSError as exc:
            raise RunnerError("historical resource inventory could not be read safely") from exc
    external = _external_bytes(directory)
    peaks = [
        report.get("peak_child_process_group_rss_bytes", 0),
        report.get("peak_child_process_group_physical_footprint_bytes", 0),
        report.get("invocation_peak_child_process_group_rss_bytes", 0),
        report.get("invocation_peak_child_process_group_physical_footprint_bytes", 0),
    ]
    if any(type(value) is not int or value < 0 for value in peaks):
        raise RunnerError("historical measured RSS inventory is invalid")
    return {
        "database_bytes": database,
        "temporary_bytes": temporary,
        "root_bytes": retained,
        "aggregate_bytes": retained + external,
        "free_bytes": shutil.disk_usage(directory).free,
        "inventory_entries": count[0],
        "peak_rss_bytes": max(peaks),
    }


def _historical_measurement_artifact(
    control_root: Path,
    grant_id: str,
    approval: dict[str, Any],
    identity: dict[str, Any],
    freeze: dict[str, Any],
    *,
    session_report_sha256: str,
    session_root_device: int,
    baseline: dict[str, int] | None,
    require_fresh: bool,
    allow_initial_maintenance_overage: bool = False,
) -> dict[str, Any]:
    import historical_grants as grants

    path = control_root / "grants" / grant_id / "measurements.json"
    raw = grants._private_regular_file(path, max_bytes=1024**2)
    try:
        value = json.loads(raw.decode("utf-8"), parse_constant=grants._reject_constant)
    except (UnicodeDecodeError, ValueError) as exc:
        raise RunnerError("historical measurement artifact is malformed") from exc
    if not isinstance(value, dict) or raw != grants.canonical_bytes(value) + b"\n":
        raise RunnerError("historical measurement artifact is not canonical JSON")
    fields = {
        "version",
        "grant_id",
        "phases",
        "identity",
        "source_snapshot_sha256",
        "receipt_evidence_sha256",
        "scope_sha256",
        "code_sha256",
        "root_volume_uuid",
        "root_device",
        "session_report_sha256",
        "measured_at_epoch",
        "forecast_seconds",
        "final_verification_seconds",
        "projected_database_bytes",
        "projected_temporary_bytes",
        "projected_root_bytes",
        "projected_aggregate_bytes",
        "projected_free_bytes",
        "inventory_entries",
        "projected_peak_rss_bytes",
        "baseline",
    }
    if set(value) != fields or value.get("version") != 1:
        raise RunnerError("historical measurement artifact has an unsupported schema")
    summary = approval.get("measurements")
    if not isinstance(summary, dict):
        raise RunnerError("historical approval has no measured inventory summary")
    actual_sha256 = grants.sha256_bytes(raw)
    approved_summary = {
        key: summary.get(key)
        for key in (
            "session_report_sha256",
            "measured_at_epoch",
            "forecast_seconds",
            "final_verification_seconds",
            "projected_database_bytes",
            "projected_temporary_bytes",
            "projected_root_bytes",
            "projected_aggregate_bytes",
            "projected_free_bytes",
            "inventory_entries",
            "projected_peak_rss_bytes",
        )
    }
    artifact_summary = {key: value.get(key) for key in approved_summary}
    if (
        summary.get("report_sha256") != actual_sha256
        or artifact_summary != approved_summary
        or value.get("grant_id") != grant_id
        or value.get("phases") != approval.get("phases")
        or value.get("identity") != identity
        or value.get("source_snapshot_sha256") != freeze["source_snapshot_sha256"]
        or value.get("receipt_evidence_sha256") != freeze["receipt_evidence_sha256"]
        or value.get("scope_sha256") != freeze["scope_sha256"]
        or value.get("code_sha256") != freeze["code_sha256"]
        or value.get("root_volume_uuid") != freeze["root_volume_uuid"]
        or type(session_root_device) is not int
        or value.get("root_device") != session_root_device
        or value.get("session_report_sha256") != session_report_sha256
    ):
        raise RunnerError("historical measured resource artifact does not match its approval")
    measured_at = value.get("measured_at_epoch")
    if type(measured_at) not in {int, float} or not math.isfinite(measured_at) or measured_at <= 0:
        raise RunnerError("historical measurement timestamp is invalid")
    if require_fresh and (
        measured_at > time.time()
        or time.time() - measured_at > 15 * 60
        or approval.get("approved_at_epoch", 0) < measured_at
        or approval.get("approved_at_epoch", 0) - measured_at > 15 * 60
    ):
        raise RunnerError("historical measurement baseline is stale at launch")
    baseline_fields = {
        "database_bytes",
        "temporary_bytes",
        "root_bytes",
        "aggregate_bytes",
        "free_bytes",
        "inventory_entries",
        "peak_rss_bytes",
    }
    recorded_baseline = value.get("baseline")
    if (
        not isinstance(recorded_baseline, dict)
        or set(recorded_baseline) != baseline_fields
        or any(type(item) is not int or item < 0 for item in recorded_baseline.values())
    ):
        raise RunnerError("historical measurement baseline is invalid")
    if baseline is not None and (
        not isinstance(baseline, dict)
        or set(baseline) != baseline_fields
        or any(type(item) is not int or item < 0 for item in baseline.values())
    ):
        raise RunnerError("current historical measurement baseline is invalid")
    if (
        require_fresh
        and baseline is not None
        and any(recorded_baseline[key] != baseline[key] for key in baseline_fields - {"free_bytes"})
    ):
        raise RunnerError("current measured resource inventory differs from approved baseline")
    projected = {
        "database_bytes": value["projected_database_bytes"],
        "temporary_bytes": value["projected_temporary_bytes"],
        "root_bytes": value["projected_root_bytes"],
        "aggregate_bytes": value["projected_aggregate_bytes"],
        "free_bytes": value["projected_free_bytes"],
        "inventory_entries": value["inventory_entries"],
        "peak_rss_bytes": value["projected_peak_rss_bytes"],
    }
    admission = grants.expected_resource_admission()
    temporary_admission = admission["storage_temporary_admission_bytes"]
    source_start_grant = (
        isinstance(approval.get("phases"), list)
        and bool(approval["phases"])
        and approval["phases"][0] == "source"
    )
    maintenance_exception = allow_initial_maintenance_overage and source_start_grant
    recorded_temporary_overage = recorded_baseline["temporary_bytes"] > projected["temporary_bytes"]
    current_temporary_overage = (
        baseline is not None and baseline["temporary_bytes"] > projected["temporary_bytes"]
    )
    if (
        recorded_baseline["temporary_bytes"] > admission["temporary_bytes"]
        or projected["temporary_bytes"] > temporary_admission
        or recorded_temporary_overage
        and not source_start_grant
        or current_temporary_overage
        and not maintenance_exception
        or current_temporary_overage
        and baseline["temporary_bytes"] > recorded_baseline["temporary_bytes"]
    ):
        raise RunnerError("historical temporary storage exceeds its approved maintenance allowance")
    if type(projected["free_bytes"]) is not int or projected["free_bytes"] < 0:
        raise RunnerError("historical free-space projection is invalid")
    if (
        projected["database_bytes"] < recorded_baseline["database_bytes"]
        or projected["root_bytes"] < recorded_baseline["root_bytes"]
        or projected["aggregate_bytes"] < recorded_baseline["aggregate_bytes"]
        or projected["free_bytes"] > recorded_baseline["free_bytes"]
        or projected["inventory_entries"] < recorded_baseline["inventory_entries"]
        or projected["peak_rss_bytes"] < recorded_baseline["peak_rss_bytes"]
        or projected["database_bytes"] > admission["storage_database_admission_bytes"]
        or projected["temporary_bytes"] > temporary_admission
        or projected["root_bytes"] > admission["root_bytes"]
        or projected["aggregate_bytes"] > admission["aggregate_bytes"]
        or projected["free_bytes"] < admission["minimum_free_bytes"]
        or projected["inventory_entries"] > admission["storage_inventory_entries"]
        or projected["peak_rss_bytes"] > admission["peak_rss_bytes"]
    ):
        raise RunnerError("historical resource projection exceeds measured fixed admission")
    if require_fresh and baseline is not None:
        reserved_growth = recorded_baseline["free_bytes"] - projected["free_bytes"]
        minimum_fresh_free = admission["minimum_free_bytes"] + reserved_growth
        if baseline["free_bytes"] < minimum_fresh_free:
            raise RunnerError("current free space no longer covers its approved growth reserve")
    if baseline is not None and (
        any(
            baseline[key] > projected[key]
            for key in (
                "database_bytes",
                "root_bytes",
                "aggregate_bytes",
                "inventory_entries",
                "peak_rss_bytes",
            )
        )
        or baseline["temporary_bytes"] > projected["temporary_bytes"]
        and not maintenance_exception
        or baseline["temporary_bytes"] > admission["temporary_bytes"]
        or baseline["free_bytes"] < admission["minimum_free_bytes"]
    ):
        raise RunnerError("current historical resource inventory exceeds approved projections")
    return value


def _historical_authority(
    directory: Path,
    control_root: Path,
    grant_id: str,
    *,
    launch_sha256: str | None,
    require_worker: bool,
    prelaunch: bool = False,
    cheap_only: bool = False,
    expected_phase: str | None = None,
    expected_substep: str | None = None,
    storage_invocation_token: str | None = None,
    storage_progress_path: Path | None = None,
    supervisor_pid: int | None = None,
) -> dict[str, Any]:
    import historical_grants as grants

    directory = _external_path(directory)
    control_root = _historical_control_root(control_root)
    bundle = grants.read_control_bundle(control_root, grant_id, prelaunch=prelaunch)
    identity = bundle["identity"]
    session_id = identity["session_id"]
    report_path = directory / "runs" / session_id / REPORT_NAME
    profile, report = _session_resource_profile(
        report_path, expected_profile="expanded", capture_id=identity["capture_id"]
    )
    if (
        report.get("session_id") != session_id
        or report.get("root_id") != identity["root_id"]
        or report.get("capture_id") != identity["capture_id"]
        or report.get("mode") not in {"full", "resume", "historical"}
        or _owner_root_id(directory) != identity["root_id"]
        or report.get("boot_stamp") != _boot_stamp()
    ):
        raise RunnerError("private historical worker session or boot binding changed")
    operator = directory / "operator"
    current_mount = _historical_current_mount(
        directory,
        operator,
        control_root,
        report,
        identity,
        bundle["freeze"],
    )
    stopped_reconciliation = report.get("stopped_worker_reconciliation")
    if (
        prelaunch
        and bundle["approval"].get("phase") == "source"
        and isinstance(stopped_reconciliation, dict)
        and stopped_reconciliation.get("recovery_kind") == "prior_boot_temporary_stop"
    ):
        _historical_reconciliation_audit(
            {
                "report": report,
                "identity": identity,
                "operator": operator,
                "freeze": bundle["freeze"],
                "current_mount": current_mount,
            }
        )
    root_device = current_mount["device"]
    root_volume = current_mount["volume_uuid"]
    boot_stamp = report.get("boot_stamp")
    if prelaunch:
        approval = bundle["approval"]
        latch = bundle["latch"]
        launch = None
        grant = {"phase": approval.get("phase"), "phases": approval.get("phases")}
        session_binding = None
    else:
        approval, latch, launch = bundle["approval"], bundle["latch"], bundle["launch"]
        grant = grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=time.time(),
            now_monotonic=time.monotonic(),
            boot_stamp=boot_stamp,
            dbt_memory_limit="4GB",
            dbt_threads=4,
        )
        if (
            launch_sha256 is not None
            and grants.sha256_bytes(
                grants._private_regular_file(control_root / "grants" / grant_id / "launch.json")
            )
            != launch_sha256
        ):
            raise RunnerError("private historical launch artifact changed")
        session_binding = grants.validate_session_binding(
            bundle["session_binding"],
            approval=approval,
            expected_identity=identity,
        )
        if grants.sha256_json(session_binding) != launch.get("session_binding_sha256"):
            raise RunnerError("private launch does not bind its session snapshot")
    if not cheap_only:
        operational_source_sha256 = _historical_operational_source_sha256(control_root)
        runtime_sha256 = grants.sha256_json(
            {
                "runtime_manifest_sha256": bundle["freeze"]["runtime_manifest_sha256"],
                "operational_source_sha256": operational_source_sha256,
            }
        )
        if (
            operational_source_sha256 != bundle["freeze"]["operational_source_sha256"]
            or runtime_sha256 != identity["runtime_sha256"]
        ):
            raise RunnerError(
                "operational runner or control source differs from the frozen runtime"
            )
        final_phase = expected_phase in {"independent-verification", "activation"}
        _historical_verify_runtime(
            control_root,
            bundle["freeze"],
            deadline_epoch=float(
                grant["deadline_epoch"] if final_phase else grant["workflow_deadline_epoch"]
            ),
            deadline_monotonic=float(
                grant["deadline_monotonic"] if final_phase else grant["workflow_deadline_monotonic"]
            ),
        )
        if require_worker:
            _historical_assert_pinned_python(control_root)
    if not prelaunch and not require_worker:
        _historical_assert_pinned_python(control_root)
    if (
        not isinstance(grant.get("phases"), list)
        or not grant["phases"]
        or expected_phase is not None
        and expected_phase not in grant["phases"]
    ):
        raise RunnerError("private historical phase is outside the approved grant range")
    next_durable_phase = grants.next_phase(bundle.get("current_phase"))
    if expected_phase is not None and expected_phase != next_durable_phase:
        raise RunnerError("private historical worker phase is not the next durable checkpoint")
    if prelaunch and approval.get("phase") != next_durable_phase:
        raise RunnerError("historical grant range does not start at the next durable checkpoint")
    report_raw_hash = grants.sha256_bytes(_historical_session_report_bytes(report_path))
    if not prelaunch and (
        session_binding.get("report_path") != str(report_path)
        or session_binding.get("boot_stamp") != boot_stamp
        or session_binding.get("root_volume_uuid") != root_volume
        or session_binding.get("root_device") != root_device
        or approval["measurements"]["session_report_sha256"] != session_binding["report_sha256"]
    ):
        raise RunnerError("private grant session binding changed")
    if require_worker:
        running = report.get("private_historical_grant")
        if (
            report.get("active_process_group") != os.getpid()
            or report.get("phase") != f"historical-{expected_phase}-{expected_substep}"
            or not isinstance(running, dict)
            or running.get("state") != "supervised-child-pending"
            or running.get("grant_id") != grant_id
            or running.get("launch_sha256") != launch_sha256
            or running.get("launch_id") != launch["launch_id"]
            or running.get("control_root") != str(control_root)
            or running.get("phase") != expected_phase
            or running.get("substep") != expected_substep
            or running.get("worker_pid") != os.getpid()
            or running.get("worker_pgid") != os.getpid()
            or not isinstance(storage_invocation_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", storage_invocation_token) is None
            or running.get("storage_invocation_token") != storage_invocation_token
            or type(supervisor_pid) is not int
            or running.get("controller_pid") != supervisor_pid
            or report.get("storage_supervision_invocation") != storage_invocation_token
            or not isinstance(storage_progress_path, Path)
            or running.get("storage_progress_path") != str(storage_progress_path)
            or report.get("storage_supervision_file") != str(storage_progress_path)
        ):
            raise RunnerError("historical worker is not the supervised child bound to this grant")
        _historical_validate_storage_handoff(
            directory,
            report_path,
            storage_progress_path,
            invocation_token=storage_invocation_token,
        )
        if report_raw_hash == session_binding["report_sha256"]:
            raise RunnerError("supervised session report did not record the child launch")
    elif not prelaunch:
        if report.get("phase") != "stopped" or report.get("active_process_group") is not None:
            raise RunnerError("private historical grant requires the exact stopped session")
        handoff = report.get("private_historical_grant")
        if (
            report_raw_hash == session_binding["report_sha256"]
            or not isinstance(handoff, dict)
            or handoff.get("grant_id") != grant_id
            or handoff.get("launch_sha256") != launch_sha256
        ):
            raise RunnerError("private grant session report lost its launch handoff")
    elif report.get("phase") != "stopped" or report.get("active_process_group") is not None:
        raise RunnerError("private historical grant requires the exact stopped session")
    source_snapshot = None
    baseline = None
    store = None
    if not cheap_only:
        store = _historical_store(operator, max_duration_seconds=MAX_OFFLINE_HISTORICAL_SECONDS)
        if _root_id(store) != identity["root_id"]:
            raise RunnerError("private historical operator root identity changed")
        store_device = _historical_path_device(store.root)
        if store_device != current_mount["device"]:
            raise RunnerError("private historical operator mount changed during authority setup")
        verified_mount = {
            "root_path": str(store.root.resolve()),
            "root_id": identity["root_id"],
            "capture_id": identity["capture_id"],
            "current_device": store_device,
            "device_lineage": current_mount["device_lineage"],
        }
        source_snapshot = _historical_source_snapshot(
            store,
            control_root,
            identity,
            bundle["freeze"],
            grant_id=grant_id,
            approval=approval,
            control=bundle["control"],
            verified_mount=verified_mount,
        )
        baseline = _historical_resource_baseline(directory, store, report)
    else:
        verified_mount = None
    measurement = _historical_measurement_artifact(
        control_root,
        grant_id,
        approval,
        identity,
        bundle["freeze"],
        session_report_sha256=(
            session_binding["report_sha256"] if session_binding is not None else report_raw_hash
        ),
        session_root_device=(
            session_binding["root_device"]
            if session_binding is not None
            else report["system"]["filesystem_device"]
        ),
        baseline=baseline,
        require_fresh=prelaunch,
        allow_initial_maintenance_overage=(
            prelaunch
            and approval.get("phases", [None])[0] == "source"
            or require_worker
            and expected_phase == "source"
            and expected_substep == "maintenance"
        ),
    )
    return {
        "directory": directory,
        "operator": operator,
        "control_root": control_root,
        "identity": identity,
        "control": bundle["control"],
        "freeze": bundle["freeze"],
        "freeze_sha256": bundle["freeze_sha256"],
        "approval": approval,
        "latch": latch,
        "launch": launch,
        "launch_sha256": launch_sha256,
        "grant": grant,
        "session_binding": session_binding,
        "report_path": report_path,
        "report": report,
        "supervised_report_phase": report["phase"] if require_worker else None,
        "profile": profile,
        "store": store,
        "current_mount": current_mount,
        "verified_mount": verified_mount,
        "source_snapshot": source_snapshot,
        "baseline": baseline,
        "measurement": measurement,
    }


def _historical_phase_grant(
    authority: dict[str, Any], grant_id: str, expected_phase: str
) -> dict[str, Any]:
    import historical_grants as grants

    approval, latch, launch = grants.read_grant_bundle(authority["control_root"], grant_id)
    if expected_phase not in approval.get("phases", []):
        raise RunnerError("historical phase seal references a different grant phase")
    grant = grants.validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=authority["identity"],
        now_epoch=float(launch.get("started_epoch", 0)) + 0.01,
        now_monotonic=float(launch.get("started_monotonic", 0)) + 0.01,
        boot_stamp=launch.get("boot_stamp"),
        dbt_memory_limit="4GB",
        dbt_threads=4,
    )
    return {"approval": approval, "latch": latch, "launch": launch, "grant": grant}


def _historical_read_artifact(path: Path, max_bytes: int = MIB) -> tuple[dict[str, Any], str]:
    from kalshi_events.common import decode, digest, regular_bytes

    raw = regular_bytes(path, max_bytes)
    value = decode(raw)
    if not isinstance(value, dict):
        raise RunnerError(f"historical artifact is not a JSON object: {path.name}")
    return value, digest(raw)


def _historical_published_release(
    authority: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    from kalshi_events import publish

    store = authority["store"]
    current = publish.current(store, catalogue_version=3)
    release_id = current.get("source_release_id")
    if (
        current.get("capture_id") != authority["identity"]["capture_id"]
        or not isinstance(release_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", release_id) is None
    ):
        raise RunnerError("historical publication does not match the frozen capture")
    release = store.path(f"releases/{release_id}")
    manifest_path = release / "manifest.json"
    manifest, manifest_sha = _historical_read_artifact(manifest_path)
    pointer, pointer_sha = _historical_read_artifact(store.path("current-historical.json"), MIB)
    if (
        manifest.get("source_release_id") != release_id
        or manifest.get("capture_id") != authority["identity"]["capture_id"]
        or manifest.get("contract") != "oddsfox.kalshi.historical.v1"
        or manifest.get("historical_export_policy") != "historical-arrow-external-merge-v1"
        or pointer.get("source_release_id") != release_id
        or pointer.get("manifest_sha256") != manifest_sha
    ):
        raise RunnerError("historical publication manifest does not match its current pointer")
    return {**current, "manifest_sha256": manifest_sha, "pointer_sha256": pointer_sha}, manifest


def _historical_release_verification(
    authority: dict[str, Any], *, grant_id_for_receipt: str | None
) -> dict[str, Any]:
    import historical_grants as grants

    current, manifest = _historical_published_release(authority)
    paths = _prepare_run_paths(authority["directory"], authority["report_path"])
    standalone = _release_verify(
        Path(str(current["output"])),
        authority["directory"],
        authority["operator"],
        paths,
        authority["report"],
        float(authority["grant"]["deadline_monotonic"]),
        storage_supervision=False,
        wall_deadline_epoch_override=float(authority["grant"]["deadline_epoch"]),
        preserve_session=True,
    )
    source_capture_complete = standalone.get("source_capture_complete")
    scope_complete = (
        standalone == manifest
        and manifest.get("coverage_scope") == "historical"
        and manifest.get("historical_contract") == "oddsfox.kalshi.historical.v1"
    )
    if (
        standalone.get("source_release_id") != current["source_release_id"]
        or standalone.get("coverage_scope") != "historical"
        or standalone.get("capture_id") != authority["identity"]["capture_id"]
        or type(source_capture_complete) is not bool
        or source_capture_complete is not manifest.get("source_capture_complete")
        or not scope_complete
    ):
        raise RunnerError("independent historical publication verification failed")
    receipt = {
        "version": 1,
        "capture_id": authority["identity"]["capture_id"],
        "release_id": current["source_release_id"],
        "release_manifest_sha256": current["manifest_sha256"],
        "coverage_scope": standalone["coverage_scope"],
        "source_capture_complete": source_capture_complete,
        "scope_complete": scope_complete,
        "verifier": "production_run._release_verify",
    }
    receipt_sha = grants.sha256_json(receipt)
    if grant_id_for_receipt is not None:
        receipt_path = authority["control_root"] / "verifications" / f"{grant_id_for_receipt}.json"
        if receipt_path.exists() or receipt_path.is_symlink():
            saved, _ = _historical_read_artifact(receipt_path)
            if saved != receipt:
                raise RunnerError("saved independent verification receipt changed")
        else:
            grants.write_once_json(receipt_path, receipt)
    return {
        "verification_receipt_sha256": receipt_sha,
        "release_id": current["source_release_id"],
        "release_manifest_sha256": current["manifest_sha256"],
        "receipt": receipt,
        "manifest": manifest,
        "standalone": standalone,
    }


def _historical_readiness_phase_origin(authority: dict[str, Any], grant_id: str) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import historical_resume, warehouse
    from kalshi_events.common import digest, json_bytes

    identity = authority["identity"]
    store = authority["store"]
    source_snapshot = authority["source_snapshot"]
    path = authority["control_root"] / "grants" / grant_id / "readiness-phase-origin.json"
    receipt, _ = _historical_read_artifact(path, 16 * MIB)
    body = {key: value for key, value in receipt.items() if key != "sha256"}
    required = {
        "version",
        "identity",
        "grant_id",
        "source_snapshot",
        "source_proof",
        "readiness_proof",
        "landing_proof",
        "verification",
    }
    if (
        set(body) != required
        or body.get("version") != 1
        or body.get("identity") != identity
        or body.get("grant_id") != grant_id
        or receipt.get("sha256") != grants.sha256_json(body)
        or body.get("source_snapshot") != source_snapshot
    ):
        raise RunnerError("original readiness verification receipt is invalid or unbound")

    snapshot = body["source_snapshot"]
    state_sha = identity["state_sha256"]
    state = warehouse.read_capture(store, identity["capture_id"])
    root = str(store.root.resolve())
    source_binding = {
        "root": root,
        "stage": identity["stage_name"],
        "capture_id": identity["capture_id"],
        "capture_state_sha256": state_sha,
        "scope_sha256": snapshot.get("binding", {}).get("scope_sha256"),
        "code_sha256": snapshot.get("binding", {}).get("code_sha256"),
    }
    source_snapshot_body = {
        key: snapshot.get(key) for key in ("version", "phase", "binding", "result")
    }
    source_result = snapshot.get("result")
    if (
        snapshot.get("version") != 1
        or snapshot.get("phase") != "historical-source-snapshot"
        or snapshot.get("binding") != source_binding
        or snapshot.get("sha256") != digest(json_bytes(source_snapshot_body))
        or snapshot.get("sha256") != authority["freeze"]["source_snapshot_sha256"]
        or not isinstance(source_result, dict)
        or source_result.get("source_sha256") != authority["freeze"]["source_sha256"]
        or source_result.get("receipt_evidence_sha256")
        != authority["freeze"]["receipt_evidence_sha256"]
        or snapshot["binding"].get("scope_sha256") != authority["freeze"]["scope_sha256"]
        or snapshot["binding"].get("code_sha256") != authority["freeze"]["code_sha256"]
    ):
        raise RunnerError("readiness receipt source snapshot does not match frozen evidence")

    source_proof = body["source_proof"]
    source_proof_binding = {
        "root": root,
        "capture_id": identity["capture_id"],
        "state_sha256": state_sha,
        "stage": identity["stage_name"],
    }
    try:
        source_proof_result = historical_resume.validate_proof(
            source_proof, "resume-source", source_proof_binding
        )
    except Exception as exc:
        raise RunnerError("readiness source-copy proof is invalid") from exc
    try:
        source_identities_match = all(
            _historical_resume_source_identity_equal(
                source_proof_result.get(identity_key),
                source_result.get(identity_key),
                store=store,
                state=state,
                verified_mount=authority.get("verified_mount"),
            )
            for identity_key in ("source_identity", "source_wal_identity")
        )
    except Exception as exc:
        raise RunnerError("readiness source-copy device lineage is invalid") from exc
    if (
        source_proof_result.get("source_sha256") != source_result.get("source_sha256")
        or source_proof_result.get("source_wal_sha256") != source_result.get("source_wal_sha256")
        or not source_identities_match
    ):
        raise RunnerError("readiness source-copy proof differs from its frozen source")

    readiness = body["readiness_proof"]
    readiness_binding = readiness.get("binding") if isinstance(readiness, dict) else None
    if not isinstance(readiness_binding, dict):
        raise RunnerError("original readiness proof is missing its binding")
    expected_readiness_binding = {
        "root": root,
        "stage": identity["stage_name"],
        "capture_id": identity["capture_id"],
        "state_sha256": state_sha,
        "source_proof_sha256": source_proof["sha256"],
        "landing_proof_sha256": readiness_binding.get("landing_proof_sha256"),
        "candidate_sha256": readiness_binding.get("candidate_sha256"),
        "scope_sha256": snapshot["binding"]["scope_sha256"],
        "schema_sha256": readiness_binding.get("schema_sha256"),
        "code_sha256": snapshot["binding"]["code_sha256"],
        "adoption_origin_sha256": None,
    }
    try:
        readiness_result = historical_resume.validate_proof(
            readiness, "historical-readiness", expected_readiness_binding
        )
    except Exception as exc:
        raise RunnerError("original readiness proof is invalid or was rewritten") from exc

    landing = body["landing_proof"]
    landing_binding = landing.get("binding") if isinstance(landing, dict) else None
    if not isinstance(landing_binding, dict):
        raise RunnerError("original readiness landing proof is missing its binding")
    try:
        landing_result = historical_resume.validate_proof(
            landing, "historical-landing", landing_binding
        )
    except Exception as exc:
        raise RunnerError("original readiness landing proof is invalid") from exc
    if (
        landing.get("sha256") != readiness_binding["landing_proof_sha256"]
        or landing_binding.get("root") != root
        or landing_binding.get("stage") != identity["stage_name"]
        or landing_binding.get("capture_id") != identity["capture_id"]
        or landing_binding.get("state_sha256") != state_sha
        or landing_binding.get("source_proof_sha256") != source_proof["sha256"]
        or landing_binding.get("scope_sha256") != snapshot["binding"]["scope_sha256"]
        or landing_binding.get("code_sha256") != snapshot["binding"]["code_sha256"]
        or landing_binding.get("candidate_sha256") != readiness_binding["candidate_sha256"]
        or landing_result.get("observation_count") != readiness_result.get("observation_count")
        or landing_result.get("membership_count") != readiness_result.get("membership_count")
        or landing_result.get("eligibility_count") != readiness_result.get("eligibility_count")
    ):
        raise RunnerError("original readiness landing proof differs from the frozen candidate")

    verification = body["verification"]
    verification_binding = verification.get("binding") if isinstance(verification, dict) else None
    if not isinstance(verification_binding, dict):
        raise RunnerError("original readiness verification is missing its binding")
    try:
        verification_result = historical_resume.validate_proof(
            verification, "historical-readiness-verification", verification_binding
        )
    except Exception as exc:
        raise RunnerError("original readiness verification receipt is invalid") from exc
    expected_counts = {
        "observation_count": readiness_result.get("observation_count"),
        "membership_count": readiness_result.get("membership_count"),
        "eligibility_count": readiness_result.get("eligibility_count"),
    }
    if (
        verification_binding.get("root") != root
        or verification_binding.get("stage") != identity["stage_name"]
        or verification_binding.get("capture_id") != identity["capture_id"]
        or verification_binding.get("state_sha256") != state_sha
        or verification_binding.get("readiness_sha256") != readiness["sha256"]
        or verification_binding.get("candidate_sha256") != readiness_binding["candidate_sha256"]
        or verification_binding.get("evidence_sha256")
        != source_result.get("receipt_evidence_sha256")
        or verification_binding.get("code_sha256") != snapshot["binding"]["code_sha256"]
        or verification_binding.get("adoption_origin_sha256") is not None
        or verification_result != expected_counts
        or expected_counts["observation_count"]
        != authority["control"]["authorized_census"]["exact_observations"]
    ):
        raise RunnerError("original readiness verification differs from frozen source or census")
    return {
        "readiness_sha256": readiness["sha256"],
        "verification_sha256": verification["sha256"],
        "capture_state_sha256": state_sha,
        "receipt_evidence_sha256": source_result["receipt_evidence_sha256"],
        "source_sha256": source_result["source_sha256"],
        "source_wal_sha256": source_result["source_wal_sha256"],
        "candidate_sha256": readiness_binding["candidate_sha256"],
        **expected_counts,
        "scope_sha256": snapshot["binding"]["scope_sha256"],
        "code_sha256": snapshot["binding"]["code_sha256"],
    }


def _historical_source_retirement_receipt(
    authority: dict[str, Any], grant_id: str, phase: str
) -> tuple[dict[str, Any], str]:
    import historical_grants as grants

    from kalshi_events import historical_resume, warehouse

    if phase not in {"adoption", "models"}:
        raise RunnerError("source-retirement receipt has an unsupported phase")
    path = authority["control_root"] / "grants" / grant_id / "source-retirement.json"
    receipt, raw_sha = _historical_read_artifact(path, 4 * MIB)
    body = {key: value for key, value in receipt.items() if key != "sha256"}
    linked = _historical_phase_grant(authority, grant_id, phase)
    approval = linked["approval"]
    expected = {
        "version": 1,
        "identity": authority["identity"],
        "grant_id": grant_id,
        "phase": phase,
        "launch_sha256": grants.sha256_bytes(
            grants._private_regular_file(
                authority["control_root"] / "grants" / grant_id / "launch.json"
            )
        ),
        "retirement_policy": "coexistence-capacity-relief-approved-v1",
        "adoption_sha256": body.get("adoption_sha256"),
        "verification_sha256": body.get("verification_sha256"),
        "cleanup_intent_sha256": body.get("cleanup_intent_sha256"),
        "source_sha256": authority["source_snapshot"]["result"]["source_sha256"],
        "source_wal_sha256": authority["source_snapshot"]["result"]["source_wal_sha256"],
    }
    verification_name = (
        "adoption-verification.json" if phase == "adoption" else "models-adoption-verification.json"
    )
    verification_path = authority["control_root"] / "grants" / grant_id / verification_name
    verification, _ = _historical_read_artifact(verification_path, 4 * MIB)
    verification_body = {
        key: verification.get(key) for key in ("version", "phase", "binding", "result")
    }
    verification_binding = verification.get("binding")
    try:
        verification_result = historical_resume.validate_proof(
            verification,
            "historical-readiness-verification",
            verification_binding,
        )
    except Exception as exc:
        raise RunnerError("source-retirement verification receipt is invalid") from exc
    stage_relative = historical_resume.stage_relative(authority["identity"]["stage_name"])
    stage_path = authority["store"].path(stage_relative)
    if stage_path.is_dir() and not stage_path.is_symlink():
        adoption_proof_path = stage_path / "resume-adoption.json"
    else:
        adoption_proof_path = authority["store"].path(
            f"builds-historical/{authority['identity']['capture_id']}/historical-adoption.json"
        )
    adoption_loaded = warehouse._load_self_proof(adoption_proof_path, "historical-adoption")
    if adoption_loaded is None:
        raise RunnerError("source-retirement receipt lost the immutable adoption proof")
    adoption_proof, adoption_result = adoption_loaded
    adoption_binding = adoption_proof.get("binding", {})
    original_verification = (
        verification.get("sha256") == adoption_binding.get("verification_sha256")
        and verification_binding.get("readiness_sha256") == adoption_binding.get("readiness_sha256")
        and verification_binding.get("candidate_sha256") == adoption_binding.get("candidate_sha256")
        and verification_result
        == {
            key: adoption_result.get(key)
            for key in ("observation_count", "membership_count", "eligibility_count")
        }
    )
    verified_adoption_origin = verification_binding.get("adoption_origin_sha256") == body.get(
        "adoption_sha256"
    )
    if (
        adoption_proof.get("sha256") != body.get("adoption_sha256")
        or adoption_binding.get("root") != str(authority["store"].root.resolve())
        or adoption_binding.get("stage") != authority["identity"]["stage_name"]
        or adoption_binding.get("capture_id") != authority["identity"]["capture_id"]
        or adoption_binding.get("state_sha256") != authority["identity"]["state_sha256"]
        or (phase == "models" and not verified_adoption_origin)
        or (
            phase == "adoption"
            and not verified_adoption_origin
            and not (
                verification_binding.get("adoption_origin_sha256") is None and original_verification
            )
        )
    ):
        raise RunnerError("retirement verification does not bind the immutable adoption lineage")
    cleanup_sha = body.get("cleanup_intent_sha256")
    origin_path = authority["control_root"] / "source" / "adoption-origin.json"
    adoption_origin, _ = _historical_read_artifact(origin_path, 4 * MIB)
    adoption_origin_body = {key: value for key, value in adoption_origin.items() if key != "sha256"}
    origin_claims = _historical_adoption_origin_claims(
        authority, adoption_origin_body.get("verification_sha256")
    )
    origin_claim = next(
        (
            claim
            for claim in origin_claims
            if claim.get("grant_id") == adoption_origin_body.get("origin_grant_id")
        ),
        None,
    )
    if (
        set(body)
        != {
            "version",
            "identity",
            "grant_id",
            "phase",
            "launch_sha256",
            "retirement_policy",
            "adoption_sha256",
            "verification_sha256",
            "cleanup_intent_sha256",
            "source_sha256",
            "source_wal_sha256",
        }
        or body.get("version") != 1
        or body.get("identity") != authority["identity"]
        or body.get("grant_id") != grant_id
        or body.get("phase") != phase
        or approval.get("retire_source") is not True
        or approval.get("retirement_policy") != expected["retirement_policy"]
        or body != expected
        or receipt.get("sha256") != grants.sha256_json(body)
        or verification.get("phase") != "historical-readiness-verification"
        or verification.get("sha256") != body.get("verification_sha256")
        or not isinstance(verification_binding, dict)
        or verification_binding.get("root") != str(authority["store"].root.resolve())
        or verification_binding.get("capture_id") != authority["identity"]["capture_id"]
        or verification_binding.get("stage") != authority["identity"]["stage_name"]
        or verification_binding.get("state_sha256") != authority["identity"]["state_sha256"]
        or verification_binding.get("evidence_sha256")
        != authority["source_snapshot"]["result"]["receipt_evidence_sha256"]
        or verification_binding.get("code_sha256")
        != authority["source_snapshot"]["binding"]["code_sha256"]
        or verification_body.get("result") != verification_result
        or verification_result.get("observation_count")
        != authority.get("control", {}).get("authorized_census", {}).get("exact_observations")
        or adoption_origin.get("sha256") != grants.sha256_json(adoption_origin_body)
        or adoption_origin_body.get("identity") != authority["identity"]
        or adoption_origin_body.get("verification_sha256")
        != adoption_binding.get("verification_sha256")
        or origin_claim is None
        or origin_claim.get("sha256") != adoption_origin_body.get("origin_claim_sha256")
        or origin_claim.get("retire_source") is not adoption_origin_body.get("retire_source")
        or origin_claim.get("retirement_policy") != adoption_origin_body.get("retirement_policy")
        or adoption_origin_body.get("adoption_sha256") != body.get("adoption_sha256")
        or phase == "adoption"
        and (
            approval.get("retire_source") is not origin_claim.get("retire_source")
            or approval.get("retirement_policy") != origin_claim.get("retirement_policy")
        )
        or adoption_origin_body.get("source_sha256")
        != authority["source_snapshot"]["result"]["source_sha256"]
        or adoption_origin_body.get("source_wal_sha256")
        != authority["source_snapshot"]["result"]["source_wal_sha256"]
        or type(cleanup_sha) is not str
        or re.fullmatch(r"[0-9a-f]{64}", cleanup_sha) is None
    ):
        raise RunnerError("source-retirement receipt is not bound to its approved grant")
    launch_sha = expected["launch_sha256"]
    substep_record = _historical_validate_substep_record(
        _historical_substep_path(authority["control_root"], grant_id, phase, "adopt"),
        identity=authority["identity"],
        grant_id=grant_id,
        launch_sha256=launch_sha,
        launch=linked["launch"],
        phase=phase,
        substep="adopt",
    )
    expected_output = {
        "adoption_sha256": body["adoption_sha256"],
        "source_retired": True,
        "source_retirement_sha256": receipt["sha256"],
    }
    if substep_record.get("output_sha256") != grants.sha256_json(expected_output):
        raise RunnerError("source-retirement receipt is not bound to a completed supervised child")
    return receipt, raw_sha


def _historical_cleanup_retirement_authorized(
    control_root: Path,
    identity: dict[str, Any],
    freeze: dict[str, Any],
    grant_id: str,
    approval: dict[str, Any],
    adoption_proof: dict[str, Any],
    cleanup_path: Path,
    *,
    store: Any,
    source_snapshot: dict[str, Any],
    control: dict[str, Any],
) -> bool:
    """Require a linked retirement grant for every durable cleanup intent."""
    from kalshi_events import warehouse

    loaded_cleanup = warehouse._load_self_proof(cleanup_path, "historical-source-cleanup")
    if loaded_cleanup is None:
        return False
    cleanup_proof, _cleanup_result = loaded_cleanup
    expected_cleanup_sha = cleanup_proof.get("sha256")
    authority = {
        "control_root": control_root,
        "identity": identity,
        "source_snapshot": source_snapshot,
        "store": store,
        "freeze": freeze,
        "control": control,
    }
    grants_root = control_root / "grants"
    try:
        with os.scandir(grants_root) as entries:
            grant_dirs = [
                entry.name
                for entry in entries
                if not entry.is_symlink() and entry.is_dir(follow_symlinks=False)
            ]
    except OSError as exc:
        raise RunnerError("historical cleanup authority inventory is unavailable") from exc
    for prior_grant_id in grant_dirs:
        for phase in ("adoption", "models"):
            receipt_path = grants_root / prior_grant_id / "source-retirement.json"
            if not receipt_path.exists() and not receipt_path.is_symlink():
                continue
            try:
                receipt, _ = _historical_source_retirement_receipt(authority, prior_grant_id, phase)
            except RunnerError:
                continue
            if receipt.get("cleanup_intent_sha256") == expected_cleanup_sha:
                return True

    if (
        approval.get("grant_id") != grant_id
        or approval.get("phase") not in {"adoption", "models"}
        or approval.get("phase") not in approval.get("phases", [])
        or approval.get("retire_source") is not True
        or approval.get("retirement_policy") != "coexistence-capacity-relief-approved-v1"
    ):
        return False
    if approval.get("phase") == "adoption":
        verification_sha = adoption_proof.get("binding", {}).get("verification_sha256")
        claims = _historical_adoption_origin_claims(authority, verification_sha)
        return any(
            claim.get("retire_source") is True
            and claim.get("retirement_policy") == approval.get("retirement_policy")
            for claim in claims
        )
    return True


def _historical_write_source_retirement_receipt(
    authority: dict[str, Any],
    adoption: dict[str, Any],
    verification: dict[str, Any],
    *,
    phase: str,
) -> dict[str, Any]:
    import historical_grants as grants

    grant_id = authority["grant"]["grant_id"]
    launch_sha = authority["launch_sha256"]
    receipt_body = {
        "version": 1,
        "identity": authority["identity"],
        "grant_id": grant_id,
        "phase": phase,
        "launch_sha256": launch_sha,
        "retirement_policy": "coexistence-capacity-relief-approved-v1",
        "adoption_sha256": adoption.get("adoption_sha256"),
        "verification_sha256": verification.get("sha256"),
        "cleanup_intent_sha256": adoption.get("cleanup_intent_sha256"),
        "source_sha256": authority["source_snapshot"]["result"]["source_sha256"],
        "source_wal_sha256": authority["source_snapshot"]["result"]["source_wal_sha256"],
    }
    if (
        authority["approval"].get("retire_source") is not True
        or authority["approval"].get("retirement_policy") != receipt_body["retirement_policy"]
        or adoption.get("source_retired") is not True
        or not isinstance(receipt_body["adoption_sha256"], str)
        or receipt_body["verification_sha256"] != verification.get("sha256")
        or not isinstance(receipt_body["cleanup_intent_sha256"], str)
    ):
        raise RunnerError("source retirement was not authorized by this phase grant")
    path = authority["control_root"] / "grants" / grant_id / "source-retirement.json"
    receipt = receipt_body | {"sha256": grants.sha256_json(receipt_body)}
    if path.exists() or path.is_symlink():
        saved, _ = _historical_read_artifact(path, 4 * MIB)
        if saved != receipt:
            raise RunnerError("source-retirement receipt already exists with different lineage")
    else:
        grants.write_once_json(path, receipt)
    return receipt


def _historical_write_readiness_phase_origin(
    authority: dict[str, Any], verification: dict[str, Any]
) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import warehouse

    identity = authority["identity"]
    grant_id = authority["grant"]["grant_id"]
    stage_relative = f"tmp/{identity['stage_name']}"
    candidate = authority["store"].path(f"{stage_relative}/resume-candidate")
    state = warehouse.read_capture(authority["store"], identity["capture_id"])
    source_proof, _source_result = warehouse._read_source_proof(
        authority["store"], state, identity["stage_name"], candidate
    )
    readiness_loaded = warehouse._load_self_proof(
        authority["store"].path(f"{stage_relative}/resume-readiness.json"),
        "historical-readiness",
    )
    if readiness_loaded is None:
        raise RunnerError("verified historical readiness lost its immutable proof")
    readiness, _readiness_result = readiness_loaded
    landing_sha = readiness["binding"].get("landing_proof_sha256")
    if not isinstance(landing_sha, str):
        raise RunnerError("verified historical readiness has no landing proof binding")
    landing_loaded = warehouse._load_self_proof(
        candidate / f"resume-landing-{landing_sha}.json", "historical-landing"
    )
    if landing_loaded is None:
        raise RunnerError("verified historical readiness lost its landing proof")
    landing, _landing_result = landing_loaded
    body = {
        "version": 1,
        "identity": identity,
        "grant_id": grant_id,
        "source_snapshot": authority["source_snapshot"],
        "source_proof": source_proof,
        "readiness_proof": readiness,
        "landing_proof": landing,
        "verification": verification,
    }
    receipt = body | {"sha256": grants.sha256_json(body)}
    path = authority["control_root"] / "grants" / grant_id / "readiness-phase-origin.json"
    if path.exists() or path.is_symlink():
        saved, _ = _historical_read_artifact(path, 16 * MIB)
        if saved != receipt:
            raise RunnerError(
                "original readiness phase receipt already exists with different proof"
            )
    else:
        grants.write_once_json(path, receipt)
    return receipt


def _historical_adoption_claim_path(control_root: Path, grant_id: str) -> Path:
    return control_root / "grants" / grant_id / "adoption-origin-claim.json"


def _historical_adoption_origin_claims(
    authority: dict[str, Any], verification_sha256: str
) -> list[dict[str, Any]]:
    import historical_grants as grants

    control_root = authority["control_root"]
    grants_root = control_root / "grants"
    matches: list[dict[str, Any]] = []
    try:
        with os.scandir(grants_root) as entries:
            for entry in entries:
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    raise RunnerError("historical grant inventory contains an unsafe entry")
                grant_id = entry.name
                if re.fullmatch(r"[0-9a-f]{32}", grant_id) is None:
                    raise RunnerError("historical grant inventory contains an invalid ID")
                claim_path = _historical_adoption_claim_path(control_root, grant_id)
                if not claim_path.exists() and not claim_path.is_symlink():
                    continue
                claim, _ = _historical_read_artifact(claim_path, 4 * MIB)
                body = {key: value for key, value in claim.items() if key != "sha256"}
                linked = _historical_phase_grant(authority, grant_id, "adoption")
                approval = linked["approval"]
                expected = {
                    "version": 1,
                    "identity": authority["identity"],
                    "grant_id": grant_id,
                    "launch_sha256": grants.sha256_bytes(
                        grants._private_regular_file(
                            control_root / "grants" / grant_id / "launch.json"
                        )
                    ),
                    "approval_sha256": grants.sha256_json(approval),
                    "verification_sha256": body.get("verification_sha256"),
                    "retire_source": approval.get("retire_source"),
                    "retirement_policy": approval.get("retirement_policy"),
                }
                if body != expected or claim.get("sha256") != grants.sha256_json(body):
                    raise RunnerError("historical adoption-origin claim is malformed")
                if body["verification_sha256"] == verification_sha256:
                    matches.append(claim)
    except OSError as exc:
        raise RunnerError("historical adoption-origin claims cannot be inventoried") from exc
    return matches


def _historical_resolve_adoption_origin_claim(
    authority: dict[str, Any],
    verification_sha256: str,
    *,
    create_if_missing: bool,
) -> dict[str, Any]:
    import historical_grants as grants

    matches = _historical_adoption_origin_claims(authority, verification_sha256)
    approval = authority["approval"]
    if matches:
        decision = (matches[0]["retire_source"], matches[0]["retirement_policy"])
        if any((item["retire_source"], item["retirement_policy"]) != decision for item in matches):
            raise RunnerError("adoption origin has conflicting immutable retirement approvals")
        if decision != (approval.get("retire_source"), approval.get("retirement_policy")):
            raise RunnerError(
                "adoption retry differs from the immutable origin retirement approval"
            )
        return matches[0]
    if not create_if_missing:
        raise RunnerError("immutable historical adoption origin has no approved grant claim")

    grant_id = authority["grant"]["grant_id"]
    body = {
        "version": 1,
        "identity": authority["identity"],
        "grant_id": grant_id,
        "launch_sha256": authority["launch_sha256"],
        "approval_sha256": grants.sha256_json(approval),
        "verification_sha256": verification_sha256,
        "retire_source": approval.get("retire_source"),
        "retirement_policy": approval.get("retirement_policy"),
    }
    path = _historical_adoption_claim_path(authority["control_root"], grant_id)
    claim = body | {"sha256": grants.sha256_json(body)}
    if path.exists() or path.is_symlink():
        saved, _ = _historical_read_artifact(path, 4 * MIB)
        if saved != claim:
            raise RunnerError("adoption-origin claim already exists with different approval")
    else:
        grants.write_once_json(path, claim)
    return claim


def _historical_write_adoption_origin(
    authority: dict[str, Any], claim: dict[str, Any], adoption_proof: dict[str, Any]
) -> dict[str, Any]:
    import historical_grants as grants

    body = {
        "version": 1,
        "identity": authority["identity"],
        "origin_grant_id": claim["grant_id"],
        "origin_claim_sha256": claim["sha256"],
        "verification_sha256": adoption_proof.get("binding", {}).get("verification_sha256"),
        "adoption_sha256": adoption_proof.get("sha256"),
        "candidate_sha256": adoption_proof.get("result", {}).get("candidate_sha256"),
        "source_sha256": authority["source_snapshot"]["result"]["source_sha256"],
        "source_wal_sha256": authority["source_snapshot"]["result"]["source_wal_sha256"],
        "retire_source": claim["retire_source"],
        "retirement_policy": claim["retirement_policy"],
    }
    if (
        body["verification_sha256"] != claim["verification_sha256"]
        or not isinstance(body["adoption_sha256"], str)
        or not isinstance(body["candidate_sha256"], str)
    ):
        raise RunnerError("adopted origin does not match its immutable grant claim")
    path = authority["control_root"] / "source" / "adoption-origin.json"
    record = body | {"sha256": grants.sha256_json(body)}
    if path.exists() or path.is_symlink():
        saved, _ = _historical_read_artifact(path, 4 * MIB)
        if saved != record:
            raise RunnerError("immutable adoption origin record changed across retry")
    else:
        grants.write_once_json(path, record)
    return record


def _historical_phase_outputs(authority: dict[str, Any], phase: str) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import historical_export, historical_resume, warehouse
    from kalshi_events.common import digest, regular_bytes

    identity = authority["identity"]
    store = authority["store"]
    source_snapshot = authority["source_snapshot"]
    source_result = source_snapshot["result"]
    census = _historical_census_proof(
        authority,
        full_revalidation_if_missing=False,
    )
    capture_id = identity["capture_id"]
    stage_relative = historical_resume.stage_relative(identity["stage_name"])
    stage = store.path(stage_relative)
    build_relative = f"builds-historical/{capture_id}"
    build_dir = store.path(build_relative)
    build_path = build_dir / "build.json"
    adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
    if phase == "source":
        maintenance = _historical_read_startup_maintenance(authority)
        source_seal_path = authority["control_root"] / "phases" / "source.json"
        if source_seal_path.exists():
            source_seal, _ = _historical_read_artifact(source_seal_path)
            source_grant_id = source_seal.get("grant_id")
        else:
            source_grant_id = authority["grant"]["grant_id"]
        source_grant = _historical_phase_grant(authority, source_grant_id, "source")
        source_launch_sha = grants.sha256_bytes(
            grants._private_regular_file(
                authority["control_root"] / "grants" / source_grant_id / "launch.json"
            )
        )
        maintenance_record = _historical_validate_substep_record(
            _historical_substep_path(
                authority["control_root"], source_grant_id, "source", "maintenance"
            ),
            identity=identity,
            grant_id=source_grant_id,
            launch_sha256=source_launch_sha,
            launch=source_grant["launch"],
            phase="source",
            substep="maintenance",
        )
        if maintenance_record.get("output_sha256") != grants.sha256_json(
            {"startup_maintenance_sha256": maintenance["sha256"]}
        ):
            raise RunnerError("source checkpoint is not bound to its startup cleanup proof")
        return {
            "startup_maintenance_sha256": maintenance["sha256"],
            "source_snapshot_sha256": source_snapshot["sha256"],
            "capture_state_sha256": source_snapshot["binding"]["capture_state_sha256"],
            "receipt_evidence_sha256": source_result["receipt_evidence_sha256"],
            "source_sha256": source_result["source_sha256"],
            "source_wal_sha256": source_result["source_wal_sha256"],
            "receipt_count": source_result["receipt_count"],
            "population_report_sha256": census["population_report_sha256"],
            "receipt_index_sha256": census["receipt_index_sha256"],
            "exact_observations": census["exact_observations"],
            "observations_by_endpoint": census["observations_by_endpoint"],
            "scope_sha256": source_snapshot["binding"]["scope_sha256"],
            "code_sha256": source_snapshot["binding"]["code_sha256"],
        }
    if phase == "readiness":
        seal_path = authority["control_root"] / "phases" / "readiness.json"
        if seal_path.exists():
            seal, _ = _historical_read_artifact(seal_path)
            linked_grant_id = seal.get("grant_id")
        elif "readiness" in authority["approval"].get("phases", []):
            linked_grant_id = authority["grant"]["grant_id"]
        else:
            raise RunnerError("readiness phase has no original linked grant")
        _historical_phase_grant(authority, linked_grant_id, "readiness")
        return _historical_readiness_phase_origin(authority, linked_grant_id)
    if phase == "adoption":
        seal_path = authority["control_root"] / "phases" / "adoption.json"
        if seal_path.exists():
            seal, _ = _historical_read_artifact(seal_path)
            linked_grant_id = seal.get("grant_id")
        elif "adoption" in authority["approval"].get("phases", []):
            linked_grant_id = authority["grant"]["grant_id"]
        else:
            raise RunnerError("adoption phase has no original linked grant")
        linked = _historical_phase_grant(authority, linked_grant_id, "adoption")
        approval = linked["approval"]
        if stage.is_dir() and not stage.is_symlink():
            adoption_loaded = warehouse._load_self_proof(adoption_path, "historical-adoption")
            if adoption_loaded is None:
                raise RunnerError("historical adoption origin proof is missing")
            adoption_proof, adoption_result = adoption_loaded
            state = warehouse.read_capture(store, capture_id)
            source_proof, source_result = warehouse._read_source_proof(
                store,
                state,
                identity["stage_name"],
                store.path(f"{stage_relative}/resume-candidate"),
            )
            origin, verified_origin = warehouse._validate_historical_adoption_origin(
                store, state, identity["stage_name"], source_proof, source_result
            )
            if origin != adoption_proof or verified_origin != adoption_result:
                raise RunnerError("historical adoption origin changed after sealing")
        elif build_path.is_file() and not build_path.is_symlink():
            certified = warehouse.certified(store, capture_id, catalogue_version=3)
            adoption_loaded = warehouse._load_self_proof(
                build_dir / "historical-adoption.json", "historical-adoption"
            )
            if adoption_loaded is None:
                raise RunnerError("installed historical build lost its adoption origin")
            adoption_proof, adoption_result = adoption_loaded
            source_result = source_snapshot["result"]
            build, _ = _historical_read_artifact(build_path)
            installed = build.get("historical_adoption")
            if (
                certified.get("historical_adoption") != installed
                or not isinstance(installed, dict)
                or installed.get("sha256") != adoption_proof.get("sha256")
                or installed.get("candidate_sha256") != adoption_result.get("candidate_sha256")
            ):
                raise RunnerError("installed build does not preserve its immutable adoption origin")
        else:
            raise RunnerError("historical adoption candidate or installed build is missing")

        origin_path = authority["control_root"] / "source" / "adoption-origin.json"
        origin, _ = _historical_read_artifact(origin_path, 4 * MIB)
        origin_body = {key: value for key, value in origin.items() if key != "sha256"}
        if (
            origin.get("sha256") != grants.sha256_json(origin_body)
            or origin_body.get("version") != 1
            or origin_body.get("identity") != identity
            or origin_body.get("adoption_sha256") != adoption_proof["sha256"]
            or origin_body.get("verification_sha256")
            != adoption_proof["binding"].get("verification_sha256")
            or origin_body.get("candidate_sha256") != adoption_result.get("candidate_sha256")
            or origin_body.get("source_sha256") != source_result["source_sha256"]
            or origin_body.get("source_wal_sha256") != source_result["source_wal_sha256"]
        ):
            raise RunnerError("immutable adoption origin record differs from its warehouse proof")
        claims = _historical_adoption_origin_claims(
            authority, adoption_proof["binding"]["verification_sha256"]
        )
        origin_claim = next(
            (claim for claim in claims if claim["grant_id"] == origin_body.get("origin_grant_id")),
            None,
        )
        if (
            origin_claim is None
            or origin_claim.get("sha256") != origin_body.get("origin_claim_sha256")
            or origin_claim.get("retire_source") is not origin_body.get("retire_source")
            or origin_claim.get("retirement_policy") != origin_body.get("retirement_policy")
            or approval.get("retire_source") is not origin_claim.get("retire_source")
            or approval.get("retirement_policy") != origin_claim.get("retirement_policy")
        ):
            raise RunnerError("adoption phase grant differs from immutable origin authority")
        retired = approval.get("retire_source") is True
        retirement_receipt_sha256 = None
        receipt_path = (
            authority["control_root"] / "grants" / linked_grant_id / "source-retirement.json"
        )
        if retired:
            retirement_receipt, _ = _historical_source_retirement_receipt(
                authority, linked_grant_id, "adoption"
            )
            retirement_receipt_sha256 = retirement_receipt["sha256"]
        elif receipt_path.exists() or receipt_path.is_symlink():
            raise RunnerError("non-retiring adoption grant has a retirement receipt")
        return {
            "adoption_sha256": adoption_proof["sha256"],
            "verification_receipt_sha256": adoption_proof["binding"]["verification_sha256"],
            "candidate_sha256": adoption_result["candidate_sha256"],
            "source_retired": retired,
            "retirement_receipt_sha256": retirement_receipt_sha256,
        }
    if phase in {"models", "certified-build"}:
        if not build_path.is_file() or build_path.is_symlink():
            raise RunnerError("historical model build receipt is missing")
        build, build_receipt_sha = _historical_read_artifact(build_path)
        if build.get("capture_id") != capture_id or build.get("catalogue_version") != 3:
            raise RunnerError("historical model build receipt does not match the capture")
        if build.get("observation_count") != census["exact_observations"]:
            raise RunnerError(
                "historical model build count differs from the authorized exact census"
            )
        certified = warehouse.certified(store, capture_id, catalogue_version=3)
        artifact_path = build_dir / "build-artifacts.json"
        model_artifact_sha = digest(regular_bytes(artifact_path, 64 * MIB))
        if model_artifact_sha != build.get("dbt_artifacts_sha256"):
            raise RunnerError("historical model build artifacts changed")
        if phase == "models":
            seal_path = authority["control_root"] / "phases" / "models.json"
            if seal_path.exists():
                seal, _ = _historical_read_artifact(seal_path)
                linked_grant_id = seal.get("grant_id")
            elif "models" in authority["approval"].get("phases", []):
                linked_grant_id = authority["grant"]["grant_id"]
            else:
                raise RunnerError("models phase has no original linked grant")
            _historical_phase_grant(authority, linked_grant_id, "models")
            source_retirement_sha256 = None
            grant_root = authority["control_root"] / "grants"
            retirements: list[tuple[float, str, str]] = []
            grant_count = 0
            try:
                with os.scandir(grant_root) as entries:
                    for entry in entries:
                        grant_count += 1
                        if grant_count > 4096:
                            raise RunnerError("historical grant inventory exceeded its bound")
                        if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                            raise RunnerError("historical grant inventory contains an unsafe entry")
                        if re.fullmatch(r"[0-9a-f]{32}", entry.name) is None:
                            raise RunnerError("historical grant inventory contains an invalid ID")
                        receipt_path = Path(entry.path) / "source-retirement.json"
                        if not receipt_path.exists() and not receipt_path.is_symlink():
                            continue
                        retirement_header, _ = _historical_read_artifact(receipt_path, 4 * MIB)
                        if retirement_header.get("phase") != "models":
                            continue
                        retirement, _ = _historical_source_retirement_receipt(
                            authority, entry.name, "models"
                        )
                        linked_retirement = _historical_phase_grant(authority, entry.name, "models")
                        retirements.append(
                            (
                                float(linked_retirement["launch"]["started_epoch"]),
                                entry.name,
                                retirement["sha256"],
                            )
                        )
            except OSError as exc:
                raise RunnerError("historical retirement receipt inventory is unavailable") from exc
            if retirements:
                source_retirement_sha256 = min(retirements)[2]
            return {
                "build_receipt_sha256": build_receipt_sha,
                "warehouse_sha256": certified["warehouse_sha256"],
                "dbt_artifacts_sha256": model_artifact_sha,
                "source_retirement_sha256": source_retirement_sha256,
            }
        return {
            "build_sha256": digest(regular_bytes(build_path, MIB)),
            "build_manifest_sha256": model_artifact_sha,
            "certified_receipt_sha256": grants.sha256_json(certified),
        }
    if phase in {"export", "verified-publication"}:
        current, manifest = _historical_published_release(authority)
        export = {
            "export_policy_sha256": grants.sha256_json(historical_export.POLICY),
            "parquet_inventory_sha256": grants.sha256_json(manifest.get("files")),
            "release_metadata_sha256": grants.sha256_json(manifest),
        }
        publication = {
            "release_id": current["source_release_id"],
            "release_manifest_sha256": current["manifest_sha256"],
            "publication_pointer_sha256": current["pointer_sha256"],
        }
        return export if phase == "export" else publication
    if phase == "independent-verification":
        seal_path = authority["control_root"] / "phases" / "independent-verification.json"
        receipt_grant = authority["grant"]["grant_id"]
        if seal_path.exists():
            seal, _ = _historical_read_artifact(seal_path)
            receipt_grant = seal["grant_id"]
        verified = _historical_release_verification(authority, grant_id_for_receipt=receipt_grant)
        return {
            key: verified[key]
            for key in (
                "verification_receipt_sha256",
                "release_id",
                "release_manifest_sha256",
            )
        }
    if phase == "activation":
        result_path = (
            authority["control_root"] / "activations" / f"{authority['grant']['grant_id']}.json"
        )
        result, _ = _historical_read_artifact(result_path, 4 * MIB)
        current, manifest = _historical_published_release(authority)
        database_path = authority["directory"] / "production.duckdb"
        certified_source = build_dir / "warehouse.duckdb"
        database_record = _historical_file_record(
            database_path, max_bytes=128 * GIB, linked_source=certified_source
        )
        for database in (database_path, certified_source):
            wal = database.with_name(database.name + ".wal")
            if wal.exists() or wal.is_symlink():
                raise RunnerError("activated database or certified warehouse has a WAL")
        relation_rows = result.get("relation_rows")
        readback = result.get("production_readback")
        audit_binding = authority["report"].get("activation_audit")
        if not isinstance(audit_binding, dict):
            raise RunnerError("historical activation lost its independent audit binding")
        from production_activation import _activation_audit_payload

        audit = _activation_audit_payload(
            authority["directory"],
            audit_binding,
            expected_session_id=identity["session_id"],
        )
        audit_sha = audit_binding.get("sha256")
        census_control = authority["control"].get("authorized_census", {})
        if (
            result.get("state") != "verified"
            or result.get("capture_id") != capture_id
            or result.get("source_release_id") != current["source_release_id"]
            or result.get("warehouse_sha256") != database_record["sha256"]
            or manifest.get("warehouse_sha256") != database_record["sha256"]
            or audit_sha != authority["approval"].get("activation_audit_sha256")
            or audit.get("capture_id") != census_control.get("capture_id")
            or audit.get("root_id") != census_control.get("root_id")
            or audit.get("capture_state_sha256") != census_control.get("capture_state_sha256")
            or audit.get("receipt_index_sha256") != census_control.get("receipt_index_sha256")
            or audit.get("population_report_sha256")
            != census_control.get("population_report_sha256")
            or audit.get("observations_by_endpoint")
            != census_control.get("observations_by_endpoint")
            or audit.get("exact_observations") != census_control.get("exact_observations")
            or not isinstance(readback, dict)
            or readback.get("read_only") is not True
            or readback.get("relation_rows") != relation_rows
            or not isinstance(relation_rows, dict)
            or set(relation_rows)
            != {"series", "events", "markets", "relationships", "combo_legs", "observation_history"}
            or relation_rows.get("observation_history") != census_control.get("exact_observations")
        ):
            raise RunnerError(
                "historical activation readback does not match its verified artifacts"
            )
        return {
            "activation_audit_sha256": audit_sha,
            "production_database_sha256": database_record["sha256"],
            "release_id": current["source_release_id"],
            "activation_readback_sha256": grants.sha256_json(readback),
            "relation_rows": relation_rows,
        }
    raise RunnerError("unknown private historical phase")


def _historical_validate_phase_chain(authority: dict[str, Any]) -> tuple[str | None, str | None]:
    import historical_grants as grants

    current, previous = grants.read_phase_chain(
        authority["control_root"] / "phases", authority["identity"]
    )
    seals = [
        _historical_read_artifact(authority["control_root"] / "phases" / f"{phase}.json")[0]
        for phase in grants.PHASES[: len(grants.PHASES) if current is not None else 0]
        if (authority["control_root"] / "phases" / f"{phase}.json").exists()
    ]
    for seal in seals:
        phase = seal["phase"]
        actual = _historical_phase_outputs(authority, phase)
        if seal["outputs"] != actual:
            raise RunnerError(f"sealed {phase} phase no longer matches verified artifacts")
        linked = _historical_phase_grant(authority, seal["grant_id"], phase)
        if phase == "adoption":
            grants.validate_adoption_retirement(
                linked["approval"], seal["outputs"].get("source_retired")
            )
    return current, previous


_HISTORICAL_PHASE_SUBSTEPS = {
    "source": ("maintenance", "snapshot"),
    "readiness": ("prepare", "verify"),
    "adoption": ("verify", "adopt"),
    "models": ("prepare", "verify", "adopt", "build"),
    "certified-build": ("certify",),
    "export": ("publish",),
    "verified-publication": ("verify",),
    "independent-verification": ("verify",),
    "activation": ("activate",),
}


def _historical_substeps_for_grant(
    phase: str, approval: dict[str, Any], store: Any, capture_id: str
) -> tuple[str, ...]:
    ordered = _HISTORICAL_PHASE_SUBSTEPS.get(phase)
    if ordered is None:
        raise RunnerError("private historical phase is not recognized")
    if phase != "models" or approval.get("retire_source") is not True:
        return ordered if phase != "models" else ("build",)
    build_path = store.path(f"builds-historical/{capture_id}/build.json")
    if build_path.is_symlink():
        raise RunnerError("installed historical build receipt is a symlink")
    if build_path.exists():
        if not build_path.is_file():
            raise RunnerError("installed historical build receipt is not a regular file")
        # An installed build means retirement already preceded this retry. Never
        # revisit the pending stage or alter its source-retirement lineage.
        return ("build",)
    stage_path = store.path(f"tmp/{approval.get('bindings', {}).get('stage_name', '')}")
    if stage_path.is_symlink() or not stage_path.is_dir():
        raise RunnerError("approved models retirement requires the exact pending stage")
    return ("prepare", "verify", "adopt", "build")


def _historical_substep_path(control_root: Path, grant_id: str, phase: str, substep: str) -> Path:
    if phase not in _HISTORICAL_PHASE_SUBSTEPS or substep not in _HISTORICAL_PHASE_SUBSTEPS[phase]:
        raise RunnerError("private historical worker substep is not recognized")
    return control_root / "grants" / grant_id / "substeps" / f"{phase}-{substep}.json"


def _historical_validate_substep_record(
    path: Path,
    *,
    identity: dict[str, Any],
    grant_id: str,
    launch_sha256: str,
    launch: dict[str, Any],
    phase: str,
    substep: str,
) -> dict[str, Any]:
    import historical_grants as grants

    record, _ = _historical_read_artifact(path, 1024**2)
    fields = {
        "version",
        "identity",
        "grant_id",
        "launch_sha256",
        "phase",
        "substep",
        "completed_at_epoch",
        "output_sha256",
    }
    if (
        set(record) != fields
        or record.get("version") != 1
        or record.get("identity") != identity
        or record.get("grant_id") != grant_id
        or record.get("launch_sha256") != launch_sha256
        or record.get("phase") != phase
        or record.get("substep") != substep
    ):
        raise RunnerError("supervised historical substep receipt is not bound to its launch")
    completed = record.get("completed_at_epoch")
    if type(completed) not in {int, float, Decimal} or not math.isfinite(float(completed)):
        raise RunnerError("supervised historical substep completion time is invalid")
    grants.validate_phase_completion_time(phase, float(completed), launch)
    if (
        not isinstance(record.get("output_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", record["output_sha256"]) is None
    ):
        raise RunnerError("supervised historical substep receipt hash is invalid")
    return record


def _historical_activation_claimed(
    control_root: Path,
    grant_id: str,
    launch_id: str,
    identity: dict[str, Any],
) -> bool:
    import historical_grants as grants

    markers = control_root / "activation-audits"
    try:
        marker_directory = markers.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RunnerError("activation audit marker directory is unavailable") from exc
    if not stat.S_ISDIR(marker_directory.st_mode):
        raise RunnerError("activation audit marker directory is unsafe")
    try:
        grants._private_directory(markers, "activation audit marker directory")
        if (
            not isinstance(grant_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", grant_id) is None
            or not isinstance(launch_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", launch_id) is None
        ):
            raise RunnerError("activation audit marker lookup identity is invalid")
        expected_identity = grants._identity(identity)
        marker_path = markers / f"{grant_id}.json"
        if not marker_path.exists() and not marker_path.is_symlink():
            return False
        marker = grants.read_json(marker_path)
    except OSError as exc:
        raise RunnerError("activation audit marker is unavailable") from exc
    except grants.ControlError as exc:
        raise RunnerError("activation audit marker is invalid") from exc
    if (
        set(marker) != {"version", "audit_sha256", "identity", "grant_id", "launch_id"}
        or marker.get("version") != 1
        or not isinstance(marker.get("audit_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", marker["audit_sha256"]) is None
        or marker.get("identity") != expected_identity
        or marker.get("grant_id") != grant_id
        or marker.get("launch_id") != launch_id
    ):
        raise RunnerError("activation audit marker is not bound to the current grant and launch")
    return True


def _read_stopped_worker_audit(operator: Path, event_id: str) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events.common import regular_bytes

    audit_path = operator / "stopped-worker-reconciliation.jsonl"
    try:
        audit_info = audit_path.lstat()
        if (
            not stat.S_ISREG(audit_info.st_mode)
            or stat.S_ISLNK(audit_info.st_mode)
            or audit_info.st_nlink != 1
            or audit_info.st_dev != operator.stat().st_dev
        ):
            raise RunnerError("stopped-worker audit readback is unsafe")
        raw = regular_bytes(audit_path, MIB)
    except OSError as exc:
        raise RunnerError("stopped-worker audit readback is unavailable") from exc
    matching = []
    for line in raw.splitlines():
        try:
            row = json.loads(line.decode("utf-8"), parse_constant=grants._reject_constant)
        except (UnicodeError, ValueError) as exc:
            raise RunnerError("stopped-worker audit readback is malformed") from exc
        if not isinstance(row, dict) or row.get("version") != 1:
            raise RunnerError("stopped-worker audit readback is malformed")
        if row.get("event_id") == event_id:
            matching.append(row)
    if len(matching) != 1:
        raise RunnerError("stopped-worker audit event is missing or duplicated")
    return matching[0]


def _historical_reconciliation_audit(authority: dict[str, Any]) -> dict[str, Any]:
    import historical_grants as grants

    report = authority["report"]
    identity = authority["identity"]
    operator = authority["operator"]
    reconciliation = report.get("stopped_worker_reconciliation")
    stop = reconciliation.get("storage_stop_evidence") if isinstance(reconciliation, dict) else None
    admission = grants.expected_resource_admission()
    expected_stop_reason = (
        f"live temporary storage reached the "
        f"{admission['storage_temporary_stop_bytes'] // GIB} GiB stop threshold"
    )
    current_boot = _boot_stamp()
    stopped = report.get("phase") == "stopped" and report.get("active_process_group") is None
    # Only the authority built after the private handoff can attest the live report.
    supervised_phase = authority.get("supervised_report_phase")
    supervised = (
        supervised_phase is not None
        and report.get("phase") == supervised_phase
        and report.get("active_process_group") == os.getpid()
    )
    if (
        report.get("boot_stamp") != current_boot
        or not (stopped or supervised)
        or not isinstance(reconciliation, dict)
        or not {
            "event_id",
            "audit_file",
            "boot_stamp",
            "stale_process_group",
            "supervisor_pid",
            "supervisor_pgid",
            "primary_stop_reason",
            "reconciliation_kind",
            "storage_stop_evidence",
        }
        <= set(reconciliation)
        or reconciliation.get("audit_file") != "stopped-worker-reconciliation.jsonl"
        or not isinstance(reconciliation.get("boot_stamp"), str)
        or not reconciliation["boot_stamp"]
        or reconciliation.get("reconciliation_kind") != "temporary_storage_stop"
        or reconciliation.get("primary_stop_reason") != expected_stop_reason
        or not isinstance(stop, dict)
        or stop.get("mode") != "historical"
        or stop.get("category") != "temporary"
        or stop.get("threshold_bytes") != admission["storage_temporary_stop_bytes"]
        or stop.get("threshold_inclusive") is not True
        or type(stop.get("observed_bytes")) is not int
        or stop["observed_bytes"] < stop["threshold_bytes"]
        or stop.get("original_process_group") != reconciliation.get("stale_process_group")
    ):
        raise RunnerError(
            "startup cleanup requires the exact reconciled temporary-stop audit "
            "and a stopped session or authenticated supervised worker"
        )
    event_id = reconciliation.get("event_id")
    stale_group = reconciliation.get("stale_process_group")
    supervisor_pid = reconciliation.get("supervisor_pid")
    supervisor_pgid = reconciliation.get("supervisor_pgid")
    stop_boot = reconciliation.get("boot_stamp")
    typed_stop_transition = None
    if (
        not isinstance(event_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", event_id) is None
        or type(stale_group) is not int
        or stale_group <= 1
        or type(supervisor_pid) is not int
        or supervisor_pid <= 1
        or type(supervisor_pgid) is not int
        or supervisor_pgid <= 1
    ):
        raise RunnerError("startup cleanup stopped-worker identity is invalid")
    if stop_boot == current_boot:
        if report.get("error") != expected_stop_reason:
            raise RunnerError("same-boot stopped-worker report lost its storage stop reason")
        try:
            if (
                _pid_exists(stale_group)
                or _process_group_exists(stale_group)
                or _pid_exists(supervisor_pid)
                or _process_group_exists(supervisor_pgid)
            ):
                raise RunnerError("reconciled stopped worker identity is live or ambiguous")
        except OSError as exc:
            raise RunnerError("reconciled stopped worker identity is ambiguous") from exc
    else:
        recovery = report.get("historical_reboot_reconciliation")
        history = report.get("reboot_recovery_history")
        reboot = history[-1] if isinstance(history, list) and history else None
        typed_stop = (
            isinstance(reconciliation, dict)
            and reconciliation.get("recovery_kind") == "prior_boot_temporary_stop"
        )
        typed_stop_index = None
        if typed_stop:
            typed_indices = (
                [
                    index
                    for index, item in enumerate(history)
                    if isinstance(item, dict)
                    and item.get("recovery_kind") == "prior_boot_temporary_stop"
                ]
                if isinstance(history, list)
                else []
            )
            if len(typed_indices) != 1:
                raise RunnerError("prior-boot stop recovery transition is missing or duplicated")
            typed_stop_index = typed_indices[0]
            typed_stop_transition = history[typed_stop_index]
        freeze = authority.get("freeze")
        current_mount = authority.get("current_mount")
        system = report.get("system")
        current_device = (
            current_mount.get("device")
            if isinstance(current_mount, dict)
            else (freeze or {}).get("root_device")
        )
        current_volume = (
            current_mount.get("volume_uuid")
            if isinstance(current_mount, dict)
            else (freeze or {}).get("root_volume_uuid")
        )
        if (
            report.get("error") is not None
            or not isinstance(recovery, dict)
            or recovery.get("state") != "complete"
            or recovery.get("capture_id") != identity["capture_id"]
            or recovery.get("root_id") != identity["root_id"]
            or recovery.get("boot_stamp") != current_boot
            or recovery.get("processing_worker_started") is not False
            or recovery.get("grant_started") is not False
            or not isinstance(reboot, dict)
            or not typed_stop
            and reboot.get("previous_boot_stamp") != stop_boot
            or reboot.get("boot_stamp") != current_boot
            or reboot.get("volume_uuid") != current_volume
            or reboot.get("filesystem_device") != current_device
            or reboot.get("processing_worker_started") is not False
            or reboot.get("grant_started") is not False
            or reboot.get("prior_process_group_ignored_after_boot_change")
            is not (reboot.get("previous_active_process_group") is not None)
            or reboot.get("previous_active_process_group") not in (None, stale_group)
            and not (
                typed_stop
                and isinstance(typed_stop_transition, dict)
                and _private_reboot_preserves_bound(reboot, typed_stop_transition)
            )
            or not isinstance(system, dict)
            or system.get("filesystem_device") != current_device
            or system.get("filesystem_volume_uuid") != current_volume
        ):
            raise RunnerError(
                "old-boot stopped-worker identity lacks exact reboot accounting proof"
            )
        if typed_stop:
            transition = typed_stop_transition
            assert isinstance(transition, dict) and isinstance(typed_stop_index, int)
            prefix = history[:typed_stop_index]
            prefix_sha256 = hashlib.sha256(
                json.dumps(prefix, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest()
            latest_is_typed_stop = typed_stop_index == len(history) - 1
            bounded = transition.get("accounting_basis") == "reserved-upper-bound"
            if (
                transition.get("previous_boot_stamp") != stop_boot
                or transition.get("previous_active_process_group") != stale_group
                or transition.get("boot_stamp") != reconciliation.get("recovered_boot_stamp")
                or transition.get("recovered_boot_stamp") != transition.get("boot_stamp")
                or transition.get("stopped_worker_event_id") != event_id
                or transition.get("stopped_worker_audit_sha256")
                != reconciliation.get("recovery_audit_sha256")
                or transition.get("stopped_worker_session_before_sha256")
                != reconciliation.get("session_before_sha256")
                or transition.get("pre_recovery_history_sha256") != prefix_sha256
                or transition.get("session_before_sha256")
                != reconciliation.get("session_before_sha256")
                or transition.get("session_before_sha256") != identity.get("prior_session_sha256")
                or reconciliation.get("recovered_boot_stamp") != transition.get("boot_stamp")
                or latest_is_typed_stop
                and (
                    recovery.get("recovery_kind") != "prior_boot_temporary_stop"
                    or recovery.get("stopped_worker_event_id") != event_id
                    or recovery.get("stopped_worker_audit_sha256")
                    != reconciliation.get("recovery_audit_sha256")
                    or recovery.get("stopped_worker_session_before_sha256")
                    != reconciliation.get("session_before_sha256")
                    or transition.get("boot_stamp") != current_boot
                )
                or not latest_is_typed_stop
                and not _private_reboot_preserves_bound(reboot, transition)
                and (
                    reboot.get("accounting_basis") != "exact"
                    or reboot.get("accounting_complete") is not True
                    or recovery.get("accounting_basis") != "exact"
                )
            ):
                raise RunnerError("prior-boot stop recovery is missing its typed audit binding")
            if bounded and (
                latest_is_typed_stop or _private_reboot_preserves_bound(reboot, transition)
            ):
                if (
                    report.get("accounting_complete") is not False
                    or report.get("accounting_basis") != "reserved-upper-bound"
                    or report.get("accounting_bound_verified") is not True
                    or recovery.get("accounting_complete") is not False
                    or recovery.get("accounting_basis") != "reserved-upper-bound"
                    or recovery.get("accounting_bound_verified") is not True
                    or report.get("consumption_state") != "reserved-upper-bound"
                    or report.get("downloaded_bytes") is not None
                    or report.get("requests") != transition.get("attempts")
                    or report.get("downloaded_bytes_upper_bound")
                    != transition.get("downloaded_bytes_upper_bound")
                    or report.get("unfinished_reservation_ids")
                    != transition.get("unfinished_reservation_ids")
                    or transition.get("accounting_complete") is not False
                    or transition.get("accounting_bound_verified") is not True
                    or not transition.get("unfinished_reservation_ids")
                    or not isinstance(transition.get("reservation_inventory_sha256"), str)
                    or re.fullmatch(r"[0-9a-f]{64}", transition["reservation_inventory_sha256"])
                    is None
                    or not isinstance(transition.get("accounting_sha256"), str)
                    or re.fullmatch(r"[0-9a-f]{64}", transition["accounting_sha256"]) is None
                ):
                    raise RunnerError("bounded reboot proof is missing its saved receipt binding")
            else:
                if (
                    latest_is_typed_stop
                    and bounded
                    or recovery.get("accounting_basis") != "exact"
                    or recovery.get("accounting_complete") is not True
                    or recovery.get("accounting_bound_verified") is not False
                    or report.get("accounting_complete") is not True
                    or report.get("accounting_basis") != "exact"
                    or report.get("accounting_bound_verified") is not False
                    or report.get("unfinished_reservation_ids") != []
                    or report.get("consumption_state") != "verified"
                    or report.get("requests") != reboot.get("attempts")
                    or report.get("downloaded_bytes") != reboot.get("downloaded_bytes")
                    or reboot.get("accounting_basis") != "exact"
                    or reboot.get("accounting_complete") is not True
                    or not isinstance(reboot.get("accounting_sha256"), str)
                    or re.fullmatch(r"[0-9a-f]{64}", reboot["accounting_sha256"]) is None
                    or latest_is_typed_stop
                    and (
                        transition.get("accounting_complete") is not True
                        or transition.get("accounting_bound_verified") is not False
                        or transition.get("accounting_basis") != "exact"
                        or transition.get("attempts") != reboot.get("attempts")
                        or transition.get("downloaded_bytes") != reboot.get("downloaded_bytes")
                    )
                ):
                    raise RunnerError("exact reboot proof is missing its saved receipt binding")
        elif (
            recovery.get("accounting_basis") != "exact"
            or reboot.get("accounting_basis") != "exact"
            or reboot.get("accounting_complete") is not True
        ):
            raise RunnerError(
                "old-boot stopped-worker identity lacks exact reboot accounting proof"
            )
    record = _read_stopped_worker_audit(operator, event_id)
    if (
        record.get("capture_id") != identity["capture_id"]
        or record.get("session_id") != identity["session_id"]
        or record.get("root_id") != identity["root_id"]
        or (
            record.get("boot_stamp") != report.get("boot_stamp")
            and record.get("boot_stamp") != stop_boot
        )
        or record.get("stale_process_group") != stale_group
        or record.get("supervisor_pid") != supervisor_pid
        or record.get("supervisor_pgid") != supervisor_pgid
        or record.get("primary_stop_reason") != expected_stop_reason
        or record.get("reconciliation_kind") != "temporary_storage_stop"
        or record.get("storage_stop_evidence") != stop
    ):
        raise RunnerError("stopped-worker audit readback does not match the selected session")
    result = {
        "event_id": event_id,
        "record_sha256": grants.sha256_json(record),
        "audit_file": reconciliation["audit_file"],
        "boot_stamp": stop_boot,
        "primary_stop_reason": expected_stop_reason,
        "supervisor_pid": supervisor_pid,
        "supervisor_pgid": supervisor_pgid,
    }
    if reconciliation.get("recovery_kind") == "prior_boot_temporary_stop":
        transition = typed_stop_transition
        if (
            stop_boot == current_boot
            or not isinstance(transition, dict)
            or record.get("recovery_kind") != "prior_boot_temporary_stop"
            or record.get("recovered_boot_stamp") != transition.get("boot_stamp")
            or record.get("report_sha256") != reconciliation.get("session_before_sha256")
            or record.get("pre_recovery_history_sha256")
            != transition.get("pre_recovery_history_sha256")
            or record.get("accounting_sha256") != transition.get("accounting_sha256")
            or record.get("accounting_basis") != transition.get("accounting_basis")
            or record.get("accounting_complete") != transition.get("accounting_complete")
            or record.get("accounting_bound_verified")
            != transition.get("accounting_bound_verified")
            or record.get("attempts") != transition.get("attempts")
            or record.get("downloaded_bytes") != transition.get("downloaded_bytes")
            or record.get("downloaded_bytes_upper_bound")
            != transition.get("downloaded_bytes_upper_bound")
            or record.get("unfinished_reservation_ids")
            != transition.get("unfinished_reservation_ids")
            or record.get("reservation_inventory_sha256")
            != transition.get("reservation_inventory_sha256")
            or record.get("previous_filesystem_device")
            != transition.get("previous_filesystem_device")
            or record.get("filesystem_device") != transition.get("filesystem_device")
            or record.get("volume_uuid") != transition.get("volume_uuid")
            or record.get("prior_process_group_ignored_after_boot_change") is not True
            or reconciliation.get("recovery_audit_sha256") != result["record_sha256"]
            or not isinstance(transition.get("session_before_sha256"), str)
            or transition.get("session_before_sha256") != identity.get("prior_session_sha256")
        ):
            raise RunnerError("prior-boot temporary-stop audit is not bound to its recovery")
        result.update(
            recovery_kind="prior_boot_temporary_stop",
            # Startup cleanup seals this proof, so it must survive later reboots.
            accounting_basis=transition["accounting_basis"],
            accounting_sha256=transition["accounting_sha256"],
            session_before_sha256=transition["session_before_sha256"],
            pre_recovery_history_sha256=transition["pre_recovery_history_sha256"],
            recovered_boot_stamp=transition["boot_stamp"],
        )
    return result


def _historical_startup_maintenance_path(authority: dict[str, Any]) -> Path:
    return authority["control_root"] / "source" / "startup-maintenance.json"


def _historical_read_startup_maintenance(authority: dict[str, Any]) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import historical_resume, warehouse

    path = _historical_startup_maintenance_path(authority)
    proof = grants.read_json(path, max_bytes=16 * MIB)
    body = {key: value for key, value in proof.items() if key != "sha256"}
    expected_fields = {
        "version",
        "identity",
        "grant_id",
        "launch_sha256",
        "source_snapshot_sha256",
        "reconciliation",
        "stage_identity",
        "pre_inventory",
        "cleanup_proofs",
        "removed_files",
        "post_inventory",
        "completed_at_epoch",
    }
    if (
        set(body) != expected_fields
        or proof.get("version") != 1
        or proof.get("identity") != authority["identity"]
        or proof.get("sha256") != grants.sha256_json(body)
        or proof.get("source_snapshot_sha256") != authority["freeze"]["source_snapshot_sha256"]
        or not isinstance(proof.get("grant_id"), str)
        or not isinstance(proof.get("launch_sha256"), str)
    ):
        raise RunnerError("startup cleanup control proof is malformed or changed")
    linked = _historical_phase_grant(authority, proof["grant_id"], "source")
    if (
        grants.sha256_bytes(
            grants._private_regular_file(
                authority["control_root"] / "grants" / proof["grant_id"] / "launch.json"
            )
        )
        != proof["launch_sha256"]
    ):
        raise RunnerError("startup cleanup proof is not bound to its source grant launch")
    if linked["launch"].get("session_binding_sha256") is None:
        raise RunnerError("startup cleanup grant lost its immutable session binding")
    grants.validate_phase_completion_time(
        "source", proof.get("completed_at_epoch", 0), linked["launch"]
    )
    reconciliation = _historical_reconciliation_audit(authority)
    if proof.get("reconciliation") != reconciliation:
        raise RunnerError("startup cleanup proof lost its stopped-worker audit readback")
    stage_relative = historical_resume.stage_relative(authority["identity"]["stage_name"])
    spill_files: dict[str, tuple[int, str]] = {}
    cleanup_proofs = proof.get("cleanup_proofs")
    if not isinstance(cleanup_proofs, list) or len(cleanup_proofs) > 64:
        raise RunnerError("startup cleanup proof inventory is invalid")
    stage_path = authority["store"].path(stage_relative)
    if stage_path.is_symlink():
        raise RunnerError("startup cleanup stage path became a symlink")
    stage_exists = stage_path.exists()
    for item in cleanup_proofs:
        if (
            not isinstance(item, dict)
            or set(item)
            != {
                "path",
                "sha256",
                "runtime",
                "spill",
                "file_count",
                "total_bytes",
                "inventory_sha256",
            }
            or not isinstance(item.get("path"), str)
            or re.fullmatch(
                r"tmp/dbt-[0-9a-f]{32}/historical-spill-cleanup(?:-[0-9a-f]{32})?\.json",
                item["path"],
            )
            is None
            or not isinstance(item.get("runtime"), str)
            or re.fullmatch(r"tmp/dbt-[0-9a-f]{32}", item["runtime"]) is None
            or item.get("spill") != f"{item['runtime']}/spill"
            or not item["path"].startswith(item["runtime"] + "/")
            or type(item.get("file_count")) is not int
            or item["file_count"] < 0
            or type(item.get("total_bytes")) is not int
            or item["total_bytes"] < 0
            or not isinstance(item.get("inventory_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", item["inventory_sha256"]) is None
        ):
            raise RunnerError("startup cleanup proof inventory entry is malformed")
        if (
            not isinstance(item.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
        ):
            raise RunnerError("startup cleanup proof hash is invalid")
        proof_path = authority["operator"] / item["path"]
        if proof_path.exists() or proof_path.is_symlink():
            cleanup, cleanup_sha = _historical_read_artifact(proof_path, MIB)
            if cleanup_sha != item["sha256"]:
                raise RunnerError("warehouse spill cleanup proof changed after readback")
            runtime = authority["operator"] / item["runtime"]
            if stage_exists:
                match = warehouse._historical_dbt_runtime_matches(
                    authority["store"], runtime, stage_relative, authority["identity"]["capture_id"]
                )
                if match is None:
                    raise RunnerError(
                        "warehouse cleanup runtime no longer matches the selected stage"
                    )
                spill, profiles_sha256, database_path = match
                ownership = {
                    "root": str(authority["store"].root.resolve()),
                    "capture_id": authority["identity"]["capture_id"],
                    "stage": stage_relative,
                    "runtime": item["runtime"],
                    "database": database_path,
                    "spill": str(spill),
                    "profiles_sha256": profiles_sha256,
                }
            else:
                ownership = cleanup.get("binding")
            if (
                not isinstance(ownership, dict)
                or ownership.get("root") != str(authority["store"].root.resolve())
                or ownership.get("capture_id") != authority["identity"]["capture_id"]
                or ownership.get("stage") != stage_relative
                or ownership.get("runtime") != item["runtime"]
                or ownership.get("spill") != str(authority["operator"] / item["spill"])
            ):
                raise RunnerError("warehouse spill cleanup proof is bound to another stage")
            result = historical_resume.validate_proof(
                cleanup,
                "abandoned-dbt-spill-cleanup",
                ownership,
            )
            files = result.get("files")
            if (
                not isinstance(files, list)
                or len(files) != item["file_count"]
                or result.get("total_bytes") != item["total_bytes"]
                or grants.sha256_json(files) != item["inventory_sha256"]
            ):
                raise RunnerError("warehouse spill cleanup proof inventory changed")
            for file in files:
                relative = file.get("path") if isinstance(file, dict) else None
                if (
                    not isinstance(relative, str)
                    or not relative.startswith(item["spill"] + "/")
                    or type(file.get("bytes")) is not int
                    or not isinstance(file.get("sha256"), str)
                    or re.fullmatch(r"[0-9a-f]{64}", file["sha256"]) is None
                ):
                    raise RunnerError("warehouse spill cleanup file record is malformed")
                existing = spill_files.get(relative)
                record = (file["bytes"], file["sha256"])
                if existing is not None and existing != record:
                    raise RunnerError("warehouse spill cleanup attempts disagree on file identity")
                spill_files[relative] = record
                target = authority["operator"] / relative
                if stage_exists and (target.exists() or target.is_symlink()):
                    raise RunnerError("audited abandoned spill file reappeared")
        elif stage_exists:
            raise RunnerError("warehouse spill cleanup proof disappeared while its stage remains")
    removed = proof.get("removed_files")
    expected_removed = [
        {"path": path, "bytes": value[0], "sha256": value[1]}
        for path, value in sorted(spill_files.items())
    ]
    if stage_exists and removed != expected_removed:
        raise RunnerError("startup cleanup removed-file inventory differs from warehouse proofs")
    if not stage_exists:
        if not isinstance(removed, list) or len(removed) > 65_536:
            raise RunnerError("retired startup cleanup inventory is invalid")
        for item in removed:
            if (
                not isinstance(item, dict)
                or set(item) != {"path", "bytes", "sha256"}
                or not isinstance(item.get("path"), str)
                or re.fullmatch(r"tmp/dbt-[0-9a-f]{32}/spill/.+", item["path"]) is None
                or type(item.get("bytes")) is not int
                or item["bytes"] < 0
                or not isinstance(item.get("sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
            ):
                raise RunnerError("retired startup cleanup file inventory is malformed")
    if type(proof.get("completed_at_epoch")) not in {int, float} or not math.isfinite(
        proof["completed_at_epoch"]
    ):
        raise RunnerError("startup cleanup completion time is invalid")
    return proof


def _historical_startup_maintenance(authority: dict[str, Any]) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import historical_resume, warehouse

    proof_path = _historical_startup_maintenance_path(authority)
    if proof_path.exists() or proof_path.is_symlink():
        return _historical_read_startup_maintenance(authority)
    reconciliation = _historical_reconciliation_audit(authority)
    store = authority["store"]
    identity = authority["identity"]
    stage_relative = historical_resume.stage_relative(identity["stage_name"])
    stage_path = store.path(stage_relative)
    device = store.root.lstat().st_dev
    stage_identity = warehouse._validate_historical_stage(stage_path, device)

    def inventory() -> dict[str, Any]:
        workspace = _live_workspace_snapshot(
            authority["operator"],
            authority["directory"],
            identity["capture_id"],
            identity["root_id"],
        )
        usage = store.usage()
        external = _external_bytes(authority["directory"])
        return {
            "retained_bytes": usage["retained_bytes"],
            "temporary_bytes": usage["temporary_bytes"],
            "database_bytes": usage["database_bytes"],
            "external_bytes": external,
            "aggregate_bytes": usage["retained_bytes"] + external,
            "free_bytes": shutil.disk_usage(authority["directory"]).free,
            "inventory_entries": len(workspace["operator_entries"]),
            "workspace": {
                key: workspace[key]
                for key in (
                    "logical_bytes",
                    "allocated_bytes",
                    "database_bytes",
                    "temporary_bytes",
                    "tmp_names",
                    "build_names",
                    "release_names",
                )
            },
        }

    with store.writer_guard():
        if not store.writer_locked_by_current_thread:
            raise RunnerError("startup cleanup failed to acquire the canonical writer guard")
        store.reconcile()
        before = inventory()
        candidates: list[tuple[Path, Path, str, str]] = []
        temporary = store.path("tmp")
        try:
            with os.scandir(temporary) as entries:
                runtimes = list(entries)
        except FileNotFoundError:
            runtimes = []
        if len(runtimes) > 4096:
            raise RunnerError("startup cleanup found too many temporary runtime entries")
        for entry in runtimes:
            if re.fullmatch(r"dbt-[0-9a-f]{32}", entry.name) is None:
                continue
            runtime = Path(entry.path)
            match = warehouse._historical_dbt_runtime_matches(
                store, runtime, stage_relative, identity["capture_id"]
            )
            if match is None:
                continue
            spill, profiles_sha256, database_path = match
            allowed_databases = {
                str(stage_path / "warehouse.duckdb"),
                str(stage_path / "resume-candidate" / "warehouse.duckdb"),
            }
            if database_path not in allowed_databases:
                raise RunnerError("startup cleanup runtime targets another historical stage")
            candidates.append((runtime, spill, profiles_sha256, database_path))
        if len(candidates) > 16:
            raise RunnerError("startup cleanup exceeds its fixed abandoned-runtime bound")
        if before["temporary_bytes"] > int(_effective_limits()["temporary_bytes"]):
            raise RunnerError("startup cleanup baseline exceeds the fixed 192 GiB hard ceiling")
        if (
            not candidates
            and before["temporary_bytes"]
            > grants.expected_resource_admission()["storage_temporary_admission_bytes"]
        ):
            raise RunnerError("over-admission temporary storage has no exact selected-stage spill")
        audit_before = _historical_reconciliation_audit(authority)
        if audit_before != reconciliation:
            raise RunnerError("stopped-worker reconciliation changed before startup cleanup")
        warehouse._cleanup_resumed_historical_dbt_spills(
            store, identity["capture_id"], stage_relative
        )
        store.reconcile()
        after = inventory()
        if warehouse._validate_historical_stage(stage_path, device) != stage_identity:
            raise RunnerError("startup cleanup changed the exact selected historical stage")
        if after["temporary_bytes"] > int(
            grants.expected_resource_admission()["storage_temporary_admission_bytes"]
        ):
            raise RunnerError(
                "audited startup cleanup did not restore processing storage admission"
            )
        if after["temporary_bytes"] >= before["temporary_bytes"] and candidates:
            raise RunnerError("audited startup cleanup did not reduce temporary storage")
        audit_after = _historical_reconciliation_audit(authority)
        if audit_after != reconciliation:
            raise RunnerError("stopped-worker reconciliation changed during startup cleanup")

        cleanup_records = []
        all_removed: dict[str, tuple[int, str]] = {}
        for runtime, spill, profiles_sha256, database_path in candidates:
            ownership = {
                "root": str(store.root.resolve()),
                "capture_id": identity["capture_id"],
                "stage": stage_relative,
                "runtime": runtime.relative_to(store.root).as_posix(),
                "database": database_path,
                "spill": str(spill),
                "profiles_sha256": profiles_sha256,
            }
            try:
                with os.scandir(runtime) as entries:
                    proof_entries = [
                        Path(entry.path)
                        for entry in entries
                        if entry.name.startswith("historical-spill-cleanup")
                    ]
            except OSError as exc:
                raise RunnerError("warehouse spill cleanup audit could not be inventoried") from exc
            for cleanup_path in proof_entries:
                cleanup, cleanup_sha = _historical_read_artifact(cleanup_path, MIB)
                result = historical_resume.validate_proof(
                    cleanup, "abandoned-dbt-spill-cleanup", ownership
                )
                files = result.get("files")
                total_bytes = result.get("total_bytes")
                if (
                    not isinstance(files, list)
                    or type(total_bytes) is not int
                    or total_bytes != sum(item.get("bytes", -1) for item in files)
                    or any(
                        not isinstance(item, dict)
                        or not isinstance(item.get("path"), str)
                        or not item["path"].startswith(
                            spill.relative_to(store.root).as_posix() + "/"
                        )
                        or type(item.get("bytes")) is not int
                        or item["bytes"] < 0
                        or not isinstance(item.get("sha256"), str)
                        or re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is None
                        for item in files
                    )
                ):
                    raise RunnerError(
                        "warehouse spill cleanup proof failed exact inventory readback"
                    )
                cleanup_records.append(
                    {
                        "path": cleanup_path.relative_to(store.root).as_posix(),
                        "sha256": cleanup_sha,
                        "runtime": ownership["runtime"],
                        "spill": spill.relative_to(store.root).as_posix(),
                        "file_count": len(files),
                        "total_bytes": total_bytes,
                        "inventory_sha256": grants.sha256_json(files),
                    }
                )
                for item in files:
                    relative = item["path"]
                    prior = all_removed.get(relative)
                    record = (item["bytes"], item["sha256"])
                    if prior is not None and prior != record:
                        raise RunnerError("warehouse cleanup proofs disagree on spill identity")
                    all_removed[relative] = record
                    target = store.path(relative)
                    if target.exists() or target.is_symlink():
                        raise RunnerError("warehouse spill cleanup left an audited file in place")
            if not proof_entries:
                raise RunnerError("warehouse spill cleanup did not leave durable audit evidence")
            stack = [spill]
            while stack:
                parent = stack.pop()
                if parent.is_symlink() or parent.lstat().st_dev != device:
                    raise RunnerError("warehouse spill path became unsafe during cleanup")
                with os.scandir(parent) as entries:
                    for item in entries:
                        details = item.stat(follow_symlinks=False)
                        if details.st_dev != device or stat.S_ISLNK(details.st_mode):
                            raise RunnerError("warehouse spill path contains an unsafe entry")
                        if stat.S_ISDIR(details.st_mode):
                            stack.append(Path(item.path))
                        else:
                            raise RunnerError("audited abandoned spill cleanup left a file behind")

        if len(cleanup_records) > 64:
            raise RunnerError("startup cleanup audit inventory exceeded its fixed bound")
        body = {
            "version": 1,
            "identity": identity,
            "grant_id": authority["grant"]["grant_id"],
            "launch_sha256": authority["launch_sha256"],
            "source_snapshot_sha256": authority["source_snapshot"]["sha256"],
            "reconciliation": reconciliation,
            "stage_identity": list(stage_identity),
            "pre_inventory": before,
            "cleanup_proofs": sorted(cleanup_records, key=lambda item: item["path"]),
            "removed_files": [
                {"path": path, "bytes": value[0], "sha256": value[1]}
                for path, value in sorted(all_removed.items())
            ],
            "post_inventory": after,
            "completed_at_epoch": time.time(),
        }
        grants.write_once_json(proof_path, body | {"sha256": grants.sha256_json(body)})
    return _historical_read_startup_maintenance(authority)


def _historical_worker_action(
    authority: dict[str, Any], phase: str, substep: str
) -> dict[str, Any]:
    import historical_grants as grants

    from kalshi_events import historical_resume, publish, warehouse
    from kalshi_events.common import digest, regular_bytes

    control_root = authority["control_root"]
    grant_id = authority["grant"]["grant_id"]
    identity = authority["identity"]
    store = authority["store"]
    verified_mount = authority.get("verified_mount")
    capture_id = identity["capture_id"]
    stage_name = identity["stage_name"]
    stage_relative = historical_resume.stage_relative(stage_name)
    stage_path = store.path(stage_relative)

    if phase == "source" and substep == "maintenance":
        proof = _historical_startup_maintenance(authority)
        return {"startup_maintenance_sha256": proof["sha256"]}

    if phase == "source" and substep == "snapshot":
        _historical_read_startup_maintenance(authority)
        snapshot = authority["source_snapshot"]
        source_path = control_root / "source" / "source-snapshot.json"
        try:
            grants.write_once_json(source_path, snapshot)
        except grants.ControlError:
            saved, _ = _historical_read_artifact(source_path, 4 * MIB)
            if saved != snapshot:
                raise RunnerError(
                    "saved source snapshot differs from current read-only proof"
                ) from None
        census = _historical_census_proof(authority, full_revalidation_if_missing=True)
        return {
            "source_snapshot_sha256": snapshot["sha256"],
            "census_sha256": grants.sha256_json(census),
        }

    if phase == "readiness":
        if substep == "prepare":
            summary = warehouse.prepare_historical_readiness(
                store, capture_id, stage_name, _verified_mount=verified_mount
            )
            if (
                summary.get("ready") is not True
                or summary.get("capture_id") != capture_id
                or summary.get("stage") != stage_name
                or summary.get("observation_count")
                != authority["control"]["authorized_census"]["exact_observations"]
            ):
                raise RunnerError(
                    "historical readiness preparation did not satisfy the frozen census"
                )
            return summary
        verification = warehouse.verify_historical_readiness(
            store, capture_id, stage_name, _verified_mount=verified_mount
        )
        if (
            verification.get("phase") != "historical-readiness-verification"
            or verification.get("binding", {}).get("capture_id") != capture_id
            or verification.get("binding", {}).get("stage") != stage_name
            or verification.get("result", {}).get("observation_count")
            != authority["control"]["authorized_census"]["exact_observations"]
        ):
            raise RunnerError("independent readiness verification differs from the frozen census")
        receipt_path = control_root / "grants" / grant_id / "readiness-verification.json"
        grants.write_once_json(receipt_path, verification)
        _historical_write_readiness_phase_origin(authority, verification)
        return {"verification_sha256": verification["sha256"]}

    if phase == "adoption":
        if substep == "verify":
            verification = warehouse.verify_historical_readiness(
                store, capture_id, stage_name, _verified_mount=verified_mount
            )
            if (
                verification.get("result", {}).get("observation_count")
                != authority["control"]["authorized_census"]["exact_observations"]
            ):
                raise RunnerError("historical adoption verification differs from the exact census")
            grants.write_once_json(
                control_root / "grants" / grant_id / "adoption-verification.json",
                verification,
            )
            return {"verification_sha256": verification["sha256"]}
        verification_path = control_root / "grants" / grant_id / "adoption-verification.json"
        verification, _ = _historical_read_artifact(verification_path, 4 * MIB)
        adoption_path = stage_path / "resume-adoption.json"
        existing_adoption = warehouse._load_self_proof(adoption_path, "historical-adoption")
        origin_verification_sha = (
            existing_adoption[0].get("binding", {}).get("verification_sha256")
            if existing_adoption is not None
            else verification.get("sha256")
        )
        claim = _historical_resolve_adoption_origin_claim(
            authority,
            origin_verification_sha,
            create_if_missing=existing_adoption is None,
        )
        approved_retirement = authority["approval"].get("retire_source") is True
        adoption = warehouse.adopt_historical_readiness(
            store,
            capture_id,
            stage_name,
            verification,
            retire_source=approved_retirement,
            retirement_policy=(
                "coexistence-capacity-relief-approved-v1" if approved_retirement else None
            ),
            _verified_mount=verified_mount,
        )
        adoption_loaded = warehouse._load_self_proof(adoption_path, "historical-adoption")
        if adoption_loaded is None or adoption_loaded[0].get("sha256") != adoption.get(
            "adoption_sha256"
        ):
            raise RunnerError("historical adoption origin failed immutable readback")
        adoption_proof = adoption_loaded[0]
        _historical_write_adoption_origin(authority, claim, adoption_proof)
        retirement_sha = None
        if approved_retirement:
            retirement = _historical_write_source_retirement_receipt(
                authority, adoption, verification, phase="adoption"
            )
            retirement_sha = retirement["sha256"]
        return {
            "adoption_sha256": adoption_proof["sha256"],
            "source_retired": adoption.get("source_retired") is True,
            "source_retirement_sha256": retirement_sha,
        }

    if phase == "models" and substep in {"prepare", "verify", "adopt"}:
        if substep == "prepare":
            summary = warehouse.prepare_historical_readiness(
                store, capture_id, stage_name, _verified_mount=verified_mount
            )
            if (
                summary.get("ready") is not True
                or summary.get("capture_id") != capture_id
                or summary.get("stage") != stage_name
                or summary.get("observation_count")
                != authority["control"]["authorized_census"]["exact_observations"]
            ):
                raise RunnerError(
                    "models retirement preparation failed current candidate and exact-census checks"
                )
            return summary
        verification_path = control_root / "grants" / grant_id / "models-adoption-verification.json"
        if substep == "verify":
            verification = warehouse.verify_historical_readiness(
                store, capture_id, stage_name, _verified_mount=verified_mount
            )
            if (
                verification.get("phase") != "historical-readiness-verification"
                or verification.get("binding", {}).get("capture_id") != capture_id
                or verification.get("binding", {}).get("stage") != stage_name
                or verification.get("result", {}).get("observation_count")
                != authority["control"]["authorized_census"]["exact_observations"]
            ):
                raise RunnerError("models retirement verification failed the exact census")
            grants.write_once_json(verification_path, verification)
            return {"verification_sha256": verification["sha256"]}
        if (
            authority["approval"].get("retire_source") is not True
            or authority["approval"].get("retirement_policy")
            != "coexistence-capacity-relief-approved-v1"
        ):
            raise RunnerError("models source retirement is outside the approved grant")
        verification, _ = _historical_read_artifact(verification_path, 4 * MIB)
        adoption = warehouse.adopt_historical_readiness(
            store,
            capture_id,
            stage_name,
            verification,
            retire_source=True,
            retirement_policy="coexistence-capacity-relief-approved-v1",
            _verified_mount=verified_mount,
        )
        receipt = _historical_write_source_retirement_receipt(
            authority, adoption, verification, phase="models"
        )
        return {
            "adoption_sha256": adoption.get("adoption_sha256"),
            "source_retired": adoption.get("source_retired") is True,
            "source_retirement_sha256": receipt["sha256"],
        }

    if phase == "models" and substep == "build":
        build_path = store.path(f"builds-historical/{capture_id}/build.json")
        if build_path.is_symlink():
            raise RunnerError("installed historical build receipt is a symlink")
        if build_path.is_file():
            # Install may have completed before a worker or phase seal was interrupted.
            # Certify that durable result; never enter the fresh landing fallback here.
            pass
        elif not build_path.exists():
            if stage_path.is_symlink() or not stage_path.is_dir():
                raise RunnerError("historical build requires the exact validated pending stage")
            warehouse._validate_historical_stage(stage_path, store.root.lstat().st_dev)
            warehouse.build_historical(
                store,
                capture_id,
                resume_historical_stage=stage_name,
                _verified_mount=verified_mount,
            )
        else:
            raise RunnerError("installed historical build receipt is not a regular file")
        build, build_receipt_sha256 = _historical_read_artifact(build_path, MIB)
        exact_observations = authority["control"]["authorized_census"]["exact_observations"]
        if (
            build.get("capture_id") != capture_id
            or build.get("catalogue_version") != 3
            or build.get("observation_count") != exact_observations
        ):
            raise RunnerError("installed historical build differs from the authorized exact census")
        adoption = build.get("historical_adoption")
        if (
            not isinstance(adoption, dict)
            or adoption.get("stage") != stage_name
            or adoption.get("source_sha256")
            != authority["source_snapshot"]["result"].get("source_sha256")
            or adoption.get("source_wal_sha256")
            != authority["source_snapshot"]["result"].get("source_wal_sha256")
            or not isinstance(adoption.get("candidate_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", adoption["candidate_sha256"]) is None
        ):
            raise RunnerError("installed historical build lost its validated adoption lineage")
        certified = warehouse.certified(store, capture_id, catalogue_version=3)
        if (
            certified.get("capture_id") != capture_id
            or certified.get("observation_count") != exact_observations
            or certified.get("historical_adoption") != adoption
        ):
            raise RunnerError(
                "installed historical build certification differs from its frozen lineage"
            )
        artifact_path = build_path.parent / "build-artifacts.json"
        model_artifact_sha256 = digest(regular_bytes(artifact_path, 64 * MIB))
        if model_artifact_sha256 != certified.get("dbt_artifacts_sha256"):
            raise RunnerError("installed historical model artifacts failed certified readback")
        return {
            "build_receipt_sha256": build_receipt_sha256,
            "warehouse_sha256": certified["warehouse_sha256"],
            "dbt_artifacts_sha256": model_artifact_sha256,
        }

    if phase == "certified-build" and substep == "certify":
        build_path = store.path(f"builds-historical/{capture_id}/build.json")
        if not build_path.is_file() or build_path.is_symlink():
            raise RunnerError("certification requires an installed historical build")
        build, _ = _historical_read_artifact(build_path)
        if (
            build.get("observation_count")
            != authority["control"]["authorized_census"]["exact_observations"]
        ):
            raise RunnerError("certified build differs from the authorized observation census")
        certified = warehouse.certified(store, capture_id, catalogue_version=3)
        return {"certified_receipt_sha256": grants.sha256_json(certified)}

    if phase == "export" and substep == "publish":
        build_path = store.path(f"builds-historical/{capture_id}/build.json")
        if not build_path.is_file() or build_path.is_symlink():
            raise RunnerError("historical publication requires an installed build")
        build, _ = _historical_read_artifact(build_path)
        if (
            build.get("observation_count")
            != authority["control"]["authorized_census"]["exact_observations"]
        ):
            raise RunnerError("historical publication build differs from exact authorized census")
        warehouse.certified(store, capture_id, catalogue_version=3)
        result = publish.publish(store, capture_id, catalogue_version=3)
        if result.get("capture_id") != capture_id:
            raise RunnerError("historical publication returned a different capture")
        return {"release_id": result.get("source_release_id")}

    if phase == "verified-publication" and substep == "verify":
        current, _ = _historical_published_release(authority)
        return {"release_id": current["source_release_id"]}

    if phase == "independent-verification" and substep == "verify":
        receipt = _historical_release_verification(authority, grant_id_for_receipt=grant_id)
        return {"verification_receipt_sha256": receipt["verification_receipt_sha256"]}

    if phase == "activation" and substep == "activate":
        current, manifest = _historical_published_release(authority)
        independent_path = control_root / "verifications" / f"{grant_id}.json"
        # Activation may be a separate grant from independent verification.
        phase_path = control_root / "phases" / "independent-verification.json"
        independent_seal, _ = _historical_read_artifact(phase_path)
        prior_grant = independent_seal["grant_id"]
        independent_path = control_root / "verifications" / f"{prior_grant}.json"
        receipt, _receipt_file_sha = _historical_read_artifact(independent_path)
        receipt_sha = grants.sha256_json(receipt)
        if (
            receipt_sha != independent_seal["outputs"]["verification_receipt_sha256"]
            or receipt.get("capture_id") != identity["capture_id"]
            or receipt.get("release_id") != current["source_release_id"]
            or receipt.get("release_manifest_sha256") != current["manifest_sha256"]
            or receipt.get("coverage_scope") != "historical"
            or type(receipt.get("source_capture_complete")) is not bool
            or receipt.get("source_capture_complete") is not manifest.get("source_capture_complete")
            or receipt.get("scope_complete") is not True
            or manifest.get("coverage_scope") != "historical"
            or manifest.get("historical_contract") != "oddsfox.kalshi.historical.v1"
        ):
            raise RunnerError("activation requires the sealed independent verification receipt")
        audit_binding = authority["report"].get("activation_audit")
        if not isinstance(audit_binding, dict):
            raise RunnerError("activation requires the session's independent audit binding")
        from production_activation import _activation_audit_payload

        audit = _activation_audit_payload(
            authority["directory"],
            audit_binding,
            expected_session_id=identity["session_id"],
        )
        census = authority["control"]["authorized_census"]
        if (
            audit_binding.get("sha256") != authority["approval"].get("activation_audit_sha256")
            or audit.get("capture_id") != census["capture_id"]
            or audit.get("root_id") != census["root_id"]
            or audit.get("capture_state_sha256") != census["capture_state_sha256"]
            or audit.get("receipt_index_sha256") != census["receipt_index_sha256"]
            or audit.get("population_report_sha256") != census["population_report_sha256"]
            or audit.get("observations_by_endpoint") != census["observations_by_endpoint"]
            or audit.get("exact_observations") != census["exact_observations"]
        ):
            raise RunnerError("activation audit differs from the bound exact census")
        audits = {
            seal["outputs"]["activation_audit_sha256"]
            for seal in [
                _historical_read_artifact(control_root / "phases" / f"{phase_name}.json")[0]
                for phase_name in grants.PHASES
                if (control_root / "phases" / f"{phase_name}.json").is_file()
            ]
            if seal["phase"] == "activation"
        }
        grants.claim_activation_audit(
            control_root,
            audit_sha256=audit_binding["sha256"],
            grant_id=grant_id,
            launch_id=authority["launch"]["launch_id"],
            identity=identity,
            prior_audits=audits,
        )
        report_path = authority["report_path"]
        authority["report"]["production_state"] = "attempting"
        _atomic_report(report_path, authority["report"])
        expected = f"historical:{current['source_release_id']}"
        capture_output = __import__("io").StringIO()
        with contextlib.redirect_stdout(capture_output):
            code = _activation_worker(
                authority["operator"],
                authority["directory"],
                expected,
                report_path,
                "expanded",
                (
                    float(authority["grant"]["deadline_epoch"]),
                    float(authority["grant"]["deadline_monotonic"]),
                ),
            )
        raw_result = capture_output.getvalue().strip()
        try:
            activation = json.loads(raw_result)
        except (TypeError, ValueError) as exc:
            raise RunnerError("historical activation worker returned invalid readback") from exc
        if code != 0 or not isinstance(activation, dict) or activation.get("state") != "verified":
            raise RunnerError("historical activation did not complete a verified readback")
        result_path = control_root / "activations" / f"{grant_id}.json"
        grants.write_once_json(result_path, activation)
        return {
            "activation_audit_sha256": audit_binding["sha256"],
            "production_database_sha256": activation.get("warehouse_sha256"),
            "relation_rows": activation.get("relation_rows"),
        }

    raise RunnerError("private historical phase action is not implemented")


def _historical_worker_main(
    directory: Path,
    control_root: Path,
    grant_id: str,
    launch_sha256: str,
    phase: str,
    substep: str,
    storage_invocation_token: str,
    storage_progress_path: Path,
) -> int:
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    import historical_grants as grants

    try:
        directory = _external_path(directory)
        control_root = _historical_control_root(control_root)
        handoff = _historical_consume_supervisor_handoff(
            directory,
            control_root,
            grant_id,
            launch_sha256,
            phase,
            substep,
            storage_invocation_token,
            storage_progress_path,
        )
        # The supervisor writes active_process_group after spawn; allow only that bounded handoff race.
        authority = None
        last_error: Exception | None = None
        for _ in range(50):
            bundle = grants.read_control_bundle(_historical_control_root(control_root), grant_id)
            session_report_path = (
                Path(directory) / "runs" / bundle["identity"]["session_id"] / REPORT_NAME
            )
            handoff_report = _strict_json(session_report_path)
            if (
                handoff_report is None
                or handoff_report.get("active_process_group") != os.getpid()
                or os.getpgrp() != os.getpid()
            ):
                time.sleep(0.1)
                continue
            try:
                authority = _historical_authority(
                    directory,
                    control_root,
                    grant_id,
                    launch_sha256=launch_sha256,
                    require_worker=True,
                    expected_phase=phase,
                    expected_substep=substep,
                    storage_invocation_token=storage_invocation_token,
                    storage_progress_path=storage_progress_path,
                    supervisor_pid=handoff["supervisor_pid"],
                )
                break
            except RunnerError as exc:
                last_error = exc
                raise
        if authority is None:
            raise last_error or RunnerError("supervised worker handoff did not complete")
        _historical_validate_phase_chain(authority)
        substep_path = _historical_substep_path(authority["control_root"], grant_id, phase, substep)
        if substep_path.exists() or substep_path.is_symlink():
            raise RunnerError("private historical substep launch was already consumed")
        ordered = _historical_substeps_for_grant(
            phase, authority["approval"], authority["store"], authority["identity"]["capture_id"]
        )
        substep_index = ordered.index(substep)
        for prior_substep in ordered[:substep_index]:
            prior = _historical_substep_path(
                authority["control_root"], grant_id, phase, prior_substep
            )
            if not prior.is_file() or prior.is_symlink():
                raise RunnerError("private historical worker substep was invoked out of order")
        token = _ACTIVE_LIMITS.set(_limits_from_report(authority["report"], "expanded"))
        heartbeat = _HistoricalStorageHeartbeat(
            directory,
            authority["report_path"],
            storage_progress_path,
            storage_invocation_token,
        )
        try:
            heartbeat.start()
            output = _historical_worker_action(authority, phase, substep)
            heartbeat.check()
        finally:
            heartbeat.close()
            _ACTIVE_LIMITS.reset(token)
        body = {
            "version": 1,
            "identity": authority["identity"],
            "grant_id": grant_id,
            "launch_sha256": launch_sha256,
            "phase": phase,
            "substep": substep,
            "completed_at_epoch": time.time(),
            "output_sha256": grants.sha256_json(output),
        }
        with grants.control_lock(authority["control_root"]):
            grants.write_once_json(substep_path, body)
        print(json.dumps({"ok": True, "phase": phase, "substep": substep}, sort_keys=True))
        return 0
    except Exception as exc:
        if phase == "activation":
            with contextlib.suppress(Exception):
                control_root = _historical_control_root(control_root)
                bundle = grants.read_control_bundle(control_root, grant_id)
                report_path = (
                    Path(directory) / "runs" / bundle["identity"]["session_id"] / REPORT_NAME
                )
                report = _strict_json(report_path)
                handoff = report.get("private_historical_grant") if report is not None else None
                if (
                    _historical_activation_claimed(
                        control_root,
                        grant_id,
                        bundle["launch"]["launch_id"],
                        bundle["identity"],
                    )
                    and isinstance(handoff, dict)
                    and handoff.get("grant_id") == grant_id
                    and handoff.get("launch_sha256") == launch_sha256
                ):
                    report["production_state"] = "unknown"
                    _atomic_report(report_path, report)
        print(json.dumps({"error": str(exc), "phase": phase, "substep": substep}, sort_keys=True))
        return 1


def _historical_stop_record(
    directory: Path,
    control_root: Path,
    grant_id: str,
    launch_sha256: str,
    *,
    outcome: str,
    phase: str,
    substep: str | None,
    completed_phases: list[str],
    reason: str | None,
    bindings: dict[str, Any],
    root_volume_uuid: str | None = None,
) -> None:
    import shlex

    import historical_grants as grants

    current, durable_sha256 = grants.read_phase_chain(control_root / "phases", bindings)
    try:
        next_phase = grants.next_phase(current)
    except grants.ControlError:
        next_phase = None
    remaining_phases = (
        grants.PHASES[grants.PHASES.index(next_phase) :] if next_phase is not None else []
    )
    proposed_grant_id = uuid.uuid4().hex if next_phase is not None else None
    interpreter = str(_historical_venv_root(control_root) / "bin" / "python")
    script = str(_historical_source_root(control_root) / "scripts" / "production_run.py")
    same_boot_reconciliation_argv = None
    changed_boot_reconciliation_argv = None
    if next_phase is None:
        next_argv = None
    elif outcome == "uncertain":
        same_boot_reconciliation_argv = [
            interpreter,
            "-B",
            script,
            "--directory",
            str(directory.resolve()),
            "--capture-id",
            bindings["capture_id"],
            "--historical",
            "--reconcile-stopped-worker",
        ]
        if (
            not isinstance(root_volume_uuid, str)
            or re.fullmatch(r"[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}", root_volume_uuid)
            is None
        ):
            raise RunnerError("uncertain historical attempt has no frozen volume UUID")
        changed_boot_reconciliation_argv = [
            interpreter,
            "-B",
            script,
            "--directory",
            str(directory.resolve()),
            "--capture-id",
            bindings["capture_id"],
            "--historical",
            "--reconcile-accounting",
            "--recover-reboot",
            "--recovery-volume-uuid",
            root_volume_uuid,
        ]
        next_argv = same_boot_reconciliation_argv
    else:
        next_argv = [
            interpreter,
            "-B",
            script,
            "--_historical-control",
            str(directory.resolve()),
            str(control_root.resolve()),
            proposed_grant_id,
        ]
    body = {
        "version": 1,
        "grant_id": grant_id,
        "launch_sha256": launch_sha256,
        "outcome": outcome,
        "phase": phase,
        "substep": substep,
        "completed_phases": completed_phases,
        "last_durable_phase": current,
        "last_durable_phase_sha256": durable_sha256,
        "next_phase": next_phase,
        "remaining_phases": remaining_phases,
        "proposed_grant_id": proposed_grant_id,
        "proposed_grant_status": "unapproved" if proposed_grant_id is not None else None,
        "next_command_argv": next_argv,
        "next_command": shlex.join(next_argv) if next_argv is not None else None,
        "same_boot_reconciliation_command_argv": same_boot_reconciliation_argv,
        "changed_boot_reconciliation_command_argv": changed_boot_reconciliation_argv,
        "reconciliation_required": outcome == "uncertain",
        "new_approval_required": next_phase is not None,
        "reason": reason,
        "written_at_epoch": time.time(),
    }
    with grants.control_lock(control_root):
        grants.write_once_json(control_root / "grants" / grant_id / "attempt-result.json", body)


def _historical_private_controller_command_matches(
    command: str,
    *,
    expected_python: Path,
    control_root: Path,
    directory: Path,
    grant_id: str,
) -> bool:
    script = str(_historical_source_root(control_root) / "scripts" / "production_run.py")
    pinned_python = _historical_venv_root(control_root) / "bin" / "python"
    return (
        command
        == " ".join(
            (
                str(pinned_python),
                "-B",
                script,
                "--_historical-control",
                str(directory),
                str(control_root),
                grant_id,
            )
        )
        and expected_python == pinned_python
    )


def _historical_consume_supervisor_handoff(
    directory: Path,
    control_root: Path,
    grant_id: str,
    launch_sha256: str,
    phase: str,
    substep: str,
    storage_invocation_token: str,
    storage_progress_path: Path,
) -> dict[str, Any]:
    import select

    raw_fd = os.environ.pop(HISTORICAL_SUPERVISOR_FD_ENV, None)
    if not isinstance(raw_fd, str) or re.fullmatch(r"(?:[3-9]|[1-9][0-9]+)", raw_fd) is None:
        raise RunnerError("private historical worker has no parent-issued supervisor pipe")
    descriptor = int(raw_fd)
    chunks: list[bytes] = []
    total = 0
    deadline = time.monotonic() + 5.0
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
                raise RunnerError("parent-issued supervisor handoff timed out")
            chunk = os.read(descriptor, min(4096, 8193 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > 8192:
                raise RunnerError("parent-issued supervisor handoff is oversized")
    except OSError as exc:
        raise RunnerError("parent-issued supervisor handoff is unreadable") from exc
    finally:
        with contextlib.suppress(OSError):
            os.close(descriptor)

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    try:
        value = json.loads(
            b"".join(chunks).decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(item)),
        )
    except (UnicodeError, ValueError) as exc:
        raise RunnerError("parent-issued supervisor handoff is malformed") from exc
    fields = {
        "version",
        "nonce",
        "directory",
        "control_root",
        "grant_id",
        "launch_sha256",
        "phase",
        "substep",
        "storage_invocation_token",
        "storage_progress_path",
        "supervisor_pid",
        "supervisor_pgid",
        "worker_pid",
        "worker_pgid",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("version") != 1
        or not isinstance(value.get("nonce"), str)
        or re.fullmatch(r"[0-9a-f]{32}", value["nonce"]) is None
        or value.get("directory") != str(directory)
        or value.get("control_root") != str(control_root)
        or value.get("grant_id") != grant_id
        or value.get("launch_sha256") != launch_sha256
        or value.get("phase") != phase
        or value.get("substep") != substep
        or value.get("storage_invocation_token") != storage_invocation_token
        or value.get("storage_progress_path") != str(storage_progress_path)
        or type(value.get("supervisor_pid")) is not int
        or value["supervisor_pid"] != os.getppid()
        or type(value.get("supervisor_pgid")) is not int
        or type(value.get("worker_pid")) is not int
        or value["worker_pid"] != os.getpid()
        or value.get("worker_pgid") != os.getpid()
        or os.getpgrp() != os.getpid()
        or os.getpgid(value["supervisor_pid"]) != value["supervisor_pgid"]
    ):
        raise RunnerError("parent-issued supervisor handoff does not match this worker")
    try:
        expected_python = _historical_venv_root(control_root) / "bin" / "python"
        result = subprocess.run(
            ["/bin/ps", "-p", str(value["supervisor_pid"]), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError("private controller process identity is unavailable") from exc
    command = result.stdout.rstrip("\r\n")
    if result.returncode != 0 or not _historical_private_controller_command_matches(
        command,
        expected_python=expected_python,
        control_root=control_root,
        directory=directory,
        grant_id=grant_id,
    ):
        raise RunnerError("supervised worker parent is not the pinned private controller")
    lock_path = directory / ".production-run.lock"
    try:
        lock_fd = os.open(lock_path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise RunnerError("private controller run lock is unavailable to its worker") from exc
    try:
        lock_info = os.fstat(lock_fd)
        if (
            not stat.S_ISREG(lock_info.st_mode)
            or lock_info.st_nlink != 1
            or lock_info.st_dev != directory.stat().st_dev
        ):
            raise RunnerError("private controller run lock is unsafe")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            raise RunnerError("private controller did not retain the canonical run lock")
    finally:
        os.close(lock_fd)
    return value


def _historical_control_main(directory: Path, control_root: Path, grant_id: str) -> int:
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    import historical_grants as grants

    directory = _external_path(directory)
    control_root = _historical_control_root(control_root)
    launch = None
    launch_sha256 = None
    report = None
    report_path = None
    paths = None
    completed: list[str] = []
    current_phase = "source"
    current_substep: str | None = None
    token = None
    run_lock_context = None
    run_lock_acquired = False
    try:
        control_root = _historical_control_root(control_root)
        _historical_assert_pinned_python(control_root)
        run_lock_context = _run_lock(directory)
        run_lock_context.__enter__()
        run_lock_acquired = True
        preflight = _historical_authority(
            directory,
            control_root,
            grant_id,
            launch_sha256=None,
            require_worker=False,
            prelaunch=True,
            cheap_only=True,
        )
        approval = preflight["approval"]
        identity = preflight["identity"]
        report = preflight["report"]
        report_path = preflight["report_path"]
        report_hash = grants.sha256_bytes(_historical_session_report_bytes(report_path))
        session_binding = {
            "version": 1,
            "grant_id": grant_id,
            "approval_sha256": grants.sha256_json(approval),
            "capture_id": identity["capture_id"],
            "session_id": identity["session_id"],
            "root_id": identity["root_id"],
            "stage_name": identity["stage_name"],
            "report_sha256": report_hash,
            "report_path": str(report_path),
            "boot_stamp": report["boot_stamp"],
            "root_volume_uuid": _volume_uuid(directory).upper(),
            "root_device": directory.stat().st_dev,
        }
        launch, launch_sha256 = grants.issue_launch(
            control_root,
            expected_identity=identity,
            requested_phase=approval["phase"],
            grant_id=grant_id,
            now_epoch=time.time(),
            now_monotonic=time.monotonic(),
            boot_stamp=report["boot_stamp"],
            session_binding=session_binding,
        )
        token = _ACTIVE_LIMITS.set(_limits_for_profile("expanded"))
        store = _historical_store(
            directory / "operator", max_duration_seconds=MAX_OFFLINE_HISTORICAL_SECONDS
        )
        if _root_id(store) != identity["root_id"]:
            raise RunnerError("private historical operator root identity changed")
        baseline = _historical_resource_baseline(directory, store, report)
        _historical_measurement_artifact(
            control_root,
            grant_id,
            approval,
            identity,
            preflight["freeze"],
            session_report_sha256=session_binding["report_sha256"],
            session_root_device=session_binding["root_device"],
            baseline=baseline,
            require_fresh=True,
            allow_initial_maintenance_overage=(approval["phases"][0] == "source"),
        )
        report["phase"] = "stopped"
        report["active_process_group"] = None
        report["private_historical_grant"] = {
            "version": 1,
            "grant_id": grant_id,
            "launch_sha256": launch_sha256,
            "launch_id": launch["launch_id"],
            "control_root": str(control_root),
            "phases": approval["phases"],
            "phase": approval["phase"],
            "substep": None,
            "controller_pid": os.getpid(),
            "controller_pgid": os.getpgrp(),
            "state": "launched",
        }
        _atomic_report(report_path, report)
        paths = _prepare_run_paths(directory, report_path)
        report["metrics_file"] = str(paths["metrics"])
        report["metrics_progress_file"] = str(paths["metrics_progress"])
        report["storage_progress_file"] = str(paths["storage_progress"])
        _atomic_report(report_path, report)

        freeze = grants.read_frozen_control(control_root, expected_identity=identity)[0]["freeze"]
        operational_source_sha = _historical_operational_source_sha256(control_root)
        runtime_sha = grants.sha256_json(
            {
                "runtime_manifest_sha256": freeze["runtime_manifest_sha256"],
                "operational_source_sha256": operational_source_sha,
            }
        )
        if (
            operational_source_sha != freeze["operational_source_sha256"]
            or runtime_sha != identity["runtime_sha256"]
        ):
            raise RunnerError("frozen operational source identity changed after grant consumption")
        first_phase = approval["phases"][0]
        final_preflight = first_phase in {"independent-verification", "activation"}
        _historical_verify_runtime(
            control_root,
            freeze,
            deadline_epoch=float(
                launch["deadline_epoch"] if final_preflight else launch["workflow_deadline_epoch"]
            ),
            deadline_monotonic=float(
                launch["deadline_monotonic"]
                if final_preflight
                else launch["workflow_deadline_monotonic"]
            ),
        )
        source_root = _historical_source_root(control_root)
        python = _historical_venv_root(control_root) / "bin" / "python"
        if not python.is_file() or (python.is_symlink() and not python.exists()):
            raise RunnerError("pinned private historical Python launcher is unavailable")
        _historical_control_root(control_root)

        for phase in approval["phases"]:
            current_phase = phase
            for substep in _historical_substeps_for_grant(
                phase, approval, store, identity["capture_id"]
            ):
                current_substep = substep
                report = _strict_json(report_path)
                if report is None:
                    raise RunnerError("historical session report disappeared during grant")
                if store is None:
                    raise RunnerError("historical storage supervisor has no reconciled Store")
                store.reconcile()
                storage_baseline = _historical_resource_baseline(directory, store, report)
                report["storage"] = {
                    "operator_retained_bytes": storage_baseline["root_bytes"],
                    "operator_temporary_bytes": storage_baseline["temporary_bytes"],
                    "operator_database_bytes": storage_baseline["database_bytes"],
                    "external_bytes": storage_baseline["aggregate_bytes"]
                    - storage_baseline["root_bytes"],
                    "aggregate_estimate_bytes": storage_baseline["aggregate_bytes"],
                    "ssd_free_bytes": storage_baseline["free_bytes"],
                }
                report["phase"] = "stopped"
                report["active_process_group"] = None
                label = f"historical-{grant_id[:8]}-{phase}-{substep}"
                stdout_path = paths["logs"] / f"{label}.stdout.json"
                stderr_path = paths["logs"] / f"{label}.stderr.log"
                storage_token = uuid.uuid4().hex
                storage_progress_path = paths["logs"] / f"{label}.storage.json"
                report["private_historical_grant"] = {
                    "version": 1,
                    "grant_id": grant_id,
                    "launch_sha256": launch_sha256,
                    "launch_id": launch["launch_id"],
                    "control_root": str(control_root),
                    "phases": approval["phases"],
                    "phase": phase,
                    "substep": substep,
                    "controller_pid": os.getpid(),
                    "controller_pgid": os.getpgrp(),
                    "storage_invocation_token": storage_token,
                    "storage_progress_path": str(storage_progress_path),
                    "state": "supervised-child-pending",
                }
                _atomic_report(report_path, report)
                command = [
                    str(python),
                    "-B",
                    str(source_root / "scripts" / "production_run.py"),
                    "--_historical-worker",
                    str(directory),
                    str(control_root),
                    grant_id,
                    launch_sha256,
                    phase,
                    substep,
                    storage_token,
                    str(storage_progress_path),
                ]
                workflow = phase not in {"independent-verification", "activation"}
                deadline_mono = float(
                    launch["workflow_deadline_monotonic"]
                    if workflow
                    else launch["deadline_monotonic"]
                )
                deadline_epoch = float(
                    launch["workflow_deadline_epoch"] if workflow else launch["deadline_epoch"]
                )
                status = _invoke(
                    command,
                    directory=directory,
                    operator=directory / "operator",
                    report=report,
                    report_path=report_path,
                    deadline=deadline_mono,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    wall_deadline_epoch_override=deadline_epoch,
                    phase_on_start=f"historical-{phase}-{substep}",
                    storage_supervision=True,
                    storage_invocation_token=storage_token,
                    metrics_progress_path=paths.get("metrics_progress"),
                    storage_progress_path=storage_progress_path,
                    storage_maintenance=(phase == "source" and substep == "maintenance"),
                    supervisor_handoff={
                        "directory": str(directory),
                        "control_root": str(control_root),
                        "grant_id": grant_id,
                        "launch_sha256": launch_sha256,
                        "phase": phase,
                        "substep": substep,
                        "storage_invocation_token": storage_token,
                        "storage_progress_path": str(storage_progress_path),
                    },
                    child_source_root=source_root,
                )
                code, reason, reaped = status
                if (
                    code is None
                    and reason
                    and reaped
                    and report.get("active_process_group") is None
                ):
                    raise RunnerError(reason)
                storage_state = _historical_validate_storage_handoff(
                    directory,
                    report_path,
                    storage_progress_path,
                    invocation_token=storage_token,
                )
                if (
                    report.get("storage_supervision_invocation") != storage_token
                    or storage_state.get("sample_count", 0) < 1
                ):
                    raise RunnerError("historical worker did not receive live storage supervision")
                report["storage_supervision_file"] = str(storage_progress_path)
                report["storage_supervision"] = {
                    key: value for key, value in storage_state.items() if key != "baseline"
                }
                _atomic_report(report_path, report)
                latest = _strict_json(report_path)
                if latest is not None:
                    report = latest
                report["active_process_group"] = (
                    None if reaped else report.get("active_process_group")
                )
                report["phase"] = "stopped" if reaped else "uncertain"
                report["private_historical_grant"]["state"] = (
                    "child-complete" if code == 0 and not reason and reaped else "stopped"
                )
                _atomic_report(report_path, report)
                if code != 0 or reason or not reaped:
                    if phase == "activation" and _historical_activation_claimed(
                        control_root, grant_id, launch["launch_id"], identity
                    ):
                        report["production_state"] = "unknown"
                        _atomic_report(report_path, report)
                    raise RunnerError(
                        reason or f"historical {phase}/{substep} child failed or was not reaped"
                    )
                child_result = _strict_json(stdout_path)
                if (
                    child_result is None
                    or child_result.get("ok") is not True
                    or child_result.get("phase") != phase
                    or child_result.get("substep") != substep
                ):
                    if phase == "activation" and _historical_activation_claimed(
                        control_root, grant_id, launch["launch_id"], identity
                    ):
                        report["production_state"] = "unknown"
                        _atomic_report(report_path, report)
                    raise RunnerError("supervised historical worker did not confirm its action")
                _historical_validate_substep_record(
                    _historical_substep_path(control_root, grant_id, phase, substep),
                    identity=identity,
                    grant_id=grant_id,
                    launch_sha256=launch_sha256,
                    launch=launch,
                    phase=phase,
                    substep=substep,
                )
                if phase == "source" and substep == "maintenance":
                    store.reconcile()
                    post_cleanup_baseline = _historical_resource_baseline(directory, store, report)
                    admission = grants.expected_resource_admission()
                    if (
                        post_cleanup_baseline["temporary_bytes"]
                        > admission["storage_temporary_admission_bytes"]
                    ):
                        raise RunnerError(
                            "startup cleanup did not restore temporary storage admission before work"
                        )
                    _historical_measurement_artifact(
                        control_root,
                        grant_id,
                        approval,
                        identity,
                        freeze,
                        session_report_sha256=session_binding["report_sha256"],
                        session_root_device=session_binding["root_device"],
                        baseline=post_cleanup_baseline,
                        require_fresh=False,
                    )
                    report["storage"] = {
                        "operator_retained_bytes": post_cleanup_baseline["root_bytes"],
                        "operator_temporary_bytes": post_cleanup_baseline["temporary_bytes"],
                        "operator_database_bytes": post_cleanup_baseline["database_bytes"],
                        "external_bytes": post_cleanup_baseline["aggregate_bytes"]
                        - post_cleanup_baseline["root_bytes"],
                        "aggregate_estimate_bytes": post_cleanup_baseline["aggregate_bytes"],
                        "ssd_free_bytes": post_cleanup_baseline["free_bytes"],
                    }
                    _atomic_report(report_path, report)

            report = _strict_json(report_path)
            if report is None:
                raise RunnerError("historical session report disappeared before phase sealing")
            report["active_process_group"] = None
            report["phase"] = "stopped"
            report["private_historical_grant"]["phase"] = phase
            report["private_historical_grant"]["substep"] = None
            report["private_historical_grant"]["state"] = "phase-ready-to-seal"
            _atomic_report(report_path, report)
            authority = _historical_authority(
                directory,
                control_root,
                grant_id,
                launch_sha256=launch_sha256,
                require_worker=False,
                expected_phase=phase,
            )
            _historical_validate_phase_chain(authority)
            outputs = _historical_phase_outputs(authority, phase)
            if phase == "adoption":
                grants.validate_adoption_retirement(approval, outputs.get("source_retired"))
            current, previous = grants.read_phase_chain(control_root / "phases", identity)
            seal = {
                "version": 1,
                "phase": phase,
                "bindings": identity,
                "previous_sha256": previous,
                "outputs": outputs,
                "completed_at_epoch": time.time(),
                "grant_id": grant_id,
            }
            grants.write_phase_seal(
                control_root,
                seal,
                bindings=identity,
                grant_launch=launch,
            )
            if phase in {"verified-publication", "activation"}:
                state_field = "production_state" if phase == "activation" else "publication_state"
                report[state_field] = "verified"
                _atomic_report(report_path, report)
            completed.append(phase)
        _historical_stop_record(
            directory,
            control_root,
            grant_id,
            launch_sha256,
            outcome="complete",
            phase=current_phase,
            substep=None,
            completed_phases=completed,
            reason=None,
            bindings=identity,
            root_volume_uuid=freeze["root_volume_uuid"],
        )
        print(json.dumps({"ok": True, "grant_id": grant_id, "completed_phases": completed}))
        return 0
    except Exception as exc:
        if launch_sha256 is not None and report_path is not None:
            current = _strict_json(report_path) or report or {}
            outcome = "stopped" if current.get("active_process_group") is None else "uncertain"
            current["phase"] = outcome
            # The current controller already validated and holds this identity.
            # Do not re-read the freeze through an incomplete call signature in
            # the failure path; the consumed launch is bound to this snapshot.
            bindings = identity if "identity" in locals() else None
            if (
                current_phase == "activation"
                and launch_sha256 is not None
                and "launch" in locals()
                and isinstance(launch, dict)
                and _historical_activation_claimed(
                    control_root, grant_id, launch["launch_id"], identity
                )
            ):
                current["production_state"] = "unknown"
            prior_private = current.get("private_historical_grant")
            private_handoff = dict(prior_private) if isinstance(prior_private, dict) else {}
            private_handoff.update(
                {
                    "version": 1,
                    "grant_id": grant_id,
                    "launch_sha256": launch_sha256,
                    "launch_id": launch.get("launch_id") if isinstance(launch, dict) else None,
                    "control_root": str(control_root),
                    "phases": (approval.get("phases") if "approval" in locals() else []),
                    "phase": current_phase,
                    "substep": current_substep,
                    "controller_pid": os.getpid(),
                    "controller_pgid": os.getpgrp(),
                    "state": "stopped",
                    "reason": str(exc),
                }
            )
            if current.get("active_process_group") is not None:
                private_handoff.setdefault("worker_pid", current["active_process_group"])
                private_handoff.setdefault("worker_pgid", current["active_process_group"])
            current["private_historical_grant"] = private_handoff
            _atomic_report(report_path, current)
            _historical_stop_record(
                directory,
                control_root,
                grant_id,
                launch_sha256,
                outcome=outcome,
                phase=current_phase,
                substep=current_substep,
                completed_phases=completed,
                reason=str(exc),
                bindings=bindings,
                root_volume_uuid=(
                    preflight["freeze"]["root_volume_uuid"] if "preflight" in locals() else None
                ),
            )
        print(json.dumps({"error": str(exc), "grant_id": grant_id}, sort_keys=True))
        return 1
    finally:
        if token is not None:
            _ACTIVE_LIMITS.reset(token)
        if run_lock_acquired and run_lock_context is not None:
            run_lock_context.__exit__(None, None, None)


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
        resume_stage = _session_historical_stage(report, mode)
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
                        **(
                            {"resume_historical_stage": resume_stage}
                            if resume_stage is not None
                            else {}
                        ),
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
    parser.add_argument("--_historical-control", nargs=3)
    parser.add_argument("--_historical-worker", nargs=8)
    parser.add_argument("--_account-root", type=Path)
    parser.add_argument("--_account-recovery", nargs="+")
    parser.add_argument("--_account-historical-initial", nargs=7)
    parser.add_argument("--_root-summary", type=Path)
    parser.add_argument("--_memory-sample", nargs=1)
    parser.add_argument("--_refresh-worker", nargs=8)
    parser.add_argument("--_activate-current", nargs="+")
    args, rest = parser.parse_known_args(argv)
    selected_workers = (
        args._historical_control,
        args._historical_worker,
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
    if args._historical_control is not None and not rest:
        directory, control_root, grant_id = args._historical_control
        return _historical_control_main(Path(directory), Path(control_root), grant_id)
    if args._historical_worker is not None and not rest:
        (
            directory,
            control_root,
            grant_id,
            launch_sha,
            phase,
            substep,
            storage_token,
            storage_progress,
        ) = args._historical_worker
        return _historical_worker_main(
            Path(directory),
            Path(control_root),
            grant_id,
            launch_sha,
            phase,
            substep,
            storage_token,
            Path(storage_progress),
        )
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
        if len(args._activate_current) not in {5, 7}:
            print(json.dumps({"error": "activation worker session identity is incomplete"}))
            return 1
        return _activation_worker(
            Path(args._activate_current[0]),
            Path(args._activate_current[1]),
            args._activate_current[2],
            Path(args._activate_current[3]),
            args._activate_current[4],
            (
                (float(args._activate_current[5]), float(args._activate_current[6]))
                if len(args._activate_current) == 7
                else None
            ),
        )
    return None


if __name__ == "__main__":
    raise SystemExit(main())
