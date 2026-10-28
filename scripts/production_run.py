"""Bounded manual production refresh and verified DuckDB activation."""

from __future__ import annotations

import argparse
import contextlib
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
from datetime import UTC, datetime
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
RECONCILIATION_TRANSPORT_SHA256 = "c05b2e1c8492d14ae437e11b9b408f82684fba95fdb77b0d66c44084dcab1a55"
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
RUN_LIMITS: dict[str, int | float | str] = {
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
    "peak_rss_bytes": 24 * GIB,
    "peak_rss_seconds": 60,
    "compressed_memory_stop_bytes": 20 * GIB,
    "sample_seconds": 15,
    "terminate_seconds": 5,
    "rate": 5,
    "compression": "gzip",
    "rate_policy": "decrease-on-429",
}
CAPTURE_ID = re.compile(r"[0-9a-f]{32}\Z")
REVISION = re.compile(r"[0-9a-f]{40}\Z")
REPORT_NAME = "session.json"
OWNER_MARKER = ".production-root.json"
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
        "SSD free space fell below 150 GiB",
    }
)
_STOP_IN_PROGRESS = False
_SPAWNING_CHILD = False
_PENDING_STOP_SIGNAL: int | None = None
_TERMINATION_ATTEMPTED: set[int] = set()


class RunnerError(RuntimeError):
    pass


class BudgetStop(RunnerError):
    """An approved bounded allowance is exhausted without an invariant failure."""


class RunnerInterrupted(RunnerError):
    """The operator requested a clean stop while a supervised stage was running."""


def validate_mode(full: bool, capture_id: str | None, activate_current: bool) -> str:
    if sum((full, capture_id is not None, activate_current)) != 1:
        raise RunnerError("choose exactly one of --full, --capture-id or --activate-current")
    if capture_id is not None and (
        not isinstance(capture_id, str) or not CAPTURE_ID.fullmatch(capture_id)
    ):
        raise RunnerError("--capture-id must be a 32-character lowercase hex ID")
    return "full" if full else "resume" if capture_id else "activate-current"


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
        float(RUN_LIMITS["workflow_seconds"]) - elapsed,
        float(RUN_LIMITS["active_cutoff_seconds"]) - elapsed,
        float(RUN_LIMITS["session_seconds"]) - elapsed,
        float(RUN_LIMITS["workflow_seconds"]) - wall_elapsed,
        float(RUN_LIMITS["active_cutoff_seconds"]) - wall_elapsed,
        float(RUN_LIMITS["session_seconds"]) - wall_elapsed,
    )
    max_root = min(
        int(RUN_LIMITS["root_bytes"]), int(RUN_LIMITS["aggregate_bytes"]) - external_reserved_bytes
    )
    attempts = int(RUN_LIMITS["attempts"]) - accounting["attempts"]
    downloads = int(RUN_LIMITS["downloaded_bytes"]) - accounting[byte_field]
    if min(workflow_remaining, attempts, downloads, max_root) <= 0:
        raise BudgetStop("approved session allowance is exhausted")
    return {
        "max-requests": attempts,
        "max-download-bytes": downloads,
        "max-root-bytes": max_root,
        "max-temp-bytes": int(RUN_LIMITS["temporary_bytes"]),
        "max-database-bytes": int(RUN_LIMITS["database_bytes"]),
        "max-response-bytes": min(int(RUN_LIMITS["response_bytes"]), downloads),
        "max-decoded-response-bytes": int(RUN_LIMITS["decoded_response_bytes"]),
        "max-duration-seconds": workflow_remaining,
        "max-rate": float(RUN_LIMITS["rate"]),
        "compression": str(RUN_LIMITS["compression"]),
        "rate-policy": str(RUN_LIMITS["rate_policy"]),
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
    value = report.get(key, start + float(RUN_LIMITS["session_seconds"]))
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerError("session time budget deadline is invalid")
    try:
        value = float(value)
    except OverflowError as exc:
        raise RunnerError("session time budget deadline is invalid") from exc
    if not math.isfinite(value):
        raise RunnerError("session time budget deadline is invalid")
    derived = start + float(RUN_LIMITS["session_seconds"])
    if not math.isfinite(derived):
        raise RunnerError("session time budget deadline is invalid")
    return min(value, derived)


def rss_limit_exceeded(
    samples: list[tuple[float, int]],
    limit: int = int(RUN_LIMITS["peak_rss_bytes"]),
    sustained: float = float(RUN_LIMITS["peak_rss_seconds"]),
) -> bool:
    """Detect uninterrupted above-limit RSS over promptly sampled timestamps."""
    above_since: float | None = None
    previous: float | None = None
    for instant, rss in samples:
        if previous is not None and instant - previous > 1.5 * float(RUN_LIMITS["sample_seconds"]):
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


def _strict_json(path: Path) -> dict[str, Any] | None:
    from kalshi_events.common import CatalogueError, regular_bytes

    def reject(value: str) -> None:
        raise ValueError(value)

    try:
        raw = regular_bytes(path, 16 * MIB)
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


def _external_path(path: Path) -> Path:
    from kalshi_events.common import CatalogueError, safe_path

    try:
        result = safe_path(path.expanduser().absolute())
    except CatalogueError as exc:
        raise RunnerError(str(exc)) from exc
    if result == SOURCE_ROOT or SOURCE_ROOT in result.parents:
        raise RunnerError("production directory must be outside the repository")
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
        return True
    return True


def _terminate(process: subprocess.Popen[Any]) -> bool:
    global _STOP_IN_PROGRESS
    if process.pid in _TERMINATION_ATTEMPTED:
        process.poll()
        reaped = not _process_group_exists(process.pid)
        if reaped:
            _TERMINATION_ATTEMPTED.discard(process.pid)
        return reaped
    was_stopping = _STOP_IN_PROGRESS
    _STOP_IN_PROGRESS = True
    try:
        grace_deadline = time.monotonic() + float(RUN_LIMITS["terminate_seconds"])
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return process.poll() is not None
        term_deadline = min(grace_deadline, time.monotonic() + 2)
        while _process_group_exists(process.pid) and time.monotonic() < term_deadline:
            process.poll()
            time.sleep(min(0.05, max(0.0, term_deadline - time.monotonic())))
        if _process_group_exists(process.pid):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        while _process_group_exists(process.pid) and time.monotonic() < grace_deadline:
            process.poll()
            time.sleep(min(0.05, max(0.0, grace_deadline - time.monotonic())))
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.0, grace_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                _TERMINATION_ATTEMPTED.add(process.pid)
                return False
        reaped = not _process_group_exists(process.pid)
        if not reaped:
            _TERMINATION_ATTEMPTED.add(process.pid)
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
) -> tuple[int | None, str | None, bool]:
    samples: list[tuple[float, int]] = []
    next_sample = time.monotonic()
    stop_reason = None
    reaped = True
    terminated = False
    while process.poll() is None:
        now = time.monotonic()
        remaining = _child_time_remaining(deadline, wall_deadline_epoch)
        if remaining <= float(RUN_LIMITS["terminate_seconds"]):
            stop_reason = "session or workflow deadline reached termination reserve"
            reaped = _terminate(process)
            terminated = True
            break
        if now >= next_sample:
            try:
                _merge_capture_notification(report, capture_notification)
                free = shutil.disk_usage(directory).free
                if original_device is not None and directory.stat().st_dev != original_device:
                    raise RunnerError("production filesystem mount changed during the run")
                rss = _process_group_rss(
                    process.pid,
                    timeout=min(5.0, max(0.01, remaining - float(RUN_LIMITS["terminate_seconds"]))),
                )
                remaining = _child_time_remaining(deadline, wall_deadline_epoch)
                if remaining <= float(RUN_LIMITS["terminate_seconds"]):
                    stop_reason = "session or workflow deadline reached termination reserve"
                    reaped = _terminate(process)
                    terminated = True
                    break
                compressed = _system_compressed_bytes(
                    timeout=min(
                        5.0,
                        max(0.01, remaining - float(RUN_LIMITS["terminate_seconds"])),
                    )
                )
                if _child_time_remaining(deadline, wall_deadline_epoch) <= float(
                    RUN_LIMITS["terminate_seconds"]
                ):
                    stop_reason = "session or workflow deadline reached termination reserve"
                    reaped = _terminate(process)
                    terminated = True
                    break
            except (OSError, subprocess.SubprocessError, RunnerError) as exc:
                if _child_time_remaining(deadline, wall_deadline_epoch) <= float(
                    RUN_LIMITS["terminate_seconds"]
                ):
                    stop_reason = "session or workflow deadline reached termination reserve"
                else:
                    stop_reason = f"resource accounting failed: {exc}"
                reaped = _terminate(process)
                terminated = True
                break
            samples.append((now, rss))
            samples = samples[-8:]
            report["resource_samples"].append(
                {
                    "at": _utc(),
                    "ssd_free_bytes": free,
                    "child_process_group_rss_bytes": rss,
                    "system_compressed_bytes": compressed,
                }
            )
            report["last_observed_epoch"] = time.time()
            report["peak_child_process_group_rss_bytes"] = max(
                report["peak_child_process_group_rss_bytes"], rss
            )
            if free < int(RUN_LIMITS["minimum_free_bytes"]):
                stop_reason = "SSD free space fell below 150 GiB"
            elif rss_limit_exceeded(samples):
                stop_reason = "process-group RSS exceeded 24 GiB for 60 seconds"
            elif compressed >= int(RUN_LIMITS["compressed_memory_stop_bytes"]):
                stop_reason = "system compressed memory reached the conservative stop threshold"
            if stop_reason:
                reaped = _terminate(process)
                terminated = True
                break
            report["last_observed_at"] = _utc()
            _atomic_report(report_path, report)
            next_sample = now + float(RUN_LIMITS["sample_seconds"])
        remaining = _child_time_remaining(deadline, wall_deadline_epoch)
        time.sleep(min(1.0, max(0.0, remaining - float(RUN_LIMITS["terminate_seconds"]))))
    if process.poll() is None and not terminated:
        reaped = _terminate(process)
        terminated = True
    elif _process_group_exists(process.pid) and not terminated:
        stop_reason = stop_reason or "child left a descendant process running"
        reaped = _terminate(process)
        terminated = True
    elif _process_group_exists(process.pid):
        stop_reason = stop_reason or "child process group remains after termination grace"
        reaped = False
    try:
        returncode = process.wait(timeout=0.1 if process.poll() is not None else 0)
    except subprocess.TimeoutExpired:
        returncode = None
        reaped = False
    return returncode, stop_reason, reaped


def _system_compressed_bytes(timeout: float = 5.0) -> int:
    """Read macOS compressor pages so swapped/compressed memory cannot evade RSS limits."""
    result = subprocess.run(
        ["vm_stat"], check=True, capture_output=True, text=True, timeout=timeout
    )
    page_size = re.search(r"page size of (\d+) bytes", result.stdout)
    compressed_pages = re.search(r"Pages stored in compressor:\s*(\d+)", result.stdout)
    if page_size is None or compressed_pages is None or int(page_size.group(1)) <= 0:
        raise RunnerError("macOS compressed-memory accounting is unavailable")
    return int(page_size.group(1)) * int(compressed_pages.group(1))


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
) -> tuple[int | None, str | None, bool]:
    from kalshi_events.common import safe_path

    env = os.environ.copy()
    env["PYTHONPATH"] = str(SOURCE_ROOT / "src")
    directory = _external_path(directory)
    original_device = directory.stat().st_dev
    recorded_device = report.get("system", {}).get("filesystem_device")
    if recorded_device != original_device:
        raise RunnerError("production filesystem changed since session preflight")
    stdout_path = safe_path(stdout_path)
    stderr_path = safe_path(stderr_path)
    if time.monotonic() >= deadline - float(RUN_LIMITS["terminate_seconds"]):
        return None, "insufficient time remains to start and reap a child", True
    wall_deadline_epoch = _child_wall_deadline_epoch(
        report, deadline, workflow_deadline=workflow_deadline
    )
    wall_now = time.time()
    if wall_deadline_epoch is not None and wall_now >= wall_deadline_epoch - float(
        RUN_LIMITS["terminate_seconds"]
    ):
        return None, "insufficient wall-clock time remains to start and reap a child", True
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process: subprocess.Popen[Any] | None = None
        try:
            global _PENDING_STOP_SIGNAL, _SPAWNING_CHILD
            wall_now = time.time()
            if time.monotonic() >= deadline - float(RUN_LIMITS["terminate_seconds"]) or (
                wall_deadline_epoch is not None
                and wall_now >= wall_deadline_epoch - float(RUN_LIMITS["terminate_seconds"])
            ):
                reason = (
                    "insufficient wall-clock time remains to start and reap a child"
                    if wall_deadline_epoch is not None
                    and wall_now >= wall_deadline_epoch - float(RUN_LIMITS["terminate_seconds"])
                    else "insufficient time remains to start and reap a child"
                )
                return None, reason, True
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
            )
        except BaseException as exc:
            _merge_capture_notification(report, capture_notification)
            if process is not None and not _terminate(process):
                raise RunnerError("child could not be reaped after supervision failure") from None
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
) -> dict[str, Any]:
    command = [sys.executable, str(Path(__file__).resolve())]
    command += (
        ["--_account-recovery", str(operator), capture_id]
        if capture_id is not None
        else ["--_account-root", str(operator)]
    )
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
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "kalshi_events.cli",
        "verify",
        "--release",
        str(release),
        "--max-root-bytes",
        str(int(RUN_LIMITS["root_bytes"])),
    ]
    stdout = paths["logs"] / "release-verify.stdout.json"
    stderr = paths["logs"] / "release-verify.stderr.log"
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
        )
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


def _read_accounting_worker(root: Path, expected_capture_id: str | None = None) -> dict[str, Any]:
    from kalshi_events.common import Limits, Store, decode, regular_bytes

    store = Store(
        root,
        Limits(
            max_rate=float(RUN_LIMITS["rate"]),
            max_requests=int(RUN_LIMITS["attempts"]),
            max_download_bytes=int(RUN_LIMITS["downloaded_bytes"]),
            max_response_bytes=int(RUN_LIMITS["response_bytes"]),
            max_decoded_response_bytes=int(RUN_LIMITS["decoded_response_bytes"]),
            max_duration_seconds=float(RUN_LIMITS["session_seconds"]),
            max_root_bytes=int(RUN_LIMITS["root_bytes"]),
            max_temp_bytes=int(RUN_LIMITS["temporary_bytes"]),
            max_database_bytes=int(RUN_LIMITS["database_bytes"]),
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
                or capture_state.get("max_response_bytes") != int(RUN_LIMITS["response_bytes"])
            ):
                raise RunnerError(
                    "recovery capture does not match the session's sealed full-scope policy"
                )
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
                    or not 0 < reservation <= int(RUN_LIMITS["response_bytes"])
                ):
                    raise RunnerError("finalized receipt has invalid reservation metadata")
                amount = receipt.get("downloaded_bytes", receipt.get("size"))
                if (
                    isinstance(amount, bool)
                    or not isinstance(amount, int)
                    or not 0
                    <= amount
                    <= int(RUN_LIMITS["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
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
            downloaded_or_reserved += int(RUN_LIMITS["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
            if (
                isinstance(reservation, bool)
                or not isinstance(reservation, int)
                or not 0 < reservation <= int(RUN_LIMITS["response_bytes"])
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

    store = Store(root, Limits(max_root_bytes=int(RUN_LIMITS["root_bytes"])))
    result: dict[str, Any] = {"root_id": _root_id(store), "current_v2": None}
    if store.path("current-v2.json").exists():
        result["current_v2"] = publish.current(store, catalogue_version=2)
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
    standalone_verified: dict[str, Any] | None = None,
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
        standalone_verified=standalone_verified,
        link_fn=link_fn,
        replace_fn=replace_fn,
        sync_fn=sync_fn,
    )


def _external_bytes(directory: Path) -> int:
    total = 0
    operator = directory / "operator"
    seen: set[tuple[int, int]] = set()
    builds = operator / "builds-v2"
    if builds.is_symlink():
        raise RunnerError("operator build directory is a symlink")
    if builds.is_dir():
        for candidate in builds.glob("*/warehouse.duckdb"):
            if candidate.is_symlink() or not candidate.is_file():
                raise RunnerError("retained warehouse path is unsafe during external accounting")
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
        "capture_notification": run_root / "capture-notification.json",
    }


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
) -> dict[str, Any]:
    """Run Store's only full accounting walk in a supervised boundary child."""
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
        retained > int(RUN_LIMITS["root_bytes"])
        or temporary > int(RUN_LIMITS["temporary_bytes"])
        or database > int(RUN_LIMITS["database_bytes"])
        or physical_estimate > int(RUN_LIMITS["aggregate_bytes"])
    ):
        raise BudgetStop("production storage allowance was exceeded")
    free = shutil.disk_usage(directory).free
    if free < int(RUN_LIMITS["minimum_free_bytes"]):
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
        RUN_LIMITS["workflow_seconds"] if workflow else RUN_LIMITS["active_cutoff_seconds"]
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
        RUN_LIMITS["workflow_seconds"] if workflow_deadline else RUN_LIMITS["active_cutoff_seconds"]
    )
    wall_now = time.time()
    deadlines = [
        started + stage_seconds,
        started + float(RUN_LIMITS["active_cutoff_seconds"]),
        wall_now + max(0.0, monotonic_deadline - time.monotonic()),
    ]
    deadlines.append(_time_budget_deadline(report, "epoch"))
    return min(deadlines)


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
) -> tuple[int | None, str | None, bool, Path, Path]:
    stdout = paths["logs"] / f"{label}.stdout.json"
    stderr = paths["logs"] / f"{label}.stderr.log"
    notification = paths.get("capture_notification") if label == "refresh" else None
    if notification is not None:
        with contextlib.suppress(FileNotFoundError):
            notification.unlink()
    report["phase"] = label
    _atomic_report(paths["report"], report)
    started = time.monotonic()
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
    )
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


def run_production(
    directory: Path,
    *,
    full: bool = False,
    capture_id: str | None = None,
    activate_only: bool = False,
    reconcile_accounting: bool = False,
    extend_session: bool = False,
    recover_reboot: bool = False,
    recovery_volume_uuid: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Run one bounded full refresh/resume or verify-and-activate current v2."""
    preflight_started = time.monotonic()
    mode = validate_mode(full, capture_id, activate_only)
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
        if mode == "resume":
            report_path, report, paths = _load_resume(
                directory,
                str(capture_id),
                reconcile_accounting=reconcile_accounting,
                extend_session=extend_session,
                recover_reboot=recover_reboot,
                recovery_volume_uuid=recovery_volume_uuid,
            )
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
                    "prior_error": report.get("error"),
                    "prior_recovery_instructions": report.get("recovery"),
                    "runtime_transition": {
                        "from_uv_lock_sha256": saved_system["uv_lock_sha256"],
                    },
                }
            paths = _prepare_run_paths(directory, report_path)
            report["metrics_file"] = str(paths["metrics"])
            report["metrics_progress_file"] = str(paths["metrics_progress"])
            if report.get("root_id") is None:
                raise RunnerError("resume root identity was not durably recorded")
        else:
            report_path, report, paths = _session_report(directory, mode)
            paths = _prepare_run_paths(directory, report_path)
            report["metrics_file"] = str(paths["metrics"])
            report["metrics_progress_file"] = str(paths["metrics_progress"])
        phases = report.setdefault("phase_seconds", {})
        phases["preflight"] = phases.get("preflight", 0) + time.monotonic() - preflight_started
        result: dict[str, Any] = report
        if mode == "activate-current":
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
        from kalshi_events.transport import BASE_URL, source_url

        report["api_origin"] = source_url()
        if mode != "activate-current" and report["api_origin"] != BASE_URL:
            raise RunnerError("production acquisition must use the public Kalshi API origin")
        report["last_observed_epoch"] = time.time()
        report["last_observed_at"] = _utc()
        _atomic_report(report_path, report)
        if shutil.disk_usage(directory).free < int(RUN_LIMITS["minimum_free_bytes"]):
            report["error"] = "SSD free space is below 150 GiB before work"
            report["phase"] = "stopped"
            _atomic_report(report_path, report)
            return result, 3
        awake = subprocess.Popen(
            ["caffeinate", "-is", "-w", str(os.getpid())],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            session_deadline = _monitor_deadline(report, workflow=False)
            standalone_result: dict[str, Any] | None = None
            if mode != "activate-current":
                snapshot = None
                if mode == "resume":
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
                if not reconcile_accounting:
                    prior = (
                        _accounting_shape(snapshot)
                        if snapshot is not None
                        else {
                            "attempts": 0,
                            "downloaded_bytes": 0,
                            "accounting_complete": True,
                        }
                    )
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
                command = _refresh_command(operator, paths, mode, capture_id, allowances)
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
                    deadline=time.monotonic() + duration,
                    label="refresh",
                    capture_id=capture_id,
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
                if not reaped:
                    stop_reason = "refresh subprocess could not be reaped within five seconds"
                if stop_reason or cli_result is None or code not in {0, 1, 3}:
                    report["accounting_complete"] = False
                    report["requests"] = None
                    report["downloaded_bytes"] = None
                    report["capture_complete"] = None
                    report["publication_state"] = "unknown"
                    report["error"] = stop_reason or "refresh failed with unknown child accounting"
                    report["resumable"] = None
                    report["phase"] = "stopped"
                    report["recovery"] = (
                        f"Preserve {operator}; do not resume until accounting is independently reconciled."
                    )
                    _atomic_report(report_path, report)
                    return result, 3 if stop_reason else 1
                report["capture_complete"] = cli_result.get("capture_complete") is True
                report["publication_state"] = cli_result.get("publication_state", "unknown")
                if cli_result.get("accounting_complete") is not True and not reconcile_accounting:
                    report["accounting_complete"] = False
                    report["error"] = "refresh reported incomplete acquisition accounting"
                    report["resumable"] = None
                    report["phase"] = "stopped"
                    _atomic_report(report_path, report)
                    return result, 1
                report["resumable"] = cli_result.get("resumable") is True
                if report["resumable"] is not True:
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
                        not reconcile_accounting or prior.get("accounting_complete") is True
                    ),
                    capture_id=capture_id if reconcile_accounting else None,
                )
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
                if not reconcile_accounting:
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
                    or report["capture_complete"] is not True
                    or report["publication_state"] != "verified"
                ):
                    report["error"] = (
                        cli_result.get("error") or "capture or publication remains incomplete"
                    )
                    report["phase"] = "stopped"
                    report["recovery"] = (
                        (
                            f"After investigating the stop, resume only this capture: --capture-id {report['capture_id']}."
                        )
                        if report.get("capture_id") and report["accounting_complete"]
                        else None
                    )
                    _atomic_report(report_path, report)
                    return result, code
                release_path = Path(str(cli_result.get("output", "")))
                release_id = cli_result.get("source_release_id")
                if not release_path.is_absolute() or not isinstance(release_id, str):
                    raise RunnerError("refresh did not report a verified v2 release")
                report["phase"] = "standalone-verification"
                _atomic_report(report_path, report)
                standalone_result = _release_verify(
                    release_path,
                    directory,
                    operator,
                    paths,
                    report,
                    _monitor_deadline(report, workflow=False),
                )
                if standalone_result.get("source_release_id") != release_id:
                    raise RunnerError("standalone verification returned a different release")
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
                current = summary.get("current_v2")
                if not isinstance(current, dict):
                    raise RunnerError("no verified current v2 release is available")
                release = Path(str(current["output"]))
                standalone_result = _release_verify(
                    release,
                    directory,
                    operator,
                    paths,
                    report,
                    _monitor_deadline(report, workflow=False),
                )
            expected_release = (
                standalone_result.get("source_release_id") if standalone_result else None
            )
            if not expected_release:
                raise RunnerError("no verified current catalogue release is available")
            report["phase"] = "activation"
            _atomic_report(report_path, report)
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--_activate-current",
                str(operator),
                str(directory),
                str(expected_release),
            ]
            code, stop_reason, reaped, activation_stdout, activation_stderr = _run_external_child(
                command,
                directory=directory,
                operator=operator,
                paths=paths,
                report=report,
                deadline=_monitor_deadline(report, workflow=False),
                label="activation",
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
                    "Run --activate-current to reverify the actual publication and hard-link state."
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
            if mode == "activate-current":
                _record_operator_accounting(report, final_accounting)
            report["production_state"] = "verified"
            report["activation"] = activated
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
                or time.time() >= _time_budget_deadline(report, "epoch")
                else 1
            )
        finally:
            _kill_awake_guard(awake)


def _activation_worker(operator: Path, directory: Path, expected: str) -> int:
    try:
        result = activate_current(operator, directory, expected_release_id=expected)
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
        "--reconcile-accounting",
        action="store_true",
        help="resume only after charging unfinished requests at their full durable reservation",
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
    if args.reconcile_accounting and not args.capture_id:
        parser.error("--reconcile-accounting requires --capture-id")
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
                reconcile_accounting=args.reconcile_accounting,
                extend_session=args.extend_session,
                recover_reboot=args.recover_reboot,
                recovery_volume_uuid=args.recovery_volume_uuid,
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
        "deadline_epoch": now + float(RUN_LIMITS["session_seconds"]),
        "started_monotonic": mono,
        "deadline_monotonic": mono + float(RUN_LIMITS["session_seconds"]),
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
        "limits": dict(RUN_LIMITS),
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "error": None,
        "recovery": None,
    }
    _atomic_report(paths["report"], report)
    return paths["report"], report, paths


def _load_resume(
    directory: Path,
    capture_id: str,
    *,
    reconcile_accounting: bool = False,
    extend_session: bool = False,
    recover_reboot: bool = False,
    recovery_volume_uuid: str | None = None,
) -> tuple[Path, dict[str, Any], dict[str, Path]]:
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
    recoverable_accounting_stop = _is_recoverable_accounting_stop(
        report, capture_id, reconcile_accounting
    )
    if (report.get("accounting_complete") is not True and not recoverable_accounting_stop) or (
        report.get("production_state") == "verified"
    ):
        raise RunnerError("resume accounting is unknown or production activation already succeeded")
    if reconcile_accounting:
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
    if (
        not rebooted
        and time.monotonic() >= _time_budget_deadline(report, "monotonic")
        and not extend_session
    ):
        raise RunnerError("48-hour session expired; networking resume is prohibited")
    if now >= _time_budget_deadline(report, "epoch") and not extend_session:
        raise RunnerError("48-hour wall-clock session expired; networking resume is prohibited")
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
        deadline_epoch = now + float(RUN_LIMITS["session_seconds"])
        deadline_monotonic = mono + float(RUN_LIMITS["session_seconds"])
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
            or not 0 < reservation <= int(RUN_LIMITS["response_bytes"])
        ):
            raise RunnerError("unfinished receipt cannot be safely charged")
        ids.add(request_id)
        inventory.append((request_id, receipt["capture_id"], reservation))
        charges += int(RUN_LIMITS["response_bytes"]) + HTTPX_MAX_RAW_CHUNK_BYTES
    if amount < charges or attempts < len(pending):
        raise RunnerError("durable reservation totals are contradictory")
    return {
        "attempts": attempts,
        "downloaded_bytes_upper_bound": amount,
        "accounting_complete": False,
        "accounting_basis": "reserved-upper-bound",
        "accounting_bound_verified": True,
        "unfinished_reservations": len(pending),
        "unfinished_response_ceiling_bytes": len(pending) * int(RUN_LIMITS["response_bytes"]),
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
    ]


def _read_cli_result(path: Path) -> dict[str, Any] | None:
    result = _strict_json(path)
    return result if result and isinstance(result.get("complete"), bool) else None


def _accounting_worker_main(root: Path, capture_id: str | None = None) -> int:
    try:
        value = _read_accounting_worker(root, capture_id)
        print(json.dumps(value, sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


def _root_summary_worker_main(root: Path) -> int:
    try:
        print(json.dumps(_read_root_summary(root), sort_keys=True, allow_nan=False))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1


def _refresh_worker_main(
    operator: Path,
    report_path: Path,
    metrics_path: Path,
    notification_path: Path,
    mode: str,
    capture_id: str | None,
    raw_limits: str,
) -> int:
    from kalshi_events import metrics, refresh
    from kalshi_events.common import Limits, Store
    from kalshi_events.transport import BASE_URL, source_url

    report = _strict_json(report_path)
    result = refresh.result_template()
    if report is None:
        print(json.dumps({**result, "error": "session report is missing"}))
        return 1
    try:
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
                    _write_capture_notification(notification_path, None, logical_root_id)
                    result, exit_code = refresh.run(
                        store,
                        full=mode == "full",
                        capture_id=capture_id,
                        compression=compression,
                        rate_policy=rate_policy,
                        on_capture_id=lambda value: _write_capture_notification(
                            notification_path, value, logical_root_id
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
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return exit_code
    except Exception as exc:
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
    parser.add_argument("--_account-recovery", nargs=2)
    parser.add_argument("--_root-summary", type=Path)
    parser.add_argument("--_refresh-worker", nargs=7)
    parser.add_argument("--_activate-current", nargs=3)
    args, rest = parser.parse_known_args(argv)
    if args._account_root is not None and not rest:
        return _accounting_worker_main(args._account_root)
    if args._account_recovery is not None and not rest:
        return _accounting_worker_main(Path(args._account_recovery[0]), args._account_recovery[1])
    if args._root_summary is not None and not rest:
        return _root_summary_worker_main(args._root_summary)
    if args._refresh_worker is not None and not rest:
        operator, report, metrics_path, notification_path, mode, capture_id, limits = (
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
        )
    if args._activate_current is not None and not rest:
        return _activation_worker(
            Path(args._activate_current[0]),
            Path(args._activate_current[1]),
            args._activate_current[2],
        )
    return None


if __name__ == "__main__":
    raise SystemExit(main())
