"""Durable local evidence, strict paths and operation ceilings."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from filelock import FileLock, Timeout

from kalshi_events import metrics

MAX_BUNDLE_BYTES = 128 * 1024**2
TICKER = re.compile(r"[\x20-\x7e]{1,256}\Z")
OWNER_MARKER = {"owner": "oddsfox.kalshi.events.v1"}
CURRENT_ROOT_MARKER = {**OWNER_MARKER, "storage_version": 5}
PREVIOUS_ROOT_MARKER = {**OWNER_MARKER, "storage_version": 2}
SCOPED_ROOT_VERSION = 4
CURRENT_ROOT_VERSION = 5


def series_scope(series_ticker: str) -> dict[str, Any]:
    return {
        "kind": "series",
        "series_ticker": ticker(series_ticker),
        "include_multivariate": False,
    }


def validate_series_scope(scope: Any) -> dict[str, Any]:
    if not isinstance(scope, dict) or set(scope) != {
        "kind",
        "series_ticker",
        "include_multivariate",
    }:
        raise CatalogueError("invalid operator series scope")
    if scope.get("kind") != "series" or scope.get("include_multivariate") is not False:
        raise CatalogueError("invalid operator series scope")
    return series_scope(scope.get("series_ticker"))


def series_root_marker(scope: dict[str, Any]) -> dict[str, Any]:
    return {
        "owner": OWNER_MARKER["owner"],
        "storage_version": CURRENT_ROOT_VERSION,
        "scope": validate_series_scope(scope),
    }


def _root_scope(marker: Any) -> dict[str, Any] | None:
    """Return the validated immutable scope for either scoped marker generation."""
    version = _root_storage_version(marker)
    if "scope" not in marker:
        return None
    if version not in {SCOPED_ROOT_VERSION, CURRENT_ROOT_VERSION}:
        raise CatalogueError("operator root has an unsupported scope version")
    return validate_series_scope(marker["scope"])


class CatalogueError(RuntimeError):
    """A failed invariant; no publication may be promoted."""


class BudgetExceeded(CatalogueError):
    """Requested work remains incomplete and can resume with a new allowance."""


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=lambda v: str(v) if isinstance(v, Decimal) else _reject(v),
        )
        + "\n"
    ).encode()


def _reject(value: Any) -> Any:
    raise TypeError(f"cannot encode {type(value).__name__}")


def _root_storage_version(marker: Any) -> int:
    if marker == OWNER_MARKER:
        return 1
    if (
        not isinstance(marker, dict)
        or marker.get("owner") != OWNER_MARKER["owner"]
        or isinstance(marker.get("storage_version"), bool)
        or not isinstance(marker.get("storage_version"), int)
    ):
        raise CatalogueError("operator root belongs to another application")
    version = marker["storage_version"]
    if version in {2, 3} and set(marker) == {"owner", "storage_version"}:
        return version
    if version == SCOPED_ROOT_VERSION and set(marker) == {"owner", "storage_version", "scope"}:
        validate_series_scope(marker["scope"])
        return version
    if version == CURRENT_ROOT_VERSION and set(marker) in (
        {"owner", "storage_version"},
        {"owner", "storage_version", "scope"},
    ):
        if "scope" in marker:
            validate_series_scope(marker["scope"])
        return version
    raise CatalogueError("operator root belongs to another application")


def _initialize_root_marker(root: Path, marker: Path, value: dict[str, Any]) -> None:
    """Install a new root marker under the canonical lock without replacing one."""
    # Check before FileLock opens writer.lock: it may truncate an unrelated file in an
    # unowned, nonempty directory. A competing initializer may leave its lock visible here;
    # the second initializer then fails closed instead of racing the marker installation.
    if any(root.iterdir()):
        raise CatalogueError("operator root must be new, empty, or a Kalshi events root")
    lock = FileLock(str(root / "writer.lock"), timeout=0)
    try:
        lock.acquire()
    except Timeout as exc:
        raise CatalogueError("another writer is initializing the operator root") from exc
    try:
        # If another initializer completed before this lock was acquired, leave its marker
        # untouched. The Store constructor validates it immediately after this helper.
        if marker.exists():
            return
        if any(entry.name != "writer.lock" for entry in root.iterdir()):
            raise CatalogueError("operator root must be new, empty, or a Kalshi events root")
        atomic_json(marker, value)
    finally:
        lock.release()


CANONICAL_CURRENT_ROOT_MARKER_BYTES = json_bytes(CURRENT_ROOT_MARKER)


def decode(body: bytes) -> Any:
    metrics.add("json_decodes")
    metrics.add("json_decoded_bytes", len(body))
    with metrics.phase("json_parsing"):
        try:
            return json.loads(body, parse_float=Decimal, parse_constant=lambda _: _reject(None))
        except (ValueError, TypeError, UnicodeError) as exc:
            raise CatalogueError("invalid JSON response or evidence") from exc


def digest(body: bytes) -> str:
    metrics.add("hashes")
    metrics.add("hash_bytes", len(body))
    with metrics.phase("hashing"):
        return hashlib.sha256(body).hexdigest()


def file_digest(path: Path, limit: int = MAX_BUNDLE_BYTES) -> tuple[str, int]:
    path = safe_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise CatalogueError("missing, nonregular or oversized evidence file")
    checksum = hashlib.sha256()
    metrics.add("hashes")
    metrics.add("file_reads")
    size = 0
    with metrics.phase("hashing"), path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            size += len(chunk)
            if size > limit:
                raise CatalogueError("evidence file grew beyond byte ceiling")
            checksum.update(chunk)
            metrics.add("hash_bytes", len(chunk))
    return checksum.hexdigest(), size


def ticker(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not TICKER.fullmatch(value)
        or "\\" in value
        or value.startswith(" ")
        or value.endswith((" ", "/"))
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise CatalogueError("invalid native ticker")
    return value


def ticker_path_segment(value: Any) -> str:
    """Quote a native ticker as one path segment, preserving prior spellings."""
    return quote(
        ticker(value), safe="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._()- "
    )


def ticker_from_path_segment(value: str) -> str:
    """Decode and validate the canonical path spelling of a native ticker."""
    decoded = unquote(value)
    if ticker_path_segment(decoded) != value:
        raise CatalogueError("invalid native ticker path segment")
    return decoded


def selections(values: list[str]) -> list[str]:
    if not 1 <= len(values) <= 100 or len(set(values)) != len(values):
        raise CatalogueError("select 1 to 100 distinct explicit market tickers")
    return [ticker(v) for v in values]


def path_is_link(path: Path) -> bool:
    """Reject symbolic links at evidence and operator-root boundaries."""
    try:
        value = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(value.st_mode)


def safe_path(path: Path) -> Path:
    path = path.absolute()
    for part in (path, *path.parents):
        if path_is_link(part):
            raise CatalogueError(f"symlink is forbidden: {part}")
    return path.resolve()


def regular_bytes(path: Path, limit: int = MAX_BUNDLE_BYTES) -> bytes:
    path = safe_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise CatalogueError(f"missing, nonregular or oversized file: {path}")
    with path.open("rb") as stream:
        body = stream.read()
    metrics.add("file_reads")
    metrics.add("file_read_bytes", len(body))
    if len(body) > limit:
        raise CatalogueError("file grew beyond limit")
    return body


def durable_mkdir(path: Path) -> None:
    """Create missing directories and persist each new entry in its parent."""
    path = safe_path(path)
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    if not current.is_dir():
        raise CatalogueError(f"directory path is not a directory: {current}")
    if current != Path(current.anchor):
        fsync_directory(current.parent)
    for directory in reversed(missing):
        with suppress(FileExistsError):
            directory.mkdir()
        fsync_directory(directory.parent)


def atomic_bytes(path: Path, body: bytes) -> None:
    path = safe_path(path)
    durable_mkdir(path.parent)
    fd, name = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            metrics.add("fsync_calls")
            with metrics.phase("file_sync"):
                os.fsync(stream.fileno())
        with metrics.phase("atomic_replace"):
            os.replace(name, path)
        fsync_directory(path.parent)
    finally:
        Path(name).unlink(missing_ok=True)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(path, json_bytes(value))


def fsync_file(path: Path) -> None:
    """Durably flush an already closed regular file."""
    path = safe_path(path)
    if not path.is_file():
        raise CatalogueError("cannot sync a missing or nonregular file")
    with path.open("rb") as stream:
        metrics.add("fsync_calls")
        with metrics.phase("file_sync"):
            os.fsync(stream.fileno())


def fsync_directory(path: Path) -> None:
    """Durably flush directory entries after rename or unlink operations."""
    path = safe_path(path)
    if not path.is_dir():
        raise CatalogueError("cannot sync a missing directory")
    descriptor = os.open(path, os.O_RDONLY)
    try:
        metrics.add("fsync_calls")
        with metrics.phase("directory_sync"):
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class Limits:
    max_rate: float = 5.0
    max_requests: int = 25_000
    max_download_bytes: int = 4 * 1024**3
    max_response_bytes: int = 384 * 1024**2
    max_decoded_response_bytes: int | None = None
    max_duration_seconds: float = 4 * 3600
    max_root_bytes: int = 64 * 1024**3
    max_temp_bytes: int = 8 * 1024**3
    max_database_bytes: int = 2 * 1024**3

    def __post_init__(self) -> None:
        if self.max_decoded_response_bytes is None:
            object.__setattr__(self, "max_decoded_response_bytes", self.max_response_bytes)
        for key in self.__dataclass_fields__:
            value = getattr(self, key)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or value <= 0
                or not math.isfinite(value)
            ):
                raise CatalogueError(f"{key} must be positive")
        if self.max_response_bytes > self.max_download_bytes:
            raise CatalogueError("response ceiling cannot exceed download ceiling")


class Store:
    """Owns one operator root.

    Byte accounting is incremental: every write, append and removal made through this object
    updates per-file sizes and category counters. `reconcile` performs the single full walk at
    invocation start and end; `sync` re-measures one subtree after third-party work.
    """

    def __init__(
        self, root: Path, limits: Limits | None = None, *, series_ticker: str | None = None
    ):
        self.root = safe_path(root)
        if self.root == Path(self.root.anchor):
            raise CatalogueError("filesystem root cannot be an operator root")
        requested_scope = series_scope(series_ticker) if series_ticker is not None else None
        self.limits = limits or Limits()
        self.started = time.monotonic()
        self._lock = threading.RLock()
        self._writer_lock: FileLock | None = None
        self._writer_owner: int | None = None
        self._storage_reservations = 0
        self._sizes: dict[str, int] = {}
        self._usage = {"retained_bytes": 0, "temporary_bytes": 0, "database_bytes": 0}
        if self.root.exists():
            if not self.root.is_dir():
                raise CatalogueError("operator root is not a directory")
        else:
            durable_mkdir(self.root)
        marker = self.root / "root.json"
        if not marker.exists():
            initial_marker = (
                series_root_marker(requested_scope)
                if requested_scope is not None
                else CURRENT_ROOT_MARKER
            )
            _initialize_root_marker(self.root, marker, initial_marker)
        marker_value = decode(regular_bytes(marker))
        self.storage_version = _root_storage_version(marker_value)
        self._scope = _root_scope(marker_value)
        if requested_scope is not None and requested_scope != self._scope:
            raise CatalogueError("series scope requires a new dedicated operator root")
        self.reconcile()

    @property
    def scope(self) -> dict[str, Any] | None:
        return dict(self._scope) if self._scope is not None else None

    @property
    def remaining(self) -> float:
        seconds = self.limits.max_duration_seconds - (time.monotonic() - self.started)
        if seconds <= 0:
            raise BudgetExceeded("operation deadline exhausted")
        return seconds

    def check_deadline(self) -> None:
        _ = self.remaining

    def path(self, relative: str) -> Path:
        result = safe_path(self.root / relative)
        if not result.is_relative_to(self.root):
            raise CatalogueError("path escapes operator root")
        return result

    def usage(self) -> dict[str, int]:
        with self._lock:
            return dict(self._usage)

    def check_storage(self, additional: int = 0) -> dict[str, int]:
        usage = self.usage()
        with self._lock:
            reserved = self._storage_reservations
        metrics.peak("reserved", reserved + additional)
        if usage["retained_bytes"] + reserved + additional > self.limits.max_root_bytes:
            raise BudgetExceeded("operator root storage exhausted")
        if usage["temporary_bytes"] + reserved + additional > self.limits.max_temp_bytes:
            raise BudgetExceeded("temporary storage exhausted")
        if usage["database_bytes"] > self.limits.max_database_bytes:
            raise BudgetExceeded("database storage exhausted")
        return usage

    def reconcile(self) -> dict[str, int]:
        """Full directory walk. Called once at invocation start and once at its end."""
        with self._lock:
            self.check_deadline()
            self._sync_prefix(self.root, check_deadline=True, require_path=True)
            self.check_deadline()
            return dict(self._usage)

    def sync(self, relative: str) -> None:
        """Re-measure one subtree; used after third-party writers such as dlt, dbt and DuckDB."""
        self._sync_prefix(self.path(relative))

    def sync_file(self, relative: str) -> None:
        """Re-measure one regular file without rescanning the rest of a large root."""
        path = self.path(relative)
        if path_is_link(path) or not path.is_file():
            raise CatalogueError("accounted file is missing or unsafe")
        with self._lock:
            self._record(path, path.stat().st_size)

    def write(self, relative: str, value: Any) -> None:
        if relative.startswith("raw/") and relative.endswith("/manifest.json"):
            phase = (
                "receipt_finalization"
                if isinstance(value, dict) and value.get("accounting_complete") is True
                else "reservation_write"
            )
            with metrics.phase(phase):
                self.write_bytes(relative, json_bytes(value))
            return
        self.write_bytes(relative, json_bytes(value))

    def write_bytes(self, relative: str, body: bytes) -> None:
        self._require_writable()
        path = self.path(relative)
        with self._lock:
            # The complete replacement exists beside the old file until rename. Serialize
            # check/write/accounting so concurrent writers cannot spend the same headroom.
            self.check_storage(len(body))
            metrics.peak("retained", self._usage["retained_bytes"] + len(body))
            metrics.peak("temporary", self._usage["temporary_bytes"] + len(body))
            self._storage_reservations += len(body)
            try:
                if relative.startswith("raw/"):
                    with metrics.phase("raw_write"):
                        atomic_bytes(path, body)
                elif relative.endswith("/state.json"):
                    with metrics.phase("snapshot_replacement"):
                        atomic_bytes(path, body)
                else:
                    atomic_bytes(path, body)
            except BaseException:
                self._record(path, path.stat().st_size if path.is_file() else None)
                raise
            else:
                self._record(path, len(body))
            finally:
                self._storage_reservations -= len(body)

    def append(self, relative: str, body: bytes) -> None:
        self._require_writable()
        path = self.path(relative)
        with self._lock:
            previous = self._sizes.get(str(path), 0)
            self.check_storage(len(body))
            self._storage_reservations += len(body)
            try:
                durable_mkdir(path.parent)
                created = not path.exists()
                with path.open("ab") as stream:
                    with metrics.phase("journal_append"):
                        stream.write(body)
                        stream.flush()
                    metrics.add("fsync_calls")
                    with metrics.phase("file_sync"):
                        os.fsync(stream.fileno())
                if created:
                    fsync_directory(path.parent)
                self._record(path, previous + len(body))
            except BaseException:
                self._record(path, path.stat().st_size if path.is_file() else None)
                raise
            finally:
                self._storage_reservations -= len(body)

    def remove(self, relative: str) -> None:
        self._require_writable()
        path = self.path(relative)
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
        self.sync(relative)

    def lock(self, *, allow_legacy: bool = False) -> Any:
        if not allow_legacy:
            self._require_writable()
        lock = FileLock(str(self.path("writer.lock")), timeout=0)
        try:
            acquisition = lock.acquire()
        except Timeout as exc:
            raise CatalogueError("another writer holds the canonical operator root lock") from exc
        try:
            self._refresh_storage_version(allow_legacy=allow_legacy)
        except BaseException:
            lock.release()
            raise
        with self._lock:
            self._writer_lock = lock
            self._writer_owner = threading.get_ident()
        # FileLock.acquire() returns its context proxy so `with store.lock()` releases the
        # acquisition already made above instead of acquiring it a second time.
        return acquisition

    @property
    def writer_locked_by_current_thread(self) -> bool:
        with self._lock:
            lock = self._writer_lock
            return (
                lock is not None
                and lock.is_locked
                and str(lock.lock_file) == str(self.path("writer.lock"))
                and self._writer_owner == threading.get_ident()
            )

    @contextmanager
    def writer_guard(self) -> Iterator[None]:
        """Hold this root's canonical writer lock, reusing it only on its owner thread."""
        with self._lock:
            held = self._writer_lock
            owned_here = (
                held is not None
                and held.is_locked
                and str(held.lock_file) == str(self.path("writer.lock"))
                and self._writer_owner == threading.get_ident()
            )
        if owned_here:
            self._refresh_storage_version()
            yield
            return
        self.lock()
        with self._lock:
            lock = self._writer_lock
        if lock is None:
            raise CatalogueError("writer lock tracking was lost")
        try:
            yield
        finally:
            with self._lock:
                if self._writer_lock is lock:
                    self._writer_lock = None
                    self._writer_owner = None
            lock.release()

    def upgrade_root(self, lock: FileLock) -> None:
        """Upgrade a legacy root marker while holding the writer lock; evidence is untouched."""
        expected_lock = str(self.path("writer.lock"))
        if not lock.is_locked or str(lock.lock_file) != expected_lock:
            raise CatalogueError("root upgrade requires the writer lock")
        current = decode(regular_bytes(self.path("root.json"), 1024**2))
        version = _root_storage_version(current)
        scope = _root_scope(current)
        if version == CURRENT_ROOT_VERSION:
            self.storage_version = version
            if scope != self._scope:
                raise CatalogueError("operator root scope changed after opening")
            return
        upgraded = series_root_marker(scope) if scope is not None else CURRENT_ROOT_MARKER
        if scope != self._scope:
            raise CatalogueError("operator root scope changed after opening")
        atomic_json(self.path("root.json"), upgraded)
        self.storage_version = CURRENT_ROOT_VERSION

    def reserve_storage(self, amount: int) -> None:
        with self._lock:
            self.check_storage(amount)
            self._storage_reservations += amount

    def release_storage(self, amount: int) -> None:
        with self._lock:
            self._storage_reservations -= amount
            if self._storage_reservations < 0:
                self._storage_reservations = 0
                raise CatalogueError("storage reservation accounting underflow")

    def spool(self, relative: str) -> EvidenceSpool:
        self._require_writable()
        return EvidenceSpool(self, relative)

    def require_writable(self) -> None:
        if self.storage_version != CURRENT_ROOT_VERSION:
            raise CatalogueError("operator root requires explicit upgrade-root before writing")

    def _refresh_storage_version(self, *, allow_legacy: bool = False) -> None:
        marker_bytes = regular_bytes(self.path("root.json"))
        if marker_bytes == CANONICAL_CURRENT_ROOT_MARKER_BYTES:
            if self._scope is not None:
                raise CatalogueError("operator root scope changed after opening")
            self.storage_version = CURRENT_ROOT_VERSION
            return
        marker = decode(marker_bytes)
        version = _root_storage_version(marker)
        scope = _root_scope(marker)
        if scope != self._scope:
            raise CatalogueError("operator root scope changed after opening")
        if version in {2, 3, SCOPED_ROOT_VERSION, CURRENT_ROOT_VERSION}:
            self.storage_version = version
            if version != CURRENT_ROOT_VERSION and not allow_legacy:
                raise CatalogueError("operator root requires explicit upgrade-root before writing")
            return
        self.storage_version = version
        if allow_legacy:
            return
        raise CatalogueError("operator root requires explicit upgrade-root before writing")

    def _require_writable(self) -> None:
        self.require_writable()

    @metrics.phase("metadata_scan")
    def _sync_prefix(
        self,
        path: Path,
        *,
        check_deadline: bool = False,
        require_path: bool = False,
    ) -> None:
        metrics.add("metadata_scans")
        prefix = str(path)
        present: dict[str, int] = {}
        if check_deadline:
            self.check_deadline()
        try:
            info = path.stat()
        except FileNotFoundError as exc:
            if require_path:
                raise CatalogueError("operator root disappeared during reconciliation") from exc
            info = None
        except OSError as exc:
            raise CatalogueError(f"cannot stat operator root path: {path}") from exc
        if require_path and (info is None or not stat.S_ISDIR(info.st_mode)):
            raise CatalogueError("operator root is not a directory during reconciliation")
        if info is not None and stat.S_ISREG(info.st_mode):
            present[prefix] = info.st_size
        elif info is not None and stat.S_ISDIR(info.st_mode):

            def scan_error(error: OSError) -> None:
                raise CatalogueError(f"cannot scan operator root path: {error}") from error

            for base, dirs, files in os.walk(path, followlinks=False, onerror=scan_error):
                if check_deadline:
                    self.check_deadline()
                for name in dirs:
                    if check_deadline:
                        self.check_deadline()
                    child = Path(base) / name
                    try:
                        if path_is_link(child):
                            raise CatalogueError(f"symlink inside operator root: {child}")
                    except OSError as exc:
                        raise CatalogueError(f"cannot stat operator root entry: {child}") from exc
                for name in files:
                    child = Path(base) / name
                    try:
                        if path_is_link(child):
                            raise CatalogueError(f"symlink inside operator root: {child}")
                    except OSError as exc:
                        raise CatalogueError(f"cannot stat operator root entry: {child}") from exc
                    try:
                        size = child.stat().st_size
                    except OSError as exc:
                        raise CatalogueError(f"cannot stat operator root file: {child}") from exc
                    if check_deadline:
                        self.check_deadline()
                    present[str(child)] = size
        if check_deadline:
            self.check_deadline()
        with self._lock:
            for key in [k for k in self._sizes if k == prefix or k.startswith(prefix + os.sep)]:
                if key not in present:
                    self._record(Path(key), None)
            for key, size in present.items():
                self._record(Path(key), size)

    def _record(self, path: Path, size: int | None) -> None:
        key = str(path)
        previous = self._sizes.get(key, 0)
        if size is None:
            self._sizes.pop(key, None)
        else:
            self._sizes[key] = size
        delta = (size or 0) - previous
        if not delta:
            return
        self._usage["retained_bytes"] += delta
        if "tmp" in path.relative_to(self.root).parts or path.name.startswith(".write-"):
            self._usage["temporary_bytes"] += delta
        if path.name.endswith((".duckdb", ".wal")):
            self._usage["database_bytes"] += delta
        metrics.peak("retained", self._usage["retained_bytes"])
        metrics.peak("temporary", self._usage["temporary_bytes"])


class EvidenceSpool:
    """Incremental same-directory file that becomes evidence only after fsync and rename."""

    def __init__(self, store: Store, relative: str):
        self.store = store
        self.destination = store.path(relative)
        durable_mkdir(self.destination.parent)
        fd, name = tempfile.mkstemp(prefix=".write-body-", dir=self.destination.parent)
        self.path = Path(name)
        self.stream = os.fdopen(fd, "wb")
        self.size = 0
        self.closed = False

    def write(self, chunk: bytes, consume_reservation: Callable[[int], None] | None = None) -> None:
        if self.closed:
            raise CatalogueError("evidence spool is already closed")
        with self.store._lock:
            if consume_reservation is not None:
                consume_reservation(len(chunk))
            self.store.check_storage(len(chunk))
            with metrics.phase("raw_write"), metrics.phase("spool_write"):
                self.stream.write(chunk)
            self.size += len(chunk)
            self.store._record(self.path, self.size)
        metrics.add("raw_write_bytes", len(chunk))

    def commit(self) -> None:
        if self.closed:
            raise CatalogueError("evidence spool is already closed")
        self.stream.flush()
        metrics.add("fsync_calls")
        with metrics.phase("file_sync"):
            os.fsync(self.stream.fileno())
        self.stream.close()
        self.closed = True
        with self.store._lock:
            with metrics.phase("atomic_replace"):
                os.replace(self.path, self.destination)
            self.store._record(self.path, None)
            self.store._record(self.destination, self.size)
            fsync_directory(self.destination.parent)

    def abort(self) -> None:
        if not self.closed:
            self.stream.close()
            self.closed = True
        self.path.unlink(missing_ok=True)
        with self.store._lock:
            self.store._record(self.path, None)

    def __enter__(self) -> EvidenceSpool:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is not None or not self.closed:
            self.abort()


@contextmanager
def interrupt_at_deadline(store: Store, interrupt: Callable[[], None]) -> Iterator[None]:
    """Run `interrupt` once the operation deadline passes, while the block is executing."""
    timer = threading.Timer(store.remaining, interrupt)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


class Budget:
    """Request, download and deadline allowance for one invocation. Safe across worker threads."""

    def __init__(
        self,
        store: Store,
        *,
        max_response_bytes: int | None = None,
        max_decoded_response_bytes: int | None = None,
    ):
        self.store = store
        self.limits = store.limits
        self.max_response_bytes = max_response_bytes or store.limits.max_response_bytes
        self.max_decoded_response_bytes = (
            max_decoded_response_bytes or store.limits.max_decoded_response_bytes
        )
        self.requests = 0
        self.downloaded_bytes = 0
        self._inflight = 0
        self._storage_by_thread: dict[int, int] = {}
        self._lock = threading.Lock()

    @property
    def remaining(self) -> float:
        return self.store.remaining

    def reserve(self) -> int:
        _ = self.remaining
        with self._lock:
            if self.requests >= self.limits.max_requests:
                raise BudgetExceeded("request allowance exhausted")
            remaining_bytes = (
                self.limits.max_download_bytes - self.downloaded_bytes - self._inflight
            )
            if remaining_bytes <= 0:
                raise BudgetExceeded("download allowance exhausted")
            allowance = min(self.max_response_bytes, remaining_bytes)
            storage_reservation = allowance + self.max_decoded_response_bytes
            self.requests += 1
            self._inflight += allowance
        try:
            self.store.reserve_storage(storage_reservation)
        except BaseException:
            with self._lock:
                self.requests -= 1
                self._inflight -= allowance
            raise
        with self._lock:
            self._storage_by_thread[threading.get_ident()] = storage_reservation
        return allowance

    def consume_storage(self, amount: int) -> None:
        ident = threading.get_ident()
        with self._lock:
            remaining = self._storage_by_thread.get(ident, 0)
            consumed = min(amount, remaining)
            self._storage_by_thread[ident] = remaining - consumed
        if consumed:
            self.store.release_storage(consumed)

    def settle(self, allowance: int) -> None:
        ident = threading.get_ident()
        with self._lock:
            self._inflight -= allowance
            storage_reservation = self._storage_by_thread.pop(ident, 0)
        self.store.release_storage(storage_reservation)

    def add_download(self, size: int) -> int:
        with self._lock:
            self.downloaded_bytes += size
            return self.downloaded_bytes

    def result(self) -> dict[str, Any]:
        with self._lock:
            return {
                "requests": self.requests,
                "downloaded_bytes": self.downloaded_bytes,
                "accounting_complete": True,
            }
