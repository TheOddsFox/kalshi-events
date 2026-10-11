"""Durable local evidence, strict paths and operation ceilings."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from filelock import FileLock, Timeout

MAX_BUNDLE_BYTES = 128 * 1024**2
TICKER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")


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


def decode(body: bytes) -> Any:
    try:
        return json.loads(body, parse_float=Decimal, parse_constant=lambda _: _reject(None))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise CatalogueError("invalid JSON response or evidence") from exc


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def file_digest(path: Path, limit: int = MAX_BUNDLE_BYTES) -> tuple[str, int]:
    path = safe_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise CatalogueError("missing, nonregular or oversized evidence file")
    checksum = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            size += len(chunk)
            if size > limit:
                raise CatalogueError("evidence file grew beyond byte ceiling")
            checksum.update(chunk)
    return checksum.hexdigest(), size


def ticker(value: Any) -> str:
    if not isinstance(value, str) or not TICKER.fullmatch(value):
        raise CatalogueError("invalid native ticker")
    return value


def selections(values: list[str]) -> list[str]:
    if not 1 <= len(values) <= 100 or len(set(values)) != len(values):
        raise CatalogueError("select 1 to 100 distinct explicit market tickers")
    return [ticker(v) for v in values]


def safe_path(path: Path) -> Path:
    path = path.absolute()
    for part in (path, *path.parents):
        if part.is_symlink():
            raise CatalogueError(f"symlink is forbidden: {part}")
    return path.resolve()


def regular_bytes(path: Path, limit: int = MAX_BUNDLE_BYTES) -> bytes:
    path = safe_path(path)
    if not path.is_file() or path.stat().st_size > limit:
        raise CatalogueError(f"missing, nonregular or oversized file: {path}")
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise CatalogueError("file grew beyond limit")
    return body


def atomic_bytes(path: Path, body: bytes) -> None:
    path = safe_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(name).unlink(missing_ok=True)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(path, json_bytes(value))


@dataclass(frozen=True)
class Limits:
    max_requests: int = 25_000
    max_download_bytes: int = 4 * 1024**3
    max_response_bytes: int = 16 * 1024**2
    max_duration_seconds: float = 4 * 3600
    max_root_bytes: int = 64 * 1024**3
    max_temp_bytes: int = 8 * 1024**3
    max_database_bytes: int = 2 * 1024**3

    def __post_init__(self) -> None:
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
    def __init__(self, root: Path, limits: Limits | None = None):
        self.root = safe_path(root)
        if self.root == Path(self.root.anchor):
            raise CatalogueError("filesystem root cannot be an operator root")
        self.limits = limits or Limits()
        self.started = time.monotonic()
        self.root.mkdir(parents=True, exist_ok=True)
        marker = self.root / "root.json"
        if not marker.exists():
            if any(self.root.iterdir()):
                raise CatalogueError("operator root must be new, empty, or a Kalshi events root")
            atomic_json(marker, {"owner": "oddsfox.kalshi.events.v1"})
        if decode(regular_bytes(marker)) != {"owner": "oddsfox.kalshi.events.v1"}:
            raise CatalogueError("operator root belongs to another application")
        self.check_storage()

    @property
    def remaining(self) -> float:
        seconds = self.limits.max_duration_seconds - (time.monotonic() - self.started)
        if seconds <= 0:
            raise BudgetExceeded("operation deadline exhausted")
        return seconds

    def path(self, relative: str) -> Path:
        result = safe_path(self.root / relative)
        if not result.is_relative_to(self.root):
            raise CatalogueError("path escapes operator root")
        return result

    def check_storage(self, additional: int = 0) -> dict[str, int]:
        total = temporary = database = 0
        for base, dirs, files in os.walk(self.root, followlinks=False):
            for name in (*dirs, *files):
                child = Path(base) / name
                if child.is_symlink():
                    raise CatalogueError(f"symlink inside operator root: {child}")
            for name in files:
                child = Path(base) / name
                size = child.stat().st_size
                total += size
                if "tmp" in child.relative_to(self.root).parts or name.startswith(".write-"):
                    temporary += size
                if name.endswith((".duckdb", ".wal")):
                    database += size
        if total + additional > self.limits.max_root_bytes:
            raise BudgetExceeded("operator root storage exhausted")
        if temporary + additional > self.limits.max_temp_bytes:
            raise BudgetExceeded("temporary storage exhausted")
        if database > self.limits.max_database_bytes:
            raise BudgetExceeded("database storage exhausted")
        return {"retained_bytes": total, "temporary_bytes": temporary, "database_bytes": database}

    def write(self, relative: str, value: Any) -> None:
        body = json_bytes(value)
        self.check_storage(len(body))
        atomic_bytes(self.path(relative), body)

    def lock(self) -> FileLock:
        lock = FileLock(str(self.path("writer.lock")), timeout=0)
        try:
            lock.acquire()
        except Timeout as exc:
            raise CatalogueError("another writer holds the canonical operator root lock") from exc
        return lock


class Budget:
    def __init__(self, store: Store):
        self.store = store
        self.limits = store.limits
        self.started = time.monotonic()
        self.requests = 0
        self.downloaded_bytes = 0

    @property
    def remaining(self) -> float:
        return self.store.remaining

    def reserve(self) -> int:
        _ = self.remaining
        if self.requests >= self.limits.max_requests:
            raise BudgetExceeded("request allowance exhausted")
        remaining_bytes = self.limits.max_download_bytes - self.downloaded_bytes
        if remaining_bytes <= 0:
            raise BudgetExceeded("download allowance exhausted")
        allowance = min(self.limits.max_response_bytes, remaining_bytes)
        self.store.check_storage(allowance * 2)
        self.requests += 1
        return allowance

    def result(self) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "downloaded_bytes": self.downloaded_bytes,
            "accounting_complete": True,
        }
