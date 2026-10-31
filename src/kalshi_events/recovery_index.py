"""Disposable, journal-anchored SQLite hints for full-capture writer recovery."""

from __future__ import annotations

import os
import sqlite3
import stat
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

from kalshi_events.common import CatalogueError, durable_mkdir, fsync_directory
from kalshi_events.normalize import PARENT_INDEX_SCHEMA, DiskParentIndex

RECOVERY_INDEX_SCHEMA = 1
NORMALIZER_VERSION = 2
_DIRECTORY = "tmp/recovery-index"
_META_SCHEMA = """create table recovery_meta(
    id integer primary key check(id=1),
    schema_version integer not null,
    normalizer_version integer not null,
    root_id text not null,
    capture_id text not null,
    settings_sha256 text not null,
    indexed_pages integer not null,
    journal_seq integer not null,
    journal_offset integer not null,
    journal_sha256 text not null
)"""
_AUXILIARY_SCHEMA = {
    "selected_membership": "create table selected_membership(scope text,ticker text,primary key(scope,ticker)) without rowid",
    "effective_lookups": "create table effective_lookups(kind text,ticker text,status int,page_id text,primary key(kind,ticker)) without rowid",
    "required_parents": "create table required_parents(kind text,ticker text,primary key(kind,ticker)) without rowid",
    "listed_union": "create table listed_union(scope text,ticker text,primary key(scope,ticker)) without rowid",
    "listed_final": "create table listed_final(scope text,ticker text,primary key(scope,ticker)) without rowid",
}
_SIDECARS = ("index.sqlite-journal", "index.sqlite-wal", "index.sqlite-shm")
_ALLOWED_FILES = {"index.sqlite", *_SIDECARS}


class UnsafeRecoveryIndexPermissions(CatalogueError):
    """The persistent cache is unavailable under the current directory permissions."""


def _owned_by_current_user(details: os.stat_result) -> bool:
    return not hasattr(os, "geteuid") or details.st_uid == os.geteuid()


def sealed_settings_sha256(state: dict[str, Any]) -> str:
    """Hash only immutable acquisition settings, excluding mutable task progress."""
    from kalshi_events.common import digest, json_bytes

    fields = (
        "acquisition_protocol",
        "acquisition_policy_sha256",
        "max_response_bytes",
        "max_decoded_response_bytes",
        "compression",
        "rate_policy",
        "selected",
        "incremental",
        "scope",
        "task_scopes",
        "cutoff_policy",
        "parent_policy",
        "root_id",
    )
    return digest(json_bytes({name: state.get(name) for name in fields}))


def _expected_schema() -> tuple[tuple[str, str, str, str], ...]:
    db = sqlite3.connect(":memory:")
    try:
        db.executescript(PARENT_INDEX_SCHEMA)
        db.execute(_META_SCHEMA)
        for statement in _AUXILIARY_SCHEMA.values():
            db.execute(statement)
        return tuple(
            db.execute(
                "select type,name,tbl_name,sql from sqlite_schema "
                "where name not like 'sqlite_%' order by type,name"
            ).fetchall()
        )
    finally:
        db.close()


_EXPECTED_SCHEMA = _expected_schema()


class RecoveryParentIndex(DiskParentIndex):
    """Parent index persisted as a cache, never as acquisition evidence."""

    def __init__(
        self,
        store: Any,
        capture_id: str,
        root_id: str,
        settings_sha256: str,
        *,
        page_rows: list[dict[str, Any]],
        page_anchors: dict[str, tuple[int, int, str]],
        genesis_anchor: tuple[int, int, str],
    ) -> None:
        self.store = store
        self.collect_winners = True
        self.processed: set[str] = set()
        self.defer_page_merges = False
        self.capture_id = capture_id
        self.root_id = root_id
        self.settings_sha256 = settings_sha256
        self.page_rows = page_rows
        self.page_anchors = page_anchors
        self.genesis_anchor = genesis_anchor
        self.directory_relative = f"{_DIRECTORY}/{capture_id}"
        self.relative = f"{self.directory_relative}/index.sqlite"
        self.path = store.path(self.relative)
        self.db: sqlite3.Connection | None = None
        self.reused_pages = 0
        self.watermark_pages = 0
        self.cache_present = False
        self._pending_pages = 0
        self._last_checkpoint = time.monotonic()
        self._merging_page = False
        self._closed = False

        self._ensure_cache_directory()
        directory = store.path(self.directory_relative)
        self._validate_layout(directory)
        if self.path.is_symlink():
            raise CatalogueError("recovery index file is unsafe")
        if self.path.exists():
            self.cache_present = True
            try:
                self._open_existing()
            except _InvalidCache:
                self._close_db()
                self._remove_owned_cache()
                self._open_new()
        else:
            if self._existing_sidecars():
                self._remove_owned_cache()
                self._ensure_cache_directory()
            self._open_new()

    def _ensure_cache_directory(self) -> None:
        directory = self.store.path(self.directory_relative)
        if directory.is_symlink():
            raise CatalogueError("recovery index directory is unsafe")
        if not directory.exists():
            durable_mkdir(directory.parent)
            # Check every ancestor before creating the capture-specific leaf. An unsafe
            # shared parent is an eligibility limit, and must not leave a cache directory
            # that later retirement cannot safely remove.
            self._validate_parent_layout(directory.parent)
            try:
                directory.mkdir(mode=0o700)
            except FileExistsError:
                pass
            else:
                fsync_directory(directory.parent)
        self._validate_layout(directory)

    def _validate_layout(self, directory: Path) -> None:
        if directory.is_symlink() or not directory.is_dir():
            raise CatalogueError("recovery index directory is unsafe")
        details = directory.stat(follow_symlinks=False)
        if not _owned_by_current_user(details) or stat.S_IMODE(details.st_mode) & 0o077:
            raise UnsafeRecoveryIndexPermissions("recovery index directory permissions are unsafe")
        self._validate_parent_layout(directory.parent)
        try:
            entries = {entry.name for entry in directory.iterdir()}
        except OSError:
            raise
        if entries - _ALLOWED_FILES:
            raise CatalogueError("recovery index directory contains an unexpected file")
        for name in entries:
            path = directory / name
            if path.is_symlink():
                raise CatalogueError("recovery index file is unsafe")
            try:
                details = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1:
                raise CatalogueError("recovery index file is unsafe")
            if not _owned_by_current_user(details) or stat.S_IMODE(details.st_mode) & 0o077:
                raise UnsafeRecoveryIndexPermissions("recovery index file permissions are unsafe")

    def _validate_parent_layout(self, parent: Path) -> None:
        while parent != self.store.root:
            if parent.is_symlink() or not parent.is_dir():
                raise CatalogueError("recovery index parent directory is unsafe")
            parent_details = parent.stat(follow_symlinks=False)
            if (
                not _owned_by_current_user(parent_details)
                or stat.S_IMODE(parent_details.st_mode) & 0o022
            ):
                raise UnsafeRecoveryIndexPermissions("recovery index parent permissions are unsafe")
            parent = parent.parent
        root_details = self.store.root.stat(follow_symlinks=False)
        if not _owned_by_current_user(root_details) or stat.S_IMODE(root_details.st_mode) & 0o022:
            raise UnsafeRecoveryIndexPermissions(
                "operator root permissions are unsafe for a recovery index"
            )

    def _existing_sidecars(self) -> bool:
        directory = self.store.path(self.directory_relative)
        return any((directory / name).exists() for name in _SIDECARS)

    def _connect(self) -> sqlite3.Connection:
        if not self.path.exists():
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(self.path, flags, 0o600)
            os.close(fd)
        self._validate_layout(self.path.parent)
        before = self.path.stat(follow_symlinks=False)
        db = sqlite3.connect(self.path, timeout=0)
        after = self.path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(after.st_mode)
            or before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
        ):
            db.close()
            raise CatalogueError("recovery index file changed while opening")
        db.enable_load_extension(False)
        db.execute("pragma trusted_schema=off")
        db.execute("pragma cache_size=-8192")
        db.execute("pragma temp_store=FILE")
        db.execute("pragma automatic_index=OFF")
        db.execute("pragma mmap_size=0")
        db.execute("pragma journal_mode=DELETE")
        db.execute("pragma synchronous=FULL")
        usage = self.store.usage()
        remaining = min(
            self.store.limits.max_temp_bytes - usage["temporary_bytes"],
            self.store.limits.max_root_bytes - usage["retained_bytes"],
        )
        page_size = db.execute("pragma page_size").fetchone()[0]
        current_pages = max(1, db.execute("pragma page_count").fetchone()[0])
        db.execute(f"pragma max_page_count={current_pages + max(0, remaining) // page_size}")
        deadline = self.store.started + self.store.limits.max_duration_seconds
        self._deadline_hit = False

        def interrupt_at_deadline() -> int:
            if time.monotonic() >= deadline:
                self._deadline_hit = True
                return 1
            return 0

        db.set_progress_handler(interrupt_at_deadline, 10_000)
        return db

    def _open_existing(self) -> None:
        try:
            self.db = self._connect()
            self._validate_schema()
            row = self.db.execute(
                "select schema_version,normalizer_version,root_id,capture_id,settings_sha256,"
                "indexed_pages,journal_seq,journal_offset,journal_sha256 "
                "from recovery_meta where id=1"
            ).fetchone()
            if row is None:
                raise _InvalidCache
            schema, normalizer, root, capture, settings, count, seq, offset, checksum = row
            if (
                schema != RECOVERY_INDEX_SCHEMA
                or normalizer != NORMALIZER_VERSION
                or root != self.root_id
                or capture != self.capture_id
                or settings != self.settings_sha256
                or isinstance(count, bool)
                or not isinstance(count, int)
                or not 0 <= count <= len(self.page_rows)
                or isinstance(seq, bool)
                or not isinstance(seq, int)
                or isinstance(offset, bool)
                or not isinstance(offset, int)
                or not isinstance(checksum, str)
            ):
                raise _InvalidCache
            expected_anchor = (
                self.genesis_anchor
                if count == 0
                else self.page_anchors.get(self.page_rows[count - 1]["relative"])
            )
            if expected_anchor != (seq, offset, checksum):
                raise _InvalidCache
            self.reused_pages = count
            self.watermark_pages = count
        except _InvalidCache:
            raise
        except sqlite3.DatabaseError as exc:
            if _cache_corruption(exc):
                raise _InvalidCache from exc
            raise

    def _open_new(self) -> None:
        self._ensure_cache_directory()
        self.db = self._connect()
        try:
            self.db.executescript(f"begin immediate;\n{PARENT_INDEX_SCHEMA}\n{_META_SCHEMA};")
            self.db.execute(
                "insert into recovery_meta values(1,?,?,?,?,?,?,?,?,?)",
                (
                    RECOVERY_INDEX_SCHEMA,
                    NORMALIZER_VERSION,
                    self.root_id,
                    self.capture_id,
                    self.settings_sha256,
                    0,
                    *self.genesis_anchor,
                ),
            )
            self.db.commit()
            self._sync_directory()
        except BaseException:
            self._close_db()
            raise

    def _validate_schema(self) -> None:
        rows = tuple(
            self.db.execute(
                "select type,name,tbl_name,sql from sqlite_schema "
                "where name not like 'sqlite_%' order by type,name"
            ).fetchall()
        )
        expected = {row[1]: row for row in _EXPECTED_SCHEMA}
        base_names = expected.keys() - _AUXILIARY_SCHEMA.keys()
        actual = {row[1]: row for row in rows}
        if not base_names.issubset(actual) or any(
            actual[name] != expected[name] for name in base_names
        ):
            raise _InvalidCache
        for name, row in actual.items():
            if name not in expected or row != expected[name]:
                raise _InvalidCache

    def _sync_directory(self) -> None:
        self.store.sync(self.directory_relative)
        self.store.check_storage()
        self.store.check_deadline()

    def _sync(self) -> None:
        if self._merging_page:
            self.store.check_deadline()
            return
        self.store.check_deadline()
        if self._pending_pages and (
            self._pending_pages >= 100 or time.monotonic() - self._last_checkpoint >= 5
        ):
            self.flush()
        else:
            self._sync_directory()

    def flush(self, *, force: bool = False) -> None:
        if self.db is None or not (force or self._pending_pages):
            self.store.check_deadline()
            return
        if not force and self._pending_pages < 100 and time.monotonic() - self._last_checkpoint < 5:
            return
        self.store.check_deadline()
        self._sync_directory()
        self.db.commit()
        self.watermark_pages += self._pending_pages
        self._pending_pages = 0
        self._last_checkpoint = time.monotonic()
        self._sync_directory()

    def merge_at_anchor(
        self,
        page: Any,
        relative: str,
        anchor: tuple[int, int, str],
    ) -> None:
        if self.db is None:
            raise CatalogueError("recovery index is closed")
        self._merging_page = True
        try:
            self._merge(page)
            expected_count = self._pending_pages + self.watermark_pages + 1
            # The cache watermark is a prefix, so a page can only extend the next slot.
            if (
                expected_count > len(self.page_rows)
                or self.page_rows[expected_count - 1]["relative"] != relative
            ):
                raise CatalogueError("recovery index page order differs from the checked journal")
            self.db.execute(
                "update recovery_meta set indexed_pages=?,journal_seq=?,journal_offset=?,journal_sha256=? where id=1",
                (expected_count, *anchor),
            )
            self._pending_pages += 1
            self.processed.add(relative)
        except sqlite3.OperationalError as exc:
            self.db.rollback()
            self._translate_sql_deadline(exc)
        except BaseException:
            self.db.rollback()
            raise
        finally:
            self._merging_page = False
        self.store.check_deadline()
        if self._pending_pages >= 100 or time.monotonic() - self._last_checkpoint >= 5:
            self.flush()
        else:
            self._sync_directory()

    def reset(self) -> None:
        if self.db is None:
            raise CatalogueError("recovery index is closed")
        if self._pending_pages:
            raise CatalogueError("cannot reset a recovery index with uncheckpointed pages")
        self.db.rollback()
        self._close_db()
        self._remove_owned_cache()
        self.processed.clear()
        self.watermark_pages = 0
        self._pending_pages = 0
        self._last_checkpoint = time.monotonic()
        self._open_new()

    def invalidate_and_reset(self) -> None:
        """Replace only this derived cache after a recognized SQLite corruption error."""
        self._close_db()
        self._remove_owned_cache()
        self.processed.clear()
        self.reused_pages = 0
        self.watermark_pages = 0
        self._pending_pages = 0
        self._open_new()

    def _remove_owned_cache(self) -> None:
        directory = self.store.path(self.directory_relative)
        if not directory.exists():
            return
        self._validate_layout(directory)
        for name in sorted(_ALLOWED_FILES):
            path = directory / name
            if path.exists():
                self.store.remove(f"{self.directory_relative}/{name}")
        self.store.remove(self.directory_relative)

    def _close_db(self) -> None:
        db, self.db = self.db, None
        if db is not None:
            db.close()

    def close(self, *, flush: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        if self.db is None:
            return
        try:
            if flush:
                self.flush(force=True)
            else:
                self.db.rollback()
        finally:
            self._close_db()
            with suppress(Exception):
                self.store.sync(self.directory_relative)

    def __del__(self) -> None:
        with suppress(Exception):
            self.close(flush=False)


class _InvalidCache(Exception):
    pass


def _cache_corruption(exc: sqlite3.DatabaseError) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int) and (code & 0xFF) in {
        sqlite3.SQLITE_CORRUPT,
        sqlite3.SQLITE_NOTADB,
        sqlite3.SQLITE_SCHEMA,
    }:
        return True
    message = str(exc).lower()
    return any(
        fragment in message
        for fragment in (
            "database disk image is malformed",
            "file is not a database",
            "no such table",
        )
    )


def remove_capture_cache(store: Any, capture_id: str) -> None:
    """Remove a cache only after its owning capture has been safely retired."""
    if len(capture_id) != 32 or any(char not in "0123456789abcdef" for char in capture_id):
        raise CatalogueError("invalid recovery cache identity")
    directory_relative = f"{_DIRECTORY}/{capture_id}"
    directory = store.path(directory_relative)
    if not directory.exists():
        return
    if directory.is_symlink() or not directory.is_dir():
        raise CatalogueError("recovery index directory is unsafe")
    details = directory.stat(follow_symlinks=False)
    if not _owned_by_current_user(details) or stat.S_IMODE(details.st_mode) & 0o077:
        raise CatalogueError("recovery index directory permissions are unsafe")
    parent = directory.parent
    while parent != store.root:
        if parent.is_symlink() or not parent.is_dir():
            raise CatalogueError("recovery index parent directory is unsafe")
        parent_details = parent.stat(follow_symlinks=False)
        if (
            not _owned_by_current_user(parent_details)
            or stat.S_IMODE(parent_details.st_mode) & 0o022
        ):
            raise CatalogueError("recovery index parent permissions are unsafe")
        parent = parent.parent
    root_details = store.root.stat(follow_symlinks=False)
    if not _owned_by_current_user(root_details) or stat.S_IMODE(root_details.st_mode) & 0o022:
        raise CatalogueError("operator root permissions are unsafe for a recovery index")
    names = {entry.name for entry in directory.iterdir()}
    if names - _ALLOWED_FILES:
        raise CatalogueError("recovery index directory contains an unexpected file")
    for name in names:
        path = directory / name
        details = path.stat(follow_symlinks=False)
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
            or not _owned_by_current_user(details)
            or stat.S_IMODE(details.st_mode) & 0o077
        ):
            raise CatalogueError("recovery index file is unsafe")
    for name in sorted(names):
        store.remove(f"{directory_relative}/{name}")
    store.remove(directory_relative)
