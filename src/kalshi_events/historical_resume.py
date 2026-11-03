"""Private helpers for validating resumable historical build stages."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import suppress
from typing import Any

from kalshi_events.common import BudgetExceeded, CatalogueError, digest, json_bytes

_STAGE_NAME = re.compile(r"build-historical-[0-9a-f]{32}")


class PageContextIndex:
    """Disk-backed page identity deduplication, rebuilt while replaying a prefix."""

    def __init__(self, store: Any, relative: str, *, max_entries: int):
        self.store = store
        self.relative = relative
        path = store.path(relative)
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if (
                not path.is_file()
                or path.is_symlink()
                or info.st_nlink != 1
                or info.st_dev != store.root.lstat().st_dev
            ):
                raise CatalogueError("historical page index is unsafe")
            store.remove(relative)
        if isinstance(max_entries, bool) or not isinstance(max_entries, int) or max_entries < 0:
            raise CatalogueError("historical page index bound is invalid")
        usage = store.usage()
        held = getattr(store, "_storage_reservations", 0)
        remaining = min(
            store.limits.max_temp_bytes - usage["temporary_bytes"] - held,
            store.limits.max_root_bytes - usage["retained_bytes"] - held,
        )
        if remaining < 4096:
            raise BudgetExceeded(
                "no bounded temporary storage is available for historical page index"
            )
        self.reservation = min(128 * 1024**2, max(16 * 1024, max_entries * 512), remaining)
        store.reserve_storage(self.reservation)
        self._deadline_hit = False
        self.db = None
        self._added = 0
        try:
            self.db = sqlite3.connect(path)
            self.db.execute("pragma cache_size=-8192")
            self.db.execute("pragma temp_store=FILE")
            self.db.execute("pragma mmap_size=0")
            self.db.execute("pragma journal_mode=OFF")
            self.db.execute("pragma synchronous=OFF")
            self.db.execute(f"pragma max_page_count={self.reservation // 4096}")
            self.db.execute(
                "create table pages(page_id text primary key, context text not null) without rowid"
            )
            self.db.set_progress_handler(self._progress, 10_000)
            self.db.commit()
        except BaseException as exc:
            try:
                if self.db is not None:
                    self.db.close()
                    self.db = None
                if path.exists():
                    with suppress(Exception):
                        store.sync_file(relative)
                    with suppress(Exception):
                        store.remove(relative)
            finally:
                store.release_storage(self.reservation)
            if isinstance(exc, sqlite3.Error) and "full" in str(exc).lower():
                raise BudgetExceeded(
                    "temporary storage exhausted indexing historical pages"
                ) from exc
            if isinstance(exc, sqlite3.Error):
                raise CatalogueError("historical page index could not be created") from exc
            raise

    def _progress(self) -> int:
        try:
            self.store.check_deadline()
            return 0
        except BudgetExceeded:
            self._deadline_hit = True
            return 1

    def reset(self) -> None:
        try:
            self.db.execute("delete from pages")
            self.db.commit()
        except sqlite3.Error as exc:
            if self._deadline_hit:
                raise BudgetExceeded(
                    "operation deadline exhausted resetting historical page index"
                ) from exc
            if "full" in str(exc).lower():
                raise BudgetExceeded(
                    "temporary storage exhausted resetting historical page index"
                ) from exc
            raise CatalogueError("historical page index could not be reset") from exc

    def add(self, page_id: str, context: str) -> bool:
        if (
            re.fullmatch(r"[0-9a-f]{32}", page_id) is None
            or re.fullmatch(r"[0-9a-f]{64}", context) is None
        ):
            raise CatalogueError("historical page identity or context is invalid")
        try:
            previous = self.db.execute(
                "select context from pages where page_id=?", [page_id]
            ).fetchone()
            if previous is not None:
                if previous[0] != context:
                    raise CatalogueError("historical page identity has contradictory context")
                return False
            self.db.execute("insert into pages values(?,?)", [page_id, context])
            self._added += 1
            if self._added % 1024 == 0:
                self.db.commit()
            return True
        except sqlite3.Error as exc:
            if self._deadline_hit:
                raise BudgetExceeded(
                    "operation deadline exhausted indexing historical pages"
                ) from exc
            if "full" in str(exc).lower():
                raise BudgetExceeded(
                    "temporary storage exhausted indexing historical pages"
                ) from exc
            raise CatalogueError("historical page index is invalid") from exc

    def close(self) -> None:
        db = getattr(self, "db", None)
        if db is None:
            return
        self.db = None
        try:
            with suppress(Exception):
                db.close()
            path = self.store.path(self.relative)
            if path.exists():
                with suppress(Exception):
                    self.store.sync_file(self.relative)
                self.store.remove(self.relative)
        finally:
            self.store.release_storage(self.reservation)


def stage_relative(name: str) -> str:
    """Return the canonical managed stage path for an explicit resume name."""
    if not isinstance(name, str) or _STAGE_NAME.fullmatch(name) is None:
        raise CatalogueError("historical resume stage identity is invalid")
    return f"tmp/{name}"


def proof_record(phase: str, binding: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    body = {"version": 1, "phase": phase, "binding": binding, "result": result}
    return {**body, "sha256": digest(json_bytes(body))}


def validate_proof(value: Any, phase: str, binding: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("version") != 1 or value.get("phase") != phase:
        raise CatalogueError("historical resume proof is missing or invalid")
    body = {key: value.get(key) for key in ("version", "phase", "binding", "result")}
    if value.get("binding") != binding or value.get("sha256") != digest(json_bytes(body)):
        raise CatalogueError("historical resume proof binding does not match its stage")
    result = value.get("result")
    if not isinstance(result, dict):
        raise CatalogueError("historical resume proof result is invalid")
    return result


def _resume_file_identity(value: Any) -> tuple[int, int, int, int]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) != 4
        or any(type(part) is not int for part in value)
        or value[0] < 0
        or value[1] < 0
        or value[2] < 0
    ):
        raise CatalogueError("historical resume database identity is invalid")
    return tuple(value)


def _verified_mount_values(
    store: Any, state: dict[str, Any], verified_mount: dict[str, Any]
) -> tuple[int, set[int]]:
    if not isinstance(verified_mount, dict):
        raise CatalogueError("historical verified mount context is invalid")
    from kalshi_events.incremental import root_id

    root_path = str(store.root.resolve())
    capture_id = state.get("capture_id")
    current_device = verified_mount.get("current_device")
    lineage = verified_mount.get("device_lineage")
    try:
        live_device = store.root.lstat().st_dev
    except OSError as exc:
        raise CatalogueError("historical verified mount root is unavailable") from exc
    live_root_id = root_id(store)
    if (
        verified_mount.get("root_path") != root_path
        or not isinstance(verified_mount.get("root_id"), str)
        or re.fullmatch(r"[0-9a-f]{32}", verified_mount["root_id"]) is None
        or verified_mount.get("root_id") != live_root_id
        or not isinstance(capture_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", capture_id) is None
        or verified_mount.get("capture_id") != capture_id
        or type(current_device) is not int
        or current_device < 0
        or current_device != live_device
        or not isinstance(lineage, (list, tuple))
        or any(type(device) is not int or device < 0 for device in lineage)
        or len(set(lineage)) != len(lineage)
        or current_device not in lineage
    ):
        raise CatalogueError("historical verified mount context does not match its root")
    return current_device, set(lineage)


def validate_resume_mount_context(
    *, store: Any, state: dict[str, Any], verified_mount: dict[str, Any]
) -> None:
    """Validate an ephemeral controller mount binding without changing proof data."""
    _verified_mount_values(store, state, verified_mount)


def normalize_resume_file_identity(
    persisted: Any,
    *,
    store: Any,
    state: dict[str, Any],
    verified_mount: dict[str, Any] | None = None,
) -> tuple[int, int, int, int]:
    """Normalize only a historical resume identity's device under a verified mount."""
    identity = _resume_file_identity(persisted)
    if verified_mount is None:
        return identity
    current_device, lineage = _verified_mount_values(store, state, verified_mount)
    if identity[0] not in lineage:
        raise CatalogueError("historical resume source device is outside the verified lineage")
    return (current_device, *identity[1:])


def resume_file_identity_matches(
    persisted: Any,
    live: Any,
    *,
    store: Any,
    state: dict[str, Any],
    verified_mount: dict[str, Any] | None = None,
) -> bool:
    """Match a persisted source identity to a live file without weakening generic checks."""
    expected = _resume_file_identity(persisted)
    actual = _resume_file_identity(live)
    if verified_mount is None:
        return expected == actual
    normalized = normalize_resume_file_identity(
        expected,
        store=store,
        state=state,
        verified_mount=verified_mount,
    )
    if actual[0] != normalized[0]:
        raise CatalogueError("historical resume source is not on the current operator mount")
    return normalized == actual


def code_identity(files: Iterable[tuple[str, bytes]]) -> str:
    """Hash named code inputs with unambiguous length framing."""
    checksum = hashlib.sha256()
    for name, body in sorted(files):
        encoded_name = name.encode("utf-8")
        checksum.update(len(encoded_name).to_bytes(8, "big"))
        checksum.update(encoded_name)
        checksum.update(len(body).to_bytes(8, "big"))
        checksum.update(body)
    return checksum.hexdigest()


def _logical_schema(connection: Any, columns: Sequence[str], nullable: set[str]) -> None:
    description = connection.execute("describe landing.observations").fetchall()
    if [row[0] for row in description] != list(columns):
        raise CatalogueError("resumed historical landing schema differs from the expected columns")
    for row in description:
        if row[1].upper() != "VARCHAR" or (row[2] == "YES") != (row[0] in nullable):
            raise CatalogueError(
                "resumed historical landing schema differs from the expected types"
            )


def _next_window_end(
    connection: Any, columns: Sequence[str], start: int, row_limit: int, byte_limit: int
) -> int:
    return _next_relation_window_end(
        connection, "landing.observations", columns, start, row_limit, byte_limit
    )


def _next_relation_window_end(
    connection: Any,
    relation: str,
    columns: Sequence[str],
    start: int,
    row_limit: int,
    byte_limit: int,
) -> int:
    lengths = " + ".join(f"strlen(coalesce(cast({column} as varchar), ''))" for column in columns)
    query = f"""select max(rowid) from (
        select rowid,
               sum(row_bytes) over (order by rowid rows unbounded preceding) as window_bytes
        from (
            select rowid, {lengths} as row_bytes
            from {relation}
            where rowid >= ? and rowid < ?
        ) sized
    ) where window_bytes <= ? or rowid = ?"""
    end = connection.execute(query, [start, start + row_limit, byte_limit, start]).fetchone()[0]
    if end is None:
        raise CatalogueError("resumed historical landing row windows are not dense")
    return int(end)


def compare_landing_rows(
    connection: Any,
    expected_rows: Iterator[Sequence[str | None]],
    columns: Sequence[str],
    *,
    nullable: set[str],
    check_deadline: Any,
    row_limit: int = 4096,
    byte_limit: int = 8 * 1024 * 1024,
) -> int:
    """Compare every logical landing value in bounded rowid and UTF-8 byte windows."""
    _logical_schema(connection, columns, nullable)
    count, minimum, maximum = connection.execute(
        "select count(*), min(rowid), max(rowid) from landing.observations"
    ).fetchone()
    if count and (minimum != 0 or maximum != count - 1):
        raise CatalogueError("resumed historical landing row positions are not dense")
    if not count and (minimum is not None or maximum is not None):
        raise CatalogueError("resumed historical landing row count is invalid")

    projection = ", ".join(columns)
    checked = 0
    start = 0
    while start < count:
        check_deadline()
        end = _next_window_end(connection, columns, start, row_limit, byte_limit)
        if end < start or end >= count:
            raise CatalogueError("resumed historical landing row window is invalid")
        rows = connection.execute(
            f"select rowid, {projection} from landing.observations "
            "where rowid between ? and ? order by rowid",
            [start, end],
        ).fetchall()
        if len(rows) != end - start + 1:
            raise CatalogueError("resumed historical landing row window has a gap")
        for rowid, *actual in rows:
            if rowid != checked:
                raise CatalogueError("resumed historical landing row positions are not dense")
            try:
                expected = next(expected_rows)
            except StopIteration as exc:
                raise CatalogueError("resumed historical landing has extra observations") from exc
            if tuple(actual) != tuple(expected):
                raise CatalogueError(
                    "resumed historical landing differs from receipt-normalized rows"
                )
            checked += 1
        start = end + 1

    try:
        next(expected_rows)
    except StopIteration:
        return checked
    raise CatalogueError("resumed historical landing is missing observations")


def _landing_prefix_update(
    checksum: Any, values: Sequence[str | None], position: dict[str, Any]
) -> None:
    encoded = json_bytes([list(values), position])
    checksum.update(len(encoded).to_bytes(8, "big"))
    checksum.update(encoded)


def resume_landing_cursor(
    expected_rows: Callable[[], Iterator[tuple[Sequence[str | None], dict[str, Any]]]],
    cursor: Any,
    binding: dict[str, Any],
    *,
    check_deadline: Any,
) -> tuple[
    Iterator[tuple[Sequence[str | None], dict[str, Any]]], Any, int, dict[str, Any] | None, int
]:
    """Validate a persisted source prefix, then return its unconsumed stream and digest state."""
    iterator = expected_rows()
    checksum = hashlib.sha256()
    if cursor is None:
        return iterator, checksum, 0, None, 0
    try:
        result = validate_proof(cursor, "historical-landing-cursor", binding)
        count = result.get("observation_position")
        prefix_sha256 = result.get("prefix_sha256")
        position = result.get("position")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or not isinstance(prefix_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", prefix_sha256) is None
            or (count == 0 and position is not None)
            or (count > 0 and not isinstance(position, dict))
            or result.get("prefix_replay_policy") != "normalize-deduplicate-and-digest-v1"
        ):
            raise CatalogueError("historical landing cursor is invalid")
        replayed = 0
        previous_position = None
        while replayed < count:
            check_deadline()
            try:
                values, current_position = next(iterator)
            except StopIteration as exc:
                raise CatalogueError("historical landing cursor prefix is missing") from exc
            _landing_prefix_update(checksum, values, current_position)
            previous_position = current_position
            replayed += 1
        if checksum.hexdigest() != prefix_sha256 or previous_position != position:
            raise CatalogueError("historical landing cursor prefix differs from its evidence")
        return iterator, checksum, count, position, replayed
    except BudgetExceeded:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
        raise
    except (CatalogueError, TypeError, ValueError):
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
        return expected_rows(), hashlib.sha256(), 0, None, 0


def compare_landing_rows_resumable(
    connection: Any,
    expected_rows: Iterator[tuple[Sequence[str | None], dict[str, Any]]],
    columns: Sequence[str],
    *,
    nullable: set[str],
    check_deadline: Any,
    binding: dict[str, Any],
    save_cursor: Any,
    start_position: int = 0,
    prefix_checksum: Any | None = None,
    last_position: dict[str, Any] | None = None,
    replayed_prefix_rows: int = 0,
    row_limit: int = 4096,
    byte_limit: int = 8 * 1024 * 1024,
) -> tuple[int, dict[str, Any]]:
    """Compare an ordered landing suffix, durably sealing each bounded candidate window."""
    _logical_schema(connection, columns, nullable)
    count, minimum, maximum = connection.execute(
        "select count(*), min(rowid), max(rowid) from landing.observations"
    ).fetchone()
    if count and (minimum != 0 or maximum != count - 1):
        raise CatalogueError("resumed historical landing row positions are not dense")
    if not count and (minimum is not None or maximum is not None):
        raise CatalogueError("resumed historical landing row count is invalid")
    if start_position > count:
        raise CatalogueError("historical landing cursor exceeds the candidate row count")

    checksum = prefix_checksum or hashlib.sha256()
    projection = ", ".join(columns)
    checked = start_position
    start = start_position
    cursor_result = {
        "observation_position": checked,
        "prefix_sha256": checksum.hexdigest(),
        "position": last_position,
        "prefix_replay_rows": replayed_prefix_rows,
        "prefix_replay_policy": "normalize-deduplicate-and-digest-v1",
    }
    while start < count:
        check_deadline()
        end = _next_window_end(connection, columns, start, row_limit, byte_limit)
        if end < start or end >= count:
            raise CatalogueError("resumed historical landing row window is invalid")
        rows = connection.execute(
            f"select rowid, {projection} from landing.observations "
            "where rowid between ? and ? order by rowid",
            [start, end],
        ).fetchall()
        if len(rows) != end - start + 1:
            raise CatalogueError("resumed historical landing row window has a gap")
        for rowid, *actual in rows:
            if rowid != checked:
                raise CatalogueError("resumed historical landing row positions are not dense")
            try:
                expected, position = next(expected_rows)
            except StopIteration as exc:
                raise CatalogueError("resumed historical landing has extra observations") from exc
            if tuple(actual) != tuple(expected):
                raise CatalogueError(
                    "resumed historical landing differs from receipt-normalized rows"
                )
            _landing_prefix_update(checksum, expected, position)
            checked += 1
            last_position = position
        cursor_result = {
            "observation_position": checked,
            "prefix_sha256": checksum.hexdigest(),
            "position": last_position,
            "prefix_replay_rows": replayed_prefix_rows,
            "prefix_replay_policy": "normalize-deduplicate-and-digest-v1",
        }
        save_cursor(proof_record("historical-landing-cursor", binding, cursor_result))
        start = end + 1

    try:
        next(expected_rows)
    except StopIteration:
        save_cursor(proof_record("historical-landing-cursor", binding, cursor_result))
        return checked, cursor_result
    raise CatalogueError("resumed historical landing is missing observations")


def digest_relation(
    connection: Any,
    relation: str,
    columns: Sequence[tuple[str, str]],
    *,
    check_deadline: Any,
    row_limit: int = 4096,
    byte_limit: int = 8 * 1024 * 1024,
) -> tuple[int, str]:
    """Digest a private materialized result in dense rowid and byte-bounded windows."""
    description = connection.execute(f"describe {relation}").fetchall()
    if [(row[0], row[1].upper()) for row in description] != [
        (name, kind.upper()) for name, kind in columns
    ]:
        raise CatalogueError("historical phase output schema differs from its proof")
    count, minimum, maximum = connection.execute(
        f"select count(*), min(rowid), max(rowid) from {relation}"
    ).fetchone()
    if count and (minimum != 0 or maximum != count - 1):
        raise CatalogueError("historical phase output row positions are not dense")
    names = [name for name, _kind in columns]
    projection = ", ".join(names)
    checksum = hashlib.sha256()
    start = 0
    checked = 0
    while start < count:
        check_deadline()
        end = _next_relation_window_end(connection, relation, names, start, row_limit, byte_limit)
        rows = connection.execute(
            f"select rowid, {projection} from {relation} "
            "where rowid between ? and ? order by rowid",
            [start, end],
        ).fetchall()
        if len(rows) != end - start + 1:
            raise CatalogueError("historical phase output row window has a gap")
        for _rowid, *values in rows:
            checksum.update(json_bytes(values))
            checked += 1
        start = end + 1
    return checked, checksum.hexdigest()


def compare_rows(
    connection: Any,
    query: str,
    expected_rows: Iterable[Sequence[Any]],
    expected_columns: Sequence[tuple[str, str]],
    *,
    check_deadline: Any,
    batch_rows: int = 4096,
) -> int:
    """Compare a bounded ordered query result to its independently rebuilt stream."""
    cursor = connection.execute(query)
    columns = cursor.description or ()
    if [(column[0], str(column[1]).upper()) for column in columns] != [
        (name, kind.upper()) for name, kind in expected_columns
    ]:
        raise CatalogueError("resumed historical projection schema differs from its receipt")
    expected = iter(expected_rows)
    count = 0
    while rows := cursor.fetchmany(batch_rows):
        check_deadline()
        for row in rows:
            try:
                wanted = next(expected)
            except StopIteration as exc:
                raise CatalogueError("resumed historical projection has extra rows") from exc
            if tuple(row) != tuple(wanted):
                raise CatalogueError("resumed historical projection differs from receipt evidence")
            count += 1
    try:
        next(expected)
    except StopIteration:
        return count
    raise CatalogueError("resumed historical projection is missing rows")


def compare_query_to_relation(
    connection: Any,
    query: str,
    relation: str,
    *,
    check_deadline: Any,
    digest_source: str | None = None,
) -> tuple[int, str] | None:
    """Check exact multiset parity in DuckDB, then digest bounded source windows."""
    expected_columns = connection.execute(f"describe select * from ({query}) expected").fetchall()
    actual_columns = connection.execute(f"describe select * from {relation}").fetchall()
    expected_schema = [(row[0], row[1].upper()) for row in expected_columns]
    actual_schema = [(row[0], row[1].upper()) for row in actual_columns]
    if not expected_schema or expected_schema != actual_schema:
        return None

    # EXCEPT ALL preserves duplicate multiplicity while keeping all comparison
    # work inside DuckDB's configured memory and spill limits.
    check_deadline()
    query = query.rstrip().removesuffix(";")
    mismatch = connection.execute(
        f"""select
            exists(
                select 1 from (({query}) except all (select * from {relation})) difference
                limit 1
            ) or exists(
                select 1 from ((select * from {relation}) except all ({query})) difference
                limit 1
            )"""
    ).fetchone()[0]
    check_deadline()
    if mismatch:
        return None
    return digest_relation(
        connection,
        digest_source or relation,
        expected_schema,
        check_deadline=check_deadline,
    )
