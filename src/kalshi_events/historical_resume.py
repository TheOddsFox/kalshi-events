"""Private helpers for validating resumable historical build stages."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from kalshi_events.common import CatalogueError, digest, json_bytes

_STAGE_NAME = re.compile(r"build-historical-[0-9a-f]{32}")


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
