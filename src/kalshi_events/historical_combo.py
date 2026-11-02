"""Bounded historical combo-leg projection for compact catalogue builds."""

from __future__ import annotations

import stat
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_events import metrics
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    fsync_directory,
    fsync_file,
    interrupt_at_deadline,
    safe_path,
)

_BATCH_ROWS = 4096
_BATCH_BYTES = 8 * 1024 * 1024
_SPOOL_ROW_GROUP_BYTES = _BATCH_BYTES // 2
_CHECKPOINT_BYTES = 64 * 1024 * 1024
_CHECKPOINT_SECONDS = 30.0
_COPY_POLL_SECONDS = 0.05
_SOURCE_QUERY = """select market_ticker,
                          json_extract(payload_json, '$.mve_selected_legs') as selected_legs
                   from main.current_markets
                   where json_extract(payload_json, '$.mve_selected_legs') is not null"""
_INPUT_SCHEMA = pa.schema(
    [
        pa.field("combo_market_ticker", pa.string(), nullable=False),
        pa.field("selected_legs", pa.string(), nullable=False),
    ]
)
_RESOLUTION_UPDATE_SQL = """
update landing.combo_legs_canonical as combo
set resolved = resolution.resolved
from (
    select canonical.rowid as row_id,
           (target.market_ticker is not null
             and target.event_ticker = canonical.event_ticker
             and native_event.event_ticker is not null) as resolved
    from landing.combo_legs_canonical as canonical
    left join (
        select market_ticker, event_ticker
        from main._historical_current_markets
    ) as target on canonical.market_ticker = target.market_ticker
    left join (
        select event_ticker
        from main._historical_current_events
    ) as native_event on canonical.event_ticker = native_event.event_ticker
) as resolution
where combo.rowid = resolution.row_id
"""


def _arrow_batches(rows: Iterable[tuple[Any, Any]], store: Store) -> Iterator[pa.Table]:
    batch: list[tuple[str, str]] = []
    batch_bytes = 0

    for market_ticker, selected_legs in rows:
        store.check_deadline()
        if not isinstance(market_ticker, str) or not isinstance(selected_legs, str):
            raise CatalogueError("historical combo source has invalid identity or JSON")
        row_bytes = len(market_ticker.encode("utf-8")) + len(selected_legs.encode("utf-8"))
        if batch and (len(batch) >= _BATCH_ROWS or batch_bytes + row_bytes > _BATCH_BYTES):
            yield _arrow_table(batch)
            batch = []
            batch_bytes = 0
        batch.append((market_ticker, selected_legs))
        batch_bytes += row_bytes
        if len(batch) >= _BATCH_ROWS or batch_bytes >= _BATCH_BYTES:
            yield _arrow_table(batch)
            batch = []
            batch_bytes = 0

    if batch:
        yield _arrow_table(batch)


def _arrow_table(rows: list[tuple[str, str]]) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array((row[0] for row in rows), type=pa.string()),
            pa.array((row[1] for row in rows), type=pa.string()),
        ],
        schema=_INPUT_SCHEMA,
    )


def _validate_target_identities(connection: duckdb.DuckDBPyConnection) -> None:
    market_columns = connection.execute(
        "describe select market_ticker, event_ticker from main._historical_current_markets limit 0"
    ).fetchall()
    event_columns = connection.execute(
        "describe select event_ticker from main._historical_current_events limit 0"
    ).fetchall()
    if [row[1].upper() for row in market_columns] != ["VARCHAR", "VARCHAR"] or [
        row[1].upper() for row in event_columns
    ] != ["VARCHAR"]:
        raise CatalogueError("historical combo resolution identities must be VARCHAR")

    invalid_identity = connection.execute(
        """select 1 from main._historical_current_markets
           where market_ticker is null
           union all
           select 1 from main._historical_current_events where event_ticker is null
           limit 1"""
    ).fetchone()
    if invalid_identity is not None:
        raise CatalogueError("historical combo resolution has a null target identity")

    duplicate_market = connection.execute(
        """select 1 from main._historical_current_markets
           group by market_ticker having count(*) > 1 limit 1"""
    ).fetchone()
    duplicate_event = connection.execute(
        """select 1 from main._historical_current_events
           group by event_ticker having count(*) > 1 limit 1"""
    ).fetchone()
    if duplicate_market is not None or duplicate_event is not None:
        raise CatalogueError("historical combo resolution target identities are not unique")


def _database_location(store: Store, database: str | Path) -> tuple[Path, str]:
    path = Path(database)
    if not path.is_absolute():
        path = store.path(path.as_posix())
    path = safe_path(path)
    if not path.is_file():
        raise CatalogueError("historical combo warehouse is missing or not a file")
    try:
        relative = path.relative_to(store.root).as_posix()
    except ValueError as exc:
        raise CatalogueError("historical combo warehouse is outside the operator root") from exc
    staging_relative = Path(relative).parent.as_posix()
    if staging_relative == ".":
        raise CatalogueError("historical combo warehouse must be inside a staged directory")
    return path, staging_relative


def _spool_market_rows(spool_path: Path, store: Store) -> Iterator[tuple[Any, Any]]:
    store.check_deadline()
    source = pq.ParquetFile(spool_path)
    if source.schema_arrow.names != ["market_ticker", "selected_legs"]:
        raise CatalogueError("historical combo spool has an unexpected schema")
    for index in range(source.metadata.num_row_groups):
        store.check_deadline()
        group = source.metadata.row_group(index)
        batch_size = _BATCH_ROWS if group.total_byte_size <= _BATCH_BYTES else 1
        for batch in source.iter_batches(
            batch_size=batch_size,
            row_groups=[index],
            columns=["market_ticker", "selected_legs"],
            use_threads=False,
        ):
            if batch.num_rows > _BATCH_ROWS or (
                batch.nbytes > _BATCH_BYTES and batch.num_rows != 1
            ):
                raise CatalogueError("historical combo spool batch exceeds its logical bound")
            for row in range(batch.num_rows):
                yield batch.column(0)[row].as_py(), batch.column(1)[row].as_py()


def _remove_spool(store: Store, spool_path: Path, staging_relative: str) -> None:
    try:
        info = spool_path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CatalogueError("historical combo spool is not a private regular file")
    spool_path.unlink()
    store.sync(staging_relative)


def _copy_source_to_spool(
    store: Store,
    database_path: Path,
    spool_path: Path,
    staging_relative: str,
    config: dict[str, Any],
) -> None:
    finished = threading.Event()
    state: dict[str, duckdb.DuckDBPyConnection] = {}
    failures: list[BaseException] = []

    def copy() -> None:
        connection = None
        try:
            connection = duckdb.connect(str(database_path), config=config)
            state["connection"] = connection
            query = (
                # Python result readers retain wide query results; COPY bounds this to narrow disk row groups.
                "copy ("
                + _SOURCE_QUERY
                + ") to ? (format parquet, compression zstd, row_group_size_bytes "
                + str(_SPOOL_ROW_GROUP_BYTES)
                + ")"
            )
            with interrupt_at_deadline(store, connection.interrupt):
                connection.execute(query, [str(spool_path)])
        except BaseException as exc:
            failures.append(exc)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except BaseException as exc:
                    failures.append(exc)
            finished.set()

    worker = threading.Thread(target=copy, name="historical-combo-copy", daemon=True)
    worker.start()
    monitor_error: BaseException | None = None
    while not finished.wait(_COPY_POLL_SECONDS):
        try:
            store.check_deadline()
            try:
                info = spool_path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise CatalogueError("historical combo spool is not a private regular file")
            store.check_storage(additional=info.st_size)
        except BaseException as exc:
            monitor_error = exc
            connection = state.get("connection")
            if connection is not None:
                with suppress(duckdb.Error):
                    connection.interrupt()
            break
    worker.join()
    if monitor_error is not None:
        raise monitor_error
    if failures:
        error = failures[0]
        if isinstance(error, duckdb.InterruptException):
            try:
                store.check_deadline()
            except BudgetExceeded as budget:
                raise budget from error
        raise error
    store.check_deadline()
    info = spool_path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CatalogueError("historical combo spool is not a private regular file")
    fsync_file(spool_path)
    fsync_directory(spool_path.parent)
    store.sync(staging_relative)
    store.check_storage()


def _checkpoint(
    connection: duckdb.DuckDBPyConnection,
    store: Store,
    staging_relative: str,
) -> None:
    store.check_deadline()
    connection.execute("checkpoint")
    store.sync(staging_relative)
    store.check_storage()
    store.check_deadline()


def _insert_batch(
    connection: duckdb.DuckDBPyConnection,
    table: pa.Table,
) -> None:
    registered = False
    connection.execute("begin transaction")
    try:
        connection.register("historical_combo_batch", table)
        registered = True
        connection.execute(
            """insert into landing.combo_legs_canonical (
                   combo_market_ticker, leg_index, event_ticker, market_ticker,
                   side, yes_settlement_value_dollars, resolved, payload_json
               )
               select market.combo_market_ticker,
                      cast(leg.key as integer),
                      json_extract_string(leg.value, '$.event_ticker'),
                      json_extract_string(leg.value, '$.market_ticker'),
                      json_extract_string(leg.value, '$.side'),
                      cast(json_extract_string(leg.value, '$.yes_settlement_value_dollars')
                           as decimal(38,18)),
                      cast(null as boolean),
                      leg.value
               from historical_combo_batch as market,
                    lateral json_each(market.selected_legs) as leg"""
        )
        connection.unregister("historical_combo_batch")
        registered = False
        connection.execute("commit")
    except BaseException:
        if registered:
            with suppress(duckdb.Error):
                connection.unregister("historical_combo_batch")
        with suppress(duckdb.Error):
            connection.execute("rollback")
        raise


def build_combo_legs(
    store: Store,
    database: str | Path,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Build the private canonical table; callers record reuse proof only on return."""
    store.check_deadline()
    database_path, staging_relative = _database_location(store, database)
    spool_path = store.path(f"{staging_relative}/combo-legs-input.parquet")
    _remove_spool(store, spool_path, staging_relative)
    connection_config = dict(config or {})
    writer: duckdb.DuckDBPyConnection | None = None
    markets = 0
    batches = 0
    try:
        writer = duckdb.connect(str(database_path), config=connection_config)
        with interrupt_at_deadline(store, writer.interrupt):
            _validate_target_identities(writer)
            writer.execute("begin transaction")
            writer.execute("create schema if not exists landing")
            writer.execute("drop table if exists landing.combo_legs_canonical")
            writer.execute(
                """create table landing.combo_legs_canonical (
                       combo_market_ticker varchar,
                       leg_index integer,
                       event_ticker varchar,
                       market_ticker varchar,
                       side varchar,
                       yes_settlement_value_dollars decimal(38,18),
                       resolved boolean,
                       payload_json json
                   )"""
            )
            writer.execute("commit")
            _checkpoint(writer, store, staging_relative)
        writer.close()
        writer = None

        _copy_source_to_spool(store, database_path, spool_path, staging_relative, connection_config)
        writer = duckdb.connect(str(database_path), config=connection_config)
        checkpoint_bytes = 0
        checkpoint_at = time.monotonic() + _CHECKPOINT_SECONDS
        with interrupt_at_deadline(store, writer.interrupt):
            for table in _arrow_batches(_spool_market_rows(spool_path, store), store):
                store.check_deadline()
                store.check_storage()
                _insert_batch(writer, table)
                markets += table.num_rows
                batches += 1
                checkpoint_bytes += table.nbytes
                try:
                    metrics.progress(batches_yielded=batches)
                except Exception:
                    metrics.add("progress_write_failures")
                store.sync(staging_relative)
                store.check_storage()
                if checkpoint_bytes >= _CHECKPOINT_BYTES or time.monotonic() >= checkpoint_at:
                    # shortcut: one slow or oversized batch can exceed the checkpoint interval; split it if WAL headroom requires it.
                    _checkpoint(writer, store, staging_relative)
                    checkpoint_bytes = 0
                    checkpoint_at = time.monotonic() + _CHECKPOINT_SECONDS

            writer.execute("begin transaction")
            try:
                # shortcut: this required single update is one transaction; split only if measured WAL exceeds the stage budget.
                writer.execute(_RESOLUTION_UPDATE_SQL)
                writer.execute("commit")
            except BaseException:
                with suppress(duckdb.Error):
                    writer.execute("rollback")
                raise
            store.sync(staging_relative)
            store.check_storage()
            legs = writer.execute("select count(*) from landing.combo_legs_canonical").fetchone()[0]
            _checkpoint(writer, store, staging_relative)

    except duckdb.InterruptException as exc:
        try:
            store.check_deadline()
        except BudgetExceeded as budget:
            raise budget from exc
        raise
    finally:
        try:
            if writer is not None:
                writer.close()
        finally:
            try:
                _remove_spool(store, spool_path, staging_relative)
            finally:
                store.sync(staging_relative)

    fsync_file(database_path)
    fsync_directory(database_path.parent)
    store.check_deadline()
    store.sync(staging_relative)
    store.check_storage()
    return {"markets": markets, "legs": int(legs), "batches": batches}
