"""Verified, external production.duckdb hard-link activation."""

from __future__ import annotations

import contextlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

_ACTIVATION_TEMP_NAME = re.compile(r"\.production\.duckdb\.[0-9a-f]{32}\.tmp\Z")
_MAX_PRODUCTION_DIRECTORY_ENTRIES = 50_000


def _activation_temporary_names(directory: Path) -> list[Path]:
    names = []
    with os.scandir(directory) as entries:
        for index, entry in enumerate(entries, start=1):
            if index > _MAX_PRODUCTION_DIRECTORY_ENTRIES:
                raise RuntimeError("production directory exceeded its activation recovery bound")
            if _ACTIVATION_TEMP_NAME.fullmatch(entry.name):
                names.append(Path(entry.path))
    return names


def _remove_stale_activation_temporaries(
    store: Any,
    production_directory: Path,
    current: dict[str, Any],
    source: Path,
    checksum: str,
    *,
    catalogue_version: int,
    max_database_bytes: int,
) -> int:
    """Remove only orphan links to the still-current certified source warehouse."""
    from kalshi_events import publish
    from kalshi_events.common import file_digest, fsync_directory

    if not store.writer_locked_by_current_thread:
        raise RuntimeError("activation temporary recovery requires the canonical writer lock")
    names = _activation_temporary_names(production_directory)
    if not names:
        return 0

    source_info = source.lstat()
    if (
        not stat.S_ISREG(source_info.st_mode)
        or source.is_symlink()
        or source_info.st_size > max_database_bytes
        or file_digest(source, max_database_bytes)[0] != checksum
    ):
        raise RuntimeError("certified warehouse changed during activation temporary recovery")
    if publish.current(store, catalogue_version=catalogue_version) != current:
        raise RuntimeError("current publication changed during activation temporary recovery")

    temporaries: list[tuple[Path, tuple[int, int]]] = []
    for temporary in names:
        info = temporary.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink < 2
            or (info.st_dev, info.st_ino) != (source_info.st_dev, source_info.st_ino)
        ):
            raise RuntimeError(
                "stale production activation temporary is not a certified source link"
            )
        if temporary.with_name(temporary.name + ".wal").exists():
            raise RuntimeError("stale production activation temporary has an uncheckpointed WAL")
        temporaries.append((temporary, (info.st_dev, info.st_ino)))

    for temporary, identity in temporaries:
        info = temporary.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink < 2
            or (info.st_dev, info.st_ino) != identity
            or (info.st_dev, info.st_ino) != (source_info.st_dev, source_info.st_ino)
        ):
            raise RuntimeError("stale production activation temporary changed during recovery")
        temporary.unlink()
    if temporaries:
        fsync_directory(production_directory)
    return len(temporaries)


def recover_activation_temporaries(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str,
    catalogue_version: int = 2,
    expected_capture_id: str | None = None,
    max_database_bytes: int = 64 * 1024**3,
    max_temp_bytes: int = 64 * 1024**3,
    max_duration_seconds: float | None = None,
) -> int:
    """Recover only validated activation hard links for the current release."""
    from kalshi_events import publish, warehouse
    from kalshi_events.common import (
        CatalogueError,
        Limits,
        Store,
    )
    from kalshi_events.paths import PathPolicyError, persistent_path, project_root

    if max_duration_seconds is not None and (
        isinstance(max_duration_seconds, bool)
        or not isinstance(max_duration_seconds, (int, float))
        or not math.isfinite(max_duration_seconds)
        or max_duration_seconds <= 0
    ):
        raise RuntimeError("activation duration limit is invalid")
    try:
        checkout = project_root(code_root=Path(__file__).resolve().parents[1])
        directory = persistent_path(
            production_directory, checkout=checkout, label="production directory"
        )
        root = persistent_path(operator_root, checkout=checkout, label="production operator root")
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc
    if not directory.is_dir() or root != directory / "operator":
        raise RuntimeError("activation paths do not match the production layout")
    if isinstance(catalogue_version, bool) or catalogue_version not in {2, 3}:
        raise RuntimeError("production activation supports catalogue versions 2 and 3")
    if not isinstance(expected_release_id, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_release_id
    ):
        raise RuntimeError("expected activation release identity is invalid")
    if expected_capture_id is not None and (
        catalogue_version != 3
        or not isinstance(expected_capture_id, str)
        or not re.fullmatch(r"[0-9a-f]{32}", expected_capture_id)
    ):
        raise RuntimeError("expected historical capture identity is invalid")
    alias = directory / "production.duckdb"
    if alias.is_symlink():
        raise RuntimeError("production database path cannot be a symlink")
    if alias.exists() and not stat.S_ISREG(alias.lstat().st_mode):
        raise RuntimeError("existing production database is not a regular file")
    if not _activation_temporary_names(directory):
        return 0

    duration = 48 * 3600 if max_duration_seconds is None else max_duration_seconds
    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * 1024**3,
        max_response_bytes=384 * 1024**2,
        max_decoded_response_bytes=768 * 1024**2,
        max_duration_seconds=duration,
        max_root_bytes=350 * 1024**3,
        max_temp_bytes=max_temp_bytes,
        max_database_bytes=max_database_bytes,
    )
    store = Store(root, limits)
    if store.scope is not None:
        raise RuntimeError("scoped series roots cannot be production catalogue roots")
    with store.lock(), warehouse.operation_context(store) as verification:
        current = publish.current(store, catalogue_version=catalogue_version)
        if current.get("source_release_id") != expected_release_id:
            raise RuntimeError("current publication changed before activation temporary recovery")
        capture_id = current.get("capture_id")
        if expected_capture_id is not None and capture_id != expected_capture_id:
            raise RuntimeError("current publication belongs to a different capture")
        standalone = publish.verify_release(Path(current["output"]), limits.max_root_bytes)
        published_manifest = {key: value for key, value in current.items() if key != "output"}
        if standalone != published_manifest:
            raise RuntimeError("standalone verification does not match the current publication")
        receipt = warehouse.certified(
            store, capture_id, catalogue_version=catalogue_version, _context=verification
        )
        source = store.path(
            f"{warehouse.build_directory(capture_id, catalogue_version)}/warehouse.duckdb"
        )
        checksum = receipt.get("warehouse_sha256")
        if not isinstance(checksum, str):
            raise RuntimeError("certified warehouse has no valid activation checksum")
        if source.with_name(source.name + ".wal").exists():
            raise RuntimeError("certified warehouse has an uncheckpointed WAL")
        return _remove_stale_activation_temporaries(
            store,
            directory,
            current,
            source,
            checksum,
            catalogue_version=catalogue_version,
            max_database_bytes=limits.max_database_bytes,
        )


def activate_current(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str | None = None,
    expected_capture_id: str | None = None,
    standalone_verified: dict[str, Any] | None = None,
    catalogue_version: int = 2,
    max_database_bytes: int = 64 * 1024**3,
    max_temp_bytes: int = 64 * 1024**3,
    max_duration_seconds: float | None = None,
    expected_observations: int | None = None,
    link_fn: Any = None,
    replace_fn: Any = None,
    sync_fn: Any = None,
) -> dict[str, Any]:
    """Certify the selected warehouse, then atomically expose its hard link."""
    import pyarrow.parquet as pq

    from kalshi_events import capture, incremental, publish, warehouse
    from kalshi_events.common import (
        CatalogueError,
        Limits,
        Store,
        digest,
        file_digest,
        fsync_directory,
        regular_bytes,
    )
    from kalshi_events.paths import PathPolicyError, persistent_path, project_root

    link_fn = os.link if link_fn is None else link_fn
    replace_fn = os.replace if replace_fn is None else replace_fn
    sync_fn = fsync_directory if sync_fn is None else sync_fn
    try:
        checkout = project_root(code_root=Path(__file__).resolve().parents[1])
        directory = persistent_path(
            production_directory, checkout=checkout, label="production directory"
        )
        root = persistent_path(operator_root, checkout=checkout, label="production operator root")
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc
    if not directory.is_dir() or root != directory / "operator":
        raise RuntimeError("activation paths do not match the production layout")
    if isinstance(catalogue_version, bool) or catalogue_version not in {2, 3}:
        raise RuntimeError("production activation supports catalogue versions 2 and 3")
    if max_duration_seconds is not None and (
        isinstance(max_duration_seconds, bool)
        or not isinstance(max_duration_seconds, (int, float))
        or not math.isfinite(max_duration_seconds)
        or max_duration_seconds <= 0
    ):
        raise RuntimeError("activation duration limit is invalid")
    if expected_observations is not None and (
        isinstance(expected_observations, bool)
        or not isinstance(expected_observations, int)
        or expected_observations < 0
    ):
        raise RuntimeError("expected historical observation count is invalid")
    alias = directory / "production.duckdb"
    if alias.is_symlink():
        raise RuntimeError("production database path cannot be a symlink")
    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * 1024**3,
        max_response_bytes=384 * 1024**2,
        max_decoded_response_bytes=768 * 1024**2,
        max_duration_seconds=48 * 3600 if max_duration_seconds is None else max_duration_seconds,
        max_root_bytes=350 * 1024**3,
        max_temp_bytes=max_temp_bytes,
        max_database_bytes=max_database_bytes,
    )
    store = Store(root, limits)
    if store.scope is not None:
        raise RuntimeError("scoped series roots cannot be production catalogue roots")
    with store.lock():
        incremental.finish_pending_retirement(store)
        with warehouse.operation_context(store) as verification:
            current = publish.current(store, catalogue_version=catalogue_version)
            release_id = current.get("source_release_id")
            capture_id = current.get("capture_id")
            if expected_release_id is not None and release_id != expected_release_id:
                raise RuntimeError("current publication changed after standalone verification")
            if expected_capture_id is not None:
                if (
                    catalogue_version != 3
                    or not isinstance(expected_capture_id, str)
                    or len(expected_capture_id) != 32
                    or any(character not in "0123456789abcdef" for character in expected_capture_id)
                ):
                    raise RuntimeError("expected historical capture identity is invalid")
                if capture_id != expected_capture_id:
                    raise RuntimeError("current publication belongs to a different capture")
            manifest_body = regular_bytes(Path(current["output"]) / "manifest.json", 1024**2)
            manifest_sha256 = digest(manifest_body)
            if standalone_verified is None:
                standalone_verified = publish.verify_release(
                    Path(current["output"]), limits.max_root_bytes
                )
            published_manifest = {key: value for key, value in current.items() if key != "output"}
            if standalone_verified != published_manifest:
                raise RuntimeError("standalone verification does not match the current publication")
            receipt = warehouse.certified(
                store, capture_id, catalogue_version=catalogue_version, _context=verification
            )
            checksum = receipt.get("warehouse_sha256")
            manifest_checksum = current.get("warehouse_sha256")
            if manifest_checksum == checksum:
                verification_mode = "warehouse_hash"
            else:
                pin = {
                    "catalogue_version": catalogue_version,
                    "source_release_id": release_id,
                    "manifest_sha256": manifest_sha256,
                    "capture_id": capture_id,
                    "root_id": current.get("root_id"),
                    "delta_depth": current.get("delta_depth"),
                    "series_scope": current.get("series_scope"),
                }
                verification_mode = "semantic"
                with incremental._baseline_data(
                    store, capture_id, catalogue_version, pin, _context=verification
                ):
                    pass
            state = capture.read_capture(store, capture_id)
            source = store.path(
                f"{warehouse.build_directory(capture_id, catalogue_version)}/warehouse.duckdb"
            )
            source_info = source.lstat()
            if not stat.S_ISREG(source_info.st_mode) or source.is_symlink():
                raise RuntimeError("certified warehouse is not a regular file")
            if source_info.st_size > limits.max_database_bytes:
                raise RuntimeError("certified warehouse exceeds the selected database limit")
            source_checksum, _ = file_digest(source, limits.max_database_bytes)
            if source_checksum != checksum:
                raise RuntimeError("certified warehouse changed before production linking")
            recovered_temporaries = _remove_stale_activation_temporaries(
                store,
                directory,
                current,
                source,
                checksum,
                catalogue_version=catalogue_version,
                max_database_bytes=limits.max_database_bytes,
            )
            if alias.is_symlink():
                raise RuntimeError("production database path cannot be a symlink")
            if alias.exists() and not stat.S_ISREG(alias.lstat().st_mode):
                raise RuntimeError("existing production database is not a regular file")
            if (
                source.with_name(source.name + ".wal").exists()
                or alias.with_name(alias.name + ".wal").exists()
            ):
                raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
            counts = {
                name: pq.ParquetFile(Path(current["output"]) / f"{name}.parquet").metadata.num_rows
                for name in warehouse.RELATIONS
            }
            if (
                expected_observations is not None
                and counts["observation_history"] != expected_observations
            ):
                raise RuntimeError(
                    "verified historical Parquet count differs from the expected observation census"
                )
            if source_info.st_dev != directory.stat().st_dev:
                raise RuntimeError(
                    "production database and warehouse must be on the same filesystem"
                )
            temporary = directory / f".production.duckdb.{uuid.uuid4().hex}.tmp"
            spill_relative = f"tmp/activation-readback-{uuid.uuid4().hex}"
            spill_directory = store.path(spill_relative)
            usage = store.usage()
            spill_budget = min(
                limits.max_temp_bytes - usage["temporary_bytes"],
                limits.max_root_bytes - usage["retained_bytes"],
            )
            if spill_budget < 1:
                raise RuntimeError(
                    "no bounded temporary storage is available for activation readback"
                )
            store.reserve_storage(spill_budget)
            spill_reserved = True
            try:
                spill_directory.mkdir(parents=True)
                store.sync(spill_relative)
                link_fn(source, temporary, follow_symlinks=False)
                linked_info = temporary.lstat()
                if (
                    not stat.S_ISREG(linked_info.st_mode)
                    or (linked_info.st_dev, linked_info.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError(
                        "temporary production link differs from the certified warehouse"
                    )
                relation_map = dict(warehouse.RELATIONS)
                query = """
import duckdb, json, sys
database, spill, spill_bytes, version, relation_map_json = sys.argv[1:]
relation_map = json.loads(relation_map_json)
config = {
    "memory_limit": "4GB",
    "threads": "4",
    "temp_directory": spill,
    "max_temp_directory_size": spill_bytes + "B",
}
connection = duckdb.connect(database, read_only=True, config=config)
try:
    result = {"relation_rows": {}, "samples": {}}
    for name, table in relation_map.items():
        result["relation_rows"][name] = connection.execute(
            f"select count(*) from main.{table}"
        ).fetchone()[0]
    samples = {
        "series": "select series_ticker, observation_id, capture_id, received_at, source_endpoint, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_series limit 1",
        "events": "select event_ticker, series_ticker, observation_id, capture_id, received_at, source_endpoint, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_events limit 1",
        "markets": "select market_ticker, event_ticker, series_ticker, usable, observation_id, capture_id, received_at, source_endpoint, cast(last_price_dollars as varchar), cast(volume_fp as varchar), cast(open_interest_fp as varchar), sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_markets limit 1",
        "observation_history": "select observation_id, capture_id, entity_kind, entity_ticker, received_at, source_endpoint, source_kind, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.observation_history limit 1",
        "combo_legs": "select combo_market_ticker, leg_index, event_ticker, market_ticker, side, cast(yes_settlement_value_dollars as varchar), sha256(cast(payload_json as varchar)) from main.combo_legs limit 1",
        "relationships": "select parent_kind, parent_ticker, child_kind, child_ticker, resolved from main.relationships limit 1",
    }
    for name, sql in samples.items():
        result["samples"][name] = connection.execute(sql).fetchone()
    if version == "3":
        row = connection.execute(
            "select historical_contract, scope, omitted_scopes, source_capture_id, source_capture_complete from main.catalogue_coverage"
        ).fetchone()
        result["catalogue_coverage"] = dict(zip(
            ["historical_contract", "scope", "omitted_scopes", "source_capture_id", "source_capture_complete"],
            row,
        ))
    print(json.dumps(result, default=str))
finally:
    connection.close()
"""
                readback = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        query,
                        str(temporary),
                        str(spill_directory),
                        str(spill_budget),
                        str(catalogue_version),
                        json.dumps(relation_map),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=min(600.0, store.remaining),
                )
                readback_data = json.loads(readback.stdout)
                readback_rows = readback_data.get("relation_rows")
                if (
                    not isinstance(readback_rows, dict)
                    or set(readback_rows) != set(counts)
                    or any(
                        isinstance(readback_rows.get(name), bool)
                        or not isinstance(readback_rows.get(name), int)
                        or readback_rows[name] != expected_rows
                        for name, expected_rows in counts.items()
                    )
                ):
                    raise RuntimeError(
                        "production DuckDB row counts differ from verified Parquet footers"
                    )
                if (
                    expected_observations is not None
                    and readback_rows.get("observation_history") != expected_observations
                ):
                    raise RuntimeError(
                        "production history count differs from the expected observation census"
                    )
                samples = readback_data.get("samples")
                if not isinstance(samples, dict) or any(
                    expected_rows > 0 and samples.get(name) is None
                    for name, table in relation_map.items()
                    if (expected_rows := counts[name]) > 0
                ):
                    raise RuntimeError(
                        "production DuckDB public readback missed a representative relation row"
                    )
                readback_coverage = readback_data.get("catalogue_coverage")
                if catalogue_version == 3:
                    expected_omitted = json.loads(current["omitted_scopes_json"])
                    if readback_coverage != {
                        "historical_contract": current.get("historical_contract"),
                        "scope": "historical",
                        "omitted_scopes": expected_omitted,
                        "source_capture_id": capture_id,
                        "source_capture_complete": current.get("source_capture_complete"),
                    }:
                        raise RuntimeError(
                            "production catalogue coverage readback differs from release"
                        )

                rechecked = publish.current(store, catalogue_version=catalogue_version)
                rechecked_manifest_sha256 = digest(
                    regular_bytes(Path(rechecked["output"]) / "manifest.json", 1024**2)
                )
                if rechecked != current or rechecked_manifest_sha256 != manifest_sha256:
                    raise RuntimeError("current publication changed during production readback")
                source_info_after = source.lstat()
                if (
                    not stat.S_ISREG(source_info_after.st_mode)
                    or (source_info_after.st_dev, source_info_after.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or source_info_after.st_size != source_info.st_size
                    or file_digest(source, limits.max_database_bytes)[0] != checksum
                ):
                    raise RuntimeError("certified warehouse changed during production readback")
                if (
                    source.with_name(source.name + ".wal").exists()
                    or alias.with_name(alias.name + ".wal").exists()
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
                linked_after = temporary.lstat()
                if not stat.S_ISREG(linked_after.st_mode) or (
                    linked_after.st_dev,
                    linked_after.st_ino,
                ) != (source_info_after.st_dev, source_info_after.st_ino):
                    raise RuntimeError("temporary production link changed during readback")
                if alias.is_symlink() or (
                    alias.exists() and not stat.S_ISREG(alias.lstat().st_mode)
                ):
                    raise RuntimeError("production database path changed during readback")

                spill_reserved = False
                try:
                    shutil.rmtree(spill_directory, ignore_errors=True)
                    store.sync(spill_relative)
                finally:
                    store.release_storage(spill_budget)

                pruned = incremental.prune(
                    store, state, release_id, catalogue_version, _context=verification
                )
                rechecked = publish.current(store, catalogue_version=catalogue_version)
                rechecked_manifest_sha256 = digest(
                    regular_bytes(Path(rechecked["output"]) / "manifest.json", 1024**2)
                )
                if rechecked != current or rechecked_manifest_sha256 != manifest_sha256:
                    raise RuntimeError("current publication changed during retention cleanup")
                receipt = warehouse.certified(
                    store, capture_id, catalogue_version=catalogue_version, _context=verification
                )
                if receipt.get("warehouse_sha256") != checksum:
                    raise RuntimeError("certified warehouse changed during retention cleanup")
                source_info_after = source.lstat()
                if (
                    not stat.S_ISREG(source_info_after.st_mode)
                    or source.is_symlink()
                    or (source_info_after.st_dev, source_info_after.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or source_info_after.st_size != source_info.st_size
                    or file_digest(source, limits.max_database_bytes)[0] != checksum
                ):
                    raise RuntimeError("certified warehouse changed before production promotion")
                if (
                    source.with_name(source.name + ".wal").exists()
                    or alias.with_name(alias.name + ".wal").exists()
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
                linked_after = temporary.lstat()
                if not stat.S_ISREG(linked_after.st_mode) or (
                    linked_after.st_dev,
                    linked_after.st_ino,
                ) != (source_info_after.st_dev, source_info_after.st_ino):
                    raise RuntimeError("temporary production link changed during retention cleanup")
                if alias.is_symlink() or (
                    alias.exists() and not stat.S_ISREG(alias.lstat().st_mode)
                ):
                    raise RuntimeError("production database path changed during retention cleanup")
                replace_fn(temporary, alias)
                sync_fn(directory)
            finally:
                try:
                    with contextlib.suppress(FileNotFoundError):
                        temporary.unlink()
                finally:
                    if spill_reserved:
                        try:
                            shutil.rmtree(spill_directory, ignore_errors=True)
                            store.sync(spill_relative)
                        finally:
                            store.release_storage(spill_budget)
            alias_info = alias.stat()
            if (alias_info.st_dev, alias_info.st_ino) != (source_info.st_dev, source_info.st_ino):
                raise RuntimeError("production hard-link inode differs from certified warehouse")
            alias_checksum, _ = file_digest(alias, limits.max_database_bytes)
            if alias_checksum != checksum:
                raise RuntimeError("production hard-link checksum differs from certified warehouse")
            return {
                "state": "verified",
                "path": str(alias),
                "source_release_id": release_id,
                "capture_id": capture_id,
                "warehouse_sha256": checksum,
                "manifest_warehouse_sha256": manifest_checksum,
                "verification_mode": verification_mode,
                "database_bytes": alias_info.st_size,
                "pruned_artifacts": pruned,
                "production_readback": {
                    "current_markets": readback_rows["markets"],
                    "relation_rows": readback_rows,
                    "samples": samples,
                    "read_only": True,
                    **(
                        {"catalogue_coverage": readback_coverage}
                        if readback_coverage is not None
                        else {}
                    ),
                },
                "relation_rows": counts,
                "coverage_scope": current.get("coverage_scope"),
                "source_capture_complete": state.get("complete") is True,
                "scope_complete": True,
                **(
                    {"catalogue_coverage": readback_coverage}
                    if readback_coverage is not None
                    else {}
                ),
                "observation_interval": {
                    "started_at": state.get("started_at"),
                    "completed_at": state.get("completed_at"),
                },
                "atomic_snapshot": False,
                "recovered_activation_temporaries": recovered_temporaries,
            }
