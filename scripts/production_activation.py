"""Verified, external production.duckdb hard-link activation."""

from __future__ import annotations

import contextlib
import json
import os
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any


def activate_current(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str | None = None,
    standalone_verified: dict[str, Any] | None = None,
    catalogue_version: int = 2,
    link_fn: Any = None,
    replace_fn: Any = None,
    sync_fn: Any = None,
) -> dict[str, Any]:
    """Certify the selected warehouse, then atomically expose its hard link."""
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
    alias = directory / "production.duckdb"
    if alias.is_symlink():
        raise RuntimeError("production database path cannot be a symlink")
    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * 1024**3,
        max_response_bytes=384 * 1024**2,
        max_decoded_response_bytes=768 * 1024**2,
        max_duration_seconds=48 * 3600,
        max_root_bytes=350 * 1024**3,
        max_temp_bytes=64 * 1024**3,
        max_database_bytes=64 * 1024**3,
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
            pruned = incremental.prune(
                store, state, release_id, catalogue_version, _context=verification
            )
            # Recheck the current release and certification after cleanup, before the
            # external production path can change.
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
            source = store.path(
                f"{warehouse.build_directory(capture_id, catalogue_version)}/warehouse.duckdb"
            )
            source_info = source.lstat()
            if not stat.S_ISREG(source_info.st_mode) or source.is_symlink():
                raise RuntimeError("certified warehouse is not a regular file")
            source_checksum, _ = file_digest(source, limits.max_database_bytes)
            if source_checksum != checksum:
                raise RuntimeError("certified warehouse changed before production linking")
            if alias.is_symlink():
                raise RuntimeError("production database path cannot be a symlink")
            if alias.exists() and not stat.S_ISREG(alias.lstat().st_mode):
                raise RuntimeError("existing production database is not a regular file")
            if (
                source.with_name(source.name + ".wal").exists()
                or alias.with_name(alias.name + ".wal").exists()
            ):
                raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
            if source_info.st_dev != directory.stat().st_dev:
                raise RuntimeError(
                    "production database and warehouse must be on the same filesystem"
                )
            temporary = directory / f".production.duckdb.{uuid.uuid4().hex}.tmp"
            try:
                link_fn(source, temporary, follow_symlinks=False)
                replace_fn(temporary, alias)
                sync_fn(directory)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
            alias_info = alias.stat()
            if (alias_info.st_dev, alias_info.st_ino) != (source_info.st_dev, source_info.st_ino):
                raise RuntimeError("production hard-link inode differs from certified warehouse")
            alias_checksum, _ = file_digest(alias, limits.max_database_bytes)
            if alias_checksum != checksum:
                raise RuntimeError("production hard-link checksum differs from certified warehouse")
            query = (
                "import duckdb,json,sys; "
                "c=duckdb.connect(sys.argv[1],read_only=True); "
                "n=c.execute('select count(*) from current_markets').fetchone()[0]; "
                "v={'current_markets':n}; "
                + (
                    "r=c.execute('select historical_contract,scope,omitted_scopes,source_capture_id,source_capture_complete from main.catalogue_coverage').fetchone(); "
                    "v['catalogue_coverage']=dict(zip(['historical_contract','scope','omitted_scopes','source_capture_id','source_capture_complete'],r)); "
                    if catalogue_version == 3
                    else ""
                )
                + "c.close(); print(json.dumps(v))"
            )
            readback = subprocess.run(
                [sys.executable, "-c", query, str(alias)],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            readback_data = json.loads(readback.stdout)
            market_count = readback_data.get("current_markets")
            if (
                isinstance(market_count, bool)
                or not isinstance(market_count, int)
                or market_count < 0
            ):
                raise RuntimeError("production DuckDB readback returned an invalid market count")
            coverage = readback_data.get("catalogue_coverage") if catalogue_version == 3 else None
            if catalogue_version == 3:
                expected_omitted = json.loads(current["omitted_scopes_json"])
                if coverage != {
                    "historical_contract": current.get("historical_contract"),
                    "scope": "historical",
                    "omitted_scopes": expected_omitted,
                    "source_capture_id": capture_id,
                    "source_capture_complete": current.get("source_capture_complete"),
                }:
                    raise RuntimeError(
                        "production catalogue coverage readback differs from release"
                    )
            counts = {}
            for filename in current["files"]:
                if filename.endswith(".parquet") and filename != "coverage.parquet":
                    import pyarrow.parquet as pq

                    counts[filename.removesuffix(".parquet")] = pq.ParquetFile(
                        Path(current["output"]) / filename
                    ).metadata.num_rows
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
                "production_readback": {"current_markets": market_count, "read_only": True},
                "relation_rows": counts,
                "coverage_scope": current.get("coverage_scope"),
                "source_capture_complete": state.get("complete") is True,
                "scope_complete": True,
                **({"catalogue_coverage": coverage} if coverage is not None else {}),
                "observation_interval": {
                    "started_at": state.get("started_at"),
                    "completed_at": state.get("completed_at"),
                },
                "atomic_snapshot": False,
            }
