"""Immutable verified catalogue releases with atomic last-good promotion."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_events.capture import complete_capture
from kalshi_events.common import (
    CatalogueError,
    Store,
    atomic_json,
    digest,
    file_digest,
    json_bytes,
    now,
    regular_bytes,
    safe_path,
)
from kalshi_events.warehouse import RELATIONS, certified

CONTRACT = "oddsfox.kalshi.catalogue.v1"
FILES = {name + ".parquet" for name in (*RELATIONS, "coverage")}


def inventory(directory: Path, names: set[str], limit: int) -> dict[str, dict[str, Any]]:
    result = {}
    for name in sorted(names):
        checksum, size = file_digest(directory / name, limit)
        result[name] = {"sha256": checksum, "size": size}
    return result


def verify_release(directory: Path, limit: int = 64 * 1024**3) -> dict[str, Any]:
    directory = safe_path(directory)
    manifest = json.loads(regular_bytes(directory / "manifest.json", 1024**2))
    if manifest.get("contract") != CONTRACT or set(manifest.get("files", {})) != FILES:
        raise CatalogueError("invalid catalogue contract or relation inventory")
    if {p.name for p in directory.iterdir()} != FILES | {"manifest.json"}:
        raise CatalogueError("release contains unexpected or missing files")
    actual = inventory(directory, FILES, limit)
    if actual != manifest["files"] or digest(json_bytes(actual).rstrip(b"\n")) != manifest.get(
        "source_release_id"
    ):
        raise CatalogueError("catalogue release integrity mismatch")
    for name in FILES:
        try:
            _ = pq.ParquetFile(directory / name).schema
        except Exception as exc:
            raise CatalogueError("invalid Parquet release relation") from exc
    return manifest


def publish(store: Store) -> dict[str, Any]:
    receipt = certified(store)
    if receipt["scope"] != "catalogue":
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    state = complete_capture(store, receipt["capture_id"])
    temporary = store.path(f"tmp/publish-{uuid.uuid4().hex}")
    temporary.mkdir(parents=True)
    try:
        with duckdb.connect(str(store.path("warehouse.duckdb")), read_only=True) as connection:
            for name, table in RELATIONS.items():
                destination = temporary / (name + ".parquet")
                # DuckDB's path parameter keeps the operator path out of SQL syntax.
                connection.execute(
                    f"copy (select * from main.{table} order by all) to ? (format parquet)",
                    [str(destination)],
                )
        coverage = [
            {
                "capture_id": state["capture_id"],
                "coverage_scope": "catalogue",
                "started_at": state["started_at"],
                "completed_at": state["completed_at"],
                "atomic_snapshot": False,
                "traversals_json": json_bytes(state["tasks"]).decode().strip(),
                "cutoff_json": json_bytes(state["cutoff"]).decode().strip(),
            }
        ]
        pq.write_table(pa.Table.from_pylist(coverage), temporary / "coverage.parquet")
        store.check_storage()
        evidence = inventory(temporary, FILES, store.limits.max_root_bytes)
        release_id = digest(json_bytes(evidence).rstrip(b"\n"))
        manifest = {
            "contract": CONTRACT,
            "source_release_id": release_id,
            "capture_id": state["capture_id"],
            "received_at": now(),
            "coverage_scope": "catalogue",
            "files": evidence,
        }
        atomic_json(temporary / "manifest.json", manifest)
        verify_release(temporary, store.limits.max_root_bytes)
        destination = store.path(f"releases/{release_id}")
        destination.parent.mkdir(exist_ok=True)
        if destination.exists():
            verify_release(destination, store.limits.max_root_bytes)
        else:
            os.rename(temporary, destination)
        _ = store.remaining
        store.write(
            "current.json",
            {
                "source_release_id": release_id,
                "manifest_sha256": digest(regular_bytes(destination / "manifest.json")),
            },
        )
        return {"output": str(destination), **manifest}
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def current(store: Store) -> dict[str, Any]:
    pointer = json.loads(regular_bytes(store.path("current.json"), 1024**2))
    release_id = pointer.get("source_release_id", "")
    if len(release_id) != 64 or any(c not in "0123456789abcdef" for c in release_id):
        raise CatalogueError("invalid current release identity")
    directory = store.path(f"releases/{release_id}")
    manifest = verify_release(directory, store.limits.max_root_bytes)
    if digest(regular_bytes(directory / "manifest.json")) != pointer.get("manifest_sha256"):
        raise CatalogueError("current pointer manifest mismatch")
    return {"output": str(directory), **manifest}
