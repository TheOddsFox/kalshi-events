"""Immutable verified catalogue releases with atomic last-good promotion.

Publication promotes `current.json` only after verification. Superseded captures, their raw
evidence, builds and releases are pruned after that promotion, so the current release, its
capture and its build always remain.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from kalshi_events import metrics
from kalshi_events.capture import (
    capture_pages,
    complete_capture,
    latest_catalogue_capture,
    read_capture,
)
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    atomic_json,
    decode,
    digest,
    file_digest,
    interrupt_at_deadline,
    json_bytes,
    now,
    regular_bytes,
    safe_path,
)
from kalshi_events.warehouse import RELATIONS, build_directory, certified

CONTRACT = "oddsfox.kalshi.catalogue.v1"
FILES = {name + ".parquet" for name in (*RELATIONS, "coverage")}


def inventory(directory: Path, names: set[str], limit: int) -> dict[str, dict[str, Any]]:
    result = {}
    for name in sorted(names):
        checksum, size = file_digest(directory / name, limit)
        result[name] = {"sha256": checksum, "size": size}
    return result


def verify_release(directory: Path, limit: int = 64 * 1024**3) -> dict[str, Any]:
    with metrics.phase("verification"):
        return _verify_release(directory, limit)


def _verify_release(directory: Path, limit: int = 64 * 1024**3) -> dict[str, Any]:
    directory = safe_path(directory)
    manifest = decode(regular_bytes(directory / "manifest.json", 1024**2))
    if (
        not isinstance(manifest, dict)
        or manifest.get("contract") != CONTRACT
        or set(manifest.get("files", {})) != FILES
    ):
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


def publish(store: Store, capture_id: str | None = None) -> dict[str, Any]:
    store.require_writable()
    capture_id = capture_id or latest_catalogue_capture(store)
    state = complete_capture(store, capture_id)
    if state["selected"] is not None:
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    receipt = certified(store, capture_id)
    if receipt["scope"] != "catalogue":
        raise CatalogueError("targeted captures cannot promote the catalogue pointer")
    store.check_deadline()
    warehouse = store.path(f"{build_directory(capture_id)}/warehouse.duckdb")
    temporary_relative = f"tmp/publish-{uuid.uuid4().hex}"
    temporary = store.path(temporary_relative)
    temporary.mkdir(parents=True)
    try:
        with (
            duckdb.connect(str(warehouse), read_only=True) as connection,
            interrupt_at_deadline(store, connection.interrupt),
            metrics.phase("export"),
        ):
            for name, table in RELATIONS.items():
                store.check_deadline()
                destination = temporary / (name + ".parquet")
                # DuckDB's path parameter keeps the operator path out of SQL syntax.
                try:
                    connection.execute(
                        f"copy (select * from main.{table} order by all) to ? (format parquet)",
                        [str(destination)],
                    )
                except duckdb.InterruptException as exc:
                    raise BudgetExceeded("operation deadline exhausted") from exc
        cutoffs = [
            {"received_at": manifest["received_at"], "cutoff": payload}
            for manifest, payload in capture_pages(store, state)
            if manifest.get("endpoint") == "/historical/cutoff" and manifest.get("status") == 200
        ]
        coverage = [
            {
                "capture_id": state["capture_id"],
                "coverage_scope": "catalogue",
                "started_at": state["started_at"],
                "completed_at": state["completed_at"],
                "atomic_snapshot": False,
                "traversals_json": json_bytes(state["tasks"]).decode().strip(),
                "cutoff_json": json_bytes(cutoffs).decode().strip(),
            }
        ]
        store.check_deadline()
        pq.write_table(pa.Table.from_pylist(coverage), temporary / "coverage.parquet")
        store.sync(temporary_relative)
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
        store.sync(temporary_relative)
        store.sync(f"releases/{release_id}")
        store.check_deadline()
        with metrics.phase("promotion"):
            store.write(
                "current.json",
                {
                    "source_release_id": release_id,
                    "manifest_sha256": digest(regular_bytes(destination / "manifest.json")),
                },
            )
        # Pruning runs only after the promoted pointer re-verifies end to end.
        promoted = current(store)
        if promoted["capture_id"] != capture_id:
            raise CatalogueError("promoted release does not belong to the published capture")
        with metrics.phase("pruning"):
            pruned = prune_superseded(store, state, release_id)
        return {"output": str(destination), **manifest, "pruned_captures": pruned}
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
        store.sync(temporary_relative)


def prune_superseded(store: Store, state: dict[str, Any], release_id: str) -> list[str]:
    """Delete catalogue captures older than the published one, with their raw evidence, builds
    and superseded releases. Runs only after the release is verified and promoted."""
    store.require_writable()
    keep = state["capture_id"]
    removed = []
    captures = store.path("captures")
    for child in sorted(captures.iterdir()) if captures.exists() else []:
        old = read_capture(store, child.name)
        if (
            old["selected"] is None
            and child.name != keep
            and old["started_at"] < state["started_at"]
        ):
            removed.append(child.name)
    if removed:
        gone = set(removed)
        for name in removed:
            shutil.rmtree(store.path(f"captures/{name}"))
        store.sync("captures")
        for manifest in list(store.path("raw").glob("*/manifest.json")):
            if decode(regular_bytes(manifest, 1024**2)).get("capture_id") in gone:
                shutil.rmtree(manifest.parent)
        store.sync("raw")
    builds = store.path("builds")
    for child in sorted(builds.iterdir()) if builds.exists() else []:
        if child.name != keep:
            shutil.rmtree(child)
    store.sync("builds")
    releases = store.path("releases")
    for child in sorted(releases.iterdir()) if releases.exists() else []:
        if child.name != release_id:
            shutil.rmtree(child)
    store.sync("releases")
    return removed


def current(store: Store) -> dict[str, Any]:
    pointer = decode(regular_bytes(store.path("current.json"), 1024**2))
    release_id = pointer.get("source_release_id", "")
    if len(release_id) != 64 or any(c not in "0123456789abcdef" for c in release_id):
        raise CatalogueError("invalid current release identity")
    directory = store.path(f"releases/{release_id}")
    manifest = verify_release(directory, store.limits.max_root_bytes)
    if digest(regular_bytes(directory / "manifest.json")) != pointer.get("manifest_sha256"):
        raise CatalogueError("current pointer manifest mismatch")
    return {"output": str(directory), **manifest}
