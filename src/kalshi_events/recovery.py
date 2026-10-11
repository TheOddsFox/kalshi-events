"""Verified backups and staged raw-only rebuild; existing publications survive failure."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any

from kalshi_events.capture import complete_capture, latest_capture, read_capture
from kalshi_events.common import (
    CatalogueError,
    Limits,
    Store,
    atomic_json,
    file_digest,
    regular_bytes,
    safe_path,
)
from kalshi_events.publish import current
from kalshi_events.warehouse import build, load


def backup(store: Store, output: Path) -> dict[str, Any]:
    output = safe_path(output)
    if output.exists() or output.is_relative_to(store.root) or store.root.is_relative_to(output):
        raise CatalogueError("backup destination must be new and disjoint from operator root")
    usage = store.check_storage()
    if usage["retained_bytes"] > store.limits.max_temp_bytes:
        raise CatalogueError("backup staging would exceed temporary byte ceiling")
    if store.path("current.json").exists():
        current(store)
    for path in store.path("captures").glob("*/state.json"):
        state = read_capture(store, path.parent.name)
        if state["complete"]:
            complete_capture(store, state["capture_id"])
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".kalshi-backup-", dir=output.parent))
    try:
        shutil.copytree(
            store.root,
            temporary / "root",
            ignore=shutil.ignore_patterns("writer.lock", ".write-*", "tmp"),
        )
        inventory = {}
        for path in sorted((temporary / "root").rglob("*")):
            if path.is_file():
                checksum, size = file_digest(path, store.limits.max_root_bytes)
                inventory[str(path.relative_to(temporary / "root"))] = {
                    "sha256": checksum,
                    "size": size,
                }
        atomic_json(
            temporary / "backup.json",
            {"contract": "oddsfox.kalshi.events.backup.v1", "files": inventory},
        )
        verify_backup(temporary, store.limits.max_root_bytes)
        os.rename(temporary, output)
        return {"output": str(output), "files": len(inventory)}
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def verify_backup(directory: Path, limit: int) -> dict[str, Any]:
    directory = safe_path(directory)
    manifest = json.loads(regular_bytes(directory / "backup.json"))
    if manifest.get("contract") != "oddsfox.kalshi.events.backup.v1" or not isinstance(
        manifest.get("files"), dict
    ):
        raise CatalogueError("invalid backup contract")
    actual = set()
    total = 0
    for path in (directory / "root").rglob("*"):
        safe_path(path)
        if path.is_file():
            actual.add(str(path.relative_to(directory / "root")))
    if actual != set(manifest["files"]):
        raise CatalogueError("backup inventory differs from files")
    for relative, expected in manifest["files"].items():
        path = safe_path(directory / "root" / relative)
        if not path.is_relative_to(directory / "root"):
            raise CatalogueError("backup path escapes root")
        checksum, size = file_digest(path, limit)
        total += size
        if total > limit or checksum != expected.get("sha256") or size != expected.get("size"):
            raise CatalogueError("backup evidence integrity or size mismatch")
    return manifest


def restore(directory: Path, output: Path, limits: Limits) -> dict[str, Any]:
    manifest = verify_backup(directory, limits.max_root_bytes)
    if sum(entry["size"] for entry in manifest["files"].values()) > limits.max_temp_bytes:
        raise CatalogueError("restore staging would exceed temporary byte ceiling")
    output = safe_path(output)
    if output.exists():
        raise CatalogueError("restore destination must be new")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".kalshi-restore-", dir=output.parent))
    try:
        shutil.copytree(directory / "root", temporary / "root")
        restored = Store(temporary / "root", limits)
        if restored.path("current.json").exists():
            current(restored)
        os.rename(temporary / "root", output)
        return {"output": str(output)}
    finally:
        shutil.rmtree(temporary)


def rebuild(store: Store) -> dict[str, Any]:
    capture_id = latest_capture(store, selected=False)
    evidence_bytes = sum(
        path.stat().st_size
        for name in ("raw", "captures")
        for path in store.path(name).rglob("*")
        if path.is_file()
    )
    store.check_storage(evidence_bytes)
    temporary = store.path(f"tmp/rebuild-{uuid.uuid4().hex}")
    temporary.mkdir(parents=True)
    try:
        staged = Store(temporary / "root", store.limits)
        staged.started = store.started
        for name in ("raw", "captures"):
            shutil.copytree(store.path(name), staged.path(name))
        states = [read_capture(staged, path.name) for path in staged.path("captures").iterdir()]
        for state in sorted(states, key=lambda value: value["started_at"]):
            if state["complete"]:
                load(staged, state["capture_id"])
                store.check_storage()
        result = build(staged, capture_id)
        store.check_storage()
        _ = store.remaining
        for name in ("warehouse.duckdb", "build-artifacts.json", "build.json"):
            os.replace(staged.path(name), store.path(name))
        # The dlt state points at its original absolute location; replay regenerates it.
        if store.path("landing").exists():
            shutil.rmtree(store.path("landing"))
        for name in ("loaded",):
            if store.path(name).exists():
                shutil.rmtree(store.path(name))
            os.rename(staged.path(name), store.path(name))
        return result
    finally:
        shutil.rmtree(temporary)
