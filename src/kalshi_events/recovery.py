"""Rebuild the published catalogue capture from retained raw evidence.

The selected publication version resolves the capture when no ID is supplied. Rebuilding
certifies a fresh warehouse from retained evidence without changing publication pointers.
"""

from __future__ import annotations

from typing import Any

from kalshi_events.capture import read_capture
from kalshi_events.common import CatalogueError, Store
from kalshi_events.publish import current
from kalshi_events.warehouse import build, build_v2_full


def replay(
    store: Store, capture_id: str | None = None, *, catalogue_version: int = 1
) -> dict[str, Any]:
    if catalogue_version not in {1, 2} or isinstance(catalogue_version, bool):
        raise CatalogueError("catalogue version must be 1 or 2")
    with store.writer_guard():
        capture_id = capture_id or current(store, catalogue_version=catalogue_version)["capture_id"]
        state = read_capture(store, capture_id)
        if state.get("incremental") is True and catalogue_version != 2:
            raise CatalogueError("incremental captures require catalogue version 2")
        if catalogue_version == 2 and state.get("incremental") is not True:
            return build_v2_full(store, capture_id, rebuild=True)
        return build(store, capture_id)


def rebuild(store: Store, *, catalogue_version: int = 1) -> dict[str, Any]:
    return replay(store, catalogue_version=catalogue_version)
