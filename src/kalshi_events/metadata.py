"""Targeted lossless JSON handoff; absence and unresolved parents cannot fall back."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from kalshi_events.capture import (
    _complete_capture,
    capture,
    capture_pages,
    latest_complete_capture,
)
from kalshi_events.common import (
    MAX_BUNDLE_BYTES,
    BudgetExceeded,
    CatalogueError,
    Store,
    atomic_bytes,
    decode,
    digest,
    durable_mkdir,
    fsync_directory,
    json_bytes,
    regular_bytes,
    safe_path,
    selections,
)
from kalshi_events.normalize import ParentIndex, project_pages

CONTRACT = "oddsfox.kalshi.metadata.v1"
RELATIONS = ("series", "events", "markets", "coverage")
FILES = {name + ".json" for name in RELATIONS}


def verify(directory: Path) -> dict[str, Any]:
    directory = safe_path(directory)
    manifest_body = regular_bytes(directory / "manifest.json", 1024**2)
    manifest = decode(manifest_body)
    if (
        not isinstance(manifest, dict)
        or manifest.get("contract") != CONTRACT
        or manifest.get("coverage_scope") != "targeted"
        or set(manifest.get("files", {})) != FILES
    ):
        raise CatalogueError("invalid metadata contract")
    if {p.name for p in directory.iterdir()} != FILES | {"manifest.json"}:
        raise CatalogueError("unexpected metadata bundle content")
    inventory = {}
    total = len(manifest_body)
    for name in sorted(FILES):
        body = regular_bytes(directory / name)
        total += len(body)
        inventory[name] = {"sha256": digest(body), "size": len(body)}
        values = decode(body)
        if not isinstance(values, list) or any(not isinstance(v, dict) for v in values):
            raise CatalogueError("metadata relations must be object arrays")
    if (
        total > MAX_BUNDLE_BYTES
        or inventory != manifest["files"]
        or digest(json_bytes(inventory).rstrip(b"\n")) != manifest.get("source_release_id")
    ):
        raise CatalogueError("metadata integrity or byte ceiling mismatch")
    return manifest


def export(
    store: Store,
    selected: list[str],
    output: Path,
    *,
    capture_id: str | None = None,
    max_output_bytes: int = MAX_BUNDLE_BYTES,
) -> dict[str, Any]:
    if store.scope is not None:
        raise CatalogueError("metadata v1 export is not supported for scoped series roots")
    selected = selections(selected)
    if isinstance(max_output_bytes, bool) or not 0 < max_output_bytes <= MAX_BUNDLE_BYTES:
        raise CatalogueError("metadata output ceiling must be between 1 and 128 MiB")
    state = _complete_capture(store, capture_id or latest_complete_capture(store))
    if state.get("incremental") is True:
        raise CatalogueError("incremental captures cannot export metadata v1; use a full capture")
    if state["selected"] is not None and not set(selected).issubset(state["selected"]):
        raise CatalogueError("capture does not cover the selected markets")
    _ = store.remaining
    parent_index = ParentIndex()

    def indexed_pages():
        for manifest, payload in capture_pages(store, state):
            parent_index.add_page(manifest, payload)
            yield manifest, payload

    projection = project_pages(indexed_pages(), selected=set(selected))
    _, parent_lookups = parent_index.effective_parent_evidence(state)
    by_market = {row["market_ticker"]: row for row in projection["markets"]}
    coverage = []
    for identity in selected:
        market = by_market.get(identity)
        previous = state["coverage"].get(identity)
        if previous and previous["status"] != "found":
            coverage.append(previous)
        elif market is None:
            coverage.append(
                {
                    "market_ticker": identity,
                    "status": "absent",
                    "error": "not observed in completed requested capture",
                }
            )
        elif not market["usable"] or (
            state.get("parent_policy") == "batched-v1"
            and _metadata_parent_missing(market, projection, parent_lookups)
        ):
            coverage.append(
                {
                    "market_ticker": identity,
                    "status": "unresolved",
                    "error": "authoritative event or series parent unavailable",
                }
            )
        else:
            coverage.append({"market_ticker": identity, "status": "found", "error": None})
    allowed = {row["market_ticker"] for row in coverage if row["status"] == "found"}
    markets = [by_market[value] for value in selected if value in allowed]
    event_ids = {row["event_ticker"] for row in markets}
    events = [row for row in projection["events"] if row["event_ticker"] in event_ids]
    series_ids = {row["series_ticker"] for row in events}
    relations = {
        "markets": markets,
        "events": events,
        "series": [row for row in projection["series"] if row["series_ticker"] in series_ids],
        "coverage": coverage,
    }
    bodies = {name + ".json": json_bytes(relations[name]) for name in RELATIONS}
    inventory = {
        name: {"sha256": digest(body), "size": len(body)} for name, body in sorted(bodies.items())
    }
    release_id = digest(json_bytes(inventory).rstrip(b"\n"))
    manifest_body = json_bytes(
        {
            "contract": CONTRACT,
            "source_release_id": release_id,
            "received_at": state["completed_at"],
            "coverage_scope": "targeted",
            "files": inventory,
        }
    )
    if sum(map(len, bodies.values())) + len(manifest_body) > max_output_bytes:
        raise CatalogueError("metadata bundle exceeds agreed output ceiling")
    output = safe_path(output)
    if output.exists():
        raise CatalogueError("metadata destination must be new")
    durable_mkdir(output.parent)
    temporary = Path(tempfile.mkdtemp(prefix=".kalshi-metadata-", dir=output.parent))
    try:
        required = sum(map(len, bodies.values())) + len(manifest_body)
        if required > store.limits.max_temp_bytes:
            raise CatalogueError("metadata staging exceeds producer temporary ceiling")
        store.check_storage(required if output.is_relative_to(store.root) else 0)
        for name, body in bodies.items():
            atomic_bytes(temporary / name, body)
        atomic_bytes(temporary / "manifest.json", manifest_body)
        verify(temporary)
        _ = store.remaining
        os.rename(temporary, output)
        fsync_directory(output.parent)
        if output.is_relative_to(store.root):
            store.sync(str(output.relative_to(store.root)))
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return {
        "output": str(output),
        "manifest_sha256": digest(manifest_body),
        "source_release_id": release_id,
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
        "root": str(store.root),
        "retained_bytes": store.check_storage()["retained_bytes"],
    }


def _metadata_parent_missing(
    market: dict[str, Any],
    projection: dict[str, list[dict[str, Any]]],
    parent_lookups: dict[tuple[str, str], tuple[int, str, str]],
) -> bool:
    """A fresh authoritative point miss overrides older list observations."""
    event_ticker = market.get("event_ticker")
    event_lookup = parent_lookups.get(("events", event_ticker))
    if event_lookup is not None and event_lookup[0] != 200:
        return True
    event = next(
        (row for row in projection["events"] if row.get("event_ticker") == event_ticker),
        None,
    )
    if event is None:
        return True
    series_ticker = event.get("series_ticker")
    series_lookup = parent_lookups.get(("series", series_ticker))
    return series_lookup is not None and series_lookup[0] != 200


def refresh(
    store: Store,
    selected: list[str],
    output: Path,
    *,
    max_output_bytes: int = MAX_BUNDLE_BYTES,
    compression: str | None = None,
    rate_policy: str | None = None,
) -> dict[str, Any]:
    if store.scope is not None:
        raise CatalogueError("metadata v1 refresh is not supported for scoped series roots")
    state = capture(
        store,
        selected=selections(selected),
        compression=compression,
        rate_policy=rate_policy,
    )
    if not state["complete"]:
        message = "targeted metadata acquisition incomplete: " + (state["error"] or "unknown error")
        if state["budget_exhausted"]:
            raise BudgetExceeded(message)
        raise CatalogueError(message)
    result = export(
        store, selected, output, capture_id=state["capture_id"], max_output_bytes=max_output_bytes
    )
    return {
        **result,
        **{key: state[key] for key in ("requests", "downloaded_bytes", "accounting_complete")},
    }
