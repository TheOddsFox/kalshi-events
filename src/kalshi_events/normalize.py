"""Lossless native identities and observation-era relationships; never ticker inference."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from kalshi_events.common import CatalogueError, digest, json_bytes, ticker

ENTITY_KEYS = {"series": "series_ticker", "events": "event_ticker", "markets": "market_ticker"}


def records(payload: Any, key: str, *, singular: bool = False) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise CatalogueError("source envelope must be an object")
    value = payload.get(key)
    if singular:
        if not isinstance(value, dict):
            raise CatalogueError(f"source envelope lacks {key} object")
        value = [value]
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise CatalogueError(f"source envelope lacks {key} object array")
    return value


def observation(
    kind: str, native: dict[str, Any], provenance: dict[str, Any], pointer: str
) -> dict[str, Any]:
    identity = native.get("event_ticker") if kind == "events" else native.get("ticker")
    key = ENTITY_KEYS[kind]
    row = {
        **native,
        key: ticker(identity),
        **provenance,
        "observation_id": digest(json_bytes([provenance["page_id"], pointer])),
        "provider_updated_at": native.get("updated_time", native.get("last_updated_ts")),
        "payload_json": json_bytes(native).decode().strip(),
    }
    if kind == "events":
        if native.get("series_ticker"):
            ticker(native["series_ticker"])
        row["series_ticker"] = native.get("series_ticker")
    if kind == "markets":
        row["event_ticker"] = ticker(native.get("event_ticker"))
        legs = native.get("mve_selected_legs", [])
        if not isinstance(legs, list) or any(not isinstance(leg, dict) for leg in legs):
            raise CatalogueError("malformed native combo legs")
        for leg in legs:
            ticker(leg.get("event_ticker"))
            ticker(leg.get("market_ticker"))
            if leg.get("side") not in {"yes", "no"}:
                raise CatalogueError("malformed native combo side")
    return row


def observations(pages: Iterable[tuple[dict[str, Any], Any]]) -> Iterator[dict[str, Any]]:
    for manifest, payload in pages:
        if manifest["status"] != 200:
            continue
        kind = manifest.get("entity_kind")
        if kind not in ENTITY_KEYS:
            continue
        provenance = {
            "capture_id": manifest["capture_id"],
            "page_id": manifest["page_id"],
            "received_at": manifest["received_at"],
            "source_endpoint": manifest["endpoint"],
            "payload_sha256": manifest["sha256"],
            "source_kind": "direct",
        }
        envelope = {"series": "series", "events": "events", "markets": "markets"}[kind]
        if manifest.get("singular"):
            envelope = {"series": "series", "events": "event", "markets": "market"}[kind]
        for index, native in enumerate(
            records(payload, envelope, singular=manifest.get("singular", False))
        ):
            row = observation(kind, native, provenance, f"/{envelope}/{index}")
            yield {"entity_kind": kind, **row}
            if kind == "events" and "markets" in native:
                for n, market in enumerate(records(native, "markets")):
                    if market.get("event_ticker") != row["event_ticker"]:
                        raise CatalogueError("nested market parent contradicts event")
                    yield {
                        "entity_kind": "markets",
                        **observation(
                            "markets",
                            market,
                            {**provenance, "source_kind": "event_reference"},
                            f"/{envelope}/{index}/markets/{n}",
                        ),
                    }


def project_pages(
    pages: Iterable[tuple[dict[str, Any], Any]], *, selected: set[str] | None = None
) -> dict[str, list[dict[str, Any]]]:
    history = (
        row
        for row in observations(pages)
        if selected is None or row["entity_kind"] != "markets" or row["market_ticker"] in selected
    )
    unique = {row["observation_id"]: row for row in history}
    history = sorted(unique.values(), key=lambda row: (row["received_at"], row["observation_id"]))
    result: dict[str, list[dict[str, Any]]] = {kind: [] for kind in ENTITY_KEYS}
    for kind, key in ENTITY_KEYS.items():
        latest = {}
        for row in history:
            if row["entity_kind"] == kind and (kind != "markets" or row["source_kind"] == "direct"):
                latest[row[key]] = {k: v for k, v in row.items() if k != "entity_kind"}
        result[kind] = [latest[k] for k in sorted(latest)]
    series = {row["series_ticker"] for row in result["series"]}
    events = {row["event_ticker"]: row for row in result["events"]}
    relationships = []
    for event in result["events"]:
        resolved = event.get("series_ticker") in series
        relationships.append(
            {
                "parent_kind": "series",
                "parent_ticker": event.get("series_ticker"),
                "child_kind": "events",
                "child_ticker": event["event_ticker"],
                "resolved": resolved,
            }
        )
    combo_legs = []
    markets = {row["market_ticker"]: row for row in result["markets"]}
    for market in result["markets"]:
        parent = events.get(market["event_ticker"])
        market["series_ticker"] = parent.get("series_ticker") if parent else None
        market["usable"] = bool(parent and market["series_ticker"] in series)
        relationships.append(
            {
                "parent_kind": "events",
                "parent_ticker": market["event_ticker"],
                "child_kind": "markets",
                "child_ticker": market["market_ticker"],
                "resolved": market["usable"],
            }
        )
        for index, leg in enumerate(market.get("mve_selected_legs", [])):
            combo_legs.append(
                {
                    **leg,
                    "combo_market_ticker": market["market_ticker"],
                    "leg_index": index,
                    "resolved": leg["market_ticker"] in markets
                    and markets[leg["market_ticker"]]["event_ticker"] == leg["event_ticker"]
                    and leg["event_ticker"] in events,
                }
            )
    return {
        **result,
        "observation_history": history,
        "relationships": relationships,
        "combo_legs": combo_legs,
    }
