"""Lossless native identities and observation-era relationships; never ticker inference."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from kalshi_events.common import CatalogueError, digest, json_bytes, ticker, validate_series_scope

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
    identity, parent = native_identity(kind, native)
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
        row["series_ticker"] = parent
    if kind == "markets":
        row["event_ticker"] = parent
    return row


def native_identity(kind: str, native: dict[str, Any]) -> tuple[str, Any]:
    """Validate native identity and relationship fields without serializing payloads."""
    identity = native.get("event_ticker") if kind == "events" else native.get("ticker")
    identity = ticker(identity)
    parent = None
    if kind == "events":
        parent = native.get("series_ticker")
        if parent:
            ticker(parent)
    elif kind == "markets":
        parent = ticker(native.get("event_ticker"))
        legs = native.get("mve_selected_legs", [])
        if not isinstance(legs, list) or any(not isinstance(leg, dict) for leg in legs):
            raise CatalogueError("malformed native combo legs")
        for leg in legs:
            ticker(leg.get("event_ticker"))
            ticker(leg.get("market_ticker"))
            if leg.get("side") not in {"yes", "no"}:
                raise CatalogueError("malformed native combo side")
    return identity, parent


class ParentIndex:
    """Compact winner identities needed to resolve capture-time parent lookups."""

    def __init__(self) -> None:
        self.series: dict[str, tuple[str, str, None]] = {}
        self.events: dict[str, tuple[str, str, str | None]] = {}
        self.markets: dict[str, tuple[str, str, str]] = {}
        self.processed: set[str] = set()
        self.scope_membership: dict[str, dict[int, set[str]]] = {}
        self.parent_lookups: dict[tuple[str, str], tuple[int, str, str]] = {}

    @staticmethod
    def _put(target: dict[str, tuple], identity: str, winner: tuple) -> None:
        previous = target.get(identity)
        if previous is None or winner[:2] > previous[:2]:
            target[identity] = winner

    def add_page(self, manifest: dict[str, Any], payload: Any) -> None:
        kind = manifest.get("entity_kind")
        series_scope = manifest.get("series_scope")
        scoped_series = (
            validate_series_scope(series_scope)["series_ticker"]
            if series_scope is not None
            else None
        )
        if manifest.get("singular") and kind in {"events", "series"}:
            endpoint = manifest.get("endpoint", "")
            prefix = "/events/" if kind == "events" else "/series/"
            if endpoint.startswith(prefix):
                identity = endpoint[len(prefix) :]
                self.parent_lookups[(kind, identity)] = (
                    manifest.get("status", 0),
                    manifest.get("page_id", ""),
                    manifest.get("request_id", ""),
                )
        if manifest.get("status") != 200:
            return
        if kind not in ENTITY_KEYS:
            return
        envelope = {"series": "series", "events": "events", "markets": "markets"}[kind]
        if manifest.get("singular"):
            envelope = {"series": "series", "events": "event", "markets": "market"}[kind]
        provenance = {
            "page_id": manifest["page_id"],
            "received_at": manifest["received_at"],
        }
        for index, native in enumerate(
            records(payload, envelope, singular=manifest.get("singular", False))
        ):
            identity, parent = native_identity(kind, native)
            if scoped_series is not None:
                if kind == "series" and identity != scoped_series:
                    raise CatalogueError("series definition differs from the scoped ticker")
                if kind == "events":
                    if parent != scoped_series:
                        raise CatalogueError("event is outside the scoped series")
                    if native.get("mve_collection_ticker") or native.get("mve_selected_legs"):
                        raise CatalogueError("series traversal returned a multivariate event")
                if kind == "markets" and (
                    native.get("mve_collection_ticker") or native.get("mve_selected_legs")
                ):
                    raise CatalogueError("series traversal returned a combo market")
            scope = manifest.get("task_scope")
            generation = manifest.get("task_generation", 0)
            if (
                isinstance(scope, str)
                and isinstance(generation, int)
                and not isinstance(generation, bool)
                and (
                    not manifest.get("singular")
                    or (kind == "series" and scope == "series" and identity == scoped_series)
                )
            ):
                if (
                    kind == "markets"
                    and scope == "markets_live"
                    and (
                        native.get("mve_collection_ticker") or bool(native.get("mve_selected_legs"))
                    )
                ):
                    membership_scope = "markets_combo"
                else:
                    membership_scope = scope
                self.scope_membership.setdefault(membership_scope, {}).setdefault(
                    generation, set()
                ).add(identity)
            winner = (
                provenance["received_at"],
                digest(json_bytes([provenance["page_id"], f"/{envelope}/{index}"])),
                parent,
            )
            if kind == "series":
                self._put(self.series, identity, winner[:2] + (None,))
            elif kind == "events":
                self._put(self.events, identity, winner)
                if "markets" in native:
                    for market in records(native, "markets"):
                        if market.get("event_ticker") != identity:
                            raise CatalogueError("nested market parent contradicts event")
                        if scoped_series is not None and (
                            market.get("mve_collection_ticker") or market.get("mve_selected_legs")
                        ):
                            raise CatalogueError("series traversal returned a nested combo market")
                        native_identity("markets", market)
            else:
                self._put(self.markets, identity, winner)


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
