"""Deterministic, in-memory source data for acquisition diagnostics only."""

from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
from collections import Counter
from collections.abc import Iterable
from typing import Any

SCENARIOS = ("balanced", "many-small", "empty", "skew", "dominant", "duplicates", "wide")


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def event_fixture(name: str = "balanced") -> dict[str, Any]:
    """Return fresh ordinary/MVE partitions, including terminal empty scopes."""
    counts = {
        "balanced": (4, 4, 4, 4),
        "many-small": (1,) * 8,
        "empty": (0, 0, 0, 0),
        "skew": (9, 1, 5, 0),
        "dominant": (1, 17, 0, 0),
        "duplicates": (4, 4, 4, 4),
        "wide": (3, 3, 3, 3),
    }
    if name not in counts:
        raise ValueError(f"unknown event fixture: {name}")
    partitions = []
    collections = {}
    for index, count in enumerate(counts[name]):
        kind = "series" if index % 2 == 0 else "collection"
        selected = f"SERIES-{index}" if kind == "series" else f"COLLECTION-{index}"
        series = selected if kind == "series" else "MVE-SERIES"
        if kind == "collection":
            collections[selected] = {
                "collection_ticker": selected,
                "series_ticker": series,
                "title": f"Collection {index}",
                "associated_event_tickers": ["CONSTITUENT-ONLY"],
            }
        events = [
            {
                "event_ticker": f"EVENT-{index}-{row}",
                "series_ticker": series,
                "title": f"Native event {index}/{row}: café 雨",
                "mutually_exclusive": False,
                "last_updated_ts": "2026-01-01T00:00:00Z" if row % 2 else None,
                "product_metadata": {
                    "rules": "Resolve using the published native definition.",
                    "exact_decimal": "0.123456789123456789",
                },
            }
            for row in range(count)
        ]
        if name == "duplicates":
            # Cross a page boundary with both an identical observation and a correction.
            events.insert(2, copy.deepcopy(events[0]))
            correction = copy.deepcopy(events[0])
            correction["title"] = "Corrected native definition"
            events.append(correction)
        if name == "wide":
            for event in events:
                event["product_metadata"]["rules"] = "Unicode café 雨; native rules. " * 512
        partitions.append(
            {"kind": kind, "ticker": selected, "series_ticker": series, "events": events}
        )
    return {"name": name, "page_size": 2, "partitions": partitions, "collections": collections}


def respond(
    fixture: dict[str, Any], endpoint: str, params: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    """Serve pure response envelopes; callers may safely mutate returned payloads."""
    params = params or {}
    prefix = "/multivariate_event_collections/"
    if endpoint.startswith(prefix):
        definition = fixture["collections"].get(endpoint[len(prefix) :])
        return (
            (200, {"multivariate_contract": copy.deepcopy(definition)}) if definition else (404, {})
        )
    if endpoint not in {"/events", "/events/multivariate"}:
        return 404, {}
    kind = "series" if endpoint == "/events" else "collection"
    if params.get("series_ticker") and params.get("collection_ticker"):
        return 400, {"error": "filters are mutually exclusive"}
    if kind == "series" and params.get("collection_ticker"):
        return 400, {"error": "ordinary events cannot use collection filtering"}
    selected = params.get("series_ticker") if kind == "series" else params.get("collection_ticker")
    partitions = [
        partition
        for partition in fixture["partitions"]
        if partition["kind"] == kind and (selected is None or partition["ticker"] == selected)
    ]
    rows = [event for partition in partitions for event in partition["events"]]
    cursor = params.get("cursor", "")
    if cursor == "":
        offset = 0
    elif isinstance(cursor, str) and cursor.startswith("offset-") and cursor[7:].isdigit():
        offset = int(cursor[7:])
        if offset <= 0 or offset >= len(rows):
            return 400, {"error": "invalid cursor"}
    else:
        return 400, {"error": "invalid cursor"}
    limit = params.get("limit", 200)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        return 400, {"error": "invalid limit"}
    end = min(offset + min(fixture["page_size"], limit), len(rows))
    return 200, {
        "events": copy.deepcopy(rows[offset:end]),
        "cursor": f"offset-{end}" if end < len(rows) else "",
    }


def encoded_body(payload: dict[str, Any], compression: str = "identity") -> bytes:
    body = _json(payload)
    if compression == "identity":
        return body
    if compression == "gzip":
        stream = io.BytesIO()
        with gzip.GzipFile(filename="", mode="wb", fileobj=stream, mtime=0) as zipped:
            zipped.write(body)
        return stream.getvalue()
    raise ValueError(f"unsupported fixture compression: {compression}")


def fixture_identity(fixture: dict[str, Any]) -> str:
    return hashlib.sha256(_json(fixture)).hexdigest()


def observation_inventory(rows: Iterable[dict[str, Any]]) -> list[tuple[str, str, int]]:
    """Compare observation multisets rather than silently deduplicating native tickers."""
    counts = Counter((row["event_ticker"], hashlib.sha256(_json(row)).hexdigest()) for row in rows)
    return [(ticker, checksum, count) for (ticker, checksum), count in sorted(counts.items())]
