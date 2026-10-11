"""Resumable traversals: response evidence precedes every cursor checkpoint."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

from kalshi_events.common import (
    Budget,
    BudgetExceeded,
    CatalogueError,
    Store,
    decode,
    digest,
    json_bytes,
    now,
    regular_bytes,
    selections,
    ticker,
)
from kalshi_events.normalize import observations, records
from kalshi_events.transport import PublicClient, read_page


def task(
    endpoint: str,
    kind: str | None,
    *,
    singular: bool = False,
    params: dict[str, Any] | None = None,
    requested: str | None = None,
) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "kind": kind,
        "singular": singular,
        "params": params or {},
        "requested": requested,
        "cursor": "",
        "seen_cursors": [],
        "restarts": 0,
        "done": False,
    }


def read_capture(store: Store, capture_id: str) -> dict[str, Any]:
    if (
        not isinstance(capture_id, str)
        or len(capture_id) != 32
        or any(c not in "0123456789abcdef" for c in capture_id)
    ):
        raise CatalogueError("invalid capture identity")
    state = decode(regular_bytes(store.path(f"captures/{capture_id}/state.json")))
    if state.get("capture_id") != capture_id or state.get("version") != 1:
        raise CatalogueError("invalid capture state")
    return state


def capture_pages(store: Store, state: dict[str, Any]) -> Iterator[tuple[dict[str, Any], Any]]:
    for relative in state["pages"]:
        manifest, payload = read_page(store, relative)
        if manifest.get("capture_id") != state["capture_id"]:
            raise CatalogueError("raw evidence belongs to another capture")
        yield manifest, payload


def save(store: Store, state: dict[str, Any]) -> None:
    store.write(f"captures/{state['capture_id']}/state.json", state)


def new_capture(store: Store, selected: list[str] | None) -> dict[str, Any]:
    work = [task("/historical/cutoff", None, singular=True)]
    if selected is None:
        work += [
            task("/series", "series"),
            task("/events", "events", params={"with_nested_markets": "false", "limit": 200}),
            task(
                "/events/multivariate",
                "events",
                params={"with_nested_markets": "false", "limit": 200},
            ),
            task("/markets", "markets", params={"limit": 200}),
            task("/historical/markets", "markets", params={"limit": 200}),
        ]
    else:
        work += [
            task(f"/markets/{ticker(value)}", "markets", singular=True, requested=value)
            for value in selections(selected)
        ]
    state = {
        "version": 1,
        "capture_id": uuid.uuid4().hex,
        "selected": selected,
        "started_at": now(),
        "completed_at": None,
        "complete": False,
        "tasks": work,
        "pages": [],
        "pending": None,
        "coverage": {},
        "error": None,
    }
    save(store, state)
    return state


def _consume(store: Store, state: dict[str, Any], index: int, relative: str) -> None:
    work = state["tasks"][index]
    manifest, payload = read_page(store, relative)
    status = manifest["status"]
    if status in {400, 410} and work["cursor"]:
        if work["restarts"] >= 3:
            raise CatalogueError("pagination cursor repeatedly invalidated")
        work.update(cursor="", seen_cursors=[], restarts=work["restarts"] + 1)
    elif status == 404 and work["singular"] and work["kind"] in {"markets", "events", "series"}:
        work["done"] = True
        if work["kind"] == "markets" and work["requested"]:
            selected = work["requested"]
            if work["endpoint"].startswith("/markets/"):
                state["tasks"].append(
                    task(
                        f"/historical/markets/{selected}",
                        "markets",
                        singular=True,
                        requested=selected,
                    )
                )
            else:
                state["coverage"][selected] = {
                    "market_ticker": selected,
                    "status": "absent",
                    "error": "not returned by live or historical market lookup",
                }
    elif status != 200:
        raise CatalogueError(f"incomplete source traversal: HTTP {status}")
    elif work["kind"] is None:
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("market_settled_ts"), str)
            or not isinstance(payload.get("trades_created_ts"), str)
        ):
            raise CatalogueError("malformed historical cutoff response")
        for key in ("market_settled_ts", "trades_created_ts"):
            try:
                instant = datetime.fromisoformat(payload[key].replace("Z", "+00:00"))
                if instant.utcoffset() != timedelta(0):
                    raise ValueError("cutoff must be UTC")
            except ValueError as exc:
                raise CatalogueError("malformed UTC historical cutoff") from exc
        state["cutoff"] = payload
        work["done"] = True
    else:
        envelope = {
            "series": "series",
            "events": "event" if work["singular"] else "events",
            "markets": "market" if work["singular"] else "markets",
        }[work["kind"]]
        rows = records(payload, envelope, singular=work["singular"])
        if work["singular"]:
            key = "event_ticker" if work["kind"] == "events" else "ticker"
            if rows[0].get(key) != work["endpoint"].rsplit("/", 1)[1]:
                raise CatalogueError("direct lookup returned a different native identity")
            work["done"] = True
        elif work["endpoint"] == "/series":
            work["done"] = True
        else:
            cursor = payload.get("cursor")
            if not isinstance(cursor, str) or len(cursor) > 8192:
                raise CatalogueError("missing or malformed pagination cursor")
            if cursor and (cursor == work["cursor"] or cursor in work["seen_cursors"]):
                raise CatalogueError("pagination cursor repeated without completion")
            if cursor:
                work["seen_cursors"].append(cursor)
            work["cursor"] = cursor
            work["done"] = not cursor
        if work["kind"] == "markets" and work["requested"]:
            state["coverage"][work["requested"]] = {
                "market_ticker": work["requested"],
                "status": "found",
                "error": None,
            }
    state["pending"] = None
    save(store, state)


def _hydrate(store: Store, state: dict[str, Any]) -> bool:
    events = {}
    series = set()
    markets = {}
    for row in observations(capture_pages(store, state)):
        if row["entity_kind"] == "series":
            series.add(row["series_ticker"])
        elif row["entity_kind"] == "events":
            events[row["event_ticker"]] = row.get("series_ticker")
        elif row["source_kind"] == "direct":
            markets[row["market_ticker"]] = row["event_ticker"]
    existing = {work["endpoint"] for work in state["tasks"]}
    missing_events = set(markets.values()) - events.keys()
    missing_series = {value for value in events.values() if value} - series
    for kind, identities, endpoint in (
        ("events", missing_events, "/events"),
        ("series", missing_series, "/series"),
    ):
        for identity in sorted(identities):
            path = f"{endpoint}/{ticker(identity)}"
            if path not in existing:
                state["tasks"].append(task(path, kind, singular=True))
    if any(not work["done"] for work in state["tasks"]):
        save(store, state)
        return True
    if state["selected"] is not None:
        for selected in state["selected"]:
            parent = markets.get(selected)
            if parent and (parent not in events or events[parent] not in series):
                state["coverage"][selected] = {
                    "market_ticker": selected,
                    "status": "unresolved",
                    "error": "authoritative event or series parent unavailable",
                }
    return False


def _request_context(state: dict[str, Any], index: int) -> dict[str, Any]:
    work = state["tasks"][index]
    return {
        "capture_id": state["capture_id"],
        "task_index": index,
        "request_id": digest(
            json_bytes([state["capture_id"], index, work["cursor"], work["restarts"]])
        ),
        "entity_kind": work["kind"],
        "singular": work["singular"],
    }


def _recover_receipts(store: Store, state: dict[str, Any]) -> dict[str, str]:
    checkpointed = set(state["pages"])
    recovered = {}
    for path in store.path("raw").glob("*/manifest.json"):
        relative = str(path.relative_to(store.root))
        if relative in checkpointed:
            continue
        manifest = decode(regular_bytes(path, 1024**2))
        if (
            manifest.get("capture_id") != state["capture_id"]
            or manifest.get("accounting_complete") is not True
            or "error" in manifest
            or manifest.get("status") not in {200, 400, 404, 410}
        ):
            continue
        index = manifest.get("task_index")
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < len(state["tasks"])
        ):
            raise CatalogueError("uncheckpointed raw receipt has invalid task identity")
        work = state["tasks"][index]
        if work["done"]:
            continue
        context = _request_context(state, index)
        if manifest.get("request_id") != context["request_id"]:
            continue
        params = {**work["params"], **({"cursor": work["cursor"]} if work["cursor"] else {})}
        if (
            manifest.get("endpoint") != work["endpoint"]
            or manifest.get("params") != params
            or manifest.get("entity_kind") != work["kind"]
            or manifest.get("singular") != work["singular"]
        ):
            raise CatalogueError("uncheckpointed raw receipt contradicts request identity")
        read_page(store, relative)
        if context["request_id"] in recovered:
            raise CatalogueError("multiple completed raw receipts claim the same request identity")
        recovered[context["request_id"]] = relative
    return recovered


def capture(
    store: Store,
    *,
    selected: list[str] | None = None,
    resume: str | None = None,
    client_factory: Any = PublicClient,
) -> dict[str, Any]:
    budget = Budget(store)
    state = read_capture(store, resume) if resume else new_capture(store, selected)
    if resume and selected is not None and selected != state["selected"]:
        raise CatalogueError("resume selection differs from capture")
    if state["complete"]:
        return {**state, **budget.result()}
    state["error"] = None
    recovered = _recover_receipts(store, state) if resume else {}
    try:
        with client_factory(store, budget) as client:
            while True:
                _ = budget.remaining
                if state["pending"]:
                    _consume(store, state, state["pending"]["index"], state["pending"]["relative"])
                index = next((i for i, work in enumerate(state["tasks"]) if not work["done"]), None)
                if index is None:
                    if _hydrate(store, state):
                        continue
                    _ = budget.remaining
                    state.update(complete=True, completed_at=now(), error=None)
                    save(store, state)
                    break
                work = state["tasks"][index]
                params = {**work["params"]}
                if work["cursor"]:
                    params["cursor"] = work["cursor"]
                context = _request_context(state, index)
                # Transport commits this request identity before HTTP or response evidence.
                client.request_context = context
                relative = recovered.pop(context["request_id"], None)
                if relative is None:
                    relative = client.get(work["endpoint"], params)
                manifest, _ = read_page(store, relative)
                store.write(
                    relative,
                    {
                        **manifest,
                        **context,
                        "page_id": relative.split("/")[1],
                    },
                )
                state["pages"].append(relative)
                state["pending"] = {"index": index, "relative": relative}
                save(store, state)
                _consume(store, state, index, relative)
    except (CatalogueError, OSError) as exc:
        state["error"] = str(exc)
        save(store, state)
        if not isinstance(exc, BudgetExceeded):
            raise
    return {**state, **budget.result()}


def complete_capture(store: Store, capture_id: str) -> dict[str, Any]:
    state = read_capture(store, capture_id)
    if (
        not state.get("complete")
        or state.get("pending")
        or any(not work["done"] for work in state["tasks"])
    ):
        raise CatalogueError("capture is incomplete; resume before build or publish")
    for _ in capture_pages(store, state):
        pass
    return state


def latest_capture(store: Store, *, selected: bool | None = None) -> str:
    candidates = []
    directory = store.path("captures")
    if directory.exists():
        for child in directory.iterdir():
            state = read_capture(store, child.name)
            if state["complete"] and (
                selected is None or (state["selected"] is not None) == selected
            ):
                candidates.append((state["completed_at"], state["capture_id"]))
    if not candidates:
        raise CatalogueError("no completed capture is available")
    return max(candidates)[1]
