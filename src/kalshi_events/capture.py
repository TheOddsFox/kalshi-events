"""Resumable traversals: raw evidence commits before checkpoints; traversals share one rate limit.

Each traversal has at most one request in flight. The main thread owns all state mutation and
checkpoint writes; worker threads only perform HTTP through the shared client.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextvars import copy_context
from datetime import datetime, timedelta
from typing import Any

from kalshi_events import metrics
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
from kalshi_events.transport import MAX_CONCURRENT_REQUESTS, PublicClient, read_page

EVENT_PAGE_LIMIT = 200
MARKET_PAGE_LIMIT = 1000
CUTOFF_ENDPOINT = "/historical/cutoff"
HISTORICAL_PREFIX = "/historical/markets"


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
        "restarts": 0,
        "done": False,
        # Committed-but-unapplied receipt path; applied on the next consume or resume.
        "pending": None,
        # Historical reads are preceded by a fresh cutoff read recorded as its own receipt.
        "cutoff_due": _historical(endpoint),
    }


def _historical(endpoint: str) -> bool:
    return endpoint.startswith(HISTORICAL_PREFIX)


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
    if "max_response_bytes" not in state:
        raise CatalogueError("pre-1.0 captures are not readable; a new operator root is required")
    ceiling = state.get("max_response_bytes")
    if isinstance(ceiling, bool) or not isinstance(ceiling, int) or ceiling <= 0:
        raise CatalogueError("invalid sealed response ceiling")
    return state


def save(store: Store, state: dict[str, Any]) -> None:
    """Rewrite the small checkpoint only; page receipts live in the append-only journal."""
    store.write(f"captures/{state['capture_id']}/state.json", state)


def _journal(state: dict[str, Any]) -> str:
    return f"captures/{state['capture_id']}/pages.jsonl"


def page_receipts(store: Store, state: dict[str, Any]) -> list[dict[str, Any]]:
    path = store.path(_journal(state))
    if not path.exists():
        return []
    rows = []
    for line in regular_bytes(path, store.limits.max_root_bytes).splitlines():
        row = decode(line)
        index = row.get("index") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("relative"), str)
            or isinstance(index, bool)
            or not isinstance(index, int)
        ):
            raise CatalogueError("invalid page journal")
        rows.append(row)
    return rows


def capture_pages(store: Store, state: dict[str, Any]) -> Iterator[tuple[dict[str, Any], Any]]:
    for row in page_receipts(store, state):
        store.check_deadline()
        manifest, payload = read_page(store, row["relative"], state["max_response_bytes"])
        if manifest.get("capture_id") != state["capture_id"]:
            raise CatalogueError("raw evidence belongs to another capture")
        yield manifest, payload


def new_capture(store: Store, selected: list[str] | None) -> dict[str, Any]:
    work = [task("/historical/cutoff", None, singular=True)]
    if selected is None:
        work += [
            task("/series", "series"),
            task(
                "/events",
                "events",
                params={"with_nested_markets": "false", "limit": EVENT_PAGE_LIMIT},
            ),
            task(
                "/events/multivariate",
                "events",
                params={"with_nested_markets": "false", "limit": EVENT_PAGE_LIMIT},
            ),
            task("/markets", "markets", params={"limit": MARKET_PAGE_LIMIT}),
            task("/historical/markets", "markets", params={"limit": MARKET_PAGE_LIMIT}),
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
        # Sealed at creation: every later read of this capture uses this ceiling.
        "max_response_bytes": store.limits.max_response_bytes,
        "tasks": work,
        "cutoff": None,
        "coverage": {},
        "error": None,
    }
    save(store, state)
    return state


def _phase(work: dict[str, Any]) -> str:
    return "cutoff" if work["cutoff_due"] else "page"


def _plan(work: dict[str, Any], phase: str) -> tuple[str, dict[str, Any], str | None, bool]:
    if phase == "cutoff":
        return CUTOFF_ENDPOINT, {}, None, True
    params = {**work["params"], **({"cursor": work["cursor"]} if work["cursor"] else {})}
    return work["endpoint"], params, work["kind"], work["singular"]


def _request_context(state: dict[str, Any], index: int) -> dict[str, Any]:
    work = state["tasks"][index]
    phase = _phase(work)
    _, _, kind, singular = _plan(work, phase)
    return {
        "capture_id": state["capture_id"],
        "task_index": index,
        "phase": phase,
        "request_id": digest(
            json_bytes([state["capture_id"], index, phase, work["cursor"], work["restarts"]])
        ),
        "entity_kind": kind,
        "singular": singular,
    }


def _record_cutoff(state: dict[str, Any], payload: Any) -> None:
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


def _apply(
    state: dict[str, Any],
    index: int,
    manifest: dict[str, Any],
    payload: Any,
    seen: dict[int, set[str]],
) -> None:
    work = state["tasks"][index]
    status = manifest["status"]
    if status in {400, 410} and work["cursor"]:
        if work["restarts"] >= 3:
            raise CatalogueError("pagination cursor repeatedly invalidated")
        work.update(cursor="", restarts=work["restarts"] + 1)
        seen.pop(index, None)
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
        _record_cutoff(state, payload)
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
            known = seen.setdefault(index, set())
            fingerprint = digest(cursor.encode()) if cursor else ""
            if cursor and (cursor == work["cursor"] or fingerprint in known):
                raise CatalogueError("pagination cursor repeated without completion")
            if cursor:
                known.add(fingerprint)
            work["cursor"] = cursor
            work["done"] = not cursor
        if work["kind"] == "markets" and work["requested"]:
            state["coverage"][work["requested"]] = {
                "market_ticker": work["requested"],
                "status": "found",
                "error": None,
            }
    work["cutoff_due"] = _historical(work["endpoint"]) and not work["done"]


def _stage(store: Store, state: dict[str, Any], index: int, relative: str) -> None:
    """Commit a raw receipt to its traversal before it is applied: checkpoint, then journal."""
    work = state["tasks"][index]
    if work["pending"] is not None:
        raise CatalogueError("traversal already has an unapplied page")
    work["pending"] = relative
    save(store, state)
    store.append(_journal(state), json_bytes({"index": index, "relative": relative}))


def _consume(
    store: Store,
    state: dict[str, Any],
    index: int,
    relative: str,
    seen: dict[int, set[str]],
) -> None:
    work = state["tasks"][index]
    manifest, payload = read_page(store, relative, state["max_response_bytes"])
    if manifest.get("request_id") != _request_context(state, index)["request_id"]:
        raise CatalogueError("raw receipt does not match its traversal")
    if _phase(work) == "cutoff":
        if manifest["status"] != 200:
            raise CatalogueError(f"historical cutoff read failed: HTTP {manifest['status']}")
        _record_cutoff(state, payload)
        work["cutoff_due"] = False
    else:
        _apply(state, index, manifest, payload, seen)
    work["pending"] = None
    save(store, state)


def _settle(store: Store, state: dict[str, Any], seen: dict[int, set[str]]) -> None:
    """Apply pages whose raw receipts were committed before a previous process stopped."""
    logged = {row["relative"] for row in page_receipts(store, state)}
    index = 0
    while index < len(state["tasks"]):
        pending = state["tasks"][index]["pending"]
        if pending is not None:
            if pending not in logged:
                store.append(_journal(state), json_bytes({"index": index, "relative": pending}))
                logged.add(pending)
            _consume(store, state, index, pending, seen)
        index += 1


def _hydrate(store: Store, state: dict[str, Any]) -> bool:
    with metrics.phase("hydration"):
        return _hydrate_pages(store, state)


def _hydrate_pages(store: Store, state: dict[str, Any]) -> bool:
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


def _recover_receipts(store: Store, state: dict[str, Any]) -> dict[str, str]:
    """Find committed raw receipts that no journal entry or checkpoint references yet."""
    logged = {row["relative"] for row in page_receipts(store, state)}
    recovered: dict[str, str] = {}
    for path in store.path("raw").glob("*/manifest.json"):
        relative = str(path.relative_to(store.root))
        if relative in logged:
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
            raise CatalogueError("uncommitted raw receipt has invalid task identity")
        work = state["tasks"][index]
        if work["done"]:
            continue
        context = _request_context(state, index)
        if manifest.get("request_id") != context["request_id"]:
            continue
        endpoint, params, kind, singular = _plan(work, _phase(work))
        if (
            manifest.get("endpoint") != endpoint
            or manifest.get("params") != params
            or manifest.get("entity_kind") != kind
            or manifest.get("singular") != singular
        ):
            raise CatalogueError("uncommitted raw receipt contradicts request identity")
        read_page(store, relative, state["max_response_bytes"])
        if context["request_id"] in recovered:
            raise CatalogueError("multiple completed raw receipts claim the same request identity")
        recovered[context["request_id"]] = relative
    return recovered


def _run(
    store: Store,
    state: dict[str, Any],
    client: Any,
    pool: ThreadPoolExecutor,
    recovered: dict[str, str],
    seen: dict[int, set[str]],
    budget: Budget,
) -> None:
    running: dict[Future, int] = {}
    busy: set[int] = set()
    failure: Exception | None = None
    while True:
        if failure is None:
            try:
                _ = budget.remaining
                index = 0
                while index < len(state["tasks"]):
                    work = state["tasks"][index]
                    if work["done"] or index in busy:
                        index += 1
                        continue
                    context = _request_context(state, index)
                    relative = recovered.pop(context["request_id"], None)
                    if relative is not None:
                        _stage(store, state, index, relative)
                        _consume(store, state, index, relative, seen)
                        continue
                    endpoint, params, _, _ = _plan(work, _phase(work))
                    busy.add(index)
                    running[
                        pool.submit(
                            copy_context().run,
                            client.get,
                            endpoint,
                            params,
                            context=context,
                        )
                    ] = index
                    index += 1
            except (CatalogueError, OSError) as exc:
                failure = exc
        if not running:
            if failure is not None:
                raise failure
            if _hydrate(store, state):
                continue
            state.update(complete=True, completed_at=now(), error=None)
            save(store, state)
            return
        finished, _ = wait(running, return_when=FIRST_COMPLETED)
        for future in finished:
            index = running.pop(future)
            busy.discard(index)
            try:
                relative = future.result()
            except (CatalogueError, OSError) as exc:
                failure = failure or exc
                continue
            try:
                _stage(store, state, index, relative)
                if failure is None:
                    _consume(store, state, index, relative, seen)
            except (CatalogueError, OSError) as exc:
                failure = failure or exc


def capture(
    store: Store,
    *,
    selected: list[str] | None = None,
    resume: str | None = None,
    client_factory: Any = None,
) -> dict[str, Any]:
    factory = client_factory or PublicClient
    state = read_capture(store, resume) if resume else new_capture(store, selected)
    if resume and selected is not None and selected != state["selected"]:
        raise CatalogueError("resume selection differs from capture")
    budget = Budget(store, max_response_bytes=state["max_response_bytes"])
    if state["complete"]:
        return {**state, **budget.result(), "budget_exhausted": False}
    state["error"] = None
    seen: dict[int, set[str]] = {}
    budget_exhausted = False
    try:
        with (
            factory(store, budget) as client,
            ThreadPoolExecutor(
                max_workers=MAX_CONCURRENT_REQUESTS, thread_name_prefix="kalshi-traversal"
            ) as pool,
        ):
            _settle(store, state, seen)
            recovered = _recover_receipts(store, state) if resume else {}
            _run(store, state, client, pool, recovered, seen, budget)
    except (CatalogueError, OSError) as exc:
        state["error"] = str(exc)
        save(store, state)
        if not isinstance(exc, BudgetExceeded):
            raise
        budget_exhausted = True
    return {**state, **budget.result(), "budget_exhausted": budget_exhausted}


def complete_capture(store: Store, capture_id: str) -> dict[str, Any]:
    state = read_capture(store, capture_id)
    if (
        not state.get("complete")
        or any(work["pending"] is not None for work in state["tasks"])
        or any(not work["done"] for work in state["tasks"])
    ):
        raise CatalogueError("capture is incomplete; resume before build or publish")
    for _ in capture_pages(store, state):
        pass
    return state


def _completed(store: Store, *, catalogue: bool | None) -> list[tuple[str, str]]:
    directory = store.path("captures")
    found = []
    if directory.exists():
        for child in directory.iterdir():
            state = read_capture(store, child.name)
            if state["complete"] and (
                catalogue is None or (state["selected"] is None) == catalogue
            ):
                found.append((state["completed_at"], state["capture_id"]))
    return found


def latest_catalogue_capture(store: Store) -> str:
    """Newest complete non-targeted capture. Targeted metadata captures are never eligible."""
    candidates = _completed(store, catalogue=True)
    if not candidates:
        raise CatalogueError("no complete catalogue capture is available")
    return max(candidates)[1]


def latest_complete_capture(store: Store) -> str:
    """Newest complete capture of any kind; used only to export targeted metadata."""
    candidates = _completed(store, catalogue=None)
    if not candidates:
        raise CatalogueError("no completed capture is available")
    return max(candidates)[1]
