"""Raw-only, non-resumable event partition acquisition for manual diagnostics."""

from __future__ import annotations

import contextvars
import hashlib
import uuid
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from decimal import Decimal
from typing import Any

from kalshi_events import metrics
from kalshi_events.common import (
    Budget,
    BudgetExceeded,
    CatalogueError,
    Store,
    digest,
    json_bytes,
    ticker,
    ticker_path_segment,
)
from kalshi_events.normalize import native_identity, records
from kalshi_events.transport import PublicClient, read_page

MAX_CURSOR_LENGTH = 8192
PAGE_SIZE = 200


def _event_payload_digest(event: dict[str, Any]) -> str:
    """Hash payload values plus the paths where JSON decimals were parsed.

    ``json_bytes`` represents Decimal values as strings for compatibility with
    existing evidence. Including typed paths prevents a Decimal from colliding
    with an identical provider string without retaining a second full payload.
    """
    decimal_paths = hashlib.sha256()
    path: list[tuple[str, str | int]] = []

    def visit(value: Any) -> None:
        if isinstance(value, Decimal):
            decimal_paths.update(json_bytes(path))
        elif isinstance(value, dict):
            for key in sorted(value):
                path.append(("key", key))
                visit(value[key])
                path.pop()
        elif isinstance(value, list):
            for index, item in enumerate(value):
                path.append(("index", index))
                visit(item)
                path.pop()

    visit(event)
    return hashlib.sha256(json_bytes([event, decimal_paths.hexdigest()])).hexdigest()


def _inventory_digest(rows: Counter[tuple[str, str]]) -> str:
    return digest(
        json_bytes(
            [[identity, payload, count] for (identity, payload), count in sorted(rows.items())]
        )
    )


def preflight_collections(
    store: Store, client: PublicClient, budget: Budget, partitions: list[dict[str, str]]
) -> dict[str, str]:
    """Fetch each selected collection definition once and bind its declared series."""
    bindings: dict[str, str] = {}
    diagnostic_id = uuid.uuid4().hex
    for index, partition in enumerate(partitions):
        if partition.get("kind") != "collection":
            continue
        selected = ticker(partition.get("ticker"))
        relative = client.get(
            f"/multivariate_event_collections/{ticker_path_segment(selected)}",
            context={
                "diagnostic_id": diagnostic_id,
                "diagnostic_kind": "collection_definition",
                "diagnostic_partition_index": index,
                "diagnostic_page": 0,
            },
        )
        manifest, payload = read_page(store, relative)
        if manifest.get("status") != 200:
            raise CatalogueError("selected collection definition was not found")
        definition = records(payload, "multivariate_contract", singular=True)[0]
        if ticker(definition.get("collection_ticker")) != selected:
            raise CatalogueError("collection definition ticker does not match selector")
        bindings[selected] = ticker(definition.get("series_ticker"))
    return bindings


def acquire_partitions(
    store: Store,
    client: PublicClient,
    budget: Budget,
    partitions: list[dict[str, str]],
    *,
    collection_series: dict[str, str],
    workers: int,
) -> dict[str, Any]:
    """Fetch selected partitions with fair pagination and main-thread page validation.

    Only compact identity and payload hashes are retained. Every observation, including
    duplicates and corrected payloads, contributes to the returned multiset inventory.
    """
    if not 1 <= workers <= 8:
        raise ValueError("partition workers must be between one and eight")
    if not 2 <= len(partitions) <= 8:
        raise ValueError("partition pilot requires between two and eight partitions")
    states = [
        {
            "partition": partition,
            "cursor": "",
            "seen_cursors": set(),
            "done": False,
            "in_flight": False,
            "page_number": 0,
            "pages": 0,
            "observations": 0,
            "inventory": Counter(),
            "identities": set(),
        }
        for partition in partitions
    ]
    queue: deque[int] = deque(range(len(states)))
    diagnostic_id = uuid.uuid4().hex
    budget_stop: str | None = None

    def request(index: int, page_number: int, cursor: str) -> str:
        state = states[index]
        partition = state["partition"]
        if partition["kind"] == "series":
            endpoint = "/events"
            params: dict[str, Any] = {
                "series_ticker": partition["ticker"],
                "with_nested_markets": False,
                "limit": PAGE_SIZE,
            }
        elif partition["kind"] == "collection":
            endpoint = "/events/multivariate"
            params = {
                "collection_ticker": partition["ticker"],
                "with_nested_markets": False,
                "limit": PAGE_SIZE,
            }
        else:
            raise CatalogueError("invalid diagnostic partition kind")
        if cursor:
            params["cursor"] = cursor
        context = {
            "diagnostic_id": diagnostic_id,
            "diagnostic_kind": "partition_events",
            "diagnostic_partition_index": index,
            "diagnostic_page": page_number,
        }
        return client.get(endpoint, params=params, context=context)

    def validate_page(index: int, relative: str) -> str:
        with (
            metrics.phase("page_validation_application"),
            metrics.thread_cpu_phase("page_processing_cpu"),
        ):
            return _validate_page(index, relative)

    def _validate_page(index: int, relative: str) -> str:
        state = states[index]
        partition = state["partition"]
        manifest, payload = read_page(store, relative)
        if manifest.get("status") != 200 or manifest.get("endpoint") not in {
            "/events",
            "/events/multivariate",
        }:
            raise CatalogueError("partition response was not a successful event page")
        rows = records(payload, "events")
        if len(rows) > PAGE_SIZE:
            raise CatalogueError("event page exceeds the requested page size")
        kind = partition["kind"]
        expected_series = (
            partition["ticker"] if kind == "series" else collection_series.get(partition["ticker"])
        )
        if kind == "collection" and expected_series is None:
            raise CatalogueError("collection definition was not preflighted")
        for event in rows:
            identity, event_series = native_identity("events", event)
            if event_series != expected_series:
                raise CatalogueError("event is outside the selected partition series")
            if kind == "collection":
                for field in ("collection_ticker", "mve_collection_ticker"):
                    if field in event and ticker(event[field]) != partition["ticker"]:
                        raise CatalogueError("event contradicts its selected collection")
            checksum = _event_payload_digest(event)
            state["inventory"][(identity, checksum)] += 1
            state["identities"].add(identity)
            state["observations"] += 1
        metrics.add("diagnostic_observations", len(rows))
        cursor = payload.get("cursor")
        if not isinstance(cursor, str) or len(cursor) > MAX_CURSOR_LENGTH:
            raise CatalogueError("invalid or overlong pagination cursor")
        if cursor and cursor in state["seen_cursors"]:
            raise CatalogueError("repeated pagination cursor")
        if cursor:
            state["seen_cursors"].add(cursor)
        state["pages"] += 1
        metrics.add("diagnostic_verified_pages")
        return cursor

    def submit(executor: ThreadPoolExecutor, index: int) -> Future[str]:
        state = states[index]
        state["in_flight"] = True
        page_number = state["page_number"]
        state["page_number"] += 1
        copied = contextvars.copy_context()
        return executor.submit(copied.run, request, index, page_number, state["cursor"])

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="kalshi-probe") as executor:
        pending: dict[Future[str], int] = {}
        try:
            while queue or pending:
                _ = budget.remaining
                while queue and len(pending) < workers:
                    index = queue.popleft()
                    state = states[index]
                    if state["done"] or state["in_flight"]:
                        continue
                    pending[submit(executor, index)] = index
                if not pending:
                    break
                with metrics.phase("scheduler_wait"):
                    finished, _ = wait(
                        pending, return_when=FIRST_COMPLETED, timeout=budget.remaining
                    )
                if not finished:
                    _ = budget.remaining
                    raise CatalogueError("partition scheduler timed out")
                for future in finished:
                    index = pending.pop(future)
                    state = states[index]
                    state["in_flight"] = False
                    relative = future.result()
                    cursor = validate_page(index, relative)
                    state["cursor"] = cursor
                    if cursor:
                        queue.append(index)
                    else:
                        state["done"] = True
        except BudgetExceeded as exc:
            if str(exc) not in {"request allowance exhausted", "download allowance exhausted"}:
                raise
            budget_stop = str(exc)
            for future in pending:
                future.cancel()
            for future, index in list(pending.items()):
                if future.cancelled():
                    continue
                try:
                    relative = future.result()
                except BudgetExceeded as exc:
                    if str(exc) not in {
                        "request allowance exhausted",
                        "download allowance exhausted",
                    }:
                        raise
                    continue
                cursor = validate_page(index, relative)
                states[index]["cursor"] = cursor
                states[index]["done"] = not bool(cursor)
                pending.pop(future, None)

    identity_partition_count: Counter[str] = Counter()
    partition_reports = []
    for state in states:
        identities = state["identities"]
        identity_partition_count.update(identities)
        partition_reports.append(
            {
                "kind": state["partition"]["kind"],
                "ticker": state["partition"]["ticker"],
                "pages": state["pages"],
                "observations": state["observations"],
                "unique_identities": len(identities),
                "identity_digests": sorted(
                    hashlib.sha256(value.encode()).hexdigest() for value in identities
                ),
                "inventory_sha256": _inventory_digest(state["inventory"]),
                "inventory_rows": sum(state["inventory"].values()),
            }
        )
    return {
        "workers": workers,
        "completed": all(state["done"] for state in states),
        "budget_stop": budget_stop,
        "pages": sum(state["pages"] for state in states),
        "observations": sum(state["observations"] for state in states),
        "overlap_unique_identities": sum(count > 1 for count in identity_partition_count.values()),
        "partitions": partition_reports,
    }
