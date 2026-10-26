"""Resumable traversals: raw evidence commits before checkpoints; traversals share one rate limit.

Each traversal has at most one request in flight. The main thread owns all state mutation and
checkpoint writes; worker threads only perform HTTP through the shared client.
"""

from __future__ import annotations

import copy
import hashlib
import os
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextvars import copy_context
from datetime import datetime, timedelta
from itertools import chain
from typing import Any

import httpx

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
    path_is_link,
    regular_bytes,
    selections,
    ticker,
    validate_series_scope,
)
from kalshi_events.normalize import DiskParentIndex, ParentIndex, native_identity, records
from kalshi_events.transport import MAX_CONCURRENT_REQUESTS, PublicClient, read_page, source_url

EVENT_PAGE_LIMIT = 200
MARKET_PAGE_LIMIT = 1000
CUTOFF_ENDPOINT = "/historical/cutoff"
HISTORICAL_PREFIX = "/historical/markets"
JOURNAL_VERSION = 2
ACQUISITION_PROTOCOL = 3
SCOPED_ACQUISITION_PROTOCOL = 4
CURRENT_ACQUISITION_PROTOCOL = 5
_PROTOCOL5_POLICIES = {
    "rate_policy": "fixed",
    "parent_policy": "batched-v1",
    "cutoff_policy": "checkpoint-v1",
}
_LEGACY_PROTOCOL5_POLICIES = {
    "parent_policy": {"singular-v1", "batched-v1"},
    "cutoff_policy": {"per-page-v1", "checkpoint-v1"},
}
_CHECKPOINT_TASK_FIELDS = {
    "cutoff_ordinal",
    "cutoff_reason",
    "pages_since_cutoff",
    "last_cutoff_at",
    "data_done",
}
_PARENT_BATCH_FIELDS = {"parent_batch", "batch_found"}
MAX_SELECTION_BATCH = 100
MAX_INITIAL_BATCH_URL_BYTES = 6144
MAX_JOURNAL_RECORD_BYTES = 16 * 1024**2
_GENESIS = "0" * 64
_SERIES_TASK_SCOPES = {
    "1": "series",
    "2": "events_live",
    "3": "markets_live",
    "4": "markets_history",
}


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


class _CaptureState(dict[str, Any]):
    """Logical capture state with non-serialized, operation-local recovery metadata."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.journal_head: dict[str, Any] | None = None
        self.seen_cursors: dict[int, set[str]] = {}
        self.page_rows: list[dict[str, Any]] = []
        self.processed: set[str] = set()
        self.request_ids: set[str] = set()
        self.journal_uncertain = False
        self.snapshot_seq: int | None = None
        self.journal_anchor_state_sha256: str | None = None
        self.recent_page: tuple[str, dict[str, Any], Any] | None = None
        self.verified_journal_sha256: str | None = None
        self.verified_snapshot_sha256: str | None = None
        self.cutoff_monotonic: dict[int, float] = {}
        self.task_identities = {_task_identity(work) for work in self.get("tasks", [])}


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


def _protocol_task(
    state: dict[str, Any],
    endpoint: str,
    kind: str | None,
    *,
    singular: bool = False,
    params: dict[str, Any] | None = None,
    requested: str | None = None,
    parent_batch: str | None = None,
) -> dict[str, Any]:
    """Create a task with only the fields enabled by its sealed protocol-5 policies."""
    work = task(endpoint, kind, singular=singular, params=params, requested=requested)
    if (
        _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL
        and state.get("cutoff_policy") == "checkpoint-v1"
        and _historical(endpoint)
        and not singular
    ):
        initial_cutoff = _historical(endpoint) and not singular
        work.update(
            cutoff_ordinal=1 if initial_cutoff else 0,
            cutoff_reason="start" if initial_cutoff else None,
            pages_since_cutoff=0,
            last_cutoff_at=None,
            data_done=False,
        )
    if parent_batch is not None:
        work.update(parent_batch=parent_batch, batch_found=[])
    return work


def _checkpoint_policy(state: dict[str, Any]) -> bool:
    return (
        _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL
        and state.get("cutoff_policy") == "checkpoint-v1"
    )


def _batched_parent_policy(state: dict[str, Any]) -> bool:
    return (
        _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL
        and state.get("parent_policy") == "batched-v1"
    )


def _historical(endpoint: str) -> bool:
    return endpoint.startswith(HISTORICAL_PREFIX)


def read_capture(
    store: Store,
    capture_id: str,
    *,
    _writer_resume: bool = False,
    _parent_index: ParentIndex | None = None,
) -> dict[str, Any]:
    if (
        not isinstance(capture_id, str)
        or len(capture_id) != 32
        or any(c not in "0123456789abcdef" for c in capture_id)
    ):
        raise CatalogueError("invalid capture identity")
    state_path = store.path(f"captures/{capture_id}/state.json")
    if not state_path.exists():
        # A durable genesis record is authoritative if the process died before its first snapshot.
        if _parent_index is not None and not _writer_resume:
            raise CatalogueError("operation-local parent index requires writer resume")
        return _read_v2(
            store,
            capture_id,
            missing_snapshot=True,
            repair_tail=_writer_resume,
            parent_index=_parent_index,
        )
    snapshot_bytes = regular_bytes(state_path, store.limits.max_root_bytes)
    state = decode(snapshot_bytes)
    if isinstance(state, dict) and state.get("version") == JOURNAL_VERSION:
        if _parent_index is not None and not _writer_resume:
            raise CatalogueError("operation-local parent index requires writer resume")
        recovered = _read_v2(
            store,
            capture_id,
            envelope=state,
            repair_tail=_writer_resume,
            parent_index=_parent_index,
        )
        recovered.verified_snapshot_sha256 = digest(snapshot_bytes)
        return recovered
    if state.get("capture_id") != capture_id or state.get("version") != 1:
        raise CatalogueError("invalid capture state")
    _acquisition_protocol(state)
    _validate_capture_root_scope(store, state)
    if "max_response_bytes" not in state:
        raise CatalogueError("pre-1.0 captures are not readable; a new operator root is required")
    ceiling = state.get("max_response_bytes")
    if isinstance(ceiling, bool) or not isinstance(ceiling, int) or ceiling <= 0:
        raise CatalogueError("invalid sealed response ceiling")
    decoded_ceiling = state.get("max_decoded_response_bytes", ceiling)
    if (
        isinstance(decoded_ceiling, bool)
        or not isinstance(decoded_ceiling, int)
        or decoded_ceiling <= 0
    ):
        raise CatalogueError("invalid sealed decoded response ceiling")
    encoding = state.get("compression", "identity")
    if encoding not in {"identity", "gzip"}:
        raise CatalogueError("invalid sealed response compression")
    return state


def save(store: Store, state: dict[str, Any]) -> None:
    """Rewrite the small checkpoint only; page receipts live in the append-only journal."""
    if state.get("journal_version") == JOURNAL_VERSION:
        _save_v2(store, state)
        return
    store.write(f"captures/{state['capture_id']}/state.json", state)


def _journal(state: dict[str, Any]) -> str:
    return f"captures/{state['capture_id']}/pages.jsonl"


def page_receipts(store: Store, state: dict[str, Any]) -> list[dict[str, Any]]:
    if state.get("journal_version") == JOURNAL_VERSION:
        if isinstance(state, _CaptureState):
            return [dict(row) for row in state.page_rows]
        return _read_v2(store, state["capture_id"]).page_rows
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


def _seen_value(seen: dict[int, set[str]]) -> dict[str, list[str]]:
    return {str(index): sorted(values) for index, values in sorted(seen.items())}


def _seen_parse(value: Any) -> dict[int, set[str]]:
    if not isinstance(value, dict):
        raise CatalogueError("invalid journal cursor checkpoint")
    result: dict[int, set[str]] = {}
    for key, rows in value.items():
        if not isinstance(key, str) or not key.isdigit() or not isinstance(rows, list):
            raise CatalogueError("invalid journal cursor checkpoint")
        index = int(key)
        if any(not isinstance(row, str) for row in rows) or len(rows) != len(set(rows)):
            raise CatalogueError("invalid journal cursor checkpoint")
        result[index] = set(rows)
    return result


def _state_digest(state: dict[str, Any], seen: dict[int, set[str]]) -> str:
    return digest(json_bytes({"state": dict(state), "seen_cursors": _seen_value(seen)}))


def _task_identity(work: dict[str, Any]) -> str:
    return digest(
        json_bytes(
            [
                work.get("endpoint"),
                work.get("params"),
                work.get("kind"),
                work.get("singular"),
                work.get("requested"),
            ]
        )
    )


def _market_batch_url_size(values: list[str]) -> int:
    params = {"limit": MARKET_PAGE_LIMIT, "tickers": ",".join(values)}
    return len(
        str(httpx.Request("GET", source_url() + "/markets", params=params).url).encode("ascii")
    )


def _batch_tickers(work: dict[str, Any]) -> list[str] | None:
    if work.get("endpoint") != "/markets":
        return None
    params = work.get("params")
    if not isinstance(params, dict):
        raise CatalogueError("invalid selected market batch parameters")
    if "tickers" not in params:
        return None
    value = params.get("tickers")
    if not isinstance(value, str):
        raise CatalogueError("invalid selected market batch")
    values = value.split(",")
    if len(values) > MAX_SELECTION_BATCH or ",".join(selections(values)) != value:
        raise CatalogueError("invalid selected market batch")
    if work.get("singular") or work.get("kind") != "markets" or work.get("requested") is not None:
        raise CatalogueError("invalid selected market batch traversal")
    if set(params) != {"limit", "tickers"} or params.get("limit") != MARKET_PAGE_LIMIT:
        raise CatalogueError("invalid selected market batch parameters")
    return values


def _acquisition_protocol(state: dict[str, Any]) -> int:
    if "acquisition_protocol" not in state:
        if "scope" in state:
            raise CatalogueError("scoped capture lacks its acquisition protocol")
        if any(key in state for key in (*_PROTOCOL5_POLICIES, "acquisition_policy_sha256")):
            raise CatalogueError("acquisition policies require protocol 5")
        return 2
    protocol = state["acquisition_protocol"]
    if (
        isinstance(protocol, bool)
        or not isinstance(protocol, int)
        or protocol
        not in {ACQUISITION_PROTOCOL, SCOPED_ACQUISITION_PROTOCOL, CURRENT_ACQUISITION_PROTOCOL}
    ):
        raise CatalogueError("unsupported acquisition protocol")
    if protocol == SCOPED_ACQUISITION_PROTOCOL:
        if "scope" not in state:
            raise CatalogueError("unsupported acquisition protocol without a series scope")
        validate_series_scope(state.get("scope"))
    if protocol == CURRENT_ACQUISITION_PROTOCOL:
        if "scope" in state:
            validate_series_scope(state.get("scope"))
        _protocol5_policies(state)
    elif any(key in state for key in (*_PROTOCOL5_POLICIES, "acquisition_policy_sha256")):
        raise CatalogueError("acquisition policies require protocol 5")
    if (
        protocol != CURRENT_ACQUISITION_PROTOCOL
        and "scope" in state
        and protocol != SCOPED_ACQUISITION_PROTOCOL
    ):
        raise CatalogueError("series scope requires acquisition protocol 4 or 5")
    return protocol


def _protocol5_policies(state: dict[str, Any]) -> dict[str, str]:
    values = {key: state.get(key) for key in _PROTOCOL5_POLICIES}
    if (
        not isinstance(values["rate_policy"], str)
        or values["rate_policy"] not in {"fixed", "decrease-on-429"}
        or any(
            not isinstance(values[key], str) or values[key] not in allowed
            for key, allowed in _LEGACY_PROTOCOL5_POLICIES.items()
        )
    ):
        raise CatalogueError("unsupported protocol 5 acquisition policy")
    expected = digest(json_bytes(values))
    if state.get("acquisition_policy_sha256") != expected:
        raise CatalogueError("protocol 5 acquisition policy checksum mismatch")
    return values


def _is_scoped_capture(state: dict[str, Any]) -> bool:
    protocol = _acquisition_protocol(state)
    return protocol == SCOPED_ACQUISITION_PROTOCOL or (
        protocol == CURRENT_ACQUISITION_PROTOCOL and "scope" in state
    )


def _series_tasks(series: str) -> list[dict[str, Any]]:
    return [
        task(CUTOFF_ENDPOINT, None, singular=True),
        task(f"/series/{series}", "series", singular=True),
        task(
            "/events",
            "events",
            params={
                "series_ticker": series,
                "with_nested_markets": "false",
                "limit": EVENT_PAGE_LIMIT,
            },
        ),
        task(
            "/markets",
            "markets",
            params={"series_ticker": series, "mve_filter": "exclude", "limit": MARKET_PAGE_LIMIT},
        ),
        task(
            HISTORICAL_PREFIX,
            "markets",
            params={"series_ticker": series, "limit": MARKET_PAGE_LIMIT},
        ),
    ]


def _validate_series_task_plan(state: dict[str, Any]) -> None:
    """Validate the fixed scoped traversals plus only authoritative event hydration tasks."""
    scope = validate_series_scope(state.get("scope"))
    series = scope["series_ticker"]
    if state.get("selected") is not None:
        raise CatalogueError("scoped captures must use the full series acquisition plan")
    if state.get("incremental") is True:
        pin = state.get("baseline")
        if (
            not isinstance(pin, dict)
            or pin.get("catalogue_version") != 2
            or pin.get("release_contract") != "oddsfox.kalshi.series.v1"
            or validate_series_scope(pin.get("series_scope")) != scope
            or pin.get("root_id") != state.get("root_id")
            or "query_from" in state
            or not isinstance(state.get("watermark_started_at"), str)
            or not isinstance(state.get("previous_watermark"), str)
        ):
            raise CatalogueError("invalid scoped incremental baseline")
    elif any(key in state for key in ("baseline", "query_from", "watermark_started_at")):
        raise CatalogueError("scoped series capture has unsupported incremental settings")
    expected = _series_tasks(series)
    tasks = state["tasks"]
    if len(tasks) < len(expected):
        raise CatalogueError("scoped capture is missing a required series traversal")
    for actual, sealed in zip(tasks[: len(expected)], expected, strict=True):
        if any(
            actual.get(key) != sealed[key]
            for key in ("endpoint", "kind", "singular", "params", "requested")
        ):
            raise CatalogueError("scoped capture traversal differs from its sealed series plan")
    if state.get("task_scopes") != _SERIES_TASK_SCOPES:
        raise CatalogueError("scoped capture has invalid reconciliation scopes")
    parent_tickers: set[str] = set()
    for work in tasks[len(expected) :]:
        endpoint = work.get("endpoint", "")
        prefix = "/events/"
        if work.get("parent_batch") is not None:
            params = work.get("params", {})
            values = params.get("tickers", "").split(",") if isinstance(params, dict) else []
            if (
                not _batched_parent_policy(state)
                or work.get("parent_batch") != "ordinary"
                or endpoint != "/events"
                or work.get("kind") != "events"
                or work.get("singular")
                or params.get("limit") != EVENT_PAGE_LIMIT
                or params.get("with_nested_markets") != "false"
                or not values
                or len(values) > MAX_SELECTION_BATCH
                or len(values) != len(set(values))
                or work.get("requested") is not None
                or work.get("cutoff_due")
            ):
                raise CatalogueError("scoped capture contains an invalid event parent batch")
            for value in values:
                ticker(value)
            continue
        if (
            work.get("kind") != "events"
            or not work.get("singular")
            or work.get("params") != {"exclude_markets": "true"}
            or work.get("requested") is not None
            or not endpoint.startswith(prefix)
            or work.get("cutoff_due")
        ):
            raise CatalogueError("scoped capture contains a non-parent traversal")
        parent = ticker(endpoint[len(prefix) :])
        if parent in parent_tickers:
            raise CatalogueError("scoped capture duplicates a parent lookup")
        parent_tickers.add(parent)


def _validate_capture_root_scope(store: Store, state: dict[str, Any]) -> None:
    scope = state.get("scope")
    if scope is not None:
        scope = validate_series_scope(scope)
    if scope != store.scope:
        raise CatalogueError("capture scope does not match the operator root")
    if scope is not None:
        root_identity = state.get("root_id")
        if not _is_root_id(root_identity):
            raise CatalogueError("scoped capture lacks a logical operator root identity")
        from kalshi_events.incremental import root_id

        if root_identity != root_id(store):
            raise CatalogueError("capture belongs to a different logical operator root")


def _is_root_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(char in "0123456789abcdef" for char in value)
    )


def _journal_checksum(record: dict[str, Any]) -> str:
    return digest(json_bytes({key: value for key, value in record.items() if key != "sha256"}))


def _validate_logical_state(state: Any, capture_id: str) -> dict[str, Any]:
    if (
        not isinstance(state, dict)
        or state.get("version") != 1
        or state.get("journal_version") != JOURNAL_VERSION
        or state.get("capture_id") != capture_id
        or not isinstance(state.get("tasks"), list)
        or not isinstance(state.get("coverage"), dict)
    ):
        raise CatalogueError("invalid journal capture state")
    _acquisition_protocol(state)
    if _is_scoped_capture(state) and (
        state.get("selected") is not None or not _is_root_id(state.get("root_id"))
    ):
        raise CatalogueError("invalid scoped capture settings")
    identities: set[str] = set()
    batch_values: list[str] = []
    has_batches = False
    for work in state["tasks"]:
        if not isinstance(work, dict) or not isinstance(work.get("endpoint"), str):
            raise CatalogueError("invalid journal traversal state")
        expected_task_fields = {
            "endpoint",
            "kind",
            "singular",
            "params",
            "requested",
            "cursor",
            "restarts",
            "done",
            "pending",
            "cutoff_due",
        }
        if (
            _checkpoint_policy(state)
            and _historical(work.get("endpoint", ""))
            and not work.get("singular")
        ):
            expected_task_fields |= _CHECKPOINT_TASK_FIELDS
        if not expected_task_fields.issubset(work) or not set(work).issubset(
            expected_task_fields | _PARENT_BATCH_FIELDS
        ):
            raise CatalogueError("invalid journal traversal state")
        if (
            not isinstance(work.get("endpoint"), str)
            or not work["endpoint"].startswith("/")
            or _task_identity(work) in identities
            or work.get("kind") not in {"events", "series", "markets", None}
            or not isinstance(work.get("singular"), bool)
            or not isinstance(work.get("params"), dict)
            or (work.get("requested") is not None and not isinstance(work["requested"], str))
            or not isinstance(work.get("cursor"), str)
            or len(work["cursor"]) > 8192
            or isinstance(work.get("restarts"), bool)
            or not isinstance(work.get("restarts"), int)
            or work["restarts"] < 0
            or not isinstance(work.get("done"), bool)
            or work.get("pending") is not None
            or not isinstance(work.get("cutoff_due"), bool)
        ):
            raise CatalogueError("invalid journal traversal state")
        has_checkpoint = (
            _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]
        )
        if has_checkpoint and (
            isinstance(work.get("cutoff_ordinal"), bool)
            or not isinstance(work.get("cutoff_ordinal"), int)
            or work["cutoff_ordinal"] < 0
            or work.get("cutoff_reason")
            not in {None, "start", "resume", "cursor-reset", "pages", "interval", "terminal"}
            or isinstance(work.get("pages_since_cutoff"), bool)
            or not isinstance(work.get("pages_since_cutoff"), int)
            or work["pages_since_cutoff"] < 0
            or (
                work.get("last_cutoff_at") is not None
                and not isinstance(work["last_cutoff_at"], str)
            )
            or not isinstance(work.get("data_done"), bool)
        ):
            raise CatalogueError("invalid historical cutoff checkpoint state")
        if has_checkpoint and (
            (
                work["cutoff_reason"] is not None
                and (not work["cutoff_due"] or work["cutoff_ordinal"] < 1)
            )
            or (
                _historical(work["endpoint"])
                and not work["singular"]
                and work["cutoff_due"]
                and work["cutoff_reason"] is None
            )
            or (work["data_done"] and not (work["done"] or work["cutoff_due"]))
        ):
            raise CatalogueError("contradictory historical cutoff checkpoint state")
        if "parent_batch" in work and (
            not _batched_parent_policy(state)
            or work.get("parent_batch") not in {"ordinary", "multivariate"}
            or not isinstance(work.get("batch_found"), list)
            or any(not isinstance(value, str) for value in work["batch_found"])
            or len(work["batch_found"]) > MAX_SELECTION_BATCH
            or len(work["batch_found"]) != len(set(work["batch_found"]))
        ):
            raise CatalogueError("invalid parent batch traversal state")
        if ("parent_batch" in work) != ("batch_found" in work):
            raise CatalogueError("incomplete parent batch traversal state")
        if "parent_batch" in work:
            endpoint = "/events" if work["parent_batch"] == "ordinary" else "/events/multivariate"
            params = work.get("params")
            if (
                work.get("endpoint") != endpoint
                or work.get("kind") != "events"
                or work.get("singular") is not False
                or work.get("requested") is not None
                or not isinstance(params, dict)
                or set(params) != {"tickers", "limit", "with_nested_markets"}
                or params.get("limit") != EVENT_PAGE_LIMIT
                or params.get("with_nested_markets") != "false"
                or not isinstance(params.get("tickers"), str)
            ):
                raise CatalogueError("invalid parent batch request")
            selected_events = params["tickers"].split(",")
            if (
                not selected_events
                or len(selected_events) < 20
                or len(selected_events) > MAX_SELECTION_BATCH
                or len(selected_events) != len(set(selected_events))
                or _event_batch_url_size(
                    selected_events, multivariate=work["parent_batch"] == "multivariate"
                )
                > MAX_INITIAL_BATCH_URL_BYTES
                or any(value not in selected_events for value in work["batch_found"])
            ):
                raise CatalogueError("invalid parent batch selection")
            for value in selected_events:
                ticker(value)
        batch = _batch_tickers(work)
        if batch is not None:
            has_batches = True
            if (
                _acquisition_protocol(state)
                not in {
                    ACQUISITION_PROTOCOL,
                    SCOPED_ACQUISITION_PROTOCOL,
                    CURRENT_ACQUISITION_PROTOCOL,
                }
                or not isinstance(state.get("selected"), list)
                or any(not isinstance(value, str) for value in state["selected"])
                or any(value not in state["selected"] for value in batch)
            ):
                raise CatalogueError("selected market batch differs from its capture selection")
            batch_values.extend(batch)
        identities.add(_task_identity(work))
    if has_batches and (
        len(batch_values) != len(set(batch_values)) or set(batch_values) != set(state["selected"])
    ):
        raise CatalogueError("selected market batches do not partition the capture selection")
    if _is_scoped_capture(state):
        _validate_series_task_plan(state)
    # Reuse legacy sealed-limit validation without serializing runtime metadata.
    if "max_response_bytes" not in state:
        raise CatalogueError("invalid sealed response ceiling")
    ceiling = state["max_response_bytes"]
    decoded_ceiling = state.get("max_decoded_response_bytes", ceiling)
    if (
        isinstance(ceiling, bool)
        or not isinstance(ceiling, int)
        or ceiling <= 0
        or isinstance(decoded_ceiling, bool)
        or not isinstance(decoded_ceiling, int)
        or decoded_ceiling <= 0
        or state.get("compression", "identity") not in {"identity", "gzip"}
    ):
        raise CatalogueError("invalid sealed response settings")
    if state.get("incremental", False):
        pin = state.get("baseline")
        is_scoped = _is_scoped_capture(state)
        if (
            not isinstance(pin, dict)
            or pin.get("catalogue_version") not in {1, 2}
            or not all(
                isinstance(pin.get(key), str)
                for key in ("source_release_id", "manifest_sha256", "capture_id", "root_id")
            )
            or not isinstance(state.get("task_scopes"), dict)
            or (
                not is_scoped
                and (
                    not isinstance(state.get("query_from"), int)
                    or isinstance(state.get("query_from"), bool)
                )
            )
        ):
            raise CatalogueError("invalid sealed incremental baseline")
    scopes = state.get("task_scopes", {})
    if not isinstance(scopes, dict):
        raise CatalogueError("invalid sealed reconciliation scopes")
    valid_scopes = {
        "series",
        "events_live",
        "events_combo",
        "markets_live",
        "markets_combo",
        "markets_history",
    }
    for key, scope in scopes.items():
        if (
            not isinstance(key, str)
            or not key.isdigit()
            or int(key) >= len(state["tasks"])
            or scope not in valid_scopes
        ):
            raise CatalogueError("invalid sealed reconciliation scope")
        work = state["tasks"][int(key)]
        endpoint = work["endpoint"]
        expected = {
            "series": endpoint
            == (
                f"/series/{state['scope']['series_ticker']}"
                if _is_scoped_capture(state)
                else "/series"
            ),
            "events_live": endpoint == "/events",
            "events_combo": endpoint == "/events/multivariate",
            "markets_live": endpoint == "/markets" and work["params"].get("mve_filter") != "only",
            "markets_combo": endpoint == "/markets" and work["params"].get("mve_filter") == "only",
            "markets_history": endpoint == "/historical/markets",
        }[scope]
        singular_series = _is_scoped_capture(state) and scope == "series" and work["singular"]
        if not expected or (work["singular"] and not singular_series):
            raise CatalogueError("reconciliation scope does not match its traversal")
        if _batch_tickers(work) is not None or work.get("parent_batch") is not None:
            raise CatalogueError("selected market batches cannot define a reconciliation scope")
    return state


def _validate_control_transition(
    genesis: dict[str, Any],
    candidate: dict[str, Any],
    kind: str,
    known_tasks: dict[str, dict[str, Any]],
) -> None:
    immutable = (
        "capture_id",
        "selected",
        "started_at",
        "max_response_bytes",
        "max_decoded_response_bytes",
        "compression",
        "journal_version",
        "acquisition_protocol",
        "rate_policy",
        "parent_policy",
        "cutoff_policy",
        "acquisition_policy_sha256",
        "scope",
        "incremental",
        "baseline",
        "root_id",
        "watermark_started_at",
        "previous_watermark",
        "query_from",
        "task_scopes",
    )
    if any(candidate.get(key) != genesis.get(key) for key in immutable):
        raise CatalogueError("journal control altered sealed capture settings")
    task_map = {_task_identity(work): work for work in candidate["tasks"] if isinstance(work, dict)}
    if len(task_map) != len(candidate["tasks"]) or not set(known_tasks).issubset(task_map):
        raise CatalogueError("journal control removed or duplicated a traversal")
    if _is_scoped_capture(candidate):
        _validate_series_task_plan(candidate)
    for identity, old in known_tasks.items():
        new = task_map[identity]
        for key in ("endpoint", "kind", "singular", "params", "requested"):
            if new.get(key) != old.get(key):
                raise CatalogueError("journal control altered traversal identity")
    if kind == "hydrate":
        if candidate.get("complete") is not False:
            raise CatalogueError("hydration control cannot complete a capture")
        for identity, work in task_map.items():
            known_tasks.setdefault(identity, copy.deepcopy(work))
        return
    if kind == "complete":
        if candidate.get("complete") is not True or not isinstance(
            candidate.get("completed_at"), str
        ):
            raise CatalogueError("invalid completion control")
        if any(
            not work.get("done") or work.get("pending") is not None for work in candidate["tasks"]
        ):
            raise CatalogueError("completion control has unfinished traversals")
        return
    if kind == "stop":
        if candidate.get("complete") is not False or not isinstance(candidate.get("error"), str):
            raise CatalogueError("invalid stop control")
        return
    if kind == "clear_error":
        if candidate.get("complete") is not False or candidate.get("error") is not None:
            raise CatalogueError("invalid error-clear control")
        return
    if kind == "cutoff_schedule":
        if candidate.get("complete") is not False or not _checkpoint_policy(candidate):
            raise CatalogueError("invalid cutoff scheduling control")
        scheduled = [
            work
            for work in candidate["tasks"]
            if work.get("cutoff_due") and work.get("cutoff_reason") in {"resume", "interval"}
        ]
        if not scheduled:
            raise CatalogueError("invalid cutoff scheduling transition")
        return
    raise CatalogueError("unknown journal control transition")


def _apply_control_record(
    state: dict[str, Any],
    seen: dict[int, set[str]],
    genesis: dict[str, Any],
    payload: dict[str, Any],
    known_tasks: dict[str, dict[str, Any]],
    *,
    replay: bool,
) -> None:
    kind = payload.get("kind")
    if kind == "hydrate":
        if set(payload) != {"kind", "tasks", "coverage", "state_sha256"}:
            raise CatalogueError("invalid hydration control fields")
        additions = payload["tasks"]
        coverage = payload["coverage"]
        if (
            not isinstance(additions, list)
            or len(additions) > 128
            or not isinstance(coverage, dict)
            or len(coverage) > 128
        ):
            raise CatalogueError("hydration control exceeds its bounded delta")
        identities: set[str] = set()
        for work in additions:
            expected_fields = {
                "endpoint",
                "kind",
                "singular",
                "params",
                "requested",
                "cursor",
                "restarts",
                "done",
                "pending",
                "cutoff_due",
            }
            if (
                _checkpoint_policy(genesis)
                and isinstance(work, dict)
                and _historical(work.get("endpoint", ""))
                and not work.get("singular")
            ):
                expected_fields |= _CHECKPOINT_TASK_FIELDS
            if (
                not isinstance(work, dict)
                or not expected_fields.issubset(work)
                or not set(work).issubset(expected_fields | _PARENT_BATCH_FIELDS)
                or not isinstance(work.get("endpoint"), str)
                or not isinstance(work.get("params"), dict)
                or not isinstance(work.get("singular"), bool)
                or work.get("cursor") != ""
                or work.get("restarts") != 0
                or work.get("done") is not False
                or work.get("pending") is not None
            ):
                raise CatalogueError("invalid hydration task")
            endpoint = work["endpoint"]
            if "parent_batch" in work:
                _validate_logical_state(
                    {
                        **genesis,
                        "tasks": [*genesis["tasks"], work],
                        "task_scopes": genesis.get("task_scopes", {}),
                    },
                    genesis["capture_id"],
                )
                identity = _task_identity(work)
                if identity in known_tasks or identity in identities:
                    raise CatalogueError("hydration control duplicates a traversal")
                identities.add(identity)
                continue
            identity = _task_identity(work)
            expected_prefix = "/events/" if work.get("kind") == "events" else "/series/"
            expected_params = (
                {"exclude_markets": "true"}
                if work.get("kind") == "events"
                and _acquisition_protocol(genesis)
                in {ACQUISITION_PROTOCOL, SCOPED_ACQUISITION_PROTOCOL, CURRENT_ACQUISITION_PROTOCOL}
                else {}
            )
            if (
                work.get("kind") not in {"events", "series"}
                or not endpoint.startswith(expected_prefix)
                or not work["singular"]
                or work["params"] != expected_params
                or work["requested"] is not None
                or not isinstance(work.get("cutoff_due"), bool)
                or work["cutoff_due"]
            ):
                raise CatalogueError("invalid hydration task state")
            ticker(endpoint[len(expected_prefix) :])
            if identity in known_tasks or identity in identities:
                raise CatalogueError("hydration control duplicates a traversal")
            identities.add(identity)
        if not _is_sha256(payload.get("state_sha256")):
            raise CatalogueError("invalid hydration state checksum")
        for key, item in coverage.items():
            if (
                not isinstance(key, str)
                or not isinstance(item, dict)
                or item.get("market_ticker") != key
                or item.get("status") not in {"found", "absent", "unresolved"}
            ):
                raise CatalogueError("invalid hydration coverage entry")
        if replay:
            with metrics.phase("control_state_copy"):
                candidate = copy.deepcopy(state)
                candidate["tasks"].extend(copy.deepcopy(additions))
                candidate["coverage"].update(copy.deepcopy(coverage))
            _validate_control_transition(genesis, candidate, kind, known_tasks)
            if payload.get("state_sha256") != _state_digest(candidate, seen):
                raise CatalogueError("hydration control state checksum mismatch")
            state.clear()
            state.update(candidate)
            _track_added_task_identities(state, additions)
        else:
            for work in additions:
                known_tasks[_task_identity(work)] = copy.deepcopy(work)
        return
    if kind == "complete":
        if set(payload) != {"kind", "completed_at", "state_sha256"}:
            raise CatalogueError("invalid completion control fields")
        if not isinstance(payload.get("completed_at"), str):
            raise CatalogueError("invalid completion timestamp")
        if not _is_sha256(payload.get("state_sha256")):
            raise CatalogueError("invalid completion state checksum")
        if replay:
            with metrics.phase("control_state_copy"):
                candidate = copy.deepcopy(state)
            candidate.update(complete=True, completed_at=payload["completed_at"], error=None)
            _validate_control_transition(genesis, candidate, kind, known_tasks)
            if payload.get("state_sha256") != _state_digest(candidate, seen):
                raise CatalogueError("completion control state checksum mismatch")
            state.clear()
            state.update(candidate)
        elif not isinstance(payload.get("state_sha256"), str):
            raise CatalogueError("invalid completion state checksum")
        return
    if kind == "cutoff_schedule":
        if set(payload) != {"kind", "index", "reason", "ordinal", "state_sha256"}:
            raise CatalogueError("invalid cutoff scheduling control fields")
        index = payload.get("index")
        reason = payload.get("reason")
        ordinal = payload.get("ordinal")
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < len(state["tasks"])
            or reason not in {"resume", "interval"}
            or isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
        ):
            raise CatalogueError("invalid cutoff scheduling control")
        if replay:
            with metrics.phase("control_state_copy"):
                candidate = copy.deepcopy(state)
            work = candidate["tasks"][index]
            if (
                not _historical(work["endpoint"])
                or work["singular"]
                or work["done"]
                or work["cutoff_due"]
                or work["cutoff_ordinal"] + 1 != ordinal
            ):
                raise CatalogueError("cutoff scheduling does not follow task state")
            work.update(cutoff_due=True, cutoff_reason=reason, cutoff_ordinal=ordinal)
            _validate_control_transition(genesis, candidate, kind, known_tasks)
            if payload.get("state_sha256") != _state_digest(candidate, seen):
                raise CatalogueError("cutoff scheduling state checksum mismatch")
            state.clear()
            state.update(candidate)
        elif not _is_sha256(payload.get("state_sha256")):
            raise CatalogueError("invalid cutoff scheduling checksum")
        return
    if kind in {"stop", "clear_error"}:
        expected = (
            {"kind", "reason", "state_sha256"} if kind == "stop" else {"kind", "state_sha256"}
        )
        if set(payload) != expected:
            raise CatalogueError("invalid stop control fields")
        if kind == "stop" and (
            not isinstance(payload.get("reason"), str) or len(payload["reason"]) > 4096
        ):
            raise CatalogueError("invalid stop reason")
        if replay:
            with metrics.phase("control_state_copy"):
                candidate = copy.deepcopy(state)
            candidate["error"] = payload["reason"] if kind == "stop" else None
            _validate_control_transition(genesis, candidate, kind, known_tasks)
            if payload.get("state_sha256") != _state_digest(candidate, seen):
                raise CatalogueError("stop control state checksum mismatch")
            state.clear()
            state.update(candidate)
        elif not _is_sha256(payload.get("state_sha256")):
            raise CatalogueError("invalid stop control checksum")
        return
    raise CatalogueError("unknown journal control transition")


def _journal_records(
    store: Store, capture_id: str, *, repair_tail: bool = False
) -> tuple[Iterator[tuple[dict[str, Any], int]], dict[str, Any]]:
    if repair_tail and not store.writer_locked_by_current_thread:
        raise CatalogueError("journal repair requires the canonical writer lock")
    path = store.path(f"captures/{capture_id}/pages.jsonl")
    if not path.exists():
        return iter(()), {"valid_end": 0, "torn": False}
    if path_is_link(path) or not path.is_file():
        raise CatalogueError("journal is not a regular file")
    total = path.stat().st_size
    if total > store.limits.max_root_bytes:
        raise CatalogueError("journal exceeds operator root limit")
    scan: dict[str, Any] = {"valid_end": 0, "torn": False, "sha256": None}

    def iterate() -> Iterator[tuple[dict[str, Any], int]]:
        valid_end = 0
        expected_seq = 1
        previous = _GENESIS
        committed_hash = hashlib.sha256()
        with path.open("rb") as stream:
            while True:
                store.check_deadline()
                start = valid_end
                raw = stream.readline(MAX_JOURNAL_RECORD_BYTES + 1)
                if not raw:
                    break
                store.check_deadline()
                if start + len(raw) > store.limits.max_root_bytes:
                    raise CatalogueError("journal grew beyond operator root limit")
                if len(raw) > MAX_JOURNAL_RECORD_BYTES:
                    raise CatalogueError("journal record exceeds the record limit")
                if not raw.endswith(b"\n"):
                    scan["torn"] = True
                    break
                record = decode(raw)
                if not isinstance(record, dict):
                    raise CatalogueError("invalid journal record")
                if json_bytes(record) != raw:
                    raise CatalogueError("journal record is not canonical JSON")
                checksum = record.get("sha256")
                seq = record.get("seq")
                if (
                    record.get("version") != JOURNAL_VERSION
                    or set(record)
                    != {"version", "capture_id", "seq", "prev", "op", "payload", "sha256"}
                    or record.get("capture_id") != capture_id
                    or isinstance(seq, bool)
                    or not isinstance(seq, int)
                    or seq != expected_seq
                    or record.get("prev") != previous
                    or not _is_sha256(checksum)
                    or checksum != _journal_checksum(record)
                    or record.get("op") not in {"start", "page", "control", "checkpoint"}
                    or not isinstance(record.get("payload"), dict)
                ):
                    raise CatalogueError("journal sequence, identity or checksum is invalid")
                previous = checksum
                expected_seq += 1
                valid_end = start + len(raw)
                with metrics.phase("journal_hashing"):
                    committed_hash.update(raw)
                metrics.add("hash_bytes", len(raw))
                scan["sha256"] = committed_hash.hexdigest()
                scan["valid_end"] = valid_end
                yield record, valid_end
        if scan["torn"] and repair_tail:
            with path.open("r+b") as stream:
                stream.truncate(valid_end)
                stream.flush()
                with metrics.phase("file_sync"):
                    os.fsync(stream.fileno())
            metrics.add("fsync_calls")
            store.sync(f"captures/{capture_id}/pages.jsonl")

    return iterate(), scan


def _read_v2(
    store: Store,
    capture_id: str,
    *,
    envelope: dict[str, Any] | None = None,
    missing_snapshot: bool = False,
    repair_tail: bool = False,
    parent_index: ParentIndex | None = None,
) -> _CaptureState:
    records, scan = _journal_records(store, capture_id, repair_tail=repair_tail)
    first_item = next(records, None)
    if first_item is None or first_item[0].get("op") != "start":
        raise CatalogueError("version 2 capture has no durable journal genesis")
    first, first_end = first_item
    start_payload = first["payload"]
    if set(start_payload) != {"state", "seen_cursors", "state_sha256"}:
        raise CatalogueError("invalid journal genesis fields")
    base = _validate_logical_state(start_payload.get("state"), capture_id)
    _validate_capture_root_scope(store, base)
    initial_seen = _seen_parse(start_payload.get("seen_cursors"))
    initial_digest = _state_digest(base, initial_seen)
    if start_payload.get("state_sha256") != initial_digest:
        raise CatalogueError("journal genesis state checksum mismatch")
    state = _CaptureState(copy.deepcopy(base))
    seen = copy.deepcopy(initial_seen)
    page_rows: list[dict[str, Any]] = []
    processed: set[str] = set()
    transition_count = 0
    seen_request_ids: set[str] = set()
    seen_relatives: set[str] = set()
    known_tasks = {_task_identity(work): work for work in base["tasks"]}
    complete_seen = False
    anchor_key = (1, first_end, first["sha256"], 0)
    if envelope is not None:
        expected_fields = {
            "version",
            "capture_id",
            "state",
            "state_sha256",
            "snapshot_sha256",
            "_journal",
            "seen_cursors",
        }
        if set(envelope) != expected_fields or envelope.get("capture_id") != capture_id:
            raise CatalogueError("invalid version 2 capture snapshot")
        snapshot_checksum = envelope.get("snapshot_sha256")
        snapshot_base = {key: value for key, value in envelope.items() if key != "snapshot_sha256"}
        if (
            not _is_sha256(snapshot_checksum)
            or digest(json_bytes(snapshot_base)) != snapshot_checksum
        ):
            raise CatalogueError("capture snapshot checksum mismatch")
        snapshot_state = _validate_logical_state(envelope.get("state"), capture_id)
        snapshot_seen = _seen_parse(envelope.get("seen_cursors"))
        if envelope.get("state_sha256") != _state_digest(snapshot_state, snapshot_seen):
            raise CatalogueError("capture snapshot state checksum mismatch")
        for key in (
            "max_response_bytes",
            "max_decoded_response_bytes",
            "compression",
            "started_at",
            "selected",
            "acquisition_protocol",
            "scope",
            "root_id",
            "rate_policy",
            "parent_policy",
            "cutoff_policy",
            "acquisition_policy_sha256",
        ):
            if snapshot_state.get(key) != base.get(key):
                raise CatalogueError("capture snapshot settings differ from journal genesis")
        journal_anchor = envelope.get("_journal")
        if not isinstance(journal_anchor, dict) or set(journal_anchor) != {
            "seq",
            "offset",
            "sha256",
            "transition_count",
        }:
            raise CatalogueError("invalid capture snapshot journal anchor")
        anchor_key = (
            journal_anchor.get("seq"),
            journal_anchor.get("offset"),
            journal_anchor.get("sha256"),
            journal_anchor.get("transition_count"),
        )
        if (
            isinstance(anchor_key[0], bool)
            or not isinstance(anchor_key[0], int)
            or isinstance(anchor_key[1], bool)
            or not isinstance(anchor_key[1], int)
            or anchor_key[1] <= 0
            or not isinstance(anchor_key[2], str)
            or not _is_sha256(anchor_key[2])
            or isinstance(anchor_key[3], bool)
            or not isinstance(anchor_key[3], int)
            or anchor_key[3] < 0
        ):
            raise CatalogueError("invalid capture snapshot journal anchor")
        if snapshot_state.get("journal_version") != JOURNAL_VERSION:
            raise CatalogueError("invalid capture snapshot version")
        state = _CaptureState(copy.deepcopy(snapshot_state))
        seen = copy.deepcopy(snapshot_seen)
    else:
        envelope = None

    matched_anchor = envelope is None
    chain_seq = 0
    chain_hash = _GENESIS
    offset = 0
    anchor_transition = 0
    tail_record = first
    for record, end_offset in chain((first_item,), records):
        tail_record = record
        chain_seq = record["seq"]
        chain_hash = record["sha256"]
        offset = end_offset
        op = record["op"]
        payload = record["payload"]
        if op == "start":
            if record["seq"] != 1:
                raise CatalogueError("journal genesis is not the first record")
        elif op == "checkpoint":
            if set(payload) != {"transition_count", "state_sha256"}:
                raise CatalogueError("invalid checkpoint fields")
            count = payload.get("transition_count")
            digest_value = payload.get("state_sha256")
            check_state = envelope is None or record["seq"] >= anchor_key[0]
            if (
                isinstance(count, bool)
                or not isinstance(count, int)
                or count != transition_count
                or not _is_sha256(digest_value)
            ):
                raise CatalogueError("journal checkpoint state does not match replay")
            if check_state and digest_value != _state_digest(state, seen):
                raise CatalogueError("journal checkpoint state does not match replay")
        elif op == "page":
            if complete_seen:
                raise CatalogueError("journal contains a page after capture completion")
            if set(payload) != {"index", "relative", "request_id"}:
                raise CatalogueError("invalid journal page fields")
            index = payload.get("index")
            relative = payload.get("relative")
            request_id = payload.get("request_id")
            if (
                isinstance(index, bool)
                or not isinstance(index, int)
                or not isinstance(relative, str)
                or not relative.startswith("raw/")
                or not _is_sha256(request_id)
            ):
                raise CatalogueError("invalid journal page reference")
            store.path(relative)
            if request_id in seen_request_ids or relative in seen_relatives:
                raise CatalogueError("journal contains a duplicate page receipt")
            seen_request_ids.add(request_id)
            seen_relatives.add(relative)
            row = {"index": index, "relative": relative, "request_id": request_id}
            page_rows.append(row)
            if envelope is None or record["seq"] > anchor_key[0]:
                manifest, page_payload = _replay_page(
                    store, state, seen, index, relative, request_id
                )
                if parent_index is not None:
                    parent_index.add_page(manifest, page_payload)
                    parent_index.processed.add(relative)
            processed.add(relative)
            transition_count += 1
        elif op == "control":
            if complete_seen:
                raise CatalogueError("journal contains a control after capture completion")
            _apply_control_record(
                state,
                seen,
                base,
                payload,
                known_tasks,
                replay=envelope is None or record["seq"] > anchor_key[0],
            )
            transition_count += 1
            if payload["kind"] == "complete":
                complete_seen = True
        elif op != "checkpoint":
            raise CatalogueError("unknown journal operation")
        if envelope is not None and (chain_seq, offset, chain_hash, transition_count) == anchor_key:
            if op not in {"start", "checkpoint"}:
                raise CatalogueError("snapshot anchor is not a start or checkpoint")
            if _state_digest(state, seen) != envelope["state_sha256"]:
                raise CatalogueError("snapshot differs from its journal anchor")
            matched_anchor = True
            anchor_transition = transition_count
            # Snapshot state is trusted only after full chain validation below; suffix replay starts after it.
        if envelope is not None and chain_seq < anchor_key[0]:
            # Page rows are indexed from the full prefix; logical state is replaced by the snapshot at anchor.
            continue
    if envelope is not None and not matched_anchor:
        raise CatalogueError("capture snapshot journal anchor is missing")
    if envelope is not None and anchor_key[1] > scan["valid_end"]:
        raise CatalogueError("capture snapshot points beyond the committed journal")
    if envelope is not None and chain_seq < anchor_key[0]:
        raise CatalogueError("capture snapshot is ahead of the durable journal")
    if envelope is not None and anchor_transition != anchor_key[3]:
        raise CatalogueError("capture snapshot transition count differs from anchor")
    if scan.get("sha256") is not None:
        metrics.add("hashes")
    # When the snapshot anchored at a checkpoint, state/seen already represent that boundary.
    # The loop above replayed suffix records only; ensure checkpoints in that suffix validate against them.
    state = _CaptureState(state)
    state.seen_cursors = seen
    state.page_rows = page_rows
    state.processed = processed
    state.request_ids = seen_request_ids
    state.journal_head = {
        "seq": chain_seq,
        "offset": offset,
        "sha256": chain_hash,
        "transition_count": transition_count,
    }
    state.journal_uncertain = False
    state.snapshot_seq = anchor_key[0] if envelope is not None else None
    state.journal_anchor_state_sha256 = None
    state.verified_journal_sha256 = scan.get("sha256")
    # The tail itself is a save point only when it is genesis or a checkpoint.
    if chain_seq and tail_record["op"] in {"start", "checkpoint"}:
        state.journal_anchor_state_sha256 = tail_record["payload"]["state_sha256"]
    return state


def _replay_page(
    store: Store,
    state: dict[str, Any],
    seen: dict[int, set[str]],
    index: int,
    relative: str,
    request_id: str,
) -> tuple[dict[str, Any], Any]:
    with metrics.phase("page_validation"), metrics.phase("transition_prepare"):
        manifest, payload, change = _read_page_change(
            store, state, seen, index, relative, request_id
        )
    metrics.add("page_transitions_prepared")
    with metrics.phase("page_application"), metrics.phase("transition_apply"):
        _apply_page_change(state, seen, index, change)
    metrics.add("page_transitions_applied")
    return manifest, payload


def _read_page_change(
    store: Store,
    state: dict[str, Any],
    seen: dict[int, set[str]],
    index: int,
    relative: str,
    request_id: str,
) -> tuple[dict[str, Any], Any, dict[str, Any]]:
    if state.get("complete"):
        raise CatalogueError("journal contains a page after capture completion")
    if not 0 <= index < len(state["tasks"]):
        raise CatalogueError("journal page task index is invalid")
    manifest, payload = read_page(
        store,
        relative,
        state["max_response_bytes"],
        state.get("max_decoded_response_bytes", state["max_response_bytes"]),
    )
    if manifest.get("capture_id") != state["capture_id"]:
        raise CatalogueError("raw evidence belongs to another capture")
    context = _request_context(state, index)
    if (
        manifest.get("request_id") != request_id
        or request_id != context["request_id"]
        or not _receipt_matches_context(manifest, context)
    ):
        raise CatalogueError("journal receipt does not match its traversal")
    return manifest, payload, _derive_page_change(state, seen, index, manifest, payload)


def _derive_page_change(
    state: dict[str, Any],
    seen: dict[int, set[str]],
    index: int,
    manifest: dict[str, Any],
    payload: Any,
) -> dict[str, Any]:
    """Validate a receipt and derive only the state changes for its traversal."""
    work = state["tasks"][index]
    updated = dict(work)
    change: dict[str, Any] = {
        "task": updated,
        "seen_clear": False,
        "seen_touch": False,
        "seen_add": None,
        "coverage_remove": (),
        "coverage": {},
        "tasks": [],
        "cutoff": None,
        "cursor_reset": False,
        "cutoff_checkpoint": False,
    }
    if _phase(work) == "cutoff":
        if manifest.get("status") != 200:
            raise CatalogueError("historical cutoff receipt is invalid")
        change["cutoff"] = _validated_cutoff(payload)
        if _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]:
            updated.update(
                cutoff_due=False,
                cutoff_reason=None,
                pages_since_cutoff=0,
                last_cutoff_at=manifest.get("received_at"),
            )
            if updated.get("data_done"):
                updated["done"] = True
            change["cutoff_checkpoint"] = True
        else:
            updated["cutoff_due"] = False
        return change

    status = manifest["status"]
    scoped = _is_scoped_capture(state)
    if scoped and status == 404 and work["kind"] == "series":
        raise CatalogueError("the scoped series definition was not found")
    if status in {400, 410} and work["cursor"]:
        if work["restarts"] >= 3:
            raise CatalogueError("pagination cursor repeatedly invalidated")
        updated.update(cursor="", restarts=work["restarts"] + 1)
        change["seen_clear"] = True
        batch = _batch_tickers(work)
        if batch is not None:
            change["coverage_remove"] = tuple(batch)
        if work.get("parent_batch") is not None:
            updated["batch_found"] = []
        if _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]:
            updated.update(
                cutoff_due=True,
                cutoff_reason="cursor-reset",
                cutoff_ordinal=work["cutoff_ordinal"] + 1,
                pages_since_cutoff=0,
            )
            change["cursor_reset"] = True
    elif status == 404 and work["singular"] and work["kind"] in {"markets", "events", "series"}:
        updated["done"] = True
        if work["kind"] == "markets" and work["requested"]:
            selected = work["requested"]
            if work["endpoint"].startswith("/markets/"):
                change["tasks"].append(
                    task(
                        f"/historical/markets/{selected}",
                        "markets",
                        singular=True,
                        requested=selected,
                    )
                )
            else:
                change["coverage"][selected] = {
                    "market_ticker": selected,
                    "status": "absent",
                    "error": "not returned by live or historical market lookup",
                }
    elif status != 200:
        raise CatalogueError(f"incomplete source traversal: HTTP {status}")
    elif work["kind"] is None:
        change["cutoff"] = _validated_cutoff(payload)
        updated["done"] = True
    else:
        envelope = {
            "series": "series",
            "events": "event" if work["singular"] else "events",
            "markets": "market" if work["singular"] else "markets",
        }[work["kind"]]
        rows = records(payload, envelope, singular=work["singular"])
        parent_batch = work.get("parent_batch")
        parent_batch_values = (
            work["params"].get("tickers", "").split(",") if parent_batch is not None else None
        )
        if parent_batch_values is not None:
            allowed_parents = set(parent_batch_values)
            for row in rows:
                identity, parent_series = native_identity("events", row)
                if identity not in allowed_parents:
                    raise CatalogueError("event parent batch returned an unrequested ticker")
                if parent_batch == "ordinary" and (
                    row.get("mve_collection_ticker") or row.get("mve_selected_legs")
                ):
                    raise CatalogueError("ordinary event parent batch returned a combo event")
                if _is_scoped_capture(state) and parent_series != state["scope"]["series_ticker"]:
                    raise CatalogueError("event parent batch returned an out-of-scope series")
        batch = _batch_tickers(work)
        batch_found: set[str] = set()
        if batch is not None:
            allowed = set(batch)
            for row in rows:
                value = ticker(row.get("ticker"))
                if value not in allowed:
                    raise CatalogueError("market batch returned an unrequested ticker")
                batch_found.add(value)
        if work["singular"]:
            key = "event_ticker" if work["kind"] == "events" else "ticker"
            if rows[0].get(key) != work["endpoint"].rsplit("/", 1)[1]:
                raise CatalogueError("direct lookup returned a different native identity")
            updated["done"] = True
        elif work["endpoint"] == "/series":
            updated["done"] = True
        else:
            cursor = payload.get("cursor")
            if not isinstance(cursor, str) or len(cursor) > 8192:
                raise CatalogueError("missing or malformed pagination cursor")
            known = seen.get(index, set())
            fingerprint = digest(cursor.encode()) if cursor else ""
            if cursor and (cursor == work["cursor"] or fingerprint in known):
                raise CatalogueError("pagination cursor repeated without completion")
            if cursor:
                change["seen_add"] = fingerprint
            change["seen_touch"] = True
            updated["cursor"] = cursor
            updated["done"] = not cursor
        if parent_batch_values is not None:
            found = set(work.get("batch_found", [])) | {
                ticker(row.get("event_ticker")) for row in rows
            }
            updated["batch_found"] = sorted(found)
            if updated["done"]:
                requested = set(parent_batch_values)
                missing = requested - found
                if parent_batch == "ordinary":
                    multivariate = multivariate_parent_plan(state, missing)
                    change["tasks"].extend(multivariate)
                    scheduled = {
                        value
                        for addition in multivariate
                        if addition.get("parent_batch") == "multivariate"
                        for value in addition["params"]["tickers"].split(",")
                    }
                    scheduled.update(
                        addition["endpoint"].rsplit("/", 1)[-1]
                        for addition in multivariate
                        if addition.get("singular") and addition["endpoint"].startswith("/events/")
                    )
                    for value in sorted(missing - scheduled):
                        change["tasks"].append(
                            _protocol_task(
                                state,
                                f"/events/{value}",
                                "events",
                                singular=True,
                                params={"exclude_markets": "true"},
                            )
                        )
                else:
                    for value in sorted(missing):
                        change["tasks"].append(
                            _protocol_task(
                                state,
                                f"/events/{value}",
                                "events",
                                singular=True,
                                params={"exclude_markets": "true"},
                            )
                        )
        if work["kind"] == "markets" and work["requested"]:
            change["coverage"][work["requested"]] = {
                "market_ticker": work["requested"],
                "status": "found",
                "error": None,
            }
        if batch is not None:
            for value in batch_found:
                change["coverage"][value] = {
                    "market_ticker": value,
                    "status": "found",
                    "error": None,
                }
            if updated["done"]:
                identities = _task_identity_set(state)
                added_identities: set[str] = set()
                for value in batch:
                    if (
                        change["coverage"]
                        .get(value, state["coverage"].get(value, {}))
                        .get("status")
                        == "found"
                    ):
                        continue
                    lookup = task(f"/markets/{value}", "markets", singular=True, requested=value)
                    identity = _task_identity(lookup)
                    if identity not in identities and identity not in added_identities:
                        change["tasks"].append(lookup)
                        added_identities.add(identity)
    if change["cursor_reset"]:
        pass
    elif _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]:
        if updated.get("data_done"):
            updated["done"] = True
            updated["cutoff_due"] = False
            updated["cutoff_reason"] = None
        else:
            pages = work["pages_since_cutoff"] + 1
            updated["pages_since_cutoff"] = pages
            if updated["done"]:
                updated.update(
                    data_done=True,
                    done=False,
                    cutoff_due=True,
                    cutoff_reason="terminal",
                    cutoff_ordinal=work["cutoff_ordinal"] + 1,
                )
            elif pages >= 100:
                updated.update(
                    cutoff_due=True,
                    cutoff_reason="pages",
                    cutoff_ordinal=work["cutoff_ordinal"] + 1,
                    pages_since_cutoff=0,
                )
            else:
                updated["cutoff_due"] = False
                updated["cutoff_reason"] = None
    elif _checkpoint_policy(state) and _phase(work) == "cutoff":
        updated["cutoff_due"] = _historical(work["endpoint"])
    else:
        updated["cutoff_due"] = _historical(work["endpoint"]) and not updated["done"]
    return change


def _apply_page_change(
    state: dict[str, Any], seen: dict[int, set[str]], index: int, change: dict[str, Any]
) -> None:
    # Keep the traversal object stable for legacy pending-page consumers.
    work = state["tasks"][index]
    work.update(change["task"])
    if change["seen_clear"]:
        seen.pop(index, None)
    elif change["seen_touch"]:
        known = seen.setdefault(index, set())
        if change["seen_add"] is not None:
            known.add(change["seen_add"])
    for key in change["coverage_remove"]:
        state["coverage"].pop(key, None)
    state["coverage"].update(change["coverage"])
    if change["cutoff"] is not None:
        state["cutoff"] = change["cutoff"]
    if change.get("cutoff_checkpoint") and isinstance(state, _CaptureState):
        state.cutoff_monotonic[index] = time.monotonic()
    if change["tasks"]:
        identities = _task_identity_set(state)
        for addition in change["tasks"]:
            identity = _task_identity(addition)
            if identity in identities:
                continue
            state["tasks"].append(addition)
            identities.add(identity)


def _validated_cutoff(payload: Any) -> dict[str, Any]:
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
    return dict(payload)


def _save_v2(store: Store, state: dict[str, Any]) -> None:
    if not isinstance(state, _CaptureState) or state.journal_head is None:
        raise CatalogueError("version 2 capture has no applied journal head")
    seen = state.seen_cursors
    digest_value = _state_digest(state, seen)
    head = state.journal_head
    anchor_digest = state.journal_anchor_state_sha256
    if anchor_digest != digest_value:
        raise CatalogueError("refusing to save a snapshot ahead of its journal")
    envelope = {
        "version": JOURNAL_VERSION,
        "capture_id": state["capture_id"],
        "state": dict(state),
        "state_sha256": digest_value,
        "_journal": dict(head),
        "seen_cursors": _seen_value(seen),
    }
    envelope["snapshot_sha256"] = digest(json_bytes(envelope))
    store.write(f"captures/{state['capture_id']}/state.json", envelope)
    state.snapshot_seq = head["seq"]
    state.journal_anchor_state_sha256 = anchor_digest


def _append_v2_record(store: Store, state: _CaptureState, op: str, payload: dict[str, Any]) -> None:
    if state.journal_uncertain:
        raise CatalogueError("cannot append to an uncertain journal")
    head = state.journal_head
    seq = 1 if head is None else head["seq"] + 1
    previous = _GENESIS if head is None else head["sha256"]
    record = {
        "version": JOURNAL_VERSION,
        "capture_id": state["capture_id"],
        "seq": seq,
        "prev": previous,
        "op": op,
        "payload": payload,
    }
    record["sha256"] = _journal_checksum(record)
    encoded = json_bytes(record)
    if len(encoded) > MAX_JOURNAL_RECORD_BYTES:
        raise CatalogueError("journal record exceeds the record limit")
    try:
        store.append(_journal(state), encoded)
    except BaseException:
        state.journal_uncertain = True
        raise
    offset = len(encoded) if head is None else head["offset"] + len(encoded)
    if head is not None:
        head.update(seq=seq, offset=offset, sha256=record["sha256"])
    else:
        state.journal_head = {
            "seq": seq,
            "offset": offset,
            "sha256": record["sha256"],
            "transition_count": 0,
        }
    if op in {"start", "checkpoint"}:
        state.journal_anchor_state_sha256 = payload["state_sha256"]
    else:
        state.journal_anchor_state_sha256 = None
    state.journal_uncertain = False


def _initialize_v2(store: Store, raw_state: dict[str, Any]) -> _CaptureState:
    state = _CaptureState(raw_state)
    state.seen_cursors = {}
    initial_digest = _state_digest(state, state.seen_cursors)
    _append_v2_record(
        store,
        state,
        "start",
        {"state": dict(state), "seen_cursors": {}, "state_sha256": initial_digest},
    )
    # The initial genesis record is itself the sequence-zero-transition snapshot anchor.
    _save_v2(store, state)
    return state


def _checkpoint_v2(store: Store, state: _CaptureState) -> None:
    if state.journal_uncertain:
        raise CatalogueError("cannot checkpoint an uncertain journal append")
    count = state.journal_head["transition_count"]
    state_hash = _state_digest(state, state.seen_cursors)
    _append_v2_record(
        store,
        state,
        "checkpoint",
        {"transition_count": count, "state_sha256": state_hash},
    )
    state.journal_head["transition_count"] = count
    _save_v2(store, state)


def _after_transition(store: Store, state: _CaptureState) -> None:
    state.journal_head["transition_count"] += 1
    if state.journal_head["transition_count"] % 100 == 0:
        _checkpoint_v2(store, state)


def _commit_control(
    store: Store, state: _CaptureState, kind: str, candidate: dict[str, Any]
) -> None:
    if kind == "hydrate":
        known = {_task_identity(work): work for work in state["tasks"]}
        _validate_control_transition(state, candidate, kind, known)
        current_identities = {_task_identity(work) for work in state["tasks"]}
        additions = [
            work for work in candidate["tasks"] if _task_identity(work) not in current_identities
        ]
        coverage = {
            key: value
            for key, value in candidate["coverage"].items()
            if state["coverage"].get(key) != value
        }
        for start in range(0, max(len(additions), len(coverage)), 128):
            tasks_part = additions[start : start + 128]
            coverage_items = list(coverage.items())[start : start + 128]
            _commit_control_delta(
                store,
                state,
                {
                    "kind": "hydrate",
                    "tasks": tasks_part,
                    "coverage": dict(coverage_items),
                },
            )
        return
    if kind == "complete":
        _commit_control_delta(
            store,
            state,
            {"kind": "complete", "completed_at": candidate["completed_at"]},
        )
        return
    if kind == "stop":
        _commit_control_delta(store, state, {"kind": "stop", "reason": candidate["error"]})
        return
    if kind == "clear_error":
        _commit_control_delta(store, state, {"kind": "clear_error"})
        return
    raise CatalogueError("unsupported journal control")


def _commit_control_delta(store: Store, state: _CaptureState, delta: dict[str, Any]) -> None:
    if state.get("complete"):
        raise CatalogueError("capture cannot transition after completion")
    with metrics.phase("control_state_copy"):
        candidate = copy.deepcopy(dict(state))
    kind = delta["kind"]
    if kind == "hydrate":
        candidate["tasks"].extend(copy.deepcopy(delta["tasks"]))
        candidate["coverage"].update(copy.deepcopy(delta["coverage"]))
    elif kind == "complete":
        candidate.update(complete=True, completed_at=delta["completed_at"], error=None)
    elif kind == "stop":
        reason = delta.get("reason")
        if not isinstance(reason, str) or not reason or len(reason) > 4096:
            raise CatalogueError("invalid stop reason")
        candidate["error"] = reason
    elif kind == "clear_error":
        if not candidate.get("error"):
            raise CatalogueError("cannot clear a capture without a recorded error")
        candidate["error"] = None
    elif kind == "cutoff_schedule":
        index = delta.get("index")
        reason = delta.get("reason")
        ordinal = delta.get("ordinal")
        if (
            set(delta) != {"kind", "index", "reason", "ordinal"}
            or isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < len(candidate["tasks"])
            or reason not in {"resume", "interval"}
            or isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
        ):
            raise CatalogueError("invalid cutoff scheduling delta")
        work = candidate["tasks"][index]
        if (
            not _checkpoint_policy(candidate)
            or not _historical(work["endpoint"])
            or work["singular"]
            or work["done"]
            or work["cutoff_due"]
            or work["cutoff_ordinal"] + 1 != ordinal
        ):
            raise CatalogueError("cutoff scheduling does not follow task state")
        work.update(cutoff_due=True, cutoff_reason=reason, cutoff_ordinal=ordinal)
    else:
        raise CatalogueError("unsupported journal control")
    known = {_task_identity(work): work for work in state["tasks"]}
    _validate_control_transition(state, candidate, kind, known)
    payload = {
        **delta,
        "state_sha256": _state_digest(candidate, state.seen_cursors),
    }
    _append_v2_record(store, state, "control", payload)
    try:
        state.clear()
        state.update(candidate)
        if kind == "hydrate":
            _track_added_task_identities(state, delta["tasks"])
        _after_transition(store, state)
    except BaseException:
        # A durable control record may have been applied only partially. Recovery
        # must replay it before another transition can be appended.
        state.journal_uncertain = True
        raise


def _commit_page_v2(
    store: Store,
    state: _CaptureState,
    index: int,
    relative: str,
    request_id: str,
    identities: ParentIndex,
) -> tuple[dict[str, Any], Any]:
    with metrics.thread_cpu_phase("page_processing_cpu"):
        return _commit_page_v2_measured(store, state, index, relative, request_id, identities)


def _commit_page_v2_measured(
    store: Store,
    state: _CaptureState,
    index: int,
    relative: str,
    request_id: str,
    identities: ParentIndex,
) -> tuple[dict[str, Any], Any]:
    if state.get("complete"):
        raise CatalogueError("capture cannot accept pages after completion")
    if relative in state.processed or request_id in state.request_ids:
        raise CatalogueError("page receipt is already committed")
    with metrics.phase("page_validation"), metrics.phase("transition_prepare"):
        manifest, payload, change = _read_page_change(
            store, state, state.seen_cursors, index, relative, request_id
        )
    metrics.add("page_transitions_prepared")
    page_index = ParentIndex()
    with metrics.phase("page_identity_validation"):
        page_index.add_page(manifest, payload)
    _append_v2_record(
        store,
        state,
        "page",
        {"index": index, "relative": relative, "request_id": request_id},
    )
    try:
        with metrics.phase("page_application"), metrics.phase("transition_apply"):
            _apply_page_change(state, state.seen_cursors, index, change)
            with metrics.phase("parent_index_merge"):
                _merge_parent_index(identities, page_index)
            state.page_rows.append({"index": index, "relative": relative, "request_id": request_id})
            state.processed.add(relative)
            state.request_ids.add(request_id)
            identities.processed.add(relative)
            metrics.endpoint_page(manifest["endpoint"])
            _after_transition(store, state)
        metrics.add("page_transitions_applied")
    except BaseException:
        # The page record is durable; recovery must decide whether it was applied.
        state.journal_uncertain = True
        raise
    return manifest, payload


def _merge_parent_index(target: ParentIndex, source: ParentIndex) -> None:
    if isinstance(target, DiskParentIndex):
        target.merge(source)
        return
    for name in ("series", "events", "markets"):
        existing = getattr(target, name)
        for identity, winner in getattr(source, name).items():
            ParentIndex._put(existing, identity, winner)
    target.parent_lookups.update(source.parent_lookups)
    for key, candidates in source.parent_batch_candidates.items():
        existing_candidates = target.parent_batch_candidates.setdefault(key, {})
        for identity, candidate in candidates.items():
            previous = existing_candidates.get(identity)
            if previous is None or candidate[0][:2] > previous[0][:2]:
                existing_candidates[identity] = candidate
    target.parent_batch_terminal.update(source.parent_batch_terminal)
    for scope, generations in source.scope_membership.items():
        target_generations = target.scope_membership.setdefault(scope, {})
        for generation, identities in generations.items():
            target_generations.setdefault(generation, set()).update(identities)


def capture_pages(
    store: Store,
    state: dict[str, Any],
    *,
    evidence: dict[str, str] | None = None,
) -> Iterator[tuple[dict[str, Any], Any]]:
    for row in page_receipts(store, state):
        store.check_deadline()
        manifest, payload = read_page(
            store,
            row["relative"],
            state["max_response_bytes"],
            state.get("max_decoded_response_bytes", state["max_response_bytes"]),
            inventory=evidence,
        )
        if manifest.get("capture_id") != state["capture_id"]:
            raise CatalogueError("raw evidence belongs to another capture")
        if not _receipt_matches_state_protocol(manifest, state):
            raise CatalogueError("raw receipt protocol differs from its capture")
        if _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL and (
            _batched_parent_policy(state) or _checkpoint_policy(state)
        ):
            _validate_receipt_task_binding(manifest, state, row)
        yield manifest, payload


def _validate_receipt_task_binding(
    manifest: dict[str, Any], state: dict[str, Any], row: dict[str, Any]
) -> None:
    """Rebind protocol-5 receipt context to its sealed task when rereading evidence."""
    index = row.get("index")
    if (
        isinstance(index, bool)
        or not isinstance(index, int)
        or not 0 <= index < len(state["tasks"])
    ):
        raise CatalogueError("raw receipt references an invalid capture task")
    work = state["tasks"][index]
    if manifest.get("capture_id") != state["capture_id"]:
        raise CatalogueError("raw receipt belongs to another capture")
    if (
        isinstance(manifest.get("task_index"), bool)
        or not isinstance(manifest.get("task_index"), int)
        or manifest.get("task_index") != index
    ):
        raise CatalogueError("raw receipt task index differs from its journal entry")
    if manifest.get("request_id") != row.get("request_id"):
        raise CatalogueError("raw receipt request ID differs from its journal entry")

    phase = manifest.get("phase")
    if phase not in {"page", "cutoff"}:
        raise CatalogueError("raw receipt has an invalid task phase")
    checkpoint_task = (
        _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]
    )
    if phase == "cutoff" and not checkpoint_task and not _historical(work["endpoint"]):
        raise CatalogueError("raw cutoff receipt does not match a historical task")

    # Reconstruct the request's cursor and generation from immutable receipt context.
    # This verifies the request ID against the task identity even after the live task
    # has advanced to a later cursor or pagination generation.
    candidate = dict(state)
    request_work = dict(work)
    params = manifest.get("params")
    if not isinstance(params, dict):
        raise CatalogueError("raw receipt request parameters are malformed")
    if phase == "page":
        if manifest.get("endpoint") != work["endpoint"]:
            raise CatalogueError("raw receipt endpoint differs from its task")
        expected_params = dict(work["params"])
        cursor = manifest.get("request_cursor", params.get("cursor", ""))
        if not isinstance(cursor, str):
            raise CatalogueError("raw receipt cursor is malformed")
        if cursor != params.get("cursor", ""):
            raise CatalogueError("raw receipt cursor context differs from its parameters")
        if cursor:
            expected_params["cursor"] = cursor
        elif "cursor" in params:
            raise CatalogueError("raw receipt has an empty cursor parameter")
        if params != expected_params:
            raise CatalogueError("raw receipt parameters differ from its task")
        request_work["cursor"] = cursor
        request_work["cutoff_due"] = False
    else:
        if manifest.get("endpoint") != CUTOFF_ENDPOINT or params:
            raise CatalogueError("raw cutoff receipt request differs from its task")
        request_work["cutoff_due"] = True
        cursor = manifest.get("request_cursor", work["cursor"])
        if not isinstance(cursor, str):
            raise CatalogueError("raw cutoff receipt cursor context is malformed")
        request_work["cursor"] = cursor

    generation = manifest.get("request_generation", manifest.get("task_generation"))
    generations = [work.get("restarts", 0)]
    if generation is not None:
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise CatalogueError("raw receipt generation is malformed")
        if generation > work.get("restarts", 0):
            raise CatalogueError("raw receipt generation exceeds the task history")
        request_work["restarts"] = generation
    if "task_generation" in manifest and manifest.get("task_generation") != generation:
        raise CatalogueError("raw receipt generation context is contradictory")
    elif work.get("restarts", 0):
        generations = list(range(work["restarts"] + 1))
    if checkpoint_task:
        ordinal = manifest.get("cutoff_ordinal")
        reason = manifest.get("cutoff_reason")
        if phase == "cutoff":
            if (
                isinstance(ordinal, bool)
                or not isinstance(ordinal, int)
                or ordinal < 1
                or ordinal > work["cutoff_ordinal"]
                or reason
                not in {"start", "resume", "cursor-reset", "pages", "interval", "terminal"}
            ):
                raise CatalogueError("raw cutoff receipt checkpoint differs from task history")
            request_work["cutoff_ordinal"] = ordinal
            request_work["cutoff_reason"] = reason
        else:
            request_work["cutoff_due"] = False
    for attempt_generation in generations:
        request_work["restarts"] = attempt_generation
        expected = _request_context_for_task(candidate, index, request_work)
        if (
            manifest.get("request_id") == expected["request_id"]
            and manifest.get("entity_kind") == expected["entity_kind"]
            and manifest.get("singular") is expected["singular"]
            and manifest.get("phase") == expected["phase"]
            and _receipt_matches_context(manifest, expected)
        ):
            return
    raise CatalogueError("raw receipt context is not bound to its capture task")


def _selected_market_batches(values: list[str]) -> list[list[str]]:
    batches: list[list[str]] = []
    current: list[str] = []
    for value in values:
        candidate = [*current, value]
        if (
            len(candidate) > MAX_SELECTION_BATCH
            or _market_batch_url_size(candidate) > MAX_INITIAL_BATCH_URL_BYTES
        ):
            if not current:
                raise CatalogueError("selected market ticker exceeds the initial URL limit")
            batches.append(current)
            current = [value]
            if _market_batch_url_size(current) > MAX_INITIAL_BATCH_URL_BYTES:
                raise CatalogueError("selected market ticker exceeds the initial URL limit")
        else:
            current = candidate
    if current:
        batches.append(current)
    return batches


def _event_batch_url_size(values: list[str], *, multivariate: bool) -> int:
    endpoint = "/events/multivariate" if multivariate else "/events"
    params = {
        "tickers": ",".join(values),
        "limit": EVENT_PAGE_LIMIT,
        "with_nested_markets": "false",
    }
    return len(
        str(httpx.Request("GET", source_url() + endpoint, params=params).url).encode("ascii")
    )


def _event_parent_batches(values: set[str] | list[str], *, multivariate: bool) -> list[list[str]]:
    """Partition deterministically; small or URL-constrained groups use singular lookups."""
    ordered = sorted({ticker(value) for value in values})
    groups: list[list[str]] = []
    current: list[str] = []
    for value in ordered:
        candidate = [*current, value]
        if (
            len(candidate) > MAX_SELECTION_BATCH
            or _event_batch_url_size(candidate, multivariate=multivariate)
            > MAX_INITIAL_BATCH_URL_BYTES
        ):
            if len(current) >= 20:
                groups.append(current)
            else:
                groups.extend([[item] for item in current])
            current = [value]
            if (
                _event_batch_url_size(current, multivariate=multivariate)
                > MAX_INITIAL_BATCH_URL_BYTES
            ):
                raise CatalogueError("event ticker exceeds the initial parent batch URL limit")
        else:
            current = candidate
    if len(current) >= 20:
        groups.append(current)
    else:
        groups.extend([[item] for item in current])
    return groups


def parent_lookup_plan(
    state: dict[str, Any], event_tickers: set[str] | list[str], series_tickers: set[str] | list[str]
) -> list[dict[str, Any]]:
    """Create deterministic parent requests for full and incremental hydration."""
    result: list[dict[str, Any]] = []
    batched = _batched_parent_policy(state)
    for values, endpoint, kind in (
        (event_tickers, "/events", "events"),
        (series_tickers, "/series", "series"),
    ):
        if kind == "events" and batched and len(values) >= 20:
            for group in _event_parent_batches(values, multivariate=False):
                if len(group) >= 20:
                    result.append(
                        _protocol_task(
                            state,
                            endpoint,
                            kind,
                            params={
                                "tickers": ",".join(group),
                                "limit": EVENT_PAGE_LIMIT,
                                "with_nested_markets": "false",
                            },
                            parent_batch="ordinary",
                        )
                    )
                else:
                    result.append(
                        _protocol_task(
                            state,
                            f"{endpoint}/{ticker(group[0])}",
                            kind,
                            singular=True,
                            params={"exclude_markets": "true"},
                        )
                    )
        else:
            for identity in sorted(values):
                params = (
                    {"exclude_markets": "true"}
                    if kind == "events"
                    and _acquisition_protocol(state)
                    in {
                        ACQUISITION_PROTOCOL,
                        SCOPED_ACQUISITION_PROTOCOL,
                        CURRENT_ACQUISITION_PROTOCOL,
                    }
                    else None
                )
                result.append(
                    _protocol_task(
                        state,
                        f"{endpoint}/{ticker(identity)}",
                        kind,
                        singular=True,
                        params=params,
                    )
                )
    return result


def multivariate_parent_plan(
    state: dict[str, Any], event_tickers: set[str] | list[str]
) -> list[dict[str, Any]]:
    if not _batched_parent_policy(state) or _is_scoped_capture(state):
        return []
    result = []
    for group in _event_parent_batches(event_tickers, multivariate=True):
        if len(group) >= 20:
            result.append(
                _protocol_task(
                    state,
                    "/events/multivariate",
                    "events",
                    params={
                        "tickers": ",".join(group),
                        "limit": EVENT_PAGE_LIMIT,
                        "with_nested_markets": "false",
                    },
                    parent_batch="multivariate",
                )
            )
        else:
            result.extend(
                _protocol_task(
                    state,
                    f"/events/{ticker(identity)}",
                    "events",
                    singular=True,
                    params={"exclude_markets": "true"},
                )
                for identity in group
            )
    return result


def new_capture(
    store: Store,
    selected: list[str] | None,
    compression: str = "gzip",
    *,
    incremental: bool = False,
    rate_policy: str = "decrease-on-429",
    _on_capture_id: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if compression not in {"identity", "gzip"}:
        raise CatalogueError("compression must be identity or gzip")
    if not isinstance(rate_policy, str) or rate_policy not in {"fixed", "decrease-on-429"}:
        raise CatalogueError("rate policy must be fixed or decrease-on-429")
    if incremental and selected:
        raise CatalogueError("incremental capture cannot be combined with market selection")
    scope = store.scope
    if scope is not None and selected is not None:
        raise CatalogueError("scoped captures require the full series acquisition plan")
    started_at = now()
    pin = None
    if incremental:
        from kalshi_events.incremental import baseline

        pin = baseline(store, started_at=started_at)
    if scope is not None:
        from kalshi_events.incremental import root_id

        logical_root_id = root_id(store, create=True)
        series = scope["series_ticker"]
        work = _series_tasks(series)
        scopes = dict(_SERIES_TASK_SCOPES)
        protocol = CURRENT_ACQUISITION_PROTOCOL
    else:
        work = [task("/historical/cutoff", None, singular=True)]
        protocol = CURRENT_ACQUISITION_PROTOCOL
    if scope is None and incremental:
        params = {"with_nested_markets": "false", "limit": EVENT_PAGE_LIMIT}
        work += [
            task("/series", "series"),
            task("/events", "events", params={**params, "min_updated_ts": pin["query_from"]}),
            task("/events/multivariate", "events", params=params),
            task(
                "/markets",
                "markets",
                params={"limit": MARKET_PAGE_LIMIT, "mve_filter": "only"},
            ),
            task(
                "/markets",
                "markets",
                params={
                    "limit": MARKET_PAGE_LIMIT,
                    "mve_filter": "exclude",
                    "min_updated_ts": pin["query_from"],
                },
            ),
            task("/historical/markets", "markets", params={"limit": MARKET_PAGE_LIMIT}),
        ]
    elif scope is None and selected is None:
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
    elif scope is None:
        work += [
            task(
                "/markets",
                "markets",
                params={"limit": MARKET_PAGE_LIMIT, "tickers": ",".join(batch)},
            )
            for batch in _selected_market_batches(selections(selected))
        ]
    if scope is None:
        scopes = {}
        for index, item in enumerate(work):
            if selected is not None:
                continue
            endpoint = item["endpoint"]
            if endpoint == "/series":
                scopes[str(index)] = "series"
            elif endpoint == "/events":
                scopes[str(index)] = "events_live"
            elif endpoint == "/events/multivariate":
                scopes[str(index)] = "events_combo"
            elif endpoint == "/markets":
                scopes[str(index)] = (
                    "markets_combo"
                    if item["params"].get("mve_filter") == "only"
                    else "markets_live"
                )
            elif endpoint == "/historical/markets":
                scopes[str(index)] = "markets_history"
    capture_id = uuid.uuid4().hex
    if _on_capture_id is not None:
        _on_capture_id(capture_id)
    state = {
        "version": 1,
        "journal_version": JOURNAL_VERSION,
        "acquisition_protocol": protocol,
        "capture_id": capture_id,
        "selected": selected,
        "started_at": started_at,
        "completed_at": None,
        "complete": False,
        # Sealed at creation: every later read of this capture uses this ceiling.
        "max_response_bytes": store.limits.max_response_bytes,
        "max_decoded_response_bytes": store.limits.max_decoded_response_bytes,
        "compression": compression,
        "tasks": work,
        "task_scopes": scopes,
        "incremental": incremental,
        "cutoff": None,
        "coverage": {},
        "error": None,
    }
    if protocol == CURRENT_ACQUISITION_PROTOCOL:
        policies = {**_PROTOCOL5_POLICIES, "rate_policy": rate_policy}
        state.update(policies)
        state["acquisition_policy_sha256"] = digest(json_bytes(policies))
        state["tasks"] = [
            _protocol_task(
                state,
                item["endpoint"],
                item["kind"],
                singular=item["singular"],
                params=item["params"],
                requested=item["requested"],
            )
            for item in work
        ]
    if scope is not None:
        state.update(scope=scope, root_id=logical_root_id)
    if pin is not None:
        state.update(pin)
    return _initialize_v2(store, state)


def _phase(work: dict[str, Any]) -> str:
    return "cutoff" if work["cutoff_due"] else "page"


def _schedule_cutoff(store: Store, state: _CaptureState, index: int, reason: str) -> None:
    work = state["tasks"][index]
    _commit_control_delta(
        store,
        state,
        {
            "kind": "cutoff_schedule",
            "index": index,
            "reason": reason,
            "ordinal": work["cutoff_ordinal"] + 1,
        },
    )


def _interval_cutoff_due(state: _CaptureState, index: int) -> bool:
    anchor = state.cutoff_monotonic.get(index)
    return anchor is not None and time.monotonic() - anchor >= 300


def _plan(work: dict[str, Any], phase: str) -> tuple[str, dict[str, Any], str | None, bool]:
    if phase == "cutoff":
        return CUTOFF_ENDPOINT, {}, None, True
    params = {**work["params"], **({"cursor": work["cursor"]} if work["cursor"] else {})}
    return work["endpoint"], params, work["kind"], work["singular"]


def _request_context(state: dict[str, Any], index: int) -> dict[str, Any]:
    return _request_context_for_task(state, index, state["tasks"][index])


def _request_context_for_task(
    state: dict[str, Any], index: int, work: dict[str, Any]
) -> dict[str, Any]:
    phase = _phase(work)
    _, _, kind, singular = _plan(work, phase)
    protocol = _acquisition_protocol(state)
    request_key = [state["capture_id"], index, phase, work["cursor"], work["restarts"]]
    if work.get("parent_batch") is not None:
        request_key.extend([work["parent_batch"], _task_identity(work), work["params"]["tickers"]])
    checkpoint_task = (
        _checkpoint_policy(state) and _historical(work["endpoint"]) and not work["singular"]
    )
    if phase == "cutoff" and checkpoint_task:
        request_key.extend([work["cutoff_reason"], work["cutoff_ordinal"]])
    if _is_scoped_capture(state):
        request_key.append(validate_series_scope(state["scope"]))
    if protocol == CURRENT_ACQUISITION_PROTOCOL:
        request_key.append(state["acquisition_policy_sha256"])
    context = {
        "capture_id": state["capture_id"],
        "task_index": index,
        "phase": phase,
        "request_id": digest(json_bytes([*request_key, protocol]))
        if protocol
        in {ACQUISITION_PROTOCOL, SCOPED_ACQUISITION_PROTOCOL, CURRENT_ACQUISITION_PROTOCOL}
        else digest(json_bytes(request_key)),
        "entity_kind": kind,
        "singular": singular,
    }
    if protocol in {
        ACQUISITION_PROTOCOL,
        SCOPED_ACQUISITION_PROTOCOL,
        CURRENT_ACQUISITION_PROTOCOL,
    }:
        context["acquisition_protocol"] = protocol
    if _is_scoped_capture(state):
        context["series_scope"] = validate_series_scope(state["scope"])
    if protocol == CURRENT_ACQUISITION_PROTOCOL:
        context["acquisition_policy_sha256"] = state["acquisition_policy_sha256"]
    scope = state.get("task_scopes", {}).get(str(index))
    if scope is not None:
        context.update(task_scope=scope, task_generation=work["restarts"])
    if work.get("parent_batch") is not None:
        context.update(
            parent_batch=work["parent_batch"],
            parent_batch_id=_task_identity(work),
            parent_batch_tickers=work["params"]["tickers"],
            task_generation=work["restarts"],
        )
    if protocol == CURRENT_ACQUISITION_PROTOCOL and (
        work.get("parent_batch") is not None or checkpoint_task
    ):
        # These immutable request inputs let later readers bind receipts to the
        # sealed task even when a snapshot skips replay of the journal prefix.
        context.update(request_cursor=work["cursor"], request_generation=work["restarts"])
    if phase == "cutoff" and checkpoint_task:
        context.update(
            cutoff_reason=work["cutoff_reason"],
            cutoff_ordinal=work["cutoff_ordinal"],
        )
    return context


def _receipt_matches_context(manifest: dict[str, Any], context: dict[str, Any]) -> bool:
    for key in (
        "parent_batch",
        "parent_batch_id",
        "parent_batch_tickers",
        "task_scope",
        "task_generation",
        "request_generation",
        "request_cursor",
        "cutoff_reason",
        "cutoff_ordinal",
    ):
        if key in {"task_generation", "request_generation", "cutoff_ordinal"}:
            for source in (manifest, context):
                if key in source and (
                    isinstance(source[key], bool) or not isinstance(source[key], int)
                ):
                    return False
        if key == "request_cursor" and key in manifest and not isinstance(manifest[key], str):
            return False
        if (key in manifest) != (key in context) or (
            key in context and manifest.get(key) != context[key]
        ):
            return False
    if ("acquisition_protocol" in manifest) != ("acquisition_protocol" in context):
        return False
    if "acquisition_protocol" not in context:
        return True
    protocol = manifest["acquisition_protocol"]
    protocol_matches = (
        isinstance(protocol, int)
        and not isinstance(protocol, bool)
        and protocol == context["acquisition_protocol"]
    )
    if not protocol_matches:
        return False
    if ("acquisition_policy_sha256" in manifest) != ("acquisition_policy_sha256" in context):
        return False
    if (
        "acquisition_policy_sha256" in context
        and manifest.get("acquisition_policy_sha256") != context["acquisition_policy_sha256"]
    ):
        return False
    if ("series_scope" in manifest) != ("series_scope" in context):
        return False
    if "series_scope" in context:
        try:
            return validate_series_scope(manifest.get("series_scope")) == context["series_scope"]
        except CatalogueError:
            return False
    return True


def _receipt_matches_state_protocol(manifest: dict[str, Any], state: dict[str, Any]) -> bool:
    protocol = _acquisition_protocol(state)
    if any(
        key in manifest for key in ("parent_batch", "parent_batch_id", "parent_batch_tickers")
    ) and not _batched_parent_policy(state):
        return False
    if ("cutoff_reason" in manifest or "cutoff_ordinal" in manifest) and (
        not _checkpoint_policy(state) or manifest.get("phase") != "cutoff"
    ):
        return False
    if ("request_cursor" in manifest or "request_generation" in manifest) and not (
        protocol == CURRENT_ACQUISITION_PROTOCOL
        and (_batched_parent_policy(state) or _checkpoint_policy(state))
    ):
        return False
    if protocol == 2:
        return "acquisition_protocol" not in manifest
    value = manifest.get("acquisition_protocol")
    if not isinstance(value, int) or isinstance(value, bool) or value != protocol:
        return False
    if ("acquisition_policy_sha256" in manifest) != (protocol == CURRENT_ACQUISITION_PROTOCOL):
        return False
    has_scope = "series_scope" in manifest
    scoped = _is_scoped_capture(state)
    if has_scope != scoped:
        return False
    if protocol == CURRENT_ACQUISITION_PROTOCOL and manifest.get(
        "acquisition_policy_sha256"
    ) != state.get("acquisition_policy_sha256"):
        return False
    if scoped:
        try:
            return validate_series_scope(manifest.get("series_scope")) == validate_series_scope(
                state.get("scope")
            )
        except CatalogueError:
            return False
    return not has_scope


def _record_cutoff(state: dict[str, Any], payload: Any) -> None:
    state["cutoff"] = _validated_cutoff(payload)


def _task_identity_set(state: dict[str, Any]) -> set[str]:
    if isinstance(state, _CaptureState):
        return state.task_identities
    return {_task_identity(work) for work in state["tasks"]}


def _track_added_task_identities(state: dict[str, Any], works: list[dict[str, Any]]) -> None:
    if isinstance(state, _CaptureState) and state.task_identities is not None:
        state.task_identities.update(_task_identity(work) for work in works)


def _apply(
    state: dict[str, Any],
    index: int,
    manifest: dict[str, Any],
    payload: Any,
    seen: dict[int, set[str]],
) -> None:
    change = _derive_page_change(state, seen, index, manifest, payload)
    _apply_page_change(state, seen, index, change)


def _stage(
    store: Store, state: dict[str, Any], index: int, relative: str, identities: ParentIndex
) -> None:
    """Commit a raw receipt to its traversal before it is applied: checkpoint, then journal."""
    if state.get("journal_version") == JOURNAL_VERSION:
        context = _request_context(state, index)
        manifest, payload = _commit_page_v2(
            store, state, index, relative, context["request_id"], identities
        )
        state.recent_page = (relative, manifest, payload)
        return
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
    identities: ParentIndex,
) -> None:
    if state.get("journal_version") == JOURNAL_VERSION:
        if relative not in state.processed:
            context = _request_context(state, index)
            manifest, payload = _commit_page_v2(
                store, state, index, relative, context["request_id"], identities
            )
            state.recent_page = (relative, manifest, payload)
        recent = state.recent_page
        if recent is not None and recent[0] == relative:
            _, manifest, payload = recent
            if relative not in identities.processed:
                identities.add_page(manifest, payload)
                identities.processed.add(relative)
            state.recent_page = None
        return
    work = state["tasks"][index]
    with metrics.phase("page_validation"), metrics.thread_cpu_phase("page_processing_cpu"):
        manifest, payload = read_page(
            store,
            relative,
            state["max_response_bytes"],
            state.get("max_decoded_response_bytes", state["max_response_bytes"]),
        )
    context = _request_context(state, index)
    if manifest.get("request_id") != context["request_id"] or not _receipt_matches_context(
        manifest, context
    ):
        raise CatalogueError("raw receipt does not match its traversal")
    with metrics.phase("page_application"), metrics.thread_cpu_phase("page_processing_cpu"):
        if _phase(work) == "cutoff":
            if manifest["status"] != 200:
                raise CatalogueError(f"historical cutoff read failed: HTTP {manifest['status']}")
            _record_cutoff(state, payload)
            work["cutoff_due"] = False
        else:
            identities.add_page(manifest, payload)
            _apply(state, index, manifest, payload, seen)
    identities.processed.add(relative)
    work["pending"] = None
    save(store, state)
    metrics.endpoint_page(manifest["endpoint"])


def _settle(
    store: Store,
    state: dict[str, Any],
    seen: dict[int, set[str]],
    identities: ParentIndex,
) -> None:
    """Apply pages whose raw receipts were committed before a previous process stopped."""
    if state.get("journal_version") == JOURNAL_VERSION:
        return
    logged = {row["relative"] for row in page_receipts(store, state)}
    index = 0
    while index < len(state["tasks"]):
        pending = state["tasks"][index]["pending"]
        if pending is not None:
            if pending not in logged:
                store.append(_journal(state), json_bytes({"index": index, "relative": pending}))
                logged.add(pending)
            _consume(store, state, index, pending, seen, identities)
        index += 1


def _hydrate(store: Store, state: dict[str, Any], identities: ParentIndex) -> bool:
    with metrics.phase("hydration"):
        if state.get("journal_version") == JOURNAL_VERSION:
            with metrics.phase("control_state_copy"):
                candidate = copy.deepcopy(dict(state))
            more = _hydrate_pages(store, candidate, identities)
            if json_bytes(dict(candidate)) != json_bytes(dict(state)):
                _commit_control(store, state, "hydrate", candidate)
            return more
        return _hydrate_pages(store, state, identities)


def _hydrate_pages(store: Store, state: dict[str, Any], identities: ParentIndex) -> bool:
    if isinstance(identities, DiskParentIndex):
        existing = _task_identity_set(state)
        added: list[dict[str, Any]] = []
        for kind, group in identities.missing_parent_batches(state):
            planned = parent_lookup_plan(
                state, group if kind == "events" else [], group if kind == "series" else []
            )
            for work in planned:
                identity = _task_identity(work)
                if identity not in existing:
                    state["tasks"].append(work)
                    existing.add(identity)
                    added.append(work)
        _track_added_task_identities(state, added)
        return any(not work["done"] for work in state["tasks"])
    effective_events, effective_lookups = identities.effective_parent_evidence(state)
    events = {identity: value[2] for identity, value in effective_events.items()}
    series = identities.series
    markets = {identity: value[2] for identity, value in identities.markets.items()}
    if _is_scoped_capture(state):
        final_event_members: set[str] = set()
        current_market_members: set[str] = set()
        for index, scope in state["task_scopes"].items():
            work = state["tasks"][int(index)]
            generation = identities.scope_membership
            if scope == "events_live":
                final_event_members.update(generation.get(scope, {}).get(work["restarts"], set()))
            elif scope in {"markets_live", "markets_history"}:
                current_market_members.update(
                    generation.get(scope, {}).get(work["restarts"], set())
                )
        missing_events = {
            event_ticker
            for market_ticker in current_market_members
            for market in [identities.markets.get(market_ticker)]
            if market is not None
            for event_ticker in [market[2]]
            if event_ticker
            and event_ticker not in final_event_members
            and ("events", event_ticker) not in effective_lookups
        }
        missing_series = set()
    elif state.get("incremental"):
        missing_events = set()
        missing_series = set()
    else:
        resolved_event_lookups = {
            identity for kind, identity in effective_lookups if kind == "events"
        }
        required_events = set(markets.values())
        batch_observations = {
            identity
            for candidates in identities.parent_batch_candidates.values()
            for identity in candidates
        }
        missing_events = (
            (required_events - events.keys())
            | (required_events & batch_observations - resolved_event_lookups)
        ) - resolved_event_lookups
        missing_series = {value for value in events.values() if value} - series.keys()
    existing_identities = _task_identity_set(state)
    for addition in parent_lookup_plan(state, missing_events, missing_series):
        identity = _task_identity(addition)
        if identity not in existing_identities:
            state["tasks"].append(addition)
            existing_identities.add(identity)
            _track_added_task_identities(state, [addition])
    if state.get("task_scopes") and (
        not _is_scoped_capture(state) or state.get("incremental") is True
    ):
        from kalshi_events.incremental import parent_lookup_tasks

        existing_identities = {_task_identity(work) for work in state["tasks"]}
        for work in parent_lookup_tasks(store, state, identities):
            if _task_identity(work) not in existing_identities:
                state["tasks"].append(work)
                existing_identities.add(_task_identity(work))
                _track_added_task_identities(state, [work])
    if any(not work["done"] for work in state["tasks"]):
        if state.get("journal_version") != JOURNAL_VERSION:
            save(store, state)
        return True
    if _is_scoped_capture(state):
        scope = validate_series_scope(state["scope"])
        series_ticker = scope["series_ticker"]
        if series_ticker not in identities.series:
            raise CatalogueError("scoped series definition is missing")
        final_event_members: set[str] = set()
        current_market_members: set[str] = set()
        for index, membership_scope in state.get("task_scopes", {}).items():
            work = state["tasks"][int(index)]
            if membership_scope == "events_live":
                final_event_members.update(
                    identities.scope_membership.get(membership_scope, {}).get(
                        work["restarts"], set()
                    )
                )
            elif membership_scope in {"markets_live", "markets_history"}:
                current_market_members.update(
                    identities.scope_membership.get(membership_scope, {}).get(
                        work["restarts"], set()
                    )
                )
        for market_ticker in current_market_members:
            market = identities.markets.get(market_ticker)
            event_ticker = market[2] if market is not None else None
            event = effective_events.get(event_ticker) if event_ticker is not None else None
            lookup = effective_lookups.get(("events", event_ticker))
            event_is_authoritative = event_ticker in final_event_members or (
                lookup is not None and lookup[0] == 200
            )
            if event is None or event[2] != series_ticker or not event_is_authoritative:
                raise CatalogueError(
                    f"market {market_ticker} lacks an authoritative in-scope event definition"
                )
    if state["selected"] is not None:
        for selected in state["selected"]:
            parent = markets.get(selected)
            event_lookup = effective_lookups.get(("events", parent)) if parent else None
            series_lookup = (
                effective_lookups.get(("series", events.get(parent))) if parent in events else None
            )
            if state["coverage"].get(selected, {}).get("status") == "found" and (
                not parent
                or parent not in events
                or events[parent] not in series
                or (
                    _batched_parent_policy(state)
                    and (
                        (event_lookup is not None and event_lookup[0] != 200)
                        or (series_lookup is not None and series_lookup[0] != 200)
                    )
                )
            ):
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
        relative = path.relative_to(store.root).as_posix()
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
        if not _receipt_matches_context(manifest, context):
            raise CatalogueError("uncommitted raw receipt contradicts acquisition protocol")
        endpoint, params, kind, singular = _plan(work, _phase(work))
        if (
            manifest.get("endpoint") != endpoint
            or manifest.get("params") != params
            or manifest.get("entity_kind") != kind
            or manifest.get("singular") != singular
        ):
            raise CatalogueError("uncommitted raw receipt contradicts request identity")
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
    identities: ParentIndex,
    *,
    resumed: bool = False,
) -> None:
    running: dict[Future, int] = {}
    busy: set[int] = set()
    ready = deque(index for index, work in enumerate(state["tasks"]) if not work["done"])
    queued = set(ready)
    metrics.add("tasks_enqueued", len(ready))
    failure: Exception | None = None
    resume_cutoffs_pending = resumed and _checkpoint_policy(state)

    def enqueue(index: int) -> None:
        if index not in busy and index not in queued and not state["tasks"][index]["done"]:
            ready.append(index)
            queued.add(index)
            metrics.add("tasks_enqueued")

    def enqueue_appended(start: int) -> None:
        for new_index in range(start, len(state["tasks"])):
            enqueue(new_index)

    while True:
        if failure is None:
            try:
                _ = budget.remaining
                while ready and len(running) < MAX_CONCURRENT_REQUESTS and failure is None:
                    with metrics.phase("scheduler_work"):
                        if resume_cutoffs_pending and not recovered and not running:
                            for task_index, task_state in enumerate(state["tasks"]):
                                if (
                                    _historical(task_state["endpoint"])
                                    and not task_state["singular"]
                                    and not task_state["done"]
                                    and not task_state["cutoff_due"]
                                ):
                                    _schedule_cutoff(store, state, task_index, "resume")
                                    enqueue(task_index)
                            resume_cutoffs_pending = False
                        index = ready.popleft()
                        queued.discard(index)
                        work = state["tasks"][index]
                        if work["done"] or index in busy:
                            continue
                        context = _request_context(state, index)
                        relative = recovered.pop(context["request_id"], None)
                        if relative is not None:
                            endpoint, _, _, _ = _plan(work, _phase(work))
                            task_count = len(state["tasks"])
                            _stage(store, state, index, relative, identities)
                            _consume(store, state, index, relative, seen, identities)
                            metrics.endpoint_page(endpoint, recovered=True)
                            enqueue_appended(task_count)
                            enqueue(index)
                            continue
                        if recovered:
                            ready.append(index)
                            queued.add(index)
                            break
                        if (
                            _checkpoint_policy(state)
                            and _historical(work["endpoint"])
                            and not work["singular"]
                            and not work["done"]
                            and not work["cutoff_due"]
                            and _interval_cutoff_due(state, index)
                        ):
                            _schedule_cutoff(store, state, index, "interval")
                            enqueue(index)
                            continue
                        endpoint, params, _, _ = _plan(work, _phase(work))
                        batch = _batch_tickers(work)
                        if (
                            batch is not None
                            and _phase(work) == "page"
                            and not work["cursor"]
                            and _market_batch_url_size(batch) > MAX_INITIAL_BATCH_URL_BYTES
                        ):
                            raise CatalogueError(
                                "selected market batch exceeds the initial URL limit"
                            )
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
                        metrics.add("futures_submitted")
                        metrics.submitted_futures(len(running))
            except Exception as exc:
                failure = exc
        if not running:
            if failure is not None:
                raise failure
            if ready:
                continue
            task_count = len(state["tasks"])
            if _hydrate(store, state, identities):
                enqueue_appended(task_count)
                continue
            if state.get("journal_version") == JOURNAL_VERSION:
                with metrics.phase("control_state_copy"):
                    candidate = copy.deepcopy(dict(state))
                candidate.update(complete=True, completed_at=now(), error=None)
                _commit_control(store, state, "complete", candidate)
                _checkpoint_v2(store, state)
            else:
                state.update(complete=True, completed_at=now(), error=None)
                save(store, state)
            return
        with metrics.phase("scheduler_wait"):
            finished, _ = wait(running, return_when=FIRST_COMPLETED)
        for future in sorted(finished, key=lambda item: running[item]):
            index = running.pop(future)
            busy.discard(index)
            metrics.add("futures_completed")
            try:
                relative = future.result()
            except Exception as exc:
                failure = failure or exc
                continue
            if isinstance(state, _CaptureState) and state.journal_uncertain:
                # The durable journal may include a record this process did not apply. Leave
                # completed receipts orphaned; the next locked resume reconciles them.
                continue
            if failure is not None:
                # Keep the finalized receipt orphaned for locked recovery; never commit pages
                # after another traversal has made the current state uncertain.
                continue
            try:
                task_count = len(state["tasks"])
                _stage(store, state, index, relative, identities)
                _consume(store, state, index, relative, seen, identities)
                enqueue_appended(task_count)
                enqueue(index)
            except Exception as exc:
                failure = failure or exc


def _capture_locked(
    store: Store,
    *,
    selected: list[str] | None = None,
    resume: str | None = None,
    client_factory: Any = None,
    compression: str | None = None,
    rate_policy: str | None = None,
    incremental: bool = False,
    _on_capture_id: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    capture_state = None
    if resume is not None:
        snapshot = store.path(f"captures/{resume}/state.json")
        if snapshot.is_file():
            value = decode(regular_bytes(snapshot, store.limits.max_root_bytes))
            capture_state = value.get("state", value) if isinstance(value, dict) else None
        else:
            records, _ = _journal_records(store, resume, repair_tail=False)
            first = next(records, None)
            capture_state = first[0].get("payload", {}).get("state") if first else None
    disk_index = (
        selected is None
        and not incremental
        and store.scope is None
        and (
            resume is None
            or isinstance(capture_state, dict)
            and _acquisition_protocol(capture_state) == CURRENT_ACQUISITION_PROTOCOL
            and capture_state.get("selected") is None
            and capture_state.get("incremental") is not True
            and "scope" not in capture_state
        )
    )
    identities: ParentIndex | DiskParentIndex = (
        DiskParentIndex(store) if disk_index else ParentIndex()
    )
    try:
        return _capture_locked_impl(
            store,
            selected=selected,
            resume=resume,
            client_factory=client_factory,
            compression=compression,
            rate_policy=rate_policy,
            incremental=incremental,
            _on_capture_id=_on_capture_id,
            identities=identities,
        )
    finally:
        if isinstance(identities, DiskParentIndex):
            identities.close()


def _capture_locked_impl(
    store: Store,
    *,
    selected: list[str] | None = None,
    resume: str | None = None,
    client_factory: Any = None,
    compression: str | None = None,
    rate_policy: str | None = None,
    incremental: bool = False,
    _on_capture_id: Callable[[str], None] | None = None,
    identities: ParentIndex | DiskParentIndex,
) -> dict[str, Any]:
    factory = client_factory or PublicClient
    if resume and _on_capture_id is not None:
        _on_capture_id(resume)
    state = (
        read_capture(
            store,
            resume,
            _writer_resume=True,
            _parent_index=identities,
        )
        if resume
        else new_capture(
            store,
            selected,
            compression if compression is not None else "gzip",
            incremental=incremental,
            rate_policy=rate_policy if rate_policy is not None else "decrease-on-429",
            _on_capture_id=_on_capture_id,
        )
    )
    if resume and incremental and not state.get("incremental"):
        raise CatalogueError("cannot resume a full capture as incremental")
    if state.get("incremental"):
        from kalshi_events.incremental import verify_pin

        verify_pin(store, state)
    if isinstance(state, _CaptureState) and resume:
        current_hash = _state_digest(state, state.seen_cursors)
        if state.snapshot_seq != state.journal_head["seq"]:
            if state.journal_anchor_state_sha256 == current_hash:
                _save_v2(store, state)
            else:
                _checkpoint_v2(store, state)
    if resume and selected is not None and selected != state["selected"]:
        raise CatalogueError("resume selection differs from capture")
    sealed_compression = state.get("compression", "identity")
    if compression is not None and compression != sealed_compression:
        raise CatalogueError("resume compression differs from capture")
    sealed_rate_policy = state.get("rate_policy", "fixed")
    if rate_policy is not None and rate_policy != sealed_rate_policy:
        raise CatalogueError("resume rate policy differs from capture")
    client_options = {}
    if sealed_compression != "identity":
        client_options["compression"] = sealed_compression
    if sealed_rate_policy != "fixed":
        client_options["rate_policy"] = sealed_rate_policy
    decoded_ceiling = state.get("max_decoded_response_bytes", state["max_response_bytes"])
    budget = Budget(
        store,
        max_response_bytes=state["max_response_bytes"],
        max_decoded_response_bytes=decoded_ceiling,
    )
    if state["complete"]:
        result = {**state, **budget.result(), "budget_exhausted": False}
        if _on_capture_id is not None:
            result["_refresh_journal_uncertain"] = bool(getattr(state, "journal_uncertain", False))
        return result
    if state.get("error"):
        if state.get("journal_version") == JOURNAL_VERSION:
            _commit_control_delta(store, state, {"kind": "clear_error"})
            _checkpoint_v2(store, state)
        else:
            state["error"] = None
            save(store, state)
    seen: dict[int, set[str]] = {}
    budget_exhausted = False
    try:
        with (
            factory(store, budget, **client_options) as client,
            ThreadPoolExecutor(
                max_workers=MAX_CONCURRENT_REQUESTS, thread_name_prefix="kalshi-traversal"
            ) as pool,
        ):
            _settle(store, state, seen, identities)
            if resume:
                for row in page_receipts(store, state):
                    if row["relative"] in identities.processed:
                        continue
                    with (
                        metrics.phase("page_validation"),
                        metrics.thread_cpu_phase("page_processing_cpu"),
                    ):
                        manifest, payload = read_page(
                            store,
                            row["relative"],
                            state["max_response_bytes"],
                            decoded_ceiling,
                        )
                    if manifest.get("capture_id") != state["capture_id"]:
                        raise CatalogueError("raw evidence belongs to another capture")
                    if not _receipt_matches_state_protocol(manifest, state):
                        raise CatalogueError("raw receipt protocol differs from its capture")
                    if _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL and (
                        _batched_parent_policy(state) or _checkpoint_policy(state)
                    ):
                        _validate_receipt_task_binding(manifest, state, row)
                    with (
                        metrics.phase("page_application"),
                        metrics.thread_cpu_phase("page_processing_cpu"),
                    ):
                        identities.add_page(manifest, payload)
                    identities.processed.add(row["relative"])
            recovered = _recover_receipts(store, state) if resume else {}
            _run(
                store,
                state,
                client,
                pool,
                recovered,
                seen,
                budget,
                identities,
                resumed=resume is not None,
            )
    except (CatalogueError, OSError) as exc:
        if state.get("journal_version") != JOURNAL_VERSION:
            state["error"] = str(exc)
            save(store, state)
        elif not state.journal_uncertain and not state.get("complete"):
            try:
                _commit_control_delta(store, state, {"kind": "stop", "reason": str(exc)[:4096]})
                _checkpoint_v2(store, state)
            except (CatalogueError, OSError):
                # Preserve the original operation failure; resume will replay any durable suffix.
                state.journal_uncertain = True
        state["error"] = str(exc)
        if not isinstance(exc, BudgetExceeded):
            raise
        budget_exhausted = True
    result = {**state, **budget.result(), "budget_exhausted": budget_exhausted}
    if _on_capture_id is not None:
        result["_refresh_journal_uncertain"] = bool(getattr(state, "journal_uncertain", False))
    return result


def capture(
    store: Store,
    *,
    selected: list[str] | None = None,
    resume: str | None = None,
    client_factory: Any = None,
    compression: str | None = None,
    rate_policy: str | None = None,
    incremental: bool = False,
    _on_capture_id: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    with store.writer_guard():
        from kalshi_events.incremental import finish_pending_retirement

        finish_pending_retirement(store)
        return _capture_locked(
            store,
            selected=selected,
            resume=resume,
            client_factory=client_factory,
            compression=compression,
            rate_policy=rate_policy,
            incremental=incremental,
            _on_capture_id=_on_capture_id,
        )


def _complete_capture(
    store: Store, capture_id: str, *, _state: dict[str, Any] | None = None
) -> dict[str, Any]:
    state = read_capture(store, capture_id) if _state is None else _state
    if state.get("capture_id") != capture_id:
        raise CatalogueError("capture state does not match the requested capture")
    if (
        not state.get("complete")
        or any(work["pending"] is not None for work in state["tasks"])
        or any(not work["done"] for work in state["tasks"])
    ):
        raise CatalogueError("capture is incomplete; resume before build or publish")
    return state


def complete_capture(store: Store, capture_id: str) -> dict[str, Any]:
    state = _complete_capture(store, capture_id)
    for _ in capture_pages(store, state):
        pass
    return state


def _completed(store: Store, *, catalogue: bool | None) -> list[tuple[str, str]]:
    directory = store.path("captures")
    found = []
    if directory.exists():
        for child in directory.iterdir():
            state = read_capture(store, child.name)
            if (
                state["complete"]
                and not state.get("incremental")
                and state.get("scope") is None
                and (catalogue is None or (state["selected"] is None) == catalogue)
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
