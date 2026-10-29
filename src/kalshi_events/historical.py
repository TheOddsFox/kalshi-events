"""Sealed historical-catalogue capture selection helpers."""

from __future__ import annotations

from typing import Any

from kalshi_events.capture import (
    CURRENT_ACQUISITION_PROTOCOL,
    CUTOFF_ENDPOINT,
    EVENT_PAGE_LIMIT,
    HISTORICAL_PREFIX,
    MARKET_PAGE_LIMIT,
    _acquisition_protocol,
)
from kalshi_events.common import CatalogueError

HISTORICAL_VERSION = 3
HISTORICAL_CONTRACT = "oddsfox.kalshi.historical.v1"

_PRIMARY_TASKS = {
    "series": ("/series", "series", {}),
    "events_live": (
        "/events",
        "events",
        {"with_nested_markets": "false", "limit": EVENT_PAGE_LIMIT},
    ),
    "markets_history": (
        f"{HISTORICAL_PREFIX}",
        "markets",
        {"limit": MARKET_PAGE_LIMIT},
    ),
}


def is_historical_capture(state: dict[str, Any]) -> bool:
    """Return whether protocol 5 sealed this capture to the historical-only plan."""
    return (
        _acquisition_protocol(state) == CURRENT_ACQUISITION_PROTOCOL
        and state.get("parent_policy") == "historical-only-v1"
    )


def _require_done(work: dict[str, Any], scope: str) -> None:
    if work.get("pending") is not None or work.get("done") is not True:
        raise CatalogueError(f"historical {scope} traversal is incomplete")
    restarts = work.get("restarts")
    if isinstance(restarts, bool) or not isinstance(restarts, int) or restarts < 0:
        raise CatalogueError(f"historical {scope} traversal has invalid generation")


def _validate_primary(work: Any, scope: str) -> None:
    endpoint, kind, params = _PRIMARY_TASKS[scope]
    if (
        not isinstance(work, dict)
        or work.get("endpoint") != endpoint
        or work.get("kind") != kind
        or work.get("singular") is not False
        or work.get("params") != params
        or work.get("requested") is not None
    ):
        raise CatalogueError(f"historical {scope} task differs from its expected request")


def historical_selection(
    state: dict[str, Any], *, allow_legacy_incomplete: bool = False
) -> dict[str, dict[str, int]]:
    """Select the final completed primary generation of each historical scope.

    Legacy captures are eligible only by explicit offline-build opt-in. This helper
    validates logical task state; callers still validate each selected receipt body.
    """
    if not isinstance(state, dict):
        raise CatalogueError("historical capture state is invalid")
    protocol = _acquisition_protocol(state)
    historical = is_historical_capture(state)
    legacy = allow_legacy_incomplete and protocol == CURRENT_ACQUISITION_PROTOCOL and not historical
    if not historical and not legacy:
        raise CatalogueError("capture is not eligible for the historical profile")
    if (
        state.get("selected") is not None
        or state.get("incremental") is not False
        or "scope" in state
        or not isinstance(state.get("tasks"), list)
        or not isinstance(state.get("task_scopes"), dict)
    ):
        raise CatalogueError("historical profile requires an unscoped full capture")
    if historical and state.get("complete") is not True:
        raise CatalogueError("historical capture is incomplete")

    tasks = state["tasks"]
    cutoff_indices = [
        index
        for index, work in enumerate(tasks)
        if isinstance(work, dict)
        and work.get("endpoint") == CUTOFF_ENDPOINT
        and work.get("singular") is True
    ]
    if len(cutoff_indices) != 1:
        raise CatalogueError("historical capture lacks its initial cutoff traversal")
    cutoff_index = cutoff_indices[0]
    cutoff = tasks[cutoff_index]
    if (
        cutoff.get("kind") is not None
        or cutoff.get("params") != {}
        or cutoff.get("requested") is not None
        or cutoff.get("pending") is not None
        or cutoff.get("done") is not True
    ):
        raise CatalogueError("historical initial cutoff traversal is incomplete")

    if historical:
        expected_scopes = {"1": "series", "2": "events_live", "3": "markets_history"}
        expected_cutoff_task = tasks[0] if tasks else None
        if (
            len(tasks) != 4
            or cutoff_index != 0
            or state["task_scopes"] != expected_scopes
            or not isinstance(expected_cutoff_task, dict)
            or (
                expected_cutoff_task.get("endpoint"),
                expected_cutoff_task.get("kind"),
                expected_cutoff_task.get("params"),
                expected_cutoff_task.get("singular"),
            )
            != (CUTOFF_ENDPOINT, None, {}, True)
        ):
            raise CatalogueError("historical capture differs from its sealed task plan")
        scope_indices = {scope: int(index) for index, scope in state["task_scopes"].items()}
    else:
        scope_indices: dict[str, int] = {}
        for index, scope in state["task_scopes"].items():
            if (
                not isinstance(index, str)
                or not index.isdigit()
                or int(index) >= len(tasks)
                or not isinstance(scope, str)
            ):
                raise CatalogueError("historical capture has an invalid task scope")
            if scope in _PRIMARY_TASKS:
                if scope in scope_indices:
                    raise CatalogueError(f"historical {scope} scope is ambiguous")
                scope_indices[scope] = int(index)

    if set(scope_indices) != set(_PRIMARY_TASKS):
        raise CatalogueError("historical capture lacks a required primary scope")

    result: dict[str, dict[str, int]] = {}
    for scope in _PRIMARY_TASKS:
        index = scope_indices[scope]
        if not 0 <= index < len(tasks):
            raise CatalogueError(f"historical {scope} task index is invalid")
        work = tasks[index]
        _validate_primary(work, scope)
        _require_done(work, scope)
        if scope == "markets_history" and "data_done" in work and work["data_done"] is not True:
            raise CatalogueError("historical markets traversal lacks its terminal cutoff")
        result[scope] = {"task_index": index, "final_generation": work["restarts"]}
    return result


def historical_task_indices(
    state: dict[str, Any], *, allow_legacy_incomplete: bool = False
) -> set[int]:
    selection = historical_selection(state, allow_legacy_incomplete=allow_legacy_incomplete)
    cutoff_index = next(
        index
        for index, work in enumerate(state["tasks"])
        if isinstance(work, dict)
        and work.get("endpoint") == CUTOFF_ENDPOINT
        and work.get("singular") is True
    )
    return {cutoff_index, *(scope["task_index"] for scope in selection.values())}
