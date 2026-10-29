"""One-invocation orchestration for safe full and incremental catalogue refreshes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from kalshi_events import capture, metrics, publish
from kalshi_events.common import BudgetExceeded, CatalogueError, Store
from kalshi_events.historical import (
    HISTORICAL_VERSION,
    historical_selection,
    is_historical_capture,
)


def result_template() -> dict[str, Any]:
    return {
        "complete": False,
        "capture_id": None,
        "capture_complete": None,
        "resumable": None,
        "stage": "preflight",
        "publication_state": "not_attempted",
        "requests": None,
        "downloaded_bytes": None,
        "accounting_complete": False,
        "output": None,
        "source_release_id": None,
        "error": None,
    }


def run(
    store: Store,
    *,
    full: bool = False,
    capture_id: str | None = None,
    compression: str | None = None,
    rate_policy: str | None = None,
    on_capture_id: Callable[[str], None] | None = None,
    historical: bool = False,
) -> tuple[dict[str, Any], int]:
    """Run refresh while the caller holds the canonical root writer lock."""
    result = result_template()
    if store.scope is not None:
        result["series_scope"] = dict(store.scope)
    try:
        if full and capture_id is not None:
            raise CatalogueError("--full and --capture-id cannot be used together")
        if historical and store.scope is not None:
            raise CatalogueError("historical catalogue mode cannot use a series root")
        if historical and not full and capture_id is None:
            raise CatalogueError("offline historical refresh requires an explicit --capture-id")

        incremental_mode = not full
        offline_historical = historical and capture_id is not None
        if capture_id is not None:
            result["capture_id"] = capture_id
            if on_capture_id is not None:
                on_capture_id(capture_id)
            state = capture.read_capture(store, capture_id)
            result["resumable"] = True
            if state.get("selected") is not None:
                raise CatalogueError("targeted captures cannot be resumed by refresh")
            if compression is not None and compression != state.get("compression", "identity"):
                raise CatalogueError("resume compression differs from capture")
            if rate_policy is not None and rate_policy != state.get("rate_policy", "fixed"):
                raise CatalogueError("resume rate policy differs from capture")
            incremental_mode = state.get("incremental") is True
        elif incremental_mode:
            if store.scope is not None:
                pointer = "current-series.json"
                if not store.path(pointer).exists():
                    raise CatalogueError("series refresh requires a verified series baseline")
            elif store.path("current-historical.json").exists():
                raise CatalogueError(
                    "historical catalogues require an explicit refresh --full; "
                    "use --historical --capture-id ID to finish saved evidence offline"
                )
            elif not (
                store.path("current-v2.json").exists() or store.path("current.json").exists()
            ):
                raise CatalogueError("incremental refresh requires a verified v1 or v2 publication")

        if offline_historical:
            result.update(
                coverage_scope="historical",
                scope_complete=False,
                requests=0,
                downloaded_bytes=0,
                accounting_complete=True,
            )
            historical_selection(state, allow_legacy_incomplete=True)
            result["resumable"] = not store.path(
                f"captures/{capture_id}/retired-scopes.json"
            ).exists()
        else:
            result["stage"] = "capture"
            with metrics.phase("capture"):
                state = capture.capture(
                    store,
                    resume=capture_id,
                    compression=compression,
                    rate_policy=rate_policy,
                    incremental=incremental_mode if capture_id is None else False,
                    _on_capture_id=lambda value: _remember_id(result, value, on_capture_id),
                )
        result["capture_id"] = state.get("capture_id", result["capture_id"])
        result["capture_complete"] = state.get("complete") is True
        if not offline_historical:
            result["requests"] = state.get("requests")
            result["downloaded_bytes"] = state.get("downloaded_bytes")
            result["accounting_complete"] = state.get("accounting_complete") is True
            result["resumable"] = None if state.get("_refresh_journal_uncertain") is True else True
        if not result["capture_complete"] and not offline_historical:
            error = state.get("error") or "capture stopped before completion"
            result["error"] = error
            return result, 3 if state.get("budget_exhausted") is True else 1

        historical_output = offline_historical or is_historical_capture(state)
        if historical_output:
            historical_selection(state, allow_legacy_incomplete=offline_historical)
            result.update(
                coverage_scope="historical",
                scope_complete=True,
                source_capture_complete=result["capture_complete"],
            )
        result["stage"] = "publication"
        result["publication_state"] = "unknown"
        published = publish.publish(
            store,
            result["capture_id"],
            catalogue_version=HISTORICAL_VERSION if historical_output else 2,
        )
        result["output"] = published.get("output")
        result["source_release_id"] = published.get("source_release_id")
        result["publication_state"] = "verified"
        result["stage"] = "complete"
        result["complete"] = True
        return result, 0
    except BudgetExceeded as exc:
        result["error"] = str(exc)
        if result["stage"] == "publication":
            result["publication_state"] = "unknown"
        return result, 3
    except Exception as exc:
        message = str(exc)
        if "deltas already depend on this baseline" in message:
            command = "series refresh --full" if store.scope is not None else "refresh --full"
            message = f"{message}; run {command}"
        result["error"] = message
        if result["stage"] == "publication":
            result["publication_state"] = "unknown"
        return result, 1


def _remember_id(
    result: dict[str, Any], value: str, callback: Callable[[str], None] | None
) -> None:
    result["capture_id"] = value
    if callback is not None:
        callback(value)
