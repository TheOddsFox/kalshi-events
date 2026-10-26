"""Small deterministic protocol-5 roots for recovery tests and opt-in benchmarks."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any
from unittest.mock import patch

from kalshi_events import capture
from kalshi_events.common import Store, digest, json_bytes
from kalshi_events.normalize import ParentIndex

CAPTURE_ID = "d" * 32
ROOT_ID = "b" * 32
FIXED_TIME = "2026-01-01T00:00:00Z"
CURSOR = "fixture-next"


def _receipt(
    store: Store,
    state: dict[str, Any],
    index: int,
    identities: ParentIndex,
    status: int,
    payload: dict[str, Any],
) -> str:
    work = state["tasks"][index]
    endpoint, params, _, _ = capture._plan(work, capture._phase(work))
    context = capture._request_context(state, index)
    body = json_bytes(payload)
    page_id = context["request_id"][:32]
    relative = f"raw/{page_id}/manifest.json"
    store.write_bytes(f"raw/{page_id}/body.bin", body)
    store.write(
        relative,
        {
            **context,
            "page_id": page_id,
            "version": 1,
            "endpoint": endpoint,
            "params": params,
            "received_at": FIXED_TIME,
            "status": status,
            "size": len(body),
            "sha256": digest(body),
            "downloaded_bytes": len(body),
            "accounting_complete": True,
            "reserved_bytes": len(body),
        },
    )
    capture._commit_page_v2(store, state, index, relative, context["request_id"], identities)
    return relative


def build_recovery_root(
    root: Path,
    scenario: str,
    *,
    repeats: int = 2,
    wide_bytes: int = 0,
    hydration_tasks: int = 1,
) -> str:
    """Build a fresh root with real protocol-5 journal transitions and receipts."""
    if scenario not in {"primary", "hydration"}:
        raise ValueError("scenario must be 'primary' or 'hydration'")
    if isinstance(repeats, bool) or not isinstance(repeats, int) or not 1 <= repeats <= 3:
        raise ValueError("repeats must be between one and the cursor reset limit")
    if isinstance(wide_bytes, bool) or not isinstance(wide_bytes, int) or wide_bytes < 0:
        raise ValueError("wide_bytes must be a nonnegative integer")
    if (
        isinstance(hydration_tasks, bool)
        or not isinstance(hydration_tasks, int)
        or not 1 <= hydration_tasks <= capture.MARKET_PAGE_LIMIT
    ):
        raise ValueError("hydration_tasks must be between 1 and one market page")

    root = Path(root)
    if root.is_symlink() or (root.exists() and (not root.is_dir() or any(root.iterdir()))):
        raise FileExistsError(f"fixture root must be new and empty: {root}")
    store = Store(root)
    identities = ParentIndex()
    with (
        store.writer_guard(),
        patch.object(capture, "now", return_value=FIXED_TIME),
        patch.object(capture.uuid, "uuid4", return_value=uuid.UUID(hex=CAPTURE_ID)),
    ):
        store.write("root-id.json", {"version": 1, "root_id": ROOT_ID})
        state = capture.new_capture(store, None, compression="identity", rate_policy="fixed")
        if scenario == "primary":
            index = 2  # The canonical primary /events traversal.
            for generation in range(repeats + 1):
                row = {
                    "event_ticker": f"EVENT-G{generation:04d}",
                    "series_ticker": "SERIES",
                    "title": "Recovery fixture" + ("x" * wide_bytes),
                }
                _receipt(
                    store,
                    state,
                    index,
                    identities,
                    200,
                    {"events": [row], "cursor": CURSOR},
                )
                if generation < repeats:
                    _receipt(store, state, index, identities, 400, {"error": "cursor reset"})
            capture._checkpoint_v2(store, state)
            return state["capture_id"]

        _receipt(
            store,
            state,
            0,
            identities,
            200,
            {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            },
        )
        _receipt(store, state, 1, identities, 200, {"series": [{"ticker": "SERIES"}]})
        _receipt(store, state, 2, identities, 200, {"events": [], "cursor": ""})
        _receipt(store, state, 3, identities, 200, {"events": [], "cursor": ""})
        _receipt(
            store,
            state,
            4,
            identities,
            200,
            {
                "markets": [
                    {
                        "ticker": f"MKT-{index:06d}",
                        "event_ticker": f"EVENT-MISSING-{index:06d}",
                        "status": "active",
                    }
                    for index in range(hydration_tasks)
                ],
                "cursor": "",
            },
        )
        history_index = 5
        _receipt(
            store,
            state,
            history_index,
            identities,
            200,
            {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            },
        )
        _receipt(store, state, history_index, identities, 200, {"markets": [], "cursor": ""})
        _receipt(
            store,
            state,
            history_index,
            identities,
            200,
            {
                "market_settled_ts": "2025-12-01T00:00:00Z",
                "trades_created_ts": "2025-12-15T00:00:00Z",
            },
        )
        capture._hydrate(store, state, identities)
        capture._checkpoint_v2(store, state)
    return state["capture_id"]
