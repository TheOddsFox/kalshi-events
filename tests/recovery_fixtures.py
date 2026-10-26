"""Small deterministic protocol-5 roots for recovery tests and opt-in benchmarks."""

from __future__ import annotations

import hashlib
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
FIXTURE_MARKER = {
    "version": 1,
    "fixture": "kalshi-events-recovery",
    "capture_id": CAPTURE_ID,
    "root_id": ROOT_ID,
}


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


def _seed_many_pages(store: Store, state: dict[str, Any], index: int, count: int) -> None:
    """Write a linear-size synthetic receipt prefix for the opt-in metadata scale case."""
    journal = store.path(capture._journal(state))
    raw_root = store.path("raw")
    raw_root.mkdir(exist_ok=True)
    head = dict(state.journal_head)
    with journal.open("ab") as stream:
        for page in range(count):
            work = state["tasks"][index]
            endpoint, params, _, _ = capture._plan(work, capture._phase(work))
            context = capture._request_context(state, index)
            payload = {
                "events": [
                    {
                        "event_ticker": f"EVENT-{page:06d}",
                        "series_ticker": "SERIES",
                    }
                ],
                "cursor": CURSOR if page == 0 else f"{CURSOR}-{page + 1}",
            }
            body = json_bytes(payload)
            page_id = context["request_id"][:32]
            directory = raw_root / page_id
            directory.mkdir()
            body_path = directory / "body.bin"
            manifest_path = directory / "manifest.json"
            # shortcut: synthetic files skip per-file fsync; use only in disposable benchmark roots.
            body_path.write_bytes(body)
            manifest = {
                **context,
                "page_id": page_id,
                "version": 1,
                "endpoint": endpoint,
                "params": params,
                "received_at": FIXED_TIME,
                "status": 200,
                "size": len(body),
                "sha256": digest(body),
                "downloaded_bytes": len(body),
                "accounting_complete": True,
                "reserved_bytes": len(body),
            }
            manifest_path.write_bytes(json_bytes(manifest))
            relative = manifest_path.relative_to(store.root).as_posix()
            record = {
                "version": capture.JOURNAL_VERSION,
                "capture_id": state["capture_id"],
                "seq": head["seq"] + 1,
                "prev": head["sha256"],
                "op": "page",
                "payload": {
                    "index": index,
                    "relative": relative,
                    "request_id": context["request_id"],
                },
            }
            record["sha256"] = capture._journal_checksum(record)
            encoded = json_bytes(record)
            stream.write(encoded)
            head.update(
                seq=record["seq"],
                offset=head["offset"] + len(encoded),
                sha256=record["sha256"],
                transition_count=head["transition_count"] + 1,
            )
            change = capture._derive_page_change(
                state, state.seen_cursors, index, manifest, payload
            )
            capture._apply_page_change(state, state.seen_cursors, index, change)
            state.page_rows.append(
                {"index": index, "relative": relative, "request_id": context["request_id"]}
            )
            state.processed.add(relative)
            state.request_ids.add(context["request_id"])
    store.reconcile()
    state.journal_head = head
    state.journal_anchor_state_sha256 = None
    capture._checkpoint_v2(store, state)


def _mark_fixture(
    store: Store,
    state: dict[str, Any],
    *,
    scenario: str,
    repeats: int,
    num_pages: int,
    wide_bytes: int,
    hydration_tasks: int,
) -> None:
    journal = store.path(capture._journal(state))
    marker = {
        **FIXTURE_MARKER,
        "scenario": scenario,
        "repeats": repeats,
        "num_pages": num_pages,
        "wide_bytes": wide_bytes,
        "hydration_tasks": hydration_tasks if scenario == "hydration" else None,
        "journal_sha256": hashlib.sha256(journal.read_bytes()).hexdigest(),
    }
    store.write(".recovery-fixture.json", marker)


def build_recovery_root(
    root: Path,
    scenario: str,
    *,
    repeats: int = 2,
    num_pages: int = 1,
    wide_bytes: int = 0,
    hydration_tasks: int = 1,
) -> str:
    """Build a fresh root with real protocol-5 journal transitions and receipts."""
    if scenario not in {"primary", "hydration"}:
        raise ValueError("scenario must be 'primary' or 'hydration'")
    if isinstance(repeats, bool) or not isinstance(repeats, int) or not 0 <= repeats <= 3:
        raise ValueError("repeats must be between zero and the cursor reset limit")
    if (
        isinstance(num_pages, bool)
        or not isinstance(num_pages, int)
        or num_pages < 1
        or num_pages * (repeats + 1) + repeats > 163_000
    ):
        raise ValueError("num_pages must fit in the 163,000-page fixture ceiling")
    if num_pages > 1000 and repeats:
        raise ValueError("large metadata fixtures require repeats=0")
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
            if repeats == 0 and num_pages > 1000:
                _seed_many_pages(store, state, index, num_pages)
                _mark_fixture(
                    store,
                    state,
                    scenario=scenario,
                    repeats=repeats,
                    num_pages=num_pages,
                    wide_bytes=wide_bytes,
                    hydration_tasks=hydration_tasks,
                )
                return state["capture_id"]
            for generation in range(repeats + 1):
                for page in range(num_pages):
                    row = {
                        "event_ticker": f"EVENT-G{generation:04d}-P{page:06d}",
                        "series_ticker": "SERIES",
                        "title": "Recovery fixture" + ("x" * wide_bytes),
                    }
                    _receipt(
                        store,
                        state,
                        index,
                        identities,
                        200,
                        {
                            "events": [row],
                            "cursor": CURSOR if page == 0 else f"{CURSOR}-{page + 1}",
                        },
                    )
                if generation < repeats:
                    _receipt(store, state, index, identities, 400, {"error": "cursor reset"})
            capture._checkpoint_v2(store, state)
            _mark_fixture(
                store,
                state,
                scenario=scenario,
                repeats=repeats,
                num_pages=num_pages,
                wide_bytes=wide_bytes,
                hydration_tasks=hydration_tasks,
            )
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
        for page in range(num_pages):
            _receipt(
                store,
                state,
                2,
                identities,
                200,
                {
                    "events": [
                        {
                            "event_ticker": f"EVENT-PRIMARY-{page:06d}",
                            "series_ticker": "SERIES",
                        }
                    ],
                    "cursor": f"{CURSOR}-{page + 1}" if page + 1 < num_pages else "",
                },
            )
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
        _mark_fixture(
            store,
            state,
            scenario=scenario,
            repeats=repeats,
            num_pages=num_pages,
            wide_bytes=wide_bytes,
            hydration_tasks=hydration_tasks,
        )
    return state["capture_id"]
