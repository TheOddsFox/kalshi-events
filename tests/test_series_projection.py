"""Complete scoped reconciliation replaces current membership without erasing history."""

from __future__ import annotations

import duckdb
import pytest

from kalshi_events import capture, warehouse
from kalshi_events.common import Store, series_scope
from kalshi_events.normalize import ParentIndex


def _state(*, scoped=True, restarts=None):
    state = {
        "capture_id": "b" * 32,
        "incremental": True,
        "tasks": [
            capture.task("/series/KXCPI", "series", singular=True)
            if scoped
            else capture.task("/series", "series"),
            capture.task("/events", "events"),
            capture.task("/markets", "markets", params={"mve_filter": "exclude"}),
            capture.task("/historical/markets", "markets"),
        ],
        "task_scopes": {
            "0": "series",
            "1": "events_live",
            "2": "markets_live",
            "3": "markets_history",
        },
    }
    if scoped:
        state["scope"] = series_scope("KXCPI")
    for task_index, scope in state["task_scopes"].items():
        state["tasks"][int(task_index)]["done"] = True
        state["tasks"][int(task_index)]["restarts"] = (restarts or {}).get(scope, 0)
    return state


def _index(state, generations):
    index = ParentIndex()
    for task_index, scope in state["task_scopes"].items():
        work = state["tasks"][int(task_index)]
        for generation, identities in generations.get(scope, {}).items():
            kind = work["kind"]
            if kind == "series":
                rows = [{"ticker": identity} for identity in sorted(identities)]
            elif kind == "events":
                rows = [
                    {"event_ticker": identity, "series_ticker": "KXCPI"}
                    for identity in sorted(identities)
                ]
            else:
                rows = [
                    {"ticker": identity, "event_ticker": "E-CURRENT"}
                    for identity in sorted(identities)
                ]
            manifest = {
                "entity_kind": kind,
                "endpoint": work["endpoint"],
                "singular": work["singular"],
                "status": 200,
                "page_id": f"{int(task_index) * 100 + generation:032x}",
                "received_at": "2026-10-20T12:00:00Z",
                "task_scope": scope,
                "task_generation": generation,
            }
            if state.get("scope") is not None:
                manifest["series_scope"] = state["scope"]
            envelope = "event" if work["singular"] and kind == "events" else kind
            index.add_page(manifest, {envelope: rows[0] if work["singular"] else rows})
    return index


def _memberships(**values):
    return {scope: set(values.get(scope, ())) for scope in warehouse.SCOPE_KIND}


@pytest.fixture
def connection():
    with duckdb.connect() as connection:
        connection.execute("create schema landing")
        connection.execute(
            "create table landing.observations "
            "(observation_id varchar primary key, entity_kind varchar, entity_ticker varchar)"
        )
        connection.execute(
            "insert into landing.observations values "
            "('old-observation', 'markets', 'M-OLD'), "
            "('keep-observation', 'markets', 'M-KEEP')"
        )
        yield connection


@pytest.mark.parametrize("scoped", [True, False])
def test_complete_scoped_refresh_replaces_live_membership_but_ordinary_delta_unions(
    tmp_path, connection, scoped
):
    store = Store(tmp_path / "root", series_ticker="KXCPI" if scoped else None)
    state = _state(scoped=scoped)
    previous = _memberships(
        series={"KXCPI"},
        events_live={"E-OLD", "E-KEEP"},
        markets_live={"M-OLD", "M-KEEP"},
        markets_history={"H-OLD"},
    )
    warehouse._write_membership(connection, previous)
    index = _index(
        state,
        {
            "series": {0: {"KXCPI"}},
            "events_live": {0: {"E-KEEP", "E-NEW"}},
            "markets_live": {0: {"M-KEEP", "M-NEW"}},
            "markets_history": {0: {"H-NEW"}},
        },
    )
    history = connection.execute("select * from landing.observations order by 1").fetchall()

    membership, changes = warehouse._projection_membership(store, state, index, connection)

    expected_events = {"E-KEEP", "E-NEW"} if scoped else {"E-OLD", "E-KEEP", "E-NEW"}
    expected_markets = {"M-KEEP", "M-NEW"} if scoped else {"M-OLD", "M-KEEP", "M-NEW"}
    assert membership == _memberships(
        series={"KXCPI"},
        events_live=expected_events,
        markets_live=expected_markets,
        markets_history={"H-NEW"},
    )
    assert warehouse._read_membership(connection) == membership
    assert changes["events_live"] == ({"E-OLD", "E-NEW"} if scoped else {"E-NEW"})
    assert changes["markets_live"] == ({"M-OLD", "M-NEW"} if scoped else {"M-NEW"})
    assert changes["markets_history"] == {"H-OLD", "H-NEW"}
    assert connection.execute("select * from landing.observations order by 1").fetchall() == history


def test_empty_scoped_refresh_removes_membership_and_later_reappearance_restores_it(
    tmp_path, connection
):
    store = Store(tmp_path / "root", series_ticker="KXCPI")
    state = _state()
    warehouse._write_membership(
        connection,
        _memberships(series={"KXCPI"}, events_live={"E-OLD"}, markets_live={"M-OLD"}),
    )
    history = connection.execute("select * from landing.observations order by 1").fetchall()
    empty = _index(state, {"series": {0: {"KXCPI"}}})

    membership, changes = warehouse._projection_membership(store, state, empty, connection)

    assert membership == _memberships(series={"KXCPI"})
    assert changes["events_live"] == {"E-OLD"}
    assert changes["markets_live"] == {"M-OLD"}
    reappeared = _index(
        state,
        {
            "series": {0: {"KXCPI"}},
            "events_live": {0: {"E-OLD"}},
            "markets_live": {0: {"M-OLD"}},
        },
    )
    membership, changes = warehouse._projection_membership(store, state, reappeared, connection)
    assert membership["markets_live"] == {"M-OLD"}
    assert changes["events_live"] == {"E-OLD"}
    assert changes["markets_live"] == {"M-OLD"}
    assert connection.execute("select * from landing.observations order by 1").fetchall() == history


@pytest.mark.parametrize("both_tiers", [False, True])
def test_market_tier_handoff_keeps_combined_membership_without_duplicate_identity(
    tmp_path, connection, both_tiers
):
    store = Store(tmp_path / "root", series_ticker="KXCPI")
    state = _state()
    warehouse._write_membership(
        connection,
        _memberships(series={"KXCPI"}, events_live={"E-CURRENT"}, markets_live={"M-MOVED"}),
    )
    index = _index(
        state,
        {
            "series": {0: {"KXCPI"}},
            "events_live": {0: {"E-CURRENT"}},
            "markets_live": {0: {"M-MOVED"} if both_tiers else set()},
            "markets_history": {0: {"M-MOVED"}},
        },
    )

    membership, _ = warehouse._projection_membership(store, state, index, connection)

    assert membership["markets_live"] == ({"M-MOVED"} if both_tiers else set())
    assert membership["markets_history"] == {"M-MOVED"}
    assert connection.execute(
        "select distinct entity_ticker from landing.membership where entity_kind = 'markets'"
    ).fetchall() == [("M-MOVED",)]


def test_final_cursor_generation_alone_defines_scoped_current_membership(tmp_path, connection):
    store = Store(tmp_path / "root", series_ticker="KXCPI")
    state = _state(restarts={"events_live": 1, "markets_live": 1})
    warehouse._write_membership(
        connection,
        _memberships(series={"KXCPI"}, events_live={"E-OLD"}, markets_live={"M-OLD"}),
    )
    history = connection.execute("select * from landing.observations order by 1").fetchall()
    index = _index(
        state,
        {
            "series": {0: {"KXCPI"}},
            "events_live": {0: {"E-OLD"}, 1: {"E-NEW"}},
            "markets_live": {0: {"M-OLD"}, 1: {"M-NEW"}},
        },
    )

    membership, changes = warehouse._projection_membership(store, state, index, connection)

    assert membership["events_live"] == {"E-NEW"}
    assert membership["markets_live"] == {"M-NEW"}
    assert changes["events_live"] == {"E-OLD", "E-NEW"}
    assert changes["markets_live"] == {"M-OLD", "M-NEW"}
    assert connection.execute("select * from landing.observations order by 1").fetchall() == history
