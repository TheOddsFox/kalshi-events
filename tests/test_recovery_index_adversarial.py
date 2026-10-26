"""Disk spill must preserve parent authority and history winners during recovery."""

from __future__ import annotations

import copy

import pytest

from kalshi_events import capture, normalize
from kalshi_events.common import digest, json_bytes


def _page(kind, rows, *, page=1, received="2026-10-26T12:00:00Z", **context):
    manifest = {
        "entity_kind": kind,
        "endpoint": f"/{kind}",
        "singular": False,
        "status": 200,
        "page_id": f"{page:032x}",
        "request_id": f"{page:064x}",
        "received_at": received,
        **context,
    }
    return manifest, {kind: rows, "cursor": ""}


def _batch_state():
    policies = {
        "rate_policy": "fixed",
        "parent_policy": "batched-v1",
        "cutoff_policy": "checkpoint-v1",
    }
    state = capture._CaptureState(
        {
            "capture_id": "c" * 32,
            "complete": False,
            "selected": None,
            "incremental": False,
            "tasks": [],
            "task_scopes": {},
            "coverage": {},
            "acquisition_protocol": 5,
            **policies,
            "acquisition_policy_sha256": digest(json_bytes(policies)),
        }
    )
    state["tasks"].append(
        capture._protocol_task(
            state,
            "/events",
            "events",
            parent_batch="ordinary",
            params={"tickers": "E", "limit": 200, "with_nested_markets": "false"},
        )
    )
    return state


def _batch_page(state, *, page, received, parent, cursor=""):
    manifest, payload = _page(
        "events",
        [{"event_ticker": "E", "series_ticker": parent}],
        page=page,
        received=received,
        **capture._request_context(state, 0),
    )
    payload["cursor"] = cursor
    return manifest, payload


def _merge(index, pages):
    for manifest, payload in pages:
        page_index = normalize.ParentIndex()
        page_index.add_page(manifest, payload)
        index.merge(page_index)


def test_disk_winners_preserve_equal_timestamp_tie_breaker_and_replay_order(store):
    pages = [
        _page("events", [{"event_ticker": "E", "series_ticker": parent}], page=page)
        for page, parent in [(1, "S-A"), (2, "S-B"), (3, "S-C")]
    ]
    reference = normalize.ParentIndex()
    for manifest, payload in pages:
        reference.add_page(manifest, payload)
    expected = [(identity, *winner) for identity, winner in sorted(reference.events.items())]

    for ordered in (pages, list(reversed(pages)), pages + pages):
        index = normalize.DiskParentIndex(store)
        try:
            _merge(index, ordered)
            assert list(index.stream_winners("events")) == expected
        finally:
            index.close()


def test_final_membership_does_not_erase_newer_winner_from_previous_generation(store):
    state = {
        "tasks": [capture.task("/events", "events")],
        "task_scopes": {"0": "events_live"},
    }
    state["tasks"][0].update(done=True, restarts=1)
    pages = [
        _page(
            "events",
            [
                {"event_ticker": "E", "series_ticker": "NEWER-SERIES"},
                {"event_ticker": "REMOVED", "series_ticker": "S"},
            ],
            page=1,
            received="2026-10-26T12:00:02Z",
            task_scope="events_live",
            task_generation=0,
        ),
        _page(
            "events",
            [{"event_ticker": "E", "series_ticker": "OLDER-SERIES"}],
            page=2,
            received="2026-10-26T12:00:01Z",
            task_scope="events_live",
            task_generation=1,
        ),
    ]
    index = normalize.DiskParentIndex(store)
    try:
        _merge(index, pages)
        assert list(index.stream_membership(state)) == [("events_live", "E")]
        winners = {row[0]: row[3] for row in index.stream_winners("events")}
        assert winners == {"E": "NEWER-SERIES", "REMOVED": "S"}
    finally:
        index.close()


@pytest.mark.parametrize("terminal", [False, True])
def test_only_final_terminal_batch_establishes_authority_without_replacing_history_winner(
    store, terminal
):
    state = _batch_state()
    old_page = _batch_page(
        state, page=1, received="2026-10-26T12:00:02Z", parent="NEWER-SERIES", cursor="OLD"
    )
    state["tasks"][0]["restarts"] = 1
    final_page = _batch_page(
        state,
        page=2,
        received="2026-10-26T12:00:01Z",
        parent="OLDER-SERIES",
        cursor="" if terminal else "NEXT",
    )
    state["tasks"][0].update(done=terminal, batch_found=["E"])
    reference = normalize.ParentIndex()
    index = normalize.DiskParentIndex(store)
    try:
        for manifest, payload in (old_page, final_page):
            reference.add_page(manifest, payload)
        _merge(index, [old_page, final_page])
        _, authority = reference.effective_parent_evidence(state)
        expected = sorted(
            (kind, ticker, value[0], value[1]) for (kind, ticker), value in authority.items()
        )
        assert list(index.stream_eligibility(state)) == expected
        assert next(index.stream_winners("events"))[3] == "NEWER-SERIES"
        assert bool(expected) is terminal
    finally:
        index.close()


def test_direct_parent_404_remains_authoritative_over_terminal_batch(store):
    state = _batch_state()
    batch = _batch_page(state, page=1, received="2026-10-26T12:00:01Z", parent="S")
    state["tasks"][0].update(done=True, batch_found=["E"])
    direct = (
        {
            "entity_kind": "events",
            "endpoint": "/events/E",
            "singular": True,
            "status": 404,
            "page_id": "b" * 32,
            "request_id": "b" * 64,
            "received_at": "2026-10-26T12:00:02Z",
        },
        {},
    )
    index = normalize.DiskParentIndex(store)
    try:
        _merge(index, [batch, direct])
        assert list(index.stream_eligibility(state)) == [("events", "E", 404, "b" * 32)]
        assert next(index.stream_winners("events"))[3] == "S"
    finally:
        index.close()


def test_disk_index_close_is_idempotent_and_removes_operation_local_state(store):
    index = normalize.DiskParentIndex(store)
    path = index.path
    _merge(index, [_page("markets", [{"ticker": "M", "event_ticker": "E"}])])
    assert path.exists()
    index.close()
    index.close()
    assert not path.exists()
    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


def test_missing_parent_work_is_sorted_and_yielded_in_bounded_groups(store):
    parents = [f"E-{number:04d}" for number in reversed(range(251))]
    page = _page(
        "markets",
        [
            {"ticker": f"M-{number:04d}", "event_ticker": parent}
            for number, parent in enumerate(parents)
        ],
        task_scope="markets_live",
        task_generation=0,
    )
    state = {
        "tasks": [capture.task("/markets", "markets")],
        "task_scopes": {"0": "markets_live"},
    }
    state["tasks"][0].update(done=True)
    index = normalize.DiskParentIndex(store)
    try:
        _merge(index, [page])
        groups = list(index.missing_parent_batches(state))
        assert [kind for kind, _ in groups] == ["events"] * 3
        assert [len(values) for _, values in groups] == [100, 100, 51]
        assert [identity for _, values in groups for identity in values] == sorted(parents)
    finally:
        index.close()


def test_membership_reader_does_not_eagerly_collect_the_result_cursor(store):
    index = normalize.DiskParentIndex(store)
    _merge(
        index,
        [
            _page(
                "events",
                [
                    {"event_ticker": f"E-{number:03d}", "series_ticker": "S"}
                    for number in range(100)
                ],
                task_scope="events_live",
                task_generation=0,
            )
        ],
    )
    state = {
        "tasks": [capture.task("/events", "events")],
        "task_scopes": {"0": "events_live"},
    }
    real_db = index.db
    read = []

    class GuardedCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def __iter__(self):
            for row in self.cursor:
                read.append(row)
                assert len(read) <= 1, "first membership result eagerly consumed unrelated entities"
                yield row

        def fetchall(self):
            raise AssertionError("membership fetched the whole catalogue")

    class GuardedConnection:
        def execute(self, query, params=()):
            cursor = real_db.execute(query, params)
            return GuardedCursor(cursor) if query.lstrip().lower().startswith("select") else cursor

        def __getattr__(self, name):
            return getattr(real_db, name)

    try:
        index.db = GuardedConnection()
        stream = index.stream_membership(state)
        assert next(stream) == ("events_live", "E-000")
        assert len(read) == 1
        stream.close()
    finally:
        index.db = real_db
        index.close()


def test_disk_hydration_plan_matches_reference_with_reassigned_and_absent_parents(store):
    state = _batch_state()
    state["journal_version"] = capture.JOURNAL_VERSION
    state["tasks"][0].update(done=True, batch_found=["E"])
    batch = _batch_page(state, page=1, received="2026-10-26T12:00:00Z", parent="S")
    markets = _page(
        "markets",
        [
            {"ticker": "M-BATCH", "event_ticker": "E"},
            {"ticker": "M-UNKNOWN", "event_ticker": "UNKNOWN"},
            {"ticker": "M-ABSENT", "event_ticker": "ABSENT"},
        ],
        page=2,
    )
    absent = (
        {
            "entity_kind": "events",
            "endpoint": "/events/ABSENT",
            "singular": True,
            "status": 404,
            "page_id": "b" * 32,
            "request_id": "b" * 64,
            "received_at": "2026-10-26T12:00:01Z",
        },
        {},
    )
    reference = normalize.ParentIndex()
    index = normalize.DiskParentIndex(store)
    try:
        for manifest, payload in [batch, markets, absent]:
            reference.add_page(manifest, payload)
        _merge(index, [batch, markets, absent])
        reference_state = copy.deepcopy(state)
        disk_state = copy.deepcopy(state)
        assert capture._hydrate_pages(store, reference_state, reference)
        assert capture._hydrate_pages(store, disk_state, index)
        assert disk_state["tasks"] == reference_state["tasks"]
        added = [work["endpoint"] for work in disk_state["tasks"]][1:]
        assert added == ["/events/UNKNOWN", "/series/S"]
    finally:
        index.close()


def test_missing_parent_candidate_join_uses_parent_and_candidate_indexes(store):
    state = _batch_state()
    batch = _batch_page(state, page=1, received="2026-10-26T12:00:00Z", parent="S")
    markets = _page(
        "markets",
        [{"ticker": f"M-{number:03d}", "event_ticker": "E"} for number in range(200)],
        page=2,
    )
    index = normalize.DiskParentIndex(store)
    statements = []
    try:
        _merge(index, [batch, markets])
        index.db.set_trace_callback(statements.append)
        list(index.missing_parent_batches(state))
        index.db.set_trace_callback(None)
        candidate_query = next(
            statement
            for statement in statements
            if "insert or ignore into required_parents" in statement.lower()
            and "from candidates c" in statement.lower()
        )
        details = [
            row[3].lower() for row in index.db.execute("explain query plan " + candidate_query)
        ]
        assert any("winners_parent" in detail and "search m" in detail for detail in details)
        assert any("candidates_ticker" in detail for detail in details)
        assert not any(detail.startswith("scan m") for detail in details)
    finally:
        index.close()
