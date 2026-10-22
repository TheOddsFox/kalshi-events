from __future__ import annotations

import copy
import hashlib
import json
import threading
from collections import Counter
from contextlib import suppress

import httpx
import pytest
from conftest import EVENTS, HISTORICAL, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import cli, warehouse
from kalshi_events.cli import main
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store, json_bytes
from kalshi_events.normalize import project_pages
from kalshi_events.transport import PublicClient


class PowerLoss(BaseException):
    pass


def checksum(value):
    return hashlib.sha256(json_bytes(value)).hexdigest()


def capture_id(store):
    return next(store.path("captures").iterdir()).name


def journal_path(store, identity):
    return store.path(f"captures/{identity}/pages.jsonl")


def snapshot_path(store, identity):
    return store.path(f"captures/{identity}/state.json")


def rehash_snapshot(envelope):
    envelope["state_sha256"] = checksum(
        {"state": envelope["state"], "seen_cursors": envelope["seen_cursors"]}
    )
    envelope["snapshot_sha256"] = checksum(
        {key: value for key, value in envelope.items() if key != "snapshot_sha256"}
    )


def assert_chain_and_anchor(store, identity):
    body = journal_path(store, identity).read_bytes()
    assert body.endswith(b"\n")
    lines = body.splitlines(keepends=True)
    records = [json.loads(line) for line in lines]
    previous = "0" * 64
    for sequence, record in enumerate(records, 1):
        assert record["version"] == 2
        assert record["capture_id"] == identity
        assert record["seq"] == sequence
        assert record["prev"] == previous
        assert record["sha256"] == checksum(
            {key: value for key, value in record.items() if key != "sha256"}
        )
        previous = record["sha256"]
    assert records[0]["op"] == "start"
    envelope = json.loads(snapshot_path(store, identity).read_text())
    assert envelope["version"] == 2
    assert envelope["capture_id"] == identity
    assert envelope["state"]["journal_version"] == 2
    assert envelope["state_sha256"] == checksum(
        {"state": envelope["state"], "seen_cursors": envelope["seen_cursors"]}
    )
    assert envelope["snapshot_sha256"] == checksum(
        {key: value for key, value in envelope.items() if key != "snapshot_sha256"}
    )
    anchor = envelope["_journal"]
    anchored = records[anchor["seq"] - 1]
    assert anchored["sha256"] == anchor["sha256"]
    assert sum(map(len, lines[: anchor["seq"]])) == anchor["offset"]
    if anchored["op"] == "start":
        assert anchor["transition_count"] == 0
    else:
        assert anchored["op"] == "checkpoint"
        assert anchored["payload"]["transition_count"] == anchor["transition_count"]
        assert anchored["payload"]["state_sha256"] == envelope["state_sha256"]
    return records, envelope


def test_new_capture_has_independently_verifiable_chain_and_snapshot_anchor(store):
    state = acquisition.capture(store, client_factory=FakeClient)
    assert state["complete"]
    records, envelope = assert_chain_and_anchor(store, state["capture_id"])
    pages = [record for record in records if record["op"] == "page"]
    assert len(pages) == state["requests"]
    projected = acquisition.page_receipts(store, state)
    assert [(row["index"], row["relative"]) for row in projected] == [
        (row["payload"]["index"], row["payload"]["relative"]) for row in pages
    ]
    assert acquisition.read_capture(store, state["capture_id"]) == envelope["state"]


def test_each_committed_page_has_its_own_successful_journal_fsync(store, monkeypatch):
    from kalshi_events import common

    original = common.os.fsync
    synced = []

    def recorded_fsync(descriptor):
        result = original(descriptor)
        actual = common.os.fstat(descriptor)
        for path in store.path("captures").glob("*/pages.jsonl"):
            expected = path.stat()
            if (actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino):
                synced.append(json.loads(path.read_bytes().splitlines()[-1])["seq"])
        return result

    monkeypatch.setattr(common.os, "fsync", recorded_fsync)
    state = acquisition.capture(store, client_factory=FakeClient)
    records, _ = assert_chain_and_anchor(store, state["capture_id"])
    page_sequences = {record["seq"] for record in records if record["op"] == "page"}
    assert len(page_sequences) == state["requests"]
    assert page_sequences <= set(synced)


@pytest.mark.parametrize("transitions,anchor", [(99, 0), (100, 100), (101, 100)])
def test_periodic_snapshot_boundary_counts_domain_transitions(
    store, monkeypatch, transitions, anchor
):
    original = store.append
    committed = 0

    def stop_before_next_transition(relative, body):
        nonlocal committed
        record = json.loads(body)
        domain = record.get("op") in {"page", "control"}
        if domain and committed == transitions:
            raise PowerLoss("stop between committed transitions")
        original(relative, body)
        if domain:
            committed += 1

    def events(params):
        page = int(params.get("cursor", "0"))
        return 200, {"events": [EVENTS[0]], "cursor": str(page + 1)}

    monkeypatch.setattr(store, "append", stop_before_next_transition)
    with pytest.raises(PowerLoss):
        acquisition.capture(
            store, client_factory=lambda s, b, **_options: FakeClient(s, b, {"/events": events})
        )
    assert committed == transitions
    _, envelope = assert_chain_and_anchor(store, capture_id(store))
    assert envelope["_journal"]["transition_count"] == anchor


@pytest.mark.parametrize("tail", [b'{"seq":', b'{"version":2}', b"\xff"])
def test_only_undelimited_tail_can_be_discarded_before_resume(store, tail):
    bounded = Store(store.root, Limits(max_requests=2))
    initial = acquisition.capture(bounded, client_factory=FakeClient)
    assert initial["complete"] is False
    path = journal_path(store, initial["capture_id"])
    committed = path.read_bytes()
    path.write_bytes(committed + tail)
    resumed = acquisition.capture(store, resume=initial["capture_id"], client_factory=FakeClient)
    assert resumed["complete"]
    assert resumed["requests"] == 5
    assert journal_path(store, initial["capture_id"]).read_bytes().startswith(committed)
    assert_chain_and_anchor(store, initial["capture_id"])
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 7


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_start",
        "missing_interior",
        "bad_hash",
        "bad_sequence",
        "interior_json",
        "committed_tail",
    ],
)
def test_committed_journal_corruption_fails_closed(store, corruption):
    state = acquisition.capture(store, client_factory=FakeClient)
    path = journal_path(store, state["capture_id"])
    lines = path.read_bytes().splitlines(keepends=True)
    if corruption == "missing_start":
        del lines[0]
    elif corruption == "missing_interior":
        del lines[len(lines) // 2]
    elif corruption == "interior_json":
        lines.insert(2, b'{"broken":\n')
    elif corruption == "committed_tail":
        lines.append(b"{}\n")
    else:
        row = json.loads(lines[2])
        row["sha256" if corruption == "bad_hash" else "seq"] = (
            "0" * 64 if corruption == "bad_hash" else row["seq"] + 1
        )
        lines[2] = json_bytes(row)
    path.write_bytes(b"".join(lines))
    with pytest.raises(CatalogueError):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("encoding", ["whitespace", "duplicate_key"])
def test_committed_record_requires_canonical_bytes_not_only_a_valid_object_hash(store, encoding):
    state = acquisition.capture(store, client_factory=FakeClient)
    records, envelope = assert_chain_and_anchor(store, state["capture_id"])
    record = {
        "version": 2,
        "capture_id": state["capture_id"],
        "seq": records[-1]["seq"] + 1,
        "prev": records[-1]["sha256"],
        "op": "checkpoint",
        "payload": {
            "transition_count": envelope["_journal"]["transition_count"],
            "state_sha256": envelope["state_sha256"],
        },
    }
    record["sha256"] = checksum(record)
    path = journal_path(store, state["capture_id"])
    original = path.read_bytes()
    canonical = json_bytes(record)
    path.write_bytes(original + canonical)
    assert acquisition.read_capture(store, state["capture_id"])["complete"]
    noncanonical = (
        json.dumps(record, sort_keys=True).encode() + b"\n"
        if encoding == "whitespace"
        else b'{"version":2,' + canonical[1:]
    )
    assert json.loads(noncanonical) == record
    path.write_bytes(original + noncanonical)
    with pytest.raises(CatalogueError):
        acquisition.read_capture(store, state["capture_id"])


def test_journal_replay_obeys_operation_deadline(store):
    state = acquisition.capture(store, client_factory=FakeClient)
    expired = Store(store.root, Limits(max_duration_seconds=1))
    expired.started -= 60
    with pytest.raises(BudgetExceeded, match="deadline"):
        acquisition.read_capture(expired, state["capture_id"])


def test_committed_record_over_sixteen_mib_is_rejected_before_json_decoding(store):
    state = acquisition.capture(store, client_factory=FakeClient)
    path = journal_path(store, state["capture_id"])
    with path.open("ab") as stream:
        stream.write(b" " * (16 * 1024**2) + b"\n")
    with pytest.raises(CatalogueError, match="record.*limit"):
        acquisition.read_capture(store, state["capture_id"])


def test_control_record_size_does_not_grow_with_cursor_history(store):
    sizes = []
    with store.lock():
        for count in (0, 20_000):
            state = acquisition.new_capture(store, selected=None)
            history = {f"{index:064x}" for index in range(count)}
            state.seen_cursors = {2: history}
            candidate = copy.deepcopy(dict(state))
            candidate["tasks"].append(acquisition.task("/events/EXTRA", "events", singular=True))
            acquisition._commit_control(store, state, "hydrate", candidate)
            raw = (
                journal_path(store, state["capture_id"]).read_bytes().splitlines(keepends=True)[-1]
            )
            record = json.loads(raw)
            assert record["op"] == "control"
            assert set(record["payload"]) == {"kind", "tasks", "coverage", "state_sha256"}
            assert record["payload"]["state_sha256"] == checksum(
                {"state": dict(state), "seen_cursors": {"2": sorted(history)}}
            )
            assert len(raw) < 4096
            sizes.append(len(raw))
    assert sizes[0] == sizes[1]


@pytest.mark.parametrize("changed", ["state", "seen_cursors"])
def test_recomputed_snapshot_checksums_cannot_replace_journal_bound_state(store, changed):
    def events(params):
        return 200, {"events": [EVENTS[0]], "cursor": "" if params.get("cursor") else "next"}

    state = acquisition.capture(
        store, client_factory=lambda s, b, **_options: FakeClient(s, b, {"/events": events})
    )
    path = snapshot_path(store, state["capture_id"])
    envelope = json.loads(path.read_text())
    if changed == "state":
        envelope["state"]["tasks"][2]["restarts"] += 1
    else:
        assert any(envelope["seen_cursors"].values())
        envelope["seen_cursors"] = {}
    rehash_snapshot(envelope)
    path.write_bytes(json_bytes(envelope))
    with pytest.raises(CatalogueError):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("field", ["seq", "offset", "sha256", "transition_count"])
def test_recomputed_snapshot_cannot_move_its_journal_anchor(store, field):
    state = acquisition.capture(store, client_factory=FakeClient)
    path = snapshot_path(store, state["capture_id"])
    envelope = json.loads(path.read_text())
    anchor = envelope["_journal"]
    anchor[field] = "0" * 64 if field == "sha256" else anchor[field] + 1
    rehash_snapshot(envelope)
    path.write_bytes(json_bytes(envelope))
    with pytest.raises(CatalogueError):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("contradiction", ["sealed_setting", "repeated_page"])
def test_valid_hash_does_not_authorize_a_contradictory_transition(store, contradiction):
    state = acquisition.capture(store, client_factory=FakeClient)
    records, envelope = assert_chain_and_anchor(store, state["capture_id"])
    if contradiction == "sealed_setting":
        original = next(row for row in reversed(records) if row["op"] == "control")
        payload = copy.deepcopy(original["payload"])
        payload["max_response_bytes"] = envelope["state"]["max_response_bytes"] + 1
        operation = "control"
    else:
        original = next(row for row in records if row["op"] == "page")
        payload = original["payload"]
        operation = "page"
    record = {
        "version": 2,
        "capture_id": state["capture_id"],
        "seq": records[-1]["seq"] + 1,
        "prev": records[-1]["sha256"],
        "op": operation,
        "payload": payload,
    }
    record["sha256"] = checksum(record)
    path = journal_path(store, state["capture_id"])
    path.write_bytes(path.read_bytes() + json_bytes(record))
    assert json.loads(snapshot_path(store, state["capture_id"]).read_text()) == envelope
    with pytest.raises(CatalogueError):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("write", ["whole_record", "partial_record"])
def test_append_error_recovers_committed_or_uncommitted_page_once(store, monkeypatch, write):
    original = store.append
    interrupted = False

    def durable_then_error(relative, body):
        nonlocal interrupted
        if not interrupted and json.loads(body).get("op") == "page":
            interrupted = True
            original(relative, body if write == "whole_record" else body[: len(body) // 2])
            raise OSError("append committed before reported failure")
        original(relative, body)

    monkeypatch.setattr(store, "append", durable_then_error)
    with pytest.raises((OSError, CatalogueError)):
        acquisition.capture(store, client_factory=FakeClient)
    monkeypatch.setattr(store, "append", original)
    identity = capture_id(store)
    resumed = acquisition.capture(store, resume=identity, client_factory=FakeClient)
    assert resumed["complete"]
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 7
    assert len(acquisition.page_receipts(store, resumed)) == 7
    assert_chain_and_anchor(store, identity)


def test_budget_stop_after_durable_page_preserves_recoverable_work(store, monkeypatch):
    original = store.append
    stopped = False

    def durable_then_budget_stop(relative, body):
        nonlocal stopped
        original(relative, body)
        if not stopped and json.loads(body).get("op") == "page":
            stopped = True
            raise BudgetExceeded("stop after durable page before application")

    monkeypatch.setattr(store, "append", durable_then_budget_stop)
    initial = acquisition.capture(store, client_factory=FakeClient)
    assert stopped and initial["complete"] is False
    assert initial["budget_exhausted"] is True
    assert acquisition.read_capture(store, initial["capture_id"])["complete"] is False
    monkeypatch.setattr(store, "append", original)
    resumed = acquisition.capture(store, resume=initial["capture_id"], client_factory=FakeClient)
    assert resumed["complete"]
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 7
    assert len(acquisition.page_receipts(store, resumed)) == 7
    assert_chain_and_anchor(store, initial["capture_id"])


@pytest.mark.parametrize("boundary", ["append", "apply", "snapshot"])
def test_crash_after_each_durable_boundary_recovers_without_duplicate_requests(
    store, monkeypatch, boundary
):
    issued = []
    guard = threading.Lock()
    crashed = False

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            with guard:
                issued.append((context or {}).get("request_id"))
            return super().get(endpoint, params, context)

    def crash_once():
        nonlocal crashed
        if not crashed:
            crashed = True
            raise PowerLoss(f"crash after {boundary}")

    with monkeypatch.context() as fault:
        if boundary == "append":
            original = store.append

            def appended(relative, body):
                original(relative, body)
                if json.loads(body).get("op") == "page":
                    crash_once()

            fault.setattr(store, "append", appended)
        elif boundary == "apply":
            original = acquisition._install_page_state

            def applied(*args, **kwargs):
                original(*args, **kwargs)
                crash_once()

            fault.setattr(acquisition, "_install_page_state", applied)
        else:
            original = store.write

            def snapshotted(relative, value):
                original(relative, value)
                if (
                    relative.endswith("/state.json")
                    and value.get("version") == 2
                    and value["_journal"]["transition_count"] > 0
                ):
                    crash_once()

            fault.setattr(store, "write", snapshotted)
        with pytest.raises(PowerLoss):
            acquisition.capture(store, client_factory=RecordingClient)
    identity = capture_id(store)
    resumed = acquisition.capture(store, resume=identity, client_factory=RecordingClient)
    assert crashed and resumed["complete"]
    assert len(issued) == len(set(issued)) == 7
    assert len(acquisition.page_receipts(store, resumed)) == 7
    assert_chain_and_anchor(store, identity)


def test_raw_body_without_usable_receipt_is_retried_after_process_death(store, monkeypatch):
    from kalshi_events.common import EvidenceSpool

    issued = []
    guard = threading.Lock()
    crashed = False

    class RecordingClient(PublicClient):
        def get(self, endpoint, params=None, context=None):
            with guard:
                issued.append((context or {}).get("request_id"))
            return super().get(endpoint, params, context)

    def factory(actual_store, budget, **options):
        client = RecordingClient(actual_store, budget, **options)
        headers = dict(client.client.headers)
        client.client.close()

        def handler(request):
            endpoint = request.url.path.removeprefix("/trade-api/v2")
            status, payload = FakeClient(actual_store, budget).response(
                endpoint, dict(request.url.params)
            )
            return httpx.Response(status, stream=httpx.ByteStream(json_bytes(payload)))

        client.client = httpx.Client(
            transport=httpx.MockTransport(handler), headers=headers, trust_env=False
        )
        return client

    original = EvidenceSpool.commit

    def raw_committed(spool):
        nonlocal crashed
        original(spool)
        with guard:
            crash = not crashed
            crashed = True
        if crash:
            raise PowerLoss("raw body committed before usable receipt")

    with monkeypatch.context() as fault:
        fault.setattr(EvidenceSpool, "commit", raw_committed)
        with pytest.raises(PowerLoss):
            acquisition.capture(store, client_factory=factory)
    identity = capture_id(store)
    resumed = acquisition.capture(store, resume=identity, client_factory=factory)
    assert resumed["complete"]
    assert len(issued) == 8
    assert sorted(Counter(issued).values()) == [1, 1, 1, 1, 1, 1, 2]
    assert len(acquisition.page_receipts(store, resumed)) == 7
    assert_chain_and_anchor(store, identity)


def test_usable_receipt_orphan_is_recovered_without_reissuing_its_request(store):
    issued = []
    interrupted = False
    guard = threading.Lock()

    class OrphanedClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            nonlocal interrupted
            with guard:
                issued.append((context or {}).get("request_id"))
            relative = super().get(endpoint, params, context)
            with guard:
                crash = not interrupted
                interrupted = True
            if crash:
                raise PowerLoss("usable receipt committed before traversal attachment")
            return relative

    with pytest.raises(PowerLoss):
        acquisition.capture(store, client_factory=OrphanedClient)
    identity = capture_id(store)
    resumed = acquisition.capture(store, resume=identity, client_factory=OrphanedClient)
    assert resumed["complete"]
    assert len(issued) == len(set(issued)) == 7
    assert len(acquisition.page_receipts(store, resumed)) == 7
    assert_chain_and_anchor(store, identity)


def test_writer_resume_decodes_prefix_and_post_snapshot_suffix_bodies_once(store, monkeypatch):
    original_append = store.append
    committed = 0
    issued = []
    guard = threading.Lock()

    class PaginatedClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            with guard:
                issued.append((context or {}).get("request_id"))
            return super().get(endpoint, params, context)

        def response(self, endpoint, params):
            if endpoint == "/events":
                page = int(params.get("cursor", "0"))
                return 200, {"events": EVENTS, "cursor": str(page + 1) if page < 109 else ""}
            return super().response(endpoint, params)

    def interrupted_append(relative, body):
        nonlocal committed
        original_append(relative, body)
        if json.loads(body).get("op") == "page":
            committed += 1
            if committed == 101:
                raise PowerLoss("post-snapshot page committed before application")

    with monkeypatch.context() as fault:
        fault.setattr(store, "append", interrupted_append)
        with pytest.raises(PowerLoss):
            acquisition.capture(store, client_factory=PaginatedClient)
    identity = capture_id(store)
    assert (
        json.loads(snapshot_path(store, identity).read_text())["_journal"]["transition_count"]
        == 100
    )
    counts = Counter()
    original_read = acquisition.read_page

    def counted_read(actual_store, relative, *args, **kwargs):
        counts[relative] += 1
        return original_read(actual_store, relative, *args, **kwargs)

    with monkeypatch.context() as count_reads:
        count_reads.setattr(acquisition, "read_page", counted_read)
        resumed = acquisition.capture(store, resume=identity, client_factory=PaginatedClient)
    assert resumed["complete"]
    assert len(issued) == len(set(issued)) == 116
    expected = {row["relative"]: 1 for row in acquisition.page_receipts(store, resumed)}
    assert counts == expected
    assert_chain_and_anchor(store, identity)


@pytest.mark.parametrize("invalid", ["ticker", "combo_side"])
def test_native_validation_failure_never_commits_or_applies_the_page(store, monkeypatch, invalid):
    native = (
        {**MARKET, "ticker": "../bad"}
        if invalid == "ticker"
        else {
            **MARKET,
            "mve_selected_legs": [
                {"event_ticker": "EVENT", "market_ticker": "MARKET", "side": "maybe"}
            ],
        }
    )
    counts = Counter()
    original_read = acquisition.read_page

    def counted_read(actual_store, relative, *args, **kwargs):
        counts[relative] += 1
        return original_read(actual_store, relative, *args, **kwargs)

    with monkeypatch.context() as count_reads:
        count_reads.setattr(acquisition, "read_page", counted_read)
        with pytest.raises(CatalogueError):
            acquisition.capture(
                store,
                client_factory=lambda s, b, **_options: FakeClient(
                    s, b, {"/markets": (200, {"markets": [native], "cursor": ""})}
                ),
            )
    identity = capture_id(store)
    bad_receipt = next(
        path
        for path in store.path("raw").glob("*/manifest.json")
        if json.loads(path.read_text())["endpoint"] == "/markets"
    )
    relative = bad_receipt.relative_to(store.root).as_posix()
    state = acquisition.read_capture(store, identity)
    assert relative not in {row["relative"] for row in acquisition.page_receipts(store, state)}
    assert next(work for work in state["tasks"] if work["endpoint"] == "/markets")["done"] is False
    assert counts[relative] == 1
    assert all(value == 1 for value in counts.values())


def test_cursor_loop_seen_before_budget_stop_is_rejected_immediately_on_resume(store):
    requested = []
    blocked = False

    class PausedClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            nonlocal blocked
            cursor = (params or {}).get("cursor", "")
            if endpoint == "/events" and cursor == "B" and not blocked:
                blocked = True
                raise BudgetExceeded("pause before next cursor")
            if endpoint == "/events":
                requested.append(cursor)
            return super().get(endpoint, params, context)

        def response(self, endpoint, params):
            if endpoint == "/events":
                cursor = params.get("cursor", "")
                return 200, {"events": [EVENTS[0]], "cursor": {"": "A", "A": "B", "B": "A"}[cursor]}
            return super().response(endpoint, params)

    paused = acquisition.capture(store, client_factory=PausedClient)
    assert paused["complete"] is False
    assert requested == ["", "A"]
    with pytest.raises(CatalogueError, match="cursor"):
        acquisition.capture(store, resume=paused["capture_id"], client_factory=PausedClient)
    assert requested == ["", "A", "B"]


def test_parent_discovery_replays_128_task_batch_and_keeps_the_129th_parent(store, monkeypatch):
    original = store.append
    interrupted = False
    parents = []

    class ParentClient(FakeClient):
        def response(self, endpoint, params):
            if endpoint == "/markets":
                return 200, {
                    "markets": [
                        {**MARKET, "ticker": f"M-{index}", "event_ticker": f"P-{index}"}
                        for index in range(129)
                    ],
                    "cursor": "",
                }
            if endpoint.startswith("/events/P-"):
                parent = endpoint.rsplit("/", 1)[1]
                parents.append(parent)
                return 200, {"event": {"event_ticker": parent, "series_ticker": "SERIES"}}
            return super().response(endpoint, params)

    def committed_batch(relative, body):
        nonlocal interrupted
        original(relative, body)
        record = json.loads(body)
        if (
            not interrupted
            and record.get("op") == "control"
            and record["payload"].get("kind") == "hydrate"
            and len(record["payload"]["tasks"]) == 128
        ):
            interrupted = True
            raise PowerLoss("first parent batch committed before application")

    with monkeypatch.context() as fault:
        fault.setattr(store, "append", committed_batch)
        with pytest.raises(PowerLoss):
            acquisition.capture(store, client_factory=ParentClient)
    identity = capture_id(store)
    resumed = acquisition.capture(store, resume=identity, client_factory=ParentClient)
    assert interrupted and resumed["complete"]
    assert len(parents) == len(set(parents)) == 129
    records, _ = assert_chain_and_anchor(store, identity)
    batches = [
        record["payload"]["tasks"]
        for record in records
        if record["op"] == "control" and record["payload"].get("kind") == "hydrate"
    ]
    assert sorted(map(len, batches)) == [1, 128]
    projected = project_pages(acquisition.capture_pages(store, resumed))
    markets = [row for row in projected["markets"] if row["market_ticker"].startswith("M-")]
    assert len(markets) == 129
    assert all(row["usable"] for row in markets)


def test_resume_retains_each_cutoff_and_does_not_repeat_a_committed_cutoff_phase(store):
    cutoffs = []
    pages = []
    paused = False
    guard = threading.Lock()

    class HistoricalClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            nonlocal paused
            cursor = (params or {}).get("cursor", "")
            if endpoint == "/historical/markets" and cursor == "1" and not paused:
                paused = True
                raise BudgetExceeded("pause after cutoff before next historical page")
            return super().get(endpoint, params, context)

        def response(self, endpoint, params):
            if endpoint == "/historical/cutoff":
                with guard:
                    value = {
                        "market_settled_ts": f"2026-01-{len(cutoffs) + 1:02}T00:00:00Z",
                        "trades_created_ts": "2026-02-01T00:00:00Z",
                    }
                    cutoffs.append(value)
                return 200, value
            if endpoint == "/historical/markets":
                page = int(params.get("cursor", "0"))
                pages.append(page)
                return 200, {
                    "markets": [{**HISTORICAL, "ticker": f"HISTORICAL-{page}"}],
                    "cursor": str(page + 1) if page < 2 else "",
                }
            return super().response(endpoint, params)

    initial = acquisition.capture(store, client_factory=HistoricalClient)
    assert initial["complete"] is False
    resumed = acquisition.capture(
        store, resume=initial["capture_id"], client_factory=HistoricalClient
    )
    assert resumed["complete"]
    assert pages == [0, 1, 2]
    retained = [
        payload
        for manifest, payload in acquisition.capture_pages(store, resumed)
        if manifest["endpoint"] == "/historical/cutoff"
    ]
    assert len(retained) == len(cutoffs) == 4
    assert sorted(map(json_bytes, retained)) == sorted(map(json_bytes, cutoffs))
    projected = project_pages(acquisition.capture_pages(store, resumed))
    assert {row["market_ticker"] for row in projected["markets"]} >= {
        "HISTORICAL-0",
        "HISTORICAL-1",
        "HISTORICAL-2",
    }
    assert_chain_and_anchor(store, initial["capture_id"])


@pytest.mark.parametrize("reader", ["read_capture", "complete_capture"])
def test_standalone_read_never_truncates_undelimited_tail(store, reader):
    state = acquisition.capture(store, client_factory=FakeClient)
    path = journal_path(store, state["capture_id"])
    path.write_bytes(path.read_bytes() + b'{"uncommitted":')
    journal = path.read_bytes()
    snapshot = snapshot_path(store, state["capture_id"]).read_bytes()
    with suppress(CatalogueError):
        getattr(acquisition, reader)(store, state["capture_id"])
    assert path.read_bytes() == journal
    assert snapshot_path(store, state["capture_id"]).read_bytes() == snapshot


def test_cli_resume_rejects_competing_writer_without_changing_evidence(store, monkeypatch, capsys):
    initial = acquisition.capture(
        Store(store.root, Limits(max_requests=2)), client_factory=FakeClient
    )
    monkeypatch.setattr(acquisition, "PublicClient", FakeClient)
    with store.lock():
        before = {
            str(path.relative_to(store.root)): path.read_bytes()
            for path in store.root.rglob("*")
            if path.is_file()
        }
        assert (
            main(["--root", str(store.root), "capture", "--capture-id", initial["capture_id"]]) == 1
        )
        assert "writer" in capsys.readouterr().err
        after = {
            str(path.relative_to(store.root)): path.read_bytes()
            for path in store.root.rglob("*")
            if path.is_file()
        }
    assert after == before


def test_exiting_lock_context_releases_lock_while_store_remains_alive(store):
    with store.lock():
        pass
    competitor = Store(store.root)
    with competitor.lock():
        assert store.root == competitor.root


def test_cli_outer_lock_and_nested_capture_release_for_next_writer(store, monkeypatch, capsys):
    created = []
    original = cli.Store

    def remembered_store(*args, **kwargs):
        actual = original(*args, **kwargs)
        created.append(actual)
        return actual

    monkeypatch.setattr(cli, "Store", remembered_store)
    monkeypatch.setattr(acquisition, "PublicClient", FakeClient)
    assert main(["--root", str(store.root), "capture", "--max-requests", "2"]) == 3
    assert json.loads(capsys.readouterr().out)["complete"] is False
    assert created
    competitor = Store(store.root)
    with competitor.lock():
        assert created[0].root == competitor.root


def test_budget_stop_reason_is_persisted_and_successful_resume_clears_it(store, capsys):
    initial = acquisition.capture(
        Store(store.root, Limits(max_requests=2)), client_factory=FakeClient
    )
    identity = initial["capture_id"]
    assert initial["budget_exhausted"] is True
    assert initial["error"]
    assert acquisition.read_capture(store, identity)["error"] == initial["error"]
    assert main(["--root", str(store.root), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert (
        next(row for row in status["captures"] if row["capture_id"] == identity)["error"]
        == (initial["error"])
    )
    resumed = acquisition.capture(store, resume=identity, client_factory=FakeClient)
    assert resumed["complete"]
    assert resumed["error"] is None
    assert acquisition.read_capture(store, identity)["error"] is None
    assert main(["--root", str(store.root), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert next(row for row in status["captures"] if row["capture_id"] == identity)["error"] is None


def test_durable_genesis_recovers_when_first_snapshot_was_never_written(store, monkeypatch):
    original = store.write

    def interrupt_first_snapshot(relative, value):
        if relative.endswith("/state.json"):
            raise PowerLoss("genesis committed before initial snapshot")
        original(relative, value)

    monkeypatch.setattr(store, "write", interrupt_first_snapshot)
    with pytest.raises(PowerLoss):
        acquisition.capture(store, client_factory=FakeClient)
    identity = capture_id(store)
    assert journal_path(store, identity).exists()
    assert not snapshot_path(store, identity).exists()
    assert not list(store.path("raw").glob("*/manifest.json"))
    monkeypatch.setattr(store, "write", original)
    resumed = acquisition.capture(store, resume=identity, client_factory=FakeClient)
    assert resumed["complete"]
    assert resumed["requests"] == 7
    records, _ = assert_chain_and_anchor(store, identity)
    assert sum(record["op"] == "start" for record in records) == 1


def test_missing_completed_snapshot_replays_journal_without_new_requests(store):
    initial = acquisition.capture(store, client_factory=FakeClient)
    snapshot_path(store, initial["capture_id"]).unlink()
    resumed = acquisition.capture(store, resume=initial["capture_id"], client_factory=FakeClient)
    assert resumed["complete"]
    assert resumed["requests"] == 0
    assert_chain_and_anchor(store, initial["capture_id"])
    assert acquisition.complete_capture(store, initial["capture_id"])["complete"]


def test_existing_invalid_snapshot_never_falls_back_to_valid_journal(store):
    initial = acquisition.capture(store, client_factory=FakeClient)
    path = snapshot_path(store, initial["capture_id"])
    path.write_bytes(b'{"invalid":')
    before = journal_path(store, initial["capture_id"]).read_bytes()
    with pytest.raises(CatalogueError):
        acquisition.capture(store, resume=initial["capture_id"], client_factory=FakeClient)
    assert path.read_bytes() == b'{"invalid":'
    assert journal_path(store, initial["capture_id"]).read_bytes() == before


def test_writer_recovery_preserves_foreign_and_unowned_partial_files(store, monkeypatch, capsys):
    initial = acquisition.capture(
        Store(store.root, Limits(max_requests=2)), client_factory=FakeClient
    )
    foreign = store.path("raw/foreign/.write-body-partial")
    foreign.parent.mkdir()
    foreign.write_bytes(b"another capture's partial response")
    store.write(
        "raw/foreign/manifest.json",
        {
            "version": 1,
            "capture_id": "e" * 32,
            "status": None,
            "reserved_bytes": 4096,
            "accounting_complete": False,
        },
    )
    unowned = store.path("raw/unowned/.write-body-partial")
    unowned.parent.mkdir()
    unowned.write_bytes(b"no manifest proves ownership")
    other_temporary = store.path("tmp/.write-body-unrelated")
    other_temporary.parent.mkdir(exist_ok=True)
    other_temporary.write_bytes(b"unrelated temporary work")
    protected = {path: path.read_bytes() for path in (foreign, unowned, other_temporary)}
    monkeypatch.setattr(acquisition, "PublicClient", FakeClient)
    assert main(["--root", str(store.root), "capture", "--capture-id", initial["capture_id"]]) == 0
    capsys.readouterr()
    assert {path: path.read_bytes() for path in protected} == protected
    retained = sum(path.stat().st_size for path in store.root.rglob("*") if path.is_file())
    assert store.reconcile()["retained_bytes"] == retained


@pytest.mark.parametrize("filename", ["state.json", "pages.jsonl"])
def test_evidence_inventory_rejects_bytes_changed_after_logical_state_was_read(
    store, monkeypatch, filename
):
    state = acquisition.capture(store, client_factory=FakeClient)
    identity = state["capture_id"]
    verified = {}
    logical = acquisition.read_capture(store, identity)
    list(acquisition.capture_pages(store, logical, evidence=verified))
    original = warehouse.file_digest
    mutated = False

    def mutate_before_hash(path, *args, **kwargs):
        nonlocal mutated
        if not mutated and path.name == filename:
            mutated = True
            path.write_bytes(path.read_bytes() + (b"{}\n" if filename == "pages.jsonl" else b" "))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(warehouse, "file_digest", mutate_before_hash)
    with pytest.raises(CatalogueError):
        warehouse.evidence_inventory(store, identity, verified_pages=verified)
    assert mutated


def test_journal_mutation_during_rebuild_preserves_previous_build(store, monkeypatch):
    state = acquisition.capture(store, client_factory=FakeClient)
    identity = state["capture_id"]
    warehouse.build(store, identity)
    directory = store.path(f"builds/{identity}")
    receipt = (directory / "build.json").read_bytes()
    database = hashlib.sha256((directory / "warehouse.duckdb").read_bytes()).hexdigest()
    original = warehouse.file_digest
    mutated = False

    def mutate_before_hash(path, *args, **kwargs):
        nonlocal mutated
        if not mutated and path.name == "pages.jsonl":
            mutated = True
            path.write_bytes(path.read_bytes() + b"{}\n")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(warehouse, "file_digest", mutate_before_hash)
    with pytest.raises(CatalogueError):
        warehouse.build(store, identity)
    assert mutated
    assert (directory / "build.json").read_bytes() == receipt
    assert hashlib.sha256((directory / "warehouse.duckdb").read_bytes()).hexdigest() == database
    assert not list(store.path("tmp").glob("build-*"))


def test_build_and_certification_reject_uncommitted_tail_until_writer_repairs_it(store):
    state = acquisition.capture(store, client_factory=FakeClient)
    identity = state["capture_id"]
    warehouse.build(store, identity)
    path = journal_path(store, identity)
    path.write_bytes(path.read_bytes() + b'{"unfinished":')
    before = path.read_bytes()
    with suppress(CatalogueError):
        acquisition.read_capture(store, identity)
    assert path.read_bytes() == before
    with pytest.raises(CatalogueError):
        warehouse.build(store, identity)
    assert path.read_bytes() == before
    with pytest.raises(CatalogueError):
        warehouse.certified(store, identity)
    assert path.read_bytes() == before
    resumed = acquisition.capture(store, resume=identity, client_factory=FakeClient)
    assert resumed["complete"]
    assert resumed["requests"] == 0
    assert path.read_bytes().endswith(b"\n")
    assert_chain_and_anchor(store, identity)
    warehouse.build(store, identity)
    assert warehouse.certified(store, identity)["capture_id"] == identity
