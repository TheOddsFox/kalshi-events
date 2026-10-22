"""Abrupt process exits across actual streamed capture durability boundaries."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import conftest as fixtures
import pyarrow.parquet as pq
import pytest
from conftest import COMBO, COMBO_EVENTS, EVENTS, HISTORICAL, MARKET, SERIES, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import publish as publishing
from kalshi_events import warehouse
from kalshi_events.common import Store
from kalshi_events.normalize import observations
from kalshi_events.transport import accounting

CRASH_EXIT = 86
RECEIVED_AT = "2026-01-02T12:00:00+00:00"


@pytest.fixture(scope="module")
def last_good_capture_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("capture-process-baseline") / "root"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fixtures, "now", lambda: "2026-01-01T12:00:00+00:00")
        patch.setattr(acquisition, "now", lambda: "2026-01-01T12:00:00+00:00")
        state = acquisition.capture(Store(root), client_factory=FakeClient)
    warehouse.build(Store(root), state["capture_id"])
    release = publishing.publish(Store(root), state["capture_id"])
    return root, state["capture_id"], release["source_release_id"]


CAPTURE_SCRIPT = r"""
import json, os, sys, threading
from pathlib import Path
import httpx
from conftest import FakeClient
from kalshi_events import capture as acquisition
from kalshi_events import transport
from kalshi_events.common import EvidenceSpool, Limits, Store, json_bytes

root, request_log, boundary = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
resume = sys.argv[4] if len(sys.argv) > 4 else None
acquisition.now = transport.now = lambda: '2026-01-02T12:00:00+00:00'
# Isolate one durability boundary from unrelated interrupted in-flight reservations.
acquisition.MAX_CONCURRENT_REQUESTS = 1
store = Store(root, Limits(max_rate=100_000, max_response_bytes=1024**2,
                          max_download_bytes=16 * 1024**2))
local = threading.local()
log_lock = threading.Lock()

class Chunks(httpx.SyncByteStream):
    def __init__(self, body):
        self.body = body
    def __iter__(self):
        yield self.body[:5]
        yield self.body[5:13]
        yield self.body[13:]

class Client(transport.PublicClient):
    def get(self, endpoint, params=None, context=None):
        local.request_id = (context or {})['request_id']
        return super().get(endpoint, params, context)

def factory(actual_store, budget, **options):
    client = Client(actual_store, budget, **options)
    headers = dict(client.client.headers)
    client.client.close()
    def handler(request):
        endpoint = request.url.path.removeprefix('/trade-api/v2')
        with log_lock, request_log.open('ab') as stream:
            stream.write(json_bytes({'request_id': local.request_id, 'endpoint': endpoint,
                                     'params': dict(request.url.params)}))
            stream.flush()
            os.fsync(stream.fileno())
        status, payload = FakeClient(actual_store, budget).response(endpoint, dict(request.url.params))
        return httpx.Response(status, stream=Chunks(json_bytes(payload)))
    client.client = httpx.Client(transport=httpx.MockTransport(handler), headers=headers,
                                trust_env=False, follow_redirects=False)
    return client

commit, write, append = EvidenceSpool.commit, Store.write, Store.append
install, replace = acquisition._install_page_state, os.replace

def committed(spool):
    result = commit(spool)
    if boundary == 'body':
        os._exit(86)
    return result

def written(self, relative, value):
    result = write(self, relative, value)
    if (boundary == 'receipt' and relative.startswith('raw/')
            and value.get('accounting_complete') is True and value.get('status') == 200):
        os._exit(86)
    return result

def appended(self, relative, body):
    result = append(self, relative, body)
    if boundary == 'journal' and relative.endswith('/pages.jsonl') and json.loads(body).get('op') == 'page':
        os._exit(86)
    return result

def installed(*args, **kwargs):
    result = install(*args, **kwargs)
    if boundary == 'apply':
        os._exit(86)
    return result

def replaced(source, destination, *args, **kwargs):
    result = replace(source, destination, *args, **kwargs)
    destination = Path(destination)
    if boundary == 'snapshot' and destination.name == 'state.json':
        envelope = json.loads(destination.read_bytes())
        if envelope.get('_journal', {}).get('transition_count', 0) > 0:
            os._exit(86)
    return result

EvidenceSpool.commit, Store.write, Store.append = committed, written, appended
acquisition._install_page_state, os.replace = installed, replaced
state = acquisition.capture(store, resume=resume, client_factory=factory)
assert state['complete']
if boundary != 'resume':
    raise AssertionError('requested crash boundary was not reached: ' + boundary)
print(json.dumps({'capture_id': state['capture_id'], 'complete': state['complete'],
                  'requests': state['requests']}))
"""


def _capture_process(root, request_log, boundary, *, resume=None):
    project = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(project / "src"), str(project / "tests"), environment.get("PYTHONPATH", "")]
    )
    arguments = [sys.executable, "-c", CAPTURE_SCRIPT, str(root), str(request_log), boundary]
    if resume is not None:
        arguments.append(resume)
    return subprocess.run(arguments, env=environment, text=True, capture_output=True, timeout=60)


def _assert_full_release(release, capture_id):
    directory = Path(release["output"])
    expected = {
        "series": {row["ticker"]: row for row in SERIES},
        "events": {row["event_ticker"]: row for row in [*EVENTS, *COMBO_EVENTS]},
        "markets": {row["ticker"]: row for row in [MARKET, COMBO, HISTORICAL]},
    }
    history = pq.read_table(directory / "observation_history.parquet").to_pylist()
    assert len(history) == 7
    history_by_id = {row["observation_id"]: row for row in history}
    assert len(history_by_id) == len(history)
    for kind, key in (
        ("series", "series_ticker"),
        ("events", "event_ticker"),
        ("markets", "market_ticker"),
    ):
        rows = pq.read_table(directory / f"{kind}.parquet").to_pylist()
        assert {row[key] for row in rows} == set(expected[kind])
        assert len(rows) == len(expected[kind])
        for row in rows:
            assert json.loads(row["payload_json"]) == expected[kind][row[key]]
            original = history_by_id[row["observation_id"]]
            assert row["capture_id"] == original["capture_id"] == capture_id
            assert row["received_at"] == original["received_at"] == RECEIVED_AT
            for field in ("provider_updated_at", "source_endpoint", "payload_json"):
                assert row[field] == original[field]
    market = next(
        row
        for row in pq.read_table(directory / "markets.parquet").to_pylist()
        if row["market_ticker"] == "MARKET"
    )
    assert market["last_price_dollars"] == Decimal("0.00123")
    assert market["volume_fp"] == Decimal("12.3456789")
    assert market["open_interest_fp"] == Decimal("2.125")
    legs = pq.read_table(directory / "combo_legs.parquet").to_pylist()
    assert len(legs) == 1
    assert (legs[0]["combo_market_ticker"], legs[0]["event_ticker"], legs[0]["market_ticker"]) == (
        "COMBO-MARKET",
        "EVENT",
        "MARKET",
    )
    assert legs[0]["resolved"] and legs[0]["side"] == "yes"
    assert legs[0]["yes_settlement_value_dollars"] == Decimal("0.00001")
    relationships = pq.read_table(directory / "relationships.parquet").to_pylist()
    assert len(relationships) == 6 and all(row["resolved"] for row in relationships)
    assert {
        (row["parent_kind"], row["parent_ticker"], row["child_kind"], row["child_ticker"])
        for row in relationships
    } == {
        *(("series", "SERIES", "events", row["event_ticker"]) for row in [*EVENTS, *COMBO_EVENTS]),
        *(
            ("events", row["event_ticker"], "markets", row["ticker"])
            for row in [MARKET, COMBO, HISTORICAL]
        ),
    }


@pytest.mark.parametrize("boundary", ["body", "receipt", "journal", "apply", "snapshot"])
def test_process_exit_capture_resumes_from_durable_evidence(
    last_good_capture_root, tmp_path, boundary
):
    source, baseline_capture, baseline_release = last_good_capture_root
    root = tmp_path / "root"
    shutil.copytree(source, root)
    request_log = tmp_path / "requests.jsonl"
    pointer = (root / "current.json").read_bytes()
    result = _capture_process(root, request_log, boundary)
    assert result.returncode == CRASH_EXIT, result.stdout + result.stderr
    identities = {path.name for path in (root / "captures").iterdir()} - {baseline_capture}
    assert len(identities) == 1
    capture_id = identities.pop()
    journal = (root / "captures" / capture_id / "pages.jsonl").read_bytes().splitlines()
    committed_pages = sum(json.loads(line)["op"] == "page" for line in journal)
    assert (
        committed_pages
        == {
            "body": 0,
            "receipt": 0,
            "journal": 1,
            "apply": 1,
            "snapshot": 7,
        }[boundary]
    )
    assert (root / "current.json").read_bytes() == pointer
    assert publishing.current(Store(root))["source_release_id"] == baseline_release
    attempts = [
        (path, json.loads(path.read_bytes()))
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == capture_id
    ]
    assert attempts
    before_requests = [json.loads(line) for line in request_log.read_bytes().splitlines()]
    before_wire_ids = {row["request_id"] for row in before_requests}
    usable_ids = {
        attempt["request_id"]
        for _, attempt in attempts
        if attempt.get("accounting_complete") is True and attempt.get("status") == 200
    }
    interrupted_ids = before_wire_ids - usable_ids
    if boundary == "body":
        assert len(attempts) == 1
        path, reservation = attempts[0]
        assert reservation["accounting_complete"] is False
        assert (path.parent / "body.bin").is_file()
        assert interrupted_ids == {reservation["request_id"]}
        assert (
            accounting(Store(root))["downloaded_or_reserved_bytes"] >= reservation["reserved_bytes"]
        )
    resumed = _capture_process(root, request_log, "resume", resume=capture_id)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert json.loads(resumed.stdout)["complete"]
    requested = [json.loads(line) for line in request_log.read_bytes().splitlines()]
    counts = Counter(row["request_id"] for row in requested)
    assert len(counts) == 7
    assert all(counts[request_id] == 1 for request_id in usable_ids)
    assert {request_id for request_id, count in counts.items() if count > 1} == interrupted_ids
    assert all(
        count == (2 if request_id in interrupted_ids else 1) for request_id, count in counts.items()
    )
    if boundary == "body":
        assert json.loads(path.read_bytes()) == reservation
        assert counts[reservation["request_id"]] == 2
        assert accounting(Store(root))["accounting_complete"] is False
        assert (
            accounting(Store(root))["downloaded_or_reserved_bytes"] >= reservation["reserved_bytes"]
        )
    assert (root / "current.json").read_bytes() == pointer
    state = acquisition.complete_capture(Store(root), capture_id)
    pages = list(acquisition.capture_pages(Store(root), state))
    assert len(pages) == 7
    page_ids = {manifest["page_id"] for manifest, _ in pages}
    assert len(page_ids) == 7
    for _, attempt in attempts:
        assert (attempt["page_id"] in page_ids) == attempt["accounting_complete"]
    assert len(list(observations(pages))) == 7
    warehouse.build(Store(root), capture_id)
    warehouse.certified(Store(root), capture_id)
    assert (root / "current.json").read_bytes() == pointer
    release = publishing.publish(Store(root), capture_id)
    assert publishing.current(Store(root))["source_release_id"] == release["source_release_id"]
    _assert_full_release(release, capture_id)
