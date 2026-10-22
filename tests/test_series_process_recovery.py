"""Real process exits must preserve scoped evidence, publication and recovery pins."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

import conftest
import duckdb
import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, incremental, publish, recovery, warehouse
from kalshi_events.common import Limits, Store, json_bytes
from kalshi_events.normalize import observations
from kalshi_events.transport import accounting

CRASH_EXIT = 86
SERIES = {"ticker": "KXCPI", "title": "Consumer Price Index"}
EVENT = {"event_ticker": "E1", "series_ticker": "KXCPI"}
OTHER_EVENT = {"event_ticker": "E2", "series_ticker": "KXCPI"}
MARKET = {
    "ticker": "M1",
    "event_ticker": "E1",
    "status": "active",
    "last_price_dollars": "0.00123",
    "volume_fp": "12.3456789",
    "open_interest_fp": "2.125",
}
OTHER_MARKET = {**MARKET, "ticker": "M2", "event_ticker": "E2"}
HISTORICAL = {
    **MARKET,
    "ticker": "H1",
    "status": "finalized",
    "result": "yes",
    "settlement_value_dollars": "1.0000",
}


def source_response(endpoint, params, *, baseline=False):
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": [EVENT, OTHER_EVENT] if baseline else [EVENT], "cursor": ""}),
        "/markets": (
            200,
            {"markets": [MARKET, OTHER_MARKET] if baseline else [MARKET], "cursor": ""},
        ),
        "/historical/markets": (200, {"markets": [HISTORICAL], "cursor": ""}),
    }
    assert endpoint in responses, f"unexpected scoped endpoint: {endpoint} {params}"
    return responses[endpoint]


def _capture_at(root, day, *, baseline=False, incremental_mode=False, max_requests=25_000):
    timestamp = f"2026-01-{day:02d}T12:00:00Z"

    class Client(FakeClient):
        def response(self, endpoint, params):
            return source_response(endpoint, params, baseline=baseline)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(conftest, "now", lambda: timestamp)
        patch.setattr(capture, "now", lambda: timestamp)
        return capture.capture(
            Store(root, Limits(max_requests=max_requests)),
            incremental=incremental_mode,
            client_factory=Client,
        )


def _process(root, identities, action, boundary, *, request_log=None, resume=None):
    project = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(project / "src"), str(project / "tests"), environment.get("PYTHONPATH")])
    )
    arguments = [
        sys.executable,
        "-c",
        PROCESS_SCRIPT,
        str(root),
        json.dumps(identities),
        action,
        boundary,
        str(request_log) if request_log else "",
        resume or "",
    ]
    return subprocess.run(arguments, env=environment, text=True, capture_output=True, timeout=120)


PROCESS_SCRIPT = r"""
import json, os, shutil, sys, threading
from pathlib import Path
import httpx
from test_series_process_recovery import source_response
from kalshi_events import capture, publish, recovery, transport
from kalshi_events.common import EvidenceSpool, Limits, Store, json_bytes

root = Path(sys.argv[1])
ids = json.loads(sys.argv[2])
action, boundary, request_log, resume = sys.argv[3:7]
capture.now = transport.now = lambda: '2026-01-06T12:00:00Z'
# The second worker sends its request while the main thread journals the prior page.
capture.MAX_CONCURRENT_REQUESTS = 2 if boundary == 'journal-inflight' else 1
store = Store(root, Limits(max_rate=100_000, max_response_bytes=1024**2,
                          max_download_bytes=16*1024**2))
local = threading.local()
log_lock = threading.Lock()
inflight_started = threading.Event()
inflight_release = threading.Event()
inflight_finished = threading.Event()

class Chunks(httpx.SyncByteStream):
    def __init__(self, body):
        self.body = body
    def __iter__(self):
        yield self.body[:5]
        yield self.body[5:13]
        yield self.body[13:]

class Client(transport.PublicClient):
    def get(self, endpoint, params=None, context=None):
        local.request_id = context['request_id']
        return super().get(endpoint, params, context)

def factory(actual_store, budget, **options):
    client = Client(actual_store, budget, **options)
    headers = dict(client.client.headers)
    client.client.close()
    def handler(request):
        endpoint = request.url.path.removeprefix('/trade-api/v2')
        with log_lock, Path(request_log).open('ab') as stream:
            stream.write(json_bytes({'request_id': local.request_id, 'endpoint': endpoint}))
            stream.flush()
            os.fsync(stream.fileno())
        if boundary == 'journal-inflight' and endpoint == '/series/KXCPI':
            inflight_started.set()
            try:
                assert inflight_release.wait(timeout=5), 'journal did not exit during the in-flight request'
            finally:
                inflight_finished.set()
        status, payload = source_response(endpoint, dict(request.url.params))
        return httpx.Response(status, stream=Chunks(json_bytes(payload)))
    client.client = httpx.Client(transport=httpx.MockTransport(handler), headers=headers,
                                trust_env=False, follow_redirects=False)
    return client

commit, write, append = EvidenceSpool.commit, Store.write, Store.append
install, rename, replace, remove = capture._apply_page_change, os.rename, os.replace, shutil.rmtree
retired = None
metadata_target = None
metadata_source = None
metadata_name = None
obsolete = root / 'captures' / ids['obsolete'] if 'obsolete' in ids else None
current_build = root / 'builds-v2' / ids['delta'] if 'delta' in ids else None
if boundary.startswith('prune-metadata-'):
    raw = next(path.parent for path in (root / 'raw').glob('*/manifest.json')
               if json.loads(path.read_bytes()).get('capture_id') == ids['obsolete'])
    metadata_source, metadata_name = {
        'capture': (obsolete, 'state.json'),
        'raw': (raw, 'manifest.json'),
        'build': (root / 'builds-v2' / ids['obsolete'], 'build.json'),
        'release': (root / 'releases' / ids['obsolete_release'], 'manifest.json'),
    }[boundary.removeprefix('prune-metadata-')]

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
    if (boundary in {'journal', 'journal-inflight'} and relative.endswith('/pages.jsonl')
            and json.loads(body).get('op') == 'page'):
        if boundary == 'journal-inflight':
            assert inflight_started.wait(timeout=5), 'queued request did not reach its durable wire log'
            assert not inflight_finished.is_set(), 'queued request finished before the journal exit'
        os._exit(86)
    return result

def installed(*args, **kwargs):
    result = install(*args, **kwargs)
    if boundary == 'apply':
        os._exit(86)
    return result

def moved(operation, source, destination, *args, **kwargs):
    global retired, metadata_target
    source, destination = Path(source), Path(destination)
    result = operation(source, destination, *args, **kwargs)
    if source == obsolete:
        retired = destination
    if source == metadata_source:
        metadata_target = destination
    should_crash = (
        (boundary == 'build-backup' and source == current_build)
        or (boundary == 'build-install' and destination == current_build)
        or (boundary == 'release-install' and destination.parent == root / 'releases')
        or (boundary == 'pointer-install' and destination == root / 'current-series.json')
        or (boundary == 'prune-plan' and destination == root / 'prune-plan.json')
        or (boundary == 'prune-retire' and source == obsolete)
    )
    if boundary == 'snapshot' and destination.name == 'state.json':
        envelope = json.loads(destination.read_bytes())
        should_crash = envelope.get('_journal', {}).get('transition_count', 0) > 0
    if should_crash:
        os._exit(86)
    return result

def removed(path, *args, **kwargs):
    if metadata_target is not None and Path(path) == metadata_target:
        identity = metadata_target / metadata_name
        assert identity.is_file()
        identity.unlink()
        os._exit(86)
    if boundary == 'prune-delete' and retired is not None and Path(path) == retired:
        files = sorted(path for path in retired.rglob('*') if path.is_file())
        assert files
        files[0].unlink()
        os._exit(86)
    return remove(path, *args, **kwargs)

EvidenceSpool.commit, Store.write, Store.append = committed, written, appended
capture._apply_page_change = installed
os.rename = lambda *args, **kwargs: moved(rename, *args, **kwargs)
os.replace = lambda *args, **kwargs: moved(replace, *args, **kwargs)
shutil.rmtree = removed
if action == 'capture':
    result = capture.capture(store, resume=resume or None, client_factory=factory)
    assert result['complete']
elif action == 'build':
    result = recovery.rebuild(store, catalogue_version=2)
else:
    result = publish.publish(store, ids['candidate'], catalogue_version=2)
if boundary != 'resume':
    raise AssertionError('requested process-exit boundary was not reached: ' + boundary)
print(json.dumps({'capture_id': result['capture_id'], 'complete': True}))
"""


@pytest.fixture(scope="module")
def certified_series_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("series-process") / "root"
    Store(root, series_ticker="KXCPI")
    baseline = _capture_at(root, 1, baseline=True)
    publish.publish(Store(root), baseline["capture_id"], catalogue_version=2)
    delta = _capture_at(root, 2, incremental_mode=True)
    publish.publish(Store(root), delta["capture_id"], catalogue_version=2)
    pending = _capture_at(root, 3, incremental_mode=True, max_requests=1)
    assert pending["complete"] is False
    return root, {
        "baseline": baseline["capture_id"],
        "delta": delta["capture_id"],
        "pending": pending["capture_id"],
    }


@pytest.fixture(scope="module")
def publication_series_root(certified_series_root, tmp_path_factory):
    source, certified_ids = certified_series_root
    root = tmp_path_factory.mktemp("series-publication") / "root"
    shutil.copytree(source, root)
    obsolete = _capture_at(root, 4)
    warehouse.build_v2_full(Store(root), obsolete["capture_id"])
    candidate = _capture_at(root, 5)
    warehouse.build_v2_full(Store(root), candidate["capture_id"])
    identities = {
        **certified_ids,
        "obsolete": obsolete["capture_id"],
        "candidate": candidate["capture_id"],
    }
    orphan = _process(
        root, {**identities, "candidate": identities["obsolete"]}, "publish", "release-install"
    )
    assert orphan.returncode == CRASH_EXIT, orphan.stdout + orphan.stderr
    releases = [
        path.parent.name
        for path in (root / "releases").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == identities["obsolete"]
    ]
    assert len(releases) == 1
    identities["obsolete_release"] = releases[0]
    return root, identities


@pytest.fixture
def recovery_root(certified_series_root, tmp_path):
    source, identities = certified_series_root
    root = tmp_path / "root"
    shutil.copytree(source, root)
    return root, dict(identities)


@pytest.fixture
def publication_recovery_root(publication_series_root, tmp_path):
    source, identities = publication_series_root
    root = tmp_path / "root"
    shutil.copytree(source, root)
    return root, dict(identities)


def _assert_scoped_closure(root, identities, *, require_builds=True):
    store = Store(root)
    current = publish.current(store, catalogue_version=2)
    assert current["contract"] == "oddsfox.kalshi.series.v1"
    assert current["series_ticker"] == "KXCPI"
    assert current["capture_id"] in {identities.get("delta"), identities.get("candidate")}
    assert not store.path("current.json").exists()
    assert not store.path("current-v2.json").exists()
    pending = capture.read_capture(store, identities["pending"])
    assert pending["complete"] is False
    assert pending["baseline"]["capture_id"] == identities["delta"]
    assert pending["baseline"]["series_scope"] == store.scope
    for name in ("baseline", "delta"):
        state = capture.complete_capture(store, identities[name])
        assert state["scope"] == store.scope
        assert list(capture.capture_pages(store, state))
        if require_builds:
            warehouse.certified(store, identities[name], catalogue_version=2)
    if require_builds:
        incremental.verify_pin(store, pending)
    return current


@pytest.mark.parametrize(
    "boundary", ["body", "receipt", "journal", "journal-inflight", "apply", "snapshot"]
)
def test_scoped_process_exit_capture_resumes_without_repeating_committed_requests(
    recovery_root, tmp_path, boundary
):
    root, identities = recovery_root
    request_log = tmp_path / "requests.jsonl"
    pointer = (root / "current-series.json").read_bytes()
    crashed = _process(root, identities, "capture", boundary, request_log=request_log)
    assert crashed.returncode == CRASH_EXIT, crashed.stdout + crashed.stderr
    created = {path.name for path in (root / "captures").iterdir()} - set(identities.values())
    assert len(created) == 1
    capture_id = created.pop()
    attempts = [
        (path, json.loads(path.read_bytes()))
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == capture_id
    ]
    assert attempts
    before_requests = [json.loads(line) for line in request_log.read_bytes().splitlines()]
    usable_ids = {
        attempt["request_id"]
        for _, attempt in attempts
        if attempt.get("accounting_complete") is True and attempt.get("status") == 200
    }
    interrupted_ids = {row["request_id"] for row in before_requests} - usable_ids
    incomplete_reservations = {
        path: path.read_bytes()
        for path, attempt in attempts
        if attempt.get("accounting_complete") is False
    }
    incomplete_ids = {json.loads(body)["request_id"] for body in incomplete_reservations.values()}
    assert interrupted_ids <= incomplete_ids
    assert (root / "current-series.json").read_bytes() == pointer
    _assert_scoped_closure(root, identities)

    resumed = _process(
        root, identities, "capture", "resume", request_log=request_log, resume=capture_id
    )

    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert json.loads(resumed.stdout)["capture_id"] == capture_id
    requests = [json.loads(line) for line in request_log.read_bytes().splitlines()]
    counts = Counter(row["request_id"] for row in requests)
    assert len(counts) == 6
    assert all(counts[request_id] == 1 for request_id in usable_ids)
    assert {request_id for request_id, count in counts.items() if count > 1} == interrupted_ids
    assert all(count == (2 if key in interrupted_ids else 1) for key, count in counts.items())
    if boundary == "body":
        assert len(interrupted_ids) == 1
        assert len(incomplete_reservations) == 1
    elif boundary == "journal-inflight":
        assert len(interrupted_ids) == len(incomplete_reservations) == 1
        assert len(usable_ids) == 1
        assert len(before_requests) == 2
        assert sum(counts.values()) == 7
    elif boundary in {"journal", "apply"}:
        assert len(interrupted_ids) <= 1
        assert len(incomplete_reservations) <= 1
    else:
        assert not interrupted_ids
        assert not incomplete_reservations
    if incomplete_reservations:
        assert all(path.read_bytes() == body for path, body in incomplete_reservations.items())
        assert accounting(Store(root))["accounting_complete"] is False
    state = capture.complete_capture(Store(root), capture_id)
    pages = list(capture.capture_pages(Store(root), state))
    assert len(pages) == len({manifest["page_id"] for manifest, _ in pages}) == 6
    assert len(list(observations(pages))) == 4
    assert all(manifest["series_scope"] == state["scope"] for manifest, _ in pages)
    assert (root / "current-series.json").read_bytes() == pointer


@pytest.mark.parametrize("boundary", ["build-backup", "build-install"])
def test_scoped_build_process_exit_recovers_delta_replacement_and_pins(recovery_root, boundary):
    root, identities = recovery_root
    pointer = (root / "current-series.json").read_bytes()
    crashed = _process(root, identities, "build", boundary)
    assert crashed.returncode == CRASH_EXIT, crashed.stdout + crashed.stderr
    _assert_scoped_closure(root, identities, require_builds=False)
    assert (root / "current-series.json").read_bytes() == pointer

    recovery.rebuild(Store(root), catalogue_version=2)

    _assert_scoped_closure(root, identities)
    assert (root / "current-series.json").read_bytes() == pointer
    assert not (root / "tmp" / f"replaced-{identities['delta']}-v2").exists()


@pytest.mark.parametrize(
    "boundary",
    [
        "release-install",
        "pointer-install",
        "prune-plan",
        "prune-retire",
        "prune-delete",
        "prune-metadata-capture",
        "prune-metadata-raw",
        "prune-metadata-build",
        "prune-metadata-release",
    ],
)
def test_scoped_publication_process_exit_preserves_pending_ancestry(
    publication_recovery_root, boundary
):
    root, identities = publication_recovery_root
    obsolete_raw = [
        path.parent
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == identities["obsolete"]
    ]
    assert obsolete_raw
    crashed = _process(root, identities, "publish", boundary)
    assert crashed.returncode == CRASH_EXIT, crashed.stdout + crashed.stderr
    _assert_scoped_closure(root, identities)

    first = publish.publish(Store(root), identities["candidate"], catalogue_version=2)
    second = publish.publish(Store(root), identities["candidate"], catalogue_version=2)

    assert first["source_release_id"] == second["source_release_id"]
    assert first["files"] == second["files"]
    assert _assert_scoped_closure(root, identities)["capture_id"] == identities["candidate"]
    assert not (root / "captures" / identities["obsolete"]).exists()
    assert not (root / "builds-v2" / identities["obsolete"]).exists()
    assert not (root / "releases" / identities["obsolete_release"]).exists()
    assert not any(path.exists() for path in obsolete_raw)
    assert not (root / "prune-plan.json").exists()
    assert not list((root / "prune-trash").glob("*"))


def _database_relations(database):
    with duckdb.connect(str(database), read_only=True) as connection:
        return {
            name: (
                connection.execute(f"describe main.{table}").fetchall(),
                sorted(
                    json_bytes(list(row))
                    for row in connection.execute(f"select * from main.{table}").fetchall()
                ),
            )
            for name, table in warehouse.RELATIONS.items()
        }


def test_scoped_incremental_projection_matches_full_reference_over_identical_history(recovery_root):
    root, identities = recovery_root
    store = Store(root)
    source = store.path(f"builds-v2/{identities['delta']}/warehouse.duckdb")
    expected = _database_relations(source)
    assert len(expected["markets"][1]) == 2
    assert len(expected["observation_history"][1]) == 10
    reference = store.path("tmp/reference")
    reference.mkdir(parents=True)
    shutil.copy2(source, reference / "warehouse.duckdb")

    warehouse.dbt(
        store,
        ["build"],
        database="tmp/reference/warehouse.duckdb",
        variables={
            "catalogue_version": 2,
            "incremental_update": False,
            "current_capture_id": identities["delta"],
            "delta_capture_id": identities["delta"],
        },
    )

    assert _database_relations(reference / "warehouse.duckdb") == expected
    assert _database_relations(source) == expected
