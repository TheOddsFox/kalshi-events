"""Durability checks using real certified roots and abrupt process termination."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import conftest as fixtures
import pytest
from conftest import COMBO, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import common, incremental, recovery, warehouse
from kalshi_events import publish as publishing
from kalshi_events.common import CatalogueError, Limits, Store

CRASH_EXIT = 86


class ReconciliationClient(FakeClient):
    def response(self, endpoint, params):
        if endpoint == "/markets" and params.get("mve_filter") == "only":
            return 200, {"markets": [COMBO], "cursor": ""}
        if endpoint == "/markets" and params.get("mve_filter") == "exclude":
            return 200, {"markets": [MARKET], "cursor": ""}
        return super().response(endpoint, params)


def _capture_at(root, timestamp, *, incremental=False, max_requests=25_000):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fixtures, "now", lambda: timestamp)
        patch.setattr(acquisition, "now", lambda: timestamp)
        return acquisition.capture(
            Store(root, Limits(max_requests=max_requests)),
            client_factory=ReconciliationClient,
            incremental=incremental,
        )


@pytest.fixture(scope="module")
def certified_recovery_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("publication-recovery") / "root"
    legacy = _capture_at(root, "2026-01-01T12:00:00+00:00")["capture_id"]
    warehouse.build(Store(root), legacy)
    publishing.publish(Store(root), legacy)
    baseline = _capture_at(root, "2026-01-02T12:00:00+00:00")["capture_id"]
    warehouse.build_v2_full(Store(root), baseline)
    publishing.publish(Store(root), baseline, catalogue_version=2)
    pending = _capture_at(root, "2026-01-03T12:00:00+00:00", incremental=True, max_requests=1)
    assert not pending["complete"] and pending["budget_exhausted"]
    obsolete = _capture_at(root, "2026-01-04T12:00:00+00:00")["capture_id"]
    warehouse.build(Store(root), obsolete)
    candidate = _capture_at(root, "2026-01-05T12:00:00+00:00")["capture_id"]
    warehouse.build_v2_full(Store(root), candidate)
    captures = {
        "legacy": legacy,
        "baseline": baseline,
        "pending": pending["capture_id"],
        "obsolete": obsolete,
        "candidate": candidate,
    }
    result = _python(root, captures, ORPHAN_RELEASE_SCRIPT)
    assert result.returncode == CRASH_EXIT, result.stdout + result.stderr
    obsolete_releases = [
        path.parent.name
        for path in (root / "releases").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == obsolete
    ]
    assert len(obsolete_releases) == 1
    captures["obsolete_release"] = obsolete_releases[0]
    return root, captures


@pytest.fixture
def recovery_root(certified_recovery_root, tmp_path):
    source, captures = certified_recovery_root
    root = tmp_path / "root"
    shutil.copytree(source, root)
    return root, captures.copy()


@pytest.fixture(scope="module")
def published_delta_recovery_root(certified_recovery_root, tmp_path_factory):
    source, _ = certified_recovery_root
    root = tmp_path_factory.mktemp("published-delta-recovery") / "root"
    shutil.copytree(source, root)
    delta = _capture_at(root, "2026-01-06T12:00:00+00:00", incremental=True)
    assert delta["complete"]
    warehouse.build(Store(root), delta["capture_id"])
    manifest = publishing.publish(Store(root), delta["capture_id"], catalogue_version=2)
    assert manifest["delta_depth"] == 1
    return root, delta["capture_id"]


@pytest.mark.parametrize("command", ["rebuild", "replay"])
def test_published_delta_retry_recovers_redundant_backup_with_the_same_database_budget(
    published_delta_recovery_root, tmp_path, monkeypatch, command
):
    source, capture_id = published_delta_recovery_root
    reference_root, root = tmp_path / "reference", tmp_path / "recovery"
    shutil.copytree(source, reference_root)
    shutil.copytree(source, root)
    run = getattr(recovery, command)
    peak_database_bytes = 0
    check_storage = Store.check_storage

    def measure_reference_usage(store, additional=0):
        nonlocal peak_database_bytes
        if store.root == reference_root:
            peak_database_bytes = max(peak_database_bytes, store.usage()["database_bytes"])
        return check_storage(store, additional)

    with monkeypatch.context() as patch:
        patch.setattr(Store, "check_storage", measure_reference_usage)
        run(Store(reference_root), catalogue_version=2)
    assert peak_database_bytes > 0

    destination = root / "builds-v2" / capture_id
    backup = root / "tmp" / f"replaced-{capture_id}-v2"
    shutil.copytree(destination, backup)
    initial_database_bytes = Store(root).usage()["database_bytes"]
    ceiling = max(peak_database_bytes, initial_database_bytes)
    state = acquisition.read_capture(Store(root), capture_id)
    baseline_database = (
        root / warehouse.build_directory(state["baseline"]["capture_id"], 2) / "warehouse.duckdb"
    )
    assert ceiling - initial_database_bytes < baseline_database.stat().st_size

    immutable_before = {
        str(path.relative_to(root)): path.read_bytes()
        for directory in (root / "captures", root / "raw", root / "releases")
        for path in directory.rglob("*")
        if path.is_file()
    }
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    limited = Store(root, Limits(max_database_bytes=ceiling))
    limited.check_storage()

    result = run(limited, catalogue_version=2)

    assert result["capture_id"] == capture_id
    assert not backup.exists()
    limited.check_storage()
    warehouse.certified(limited, capture_id, catalogue_version=2)
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert {
        str(path.relative_to(root)): path.read_bytes()
        for directory in (root / "captures", root / "raw", root / "releases")
        for path in directory.rglob("*")
        if path.is_file()
    } == immutable_before
    manifest = publishing.current(limited, catalogue_version=2)
    assert manifest["capture_id"] == capture_id


def _python(root, captures, script, *arguments):
    environment = os.environ.copy()
    source = str(Path(__file__).resolve().parents[1] / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        [source, *filter(None, [environment.get("PYTHONPATH")])]
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(root), json.dumps(captures), *arguments],
        env=environment,
        text=True,
        capture_output=True,
        timeout=120,
    )
    return result


ORPHAN_RELEASE_SCRIPT = r"""
import json, os, sys
from pathlib import Path
from kalshi_events.common import Store
from kalshi_events import publish as publishing

root, captures = Path(sys.argv[1]), json.loads(sys.argv[2])
rename = os.rename
def interrupted_rename(source, destination, *args, **kwargs):
    result = rename(source, destination, *args, **kwargs)
    if Path(destination).parent == root / 'releases':
        os._exit(86)
    return result
os.rename = interrupted_rename
publishing.publish(Store(root), captures['obsolete'])
raise AssertionError('unreachable release was not installed')
"""


CRASH_SCRIPT = r"""
import json, os, shutil, sys
from pathlib import Path
from kalshi_events.common import Store
from kalshi_events import recovery
from kalshi_events import publish as publishing

root, captures, boundary = Path(sys.argv[1]), json.loads(sys.argv[2]), sys.argv[3]
catalogue_version = int(sys.argv[4]) if len(sys.argv) > 4 else 2
build_capture = captures['legacy'] if catalogue_version == 1 else captures['baseline']
baseline = root / ('builds' if catalogue_version == 1 else 'builds-v2') / build_capture
obsolete = root / 'captures' / captures['obsolete']
original_rename, original_replace, original_rmtree = os.rename, os.replace, shutil.rmtree
retired = None
metadata_retired = None
metadata_source = None
identity_name = None
if boundary.startswith('prune-metadata-'):
    kind = boundary.removeprefix('prune-metadata-')
    raw = next(path.parent for path in (root / 'raw').glob('*/manifest.json')
               if json.loads(path.read_bytes()).get('capture_id') == captures['obsolete'])
    metadata_source, identity_name = {
        'capture': (obsolete, 'state.json'),
        'raw': (raw, 'manifest.json'),
        'build': (root / 'builds' / captures['obsolete'], 'build.json'),
        'release': (root / 'releases' / captures['obsolete_release'], 'manifest.json'),
    }[kind]

def moved(operation, source, destination, *args, **kwargs):
    global retired, metadata_retired
    source, destination = Path(source), Path(destination)
    result = operation(source, destination, *args, **kwargs)
    if source == obsolete:
        retired = destination
    if source == metadata_source:
        metadata_retired = destination
    crashed = (
        (boundary == 'build-backup' and source == baseline)
        or (boundary == 'build-install' and destination == baseline)
        or (boundary == 'release-install' and destination.parent == root / 'releases')
        or (boundary == 'pointer-install' and destination == root / 'current-v2.json')
        or (boundary == 'prune-plan' and destination == root / 'prune-plan.json')
        or (boundary == 'prune-retire' and source == obsolete)
    )
    if crashed:
        os._exit(86)
    return result

os.rename = lambda *a, **kw: moved(original_rename, *a, **kw)
os.replace = lambda *a, **kw: moved(original_replace, *a, **kw)

def interrupted_delete(path, *args, **kwargs):
    if metadata_retired is not None and Path(path) == metadata_retired:
        identity = metadata_retired / identity_name
        assert identity.is_file(), 'identity metadata was absent before deletion'
        identity.unlink()
        os._exit(86)
    if boundary == 'prune-delete' and retired is not None and Path(path) == retired:
        files = sorted(item for item in retired.rglob('*') if item.is_file())
        assert files, 'retired capture was empty before deletion'
        files[0].unlink()
        os._exit(86)
    return original_rmtree(path, *args, **kwargs)

shutil.rmtree = interrupted_delete
if boundary.startswith('build-'):
    recovery.rebuild(Store(root), catalogue_version=catalogue_version)
else:
    publishing.publish(Store(root), captures['candidate'], catalogue_version=2)
raise AssertionError('requested crash boundary was not reached: ' + boundary)
"""


CHECK_SCRIPT = r"""
import hashlib, json, sys
from pathlib import Path
from kalshi_events.common import Store
from kalshi_events.capture import complete_capture, read_capture
from kalshi_events.incremental import root_id, verify_pin
from kalshi_events.warehouse import certified
from kalshi_events import publish as publishing

root, captures = Path(sys.argv[1]), json.loads(sys.argv[2])
store = Store(root)
legacy = publishing.current(store)
v2 = publishing.current(store, catalogue_version=2)
assert legacy['capture_id'] == captures['legacy']
assert v2['capture_id'] in {captures['baseline'], captures['candidate']}
pending = read_capture(store, captures['pending'])
assert not pending['complete']
if sys.argv[3] == 'require-builds':
    verify_pin(store, pending)
else:
    pin = pending['baseline']
    assert pin['catalogue_version'] == 2
    assert pin['root_id'] == root_id(store)
    pinned_directory = root / 'releases' / pin['source_release_id']
    pinned = publishing.verify_release(pinned_directory)
    assert pinned['capture_id'] == pin['capture_id'] == captures['baseline']
    assert pinned['root_id'] == pin['root_id']
    assert pinned['source_release_id'] == pin['source_release_id']
    assert hashlib.sha256((pinned_directory / 'manifest.json').read_bytes()).hexdigest() == pin['manifest_sha256']
for name, version in [('legacy', 1), ('baseline', 2)]:
    capture_id = captures[name]
    complete_capture(store, capture_id)
    # A build rename crash can temporarily leave the baseline under its recovery backup.
    if sys.argv[3] == 'require-builds':
        directory = 'builds' if version == 1 else 'builds-v2'
        assert (root / directory / capture_id / 'warehouse.duckdb').is_file()
        certified(store, capture_id, catalogue_version=version)
if sys.argv[3] == 'require-builds' and v2['capture_id'] == captures['candidate']:
    certified(store, captures['candidate'], catalogue_version=2)
print(json.dumps({'legacy': legacy['source_release_id'], 'v2': v2['source_release_id']}))
"""


def _check_fresh(root, captures, *, require_builds=True):
    result = _python(
        root, captures, CHECK_SCRIPT, "require-builds" if require_builds else "allow-backup"
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("boundary", ["build-backup", "build-install"])
@pytest.mark.parametrize("catalogue_version", [1, 2])
def test_process_exit_during_build_replacement_is_recoverable(
    recovery_root, boundary, catalogue_version
):
    root, captures = recovery_root
    pointers = _check_fresh(root, captures)
    result = _python(root, captures, CRASH_SCRIPT, boundary, str(catalogue_version))
    assert result.returncode == CRASH_EXIT, result.stdout + result.stderr
    assert _check_fresh(root, captures, require_builds=False) == pointers
    capture_id = captures["legacy"] if catalogue_version == 1 else captures["baseline"]
    recovery.rebuild(Store(root), catalogue_version=catalogue_version)
    warehouse.certified(Store(root), capture_id, catalogue_version=catalogue_version)
    published = publishing.publish(Store(root), capture_id, catalogue_version=catalogue_version)
    assert published["source_release_id"] == pointers["legacy" if catalogue_version == 1 else "v2"]
    assert _check_fresh(root, captures) == pointers


@pytest.mark.parametrize(
    "boundary",
    [
        "release-install",
        "pointer-install",
        "prune-retire",
        "prune-delete",
        "prune-metadata-capture",
        "prune-metadata-raw",
        "prune-metadata-build",
        "prune-metadata-release",
    ],
)
def test_process_exit_during_publication_preserves_both_pointers_and_pins(recovery_root, boundary):
    root, captures = recovery_root
    legacy = (root / "current.json").read_bytes()
    obsolete_raw = [
        path.parent
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == captures["obsolete"]
    ]
    assert obsolete_raw
    result = _python(root, captures, CRASH_SCRIPT, boundary)
    assert result.returncode == CRASH_EXIT, result.stdout + result.stderr
    _check_fresh(root, captures)
    first = publishing.publish(Store(root), captures["candidate"], catalogue_version=2)
    second = publishing.publish(Store(root), captures["candidate"], catalogue_version=2)
    assert first["source_release_id"] == second["source_release_id"]
    assert first["files"] == second["files"]
    assert (root / "current.json").read_bytes() == legacy
    assert not (root / "captures" / captures["obsolete"]).exists()
    assert not any(path.exists() for path in obsolete_raw)
    assert not (root / "builds" / captures["obsolete"]).exists()
    assert not (root / "releases" / captures["obsolete_release"]).exists()
    assert not list((root / "prune-trash").glob("*"))
    pointers = _check_fresh(root, captures)
    assert pointers["v2"] == first["source_release_id"]


def test_pending_prune_finishes_before_an_obsolete_candidate_can_publish(
    recovery_root, monkeypatch
):
    root, captures = recovery_root
    obsolete_raw = [
        path.parent
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == captures["obsolete"]
    ]
    assert obsolete_raw
    result = _python(root, captures, CRASH_SCRIPT, "prune-plan")
    assert result.returncode == CRASH_EXIT, result.stdout + result.stderr
    assert (root / "prune-plan.json").is_file()
    assert (root / "captures" / captures["obsolete"]).is_dir()
    assert all(path.is_dir() for path in obsolete_raw)
    before = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    certified_candidates = []
    certified = publishing.certified

    def observed_certified(store, capture_id, **kwargs):
        certified_candidates.append(capture_id)
        return certified(store, capture_id, **kwargs)

    monkeypatch.setattr(publishing, "certified", observed_certified)
    with pytest.raises(CatalogueError):
        publishing.publish(Store(root), captures["obsolete"])
    assert captures["obsolete"] not in certified_candidates
    assert {name: (root / name).read_bytes() for name in before} == before
    assert not (root / "captures" / captures["obsolete"]).exists()
    assert not (root / "builds" / captures["obsolete"]).exists()
    assert not any(path.exists() for path in obsolete_raw)
    assert not (root / "prune-plan.json").exists()
    assert not list((root / "prune-trash").glob("*"))
    _check_fresh(root, captures)


@pytest.mark.parametrize("operator_file", ["source", "target"])
def test_malformed_prune_plan_cannot_delete_unknown_operator_files(recovery_root, operator_file):
    root, _ = recovery_root
    store = Store(root)
    if operator_file == "source":
        source = "raw/operator-notes"
        target = f"prune-trash/{'a' * 32}-raw-operator-notes"
        owned = source
    else:
        source = f"raw/{'a' * 32}"
        target = "prune-trash/user-notes"
        owned = target
    body = b"Operator notes: retain across catalogue pruning.\n"
    store.write_bytes(owned, body)
    plan = {"version": 1, "kind": "prune", "entries": [{"source": source, "target": target}]}
    store.write("prune-plan.json", plan)
    before = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert store.path(owned).read_bytes() == body
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in before} == before
    other = target if operator_file == "source" else source
    assert not store.path(other).exists()


def _artifact_files(root, relatives):
    return {
        str(path.relative_to(root)): common.file_digest(path, 2 * 1024**3)
        for relative in relatives
        for path in (root / relative).rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize("separator", ["//", "/./"])
@pytest.mark.parametrize("aliased_field", ["source", "target"])
def test_prune_plan_rejects_path_aliases_before_retiring_any_artifact(
    recovery_root, separator, aliased_field
):
    root, captures = recovery_root
    store = Store(root)
    identity = captures["obsolete"]
    source = f"captures/{identity}"
    entries = [{"source": source, "target": f"prune-trash/{uuid.uuid4().hex}-captures-{identity}"}]
    if aliased_field == "source":
        entries.append(
            {
                "source": f"captures{separator}{identity}",
                "target": f"prune-trash/{uuid.uuid4().hex}-captures-{identity}",
            }
        )
    else:
        entries[0]["target"] = f"prune-trash{separator}{uuid.uuid4().hex}-captures-{identity}"
    plan = {"version": 1, "kind": "prune", "entries": entries}
    store.write("prune-plan.json", plan)
    before = _artifact_files(root, [source])
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert store.path(source).is_dir()
    assert _artifact_files(root, [source]) == before
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert not list((root / "prune-trash").glob("*"))
    assert not store.path("prune-progress.jsonl").exists()


@pytest.mark.parametrize("later_error", ["wrong-metadata-identity", "untracked-tombstone"])
def test_all_retirement_entries_are_validated_before_the_first_deletion(recovery_root, later_error):
    root, captures = recovery_root
    store = Store(root)
    first = f"captures/{captures['obsolete']}"
    raw = next(
        path.parent
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == captures["obsolete"]
    )
    if later_error == "wrong-metadata-identity":
        second_path = root / "raw" / uuid.uuid4().hex
        shutil.copytree(raw, second_path)
    else:
        second_path = raw
    second = second_path.relative_to(root).as_posix()
    entries = [
        {"source": first, "target": f"prune-trash/{uuid.uuid4().hex}-captures-{Path(first).name}"},
        {"source": second, "target": f"prune-trash/{uuid.uuid4().hex}-raw-{second_path.name}"},
    ]
    tracked_paths = [first, second]
    if later_error == "untracked-tombstone":
        target = store.path(entries[1]["target"])
        target.parent.mkdir(exist_ok=True)
        shutil.copytree(second_path, target)
        tracked_paths.append(entries[1]["target"])
    plan = {"version": 1, "kind": "prune", "entries": entries}
    store.write("prune-plan.json", plan)
    before = _artifact_files(root, tracked_paths)
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert all(store.path(relative).is_dir() for relative in tracked_paths)
    assert _artifact_files(root, tracked_paths) == before
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert not store.path("prune-progress.jsonl").exists()


def test_unlisted_trash_child_blocks_retirement_before_any_source_moves(recovery_root):
    root, captures = recovery_root
    store = Store(root)
    source = f"captures/{captures['obsolete']}"
    plan = {
        "version": 1,
        "kind": "prune",
        "entries": [
            {
                "source": source,
                "target": f"prune-trash/{uuid.uuid4().hex}-captures-{captures['obsolete']}",
            }
        ],
    }
    store.write("prune-plan.json", plan)
    notes = b"Unlisted operator-owned trash child: preserve this file.\n"
    store.write_bytes("prune-trash/user-notes", notes)
    before = _artifact_files(root, [source])
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert store.path(source).is_dir()
    assert _artifact_files(root, [source]) == before
    assert store.path("prune-trash/user-notes").read_bytes() == notes
    assert not store.path(plan["entries"][0]["target"]).exists()
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert not store.path("prune-progress.jsonl").exists()


def test_tracked_regular_file_tombstone_is_not_a_resumable_partial_directory(recovery_root):
    root, captures = recovery_root
    store = Store(root)
    source = f"captures/{captures['obsolete']}"
    target = f"prune-trash/{uuid.uuid4().hex}-captures-{captures['obsolete']}"
    plan = {"version": 1, "kind": "prune", "entries": [{"source": source, "target": target}]}
    store.write("prune-plan.json", plan)
    progress = {
        "version": 1,
        "plan_sha256": common.digest(store.path("prune-plan.json").read_bytes()),
        "index": 0,
        "source": source,
        "target": target,
    }
    store.append("prune-progress.jsonl", common.json_bytes(progress))
    preserved = f"tmp/preserved-test-{captures['obsolete']}"
    os.rename(store.path(source), store.path(preserved))
    body = b"A regular file cannot be a partially deleted producer directory.\n"
    store.write_bytes(target, body)
    before = _artifact_files(root, [preserved])
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert store.path(target).is_file()
    assert store.path(target).read_bytes() == body
    assert _artifact_files(root, [preserved]) == before
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert store.path("prune-progress.jsonl").read_bytes() == common.json_bytes(progress)
    assert {name: (root / name).read_bytes() for name in pointers} == pointers


@pytest.mark.parametrize("unrelated_kind", ["capture", "raw", "build", "release"])
def test_discard_plan_rejects_valid_artifacts_outside_its_incomplete_capture(
    recovery_root, unrelated_kind
):
    root, captures = recovery_root
    store = Store(root)
    victim = captures["pending"]
    raw_by_capture = {}
    for path in (root / "raw").glob("*/manifest.json"):
        raw_by_capture.setdefault(json.loads(path.read_bytes())["capture_id"], []).append(
            path.parent.relative_to(root).as_posix()
        )
    victim_sources = [f"captures/{victim}", *raw_by_capture[victim]]
    assert raw_by_capture[victim]
    unrelated = {
        "capture": f"captures/{captures['obsolete']}",
        "raw": raw_by_capture[captures["obsolete"]][0],
        "build": f"builds/{captures['obsolete']}",
        "release": f"releases/{captures['obsolete_release']}",
    }[unrelated_kind]
    sources = [*victim_sources, unrelated]
    entries = [
        {
            "source": source,
            "target": f"prune-trash/{uuid.uuid4().hex}-{'-'.join(Path(source).parts)}",
        }
        for source in sources
    ]
    plan = {"version": 1, "kind": "discard", "capture_id": victim, "entries": entries}
    store.write("prune-plan.json", plan)
    before = _artifact_files(root, sources)
    assert before
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert _artifact_files(root, sources) == before
    assert all(store.path(source).is_dir() for source in sources)
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert not list((root / "prune-trash").glob("*"))


def test_discard_plan_rejects_a_completed_unprotected_capture(recovery_root):
    root, captures = recovery_root
    store = Store(root)
    victim = captures["obsolete"]
    assert acquisition.read_capture(store, victim)["complete"]
    raw_sources = [
        path.parent.relative_to(root).as_posix()
        for path in (root / "raw").glob("*/manifest.json")
        if json.loads(path.read_bytes()).get("capture_id") == victim
    ]
    assert raw_sources
    sources = [f"captures/{victim}", *raw_sources]
    closure = [*sources, f"builds/{victim}", f"releases/{captures['obsolete_release']}"]
    plan = {
        "version": 1,
        "kind": "discard",
        "capture_id": victim,
        "entries": [
            {
                "source": source,
                "target": f"prune-trash/{uuid.uuid4().hex}-{'-'.join(Path(source).parts)}",
            }
            for source in sources
        ],
    }
    store.write("prune-plan.json", plan)
    before = _artifact_files(root, closure)
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    with store.writer_guard(), pytest.raises(CatalogueError):
        incremental.finish_pending_retirement(store)
    assert _artifact_files(root, closure) == before
    assert all(store.path(source).is_dir() for source in closure)
    assert json.loads(store.path("prune-plan.json").read_bytes()) == plan
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    assert not list((root / "prune-trash").glob("*"))
    assert not store.path("prune-progress.jsonl").exists()


def _trace_syncs(monkeypatch, events, *, fail_file=None, fail_directory=None):
    for name, kind, fail_path in (
        ("fsync_file", "file", fail_file),
        ("fsync_directory", "directory", fail_directory),
    ):
        original = getattr(common, name)

        def traced(path, *, original=original, kind=kind, fail_path=fail_path):
            path = Path(path)
            if fail_path is not None and fail_path(path):
                events.append(("failed", path))
                raise OSError("injected durability failure")
            result = original(path)
            events.append((kind, path))
            return result

        for module in (common, warehouse, publishing):
            if getattr(module, name, None) is original:
                monkeypatch.setattr(module, name, traced)


@pytest.mark.parametrize("catalogue_version", [1, 2])
def test_failed_install_never_deletes_good_destination_before_validating_corrupt_backup(
    recovery_root, monkeypatch, catalogue_version
):
    root, captures = recovery_root
    store = Store(root)
    capture_id = captures["legacy"] if catalogue_version == 1 else captures["baseline"]
    relative = warehouse.build_directory(capture_id, catalogue_version)
    destination = store.path(relative)
    staging = store.path(f"tmp/installer-proof-{uuid.uuid4().hex}")
    shutil.copytree(destination, staging)
    store.sync("tmp")
    warehouse.certified(store, capture_id, catalogue_version=catalogue_version, _directory=staging)
    corrupted = json.loads((destination / "build.json").read_bytes())
    corrupted["warehouse_sha256"] = "0" * 64
    store.write(f"{relative}/build.json", corrupted)
    corrupt_receipt = (destination / "build.json").read_bytes()
    pointers = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    renamed = os.rename
    synced = warehouse.fsync_directory
    installed = failed = False

    def observed_rename(source, target, *args, **kwargs):
        nonlocal installed
        result = renamed(source, target, *args, **kwargs)
        if Path(source) == staging and Path(target) == destination:
            installed = True
        return result

    def fail_after_install(path):
        nonlocal failed
        if installed and not failed:
            failed = True
            raise OSError("transient sync failure after valid candidate installation")
        return synced(path)

    monkeypatch.setattr(os, "rename", observed_rename)
    monkeypatch.setattr(warehouse, "fsync_directory", fail_after_install)
    with store.writer_guard(), pytest.raises(CatalogueError):
        warehouse._install_build(store, capture_id, catalogue_version, staging)
    assert installed and failed
    backup = store.path(f"tmp/replaced-{capture_id}-v{catalogue_version}")
    assert backup.is_dir() and destination.is_dir()
    assert (backup / "build.json").read_bytes() == corrupt_receipt
    warehouse.certified(Store(root), capture_id, catalogue_version=catalogue_version)
    assert {name: (root / name).read_bytes() for name in pointers} == pointers
    _check_fresh(root, captures)
    fresh = Store(root)
    with fresh.writer_guard():
        warehouse._recover_build(fresh, capture_id, catalogue_version)
    assert not backup.exists()
    warehouse.certified(Store(root), capture_id, catalogue_version=catalogue_version)
    assert {name: (root / name).read_bytes() for name in pointers} == pointers


def test_staged_warehouse_sync_failure_preserves_certified_build(recovery_root, monkeypatch):
    root, captures = recovery_root
    directory = root / "builds-v2" / captures["baseline"]
    before = {
        path.name: common.file_digest(path, 2 * 1024**3)
        for path in directory.iterdir()
        if path.is_file()
    }
    pointers = _check_fresh(root, captures)
    events = []
    _trace_syncs(
        monkeypatch,
        events,
        fail_file=lambda path: path.name == "warehouse.duckdb" and path.parent != directory,
    )
    with pytest.raises(OSError, match="injected durability failure"):
        recovery.rebuild(Store(root), catalogue_version=2)
    assert any(kind == "failed" for kind, _ in events)
    assert {
        path.name: common.file_digest(path, 2 * 1024**3)
        for path in directory.iterdir()
        if path.is_file()
    } == before
    warehouse.certified(Store(root), captures["baseline"], catalogue_version=2)
    assert _check_fresh(root, captures) == pointers


def test_every_export_and_release_directory_are_durable_before_pointer(recovery_root, monkeypatch):
    root, captures = recovery_root
    events = []
    _trace_syncs(monkeypatch, events)
    replace = os.replace
    checked = False

    def observed_replace(source, destination, *args, **kwargs):
        nonlocal checked
        if Path(destination) == root / "current-v2.json":
            assert {path.name for kind, path in events if kind == "file"} >= publishing.FILES
            export_parents = {
                path.parent for kind, path in events if kind == "file" and path.suffix == ".parquet"
            }
            assert len(export_parents) == 1
            assert ("directory", export_parents.pop()) in events
            assert ("directory", root / "releases") in events
            checked = True
        return replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "replace", observed_replace)
    publishing.publish(Store(root), captures["candidate"], catalogue_version=2)
    assert checked
    _check_fresh(root, captures)


@pytest.mark.parametrize("failure", ["export-file", "release-parent"])
def test_pre_pointer_sync_failure_preserves_pointers_and_defers_pruning(
    recovery_root, monkeypatch, failure
):
    root, captures = recovery_root
    before = {name: (root / name).read_bytes() for name in ("current.json", "current-v2.json")}
    events = []
    _trace_syncs(
        monkeypatch,
        events,
        fail_file=(lambda path: path.name == "coverage.parquet")
        if failure == "export-file"
        else None,
        fail_directory=(lambda path: path == root / "releases")
        if failure == "release-parent"
        else None,
    )
    with pytest.raises(OSError, match="injected durability failure"):
        publishing.publish(Store(root), captures["candidate"], catalogue_version=2)
    assert any(kind == "failed" for kind, _ in events)
    assert {name: (root / name).read_bytes() for name in before} == before
    assert (root / "captures" / captures["obsolete"]).is_dir()
    acquisition.complete_capture(Store(root), captures["obsolete"])
    _check_fresh(root, captures)


def test_pointer_directory_sync_failure_never_prunes_reachable_evidence(recovery_root, monkeypatch):
    root, captures = recovery_root
    legacy = (root / "current.json").read_bytes()
    replaced = False
    failed = False
    replace = os.replace
    fsync = common.fsync_directory

    def observed_replace(source, destination, *args, **kwargs):
        nonlocal replaced
        result = replace(source, destination, *args, **kwargs)
        if Path(destination) == root / "current-v2.json":
            replaced = True
        return result

    def failed_sync(path):
        nonlocal failed
        if replaced and path == root and not failed:
            failed = True
            raise OSError("injected pointer directory sync failure")
        return fsync(path)

    monkeypatch.setattr(os, "replace", observed_replace)
    monkeypatch.setattr(common, "fsync_directory", failed_sync)
    with pytest.raises(OSError, match="injected pointer directory sync failure"):
        publishing.publish(Store(root), captures["candidate"], catalogue_version=2)
    assert replaced and failed
    assert (root / "current.json").read_bytes() == legacy
    assert (root / "captures" / captures["obsolete"]).is_dir()
    acquisition.complete_capture(Store(root), captures["obsolete"])
    _check_fresh(root, captures)
