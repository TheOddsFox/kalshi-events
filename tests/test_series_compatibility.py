"""Scoped roots cannot be mistaken for exchange-wide roots or change under a writer."""

from __future__ import annotations

import copy
import json

import pytest

from kalshi_events import common
from kalshi_events.common import CatalogueError, Store, json_bytes

SERIES_SCOPE = {
    "kind": "series",
    "series_ticker": "KXCPI",
    "include_multivariate": False,
}


def _scoped_marker():
    return {**common.OWNER_MARKER, "storage_version": 4, "scope": copy.deepcopy(SERIES_SCOPE)}


def _current_scoped_marker():
    return {**common.OWNER_MARKER, "storage_version": 5, "scope": copy.deepcopy(SERIES_SCOPE)}


@pytest.mark.parametrize("existing_empty_directory", [False, True])
def test_new_series_root_seals_scope_and_reopens_without_selection(
    tmp_path, existing_empty_directory
):
    root = tmp_path / "series"
    if existing_empty_directory:
        root.mkdir()
    store = Store(root, series_ticker="KXCPI")

    assert store.storage_version == 5
    assert store.scope == SERIES_SCOPE
    assert json.loads(store.path("root.json").read_text()) == _current_scoped_marker()
    reopened = Store(root)
    assert reopened.storage_version == 5
    assert reopened.scope == SERIES_SCOPE
    with reopened.writer_guard():
        reopened.write("sentinel.json", {"scope": "KXCPI"})
    assert json.loads(store.path("sentinel.json").read_text()) == {"scope": "KXCPI"}


def test_explicit_matching_scope_can_reopen_series_root(tmp_path):
    initial = Store(tmp_path / "series", series_ticker="KXCPI")
    marker = initial.path("root.json").read_bytes()
    reopened = Store(initial.root, series_ticker="KXCPI")
    assert reopened.scope == SERIES_SCOPE
    assert reopened.path("root.json").read_bytes() == marker


def test_scope_descriptor_cannot_mutate_the_open_roots_scope(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    descriptor = store.scope
    descriptor["series_ticker"] = "KXFEDDECISION"
    descriptor["include_multivariate"] = True

    with store.writer_guard():
        assert store.scope == SERIES_SCOPE
    assert json.loads(store.path("root.json").read_text()) == _current_scoped_marker()


def test_upgrade_scoped_v4_preserves_scope_identity_and_evidence(tmp_path):
    root = tmp_path / "series"
    root.mkdir()
    marker_path = root / "root.json"
    marker_path.write_bytes(json_bytes(_scoped_marker()))
    (root / "root-id.json").write_bytes(
        json_bytes({"version": 1, "root_id": "0123456789abcdef0123456789abcdef"})
    )
    store = Store(root)
    from kalshi_events.incremental import root_id

    identity = root_id(store)
    marker = marker_path.read_bytes()
    evidence = store.path("evidence.bin")
    evidence.write_bytes(b"keep scoped evidence")

    with store.lock(allow_legacy=True) as lock:
        store.upgrade_root(lock)

    assert store.path("root.json").read_bytes() != marker
    assert json.loads(store.path("root.json").read_text()) == _current_scoped_marker()
    assert evidence.read_bytes() == b"keep scoped evidence"
    assert store.storage_version == 5
    assert store.scope == SERIES_SCOPE
    assert root_id(store) == identity


def test_existing_series_root_rejects_conflicting_selection_without_rewriting(tmp_path):
    initial = Store(tmp_path / "series", series_ticker="KXCPI")
    marker = initial.path("root.json").read_bytes()
    with pytest.raises(CatalogueError):
        Store(initial.root, series_ticker="KXFEDDECISION")
    assert initial.path("root.json").read_bytes() == marker


@pytest.mark.parametrize("version", [1, 2, 3])
def test_series_selection_never_converts_existing_ordinary_root(tmp_path, version):
    root = tmp_path / "ordinary"
    root.mkdir()
    marker = (
        common.OWNER_MARKER if version == 1 else {**common.OWNER_MARKER, "storage_version": version}
    )
    (root / "root.json").write_bytes(json_bytes(marker))
    (root / "evidence.bin").write_bytes(b"retained evidence")
    before = {path.name: path.read_bytes() for path in root.iterdir()}

    with pytest.raises(CatalogueError):
        Store(root, series_ticker="KXCPI")

    assert {path.name: path.read_bytes() for path in root.iterdir()} == before


def test_ordinary_root_creation_uses_storage_five(tmp_path):
    store = Store(tmp_path / "ordinary")
    assert store.storage_version == 5
    assert store.scope is None
    assert store.path("root.json").read_bytes() == json_bytes(common.CURRENT_ROOT_MARKER)


def test_nonempty_unowned_directory_cannot_become_series_root(tmp_path):
    root = tmp_path / "not-an-operator-root"
    root.mkdir()
    existing = root / "keep.bin"
    existing.write_bytes(b"must survive")

    with pytest.raises(CatalogueError):
        Store(root, series_ticker="KXCPI")

    assert existing.read_bytes() == b"must survive"
    assert not (root / "root.json").exists()


@pytest.mark.parametrize("value", [True, False, "", "../KXCPI", "KXCPI,KXFED", "KXCPI/OTHER"])
def test_invalid_series_selection_is_rejected(tmp_path, value):
    with pytest.raises(CatalogueError):
        Store(tmp_path / "invalid", series_ticker=value)


@pytest.mark.parametrize(
    "change",
    [
        "boolean-version",
        "float-version",
        "missing-scope",
        "missing-ticker",
        "boolean-ticker",
        "invalid-ticker",
        "wrong-kind",
        "combo-enabled",
        "integer-combo-flag",
        "unknown-scope-field",
        "unknown-marker-field",
        "scope-on-v3",
    ],
)
def test_malformed_scoped_markers_fail_closed_without_repair(tmp_path, change):
    root = tmp_path / "malformed"
    root.mkdir()
    marker = _scoped_marker()
    if change == "boolean-version":
        marker["storage_version"] = True
    elif change == "float-version":
        marker["storage_version"] = 4.0
    elif change == "missing-scope":
        marker.pop("scope")
    elif change == "missing-ticker":
        marker["scope"].pop("series_ticker")
    elif change == "boolean-ticker":
        marker["scope"]["series_ticker"] = True
    elif change == "invalid-ticker":
        marker["scope"]["series_ticker"] = "../KXCPI"
    elif change == "wrong-kind":
        marker["scope"]["kind"] = "catalogue"
    elif change == "combo-enabled":
        marker["scope"]["include_multivariate"] = True
    elif change == "integer-combo-flag":
        marker["scope"]["include_multivariate"] = 0
    elif change == "unknown-scope-field":
        marker["scope"]["other"] = "unsealed setting"
    elif change == "unknown-marker-field":
        marker["other"] = "unsealed setting"
    else:
        marker["storage_version"] = 3
    body = json_bytes(marker)
    (root / "root.json").write_bytes(body)

    with pytest.raises(CatalogueError):
        Store(root)

    assert (root / "root.json").read_bytes() == body
    assert list(root.iterdir()) == [root / "root.json"]


@pytest.mark.parametrize("guard", ["nested", "reacquired"])
@pytest.mark.parametrize("change", ["other-series", "ordinary-v3", "combo-enabled"])
def test_series_writer_guards_recheck_exact_scope(tmp_path, guard, change):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    marker_path = store.path("root.json")
    original = marker_path.read_bytes()
    altered = _scoped_marker()
    if change == "other-series":
        altered["scope"]["series_ticker"] = "KXFEDDECISION"
    elif change == "ordinary-v3":
        altered = common.CURRENT_ROOT_MARKER
    else:
        altered["scope"]["include_multivariate"] = True
    sentinel = store.path("unsafe-write.json")

    if guard == "nested":
        with store.writer_guard():
            marker_path.write_bytes(json_bytes(altered))
            try:
                with pytest.raises(CatalogueError), store.writer_guard():
                    sentinel.write_bytes(b"unsafe")
                assert store.writer_locked_by_current_thread
            finally:
                marker_path.write_bytes(original)
    else:
        with store.writer_guard():
            pass
        marker_path.write_bytes(json_bytes(altered))
        with pytest.raises(CatalogueError), store.writer_guard():
            sentinel.write_bytes(b"unsafe")
        assert not store.writer_locked_by_current_thread

    assert not sentinel.exists()


def test_scope_marker_change_during_lock_acquisition_releases_lock(tmp_path, monkeypatch):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    acquire = common.FileLock.acquire
    changed = False

    def mutate_after_acquiring(lock, *args, **kwargs):
        nonlocal changed
        acquired = acquire(lock, *args, **kwargs)
        if not changed:
            changed = True
            marker = _scoped_marker()
            marker["scope"]["series_ticker"] = "KXFEDDECISION"
            store.path("root.json").write_bytes(json_bytes(marker))
        return acquired

    monkeypatch.setattr(common.FileLock, "acquire", mutate_after_acquiring)
    with pytest.raises(CatalogueError), store.writer_guard():
        pytest.fail("scope was not rechecked after writer-lock acquisition")
    assert not store.writer_locked_by_current_thread
    with common.FileLock(str(store.path("writer.lock")), timeout=0):
        pass
