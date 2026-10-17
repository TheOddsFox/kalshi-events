"""Writer guards read the marker freshly even when its canonical bytes skip JSON parsing."""

from __future__ import annotations

import json

import pytest

from kalshi_events import common, metrics
from kalshi_events.common import (
    CURRENT_ROOT_MARKER,
    OWNER_MARKER,
    CatalogueError,
    Store,
    json_bytes,
)


def test_canonical_writer_guards_read_fresh_bytes_without_json_decodes(store, monkeypatch):
    marker = store.root / "root.json"
    expected = marker.read_bytes()
    assert expected == json_bytes(CURRENT_ROOT_MARKER)
    reads = []
    regular_bytes = common.regular_bytes

    def observed_read(path, *args, **kwargs):
        body = regular_bytes(path, *args, **kwargs)
        if path == marker:
            reads.append(body)
        return body

    monkeypatch.setattr(common, "regular_bytes", observed_read)
    with metrics.collect() as measured:
        with store.writer_guard():
            with store.writer_guard():
                assert store.storage_version == 2
            with store.writer_guard():
                assert store.storage_version == 2
        with store.writer_guard():
            assert store.storage_version == 2

    assert reads == [expected] * 4
    assert measured.report()["counters"].get("json_decodes", 0) == 0
    assert marker.read_bytes() == expected


@pytest.mark.parametrize("encoding", ["whitespace", "reordered", "escaped"])
def test_equivalent_noncanonical_markers_use_the_json_fallback(store, encoding):
    marker = store.root / "root.json"
    if encoding == "whitespace":
        body = (json.dumps(CURRENT_ROOT_MARKER, indent=4) + " \n").encode()
    elif encoding == "reordered":
        body = json.dumps(dict(reversed(list(CURRENT_ROOT_MARKER.items())))).encode()
    else:
        body = json_bytes(CURRENT_ROOT_MARKER).replace(b"oddsfox", b"\\u006fddsfox")
    assert body != json_bytes(CURRENT_ROOT_MARKER)
    marker.write_bytes(body)

    with metrics.collect() as measured, store.writer_guard(), store.writer_guard():
        assert store.storage_version == 2

    assert measured.report()["counters"]["json_decodes"] == 2
    assert marker.read_bytes() == body


def test_legacy_marker_remains_readable_only_with_explicit_legacy_writer_lock(tmp_path):
    root = tmp_path / "legacy"
    root.mkdir()
    marker = root / "root.json"
    marker.write_bytes(json_bytes(OWNER_MARKER))
    store = Store(root)
    assert store.storage_version == 1

    with store.lock(allow_legacy=True):
        assert store.writer_locked_by_current_thread
        with pytest.raises(CatalogueError, match="upgrade-root"), store.writer_guard():
            pytest.fail("a legacy root cannot enter a normal writer guard")
    with pytest.raises(CatalogueError, match="upgrade-root"), store.writer_guard():
        pytest.fail("a legacy root cannot acquire a normal writer guard")
    assert marker.read_bytes() == json_bytes(OWNER_MARKER)


def _change_marker(marker, change, tmp_path):
    if change == "missing":
        marker.unlink()
    elif change == "symlink":
        target = tmp_path / "external-marker.json"
        target.write_bytes(json_bytes(CURRENT_ROOT_MARKER))
        marker.unlink()
        marker.symlink_to(target)
    else:
        body = {
            "legacy": json_bytes(OWNER_MARKER),
            "foreign": json_bytes({"owner": "another.application", "storage_version": 2}),
            "wrong-version": json_bytes({**CURRENT_ROOT_MARKER, "storage_version": 3}),
            "malformed": b'{"owner":',
            "trailing-data": json_bytes(CURRENT_ROOT_MARKER) + b"not-json",
            "nonobject": b"[]\n",
        }[change]
        marker.write_bytes(body)


@pytest.mark.parametrize(
    "change",
    [
        "legacy",
        "foreign",
        "wrong-version",
        "malformed",
        "trailing-data",
        "nonobject",
        "missing",
        "symlink",
    ],
)
@pytest.mark.parametrize("guard", ["nested", "reacquired"])
def test_every_writer_guard_rejects_marker_mutations(store, tmp_path, change, guard):
    marker = store.root / "root.json"
    sentinel = store.root / "must-not-be-written"
    if guard == "nested":
        with store.writer_guard():
            _change_marker(marker, change, tmp_path)
            with pytest.raises(CatalogueError), store.writer_guard():
                sentinel.write_text("unsafe write")
            assert store.writer_locked_by_current_thread
            if marker.is_symlink():
                marker.unlink()
            marker.write_bytes(json_bytes(CURRENT_ROOT_MARKER))
            with store.writer_guard():
                assert store.storage_version == 2
    else:
        with store.writer_guard():
            pass
        _change_marker(marker, change, tmp_path)
        with pytest.raises(CatalogueError), store.writer_guard():
            sentinel.write_text("unsafe write")
        assert not store.writer_locked_by_current_thread
    assert not sentinel.exists()
    if change == "symlink":
        assert (tmp_path / "external-marker.json").read_bytes() == json_bytes(CURRENT_ROOT_MARKER)
