from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from kalshi_events import common
from kalshi_events.common import CatalogueError, Store


@pytest.mark.parametrize(
    ("first_ticker", "second_ticker", "expected_version"),
    [(None, "KXCPI", 3), ("KXCPI", None, 4)],
)
def test_concurrent_root_initializers_cannot_replace_each_others_marker(
    tmp_path, monkeypatch, first_ticker, second_ticker, expected_version
):
    root = tmp_path / "root"
    root.mkdir()
    entered_install = threading.Event()
    allow_install = threading.Event()
    atomic_json = common.atomic_json

    def pause_marker_install(path, value):
        if path == root / "root.json":
            entered_install.set()
            assert allow_install.wait(timeout=5)
        atomic_json(path, value)

    monkeypatch.setattr(common, "atomic_json", pause_marker_install)
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(Store, root, series_ticker=first_ticker)
        assert entered_install.wait(timeout=5)

        # The first initializer owns the canonical writer lock while marker installation
        # is paused. The second must fail before it can install a different root profile.
        lock_probe = common.FileLock(str(root / "writer.lock"), timeout=0)
        with pytest.raises(common.Timeout):
            lock_probe.acquire()
        with pytest.raises(CatalogueError):
            Store(root, series_ticker=second_ticker)

        allow_install.set()
        winner = first.result(timeout=5)

    marker_bytes = winner.path("root.json").read_bytes()
    marker = json.loads(marker_bytes)
    assert marker["storage_version"] == expected_version
    assert winner.storage_version == expected_version
    if expected_version == 4:
        assert winner.scope == common.series_scope("KXCPI")
    else:
        assert winner.scope is None
        assert marker_bytes == common.json_bytes(common.CURRENT_ROOT_MARKER)
    with pytest.raises(CatalogueError):
        Store(root, series_ticker=("KXCPI" if expected_version == 3 else "KXGDP"))
    assert winner.path("root.json").read_bytes() == marker_bytes


def test_nonempty_unowned_writer_lock_is_rejected_before_filelock_touches_it(tmp_path, monkeypatch):
    root = tmp_path / "unowned"
    root.mkdir()
    lock_path = root / "writer.lock"
    lock_path.write_bytes(b"user data must remain untouched")

    def forbidden_acquire(*_args, **_kwargs):
        pytest.fail("FileLock must not open an unowned nonempty root")

    monkeypatch.setattr(common.FileLock, "acquire", forbidden_acquire)
    with pytest.raises(CatalogueError, match="new, empty"):
        Store(root, series_ticker="KXCPI")
    assert lock_path.read_bytes() == b"user data must remain untouched"
    assert list(root.iterdir()) == [lock_path]
