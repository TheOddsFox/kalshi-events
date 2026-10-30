from __future__ import annotations

from pathlib import Path

from conftest import FakeClient

from kalshi_events import capture, metrics
from kalshi_events.common import Limits, Store
from kalshi_events.incremental import root_id


def test_root_id_capture_and_resume_use_persistent_parent_index(tmp_path):
    root = tmp_path / "root"
    limited_store = Store(root, Limits(max_requests=3))
    with limited_store.lock():
        logical_root_id = root_id(limited_store, create=True)

    partial = capture.capture(
        limited_store,
        client_factory=FakeClient,
        catalogue_profile="exchange",
    )
    capture_id = partial["capture_id"]
    assert not partial["complete"]
    assert partial["budget_exhausted"]
    assert Path(root / "tmp" / "recovery-index" / capture_id / "index.sqlite").is_file()

    resumed_store = Store(root, Limits(max_requests=100))
    with metrics.collect() as measured:
        resumed = capture.capture(
            resumed_store,
            resume=capture_id,
            client_factory=FakeClient,
            catalogue_profile="exchange",
        )

    assert resumed["capture_id"] == capture_id
    assert resumed["complete"]
    assert root_id(resumed_store) == logical_root_id
    assert measured.counters["recovery_cache_enabled"] == 1
