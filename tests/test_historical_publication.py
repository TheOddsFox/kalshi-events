from __future__ import annotations

import json
import shutil
import threading

import pytest
from conftest import FakeClient

from kalshi_events import capture, incremental, publish, recovery, warehouse
from kalshi_events.common import CatalogueError, Store, digest, regular_bytes
from kalshi_events.historical import (
    historical_selection,
    historical_task_indices,
    is_historical_capture,
)


@pytest.fixture(scope="module")
def historical_publication(tmp_path_factory):
    root = tmp_path_factory.mktemp("historical-publication") / "root"
    store = Store(root)

    legacy_state = capture.capture(store, client_factory=FakeClient, catalogue_profile="exchange")
    warehouse.build(store, legacy_state["capture_id"])
    legacy_release = publish.publish(store, legacy_state["capture_id"])
    legacy_pointer = store.path("current.json").read_bytes()
    legacy_manifest = store.path(
        f"releases/{legacy_release['source_release_id']}/manifest.json"
    ).read_bytes()

    historical_task_ready = threading.Event()
    capture_ids: list[str] = []
    consume = capture._consume

    class InterruptedExchangeClient(FakeClient):
        def response(self, endpoint, params):
            if endpoint == "/events/multivariate":
                if not historical_task_ready.wait(timeout=30):
                    raise CatalogueError("synthetic excluded-scope failure timed out")
                raise CatalogueError("synthetic excluded-scope failure")
            return super().response(endpoint, params)

    def observed_consume(store, state, index, relative, seen, identities):
        consume(store, state, index, relative, seen, identities)
        if index == 5 and state["tasks"][index]["done"]:
            historical_task_ready.set()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(capture, "_consume", observed_consume)
        with pytest.raises(CatalogueError, match="synthetic excluded-scope failure"):
            capture.capture(
                store,
                client_factory=InterruptedExchangeClient,
                catalogue_profile="exchange",
                _on_capture_id=capture_ids.append,
            )

    capture_id = capture_ids[0]
    source_state = capture.read_capture(store, capture_id)
    assert source_state["complete"] is False
    selection = historical_selection(source_state, allow_legacy_incomplete=True)
    selected_indices = historical_task_indices(source_state, allow_legacy_incomplete=True)
    assert set(selection) == {"series", "events_live", "markets_history"}
    assert all(source_state["tasks"][index]["done"] for index in selected_indices)
    assert source_state["tasks"][3]["done"] is False

    warehouse.build_historical(store, capture_id)
    historical_release = publish.publish(store, capture_id, catalogue_version=3)
    return {
        "root": root,
        "capture_id": capture_id,
        "legacy_pointer": legacy_pointer,
        "legacy_manifest": legacy_manifest,
        "legacy_release_id": legacy_release["source_release_id"],
        "historical_release": historical_release,
    }


@pytest.fixture
def isolated_publication(historical_publication, tmp_path):
    root = tmp_path / "root"
    shutil.copytree(historical_publication["root"], root)
    return Store(root), historical_publication


def test_incomplete_legacy_capture_publishes_only_scoped_history_and_keeps_evidence(
    isolated_publication,
):
    store, info = isolated_publication
    capture_id = info["capture_id"]
    manifest = publish.current(store, catalogue_version=3)
    assert manifest["contract"] == "oddsfox.kalshi.historical.v1"
    assert manifest["coverage_scope"] == "historical"
    assert manifest["source_capture_complete"] is False
    assert manifest["included_scopes_json"] == '["events_live","markets_history","series"]'
    assert json.loads(manifest["omitted_scopes_json"]) == [
        "events_combo",
        "markets_live",
        "markets_combo",
    ]
    assert publish.current(store)["source_release_id"] == info["legacy_release_id"]
    assert store.path("current.json").read_bytes() == info["legacy_pointer"]
    assert (
        store.path(f"releases/{info['legacy_release_id']}/manifest.json").read_bytes()
        == info["legacy_manifest"]
    )

    source_state = capture.read_capture(store, capture_id)
    selected = historical_task_indices(source_state, allow_legacy_incomplete=True)
    selected_rows = [
        row for row in capture.page_receipts(store, source_state) if row["index"] in selected
    ]
    assert selected_rows
    assert all(
        store.path(row["relative"]).is_file()
        and store.path(row["relative"].removesuffix("manifest.json") + "body.bin").is_file()
        for row in selected_rows
    )
    with pytest.raises(CatalogueError, match="reachable from a published release"):
        incremental.discard(store, capture_id)
    assert capture.read_capture(store, capture_id)["complete"] is False
    with pytest.raises(CatalogueError, match="capture is incomplete"):
        publish.publish(store, capture_id, catalogue_version=1)
    with pytest.raises(CatalogueError, match="capture is incomplete"):
        publish.publish(store, capture_id, catalogue_version=2)


def test_historical_coverage_tampering_is_detected(isolated_publication):
    store, info = isolated_publication
    release_id = info["historical_release"]["source_release_id"]
    path = store.path(f"releases/{release_id}/manifest.json")
    manifest = json.loads(path.read_bytes())
    manifest["source_capture_complete"] = True
    store.write(f"releases/{release_id}/manifest.json", manifest)
    with pytest.raises(CatalogueError, match="coverage binding mismatch"):
        publish.verify_release(path.parent)


def test_sealed_historical_capture_uses_only_its_contract(isolated_publication):
    store, _ = isolated_publication
    state = capture.capture(store, client_factory=FakeClient)
    assert state["complete"] is True
    assert is_historical_capture(state)
    for version in (1, 2):
        with pytest.raises(CatalogueError, match="only publish as catalogue version 3"):
            publish.publish(store, state["capture_id"], catalogue_version=version)
    with pytest.raises(CatalogueError, match="explicit capture ID"):
        publish.publish(store, catalogue_version=3)

    warehouse.build_historical(store, state["capture_id"])
    release = publish.publish(store, state["capture_id"], catalogue_version=3)
    assert release["contract"] == "oddsfox.kalshi.historical.v1"
    assert release["source_capture_complete"] is True
    assert publish.current(store, catalogue_version=3)["capture_id"] == state["capture_id"]


def test_historical_release_cannot_be_pinned_as_an_incremental_baseline(isolated_publication):
    store, info = isolated_publication
    release = publish.current(store, catalogue_version=3)
    manifest_path = store.path(f"releases/{release['source_release_id']}/manifest.json")
    root_id = incremental.root_id(store)
    pin = {
        "catalogue_version": 2,
        "root_id": root_id,
        "source_release_id": release["source_release_id"],
        "manifest_sha256": digest(regular_bytes(manifest_path)),
        "capture_id": info["capture_id"],
        "delta_depth": None,
    }
    with pytest.raises(
        CatalogueError, match="historical publications cannot be incremental baselines"
    ):
        incremental.verify_pin(store, {"baseline": pin})
    pin["catalogue_version"] = 3
    with pytest.raises(
        CatalogueError, match="incremental baseline must use catalogue version 1 or 2"
    ):
        incremental.verify_pin(store, {"baseline": pin})


def test_historical_replay_rebuilds_source_without_changing_publication(isolated_publication):
    store, info = isolated_publication
    pointer = store.path("current-historical.json").read_bytes()
    result = recovery.replay(store, info["capture_id"], catalogue_version=3)
    assert result["capture_id"] == info["capture_id"]
    warehouse.certified(store, info["capture_id"], catalogue_version=3)
    assert store.path("current-historical.json").read_bytes() == pointer
    assert capture.read_capture(store, info["capture_id"])["complete"] is False
