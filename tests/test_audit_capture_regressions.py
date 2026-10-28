from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import FakeClient

from kalshi_events import capture, publish
from kalshi_events.common import CatalogueError, Store, digest, json_bytes
from kalshi_events.normalize import native_identity


@pytest.mark.parametrize("parent", [0, False, [], {}, 1])
def test_event_identity_rejects_non_string_parent(parent):
    with pytest.raises(CatalogueError, match="native event series ticker"):
        native_identity("events", {"event_ticker": "EVENT", "series_ticker": parent})


@pytest.mark.parametrize(
    ("native", "expected"),
    [
        ({"event_ticker": "EVENT"}, None),
        ({"event_ticker": "EVENT", "series_ticker": None}, None),
        ({"event_ticker": "EVENT", "series_ticker": ""}, ""),
        ({"event_ticker": "EVENT", "series_ticker": "SERIES"}, "SERIES"),
    ],
)
def test_event_identity_preserves_supported_parent_values(native, expected):
    assert native_identity("events", native) == ("EVENT", expected)


def _interrupted_target_capture(store, monkeypatch):
    original = capture._commit_control

    def interrupt_before_completion(actual_store, state, kind, candidate):
        if kind == "complete":
            raise RuntimeError("injected interruption before completion control")
        return original(actual_store, state, kind, candidate)

    with monkeypatch.context() as patch:
        patch.setattr(capture, "_commit_control", interrupt_before_completion)
        with pytest.raises(RuntimeError, match="injected interruption"):
            capture.capture(store, selected=["MARKET"], client_factory=FakeClient)

    capture_id = next(store.path("captures").iterdir()).name
    state = capture.read_capture(store, capture_id)
    assert state["complete"] is False
    assert all(work["done"] and work["pending"] is None for work in state["tasks"])
    return capture_id


@pytest.mark.parametrize("tamper", ["endpoint", "params", "none"])
def test_replay_binds_receipt_endpoint_and_params_before_completion(store, monkeypatch, tamper):
    capture_id = _interrupted_target_capture(store, monkeypatch)
    state = capture.read_capture(store, capture_id)
    row = next(item for item in capture.page_receipts(store, state) if item["index"] == 1)
    manifest_path = store.path(row["relative"])
    manifest = json.loads(manifest_path.read_bytes())
    assert manifest["endpoint"] == "/markets"

    journal_path = store.path(f"captures/{capture_id}/pages.jsonl")
    journal_before = journal_path.read_bytes()
    body_path = manifest_path.with_name("body.bin")
    body_before = body_path.read_bytes()
    request_id = manifest["request_id"]
    if tamper == "endpoint":
        manifest["endpoint"] = "/events"
    elif tamper == "params":
        manifest["params"]["tickers"] = "OTHER"
    if tamper != "none":
        store.write(row["relative"], manifest)
        assert manifest["request_id"] == request_id
        assert body_path.read_bytes() == body_before
        assert journal_path.read_bytes() == journal_before
        with pytest.raises(CatalogueError, match="raw receipt (endpoint|parameters)"):
            capture.capture(store, resume=capture_id, client_factory=FakeClient)
        return

    resumed = capture.capture(store, resume=capture_id, client_factory=FakeClient)
    assert resumed["complete"] is True
    assert capture.complete_capture(store, capture_id)["complete"] is True


def _write_release(store: Store, pointer_id: str | None = None) -> str:
    staging = store.root / "release-staging"
    staging.mkdir()
    inventory = {}
    for filename in sorted(publish.FILES):
        path = staging / filename
        if filename == "coverage.parquet":
            table = pa.Table.from_pylist([{"capture_id": "a" * 32, "coverage_scope": "catalogue"}])
        else:
            table = pa.table({"placeholder": pa.array([], type=pa.string())})
        pq.write_table(table, path)
        body = path.read_bytes()
        inventory[filename] = {"sha256": digest(body), "size": len(body)}

    release_id = digest(json_bytes(inventory).rstrip(b"\n"))
    manifest = {
        "contract": publish.CONTRACT,
        "source_release_id": release_id,
        "capture_id": "a" * 32,
        "received_at": "2026-01-01T00:00:00Z",
        "coverage_scope": "catalogue",
        "files": inventory,
    }
    manifest_body = json_bytes(manifest)
    (staging / "manifest.json").write_bytes(manifest_body)
    pointer_id = pointer_id or release_id
    release = store.path(f"releases/{pointer_id}")
    release.parent.mkdir()
    staging.rename(release)
    store.write(
        "current.json",
        {"source_release_id": pointer_id, "manifest_sha256": digest(manifest_body)},
    )
    return release_id


def test_current_rejects_pointer_alias_and_accepts_canonical_release(store):
    release_id = _write_release(store, pointer_id="b" * 64)
    with pytest.raises(CatalogueError, match="pointer release identity mismatch"):
        publish.current(store)

    store.path(f"releases/{'b' * 64}").rename(store.path(f"releases/{release_id}"))
    manifest_sha256 = digest(store.path(f"releases/{release_id}/manifest.json").read_bytes())
    store.write(
        "current.json",
        {"source_release_id": release_id, "manifest_sha256": manifest_sha256},
    )
    assert publish.current(store)["source_release_id"] == release_id
