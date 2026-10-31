from __future__ import annotations

import pytest
from conftest import FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.common import CatalogueError, Store, digest, json_bytes


def _state(store: Store):
    return acquisition.new_capture(store, None, catalogue_profile="exchange")


def _orphan(store: Store, state, index: int, payload, relative: str, *, body_bytes=None, **changes):
    context = acquisition._request_context(state, index)
    endpoint, params, _, singular = acquisition._plan(state["tasks"][index], "page")
    body = json_bytes(payload) if body_bytes is None else body_bytes
    manifest = {
        **context,
        "page_id": relative.split("/")[1],
        "version": 1,
        "endpoint": endpoint,
        "params": params,
        "singular": singular,
        "received_at": "2026-01-02T12:00:00Z",
        "status": 200,
        "size": len(body),
        "sha256": digest(body),
        "downloaded_bytes": len(body),
        "accounting_complete": True,
    }
    manifest.update(changes)
    store.write_bytes(relative.removesuffix("manifest.json") + "body.bin", body)
    store.write(relative, manifest)
    return context, relative


@pytest.mark.parametrize("body_bytes", [None, b"{"], ids=["invalid-envelope", "invalid-json"])
def test_invalid_hash_valid_orphan_is_retried_and_preserved(tmp_path, body_bytes):
    store = Store(tmp_path / "root")
    state = _state(store)
    context, relative = _orphan(
        store,
        state,
        1,
        {"not_series": []},
        "raw/orphan-invalid/manifest.json",
        body_bytes=body_bytes,
    )
    original_manifest = store.path(relative).read_bytes()
    original_body = store.path("raw/orphan-invalid/body.bin").read_bytes()
    requests = []

    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            requests.append((endpoint, context["request_id"]))
            return super().get(endpoint, params, context)

    resumed = acquisition.capture(
        store,
        resume=state["capture_id"],
        client_factory=RecordingClient,
    )

    assert resumed["complete"]
    assert ("/series", context["request_id"]) in requests
    assert store.path(relative).read_bytes() == original_manifest
    assert store.path("raw/orphan-invalid/body.bin").read_bytes() == original_body
    assert relative not in {row["relative"] for row in acquisition.page_receipts(store, resumed)}


def test_invalid_orphan_does_not_duplicate_a_valid_same_context_receipt(tmp_path):
    store = Store(tmp_path / "root")
    state = _state(store)
    context, invalid = _orphan(store, state, 1, {"not_series": []}, "raw/a-invalid/manifest.json")
    _, valid = _orphan(store, state, 1, {"series": []}, "raw/z-valid/manifest.json")

    assert acquisition._recover_receipts(store, state) == {context["request_id"]: valid}
    assert store.path(invalid).is_file()
    assert store.path(valid).is_file()
    assert state.page_rows == []


def test_recovery_still_fails_on_orphan_integrity_mismatch(tmp_path):
    store = Store(tmp_path / "root")
    state = _state(store)
    _, relative = _orphan(store, state, 1, {"series": []}, "raw/orphan/manifest.json")
    store.write_bytes("raw/orphan/body.bin", b"{")

    with pytest.raises(CatalogueError, match="integrity mismatch"):
        acquisition._recover_receipts(store, state)


def test_recovery_still_fails_on_orphan_context_mismatch(tmp_path):
    store = Store(tmp_path / "root")
    state = _state(store)
    _, relative = _orphan(
        store,
        state,
        1,
        {"series": []},
        "raw/orphan/manifest.json",
        acquisition_protocol=acquisition.CURRENT_ACQUISITION_PROTOCOL - 1,
    )

    with pytest.raises(CatalogueError, match="traversal|protocol"):
        acquisition._recover_receipts(store, state)


def test_recovery_still_fails_on_orphan_scope_mismatch(tmp_path):
    store = Store(tmp_path / "root", series_ticker="SERIES")
    with store.writer_guard():
        state = acquisition.new_capture(store, None)
    _orphan(
        store,
        state,
        2,
        {"events": [{"event_ticker": "EVENT", "series_ticker": "OTHER"}], "cursor": ""},
        "raw/orphan/manifest.json",
    )

    with pytest.raises(CatalogueError, match="outside the scoped series"):
        acquisition._recover_receipts(store, state)


def test_recovery_does_not_skip_untyped_preflight_faults(tmp_path, monkeypatch):
    store = Store(tmp_path / "root")
    state = _state(store)
    _orphan(store, state, 1, {"series": []}, "raw/orphan/manifest.json")

    def fail_read(*_args, **_kwargs):
        raise OSError("raw evidence unavailable")

    monkeypatch.setattr(acquisition, "read_page", fail_read)
    with pytest.raises(OSError, match="raw evidence unavailable"):
        acquisition._recover_receipts(store, state)
