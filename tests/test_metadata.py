from __future__ import annotations

import json

import pytest
from conftest import MARKET, FakeClient

from kalshi_events.capture import capture
from kalshi_events.common import CatalogueError, Limits, Store, selections
from kalshi_events.metadata import export, verify


def test_targeted_contract_lossless_and_checksums(store, factory, tmp_path):
    state = capture(store, selected=["MARKET", "HISTORICAL", "MISSING"], client_factory=factory)
    output = tmp_path / "metadata"
    result = export(
        store, ["MARKET", "HISTORICAL", "MISSING"], output, capture_id=state["capture_id"]
    )
    manifest = verify(output)
    assert result["source_release_id"] == manifest["source_release_id"]
    assert set(manifest["files"]) == {"markets.json", "events.json", "series.json", "coverage.json"}
    markets = json.loads((output / "markets.json").read_text())
    assert len(markets) == 2
    assert markets[0]["series_ticker"] == "SERIES"
    assert markets[0]["usable"] is True
    assert markets[0]["last_price_dollars"] == "0.00123"
    coverage = json.loads((output / "coverage.json").read_text())
    assert [row["status"] for row in coverage] == ["found", "found", "absent"]
    (output / "markets.json").write_text("[]")
    with pytest.raises(CatalogueError, match="integrity"):
        verify(output)


def test_no_stale_fallback_after_direct_absence(store, factory, tmp_path):
    capture(store, selected=["MARKET"], client_factory=factory)
    state = capture(
        store,
        selected=["MARKET"],
        client_factory=lambda s, b: FakeClient(s, b, {"/markets/MARKET": (404, {})}),
    )
    output = tmp_path / "metadata"
    export(store, ["MARKET"], output)
    assert state["coverage"]["MARKET"]["status"] == "absent"
    assert json.loads((output / "markets.json").read_text()) == []


def test_output_ceiling_and_new_destination(store, factory, tmp_path):
    state = capture(store, selected=["MARKET"], client_factory=factory)
    with pytest.raises(CatalogueError, match="ceiling"):
        export(
            store,
            ["MARKET"],
            tmp_path / "too-small",
            capture_id=state["capture_id"],
            max_output_bytes=100,
        )
    assert not (tmp_path / "too-small").exists()
    output = tmp_path / "bundle"
    export(store, ["MARKET"], output)
    with pytest.raises(CatalogueError, match="new"):
        export(store, ["MARKET"], output)
    with pytest.raises(CatalogueError, match="128"):
        export(store, ["MARKET"], tmp_path / "oversized", max_output_bytes=128 * 1024**2 + 1)


def test_metadata_above_old_32mib_file_ceiling(store, tmp_path):
    limits = Limits(max_download_bytes=256 * 1024**2, max_response_bytes=40 * 1024**2)
    store = Store(store.root, limits)
    native = {**MARKET, "rules_primary": "x" * (33 * 1024**2)}
    state = capture(
        store,
        selected=["MARKET"],
        client_factory=lambda s, b: FakeClient(
            s, b, {"/markets/MARKET": (200, {"market": native})}
        ),
    )
    output = tmp_path / "large"
    export(store, ["MARKET"], output, capture_id=state["capture_id"])
    assert (output / "markets.json").stat().st_size > 32 * 1024**2
    verify(output)


@pytest.mark.parametrize("values", [[], ["A"] * 2, ["A"] * 101, ["../bad"], ["bad/value"]])
def test_selection_trust_boundary(values):
    with pytest.raises(CatalogueError):
        selections(values)


def test_symlinks_are_refused(store, factory, tmp_path):
    state = capture(store, selected=["MARKET"], client_factory=factory)
    link = tmp_path / "linked"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(CatalogueError, match="symlink"):
        export(store, ["MARKET"], link / "output", capture_id=state["capture_id"])
