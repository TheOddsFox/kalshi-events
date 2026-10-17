from __future__ import annotations

import json

import duckdb
import pytest
from test_incremental_catalogue import (
    Snapshot,
    capture_at,
)
from test_incremental_catalogue import (
    baseline as baseline,
)
from test_incremental_catalogue import (
    certified_baseline as certified_baseline,
)

from kalshi_events import incremental, recovery, warehouse
from kalshi_events import publish as publishing
from kalshi_events.common import CatalogueError, digest, file_digest, regular_bytes


def update_manifest(store, release, change):
    relative = f"releases/{release['source_release_id']}/manifest.json"
    manifest = json.loads(store.path(relative).read_text())
    change(manifest)
    store.write(relative, manifest)
    pointer = json.loads(store.path("current-v2.json").read_text())
    pointer["manifest_sha256"] = digest(regular_bytes(store.path(relative)))
    store.write("current-v2.json", pointer)
    return manifest


def recertify_physical_database(store, capture_id):
    relative = f"builds-v2/{capture_id}/build.json"
    receipt = json.loads(store.path(relative).read_text())
    database = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(relative, receipt)
    return warehouse.certified(store, capture_id, catalogue_version=2)


def semantic_queries(monkeypatch):
    queries = []
    original = incremental.duckdb.connect

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query, *args, **kwargs):
            if "except all" in query.lower():
                queries.append(query)
            return self.connection.execute(query, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def __enter__(self):
            self.connection.__enter__()
            return self

        def __exit__(self, *args):
            return self.connection.__exit__(*args)

    def connected(*args, **kwargs):
        return Connection(original(*args, **kwargs))

    monkeypatch.setattr(incremental.duckdb, "connect", connected)
    return queries


def test_new_v2_manifest_pins_the_freshly_certified_database_and_v1_omits_it(baseline):
    store, capture_id, legacy, version_two = baseline
    manifest = publishing.verify_release(store.path(f"releases/{version_two['source_release_id']}"))
    receipt = warehouse.certified(store, capture_id, catalogue_version=2)
    assert manifest["warehouse_sha256"] == receipt["warehouse_sha256"]
    legacy_manifest = publishing.verify_release(
        store.path(f"releases/{legacy['source_release_id']}")
    )
    assert "warehouse_sha256" not in legacy_manifest


@pytest.mark.parametrize(
    "invalid", [None, True, 42, [], {}, "", "a" * 63, "a" * 65, "A" * 64, "g" * 64]
)
def test_optional_warehouse_checksum_rejects_invalid_types_lengths_and_case(baseline, invalid):
    store, _, _, release = baseline
    update_manifest(store, release, lambda manifest: manifest.update(warehouse_sha256=invalid))
    with pytest.raises(CatalogueError, match="warehouse checksum"):
        publishing.verify_release(store.path(f"releases/{release['source_release_id']}"))


def test_matching_v2_database_hash_skips_semantic_comparison_after_fresh_certification(
    baseline, monkeypatch
):
    store, _, _, _ = baseline
    queries = semantic_queries(monkeypatch)
    snapshot = Snapshot()
    assert capture_at(store, snapshot, incremental=True)["complete"]
    assert snapshot.requests
    assert queries == []


@pytest.mark.parametrize("checksum", [None, "0" * 64])
def test_absent_or_different_database_hash_falls_back_to_semantic_comparison(
    baseline, monkeypatch, checksum
):
    store, _, _, release = baseline

    def changed(manifest):
        if checksum is None:
            manifest.pop("warehouse_sha256")
        else:
            manifest["warehouse_sha256"] = checksum

    update_manifest(store, release, changed)
    queries = semantic_queries(monkeypatch)
    assert capture_at(store, Snapshot(), incremental=True)["complete"]
    assert queries, "a missing or mismatched physical hash requires semantic comparison"


def test_freshly_recertified_altered_projection_cannot_bypass_pinned_release(baseline):
    store, capture_id, _, _ = baseline
    database = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "update main.current_markets set last_price_dollars = 0.999 where market_ticker = 'M1'"
        )
        connection.execute("checkpoint")
    recertify_physical_database(store, capture_id)
    snapshot = Snapshot()
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError, match="differ|baseline|projection"):
        capture_at(store, snapshot, incremental=True)
    assert snapshot.requests == []
    assert store.path("current-v2.json").read_bytes() == pointer


def test_physically_different_equivalent_rebuild_uses_semantic_fallback_and_is_accepted(
    baseline, monkeypatch
):
    store, capture_id, _, release = baseline
    manifest_path = store.path(f"releases/{release['source_release_id']}/manifest.json")
    frozen_manifest = manifest_path.read_bytes()
    frozen_hash = json.loads(frozen_manifest)["warehouse_sha256"]
    recovery.rebuild(store, catalogue_version=2)
    database = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute("create table physical_rebuild_marker as select 123::integer as marker")
        connection.execute("checkpoint")
    receipt = recertify_physical_database(store, capture_id)
    assert receipt["warehouse_sha256"] != frozen_hash
    queries = semantic_queries(monkeypatch)
    assert capture_at(store, Snapshot(), incremental=True)["complete"]
    assert queries
    assert manifest_path.read_bytes() == frozen_manifest
    republished = publishing.publish(store, capture_id, catalogue_version=2)
    assert republished["source_release_id"] == release["source_release_id"]
    assert manifest_path.read_bytes() == frozen_manifest
    assert republished["warehouse_sha256"] == frozen_hash


@pytest.mark.parametrize("legacy_manifest", [False, True])
def test_republication_reuses_existing_immutable_manifest_with_or_without_database_hash(
    baseline, legacy_manifest
):
    store, capture_id, _, release = baseline
    if legacy_manifest:
        update_manifest(store, release, lambda manifest: manifest.pop("warehouse_sha256"))
    manifest_path = store.path(f"releases/{release['source_release_id']}/manifest.json")
    frozen = manifest_path.read_bytes()
    republished = publishing.publish(store, capture_id, catalogue_version=2)
    assert republished["source_release_id"] == release["source_release_id"]
    assert manifest_path.read_bytes() == frozen
    assert ("warehouse_sha256" in republished) == (not legacy_manifest)
