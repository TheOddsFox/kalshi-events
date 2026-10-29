"""A standalone v2 verifier binds lineage to the checksummed coverage relation."""

from __future__ import annotations

import copy
import shutil
from datetime import datetime, timedelta

import conftest as fixtures
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import COMBO, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import publish as publishing
from kalshi_events import warehouse
from kalshi_events.common import CatalogueError, Store, digest, json_bytes

BOUND_FIELDS = (
    "capture_id",
    "root_id",
    "baseline_release_id",
    "baseline_manifest_sha256",
    "baseline_capture_id",
    "previous_watermark",
    "watermark_started_at",
    "delta_depth",
    "membership_sha256",
)


class ReconciliationClient(FakeClient):
    def response(self, endpoint, params):
        if endpoint == "/markets" and params.get("mve_filter") == "only":
            return 200, {"markets": [COMBO], "cursor": ""}
        if endpoint == "/markets" and params.get("mve_filter") == "exclude":
            return 200, {"markets": [MARKET], "cursor": ""}
        return super().response(endpoint, params)


@pytest.fixture(scope="module")
def lineage_releases(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("release-lineage") / "root")
    releases = {}
    for day in (1, 2):
        timestamp = f"2026-01-0{day}T12:00:00+00:00"
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(fixtures, "now", lambda timestamp=timestamp: timestamp)
            patch.setattr(acquisition, "now", lambda timestamp=timestamp: timestamp)
            state = acquisition.capture(
                store,
                client_factory=ReconciliationClient,
                incremental=day == 2,
                catalogue_profile="exchange",
            )
        assert state["complete"]
        warehouse.build(store, state["capture_id"])
        if day == 1:
            legacy = publishing.publish(store, state["capture_id"])
            releases["legacy"] = store.path(f"releases/{legacy['source_release_id']}")
        result = publishing.publish(store, state["capture_id"], catalogue_version=2)
        releases["baseline" if day == 1 else "delta"] = store.path(
            f"releases/{result['source_release_id']}"
        )
    return releases


@pytest.fixture
def delta_release(lineage_releases, tmp_path):
    directory = tmp_path / "release"
    shutil.copytree(lineage_releases["delta"], directory)
    manifest = publishing.verify_release(directory)
    assert manifest["delta_depth"] == 1
    coverage = pq.read_table(directory / "coverage.parquet").to_pylist()
    assert len(coverage) == 1
    assert all(manifest[field] == coverage[0][field] for field in BOUND_FIELDS)
    return directory, manifest


def _different_value(field, original):
    if field in {"watermark_started_at", "previous_watermark"}:
        return (
            datetime.fromisoformat(original.replace("Z", "+00:00")) + timedelta(seconds=1)
        ).isoformat()
    if field == "delta_depth":
        return original + 1
    return ("0" if original[0] != "0" else "1") + original[1:]


@pytest.mark.parametrize("field", BOUND_FIELDS)
def test_manifest_cannot_relabel_a_checksummed_delta_release(delta_release, field):
    directory, manifest = delta_release
    parquet_before = {path.name: path.read_bytes() for path in directory.glob("*.parquet")}
    altered = copy.deepcopy(manifest)
    altered[field] = _different_value(field, manifest[field])
    (directory / "manifest.json").write_bytes(json_bytes(altered))

    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)

    assert {path.name: path.read_bytes() for path in directory.glob("*.parquet")} == parquet_before
    assert altered["source_release_id"] == manifest["source_release_id"]


def _replace_coverage(directory, manifest, table, **writer_options):
    pq.write_table(table, directory / "coverage.parquet", **writer_options)
    altered = copy.deepcopy(manifest)
    altered["files"] = publishing.inventory(directory, publishing.FILES, 64 * 1024**3)
    altered["source_release_id"] = digest(json_bytes(altered["files"]).rstrip(b"\n"))
    (directory / "manifest.json").write_bytes(json_bytes(altered))


@pytest.mark.parametrize("rows", [0, 2])
def test_v2_coverage_requires_exactly_one_binding_row(delta_release, rows):
    directory, manifest = delta_release
    table = pq.read_table(directory / "coverage.parquet")
    altered = table.slice(0, 0) if rows == 0 else pa.concat_tables([table, table])
    _replace_coverage(directory, manifest, altered)
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)


@pytest.mark.parametrize("field", BOUND_FIELDS)
def test_v2_coverage_cannot_omit_a_bound_field(delta_release, field):
    directory, manifest = delta_release
    table = pq.read_table(directory / "coverage.parquet").drop([field])
    _replace_coverage(directory, manifest, table)
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)


@pytest.mark.parametrize("value", [None, "1", True])
def test_v2_coverage_depth_requires_the_bound_integer(delta_release, value):
    directory, manifest = delta_release
    row = pq.read_table(directory / "coverage.parquet").to_pylist()[0]
    row["delta_depth"] = value
    _replace_coverage(directory, manifest, pa.Table.from_pylist([row]))
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)


def test_v2_coverage_rejects_duplicate_lineage_columns(delta_release):
    directory, manifest = delta_release
    table = pq.read_table(directory / "coverage.parquet")
    _replace_coverage(directory, manifest, table.append_column("root_id", table["root_id"]))
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)


def test_v2_coverage_rejects_nested_columns_before_lineage(delta_release):
    directory, manifest = delta_release
    table = pq.read_table(directory / "coverage.parquet")
    nested = pa.array([{"first": "one", "second": "two"}])
    _replace_coverage(directory, manifest, table.add_column(0, "unexpected", nested))
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)


@pytest.mark.parametrize("dictionary", [False, True])
def test_v2_coverage_rejects_oversized_compressed_lineage_before_decoding(
    delta_release, monkeypatch, dictionary
):
    directory, manifest = delta_release
    table = pq.read_table(directory / "coverage.parquet")
    index = table.schema.get_field_index("root_id")
    table = table.set_column(index, "root_id", pa.array(["a" * (2 * 1024**2)]))
    _replace_coverage(
        directory,
        manifest,
        table,
        compression="zstd",
        use_dictionary=dictionary,
        write_statistics=False,
    )
    decoded = []
    read = pq.ParquetFile.read

    def observed_read(parquet, *args, **kwargs):
        decoded.append(True)
        return read(parquet, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read", observed_read)
    with pytest.raises(CatalogueError):
        publishing.verify_release(directory)
    assert decoded == [], "oversized lineage must be rejected before allocation and decoding"


@pytest.mark.parametrize("hint", ["removed", "changed"])
def test_optional_warehouse_hint_does_not_change_the_release_binding(delta_release, hint):
    directory, manifest = delta_release
    altered = copy.deepcopy(manifest)
    if hint == "removed":
        altered.pop("warehouse_sha256", None)
    else:
        altered["warehouse_sha256"] = "f" * 64
    (directory / "manifest.json").write_bytes(json_bytes(altered))

    assert publishing.verify_release(directory) == altered
    assert altered["source_release_id"] == manifest["source_release_id"]


@pytest.mark.parametrize("kind", ["baseline", "legacy"])
def test_existing_full_and_v1_releases_remain_readable(lineage_releases, kind):
    directory = lineage_releases[kind]
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    manifest = publishing.verify_release(directory)
    assert manifest["contract"] == f"oddsfox.kalshi.catalogue.v{1 if kind == 'legacy' else 2}"
    if kind == "baseline":
        assert manifest["delta_depth"] == 0
        assert manifest["baseline_capture_id"] is None
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before


def test_v2_verifier_uses_pyarrow17_compatible_constructor_arguments(delta_release, monkeypatch):
    directory, manifest = delta_release
    constructor = pq.ParquetFile
    inspected = []

    def pyarrow17_constructor(
        source, *, thrift_string_size_limit=None, thrift_container_size_limit=None
    ):
        # This subset of the 17.x signature rejects newer-only constructor keywords.
        inspected.append(source)
        return constructor(
            source,
            thrift_string_size_limit=thrift_string_size_limit,
            thrift_container_size_limit=thrift_container_size_limit,
        )

    monkeypatch.setattr(pq, "ParquetFile", pyarrow17_constructor)

    assert publishing.verify_release(directory) == manifest
    assert directory / "coverage.parquet" in inspected
