from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
from decimal import Decimal
from pathlib import Path

import dlt
import duckdb
import pyarrow.parquet as pq
import pytest
from test_arrow_landing import _row
from test_incremental_catalogue import BASE_TIME, Snapshot, build_delta, capture_at, relation

from kalshi_events import capture as acquisition
from kalshi_events import publish as publishing
from kalshi_events import recovery, warehouse
from kalshi_events.common import Store

_ROOT_ENV = "KALSHI_DUCKDB_UPGRADE_ROOT"
_CAPTURE_ID = "1" * 32
_RECEIPT_COLUMNS = (
    "observation_id",
    "capture_id",
    "received_at",
    "provider_updated_at",
    "entity_ticker",
    "series_ticker",
    "payload_json",
)


def _upgrade_root() -> Path:
    value = os.environ.get(_ROOT_ENV)
    if not value:
        pytest.skip(f"{_ROOT_ENV} is set by scripts/check-duckdb-upgrade.py")
    return Path(value)


def _load_harness():
    script = Path(__file__).resolve().parents[1] / "scripts/check-duckdb-upgrade.py"
    spec = importlib.util.spec_from_file_location("duckdb_upgrade_harness", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _landing_schema(connection):
    columns = set(warehouse._LANDING_COLUMNS)
    return [
        [row[0], row[1], row[2]]
        for row in connection.execute("describe landing.observations").fetchall()
        if row[0] in columns
    ]


def _reference_rows(connection):
    columns = ", ".join(_RECEIPT_COLUMNS)
    return [
        list(row)
        for row in connection.execute(
            f"select {columns} from landing.observations order by observation_id"
        ).fetchall()
    ]


def _series_snapshot():
    snapshot = Snapshot()
    snapshot.combos[0]["mve_selected_legs"][0]["yes_settlement_value_dollars"] = "0.00001"
    return snapshot


def _assert_series_output(store, capture_id, release):
    markets = {
        row["market_ticker"]: row
        for row in relation(store, capture_id, "current_markets", catalogue_version=2)
    }
    assert markets["M1"]["last_price_dollars"] == Decimal("0.00123")
    assert markets["M1"]["volume_fp"] == Decimal("12.3456789")
    with duckdb.connect() as connection:
        value = connection.execute(
            "select yes_settlement_value_dollars from read_parquet(?) "
            "where combo_market_ticker = 'C'",
            [str(Path(release["output"]) / "combo_legs.parquet")],
        ).fetchone()[0]
    assert value == Decimal("0.00001")


def _database_hash(store, directory: str, capture_id: str) -> str:
    path = store.path(f"{directory}/{capture_id}/warehouse.duckdb")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _database_storage_version(path: Path) -> str:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(
            "select tags['storage_version'] from duckdb_databases() where not internal"
        ).fetchone()[0]


def _capture_input_hashes(store: Store, capture_id: str) -> dict[str, str]:
    state = acquisition.read_capture(store, capture_id)
    paths = {
        store.path(f"captures/{capture_id}/state.json"),
        store.path(f"captures/{capture_id}/pages.jsonl"),
    }
    receipts = acquisition.page_receipts(store, state)
    assert receipts
    receipt_paths = []
    for receipt in receipts:
        manifest = store.path(receipt["relative"])
        body = manifest.parent / "body.bin"
        assert manifest.is_file() and body.is_file()
        receipt_paths.extend((manifest, body))
    paths.update(receipt_paths)
    hashes = {
        str(path.relative_to(store.root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
        if path.is_file()
    }
    for receipt in receipts:
        manifest_relative = receipt["relative"]
        body_relative = (store.path(manifest_relative).parent / "body.bin").relative_to(store.root)
        assert hashes[str(manifest_relative)]
        assert hashes[str(body_relative)]
    return hashes


def _schema_receipt(schema):
    def metadata_receipt(metadata):
        return {key.hex(): value.hex() for key, value in sorted((metadata or {}).items())}

    return {
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
                "metadata": metadata_receipt(field.metadata),
            }
            for field in schema
        ],
        "metadata": metadata_receipt(schema.metadata),
    }


def test_write_reference_dictionary_baseline(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "1.5.6"
    monkeypatch.chdir(tmp_path)
    store = Store(root)
    directory = store.path("tmp/dict-upgrade")
    directory.mkdir(parents=True)
    seed = _row(
        observation_id="observation-existing",
        capture_id=_CAPTURE_ID,
        entity_ticker="SERIES-EXISTING",
        series_ticker="SERIES-EXISTING",
        received_at="2026-10-26T12:30:00Z",
        payload_json='{"price":0.1,"label":"reference"}',
    )
    logical_columns = {
        field.name: {"data_type": "text", "nullable": field.nullable}
        for field in warehouse._LANDING_SCHEMA
    }
    dlt.pipeline(
        pipeline_name="kalshi_catalogue",
        pipelines_dir=str(directory / "landing"),
        destination=dlt.destinations.duckdb(str(directory / "warehouse.duckdb")),
        dataset_name="landing",
    ).run(
        dlt.resource(
            [seed],
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )
    with duckdb.connect(str(directory / "warehouse.duckdb"), read_only=True) as connection:
        receipt = {
            "duckdb_version": duckdb.__version__,
            "schema": _landing_schema(connection),
            "rows": _reference_rows(connection),
        }
    (directory / "reference-receipt.json").write_text(json.dumps(receipt))
    store.sync(str(directory.relative_to(store.root)))
    assert receipt["rows"] == [
        [
            "observation-existing",
            _CAPTURE_ID,
            "2026-10-26T12:30:00Z",
            None,
            "SERIES-EXISTING",
            "SERIES-EXISTING",
            '{"price":0.1,"label":"reference"}',
        ]
    ]


def test_candidate_arrow_merge_replays_reference_dictionary_baseline(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "2.0.0"
    monkeypatch.chdir(tmp_path)
    store = Store(root)
    directory = store.path("tmp/dict-upgrade")
    receipt = json.loads((directory / "reference-receipt.json").read_text())
    assert receipt["duckdb_version"] == "1.5.6"
    assert receipt["rows"] == [
        [
            "observation-existing",
            _CAPTURE_ID,
            "2026-10-26T12:30:00Z",
            None,
            "SERIES-EXISTING",
            "SERIES-EXISTING",
            '{"price":0.1,"label":"reference"}',
        ]
    ]
    wide_text = "界🌉" * 8192
    prices = (
        "0.12345678901234567890123456789",
        "-0.00000000000000000000000000001",
    )
    source_rows = [
        {
            "entity_kind": "series",
            "series_ticker": ticker,
            "observation_id": observation_id,
            "received_at": "2026-10-27T12:30:00.123456789+05:30",
            "provider_updated_at": None,
            "source_endpoint": "/series",
            "source_kind": "direct",
            "payload_json": f'{{"price":{price},"label":"{wide_text}"}}',
        }
        for observation_id, ticker, price in (
            ("observation-existing", "SERIES-EXISTING", prices[0]),
            ("observation-new", "SERIES-NEW", prices[1]),
        )
    ]
    manifest = {
        "capture_id": _CAPTURE_ID,
        "page_id": "page-null-unicode",
        "received_at": "2026-10-27T12:30:00.123456789+05:30",
        "endpoint": "/series",
        "sha256": "f" * 64,
        "status": 200,
    }
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: [(manifest, {})])
    monkeypatch.setattr(warehouse, "observations", lambda _pages: iter(source_rows))

    assert warehouse._land_rows(store, {"capture_id": _CAPTURE_ID}, directory) == 2
    with duckdb.connect(str(directory / "warehouse.duckdb"), read_only=True) as connection:
        assert _landing_schema(connection) == receipt["schema"]
        first_result = _reference_rows(connection)
        assert connection.execute("select count(*) from landing.observations").fetchone() == (2,)

    assert first_result == [
        [
            row["observation_id"],
            _CAPTURE_ID,
            manifest["received_at"],
            None,
            row["series_ticker"],
            row["series_ticker"],
            row["payload_json"],
        ]
        for row in source_rows
    ]
    assert warehouse._land_rows(store, {"capture_id": _CAPTURE_ID}, directory) == 2
    with duckdb.connect(str(directory / "warehouse.duckdb"), read_only=True) as connection:
        assert _reference_rows(connection) == first_result
        assert connection.execute("select count(*) from landing.observations").fetchone() == (2,)


def test_write_reference_series_baselines(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "1.5.6"
    monkeypatch.chdir(tmp_path)
    receipts = {}
    for name, include_v2 in (("series-v1", False), ("series-v2", True)):
        store = Store(root / name)
        state = capture_at(store, _series_snapshot(), BASE_TIME)
        assert state["complete"]
        warehouse.build(store, state["capture_id"])
        publishing.publish(store, state["capture_id"])
        receipts[name] = state["capture_id"]
        if include_v2:
            publishing.publish(store, state["capture_id"], catalogue_version=2)
    (root / "series-receipt.json").write_text(json.dumps(receipts))


def test_candidate_bootstraps_v2_and_deltas_reference_baselines(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "2.0.0"
    monkeypatch.chdir(tmp_path)
    receipts = json.loads((root / "series-receipt.json").read_text())

    v1_store = Store(root / "series-v1")
    v1_capture_id = receipts["series-v1"]
    v1_hash = _database_hash(v1_store, "builds", v1_capture_id)
    legacy_path = v1_store.path(f"builds/{v1_capture_id}/warehouse.duckdb")
    assert _database_storage_version(legacy_path).startswith("v1.")
    bootstrap = warehouse.build_v2_full(v1_store, v1_capture_id)
    native_path = v1_store.path(f"builds-v2/{v1_capture_id}/warehouse.duckdb")
    assert _database_storage_version(native_path) == "v2.0.0+"
    assert warehouse.certified(v1_store, v1_capture_id, catalogue_version=2) == bootstrap
    bootstrap_release = publishing.publish(v1_store, v1_capture_id, catalogue_version=2)
    assert _database_hash(v1_store, "builds", v1_capture_id) == v1_hash
    _assert_series_output(v1_store, v1_capture_id, bootstrap_release)

    v2_store = Store(root / "series-v2")
    v2_hash = _database_hash(v2_store, "builds-v2", receipts["series-v2"])
    delta = build_delta(v2_store, _series_snapshot())
    assert delta["baseline"]["capture_id"] == receipts["series-v2"]
    delta_release = publishing.publish(v2_store, delta["capture_id"], catalogue_version=2)
    assert _database_hash(v2_store, "builds-v2", receipts["series-v2"]) == v2_hash
    _assert_series_output(v2_store, delta["capture_id"], delta_release)


def test_candidate_rebuild_preserves_reference_v1_public_schemas(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "2.0.0"
    monkeypatch.chdir(tmp_path)

    reference_store = Store(root / "series-v1")
    reference_release = publishing.current(reference_store)
    assert reference_release["contract"] == publishing.CONTRACT
    capture_id = reference_release["capture_id"]
    reference_release_path = Path(reference_release["output"])
    reference_manifest_path = reference_release_path / "manifest.json"
    reference_manifest_bytes = reference_manifest_path.read_bytes()
    reference_pointer_path = reference_store.path("current.json")
    reference_pointer_bytes = reference_pointer_path.read_bytes()
    reference_database = reference_store.path(f"builds/{capture_id}/warehouse.duckdb")
    assert _database_storage_version(reference_database).startswith("v1.")
    reference_database_sha256 = hashlib.sha256(reference_database.read_bytes()).hexdigest()
    reference_receipts = _capture_input_hashes(reference_store, capture_id)

    candidate_root = root / "series-v1-candidate"
    shutil.copytree(reference_store.root, candidate_root)
    candidate_store = Store(candidate_root)
    assert publishing.current(candidate_store)["capture_id"] == capture_id
    assert _capture_input_hashes(candidate_store, capture_id) == reference_receipts

    rebuilt = recovery.rebuild(candidate_store)
    assert rebuilt["capture_id"] == capture_id
    assert _capture_input_hashes(reference_store, capture_id) == reference_receipts
    assert _capture_input_hashes(candidate_store, capture_id) == reference_receipts
    candidate_database = candidate_store.path(f"builds/{capture_id}/warehouse.duckdb")
    assert _database_storage_version(candidate_database).startswith("v2.0.0+")
    candidate_release = publishing.publish(candidate_store, capture_id, catalogue_version=1)
    assert candidate_release["contract"] == publishing.CONTRACT
    assert candidate_release["capture_id"] == capture_id

    expected_files = {f"{name}.parquet" for name in (*warehouse.RELATIONS, "coverage")}
    assert expected_files == publishing.FILES
    comparison = {
        "capture_id": capture_id,
        "reference_release_id": reference_release["source_release_id"],
        "candidate_release_id": candidate_release["source_release_id"],
        "reference_build_sha256": reference_database_sha256,
        "candidate_build_sha256": hashlib.sha256(candidate_database.read_bytes()).hexdigest(),
        "reference_manifest_sha256": hashlib.sha256(reference_manifest_bytes).hexdigest(),
        "reference_pointer_sha256": hashlib.sha256(reference_pointer_bytes).hexdigest(),
        "files": {},
    }
    report_path = root / "series-v1-public-schema-comparison.json"
    candidate_release_path = Path(candidate_release["output"])
    for filename in sorted(expected_files):
        reference_path = reference_release_path / filename
        candidate_path = candidate_release_path / filename
        reference_table = pq.read_table(reference_path)
        candidate_table = pq.read_table(candidate_path)
        schemas_equal = reference_table.schema.equals(candidate_table.schema, check_metadata=True)
        reference_rows = reference_table.to_pylist()
        candidate_rows = candidate_table.to_pylist()
        rows_equal = reference_rows == candidate_rows
        comparison["files"][filename] = {
            "reference_sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
            "candidate_sha256": hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
            "row_count": len(reference_rows),
            "schemas_equal": schemas_equal,
            "ordered_values_equal": rows_equal,
            "reference_schema": _schema_receipt(reference_table.schema),
            "candidate_schema": _schema_receipt(candidate_table.schema),
        }
        report_path.write_text(json.dumps(comparison, indent=2, sort_keys=True) + "\n")
        assert schemas_equal, (
            f"{filename} Arrow schema differs between DuckDB 1.5.6 and 2.0.0:\n"
            f"reference: {reference_table.schema!r}\n"
            f"candidate: {candidate_table.schema!r}"
        )
        assert rows_equal, f"{filename} ordered values differ"

    assert _capture_input_hashes(candidate_store, capture_id) == reference_receipts
    assert hashlib.sha256(reference_database.read_bytes()).hexdigest() == reference_database_sha256
    assert reference_manifest_path.read_bytes() == reference_manifest_bytes
    assert reference_pointer_path.read_bytes() == reference_pointer_bytes


def test_reference_reader_rejects_candidate_native_v2_baseline(monkeypatch, tmp_path):
    root = _upgrade_root()
    assert duckdb.__version__ == "1.5.6"
    monkeypatch.chdir(tmp_path)
    receipts = json.loads((root / "series-receipt.json").read_text())
    path = root / "series-v1" / "builds-v2" / receipts["series-v1"] / "warehouse.duckdb"
    assert path.is_file()

    with pytest.raises(duckdb.Error):
        duckdb.connect(str(path), read_only=True)


def test_harness_preserves_venv_symlink_and_resolves_relative_paths(tmp_path):
    harness = _load_harness()
    target = tmp_path / "base-python"
    target.write_text("interpreter")
    interpreter = tmp_path / "venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(target)

    resolved = harness._interpreter_path(Path("venv/bin/python"), tmp_path)
    assert resolved == interpreter.absolute()
    assert resolved.is_symlink()
    assert harness._absolute_path(Path("logs"), tmp_path) == (tmp_path / "logs").absolute()


def test_harness_imports_application_from_expected_source():
    source = os.environ.get("KALSHI_DUCKDB_UPGRADE_APPLICATION_SOURCE")
    if not source:
        pytest.skip("source origin is set by scripts/check-duckdb-upgrade.py")
    expected = Path(source).resolve() / "src/kalshi_events"
    assert Path(warehouse.__file__).resolve().is_relative_to(expected)


def test_harness_timeout_reaps_child_group_and_saves_logs(tmp_path):
    harness = _load_harness()
    benchmark = harness._load_benchmark_supervisor()
    marker = tmp_path / "descendant-terminated"
    descendant_code = (
        "import signal,sys,time; "
        f"signal.signal(signal.SIGTERM, lambda *_: (open({str(marker)!r}, 'w').write('done'), "
        "sys.exit(0))); time.sleep(60)"
    )
    worker = tmp_path / "fake-python"
    worker.write_text(
        "#!/usr/bin/env python3\n"
        "import subprocess,sys,time\n"
        f"subprocess.Popen([sys.executable, '-c', {descendant_code!r}])\n"
        "time.sleep(60)\n"
    )
    worker.chmod(0o755)

    status = harness._run(
        "tiny timeout",
        worker,
        ("ignored-test",),
        os.environ.copy(),
        tmp_path / "logs",
        benchmark._run_supervised,
        timeout=6,
    )

    assert status == 124
    assert marker.read_text() == "done"
    assert "timed out after 6 seconds" in (tmp_path / "logs/tiny-timeout.stderr.log").read_text()
