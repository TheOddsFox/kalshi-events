from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from decimal import Decimal
from pathlib import Path

import dlt
import duckdb
import pytest
from test_arrow_landing import _row
from test_incremental_catalogue import BASE_TIME, Snapshot, build_delta, capture_at, relation

from kalshi_events import publish as publishing
from kalshi_events import warehouse
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
