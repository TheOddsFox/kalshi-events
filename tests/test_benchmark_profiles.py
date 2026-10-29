from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts/benchmark-pipeline.py"
SPEC = importlib.util.spec_from_file_location("benchmark_pipeline", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)

# Frozen from the original committed FixtureClient and capture request order.
FIXTURE_SHA256 = {
    197: "daae576d1b834f674be75ddec22625907f186e56dbaa6f18d33e4ba153d8748c",
    1203: "18bc16bf79c79af4d55c280138f8cbe1f5a9b3514f63835a3830a47ab9d7da4d",
}


def _canonical_full_fixture_hash(markets: int) -> tuple[str, int, int]:
    fixture = benchmark.FixtureClient(markets)
    requests = [
        ("/historical/cutoff", {}),
        ("/series", {}),
        ("/events", {}),
        ("/events/multivariate", {}),
    ]
    requests.extend(
        ("/markets", {"cursor": str(start)} if start else {})
        for start in range(0, markets, benchmark.PAGE_SIZE)
    )
    requests.extend([("/historical/cutoff", {}), ("/historical/markets", {})])
    digest = hashlib.sha256()
    body_bytes = 0
    for endpoint, params in requests:
        status, payload = fixture.response(endpoint, params)
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        label = endpoint + "?" + json.dumps(params, sort_keys=True, separators=(",", ":"))
        digest.update(
            label.encode()
            + b"\0"
            + str(status).encode()
            + b"\0"
            + str(len(body)).encode()
            + b"\0"
            + body
            + b"\n"
        )
        body_bytes += len(body)
    return digest.hexdigest(), body_bytes, len(requests)


@pytest.mark.parametrize(
    ("market_count", "expected_bytes", "expected_requests"),
    [(197, 74904, 7), (1203, 455413, 13)],
)
def test_full_fixture_matches_original_request_hash(
    market_count: int, expected_bytes: int, expected_requests: int
):
    fixture_hash, body_bytes, requests = _canonical_full_fixture_hash(market_count)
    assert fixture_hash == FIXTURE_SHA256[market_count]
    assert body_bytes == expected_bytes
    assert requests == expected_requests


def test_worker_source_selection_imports_each_requested_tree_in_isolated_process(
    tmp_path: Path,
):
    sources = [tmp_path / "source-a", tmp_path / "source-b"]
    for source, value in zip(sources, ("a", "b"), strict=True):
        package = source / "src/kalshi_events"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        (package / "origin.py").write_text(f"VALUE = {value!r}\n")

    code = "\n".join(
        [
            "import importlib.util, json, sys",
            f"spec = importlib.util.spec_from_file_location('pipeline', {str(SCRIPT)!r})",
            "module = importlib.util.module_from_spec(spec)",
            "spec.loader.exec_module(module)",
            f"sources = {list(map(str, sources))!r}",
            "observed = []",
            "for source in sources:",
            "    module._select_application_source(__import__('pathlib').Path(source))",
            "    origin = __import__('kalshi_events.origin', fromlist=['VALUE'])",
            "    observed.append((origin.VALUE, origin.__file__))",
            "print(json.dumps(observed))",
        ]
    )
    environment = os.environ.copy()
    environment["KALSHI_EVENTS_API_URL"] = "http://127.0.0.1:9/trade-api/v2"
    environment["KALSHI_EVENTS_ALLOW_LOOPBACK"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    observed = json.loads(result.stdout)
    assert [value for value, _origin in observed] == ["a", "b"]
    assert Path(observed[0][1]).is_relative_to(sources[0] / "src")
    assert Path(observed[1][1]).is_relative_to(sources[1] / "src")


def test_structural_fixture_covers_pagination_combo_legs_and_native_decimals():
    fixture = benchmark.FixtureClient(1203)
    cursor = "0"
    pages = []
    markets = []
    while cursor:
        params = {"cursor": cursor} if cursor != "0" else {}
        _, payload = fixture.response("/markets", params)
        pages.append(payload)
        markets.extend(payload["markets"])
        cursor = payload["cursor"]

    combos = [row for row in markets if row["mve_selected_legs"]]
    assert len(pages) == 7
    assert len(markets) == 1203
    assert [row["ticker"] for row in combos] == ["MKT-001001"]
    assert combos[0]["mve_selected_legs"] == [
        {"event_ticker": "EVENT", "market_ticker": "MKT-000000", "side": "yes"}
    ]
    assert combos[0]["rules_primary"]
    assert combos[0]["floor_strike"] == "3.123456789123456789"
    assert combos[0]["last_price_dollars"] == "0.001"
    assert combos[0]["volume_fp"] == "12.3456789"


def test_combo_heavy_fixture_is_deterministic_and_separate_from_frozen_fixture():
    fixture = benchmark.ComboHeavyFixtureClient(1000)
    first = [fixture._market(index) for index in range(1000)]
    second = [fixture._market(index) for index in range(1000)]
    assert first == second
    assert sum(bool(row["mve_selected_legs"]) for row in first) == 950
    leg_counts = sorted(len(row["mve_selected_legs"]) for row in first if row["mve_selected_legs"])
    assert 200 in leg_counts
    assert {1, 2, 4, 8, 32, 200}.issubset(set(leg_counts))
    assert any(
        leg["event_ticker"].startswith("MISSING-EVENT")
        for row in first
        for leg in row["mve_selected_legs"]
    )
    assert any(
        leg["market_ticker"].startswith("MISSING-MARKET")
        for row in first
        for leg in row["mve_selected_legs"]
    )
    assert max(len(row["rules_primary"]) for row in first) > len(
        benchmark.FixtureClient(1000)._market(0)["rules_primary"]
    )
    _, history = fixture.response("/historical/markets", {})
    assert sum(bool(row["mve_selected_legs"]) for row in history["markets"]) == 190
    assert history["markets"][0]["settlement_value_dollars"] == "1.000000000000000001"


def test_combo_heavy_fixture_honors_refresh_scenarios_without_changing_frozen_hashes():
    baseline = benchmark.ComboHeavyFixtureClient(1000, "sparse-change", "baseline")
    no_change = benchmark.ComboHeavyFixtureClient(1000, "no-change", "refresh")
    sparse = benchmark.ComboHeavyFixtureClient(1000, "sparse-change", "refresh")
    all_change = benchmark.ComboHeavyFixtureClient(1000, "all-change", "refresh")

    assert baseline._market(0)["title"] == "Fixture contract 0"
    assert no_change.response("/events", {"min_updated_ts": "x"})[1]["events"] == []
    assert sparse.response("/events", {"min_updated_ts": "x"})[1]["events"]
    assert sparse._market(1)["title"] == "Updated combo contract"
    assert sparse._market(20)["title"] != "Updated combo contract"
    assert all_change._market(20)["title"] == "Updated contract"

    _, no_change_history = no_change.response("/historical/markets", {})
    _, sparse_history = sparse.response("/historical/markets", {})
    _, all_change_history = all_change.response("/historical/markets", {})

    def history_rows(fixture, first_page):
        rows = list(first_page["markets"])
        cursor = first_page["cursor"]
        while cursor:
            _, page = fixture.response("/historical/markets", {"cursor": cursor})
            rows.extend(page["markets"])
            cursor = page["cursor"]
        return rows

    no_change_rows = history_rows(no_change, no_change_history)
    sparse_rows = history_rows(sparse, sparse_history)
    all_change_rows = history_rows(all_change, all_change_history)
    expected_tickers = [f"HIST-{index:06d}" for index in range(1000)]
    assert [row["ticker"] for row in no_change_rows] == expected_tickers
    assert [row["ticker"] for row in sparse_rows] == expected_tickers
    assert [row["ticker"] for row in all_change_rows] == expected_tickers
    assert [row["title"] for row in no_change_rows] == [
        baseline._market(index)["title"] for index in range(1000)
    ]
    changed_history_indexes = {
        index
        for index, row in enumerate(sparse_rows)
        if row["title"] != baseline._market(index)["title"]
    }
    assert changed_history_indexes == {0, 1}
    rows = list(all_change_history["markets"])
    cursor = all_change_history["cursor"]
    while cursor:
        _, page = all_change.response("/historical/markets", {"cursor": cursor})
        rows.extend(page["markets"])
        cursor = page["cursor"]
    assert len(rows) == 1000
    assert (
        benchmark._fixture_sha256(197)
        == "b31fe8247035fe326c055c320fc14b6fe2e675fbc197057d9bb7595bdd4034d8"
    )
    assert benchmark._fixture_sha256(
        197, fixture_variant="combo-heavy"
    ) != benchmark._fixture_sha256(197)


def test_combo_heavy_fixture_hash_includes_later_historical_pages(monkeypatch):
    original = benchmark._fixture_sha256(401, "full", "combo-heavy")
    response = benchmark.ComboHeavyFixtureClient.response

    def changed_last_history_page(self, endpoint, params):
        status, payload = response(self, endpoint, params)
        if endpoint == "/historical/markets" and params.get("cursor") == "400":
            payload["markets"][0]["ticker"] += "-changed"
        return status, payload

    monkeypatch.setattr(benchmark.ComboHeavyFixtureClient, "response", changed_last_history_page)
    assert benchmark._fixture_sha256(401, "full", "combo-heavy") != original


def test_combo_heavy_profiles_do_not_legacy_freeze_new_acquisition_policy():
    class Capture:
        def _initialize_v2(self, _store, state):
            return state

    assert not benchmark._freeze_legacy_acquisition(
        "full", "full", Capture(), fixture_variant="combo-heavy"
    )
    assert benchmark._freeze_legacy_acquisition("full", "full", Capture())


@pytest.mark.parametrize(
    ("profile", "expected"),
    [
        ("full", 2003),
        ("no-change", 1952),
        ("sparse-change", 1954),
        ("all-change", 2003),
    ],
)
def test_combo_heavy_expected_observation_counts_include_live_and_history_tiers(profile, expected):
    assert benchmark._fixture_observation_count(1000, profile, "combo-heavy") == expected


@pytest.mark.parametrize("profile", ["full", "no-change", "sparse-change", "all-change"])
def test_combo_heavy_expected_observation_count_matches_fixture_pages(profile):
    markets = 20
    fixture = benchmark.ComboHeavyFixtureClient(markets, profile, "refresh")

    def count_pages(endpoint, params):
        count = 0
        cursor = params.get("cursor", "")
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            _, payload = fixture.response(endpoint, page_params)
            count += sum(len(payload.get(key, [])) for key in ("series", "events", "markets"))
            cursor = payload.get("cursor", "")
            if not cursor:
                return count

    observed = count_pages("/series", {})
    if profile == "full":
        observed += count_pages("/events", {})
        observed += count_pages("/markets", {})
    else:
        observed += count_pages("/events", {"min_updated_ts": "fixture-watermark"})
        observed += count_pages("/markets", {"mve_filter": "only"})
        observed += count_pages(
            "/markets", {"mve_filter": "exclude", "min_updated_ts": "fixture-watermark"}
        )
    observed += count_pages("/events/multivariate", {})
    observed += count_pages("/historical/markets", {})
    assert benchmark._fixture_observation_count(markets, profile, "combo-heavy") == observed


def test_combo_heavy_combo_summary_counts_live_and_historical_tiers():
    live_markets, live_legs = benchmark._fixture_combo_counts(20, "combo-heavy")
    summary = benchmark._fixture_combo_summary(20, "full", "combo-heavy")
    assert live_markets == 19
    assert summary == {
        "combo_markets": 2 * live_markets,
        "combo_legs_expected": 2 * live_legs,
        "live_combo_markets": live_markets,
        "live_combo_legs_expected": live_legs,
        "historical_combo_markets": live_markets,
        "historical_combo_legs_expected": live_legs,
    }
    sparse = benchmark._fixture_combo_summary(20, "sparse-change", "combo-heavy")
    assert sparse["combo_markets"] == 38
    assert sparse["combo_legs_expected"] == 2 * live_legs


@pytest.mark.parametrize(("markets", "pages"), [(197, 1), (1000, 5)])
def test_full_scope_pages_use_combined_live_and_historical_traversals(markets, pages):
    assert benchmark._fixture_full_scope_market_pages(markets, "combo-heavy") == (
        2 * pages,
        {"live_combined": pages, "historical": pages},
    )
    assert benchmark._fixture_full_scope_market_pages(markets, "frozen") == (
        pages,
        {"combined": pages},
    )


def test_combo_heavy_refresh_summary_reports_full_history_reconciliation():
    assert benchmark._fixture_refresh_history_count(1000, "no-change", "combo-heavy") == 1000
    assert benchmark._fixture_refresh_history_count(1000, "sparse-change", "combo-heavy") == 1000
    assert benchmark._fixture_historical_market_count(1000, "sparse-change", "combo-heavy") == 1000
    assert benchmark._fixture_refresh_history_count(1000, "full", "combo-heavy") == 0
    assert benchmark._fixture_historical_market_count(197, "targeted", "frozen") is None


def test_frozen_fixture_observation_count_remains_compatible():
    assert benchmark._fixture_observation_count(197, "full", "frozen") == 200
    assert benchmark._fixture_observation_count(197, "sparse-change", "frozen") == 201
    assert benchmark._fixture_historical_market_count(197, "full", "frozen") == 0
    assert benchmark._fixture_historical_market_count(197, "no-change", "frozen") == 1
    assert benchmark._fixture_historical_market_count(197, "all-change", "frozen") == 1


def test_frozen_v2_gate_requires_same_receipt_outputs_and_repeatable_identity():
    comparison = {
        "pair": 0,
        "same_capture_id": True,
        "semantic_relations_match": True,
        "parquet_schema_match": True,
        "artifact_bytes_match": True,
        "release_id_match": True,
        "logical_release_manifest_match": True,
        "warehouse_sha256_match": False,
        "lineage_match": True,
    }
    gate = benchmark._frozen_v2_gate([comparison], {"reference": True, "candidate": True})
    assert gate["passed"] is True
    comparison["artifact_bytes_match"] = False
    gate = benchmark._frozen_v2_gate([comparison], {"reference": True, "candidate": True})
    assert gate["passed"] is False


def test_logical_lineage_comparison_ignores_but_validates_physical_warehouse_hashes():
    fields = {
        "root_id": "a" * 32,
        "baseline_release_id": "release",
        "baseline_manifest_sha256": "b" * 64,
        "baseline_capture_id": "capture",
        "previous_watermark": "2026-01-01T00:00:00Z",
        "watermark_started_at": "2026-01-02T00:00:00Z",
        "delta_depth": 0,
        "membership_sha256": "c" * 64,
    }
    reference = {"lineage": {**fields, "warehouse_sha256": "d" * 64}, "lineage_valid": True}
    candidate = {"lineage": {**fields, "warehouse_sha256": "e" * 64}, "lineage_valid": True}
    assert benchmark._logical_lineage_match(reference, candidate)
    assert benchmark._logical_lineage_signature(reference) == benchmark._logical_lineage_signature(
        candidate
    )
    candidate["lineage"]["warehouse_sha256"] = "invalid"
    assert not benchmark._logical_lineage_match(reference, candidate)


def _dbt_profile_artifacts(sql: str) -> dict:
    return {
        "manifest.json": {
            "content": {
                "nodes": {
                    f"model.kalshi_events.{name}": {
                        "name": name,
                        "package_name": "kalshi_events",
                        "resource_type": "model",
                        "compiled_code": sql,
                    }
                    for name in benchmark.DUCKDB_PROFILE_MODEL_NAMES
                }
            }
        }
    }


def _write_duckdb_profile_build(build: Path, sql: str) -> None:
    import duckdb

    build.mkdir(parents=True)
    with duckdb.connect(str(build / "warehouse.duckdb")) as connection:
        connection.execute("CREATE TABLE source_rows AS SELECT * FROM range(8)")
    (build / "build-artifacts.json").write_text(json.dumps(_dbt_profile_artifacts(sql)))


def test_duckdb_profile_runs_compiled_hotspot_sql_on_a_copy(tmp_path: Path):
    duckdb = pytest.importorskip("duckdb")
    if not benchmark._duckdb_tracked_metrics_supported(duckdb.__version__):
        pytest.skip("DuckDB 2.x tracked_metrics is required")

    report_dir = tmp_path / "report"
    run_root = report_dir / "root-candidate-0"
    build = run_root / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE source_rows AS SELECT * FROM range(8)")
    sql = "SELECT count(*) AS row_count FROM main.source_rows"
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts(sql)))
    original_sha256 = hashlib.sha256(database.read_bytes()).hexdigest()

    profile = benchmark._run_duckdb_profile_json(
        SCRIPT.parents[1], report_dir, run_root, "capture-1", 1, "candidate", 0
    )

    assert profile["status"] == "profiled"
    assert profile["duckdb_version"] == duckdb.__version__
    assert set(profile["profile_files"]) == {
        f"{name}.json" for name in benchmark.DUCKDB_PROFILE_MODEL_NAMES
    }
    assert Path(profile["summary_file"]).is_relative_to(report_dir)
    assert profile["checkpoint_profile_file"] == "checkpoint.json"
    assert (Path(profile["summary_file"]).parent / profile["checkpoint_profile_file"]).is_file()
    profile_dir = Path(profile["summary_file"]).parent
    assert not (profile_dir / "warehouse.duckdb").exists()
    assert not (profile_dir / "warehouse.duckdb.wal").exists()
    assert not (profile_dir / "duckdb-temp").exists()
    assert not (profile_dir / "profiling-init.txt").exists()
    assert not (profile_dir / "tmp").exists()
    assert not (profile_dir / "dlt-data").exists()
    assert {path.name for path in profile_dir.iterdir()} == {
        "summary.json",
        "checkpoint.json",
        "current_markets.json",
        "combo_legs.json",
        "relationships.json",
        "stdout.log",
        "stderr.log",
    }
    assert hashlib.sha256(database.read_bytes()).hexdigest() == original_sha256
    summary = json.loads(Path(profile["summary_file"]).read_text())
    assert summary["execution_contract"].startswith("compiled dbt SELECT query-only replay")
    assert summary["database"]["source_file_bytes"] == database.stat().st_size
    assert (
        summary["database"]["profile_copy_file_bytes_before_checkpoint"] == database.stat().st_size
    )
    assert summary["database"]["database_open_seconds"] >= 0
    assert summary["database"]["checkpoint_wall_seconds"] >= 0
    assert summary["database"]["database_size"]["database_size"]
    assert summary["database"]["storage_version"].startswith("v2.")
    assert summary["database"]["query_connection_open_seconds"] >= 0
    assert "not build-time checkpoint cost" in summary["database"]["checkpoint_contract"]
    assert summary["limits"]["max_temp_directory_size_bytes"] == 4 * 1024**3
    assert summary["process_peak_rss_bytes"] is None or summary["process_peak_rss_bytes"] > 0
    for model in summary["models"]:
        output = Path(profile["summary_file"]).parent / model["profile_file"]
        query_profile = json.loads(output.read_text())
        assert query_profile["query"]["sql"] == sql
        metrics = model["metrics"]["values"]
        assert metrics["query.total_time"] is not None
        assert metrics["system.peak_buffer_memory"] is None or isinstance(
            metrics["system.peak_buffer_memory"], int
        )
        assert "system.peak_buffer_memory" in summary["tracked_metrics"]
        assert "system.peak_buffer_memory" in metrics


def test_duckdb_156_profile_report_marks_tracked_metrics_unavailable(tmp_path: Path, monkeypatch):
    class Cursor:
        def __init__(self, description, row):
            self.description = description
            self.row = row

        def fetchone(self):
            return self.row

    class Connection:
        @staticmethod
        def execute(sql):
            if sql == "PRAGMA database_size":
                return Cursor([("database_size",), ("memory_usage",)], ("1 MiB", "0 bytes"))
            if "duckdb_databases()" in sql:
                return Cursor([("storage_version",)], ("v1.5.6+",))
            return Cursor([], ())

        @staticmethod
        def close():
            pass

    fake_duckdb = SimpleNamespace(
        __version__="1.5.6", connect=lambda *_args, **_kwargs: Connection()
    )
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    report_dir = tmp_path / "report"
    build = report_dir / "root" / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    database.write_bytes(b"disposable")
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts("SELECT 1")))
    profile_dir = report_dir / "duckdb-profiles" / "reference-pair-0"

    summary = benchmark._duckdb_profile_worker(
        database, artifacts, report_dir, profile_dir, SCRIPT.parents[1]
    )

    assert summary["status"] == "unavailable"
    assert "DuckDB 1.5.6" in summary["reason"]
    assert summary["tracked_metrics"] == []
    assert summary["database"]["database_open_seconds"] >= 0
    assert summary["database"]["checkpoint_wall_seconds"] >= 0
    assert summary["database"]["storage_version"] == "v1.5.6+"
    assert summary["database"]["checkpoint_profile_metrics"]["storage.checkpoint_latency"] is None
    assert summary["database"]["checkpoint_profile_unavailable_metrics"]
    assert summary["models"] == []


def test_duckdb_profile_worker_rejects_non_report_inputs(tmp_path: Path):
    report_dir = tmp_path / "report"
    report_dir.mkdir()
    database = tmp_path / "outside.duckdb"
    artifacts = report_dir / "build-artifacts.json"
    with pytest.raises(ValueError, match="inside the external report directory"):
        benchmark._duckdb_profile_worker(
            database, artifacts, report_dir, report_dir / "profiles", SCRIPT.parents[1]
        )


def test_duckdb_profile_worker_validates_output_before_creating_it():
    repository = SCRIPT.parents[1]
    output_dir = repository / "tests" / "must-not-create-dbt-profile"
    with pytest.raises(ValueError, match="strict descendant"):
        benchmark._duckdb_profile_worker(SCRIPT, SCRIPT, repository, output_dir, repository)
    assert not output_dir.exists()


def test_duckdb_profile_worker_preserves_preexisting_initialization_marker(tmp_path: Path):
    report_dir = tmp_path / "report"
    output_dir = report_dir / "duckdb-profiles" / "test-pair-0"
    output_dir.mkdir(parents=True)
    marker = output_dir / "profiling-init.txt"
    marker.write_text("preserve me")

    with pytest.raises(ValueError, match="initialization output already exists"):
        benchmark._duckdb_profile_worker(
            report_dir / "warehouse.duckdb",
            report_dir / "build-artifacts.json",
            report_dir,
            output_dir,
            SCRIPT.parents[1],
        )

    assert marker.read_text() == "preserve me"


def test_duckdb_profile_worker_rejects_output_symlink_without_touching_target(tmp_path: Path):
    report_dir = tmp_path / "report"
    build = report_dir / "root" / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    database.write_bytes(b"disposable")
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts("SELECT 1")))
    profile_dir = report_dir / "duckdb-profiles" / "reference-pair-0"
    (profile_dir / "tmp").mkdir(parents=True)
    (profile_dir / "dlt-data").mkdir()
    target = tmp_path / "protected-summary.json"
    target.write_text("leave this alone")
    (profile_dir / "summary.json").symlink_to(target)

    with pytest.raises(ValueError, match="symlink is forbidden"):
        benchmark._duckdb_profile_worker(
            database, artifacts, report_dir, profile_dir, SCRIPT.parents[1]
        )

    assert target.read_text() == "leave this alone"
    assert (profile_dir / "summary.json").is_symlink()


def test_duckdb_profile_worker_rejects_output_symlink_ancestor_before_creation(tmp_path: Path):
    report_dir = tmp_path / "report"
    build = report_dir / "root" / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    database.write_bytes(b"disposable")
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts("SELECT 1")))
    target = report_dir / "actual-profiles"
    target.mkdir()
    linked = report_dir / "linked-profiles"
    linked.symlink_to(target, target_is_directory=True)
    output_dir = linked / "reference-pair-0"

    with pytest.raises(ValueError, match="symlink is forbidden"):
        benchmark._duckdb_profile_worker(
            database, artifacts, report_dir, output_dir, SCRIPT.parents[1]
        )

    assert not (target / output_dir.name).exists()
    assert linked.is_symlink()


def test_duckdb_profile_worker_preserves_preexisting_wal_marker(tmp_path: Path):
    report_dir = tmp_path / "report"
    build = report_dir / "root" / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    database.write_bytes(b"disposable")
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts("SELECT 1")))
    profile_dir = report_dir / "duckdb-profiles" / "reference-pair-0"
    profile_dir.mkdir(parents=True)
    wal = profile_dir / "warehouse.duckdb.wal"
    wal.write_bytes(b"preserve me")

    with pytest.raises(ValueError, match="profile WAL already exists"):
        benchmark._duckdb_profile_worker(
            database, artifacts, report_dir, profile_dir, SCRIPT.parents[1]
        )

    assert wal.read_bytes() == b"preserve me"


def test_duckdb_profile_worker_removes_copy_after_compiled_query_failure(tmp_path: Path):
    duckdb = pytest.importorskip("duckdb")
    if not benchmark._duckdb_tracked_metrics_supported(duckdb.__version__):
        pytest.skip("DuckDB 2.x tracked_metrics is required")
    report_dir = tmp_path / "report"
    build = report_dir / "root" / "builds" / "capture-1"
    build.mkdir(parents=True)
    database = build / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE source_rows AS SELECT * FROM range(8)")
    artifacts = build / "build-artifacts.json"
    artifacts.write_text(json.dumps(_dbt_profile_artifacts("SELECT * FROM missing_table")))
    profile_dir = report_dir / "duckdb-profiles" / "failure-pair-0"
    original_sha256 = hashlib.sha256(database.read_bytes()).hexdigest()

    with pytest.raises(duckdb.CatalogException):
        benchmark._duckdb_profile_worker(
            database, artifacts, report_dir, profile_dir, SCRIPT.parents[1]
        )

    assert hashlib.sha256(database.read_bytes()).hexdigest() == original_sha256
    assert not (profile_dir / "warehouse.duckdb").exists()
    assert not (profile_dir / "warehouse.duckdb.wal").exists()
    assert not (profile_dir / "duckdb-temp").exists()


@pytest.mark.parametrize(
    ("profile", "expected_build_dir"),
    [
        ("full", "builds"),
        ("resume", "builds"),
        ("sparse-change", "builds-v2"),
        ("all-change", "builds-v2"),
    ],
)
def test_benchmark_profile_uses_compiled_sql_from_its_build(
    tmp_path: Path, monkeypatch, profile: str, expected_build_dir: str
):
    duckdb = pytest.importorskip("duckdb")
    if not benchmark._duckdb_tracked_metrics_supported(duckdb.__version__):
        pytest.skip("DuckDB 2.x tracked_metrics is required")
    report_dir = tmp_path / "report"
    report_dir.mkdir()
    baseline_root = report_dir / "baseline"
    baseline_root.mkdir()
    worker = {}

    def worker_command(
        _source, root, result_path, _markets, profile, _scenario, refresh_mode, *_args
    ):
        worker.update(
            root=root, result_path=result_path, profile=profile, refresh_mode=refresh_mode
        )
        return ["synthetic-worker"]

    run_supervised_real = benchmark._run_supervised

    def run_supervised(command, **kwargs):
        if "--worker-duckdb-profile" in command:
            return run_supervised_real(command, **kwargs)
        build = worker["root"] / expected_build_dir / "capture-1"
        _write_duckdb_profile_build(build, "SELECT count(*) FROM main.source_rows")
        worker["result_path"].write_text(
            json.dumps(
                {
                    "capture_id": "capture-1",
                    "profile": worker["profile"],
                    "refresh_mode": worker["refresh_mode"],
                }
            )
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(benchmark, "_worker_command", worker_command)
    monkeypatch.setattr(benchmark, "_run_supervised", run_supervised)
    result = benchmark._run_one(
        SCRIPT.parents[1],
        report_dir,
        "candidate",
        0,
        1,
        profile,
        "candidate",
        (baseline_root, {"capture_id": "b" * 32}, 0.0),
        duckdb_profile_json=True,
    )

    assert worker["refresh_mode"] == benchmark._paired_refresh_mode(profile, "candidate")
    assert result["duckdb_profile_json"]["status"] == "profiled"
    assert not (report_dir / "root-candidate-0").exists()


@pytest.mark.parametrize(
    ("clock_values", "group_survives_kill", "expected_result"),
    [
        # The TERM wait deadline expires between its loop check and sleep.
        ([0.0, 0.1, 3.1], False, True),
        # The KILL wait deadline expires between its loop check and sleep.
        ([0.0, 3.1, 3.1, 3.2, 5.2], True, False),
    ],
)
def test_terminate_worker_group_clamps_deadline_crossing_sleep(
    monkeypatch, clock_values, group_survives_kill, expected_result
):
    clock = iter(clock_values)

    def monotonic():
        return next(clock, 100.0)

    sleeps = []
    signals = []
    group_alive = [True]

    class FakeProcess:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.returncode = 0
            return self.returncode

    process = FakeProcess()

    def sleep(duration):
        sleeps.append(duration)
        if duration < 0:
            raise ValueError("sleep length must be non-negative")

    def killpg(_pid, sig):
        signals.append(sig)
        if sig == 9 and not group_survives_kill:
            group_alive[0] = False

    monkeypatch.setattr(benchmark, "time", SimpleNamespace(monotonic=monotonic, sleep=sleep))
    monkeypatch.setattr(benchmark.os, "killpg", killpg)
    monkeypatch.setattr(benchmark, "_worker_group_alive", lambda _process: group_alive[0])

    assert benchmark._terminate_worker_group(process, reserve_seconds=5.0) is expected_result
    assert signals == [15, 9]
    assert sleeps and all(duration >= 0 for duration in sleeps)
    assert process.returncode == 0


def test_frozen_build_preserves_clone_when_worker_group_cannot_be_reaped(tmp_path, monkeypatch):
    clone = tmp_path / "frozen-v2-root-reference-pair-0"
    sentinel = clone / "worker-evidence"

    class FakeProcess:
        pid = 123

        def communicate(self, timeout):
            raise subprocess.TimeoutExpired("fake worker", timeout)

    def popen(*_args, **_kwargs):
        clone.mkdir()
        sentinel.write_text("still owned by worker")
        return FakeProcess()

    monkeypatch.setattr(benchmark.subprocess, "Popen", popen)
    monkeypatch.setattr(benchmark, "_terminate_worker_group", lambda *_args: False)
    with pytest.raises(benchmark._UnreapedWorkerGroup, match="descendants remain after timeout"):
        benchmark._run_frozen_v2_build(
            tmp_path / "source", tmp_path, "reference", "pair-0", tmp_path / "seed", "capture-id"
        )
    assert sentinel.read_text() == "still owned by worker"


@pytest.mark.parametrize(
    ("leader_exited", "expected_error"),
    [
        (False, "storage sample worker descendants remain alive"),
        (True, "storage sample worker exited with live descendants"),
    ],
)
def test_storage_sample_does_not_repeat_failed_reaping(
    tmp_path: Path, monkeypatch, leader_exited, expected_error
):
    root = tmp_path / "storage-root-reference"
    sentinel = root / "worker-evidence"
    clock = iter([0.0, 176.0])
    terminations = []

    class FakeProcess:
        returncode = 0 if leader_exited else None

        def poll(self):
            return self.returncode

        def wait(self):
            return self.returncode

    process = FakeProcess()

    def popen(*_args, **_kwargs):
        root.mkdir()
        sentinel.write_text("still owned by worker")
        return process

    def terminate(worker):
        terminations.append(worker)
        return False

    monkeypatch.setattr(benchmark.subprocess, "Popen", popen)
    monkeypatch.setattr(benchmark, "_terminate_worker_group", terminate)
    monkeypatch.setattr(benchmark, "_worker_group_alive", lambda _process: True)
    monkeypatch.setattr(benchmark, "time", SimpleNamespace(monotonic=lambda: next(clock, 176.0)))
    with pytest.raises(benchmark._UnreapedWorkerGroup) as error:
        benchmark._sampled_storage_run(tmp_path / "source", tmp_path, "reference", 1, "full")
    assert terminations == [process]
    assert str(error.value) == expected_error
    assert sentinel.read_text() == "still owned by worker"


@pytest.mark.skipif(
    os.name != "posix", reason="benchmark workers use POSIX process groups on macOS"
)
def test_supervisor_terminates_descendants_before_returning(tmp_path: Path, monkeypatch):
    processes = []
    original_popen = benchmark.subprocess.Popen

    def tracked_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(benchmark.subprocess, "Popen", tracked_popen)
    command = [
        sys.executable,
        "-c",
        "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])",
    ]
    with pytest.raises(RuntimeError, match="descendants were still running"):
        benchmark._run_supervised(command, timeout=8)
    assert len(processes) == 1
    assert processes[0].poll() is not None
    assert not benchmark._worker_group_alive(processes[0])


@pytest.mark.skipif(os.name != "posix", reason="benchmark uses POSIX signal and process groups")
def test_sigterm_during_supervision_reaps_worker_group_before_exit(tmp_path: Path):
    worker_pid_file = tmp_path / "worker.pid"
    cleanup_marker = tmp_path / "caller-cleanup-ran"
    descendant_code = (
        "import os,sys,time; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(60)"
    )
    child_code = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {descendant_code!r}, sys.argv[1]]); "
        "time.sleep(60)"
    )
    parent_code = """
import importlib.util, sys
spec = importlib.util.spec_from_file_location('benchmark_under_test', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module._run_supervised([sys.executable, '-c', sys.argv[4], sys.argv[2]], timeout=60)
open(sys.argv[3], 'w').write('unexpected')
"""
    parent = subprocess.Popen(
        [
            sys.executable,
            "-c",
            parent_code,
            str(SCRIPT),
            str(worker_pid_file),
            str(cleanup_marker),
            child_code,
        ],
        start_new_session=True,
    )
    deadline = time.monotonic() + 10
    while not worker_pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert worker_pid_file.exists(), "supervised worker did not start"
    worker_pid = int(worker_pid_file.read_text())
    os.kill(parent.pid, signal.SIGTERM)
    assert parent.wait(timeout=10) == 128 + signal.SIGTERM
    assert not cleanup_marker.exists(), "caller cleanup must not run after termination"
    for _ in range(100):
        try:
            os.kill(worker_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.02)
    else:
        pytest.fail("supervised worker remained alive after benchmark parent SIGTERM")


@pytest.mark.skipif(os.name != "posix", reason="benchmark SIGTERM handler is POSIX-specific")
def test_supervisor_restores_prior_sigterm_handler_after_normal_exit():
    previous = signal.getsignal(signal.SIGTERM)
    assert benchmark._run_supervised([sys.executable, "-c", "pass"], timeout=6).returncode == 0
    assert signal.getsignal(signal.SIGTERM) is previous


def test_changed_acquisition_gate_uses_native_relations_not_receipt_identity():
    pair = {"pair": 0, "native_values_and_membership_match": True, "capture_id_match": False}
    assert benchmark._native_acquisition_gate([pair], {"reference": True})["passed"] is True
    pair["native_values_and_membership_match"] = False
    assert benchmark._native_acquisition_gate([pair], {"reference": True})["passed"] is False


def test_root_inventory_detects_content_changes_not_only_metadata(tmp_path: Path):
    root = tmp_path / "capture"
    root.mkdir()
    evidence = root / "receipt.bin"
    evidence.write_bytes(b"first")
    before = benchmark._root_inventory(root)
    evidence.write_bytes(b"other")
    after = benchmark._root_inventory(root)
    assert before != after


def test_benchmark_child_timeout_is_bounded_to_three_minutes():
    assert benchmark.BENCHMARK_CHILD_TIMEOUT_SECONDS == 180


def test_frozen_benchmark_worker_commands_pass_same_capture_root_and_id(tmp_path: Path):
    source = tmp_path / "candidate"
    frozen = tmp_path / "seed"
    clone = tmp_path / "clone"
    result = tmp_path / "result.json"
    command = benchmark._frozen_build_worker_command(source, frozen, clone, "capture-1", result)
    assert command[command.index("--frozen-root") + 1] == str(frozen)
    assert command[command.index("--root") + 1] == str(clone)
    assert command[command.index("--capture-id") + 1] == "capture-1"

    capture_command = benchmark._worker_command(
        source,
        clone,
        result,
        1000,
        "full",
        fixture_variant="combo-heavy",
        capture_only=True,
    )
    assert "--capture-only" in capture_command
    assert capture_command[capture_command.index("--fixture") + 1] == "combo-heavy"


def test_frozen_worker_rejects_output_paths_before_copying(tmp_path: Path, monkeypatch):
    import shutil

    source = benchmark._script_checkout()
    frozen = tmp_path / "frozen"
    frozen.mkdir()
    output = source / ".benchmark-output-must-not-be-created"
    copied = False

    def unexpected_copy(*_args, **_kwargs):
        nonlocal copied
        copied = True
        raise AssertionError("copy must not run before path validation")

    monkeypatch.setattr(shutil, "copytree", unexpected_copy)
    with pytest.raises(ValueError, match="strict descendant"):
        benchmark._frozen_v2_build_worker(
            source, frozen, output, "capture-id", tmp_path / "result.json"
        )
    assert not copied
    assert not output.exists()


@pytest.mark.parametrize("count", [1, 100, 10_000, 100_000])
def test_parent_hydration_fixture_scales_with_distinct_stable_identities(count: int):
    parents = benchmark.parent_hydration_fixture(count)
    assert len(parents) == count
    assert len(set(parents)) == count
    assert parents[0] == "EVENT-000000"
    assert parents[-1] == f"EVENT-{count - 1:06d}"


@pytest.mark.parametrize("bad_count", [0, -1, True, 1.5])
def test_parent_hydration_fixture_rejects_invalid_counts(bad_count):
    with pytest.raises(ValueError, match="positive integer"):
        benchmark.parent_hydration_fixture(bad_count)


def test_selected_stress_fixture_covers_lookup_edges_without_changing_frozen_full_fixture():
    tickers = benchmark.selected_stress_tickers()
    fixture = benchmark.SelectedStressFixtureClient()
    assert len(tickers) == len(set(tickers)) == 100
    assert len(tickers[-1]) == 256
    assert (
        benchmark._selected_stress_fixture_sha256() == benchmark._selected_stress_fixture_sha256()
    )

    assert fixture.response("/markets/S000", {})[0] == 200
    assert fixture.response("/markets/HIST", {})[0] == 404
    assert fixture.response("/historical/markets/HIST", {})[0] == 200
    assert fixture.response("/markets/MISS", {})[0] == 404
    assert fixture.response("/historical/markets/MISS", {})[0] == 404
    assert fixture.response("/markets/ORPHAN", {})[0] == 200
    assert fixture.response("/events/ORPHAN-EVENT", {})[0] == 404
    event = fixture.response("/events/EVENT", {})[1]["event"]
    assert len(event["markets"]) == 2000
    market = fixture.response("/markets/S000", {})[1]["market"]
    assert market["close_time"] is None
    assert market["updated_time"].endswith("+01:00")


def test_selected_single_and_fallback_profiles_have_stable_fixture_identities():
    single = benchmark._selected_profile_tickers("selected-single")
    fallback = benchmark._selected_profile_tickers("selected-fallback")
    assert single == ["S000"]
    assert fallback == ["HIST", "MISS"]
    assert benchmark._selected_stress_fixture_sha256(
        single
    ) == benchmark._selected_stress_fixture_sha256(single)
    assert benchmark._selected_stress_fixture_sha256(
        fallback
    ) == benchmark._selected_stress_fixture_sha256(fallback)
    assert benchmark._selected_stress_fixture_sha256(
        single
    ) != benchmark._selected_stress_fixture_sha256(fallback)

    fixture = benchmark.SelectedStressFixtureClient()
    for endpoint in (
        "/markets/HIST",
        "/historical/markets/HIST",
        "/markets/MISS",
        "/historical/markets/MISS",
    ):
        fixture.response(endpoint, {})
    assert fixture.fallback_request_count == 4


def test_selected_fixture_page_uuid_is_request_canonical_and_cutoff_stays_context_bound():
    params = {"tickers": "HIST,MISS", "limit": 1000}
    first = benchmark._benchmark_page_uuid(
        "selected-stress", "/markets", params, "capture-a", "task-request-a"
    )
    reordered_task = benchmark._benchmark_page_uuid(
        "selected-stress", "/markets", params, "capture-a", "task-request-b"
    )
    assert first == reordered_task
    assert first != benchmark._benchmark_page_uuid(
        "selected-stress", "/markets", params, "capture-b", "task-request-a"
    )
    assert (
        benchmark._benchmark_page_uuid(
            "selected-stress", "/historical/cutoff", {}, "capture-a", "cutoff-context"
        )
        == "cutoff-context"
    )
    assert (
        benchmark._benchmark_page_uuid(
            "targeted", "/markets/MKT-000000", {}, "capture-a", "legacy-context"
        )
        == "legacy-context"
    )


def test_composed_refresh_fixture_uses_context_uuid32_for_every_receipt():
    for endpoint, params in (
        ("/historical/cutoff", {}),
        ("/markets", {"mve_filter": "exclude", "min_updated_ts": "1"}),
    ):
        first = benchmark._benchmark_page_uuid("no-change", endpoint, params, "capture-a", "a" * 64)
        distinct_request = benchmark._benchmark_page_uuid(
            "no-change", endpoint, params, "capture-a", "b" * 64
        )
        assert first != distinct_request
        assert len(first) == 32
        assert all(char in "0123456789abcdef" for char in first)
    assert (
        benchmark._benchmark_page_uuid(
            "full", "/markets", {}, "capture-a", "original-frozen-context"
        )
        == "original-frozen-context"
    )
    assert benchmark._receipt_uuid_format("no-change") == "context-request-id-32-hex"
    assert benchmark._receipt_uuid_format("full") == "context-request-id-64-hex"


def test_selected_stress_batch_and_parent_filter_are_exact_and_paginated():
    fixture = benchmark.SelectedStressFixtureClient()
    selected = benchmark.selected_stress_tickers()
    status, first = fixture.response("/markets", {"tickers": ",".join(selected), "limit": "1000"})
    assert status == 200
    assert first["cursor"] == ""
    assert [row["ticker"] for row in first["markets"]] == [
        *[f"S{i:03d}" for i in range(96)],
        "ORPHAN",
        selected[-1],
    ]
    assert len(fixture.response("/events/EVENT", {})[1]["event"]["markets"]) == 2000
    filtered = fixture.response("/events/EVENT", {"exclude_markets": "true"})[1]["event"]
    assert "markets" not in filtered


def test_selected_native_semantics_ignore_only_capture_provenance(tmp_path: Path):
    rows = {
        "markets": [
            {
                "market_ticker": "MKT-1",
                "title": "Native title",
                "payload_json": '{"ticker":"MKT-1","title":"Native title"}',
                "capture_id": "a",
                "page_id": "b",
                "received_at": "time-a",
                "source_endpoint": "/markets/MKT-1",
                "payload_sha256": "c",
                "source_kind": "direct",
                "observation_id": "d",
            }
        ],
        "events": [],
        "series": [],
    }
    for relation, values in rows.items():
        (tmp_path / f"{relation}.json").write_text(json.dumps(values))
    reference = benchmark._selected_relation_semantics(tmp_path)
    rows["markets"][0].update(capture_id="new-capture", page_id="new-page", received_at="time-b")
    (tmp_path / "markets.json").write_text(json.dumps(rows["markets"]))
    candidate = benchmark._selected_relation_semantics(tmp_path)
    assert reference == candidate

    rows["markets"][0]["title"] = "Tampered native title"
    rows["markets"][0]["payload_json"] = '{"ticker":"MKT-1","title":"Tampered native title"}'
    (tmp_path / "markets.json").write_text(json.dumps(rows["markets"]))
    assert benchmark._selected_relation_semantics(tmp_path) != reference


def test_selected_event_semantics_ignore_only_nested_child_markets(tmp_path: Path):
    native = {
        "event_ticker": "EVENT",
        "series_ticker": "SERIES",
        "title": "Native event title",
        "markets": [{"ticker": "NOISE-1"}],
    }
    row = {
        **native,
        "payload_json": json.dumps(native),
        "capture_id": "capture-a",
        "page_id": "page-a",
        "received_at": "2026-01-01T00:00:00Z",
        "source_endpoint": "/events/EVENT",
        "payload_sha256": "a" * 64,
        "source_kind": "direct",
        "observation_id": "b" * 64,
    }
    for relation in ("markets", "series"):
        (tmp_path / f"{relation}.json").write_text("[]")
    (tmp_path / "events.json").write_text(json.dumps([row]))
    reference = benchmark._selected_relation_semantics(tmp_path)

    native["markets"] = [{"ticker": "NOISE-2"}, {"ticker": "NOISE-3"}]
    row["markets"] = native["markets"]
    row["payload_json"] = json.dumps(native)
    row["capture_id"] = "capture-b"
    row["page_id"] = "page-b"
    row["received_at"] = "2026-01-02T00:00:00Z"
    row["source_endpoint"] = "/events/EVENT?exclude_markets=true"
    row["payload_sha256"] = "c" * 64
    row["observation_id"] = "d" * 64
    (tmp_path / "events.json").write_text(json.dumps([row]))
    assert benchmark._selected_relation_semantics(tmp_path) == reference

    native["title"] = "Tampered event title"
    row["title"] = native["title"]
    row["payload_json"] = json.dumps(native)
    (tmp_path / "events.json").write_text(json.dumps([row]))
    assert benchmark._selected_relation_semantics(tmp_path) != reference


def test_selected_comparison_rejects_native_coverage_or_provenance_mismatch():
    reference = {
        "selected_tickers": ["MKT-1"],
        "selected_semantics": {
            relation: {"rows": 0, "sha256": f"{index:x}" * 64}
            for index, relation in enumerate(("markets", "events", "series"), start=1)
        },
        "selected_coverage": [{"market_ticker": "MKT-1", "status": "found"}],
        "selected_provenance_valid": True,
    }
    candidate = json.loads(json.dumps(reference))
    assert all(benchmark._selected_comparison(reference, candidate).values())
    for key, value in (
        ("selected_semantics", {"markets": {"sha256": "tampered"}}),
        ("selected_coverage", [{"market_ticker": "MKT-1", "status": "absent"}]),
        ("selected_provenance_valid", False),
        ("selected_tickers", None),
        ("selected_semantics", None),
    ):
        modified = {**candidate, key: value}
        assert not all(benchmark._selected_comparison(reference, modified).values())


def test_selected_comparison_fails_closed_when_both_evidence_payloads_are_missing():
    incomplete = {"selected_provenance_valid": True}
    assert not all(benchmark._selected_comparison(incomplete, incomplete).values())


def test_selected_raw_receipt_provenance_tampering_fails(tmp_path: Path, monkeypatch):
    from kalshi_events import capture, normalize

    manifest = {
        "capture_id": "a" * 32,
        "page_id": "b" * 32,
        "received_at": "2026-01-01T00:00:00Z",
        "endpoint": "/markets/A",
        "params": {},
        "sha256": "c" * 64,
        "entity_kind": "markets",
        "singular": True,
        "status": 200,
    }
    payload = {
        "market": {
            "ticker": "A",
            "event_ticker": "E",
            "status": "active",
            "title": "Native",
        }
    }
    monkeypatch.setattr(capture, "capture_pages", lambda _store, _state: [(manifest, payload)])
    row = next(normalize.observations([(manifest, payload)]))
    row["entity_kind"] = "markets"
    for relation, rows in (
        ("markets", [row]),
        ("events", []),
        ("series", []),
        ("coverage", [{"market_ticker": "A", "status": "found"}]),
    ):
        (tmp_path / f"{relation}.json").write_text(json.dumps(rows))
    state = {"selected": ["A"]}
    assert benchmark._selected_provenance_is_valid(object(), state, tmp_path, ["A"])

    tampered = dict(row, payload_sha256="d" * 64)
    (tmp_path / "markets.json").write_text(json.dumps([tampered]))
    assert not benchmark._selected_provenance_is_valid(object(), state, tmp_path, ["A"])


def test_history_stress_fixture_is_broad_and_refreshes_about_one_percent():
    fixture = benchmark.FixtureClient(
        benchmark.HISTORY_STRESS_IDENTITIES, "history-sparse", "baseline"
    )
    history = benchmark.FixtureClient._historical_market(249_999)
    assert benchmark.HISTORY_STRESS_COUNT == 250_000
    assert benchmark.HISTORY_STRESS_IDENTITIES == 10_000
    assert benchmark.HISTORY_STRESS_PASSES == 25
    assert history["ticker"] == "HIST-249999"
    assert history["settlement_ts"].endswith("+01:00")

    status, first = fixture.response("/markets", {"limit": "1000"})
    _, repeated = fixture.response(
        "/markets", {"limit": "1000", "cursor": str(benchmark.HISTORY_STRESS_IDENTITIES)}
    )
    assert status == 200
    assert len(first["markets"]) == len(repeated["markets"]) == benchmark.PAGE_SIZE
    assert first["markets"][0] == repeated["markets"][0]
    assert len(fixture.response("/historical/markets", {})[1]["markets"]) == 1

    refresh = benchmark.FixtureClient(1000, "history-sparse", "refresh")
    assert len(refresh.response("/historical/markets", {})[1]["markets"]) == 1

    selected = refresh.response("/markets", {"mve_filter": "exclude", "min_updated_ts": "1"})[1][
        "markets"
    ]
    assert len(selected) == 10
    assert [row["ticker"] for row in selected[:3]] == [
        "MKT-000000",
        "MKT-000100",
        "MKT-000200",
    ]


def test_existing_byte_identity_profiles_freeze_legacy_acquisition_protocol():
    class Capture:
        _CHECKPOINT_TASK_FIELDS = {
            "cutoff_ordinal",
            "cutoff_reason",
            "pages_since_cutoff",
            "last_cutoff_at",
            "data_done",
        }

        def _initialize_v2(self, _store, state):
            return state

    capture = Capture()
    assert benchmark._freeze_legacy_acquisition("full", "full", capture)
    state = {
        "acquisition_protocol": 5,
        "rate_policy": "fixed",
        "parent_policy": "singular-v1",
        "cutoff_policy": "per-page-v1",
        "acquisition_policy_sha256": "a" * 64,
        "tasks": [{"endpoint": "/historical/markets", "cutoff_ordinal": 1}],
    }
    assert capture._initialize_v2(None, state)["tasks"] == [{"endpoint": "/historical/markets"}]

    current = Capture()
    assert not benchmark._freeze_legacy_acquisition("selected-stress", "full", current)
    assert benchmark._freeze_legacy_acquisition("history-sparse", "history-sparse", current)
    assert benchmark._freeze_legacy_acquisition("baseline-v2-setup", "full", Capture())
    baseline = Capture()
    assert benchmark._freeze_legacy_acquisition("baseline-setup", "history-sparse", baseline)


def test_legacy_freeze_supports_reference_capture_without_protocol5_fields():
    class ReferenceCapture:
        def _initialize_v2(self, _store, state):
            return state

    capture = ReferenceCapture()
    assert benchmark._freeze_legacy_acquisition("full", "full", capture)
    state = {
        "acquisition_protocol": 2,
        "compression": "gzip",
        "tasks": [{"endpoint": "/historical/markets", "params": {}}],
    }

    frozen = capture._initialize_v2(None, state)

    assert frozen["tasks"] == [{"endpoint": "/historical/markets", "params": {}}]
    assert frozen["compression"] == "identity"
    assert "acquisition_protocol" not in frozen


def test_legacy_targeted_freeze_restores_original_singular_task_plan():
    class Capture:
        _CHECKPOINT_TASK_FIELDS = {
            "cutoff_ordinal",
            "cutoff_reason",
            "pages_since_cutoff",
            "last_cutoff_at",
            "data_done",
        }

        @staticmethod
        def task(endpoint, kind, *, singular=False, requested=None):
            return {
                "endpoint": endpoint,
                "kind": kind,
                "singular": singular,
                "requested": requested,
                "params": {},
            }

        @staticmethod
        def ticker(value):
            return value

        @staticmethod
        def _initialize_v2(_store, state):
            return state

    capture = Capture()
    assert benchmark._freeze_legacy_acquisition("targeted", "full", capture)
    state = {
        "acquisition_protocol": 3,
        "selected": ["A", "B"],
        "task_scopes": {"1": "selected"},
        "tasks": [
            Capture.task("/historical/cutoff", None),
            {
                "endpoint": "/markets",
                "kind": "markets",
                "singular": False,
                "requested": None,
                "params": {"tickers": "A,B", "limit": 1000},
            },
        ],
    }
    initialized = capture._initialize_v2(None, state)
    assert "acquisition_protocol" not in initialized
    assert initialized["task_scopes"] == {}
    assert [(task["endpoint"], task["requested"]) for task in initialized["tasks"]] == [
        ("/historical/cutoff", None),
        ("/markets/A", "A"),
        ("/markets/B", "B"),
    ]


def test_incremental_fixture_honors_update_and_reconciliation_filters():
    no_change = benchmark.FixtureClient(1203, "no-change", "refresh")
    sparse = benchmark.FixtureClient(1203, "sparse-change", "refresh")
    all_change = benchmark.FixtureClient(1203, "all-change", "refresh")

    assert no_change.response("/events", {"min_updated_ts": "1"})[1]["events"] == []
    assert (
        sparse.response("/events", {"min_updated_ts": "1"})[1]["events"][0]["title"]
        == "Sparse update"
    )
    sparse_markets = sparse.response("/markets", {"mve_filter": "exclude", "min_updated_ts": "1"})[
        1
    ]["markets"]
    assert [market["ticker"] for market in sparse_markets] == ["MKT-000000"]
    combo_markets = all_change.response("/markets", {"mve_filter": "only"})[1]["markets"]
    assert [market["ticker"] for market in combo_markets] == ["MKT-001001"]
    assert len(all_change.response("/historical/markets", {})[1]["markets"]) == 1


@pytest.mark.parametrize("incremental", [False, True])
def test_paired_correctness_gate_fails_on_mismatches(incremental: bool):
    pair = {
        "pair": 0,
        "semantics_match": False,
        "artifact_bytes_match": False,
        "release_id_match": True,
        "release_manifest_match": True,
        "publication_pointer_match": True,
    }
    gate = benchmark._correctness_gate(
        [pair], incremental=incremental, identity_stability={"reference": True}
    )
    assert gate["passed"] is False
    assert gate["failed_pairs"] == [0]


def test_incremental_identity_stability_allows_physical_warehouse_hint_variance():
    lineage = {
        "root_id": benchmark.FIXED_ROOT_ID,
        "baseline_release_id": "a" * 64,
        "baseline_manifest_sha256": "b" * 64,
        "baseline_capture_id": "capture-1",
        "previous_watermark": "2026-01-01T00:00:00Z",
        "watermark_started_at": "2026-01-02T00:00:00Z",
        "delta_depth": 1,
        "membership_sha256": "c" * 64,
    }
    runs = [
        {
            "release_id": "d" * 64,
            "artifact_sha256": {"events.parquet": "e" * 64},
            "release_manifest_sha256": str(index),
            "publication_pointer_sha256": str(index),
            "lineage": {**lineage, "warehouse_sha256": f"{index:x}" * 64},
            "lineage_valid": True,
        }
        for index in (1, 2, 3)
    ]
    assert benchmark._identity_stability({"compare": runs}, incremental=True) == {"compare": True}
    runs[1]["lineage"]["root_id"] = "f" * 32
    assert benchmark._identity_stability({"compare": runs}, incremental=True) == {"compare": False}


def test_v2_identity_stability_ignores_only_physical_hints_and_requires_public_outputs():
    lineage = {
        "root_id": benchmark.FIXED_ROOT_ID,
        "baseline_release_id": None,
        "baseline_manifest_sha256": None,
        "baseline_capture_id": None,
        "previous_watermark": None,
        "watermark_started_at": "2026-01-01T00:00:00Z",
        "delta_depth": 0,
        "membership_sha256": "c" * 64,
    }
    runs = [
        {
            "release_id": "d" * 64,
            "artifact_sha256": {"markets.parquet": "e" * 64},
            "release_manifest_sha256": f"manifest-{index}",
            "publication_pointer_sha256": f"pointer-{index}",
            "lineage": {**lineage, "warehouse_sha256": f"{index:x}" * 64},
            "lineage_valid": True,
        }
        for index in (1, 2, 3)
    ]
    assert benchmark._identity_stability(
        {"source": runs}, incremental=False, v2_physical_hint=True
    ) == {"source": True}
    runs[1]["artifact_sha256"]["markets.parquet"] = "f" * 64
    assert benchmark._identity_stability(
        {"source": runs}, incremental=False, v2_physical_hint=True
    ) == {"source": False}


def test_v2_gate_requires_public_parity_and_logical_lineage():
    pair = {
        "pair": 0,
        "artifact_bytes_match": True,
        "release_id_match": True,
        "lineage_match": False,
    }
    gate = benchmark._correctness_gate(
        [pair],
        incremental=False,
        identity_stability={"source": True},
        v2_output=True,
    )
    assert gate["passed"] is False
    assert gate["failed_pairs"] == [0]

    assert not benchmark._logical_lineage_match(
        {"lineage": {}}, {"lineage": {}, "lineage_valid": True}
    )


def test_selected_correctness_gate_requires_nonempty_selection_identity():
    pair = {
        "pair": 0,
        "selection_match": False,
        "semantics_match": True,
        "coverage_match": True,
        "provenance_match": True,
    }
    gate = benchmark._correctness_gate(
        [pair],
        incremental=False,
        identity_stability={"source": True},
        selected_stress=True,
    )
    assert gate["passed"] is False
    assert gate["failed_pairs"] == [0]


def test_history_sparse_refresh_is_incremental_for_both_roles():
    assert benchmark._paired_refresh_mode("history-sparse", "reference") == "incremental"
    assert benchmark._paired_refresh_mode("history-sparse", "candidate") == "incremental"
    assert benchmark._paired_refresh_mode("sparse-change", "reference") == "full"
    assert benchmark._paired_refresh_mode("sparse-change", "candidate") == "incremental"


def test_incremental_root_seed_uses_writer_guard_and_fixed_identity():
    class DummyStore:
        def __init__(self):
            self.locked = False
            self.values = {}

        @contextmanager
        def writer_guard(self):
            self.locked = True
            try:
                yield
            finally:
                self.locked = False

        def write(self, path, value):
            assert self.locked
            self.values[path] = value

    store = DummyStore()
    benchmark._seed_incremental_root(store)
    assert store.values == {"root-id.json": {"version": 1, "root_id": benchmark.FIXED_ROOT_ID}}
    assert store.locked is False


@pytest.mark.parametrize(
    ("profile", "refresh_mode", "expected"),
    [
        ("full", "incremental", "current.json"),
        ("targeted", "incremental", "current.json"),
        ("baseline-setup", "incremental", "current-v2.json"),
        ("baseline-v2-setup", "full", "current-v2.json"),
        ("baseline-setup", "full", "current.json"),
        ("resume", "incremental", "current.json"),
        ("no-change", "incremental", "current-v2.json"),
        ("sparse-change", "full", "current.json"),
    ],
)
def test_publication_pointer_follows_published_contract(profile, refresh_mode, expected):
    assert benchmark._publication_pointer_name(profile, refresh_mode) == expected


def test_baseline_v2_setup_calls_only_direct_v2_build():
    calls = []

    class Warehouse:
        @staticmethod
        def build_v2_full(store, capture_id):
            calls.append((store, capture_id))
            return "v2-build"

    assert benchmark._direct_v2_setup_build(Warehouse, "store", "capture") == "v2-build"
    assert calls == [("store", "capture")]


def test_publication_pointer_observer_checks_release_and_manifest(tmp_path: Path):
    pointer = tmp_path / "current.json"
    pointer.write_text(json.dumps({"source_release_id": "a" * 64, "manifest_sha256": "b" * 64}))
    assert (
        benchmark._publication_pointer_sha256(tmp_path, "current.json", "a" * 64, "b" * 64)
        == hashlib.sha256(pointer.read_bytes()).hexdigest()
    )
    with pytest.raises(AssertionError, match="another release"):
        benchmark._publication_pointer_sha256(tmp_path, "current.json", "c" * 64, "b" * 64)


def test_legacy_observer_counts_imported_normalize_digest_alias():
    class Metrics:
        def __init__(self):
            self.counters = {}

        def add(self, name, value=1):
            self.counters[name] = self.counters.get(name, 0) + value

        @contextmanager
        def phase(self, _name):
            yield

        def peak(self, *_args):
            pass

    def unused(*_args, **_kwargs):
        pass

    class Store:
        _sync_prefix = unused
        _record = unused
        write_bytes = unused
        write = unused
        append = unused

    def digest(body):
        return hashlib.sha256(body).hexdigest()

    common = SimpleNamespace(
        digest=digest,
        Store=Store,
        atomic_bytes=unused,
    )
    normalize = SimpleNamespace(digest=digest)
    metrics = Metrics()

    benchmark._instrument_baseline(metrics, [common, normalize])
    body = b"row-observation"
    assert normalize.digest(body) == hashlib.sha256(body).hexdigest()
    assert metrics.counters["hashes"] == 1
    assert metrics.counters["hash_bytes"] == len(body)


def test_dlt_trace_observer_reports_only_extract_normalize_and_load_phases():
    start = datetime(2026, 1, 1)

    class Pipeline:
        def __init__(self):
            self.last_trace = SimpleNamespace(
                steps=[
                    SimpleNamespace(
                        step=name,
                        started_at=start,
                        finished_at=start + timedelta(seconds=seconds),
                    )
                    for name, seconds in (
                        ("extract", 1.25),
                        ("normalize", 2.5),
                        ("load", 0.75),
                        ("run", 4.5),
                    )
                ]
            )
            self.calls = 0

        def run(self, data, **kwargs):
            self.calls += 1
            return data, kwargs

    pipeline = Pipeline()
    warehouse = SimpleNamespace(dlt=SimpleNamespace(pipeline=lambda **_kwargs: pipeline))
    timings = {}
    benchmark._instrument_dlt_trace(warehouse, timings)
    result = warehouse.dlt.pipeline().run("data", loader_file_format="parquet")

    assert result == ("data", {"loader_file_format": "parquet"})
    assert pipeline.calls == 1
    assert timings == {"extract": 1.25, "normalize": 2.5, "load": 0.75}


def test_warehouse_json_proxy_counts_two_direct_artifact_parses_locally():
    class Metrics:
        def __init__(self):
            self.counters = {}
            self.phases = {}

        def add(self, name, value=1):
            self.counters[name] = self.counters.get(name, 0) + value

        @contextmanager
        def phase(self, name):
            self.phases[name] = self.phases.get(name, 0) + 1
            yield

    original_loads = json.loads
    metrics = Metrics()
    warehouse = SimpleNamespace(json=json)
    benchmark._instrument_warehouse_json_loads(warehouse, metrics)

    assert warehouse.json.loads(b'{"nodes": {}}') == {"nodes": {}}
    assert warehouse.json.loads(b'{"results": []}') == {"results": []}
    assert metrics.counters["json_decodes"] == 2
    assert metrics.counters["first_party_artifact_json_decodes"] == 2
    assert metrics.counters["json_decoded_bytes"] == len(b'{"nodes": {}}') + len(b'{"results": []}')
    assert (
        metrics.counters["first_party_artifact_json_decoded_bytes"]
        == metrics.counters["json_decoded_bytes"]
    )
    assert metrics.phases["json_parsing"] == 2
    assert json.loads is original_loads


def test_worker_destinations_are_confined_to_checkout_data_and_avoid_nested_checkouts(
    tmp_path: Path,
):
    source = tmp_path / "source"
    source.mkdir()
    benchmark._validate_worker_destinations(tmp_path / "data", tmp_path / "result.json", source)
    with pytest.raises(ValueError, match="overlaps a source checkout"):
        benchmark._validate_worker_destinations(source / "data", tmp_path / "result.json", source)
    other_repo = tmp_path / "other-repo"
    (other_repo / ".git").mkdir(parents=True)
    with pytest.raises(ValueError, match="nested Git"):
        benchmark._validate_worker_destinations(
            other_repo / "data", tmp_path / "result.json", source
        )


def test_worker_rejects_hardlinked_result_before_benchmark_work(tmp_path: Path, monkeypatch):
    target = tmp_path / "protected-result.json"
    target.write_text("protected")
    result_path = tmp_path / "worker-result.json"
    os.link(target, result_path)
    root = tmp_path / "worker-root"

    def benchmark_must_not_start(_source):
        raise AssertionError("result validation must precede benchmark work")

    monkeypatch.setattr(benchmark, "_select_application_source", benchmark_must_not_start)
    with pytest.raises(ValueError, match="worker result path must be new"):
        benchmark._worker(SCRIPT.parents[1], root, 1, result_path)

    assert target.read_text() == "protected"
    assert not root.exists()


def test_frozen_worker_rejects_hardlinked_result_before_clone(tmp_path: Path, monkeypatch):
    import shutil

    frozen = tmp_path / "frozen"
    frozen.mkdir()
    target = tmp_path / "protected-result.json"
    target.write_text("protected")
    result_path = tmp_path / "worker-result.json"
    os.link(target, result_path)
    root = tmp_path / "clone"

    def clone_must_not_start(*_args, **_kwargs):
        raise AssertionError("result validation must precede clone work")

    monkeypatch.setattr(shutil, "copytree", clone_must_not_start)
    with pytest.raises(ValueError, match="worker result path must be new"):
        benchmark._frozen_v2_build_worker(
            SCRIPT.parents[1], frozen, root, "capture-id", result_path
        )

    assert target.read_text() == "protected"
    assert not root.exists()


@pytest.mark.parametrize(
    ("profile", "refresh_mode", "baseline_capture_id"),
    [("full", "full", None), ("sparse-change", "incremental", "a" * 32)],
)
def test_worker_refuses_existing_operator_like_root_without_parent_authorization(
    tmp_path: Path, monkeypatch, profile, refresh_mode, baseline_capture_id
):
    root = tmp_path / "operator-like-root"
    root.mkdir()
    (root / "root.json").write_text('{"root_id":"operator"}\n')
    receipt = root / "receipt.json"
    receipt.write_text('{"status":"protected"}\n')
    before = receipt.read_bytes()

    def benchmark_must_not_start(_source):
        raise AssertionError("existing-root validation must precede app import")

    monkeypatch.setattr(benchmark, "_select_application_source", benchmark_must_not_start)
    with pytest.raises(ValueError, match="worker root must be new|authorization is missing"):
        benchmark._worker(
            SCRIPT.parents[1],
            root,
            1,
            tmp_path / "worker-result.json",
            profile=profile,
            scenario="sparse-change" if profile == "sparse-change" else "full",
            refresh_mode=refresh_mode,
            baseline_capture_id=baseline_capture_id,
        )

    assert receipt.read_bytes() == before


def test_worker_accepts_and_consumes_parent_authorized_baseline_clone(tmp_path: Path, monkeypatch):
    root = tmp_path / "prepared-clone"
    root.mkdir()
    (root / "root.json").write_text('{"root_id":"synthetic"}\n')
    receipt = root / "receipt.json"
    receipt.write_text('{"status":"baseline"}\n')
    baseline_capture_id = "b" * 32
    benchmark._write_clone_authorization(root, "sparse-change", "incremental", baseline_capture_id)

    def stop_after_authorization(_source):
        raise AssertionError("parent-authorized seed reached selected-source import")

    monkeypatch.setattr(benchmark, "_select_application_source", stop_after_authorization)
    with pytest.raises(AssertionError, match="reached selected-source import"):
        benchmark._worker(
            SCRIPT.parents[1],
            root,
            1,
            tmp_path / "worker-result.json",
            profile="sparse-change",
            scenario="sparse-change",
            refresh_mode="incremental",
            baseline_capture_id=baseline_capture_id,
        )

    assert not benchmark._clone_authorization_path(root).exists()
    assert receipt.read_text() == '{"status":"baseline"}\n'


@pytest.mark.parametrize(
    ("arguments", "expected_error"),
    [
        (
            ["--worker", "--markets", "0", "--root", "/tmp/root", "--result", "/tmp/result.json"],
            "markets must be positive",
        ),
        (["--worker", "--markets", "1"], "requires --root and --result"),
        (["--markets", "0"], "markets must be positive"),
    ],
)
def test_worker_cli_rejects_invalid_required_arguments(arguments, expected_error):
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, check=False
    )
    assert result.returncode == 2
    assert expected_error in result.stderr


def test_frozen_v2_cli_requires_an_explicit_prepared_capture(tmp_path: Path):
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--profile",
            "frozen-v2-build",
            "--report-dir",
            str(tmp_path / "report"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "requires --compare-root, --frozen-root, and --capture-id" in result.stderr


def test_duckdb_profile_flag_rejects_profiles_without_dbt_warehouses(tmp_path: Path):
    report_dir = tmp_path / "report"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--profile",
            "targeted",
            "--duckdb-profile-json",
            "--report-dir",
            str(report_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "requires a benchmark profile that builds a warehouse" in result.stderr
    assert not report_dir.exists()


def test_capture_preparation_cli_requires_combo_heavy_fixture(tmp_path: Path):
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--prepare-frozen-capture",
            str(tmp_path / "capture"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "requires --fixture combo-heavy --profile full" in result.stderr


def test_combo_fixture_worker_preserves_sealed_transport_options(tmp_path: Path):
    root = tmp_path / "capture-root"
    result_path = tmp_path / "capture-result.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--worker",
            "--markets",
            "1",
            "--root",
            str(root),
            "--result",
            str(result_path),
            "--worker-profile",
            "full",
            "--fixture",
            "combo-heavy",
            "--capture-only",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    metrics = json.loads(result_path.read_text())
    assert metrics["rate_policy"]["policy"] == "decrease-on-429"
    assert metrics["rate_policy"]["initial_rate"] == 1000
    assert metrics["endpoint_families"]["/markets"]["requested_encodings"] == {"gzip": 1}
    assert metrics["endpoint_families"]["/markets"]["observed_encodings"] == {"identity": 1}
