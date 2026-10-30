from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from kalshi_events.capture import (
    CURRENT_ACQUISITION_PROTOCOL,
    CUTOFF_ENDPOINT,
    EVENT_PAGE_LIMIT,
    HISTORICAL_PREFIX,
    MARKET_PAGE_LIMIT,
)
from kalshi_events.common import CatalogueError, digest, json_bytes

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/benchmark-historical-compact.py"
SPEC = importlib.util.spec_from_file_location("benchmark_historical_compact", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_sample_selects_page_positions_and_size_tails_per_generation():
    pages = [
        {
            "relative": f"raw/{index:032x}/manifest.json",
            "endpoint": endpoint,
            "generation": generation,
            "order": index,
            "decoded_bytes": size,
            "status": 200,
        }
        for endpoint, generation, start in (("/series", 0, 0), ("/historical/markets", 1, 10))
        for index, size in enumerate((10, 20, 30, 40, 50, 60, 70), start)
    ]

    sample = benchmark._stratified_sample(pages)

    selected = {(page["endpoint"], page["generation"], page["order"]) for page in sample}
    assert ("/series", 0, 0) in selected
    assert ("/series", 0, 3) in selected
    assert ("/series", 0, 6) in selected
    assert ("/historical/markets", 1, 10) in selected
    assert ("/historical/markets", 1, 13) in selected
    assert ("/historical/markets", 1, 16) in selected


def test_data_sample_separates_control_receipts_and_capture_row_bound():
    pages = [
        {
            "relative": f"raw/{index:032x}/manifest.json",
            "endpoint": endpoint,
            "generation": 0,
            "order": index,
            "decoded_bytes": 10 + index,
            "encoded_bytes": 100 + index,
            "status": status,
        }
        for index, (endpoint, status) in enumerate(
            (("/series", 200), ("/events", 404), ("/historical/cutoff", 200))
        )
    ]

    data = benchmark._stratified_sample(pages)
    controls = benchmark._control_sample(pages)

    assert [page["endpoint"] for page in data] == ["/series"]
    assert {(page["endpoint"], page["status"]) for page in controls} == {
        ("/events", 404),
        ("/historical/cutoff", 200),
    }
    assert benchmark._control_receipt_counts(pages) == [
        {"endpoint": "/events", "generation": 0, "status": 404, "receipts": 1},
        {"endpoint": "/historical/cutoff", "generation": 0, "status": 200, "receipts": 1},
    ]
    population = benchmark._receipt_population_metadata(pages)
    assert population["data_strata"] == [
        {
            "endpoint": "/series",
            "generation": 0,
            "candidate_pages": 1,
            "encoded_bytes": 100,
            "decoded_bytes": 10,
            "median_decoded_page_bytes": 10,
            "max_decoded_page_bytes": 10,
        }
    ]
    assert population["control_receipts"] == benchmark._control_receipt_counts(pages)
    benchmark._check_capture_entry_count(322_030)
    benchmark._check_capture_entry_count(benchmark.MAX_CAPTURE_ENTRIES)
    with pytest.raises(CatalogueError, match="too many receipt entries"):
        benchmark._check_capture_entry_count(benchmark.MAX_CAPTURE_ENTRIES + 1)


def test_task_plan_uses_production_legacy_opt_in_and_excludes_retired_scopes():
    policies = {
        "rate_policy": "fixed",
        "parent_policy": "batched-v1",
        "cutoff_policy": "checkpoint-v1",
    }
    tasks = [
        {
            "endpoint": CUTOFF_ENDPOINT,
            "kind": None,
            "params": {},
            "singular": True,
            "requested": None,
            "pending": None,
            "done": True,
        },
        {
            "endpoint": "/series",
            "kind": "series",
            "params": {},
            "singular": False,
            "requested": None,
            "pending": None,
            "done": True,
            "restarts": 0,
        },
        {
            "endpoint": "/events",
            "kind": "events",
            "params": {"with_nested_markets": "false", "limit": EVENT_PAGE_LIMIT},
            "singular": False,
            "requested": None,
            "pending": None,
            "done": True,
            "restarts": 0,
        },
        {
            "endpoint": "/events/multivariate",
            "kind": "events",
            "params": {},
            "singular": False,
            "requested": None,
            "pending": None,
            "done": False,
            "restarts": 0,
        },
        {
            "endpoint": "/markets",
            "kind": "markets",
            "params": {},
            "singular": False,
            "requested": None,
            "pending": None,
            "done": False,
            "restarts": 0,
        },
        {
            "endpoint": HISTORICAL_PREFIX,
            "kind": "markets",
            "params": {"limit": MARKET_PAGE_LIMIT},
            "singular": False,
            "requested": None,
            "pending": None,
            "done": True,
            "restarts": 0,
            "data_done": True,
        },
    ]
    state = {
        "capture_id": "a" * 32,
        "acquisition_protocol": CURRENT_ACQUISITION_PROTOCOL,
        "acquisition_policy_sha256": digest(json_bytes(policies)),
        **policies,
        "selected": None,
        "incremental": False,
        "task_scopes": {
            "1": "series",
            "2": "events_live",
            "3": "events_combo",
            "4": "markets_live",
            "5": "markets_history",
        },
        "tasks": tasks,
    }

    selection, task_indices, excluded = benchmark._historical_task_plan(state)

    assert set(selection) == {"series", "events_live", "markets_history"}
    assert task_indices == {0, 1, 2, 5}
    assert {item["scope"] for item in excluded} == {"events_combo", "markets_live"}


def test_profile_detects_wide_rules_combo_legs_unicode_and_decimal_strings():
    payload = {
        "markets": [
            {
                "rules_primary": "".join(chr(0x400 + index) for index in range(5000)),
                "title": "café",
                "last_price_dollars": "0.000000000000000001",
                "mve_selected_legs": [{"market_ticker": f"M{i}"} for i in range(8)],
            }
        ]
    }

    profile = benchmark._row_profile([{"payload_json": "{}"}], payload)

    assert profile["wide_rule_fields"] == 1
    assert profile["high_entropy_rule_fields"] == 1
    assert profile["max_combo_legs"] == 8
    assert profile["unicode_scalars"] > 0
    assert profile["decimal_strings"] == 1


def test_capacity_forecast_separates_uncertainty_coverage_and_operational_preconditions():
    sample = [
        {
            "endpoint": (
                "/series",
                "/events",
                "/historical/markets",
                "/series",
                "/events",
                "/historical/markets",
            )[index % 6],
            "generation": index % 2,
            "observations": 10,
            "decoded_bytes": 100,
            "landing_utf8_bytes": 1000,
            "wide_rule_fields": 1,
            "high_entropy_rule_fields": 1,
            "repeated_rule_values": 1,
            "decimal_strings": 1,
            "unicode_scalars": 1,
            "max_combo_legs": 8,
        }
        for index in range(8)
    ]
    sample.append({"sample_duplicate_entities": 1, "metadata_coverage_complete": True})
    legacy = {"database_bytes": 10_000}
    native = {
        "database_bytes": 10_000,
        "peak_temporary_bytes": 15_000,
        "build_peak_temporary_bytes": 14_000,
        "export_peak_temporary_bytes": 15_000,
        "parent_index_bytes": 1000,
    }
    population_metadata = [
        {"endpoint": "/series", "generation": 0, "candidate_pages": 2, "decoded_bytes": 200},
        {"endpoint": "/series", "generation": 1, "candidate_pages": 1, "decoded_bytes": 100},
        {"endpoint": "/events", "generation": 0, "candidate_pages": 1, "decoded_bytes": 100},
        {"endpoint": "/events", "generation": 1, "candidate_pages": 2, "decoded_bytes": 200},
        {
            "endpoint": "/historical/markets",
            "generation": 0,
            "candidate_pages": 1,
            "decoded_bytes": 100,
        },
        {
            "endpoint": "/historical/markets",
            "generation": 1,
            "candidate_pages": 1,
            "decoded_bytes": 100,
        },
    ]

    protected_index = int(6.7 * 1024**3)
    forecast = benchmark._capacity_forecast(
        sample,
        legacy,
        native,
        5000,
        {"database_bytes": 0, "export_bytes": 0, "total_bytes": 0},
        benchmark.RAW_ROOT_BASELINE_ASSUMPTION_BYTES,
        protected_index,
        0,
        1000,
        population_metadata,
    )
    missing_strata = [
        *sample[:-1],
        {"sample_duplicate_entities": 0, "metadata_coverage_complete": True},
    ]
    insufficient = benchmark._capacity_forecast(
        missing_strata,
        legacy,
        native,
        5000,
        {"database_bytes": 0, "export_bytes": 0, "total_bytes": 0},
        benchmark.RAW_ROOT_BASELINE_ASSUMPTION_BYTES,
        0,
        1,
        1000,
        population_metadata,
    )

    assert forecast["twofold_population_sensitivity_observations"] == 2000
    assert forecast["upper_observations"] is None
    assert forecast["sensitivity_is_formal_bound"] is False
    assert forecast["sample_coverage_sufficient"] is True
    assert forecast["forecast_fits"] is False
    assert forecast["spill_upper_known"] is False
    assert forecast["forecast_status"] == "not_admitted_spill_upper_unknown"
    assert (
        forecast["projected_temporary_peak_bytes"]["components_sensitivity"][
            "protected_index_baseline"
        ]
        == protected_index
    )
    temporary_components = forecast["projected_temporary_peak_bytes"]["components_sensitivity"]
    assert forecast["projected_temporary_peak_bytes"]["upper"] is None
    assert forecast["projected_temporary_peak_bytes"]["scenario_with_4_gib_spill_reserve"] == max(
        temporary_components["build_phase_with_wal"],
        temporary_components["export_phase_with_sort_spill_reserve"],
    )
    assert forecast["production_admitted"] is False
    assert forecast["capacity_admitted"] is False
    assert forecast["root_limit_bytes"] == 350 * 1024**3
    assert forecast["aggregate_limit_bytes"] == 400 * 1024**3
    assert forecast["projected_root_bytes"]["scenario_fits_limit"] is True
    assert forecast["projected_root_bytes"]["raw_root_baseline_verified"] is False
    root_build_components = forecast["projected_temporary_peak_bytes"]["components_sensitivity"]
    assert forecast["projected_root_bytes"]["build_phase_upper"] == (
        benchmark.RAW_ROOT_BASELINE_ASSUMPTION_BYTES
        + protected_index
        + root_build_components["build_phase_peak_scaled"]
        + root_build_components["wal_reserve"]
    )
    assert forecast["projected_aggregate_bytes"]["scenario_fits_limit"] is True
    assert forecast["projected_aggregate_bytes"]["upper"] is None
    assert forecast["fresh_root_inventory_required"] is True
    assert forecast["operational_preconditions_clear"] is False
    assert any("fresh root inventory" in item for item in forecast["forecast_blockers"])
    assert insufficient["sample_coverage_sufficient"] is False
    assert insufficient["forecast_fits"] is False
    assert insufficient["operational_preconditions_clear"] is False


def test_help_does_not_start_sampling_or_create_an_output_directory(tmp_path):
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0
    assert "read-only operator root" in result.stdout
    assert not list(tmp_path.iterdir())
