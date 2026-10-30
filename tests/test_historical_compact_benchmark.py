from __future__ import annotations

import importlib.util
from pathlib import Path

from kalshi_events.capture import (
    CURRENT_ACQUISITION_PROTOCOL,
    CUTOFF_ENDPOINT,
    EVENT_PAGE_LIMIT,
    HISTORICAL_PREFIX,
    MARKET_PAGE_LIMIT,
)
from kalshi_events.common import digest, json_bytes

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
            "endpoint": ("/series", "/events", "/historical/markets", "/series")[index % 4],
            "generation": index % 2,
            "observations": 10,
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
    native = {"database_bytes": 10_000, "peak_temporary_bytes": 12_000}

    protected_index = int(6.7 * 1024**3)
    forecast = benchmark._capacity_forecast(
        sample,
        legacy,
        native,
        5000,
        {"database_bytes": 0, "export_bytes": 0, "total_bytes": 0},
        benchmark.RAW_ROOT_BASELINE_BYTES,
        protected_index,
        0,
        1000,
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
        benchmark.RAW_ROOT_BASELINE_BYTES,
        0,
        1,
        1000,
    )

    assert forecast["upper_observations"] == 2000
    assert forecast["sample_coverage_sufficient"] is True
    assert forecast["forecast_fits"] is True
    assert (
        forecast["projected_temporary_peak_bytes"]["components_upper"]["protected_index_baseline"]
        == protected_index
    )
    assert forecast["production_admitted"] is False
    assert forecast["capacity_admitted"] is False
    assert insufficient["sample_coverage_sufficient"] is False
    assert insufficient["forecast_fits"] is True
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
