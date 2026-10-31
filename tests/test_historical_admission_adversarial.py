from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from kalshi_events.common import CatalogueError, Limits, Store, digest, json_bytes

REPO = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


benchmark = _load(
    "historical_admission_benchmark", REPO / "scripts/benchmark-historical-compact.py"
)
admission = _load("historical_admission", REPO / "scripts/historical_admission.py")


class _CaptureState(dict):
    verified_snapshot_sha256 = "c" * 64


def _census_fixture(tmp_path, monkeypatch):
    root = tmp_path / "operator"
    root.mkdir()
    (root / "root-id.json").write_text(
        json.dumps({"version": 1, "root_id": "b" * 32}), encoding="utf-8"
    )
    capture_id = "a" * 32
    page_rows = [
        {
            "index": 2,
            "relative": f"raw/unselected-{order}/manifest.json",
            "request_id": f"{order:064x}",
        }
        for order in range(8)
    ]
    page_rows[2] = {"index": 1, "relative": "raw/data-a/manifest.json", "request_id": "1" * 64}
    page_rows[4] = {"index": 1, "relative": "raw/control/manifest.json", "request_id": "2" * 64}
    page_rows[6] = {"index": 1, "relative": "raw/data-b/manifest.json", "request_id": "3" * 64}
    state = _CaptureState(
        capture_id=capture_id,
        page_rows=page_rows,
        complete=False,
        tasks=[{}, {"done": True}],
    )
    state.page_rows = page_rows
    selection = {"series": {"task_index": 1}}
    task_indices = {1}
    retired = []

    def historical_task_plan(current_state):
        if current_state["tasks"][1].get("done") is not True:
            raise CatalogueError("historical series traversal is incomplete")
        return selection, task_indices, retired

    monkeypatch.setattr(benchmark, "_historical_task_plan", historical_task_plan)

    metadata = {
        2: {
            **page_rows[2],
            "order": 2,
            "endpoint": "/series",
            "generation": 0,
            "encoded_bytes": 7,
            "decoded_bytes": 10,
            "status": 200,
            "manifest_bytes": 11,
        },
        4: {
            **page_rows[4],
            "order": 4,
            "endpoint": "/events",
            "generation": 0,
            "encoded_bytes": 3,
            "decoded_bytes": 4,
            "status": 404,
            "manifest_bytes": 13,
        },
        6: {
            **page_rows[6],
            "order": 6,
            "endpoint": "/series",
            "generation": 0,
            "encoded_bytes": 15,
            "decoded_bytes": 20,
            "status": 200,
            "manifest_bytes": 17,
        },
    }
    monkeypatch.setattr(
        benchmark,
        "_page_metadata",
        lambda _source, _row, order, _state: dict(metadata[order]),
    )
    source = SimpleNamespace(root=root, check_deadline=lambda: None)
    population_path = tmp_path / "population.json"
    index_path = tmp_path / "receipt-index.json"
    original_entries = [
        {key: metadata[order][key] for key in admission.INDEX_PAGE_FIELDS} for order in (2, 6)
    ]

    def bind(entries=None, population_overrides=None, manifest_metadata_bytes=41):
        pages = original_entries if entries is None else entries
        receipt_index = {
            "capture_id": capture_id,
            "capture_state_sha256": state.verified_snapshot_sha256,
            "historical_selection": selection,
            "selected_task_indices": [1],
            "retired_scopes_excluded": retired,
            "data_candidate_pages": 2,
            "manifest_metadata_bytes": manifest_metadata_bytes,
            "pages": pages,
        }
        index_raw = json.dumps(receipt_index, sort_keys=True).encode()
        index_path.write_bytes(index_raw)
        page_metadata = [metadata[2], metadata[6]]
        census = benchmark._receipt_population_metadata(page_metadata)
        population = {
            "status": "complete",
            "capture_id": capture_id,
            "capture_state_sha256": state.verified_snapshot_sha256,
            "metadata_index_sha256": hashlib.sha256(index_raw).hexdigest(),
            "candidate_pages": 2,
            "pages_processed": 2,
            "next_index": 2,
            "candidate_population": census,
            "encoded_bytes_verified": 22,
            "decoded_bytes_verified": 30,
            "page_counts": {"/series": 2},
            "observation_counts": {"/series": 30, "nested_event_markets": 0},
        }
        population.update(population_overrides or {})
        population_path.write_text(json.dumps(population, sort_keys=True), encoding="utf-8")
        return admission._admission_census_binding(
            source,
            state,
            population_path,
            index_path,
            benchmark,
            deadline=float("inf"),
        )

    return bind, source, population_path, index_path, original_entries, state


def test_census_manifest_metadata_includes_controls_for_legacy_index_compatibility(
    tmp_path, monkeypatch
):
    bind, *_rest = _census_fixture(tmp_path, monkeypatch)

    result = bind()

    assert result["manifest_metadata_bytes"] == 41
    with pytest.raises(CatalogueError, match="population totals differ"):
        bind(manifest_metadata_bytes=28)


def test_global_incomplete_capture_is_accepted_when_selected_historical_tasks_are_complete(
    tmp_path, monkeypatch
):
    bind, _source, _population, _index, _entries, state = _census_fixture(tmp_path, monkeypatch)

    result = bind()

    assert state["complete"] is False
    assert result["selection"] == {"series": {"task_index": 1}}


def test_incomplete_selected_historical_traversal_is_still_rejected(tmp_path, monkeypatch):
    import kalshi_events.capture as capture

    policies = {
        "rate_policy": "decrease-on-429",
        "parent_policy": "batched-v1",
        "cutoff_policy": "checkpoint-v1",
    }

    def task(endpoint, kind, params, *, singular=False, done=True, **extra):
        return {
            "endpoint": endpoint,
            "kind": kind,
            "params": params,
            "singular": singular,
            "requested": None,
            "pending": None,
            "done": done,
            "restarts": 0,
            **extra,
        }

    state = {
        **policies,
        "acquisition_protocol": capture.CURRENT_ACQUISITION_PROTOCOL,
        "acquisition_policy_sha256": digest(json_bytes(policies)),
        "capture_id": "a" * 32,
        "complete": False,
        "selected": None,
        "incremental": False,
        "tasks": [
            task(capture.CUTOFF_ENDPOINT, None, {}, singular=True),
            task("/series", "series", {}),
            task(
                "/events",
                "events",
                {"with_nested_markets": "false", "limit": capture.EVENT_PAGE_LIMIT},
            ),
            task(
                "/historical/markets",
                "markets",
                {"limit": capture.MARKET_PAGE_LIMIT},
                data_done=True,
            ),
            task(
                "/events/multivariate",
                "events",
                {"limit": capture.EVENT_PAGE_LIMIT},
                done=False,
            ),
        ],
        "task_scopes": {
            "1": "series",
            "2": "events_live",
            "3": "markets_history",
            "4": "events_combo",
        },
    }
    state["acquisition_policy_sha256"] = digest(
        json_bytes({key: state[key] for key in capture._PROTOCOL5_POLICIES})
    )

    selection, task_indices, retired = benchmark._historical_task_plan(state)

    assert state["complete"] is False
    assert set(selection) == {"series", "events_live", "markets_history"}
    assert task_indices == {0, 1, 2, 3}
    assert retired
    state["tasks"][3]["done"] = False
    with pytest.raises(CatalogueError, match="historical markets_history traversal is incomplete"):
        benchmark._historical_task_plan(state)


def test_census_accepts_sparse_global_orders_and_binds_each_page_to_current_state(
    tmp_path, monkeypatch
):
    bind, source, population_path, index_path, entries, _state = _census_fixture(
        tmp_path, monkeypatch
    )
    before = {
        path: path.read_bytes()
        for path in (source.root / "root-id.json", population_path, index_path)
        if path.exists()
    }

    result = bind()

    assert [page["order"] for page in result["candidates"]] == [2, 6]
    assert [page["order"] for page in result["controls"]] == [4]
    assert result["population"]["candidate_pages"] == 2
    assert result["population"]["exact_observations"] == 30
    assert (
        result["fingerprints"]["receipt_index_sha256"]
        == hashlib.sha256(index_path.read_bytes()).hexdigest()
    )
    assert len(entries) == 2
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("duplicate", "duplicate journal order"),
        ("swapped", "current journal order"),
        ("foreign", "data receipt is missing from the census index"),
        ("same_page_metadata", "differs from the current operator root"),
    ],
)
def test_census_rejects_duplicate_swapped_foreign_or_misbound_pages(
    tmp_path, monkeypatch, mutation, message
):
    bind, _source, _population_path, _index_path, original, _state = _census_fixture(
        tmp_path, monkeypatch
    )
    entries = [dict(entry) for entry in original]
    if mutation == "duplicate":
        entries[1] = dict(entries[0])
    elif mutation == "swapped":
        for key in ("relative", "request_id"):
            entries[0][key], entries[1][key] = entries[1][key], entries[0][key]
    elif mutation == "foreign":
        entries[1].update(
            order=1,
            index=2,
            relative="raw/unselected-1/manifest.json",
            request_id=f"{1:064x}",
        )
    else:
        entries[1]["encoded_bytes"] += 1

    with pytest.raises(CatalogueError, match=message):
        bind(entries)


def test_census_rejects_a_population_report_from_a_different_state(tmp_path, monkeypatch):
    bind, *_rest = _census_fixture(tmp_path, monkeypatch)

    with pytest.raises(CatalogueError, match="not bound to the current capture"):
        bind(population_overrides={"capture_state_sha256": "d" * 64})


def test_sample_order_is_deterministic_stratified_and_preserves_whole_receipts():
    candidates = [
        {
            "relative": f"raw/{endpoint.strip('/')}/{index}/manifest.json",
            "request_id": f"{endpoint}{index}",
            "endpoint": endpoint,
            "generation": generation,
            "order": index,
            "decoded_bytes": 100 + index * 10,
            "encoded_bytes": 80 + index,
        }
        for endpoint, generation, count in (("/events", 0, 12), ("/series", 1, 8))
        for index in range(count)
    ]

    ordered, reasons, strata = admission._admission_sample_order(candidates, "a" * 32)
    again, _, _ = admission._admission_sample_order(candidates, "a" * 32)

    assert [page["relative"] for page in ordered] == [page["relative"] for page in again]
    assert len(ordered) == len(candidates)
    assert {page["relative"] for page in ordered} == {page["relative"] for page in candidates}
    assert all(page in candidates for page in ordered)
    assert {item["candidate_pages"] for item in strata} == {8, 12}
    assert all(item["seeded_pages"] >= 3 for item in strata)
    for endpoint, generation, _count in (("/events", 0, 12), ("/series", 1, 8)):
        group = sorted(
            (
                page
                for page in candidates
                if (page["endpoint"], page["generation"]) == (endpoint, generation)
            ),
            key=lambda page: page["order"],
        )
        assert "first_position" in reasons[group[0]["relative"]]
        assert "middle_position" in reasons[group[len(group) // 2]["relative"]]
        assert "last_position" in reasons[group[-1]["relative"]]


def test_sample_order_rejects_duplicate_paths_and_foreign_hints():
    candidate = {
        "relative": "raw/events/one/manifest.json",
        "request_id": "1" * 64,
        "endpoint": "/events",
        "generation": 0,
        "order": 0,
        "decoded_bytes": 1,
        "encoded_bytes": 1,
    }

    with pytest.raises(CatalogueError, match="not unique"):
        admission._admission_sample_order([candidate, dict(candidate)], "a" * 32)
    with pytest.raises(CatalogueError, match="foreign receipt"):
        admission._admission_sample_order([candidate], "a" * 32, {"raw/foreign/manifest.json"})


def _forecast_inputs():
    targets = (250_000, 1_000_000, 3_000_000)
    seed_reasons = [
        "first_position",
        "middle_position",
        "last_position",
        "smallest_decoded",
        "middle_decoded",
        "largest_decoded",
        "poorest_compression",
    ]
    stages = []
    for target in targets:
        profile = {
            "endpoint": "/events",
            "generation": 0,
            "observations": target,
            "landing_utf8_bytes": target * 100,
            "decoded_bytes": target * 100,
            "seed_reasons": seed_reasons,
            "wide_rule_fields": 1,
            "high_entropy_rule_fields": 1,
            "max_combo_legs": 1,
            "repeated_rule_values": 1,
            "unicode_scalars": 1,
            "decimal_strings": 1,
        }
        stages.append(
            {
                "target_observations": target,
                "observations": target,
                "profiles": [profile],
                "database_phase_bytes": target * 45,
                "database_phase_allocated_bytes": target * 40,
                "build_database_component_bytes": target * 50,
                "build_database_allocated_component_bytes": target * 45,
                "export_bytes": target * 10,
                "build_peak_temporary_bytes": target * 4,
                "build_peak_temporary_allocated_bytes": target * 3,
                "export_peak_temporary_bytes": target * 5,
                "export_peak_temporary_allocated_bytes": target * 4,
                "verification_peak_temporary_bytes": target * 6,
                "verification_peak_temporary_allocated_bytes": target * 5,
                "spill_peak_bytes": 0,
                "spill_peak_allocated_bytes": 0,
                "elapsed_seconds": target / 1000,
                "verification_seconds": target / 10_000,
                "spill_forecast_supported": True,
                "telemetry_complete": True,
                "verification_complete": True,
                "sample_duplicate_entities": 1 if target == targets[-1] else 0,
            }
        )
    population = {
        "exact_observations": 3_000_000,
        "observations_by_endpoint": {"/events": 3_000_000},
        "data_strata": [
            {
                "endpoint": "/events",
                "generation": 0,
                "candidate_pages": 1,
                "encoded_bytes": 300_000_000,
                "decoded_bytes": 300_000_000,
            }
        ],
    }
    operator = {
        "logical_bytes": 0,
        "allocated_bytes": 0,
        "database_bytes": 0,
        "database_allocated_bytes": 0,
        "temporary_bytes": 0,
        "temporary_allocated_bytes": 0,
    }
    baseline = {
        "baseline": {
            "operator": operator,
            "external": {"logical_bytes": 0, "allocated_bytes": 0},
        },
        "observed": {
            "isolated_artifacts": {"logical_bytes": 0, "allocated_bytes": 0},
            "filesystem_free_bytes": 200 * 1024**3,
        },
    }
    old_stress = {
        "bound": True,
        "bytes": 109_613_465_550,
        "gib": 102.085,
    }
    return population, stages, baseline, old_stress


def _valid_preflight():
    free_bytes = 150 * 1024**3
    return {
        "preflight_complete": True,
        "scope_complete": True,
        "no_writer_lock": True,
        "memory": {"system_memory_pressure": "normal"},
        "free_bytes": free_bytes,
        "operator_volume_uuid": "same-volume",
        "output_volume_uuid": "same-volume",
        "blockers": [],
    }


def test_complete_nested_forecast_can_admit_but_never_certifies_production():
    population, stages, baseline, old_stress = _forecast_inputs()

    forecast = admission._admission_forecast(population, stages, baseline, old_stress)
    decision = admission._admission_decision(forecast, _valid_preflight(), old_stress)

    assert forecast["supported"] is True
    assert forecast["blockers"] == []
    assert decision["decision"] == "ADMIT"
    assert decision["launch_allowed"] is True
    assert decision["production_admitted"] is False


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("missing_peak", "multi-scale resource telemetry is incomplete"),
        ("unknown_database", "database, export, or landing measurements are unavailable"),
        ("missing_spill", "build/export spill forecast is unsupported"),
        (
            "missing_supervision",
            "supervisor or semantic/hash/schema verification telemetry is incomplete",
        ),
        ("nonlinear", "measured scale growth is nonlinear"),
        ("unknown_capacity", "projected filesystem free space falls below 150 GiB or is unknown"),
        ("missing_target", "one or more nested whole-receipt sample targets were not completed"),
        ("missing_endpoint", "exact census population is unknown or inconsistent"),
    ],
)
def test_unknown_or_unsafe_forecast_inputs_never_become_admission(failure, message):
    population, stages, baseline, old_stress = _forecast_inputs()
    if failure == "missing_peak":
        stages[1].pop("build_peak_temporary_bytes")
    elif failure == "unknown_database":
        stages[2]["build_database_component_bytes"] = None
    elif failure == "missing_spill":
        stages[0]["spill_forecast_supported"] = False
    elif failure == "missing_supervision":
        stages[0]["verification_complete"] = False
    elif failure == "nonlinear":
        stages[0]["export_bytes"] *= 2
    elif failure == "unknown_capacity":
        baseline["observed"]["filesystem_free_bytes"] = None
    elif failure == "missing_target":
        stages.pop(0)
    else:
        population["observations_by_endpoint"]["/series"] = 1

    forecast = admission._admission_forecast(population, stages, baseline, old_stress)
    decision = admission._admission_decision(forecast, _valid_preflight(), old_stress)

    assert forecast["supported"] is False
    assert any(message in blocker for blocker in forecast["blockers"])
    assert decision["decision"] == "NO_ADMIT"
    assert decision["launch_allowed"] is False
    assert decision["capacity_admitted"] is False


def test_database_scenario_includes_existing_database_and_wal_baseline():
    population, stages, baseline, old_stress = _forecast_inputs()
    gib = 1024**3
    baseline["baseline"]["operator"].update(
        logical_bytes=100 * gib,
        allocated_bytes=100 * gib,
        database_bytes=100 * gib,
        database_allocated_bytes=100 * gib,
    )
    baseline["observed"]["filesystem_free_bytes"] = 300 * gib

    forecast = admission._admission_forecast(population, stages, baseline, old_stress)

    assert forecast["supported"] is False
    assert forecast["endpoint_weighted"]["database_baseline_bytes"] == 100 * gib
    assert forecast["endpoint_weighted"]["database_scenario_total_bytes"] > 96 * gib
    assert any("database baseline plus" in item for item in forecast["blockers"])


def test_verification_workspace_is_included_in_phase_and_temporary_scenarios():
    population, stages, baseline, old_stress = _forecast_inputs()
    for stage in stages:
        target = stage["target_observations"]
        stage["verification_peak_temporary_bytes"] = target * 500
        stage["verification_peak_temporary_allocated_bytes"] = target * 400

    forecast = admission._admission_forecast(population, stages, baseline, old_stress)

    weighted = forecast["endpoint_weighted"]
    assert (
        weighted["verification_temporary_growth_bytes"] > weighted["export_temporary_growth_bytes"]
    )
    assert (
        weighted["temporary_scenario_total_bytes"]
        >= weighted["verification_temporary_growth_bytes"]
    )
    assert weighted["operator_root_scenario_bytes"] == 654 * 3_000_000


def test_preflight_blockers_and_unbound_old_stress_cannot_be_overridden():
    population, stages, baseline, old_stress = _forecast_inputs()
    forecast = admission._admission_forecast(population, stages, baseline, old_stress)

    preflight = _valid_preflight()
    preflight["blockers"] = ["selected source scope is incomplete"]
    preflight_blocked = admission._admission_decision(forecast, preflight, old_stress)
    stress_unbound = admission._admission_decision(forecast, _valid_preflight(), {"bound": False})

    assert preflight_blocked["decision"] == "NO_ADMIT"
    assert "selected source scope is incomplete" in preflight_blocked["blockers"]
    assert stress_unbound["decision"] == "NO_ADMIT"
    assert any("stress is unbound" in blocker for blocker in stress_unbound["blockers"])


@pytest.mark.parametrize(
    "mutation",
    (
        "missing_preflight_complete",
        "incomplete_preflight",
        "missing_scope_complete",
        "incomplete_scope",
        "writer_lock",
        "high_memory_pressure",
        "free_space_below_boundary",
        "missing_volume_id",
        "different_volumes",
    ),
)
def test_preflight_unknown_or_unsafe_state_never_admits(mutation):
    population, stages, baseline, old_stress = _forecast_inputs()
    forecast = admission._admission_forecast(population, stages, baseline, old_stress)
    preflight = _valid_preflight()
    if mutation == "missing_preflight_complete":
        del preflight["preflight_complete"]
    elif mutation == "incomplete_preflight":
        preflight["preflight_complete"] = False
    elif mutation == "missing_scope_complete":
        del preflight["scope_complete"]
    elif mutation == "incomplete_scope":
        preflight["scope_complete"] = False
    elif mutation == "writer_lock":
        preflight["no_writer_lock"] = False
    elif mutation == "high_memory_pressure":
        preflight["memory"]["system_memory_pressure"] = "warn"
    elif mutation == "free_space_below_boundary":
        preflight["free_bytes"] -= 1
    elif mutation == "missing_volume_id":
        preflight["operator_volume_uuid"] = None
    else:
        preflight["output_volume_uuid"] = "another-volume"

    decision = admission._admission_decision(forecast, preflight, old_stress)

    assert decision["decision"] == "NO_ADMIT"
    assert decision["launch_allowed"] is False


def test_storage_accounting_counts_database_and_tmp_logically_but_deduplicates_inodes(
    tmp_path,
):
    operator = tmp_path / "operator"
    production = tmp_path / "production"
    output = tmp_path / "output"
    database = operator / "tmp" / "build" / "warehouse.duckdb"
    alias = production / "diagnostics" / "same-db.duckdb"
    output_file = output / "export.parquet"
    database.parent.mkdir(parents=True)
    alias.parent.mkdir(parents=True)
    output.mkdir()
    database.write_bytes(b"database bytes")
    os.link(database, alias)
    output_file.write_bytes(b"export bytes")

    measured = admission._measure_admission_storage(
        operator, production, output, device=operator.stat().st_dev
    )

    assert measured["operator"]["database_bytes"] == database.stat().st_size
    assert measured["operator"]["temporary_bytes"] == database.stat().st_size
    assert measured["operator"]["logical_bytes"] == database.stat().st_size
    assert measured["external"]["logical_bytes"] == alias.stat().st_size
    assert measured["external"]["allocated_bytes"] == 0
    assert measured["aggregate_logical_bytes"] == (
        database.stat().st_size * 2 + output_file.stat().st_size
    )
    assert measured["aggregate_allocated_bytes"] == (
        database.stat().st_blocks * 512 + output_file.stat().st_blocks * 512
    )


def test_verification_duckdb_config_is_bounded_and_spill_is_in_all_categories(tmp_path):
    from kalshi_events import warehouse

    root = tmp_path / "operator"
    root.mkdir()
    store = Store(
        root,
        Limits(
            max_duration_seconds=1200,
            max_root_bytes=16 * 1024**3,
            max_temp_bytes=16 * 1024**3,
            max_database_bytes=16 * 1024**3,
        ),
    )
    verification = store.path("tmp/verification")
    verification.mkdir(parents=True)

    with warehouse._duckdb_work_config(store, verification) as config:
        assert config["memory_limit"] == warehouse.DBT_MEMORY_LIMIT
        assert config["threads"] == str(warehouse.DBT_THREADS)
        assert Path(config["temp_directory"]).is_relative_to(verification)
        assert int(config["max_temp_directory_size"].removesuffix("B")) > 0
        with warehouse.duckdb.connect(":memory:", config=config) as connection:
            assert connection.execute("select 1").fetchone() == (1,)
        spill_file = Path(config["temp_directory"]) / "bounded-test.spill"
        spill_file.write_bytes(b"observed spill")
        store.sync("tmp/verification")

    measured = admission._measure_tree(root, device=root.stat().st_dev)
    assert measured["spill_bytes"] == len(b"observed spill")
    assert measured["verification_bytes"] == len(b"observed spill")
    assert measured["temporary_bytes"] == len(b"observed spill")


def test_storage_accounting_classifies_dbt_spill_directory_with_generic_filename(tmp_path):
    root = tmp_path / "fixture"
    spill = root / "tmp" / "dbt-uuid" / "spill" / "foo.tmp"
    spill.parent.mkdir(parents=True)
    spill.write_bytes(b"dbt temporary spill")

    measured = admission._measure_tree(root, device=root.stat().st_dev)

    assert measured["spill_bytes"] == spill.stat().st_size
    assert measured["spill_allocated_bytes"] == spill.stat().st_blocks * 512
    assert measured["temporary_bytes"] == spill.stat().st_size


def test_operational_census_pins_root_capture_and_exact_population():
    binding = {
        "capture_id": admission.AUTHORIZED_CAPTURE_ID,
        "root_id": admission.AUTHORIZED_ROOT_ID,
        "capture_state_sha256": admission.AUTHORIZED_STATE_SHA256,
        "fingerprints": {"receipt_index_sha256": admission.AUTHORIZED_INDEX_SHA256},
        "population": {
            **admission.AUTHORIZED_POPULATION,
            "observations_by_endpoint": dict(
                admission.AUTHORIZED_POPULATION["observations_by_endpoint"]
            ),
        },
    }

    result = admission._check_authorized_census(binding)

    assert result["checked"] is True
    binding["population"]["observations_by_endpoint"]["/events"] += 1
    with pytest.raises(CatalogueError, match="approved capture, root, and exact population"):
        admission._check_authorized_census(binding)


def test_untrusted_prior_tail_hint_does_not_replace_fresh_profile_evidence():
    population, stages, baseline, old_stress = _forecast_inputs()
    stages[-1]["profiles"][0]["max_combo_legs"] = 0

    forecast = admission._admission_forecast(population, stages, baseline, old_stress)

    assert forecast["supported"] is False
    assert any("freshly normalized full-sample combo" in item for item in forecast["blockers"])
