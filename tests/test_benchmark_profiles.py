from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
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
        def _initialize_v2(self, _store, state):
            return state

    capture = Capture()
    assert benchmark._freeze_legacy_acquisition("full", "full", capture)
    state = {"acquisition_protocol": 3}
    assert capture._initialize_v2(None, state) == {}

    current = Capture()
    assert not benchmark._freeze_legacy_acquisition("selected-stress", "full", current)
    assert benchmark._freeze_legacy_acquisition("history-sparse", "history-sparse", current)
    assert benchmark._freeze_legacy_acquisition("baseline-v2-setup", "full", Capture())
    baseline = Capture()
    assert benchmark._freeze_legacy_acquisition("baseline-setup", "history-sparse", baseline)


def test_legacy_targeted_freeze_restores_original_singular_task_plan():
    class Capture:
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


def test_worker_destinations_are_outside_known_and_unlisted_checkouts(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    benchmark._validate_worker_destinations(tmp_path / "data", tmp_path / "result.json", source)
    with pytest.raises(ValueError, match="source checkout"):
        benchmark._validate_worker_destinations(source / "data", tmp_path / "result.json", source)
    other_repo = tmp_path / "other-repo"
    (other_repo / ".git").mkdir(parents=True)
    with pytest.raises(ValueError, match="Git checkout"):
        benchmark._validate_worker_destinations(
            other_repo / "data", tmp_path / "result.json", source
        )


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
