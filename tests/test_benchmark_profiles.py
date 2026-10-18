from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from contextlib import contextmanager
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


def test_selected_comparison_rejects_native_coverage_or_provenance_mismatch():
    reference = {
        "selected_semantics": {"markets": {"sha256": "a"}},
        "selected_coverage": [{"market_ticker": "MKT-1", "status": "found"}],
        "selected_provenance_valid": True,
    }
    candidate = json.loads(json.dumps(reference))
    assert all(benchmark._selected_comparison(reference, candidate).values())
    for key, value in (
        ("selected_semantics", {"markets": {"sha256": "tampered"}}),
        ("selected_coverage", [{"market_ticker": "MKT-1", "status": "absent"}]),
        ("selected_provenance_valid", False),
    ):
        modified = {**candidate, key: value}
        assert not all(benchmark._selected_comparison(reference, modified).values())


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
    fixture = benchmark.FixtureClient(1000, "history-sparse", "baseline")
    history = benchmark.FixtureClient._historical_market(249_999)
    assert benchmark.HISTORY_STRESS_COUNT == 250_000
    assert benchmark.HISTORY_STRESS_IDENTITIES == 10_000
    assert benchmark.HISTORY_STRESS_PASSES == 25
    assert history["ticker"] == "HIST-249999"
    assert history["settlement_ts"].endswith("+01:00")

    status, first = fixture.response("/historical/markets", {})
    _, repeated = fixture.response(
        "/historical/markets", {"cursor": str(benchmark.HISTORY_STRESS_IDENTITIES)}
    )
    assert status == 200
    assert len(first["markets"]) == len(repeated["markets"]) == benchmark.PAGE_SIZE
    assert first["markets"][0] == repeated["markets"][0]

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
    assert not benchmark._freeze_legacy_acquisition("history-sparse", "history-sparse", current)


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
        }
        for index in (1, 2, 3)
    ]
    assert benchmark._identity_stability({"compare": runs}, incremental=True) == {"compare": True}
    runs[1]["lineage"]["root_id"] = "f" * 32
    assert benchmark._identity_stability({"compare": runs}, incremental=True) == {"compare": False}


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
