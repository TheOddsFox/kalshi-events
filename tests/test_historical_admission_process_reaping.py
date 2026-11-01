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
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


admission = _load("historical_admission_reaping", REPO / "scripts/historical_admission.py")
benchmark = _load(
    "historical_admission_worker_deadline",
    REPO / "scripts/benchmark-historical-compact.py",
)
production_run = _load("production_run_admission_reaping", REPO / "scripts/production_run.py")


def _write_four_row_worker_fixture(root: Path) -> tuple[str, list[int]]:
    from kalshi_events.common import CURRENT_ROOT_MARKER

    capture_id = "c7a76754f7b74d5baa37a7b064d39aa7"
    policy_sha = "4f4f6bd916069ccc53707481bb677333a97383160b8b880cac01d6c20f564b76"
    tasks = [
        {
            "cursor": "",
            "cutoff_due": False,
            "done": True,
            "endpoint": "/historical/cutoff",
            "kind": None,
            "params": {},
            "pending": None,
            "requested": None,
            "singular": True,
            "restarts": 0,
        },
        {
            "cursor": "",
            "cutoff_due": False,
            "done": True,
            "endpoint": "/series",
            "kind": "series",
            "params": {},
            "pending": None,
            "requested": None,
            "singular": False,
            "restarts": 0,
        },
        {
            "cursor": "",
            "cutoff_due": False,
            "done": True,
            "endpoint": "/events",
            "kind": "events",
            "params": {"limit": 200, "with_nested_markets": "false"},
            "pending": None,
            "requested": None,
            "singular": False,
            "restarts": 0,
        },
        {
            "cursor": "",
            "cutoff_due": False,
            "cutoff_ordinal": 2,
            "cutoff_reason": None,
            "data_done": True,
            "done": True,
            "endpoint": "/historical/markets",
            "kind": "markets",
            "last_cutoff_at": "2026-10-31T20:16:28.501667Z",
            "pages_since_cutoff": 0,
            "params": {"limit": 1000},
            "pending": None,
            "requested": None,
            "singular": False,
            "restarts": 0,
        },
    ]
    state = {
        "accounting_complete": True,
        "acquisition_policy_sha256": policy_sha,
        "acquisition_protocol": 5,
        "budget_exhausted": False,
        "capture_id": capture_id,
        "complete": False,
        "compression": "gzip",
        "cutoff": {
            "market_settled_ts": "2026-01-01T00:00:00Z",
            "trades_created_ts": "2026-02-01T00:00:00Z",
        },
        "cutoff_policy": "checkpoint-v1",
        "max_decoded_response_bytes": 402653184,
        "max_response_bytes": 402653184,
        "parent_policy": "historical-only-v1",
        "rate_policy": "decrease-on-429",
        "task_scopes": {"1": "series", "2": "events_live", "3": "markets_history"},
        "tasks": tasks,
        "version": 1,
    }
    root.mkdir(parents=True)
    (root / "root.json").write_text(json.dumps(CURRENT_ROOT_MARKER), encoding="utf-8")
    (root / "diagnostic-state.json").write_text(json.dumps(state), encoding="utf-8")
    (root / ".diagnostic-sample.json").write_text(
        json.dumps(
            {
                "kind": "historical-admission-subset",
                "source_capture_id": capture_id,
                "complete": False,
                "certified": False,
                "published": False,
            }
        ),
        encoding="utf-8",
    )

    receipt_specs = [
        (
            0,
            "483d007117254456aad8ce411cef0b9f",
            "00e7239421a600837a8975a47cc25f26a22034cf31af1841cffe1739e5ede0e4",
            "/historical/cutoff",
            None,
            {},
            True,
            None,
            None,
            b'{"market_settled_ts":"2026-01-01T00:00:00Z","trades_created_ts":"2026-02-01T00:00:00Z"}',
        ),
        (
            1,
            "53dc085e0fa44fe2956b01f813af6ebe",
            "65bb5b75c0d080f225450ae6f902e585c68fa98491a0888044646b781cf63f24",
            "/series",
            "series",
            {},
            False,
            "series",
            None,
            b'{"series":[{"contract_url":"https://example.test/rules","fee_multiplier":"1.25","ticker":"SERIES","title":"Native contracts"}]}',
        ),
        (
            2,
            "bb36c05a25aa4eabb04ec1d3fd7f6e52",
            "21f4f7a0f41ed1a47cdaf767218a326f70445aef895618a1b2d0d066ec6bd5bd",
            "/events",
            "events",
            {"limit": 200, "with_nested_markets": "false"},
            False,
            "events_live",
            None,
            b'{"cursor":"","events":[{"event_ticker":"EVENT","mutually_exclusive":false,"series_ticker":"SERIES","title":"Ordinary event"},{"event_ticker":"HISTORY","series_ticker":"SERIES","title":"Archived event"}]}',
        ),
        (
            3,
            "bddf65680b804193a81966bc5de666fd",
            "060bb4bbdb57efca54d3180f71e8a9e948ba541a05b6b179b3fcd9e21ebf41db",
            "/historical/markets",
            "markets",
            {"limit": 1000},
            False,
            "markets_history",
            0,
            b'{"cursor":"","markets":[{"event_ticker":"HISTORY","floor_strike":"3.123456789123456789","last_price_dollars":"0.00123","open_interest_fp":"2.125","price_level_structure":"deci_cent","price_ranges":[{"end":"1","start":"0","step":"0.0001"}],"result":"no","rules_primary":"Resolve YES only under native rules","rules_secondary":"Details","settlement_ts":"2025-01-01T00:00:00Z","settlement_value_dollars":"0.0000","status":"finalized","ticker":"HISTORICAL","volume_fp":"12.3456789"}]}',
        ),
    ]
    page_rows = []
    for (
        index,
        page_id,
        request_id,
        endpoint,
        kind,
        params,
        singular,
        scope,
        generation,
        body,
    ) in receipt_specs:
        relative = f"raw/{page_id}/manifest.json"
        manifest = {
            "accounting_complete": True,
            "acquisition_policy_sha256": policy_sha,
            "acquisition_protocol": 5,
            "capture_id": capture_id,
            "downloaded_bytes": len(body),
            "endpoint": endpoint,
            "entity_kind": kind,
            "page_id": page_id,
            "params": params,
            "phase": "page",
            "received_at": "2026-10-31T20:16:28.491452Z",
            "request_id": request_id,
            "sha256": hashlib.sha256(body).hexdigest(),
            "singular": singular,
            "size": len(body),
            "status": 200,
            "task_index": index,
            "version": 1,
        }
        if scope is not None:
            manifest["task_scope"] = scope
        if index > 0:
            manifest["task_generation"] = 0
        if generation is not None:
            manifest.update(
                request_cursor="",
                request_generation=generation,
            )
        directory = root / "raw" / page_id
        directory.mkdir(parents=True)
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (directory / "body.bin").write_bytes(body)
        page_rows.append({"index": index, "relative": relative, "request_id": request_id})
    (root / "diagnostic-page-index.json").write_text(json.dumps(page_rows), encoding="utf-8")
    return capture_id, [0, 1, 2, 3]


def test_admission_reaps_descendant_after_worker_leader_exits_on_term(tmp_path):
    ready = tmp_path / "descendant-ready"
    descendant = (
        "import pathlib,signal,sys,time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "pathlib.Path(sys.argv[1]).touch(); time.sleep(60)"
    )
    parent = (
        "import subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]); time.sleep(60)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", parent, descendant, str(ready)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if ready.exists() and production_run._process_group_exists(process.pid):
                break
            time.sleep(0.02)
        assert ready.exists()
        assert production_run._process_group_exists(process.pid)

        assert admission._terminate_admission_group(process, production_run) is True
        assert process.poll() is not None
        assert production_run._process_group_exists(process.pid) is False
    finally:
        try:
            if production_run._process_group_exists(process.pid):
                os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
        process.wait(timeout=5)


def test_worker_store_and_deadline_use_supervisor_remaining_allowance():
    from kalshi_events.common import BudgetExceeded

    @dataclass(frozen=True)
    class FakeLimits:
        max_duration_seconds: float

    class FakeStore:
        def __init__(self, _root, *, limits):
            self.limits = limits

    now = time.monotonic()
    deadline = now + 7 * 60
    checked_deadline, remaining = benchmark._admission_worker_deadline(deadline, now=now)
    deadline_store = benchmark._admission_deadline_store_type(
        FakeStore, BudgetExceeded, checked_deadline
    )
    store = deadline_store(Path("/unused"), limits=FakeLimits(1200))

    assert 0 < remaining < 20 * 60
    assert 0 < store.limits.max_duration_seconds < 7 * 60
    assert store.remaining <= remaining
    assert store.remaining < 20 * 60


def test_worker_rejects_invalid_or_out_of_range_supervisor_deadlines():
    for value in (None, True, float("nan"), 99, 100 + 20 * 60 + 1):
        with pytest.raises(ValueError):
            benchmark._admission_worker_deadline(value, now=100)


def test_real_native_worker_supervisor_completes_all_four_row_phases(tmp_path):
    operator_root = tmp_path / "operator"
    output_root = tmp_path / "supervisor-output"
    operator_root.mkdir()
    output_root.mkdir()
    fixture_root = output_root / "fixture"
    capture_id, task_indices = _write_four_row_worker_fixture(fixture_root)
    started = time.monotonic()

    child_args = dict(
        benchmark=benchmark,
        output_root=output_root,
        fixture_root=fixture_root,
        capture_id=capture_id,
        task_indices=task_indices,
        target=4,
        start=started,
        total_deadline=started + 2 * 60 * 60,
        device=tmp_path.stat().st_dev,
        operator_root=operator_root,
    )
    marker = (fixture_root / ".diagnostic-sample.json").read_bytes()
    result = admission._run_admission_child(**child_args)

    assert (fixture_root / "tmp" / "verification").is_dir()
    admission._clear_worker_outputs(fixture_root, capture_id)
    assert not (fixture_root / "tmp" / "verification").exists()
    assert (fixture_root / ".diagnostic-sample.json").read_bytes() == marker

    result = admission._run_admission_child(**child_args)

    assert result["observations"] == 4
    assert result["complete"] is False
    assert result["certified"] is False
    assert result["published"] is False
    assert result["verification_complete"] is True
    assert result["export_files"]
    assert set(result["verification"]["relations"]) == {
        "series",
        "events",
        "markets",
        "combo_legs",
        "relationships",
        "observation_history",
    }
    assert result["supervisor"]["sample_count"] > 0
    assert set(result["supervisor"]["phase_sample_coverage"]) == {
        "build",
        "export",
        "verification",
    }
    assert type(result["verification_peak_temporary_bytes"]) is int
    assert type(result["verification_peak_temporary_allocated_bytes"]) is int


def test_in_process_native_worker_uses_historical_export_config(tmp_path, monkeypatch):
    from kalshi_events import publish

    checkout = tmp_path / "checkout"
    fixture_root = checkout / "data" / "fixture"
    fixture_root.parent.mkdir(parents=True)
    capture_id, task_indices = _write_four_row_worker_fixture(fixture_root)
    report_path = checkout / "data" / "worker-report.json"
    export_settings = []
    original_export = publish._export_connection

    @contextmanager
    def record_export(*args, **kwargs):
        assert kwargs.get("historical_export") is True
        with original_export(*args, **kwargs) as connection:
            export_settings.append(
                connection.execute("select current_setting('preserve_insertion_order')").fetchone()[
                    0
                ]
            )
            yield connection

    monkeypatch.setattr(benchmark, "_checkout", lambda: checkout)
    monkeypatch.setattr(publish, "_export_connection", record_export)
    monkeypatch.setattr(benchmark, "_ADMISSION_WORKER_PROGRESS", None)
    result = benchmark._worker(
        SimpleNamespace(
            admission_worker=True,
            admission_deadline_monotonic=time.monotonic() + 20 * 60,
            worker="native",
            fixture_root=fixture_root,
            worker_report=report_path,
            capture_id=capture_id,
            task_indices=task_indices,
        )
    )

    assert result == 0
    assert export_settings == [False]
    assert report_path.is_file()


def test_supervisor_build_database_high_water_includes_coexisting_wal_and_index(tmp_path):
    worker = tmp_path / "burst_worker.py"
    worker.write_text(
        """\
import json, sys, time
from pathlib import Path
arguments = sys.argv
fixture = Path(arguments[arguments.index('--fixture-root') + 1])
report = Path(arguments[arguments.index('--worker-report') + 1])
deadline = float(arguments[arguments.index('--admission-deadline-monotonic') + 1])
phase = fixture / '.benchmark-phase'
temporary = fixture / 'tmp'
temporary.mkdir(exist_ok=True)
database = temporary / 'warehouse.duckdb'
wal = temporary / 'warehouse.duckdb.wal'
index = temporary / 'index.sqlite'
database.write_bytes(b'd' * (2 * 1024 * 1024))
wal.write_bytes(b'w' * (1024 * 1024))
index.write_bytes(b'i' * (256 * 1024))
phase.write_text('build', encoding='ascii')
time.sleep(0.65)
database.write_bytes(b'd' * 4096)
wal.unlink()
index.write_bytes(b'i' * 1024)
phase.write_text('export', encoding='ascii')
time.sleep(0.35)
phase.write_text('verification', encoding='ascii')
time.sleep(0.35)
phase.unlink()
report.write_text(json.dumps({
    'effective_deadline_kind': 'absolute_monotonic_from_supervisor',
    'effective_deadline_monotonic': deadline,
    'effective_deadline_remaining_at_worker_entry_seconds': 1100,
    'effective_store_limit_seconds': 1000,
    'complete': False,
    'certified': False,
    'published': False,
    'verification': {'complete': True},
    'verification_seconds': 0.1,
    'elapsed_seconds': 1.2,
    'build_database_component_bytes': 8192,
    'build_database_allocated_component_bytes': 4096,
    'build_phase_temporary_bytes': 0,
    'build_phase_temporary_allocated_bytes': 0,
    'export_phase_temporary_bytes': 0,
    'export_phase_temporary_allocated_bytes': 0,
    'verification_phase_temporary_bytes': 0,
    'verification_phase_temporary_allocated_bytes': 0,
    'export_bytes': 1,
    'spill_limit_enforced_by_store': True,
    'spill_directory_configured': True,
    'spill_limit_bytes': 1024,
}), encoding='utf-8')
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "supervisor-output"
    output_root.mkdir()
    fixture_root = output_root / "fixture"
    fixture_root.mkdir()
    operator_root = tmp_path / "operator"
    operator_root.mkdir()
    started = time.monotonic()
    fake_benchmark = SimpleNamespace(
        __file__=worker,
        _checkout=lambda: REPO,
        _read_json=lambda path, _limit: json.loads(Path(path).read_text(encoding="utf-8")),
    )

    result = admission._run_admission_child(
        benchmark=fake_benchmark,
        output_root=output_root,
        fixture_root=fixture_root,
        capture_id="c" * 32,
        task_indices=[0],
        target=4,
        start=started,
        total_deadline=started + 2 * 60 * 60,
        device=tmp_path.stat().st_dev,
        operator_root=operator_root,
    )

    build = result["supervisor"]["phase_high_water"]["build"]
    assert build["database_component_high_water_bytes"] >= 3 * 1024**2
    assert build["database_component_allocated_high_water_bytes"] >= 3 * 1024**2
    assert result["build_database_component_bytes"] == build["database_component_high_water_bytes"]
    assert result["build_database_component_bytes"] > 8192
    assert (
        result["build_database_allocated_component_bytes"]
        == build["database_component_allocated_high_water_bytes"]
    )
    assert result["build_database_allocated_component_bytes"] > 4096


def test_failed_worker_preserves_partial_source_and_supervisor_evidence(tmp_path):
    worker = tmp_path / "failed_worker.py"
    worker.write_text(
        """\
import json, sys, time
from pathlib import Path
arguments = sys.argv
fixture = Path(arguments[arguments.index('--fixture-root') + 1])
report = Path(arguments[arguments.index('--worker-report') + 1])
temporary = fixture / 'tmp'
temporary.mkdir(exist_ok=True)
(fixture / '.benchmark-phase').write_text('build', encoding='ascii')
(temporary / 'build.bin').write_bytes(b'b' * 1048576)
(fixture / 'diagnostic-builds' / 'capture').mkdir(parents=True)
(fixture / 'diagnostic-builds' / 'capture' / 'warehouse.duckdb').write_bytes(b'd' * 1048576)
(fixture / 'diagnostic-builds' / 'capture' / 'warehouse.duckdb.wal').write_bytes(b'w' * 524288)
(fixture / 'index.sqlite').write_bytes(b'i' * 262144)
time.sleep(0.7)
(fixture / '.benchmark-phase').write_text('export', encoding='ascii')
(temporary / 'export.bin').write_bytes(b'e' * 2097152)
time.sleep(0.7)
progress = {
    'phase': 'export',
    'source_capture_id': 'c' * 32,
    'target_observations': 250000,
    'selected_data_receipts': 2,
    'selected_control_receipts': 1,
    'source_receipt_identities': [
        {'relative': 'raw/page/manifest.json', 'request_id': 'request-1'}
    ],
    'native_observations_landed': 251234,
    'native_landing_complete': True,
    'verified_evidence_sha256': {'raw/page/manifest.json': 'a' * 64},
    'verified_evidence_count': 2,
    'export_current_relation': 'markets',
    'export_completed_relations': ['series'],
    'verification_complete': False,
    'verification_peak_temporary_bytes': None,
    'verification_peak_temporary_allocated_bytes': None,
}
report.write_text(json.dumps({
    'status': 'failed_incomplete',
    'error': 'duckdb.OutOfMemoryException: synthetic export failure',
    'worker_progress': progress,
    'observations': progress['native_observations_landed'],
    'verified_pages': progress['verified_evidence_count'],
    'complete': False,
    'certified': False,
    'published': False,
    'verification_complete': False,
    'verification_peak_temporary_bytes': None,
    'verification_peak_temporary_allocated_bytes': None,
}), encoding='utf-8')
raise SystemExit(2)
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "supervisor-output"
    output_root.mkdir()
    fixture_root = output_root / "fixture"
    (fixture_root / "tmp").mkdir(parents=True)
    operator_root = tmp_path / "operator"
    operator_root.mkdir()
    source_sample = {
        "target_observations": 250_000,
        "normalized_source_observations": 251_234,
        "selected_data_receipts": 2,
        "selected_control_receipts": 1,
        "source_profile_complete": True,
        "native_landing_complete": False,
        "verification_complete": False,
        "receipt_identities": [{"relative": "raw/page/manifest.json", "request_id": "request-1"}],
    }
    started = time.monotonic()
    fake_benchmark = SimpleNamespace(
        __file__=worker,
        _checkout=lambda: REPO,
        _read_json=lambda path, _limit: json.loads(Path(path).read_text(encoding="utf-8")),
    )

    with pytest.raises(admission.AdmissionChildFailure) as raised:
        admission._run_admission_child(
            benchmark=fake_benchmark,
            output_root=output_root,
            fixture_root=fixture_root,
            capture_id="c" * 32,
            task_indices=[0, 1, 2],
            target=250_000,
            start=started,
            total_deadline=started + 2 * 60 * 60,
            device=tmp_path.stat().st_dev,
            operator_root=operator_root,
            source_sample=source_sample,
        )

    persisted = json.loads(
        (output_root / "children" / "native-250000.json").read_text(encoding="utf-8")
    )
    partial = raised.value.partial_record
    assert persisted == partial
    assert partial["status"] == "failed_incomplete"
    assert "OutOfMemoryException" in partial["error"]
    assert partial["source_sample"] == source_sample
    assert partial["worker_progress"]["native_observations_landed"] == 251_234
    assert partial["worker_progress"]["verified_evidence_count"] == 2
    assert partial["worker_progress"]["export_current_relation"] == "markets"
    assert partial["worker_progress"]["export_completed_relations"] == ["series"]
    assert partial["supervisor"]["phase_sample_coverage"]["build"] > 0
    assert partial["supervisor"]["phase_sample_coverage"]["export"] > 0
    assert (
        partial["supervisor"]["phase_high_water"]["build"]["database_component_high_water_bytes"]
        >= 1_835_008
    )
    assert (
        partial["supervisor"]["phase_high_water"]["build"][
            "database_component_allocated_high_water_bytes"
        ]
        >= 1_835_008
    )
    assert partial["build_peak_temporary_bytes"] >= 1_048_576
    assert partial["export_peak_temporary_bytes"] >= 3_145_728
    assert partial["verification_peak_temporary_bytes"] is None
    assert partial["verification_peak_temporary_allocated_bytes"] is None
    assert partial["verification_complete"] is False
    assert partial["complete"] is False
    assert partial["certified"] is False
    assert partial["published"] is False
    assert partial["decision"]["decision"] == "NO_ADMIT"
    assert partial["supervisor"]["telemetry_complete"] is False


@pytest.mark.parametrize(
    ("stop_reason", "expected_error"),
    [
        ("deadline", "deadline"),
        ("artifact_limit", "diagnostic ceiling"),
        ("free_space_floor", "150 GiB floor"),
    ],
)
def test_supervisor_stop_persists_partial_snapshot(
    tmp_path, monkeypatch, stop_reason, expected_error
):
    worker = tmp_path / "long_worker.py"
    worker.write_text(
        """\
import sys, time
from pathlib import Path
arguments = sys.argv
fixture = Path(arguments[arguments.index('--fixture-root') + 1])
phase = fixture / '.benchmark-phase'
phase.write_text('build', encoding='ascii')
(fixture / 'tmp' / 'build.bin').write_bytes(b'b' * 1048576)
while True:
    time.sleep(1)
""",
        encoding="utf-8",
    )
    fixture_root = tmp_path / "fixture"
    (fixture_root / "tmp").mkdir(parents=True)
    output_root = tmp_path / "supervisor-output"
    output_root.mkdir()
    operator_root = tmp_path / "operator"
    operator_root.mkdir()
    phase_path = fixture_root / ".benchmark-phase"
    real_measure = admission._measure_tree
    real_monotonic = time.monotonic
    real_sleep = time.sleep

    def measured_tree(path, *, device):
        if Path(path) in (output_root, fixture_root / "tmp"):
            wait_until = real_monotonic() + 5
            while (
                not phase_path.is_file() or not (fixture_root / "tmp" / "build.bin").is_file()
            ) and real_monotonic() < wait_until:
                real_sleep(0.01)
        result = real_measure(path, device=device)
        if stop_reason == "artifact_limit" and Path(path) == output_root:
            result["logical_bytes"] = 16 * 1024**3 + 1
        return result

    monkeypatch.setattr(admission, "_measure_tree", measured_tree)
    if stop_reason == "free_space_floor":
        monkeypatch.setattr(
            admission.shutil,
            "disk_usage",
            lambda _path: SimpleNamespace(free=149 * 1024**3),
        )
    if stop_reason == "deadline":
        clock = {"value": real_monotonic(), "calls": 0}

        def simulated_monotonic():
            clock["calls"] += 1
            if clock["calls"] == 1:
                return clock["value"]
            if clock["calls"] == 2:
                return clock["value"] + 1
            return clock["value"]

        def jump_past_deadline(_seconds):
            clock["value"] += 1201
            real_sleep(0.25)

        monkeypatch.setattr(
            admission,
            "time",
            SimpleNamespace(monotonic=simulated_monotonic, sleep=jump_past_deadline),
        )

    source_sample = {
        "target_observations": 250_000,
        "normalized_source_observations": 251_234,
        "selected_data_receipts": 2,
        "selected_control_receipts": 1,
        "native_landing_complete": False,
        "verification_complete": False,
        "receipt_identities": [{"relative": "raw/page/manifest.json", "request_id": "request-1"}],
    }
    started = real_monotonic()
    fake_benchmark = SimpleNamespace(
        __file__=worker,
        _checkout=lambda: REPO,
        _read_json=lambda path, _limit: json.loads(Path(path).read_text(encoding="utf-8")),
    )

    with pytest.raises((TimeoutError, RuntimeError)) as raised:
        admission._run_admission_child(
            benchmark=fake_benchmark,
            output_root=output_root,
            fixture_root=fixture_root,
            capture_id="c" * 32,
            task_indices=[0, 1, 2],
            target=250_000,
            start=started,
            total_deadline=started + 2 * 60 * 60,
            device=tmp_path.stat().st_dev,
            operator_root=operator_root,
            source_sample=source_sample,
        )

    partial = raised.value.partial_record
    persisted = json.loads(
        (output_root / "children" / "native-250000.json").read_text(encoding="utf-8")
    )
    assert persisted == partial
    assert expected_error in partial["primary_error"]
    assert partial["source_sample"] == source_sample
    assert partial["supervisor"]["sample_count"] >= 1
    assert partial["supervisor"]["phase_sample_coverage"]["build"] >= 1
    assert partial["build_peak_temporary_bytes"] >= 1_048_576
    assert partial["verification_peak_temporary_bytes"] is None
    assert partial["verification_peak_temporary_allocated_bytes"] is None
    assert partial["worker_report_error"] is not None
    assert partial["complete"] is False
    assert partial["certified"] is False
    assert partial["published"] is False
    assert partial["decision"]["decision"] == "NO_ADMIT"


def test_admission_worker_exception_report_keeps_live_progress(tmp_path, monkeypatch):
    report_path = tmp_path / "worker-report.json"
    progress = {
        "phase": "export",
        "native_observations_landed": 251_234,
        "native_landing_complete": True,
        "verified_evidence_count": 6,
        "export_current_relation": "markets",
        "export_completed_relations": ["series"],
        "verification_complete": False,
        "verification_peak_temporary_bytes": None,
        "verification_peak_temporary_allocated_bytes": None,
    }
    args = SimpleNamespace(
        worker="native",
        admission_check=False,
        admission_worker=True,
        admission_deadline_monotonic=100.0,
        fixture_root=tmp_path / "fixture",
        worker_report=report_path,
    )

    class FakeParser:
        def parse_args(self):
            return args

    def fail_worker(_args):
        raise RuntimeError("duckdb.OutOfMemoryException: synthetic export failure")

    monkeypatch.setattr(benchmark, "_parser", FakeParser)
    monkeypatch.setattr(
        benchmark, "_worker_paths", lambda fixture, report, _checkout: (fixture, report)
    )
    monkeypatch.setattr(benchmark, "_worker", fail_worker)
    monkeypatch.setattr(benchmark, "_ADMISSION_WORKER_PROGRESS", progress)

    assert benchmark.main() == 2
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed_incomplete"
    assert "OutOfMemoryException" in report["error"]
    assert report["worker_progress"] == progress
    assert report["observations"] == 251_234
    assert report["verified_pages"] == 6
    assert report["verification_peak_temporary_bytes"] is None
    assert report["verification_peak_temporary_allocated_bytes"] is None
    assert report["complete"] is False
    assert report["certified"] is False
    assert report["published"] is False


def test_parent_sigterm_reaps_admission_worker_and_preserves_cancel_report(tmp_path):
    worker = tmp_path / "worker.py"
    worker.write_text(
        """\
import os, signal, subprocess, sys, time
from pathlib import Path
tmp = Path(os.environ["TMPDIR"])
report = Path(sys.argv[sys.argv.index("--worker-report") + 1])
report.with_name("worker.pid").write_text(str(os.getpid()))
(tmp / "partial.bin").write_bytes(b"partial diagnostic evidence")
desc = (
    "import os,pathlib,signal,sys,time; "
    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
    "pathlib.Path(sys.argv[2]).touch(); time.sleep(60)"
)
subprocess.Popen([
    sys.executable, "-c", desc, str(tmp / "descendant.pid"), str(tmp / "descendant-ready")
])
deadline = time.monotonic() + 5
while not (tmp / "descendant-ready").exists() and time.monotonic() < deadline:
    time.sleep(0.01)
print("worker-started", flush=True)
while True:
    time.sleep(1)
""",
        encoding="utf-8",
    )
    driver = tmp_path / "driver.py"
    driver.write_text(
        "import contextlib,sys,time\n"
        f"sys.path[:0] = [{str(REPO / 'scripts')!r}, {str(REPO / 'src')!r}]\n"
        "from pathlib import Path\n"
        "from types import SimpleNamespace\n"
        "import historical_admission as admission, production_run\n"
        "import kalshi_events.common as common, kalshi_events.paths as paths\n"
        "base=Path(sys.argv[1]); worker=Path(sys.argv[2]); operator=base/'operator'; operator.mkdir()\n"
        "output=base/'output'\n"
        "common.safe_path=lambda value: Path(value).absolute()\n"
        "paths.persistent_path=lambda value, **kwargs: Path(value).absolute()\n"
        "@contextlib.contextmanager\n"
        "def writer_lock(_root): yield ('fake-lock', 1)\n"
        "production_run._operator_writer_lock=writer_lock\n"
        "def overlap(left,right):\n"
        "    left=Path(left).absolute(); right=Path(right).absolute()\n"
        "    return left.is_relative_to(right) or right.is_relative_to(left)\n"
        "fake_benchmark=SimpleNamespace(__file__=worker, _checkout=lambda: Path("
        + repr(str(REPO))
        + "), _paths_overlap=overlap, ReadOnlyStore=lambda root, **kwargs: SimpleNamespace(root=root))\n"
        "def preflight(operator_root, output_root, runner, device):\n"
        "    fixture=output_root/'fixture'; (fixture/'tmp').mkdir(parents=True)\n"
        "    return admission._run_admission_child(benchmark=fake_benchmark, output_root=output_root, fixture_root=fixture, capture_id='a'*32, task_indices=[], target=250000, start=time.monotonic(), total_deadline=time.monotonic()+7200, device=device, operator_root=operator_root)\n"
        "admission._preflight=preflight\n"
        "args=SimpleNamespace(operator_root=operator, output_dir=output, population_report=base/'pop.json', receipt_index=base/'index.json', stride_report=base/'stride.json', old_stress_report=base/'stress.json', capture_id='a'*32)\n"
        "raise SystemExit(admission.run(args, fake_benchmark))\n",
        encoding="utf-8",
    )

    parent = subprocess.Popen(
        [sys.executable, str(driver), str(tmp_path), str(worker)],
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        ready = tmp_path / "output" / "tmp" / "descendant-ready"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not ready.exists():
            if parent.poll() is not None:
                break
            time.sleep(0.02)
        assert ready.exists(), (
            parent.stdout.read() if parent.poll() is not None else "worker did not start"
        )
        worker_pid = int((tmp_path / "output" / "children" / "worker.pid").read_text())
        descendant_pid = int((tmp_path / "output" / "tmp" / "descendant.pid").read_text())

        os.kill(parent.pid, signal.SIGTERM)
        assert parent.wait(timeout=10) == 2
        assert production_run._process_group_exists(worker_pid) is False
        assert production_run._process_group_exists(descendant_pid) is False

        report = __import__("json").loads(
            (tmp_path / "output" / "report.json").read_text(encoding="utf-8")
        )
        assert report["status"] == "cancelled"
        assert report["complete"] is False
        assert report["decision"]["decision"] == "NO_ADMIT"
        assert (tmp_path / "output" / "tmp" / "partial.bin").read_bytes() == (
            b"partial diagnostic evidence"
        )
        log = tmp_path / "output" / "children" / "native-250000.log"
        assert log.is_file()
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
