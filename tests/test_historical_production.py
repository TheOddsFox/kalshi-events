"""Historical production work stays offline and reports its narrower coverage."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import FakeClient

from kalshi_events import capture, publish, transport, warehouse
from kalshi_events.common import CatalogueError, Store

RUNNER = Path(__file__).parents[1] / "scripts" / "production_run.py"
ACTIVATION = Path(__file__).parents[1] / "scripts" / "production_activation.py"
CAPTURE_ID = "a" * 32
RELEASE_ID = "b" * 64
RESERVED_BYTES = 384 * 1024**2 + 64 * 1024


@pytest.fixture(autouse=True)
def loopback_api(monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("historical_production_run", RUNNER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def production_activation():
    spec = importlib.util.spec_from_file_location("historical_production_activation", ACTIVATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def historical_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("historical-source") / "operator"
    store = Store(root)
    state = capture.capture(store, client_factory=FakeClient)
    assert state["complete"] is True
    manifest = publish.publish(store, state["capture_id"], catalogue_version=3)
    return root, manifest


@pytest.fixture
def historical_candidate(historical_root, tmp_path):
    source, manifest = historical_root
    directory = tmp_path / "production"
    root = directory / "operator"
    directory.mkdir()
    shutil.copytree(source, root)
    return (
        directory,
        root,
        root / warehouse.build_directory(manifest["capture_id"], 3) / "warehouse.duckdb",
        manifest,
    )


def _snapshot():
    reservations = [
        {
            "capture_id": CAPTURE_ID,
            "request_id": f"{index:064x}",
            "reserved_bytes": 1024,
            "status": None,
            "accounting_complete": False,
            "acquisition_protocol": 5,
            "version": 1,
        }
        for index in range(4)
    ]
    return {
        "attempts": 12,
        "downloaded_or_reserved_bytes": 4 * RESERVED_BYTES,
        "accounting_complete": False,
        "unfinished_reservations": reservations,
        "storage": {
            "retained_bytes": 1,
            "temporary_bytes": 0,
            "database_bytes": 1,
        },
        "root_id": "c" * 32,
    }


def _historical_report(directory: Path, operator_root: Path, production) -> tuple[Path, dict]:
    run_id = "d" * 32
    run_root = directory / "runs" / run_id
    (run_root / "logs").mkdir(parents=True)
    now_epoch = time.time()
    now_mono = time.monotonic()
    snapshot = _snapshot()
    store = Store(operator_root)
    with store.lock():
        from kalshi_events.incremental import root_id

        snapshot["root_id"] = root_id(store, create=True)
    report = {
        "version": 1,
        "session_id": run_id,
        "mode": "full",
        "started_epoch": now_epoch - 10,
        "deadline_epoch": now_epoch + production.RUN_LIMITS["session_seconds"] - 10,
        "started_monotonic": now_mono - 10,
        "deadline_monotonic": now_mono + production.RUN_LIMITS["session_seconds"] - 10,
        "time_budget_started_epoch": now_epoch - 10,
        "time_budget_started_monotonic": now_mono - 10,
        "last_observed_epoch": now_epoch,
        "boot_stamp": "same boot",
        "system": {"filesystem_device": directory.stat().st_dev},
        "capture_id": CAPTURE_ID,
        "root_id": snapshot["root_id"],
        "capture_complete": False,
        "publication_state": "unknown",
        "production_state": "not_attempted",
        "accounting_complete": False,
        "requests": snapshot["attempts"],
        "downloaded_bytes": None,
        "downloaded_bytes_upper_bound": snapshot["downloaded_or_reserved_bytes"],
        "accounting_basis": "reserved-upper-bound",
        "accounting_bound_verified": True,
        "unfinished_reservation_ids": sorted(
            item["request_id"] for item in snapshot["unfinished_reservations"]
        ),
        "phase": "stopped",
        "active_process_group": None,
        "phase_seconds": {},
    }
    owner = directory / production.OWNER_MARKER
    production._atomic_report(owner, {"version": 1, "root_id": report["root_id"]})
    report_path = run_root / production.REPORT_NAME
    production._atomic_report(report_path, report)
    return report_path, report


def test_historical_flag_selects_offline_build_or_version_three_activation(production):
    assert production.validate_mode(False, CAPTURE_ID, False) == "resume"
    assert production.validate_mode(False, CAPTURE_ID, False, historical=True) == "historical"
    assert production.validate_mode(False, None, True, historical=True) == "activate-historical"
    assert (
        production.validate_mode(
            False,
            CAPTURE_ID,
            False,
            historical=True,
            offline_duration_seconds=production.MAX_OFFLINE_HISTORICAL_SECONDS,
        )
        == "historical"
    )
    with pytest.raises(production.RunnerError):
        production.validate_mode(True, None, False, historical=True)
    with pytest.raises(production.RunnerError, match="--historical --capture-id"):
        production.validate_mode(False, CAPTURE_ID, False, offline_duration_seconds=1)
    for duration in (0, -1, float("inf"), float("nan"), 14_401):
        with pytest.raises(production.RunnerError):
            production.validate_mode(
                False,
                CAPTURE_ID,
                False,
                historical=True,
                offline_duration_seconds=duration,
            )


def test_historical_resume_uses_existing_report_and_does_not_extend_its_window(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        root_id(store, create=True)
    report_path, initial = _historical_report(directory, operator, production)
    initial["system"]["filesystem_device"] = directory.stat().st_dev
    production._atomic_report(report_path, initial)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "same boot")
    deadlines = (
        initial["time_budget_started_epoch"],
        initial["deadline_epoch"],
        initial["time_budget_started_monotonic"],
        initial["deadline_monotonic"],
    )

    loaded_path, loaded, _paths = production._load_resume(directory, CAPTURE_ID, historical=True)

    assert loaded_path == report_path
    assert loaded["accounting_complete"] is False
    assert loaded["accounting_basis"] == "reserved-upper-bound"
    assert loaded["requests"] == initial["requests"]
    assert loaded["downloaded_bytes_upper_bound"] == initial["downloaded_bytes_upper_bound"]
    assert deadlines == (
        loaded["time_budget_started_epoch"],
        loaded["deadline_epoch"],
        loaded["time_budget_started_monotonic"],
        loaded["deadline_monotonic"],
    )
    with pytest.raises(production.RunnerError):
        production._load_resume(directory, CAPTURE_ID)
    expired = dict(loaded)
    expired["deadline_epoch"] = time.time() - 1
    expired["deadline_monotonic"] = time.monotonic() - 1
    production._atomic_report(report_path, expired)
    with pytest.raises(production.RunnerError, match="48-hour session expired"):
        production._load_resume(directory, CAPTURE_ID, historical=True)


@pytest.mark.parametrize("offline_window", [False, True])
def test_offline_historical_work_accepts_scoped_completion_without_faking_capture_completion(
    production, tmp_path, monkeypatch, offline_window
):
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        root_id(store, create=True)
    report_path, report = _historical_report(directory, operator, production)
    report["system"]["filesystem_device"] = directory.stat().st_dev
    original_deadlines = {
        key: report[key]
        for key in (
            "started_epoch",
            "deadline_epoch",
            "started_monotonic",
            "deadline_monotonic",
            "time_budget_started_epoch",
            "time_budget_started_monotonic",
        )
    }
    if offline_window:
        past_epoch = time.time() - 50 * 3600
        past_monotonic = time.monotonic() - 50 * 3600
        report["started_epoch"] = past_epoch
        report["time_budget_started_epoch"] = past_epoch
        report["deadline_epoch"] = past_epoch + production.RUN_LIMITS["session_seconds"]
        report["started_monotonic"] = past_monotonic
        report["time_budget_started_monotonic"] = past_monotonic
        report["deadline_monotonic"] = past_monotonic + production.RUN_LIMITS["session_seconds"]
        original_deadlines.update({key: report[key] for key in original_deadlines})
    production._atomic_report(report_path, report)
    snapshot = _snapshot()
    snapshot["root_id"] = report["root_id"]

    class Awake:
        pid = 424242

        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self):
            return None

    monkeypatch.setattr(
        production, "_package_versions", lambda: production.SUPPORTED_RUNTIME_VERSIONS
    )
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(
        production, "_system_info", lambda path: {"filesystem_device": path.stat().st_dev}
    )
    monkeypatch.setattr(production, "_boot_stamp", lambda: "same boot")
    monkeypatch.setattr(production, "_validate_runtime_versions", lambda _versions: None)
    monkeypatch.setattr(production.subprocess, "Popen", Awake)
    monkeypatch.setattr(production, "_kill_awake_guard", lambda _awake: None)
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(
            free=1000 * 1024**3, total=1200 * 1024**3, used=200 * 1024**3
        ),
    )
    monkeypatch.setattr(production, "_resource_guard", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(
        transport,
        "source_url",
        lambda: pytest.fail("offline historical mode inspected the API origin"),
    )
    events = []

    def run_child(command, *, paths, label, deadline, report, **_kwargs):
        events.append((label, command))
        if offline_window:
            active = production._ACTIVE_OFFLINE_HISTORICAL_WINDOW
            assert active is not None
            assert deadline <= active["deadline_monotonic"]
            wall_deadline = production._child_wall_deadline_epoch(
                report, deadline, workflow_deadline=label == "refresh"
            )
            assert wall_deadline <= active["deadline_epoch"]
        stdout = paths["logs"] / f"{label}.stdout.json"
        stderr = paths["logs"] / f"{label}.stderr.log"
        stderr.touch()
        if label == "refresh":
            assert command[-3] == "historical"
            assert command[-2] == CAPTURE_ID
            if offline_window:
                limits = json.loads(command[-1])
                assert 0 < limits["max-duration-seconds"] <= 3600
            value = {
                "complete": True,
                "capture_id": CAPTURE_ID,
                "capture_complete": False,
                "source_capture_complete": False,
                "scope_complete": True,
                "coverage_scope": "historical",
                "publication_state": "verified",
                "accounting_complete": False,
                "requests": None,
                "downloaded_bytes": None,
                "resumable": None,
                "output": str(tmp_path / "historical-release"),
                "source_release_id": RELEASE_ID,
                "error": None,
            }
            code = 0
        else:
            assert label == "activation"
            assert command[-1] == f"historical:{RELEASE_ID}"
            value = {
                "state": "verified",
                "capture_id": CAPTURE_ID,
                "coverage_scope": "historical",
                "source_capture_complete": False,
                "scope_complete": True,
                "catalogue_coverage": {"scope": "historical"},
            }
            code = 0
        stdout.write_text(json.dumps(value))
        return code, None, True, stdout, stderr

    monkeypatch.setattr(production, "_run_external_child", run_child)

    def verify_release(*args, **kwargs):
        if offline_window:
            deadline = kwargs.get("deadline", args[-1])
            assert deadline <= production._ACTIVE_OFFLINE_HISTORICAL_WINDOW["deadline_monotonic"]
        return {
            "source_release_id": RELEASE_ID,
            "coverage_scope": "historical",
            "source_capture_complete": False,
        }

    monkeypatch.setattr(production, "_release_verify", verify_release)

    result, code = production.run_production(
        directory,
        capture_id=CAPTURE_ID,
        historical=True,
        offline_duration_seconds=3600 if offline_window else None,
    )

    assert code == 0, result.get("error")
    assert [label for label, _command in events] == ["refresh", "activation"]
    assert result["capture_complete"] is False
    assert result["source_capture_complete"] is False
    assert result["scope_complete"] is True
    assert result["coverage_scope"] == "historical"
    assert result["accounting_complete"] is False
    assert result["requests"] == snapshot["attempts"]
    assert result["downloaded_bytes"] is None
    assert result["downloaded_bytes_upper_bound"] == snapshot["downloaded_or_reserved_bytes"]
    assert result["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }
    assert {key: result[key] for key in original_deadlines} == original_deadlines
    assert result.get("session_extension_history") is None
    if offline_window:
        window = result["offline_historical_window"]
        assert window["capture_id"] == CAPTURE_ID
        assert window["root_id"] == result["root_id"]
        assert window["revision"] == "e" * 40
        assert 3599 < window["deadline_epoch"] - window["started_epoch"] <= 3600
        assert [label for label, _command in events] == ["refresh", "activation"]

        # Repeating the flag reuses the established deadline, even if it asks
        # for a longer duration. It cannot grant a second offline window.
        saved = production._strict_json(report_path)
        saved["production_state"] = "not_attempted"
        saved["phase"] = "stopped"
        production._atomic_report(report_path, saved)
        loaded_path, loaded, _paths = production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=7200,
        )
        reused = production._configure_offline_historical_window(loaded, CAPTURE_ID, 7200)
        assert loaded_path == report_path
        assert reused["deadline_epoch"] == window["deadline_epoch"]
        assert reused["deadline_monotonic"] == window["deadline_monotonic"]

        saved["offline_historical_window"]["started_epoch"] -= 7200
        saved["offline_historical_window"]["deadline_epoch"] -= 7200
        saved["offline_historical_window"]["started_monotonic"] -= 7200
        saved["offline_historical_window"]["deadline_monotonic"] -= 7200
        production._atomic_report(report_path, saved)
        with pytest.raises(production.RunnerError, match="offline historical window expired"):
            production._load_resume(
                directory,
                CAPTURE_ID,
                historical=True,
                offline_duration_seconds=3600,
            )


def test_historical_refresh_worker_does_not_read_api_or_call_capture(
    production, tmp_path, monkeypatch, capsys
):
    operator = tmp_path / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        logical_root = root_id(store, create=True)
    production._atomic_report(
        tmp_path / production.OWNER_MARKER,
        {"version": 1, "root_id": logical_root},
    )
    report = tmp_path / "run.json"
    report.write_text("{}")
    metrics = tmp_path / "metrics.json"
    notification = tmp_path / "capture-notification.json"
    limits = {
        "max-rate": 5,
        "max-requests": 100,
        "max-download-bytes": 1024**3,
        "max-response-bytes": 384 * 1024**2,
        "max-decoded-response-bytes": 768 * 1024**2,
        "max-duration-seconds": 3600,
        "max-root-bytes": 350 * 1024**3,
        "max-temp-bytes": 64 * 1024**3,
        "max-database-bytes": 64 * 1024**3,
        "compression": "gzip",
        "rate-policy": "decrease-on-429",
    }
    called = {}

    def historical_run(_store, **kwargs):
        called.update(kwargs)
        return (
            {
                "complete": True,
                "capture_id": CAPTURE_ID,
                "capture_complete": False,
                "source_capture_complete": False,
                "scope_complete": True,
                "coverage_scope": "historical",
                "publication_state": "verified",
                "accounting_complete": False,
            },
            0,
        )

    from kalshi_events import refresh

    monkeypatch.setattr(refresh, "run", historical_run)
    monkeypatch.setattr(transport, "source_url", lambda: pytest.fail("API origin read"))
    monkeypatch.setattr(capture, "capture", lambda *_args, **_kwargs: pytest.fail("capture called"))

    code = production._refresh_worker_main(
        operator,
        report,
        metrics,
        notification,
        "historical",
        CAPTURE_ID,
        json.dumps(limits),
    )

    assert code == 0
    assert called["historical"] is True
    assert called["capture_id"] == CAPTURE_ID
    assert capsys.readouterr().out
    assert not notification.exists()


def test_historical_activation_reads_coverage_and_uses_build_three(
    production_activation, historical_candidate
):
    directory, root, warehouse_path, manifest = historical_candidate

    result = production_activation.activate_current(
        root,
        directory,
        expected_release_id=manifest["source_release_id"],
        catalogue_version=3,
    )

    assert result["coverage_scope"] == "historical"
    assert result["source_capture_complete"] is True
    assert result["scope_complete"] is True
    assert result["catalogue_coverage"]["scope"] == "historical"
    assert result["catalogue_coverage"]["source_capture_id"] == manifest["capture_id"]
    assert (directory / "production.duckdb").stat().st_ino == warehouse_path.stat().st_ino


def test_historical_activation_rejects_v2_selector_wal_and_changed_build_hash(
    production_activation, historical_candidate
):
    directory, root, warehouse_path, _manifest = historical_candidate
    with pytest.raises((CatalogueError, OSError)):
        production_activation.activate_current(root, directory, catalogue_version=2)
    assert not (directory / "production.duckdb").exists()

    wal = warehouse_path.with_name(warehouse_path.name + ".wal")
    wal.write_bytes(b"uncheckpointed")
    with pytest.raises(CatalogueError):
        production_activation.activate_current(root, directory, catalogue_version=3)
    wal.unlink()

    with warehouse_path.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(CatalogueError):
        production_activation.activate_current(root, directory, catalogue_version=3)
    assert not (directory / "production.duckdb").exists()


def test_historical_release_cannot_be_a_v2_incremental_baseline(historical_candidate):
    _directory, root, _warehouse_path, _manifest = historical_candidate
    store = Store(root)
    with store.lock():
        from kalshi_events.incremental import root_id, verify_pin

        identity = root_id(store)
        with pytest.raises(CatalogueError, match="catalogue version 1 or 2"):
            verify_pin(
                store,
                {
                    "baseline": {
                        "catalogue_version": 3,
                        "root_id": identity,
                    }
                },
            )
