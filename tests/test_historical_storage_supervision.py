"""Focused tests for offline storage supervision and stale-worker recovery."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from kalshi_events import capture, common, incremental, warehouse

RUNNER = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("storage_supervision_runner", RUNNER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _workspace(tmp_path: Path, production, *, external_size: int = 0):
    directory = tmp_path / "production"
    operator = directory / "operator"
    run_root = directory / "runs" / ("d" * 32)
    logs = run_root / "logs"
    operator.mkdir(parents=True)
    logs.mkdir(parents=True)
    capture_id = "a" * 32
    root_id = "b" * 32
    (operator / "root-id.json").write_text(
        json.dumps({"version": 1, "root_id": root_id}), encoding="utf-8"
    )
    (operator / "root.json").write_text(
        json.dumps({"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}),
        encoding="utf-8",
    )
    production._atomic_report(
        directory / production.OWNER_MARKER, {"version": 1, "root_id": root_id}
    )
    report_path = run_root / production.REPORT_NAME
    production._atomic_report(report_path, {"capture_id": capture_id, "root_id": root_id})
    report = {
        "capture_id": capture_id,
        "root_id": root_id,
        "storage": {
            "operator_retained_bytes": 0,
            "operator_temporary_bytes": 0,
            "operator_database_bytes": 0,
            "external_bytes": external_size,
            "aggregate_estimate_bytes": external_size,
        },
    }
    external_paths = {
        "session_report": report_path,
        "stdout": logs / "refresh.stdout.json",
        "stderr": logs / "refresh.stderr.log",
    }
    progress = run_root / f"storage-{'e' * 32}.json"
    external_paths["storage_progress"] = progress
    return directory, operator, run_root, logs, report_path, report, external_paths, progress


def test_offline_workflow_reserves_twelfth_but_preserves_both_grant_clocks(production, monkeypatch):
    original = {
        "started_epoch": 1_000.0,
        "deadline_epoch": 87_400.0,
        "started_monotonic": 2_000.0,
        "deadline_monotonic": 88_400.0,
    }
    window = dict(original)
    assert production._offline_workflow_reserve(window) == 7_200
    monkeypatch.setattr(production.time, "time", lambda: 1_001.0)
    monkeypatch.setattr(production.time, "monotonic", lambda: 2_001.0)
    monkeypatch.setattr(production, "_ACTIVE_OFFLINE_HISTORICAL_WINDOW", window)
    workflow = production._monitor_deadline({}, workflow=True)
    standalone = production._monitor_deadline({}, workflow=False)
    assert workflow == window["deadline_monotonic"] - 7_200
    assert standalone == window["deadline_monotonic"]
    assert window == original


def test_live_storage_stops_on_a_single_sample_above_database_threshold(production, tmp_path):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    state = production._begin_storage_supervision(directory, operator, report, progress, external)
    database = operator / "tmp" / f"build-historical-{report['capture_id']}" / "warehouse.duckdb"
    database.parent.mkdir(parents=True)
    with database.open("wb") as stream:
        stream.truncate(int(production.RUN_LIMITS["storage_database_stop_bytes"]) + 1)
    reason = production._sample_live_storage(directory, operator, report, state, progress, external)
    assert reason == "live database storage reached the 56 GiB stop threshold"
    assert state["current"]["operator_database_bytes"] == (
        int(production.RUN_LIMITS["storage_database_stop_bytes"]) + 1
    )
    assert state["target_interval_seconds"] == 1
    assert (
        state["current"]["operator_live_workspace_allocated_bytes"]
        < state["current"]["operator_database_bytes"]
    )


def test_live_storage_stops_on_temporary_burst_above_category_threshold(production, tmp_path):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    state = production._begin_storage_supervision(directory, operator, report, progress, external)
    spool = operator / "tmp" / f"publish-{'c' * 32}" / "archive.tmp"
    spool.parent.mkdir(parents=True)
    with spool.open("wb") as stream:
        stream.truncate(int(production.RUN_LIMITS["storage_temporary_stop_bytes"]) + 1)
    reason = production._sample_live_storage(directory, operator, report, state, progress, external)
    assert reason == "live temporary storage reached the 56 GiB stop threshold"


@pytest.mark.parametrize(
    ("category", "stop_key", "workspace", "filename", "threshold_gib"),
    [
        ("database", "storage_database_stop_bytes", "build-historical", "warehouse.duckdb", 112),
        ("temporary", "storage_temporary_stop_bytes", "publish", "spill.bin", 168),
    ],
)
def test_expanded_storage_stops_at_exact_threshold(
    production, tmp_path, category, stop_key, workspace, filename, threshold_gib
):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    token = production._ACTIVE_LIMITS.set(production._limits_for_profile("expanded"))
    try:
        state = production._begin_storage_supervision(
            directory, operator, report, progress, external
        )
        capture_id = report["capture_id"]
        if category == "database":
            target = operator / "tmp" / f"{workspace}-{capture_id}" / filename
        else:
            target = operator / "tmp" / f"{workspace}-{capture_id}" / filename
        target.parent.mkdir(parents=True)
        with target.open("wb") as stream:
            stream.truncate(int(production._limits_for_profile("expanded")[stop_key]))
        reason = production._sample_live_storage(
            directory, operator, report, state, progress, external
        )
        assert reason == f"live {category} storage reached the {threshold_gib} GiB stop threshold"
    finally:
        production._ACTIVE_LIMITS.reset(token)
    assert (
        production._effective_limits()["database_bytes"] == production.RUN_LIMITS["database_bytes"]
    )


@pytest.mark.parametrize(
    ("category", "admission_bytes", "stop_bytes", "stop_gib"),
    [
        ("database", 96 * 1024**3, 112 * 1024**3, 112),
        ("temporary", 144 * 1024**3, 168 * 1024**3, 168),
    ],
)
def test_expanded_storage_baseline_uses_stop_threshold_not_forecast_admission(
    production, tmp_path, category, admission_bytes, stop_bytes, stop_gib
):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    field = f"operator_{category}_bytes"
    token = production._ACTIVE_LIMITS.set(production._limits_for_profile("expanded"))
    try:
        report["storage"][field] = admission_bytes + 1
        production._begin_storage_supervision(directory, operator, report, progress, external)
        progress.unlink()
        report["storage"][field] = stop_bytes
        with pytest.raises(
            production.RunnerError,
            match=f"verified {category} storage baseline reaches the {stop_gib} GiB stop threshold",
        ):
            production._begin_storage_supervision(directory, operator, report, progress, external)
    finally:
        production._ACTIVE_LIMITS.reset(token)


@pytest.mark.parametrize("category", ["database", "temporary"])
def test_default_storage_baseline_keeps_strict_56_gib_stop(production, tmp_path, category):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    field = f"operator_{category}_bytes"
    threshold = production.RUN_LIMITS[f"storage_{category}_stop_bytes"]
    report["storage"][field] = threshold
    production._begin_storage_supervision(directory, operator, report, progress, external)
    progress.unlink()
    report["storage"][field] = threshold + 1

    with pytest.raises(
        production.RunnerError,
        match=f"verified {category} storage baseline reaches the 56 GiB stop threshold",
    ):
        production._begin_storage_supervision(directory, operator, report, progress, external)


def test_live_storage_fails_closed_on_unknown_or_symlink_tmp_entries(production, tmp_path):
    directory, operator, _run_root, _logs, _report_path, _report, external, _progress = _workspace(
        tmp_path, production
    )
    tmp = operator / "tmp"
    tmp.mkdir()
    (tmp / "unknown-output").mkdir()
    with pytest.raises(production.RunnerError, match="unknown live temporary workspace"):
        production._live_workspace_snapshot(operator, directory, "a" * 32, "b" * 32)

    (tmp / "unknown-output").rmdir()
    (tmp / f"build-historical-{'a' * 32}").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(production.RunnerError):
        production._live_workspace_snapshot(operator, directory, "a" * 32, "b" * 32)

    (tmp / f"build-historical-{'a' * 32}").unlink()
    (directory / "unexpected-output").mkdir()
    with pytest.raises(production.RunnerError, match="unknown production-root workspace"):
        production._live_external_files(external, directory=directory)


def test_live_inventory_counts_operator_atomic_pointer_temp(production, tmp_path):
    directory, operator, *_rest = _workspace(tmp_path, production)
    pointer = operator / "current-historical.json"
    pointer.write_text("{}", encoding="utf-8")
    temporary = operator / ".write-AbC123_x"
    temporary.write_bytes(b"publisher temp")
    snapshot = production._live_workspace_snapshot(operator, directory, "a" * 32, "b" * 32)
    assert snapshot["temporary_bytes"] == temporary.stat().st_size
    assert snapshot["logical_bytes"] == temporary.stat().st_size + pointer.stat().st_size

    tmp_atomic = operator / "tmp" / ".write-AbC123_x"
    tmp_atomic.parent.mkdir()
    tmp_atomic.write_bytes(b"native atomic temporary")
    snapshot = production._live_workspace_snapshot(operator, directory, "a" * 32, "b" * 32)
    assert snapshot["temporary_bytes"] == tmp_atomic.stat().st_size + temporary.stat().st_size


def test_activation_temporary_is_inventoried_and_deduplicated_with_source(production, tmp_path):
    directory, operator, *_rest = _workspace(tmp_path, production)
    capture_id = "a" * 32
    source = operator / "builds-historical" / capture_id / "warehouse.duckdb"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"certified database")
    workspace = production._live_workspace_snapshot(operator, directory, capture_id, "b" * 32)
    temporary = directory / f".production.duckdb.{'f' * 32}.tmp"
    temporary.hardlink_to(source)
    external = {"session_report": directory / "runs" / ("d" * 32) / "session.json"}
    files = production._live_external_files(
        external, directory=directory, seen=workspace["allocated_inodes"]
    )
    assert files[f"activation:{temporary.name}"] == 0


def test_reconciliation_flag_requires_historical_capture_mode(production, tmp_path):
    with pytest.raises(SystemExit) as raised:
        production.main(
            [
                "--directory",
                str(tmp_path / "unused"),
                "--capture-id",
                "a" * 32,
                "--reconcile-stopped-worker",
            ]
        )
    assert raised.value.code == 2


def test_initial_historical_preflight_finishes_validated_retirement_before_storage_snapshot(
    production, monkeypatch, tmp_path
):
    events = []
    root = tmp_path / "operator"
    root.mkdir()
    (root / "raw").mkdir()
    capture_id = "a" * 32
    root_id = "b" * 32

    class FakeStore:
        def __init__(self, store_root, _limits):
            self.root = store_root
            self.locked = False

        @contextmanager
        def lock(self):
            events.append("lock-enter")
            self.locked = True
            try:
                yield self
            finally:
                self.locked = False
                events.append("lock-exit")

        @property
        def writer_locked_by_current_thread(self):
            return self.locked

        def path(self, relative):
            return self.root / relative

        def usage(self):
            events.append("storage-snapshot")
            return {"retained_bytes": 10, "temporary_bytes": 2, "database_bytes": 3}

    monkeypatch.setattr(common, "Store", FakeStore)
    monkeypatch.setattr(
        capture,
        "read_capture",
        lambda _store, requested: {
            "capture_id": requested,
            "selected": None,
            "incremental": False,
            "acquisition_protocol": 5,
            "max_response_bytes": int(production.RUN_LIMITS["response_bytes"]),
        },
    )
    monkeypatch.setattr(
        production,
        "_validate_initial_historical_accounting_session",
        lambda *_args, **_kwargs: events.append("session-identity-validated"),
    )

    def finish_retirement(store):
        assert store.writer_locked_by_current_thread
        events.append("finish-pending-retirement")

    monkeypatch.setattr(incremental, "finish_pending_retirement", finish_retirement)
    monkeypatch.setattr(
        warehouse,
        "recover_and_cleanup_abandoned_historical_stages",
        lambda _store, _capture: events.append("historical-stage-cleanup"),
    )
    monkeypatch.setattr(production, "_root_id", lambda _store: root_id)

    result = production._read_accounting_worker(
        root,
        capture_id,
        cleanup_historical_stages=True,
        expected_directory=tmp_path / "production",
        report_path=tmp_path / "production" / "runs" / ("d" * 32) / "session.json",
        expected_root_id=root_id,
        expected_revision="c" * 40,
    )

    assert events.index("session-identity-validated") < events.index("finish-pending-retirement")
    assert events.index("finish-pending-retirement") < events.index("historical-stage-cleanup")
    assert events.index("historical-stage-cleanup") < events.index("storage-snapshot")
    assert events[-1] == "lock-exit"
    assert result["storage"] == {
        "retained_bytes": 10,
        "temporary_bytes": 2,
        "database_bytes": 3,
    }


def test_live_storage_fails_closed_when_progress_or_external_file_is_missing_or_unsafe(
    production, tmp_path
):
    directory, operator, _run_root, _logs, _report_path, report, external, progress = _workspace(
        tmp_path, production
    )
    state = production._begin_storage_supervision(directory, operator, report, progress, external)
    progress.unlink()
    with pytest.raises(production.RunnerError, match="storage progress report is missing"):
        production._sample_live_storage(directory, operator, report, state, progress, external)

    progress.write_text("{}", encoding="utf-8")
    progress.unlink()
    target = _logs / "unsafe-target"
    target.write_text("not the expected log", encoding="utf-8")
    external["stderr"].symlink_to(target)
    with pytest.raises(production.RunnerError, match="live external artifact is unsafe"):
        production._sample_live_storage(directory, operator, report, state, progress, external)


def _stopped_session(tmp_path: Path, production, monkeypatch):
    capture_id, root_id, session_id = "a" * 32, "b" * 32, "d" * 32
    child_group, supervisor_pid = 987_654_321, 987_654_322
    directory = tmp_path / "production"
    operator = directory / "operator"
    run_root = directory / "runs" / session_id
    source_runtime = tmp_path / "runtime-repo" / "data" / "runtime" / "run-1"
    source_root = source_runtime / "source"
    source_root.mkdir(parents=True)
    (operator / "captures").mkdir(parents=True)
    (operator / "root-id.json").write_text(
        json.dumps({"version": 1, "root_id": root_id}), encoding="utf-8"
    )
    (operator / "root.json").write_text(
        json.dumps({"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}),
        encoding="utf-8",
    )
    (operator / "writer.lock").write_bytes(b"")
    directory.mkdir(exist_ok=True)
    production._atomic_report(
        directory / production.OWNER_MARKER, {"version": 1, "root_id": root_id}
    )
    (run_root / "logs").mkdir(parents=True)
    report_path = run_root / production.REPORT_NAME
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": "historical",
        "source_root": str(source_root),
        "revision": "c" * 40,
        "capture_id": capture_id,
        "root_id": root_id,
        "boot_stamp": "saved-boot",
        "phase": "stopped",
        "active_process_group": child_group,
        "production_state": "not_attempted",
        "error": "storage limit stop",
        "requests": 17,
        "downloaded_bytes": 1234,
        "offline_historical_window": {"root_id": root_id},
    }
    production._atomic_report(report_path, report)
    script = source_root / "scripts" / "production_run.py"
    script.parent.mkdir()
    script.write_text("# saved worker command", encoding="utf-8")
    stdout = source_runtime / "logs" / "worker.stdout.log"
    stdout.parent.mkdir()
    stdout.write_bytes(b"")
    started_at = "2026-10-30T12:00:00+00:00"
    launch = {
        "session": str(report_path),
        "capture_id": capture_id,
        "revision": report["revision"],
        "pid": supervisor_pid,
        "pgid": supervisor_pid,
        "offline_only": True,
        "automatic_retry": False,
        "command": [
            sys.executable,
            str(script),
            "--historical",
            "--capture-id",
            capture_id,
            "--directory",
            str(directory),
        ],
        "process_identity": f"{supervisor_pid} {supervisor_pid} saved-worker",
        "stdout": str(stdout),
        "started_at": started_at,
    }
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    evidence = {
        "capture_id": capture_id,
        "revision": report["revision"],
        "old_active_child_group": child_group,
        "supervisor_pid": supervisor_pid,
        "signal": "SIGTERM to exact original supervisor only",
        "original_pids_absent": True,
        "owned_group_members": [],
        "production_exists": False,
        "automatic_retry": False,
        "session_phase": "stopped",
        "session_error": report["error"],
        "database_limit_bytes": int(production.RUN_LIMITS["database_bytes"]),
        "observed_database_bytes": int(production.RUN_LIMITS["database_bytes"]) + 1,
        "network_activity_this_invocation": {
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
        "session_before_sha256": "e" * 64,
        "supervisor_process_started_utc": started_at,
    }
    (source_runtime / "logs" / "storage-safety-stop.json").write_text(
        json.dumps(evidence), encoding="utf-8"
    )
    monkeypatch.setattr(production, "SOURCE_ROOT", tmp_path / "runtime-repo")
    monkeypatch.setattr(production, "_boot_stamp", lambda: "saved-boot")
    monkeypatch.setattr(
        production,
        "_validate_offline_historical_window",
        lambda *_args, **_kwargs: {"root_id": root_id},
    )
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *_args, **_kwargs: (operator.stat().st_dev, operator.stat().st_ino),
    )
    return directory, operator, report_path, report, child_group, supervisor_pid


def test_stopped_worker_reconciliation_audits_then_clears_only_stale_group(
    production, tmp_path, monkeypatch
):
    directory, operator, report_path, original, child_group, _supervisor_pid = _stopped_session(
        tmp_path, production, monkeypatch
    )
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    result = production._reconcile_stopped_worker(directory, original["capture_id"])
    saved = production._strict_json(report_path)
    audit_path = operator / "stopped-worker-reconciliation.jsonl"
    audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
    assert result["active_process_group"] is None
    assert saved["error"] == original["error"]
    assert saved["requests"] == original["requests"]
    assert saved["downloaded_bytes"] == original["downloaded_bytes"]
    assert saved["offline_historical_window"] == original["offline_historical_window"]
    assert audit["stale_process_group"] == child_group


def test_stopped_worker_reconciliation_uses_saved_expanded_database_limit(
    production, tmp_path, monkeypatch
):
    directory, operator, report_path, original, _child_group, _supervisor_pid = _stopped_session(
        tmp_path, production, monkeypatch
    )
    production._bind_report_resource_profile(original, "expanded", transition=True)
    report_path.write_text(json.dumps(original), encoding="utf-8")
    runtime_root = production.SOURCE_ROOT / "data" / "runtime" / "run-1"
    evidence_path = runtime_root / "logs" / "storage-safety-stop.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence["database_limit_bytes"] = 128 * 1024**3
    evidence["observed_database_bytes"] = 128 * 1024**3 + 1
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    result = production._reconcile_stopped_worker(directory, original["capture_id"])

    assert result["active_process_group"] is None
    assert result["resource_profile"] == "expanded"
    assert result["error"] == original["error"]
    assert (operator / "stopped-worker-reconciliation.jsonl").is_file()


def test_stopped_worker_reconciliation_rejects_reused_child_pid_in_another_group(
    production, tmp_path, monkeypatch
):
    directory, operator, report_path, original, child_group, _supervisor_pid = _stopped_session(
        tmp_path, production, monkeypatch
    )
    monkeypatch.setattr(production, "_pid_exists", lambda pid: pid == child_group)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    with pytest.raises(production.RunnerError, match="still alive or ambiguous"):
        production._reconcile_stopped_worker(directory, original["capture_id"])
    assert production._strict_json(report_path)["active_process_group"] == child_group
    assert not (operator / "stopped-worker-reconciliation.jsonl").exists()


def test_process_supervision_keeps_primary_reason_separate_from_reap_status(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    directory.mkdir()
    operator = directory / "operator"
    operator.mkdir()
    report_path = directory / "session.json"
    report_path.write_text("{}", encoding="utf-8")
    report = {}
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            production.RunnerError("bounded memory sample unavailable")
        ),
    )
    try:
        returncode, reason, reaped = production._monitor(
            child,
            directory,
            operator,
            report,
            report_path,
            time.monotonic() + 30,
        )
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    assert returncode is not None
    assert reaped is True
    assert reason == "resource accounting failed: memory snapshot unavailable"
    assert report["process_supervision"]["primary_stop_reason"] == reason
    assert report["process_supervision"]["leader_reaped"] is True
    assert report["process_supervision"]["group_absent"] is True


def test_no_child_start_does_not_reuse_old_storage_supervision_state(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    operator = directory / "operator"
    report_path = directory / "runs" / ("d" * 32) / "session.json"
    logs = report_path.parent / "logs"
    logs.mkdir(parents=True)
    operator.mkdir(parents=True)
    report = {"process_supervision": {"primary_stop_reason": "prior stop"}, "phase_seconds": {}}
    paths = {
        "directory": directory,
        "report": report_path,
        "logs": logs,
        "storage_progress": report_path.parent / f"storage-{'e' * 32}.json",
        "metrics_progress": report_path.parent / f"metrics-{'f' * 32}.json.progress.json",
    }
    production._atomic_report(report_path, report)
    monkeypatch.setattr(production, "_invoke", lambda *_args, **_kwargs: (None, "deadline", True))
    result = production._run_external_child(
        ["unused"],
        directory=directory,
        operator=operator,
        paths=paths,
        report=report,
        deadline=time.monotonic() + 10,
        label="refresh",
        storage_supervision=True,
    )
    assert result[:3] == (None, "deadline", True)
    assert report["process_supervision"]["primary_stop_reason"] == "prior stop"
    assert "storage_supervision" not in report


def test_started_child_with_missing_fresh_storage_report_fails_closed(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    operator = directory / "operator"
    report_path = directory / "runs" / ("d" * 32) / "session.json"
    logs = report_path.parent / "logs"
    logs.mkdir(parents=True)
    operator.mkdir(parents=True)
    report = {"phase_seconds": {}}
    paths = {
        "directory": directory,
        "report": report_path,
        "logs": logs,
        "storage_progress": report_path.parent / f"storage-{'e' * 32}.json",
        "metrics_progress": report_path.parent / f"metrics-{'f' * 32}.json.progress.json",
    }
    production._atomic_report(report_path, report)

    def started_without_report(_command, **kwargs):
        report["storage_supervision_invocation"] = kwargs["storage_invocation_token"]
        return 0, None, True

    monkeypatch.setattr(production, "_invoke", started_without_report)
    with pytest.raises(
        production.RunnerError, match="storage report disappeared after child start"
    ):
        production._run_external_child(
            ["unused"],
            directory=directory,
            operator=operator,
            paths=paths,
            report=report,
            deadline=time.monotonic() + 10,
            label="refresh",
            storage_supervision=True,
        )


def test_monitor_records_cleanup_errors_without_replacing_primary_reason(
    production, tmp_path, monkeypatch
):
    class Child:
        pid = 12345

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("unreaped test child", timeout)

    child = Child()
    report = {}
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            production.RunnerError("bounded memory sample unavailable")
        ),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _group: (_ for _ in ()).throw(PermissionError("probe denied")),
    )

    def terminate(process):
        process._production_termination_outcome = {
            "leader_reaped": False,
            "group_absent": None,
            "uncertain": True,
            "termination_errors": ["SIGTERM: permission denied"],
        }
        return False

    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_atomic_report", lambda *_args: None)
    _code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        time.monotonic() + 30,
    )
    assert reaped is False
    assert reason == "resource accounting failed: memory snapshot unavailable"
    assert report["process_supervision"]["primary_stop_reason"] == reason
    assert report["process_supervision"]["termination_errors"] == [
        "SIGTERM: permission denied",
        "process-group probe: probe denied",
        "process-group probe: probe denied",
    ]
