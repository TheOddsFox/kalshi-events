"""Adversarial boundaries for the production memory guard."""

from __future__ import annotations

import importlib.util
import json
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_run_memory_safety_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sample(group: int | None, *, pressure_at: float = 100.0) -> dict[str, object]:
    return {
        "child_process_group_rss_bytes": None if group is None else 0,
        "system_compressed_bytes": 0,
        "system_compressor_occupied_bytes": 0,
        "system_memory_pressure": "normal",
        "system_memory_pressure_observed_monotonic": pressure_at,
        "child_process_group_physical_footprint_bytes": None if group is None else 0,
        "supervisor_physical_footprint_bytes": 0,
    }


def _modern_memory_stop_session(tmp_path, production, monkeypatch, *, keep_legacy_aliases=False):
    from test_historical_storage_supervision import _stopped_session

    directory, operator, report_path, report, worker_group, supervisor_pid = _stopped_session(
        tmp_path, production, monkeypatch
    )
    token = "f" * 32
    stop_reason = "resource accounting failed: memory snapshot unavailable"
    report.update(
        {
            "error": stop_reason,
            "network_activity_this_invocation": {
                "requests": 0,
                "downloaded_bytes": 0,
                "accounting_complete": True,
            },
            "process_supervision": {
                "leader_reaped": True,
                "group_absent": None,
                "uncertain": True,
                "termination_attempted": True,
                "termination_errors": ["process-group probe: operation not permitted"],
                "primary_stop_reason": stop_reason,
                "invocation_token": token,
            },
            "storage_supervision_invocation": token,
        }
    )
    production._atomic_report(report_path, report)

    source_root = Path(report["source_root"])
    launch_path = source_root.parent / "latest-launch.json"
    launch = json.loads(launch_path.read_text(encoding="utf-8"))
    launch.update(
        {
            "root_id": report["root_id"],
            "launched_at_utc": "2026-10-30T12:00:00+00:00",
            "process_identity_utc": (
                f"{supervisor_pid} {supervisor_pid} Fri Oct 30 12:00:00 2026 "
                + " ".join(launch["command"])
            ),
        }
    )
    if not keep_legacy_aliases:
        launch.pop("process_identity", None)
        launch.pop("started_at", None)
    launch_path.write_text(json.dumps(launch), encoding="utf-8")
    evidence_path = source_root.parent / "logs" / "storage-safety-stop.json"
    evidence_path.unlink()
    return (
        directory,
        operator,
        report_path,
        report,
        worker_group,
        supervisor_pid,
        launch_path,
        evidence_path,
        token,
    )


@pytest.mark.parametrize("observed_at", [True, -1.0, float("inf"), 101.0])
def test_sample_memory_rejects_non_monotonic_or_future_pressure_timestamps(
    production, monkeypatch, observed_at
):
    payload = _sample(123, pressure_at=observed_at)

    class Worker:
        returncode = 0

        def communicate(self, *, timeout):
            assert timeout == 4.5
            return json.dumps(payload, allow_nan=True), ""

    monkeypatch.setattr(production.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Worker())

    with pytest.raises(production.RunnerError, match="timestamp"):
        production._sample_memory(123)


def test_memory_worker_reports_probe_family_status_and_bounded_sanitized_stderr(
    production, monkeypatch, capsys
):
    def failed_probe(_group):
        raise subprocess.CalledProcessError(
            7,
            ["/usr/sbin/sysctl", "-n", "sensitive payload"],
            stderr="operation not permitted\n/private/tmp/secret " + "x" * 600,
        )

    monkeypatch.setattr(production, "_memory_sample_worker", failed_probe)

    assert production._memory_sample_worker_main(123) == 1

    error = json.loads(capsys.readouterr().out)["error"]
    assert error.startswith("sysctl probe exited with status 7; stderr: operation not permitted")
    assert "/usr/sbin/sysctl" not in error
    assert "sensitive payload" not in error
    assert "/private/tmp/secret" not in error
    assert len(error) <= 512


def test_sample_memory_keeps_sanitized_stderr_when_worker_json_is_malformed(
    production, monkeypatch
):
    class Worker:
        returncode = 0

        def communicate(self, *, timeout):
            assert timeout == 4.5
            return "not JSON", "permission denied at /private/tmp/secret"

    monkeypatch.setattr(production.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Worker())

    with pytest.raises(production.RunnerError) as failure:
        production._sample_memory(123)

    assert "malformed JSON" in str(failure.value)
    assert "worker stderr: permission denied at <path>" in str(failure.value)
    assert "/private/tmp/secret" not in str(failure.value)
    assert "caused by JSONDecodeError" in production._memory_sample_error(failure.value)


def test_sample_memory_caps_worker_timeout_and_reaps_it_after_timeout(production, monkeypatch):
    clock = [100.0]
    timeouts = []
    killed = []

    class Worker:
        pid = 456
        returncode = None

        def communicate(self, *, timeout):
            timeouts.append(timeout)
            if len(timeouts) == 1:
                clock[0] += timeout
                raise subprocess.TimeoutExpired("memory-worker", timeout)
            return "", ""

    def start_worker(*_args, **kwargs):
        assert kwargs["start_new_session"] is True
        return Worker()

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "Popen", start_worker)
    monkeypatch.setattr(
        production.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )

    with pytest.raises(production.RunnerError, match="timed out"):
        production._sample_memory(123, timeout=8.0)

    assert timeouts == [4.5, 0.5]
    assert sum(timeouts) == 5.0
    assert killed == [(456, signal.SIGKILL)]


def test_modern_memory_stop_reconciles_without_a_storage_stop_file(
    production, tmp_path, monkeypatch
):
    (
        directory,
        _operator,
        report_path,
        original,
        worker_group,
        supervisor_pid,
        _launch_path,
        evidence_path,
        token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch)
    assert not evidence_path.exists()
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    result = production._reconcile_stopped_worker(directory, original["capture_id"])

    assert result["active_process_group"] is None
    assert result["error"] == "resource accounting failed: memory snapshot unavailable"
    assert result["process_supervision"]["uncertain"] is True
    assert result["process_supervision"]["invocation_token"] == token
    assert result["requests"] == 17 and result["downloaded_bytes"] == 1234
    audit_path = directory / "operator" / "stopped-worker-reconciliation.jsonl"
    audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    assert audit["supervisor_pid"] == supervisor_pid
    assert audit["stale_process_group"] == worker_group
    assert len(audit["launch_sha256"]) == len(audit["report_sha256"]) == 64
    assert production._strict_json(report_path)["active_process_group"] is None


@pytest.mark.parametrize(
    "binding",
    [
        "launch_root",
        "launch_capture",
        "launch_revision",
        "launch_session",
        "launch_command",
        "launch_time",
        "process_identity",
        "identity_weekday",
        "report_root",
        "report_revision",
        "report_boot",
        "primary_reason",
        "termination_outcome",
        "invocation_token",
        "network",
    ],
)
def test_modern_memory_stop_rejects_wrong_bindings(production, tmp_path, monkeypatch, binding):
    (
        directory,
        operator,
        report_path,
        report,
        _worker_group,
        _supervisor_pid,
        launch_path,
        _evidence_path,
        _token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch)
    launch = json.loads(launch_path.read_text(encoding="utf-8"))
    if binding == "launch_root":
        launch["root_id"] = "e" * 32
    elif binding == "launch_capture":
        launch["capture_id"] = "e" * 32
    elif binding == "launch_revision":
        launch["revision"] = "d" * 40
    elif binding == "launch_session":
        launch["session"] = str(report_path.with_name("other-session.json"))
    elif binding == "launch_command":
        launch["command"][launch["command"].index("--directory") + 1] = str(
            tmp_path / "other-production"
        )
    elif binding == "launch_time":
        launch["launched_at_utc"] = "2026-10-30T12:00:01+00:00"
    elif binding == "process_identity":
        launch["process_identity_utc"] = launch["process_identity_utc"].replace(
            "Fri Oct 30 12:00:00 2026", "Fri Oct 30 12:00:01 2026"
        )
    elif binding == "identity_weekday":
        launch["process_identity_utc"] = launch["process_identity_utc"].replace(
            "Fri Oct 30 12:00:00 2026", "Thu Oct 30 12:00:00 2026"
        )
    elif binding == "report_root":
        report["root_id"] = "e" * 32
    elif binding == "report_revision":
        report["revision"] = "d" * 40
    elif binding == "report_boot":
        report["boot_stamp"] = "another-boot"
    elif binding == "primary_reason":
        report["process_supervision"]["primary_stop_reason"] = "another stop"
    elif binding == "termination_outcome":
        report["process_supervision"]["uncertain"] = False
    elif binding == "invocation_token":
        report["process_supervision"]["invocation_token"] = "0" * 32
    else:
        report["network_activity_this_invocation"]["requests"] = 1
    launch_path.write_text(json.dumps(launch), encoding="utf-8")
    production._atomic_report(report_path, report)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    with pytest.raises(production.RunnerError):
        production._reconcile_stopped_worker(directory, "a" * 32)

    assert production._strict_json(report_path)["active_process_group"] == 987_654_321
    assert not (operator / "stopped-worker-reconciliation.jsonl").exists()


@pytest.mark.parametrize("alias", ["process_identity", "started_at"])
def test_modern_memory_stop_rejects_conflicting_legacy_launch_aliases(
    production, tmp_path, monkeypatch, alias
):
    (
        directory,
        _operator,
        _report_path,
        original,
        _worker_group,
        _supervisor_pid,
        launch_path,
        evidence_path,
        _token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch, keep_legacy_aliases=True)
    assert not evidence_path.exists()
    launch = json.loads(launch_path.read_text(encoding="utf-8"))
    if alias == "process_identity":
        launch[alias] = "999 999 different process"
    else:
        launch[alias] = "2026-10-30T12:00:01+00:00"
    launch_path.write_text(json.dumps(launch), encoding="utf-8")
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    with pytest.raises(production.RunnerError, match="legacy latest-launch"):
        production._reconcile_stopped_worker(directory, original["capture_id"])


@pytest.mark.parametrize("probe", ["live_pid", "live_group", "permission_error"])
def test_modern_memory_stop_fails_closed_on_live_or_inaccessible_process_probes(
    production, tmp_path, monkeypatch, probe
):
    (
        directory,
        operator,
        report_path,
        original,
        worker_group,
        supervisor_pid,
        _launch_path,
        _evidence_path,
        _token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch)
    if probe == "live_pid":
        monkeypatch.setattr(production, "_pid_exists", lambda pid: pid == worker_group)
        monkeypatch.setattr(production, "_process_group_exists", lambda _pgid: False)
    elif probe == "live_group":
        monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
        monkeypatch.setattr(production, "_process_group_exists", lambda pgid: pgid == worker_group)
    else:
        monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)

        def permission_error(pgid):
            if pgid == supervisor_pid:
                raise PermissionError("probe denied")
            return False

        monkeypatch.setattr(production, "_process_group_exists", permission_error)

    with pytest.raises(production.RunnerError):
        production._reconcile_stopped_worker(directory, original["capture_id"])

    assert production._strict_json(report_path)["active_process_group"] == worker_group
    assert not (operator / "stopped-worker-reconciliation.jsonl").exists()


def test_modern_memory_stop_rechecks_process_absence_under_writer_lock(
    production, tmp_path, monkeypatch
):
    (
        directory,
        operator,
        report_path,
        original,
        worker_group,
        _supervisor_pid,
        _launch_path,
        _evidence_path,
        _token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch)
    worker_probes = 0

    def pid_exists(pid):
        nonlocal worker_probes
        if pid == worker_group:
            worker_probes += 1
            return worker_probes == 2
        return False

    monkeypatch.setattr(production, "_pid_exists", pid_exists)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pgid: False)

    with pytest.raises(production.RunnerError, match="still alive or ambiguous"):
        production._reconcile_stopped_worker(directory, original["capture_id"])

    assert worker_probes == 2
    assert production._strict_json(report_path)["active_process_group"] == worker_group
    assert not (operator / "stopped-worker-reconciliation.jsonl").exists()


@pytest.mark.parametrize("input_file", ["launch", "report"])
def test_modern_memory_stop_rechecks_hashed_inputs_under_both_locks(
    production, tmp_path, monkeypatch, input_file
):
    (
        directory,
        operator,
        report_path,
        original,
        _worker_group,
        _supervisor_pid,
        launch_path,
        _evidence_path,
        _token,
    ) = _modern_memory_stop_session(tmp_path, production, monkeypatch)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    def mutate_under_writer_lock(*_args, **_kwargs):
        if input_file == "launch":
            launch = json.loads(launch_path.read_text(encoding="utf-8"))
            launch["additional_test_field"] = True
            launch_path.write_text(json.dumps(launch), encoding="utf-8")
        else:
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["additional_test_field"] = True
            production._atomic_report(report_path, report)
        return operator.stat().st_dev, operator.stat().st_ino

    monkeypatch.setattr(production, "_validate_offline_renewal_root", mutate_under_writer_lock)

    with pytest.raises(production.RunnerError, match="changed during"):
        production._reconcile_stopped_worker(directory, original["capture_id"])

    assert not (operator / "stopped-worker-reconciliation.jsonl").exists()
    assert (
        production._strict_json(report_path)["active_process_group"]
        == original["active_process_group"]
    )


def test_stopped_worker_audit_allows_a_distinct_verified_attempt_in_one_session(
    production, tmp_path
):
    operator = tmp_path / "operator"
    operator.mkdir()
    first = {
        "version": 1,
        "event_id": "a" * 64,
        "reconciled_at": "2026-11-01T00:00:00Z",
        "session_id": "session",
        "supervisor_pid": 101,
        "stale_process_group": 201,
    }
    second = {
        **first,
        "event_id": "b" * 64,
        "supervisor_pid": 102,
        "stale_process_group": 202,
        "invocation_token": "c" * 32,
    }

    assert production._append_stopped_worker_audit(operator, first) == first
    assert production._append_stopped_worker_audit(operator, first) == first
    with pytest.raises(production.RunnerError, match="event conflicts with its prior record"):
        production._append_stopped_worker_audit(
            operator, {**first, "primary_stop_reason": "changed"}
        )
    with pytest.raises(production.RunnerError, match="different reconciliation audit"):
        production._append_stopped_worker_audit(operator, second)
    assert (
        production._append_stopped_worker_audit(operator, second, allow_distinct_attempts=True)
        == second
    )
    conflict = {**second, "event_id": "d" * 64, "stale_process_group": 201}
    with pytest.raises(production.RunnerError, match="different reconciliation audit"):
        production._append_stopped_worker_audit(operator, conflict, allow_distinct_attempts=True)
    reused_token = {
        **second,
        "event_id": "e" * 64,
        "supervisor_pid": 103,
        "stale_process_group": 203,
    }
    with pytest.raises(production.RunnerError, match="different reconciliation audit"):
        production._append_stopped_worker_audit(
            operator, reused_token, allow_distinct_attempts=True
        )
    rows = (operator / "stopped-worker-reconciliation.jsonl").read_text().splitlines()
    assert [json.loads(row) for row in rows] == [first, second]


def test_warning_gap_at_22_5_seconds_remains_continuous(production, tmp_path, monkeypatch):
    clock = [100.0]
    sample_count = 0

    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return -15

    child = Child()
    terminated = []

    def sample(group, *, timeout):
        nonlocal sample_count
        sample_count += 1
        if sample_count == 2:
            clock[0] += 5.0
        value = _sample(group, pressure_at=clock[0])
        value["system_memory_pressure"] = "warn"
        return value

    def sleep(delay):
        clock[0] += delay
        if clock[0] == 115.0:
            clock[0] += 2.5

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.time, "sleep", sleep)
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", lambda _child: terminated.append(True) or True)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_args: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_args: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1000 * 1024**3),
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        child, tmp_path, tmp_path / "operator", report, tmp_path / "session.json", 500
    )

    assert code == -15 and reaped
    assert reason == "system memory pressure remained warning for 60 seconds"
    assert sample_count == 5
    assert report["resource_samples"][-1]["system_memory_pressure_observed_monotonic"] == 162.5
    assert terminated == [True]


def test_monitor_keeps_specific_sampler_failure_but_preserves_generic_stop_and_clears_on_valid_sample(
    production, tmp_path, monkeypatch
):
    clock = [100.0]
    invalid = _sample(123, pressure_at=100.0)
    invalid["system_memory_pressure"] = "unknown"
    valid = _sample(123, pressure_at=100.0)

    class Worker:
        returncode = 0

        def __init__(self, payload):
            self.payload = payload

        def communicate(self, *, timeout):
            return json.dumps(self.payload), ""

    workers = iter((Worker(invalid), Worker(valid)))
    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay)
    )
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: next(workers))
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_args: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_args: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )

    class Child:
        pid = 123

        def __init__(self):
            self.returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return self.returncode

    first = Child()
    terminated = []

    def terminate(child):
        child.returncode = -15
        terminated.append(child)
        return True

    monkeypatch.setattr(production, "_terminate", terminate)
    report = {
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "previous_stop": {"memory_sample_error": "prior bounded detail"},
        "stop_history": [{"memory_sample_error": "prior bounded detail"}],
    }

    code, reason, reaped = production._monitor(
        first, tmp_path, tmp_path / "operator", report, tmp_path / "session.json", 500
    )

    assert code == -15 and reaped
    assert reason == "resource accounting failed: memory snapshot unavailable"
    assert reason in production.RECOVERABLE_ACCOUNTING_STOP_REASONS
    assert report["process_supervision"]["primary_stop_reason"] == reason
    assert report["memory_sample_error"] == "bounded memory sample has unknown pressure state"
    assert report["resource_samples"] == []
    assert terminated == [first]

    second = Child()

    def exit_after_sleep(delay):
        clock[0] += delay
        second.returncode = 0

    monkeypatch.setattr(production.time, "sleep", exit_after_sleep)
    code, reason, reaped = production._monitor(
        second, tmp_path, tmp_path / "operator", report, tmp_path / "session.json", 500
    )

    assert code == 0 and reason is None and reaped
    assert "memory_sample_error" not in report
    assert report["previous_stop"]["memory_sample_error"] == "prior bounded detail"
    assert report["stop_history"] == [{"memory_sample_error": "prior bounded detail"}]
    assert len(report["resource_samples"]) == 1


def test_validated_resume_moves_memory_diagnostic_into_stop_history(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    directory.mkdir()
    capture_id = "a" * 32
    root_id = "b" * 32
    monkeypatch.setattr(
        production,
        "_system_info",
        lambda path: {
            "filesystem_device": path.stat().st_dev,
            "filesystem_volume_uuid": "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        },
    )
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: root_id)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "current-boot")
    monkeypatch.setattr(production, "_revision", lambda: "c" * 40)

    report_path, report, _paths = production._session_report(directory, "full", capture_id)
    diagnostic = "bounded memory sample failed: sysctl probe exited with status 7"
    report.update(
        {
            "root_id": root_id,
            "phase": "stopped",
            "error": "resource accounting failed: memory snapshot unavailable",
            "recovery": "preserve the prior capture",
            "memory_sample_error": diagnostic,
        }
    )
    production._atomic_report(report_path, report)

    _path, resumed, _paths = production._load_resume(directory, capture_id)

    assert "memory_sample_error" not in resumed
    assert resumed["previous_stop"]["memory_sample_error"] == diagnostic
    assert resumed["stop_history"][-1]["memory_sample_error"] == diagnostic
