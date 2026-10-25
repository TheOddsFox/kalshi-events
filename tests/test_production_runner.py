"""Production session boundaries must fail before starting acquisition."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_run", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("full", "capture_id", "activate_current"),
    [(True, None, False), (False, "a" * 32, False), (False, None, True)],
)
def test_exactly_one_production_mode_is_accepted(production, full, capture_id, activate_current):
    production.validate_mode(full, capture_id, activate_current)


@pytest.mark.parametrize(
    ("full", "capture_id", "activate_current"),
    [
        (False, None, False),
        (True, "a" * 32, False),
        (True, None, True),
        (False, "a" * 32, True),
        (True, "a" * 32, True),
    ],
)
def test_missing_or_conflicting_modes_are_rejected(production, full, capture_id, activate_current):
    with pytest.raises((ValueError, RuntimeError)):
        production.validate_mode(full, capture_id, activate_current)


@pytest.mark.parametrize("capture_id", ["", "a" * 31, "a" * 33, "g" * 32, "../outside", True])
def test_resume_requires_a_native_capture_id(production, capture_id):
    with pytest.raises((ValueError, RuntimeError)):
        production.validate_mode(False, capture_id, False)


def test_rss_stop_requires_a_continuous_minute_strictly_over_the_limit(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded(
        [(instant, limit + 1) for instant in (0, 15, 30, 45, 59.999)]
    )
    assert production.rss_limit_exceeded([(instant, limit + 1) for instant in (0, 15, 30, 45, 60)])
    assert not production.rss_limit_exceeded([(instant, limit) for instant in (0, 15, 30, 45, 60)])
    assert not production.rss_limit_exceeded([])


def test_under_limit_rss_sample_resets_the_continuous_stop_window(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded(
        [(0, limit + 1), (15, limit + 1), (30, limit)]
        + [(instant, limit + 1) for instant in (45, 60, 75, 90)]
    )
    assert production.rss_limit_exceeded(
        [(0, limit + 1), (15, limit + 1), (30, limit)]
        + [(instant, limit + 1) for instant in (45, 60, 75, 90, 105)]
    )


def test_missed_rss_samples_do_not_establish_a_continuous_minute(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded([(0, limit + 1), (60, limit + 1)])


def test_resume_subtracts_trustworthy_prior_acquisition_from_aggregate_limits(production):
    limits = production.RUN_LIMITS
    prior = {"attempts": 123, "downloaded_bytes": 456, "accounting_complete": True}
    remaining = production.remaining_allowances(prior, 100, now=160)
    assert remaining["max-requests"] == limits["attempts"] - 123
    assert remaining["max-download-bytes"] == limits["downloaded_bytes"] - 456
    assert remaining["max-duration-seconds"] <= limits["workflow_seconds"] - 60
    assert remaining["max-rate"] == 5
    assert remaining["compression"] == "gzip"
    assert remaining["rate-policy"] == "decrease-on-429"
    assert remaining["max-response-bytes"] == 384 * 1024**2
    assert remaining["max-decoded-response-bytes"] == 768 * 1024**2


@pytest.mark.parametrize(
    "prior",
    [
        {"attempts": None, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": None, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": False},
        {"attempts": -1, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": True, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": float("nan"), "accounting_complete": True},
    ],
)
def test_unknown_or_malformed_prior_accounting_never_becomes_zero(production, prior):
    with pytest.raises((ValueError, RuntimeError)):
        production.remaining_allowances(prior, 100, now=100)


@pytest.mark.parametrize("exhausted", ["attempts", "downloaded_bytes", "time"])
def test_exhausted_budget_stops_before_starting_another_workflow(production, exhausted):
    prior = {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": True}
    now = 100
    if exhausted == "time":
        now += production.RUN_LIMITS["workflow_seconds"]
    else:
        prior[exhausted] = production.RUN_LIMITS[exhausted]
    with pytest.raises((ValueError, RuntimeError)):
        production.remaining_allowances(prior, 100, now=now)


def test_response_reservation_is_clamped_to_remaining_download_budget(production):
    remaining = production.remaining_allowances(
        {
            "attempts": 0,
            "downloaded_bytes": production.RUN_LIMITS["downloaded_bytes"] - 100,
            "accounting_complete": True,
        },
        100,
        now=100,
    )
    assert remaining["max-download-bytes"] == 100
    assert 0 < remaining["max-response-bytes"] <= 100


def _live_process(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="],
        capture_output=True,
        text=True,
        check=False,
        timeout=2,
    )
    return result.returncode == 0 and not result.stdout.strip().startswith("Z")


def test_termination_reaps_leader_and_kills_a_term_ignoring_descendant(production, tmp_path):
    ready = tmp_path / "ready.json"
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import json,os,signal,sys,time; "
            "pid=os.fork(); "
            "signal.signal(signal.SIGTERM,signal.SIG_IGN if pid==0 else signal.SIG_DFL); "
            "open(sys.argv[1],'w').write(json.dumps({'pid':os.getpid()})) if pid==0 else None; "
            "time.sleep(60)",
            str(ready),
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    descendant = None
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if ready.exists():
                try:
                    descendant = json.loads(ready.read_text())["pid"]
                except (ValueError, KeyError):
                    pass
                else:
                    break
            time.sleep(0.01)
        assert descendant is not None, "descendant failed to initialize"
        assert _live_process(descendant)
        assert production._terminate(child) is True
        deadline = time.monotonic() + 2
        while _live_process(descendant) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not _live_process(descendant), "leader exit left a networking-capable child alive"
        assert child.poll() is not None
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=3)


def test_child_startup_failure_does_not_proceed_to_monitoring(production, tmp_path, monkeypatch):
    def fail_start(_command, **kwargs):
        assert kwargs["shell"] is False
        assert kwargs["start_new_session"] is True
        raise OSError("injected child startup failure")

    monkeypatch.setattr(production.subprocess, "Popen", fail_start)
    monkeypatch.setattr(
        production, "_monitor", lambda *_a, **_kw: pytest.fail("monitor ran without a child")
    )
    with pytest.raises(OSError, match="child startup failure"):
        production._invoke(
            [sys.executable, "-c", "pass"],
            directory=tmp_path,
            operator=tmp_path / "operator",
            report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
            report_path=tmp_path / "session.json",
            deadline=time.monotonic() + 10,
            stdout_path=tmp_path / "stdout",
            stderr_path=tmp_path / "stderr",
        )


@pytest.mark.parametrize("fail_at", [1, 2])
def test_report_failure_after_child_startup_still_terminates_the_child(
    production, tmp_path, monkeypatch, fail_at
):
    started = []
    original = subprocess.Popen
    writes = []

    def start(*args, **kwargs):
        child = original(*args, **kwargs)
        started.append(child)
        return child

    def fail_report(*_args, **_kwargs):
        writes.append(True)
        if len(writes) == fail_at:
            raise OSError("injected report synchronization failure")

    def rss(_group, *, timeout):
        assert 0 < timeout <= 5
        return 0

    monkeypatch.setattr(production.subprocess, "Popen", start)
    monkeypatch.setattr(production, "_atomic_report", fail_report)
    monkeypatch.setattr(production, "_process_group_rss", rss)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    try:
        with pytest.raises(OSError, match="report synchronization failure"):
            production._invoke(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                directory=tmp_path,
                operator=tmp_path / "operator",
                report={
                    "resource_samples": [],
                    "peak_child_process_group_rss_bytes": 0,
                    "system": {"filesystem_device": tmp_path.stat().st_dev},
                },
                report_path=tmp_path / "session.json",
                deadline=time.monotonic() + 10,
                stdout_path=tmp_path / "stdout",
                stderr_path=tmp_path / "stderr",
            )
        assert len(started) == 1
        assert started[0].poll() is not None, "report failure leaked the running child"
    finally:
        for child in started:
            if child.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=3)


def test_failed_reaping_does_not_receive_a_second_termination_allowance(
    production, tmp_path, monkeypatch
):
    calls = []

    class UnreapedChild:
        pid = 424242

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("unreaped child", timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: 100)
    monkeypatch.setattr(production, "_terminate", lambda _child: calls.append(True) or False)
    code, reason, reaped = production._monitor(
        UnreapedChild(), tmp_path, tmp_path / "operator", {}, tmp_path / "session.json", 100
    )
    assert code is None
    assert reason
    assert reaped is False
    assert len(calls) == 1, "one stop received more than its five-second termination allowance"


def test_fresh_session_rejects_nonempty_unmanaged_directory_without_modification(
    production, tmp_path
):
    directory = tmp_path / "unmanaged"
    directory.mkdir()
    sentinel = directory / "unrelated.txt"
    sentinel.write_text("preserve")
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(directory, "full")
    assert list(directory.iterdir()) == [sentinel]
    assert sentinel.read_text() == "preserve"


@pytest.mark.parametrize("mode", ["full", "resume", "activate-current"])
def test_symlinked_production_directory_is_rejected(production, tmp_path, mode):
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(link, mode)
    assert not list(target.iterdir())


def test_production_directory_cannot_be_inside_checkout(production):
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(SCRIPT.parent.parent / "production-not-allowed", "full")


@pytest.mark.parametrize(
    "stop", ["rss", "free_space", "unknown_resources", "deadline", "mount_change"]
)
def test_resource_monitor_stops_and_reaps_without_starting_more_work(
    production, tmp_path, monkeypatch, stop
):
    clock = [100.0]
    terminated = []

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def terminate(actual_child):
        assert actual_child is child
        terminated.append(clock[0])
        child.returncode = -15
        return True

    def rss(_group, *, timeout):
        assert 0 < timeout <= 5
        if stop == "unknown_resources":
            raise OSError("resource inspection unavailable")
        return 24 * 1024**3 + 1 if stop == "rss" else 0

    free = production.RUN_LIMITS["minimum_free_bytes"] - (1 if stop == "free_space" else 0)
    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda duration: clock.__setitem__(0, clock[0] + duration)
    )
    monkeypatch.setattr(production, "_process_group_rss", rss)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=free))
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}
    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        100 if stop == "deadline" else 200,
        original_device=tmp_path.stat().st_dev + 1 if stop == "mount_change" else None,
    )
    assert code == -15
    assert reason
    assert reaped is True
    assert len(terminated) == 1
    if stop == "rss":
        assert terminated == [160]
        assert len(report["resource_samples"]) == 5
    else:
        assert terminated == [100]


@pytest.mark.parametrize("filename", ["run-production.py", "production_run.py"])
def test_script_entries_work_outside_checkout_with_only_src_on_pythonpath(tmp_path, filename):
    script = SCRIPT.with_name(filename)
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SCRIPT.parent.parent / "src")},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--directory" in result.stdout
    assert "--activate-current" in result.stdout


def test_allocation_survives_parent_heartbeat_before_child_result_is_available(
    production, tmp_path, monkeypatch
):
    notification = tmp_path / "capture-notification.json"
    report_path = tmp_path / "session.json"
    report = {
        "capture_id": None,
        "root_id": None,
        "accounting_complete": True,
        "resumable": None,
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "system": {"filesystem_device": tmp_path.stat().st_dev},
    }

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def allocate_during_sample(_group, *, timeout):
        assert 0 < timeout <= 5
        production._write_capture_notification(notification, "b" * 32, "a" * 32)
        child.returncode = -15
        return 0

    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: child)
    monkeypatch.setattr(production, "_process_group_rss", allocate_during_sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production.time, "sleep", lambda _duration: None)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    code, _reason, reaped = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=report_path,
        deadline=time.monotonic() + 10,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
        capture_notification=notification,
    )
    assert code == -15 and reaped
    assert report["capture_id"] == "b" * 32
    assert report["root_id"] == "a" * 32
    assert report["accounting_complete"] is False
    assert report["resumable"] is None
    assert json.loads(notification.read_text())["capture_id"] == "b" * 32


def test_refresh_worker_rejects_nonpublic_origin_before_creating_operator_data(
    production, tmp_path, monkeypatch, capsys
):
    from kalshi_events import transport

    report = tmp_path / "session.json"
    report.write_text("{}")
    root = tmp_path / "operator"
    monkeypatch.setattr(transport, "source_url", lambda: "https://untrusted.example/trade-api/v2")
    limits = production.remaining_allowances(
        {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": True}, 100, now=100
    )
    code = production._refresh_worker_main(
        root,
        report,
        tmp_path / "metrics.json",
        tmp_path / "capture-notification.json",
        "full",
        None,
        json.dumps(limits),
    )
    value = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "public Kalshi API origin" in value["error"]
    assert not root.exists()
    assert not (tmp_path / "metrics.json").exists()


@pytest.mark.parametrize("workflow", [False, True])
@pytest.mark.parametrize("wall_elapsed", [-100, 20, 200])
def test_wall_clock_changes_cannot_extend_monotonic_stage_deadline(
    production, monkeypatch, workflow, wall_elapsed
):
    report = {"started_monotonic": 100, "started_epoch": 1000}
    monkeypatch.setattr(production.time, "monotonic", lambda: 160)
    monkeypatch.setattr(production.time, "time", lambda: 1000 + wall_elapsed)
    limit = production.RUN_LIMITS["workflow_seconds" if workflow else "active_cutoff_seconds"]
    deadline = production._monitor_deadline(report, workflow=workflow)
    assert deadline == min(100 + limit, 160 + limit - wall_elapsed)
    assert deadline <= 100 + limit


@pytest.mark.parametrize("workflow", [False, True])
def test_sleep_or_forward_wall_jump_exhausts_stage_even_if_monotonic_barely_advanced(
    production, monkeypatch, workflow
):
    report = {"started_monotonic": 100, "started_epoch": 1000}
    limit = production.RUN_LIMITS["workflow_seconds" if workflow else "active_cutoff_seconds"]
    monkeypatch.setattr(production.time, "monotonic", lambda: 160)
    monkeypatch.setattr(production.time, "time", lambda: 1000 + limit + 1)
    assert production._monitor_deadline(report, workflow=workflow) == 160


def test_child_start_is_refused_without_its_termination_reserve(production, tmp_path, monkeypatch):
    monkeypatch.setattr(
        production.subprocess, "Popen", lambda *_a, **_kw: pytest.fail("late child was started")
    )
    code, reason, reaped = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
        report_path=tmp_path / "session.json",
        deadline=time.monotonic() + production.RUN_LIMITS["terminate_seconds"] - 0.1,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )
    assert code is None
    assert reason
    assert reaped is True
    assert not (tmp_path / "stdout").exists()


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGHUP])
def test_public_supervisor_signal_reaps_its_isolated_worker_and_records_uncertainty(
    tmp_path, stop_signal
):
    directory = tmp_path / "production"
    ready = tmp_path / "worker.json"
    harness = """
import json,os,pathlib,runpy,sys
from types import SimpleNamespace
sys.path.insert(0,sys.argv[1])
import production_run as runner
from kalshi_events import transport
runner._revision=lambda:'a'*40
runner._boot_stamp=lambda:'test boot'
runner._system_info=lambda directory:{'filesystem_device':directory.stat().st_dev}
runner.shutil.disk_usage=lambda directory:SimpleNamespace(free=1000*1024**3)
transport.source_url=lambda:transport.BASE_URL
worker=('import json,os,sys,time; '
        'open(sys.argv[1],"w").write(json.dumps({"pid":os.getpid(),"pgid":os.getpgrp()})); '
        'time.sleep(60)')
ready_path=sys.argv[4]
runner._refresh_command=lambda *a,**kw:[sys.executable,'-c',worker,ready_path]
sys.argv=[sys.argv[2],'--directory',sys.argv[3],'--full']
runpy.run_path(sys.argv[0],run_name='__main__')
"""
    supervisor = subprocess.Popen(
        [
            sys.executable,
            "-c",
            harness,
            str(SCRIPT.parent),
            str(SCRIPT.with_name("run-production.py")),
            str(directory),
            str(ready),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SCRIPT.parent.parent / "src")},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    worker = None
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if ready.exists():
                with contextlib.suppress(ValueError):
                    worker = json.loads(ready.read_text())
                    break
            if supervisor.poll() is not None:
                break
            time.sleep(0.01)
        assert worker is not None, supervisor.communicate(timeout=7)
        assert worker["pgid"] != supervisor.pid
        assert _live_process(worker["pid"])
        supervisor.send_signal(stop_signal)
        stdout, stderr = supervisor.communicate(timeout=7)
        assert supervisor.returncode != 0, (stdout, stderr)
        assert not _live_process(worker["pid"]), "public stop left its isolated worker running"
        report = json.loads(next((directory / "runs").glob("*/session.json")).read_text())
        assert report["phase"] == "stopped"
        assert report["accounting_complete"] is False
        assert report["requests"] is None
        assert report["downloaded_bytes"] is None
        assert report["production_state"] != "verified"
        assert report["active_process_group"] is None
        assert report["error"]
        assert not (directory / "production.duckdb").exists()
    finally:
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=7)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(supervisor.pid, signal.SIGKILL)
                supervisor.wait(timeout=3)
        if worker is None and ready.exists():
            with contextlib.suppress(ValueError):
                worker = json.loads(ready.read_text())
        if worker is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(worker["pgid"], signal.SIGKILL)


@pytest.mark.parametrize("returncode", [0, 1])
def test_standalone_verification_records_elapsed_time_on_success_and_failure(
    production, tmp_path, monkeypatch, returncode
):
    clock = [100.0]
    release = tmp_path / "release"
    report = {"phase_seconds": {"standalone-verification": 2.5, "capture": 99}}

    def invoke(command, **kwargs):
        assert command[0] == sys.executable
        assert command[command.index("--release") + 1] == str(release)
        assert kwargs["deadline"] == 150
        clock[0] += 7.5
        kwargs["stdout_path"].write_text(json.dumps({"source_release_id": "a" * 64}))
        return returncode, None, True

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production, "_invoke", invoke)
    args = (
        release,
        tmp_path,
        tmp_path / "operator",
        {"logs": tmp_path, "report": tmp_path / "session.json"},
        report,
        150,
    )
    if returncode:
        with pytest.raises(production.RunnerError, match="standalone release verification"):
            production._release_verify(*args)
    else:
        assert production._release_verify(*args)["source_release_id"] == "a" * 64
    assert report["phase_seconds"] == {"standalone-verification": 10.0, "capture": 99}


def test_stop_during_resource_termination_is_guarded_and_restores_previous_handlers(
    production, monkeypatch
):
    child = SimpleNamespace(pid=424242, poll=lambda: 0)
    original_handlers = {
        item: signal.getsignal(item) for item in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
    }
    calls = []

    def killpg(group, delivered):
        assert group == child.pid and delivered == signal.SIGTERM
        assert production._STOP_IN_PROGRESS is True
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)
        calls.append(True)

    monkeypatch.setattr(production.os, "killpg", killpg)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_TERMINATION_ATTEMPTED", set())
    with production._graceful_stop_handlers():
        assert production._STOP_IN_PROGRESS is False
        assert production._terminate(child) is True
        assert calls == [True]
        assert production._STOP_IN_PROGRESS is False
        production._STOP_IN_PROGRESS = True
        assert production._terminate(child) is True
        assert production._STOP_IN_PROGRESS is True
    assert production._STOP_IN_PROGRESS is False
    assert {item: signal.getsignal(item) for item in original_handlers} == original_handlers


@pytest.mark.parametrize("leader_reaped", [False, True])
def test_unreaped_group_receives_only_one_grace_even_if_cleanup_is_reentered(
    production, monkeypatch, leader_reaped
):
    clock = [100.0]
    exists = [True]
    signals = []

    class Child:
        pid = 424242

        def poll(self):
            return 0 if leader_reaped else None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("unreaped child", timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda duration: clock.__setitem__(0, clock[0] + duration)
    )
    monkeypatch.setattr(production.os, "killpg", lambda group, sent: signals.append((group, sent)))
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: exists[0])
    monkeypatch.setattr(production, "_TERMINATION_ATTEMPTED", set())
    child = Child()
    assert production._terminate(child) is False
    assert clock[0] == pytest.approx(105)
    prior_signals = signals.copy()
    assert production._terminate(child) is False
    assert clock[0] == pytest.approx(105), "reentry granted a second five-second grace"
    assert signals == prior_signals
    exists[0] = False
    assert production._terminate(child) is True
    assert clock[0] == pytest.approx(105)


def test_environment_report_uses_the_installed_distribution_name(production, monkeypatch):
    import importlib.metadata

    requested = []

    def version(distribution):
        requested.append(distribution)
        return "test-version"

    monkeypatch.setattr(importlib.metadata, "version", version)
    versions = production._package_versions()
    assert versions["oddsfox-kalshi-events"] == "test-version"
    assert "kalshi-events" not in requested
