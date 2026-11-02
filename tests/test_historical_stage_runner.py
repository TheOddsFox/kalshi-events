from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"
STAGE = "build-historical-" + "e" * 32
CAPTURE_ID = "a" * 32
ROOT_ID = "b" * 32
REVISION = "c" * 40
OLD_REVISION = "d" * 40
RECONCILIATION_REVISION = "e" * 40


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_run_historical_stage_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _invocation_report(production, stage=STAGE):
    report = {
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "revision": REVISION,
        "boot_stamp": "saved-boot",
    }
    production._record_historical_invocation(report, stage, revision=REVISION)
    return report


def test_historical_stage_selection_is_sealed_to_latest_invocation(production):
    report = _invocation_report(production)

    assert production._session_historical_stage(report, "historical") == STAGE
    report["resume_historical_stage"] = "build-historical-" + "f" * 32
    with pytest.raises(production.RunnerError, match="sealed to this invocation"):
        production._session_historical_stage(report, "historical")
    with pytest.raises(production.RunnerError):
        production._session_historical_stage(report, "resume")


def test_cli_requires_historical_exact_capture_and_managed_stage_basename(
    production, tmp_path, monkeypatch, capsys
):
    called = {}
    monkeypatch.setattr(
        production,
        "run_production",
        lambda directory, **kwargs: called.update(directory=directory, **kwargs) or ({}, 0),
    )
    assert (
        production.main(
            [
                "--directory",
                str(tmp_path / "production"),
                "--capture-id",
                CAPTURE_ID,
                "--historical",
                "--resume-historical-stage",
                STAGE,
            ]
        )
        == 0
    )
    assert called["resume_historical_stage"] == STAGE
    capsys.readouterr()

    for args, message in (
        (["--capture-id", CAPTURE_ID, "--resume-historical-stage", STAGE], "requires --historical"),
        (
            [
                "--capture-id",
                CAPTURE_ID,
                "--historical",
                "--resume-historical-stage",
                "tmp/" + STAGE,
            ],
            "full build-historical stage basename",
        ),
        (["--full", "--resume-historical-stage", STAGE], "requires --historical"),
    ):
        with pytest.raises(SystemExit) as raised:
            production.main(["--directory", str(tmp_path / "production"), *args])
        assert raised.value.code == 2
        assert message in capsys.readouterr().err


def test_run_production_api_rejects_stage_outside_historical_capture(production, tmp_path):
    with pytest.raises(production.RunnerError, match="requires --historical --capture-id"):
        production.run_production(
            tmp_path / "production", capture_id=CAPTURE_ID, resume_historical_stage=STAGE
        )
    with pytest.raises(production.RunnerError, match="full build-historical stage basename"):
        production.run_production(
            tmp_path / "production",
            capture_id=CAPTURE_ID,
            historical=True,
            resume_historical_stage="../" + STAGE,
        )


@pytest.mark.parametrize("stale_activation", [False, True])
def test_initial_accounting_preserves_only_the_selected_stage(
    production, tmp_path, monkeypatch, stale_activation
):
    import production_activation

    from kalshi_events import capture, common, incremental, warehouse

    root = tmp_path / "production" / "operator"
    (root / "raw").mkdir(parents=True)

    class FakeStore:
        def __init__(self, store_root, _limits):
            self.root = Path(store_root)
            self.limits = _limits
            self.remaining = 120

        def lock(self):
            return contextlib.nullcontext(self)

        def path(self, relative):
            return self.root / relative

        def usage(self):
            return {"retained_bytes": 10, "temporary_bytes": 20, "database_bytes": 30}

    monkeypatch.setattr(common, "Store", FakeStore)
    monkeypatch.setattr(
        capture,
        "read_capture",
        lambda _store, capture_id: {
            "capture_id": capture_id,
            "selected": None,
            "incremental": False,
            "acquisition_protocol": 5,
            "max_response_bytes": int(production.RUN_LIMITS["response_bytes"]),
        },
    )
    session = _invocation_report(production)
    monkeypatch.setattr(
        production, "_validate_initial_historical_accounting_session", lambda *_args: session
    )
    monkeypatch.setattr(incremental, "finish_pending_retirement", lambda _store: None)
    calls = []
    monkeypatch.setattr(
        production_activation,
        "_activation_temporary_names",
        lambda _directory: ["stale"] if stale_activation else [],
    )
    monkeypatch.setattr(
        production_activation,
        "recover_activation_temporaries",
        lambda *_args, **kwargs: calls.append(("activation", kwargs)),
    )
    (root / "current-historical.json").write_text(json.dumps({"source_release_id": "c" * 64}))
    monkeypatch.setattr(
        warehouse,
        "recover_and_cleanup_abandoned_historical_stages",
        lambda _store, capture_id, *, preserve_stage=None: calls.append(
            (capture_id, preserve_stage)
        ),
    )
    monkeypatch.setattr(production, "_root_id", lambda _store: ROOT_ID)

    result = production._read_accounting_worker(
        root,
        CAPTURE_ID,
        cleanup_historical_stages=True,
        expected_directory=tmp_path / "production",
        report_path=tmp_path / "production" / "runs" / ("f" * 32) / "session.json",
        expected_root_id=ROOT_ID,
        expected_revision=REVISION,
    )

    if stale_activation:
        assert calls[0][0] == "activation"
        assert calls[0][1]["expected_release_id"] == "c" * 64
        assert calls[0][1]["expected_capture_id"] == CAPTURE_ID
        assert calls[0][1]["max_duration_seconds"] == 120
    assert calls[-1] == (CAPTURE_ID, STAGE)
    assert result["storage"] == {
        "retained_bytes": 10,
        "temporary_bytes": 20,
        "database_bytes": 30,
    }


def test_refresh_worker_passes_the_sealed_stage_to_refresh_run(
    production, tmp_path, monkeypatch, capsys
):
    from kalshi_events import common, incremental, metrics, refresh

    directory = tmp_path / "production"
    operator = directory / "operator"
    report_path = directory / "runs" / ("f" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    report = _invocation_report(production)
    report.update(resource_profile="default", limits=production.RUN_LIMITS)
    report_path.write_text(json.dumps(report), encoding="utf-8")

    class FakeStore:
        def __init__(self, *_args):
            pass

        def lock(self):
            return contextlib.nullcontext(self)

        def reconcile(self):
            pass

    class Stream:
        def close(self):
            pass

    class Measured:
        def progress(self, _phase):
            pass

    @contextmanager
    def collect(**_kwargs):
        yield Measured()

    called = {}
    monkeypatch.setattr(production, "_external_path", lambda path: Path(path))
    monkeypatch.setattr(
        production, "_session_resource_profile", lambda *_args, **_kwargs: ("default", report)
    )
    monkeypatch.setattr(production, "_limits_from_report", lambda *_args: production.RUN_LIMITS)
    monkeypatch.setattr(production, "_bind_owner", lambda *_args: None)
    monkeypatch.setattr(common, "Store", FakeStore)
    monkeypatch.setattr(incremental, "root_id", lambda *_args, **_kwargs: ROOT_ID)
    monkeypatch.setattr(metrics, "check_progress_path", lambda _path: None)
    monkeypatch.setattr(metrics, "reserve_report", lambda _path: Stream())
    monkeypatch.setattr(metrics, "collect", collect)
    monkeypatch.setattr(metrics, "write_report", lambda *_args: None)
    monkeypatch.setattr(
        refresh,
        "run",
        lambda _store, **kwargs: called.update(kwargs) or ({"complete": True}, 0),
    )
    limits = {
        "max-rate": 5,
        "max-requests": 100,
        "max-download-bytes": 1024,
        "max-response-bytes": 1024,
        "max-decoded-response-bytes": 2048,
        "max-duration-seconds": 3600,
        "max-root-bytes": 1024,
        "max-temp-bytes": 64 * 1024**3,
        "max-database-bytes": 64 * 1024**3,
        "compression": "gzip",
        "rate-policy": "decrease-on-429",
    }

    code = production._refresh_worker_main(
        operator,
        report_path,
        tmp_path / "metrics.json",
        tmp_path / "capture-notification.json",
        "historical",
        CAPTURE_ID,
        json.dumps(limits),
    )

    assert code == 0
    assert called["resume_historical_stage"] == STAGE
    capsys.readouterr()


def _current_temporary_stop(tmp_path, production, monkeypatch):
    directory = tmp_path / "production"
    operator = directory / "operator"
    session_id = "f" * 32
    report_path = directory / "runs" / session_id / production.REPORT_NAME
    source_runtime = tmp_path / "runtime-repo" / "data" / "runtime" / "current-run"
    source_root = source_runtime / "source"
    script = source_root / "scripts" / "run-production.py"
    script.parent.mkdir(parents=True)
    script.write_text("# synthetic runner", encoding="utf-8")
    stdout = source_runtime / "logs" / "worker.stdout.log"
    stdout.parent.mkdir(parents=True)
    stdout.write_bytes(b"")

    operator.mkdir(parents=True)
    (operator / "root.json").write_text(
        json.dumps({"owner": "oddsfox.kalshi.events.v1", "storage_version": 5}),
        encoding="utf-8",
    )
    (operator / "writer.lock").write_bytes(b"")
    directory.mkdir(exist_ok=True)
    production._atomic_report(
        directory / production.OWNER_MARKER, {"version": 1, "root_id": ROOT_ID}
    )
    report_path.parent.mkdir(parents=True)

    started = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=10)
    started_epoch = started.timestamp()
    started_mono = production.time.monotonic() - 10
    window_start = started + timedelta(seconds=1)
    checked_at = started + timedelta(seconds=2)
    window = {
        "version": 2,
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "revision": REVISION,
        "started_at": window_start.isoformat().replace("+00:00", "Z"),
        "started_epoch": started_epoch + 1,
        "started_monotonic": started_mono + 1,
        "deadline_epoch": started_epoch + 86_401,
        "deadline_monotonic": started_mono + 86_401,
    }
    group = 876_543
    token = "1" * 32
    error = "live temporary storage reached the 168 GiB stop threshold"
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": "full",
        "source_root": str(source_root),
        "revision": REVISION,
        "original_revision": "7" * 40,
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "boot_stamp": "saved-boot",
        "phase": "stopped",
        "active_process_group": group,
        "production_state": "not_attempted",
        "error": error,
        "network_activity_this_invocation": {
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
        "storage_supervision_invocation": token,
        "storage_supervision": {
            "current": {"operator_temporary_bytes": 180_388_626_432 + 100},
        },
        "process_supervision": {
            "primary_stop_reason": error,
            "termination_attempted": True,
            "leader_reaped": True,
            "group_absent": None,
            "uncertain": True,
            "invocation_token": token,
            "termination_errors": ["process-group probe: [Errno 1] Operation not permitted"],
        },
        "offline_historical_window": window,
        "limits": dict(production.RUN_LIMITS),
    }
    production._bind_report_resource_profile(report, "expanded", transition=True)
    production._atomic_report(report_path, report)

    command = [
        sys.executable,
        str(script),
        "--directory",
        str(directory),
        "--historical",
        "--capture-id",
        CAPTURE_ID,
    ]
    pid = 123_456
    started_text = started.strftime("%a %b %d %H:%M:%S %Y")
    launch = {
        "session": str(report_path),
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "revision": OLD_REVISION,
        "processing_revision": REVISION,
        "reconciliation_code_source": {"revision": OLD_REVISION},
        "pid": pid,
        "pgid": pid,
        "state": "launched",
        "offline_only": True,
        "automatic_retry": False,
        "command": command,
        "process_identity_utc": f"{pid} {pid} {started_text} {' '.join(command)}",
        "launched_at_utc": started.isoformat(),
        "started_epoch": started_epoch,
        "launched_monotonic": started_mono,
        "processing_deadline_epoch": window["deadline_epoch"] - 600,
        "active_end_epoch": window["deadline_epoch"] + 120,
        "outer_deadline_epoch": window["deadline_epoch"] + 720,
        "processing_deadline_monotonic": window["deadline_monotonic"] - 600,
        "active_end_monotonic": window["deadline_monotonic"] + 120,
        "outer_deadline_monotonic": window["deadline_monotonic"] + 720,
        "stdout": str(stdout),
    }
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    checks = {
        "checked_at_utc": checked_at.isoformat(),
        "fresh_processing_cutoff_and_active_end_enforced": True,
        "original_grant_and_reconciliation_history_preserved": True,
        "new_window": window,
    }
    (source_runtime / "launch-checks.json").write_text(json.dumps(checks), encoding="utf-8")

    monkeypatch.setattr(production, "SOURCE_ROOT", tmp_path / "runtime-repo")
    monkeypatch.setattr(production, "_revision", lambda: REVISION)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "saved-boot")
    monkeypatch.setattr(
        production,
        "_validate_offline_historical_window",
        lambda *_args, **_kwargs: {"root_id": ROOT_ID},
    )
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *_args, **_kwargs: (operator.stat().st_dev, operator.stat().st_ino),
    )
    return directory, operator, report_path, report, group, pid, launch


def _bind_inherited_stdout(tmp_path, report_path, report, launch):
    runtime_directory = Path(report["source_root"]).parent
    runtime_root = runtime_directory.parent
    run_set = runtime_root / "previous-projection-run"
    run_directory = run_set / "run-20261102T082419Z-01234567"
    run_directory.mkdir(parents=True, mode=0o700)
    run_set.chmod(0o700)
    stdout = run_directory / "stdout.log"
    stdout.write_bytes(b"inherited historical runner log\n")

    approval_id = "9" * 32
    archive_directory = runtime_directory / "continuation-archives" / approval_id
    archive_directory.mkdir(parents=True, mode=0o700)
    archive_directory.parent.chmod(0o700)
    archive_directory.chmod(0o700)
    failed_path = archive_directory / "failed-latest-launch.json"
    approval_path = archive_directory / "approval.json"
    failed = {
        "state": "preflight_or_launch_failed",
        "session": str(report_path),
        "capture_id": report["capture_id"],
        "root_id": report["root_id"],
        "revision": launch["revision"],
        "stdout": str(stdout),
        "offline_only": True,
        "automatic_retry": False,
    }

    def archive_json(path, value):
        raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        path.write_bytes(raw)
        path.chmod(0o600)
        return hashlib.sha256(raw).hexdigest()

    failed_sha = archive_json(failed_path, failed)
    approval = {
        "approval_id": approval_id,
        "action": "continue_reconciled_cleanup_once",
        "capture_id": report["capture_id"],
        "root_id": report["root_id"],
        "revision": launch["processing_revision"],
        "failed_launch_sha256": failed_sha,
    }
    approval_sha = archive_json(approval_path, approval)
    launch["stdout"] = str(stdout)
    launch["continuation_authorization"] = {
        "approval_id": approval_id,
        "failed_latch_archive": str(failed_path),
        "failed_latch_sha256": failed_sha,
        "approval_archive": str(approval_path),
        "approval_sha256": approval_sha,
    }
    source_runtime = runtime_directory
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    return stdout, failed_path, approval_path, failed, approval


def _rewrite_inherited_archive_bindings(launch, failed_path, approval_path, failed, approval):
    auth = launch["continuation_authorization"]
    failed_raw = (json.dumps(failed, sort_keys=True, separators=(",", ":")) + "\n").encode()
    failed_path.write_bytes(failed_raw)
    failed_path.chmod(0o600)
    failed_sha = hashlib.sha256(failed_raw).hexdigest()
    approval["failed_launch_sha256"] = failed_sha
    approval_raw = (json.dumps(approval, sort_keys=True, separators=(",", ":")) + "\n").encode()
    approval_path.write_bytes(approval_raw)
    approval_path.chmod(0o600)
    auth["failed_latch_sha256"] = failed_sha
    auth["approval_sha256"] = hashlib.sha256(approval_raw).hexdigest()


def test_current_processing_revision_reconciles_typed_temp_stop_and_keeps_evidence(
    production, tmp_path, monkeypatch
):
    directory, operator, report_path, original, group, _pid, _launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    result = production._reconcile_stopped_worker(directory, CAPTURE_ID)
    saved = production._strict_json(report_path)
    stop = saved["storage_safety_stop"]
    audit = json.loads(
        (operator / "stopped-worker-reconciliation.jsonl").read_text().splitlines()[0]
    )

    assert result["active_process_group"] is None
    assert saved["error"] == original["error"]
    assert saved["limits"] == original["limits"]
    assert saved["offline_historical_window"] == original["offline_historical_window"]
    assert saved["process_supervision"] == original["process_supervision"]
    assert stop == {
        "version": 1,
        "mode": "historical",
        "category": "temporary",
        "invocation_id": original["storage_supervision_invocation"],
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "revision": REVISION,
        "revision_source": "processing_revision",
        "observed_bytes": 180_388_626_432 + 100,
        "threshold_bytes": 180_388_626_432,
        "threshold_inclusive": True,
        "original_process_group": group,
    }
    assert audit["storage_stop_evidence"]["original_process_group"] == group
    assert audit["storage_stop_evidence"]["threshold_bytes"] == 180_388_626_432
    assert audit["launch_revision_source"] == "processing_revision"
    assert audit["processing_revision"] == REVISION
    assert audit["reconciliation_runtime_revision"] == RECONCILIATION_REVISION
    assert saved["stopped_worker_reconciliation"]["processing_revision"] == REVISION
    assert (
        saved["stopped_worker_reconciliation"]["reconciliation_runtime_revision"]
        == RECONCILIATION_REVISION
    )


def _seed_prior_reconciliation_audit(
    operator: Path,
    report: dict,
    *,
    supervisor_pid: int,
    supervisor_pgid: int | None,
    stale_process_group: int,
) -> dict:
    entry = {
        "version": 1,
        "event_id": hashlib.sha256(
            f"prior-{supervisor_pid}-{stale_process_group}".encode()
        ).hexdigest(),
        "reconciled_at": "2026-11-01T12:00:00+00:00",
        "session_id": report["session_id"],
        "capture_id": report["capture_id"],
        "root_id": report["root_id"],
        "supervisor_pid": supervisor_pid,
        "stale_process_group": stale_process_group,
    }
    if supervisor_pgid is not None:
        entry["supervisor_pgid"] = supervisor_pgid
    path = operator / "stopped-worker-reconciliation.jsonl"
    path.write_text(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(0o600)
    return entry


def test_temporary_stop_appends_new_owned_attempt_and_preserves_prior_audit(
    production, tmp_path, monkeypatch
):
    directory, operator, report_path, report, group, pid, _launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    prior = _seed_prior_reconciliation_audit(
        operator,
        report,
        supervisor_pid=pid + 10,
        supervisor_pgid=None,
        stale_process_group=group + 10,
    )
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    result = production._reconcile_stopped_worker(directory, CAPTURE_ID)

    entries = [
        json.loads(line)
        for line in (operator / "stopped-worker-reconciliation.jsonl").read_text().splitlines()
    ]
    assert result["active_process_group"] is None
    assert len(entries) == 2
    assert entries[0] == prior
    assert entries[1]["supervisor_pid"] == pid
    assert entries[1]["stale_process_group"] == group
    assert production._strict_json(report_path)["active_process_group"] is None


def test_temporary_stop_rejects_conflicting_audit_for_same_attempt(
    production, tmp_path, monkeypatch
):
    directory, operator, _report_path, report, group, pid, _launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    _seed_prior_reconciliation_audit(
        operator,
        report,
        supervisor_pid=pid,
        supervisor_pgid=pid,
        stale_process_group=group,
    )
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)

    with pytest.raises(production.RunnerError, match="different reconciliation audit"):
        production._reconcile_stopped_worker(directory, CAPTURE_ID)

    entries = (operator / "stopped-worker-reconciliation.jsonl").read_text().splitlines()
    assert len(entries) == 1


def test_old_processing_revision_binds_under_new_reconciler_and_tampering_fails(
    production, tmp_path, monkeypatch
):
    directory, _operator, report_path, report, _group, _pid, launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)

    evidence, *_ = production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)
    assert evidence["launch_revision_source"] == "processing_revision"
    assert evidence["processing_revision"] == REVISION
    assert evidence["reconciliation_runtime_revision"] == RECONCILIATION_REVISION

    launch["processing_revision"] = RECONCILIATION_REVISION
    source_runtime = Path(report["source_root"]).parent
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    with pytest.raises(production.RunnerError, match="not bound to this stopped session"):
        production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)


def test_current_processing_revision_accepts_checksummed_inherited_managed_stdout(
    production, tmp_path, monkeypatch
):
    directory, _operator, report_path, report, _group, _pid, launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    stdout, _failed_path, _approval_path, _failed, _approval = _bind_inherited_stdout(
        tmp_path, report_path, report, launch
    )
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)

    evidence, *_ = production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)

    assert evidence["launch_revision_source"] == "processing_revision"
    assert Path(launch["stdout"]) == stdout


@pytest.mark.parametrize("field", ("root_id", "capture_id"))
def test_inherited_stdout_rejects_wrong_archived_capture_or_root(
    production, tmp_path, monkeypatch, field
):
    directory, _operator, report_path, report, _group, _pid, launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    _, failed_path, approval_path, failed, approval = _bind_inherited_stdout(
        tmp_path, report_path, report, launch
    )
    failed[field] = "f" * 32
    _rewrite_inherited_archive_bindings(launch, failed_path, approval_path, failed, approval)
    source_runtime = Path(report["source_root"]).parent
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)

    with pytest.raises(production.RunnerError, match="approved failed launch"):
        production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)


def test_inherited_stdout_rejects_tampered_archive_hash_and_unrelated_log(
    production, tmp_path, monkeypatch
):
    directory, _operator, report_path, report, _group, _pid, launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    stdout, failed_path, approval_path, failed, approval = _bind_inherited_stdout(
        tmp_path, report_path, report, launch
    )
    failed_path.write_bytes(failed_path.read_bytes() + b"tampered\n")
    source_runtime = Path(report["source_root"]).parent
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    monkeypatch.setattr(production, "_revision", lambda: RECONCILIATION_REVISION)

    with pytest.raises(production.RunnerError, match="archive checksum"):
        production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)

    _rewrite_inherited_archive_bindings(launch, failed_path, approval_path, failed, approval)
    unrelated = stdout.parent.parent / "unrelated.log"
    unrelated.write_bytes(b"not the archived run stdout\n")
    launch["stdout"] = str(unrelated)
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")
    with pytest.raises(production.RunnerError, match="approved failed launch"):
        production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)


def test_session_revision_keeps_the_normal_stdout_parent_restriction(
    production, tmp_path, monkeypatch
):
    directory, _operator, report_path, report, _group, _pid, launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )
    stdout, _failed_path, _approval_path, _failed, _approval = _bind_inherited_stdout(
        tmp_path, report_path, report, launch
    )
    launch["revision"] = report["revision"]
    source_runtime = Path(report["source_root"]).parent
    (source_runtime / "latest-launch.json").write_text(json.dumps(launch), encoding="utf-8")

    with pytest.raises(production.RunnerError, match="stdout path is outside its managed run"):
        production._stopped_worker_evidence(directory, report_path, report, CAPTURE_ID)
    assert stdout.is_file()


def test_temporary_stop_evidence_survives_eprem_process_group_probe(
    production, tmp_path, monkeypatch
):
    directory, _operator, report_path, _original, group, _pid, _launch = _current_temporary_stop(
        tmp_path, production, monkeypatch
    )

    def denied(pgid):
        if pgid == group:
            raise PermissionError("Operation not permitted")
        return False

    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", denied)

    with pytest.raises(production.RunnerError, match="absence could not be verified"):
        production._reconcile_stopped_worker(directory, CAPTURE_ID)

    saved = production._strict_json(report_path)
    assert saved["active_process_group"] == group
    assert saved["storage_safety_stop"]["original_process_group"] == group
    assert saved["storage_safety_stop"]["threshold_bytes"] == 180_388_626_432


def test_temporary_stop_pid_reuse_is_proven_without_signaling_unrelated_process(
    production, tmp_path, monkeypatch
):
    directory, _operator, _report_path, _original, group, supervisor_pid, launch = (
        _current_temporary_stop(tmp_path, production, monkeypatch)
    )
    monkeypatch.setattr(production, "_process_group_exists", lambda _pgid: False)
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda pid: pid in {group, supervisor_pid},
    )
    monkeypatch.setattr(production.os, "getpgid", lambda _pid: supervisor_pid + 1)
    monkeypatch.setattr(
        production.subprocess,
        "run",
        lambda command, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=(
                f"{supervisor_pid} {supervisor_pid + 1} Tue Nov  3 08:24:19 2026 unrelated process"
            ),
        ),
    )

    result = production._reconcile_stopped_worker(directory, CAPTURE_ID)

    assert result["active_process_group"] is None
    assert launch["process_identity_utc"].endswith(" ".join(launch["command"]))
