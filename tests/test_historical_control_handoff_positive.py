"""Exercise the real controller-to-worker pipe and live storage boundary.

The frozen runtime/census authority and data action are synthetic in this test;
the inherited pipe, worker/parent identity, canonical run lock, and live storage
observer are the real production implementations.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import textwrap
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
PRODUCTION_PATH = SCRIPTS / "production_run.py"
GRANTS_SPEC = importlib.util.spec_from_file_location(
    "positive_handoff_historical_grants", SCRIPTS / "historical_grants.py"
)
assert GRANTS_SPEC and GRANTS_SPEC.loader
grants = importlib.util.module_from_spec(GRANTS_SPEC)
GRANTS_SPEC.loader.exec_module(grants)
PRODUCTION_SPEC = importlib.util.spec_from_file_location(
    "positive_handoff_production_run", PRODUCTION_PATH
)
assert PRODUCTION_SPEC and PRODUCTION_SPEC.loader
production = importlib.util.module_from_spec(PRODUCTION_SPEC)
PRODUCTION_SPEC.loader.exec_module(production)


def _new_control_root() -> Path:
    runtime = ROOT / "data" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    for offset in range(3650):
        day = date.today() - timedelta(days=offset)
        path = runtime / f"kalshi-historical-completion-{day:%Y%m%d}"
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            continue
        path.chmod(0o700)
        return path
    raise AssertionError("no unused managed historical control directory name")


def _bytecode_files(*roots: Path) -> set[Path]:
    return {path for root in roots for path in root.rglob("*.pyc")}


def _write_control_bundle(
    control_root: Path,
    identity: dict[str, object],
    report_path: Path,
    *,
    boot_stamp: str,
    root_volume_uuid: str,
    root_device: int,
) -> str:
    template_identity = {
        **identity,
        "state_sha256": None,
        "source_sha256": None,
        "runtime_sha256": None,
    }
    attempts = {
        "attempt_outcome_sha256": "a" * 64,
        "failed_launch_sha256": "b" * 64,
        "audit_after_sha256": "c" * 64,
        "failed_session_sha256": template_identity["prior_session_sha256"],
    }
    attempt_bundle = {
        "attempt_outcome_sha256": {
            "path": "attempt.json",
            "sha256": attempts["attempt_outcome_sha256"],
        },
        "failed_launch_sha256": {
            "path": "launch.json",
            "sha256": attempts["failed_launch_sha256"],
        },
        "audit_after_sha256": {"path": "audit.jsonl", "sha256": attempts["audit_after_sha256"]},
        "failed_session_sha256": {
            "path": "session.json",
            "sha256": attempts["failed_session_sha256"],
        },
    }
    attempts["bundle_sha256"] = grants.sha256_json(attempt_bundle)
    template_identity["failed_attempts_sha256"] = attempts["bundle_sha256"]
    template = {
        "version": 1,
        "state": "inert",
        "identity": template_identity,
        "prior_attempts": attempts,
        "fresh_pins": {"source": None, "state": None, "runtime": None, "approval": None},
        "phase_seals": [],
        "authorized_census": {
            "population_report_path": str(control_root / "synthetic-population.json"),
            "population_report_sha256": "d" * 64,
            "receipt_index_path": str(control_root / "synthetic-receipt-index.json"),
            "receipt_index_sha256": "e" * 64,
            "capture_id": template_identity["capture_id"],
            "root_id": template_identity["root_id"],
            "capture_state_sha256": "1" * 64,
            "candidate_pages": 3,
            "observations_by_endpoint": {"/series": 2, "/events": 3, "/historical/markets": 5},
            "nested_event_markets": 0,
            "exact_observations": 10,
        },
    }
    control_sha256 = grants.write_once_json(control_root / "control.json", template)
    runtime_manifest_sha256 = "2" * 64
    operational_source_sha256 = "3" * 64
    runtime_sha256 = grants.sha256_json(
        {
            "runtime_manifest_sha256": runtime_manifest_sha256,
            "operational_source_sha256": operational_source_sha256,
        }
    )
    frozen_identity = {
        **template_identity,
        "state_sha256": "4" * 64,
        "source_sha256": "5" * 64,
        "runtime_sha256": runtime_sha256,
    }
    frozen = {
        "version": 1,
        "control_sha256": control_sha256,
        "identity": frozen_identity,
        "state_sha256": frozen_identity["state_sha256"],
        "source_sha256": frozen_identity["source_sha256"],
        "runtime_sha256": runtime_sha256,
        "source_snapshot_sha256": "6" * 64,
        "receipt_evidence_sha256": "7" * 64,
        "scope_sha256": "8" * 64,
        "code_sha256": "9" * 64,
        "runtime_manifest_sha256": runtime_manifest_sha256,
        "operational_source_sha256": operational_source_sha256,
        "root_volume_uuid": root_volume_uuid,
        "root_device": root_device,
        "frozen_at_epoch": time.time(),
    }
    grants.write_once_json(control_root / "freeze.json", frozen)

    grant_id = uuid.uuid4().hex
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    report_sha256 = grants.sha256_bytes(report_path.read_bytes())
    approval = {
        "version": 1,
        "grant_id": grant_id,
        "phase": "source",
        "phases": ["source"],
        "bindings": frozen_identity,
        "approved_at_epoch": now_epoch - 1,
        "duration_seconds": 24 * 60 * 60,
        "forecast_seconds": 16 * 60 * 60,
        "measured_final_verification_seconds": 0,
        "measurements": {
            "report_sha256": "a" * 64,
            "session_report_sha256": report_sha256,
            "measured_at_epoch": now_epoch - 0.5,
            "forecast_seconds": 16 * 60 * 60,
            "final_verification_seconds": 0,
            "projected_database_bytes": 90 * grants.GIB,
            "projected_temporary_bytes": 130 * grants.GIB,
            "projected_root_bytes": 300 * grants.GIB,
            "projected_aggregate_bytes": 350 * grants.GIB,
            "projected_free_bytes": 160 * grants.GIB,
            "inventory_entries": 50_000,
            "projected_peak_rss_bytes": 20 * grants.GIB,
        },
        "resource_admission": grants.expected_resource_admission(),
        "freeze": {"engineering": True, "runtime": True, "controls": True},
        "retire_source": False,
        "retirement_policy": None,
        "activation_audit_sha256": None,
    }
    latch = {
        "version": 1,
        "grant_id": grant_id,
        "approval_sha256": grants.sha256_json(approval),
        "nonce": uuid.uuid4().hex,
        "created_at_epoch": now_epoch - 0.25,
    }
    session_binding = {
        "version": 1,
        "grant_id": grant_id,
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": frozen_identity["capture_id"],
        "session_id": frozen_identity["session_id"],
        "root_id": frozen_identity["root_id"],
        "stage_name": frozen_identity["stage_name"],
        "report_sha256": report_sha256,
        "report_path": str(report_path),
        "boot_stamp": boot_stamp,
        "root_volume_uuid": root_volume_uuid,
        "root_device": root_device,
    }
    launch = grants.create_launch_record(
        approval,
        latch,
        now_epoch=now_epoch,
        now_monotonic=now_monotonic,
        boot_stamp=boot_stamp,
        launch_id=uuid.uuid4().hex,
        session_binding_sha256=grants.sha256_json(session_binding),
    )
    grants.validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=frozen_identity,
        now_epoch=now_epoch + 0.01,
        now_monotonic=now_monotonic + 0.01,
        boot_stamp=boot_stamp,
    )
    grants.validate_session_binding(
        session_binding,
        approval=approval,
        expected_identity=frozen_identity,
    )
    for name, value in (
        ("approval.json", approval),
        ("latch.json", latch),
        ("launch.json", launch),
        ("session-binding.json", session_binding),
    ):
        grants.write_once_json(control_root / "grants" / grant_id / name, value)
    return grant_id


def test_private_worker_accepts_real_controller_pipe_with_lock_and_storage_observer():
    run_id = uuid.uuid4().hex
    evidence = ROOT / "data" / "testing" / "historical-control-handoff-positive" / run_id
    evidence.mkdir(parents=True, mode=0o700)
    production_root = evidence / "production"
    operator = production_root / "operator"
    report_path = production_root / "runs" / run_id / production.REPORT_NAME
    logs = report_path.parent / "logs"
    operator.mkdir(parents=True, mode=0o700)
    logs.mkdir(parents=True, mode=0o700)
    (production_root / "runs").mkdir(exist_ok=True, mode=0o700)

    capture_id = uuid.uuid4().hex
    session_id = run_id
    root_id = uuid.uuid4().hex
    boot_stamp = "synthetic-positive-handoff-boot"
    root_volume_uuid = str(uuid.uuid4()).upper()
    (production_root / production.OWNER_MARKER).write_text(
        json.dumps({"version": 1, "root_id": root_id}) + "\n"
    )
    (production_root / ".production-run.lock").touch(mode=0o600)
    (operator / "root-id.json").write_text(json.dumps({"version": 1, "root_id": root_id}) + "\n")
    (operator / "writer.lock").touch(mode=0o600)

    limits = production._limits_for_profile("expanded")
    report = {
        "version": 1,
        "session_id": session_id,
        "capture_id": capture_id,
        "root_id": root_id,
        "mode": "historical",
        "phase": "stopped",
        "active_process_group": None,
        "resource_profile": "expanded",
        "resource_profile_history": [
            {
                "profile": "expanded",
                "limits": limits,
                "policy": production._resource_policy_record("expanded", limits),
                "selected_at": "synthetic fixture",
                "reason": "positive inherited-pipe test",
            }
        ],
        "limits": limits,
        "effective_resource_policy": production._resource_policy_record("expanded", limits),
        "started_epoch": time.time(),
        "boot_stamp": boot_stamp,
        "phase_seconds": {},
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "peak_child_process_group_physical_footprint_bytes": 0,
        "invocation_peak_child_process_group_rss_bytes": 0,
        "invocation_peak_child_process_group_physical_footprint_bytes": 0,
        "private_historical_grant": {"state": "synthetic-test-setup"},
        "system": {"filesystem_device": production_root.stat().st_dev},
        "storage": {
            "operator_retained_bytes": 0,
            "operator_temporary_bytes": 0,
            "operator_database_bytes": 0,
            "external_bytes": 0,
            "aggregate_estimate_bytes": 0,
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    production._atomic_report(report_path, report)
    workspace = production._live_workspace_snapshot(operator, production_root, capture_id, root_id)
    report["storage"] = {
        "operator_retained_bytes": workspace["logical_bytes"],
        "operator_temporary_bytes": workspace["temporary_bytes"],
        "operator_database_bytes": workspace["database_bytes"],
        "external_bytes": production._external_bytes(production_root),
        "aggregate_estimate_bytes": workspace["logical_bytes"]
        + production._external_bytes(production_root),
    }
    production._atomic_report(report_path, report)

    identity: dict[str, object] = {
        "capture_id": capture_id,
        "session_id": session_id,
        "root_id": root_id,
        "stage_name": "build-historical-" + uuid.uuid4().hex,
        "failed_attempts_sha256": "a" * 64,
        "prior_session_sha256": "b" * 64,
        "state_sha256": "c" * 64,
        "source_sha256": "d" * 64,
        "runtime_sha256": "e" * 64,
    }
    control_root = _new_control_root()
    grant_id = _write_control_bundle(
        control_root,
        identity,
        report_path,
        boot_stamp=boot_stamp,
        root_volume_uuid=root_volume_uuid,
        root_device=production_root.stat().st_dev,
    )
    launch_path = control_root / "grants" / grant_id / "launch.json"
    bundle = grants.read_control_bundle(control_root, grant_id)
    assert bundle["identity"]["capture_id"] == capture_id
    launch_sha256 = grants.sha256_bytes(launch_path.read_bytes())
    token = uuid.uuid4().hex
    storage_log_dir = logs / f"invocation-{token}"
    storage_log_dir.mkdir(mode=0o700)
    storage_progress = storage_log_dir / "source-maintenance.storage.json"
    stdout_path = logs / "worker.stdout.json"
    stderr_path = logs / "worker.stderr.log"
    controller_path = evidence / "controller-helper.py"
    sitecustomize_path = evidence / "sitecustomize.py"
    worker_path = evidence / "worker.py"
    config_path = evidence / "controller-config.json"
    controller_result_path = evidence / "controller-result.json"
    controller_ps_path = evidence / "controller-ps.json"
    authority_path = evidence / "authority-entered.json"
    action_path = evidence / "synthetic-action.json"
    grandchild_source_root = evidence / "grandchild-source"
    grandchild_source_root.mkdir(mode=0o700)
    grandchild_module_path = grandchild_source_root / "handoff_bytecode_probe.py"
    grandchild_script_path = grandchild_source_root / "unflagged_python_child.py"
    grandchild_module_path.write_text('MARKER = "handoff-bytecode-probe-imported"\n')
    grandchild_script_path.write_text(
        textwrap.dedent(
            """\
            import json, os, sys
            import handoff_bytecode_probe

            print(json.dumps({
                "dont_write_bytecode": sys.dont_write_bytecode,
                "bytecode_env": os.environ.get("PYTHONDONTWRITEBYTECODE"),
                "module_marker": handoff_bytecode_probe.MARKER,
            }, sort_keys=True))
            """
        )
    )
    worker_code = textwrap.dedent(
        f"""\
        import json, os, subprocess, sys, time
        from pathlib import Path
        root = Path({str(ROOT)!r})
        sys.path.insert(0, str(root / "scripts"))
        import historical_grants as grants
        import production_run as production
        (production_path, directory, control_root, grant_id, launch_sha256,
         phase, substep, token, progress, report_path, authority_path, action_path) = sys.argv[1:13]
        directory = Path(directory)
        control_root = Path(control_root)
        progress = Path(progress)
        report_path = Path(report_path)
        authority_path = Path(authority_path)
        action_path = Path(action_path)
        directory = production._external_path(directory)
        control_root = production._historical_control_root(control_root)
        production._historical_assert_pinned_python(control_root)
        identity_probe = subprocess.run(
            ["/bin/ps", "-p", str(os.getppid()), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        controller_command = identity_probe.stdout.strip()
        expected_python = str(production._historical_venv_root(control_root) / "bin" / "python")
        expected_production = str(production.SOURCE_ROOT / "scripts" / "production_run.py")
        expected_controller_command = " ".join((
            expected_python,
            "-B",
            expected_production,
            "--_historical-control",
            str(directory),
            str(control_root),
            grant_id,
        ))
        worker_identity_probe = subprocess.run(
            ["/bin/ps", "-p", str(os.getpid()), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        Path({str(controller_ps_path)!r}).write_text(
            json.dumps({{
                "returncode": identity_probe.returncode,
                "stdout": controller_command,
                "stderr": identity_probe.stderr,
                "expected_python": expected_python,
                "expected_production": expected_production,
                "worker_returncode": worker_identity_probe.returncode,
                "worker_command": worker_identity_probe.stdout.strip(),
                "directory": str(directory),
                "control_root": str(control_root),
                "grant_id": grant_id,
                "worker_pid": os.getpid(),
                "parent_pid": os.getppid(),
                "checks": {{
                    "returncode": identity_probe.returncode == 0,
                    "python": expected_python in controller_command,
                    "production": expected_production in controller_command,
                    "mode": "--_historical-control" in controller_command,
                    "directory": str(directory) in controller_command,
                    "control_root": str(control_root) in controller_command,
                    "grant_id": grant_id in controller_command,
                    "exact_controller_argv": controller_command == expected_controller_command,
                }},
            }}) + "\\n"
        )
        def synthetic_authority(directory_arg, control_arg, grant_arg, **kwargs):
            assert "{production.HISTORICAL_SUPERVISOR_FD_ENV}" not in os.environ
            assert kwargs.get("require_worker") is True
            assert kwargs.get("supervisor_pid") == os.getppid()
            bundle = grants.read_control_bundle(control_arg, grant_arg)
            report = production._strict_json(report_path)
            assert report["active_process_group"] == os.getpid()
            assert report["private_historical_grant"]["controller_pid"] == os.getppid()
            assert report["storage_supervision_invocation"] == token
            assert report["storage_supervision_file"] == str(progress)
            approval = bundle["approval"]
            assert approval["phase"] == "source" and approval["phases"] == ["source"]

            class SourcePhaseStoreSentinel:
                # Source-phase substep selection returns before Store access.

                def path(self, *args, **kwargs):
                    raise AssertionError("source-phase handoff unexpectedly accessed the Store")

            authority_path.write_text(json.dumps({{"worker_pid": os.getpid(), "parent_pid": os.getppid(), "pgid": os.getpgrp()}}) + "\\n")
            return {{"identity": bundle["identity"], "approval": approval, "store": SourcePhaseStoreSentinel(), "control_root": control_arg, "report_path": report_path, "report": report}}
        def synthetic_action(authority, action_phase, action_substep):
            assert authority_path.is_file()
            assert action_phase == phase and action_substep == substep
            assert os.environ.get("PYTHONDONTWRITEBYTECODE") == "1"
            state = production._historical_validate_storage_handoff(
                directory, report_path, progress, invocation_token=token
            )
            time.sleep(2.2)
            state = production._historical_validate_storage_handoff(
                directory, report_path, progress, invocation_token=token
            )
            grandchild_command = [
                str(production._historical_venv_root(control_root) / "bin" / "python"),
                {str(grandchild_script_path)!r},
            ]
            grandchild = subprocess.run(
                grandchild_command,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert grandchild.returncode == 0, grandchild.stderr
            grandchild_result = json.loads(grandchild.stdout)
            assert grandchild_result["dont_write_bytecode"] is True
            assert grandchild_result["bytecode_env"] == "1"
            assert grandchild_result["module_marker"] == "handoff-bytecode-probe-imported"
            action_path.write_text(json.dumps({{"worker_pid": os.getpid(), "parent_pid": os.getppid(), "pgid": os.getpgrp(), "sample_count": state["sample_count"], "grandchild_command": grandchild_command, "grandchild_result": grandchild_result}}) + "\\n")
            return {{"synthetic_action": "reached", "sample_count": state["sample_count"]}}
        production._historical_authority = synthetic_authority
        production._historical_worker_action = synthetic_action
        result = production._historical_worker_main(
            directory, control_root, grant_id, launch_sha256, phase, substep, token, progress
        )
        raise SystemExit(result)
        """
    )
    worker_path.write_text(worker_code)

    pinned_python = str(production._historical_venv_root(control_root) / "bin" / "python")
    worker_command = [
        pinned_python,
        "-B",
        str(worker_path),
        str(PRODUCTION_PATH),
        str(production_root),
        str(control_root),
        grant_id,
        launch_sha256,
        "source",
        "maintenance",
        token,
        str(storage_progress),
        str(report_path),
        str(authority_path),
        str(action_path),
    ]
    config = {
        "root": str(ROOT),
        "production_path": str(PRODUCTION_PATH),
        "directory": str(production_root),
        "operator": str(operator),
        "control_root": str(control_root),
        "grant_id": grant_id,
        "launch_sha256": launch_sha256,
        "phase": "source",
        "substep": "maintenance",
        "token": token,
        "progress": str(storage_progress),
        "report_path": str(report_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "worker_path": str(worker_path),
        "worker_command": worker_command,
        "authority_path": str(authority_path),
        "action_path": str(action_path),
        "result_path": str(controller_result_path),
    }
    config_path.write_text(json.dumps(config, sort_keys=True) + "\n")
    controller_code = textwrap.dedent(
        """\
        import json, os, sys, time
        from pathlib import Path

        def private_controller(directory, control_root, grant_id):
            production = sys.modules["__main__"]
            import historical_grants as grants

            assert "PYTHONDONTWRITEBYTECODE" not in os.environ
            os.environ["__PYVENV_LAUNCHER__"] = sys.executable
            config = json.loads(Path(os.environ["KALSHI_TEST_HANDOFF_CONFIG"]).read_text())
            directory = Path(directory)
            control_root = Path(control_root)
            production._historical_assert_pinned_python(control_root)
            report_path = Path(config["report_path"])
            report = production._strict_json(report_path)
            running = report["private_historical_grant"]
            running.update({"grant_id": grant_id, "launch_sha256": config["launch_sha256"], "phases": ["source"], "phase": "source", "substep": "maintenance", "controller_pid": os.getpid(), "storage_invocation_token": config["token"], "storage_progress_path": config["progress"], "state": "supervised-child-pending"})
            production._atomic_report(report_path, report)
            command = config["worker_command"]
            with production._run_lock(directory):
                result = production._invoke(
                    command,
                    directory=directory,
                    operator=Path(config["operator"]),
                    report=report,
                    report_path=report_path,
                    deadline=time.monotonic() + 45,
                    wall_deadline_epoch_override=time.time() + 45,
                    stdout_path=Path(config["stdout_path"]),
                    stderr_path=Path(config["stderr_path"]),
                    phase_on_start="historical-source-maintenance",
                    storage_supervision=True,
                    storage_invocation_token=config["token"],
                    storage_progress_path=Path(config["progress"]),
                    supervisor_handoff={"directory": config["directory"], "control_root": config["control_root"], "grant_id": config["grant_id"], "launch_sha256": config["launch_sha256"], "phase": config["phase"], "substep": config["substep"], "storage_invocation_token": config["token"], "storage_progress_path": config["progress"]},
                )
            production._atomic_report(report_path, report)
            output = {"controller_pid": os.getpid(), "controller_pgid": os.getpgrp(), "worker_status": result, "report_path": config["report_path"], "progress_path": config["progress"], "bytecode_env_absent": "PYTHONDONTWRITEBYTECODE" not in os.environ}
            Path(config["result_path"]).write_text(json.dumps(output, sort_keys=True) + "\\n")
            print(json.dumps(output, sort_keys=True))
            return 0
        """
    )
    controller_path.write_text(controller_code)
    sitecustomize_path.write_text(
        textwrap.dedent(
            """\
            import importlib.util, os, sys
            helper_path = os.environ["KALSHI_TEST_HANDOFF_CONTROLLER"]
            spec = importlib.util.spec_from_file_location("positive_handoff_controller", helper_path)
            helper = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(helper)
            def route_private_controller(frame, event, arg):
                if event == "call" and frame.f_code.co_name == "main" and frame.f_globals.get("__name__") == "__main__":
                    frame.f_globals["_historical_control_main"] = helper.private_controller
                    sys.settrace(None)
                    return None
                return route_private_controller
            sys.settrace(route_private_controller)
            """
        )
    )
    command = [
        pinned_python,
        "-B",
        str(PRODUCTION_PATH),
        "--_historical-control",
        str(production_root),
        str(control_root),
        grant_id,
    ]
    source_bytecode_before = _bytecode_files(SCRIPTS, ROOT / "src")
    runtime_bytecode_before = _bytecode_files(grandchild_source_root)
    process_environment = {
        **os.environ,
        "__PYVENV_LAUNCHER__": sys.executable,
        "PYTHONPATH": str(evidence),
        "KALSHI_TEST_HANDOFF_CONFIG": str(config_path),
        "KALSHI_TEST_HANDOFF_CONTROLLER": str(controller_path),
    }
    process_environment.pop("PYTHONDONTWRITEBYTECODE", None)
    assert "PYTHONDONTWRITEBYTECODE" not in process_environment
    started_at = time.time()
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=process_environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        controller_stdout, controller_stderr = process.communicate(timeout=75)
    except subprocess.TimeoutExpired:
        process.kill()
        controller_stdout, controller_stderr = process.communicate(timeout=5)
        (evidence / "controller.stdout.log").write_text(controller_stdout)
        (evidence / "controller.stderr.log").write_text(controller_stderr)
        pytest.fail(f"synthetic controller exceeded its hard timeout; evidence: {evidence}")
    (evidence / "controller.stdout.log").write_text(controller_stdout)
    (evidence / "controller.stderr.log").write_text(controller_stderr)
    (evidence / "invocation.json").write_text(
        json.dumps(
            {
                "command": command,
                "controller_pid": process.pid,
                "started_at_epoch": started_at,
                "returncode": process.returncode,
            },
            sort_keys=True,
        )
        + "\n"
    )
    assert process.returncode == 0, (
        f"positive controller failed; evidence: {evidence}; "
        f"stdout={controller_stdout!r}; stderr={controller_stderr!r}"
    )
    assert _bytecode_files(SCRIPTS, ROOT / "src") == source_bytecode_before, (
        f"controller or worker created source bytecode; evidence: {evidence}"
    )
    assert _bytecode_files(grandchild_source_root) == runtime_bytecode_before, (
        f"unflagged Python grandchild created runtime source bytecode; evidence: {evidence}"
    )
    result = json.loads(controller_result_path.read_text())
    assert result["controller_pid"] == process.pid
    assert result["worker_status"] == [0, None, True]
    assert result["bytecode_env_absent"] is True
    controller_identity = json.loads(controller_ps_path.read_text())
    assert controller_identity["stdout"] == " ".join(command)
    assert controller_identity["worker_returncode"] == 0
    assert controller_identity["worker_command"] == " ".join(worker_command)
    assert all(controller_identity["checks"].values())
    assert authority_path.is_file()
    assert action_path.is_file()
    authority = json.loads(authority_path.read_text())
    action = json.loads(action_path.read_text())
    assert authority["parent_pid"] == result["controller_pid"]
    assert authority["worker_pid"] == action["worker_pid"]
    assert authority["pgid"] == action["pgid"] == action["worker_pid"]
    assert action["parent_pid"] == result["controller_pid"]
    assert action["sample_count"] >= 1
    assert action["grandchild_command"] == [pinned_python, str(grandchild_script_path)]
    assert "-B" not in action["grandchild_command"]
    assert action["grandchild_result"] == {
        "dont_write_bytecode": True,
        "bytecode_env": "1",
        "module_marker": "handoff-bytecode-probe-imported",
    }
    final_report = production._strict_json(report_path)
    assert final_report["active_process_group"] is None
    assert final_report["storage_supervision_invocation"] == token
    assert final_report["storage_supervision_file"] == str(storage_progress)
    assert (
        production._historical_validate_storage_handoff(
            production_root, report_path, storage_progress, invocation_token=token
        )["sample_count"]
        >= 1
    )
    receipt = grants.read_json(
        control_root / "grants" / grant_id / "substeps" / "source-maintenance.json"
    )
    assert receipt["grant_id"] == grant_id
    assert receipt["phase"] == "source" and receipt["substep"] == "maintenance"
    with production._run_lock(production_root):
        pass
    (evidence / "summary.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "controller_pid": result["controller_pid"],
                "worker_pid": action["worker_pid"],
                "worker_pgid": action["pgid"],
                "storage_sample_count": action["sample_count"],
                "evidence_directory": str(evidence),
                "control_root": str(control_root),
            },
            sort_keys=True,
        )
        + "\n"
    )
