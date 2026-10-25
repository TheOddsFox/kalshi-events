"""Exercise production orchestration offline through its subprocess boundary."""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
from types import SimpleNamespace

import pytest
import test_production_activation as activation_tests
from conftest import FakeClient

from kalshi_events import capture, publish, transport
from kalshi_events.common import BudgetExceeded, Store
from kalshi_events.transport import accounting

production = activation_tests.production
certified_production_root = activation_tests.certified_production_root


@pytest.fixture
def workflow(production, certified_production_root, tmp_path, monkeypatch):
    source, manifest = certified_production_root
    raw_accounting = accounting(Store(source))
    state = SimpleNamespace(events=[], outcomes=[], commands=[], source=source, manifest=manifest)
    state.actual = False
    snapshot = {
        **raw_accounting,
        "storage": Store(source).usage(),
        "root_id": json.loads((source / "root-id.json").read_text())["root_id"],
    }

    class Awake:
        pid = 424242

        def __init__(self, command, **kwargs):
            assert command[0] == "caffeinate", "unexpected real subprocess escaped fake boundary"
            assert kwargs["start_new_session"] is True
            state.events.append("awake")

        def poll(self):
            return None

    actual_popen = production.subprocess.Popen
    resource_guard = production._resource_guard

    def popen(command, **kwargs):
        if command[0] == "caffeinate":
            return Awake(command, **kwargs)
        return actual_popen(command, **kwargs)

    def run_child(command, *, directory, operator, paths, report, deadline, label, **kwargs):
        assert deadline > production.time.monotonic()
        assert command[0] == sys.executable
        state.commands.append(command)
        state.events.append(label)
        stdout = paths["logs"] / f"{label}.stdout.json"
        stderr = paths["logs"] / f"{label}.stderr.log"
        stderr.touch()
        no_result = False
        if label == "refresh":
            settings = state.outcomes.pop(0) if state.outcomes else {}
            no_result = settings.pop("_no_result", False)
            if state.actual:
                values = json.loads(command[-1])
                values.update(settings)
                captured = io.StringIO()
                with contextlib.redirect_stdout(captured):
                    code = production._refresh_worker_main(
                        operator,
                        paths["report"],
                        paths["metrics"],
                        paths["capture_notification"],
                        command[-3],
                        None if command[-2] == "-" else command[-2],
                        json.dumps(values),
                    )
                value = json.loads(captured.getvalue())
                production._merge_capture_notification(report, paths["capture_notification"])
            else:
                if not operator.exists():
                    shutil.copytree(source, operator)
                production._bind_owner(directory, snapshot["root_id"], command[-3])
                value = {
                    "complete": True,
                    "capture_id": manifest["capture_id"],
                    "capture_complete": True,
                    "resumable": True,
                    "publication_state": "verified",
                    "accounting_complete": True,
                    "requests": raw_accounting["attempts"],
                    "downloaded_bytes": raw_accounting["downloaded_or_reserved_bytes"],
                    "source_release_id": manifest["source_release_id"],
                    "output": str(operator / "releases" / manifest["source_release_id"]),
                    "error": None,
                    **settings,
                }
                code = value.pop("returncode", 0)
        elif label == "activation":
            current = publish.current(Store(operator), catalogue_version=2)
            value = production.activate_current(
                operator, directory, expected_release_id=current["source_release_id"]
            )
            code = 0
        else:
            pytest.fail(f"unexpected child stage {label}")
        if no_result:
            production._write_capture_notification(
                paths["capture_notification"], manifest["capture_id"], snapshot["root_id"]
            )
            production._merge_capture_notification(report, paths["capture_notification"])
        else:
            stdout.write_text(json.dumps(value))
        return code, None, True, stdout, stderr

    def independently_verify(release, *_args, **_kwargs):
        state.events.append("independent-verification")
        return publish.verify_release(release)

    monkeypatch.setattr(production, "_revision", lambda: "a" * 40)
    monkeypatch.setattr(
        production,
        "_system_info",
        lambda directory: {"filesystem_device": directory.stat().st_dev},
    )
    monkeypatch.setattr(production, "_boot_stamp", lambda: "same boot")
    monkeypatch.setattr(production.subprocess, "Popen", popen)
    monkeypatch.setattr(
        production, "_kill_awake_guard", lambda _awake: state.events.append("sleep-restored")
    )
    monkeypatch.setattr(production, "_run_external_child", run_child)
    monkeypatch.setattr(
        production,
        "_resource_guard",
        lambda *a, **kw: resource_guard(*a, **kw) if state.actual else snapshot,
    )
    monkeypatch.setattr(
        production,
        "_child_accounting",
        lambda operator, *_a, **_kw: production._read_accounting_worker(operator),
    )
    monkeypatch.setattr(
        production,
        "_root_summary",
        lambda operator, *_a, **_kw: {
            "root_id": snapshot["root_id"],
            "current_v2": publish.current(Store(operator), catalogue_version=2),
        },
    )
    monkeypatch.setattr(production, "_release_verify", independently_verify)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    state.directory = tmp_path / "production"
    actual_capture = capture.capture
    monkeypatch.setattr(
        capture,
        "capture",
        lambda *a, **kw: actual_capture(*a, client_factory=FakeClient, **kw),
    )
    monkeypatch.setattr(transport, "source_url", lambda: transport.BASE_URL)
    return state


def test_full_workflow_verifies_before_activating_a_single_production_database(
    production, workflow
):
    result, code = production.run_production(workflow.directory, full=True)
    assert code == 0, result
    assert result["phase"] == "complete"
    assert result["capture_complete"] is True
    assert result["publication_state"] == "verified"
    assert result["production_state"] == "verified"
    assert workflow.events == [
        "awake",
        "refresh",
        "independent-verification",
        "activation",
        "sleep-restored",
    ]
    assert (workflow.directory / "production.duckdb").is_file()
    assert not (workflow.directory / "operator" / "current.json").exists()
    command = workflow.commands[0]
    assert command[-3] == "full"
    limits = json.loads(command[-1])
    assert limits["max-rate"] == 5
    assert limits["compression"] == "gzip"
    assert limits["max-requests"] == 850000


def test_complete_capture_budget_stop_preserves_id_and_exact_resume_subtracts_prior_consumption(
    production, workflow
):
    workflow.outcomes = [
        {
            "returncode": 3,
            "complete": False,
            "capture_complete": True,
            "publication_state": "unknown",
            "error": "publication budget exhausted",
        },
        {"requests": 0, "downloaded_bytes": 0},
    ]
    partial, code = production.run_production(workflow.directory, full=True)
    assert code == 3
    assert partial["capture_id"] == workflow.manifest["capture_id"]
    assert partial["capture_complete"] is True
    assert partial["production_state"] != "verified"
    assert not (workflow.directory / "production.duckdb").exists()
    assert "independent-verification" not in workflow.events

    complete, code = production.run_production(
        workflow.directory, capture_id=workflow.manifest["capture_id"]
    )
    assert code == 0
    assert complete["production_state"] == "verified"
    command = workflow.commands[1]
    assert command[-3] == "resume"
    assert command[-2] == workflow.manifest["capture_id"]
    assert json.loads(command[-1])["max-requests"] == (
        production.RUN_LIMITS["attempts"] - partial["requests"]
    )
    assert complete["requests"] == partial["requests"]
    assert complete["downloaded_bytes"] == partial["downloaded_bytes"]


def test_unknown_child_accounting_blocks_verification_activation_and_follow_on_networking(
    production, workflow
):
    workflow.outcomes = [{"accounting_complete": False, "requests": None, "downloaded_bytes": None}]
    partial, code = production.run_production(workflow.directory, full=True)
    assert code != 0
    assert partial["accounting_complete"] is False
    assert partial["requests"] is None
    assert partial["downloaded_bytes"] is None
    assert partial["production_state"] != "verified"
    assert workflow.events == ["awake", "refresh", "sleep-restored"]
    assert not (workflow.directory / "production.duckdb").exists()


def test_activation_only_never_starts_acquisition(production, workflow):
    shutil.copytree(workflow.source, workflow.directory / "operator")
    (workflow.directory / "runs").mkdir()
    production._bind_owner(
        workflow.directory,
        json.loads((workflow.source / "root-id.json").read_text())["root_id"],
        "full",
    )
    result, code = production.run_production(workflow.directory, activate_only=True)
    assert code == 0, result
    assert result["production_state"] == "verified"
    assert result["requests"] is None
    assert result["downloaded_bytes"] is None
    assert result["accounting_complete"] is False
    assert result["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }
    assert result["operator_accounting"]["state"] == "verified"
    assert result["operator_accounting"]["requests"] > 0
    assert workflow.events == ["awake", "independent-verification", "activation", "sleep-restored"]
    assert all("refresh" not in command for command in workflow.commands)


def test_real_small_full_capture_build_and_activation_have_coherent_identity(production, workflow):
    workflow.actual = True
    result, code = production.run_production(workflow.directory, full=True)
    assert code == 0, result
    assert result["accounting_complete"] is True
    assert result["capture_complete"] is True
    assert result["production_state"] == "verified"
    actual = accounting(Store(workflow.directory / "operator"))
    assert result["requests"] == actual["attempts"]
    assert result["downloaded_bytes"] == actual["downloaded_or_reserved_bytes"]
    assert result["activation"]["capture_id"] == result["capture_id"]
    marker = json.loads((workflow.directory / production.OWNER_MARKER).read_text())
    assert result["root_id"] == marker["root_id"]


def test_real_partial_full_capture_has_identity_before_build_and_resumes_exactly(
    production, workflow
):
    workflow.actual = True
    workflow.outcomes = [{"max-requests": 1}]
    partial, code = production.run_production(workflow.directory, full=True)
    assert code == 3, partial
    assert partial["capture_complete"] is False
    assert partial["accounting_complete"] is True
    assert partial["root_id"]
    assert partial["requests"] == 1
    capture_id = partial["capture_id"]
    assert capture_id
    assert not (workflow.directory / "production.duckdb").exists()
    complete, code = production.run_production(workflow.directory, capture_id=capture_id)
    assert code == 0, complete
    assert complete["capture_id"] == capture_id
    assert complete["root_id"] == partial["root_id"]
    assert complete["requests"] > partial["requests"]
    assert complete["accounting_complete"] is True


def test_real_complete_capture_resume_does_not_repeat_network_acquisition(
    production, workflow, monkeypatch
):
    workflow.actual = True
    original = publish.publish

    def budget_stop(*_a, **_kw):
        raise BudgetExceeded("publication budget exhausted")

    monkeypatch.setattr(publish, "publish", budget_stop)
    partial, code = production.run_production(workflow.directory, full=True)
    assert code == 3, partial
    assert partial["capture_complete"] is True
    assert partial["accounting_complete"] is True
    monkeypatch.setattr(publish, "publish", original)
    monkeypatch.setattr(
        FakeClient, "get", lambda *_a, **_kw: pytest.fail("completed capture used networking")
    )
    complete, code = production.run_production(workflow.directory, capture_id=partial["capture_id"])
    assert code == 0, complete
    assert complete["requests"] == partial["requests"]
    assert complete["downloaded_bytes"] == partial["downloaded_bytes"]
    assert complete["production_state"] == "verified"


@pytest.mark.parametrize("invalid", ["expired", "backward_clock", "reboot", "unknown", "root"])
def test_invalid_recovery_context_never_starts_another_child(production, workflow, invalid):
    workflow.outcomes = [{"returncode": 3, "complete": False, "publication_state": "unknown"}]
    partial, code = production.run_production(workflow.directory, full=True)
    assert code == 3
    path = next((workflow.directory / "runs").glob("*/session.json"))
    saved = json.loads(path.read_text())
    if invalid == "expired":
        saved["deadline_epoch"] = production.time.time() - 1
    elif invalid == "backward_clock":
        saved["last_observed_epoch"] = production.time.time() + 100
    elif invalid == "reboot":
        saved["boot_stamp"] = "a different boot"
    elif invalid == "unknown":
        saved["accounting_complete"] = False
    else:
        saved["root_id"] = "f" * 32
    path.write_text(json.dumps(saved))
    prior_events = workflow.events.copy()
    with pytest.raises(production.RunnerError):
        production.run_production(workflow.directory, capture_id=partial["capture_id"])
    assert workflow.events == prior_events


def test_missing_child_result_preserves_uncertainty_about_consumption_and_promotion(
    production, workflow
):
    workflow.outcomes = [{"_no_result": True, "returncode": -9}]
    result, code = production.run_production(workflow.directory, full=True)
    assert code != 0
    assert result["capture_id"] == workflow.manifest["capture_id"]
    assert result["resumable"] is None
    assert result["accounting_complete"] is False
    assert result["requests"] is None
    assert result["downloaded_bytes"] is None
    assert result["capture_complete"] is None
    assert result["publication_state"] == "unknown"
    assert result["production_state"] != "verified"
    assert workflow.events == ["awake", "refresh", "sleep-restored"]


def test_activation_only_enforces_storage_limits_before_promoting(
    production, workflow, monkeypatch
):
    shutil.copytree(workflow.source, workflow.directory / "operator")
    (workflow.directory / "runs").mkdir()
    production._bind_owner(
        workflow.directory,
        json.loads((workflow.source / "root-id.json").read_text())["root_id"],
        "full",
    )

    def storage_failure(*_args, **_kwargs):
        raise production.RunnerError("production aggregate storage limit exceeded")

    monkeypatch.setattr(production, "_resource_guard", storage_failure)
    result, code = production.run_production(workflow.directory, activate_only=True)
    assert code != 0
    assert "storage limit exceeded" in result["error"]
    assert result["production_state"] != "verified"
    assert not (workflow.directory / "production.duckdb").exists()
    assert "activation" not in workflow.events


def test_failed_post_child_accounting_does_not_leave_prior_zero_as_confirmed_total(
    production, workflow, monkeypatch
):
    def failed_accounting(*_args, **_kwargs):
        raise production.RunnerError("durable receipt accounting is unknown")

    monkeypatch.setattr(production, "_resource_guard", failed_accounting)
    result, code = production.run_production(workflow.directory, full=True)
    assert code != 0
    assert result["accounting_complete"] is False
    assert result["requests"] is None
    assert result["downloaded_bytes"] is None
    assert result["production_state"] != "verified"
    assert "activation" not in workflow.events


@pytest.mark.parametrize("exit_code", [1, 3])
def test_nonzero_refresh_exit_never_activates_even_if_its_json_claims_success(
    production, workflow, exit_code
):
    workflow.outcomes = [
        {"returncode": exit_code, "complete": True, "publication_state": "verified"}
    ]
    result, code = production.run_production(workflow.directory, full=True)
    assert code != 0
    assert result["production_state"] != "verified"
    assert not (workflow.directory / "production.duckdb").exists()
    assert workflow.events == ["awake", "refresh", "sleep-restored"]


def test_expired_workflow_budget_returns_three_before_accounting_or_network_resume(
    production, workflow, monkeypatch
):
    workflow.outcomes = [{"returncode": 3, "complete": False, "publication_state": "unknown"}]
    stopped, code = production.run_production(workflow.directory, full=True)
    assert code == 3
    prior_commands = workflow.commands.copy()
    elapsed = production.RUN_LIMITS["workflow_seconds"] + 1
    instant = production.time.monotonic() + elapsed
    wall = production.time.time() + elapsed
    monkeypatch.setattr(production.time, "monotonic", lambda: instant)
    monkeypatch.setattr(production.time, "time", lambda: wall)
    monkeypatch.setattr(
        production,
        "_resource_guard",
        lambda *_a, **_kw: pytest.fail("expired workflow started an accounting child"),
    )
    result, code = production.run_production(workflow.directory, capture_id=stopped["capture_id"])
    assert code == 3, result
    assert result["capture_id"] == stopped["capture_id"]
    assert result["production_state"] != "verified"
    assert workflow.commands == prior_commands
