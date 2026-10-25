from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kalshi_events import capture as acquisition
from kalshi_events.common import Store

SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-acquisition.py"
SPEC = importlib.util.spec_from_file_location("acquisition_probe", SCRIPT)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
REAL_RECEIPT_SUMMARY = probe._receipt_summary


def test_rates_default_and_validation() -> None:
    assert probe.parse_rates(None) == [5.0]
    assert probe.parse_rates(["5", "10", "20"]) == [5.0, 10.0, 20.0]
    for values in ([], ["1", "1"], ["nan"], ["inf"], ["0"], ["20.01"], ["1", "2", "3", "4"]):
        with pytest.raises(ValueError):
            probe.parse_rates(values)


def test_output_must_be_new_external_and_without_symlinks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ValueError, match="outside"):
        probe.validate_output(repo / "out", repo)
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(ValueError, match="already exist"):
        probe.validate_output(existing, repo)
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinks"):
        probe.validate_output(link / "new", repo)
    broken_link = tmp_path / "broken-link"
    broken_link.symlink_to(tmp_path / "missing", target_is_directory=True)
    with pytest.raises(ValueError, match="symlinks"):
        probe.validate_output(broken_link, repo)


class FakePopen:
    commands: list[list[str]] = []
    instances: list[FakePopen] = []
    wait_timeouts: list[float | None] = []
    behavior: str = "success"
    startup: Any = None
    receipt: dict[str, Any] | None = None
    worker_accounting: dict[str, Any] = {
        "attempts": 1,
        "downloaded_or_reserved_bytes": 123,
        "accounting_complete": True,
        "retained_bytes": 100,
        "temporary_bytes": 0,
    }
    worker_metrics: dict[str, Any] | None = {
        "counters": {"requests": 1},
        "phases_seconds": {"request": 0.1},
        "store_accounted_high_water_bytes": {"temporary": 10},
        "endpoint_families": {},
        "elapsed_seconds": 0.2,
        "peak_http_concurrency": 1,
        "peak_rss_bytes": {"self": 10, "children": 0},
    }
    worker_returncode = 0
    invalid_worker_report = False
    missing_capture = False

    def __init__(
        self, command: list[str], *, stdout: Any, stderr: Any, env: dict[str, str], shell: bool
    ):
        assert shell is False
        self.commands.append(command)
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = 0
        self.killed = False
        self.behavior = type(self).behavior
        self.instances.append(self)
        if self.startup is not None:
            self.startup()
        if self.behavior != "timeout":
            result_path = Path(command[command.index("--result") + 1])
            report = {
                "version": 1,
                "command": "capture",
                "returncode": self.worker_returncode,
                "capture": None
                if self.missing_capture
                else {
                    "capture_id": "capture-1",
                    "complete": self.worker_returncode == 0,
                    "error": None,
                },
                "accounting": dict(self.worker_accounting),
                "receipts": {
                    "data_pages": 1,
                    "control_pages": 0,
                    "control_responses": 0,
                    "non_data_responses": 0,
                    "attempts": self.worker_accounting["attempts"],
                    "retry_attempts": 0,
                    "encoded_bytes": 123,
                    "http_429": False,
                    "observed_encodings": {"identity": 1},
                    "response_statuses": {"200": 1},
                },
                "metrics": self.worker_metrics,
                "timings": {
                    "acquisition_seconds": 0.1,
                    "accounting_seconds": 0.01,
                    "worker_seconds": 0.12,
                },
                "error": None,
            }
            if self.invalid_worker_report:
                report["receipts"]["attempts"] = True
            result_path.write_text(json.dumps(report), encoding="utf-8")

    def wait(self, timeout: float | None = None) -> int:
        self.wait_timeouts.append(timeout)
        if self.behavior == "timeout" and not self.killed:
            raise subprocess.TimeoutExpired("probe", timeout)
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


def _patch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(probe, "_account", lambda root: pytest.fail("parent accounting ran"))
    FakePopen.behavior = "success"
    FakePopen.startup = None
    FakePopen.receipt = None
    FakePopen.worker_accounting = {
        "attempts": 1,
        "downloaded_or_reserved_bytes": 123,
        "accounting_complete": True,
        "retained_bytes": 100,
        "temporary_bytes": 0,
    }
    FakePopen.worker_metrics = {
        "counters": {"requests": 1},
        "phases_seconds": {"request": 0.1},
        "store_accounted_high_water_bytes": {"temporary": 10},
        "endpoint_families": {},
        "elapsed_seconds": 0.2,
        "peak_http_concurrency": 1,
        "peak_rss_bytes": {"self": 10, "children": 0},
    }
    FakePopen.worker_returncode = 0
    FakePopen.invalid_worker_report = False
    FakePopen.missing_capture = False


def _cli_arguments(command: list[str]) -> list[str]:
    return command[command.index("--") + 1 :]


def test_serial_identity_then_gzip_with_fixed_bounded_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.behavior = "success"
    output = tmp_path / "probe"
    report = probe.run(output, [5.0])
    assert report["complete"] is True
    assert [sample["compression"] for sample in report["samples"]] == ["identity", "gzip"]
    assert [sample["accounting"] for sample in report["samples"]] == ["verified", "verified"]
    assert report["accounting"] == {
        "attempts": 2,
        "downloaded_bytes": 246,
        "retained_bytes": 200,
        "temporary_bytes": 0,
        "temporary_high_water_bytes": 10,
    }
    assert report["limits"]["total"]["attempts"] == 72
    assert report["limits"]["total"]["seconds"] == 180.0
    assert report["limits"]["total"]["downloaded_bytes"] == 256 * probe.MIB
    assert report["limits"]["per_sample"]["attempts"] == 12
    assert report["limits"]["per_sample"]["seconds"] == 30.0
    assert len(FakePopen.commands) == 2
    identity, gzip = FakePopen.commands
    identity_cli, gzip_cli = _cli_arguments(identity), _cli_arguments(gzip)
    assert (
        identity[:2]
        == gzip[:2]
        == [
            probe.sys.executable,
            str(probe.SOURCE_ROOT / "scripts" / "probe-acquisition-worker.py"),
        ]
    )
    assert identity_cli[identity_cli.index("--max-requests") + 1] == "12"
    assert identity_cli[identity_cli.index("--max-response-bytes") + 1] == str(32 * probe.MIB)
    assert identity_cli[identity_cli.index("--max-decoded-response-bytes") + 1] == str(
        64 * probe.MIB
    )
    assert gzip_cli[gzip_cli.index("--max-requests") + 1] == "12"
    assert gzip_cli[gzip_cli.index("--max-download-bytes") + 1] == str(64 * probe.MIB)
    assert identity_cli[identity_cli.index("--compression") + 1] == "identity"
    assert gzip_cli[gzip_cli.index("--compression") + 1] == "gzip"
    assert identity_cli[identity_cli.index("--rate-policy") + 1] == "fixed"
    assert gzip_cli[gzip_cli.index("--rate-policy") + 1] == "fixed"
    assert report["samples"][1]["receipts"]["observed_encodings"] == {"identity": 1}
    assert report["samples"][1]["encoding_fallback"] is True
    assert (output / "reports" / "summary.json").is_file()
    assert (output / "logs" / "sample-01.stdout.json").is_file()
    assert (output / "metrics" / "sample-01.json").parent.is_dir()


def test_unknown_accounting_stops_remaining_scenarios_and_keeps_partial_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.invalid_worker_report = True
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.wait_timeouts = []
    output = tmp_path / "probe"
    report = probe.run(output, [5.0, 10.0])
    assert report["complete"] is False
    assert report["stop_reason"] == "worker did not exit cleanly or returned untrusted accounting"
    assert len(FakePopen.commands) == 1
    saved = json.loads((output / "reports" / "summary.json").read_text())
    assert saved["samples"][0]["accounting"] == "unknown"
    assert saved["accounting"]["attempts"] is None


def test_timeout_kills_and_reaps_child_then_preserves_evidence_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.wait_timeouts = []
    FakePopen.behavior = "timeout"
    report = probe.run(tmp_path / "probe", [10.0])
    sample = report["samples"][0]
    assert sample["status"] == "timed_out"
    assert sample["stop_reason"] == "sample deadline exceeded; child terminated"
    assert len(FakePopen.commands) == 1
    assert FakePopen.instances[0].killed is True
    assert Path(sample["root"]).is_dir()
    assert Path(sample["logs"]["stderr"]).is_file()


def test_unexpected_capture_failure_stops_matrix_after_supervised_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.worker_returncode = 1
    FakePopen.commands = []
    report = probe.run(tmp_path / "probe", [5.0, 10.0])
    assert len(FakePopen.commands) == 1
    assert report["samples"][0]["status"] == "partial"
    assert report["samples"][0]["accounting"] == "verified"
    assert report["samples"][0]["stop_reason"] == "capture worker reported an unexpected failure"
    assert report["accounting"]["attempts"] == 1


def test_success_exit_without_capture_result_is_untrusted_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.missing_capture = True
    FakePopen.commands = []
    report = probe.run(tmp_path / "probe", [5.0, 10.0])
    assert len(FakePopen.commands) == 1
    assert report["samples"][0]["accounting"] == "unknown"
    assert report["samples"][0]["status"] == "invalid_worker_report"
    assert report["stop_reason"] == "worker did not exit cleanly or returned untrusted accounting"


def test_committed_page_count_uses_validated_capture_rows_not_journal_lines(store, factory) -> None:
    state = acquisition.capture(store, client_factory=factory)
    restored = acquisition.read_capture(store, state["capture_id"])
    expected = len(acquisition.page_receipts(store, restored))
    journal = store.path(f"captures/{state['capture_id']}/pages.jsonl")
    journal_lines = len(journal.read_bytes().splitlines())
    summary = probe._receipt_summary(store.root, state["capture_id"])
    assert expected > 0
    assert journal_lines > expected
    assert summary["committed_pages"] == expected


def test_accounting_does_not_initialize_missing_operator_root(tmp_path: Path) -> None:
    root = tmp_path / "empty-root"
    root.mkdir()
    assert probe._account(root) is None
    assert not (root / "root.json").exists()


def test_existing_temporary_files_reduce_next_sample_allowance_and_accumulate_high_water(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.worker_accounting["temporary_bytes"] = 40
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.wait_timeouts = []
    FakePopen.behavior = "success"
    report = probe.run(tmp_path / "probe", [10.0])
    second_cli = _cli_arguments(FakePopen.commands[1])
    assert second_cli[second_cli.index("--max-temp-bytes") + 1] == str(
        probe.TOTAL["temporary_bytes"] - 40
    )
    assert report["accounting"]["temporary_bytes"] == 80
    assert report["accounting"]["temporary_high_water_bytes"] == 50


def test_missing_metrics_stops_even_when_receipt_accounting_is_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.worker_accounting["temporary_bytes"] = 17
    FakePopen.worker_metrics = None
    FakePopen.commands = []
    FakePopen.instances = []
    report = probe.run(tmp_path / "probe", [10.0])
    assert len(FakePopen.commands) == 1
    assert report["stop_reason"] == "accounting or storage usage is unknown"
    assert report["samples"][0]["accounting"] == "unknown"
    assert report["accounting"]["temporary_high_water_bytes"] is None


def test_popen_startup_consumes_parent_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    clock = [178.0]
    monkeypatch.setattr(probe, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.wait_timeouts = []
    FakePopen.behavior = "success"
    FakePopen.startup = staticmethod(lambda: clock.__setitem__(0, clock[0] + 1.0))
    try:
        report = probe.run(tmp_path / "probe", [10.0], invocation_started=0.0)
    finally:
        FakePopen.startup = None
    assert FakePopen.wait_timeouts[0] == pytest.approx(1.0)
    assert report["complete"] is False
    assert report["stop_reason"] == "sample deadline exceeded; child terminated"


def test_accounting_includes_orphan_temporary_files(tmp_path: Path) -> None:
    root = tmp_path / "root"
    store = Store(root)
    orphan = store.path("raw/attempt/.write-body-orphan")
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"orphan-temp")
    actual = probe._account(root)
    assert actual is not None
    assert actual["temporary_bytes"] == len(b"orphan-temp")


@pytest.mark.parametrize("bad_value", ["bad", True, -1])
def test_receipt_with_malformed_accounting_fields_is_not_trusted(
    tmp_path: Path, bad_value: Any
) -> None:
    root = tmp_path / "root"
    manifest = root / "raw" / "attempt" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "status": 200,
                "accounting_complete": True,
                "size": 5,
                "sha256": "a" * 64,
                "downloaded_bytes": bad_value,
            }
        )
    )
    summary = probe._receipt_summary(root, None)
    assert summary["valid"] is False
    assert summary["observed_encodings"] == {}


def test_error_receipt_does_not_claim_observed_identity_encoding(tmp_path: Path) -> None:
    root = tmp_path / "root"
    manifest = root / "raw" / "attempt" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "status": 200,
                "accounting_complete": True,
                "size": 5,
                "sha256": "a" * 64,
                "downloaded_bytes": 5,
                "error": "rejected response",
            }
        )
    )
    summary = probe._receipt_summary(root, None)
    assert summary["valid"] is True
    assert summary["observed_encodings"] == {}


def test_malformed_receipt_field_stops_with_partial_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_success(monkeypatch)
    FakePopen.commands = []
    FakePopen.instances = []
    FakePopen.invalid_worker_report = True
    report = probe.run(tmp_path / "probe", [10.0])
    assert report["complete"] is False
    assert report["stop_reason"] == "worker did not exit cleanly or returned untrusted accounting"
    assert report["samples"][0]["accounting"] == "unknown"
    assert (tmp_path / "probe" / "reports" / "summary.json").is_file()


@pytest.mark.parametrize("bad_peak", ["100", True, -1, float("nan"), {"bytes": 100}])
def test_malformed_temporary_peak_stops_and_marks_storage_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_peak: Any
) -> None:
    _patch_success(monkeypatch)
    FakePopen.worker_metrics["store_accounted_high_water_bytes"]["temporary"] = bad_peak
    report = probe.run(tmp_path / "probe", [10.0])
    assert report["complete"] is False
    assert report["stop_reason"] == "worker did not exit cleanly or returned untrusted accounting"
    assert report["accounting"]["temporary_bytes"] is None
    assert report["accounting"]["temporary_high_water_bytes"] is None
