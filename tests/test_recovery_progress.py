from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from kalshi_events import cli, metrics
from kalshi_events.common import BudgetExceeded, Store


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def monotonic(self) -> float:
        return self.value


def test_progress_flushes_phase_changes_and_throttles_same_phase(tmp_path, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(metrics.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(uuid, "uuid4", lambda: pytest.fail("metrics must not consume capture IDs"))
    links = []
    replacements = []
    real_link = metrics.os.link
    real_replace = metrics.os.replace

    def link(source, destination, **kwargs):
        links.append((Path(source), Path(destination)))
        return real_link(source, destination, **kwargs)

    def replace(source, destination):
        source, destination = Path(source), Path(destination)
        assert source.parent == destination.parent
        assert source.is_file() and destination.is_file()
        json.loads(destination.read_text())
        replacements.append((source, destination))
        real_replace(source, destination)
        json.loads(destination.read_text())

    monkeypatch.setattr(metrics.os, "link", link)
    monkeypatch.setattr(metrics.os, "replace", replace)
    path = tmp_path / "metrics.json.progress.json"

    with metrics.collect(progress_path=path) as measured:
        clock.value = 101.0
        measured.progress("recovery_validation", journal_records=1)
        clock.value = 115.9
        measured.progress(journal_records=2)
        assert json.loads(path.read_text())["counters"]["journal_records"] == 1
        clock.value = 116.0
        measured.progress(journal_records=3)

    final = json.loads(path.read_text())
    assert final["phase"] == "complete"
    assert final["elapsed_seconds"] == 16.0
    assert final["counters"]["journal_records"] == 3
    assert len(links) == 1
    assert len(replacements) == 3
    assert list(tmp_path.glob(".metrics-progress-*.tmp")) == []


@pytest.mark.parametrize("kind", ["existing", "symlink"])
def test_progress_refuses_preexisting_or_symlink_companions(tmp_path, kind):
    target = tmp_path / "target.json"
    target.write_text("keep\n")
    path = tmp_path / "metrics.json.progress.json"
    if kind == "existing":
        path.write_text("keep\n")
        expected = "already exists"
    else:
        path.symlink_to(target)
        expected = "symlink"

    with (
        pytest.raises((FileExistsError, OSError), match=expected),
        metrics.collect(progress_path=path),
    ):
        pytest.fail("unsafe progress path must fail before work starts")

    assert target.read_text() == "keep\n"
    assert list(tmp_path.glob(".metrics-progress-*.tmp")) == []


def test_progress_io_error_does_not_replace_operation_exception(tmp_path, monkeypatch):
    path = tmp_path / "metrics.json.progress.json"
    original = metrics.Metrics._write_progress
    calls = 0

    def fail_on_finish(self, now):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic progress fsync failure")
        return original(self, now)

    monkeypatch.setattr(metrics.Metrics, "_write_progress", fail_on_finish)
    with (
        pytest.raises(RuntimeError, match="original operation failure"),
        metrics.collect(progress_path=path) as measured,
    ):
        measured.progress("recovery_validation")
        raise RuntimeError("original operation failure")

    assert calls == 3
    assert json.loads(path.read_text())["phase"] == "recovery_validation"


def test_progress_is_noop_when_collection_has_no_companion(tmp_path):
    with metrics.collect() as measured:
        measured.progress("unknown_phase", unknown_counter=object())
        metrics.progress("unknown_phase", unknown_counter=object())

    assert list(tmp_path.iterdir()) == []


def test_cli_metrics_file_writes_companion_without_changing_result_or_report(tmp_path, capsys):
    root = tmp_path / "root"
    Store(root)
    assert cli.main(["--root", str(root), "status"]) == 0
    original_result = capsys.readouterr().out
    report_path = tmp_path / "metrics.json"

    assert cli.main(["--root", str(root), "--metrics-file", str(report_path), "status"]) == 0

    assert capsys.readouterr().out == original_result
    report = json.loads(report_path.read_text())
    progress = json.loads(Path(f"{report_path}.progress.json").read_text())
    assert report.keys() == metrics.Metrics().report().keys()
    assert progress["phase"] == "complete"
    assert progress["version"] == 1


@pytest.mark.parametrize("outcome", ["incomplete", "budget"])
def test_cli_incomplete_result_finishes_progress_as_stopped(tmp_path, capsys, monkeypatch, outcome):
    if outcome == "incomplete":
        monkeypatch.setattr(cli, "execute", lambda _args: {"complete": False})
    else:

        def exhausted(_args):
            raise BudgetExceeded("synthetic allowance exhaustion")

        monkeypatch.setattr(cli, "execute", exhausted)
    report_path = tmp_path / "metrics.json"

    assert (
        cli.main(
            [
                "--root",
                str(tmp_path / "root"),
                "--metrics-file",
                str(report_path),
                "status",
            ]
        )
        == 3
    )

    result = json.loads(capsys.readouterr().out)
    progress = json.loads(Path(f"{report_path}.progress.json").read_text())
    assert result["complete"] is False
    assert progress["phase"] == "stopped"
