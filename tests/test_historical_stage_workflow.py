"""Explicit stage continuation cannot initiate acquisition or a fresh load."""

import json

import pytest

from kalshi_events import capture, cli, publish, refresh
from kalshi_events.common import CatalogueError

CAPTURE = "a" * 32
STAGE = "build-historical-" + "b" * 32


@pytest.mark.parametrize("stage", ["../stage", "/tmp/stage", "b" * 32, "build-historical-x", True])
def test_resume_stage_rejects_unsafe_identity_before_reading_capture(store, monkeypatch, stage):
    monkeypatch.setattr(capture, "read_capture", lambda *a, **k: pytest.fail("read capture"))
    result, code = refresh.run(
        store, historical=True, capture_id=CAPTURE, resume_historical_stage=stage
    )
    assert code == 1
    assert "invalid historical resume stage" in result["error"]


@pytest.mark.parametrize("options", [{}, {"full": True}, {"capture_id": CAPTURE}])
def test_resume_stage_requires_offline_historical_mode(store, options):
    result, code = refresh.run(store, resume_historical_stage=STAGE, **options)
    assert code == 1
    assert "exact offline historical" in result["error"]


def test_resume_stage_is_passed_to_publication_without_acquisition(store, monkeypatch):
    state = {"capture_id": CAPTURE, "selected": None, "complete": False}
    monkeypatch.setattr(capture, "read_capture", lambda *a, **k: state)
    monkeypatch.setattr(capture, "capture", lambda *a, **k: pytest.fail("acquisition"))
    monkeypatch.setattr(refresh, "historical_selection", lambda *a, **k: {})

    def fail_adoption(_store, capture_id, *, catalogue_version, resume_historical_stage):
        assert capture_id == CAPTURE
        assert catalogue_version == 3
        assert resume_historical_stage == STAGE
        raise CatalogueError("selected stage could not be adopted")

    monkeypatch.setattr(publish, "publish", fail_adoption)
    result, code = refresh.run(
        store, historical=True, capture_id=CAPTURE, resume_historical_stage=STAGE
    )
    assert code == 1
    assert result["error"] == "selected stage could not be adopted"
    assert result["requests"] == result["downloaded_bytes"] == 0
    assert result["publication_state"] == "unknown"


def test_cli_stage_selection_requires_historical_before_root_creation(tmp_path, capsys):
    root = tmp_path / "never-created"
    code = cli.main(
        [
            "--root",
            str(root),
            "refresh",
            "--capture-id",
            CAPTURE,
            "--resume-historical-stage",
            STAGE,
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "requires --historical --capture-id" in result["error"]
    assert not root.exists()
