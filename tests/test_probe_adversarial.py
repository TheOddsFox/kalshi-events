"""Probe reports must preserve uncertainty at child-output boundaries."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "probe-acquisition.py"
SPEC = importlib.util.spec_from_file_location("probe_adversarial", SCRIPT)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def test_pending_or_network_failed_attempt_is_not_observed_identity_encoding(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    compressed = gzip.compress(b"", mtime=0)
    encoded_sha256 = hashlib.sha256(compressed).hexdigest()
    receipts = [
        {"status": None, "accounting_complete": False, "reserved_bytes": 128},
        {"status": None, "accounting_complete": True, "downloaded_bytes": 0},
        {
            "status": 200,
            "accounting_complete": True,
            "downloaded_bytes": 0,
            "version": 1,
            "size": 0,
            "sha256": empty_sha256,
        },
        {
            "status": 200,
            "accounting_complete": True,
            "downloaded_bytes": len(compressed),
            "version": 2,
            "content_encoding": "gzip",
            "size": len(compressed),
            "sha256": encoded_sha256,
            "encoded_size": len(compressed),
            "encoded_sha256": encoded_sha256,
            "decoded_size": 0,
            "decoded_sha256": empty_sha256,
        },
    ]
    for index, receipt in enumerate(receipts):
        directory = root / "raw" / str(index)
        directory.mkdir(parents=True)
        (directory / "manifest.json").write_text(json.dumps(receipt), encoding="utf-8")
    result = probe._receipt_summary(root, None)
    assert result["attempts"] == 4
    assert result["observed_encodings"].get("identity") == 1
    assert result["observed_encodings"].get("gzip") == 1


@pytest.mark.parametrize("failure_source", ["metrics", "receipt"])
def test_invalid_utf8_child_json_stops_matrix_and_preserves_partial_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_source: str
) -> None:
    commands = []

    class InvalidMetricsChild:
        def __init__(self, command, *, stdout, stderr, env, shell):
            assert shell is False
            commands.append(command)
            stdout.write(json.dumps({"complete": False, "capture_id": "a" * 32}))
            metrics = Path(command[command.index("--metrics-file") + 1])
            if failure_source == "metrics":
                metrics.write_bytes(b"\xff")
            else:
                root = Path(command[command.index("--root") + 1])
                receipt = root / "raw" / "partial" / "manifest.json"
                receipt.parent.mkdir(parents=True)
                receipt.write_bytes(b"\xff")

        def wait(self, timeout):
            return 3

    monkeypatch.setattr(probe.subprocess, "Popen", InvalidMetricsChild)
    monkeypatch.setattr(
        probe,
        "_account",
        lambda root: (
            None
            if failure_source == "receipt"
            else {
                "attempts": 1,
                "downloaded_or_reserved_bytes": 4,
                "accounting_complete": True,
                "retained_bytes": 4,
                "temporary_bytes": 0,
            }
        ),
    )
    destination = tmp_path / "probe"
    report = probe.run(destination, [5.0, 10.0])
    assert report["complete"] is False
    assert len(commands) == 1
    assert report["samples"][0]["accounting"] == "unknown"
    assert report["stop_reason"]
    assert Path(report["samples"][0]["root"]).is_dir()
    saved = json.loads((destination / "reports" / "summary.json").read_text(encoding="utf-8"))
    assert saved["complete"] is False
    assert saved["samples"][0]["accounting"] == "unknown"
