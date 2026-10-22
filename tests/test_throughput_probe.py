from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from probe_fixtures import encoded_body, event_fixture, respond

from kalshi_events import capture, cli, metrics
from kalshi_events.common import Budget, CatalogueError, Limits, Store, json_bytes
from kalshi_events.partition_probe import acquire_partitions, preflight_collections
from kalshi_events.transport import read_page

SCRIPT = Path(__file__).parents[1] / "scripts" / "acquisition_probe.py"
SPEC = importlib.util.spec_from_file_location("acquisition_probe_test", SCRIPT)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)


@pytest.fixture(autouse=True)
def _force_probe_children_to_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    # A missed subprocess mock must never turn this offline unit module into a
    # live public-API probe.
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")


def _worker_report(
    *, attempts: int = 100, downloaded: int = 1000, http_429: bool = False
) -> dict[str, Any]:
    return {
        "version": 1,
        "command": "capture",
        "returncode": 0,
        "capture": {"capture_id": "a" * 32, "complete": True, "error": None},
        "accounting": {
            "attempts": attempts,
            "downloaded_or_reserved_bytes": downloaded,
            "accounting_complete": True,
            "retained_bytes": downloaded,
            "temporary_bytes": 0,
        },
        "receipts": {
            "data_pages": 99,
            "control_pages": 1,
            "control_responses": 1,
            "non_data_responses": 0,
            "attempts": attempts,
            "retry_attempts": 0,
            "encoded_bytes": downloaded,
            "http_429": http_429,
            "response_statuses": {
                "200": attempts - int(http_429),
                **({"429": 1} if http_429 else {}),
            },
            "observed_encodings": {"identity": attempts},
        },
        "metrics": {
            "counters": {"requests": attempts},
            "phases_seconds": {"capture": 10.0, "request": 8.0},
            "store_accounted_high_water_bytes": {"temporary": 4},
            "endpoint_families": {},
            "elapsed_seconds": 10.5,
            "peak_http_concurrency": 1,
            "peak_rss_bytes": {"self": 1234, "children": None},
        },
        "timings": {"acquisition_seconds": 10.0, "accounting_seconds": 0.1, "worker_seconds": 10.6},
        "error": None,
    }


class FakeWorker:
    reports: list[dict[str, Any]] = []
    commands: list[list[str]] = []
    behavior = "success"
    killed = False

    def __init__(
        self, command: list[str], *, stdout: Any, stderr: Any, env: dict[str, str], shell: bool
    ):
        assert shell is False
        self.commands.append(command)
        self.returncode = 0
        self.killed = False
        self.report = self.reports.pop(0) if self.reports else _worker_report()
        root = Path(command[command.index("--root") + 1])
        summary_path = root.parent.parent / "reports" / "summary.json"
        active = json.loads(summary_path.read_text(encoding="utf-8"))["active_scenario"]
        assert active["root"] == str(root)
        result_path = Path(command[command.index("--result") + 1])
        result_path.write_text(json.dumps(self.report), encoding="utf-8")

    def wait(self, timeout: float | None = None) -> int:
        if type(self).behavior == "timeout" and not self.killed:
            raise subprocess.TimeoutExpired("probe-worker", timeout)
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


def setup_function() -> None:
    FakeWorker.reports = []
    FakeWorker.commands = []
    FakeWorker.behavior = "success"


def test_throughput_rates_sort_and_reject_invalid_values() -> None:
    assert probe.parse_throughput_rates(None) == [10.0]
    assert probe.parse_throughput_rates(["20", "5", "10"]) == [5.0, 10.0, 20.0]
    for values in ([], ["1", "1"], ["nan"], ["inf"], ["0"], ["20.1"], ["1", "2", "3", "4"]):
        with pytest.raises(ValueError):
            probe.parse_throughput_rates(values)


def test_partition_selection_is_explicit_bounded_and_distinct() -> None:
    assert probe.validate_throughput_selection(None, None) == []
    selection = probe.validate_throughput_selection(["KXALPHA", "KXBETA"], ["KXCOLLECTION"])
    assert selection == [
        {"kind": "series", "ticker": "KXALPHA"},
        {"kind": "series", "ticker": "KXBETA"},
        {"kind": "collection", "ticker": "KXCOLLECTION"},
    ]
    for series, collections in (
        (["KXONE"], None),
        (["KXONE", "KXONE"], None),
        (None, ["KXONE", "KXONE"]),
    ):
        with pytest.raises(ValueError):
            probe.validate_throughput_selection(series, collections)


def test_global_matrix_uses_active_python_and_subtracts_each_sample(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "throughput", [5.0, 10.0])
    assert report["complete"] is True
    assert report["version"] == 2
    assert [(sample["rate"], sample["compression"]) for sample in report["samples"]] == [
        (5.0, "identity"),
        (5.0, "gzip"),
        (10.0, "identity"),
        (10.0, "gzip"),
    ]
    assert [sample["throughput_conclusion"] for sample in report["samples"]] == ["insufficient"] * 4
    assert report["samples"][1]["encoding_fallback"] is True
    commands = FakeWorker.commands
    assert all(command[0] == sys.executable for command in commands)
    assert all(command[command.index("--max-requests") + 1] == "250" for command in commands)
    assert all(
        command[command.index("--max-response-bytes") + 1] == str(32 * probe.MIB)
        for command in commands
    )
    assert all(
        command[command.index("--max-decoded-response-bytes") + 1] == str(64 * probe.MIB)
        for command in commands
    )
    assert report["accounting"]["attempts"] == 400
    assert (tmp_path / "throughput" / "reports" / "summary.json").is_file()


def test_http_429_stops_remaining_scenarios_and_preserves_partial_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    FakeWorker.reports = [_worker_report(http_429=True)]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "throttled", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert report["complete"] is False
    assert "429" in report["stop_reason"]
    saved = json.loads((tmp_path / "throttled" / "reports" / "summary.json").read_text())
    assert len(saved["samples"]) == 1


def test_aggregate_attempt_allowance_is_subtracted_before_each_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    monkeypatch.setitem(probe.TOTAL, "attempts", 150)
    monkeypatch.setitem(probe.GLOBAL, "attempts", 100)
    report = probe.run_throughput(tmp_path / "bounded", [10.0])
    assert len(FakeWorker.commands) == 2
    assert FakeWorker.commands[0][FakeWorker.commands[0].index("--max-requests") + 1] == "100"
    assert FakeWorker.commands[1][FakeWorker.commands[1].index("--max-requests") + 1] == "50"
    assert report["stop_reason"] == "worker accounting exceeded its assigned allowance"
    assert report["accounting"]["attempts"] is None


def test_temporary_high_water_includes_prior_roots_retained_temporary_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = _worker_report()
    first["accounting"]["temporary_bytes"] = 10
    first["metrics"]["store_accounted_high_water_bytes"]["temporary"] = 15
    second = _worker_report()
    second["accounting"]["temporary_bytes"] = 5
    second["metrics"]["store_accounted_high_water_bytes"]["temporary"] = 20
    FakeWorker.reports = [first, second]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "temp-high-water", [10.0])
    assert report["complete"] is True
    assert report["samples"][1]["aggregate_temporary_high_water_bytes"] == 30
    assert report["accounting"]["temporary_high_water_bytes"] == 30


def test_missing_acquisition_duration_stays_unknown_without_zero_throughput(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    worker = _worker_report()
    worker["timings"]["acquisition_seconds"] = None
    FakeWorker.reports = [worker]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "unknown-duration", [10.0])
    sample = report["samples"][0]
    assert sample["acquisition_seconds"] is None
    assert sample["throughput_conclusion"] == "unknown"
    assert sample["attempts_per_second"] is None
    assert sample["validated_data_pages_per_second"] is None


def test_untrusted_worker_accounting_stops_and_marks_totals_unknown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    invalid = _worker_report()
    invalid["accounting"]["attempts"] = True
    FakeWorker.reports = [invalid]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "unknown", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert report["samples"][0]["accounting"] == "unknown"
    assert report["accounting"]["attempts"] is None


@pytest.mark.parametrize(
    ("command_returncode", "error", "expected_reason"),
    [
        (1, "source failed", "unexpected capture command failure"),
        (
            3,
            "operator root storage exhausted",
            "command stopped for a non-request/download budget condition",
        ),
    ],
)
def test_worker_command_failure_is_not_hidden_by_successful_supervisor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command_returncode: int,
    error: str,
    expected_reason: str,
) -> None:
    failed = _worker_report()
    failed["returncode"] = command_returncode
    failed["capture"] = {"capture_id": "a" * 32, "complete": False, "error": error}
    FakeWorker.reports = [failed]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / f"command-{command_returncode}", [10.0])
    assert report["samples"][0]["supervisor_returncode"] == 0
    assert report["samples"][0]["command_returncode"] == command_returncode
    assert report["stop_reason"] == expected_reason


def test_trustworthy_request_budget_stop_allows_next_fresh_scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    partial = _worker_report()
    partial["returncode"] = 3
    partial["capture"] = {
        "capture_id": "a" * 32,
        "complete": False,
        "error": "request allowance exhausted",
    }
    FakeWorker.reports = [partial, _worker_report()]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "budget-partial", [10.0])
    assert report["complete"] is True
    assert [sample["status"] for sample in report["samples"]] == ["partial", "completed"]


def test_partition_mode_writes_initial_summary_before_supervised_capture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class CannotStartWorker:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise OSError("offline supervisor fixture")

    monkeypatch.setattr(probe.subprocess, "Popen", CannotStartWorker)
    output = tmp_path / "pilot-pending"
    report = probe.run_throughput(
        output,
        [10.0],
        partitions=[{"kind": "series", "ticker": "KXONE"}, {"kind": "series", "ticker": "KXTWO"}],
    )
    assert report["complete"] is False
    assert len(report["samples"]) == 1
    assert report["samples"][0]["status"] == "failed"
    assert "could not start" in report["stop_reason"]
    assert json.loads((output / "reports" / "summary.json").read_text())["version"] == 2


def test_supervised_worker_accounts_and_writes_bounded_result_before_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    Store(root)
    metrics_path = tmp_path / "metrics.json"
    result_path = tmp_path / "worker.json"

    def fake_capture(arguments: list[str]) -> int:
        metrics = Path(arguments[arguments.index("--metrics-file") + 1])
        metrics.write_text(
            json.dumps(
                {
                    "counters": {"requests": 0},
                    "phases_seconds": {},
                    "store_accounted_high_water_bytes": {"temporary": 0},
                    "endpoint_families": {},
                    "elapsed_seconds": 0.1,
                    "peak_http_concurrency": 0,
                    "peak_rss_bytes": {"self": 1000, "children": None},
                }
            ),
            encoding="utf-8",
        )
        print(json.dumps({"capture_id": None, "complete": False, "error": "test stop"}))
        return 3

    monkeypatch.setattr(cli, "main", fake_capture)
    arguments = [
        "--result",
        str(result_path),
        "--",
        "--root",
        str(root),
        "--metrics-file",
        str(metrics_path),
        "capture",
        "--max-rate",
        "10",
        "--max-requests",
        "10",
        "--max-download-bytes",
        "4096",
        "--max-response-bytes",
        "1024",
        "--max-decoded-response-bytes",
        "2048",
        "--max-duration-seconds",
        "5",
        "--max-root-bytes",
        "8192",
        "--max-temp-bytes",
        "4096",
        "--compression",
        "identity",
    ]
    assert probe.worker_main(arguments) == 0
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert probe._valid_worker_report(result)
    assert result["returncode"] == 3
    assert result["accounting"]["accounting_complete"] is True
    assert result["timings"]["accounting_seconds"] >= 0
    assert result["timings"]["acquisition_seconds"] is None


def test_only_committed_http_200_data_pages_count_for_throughput(
    monkeypatch: pytest.MonkeyPatch, store: Any
) -> None:
    statuses = [200, 404, 400, 410, 200]
    endpoints = [
        "/events",
        "/events/PARENT",
        "/events",
        "/historical/markets",
        "/historical/cutoff",
    ]
    rows = []
    for index, (status, endpoint) in enumerate(zip(statuses, endpoints, strict=True)):
        relative = f"raw/page-{index}/manifest.json"
        store.write(
            relative,
            {
                "version": 1,
                "endpoint": endpoint,
                "status": status,
                "size": 0,
                "downloaded_bytes": 0,
                "accounting_complete": True,
                "sha256": "0" * 64,
            },
        )
        rows.append({"relative": relative, "index": index})
    monkeypatch.setattr(capture, "read_capture", lambda *_args: {"capture_id": "fixture"})
    monkeypatch.setattr(capture, "page_receipts", lambda *_args: rows)
    report = probe._inspect_capture_receipts(store, capture, {"capture_id": "fixture"})
    assert report["data_pages"] == 1
    assert report["control_pages"] == 4
    assert report["control_responses"] == 1
    assert report["non_data_responses"] == 3


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", True),
        ("status", "bad"),
        ("accounting_complete", "yes"),
        ("downloaded_bytes", True),
        ("downloaded_bytes", -1),
        ("retry_delay_seconds", True),
        ("retry_delay_seconds", -0.1),
        ("retry_delay_seconds", "soon"),
    ],
)
def test_worker_rejects_malformed_retry_and_receipt_accounting_types(
    tmp_path: Path, field: str, value: Any
) -> None:
    store = Store(tmp_path / "malformed-root")
    manifest: dict[str, Any] = {
        "version": 1,
        "endpoint": "/markets",
        "status": 200,
        "size": 1,
        "downloaded_bytes": 1,
        "accounting_complete": True,
        "sha256": "0" * 64,
    }
    manifest[field] = value
    store.write("raw/attempt/manifest.json", manifest)
    with pytest.raises(CatalogueError, match="malformed raw receipt accounting"):
        probe._inspect_capture_receipts(store, capture, None)


def test_worker_timeout_kills_child_and_stops_matrix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class TimeoutWorker(FakeWorker):
        def wait(self, timeout: float | None = None) -> int:
            if not self.killed:
                raise subprocess.TimeoutExpired("probe-worker", timeout)
            return self.returncode

        def kill(self) -> None:
            self.killed = True
            type(self).killed = True
            self.returncode = -9

    monkeypatch.setattr(probe.subprocess, "Popen", TimeoutWorker)
    report = probe.run_throughput(tmp_path / "timeout", [10.0, 15.0])
    assert len(TimeoutWorker.commands) == 1
    assert report["complete"] is False
    assert report["accounting"]["attempts"] is None


def test_nonzero_supervisor_exit_ignores_seemingly_valid_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class ExitAfterReport(FakeWorker):
        def __init__(self, *args: Any, **kwargs: Any):
            super().__init__(*args, **kwargs)
            self.returncode = 1

    monkeypatch.setattr(probe.subprocess, "Popen", ExitAfterReport)
    report = probe.run_throughput(tmp_path / "worker-exit", [10.0])
    assert len(ExitAfterReport.commands) == 1
    assert report["complete"] is False
    assert report["accounting"]["attempts"] is None
    assert "exit cleanly" in report["stop_reason"]


class FixtureClient:
    def __init__(self, store: Store, fixture: dict[str, Any], *, compression: str = "identity"):
        self.store = store
        self.fixture = fixture
        self.compression = compression
        self.active = 0
        self.peak = 0
        self.lock = threading.Lock()

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> str:
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(0.003)
        try:
            status, payload = respond(self.fixture, endpoint, params)
            if status != 200:
                raise CatalogueError(f"unexpected fixture status: {status}")
            body = encoded_body(payload, self.compression)
            receipt_id = uuid.uuid4().hex
            relative = f"raw/{receipt_id}/manifest.json"
            self.store.write_bytes(f"raw/{receipt_id}/body.bin", body)
            manifest = {
                **(context or {}),
                "version": 1,
                "page_id": receipt_id,
                "endpoint": endpoint,
                "params": params or {},
                "status": 200,
                "received_at": "2026-01-01T00:00:00Z",
                "accounting_complete": True,
                "downloaded_bytes": len(body),
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
            if self.compression == "gzip":
                decoded = gzip.decompress(body)
                manifest.update(
                    version=2,
                    content_encoding="gzip",
                    encoded_size=len(body),
                    encoded_sha256=hashlib.sha256(body).hexdigest(),
                    decoded_size=len(decoded),
                    decoded_sha256=hashlib.sha256(decoded).hexdigest(),
                )
            self.store.write(relative, manifest)
            return relative
        finally:
            with self.lock:
                self.active -= 1


def _pilot_store(root: Path) -> tuple[Store, Budget]:
    limits = Limits(
        max_rate=20,
        max_requests=200,
        max_download_bytes=512 * 1024**2,
        max_response_bytes=32 * 1024**2,
        max_decoded_response_bytes=64 * 1024**2,
        max_duration_seconds=60,
        max_root_bytes=512 * 1024**2,
        max_temp_bytes=128 * 1024**2,
    )
    store = Store(root, limits)
    return store, Budget(store)


def test_raw_partition_pilot_preserves_observation_multisets_and_isolated_roots(
    tmp_path: Path,
) -> None:
    fixture = event_fixture("duplicates")
    partitions = [
        {"kind": part["kind"], "ticker": part["ticker"]} for part in fixture["partitions"]
    ]
    roots = [tmp_path / "serial", tmp_path / "parallel"]
    results = []
    clients = []
    for root, workers in zip(roots, (1, 8), strict=True):
        store, budget = _pilot_store(root)
        client = FixtureClient(store, fixture, compression="gzip")
        with store.writer_guard(), metrics.collect() as measured:
            bindings = preflight_collections(store, client, budget, partitions)
            result = acquire_partitions(
                store,
                client,
                budget,
                partitions,
                collection_series=bindings,
                workers=workers,
            )
            report = measured.report()
        assert result["completed"] is True
        assert report["counters"]["diagnostic_verified_pages"] == result["pages"]
        results.append(result)
        clients.append(client)
        artifact_names = {path.relative_to(root).parts[0] for path in root.iterdir()}
        assert artifact_names <= {"root.json", "writer.lock", "raw"}
        assert not {"captures", "builds", "releases", "current.json"}.intersection(artifact_names)
    assert clients[0].peak == 1
    assert clients[1].peak > 1
    assert results[0]["observations"] == results[1]["observations"] == 24
    assert results[0]["overlap_unique_identities"] == results[1]["overlap_unique_identities"]
    assert [row["inventory_sha256"] for row in results[0]["partitions"]] == [
        row["inventory_sha256"] for row in results[1]["partitions"]
    ]
    manifests = list((roots[0] / "raw").glob("*/manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert "diagnostic_id" in manifest
    assert not {"capture_id", "task_id", "request_id"}.intersection(manifest)


def test_partition_pilot_rejects_scope_mismatch_and_repeated_cursor(tmp_path: Path) -> None:
    fixture = event_fixture("balanced")
    partitions = [
        {"kind": "series", "ticker": "SERIES-0"},
        {"kind": "collection", "ticker": "COLLECTION-1"},
    ]
    store, budget = _pilot_store(tmp_path / "scope")
    client = FixtureClient(store, fixture)
    original = client.get

    def wrong_scope(endpoint, params=None, context=None):
        path = original(endpoint, params, context)
        manifest, payload = read_page(store, path)
        payload["events"][0]["series_ticker"] = "SERIES-WRONG"
        body = json_bytes(payload)
        body_path = store.path(path).parent / "body.bin"
        body_path.write_bytes(body)
        manifest.update(version=1, size=len(body), sha256=hashlib.sha256(body).hexdigest())
        store.write(path, manifest)
        return path

    client.get = wrong_scope
    with (
        store.writer_guard(),
        pytest.raises(CatalogueError, match="outside the selected partition"),
    ):
        acquire_partitions(
            store,
            client,
            budget,
            partitions,
            collection_series={"COLLECTION-1": "MVE-SERIES"},
            workers=1,
        )

    fixture = event_fixture("balanced")
    store, budget = _pilot_store(tmp_path / "cursor")
    client = FixtureClient(store, fixture)
    original = client.get

    def repeated_cursor(endpoint, params=None, context=None):
        path = original(endpoint, params, context)
        if params and params.get("cursor"):
            manifest, payload = read_page(store, path)
            payload["cursor"] = params["cursor"]
            body = json_bytes(payload)
            body_path = store.path(path).parent / "body.bin"
            body_path.write_bytes(body)
            manifest.update(version=1, size=len(body), sha256=hashlib.sha256(body).hexdigest())
            store.write(path, manifest)
        return path

    client.get = repeated_cursor
    with store.writer_guard(), pytest.raises(CatalogueError, match="repeated pagination cursor"):
        acquire_partitions(
            store,
            client,
            budget,
            partitions,
            collection_series={"COLLECTION-1": "MVE-SERIES"},
            workers=1,
        )


def test_pilot_report_validation_rejects_http_429_flag_mismatch() -> None:
    report = {
        "version": 1,
        "command": "partition",
        "returncode": 0,
        "result": {
            "workers": 1,
            "completed": True,
            "budget_stop": None,
            "pages": 0,
            "observations": 0,
            "overlap_unique_identities": 0,
            "partitions": [],
        },
        "accounting": {
            "attempts": 1,
            "downloaded_or_reserved_bytes": 0,
            "accounting_complete": True,
            "retained_bytes": 0,
            "temporary_bytes": 0,
        },
        "receipts": {
            "response_statuses": {"429": 1},
            "observed_encodings": {},
            "retry_attempts": 0,
            "encoded_bytes": 0,
            "http_429": False,
        },
        "metrics": None,
        "timings": {"acquisition_seconds": 0, "accounting_seconds": 0, "worker_seconds": 0},
        "error": None,
    }
    assert not probe._valid_pilot_report(report, "partitions")


class MixedWorker(FakeWorker):
    pilot_configs: list[dict[str, Any]] = []

    def __init__(self, command: list[str], **kwargs: Any):
        if "--partition-worker" not in command:
            super().__init__(command, **kwargs)
            return
        self.commands.append(command)
        self.returncode = 0
        config = json.loads(Path(command[command.index("--config") + 1]).read_text())
        self.pilot_configs.append(config)
        operation = config["operation"]
        attempts = 1 if operation == "preflight" else 2
        result = (
            {"collection_series": {"KXCOLL": "KXMVE"}, "completed": True}
            if operation == "preflight"
            else {
                "workers": config["workers"],
                "completed": True,
                "budget_stop": None,
                "pages": 2,
                "observations": 0,
                "overlap_unique_identities": 0,
                "partitions": [
                    {
                        "kind": row["kind"],
                        "ticker": row["ticker"],
                        "pages": 1,
                        "observations": 0,
                        "unique_identities": 0,
                        "identity_digests": [],
                        "inventory_sha256": "0" * 64,
                        "inventory_rows": 0,
                    }
                    for row in config["partitions"]
                ],
            }
        )
        metrics_value = {
            "version": 1,
            "elapsed_seconds": 0.1,
            "phases_seconds": {},
            "counters": {"diagnostic_verified_pages": 0 if operation == "preflight" else 2},
            "store_accounted_high_water_bytes": {"temporary": 0},
            "peak_http_concurrency": 1,
            "peak_rss_bytes": {"self": 1000, "children": None},
            "endpoint_families": {},
        }
        report = {
            "version": 1,
            "command": "partition",
            "returncode": 0,
            "result": result,
            "accounting": {
                "attempts": attempts,
                "downloaded_or_reserved_bytes": 100,
                "accounting_complete": True,
                "retained_bytes": 100,
                "temporary_bytes": 0,
            },
            "receipts": {
                "response_statuses": {"200": attempts},
                "observed_encodings": {"gzip": attempts},
                "retry_attempts": 0,
                "encoded_bytes": 100,
                "http_429": False,
            },
            "metrics": metrics_value,
            "timings": {
                "acquisition_seconds": 0.1,
                "accounting_seconds": 0.01,
                "worker_seconds": 0.11,
            },
            "error": None,
        }
        Path(command[command.index("--result") + 1]).write_text(json.dumps(report))


def test_partition_scenarios_follow_low_rate_globals_and_preflight_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    MixedWorker.commands = []
    MixedWorker.reports = []
    MixedWorker.pilot_configs = []
    monkeypatch.setattr(probe.subprocess, "Popen", MixedWorker)
    partitions = [
        {"kind": "series", "ticker": "KXSERIES"},
        {"kind": "collection", "ticker": "KXCOLL"},
    ]
    report = probe.run_throughput(tmp_path / "with-partitions", [5.0, 10.0], partitions=partitions)
    assert report["complete"] is True
    assert ["--partition-worker" in command for command in MixedWorker.commands] == [
        True,
        False,
        False,
        True,
        True,
        False,
        False,
    ]
    pilot_configs = MixedWorker.pilot_configs
    assert [config["operation"] for config in pilot_configs] == [
        "preflight",
        "partitions",
        "partitions",
    ]
    assert [config["workers"] for config in pilot_configs] == [1, 1, 8]
    assert all(config["compression"] == "gzip" for config in pilot_configs)
    assert all(config["limits"]["max_rate"] == 5.0 for config in pilot_configs)
    pilot_samples = [
        sample for sample in report["samples"] if sample.get("kind") in {"preflight", "partitions"}
    ]
    assert pilot_samples[0]["throughput_conclusion"] == "not_applicable"
    assert all(sample["throughput_conclusion"] == "insufficient" for sample in pilot_samples[1:])
    assert all(sample["decoded_bytes"] is None for sample in pilot_samples)
    assert all(
        sample["achieved_attempts_per_second"] == sample["attempts_per_second"]
        for sample in pilot_samples[1:]
    )
    assert report["partition_comparison"]["status"] == "identical"
    assert report["partition_comparison"]["partitions"][0]["identity_intersection"] == 0
    assert report["partition_comparison"]["partitions"][0]["observation_inventory"] == "identical"
    assert all(
        "identity_digests" not in row
        for sample in report["samples"]
        if sample.get("kind") == "partitions"
        for row in sample["result"]["partitions"]
    )


def test_partition_child_report_validates_exact_scope_and_partial_errors() -> None:
    partition = {"kind": "series", "ticker": "KXSERIES"}
    metrics_value = {
        "counters": {"diagnostic_verified_pages": 0},
        "phases_seconds": {},
        "store_accounted_high_water_bytes": {"temporary": 0},
        "peak_http_concurrency": 1,
        "peak_rss_bytes": {"self": 1000, "children": None},
        "endpoint_families": {},
    }
    base = {
        "version": 1,
        "command": "partition",
        "returncode": 1,
        "result": {"completed": False, "failure": "CatalogueError", "pages": 0, "observations": 0},
        "accounting": {
            "attempts": 1,
            "downloaded_or_reserved_bytes": 0,
            "accounting_complete": True,
            "retained_bytes": 1,
            "temporary_bytes": 0,
        },
        "receipts": {
            "response_statuses": {"200": 1},
            "observed_encodings": {"identity": 1},
            "retry_attempts": 0,
            "encoded_bytes": 0,
            "http_429": False,
        },
        "metrics": metrics_value,
        "timings": {"acquisition_seconds": None, "accounting_seconds": None, "worker_seconds": 0.1},
        "error": "CatalogueError: selected scope failed",
    }
    assert not probe._valid_pilot_report(base, "partitions", partitions=[partition], workers=1)
    mismatch = json.loads(json.dumps(base))
    mismatch["result"]["failure"] = "" + "unexpected"
    assert not probe._valid_pilot_report(mismatch, "partitions", partitions=[partition], workers=8)
