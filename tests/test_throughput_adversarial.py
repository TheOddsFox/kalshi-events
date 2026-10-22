"""Adversarial supervisor boundaries; no child process or network is started."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from test_throughput_probe import FakeWorker, _worker_report, probe


@pytest.fixture(autouse=True)
def reset_workers() -> None:
    FakeWorker.reports = []
    FakeWorker.commands = []
    FakeWorker.behavior = "success"


@pytest.mark.parametrize(
    "body",
    [b"", b"[]", b"null", b"{", b"\xff", b'{"n":NaN}', b'{"n":Infinity}', b'{"n":1e999}'],
)
def test_worker_report_parser_rejects_malformed_or_nonfinite_json(
    tmp_path: Path, body: bytes
) -> None:
    path = tmp_path / "report.json"
    path.write_bytes(body)
    assert probe._load_report(path) is None


def test_worker_report_size_ceiling_is_inclusive(tmp_path: Path) -> None:
    path = tmp_path / "report.json"
    body = b'{"padding":"' + b"x" * (probe.MAX_REPORT_BYTES - 14) + b'"}'
    assert len(body) == probe.MAX_REPORT_BYTES
    path.write_bytes(body)
    assert probe._load_report(path) is not None
    path.write_bytes(body + b" ")
    assert probe._load_report(path) is None


def test_worker_report_symlink_is_untrusted(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "report.json"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.fail(f"POSIX symlink creation failed: {exc}")
    assert probe._load_report(link) is None


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("accounting", "downloaded_or_reserved_bytes", True),
        ("accounting", "temporary_bytes", -1),
        ("accounting", "retained_bytes", 1.5),
        ("accounting", "accounting_complete", 1),
        ("receipts", "data_pages", False),
        ("receipts", "encoded_bytes", -1),
        ("receipts", "response_statuses", {"200": True}),
        ("receipts", "observed_encodings", {"br": 1}),
        ("metrics", "peak_http_concurrency", True),
        ("metrics", "peak_rss_bytes", {"self": -1}),
        ("metrics", "phases_seconds", {"request": float("inf")}),
        ("metrics", "phases_seconds", {"request": float("nan")}),
        ("metrics", "phases_seconds", {"request": True}),
        ("metrics", "store_accounted_high_water_bytes", {"temporary": True}),
        ("timings", "accounting_seconds", float("inf")),
        ("timings", "worker_seconds", -1),
        ("timings", "acquisition_seconds", True),
    ],
)
def test_worker_report_rejects_invalid_typed_measurements(
    section: str, field: str, value: Any
) -> None:
    report = _worker_report()
    report[section][field] = value
    assert probe._valid_worker_report(report) is False


def test_global_report_requires_exact_envelope() -> None:
    report = _worker_report()
    report["unexpected"] = "unbounded trace"
    assert probe._valid_worker_report(report) is False


def test_output_rejects_symlink_ancestor_before_starting_child(tmp_path: Path) -> None:
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "linked"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as exc:
        pytest.fail(f"POSIX symlink creation failed: {exc}")
    with pytest.raises(ValueError, match="symlinks"):
        probe.run_throughput(link / "output", [10.0])
    assert not (target / "output").exists()


def test_output_rejects_checkout_before_creating_directories() -> None:
    output = Path(probe.__file__).resolve().parents[1] / "should-never-be-probe-output"
    with pytest.raises(ValueError, match="outside the repository"):
        probe.run_throughput(output, [10.0])
    assert not output.exists()


def test_child_startup_failure_preserves_unknown_accounting_and_initial_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attempts = []

    def fail_start(*args: Any, **kwargs: Any) -> None:
        attempts.append(args)
        raise OSError("cannot create process")

    monkeypatch.setattr(probe.subprocess, "Popen", fail_start)
    output = tmp_path / "startup-failure"
    result = probe.run_throughput(output, [10.0, 15.0])
    saved = json.loads((output / "reports" / "summary.json").read_text())
    assert len(attempts) == 1
    assert result["complete"] is False
    assert all(value is None for value in result["accounting"].values())
    assert "active_scenario" not in saved
    assert saved["stop_reason"] == result["stop_reason"]


@pytest.mark.parametrize("kill_fails", [False, True])
def test_unreapable_or_unkillable_child_stops_all_remaining_scenarios(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kill_fails: bool
) -> None:
    class Unreapable(FakeWorker):
        def wait(self, timeout: float | None = None) -> int:
            raise subprocess.TimeoutExpired("worker", timeout)

        def kill(self) -> None:
            if kill_fails:
                raise OSError("termination denied")
            self.killed = True

    monkeypatch.setattr(probe.subprocess, "Popen", Unreapable)
    result = probe.run_throughput(tmp_path / "unreapable", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert result["complete"] is False
    assert all(value is None for value in result["accounting"].values())


def test_remaining_download_and_storage_allowances_are_passed_to_next_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setitem(probe.TOTAL, "downloaded_bytes", 5000)
    monkeypatch.setitem(probe.TOTAL, "retained_bytes", 8000)
    monkeypatch.setitem(probe.TOTAL, "temporary_bytes", 2000)
    first = _worker_report(downloaded=1000)
    first["accounting"]["retained_bytes"] = 1500
    first["accounting"]["temporary_bytes"] = 100
    first["metrics"]["store_accounted_high_water_bytes"]["temporary"] = 200
    FakeWorker.reports = [first, _worker_report(downloaded=1000)]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    result = probe.run_throughput(tmp_path / "remaining", [10.0])
    assert result["complete"] is True
    second = FakeWorker.commands[1]
    assert second[second.index("--max-download-bytes") + 1] == "4000"
    assert second[second.index("--max-root-bytes") + 1] == "6500"
    assert second[second.index("--max-temp-bytes") + 1] == "1900"


@pytest.mark.parametrize("field", ["retained_bytes", "temporary_bytes"])
def test_storage_accounting_over_allowance_stops_and_becomes_unknown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, field: str
) -> None:
    monkeypatch.setitem(probe.TOTAL, field, 2000)
    bad = _worker_report()
    bad["accounting"][field] = 2001
    FakeWorker.reports = [bad]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    result = probe.run_throughput(tmp_path / field, [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert result["complete"] is False
    assert result["samples"][0]["accounting"] == "unknown"
    assert all(value is None for value in result["accounting"].values())


def test_recovered_429_status_cannot_be_hidden_by_summary_boolean(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recovered = _worker_report(http_429=False)
    recovered["receipts"]["response_statuses"] = {"200": 99, "429": 1}
    recovered["receipts"]["retry_attempts"] = 1
    FakeWorker.reports = [recovered]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    result = probe.run_throughput(tmp_path / "recovered-throttle", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert result["complete"] is False


def _partition_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fixture: Any, hook=None):
    import httpx
    from probe_fixtures import encoded_body, respond

    from kalshi_events.common import Budget, Limits, Store
    from kalshi_events.transport import PublicClient

    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1/trade-api/v2")
    store = Store(tmp_path, limits=Limits(max_rate=1000))
    budget = Budget(store)
    client = PublicClient(store, budget, compression="gzip")
    client.client.close()

    def handler(request):
        params = dict(request.url.params)
        if "limit" in params:
            params["limit"] = int(params["limit"])
        if hook:
            hook(params)
        endpoint = request.url.path.removeprefix("/trade-api/v2")
        status, payload = respond(fixture, endpoint, params)
        return httpx.Response(
            status,
            stream=httpx.ByteStream(encoded_body(payload, "gzip")),
            headers={"Content-Encoding": "gzip"},
        )

    client.client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
    return store, budget, client


def test_parallel_pilot_has_one_outstanding_request_per_partition_and_shared_limiter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import threading

    from probe_fixtures import event_fixture

    from kalshi_events.metrics import collect
    from kalshi_events.partition_probe import acquire_partitions

    fixture = event_fixture("balanced")
    # Only two workers are runnable, and both first pages must reach the handler.
    # A serial implementation fails the barrier rather than a wall-time comparison.
    partitions = [
        {"kind": row["kind"], "ticker": row["ticker"]} for row in fixture["partitions"][:2]
    ]
    barrier = threading.Barrier(2)
    guard = threading.Lock()
    active: set[str] = set()
    seen_first: set[str] = set()

    def synchronize(params):
        selected = params.get("series_ticker") or params["collection_ticker"]
        with guard:
            assert selected not in active
            active.add(selected)
            first = selected not in seen_first
            seen_first.add(selected)
        try:
            if first:
                barrier.wait(timeout=5)
        finally:
            with guard:
                active.remove(selected)

    store, budget, client = _partition_client(
        tmp_path / "concurrent", monkeypatch, fixture, synchronize
    )
    limiter_calls = []
    original_acquire = client.limiter.acquire
    original_get = client.get
    outstanding: set[str] = set()

    def track_entire_request(endpoint, params=None, context=None):
        selected = params.get("series_ticker") or params["collection_ticker"]
        with guard:
            assert selected not in outstanding
            outstanding.add(selected)
        try:
            return original_get(endpoint, params=params, context=context)
        finally:
            with guard:
                outstanding.remove(selected)

    monkeypatch.setattr(client, "get", track_entire_request)

    def counted_acquire(actual_budget):
        assert actual_budget is budget
        with guard:
            limiter_calls.append(threading.get_ident())
        return original_acquire(actual_budget)

    monkeypatch.setattr(client.limiter, "acquire", counted_acquire)
    try:
        with collect() as measured:
            result = acquire_partitions(
                store,
                client,
                budget,
                partitions,
                collection_series={"COLLECTION-1": "MVE-SERIES"},
                workers=8,
            )
    finally:
        client.client.close()
    assert result["completed"] is True
    assert result["pages"] == 4
    assert len(limiter_calls) == 4
    assert len(set(limiter_calls)) >= 2
    assert measured.report()["peak_http_concurrency"] == 2


def test_shared_collection_series_overlap_preserves_every_observation_serial_and_parallel(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import copy
    from collections import Counter

    from probe_fixtures import event_fixture

    from kalshi_events.common import digest, json_bytes
    from kalshi_events.partition_probe import acquire_partitions

    fixture = event_fixture("duplicates")
    first, second = fixture["partitions"][1], fixture["partitions"][3]
    second["events"].append(copy.deepcopy(first["events"][0]))
    json.loads(
        json.dumps([first["events"], second["events"]]),
        parse_float=lambda _: pytest.fail("this fixture oracle requires no JSON decimal values"),
    )
    empty_decimal_paths = digest(b"")
    selected = [{"kind": "collection", "ticker": row["ticker"]} for row in (first, second)]
    bindings = {row["ticker"]: "MVE-SERIES" for row in (first, second)}
    results = []
    for workers in (1, 8):
        store, budget, client = _partition_client(tmp_path / str(workers), monkeypatch, fixture)
        try:
            result = acquire_partitions(
                store, client, budget, selected, collection_series=bindings, workers=workers
            )
        finally:
            client.client.close()
        assert result["overlap_unique_identities"] == 1
        assert result["observations"] == 13
        assert result["completed"] is True
        for reported, source in zip(result["partitions"], (first, second), strict=True):
            counts = Counter(
                (row["event_ticker"], digest(json_bytes([row, empty_decimal_paths])))
                for row in source["events"]
            )
            expected = [
                [identity, checksum, count]
                for (identity, checksum), count in sorted(counts.items())
            ]
            assert reported["inventory_sha256"] == digest(json_bytes(expected))
            assert reported["observations"] == len(source["events"])
        results.append(result["partitions"])
    assert results[0] == results[1]


def test_raw_pilot_evidence_is_ineligible_for_every_production_entrypoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from probe_fixtures import event_fixture

    from kalshi_events import capture, metadata, publish, recovery, warehouse
    from kalshi_events.common import CatalogueError
    from kalshi_events.partition_probe import acquire_partitions

    fixture = event_fixture("empty")
    partitions = [
        {"kind": row["kind"], "ticker": row["ticker"]} for row in fixture["partitions"][:2]
    ]
    store, budget, client = _partition_client(tmp_path / "isolated", monkeypatch, fixture)
    try:
        result = acquire_partitions(
            store,
            client,
            budget,
            partitions,
            collection_series={"COLLECTION-1": "MVE-SERIES"},
            workers=1,
        )
    finally:
        client.client.close()
    assert result["completed"] is True
    receipts = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    assert len(receipts) == 2
    assert all("diagnostic_id" in row for row in receipts)
    assert all(not {"capture_id", "task_index", "request_id"}.intersection(row) for row in receipts)
    diagnostic_id = receipts[0]["diagnostic_id"]
    operations = [
        lambda: capture.latest_catalogue_capture(store),
        lambda: capture.latest_complete_capture(store),
        lambda: warehouse.build(store, diagnostic_id),
        lambda: recovery.replay(store, diagnostic_id),
        lambda: metadata.export(store, ["MARKET"], tmp_path / "bundle"),
        lambda: publish.publish(store),
    ]
    for operation in operations:
        with pytest.raises(CatalogueError):
            operation()
    assert not store.path("captures").exists() or not list(store.path("captures").iterdir())
    with store.writer_guard():
        state = capture.new_capture(store, None)
        assert capture._recover_receipts(store, state) == {}
    assert not store.path("current.json").exists()
    assert not store.path("current-v2.json").exists()
