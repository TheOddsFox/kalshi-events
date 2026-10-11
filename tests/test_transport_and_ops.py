from __future__ import annotations

import math

import httpx
import pytest

from kalshi_events.cli import main
from kalshi_events.common import (
    Budget,
    BudgetExceeded,
    CatalogueError,
    Limits,
    Store,
    atomic_json,
)
from kalshi_events.transport import PublicClient, read_page, source_url


def test_small_download_allowance_covers_several_responses_and_stops_at_zero(store, monkeypatch):
    from kalshi_events.transport import accounting

    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)
    body = b'{"series":[]}'
    total = len(body) * 3
    bounded = Store(
        store.root, Limits(max_requests=4, max_response_bytes=total, max_download_bytes=total)
    )
    budget = Budget(bounded)
    requests = []
    receipts = []

    def source(request):
        requests.append(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(body))

    with PublicClient(bounded, budget) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        for _ in range(3):
            receipt, payload = read_page(bounded, client.get("/series"))
            receipts.append(receipt)
            assert payload == {"series": []}
        with pytest.raises(BudgetExceeded, match="download allowance exhausted"):
            client.get("/series")
    assert [receipt["reserved_bytes"] for receipt in receipts] == [
        total,
        total - len(body),
        len(body),
    ]
    assert budget.requests == len(requests) == 3
    assert budget.downloaded_bytes == total
    assert accounting(bounded) == {
        "attempts": 3,
        "downloaded_or_reserved_bytes": total,
        "accounting_complete": True,
    }


@pytest.mark.parametrize("declared", [False, True])
def test_remaining_download_ceiling_rejects_oversized_next_response(store, monkeypatch, declared):
    import json

    from kalshi_events.transport import accounting

    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)
    body = b'{"series":[]}'
    total = len(body) + 8
    bounded = Store(
        store.root, Limits(max_requests=4, max_response_bytes=total, max_download_bytes=total)
    )
    budget = Budget(bounded)
    overflow = b"x" * 9
    responses = [
        httpx.Response(200, stream=httpx.ByteStream(body)),
        httpx.Response(
            200,
            headers={"Content-Length": str(len(overflow))} if declared else {},
            stream=httpx.ByteStream(overflow),
        ),
    ]
    with PublicClient(bounded, budget) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(lambda _: responses.pop(0)), trust_env=False
        )
        read_page(bounded, client.get("/series"))
        with pytest.raises(CatalogueError, match="ceiling"):
            client.get("/series")
    receipts = [
        json.loads(path.read_text()) for path in bounded.path("raw").glob("*/manifest.json")
    ]
    rejected = next(receipt for receipt in receipts if receipt.get("error"))
    assert rejected["reserved_bytes"] == 8
    assert rejected["accounting_complete"] is True
    assert "sha256" not in rejected
    assert not bounded.path(f"raw/{rejected['page_id']}/body.bin").exists()
    actual_rejected_bytes = 0 if declared else len(overflow)
    assert rejected["downloaded_bytes"] == actual_rejected_bytes
    assert budget.downloaded_bytes == len(body) + actual_rejected_bytes
    assert budget.requests == 2
    assert accounting(bounded)["downloaded_or_reserved_bytes"] == budget.downloaded_bytes
    if not declared:
        with pytest.raises(BudgetExceeded, match="download allowance exhausted"):
            budget.reserve()


def test_fixed_origin_no_credentials_redirects_or_ambient_proxy(monkeypatch, store):
    monkeypatch.setenv("HTTPS_PROXY", "http://untrusted.invalid:8080")
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "https://evil.example/trade-api/v2")
    with pytest.raises(CatalogueError, match="origin"):
        source_url()
    monkeypatch.setenv(
        "KALSHI_EVENTS_API_URL", "https://user:secret@external-api.kalshi.com/trade-api/v2"
    )
    with pytest.raises(CatalogueError, match="credentials"):
        source_url()
    monkeypatch.delenv("KALSHI_EVENTS_API_URL")
    with PublicClient(store, Budget(store)) as client:
        assert not client.client.trust_env
        assert not client.client.follow_redirects
        assert not client.client.cookies


def test_loopback_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.delenv("KALSHI_EVENTS_ALLOW_LOOPBACK", raising=False)
    with pytest.raises(CatalogueError):
        source_url()
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    assert source_url() == "http://127.0.0.1:9/trade-api/v2"
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://example.com/trade-api/v2")
    with pytest.raises(CatalogueError):
        source_url()


def test_streaming_cap_and_retry_attempts_accounted(store, monkeypatch):
    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)
    bounded = Store(
        store.root, Limits(max_requests=4, max_response_bytes=32, max_download_bytes=128)
    )
    responses = [
        httpx.Response(429, stream=httpx.ByteStream(b'{"rate":"limited"}')),
        httpx.Response(200, stream=httpx.ByteStream(b'{"series":[]}')),
    ]
    budget = Budget(bounded)
    with PublicClient(bounded, budget) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(lambda request: responses.pop(0)),
            trust_env=False,
            follow_redirects=False,
        )
        relative = client.get("/series")
        manifest, payload = read_page(bounded, relative)
        assert payload == {"series": []}
        assert manifest["accounting_complete"]
        assert budget.requests == 2
        assert budget.downloaded_bytes == len(b'{"rate":"limited"}') + len(b'{"series":[]}')
    assert len(list(bounded.path("raw").iterdir())) == 2
    with PublicClient(bounded, Budget(bounded)) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, stream=httpx.ByteStream(b"x" * 33))
            ),
            trust_env=False,
        )
        with pytest.raises(CatalogueError, match="ceiling"):
            client.get("/series")


def test_redirect_is_rejected_without_second_host_request(store, monkeypatch):
    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)
    requests = []

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(
            302,
            headers={"Location": "http://evil.example/data"},
            stream=httpx.ByteStream(b"redirect"),
        )

    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(handler), trust_env=False, follow_redirects=False
        )
        with pytest.raises(CatalogueError, match="HTTP 302"):
            client.get("/series")
    assert len(requests) == 1


def test_busy_lock_and_symlink_root(store, tmp_path):
    with store.lock(), pytest.raises(CatalogueError, match="writer"):
        store.lock()
    linked = tmp_path / "linked"
    linked.symlink_to(store.root, target_is_directory=True)
    with pytest.raises(CatalogueError, match="symlink"):
        Store(linked)
    with pytest.raises(CatalogueError, match="escapes"):
        store.path("../escape")


def test_storage_and_deadline_are_fail_closed(store):
    bounded = Store(store.root, Limits(max_root_bytes=256, max_temp_bytes=128))
    with pytest.raises(BudgetExceeded, match="storage"):
        bounded.write("big.json", {"value": "x" * 300})
    deadline = Store(store.root, Limits(max_duration_seconds=0.001))
    deadline.started -= 1
    with pytest.raises(BudgetExceeded, match="deadline"):
        Budget(deadline).reserve()
    with pytest.raises(CatalogueError):
        Limits(max_duration_seconds=math.inf)


def test_raw_body_tampering_rejected(store):
    body = store.path("raw/page/body.bin")
    body.parent.mkdir(parents=True)
    body.write_bytes(b"{}")
    atomic_json(
        store.path("raw/page/manifest.json"),
        {"version": 1, "status": 200, "size": 2, "sha256": "0" * 64},
    )
    with pytest.raises(CatalogueError, match="integrity"):
        read_page(store, "raw/page/manifest.json")


def test_cli_new_metadata_syntax_and_validation(tmp_path, capsys):
    assert (
        main(
            [
                "--root",
                str(tmp_path / "root"),
                "metadata",
                "export",
                "--market-ticker",
                "../bad",
                "--output",
                str(tmp_path / "out"),
            ]
        )
        == 1
    )
    assert "invalid native ticker" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_dbt_uses_its_installed_environment_not_ambient_path(store, tmp_path, monkeypatch):
    from kalshi_events.warehouse import dbt

    fake = tmp_path / "dbt"
    marker = tmp_path / "ambient-was-invoked"
    fake.write_text("#!/bin/sh\ntouch '" + str(marker) + "'\nexit 77\n")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert dbt(store, ["parse"]).returncode == 0
    assert not marker.exists()


def test_crashed_attempt_retains_conservative_reservation(store):
    from kalshi_events.transport import accounting

    store.write(
        "raw/crashed/manifest.json",
        {
            "version": 1,
            "accounting_complete": False,
            "reserved_bytes": 16 * 1024**2,
            "status": None,
        },
    )
    result = accounting(store)
    assert result == {
        "attempts": 1,
        "downloaded_or_reserved_bytes": 16 * 1024**2,
        "accounting_complete": False,
    }


def test_partial_http_read_attempts_remain_accounted_and_capture_can_resume(store, monkeypatch):
    import json

    from conftest import FakeClient

    from kalshi_events.capture import capture, read_capture
    from kalshi_events.transport import accounting

    monkeypatch.setattr("kalshi_events.transport.time.sleep", lambda _: None)

    class PartialRead(httpx.SyncByteStream):
        def __iter__(self):
            yield b'{"market":'
            raise httpx.ReadError("synthetic interrupted HTTP 200 stream")

    def broken_source(actual_store, budget):
        client = PublicClient(actual_store, budget)
        client.client.close()

        def source(request):
            endpoint = request.url.path.removeprefix("/trade-api/v2")
            if endpoint == "/markets/MARKET":
                return httpx.Response(200, stream=PartialRead())
            status, payload = FakeClient(actual_store, budget).response(
                endpoint, dict(request.url.params)
            )
            return httpx.Response(status, stream=httpx.ByteStream(json.dumps(payload).encode()))

        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        return client

    with pytest.raises(CatalogueError, match="request failed"):
        capture(store, selected=["MARKET"], client_factory=broken_source)
    capture_id = next(store.path("captures").iterdir()).name
    failed = read_capture(store, capture_id)
    assert failed["complete"] is False and len(failed["pages"]) == 1
    assert accounting(store)["attempts"] == 5
    errors = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    assert len([row for row in errors if row.get("error") == "ReadError"]) == 4
    resumed = capture(store, resume=capture_id, client_factory=FakeClient)
    assert resumed["complete"] and resumed["requests"] == 3
    assert accounting(store)["attempts"] == 8
    assert accounting(store)["accounting_complete"] is True
