from __future__ import annotations

import gzip
import hashlib
import json
import tracemalloc
from contextlib import contextmanager

import httpx
import pytest

from kalshi_events.cli import main
from kalshi_events.common import Budget, BudgetExceeded, CatalogueError, Limits, Store, json_bytes
from kalshi_events.transport import PublicClient, accounting, read_page

LEGACY_MARKER = {"owner": "oddsfox.kalshi.events.v1"}
BODY = b'{"series":[{"ticker":"SERIES","title":"exact wire evidence"}]}\n'


class Chunks(httpx.SyncByteStream):
    def __init__(self, body, size=7):
        self.body = body
        self.size = size

    def __iter__(self):
        for offset in range(0, len(self.body), self.size):
            yield self.body[offset : offset + self.size]


@contextmanager
def source_client(store, handler, *, compression="gzip", budget=None):
    budget = budget or Budget(store)
    with PublicClient(store, budget, compression=compression) as client:
        headers = dict(client.client.headers)
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(handler),
            headers=headers,
            trust_env=False,
            follow_redirects=False,
        )
        yield client, budget


def response(body, *, encoding="gzip", status=200, headers=None):
    return httpx.Response(
        status,
        headers={"Content-Encoding": encoding, **(headers or {})},
        stream=Chunks(body),
    )


def receipts(store):
    return [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]


def test_gzip_preserves_exact_wire_bytes_and_independent_integrity(store):
    wire = gzip.compress(BODY, mtime=0)
    requested = []

    def handler(request):
        requested.append(request.headers["Accept-Encoding"])
        return response(wire)

    with source_client(store, handler) as (client, budget):
        relative = client.get("/series")
    manifest, payload = read_page(store, relative)
    assert requested == ["gzip"]
    assert payload == json.loads(BODY)
    assert store.path(relative).with_name("body.bin").read_bytes() == wire
    assert manifest["version"] == 2
    assert manifest["content_encoding"] == "gzip"
    assert manifest["encoded_size"] == len(wire)
    assert manifest["encoded_sha256"] == hashlib.sha256(wire).hexdigest()
    assert manifest["size"] == manifest["encoded_size"]
    assert manifest["sha256"] == manifest["encoded_sha256"]
    assert manifest["decoded_size"] == len(BODY)
    assert manifest["decoded_sha256"] == hashlib.sha256(BODY).hexdigest()
    assert budget.downloaded_bytes == len(wire)


@pytest.mark.parametrize("requested", ["identity", "gzip"])
def test_identity_response_remains_legacy_evidence_even_when_gzip_requested(store, requested):
    with source_client(
        store, lambda _: response(BODY, encoding="identity"), compression=requested
    ) as (client, budget):
        relative = client.get("/series")
    manifest, payload = read_page(store, relative)
    assert manifest["version"] == 1
    assert manifest["size"] == len(BODY)
    assert manifest["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert payload == json.loads(BODY)
    assert budget.downloaded_bytes == len(BODY)


def damaged_gzip(case):
    wire = gzip.compress(BODY, mtime=0)
    if case == "truncated_header":
        return wire[:5]
    if case == "truncated_body":
        return wire[:-10]
    if case == "truncated_trailer":
        return wire[:-1]
    if case == "bad_crc":
        return wire[:-8] + bytes([wire[-8] ^ 1]) + wire[-7:]
    if case == "trailing_bytes":
        return wire + b"junk"
    return wire + gzip.compress(b"{}", mtime=0)


@pytest.mark.parametrize(
    "case",
    [
        "truncated_header",
        "truncated_body",
        "truncated_trailer",
        "bad_crc",
        "trailing_bytes",
        "multiple_members",
    ],
)
def test_invalid_or_multiple_gzip_members_cannot_be_usable_evidence(store, case):
    wire = damaged_gzip(case)
    with (
        source_client(store, lambda _: response(wire)) as (client, budget),
        pytest.raises(CatalogueError),
    ):
        client.get("/series")
    assert budget.downloaded_bytes > 0
    assert budget.downloaded_bytes <= len(wire)
    assert all(row.get("error") for row in receipts(store))
    assert not list(store.root.rglob(".write-*"))
    assert store.usage()["temporary_bytes"] == 0


@pytest.mark.parametrize("encoding", ["br", "deflate", "gzip, identity"])
def test_unsupported_encoding_rejected_without_usable_receipt(store, encoding):
    with (
        source_client(store, lambda _: response(BODY, encoding=encoding)) as (client, _),
        pytest.raises(CatalogueError),
    ):
        client.get("/series")
    assert all(row.get("error") for row in receipts(store))


@pytest.mark.parametrize("ceiling_kind", ["encoded", "decoded"])
@pytest.mark.parametrize("extra", [0, 1])
def test_encoded_and_decoded_ceilings_are_independent(tmp_path, ceiling_kind, extra):
    body = b'{"padding":"' + b"x" * 512 + b'"}'
    wire = gzip.compress(body, mtime=0)
    encoded_limit = len(wire) - extra if ceiling_kind == "encoded" else len(wire)
    decoded_limit = len(body) - extra if ceiling_kind == "decoded" else len(body)
    store = Store(
        tmp_path / "root",
        Limits(
            max_response_bytes=encoded_limit,
            max_decoded_response_bytes=decoded_limit,
            max_download_bytes=4096,
        ),
    )
    with source_client(store, lambda _: response(wire)) as (client, _):
        if extra:
            with pytest.raises(CatalogueError) as raised:
                client.get("/series")
            assert not isinstance(raised.value, BudgetExceeded)
        else:
            relative = client.get("/series")
            assert read_page(store, relative)[1] == json.loads(body)


@pytest.mark.parametrize("declared", [None, "1"])
def test_false_or_missing_content_length_cannot_bypass_decoded_ceiling(tmp_path, declared):
    body = b'{"padding":"' + b"x" * 4096 + b'"}'
    wire = gzip.compress(body, mtime=0)
    store = Store(
        tmp_path / "root",
        Limits(max_response_bytes=512, max_decoded_response_bytes=128),
    )
    headers = {} if declared is None else {"Content-Length": declared}
    with (
        source_client(store, lambda _: response(wire, headers=headers)) as (client, budget),
        pytest.raises(CatalogueError),
    ):
        client.get("/series")
    assert budget.downloaded_bytes > 0
    allowance = budget.reserve()
    assert allowance == store.limits.max_response_bytes
    budget.settle(allowance)
    assert store.usage()["temporary_bytes"] == 0


def test_retry_download_accounting_counts_wire_not_expanded_bytes(store, advancing_clock):
    busy = gzip.compress(b'{"message":"retry"}', mtime=0)
    ok = gzip.compress(BODY, mtime=0)
    responses = [response(busy, status=503), response(ok)]
    with source_client(store, lambda _: responses.pop(0)) as (client, budget):
        relative = client.get("/series")
    assert read_page(store, relative)[1] == json.loads(BODY)
    assert budget.requests == 2
    assert budget.downloaded_bytes == len(busy) + len(ok)
    assert accounting(store)["downloaded_or_reserved_bytes"] == len(busy) + len(ok)


def test_partial_gzip_read_accounts_each_attempt_and_cleans_spool(store, advancing_clock):
    prefix = gzip.compress(BODY, mtime=0)[:12]

    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield prefix
            raise httpx.ReadError("interrupted stream")

    def handler(_):
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=Interrupted())

    with source_client(store, handler) as (client, budget), pytest.raises(CatalogueError):
        client.get("/series")
    assert budget.requests == 4
    assert budget.downloaded_bytes == 4 * len(prefix)
    assert accounting(store)["downloaded_or_reserved_bytes"] == 4 * len(prefix)
    assert accounting(store)["accounting_complete"] is True
    assert not list(store.root.rglob("body.bin"))
    assert not list(store.root.rglob(".write-*"))
    assert store.usage()["temporary_bytes"] == 0


def test_response_is_spooled_and_accounted_before_next_chunk_is_requested(store):
    observed = []

    class Inspected(httpx.SyncByteStream):
        def __iter__(self):
            yield BODY[:20]
            observed.append(store.usage()["temporary_bytes"])
            yield BODY[20:]

    with source_client(
        store,
        lambda _: httpx.Response(200, stream=Inspected()),
        compression="identity",
    ) as (client, _):
        relative = client.get("/series")
    assert observed and observed[0] >= 20
    assert read_page(store, relative)[1] == json.loads(BODY)
    assert store.usage()["temporary_bytes"] == 0
    accounted = store.usage()["retained_bytes"]
    store.reconcile()
    assert store.usage()["retained_bytes"] == accounted


@pytest.mark.parametrize("boundary", ["write", "commit"])
def test_disk_failure_discards_spool_and_releases_reservation(store, monkeypatch, boundary):
    from kalshi_events.common import EvidenceSpool

    original = getattr(EvidenceSpool, boundary)

    def failing_io(spool, *args):
        if boundary == "write":
            original(spool, *args)
        raise OSError("synthetic disk full")

    monkeypatch.setattr(EvidenceSpool, boundary, failing_io)
    with (
        source_client(store, lambda _: response(gzip.compress(BODY, mtime=0))) as (client, budget),
        pytest.raises(OSError, match="disk full"),
    ):
        client.get("/series")
    assert budget.downloaded_bytes > 0
    assert not list(store.root.rglob("body.bin"))
    assert not list(store.root.rglob(".write-*"))
    assert store.usage()["temporary_bytes"] == 0
    assert all(
        row.get("accounting_complete") is False or row.get("error") for row in receipts(store)
    )
    allowance = budget.reserve()
    budget.settle(allowance)


@pytest.mark.parametrize(
    "field", ["encoded_sha256", "decoded_sha256", "encoded_size", "decoded_size"]
)
def test_read_page_rejects_tampered_gzip_integrity_fields(store, field):
    with source_client(store, lambda _: response(gzip.compress(BODY, mtime=0))) as (client, _):
        relative = client.get("/series")
    manifest = json.loads(store.path(relative).read_text())
    manifest[field] = "0" * 64 if field.endswith("sha256") else manifest[field] + 1
    store.write(relative, manifest)
    with pytest.raises(CatalogueError):
        read_page(store, relative)


def test_changed_gzip_header_is_rejected_even_when_decoded_payload_is_unchanged(store):
    wire = gzip.compress(BODY, mtime=0)
    with source_client(store, lambda _: response(wire)) as (client, _):
        relative = client.get("/series")
    altered = wire[:4] + bytes([wire[4] ^ 1]) + wire[5:]
    assert gzip.decompress(altered) == BODY
    store.path(relative).with_name("body.bin").write_bytes(altered)
    with pytest.raises(CatalogueError):
        read_page(store, relative)


def test_sealed_encoded_and_decoded_read_limits_override_later_store_limits(store):
    wire = gzip.compress(BODY, mtime=0)
    with source_client(store, lambda _: response(wire)) as (client, _):
        relative = client.get("/series")
    tighter = Store(
        store.root,
        Limits(max_response_bytes=1, max_decoded_response_bytes=1),
    )
    assert read_page(tighter, relative, limit=len(wire), decoded_limit=len(BODY))[1] == (
        json.loads(BODY)
    )
    with pytest.raises(CatalogueError):
        read_page(tighter, relative, limit=len(wire), decoded_limit=len(BODY) - 1)


def test_one_compressed_chunk_cannot_allocate_its_full_expansion(tmp_path):
    wire = gzip.compress(b"x" * (16 * 1024**2), mtime=0)
    store = Store(
        tmp_path / "root",
        Limits(max_response_bytes=64 * 1024, max_decoded_response_bytes=1024),
    )
    with source_client(
        store,
        lambda _: httpx.Response(
            200, headers={"Content-Encoding": "gzip"}, stream=httpx.ByteStream(wire)
        ),
    ) as (client, _):
        tracemalloc.start()
        try:
            with pytest.raises(CatalogueError):
                client.get("/series")
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    assert peak < 2 * 1024**2
    assert store.usage()["temporary_bytes"] == 0


@pytest.mark.parametrize("category", ["root", "temporary"])
def test_concurrent_reservations_share_storage_allowance(tmp_path, category):
    limits = Limits(
        max_response_bytes=1024,
        max_decoded_response_bytes=1024,
        max_root_bytes=2500 if category == "root" else 1024**2,
        max_temp_bytes=2500 if category == "temporary" else 1024**2,
    )
    store = Store(tmp_path / "root", limits)
    budgets = [Budget(store) for _ in range(3)]
    outstanding = []
    try:
        for budget in budgets:
            try:
                allowance = budget.reserve()
            except BudgetExceeded:
                break
            outstanding.append((budget, allowance))
        assert outstanding
        assert len(outstanding) < len(budgets)
        assert sum(allowance for _, allowance in outstanding) <= 2500
    finally:
        for budget, allowance in outstanding:
            budget.settle(allowance)
    allowance = budgets[0].reserve()
    budgets[0].settle(allowance)


def test_deadline_during_stream_counts_delivered_chunk_and_cleans_spool(store):
    wire = gzip.compress(BODY, mtime=0)

    class Expiring(httpx.SyncByteStream):
        def __iter__(self):
            yield wire[:10]
            store.started -= store.limits.max_duration_seconds + 1
            yield wire[10:]

    with (
        source_client(
            store,
            lambda _: httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=Expiring()),
        ) as (client, budget),
        pytest.raises(BudgetExceeded),
    ):
        client.get("/series")
    assert budget.downloaded_bytes == len(wire)
    assert store.usage()["temporary_bytes"] == 0
    assert not list(store.root.rglob("body.bin"))
    assert all(row.get("error") for row in receipts(store))


def test_deadline_after_receipt_read_stops_gzip_readback(store, monkeypatch):
    from kalshi_events import transport

    with source_client(store, lambda _: response(gzip.compress(BODY, mtime=0))) as (client, _):
        relative = client.get("/series")
    original = transport.decode

    def expire_after_manifest(body):
        payload = original(body)
        if isinstance(payload, dict) and payload.get("version") == 2:
            store.started -= store.limits.max_duration_seconds + 1
        return payload

    monkeypatch.setattr(transport, "decode", expire_after_manifest)
    with pytest.raises(BudgetExceeded):
        read_page(store, relative)


@pytest.mark.parametrize("operation", ["acquisition", "readback"])
def test_deadline_expiring_inside_gzip_decode_cannot_finish_successfully(
    store, monkeypatch, operation
):
    from kalshi_events import transport

    wire = gzip.compress(BODY, mtime=0)
    if operation == "readback":
        with source_client(store, lambda _: response(wire)) as (client, _):
            relative = client.get("/series")
    original = transport.zlib.decompressobj

    class ExpiringDecoder:
        def __init__(self, *args, **kwargs):
            self.decoder = original(*args, **kwargs)

        def decompress(self, *args, **kwargs):
            decoded = self.decoder.decompress(*args, **kwargs)
            store.started -= store.limits.max_duration_seconds + 1
            return decoded

        def __getattr__(self, name):
            return getattr(self.decoder, name)

    monkeypatch.setattr(transport.zlib, "decompressobj", ExpiringDecoder)
    if operation == "readback":
        with pytest.raises(BudgetExceeded):
            read_page(store, relative)
    else:
        with (
            source_client(store, lambda _: response(wire)) as (client, _),
            pytest.raises(BudgetExceeded),
        ):
            client.get("/series")
        assert not list(store.root.rglob("body.bin"))
        assert store.usage()["temporary_bytes"] == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("content_encoding", "br"),
        ("content_encoding", ["gzip"]),
        ("encoded_size", -1),
        ("encoded_size", True),
        ("encoded_size", "10"),
        ("encoded_size", "matching_float"),
        ("decoded_size", -1),
        ("decoded_size", True),
        ("decoded_size", 1.5),
        ("decoded_size", "matching_float"),
        ("encoded_sha256", None),
        ("encoded_sha256", []),
        ("decoded_sha256", {}),
        ("decoded_sha256", "bad"),
    ],
)
def test_invalid_receipt_fields_fail_as_catalogue_errors(store, field, value):
    with source_client(store, lambda _: response(gzip.compress(BODY, mtime=0))) as (client, _):
        relative = client.get("/series")
    manifest = json.loads(store.path(relative).read_text())
    manifest[field] = float(manifest[field]) if value == "matching_float" else value
    store.write(relative, manifest)
    with pytest.raises(CatalogueError):
        read_page(store, relative)


def test_boolean_decoded_size_is_invalid_even_when_numerically_equal(store):
    with source_client(store, lambda _: response(gzip.compress(b"1", mtime=0))) as (client, _):
        relative = client.get("/series")
    manifest = json.loads(store.path(relative).read_text())
    assert manifest["decoded_size"] == 1
    manifest["decoded_size"] = True
    store.write(relative, manifest)
    with pytest.raises(CatalogueError):
        read_page(store, relative)


def test_new_root_marker_is_rejected_by_legacy_exact_marker_check(store):
    marker = json.loads(store.path("root.json").read_text())
    assert marker == {**LEGACY_MARKER, "storage_version": 5}

    def legacy_writer():
        # This is the v1 Store constructor's exact marker guard.
        if json.loads(store.path("root.json").read_text()) != LEGACY_MARKER:
            raise CatalogueError("operator root belongs to another application")
        store.path("legacy-write").write_bytes(b"unexpected write")

    with pytest.raises(CatalogueError, match="another application"):
        legacy_writer()
    assert not store.path("legacy-write").exists()


def test_explicit_root_upgrade_preserves_existing_evidence_and_requires_lock(tmp_path, capsys):
    root = tmp_path / "root"
    root.mkdir()
    (root / "root.json").write_bytes(json_bytes(LEGACY_MARKER))
    evidence = root / "raw" / "old-page" / "body.bin"
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(BODY)
    before = evidence.read_bytes()
    legacy = Store(root)
    with legacy.lock(allow_legacy=True):
        assert main(["--root", str(root), "upgrade-root"]) == 1
    assert json.loads((root / "root.json").read_text()) == LEGACY_MARKER
    capsys.readouterr()
    assert main(["--root", str(root), "upgrade-root"]) == 0
    assert json.loads((root / "root.json").read_text()) == {
        **LEGACY_MARKER,
        "storage_version": 5,
    }
    assert evidence.read_bytes() == before


def test_root_upgrade_cannot_use_another_roots_writer_lock(tmp_path):
    root = tmp_path / "legacy-root"
    root.mkdir()
    marker = root / "root.json"
    marker.write_bytes(json_bytes(LEGACY_MARKER))
    legacy = Store(root)
    another = Store(tmp_path / "another-root")
    with another.lock() as foreign_lock, pytest.raises(CatalogueError):
        legacy.upgrade_root(foreign_lock)
    assert json.loads(marker.read_text()) == LEGACY_MARKER


@pytest.mark.parametrize("operation", ["load", "dbt", "build", "publish", "prune"])
def test_direct_producer_refuses_legacy_root_before_any_tree_change(tmp_path, operation):
    from kalshi_events import publish, warehouse

    root = tmp_path / "legacy-root"
    root.mkdir()
    (root / "root.json").write_bytes(json_bytes(LEGACY_MARKER))
    (root / "evidence").mkdir()
    (root / "evidence" / "keep.bin").write_bytes(BODY)
    legacy = Store(root)

    def snapshot():
        return {
            str(path.relative_to(root)): (
                path.read_bytes() if path.is_file() else None,
                path.stat().st_mtime_ns,
            )
            for path in [root, *root.rglob("*")]
        }

    before = snapshot()
    capture_id = "f" * 32
    with pytest.raises(CatalogueError, match="upgrade-root"):
        if operation == "dbt":
            warehouse.dbt(legacy, ["parse"])
        elif operation == "publish":
            publish.publish(legacy, capture_id)
        elif operation == "prune":
            publish.prune_superseded(legacy, {"capture_id": capture_id}, "a" * 64)
        else:
            getattr(warehouse, operation)(legacy, capture_id)
    assert snapshot() == before


def test_failed_spool_rename_remains_accounted_until_abort(store, monkeypatch):
    from kalshi_events import common

    retained = store.usage()["retained_bytes"]
    spool = store.spool("raw/rename-failure/body.bin")
    spool.write(BODY)

    def failed_replace(source, destination):
        raise OSError("synthetic rename failure")

    monkeypatch.setattr(common.os, "replace", failed_replace)
    with pytest.raises(OSError, match="rename failure"):
        spool.commit()
    assert spool.path.read_bytes() == BODY
    assert not store.path("raw/rename-failure/body.bin").exists()
    assert store.usage()["temporary_bytes"] == len(BODY)
    assert store.usage()["retained_bytes"] == retained + len(BODY)
    assert store.reconcile()["temporary_bytes"] == len(BODY)
    spool.abort()
    assert store.usage()["temporary_bytes"] == 0
    assert store.usage()["retained_bytes"] == retained
