from __future__ import annotations

import copy
import json
from collections import Counter
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path

import pytest
from test_incremental_catalogue import baseline as baseline
from test_incremental_catalogue import certified_baseline as certified_baseline

from kalshi_events import capture as acquisition
from kalshi_events import incremental, metrics, warehouse
from kalshi_events import publish as publishing
from kalshi_events.common import CatalogueError, Store, file_digest


def issue_proof(store, capture_id, version=1):
    state = acquisition.read_capture(store, capture_id)
    verified = {}
    list(acquisition.capture_pages(store, state, evidence=verified))
    return warehouse._operation_evidence(store, state, version, verified)


def first_body(store, capture_id):
    state = acquisition.read_capture(store, capture_id)
    row = acquisition.page_receipts(store, state)[0]
    return store.path(row["relative"]).with_name("body.bin")


def test_operation_proof_accepts_its_own_freshly_verified_input_identity(baseline):
    store, capture_id, _, _ = baseline
    with store.lock():
        proof = issue_proof(store, capture_id)
        state, rows = warehouse._validate_operation_evidence(store, proof, capture_id, 1)
        assert state["capture_id"] == capture_id
        assert list(rows) == acquisition.page_receipts(store, state)


@pytest.mark.parametrize("wrong", ["capture", "version", "root"])
def test_operation_proof_rejects_wrong_capture_version_or_operator_root(baseline, tmp_path, wrong):
    store, capture_id, _, _ = baseline
    with store.lock():
        proof = issue_proof(store, capture_id)
        target = Store(tmp_path / "foreign-root") if wrong == "root" else store
        requested_id = "f" * 32 if wrong == "capture" else capture_id
        version = 2 if wrong == "version" else 1
        with pytest.raises(CatalogueError, match="operation evidence|identity|root|lock"):
            warehouse._validate_operation_evidence(target, proof, requested_id, version)


@pytest.mark.parametrize("changed", ["state", "pages", "checkpoints", "verified", "evidence"])
def test_operation_proof_binds_every_reused_input_and_digest_map(baseline, changed):
    store, capture_id, _, _ = baseline
    with store.lock():
        proof = issue_proof(store, capture_id)
        if changed == "state":
            state = copy.deepcopy(dict(proof.state))
            state["error"] = "changed after verification"
            proof = replace(proof, state=state)
        elif changed == "pages":
            rows = copy.deepcopy(proof.page_rows)
            rows[0]["relative"] = "raw/" + "0" * 32 + "/manifest.json"
            proof = replace(proof, page_rows=rows)
        else:
            attribute = "verified_pages" if changed == "verified" else changed
            values = dict(getattr(proof, attribute))
            values[next(iter(values))] = "0" * 64
            proof = replace(proof, **{attribute: values})
        with pytest.raises(CatalogueError, match="operation evidence|identity|binding"):
            warehouse._validate_operation_evidence(store, proof, capture_id, 1)


@pytest.mark.parametrize("filename", ["state.json", "pages.jsonl"])
def test_operation_proof_freshly_rechecks_checkpoint_bytes_after_landing(baseline, filename):
    store, capture_id, _, _ = baseline
    with store.lock():
        proof = issue_proof(store, capture_id)
        path = store.path(f"captures/{capture_id}/{filename}")
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(CatalogueError, match="checkpoint|journal|snapshot|operation evidence"):
            warehouse._validate_operation_evidence(store, proof, capture_id, 1)


def test_operation_proof_expires_when_its_writer_operation_ends(baseline):
    store, capture_id, _, _ = baseline
    with store.lock():
        proof = issue_proof(store, capture_id)
    body = first_body(store, capture_id)
    body.write_bytes(body.read_bytes() + b"altered between operations")
    with store.lock(), pytest.raises(CatalogueError):
        warehouse._validate_operation_evidence(store, proof, capture_id, 1)
    with pytest.raises(CatalogueError):
        warehouse.certified(store, capture_id)


def test_reused_python_object_id_cannot_make_an_expired_writer_proof_valid(baseline, monkeypatch):
    store, capture_id, _, _ = baseline
    # A Python object ID can be reused after an older lock is collected.
    monkeypatch.setattr(warehouse, "id", lambda _value: 7, raising=False)
    with store.lock():
        proof = issue_proof(store, capture_id)
    with store.lock(), pytest.raises(CatalogueError):
        warehouse._validate_operation_evidence(store, proof, capture_id, 1)


@pytest.mark.parametrize("filename", ["state.json", "pages.jsonl"])
def test_checkpoint_tampering_after_landing_never_replaces_certified_build(
    baseline, monkeypatch, filename
):
    store, capture_id, _, _ = baseline
    database = store.path(f"builds/{capture_id}/warehouse.duckdb")
    previous = file_digest(database, store.limits.max_database_bytes)[0]
    pointer = store.path("current.json").read_bytes()
    original = warehouse._land

    def tampered(*args, **kwargs):
        result = original(*args, **kwargs)
        path = store.path(f"captures/{capture_id}/{filename}")
        path.write_bytes(path.read_bytes() + b" ")
        return result

    monkeypatch.setattr(warehouse, "_land", tampered)
    with pytest.raises(CatalogueError, match="checkpoint|journal|snapshot|evidence"):
        warehouse.build(store, capture_id)
    assert file_digest(database, store.limits.max_database_bytes)[0] == previous
    assert store.path("current.json").read_bytes() == pointer


def test_build_reads_each_raw_body_once_and_standalone_certification_reads_it_freshly(
    baseline, monkeypatch
):
    store, capture_id, _, _ = baseline
    state = acquisition.read_capture(store, capture_id)
    paths = {
        store.path(row["relative"]).with_name("body.bin")
        for row in acquisition.page_receipts(store, state)
    }
    reads = Counter()
    original = Path.open

    def audited(path, mode="r", *args, **kwargs):
        if path in paths and "r" in mode:
            reads[path] += 1
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", audited)
    warehouse.build(store, capture_id)
    assert reads == Counter({path: 1 for path in paths})
    reads.clear()
    warehouse.certified(store, capture_id)
    assert reads == Counter({path: 1 for path in paths})


def test_cross_command_raw_tampering_still_blocks_publication_after_operation_reuse(baseline):
    store, capture_id, _, _ = baseline
    warehouse.build(store, capture_id)
    pointer = store.path("current.json").read_bytes()
    body = first_body(store, capture_id)
    body.write_bytes(body.read_bytes() + b"altered after build")
    with pytest.raises(CatalogueError):
        publishing.publish(store, capture_id)
    assert store.path("current.json").read_bytes() == pointer


def test_operation_context_requires_writer_lease(store):
    with pytest.raises(CatalogueError, match="writer lock"), warehouse.operation_context(store):
        pytest.fail("context opened without the writer lock")


def test_context_preserves_cutoff_history_and_decodes_only_cutoff_receipts(baseline, monkeypatch):
    store, capture_id, _, _ = baseline
    state = acquisition.read_capture(store, capture_id)
    paths = {
        store.path(row["relative"]).with_name("body.bin")
        for row in acquisition.page_receipts(store, state)
    }
    reads = Counter()
    decoded_endpoints = []
    original = Path.open
    original_reader = warehouse.read_page

    def audited(path, mode="r", *args, **kwargs):
        if path in paths and "r" in mode:
            reads[path] += 1
        return original(path, mode, *args, **kwargs)

    def decoded(*args, **kwargs):
        manifest, payload = original_reader(*args, **kwargs)
        if payload is not None:
            decoded_endpoints.append(manifest["endpoint"])
        return manifest, payload

    with store.lock(), warehouse.operation_context(store) as context:
        first_cutoffs = []
        warehouse.certified(store, capture_id, cutoff_sink=first_cutoffs, _context=context)
        assert len(first_cutoffs) == 3
        monkeypatch.setattr(Path, "open", audited)
        monkeypatch.setattr(warehouse, "read_page", decoded)
        second_cutoffs = []
        with metrics.collect() as measured:
            warehouse.certified(store, capture_id, cutoff_sink=second_cutoffs, _context=context)
        assert second_cutoffs == first_cutoffs
        assert all(reads[path] >= 1 for path in paths)
        assert all(endpoint == "/historical/cutoff" for endpoint in decoded_endpoints)
        assert measured.report()["counters"].get("raw_body_json_decodes", 0) <= len(first_cutoffs)


def test_mutating_returned_receipt_does_not_poison_immutable_context_evidence(baseline):
    store, capture_id, _, _ = baseline
    with store.lock(), warehouse.operation_context(store) as context:
        receipt = warehouse.certified(store, capture_id, _context=context)
        expected = copy.deepcopy(receipt)
        receipt["warehouse_sha256"] = "0" * 64
        receipt["evidence"][next(iter(receipt["evidence"]))] = "0" * 64
        assert warehouse.certified(store, capture_id, _context=context) == expected


@pytest.mark.parametrize(
    "changed", ["body", "manifest", "build", "database", "checkpoint", "artifacts"]
)
def test_context_reuse_rehashes_original_bytes_and_rejects_same_operation_mutation(
    baseline, changed
):
    store, capture_id, _, _ = baseline
    with store.lock(), warehouse.operation_context(store) as context:
        warehouse.certified(store, capture_id, _context=context)
        body = first_body(store, capture_id)
        paths = {
            "body": body,
            "manifest": body.with_name("manifest.json"),
            "build": store.path(f"builds/{capture_id}/build.json"),
            "database": store.path(f"builds/{capture_id}/warehouse.duckdb"),
            "checkpoint": store.path(f"captures/{capture_id}/state.json"),
            "artifacts": store.path(f"builds/{capture_id}/build-artifacts.json"),
        }
        path = paths[changed]
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(CatalogueError):
            warehouse.certified(store, capture_id, _context=context)


def test_closed_context_is_not_reusable_even_when_the_writer_lock_is_still_held(baseline):
    store, capture_id, _, _ = baseline
    with store.lock():
        with warehouse.operation_context(store) as context:
            warehouse.certified(store, capture_id, _context=context)
            assert context.captures
        assert context.closed
        assert context.captures == {}
        with pytest.raises(CatalogueError, match="writer lease|context"):
            warehouse.certified(store, capture_id, _context=context)


def test_open_context_cannot_cross_a_writer_lock_release_and_reacquisition(baseline):
    store, capture_id, _, _ = baseline
    with ExitStack() as lifetime:
        with store.lock():
            context = lifetime.enter_context(warehouse.operation_context(store))
            warehouse.certified(store, capture_id, _context=context)
        with store.lock(), pytest.raises(CatalogueError, match="writer lease|context"):
            warehouse.certified(store, capture_id, _context=context)


def test_publication_and_pruning_share_one_context_then_close_it(baseline, monkeypatch):
    store, capture_id, _, _ = baseline
    contexts = []
    original = warehouse.certified

    def audited(*args, **kwargs):
        contexts.append(kwargs.get("_context"))
        return original(*args, **kwargs)

    monkeypatch.setattr(warehouse, "certified", audited)
    monkeypatch.setattr(publishing, "certified", audited)
    publishing.publish(store, capture_id)
    assert len(contexts) >= 2, "publication and retention must both verify the evidence closure"
    assert contexts[0] is not None
    assert all(context is contexts[0] for context in contexts)
    assert contexts[0].closed
    assert contexts[0].captures == {}


def test_pruning_reloads_changed_publication_pointers_instead_of_cached_roots(baseline):
    store, capture_id, legacy, _ = baseline
    state = acquisition.read_capture(store, capture_id)
    with store.lock(), warehouse.operation_context(store) as context:
        warehouse.certified(store, capture_id, _context=context)
        warehouse.certified(store, capture_id, catalogue_version=2, _context=context)
        pointer = store.path("current-v2.json")
        original = pointer.read_bytes()
        changed = json.loads(original)
        changed["source_release_id"] = "0" * 64
        store.write("current-v2.json", changed)
        body = first_body(store, capture_id)
        with pytest.raises(CatalogueError):
            incremental.prune(store, state, legacy["source_release_id"], 1, _context=context)
        assert body.is_file()
        assert store.path(f"captures/{capture_id}").is_dir()
        assert store.path(f"builds/{capture_id}/warehouse.duckdb").is_file()
