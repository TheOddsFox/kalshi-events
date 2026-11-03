from __future__ import annotations

import importlib.util
import os
import shutil
import sys
import uuid
from pathlib import Path

import duckdb
import pytest
from conftest import FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import historical_resume, warehouse
from kalshi_events.common import CatalogueError, Store
from kalshi_events.incremental import root_id

ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


grants = _load_script("historical_mount_recovery_grants", ROOT / "scripts" / "historical_grants.py")
production = _load_script(
    "historical_mount_recovery_production", ROOT / "scripts" / "production_run.py"
)


def _factory(active_store, budget, **options):
    return FakeClient(active_store, budget, {}, **options)


def _simulate_device(identity_function, paths: set[Path], device: int):
    def simulated(path: Path):
        identity = identity_function(path)
        if path in paths:
            return (device, *identity[1:])
        return identity

    return simulated


def _validate_source(store, state, stage_name, source_result, source_proof_sha256, mount):
    stage = store.path(f"tmp/{stage_name}")
    source = stage / "warehouse.duckdb"
    warehouse._validate_historical_resume_source_presence(
        store,
        source,
        source.with_name(source.name + ".wal"),
        source_result,
        adopted=False,
        cleanup_intent=stage / "resume-cleanup-intent.json",
        stage_name=stage_name,
        state=state,
        source_proof_sha256=source_proof_sha256,
        _verified_mount=mount,
    )


def test_real_historical_proofs_survive_d0_d1_d2_and_cleanup_retry(tmp_path, monkeypatch):
    store = Store(tmp_path / "operator")
    captured = acquisition.capture(store, client_factory=_factory)
    capture_id = captured["capture_id"]
    state = warehouse.read_capture(store, capture_id)
    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    warehouse.build_historical(store, capture_id)

    stage_name = "build-historical-" + uuid.uuid4().hex
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    source = stage / "warehouse.duckdb"
    seed = store.path(f"builds-historical/{capture_id}/warehouse.duckdb")
    shutil.copyfile(seed, source)

    # Make a real DuckDB WAL and preserve it beside the staged source.
    wal_seed = store.path("tmp/mount-lineage-wal-seed.duckdb")
    shutil.copyfile(source, wal_seed)
    with duckdb.connect(str(wal_seed)) as connection:
        connection.execute("begin transaction")
        connection.execute("create table mount_lineage_probe(value bigint)")
        connection.execute("insert into mount_lineage_probe values (1)")
        connection.execute("commit")
        wal_seed_path = wal_seed.with_name(wal_seed.name + ".wal")
        assert wal_seed_path.is_file()
        shutil.copyfile(wal_seed, source)
        source_wal = source.with_name(source.name + ".wal")
        shutil.copyfile(wal_seed_path, source_wal)
    store.sync(stage_relative)

    current_device = store.root.lstat().st_dev
    device_d0 = current_device + 101
    device_d1 = current_device + 202
    source_paths = {source, source_wal}
    real_identity = warehouse._resume_file_identity

    # The source snapshot is produced by the real validator while the actual
    # source files report their D0 device, preserving their real inode/size/time.
    with monkeypatch.context() as patch:
        patch.setattr(
            warehouse,
            "_resume_file_identity",
            _simulate_device(real_identity, source_paths, device_d0),
        )
        frozen_snapshot = warehouse.historical_readiness_snapshot(store, capture_id, stage_name)

    control_root = tmp_path / "controls"
    (control_root / "source").mkdir(parents=True, mode=0o700)
    control_root.chmod(0o700)
    frozen_snapshot_path = control_root / "source" / "source-snapshot.json"
    grants.write_once_json(frozen_snapshot_path, frozen_snapshot)
    frozen_snapshot_bytes = frozen_snapshot_path.read_bytes()
    freeze = {
        "state_sha256": frozen_snapshot["binding"]["capture_state_sha256"],
        "source_sha256": frozen_snapshot["result"]["source_sha256"],
        "source_snapshot_sha256": frozen_snapshot["sha256"],
        "receipt_evidence_sha256": frozen_snapshot["result"]["receipt_evidence_sha256"],
        "scope_sha256": frozen_snapshot["binding"]["scope_sha256"],
        "code_sha256": frozen_snapshot["binding"]["code_sha256"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": device_d0,
    }
    identity = {"capture_id": capture_id, "stage_name": stage_name}

    # The candidate copy proof records D1. All copying, hashing, DuckDB recovery,
    # landing comparison, and proof construction remain real.
    with monkeypatch.context() as patch:
        patch.setattr(
            warehouse,
            "_resume_file_identity",
            _simulate_device(real_identity, source_paths, device_d1),
        )
        first_prepare = warehouse.prepare_historical_readiness(store, capture_id, stage_name)
    assert first_prepare["ready"] is True

    candidate = store.path(f"{stage_relative}/resume-candidate")
    source_proof_path = candidate / "resume-source.json"
    landing_proof_path = candidate / "resume-landing.json"
    source_proof_bytes = source_proof_path.read_bytes()
    landing_proof_bytes = landing_proof_path.read_bytes()
    source_proof = warehouse._load_self_proof(source_proof_path, "resume-source")
    assert source_proof is not None
    source_proof_value, source_result = source_proof
    assert source_result["source_identity"][0] == device_d1
    assert source_result["source_wal_identity"][0] == device_d1

    # D2 is the real device. A verified context binds the root, logical root,
    # capture, live device, and the complete observed lineage.
    assert warehouse._resume_file_identity(source)[0] == current_device
    assert warehouse._resume_file_identity(source_wal)[0] == current_device
    verified_mount = {
        "root_path": str(store.root.resolve()),
        "root_id": logical_root_id,
        "capture_id": capture_id,
        "current_device": current_device,
        "device_lineage": [device_d0, device_d1, current_device],
    }

    # Exercise the actual controller boundary: it reads the persisted D0 proof,
    # creates a fresh source snapshot from the real files, and compares them
    # through the supplied D0→D1→D2 lineage.
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    accepted_snapshot = production._historical_source_snapshot(
        store,
        control_root,
        identity,
        freeze,
        grant_id="a" * 32,
        approval={},
        control={},
        verified_mount=verified_mount,
    )
    assert accepted_snapshot == frozen_snapshot
    assert frozen_snapshot_path.read_bytes() == frozen_snapshot_bytes

    # The controller rejects absent or incomplete lineage and misbound roots.
    for invalid_mount in (
        None,
        {**verified_mount, "device_lineage": [device_d1, current_device]},
        {**verified_mount, "root_path": str(tmp_path / "other-root")},
        {**verified_mount, "root_id": "f" * 32},
        {**verified_mount, "capture_id": "e" * 32},
        {**verified_mount, "current_device": current_device + 1},
    ):
        with pytest.raises((production.RunnerError, CatalogueError)):
            production._historical_source_snapshot(
                store,
                control_root,
                identity,
                freeze,
                grant_id="a" * 32,
                approval={},
                control={},
                verified_mount=invalid_mount,
            )

    # A D2 prepare reuses the copied candidate and sealed landing. It must not
    # copy source bytes again or reload landing rows after the remount.
    with monkeypatch.context() as patch:
        patch.setattr(
            warehouse,
            "_copy_resume_file",
            lambda *_args, **_kwargs: pytest.fail("remount recopied source files"),
        )
        patch.setattr(
            warehouse,
            "_iter_historical_landing_rows",
            lambda *_args, **_kwargs: pytest.fail("remount reloaded landing rows"),
        )
        replay_prepare = warehouse.prepare_historical_readiness(
            store, capture_id, stage_name, _verified_mount=verified_mount
        )
    assert replay_prepare["readiness_sha256"] == first_prepare["readiness_sha256"]
    assert source_proof_path.read_bytes() == source_proof_bytes
    assert landing_proof_path.read_bytes() == landing_proof_bytes

    # Check persisted D0 and D1 identities against the live D2 file, while
    # retaining strict no-context and old-device rejection behavior.
    live_source_identity = warehouse._resume_file_identity(source)
    for saved_identity in (
        frozen_snapshot["result"]["source_identity"],
        source_result["source_identity"],
    ):
        assert historical_resume.resume_file_identity_matches(
            saved_identity,
            live_source_identity,
            store=store,
            state=state,
            verified_mount=verified_mount,
        )
        assert not historical_resume.resume_file_identity_matches(
            saved_identity, live_source_identity, store=store, state=state
        )
        with pytest.raises(CatalogueError, match="current operator mount"):
            historical_resume.resume_file_identity_matches(
                saved_identity,
                (saved_identity[0], *live_source_identity[1:]),
                store=store,
                state=state,
                verified_mount=verified_mount,
            )
    outside_lineage = {**verified_mount, "device_lineage": [device_d1, current_device]}
    with pytest.raises(CatalogueError, match="outside the verified lineage"):
        historical_resume.normalize_resume_file_identity(
            frozen_snapshot["result"]["source_identity"],
            store=store,
            state=state,
            verified_mount=outside_lineage,
        )

    # Identity fields and content are independently sealed. Restore every
    # mutation before continuing into real verification and adoption.
    original_source_bytes = source.read_bytes()
    source_stat = source.stat()
    with pytest.raises(CatalogueError, match="source database changed after staging"):
        try:
            source.write_bytes(original_source_bytes + b"x")
            _validate_source(
                store,
                state,
                stage_name,
                source_result,
                source_proof_value["sha256"],
                verified_mount,
            )
        finally:
            source.write_bytes(original_source_bytes)
            os.utime(source, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))

    with pytest.raises(CatalogueError, match="source database changed after staging"):
        try:
            os.utime(
                source,
                ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns + 1_000_000),
            )
            _validate_source(
                store,
                state,
                stage_name,
                source_result,
                source_proof_value["sha256"],
                verified_mount,
            )
        finally:
            os.utime(source, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))

    with pytest.raises(CatalogueError, match="source database changed after staging"):
        backup = source.with_name("warehouse.duckdb.mount-test-backup")
        try:
            source.rename(backup)
            shutil.copyfile(backup, source)
            _validate_source(
                store,
                state,
                stage_name,
                source_result,
                source_proof_value["sha256"],
                verified_mount,
            )
        finally:
            if source.exists():
                source.unlink()
            backup.rename(source)

    with pytest.raises(CatalogueError, match="source database changed after staging"):
        try:
            changed = bytearray(original_source_bytes)
            changed[0] ^= 1
            source.write_bytes(changed)
            os.utime(source, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
            _validate_source(
                store,
                state,
                stage_name,
                source_result,
                source_proof_value["sha256"],
                verified_mount,
            )
        finally:
            source.write_bytes(original_source_bytes)
            os.utime(source, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))

    original_wal_bytes = source_wal.read_bytes()
    wal_stat = source_wal.stat()
    with pytest.raises(CatalogueError, match="source WAL changed after staging"):
        try:
            changed = bytearray(original_wal_bytes)
            changed[0] ^= 1
            source_wal.write_bytes(changed)
            os.utime(source_wal, ns=(wal_stat.st_atime_ns, wal_stat.st_mtime_ns))
            _validate_source(
                store,
                state,
                stage_name,
                source_result,
                source_proof_value["sha256"],
                verified_mount,
            )
        finally:
            source_wal.write_bytes(original_wal_bytes)
            os.utime(source_wal, ns=(wal_stat.st_atime_ns, wal_stat.st_mtime_ns))

    # An actual source without a WAL cannot silently acquire one after sealing.
    no_wal_stage_name = "build-historical-" + uuid.uuid4().hex
    no_wal_stage_relative = f"tmp/{no_wal_stage_name}"
    no_wal_stage = store.path(no_wal_stage_relative)
    no_wal_stage.mkdir(parents=True)
    no_wal_source = no_wal_stage / "warehouse.duckdb"
    shutil.copyfile(seed, no_wal_source)
    store.sync(no_wal_stage_relative)
    no_wal_snapshot = warehouse.historical_readiness_snapshot(
        store, capture_id, no_wal_stage_name, _verified_mount=verified_mount
    )
    assert no_wal_snapshot["result"]["source_wal_identity"] is None
    no_wal_path = no_wal_source.with_name(no_wal_source.name + ".wal")
    no_wal_path.write_bytes(b"new wal after source snapshot")
    with pytest.raises(CatalogueError, match="gained a WAL after staging"):
        warehouse._validate_historical_resume_source_presence(
            store,
            no_wal_source,
            no_wal_path,
            no_wal_snapshot["result"],
            adopted=False,
            cleanup_intent=no_wal_stage / "resume-cleanup-intent.json",
            stage_name=no_wal_stage_name,
            state=state,
            source_proof_sha256=no_wal_snapshot["sha256"],
            _verified_mount=verified_mount,
        )

    verification = warehouse.verify_historical_readiness(
        store, capture_id, stage_name, _verified_mount=verified_mount
    )
    real_unlink = Path.unlink

    def stop_after_database_unlink(path, *args, **kwargs):
        if Path(path) == source_wal:
            assert not source.exists()
            raise OSError("simulated interruption after source database unlink")
        return real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", stop_after_database_unlink)
        with pytest.raises(OSError, match="after source database unlink"):
            warehouse.adopt_historical_readiness(
                store,
                capture_id,
                stage_name,
                verification,
                retire_source=True,
                retirement_policy="coexistence-capacity-relief-approved-v1",
                _verified_mount=verified_mount,
            )

    assert not source.exists() and source_wal.is_file()
    assert (stage / "resume-cleanup-intent.json").is_file()
    retry_verification = warehouse.verify_historical_readiness(
        store, capture_id, stage_name, _verified_mount=verified_mount
    )
    retired = warehouse.adopt_historical_readiness(
        store,
        capture_id,
        stage_name,
        retry_verification,
        retire_source=True,
        retirement_policy="coexistence-capacity-relief-approved-v1",
        _verified_mount=verified_mount,
    )
    assert retired["source_retired"] is True
    assert not source.exists() and not source_wal.exists()
    assert source_proof_path.read_bytes() == source_proof_bytes
    assert landing_proof_path.read_bytes() == landing_proof_bytes
    assert frozen_snapshot_path.read_bytes() == frozen_snapshot_bytes


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("root_path", "/different/root"),
        ("root_id", "c" * 32),
        ("capture_id", "d" * 32),
        ("current_device", -1),
        ("device_lineage", [0]),
    ],
)
def test_mount_context_rejects_unbound_identity_fields(store: Store, field, replacement):
    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    state = {"capture_id": "b" * 32}
    current_device = store.root.lstat().st_dev
    prior_device = current_device + 1
    verified_mount = {
        "root_path": str(store.root.resolve()),
        "root_id": logical_root_id,
        "capture_id": state["capture_id"],
        "current_device": current_device,
        "device_lineage": [prior_device, current_device],
    }
    verified_mount[field] = replacement

    with pytest.raises(CatalogueError):
        historical_resume.normalize_resume_file_identity(
            [prior_device, 17, 23, 31],
            store=store,
            state=state,
            verified_mount=verified_mount,
        )
