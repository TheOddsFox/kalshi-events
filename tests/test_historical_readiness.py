from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
from conftest import FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import historical_resume, normalize, warehouse
from kalshi_events.common import (
    CatalogueError,
    Store,
    decode,
    fsync_directory,
    fsync_file,
    regular_bytes,
)


def _factory(responses=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: Client(store, budget, responses, **options)


def _row(index: int) -> tuple[str | None, ...]:
    values = {name: f"{name}-{index}" for name in warehouse._LANDING_COLUMNS}
    values.update(
        observation_id=f"obs-{index}",
        capture_id="c" * 32,
        entity_kind="markets",
        entity_ticker=f"M-{index}",
        provider_updated_at=None,
        series_ticker=None,
        task_scope="markets_history",
    )
    return tuple(values[name] for name in warehouse._LANDING_COLUMNS)


def _position(index: int) -> dict[str, object]:
    page = index // 2
    return {
        "receipt_position": page + 1,
        "page_id": f"{page + 1:032x}",
        "page_context_sha256": f"{page + 1:064x}",
        "row_offset": index % 2,
    }


def _landing_connection(rows, database: Path | None = None):
    connection = duckdb.connect(str(database)) if database is not None else duckdb.connect()
    connection.execute("create schema landing")
    nullable = {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"}
    columns = ", ".join(
        f'"{name}" varchar{"" if name in nullable else " not null"}'
        for name in warehouse._LANDING_COLUMNS
    )
    connection.execute(f"create table landing.observations ({columns})")
    if rows:
        connection.executemany(
            f"insert into landing.observations values ({','.join('?' for _ in rows[0])})",
            rows,
        )
    return connection


def test_cursor_replays_verified_prefix_but_does_not_compare_it_twice(monkeypatch):
    rows = [_row(index) for index in range(5)]
    positions = [_position(index) for index in range(len(rows))]
    binding = {"candidate_sha256": "a" * 64, "scope_sha256": "b" * 64}
    saved = []
    connection = _landing_connection(rows)
    try:
        checked, result = historical_resume.compare_landing_rows_resumable(
            connection,
            iter(zip(rows, positions, strict=True)),
            warehouse._LANDING_COLUMNS,
            nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
            check_deadline=lambda: None,
            binding=binding,
            save_cursor=saved.append,
            row_limit=2,
            byte_limit=1024,
        )
        assert checked == len(rows)
        assert result["observation_position"] == len(rows)
        assert saved

        replayed = []

        def expected_rows():
            replayed.extend(range(len(rows)))
            yield from zip(rows, positions, strict=True)

        cursor, prefix, start, last_position, prefix_rows = historical_resume.resume_landing_cursor(
            expected_rows,
            saved[-1],
            binding,
            check_deadline=lambda: None,
        )
        assert replayed == list(range(len(rows)))
        assert start == len(rows)
        assert prefix_rows == len(rows)
        assert last_position == positions[-1]

        monkeypatch.setattr(
            historical_resume,
            "_next_window_end",
            lambda *args, **kwargs: pytest.fail("cursor resume compared an already sealed prefix"),
        )
        resumed, _ = historical_resume.compare_landing_rows_resumable(
            connection,
            cursor,
            warehouse._LANDING_COLUMNS,
            nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
            check_deadline=lambda: None,
            binding=binding,
            save_cursor=lambda _proof: None,
            start_position=start,
            prefix_checksum=prefix,
            last_position=last_position,
            replayed_prefix_rows=prefix_rows,
        )
        assert resumed == len(rows)
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("changed_key", "changed_value"),
    [
        ("candidate_sha256", "b" * 64),
        ("evidence_sha256", "c" * 64),
        ("code_sha256", "d" * 64),
    ],
)
def test_cursor_binding_or_prefix_change_restarts_from_zero(changed_key, changed_value):
    rows = [_row(index) for index in range(3)]
    positions = [_position(index) for index in range(len(rows))]
    binding = {
        "candidate_sha256": "a" * 64,
        "evidence_sha256": "e" * 64,
        "code_sha256": "f" * 64,
    }
    proof = historical_resume.proof_record(
        "historical-landing-cursor",
        binding,
        {
            "observation_position": len(rows),
            "prefix_sha256": "0" * 64,
            "position": positions[-1],
            "prefix_replay_rows": 0,
            "prefix_replay_policy": "normalize-deduplicate-and-digest-v1",
        },
    )
    fresh, _checksum, start, position, replayed = historical_resume.resume_landing_cursor(
        lambda: iter(zip(rows, positions, strict=True)),
        proof,
        {**binding, changed_key: changed_value},
        check_deadline=lambda: None,
    )
    assert start == replayed == 0 and position is None
    assert next(fresh) == (rows[0], positions[0])

    cursor = historical_resume.proof_record(
        "historical-landing-cursor",
        binding,
        {
            "observation_position": 1,
            "prefix_sha256": "0" * 64,
            "position": positions[0],
            "prefix_replay_rows": 0,
            "prefix_replay_policy": "normalize-deduplicate-and-digest-v1",
        },
    )
    replay_calls = []

    def changed_prefix():
        replay_calls.append(True)
        changed = ("changed", *rows[0][1:])
        yield changed, positions[0]
        yield from zip(rows[1:], positions[1:], strict=True)

    fresh, _checksum, start, position, replayed = historical_resume.resume_landing_cursor(
        changed_prefix,
        cursor,
        binding,
        check_deadline=lambda: None,
    )
    assert replay_calls == [True]
    assert start == replayed == 0 and position is None
    assert next(fresh) == (("changed", *rows[0][1:]), positions[0])
    assert replay_calls == [True, True]


def test_persisted_mid_prefix_cursor_replays_prefix_then_detects_changed_suffix(
    store: Store, monkeypatch
):
    rows = [_row(index) for index in range(5)]
    positions = [_position(index) for index in range(len(rows))]
    binding = {
        "candidate_sha256": "a" * 64,
        "evidence_sha256": "b" * 64,
        "code_sha256": "c" * 64,
    }
    database = store.path("tmp/test-historical-cursor.duckdb")
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = _landing_connection(rows, database)
    cursor_relative = "tmp/test-historical-cursor.json"
    cursor_path = store.path(cursor_relative)

    def interrupt_after_durable_cursor(proof):
        store.write(cursor_relative, proof)
        fsync_file(cursor_path)
        fsync_directory(cursor_path.parent)
        raise RuntimeError("simulated worker stop after checkpoint")

    try:
        with pytest.raises(RuntimeError, match="simulated worker stop"):
            historical_resume.compare_landing_rows_resumable(
                connection,
                iter(zip(rows, positions, strict=True)),
                warehouse._LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=lambda: None,
                binding=binding,
                save_cursor=interrupt_after_durable_cursor,
                row_limit=2,
                byte_limit=1024,
            )
    finally:
        connection.close()

    cursor = decode(regular_bytes(cursor_path, 1024**2))
    assert cursor["result"]["observation_position"] == 2
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "update landing.observations set observation_id = 'changed' where rowid = 3"
        )
        expected, checksum, start, last_position, replayed = (
            historical_resume.resume_landing_cursor(
                lambda: iter(zip(rows, positions, strict=True)),
                cursor,
                binding,
                check_deadline=lambda: None,
            )
        )
        assert (start, replayed, last_position) == (2, 2, positions[1])

        starts = []
        original_window = historical_resume._next_window_end

        def record_window(connection, columns, start, row_limit, byte_limit):
            starts.append(start)
            return original_window(connection, columns, start, row_limit, byte_limit)

        monkeypatch.setattr(historical_resume, "_next_window_end", record_window)
        with pytest.raises(CatalogueError, match="differs"):
            historical_resume.compare_landing_rows_resumable(
                connection,
                expected,
                warehouse._LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=lambda: None,
                binding=binding,
                save_cursor=lambda _proof: None,
                start_position=start,
                prefix_checksum=checksum,
                last_position=last_position,
                replayed_prefix_rows=replayed,
                row_limit=2,
                byte_limit=1024,
            )
        assert starts[0] == 2
    finally:
        connection.close()


@pytest.mark.parametrize("actual_count, message", [(3, "missing"), (5, "extra")])
def test_cursor_comparison_rejects_missing_or_extra_rows(actual_count, message):
    rows = [_row(index) for index in range(4)]
    actual = rows[:actual_count] if actual_count < len(rows) else [*rows, _row(99)]
    connection = _landing_connection(actual)
    try:
        with pytest.raises(CatalogueError, match=message):
            historical_resume.compare_landing_rows_resumable(
                connection,
                iter(zip(rows, [_position(i) for i in range(len(rows))], strict=True)),
                warehouse._LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=lambda: None,
                binding={"candidate_sha256": "a" * 64},
                save_cursor=lambda _proof: None,
            )
    finally:
        connection.close()


def test_empty_landing_has_a_complete_zero_row_cursor():
    connection = _landing_connection([])
    saved = []
    try:
        count, result = historical_resume.compare_landing_rows_resumable(
            connection,
            iter(()),
            warehouse._LANDING_COLUMNS,
            nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
            check_deadline=lambda: None,
            binding={"candidate_sha256": "a" * 64},
            save_cursor=saved.append,
        )
        assert count == 0
        assert result["observation_position"] == 0
        assert len(saved) == 1
    finally:
        connection.close()


def test_page_context_index_is_disk_backed_deduplicated_and_bounded(store: Store):
    relative = "tmp/test-historical-page-index.sqlite"
    store.path("tmp").mkdir(parents=True, exist_ok=True)
    index = historical_resume.PageContextIndex(store, relative, max_entries=8)
    try:
        assert store._storage_reservations == index.reservation
        assert index.reservation <= 128 * 1024**2
        assert index.add("1" * 32, "a" * 64)
        assert not index.add("1" * 32, "a" * 64)
        with pytest.raises(CatalogueError, match="contradictory"):
            index.add("1" * 32, "b" * 64)
        assert store.path(relative).is_file()
        with warehouse._duckdb_work_config(store, store.path("tmp/page-index-spill")) as config:
            assert int(config["max_temp_directory_size"].removesuffix("B")) > 0
    finally:
        index.close()
    assert not store.path(relative).exists()
    assert store._storage_reservations == 0


def test_readiness_verify_adopt_and_install_keep_source_and_lineage(store: Store, monkeypatch):
    state = acquisition.capture(store, client_factory=_factory({}))
    capture_id = state["capture_id"]
    warehouse.build_historical(store, capture_id)

    stage_name = "build-historical-" + "d" * 32
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    source = stage / "warehouse.duckdb"
    shutil.copyfile(store.path(f"builds-historical/{capture_id}/warehouse.duckdb"), source)
    store.sync(stage_relative)

    source_snapshot = warehouse.historical_readiness_snapshot(store, capture_id, stage_name)
    assert source_snapshot["phase"] == "historical-source-snapshot"
    source_hash = source_snapshot["result"]["source_sha256"]
    assert source_hash == warehouse.file_digest(source, store.limits.max_database_bytes)[0]

    prepared = warehouse.prepare_historical_readiness(store, capture_id, stage_name)
    assert prepared["ready"] is True and prepared["adopted"] is False
    assert prepared["source_sha256"] == source_hash
    assert prepared["capture_state_sha256"] == source_snapshot["binding"]["capture_state_sha256"]
    assert (
        prepared["receipt_evidence_sha256"] == source_snapshot["result"]["receipt_evidence_sha256"]
    )
    candidate = store.path(f"{stage_relative}/resume-candidate")
    before_verify = sorted(path.name for path in candidate.iterdir())
    candidate_hash = prepared["candidate_sha256"]

    verification = warehouse.verify_historical_readiness(store, capture_id, stage_name)
    assert verification["phase"] == "historical-readiness-verification"
    assert verification["binding"]["candidate_sha256"] == candidate_hash
    assert sorted(path.name for path in candidate.iterdir()) == before_verify

    with pytest.raises(CatalogueError, match="receipt"):
        warehouse.adopt_historical_readiness(store, capture_id, stage_name, {})
    assert not store.path(f"{stage_relative}/resume-adoption.json").exists()

    adopted = warehouse.adopt_historical_readiness(store, capture_id, stage_name, verification)
    assert adopted["adopted"] is True
    assert adopted["source_retired"] is False
    assert source.is_file()
    assert not store.path(f"{stage_relative}/resume-cleanup-intent.json").exists()

    monkeypatch.setattr(
        warehouse,
        "_iter_historical_landing_rows",
        lambda *args, **kwargs: pytest.fail("exact adopted phase seal must reuse landing proof"),
    )
    receipt = warehouse.build_historical(store, capture_id, resume_historical_stage=stage_name)
    assert receipt["historical_adoption"]["sha256"] == adopted["adoption_sha256"]
    installed = store.path(f"builds-historical/{capture_id}/historical-adoption.json")
    assert installed.is_file()
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt


def test_adopted_candidate_revalidates_current_phase_before_late_retirement(
    store: Store, monkeypatch
):
    state = acquisition.capture(store, client_factory=_factory({}))
    capture_id = state["capture_id"]
    warehouse.build_historical(store, capture_id)

    stage_name = "build-historical-" + "e" * 32
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    source = stage / "warehouse.duckdb"
    shutil.copyfile(store.path(f"builds-historical/{capture_id}/warehouse.duckdb"), source)
    store.sync(stage_relative)

    prepared = warehouse.prepare_historical_readiness(store, capture_id, stage_name)
    verification = warehouse.verify_historical_readiness(store, capture_id, stage_name)
    adopted = warehouse.adopt_historical_readiness(store, capture_id, stage_name, verification)
    adoption_path = store.path(f"{stage_relative}/resume-adoption.json")
    origin_bytes = adoption_path.read_bytes()
    original_candidate_sha256 = prepared["candidate_sha256"]

    candidate = store.path(f"{stage_relative}/resume-candidate/warehouse.duckdb")
    with duckdb.connect(str(candidate)) as connection:
        connection.execute("create schema derived")
        connection.execute("create table derived.marker (value varchar)")
    changed_candidate_sha256 = warehouse.file_digest(candidate, store.limits.max_database_bytes)[0]
    assert changed_candidate_sha256 != original_candidate_sha256

    monkeypatch.setattr(
        warehouse,
        "_copy_resume_file",
        lambda *args, **kwargs: pytest.fail("adopted candidate must not recopy its source"),
    )
    replay_calls = 0
    original_rows = warehouse._iter_historical_landing_rows

    def count_replay(*args, **kwargs):
        nonlocal replay_calls
        replay_calls += 1
        yield from original_rows(*args, **kwargs)

    monkeypatch.setattr(warehouse, "_iter_historical_landing_rows", count_replay)
    revalidated = warehouse.prepare_historical_readiness(store, capture_id, stage_name)
    assert revalidated["ready"] is True and revalidated["adopted"] is True
    assert revalidated["adoption_sha256"] == adopted["adoption_sha256"]
    assert revalidated["candidate_sha256"] == changed_candidate_sha256
    assert replay_calls > 0

    current_verification = warehouse.verify_historical_readiness(store, capture_id, stage_name)
    assert current_verification["binding"]["candidate_sha256"] == changed_candidate_sha256
    assert current_verification["binding"]["adoption_origin_sha256"] == adopted["adoption_sha256"]
    late_adoption = warehouse.adopt_historical_readiness(
        store,
        capture_id,
        stage_name,
        current_verification,
        retire_source=True,
        retirement_policy="coexistence-capacity-relief-approved-v1",
    )
    assert late_adoption["adoption_sha256"] == adopted["adoption_sha256"]
    assert late_adoption["candidate_sha256"] == changed_candidate_sha256
    assert not source.exists()
    assert adoption_path.read_bytes() == origin_bytes
    assert store.path(f"{stage_relative}/resume-cleanup-intent.json").is_file()


@pytest.mark.parametrize("module", [acquisition, historical_resume, normalize])
def test_runtime_code_identity_covers_semantic_package_modules(monkeypatch, module):
    before = warehouse._historical_resume_code_identity()
    original = Path.read_bytes
    target = Path(module.__file__).resolve()

    def changed_read(path):
        body = original(path)
        if path.resolve() == target:
            return body + b"\n# synthetic semantic change\n"
        return body

    monkeypatch.setattr(Path, "read_bytes", changed_read)
    assert warehouse._historical_resume_code_identity() != before


def test_resume_file_identity_normalizes_only_a_verified_device_change(store: Store):
    from kalshi_events.incremental import root_id

    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    state = {"capture_id": "b" * 32}
    current_device = store.root.lstat().st_dev
    prior_device = current_device + 1
    persisted = [prior_device, 17, 23, 31]
    live = (current_device, 17, 23, 31)
    verified_mount = {
        "root_path": str(store.root.resolve()),
        "root_id": logical_root_id,
        "capture_id": state["capture_id"],
        "current_device": current_device,
        "device_lineage": [prior_device, current_device],
    }

    assert (
        historical_resume.normalize_resume_file_identity(
            persisted, store=store, state=state, verified_mount=verified_mount
        )
        == live
    )
    assert historical_resume.resume_file_identity_matches(
        persisted, live, store=store, state=state, verified_mount=verified_mount
    )
    assert historical_resume.normalize_resume_file_identity(
        persisted, store=store, state=state
    ) == tuple(persisted)
    assert not historical_resume.resume_file_identity_matches(
        persisted, live, store=store, state=state
    )
    for offset in (1, 2, 3):
        changed = list(live)
        changed[offset] += 1
        assert not historical_resume.resume_file_identity_matches(
            persisted, changed, store=store, state=state, verified_mount=verified_mount
        )
    with pytest.raises(CatalogueError, match="current operator mount"):
        historical_resume.resume_file_identity_matches(
            persisted,
            (prior_device, 17, 23, 31),
            store=store,
            state=state,
            verified_mount=verified_mount,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("root_path", "/another/operator-root"),
        ("root_id", "c" * 32),
        ("capture_id", "d" * 32),
        ("current_device", -1),
        ("device_lineage", [0]),
    ],
)
def test_resume_file_identity_rejects_unbound_mount_context(store: Store, field, value):
    from kalshi_events.incremental import root_id

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
    verified_mount[field] = value

    with pytest.raises(CatalogueError):
        historical_resume.normalize_resume_file_identity(
            [prior_device, 17, 23, 31],
            store=store,
            state=state,
            verified_mount=verified_mount,
        )


def test_stage_cleanup_preserves_unrelated_adoption_across_mount_change(store: Store, monkeypatch):
    from kalshi_events.incremental import root_id

    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    active = acquisition.capture(store, client_factory=_factory({}))
    unrelated = acquisition.capture(store, client_factory=_factory({}))
    active_state = warehouse.read_capture(store, active["capture_id"])
    for state in (active, unrelated):
        warehouse.build_historical(store, state["capture_id"])

    current_device = store.root.lstat().st_dev
    prior_device = current_device + 1
    original_identity = warehouse._resume_file_identity

    def prior_mount_identity(path):
        _device, inode, size, mtime_ns = original_identity(path)
        return prior_device, inode, size, mtime_ns

    active_stage_name = "build-historical-" + "f" * 32
    unrelated_stage_name = "build-historical-" + "e" * 32
    stages = []
    monkeypatch.setattr(warehouse, "_resume_file_identity", prior_mount_identity)
    for state, stage_name in (
        (active, active_stage_name),
        (unrelated, unrelated_stage_name),
    ):
        stage_relative = f"tmp/{stage_name}"
        stage = store.path(stage_relative)
        stage.mkdir(parents=True)
        source = stage / "warehouse.duckdb"
        shutil.copyfile(
            store.path(f"builds-historical/{state['capture_id']}/warehouse.duckdb"), source
        )
        store.sync(stage_relative)
        warehouse.prepare_historical_readiness(store, state["capture_id"], stage_name)
        verification = warehouse.verify_historical_readiness(store, state["capture_id"], stage_name)
        warehouse.adopt_historical_readiness(store, state["capture_id"], stage_name, verification)
        proof_paths = (
            stage / "resume-candidate" / "resume-source.json",
            stage / "resume-adoption.json",
        )
        stages.append((stage, proof_paths, tuple(path.read_bytes() for path in proof_paths)))

    monkeypatch.setattr(warehouse, "_resume_file_identity", original_identity)
    context = {
        "root_path": str(store.root.resolve()),
        "root_id": logical_root_id,
        "capture_id": active["capture_id"],
        "current_device": current_device,
        "device_lineage": [prior_device, current_device],
    }
    fresh_snapshot = warehouse.historical_readiness_snapshot(
        store, active["capture_id"], active_stage_name, _verified_mount=context
    )
    _source_proof, source_result = warehouse._read_source_proof(
        store,
        warehouse.read_capture(store, active["capture_id"]),
        active_stage_name,
        store.path(f"tmp/{active_stage_name}/resume-candidate"),
    )
    assert fresh_snapshot["result"]["source_identity"][0] == current_device
    assert historical_resume.normalize_resume_file_identity(
        source_result["source_identity"],
        store=store,
        state=active_state,
        verified_mount=context,
    ) == tuple(fresh_snapshot["result"]["source_identity"])
    with store.writer_guard():
        warehouse.recover_and_cleanup_abandoned_historical_stages(
            store, active["capture_id"], _verified_mount=context
        )

    for stage, proof_paths, proof_bytes in stages:
        assert stage.is_dir()
        assert tuple(path.read_bytes() for path in proof_paths) == proof_bytes
