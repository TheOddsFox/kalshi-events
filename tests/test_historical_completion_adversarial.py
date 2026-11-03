from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import pytest
from conftest import FakeClient

from kalshi_events import capture, historical_resume, warehouse
from kalshi_events.common import BudgetExceeded, CatalogueError, Store
from kalshi_events.historical import HISTORICAL_CONTRACT

STAGE_NAME = "build-historical-" + "f" * 32
RETIREMENT_POLICY = "coexistence-capacity-relief-approved-v1"


@pytest.fixture(scope="module")
def historical_source(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("historical-completion") / "operator")
    state = capture.capture(store, client_factory=FakeClient)
    warehouse.build_historical(store, state["capture_id"])
    return store.root, state["capture_id"]


@pytest.fixture
def completion_stage(tmp_path, historical_source):
    source_root, capture_id = historical_source
    root = tmp_path / "operator"
    shutil.copytree(source_root, root)
    store = Store(root)
    stage = store.path(f"tmp/{STAGE_NAME}")
    stage.mkdir(parents=True)
    source = store.path(f"{warehouse.build_directory(capture_id, 3)}/warehouse.duckdb")
    shutil.copyfile(source, stage / "warehouse.duckdb")
    return store, capture_id, stage


def _stage_snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def _verify_in_separate_process(store: Store, capture_id: str) -> dict:
    repository = Path(__file__).parents[1]
    script = """
import json, sys
from pathlib import Path
from kalshi_events.common import Store
from kalshi_events.warehouse import verify_historical_readiness
print(json.dumps(verify_historical_readiness(Store(Path(sys.argv[1])), sys.argv[2], sys.argv[3]), sort_keys=True))
"""
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    source_path = str(repository / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        value for value in (source_path, environment.get("PYTHONPATH", "")) if value
    )
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(store.root),
            capture_id,
            STAGE_NAME,
        ],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _source_with_durable_wal(stage: Path, tmp_path: Path) -> tuple[Path, Path]:
    source = stage / "warehouse.duckdb"
    snapshot = tmp_path / "source-with-wal.duckdb"
    shutil.copyfile(source, snapshot)
    with duckdb.connect(str(snapshot)) as connection:
        connection.execute("begin transaction")
        connection.execute("create table retirement_fault_probe(value bigint)")
        connection.execute("insert into retirement_fault_probe values (1)")
        connection.execute("commit")
        wal = snapshot.with_name(snapshot.name + ".wal")
        assert wal.is_file(), "DuckDB did not create the synthetic source WAL"
        shutil.copyfile(snapshot, source)
        source_wal = source.with_name(source.name + ".wal")
        shutil.copyfile(wal, source_wal)
    return source, source_wal


def _landing_counts(database: Path) -> dict[str, int]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return {
            "observation_count": connection.execute(
                "select count(*) from landing.observations"
            ).fetchone()[0],
            "membership_count": connection.execute(
                "select count(*) from landing.membership"
            ).fetchone()[0],
            "eligibility_count": connection.execute(
                "select count(*) from landing.parent_eligibility"
            ).fetchone()[0],
        }


def test_interrupted_wal_retirement_keeps_intent_and_retries_exact_adoption(
    completion_stage, tmp_path, monkeypatch
):
    store, capture_id, stage = completion_stage
    source, source_wal = _source_with_durable_wal(stage, tmp_path)
    source_hash = warehouse.file_digest(source, store.limits.max_database_bytes)[0]
    source_wal_hash = warehouse.file_digest(source_wal, store.limits.max_database_bytes)[0]
    source_identity = warehouse._resume_file_identity(source)
    source_wal_identity = warehouse._resume_file_identity(source_wal)

    readiness = warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
    assert readiness["ready"] is True
    before_verify = _stage_snapshot(stage)
    verification = _verify_in_separate_process(store, capture_id)
    assert _stage_snapshot(stage) == before_verify

    source_fsyncs: set[Path] = set()
    directory_fsyncs: set[Path] = set()
    deletion_order: list[Path] = []
    real_fsync_file = warehouse.fsync_file
    real_fsync_directory = warehouse.fsync_directory
    real_unlink = Path.unlink
    intent_path = stage / "resume-cleanup-intent.json"

    def record_fsync_file(path):
        result = real_fsync_file(path)
        source_fsyncs.add(Path(path))
        return result

    def record_fsync_directory(path):
        result = real_fsync_directory(path)
        directory_fsyncs.add(Path(path))
        return result

    def interrupt_wal_unlink(path, *args, **kwargs):
        if Path(path) in {source, source_wal}:
            assert intent_path.is_file(), "source deletion preceded durable cleanup intent"
            assert intent_path in source_fsyncs
            assert stage in directory_fsyncs
            intent = json.loads(intent_path.read_text())
            assert intent["binding"]["retirement_policy"] == RETIREMENT_POLICY
            assert intent["result"]["source_sha256"] == source_hash
            assert intent["result"]["source_wal_sha256"] == source_wal_hash
            assert intent["result"]["source_identity"] == list(source_identity)
            assert intent["result"]["source_wal_identity"] == list(source_wal_identity)
            deletion_order.append(Path(path))
            if Path(path) == source_wal:
                assert not source.exists(), "WAL removal must follow the exact source DB"
                raise OSError("simulated interruption during source WAL retirement")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(warehouse, "fsync_file", record_fsync_file)
    monkeypatch.setattr(warehouse, "fsync_directory", record_fsync_directory)
    monkeypatch.setattr(Path, "unlink", interrupt_wal_unlink)
    with pytest.raises(OSError, match="simulated interruption"):
        warehouse.adopt_historical_readiness(
            store,
            capture_id,
            STAGE_NAME,
            verification,
            retire_source=True,
            retirement_policy=RETIREMENT_POLICY,
        )

    assert deletion_order == [source, source_wal]
    assert not source.exists() and source_wal.is_file()
    assert (stage / "resume-adoption.json").is_file()
    assert intent_path.is_file()
    candidate = stage / "resume-candidate" / "warehouse.duckdb"
    assert candidate.is_file()
    candidate_hash = warehouse.file_digest(candidate, store.limits.max_database_bytes)[0]
    monkeypatch.setattr(Path, "unlink", real_unlink)

    adopted = warehouse.adopt_historical_readiness(
        store,
        capture_id,
        STAGE_NAME,
        verification,
        retire_source=True,
        retirement_policy=RETIREMENT_POLICY,
    )

    assert adopted["source_retired"] is True
    assert adopted["source_sha256"] == source_hash
    assert adopted["candidate_sha256"] != source_hash
    assert not source.exists() and not source_wal.exists()
    assert warehouse.file_digest(candidate, store.limits.max_database_bytes)[0] == candidate_hash


def test_adopted_origin_survives_derived_phase_and_installed_build(completion_stage, monkeypatch):
    store, capture_id, stage = completion_stage
    source = stage / "warehouse.duckdb"
    source_hash = warehouse.file_digest(source, store.limits.max_database_bytes)[0]
    warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
    verification = warehouse.verify_historical_readiness(store, capture_id, STAGE_NAME)
    adoption = warehouse.adopt_historical_readiness(store, capture_id, STAGE_NAME, verification)
    adoption_bytes = (stage / "resume-adoption.json").read_bytes()
    origin_candidate_hash = adoption["candidate_sha256"]

    candidate = stage / "resume-candidate" / "warehouse.duckdb"
    with duckdb.connect(str(candidate)) as connection:
        connection.execute(
            "create table main.controlled_derived_phase_probe as select 'derived' as value"
        )
        connection.execute("checkpoint")
    derived_candidate_hash = warehouse.file_digest(candidate, store.limits.max_database_bytes)[0]
    assert derived_candidate_hash != origin_candidate_hash

    monkeypatch.setattr(
        warehouse,
        "_copy_resume_file",
        lambda *args, **kwargs: pytest.fail("adopted candidate must not be recopied"),
    )
    refreshed = warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
    assert refreshed["ready"] is True
    assert refreshed["adopted"] is True
    assert refreshed["origin_candidate_sha256"] == origin_candidate_hash
    assert refreshed["candidate_sha256"] == derived_candidate_hash
    assert refreshed["prefix_replay_rows"] == 0
    assert warehouse.file_digest(source, store.limits.max_database_bytes)[0] == source_hash
    assert (stage / "resume-adoption.json").read_bytes() == adoption_bytes

    receipt = warehouse.build_historical(store, capture_id, resume_historical_stage=STAGE_NAME)
    build = store.path(f"{warehouse.build_directory(capture_id, 3)}/historical-adoption.json")
    installed_adoption = json.loads(build.read_text())
    assert receipt["historical_adoption"]["sha256"] == installed_adoption["sha256"]
    assert receipt["historical_adoption"]["candidate_sha256"] == origin_candidate_hash
    assert receipt["historical_adoption"]["source_sha256"] == source_hash
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt
    assert not stage.exists()


@pytest.mark.parametrize(
    "tamper_suffix", [False, True], ids=["resume-prefix", "reject-stale-cursor"]
)
def test_prepare_persists_and_revalidates_interrupted_landing_cursor(
    completion_stage, monkeypatch, tamper_suffix
):
    store, capture_id, stage = completion_stage
    source = stage / "warehouse.duckdb"
    source_hash = warehouse.file_digest(source, store.limits.max_database_bytes)[0]
    total_rows = _landing_counts(source)["observation_count"]
    assert total_rows > 1, "fixture must compare a nonempty real historical landing"

    candidate = stage / "resume-candidate"
    candidate_database = candidate / "warehouse.duckdb"
    cursor_path = candidate / "resume-landing-cursor.json"
    real_compare = historical_resume.compare_landing_rows_resumable

    def interrupt_after_first_durable_cursor(connection, expected_rows, columns, **kwargs):
        assert kwargs["start_position"] == 0
        assert kwargs["replayed_prefix_rows"] == 0
        save_cursor = kwargs["save_cursor"]
        kwargs["row_limit"] = 1

        def save_then_interrupt(proof):
            save_cursor(proof)
            assert cursor_path.is_file()
            saved = json.loads(cursor_path.read_text())
            assert saved == proof
            assert saved["result"]["observation_position"] == 1
            assert saved["result"]["observation_position"] < total_rows
            raise BudgetExceeded("simulated budget stop after durable landing cursor")

        kwargs["save_cursor"] = save_then_interrupt
        return real_compare(connection, expected_rows, columns, **kwargs)

    monkeypatch.setattr(
        historical_resume,
        "compare_landing_rows_resumable",
        interrupt_after_first_durable_cursor,
    )
    with pytest.raises(BudgetExceeded, match="durable landing cursor"):
        warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)

    assert cursor_path.is_file()
    persisted_cursor = json.loads(cursor_path.read_text())
    assert persisted_cursor["result"]["observation_position"] == 1
    assert persisted_cursor["result"]["observation_position"] < total_rows
    assert source.is_file()
    assert warehouse.file_digest(source, store.limits.max_database_bytes)[0] == source_hash
    assert candidate_database.is_file()
    original_candidate_hash = warehouse.file_digest(
        candidate_database, store.limits.max_database_bytes
    )[0]
    assert persisted_cursor["binding"]["candidate_sha256"] == original_candidate_hash
    assert not (stage / "resume-readiness.json").exists()
    assert not (stage / "resume-adoption.json").exists()
    assert not (stage / "resume-cleanup-intent.json").exists()
    assert not list(candidate.glob("resume-page-index.sqlite*"))
    assert store._storage_reservations == 0

    if tamper_suffix:
        with duckdb.connect(str(candidate_database)) as connection:
            connection.execute(
                "update landing.observations set observation_id = observation_id || '-tampered' "
                "where rowid = ?",
                [total_rows - 1],
            )
            connection.execute("checkpoint")
        assert (
            warehouse.file_digest(candidate_database, store.limits.max_database_bytes)[0]
            != original_candidate_hash
        )

    monkeypatch.setattr(
        warehouse,
        "_copy_resume_file",
        lambda *args, **kwargs: pytest.fail("retry must reuse the persisted candidate"),
    )
    attempts = []

    def observe_retry(connection, expected_rows, columns, **kwargs):
        kwargs["row_limit"] = 1
        attempts.append(
            (kwargs["start_position"], kwargs["replayed_prefix_rows"], kwargs["row_limit"])
        )
        return real_compare(connection, expected_rows, columns, **kwargs)

    monkeypatch.setattr(historical_resume, "compare_landing_rows_resumable", observe_retry)
    if tamper_suffix:
        with pytest.raises(CatalogueError, match="differs from receipt-normalized rows"):
            warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
        assert attempts == [(0, 0, 1)]
        assert not (stage / "resume-readiness.json").exists()
        assert not (stage / "resume-adoption.json").exists()
        assert not (stage / "resume-cleanup-intent.json").exists()
        assert source.is_file()
        assert warehouse.file_digest(source, store.limits.max_database_bytes)[0] == source_hash
        assert candidate_database.is_file()
        assert not list(candidate.glob("resume-page-index.sqlite*"))
        assert store._storage_reservations == 0
        return

    readiness = warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
    assert attempts == [(1, 1, 1)]
    assert readiness["ready"] is True
    assert readiness["prefix_replay_rows"] == 1
    assert readiness["observation_count"] == total_rows
    assert readiness["candidate_sha256"] == original_candidate_hash
    assert readiness["membership_count"] > 0
    assert readiness["eligibility_count"] >= 0
    assert warehouse.file_digest(source, store.limits.max_database_bytes)[0] == source_hash

    expected_counts = _landing_counts(candidate_database)
    assert {name: readiness[name] for name in expected_counts} == expected_counts
    state = capture.read_capture(store, capture_id)
    warehouse._check_historical_coverage(
        candidate_database, capture_id, state.get("complete") is True
    )
    with duckdb.connect(str(candidate_database), read_only=True) as connection:
        coverage = connection.execute(
            "select historical_contract, scope, omitted_scopes, source_capture_id, "
            "source_capture_complete from main.catalogue_coverage"
        ).fetchone()
    assert coverage == (
        HISTORICAL_CONTRACT,
        "historical",
        ["events_combo", "markets_live", "markets_combo"],
        capture_id,
        state.get("complete") is True,
    )

    before_verification = _stage_snapshot(stage)
    verification = _verify_in_separate_process(store, capture_id)
    assert _stage_snapshot(stage) == before_verification
    assert verification["phase"] == "historical-readiness-verification"
    assert verification["result"] == expected_counts
    assert verification["binding"]["candidate_sha256"] == original_candidate_hash


def test_malformed_candidate_releases_page_index_and_same_store_can_retry(
    completion_stage, monkeypatch
):
    store, capture_id, stage = completion_stage
    source = stage / "warehouse.duckdb"
    source_hash = warehouse.file_digest(source, store.limits.max_database_bytes)[0]
    state = capture.read_capture(store, capture_id)
    selection, task_indices = warehouse._historical_profile(state)

    with store.writer_guard():
        candidate_relative, candidate, source_proof_sha256, recovered_wal = (
            warehouse._prepare_historical_resume_candidate(store, state, STAGE_NAME)
        )
        assert recovered_wal is False
        index = warehouse.DiskParentIndex(store, collect_winners=False)
        try:
            database = candidate / "warehouse.duckdb"
            with duckdb.connect(str(database)) as connection:
                connection.execute("drop table landing.observations")
                connection.execute("checkpoint")

            with pytest.raises(CatalogueError, match="candidate landing schema is invalid"):
                warehouse._resume_historical_landing(
                    store,
                    state,
                    STAGE_NAME,
                    candidate_relative,
                    candidate,
                    source_proof_sha256,
                    index,
                    task_indices,
                    selection,
                    force_revalidate=True,
                )
        finally:
            index.close()

    page_index_files = list(candidate.glob("resume-page-index.sqlite*"))
    assert page_index_files == []
    assert store._storage_reservations == 0
    assert source.is_file()
    assert warehouse.file_digest(source, store.limits.max_database_bytes)[0] == source_hash
    assert not (candidate / "resume-landing-cursor.json").exists()

    shutil.copyfile(source, candidate / "warehouse.duckdb")
    assert (
        warehouse.file_digest(candidate / "warehouse.duckdb", store.limits.max_database_bytes)[0]
        == source_hash
    )
    monkeypatch.setattr(
        warehouse,
        "_copy_resume_file",
        lambda *args, **kwargs: pytest.fail("retry must reuse the repaired candidate"),
    )
    readiness = warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)

    assert readiness["ready"] is True
    assert readiness["observation_count"] == _landing_counts(source)["observation_count"]
    assert readiness["observation_count"] > 1
    assert readiness["prefix_replay_rows"] == 0
    assert store._storage_reservations == 0
    assert list(candidate.glob("resume-page-index.sqlite*")) == []


def test_slow_candidate_count_interrupts_as_budget_before_page_index(monkeypatch, completion_stage):
    store, capture_id, stage = completion_stage
    state = capture.read_capture(store, capture_id)
    selection, task_indices = warehouse._historical_profile(state)
    candidate_database = stage / "resume-candidate" / "warehouse.duckdb"

    with store.writer_guard():
        candidate_relative, candidate, source_proof_sha256, recovered_wal = (
            warehouse._prepare_historical_resume_candidate(store, state, STAGE_NAME)
        )
        assert recovered_wal is False
        index = warehouse.DiskParentIndex(store, collect_winners=False)
        real_connect = warehouse.duckdb.connect
        connections = []
        original_started = store.started

        class SlowCandidateConnection:
            def __init__(self, connection):
                self.connection = connection
                self.production_interrupt_called = False
                self.fallback_interrupt_called = False
                self.native_query_interrupted = False
                self.slow_count_entered = False

            def __enter__(self):
                self.connection.__enter__()
                # Let setup finish, then give the real count query a short live deadline.
                store.started = time.monotonic() - store.limits.max_duration_seconds + 0.2
                return self

            def __exit__(self, *args):
                return self.connection.__exit__(*args)

            def interrupt(self):
                self.production_interrupt_called = True
                self.connection.interrupt()

            def _safety_interrupt(self):
                self.fallback_interrupt_called = True
                self.connection.interrupt()

            def execute(self, query, *args, **kwargs):
                if query.strip().lower() != "select count(*) from landing.observations":
                    return self.connection.execute(query, *args, **kwargs)
                self.slow_count_entered = True
                # Keep old implementations bounded while requiring the Store timer to win.
                fallback = threading.Timer(1.5, self._safety_interrupt)
                fallback.daemon = True
                fallback.start()
                try:
                    return self.connection.execute(
                        "select count(*) from landing.observations "
                        "cross join range(1000000000000) as slow(i) "
                        "where hash(i) = 0"
                    )
                except duckdb.InterruptException:
                    self.native_query_interrupted = True
                    raise
                finally:
                    fallback.cancel()

        def connect(path, *args, **kwargs):
            connection = real_connect(path, *args, **kwargs)
            if Path(path) == candidate_database:
                wrapper = SlowCandidateConnection(connection)
                connections.append(wrapper)
                return wrapper
            return connection

        monkeypatch.setattr(warehouse.duckdb, "connect", connect)
        try:
            with pytest.raises(BudgetExceeded, match="counting historical candidate observations"):
                warehouse._resume_historical_landing(
                    store,
                    state,
                    STAGE_NAME,
                    candidate_relative,
                    candidate,
                    source_proof_sha256,
                    index,
                    task_indices,
                    selection,
                    force_revalidate=True,
                )
        finally:
            index.close()
            store.started = original_started

    assert len(connections) == 1
    assert connections[0].slow_count_entered is True
    assert connections[0].production_interrupt_called is True
    assert connections[0].native_query_interrupted is True
    assert connections[0].fallback_interrupt_called is False
    assert not list(candidate.glob("resume-page-index.sqlite*"))
    assert store._storage_reservations == 0
    assert not (candidate / "resume-landing-cursor.json").exists()

    monkeypatch.setattr(warehouse.duckdb, "connect", real_connect)
    readiness = warehouse.prepare_historical_readiness(store, capture_id, STAGE_NAME)
    assert readiness["ready"] is True
    assert readiness["observation_count"] > 1
    assert readiness["prefix_replay_rows"] == 0
    assert not list(candidate.glob("resume-page-index.sqlite*"))
    assert store._storage_reservations == 0
