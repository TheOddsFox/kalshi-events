from __future__ import annotations

import json
import shutil

import duckdb
import pytest
from conftest import FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import historical_resume, warehouse
from kalshi_events.common import BudgetExceeded, CatalogueError, Store


def _factory(responses=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: Client(store, budget, responses, **options)


def _landing(connection):
    nullable = {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"}
    connection.execute("create schema landing")
    columns = ", ".join(
        f'"{name}" varchar{"" if name in nullable else " not null"}'
        for name in warehouse._LANDING_COLUMNS
    )
    connection.execute(f"create table landing.observations ({columns})")


def _row(index: int, *, payload: str | None = None) -> tuple[str | None, ...]:
    values = {
        "observation_id": f"id-{index}",
        "capture_id": "c" * 32,
        "entity_kind": "markets",
        "entity_ticker": f"M-{index}",
        "received_at": "2026-11-02T00:00:00Z",
        "provider_updated_at": None,
        "source_endpoint": "/historical/markets",
        "source_kind": "direct",
        "payload_json": payload or "{}",
        "event_ticker": f"E-{index}",
        "series_ticker": None,
        "task_scope": "markets_history",
        "task_generation": "0",
    }
    return tuple(values[name] for name in warehouse._LANDING_COLUMNS)


def test_resume_landing_compares_all_columns_in_small_windows():
    rows = [_row(0), _row(1, payload="🌉" * 8), _row(2), _row(3)]
    with duckdb.connect() as connection:
        _landing(connection)
        connection.executemany(
            f"insert into landing.observations values ({','.join('?' for _ in rows[0])})",
            rows,
        )
        assert historical_resume.compare_landing_rows(
            connection,
            iter(rows),
            warehouse._LANDING_COLUMNS,
            nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
            check_deadline=lambda: None,
            row_limit=2,
            byte_limit=32,
        ) == len(rows)


def test_resume_landing_uses_utf8_byte_windows_and_keeps_oversized_rows_alone():
    rows = []
    for index, payload in enumerate(("🌉" * 4, "🌉", "🌉")):
        values = {name: "a" for name in warehouse._LANDING_COLUMNS}
        values.update(
            observation_id=f"{index}",
            provider_updated_at=None,
            payload_json=payload,
            series_ticker=None,
        )
        rows.append(tuple(values[name] for name in warehouse._LANDING_COLUMNS))

    with duckdb.connect() as connection:
        _landing(connection)
        connection.executemany(
            f"insert into landing.observations values ({','.join('?' for _ in rows[0])})",
            rows,
        )
        assert (
            historical_resume._next_window_end(connection, warehouse._LANDING_COLUMNS, 0, 10, 20)
            == 0
        )
        assert (
            historical_resume._next_window_end(connection, warehouse._LANDING_COLUMNS, 1, 10, 20)
            == 1
        )
        assert historical_resume.compare_landing_rows(
            connection,
            iter(rows),
            warehouse._LANDING_COLUMNS,
            nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
            check_deadline=lambda: None,
            row_limit=10,
            byte_limit=20,
        ) == len(rows)


def test_resume_landing_rejects_value_and_schema_changes():
    with duckdb.connect() as connection:
        _landing(connection)
        row = _row(0)
        connection.execute(
            f"insert into landing.observations values ({','.join('?' for _ in row)})", row
        )
        changed = list(row)
        changed[8] = '{"different":true}'
        with pytest.raises(CatalogueError, match="differs"):
            historical_resume.compare_landing_rows(
                connection,
                iter([tuple(changed)]),
                warehouse._LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=lambda: None,
            )

    with duckdb.connect() as connection:
        _landing(connection)
        connection.execute("alter table landing.observations add column rowid varchar")
        with pytest.raises(CatalogueError, match="schema"):
            historical_resume.compare_landing_rows(
                connection,
                iter(()),
                warehouse._LANDING_COLUMNS,
                nullable={"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
                check_deadline=lambda: None,
            )


def test_resume_stage_identity_and_phase_proof_are_bound():
    name = "build-historical-" + "a" * 32
    assert historical_resume.stage_relative(name) == f"tmp/{name}"
    with pytest.raises(CatalogueError):
        historical_resume.stage_relative("a" * 32)
    proof = historical_resume.proof_record("landing", {"capture_id": "a"}, {"rows": 1})
    assert historical_resume.validate_proof(proof, "landing", {"capture_id": "a"}) == {"rows": 1}
    with pytest.raises(CatalogueError, match="binding"):
        historical_resume.validate_proof(proof, "landing", {"capture_id": "b"})


def test_resume_copy_checks_deadline_between_megabyte_chunks(tmp_path, store: Store, monkeypatch):
    source = tmp_path / "source.duckdb"
    destination = tmp_path / "candidate.duckdb"
    source.write_bytes(b"x" * (3 * 1024 * 1024))
    checks = 0

    def expire_after_first_chunk():
        nonlocal checks
        checks += 1
        if checks == 3:
            raise BudgetExceeded("synthetic deadline")

    monkeypatch.setattr(store, "check_deadline", expire_after_first_chunk)
    with pytest.raises(BudgetExceeded, match="synthetic deadline"):
        warehouse._copy_resume_file(store, source, destination, 4 * 1024 * 1024)
    assert destination.stat().st_size == 1024 * 1024
    assert source.stat().st_size == 3 * 1024 * 1024


@pytest.mark.parametrize("candidate_database", [False, True])
def test_cleanup_preserves_explicit_stage_and_only_removes_its_bound_dbt_spill(
    store: Store, candidate_database: bool
):
    state = acquisition.capture(store, client_factory=_factory({}))
    stage_name = "build-historical-" + "d" * 32
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    database = (
        stage / "resume-candidate" / "warehouse.duckdb"
        if candidate_database
        else stage / "warehouse.duckdb"
    )
    database.parent.mkdir(parents=True, exist_ok=True)
    database.write_bytes(b"preserved candidate")
    abandoned = store.path("tmp/build-historical-" + "e" * 32)
    abandoned.mkdir()
    (abandoned / "evidence.bin").write_bytes(b"abandoned")

    runtime = store.path("tmp/dbt-" + "f" * 32)
    spill = runtime / "spill"
    spill.mkdir(parents=True)
    spill_file = spill / "spill.bin"
    spill_file.write_bytes(b"temporary dbt pages")
    (runtime / "logs").mkdir()
    (runtime / "logs" / "dbt.log").write_text("forensic log")
    store.write(
        f"tmp/{runtime.name}/profiles.yml",
        {
            "kalshi_events": {
                "target": "local",
                "outputs": {
                    "local": {
                        "path": str(database),
                        "config_options": {"temp_directory": str(spill)},
                    }
                },
            }
        },
    )
    store.sync("tmp")

    with store.lock():
        warehouse.recover_and_cleanup_abandoned_historical_stages(
            store, state["capture_id"], preserve_stage=stage_name
        )

    assert database.read_bytes() == b"preserved candidate"
    assert not abandoned.exists()
    assert not spill_file.exists()
    assert (runtime / "profiles.yml").is_file()
    assert (runtime / "logs" / "dbt.log").read_text() == "forensic log"
    cleanup_proof = json.loads((runtime / "historical-spill-cleanup.json").read_text())
    assert cleanup_proof["result"]["total_bytes"] == len(b"temporary dbt pages")
    assert cleanup_proof["binding"]["database"] == str(database)


@pytest.mark.parametrize(
    "case",
    [
        "matching",
        "missing",
        "wrong-capture",
        "disabled",
        "inline-conflict",
        "symlink",
        "symlink-parent",
        "hardlink",
        "large",
    ],
)
def test_interrupted_dbt_manifest_uses_bounded_compiled_artifacts(store: Store, case: str):
    capture_id = "a" * 32
    stage = "tmp/build-historical-" + "d" * 32
    database = store.path(f"{stage}/warehouse.duckdb")
    database.parent.mkdir(parents=True)
    database.write_bytes(b"saved database")
    runtime = store.path("tmp/dbt-" + "f" * 32)
    spill = runtime / "spill"
    spill.mkdir(parents=True)
    (spill / "spill.bin").write_bytes(b"temporary pages")
    store.write(
        f"tmp/{runtime.name}/profiles.yml",
        {
            "kalshi_events": {
                "outputs": {
                    "local": {
                        "path": str(database),
                        "config_options": {"temp_directory": str(spill)},
                    }
                }
            }
        },
    )
    nodes = {}
    compiled = runtime / "target/compiled/kalshi_events/models"
    compiled.mkdir(parents=True)
    for name in ("events", "series"):
        model = f"_historical_current_{name}"
        nodes[f"model.kalshi_events.{model}"] = {
            "config": {"enabled": True},
            "original_file_path": f"models/{model}.sql",
        }
        (compiled / f"{model}.sql").write_text(
            f"select * from population p\nwhere p.capture_id = '{capture_id}'\n"
        )
    events = compiled / "_historical_current_events.sql"
    if case == "missing":
        events.unlink()
    elif case == "wrong-capture":
        events.write_text("where p.capture_id = '" + "b" * 32 + "'\n")
    elif case == "disabled":
        nodes["model.kalshi_events._historical_current_events"]["config"]["enabled"] = False
    elif case == "inline-conflict":
        nodes["model.kalshi_events._historical_current_events"]["compiled_code"] = "select 1"
    elif case == "symlink-parent":
        original = compiled.with_name("original")
        compiled.rename(original)
        compiled.symlink_to(original, target_is_directory=True)
    elif case in {"symlink", "hardlink"}:
        original = events.with_suffix(".original")
        events.rename(original)
        if case == "symlink":
            events.symlink_to(original)
        else:
            events.hardlink_to(original)
    elif case == "large":
        with events.open("ab") as stream:
            stream.truncate(1024**2 + 1)
    store.write(
        f"tmp/{runtime.name}/target/manifest.json",
        {"metadata": {"project_name": "kalshi_events"}, "nodes": nodes},
    )

    with store.writer_guard():
        warehouse._cleanup_resumed_historical_dbt_spills(store, capture_id, stage)
    assert (spill / "spill.bin").exists() is (case != "matching")
    assert database.read_bytes() == b"saved database"


def test_partial_spill_cleanup_retry_keeps_the_original_inventory(store: Store, monkeypatch):
    state = acquisition.capture(store, client_factory=_factory({}))
    stage_name = "build-historical-" + "e" * 32
    stage_relative = f"tmp/{stage_name}"
    database = store.path(f"{stage_relative}/warehouse.duckdb")
    database.parent.mkdir(parents=True)
    database.write_bytes(b"preserved source")

    runtime = store.path("tmp/dbt-" + "f" * 32)
    spill = runtime / "spill"
    spill.mkdir(parents=True)
    first = spill / "first.bin"
    second = spill / "second.bin"
    first.write_bytes(b"first page")
    second.write_bytes(b"second page")
    store.write(
        f"tmp/{runtime.name}/profiles.yml",
        {
            "kalshi_events": {
                "target": "local",
                "outputs": {
                    "local": {
                        "path": str(database),
                        "config_options": {"temp_directory": str(spill)},
                    }
                },
            }
        },
    )

    original_remove = store.remove
    removed = 0

    def fail_after_one(relative):
        nonlocal removed
        if relative.startswith(f"tmp/{runtime.name}/spill/"):
            removed += 1
            if removed == 2:
                raise OSError("simulated interrupted cleanup")
        original_remove(relative)

    monkeypatch.setattr(store, "remove", fail_after_one)
    with pytest.raises(OSError, match="simulated interrupted"):
        warehouse._cleanup_resumed_historical_dbt_spills(store, state["capture_id"], stage_relative)

    original_inventory = json.loads((runtime / "historical-spill-cleanup.json").read_text())[
        "result"
    ]["files"]
    assert {item["path"] for item in original_inventory} == {
        first.relative_to(store.root).as_posix(),
        second.relative_to(store.root).as_posix(),
    }
    monkeypatch.setattr(store, "remove", original_remove)
    warehouse._cleanup_resumed_historical_dbt_spills(store, state["capture_id"], stage_relative)
    proof_paths = list(runtime.glob("historical-spill-cleanup*.json"))
    assert len(proof_paths) == 2
    assert (
        json.loads((runtime / "historical-spill-cleanup.json").read_text())["result"]["files"]
        == original_inventory
    )
    retry_proof = next(path for path in proof_paths if path.name != "historical-spill-cleanup.json")
    retry_files = json.loads(retry_proof.read_text())["result"]["files"]
    assert len(retry_files) == 1
    warehouse._cleanup_resumed_historical_dbt_spills(store, state["capture_id"], stage_relative)
    updated_proofs = list(runtime.glob("historical-spill-cleanup*.json"))
    assert len(updated_proofs) == 3
    latest_proof = next(path for path in updated_proofs if path not in proof_paths)
    assert json.loads(latest_proof.read_text())["result"]["files"] == []
    assert not first.exists() and not second.exists()


def test_explicit_resume_adopts_valid_landing_and_reuses_exact_derived_models(
    store: Store, monkeypatch
):
    state = acquisition.capture(store, client_factory=_factory({}))
    capture_id = state["capture_id"]
    warehouse.build_historical(store, capture_id)

    stage_name = "build-historical-" + "c" * 32
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    source_db = stage / "warehouse.duckdb"
    built_db = store.path(f"builds-historical/{capture_id}/warehouse.duckdb")
    shutil.copyfile(built_db, source_db)

    # Model the process stopping after compact models and relationships are
    # complete but before combo materialization and final tests.
    with duckdb.connect(str(source_db)) as connection:
        connection.execute("drop view main.combo_legs")
        connection.execute("drop table landing.combo_legs_canonical")
        connection.execute("checkpoint")
    source_db_hash = warehouse.file_digest(source_db, store.limits.max_database_bytes)
    source_wal = source_db.with_name(source_db.name + ".wal")
    source_wal_hash = (
        warehouse.file_digest(source_wal, store.limits.max_database_bytes)
        if source_wal.exists()
        else None
    )

    with duckdb.connect(str(source_db), read_only=True) as connection:
        description = connection.execute("describe landing.observations").fetchall()
        assert tuple(column[0] for column in description) == warehouse._LANDING_COLUMNS
        assert connection.execute("select count(*) from landing.observations").fetchone()[0] > 0

    monkeypatch.setattr(
        warehouse,
        "_land_historical_rows",
        lambda *args, **kwargs: pytest.fail("resume must not reload historical rows"),
    )
    monkeypatch.setattr(
        warehouse,
        "_land",
        lambda *args, **kwargs: pytest.fail("resume must not use general loader"),
    )

    dbt_calls = []
    original_dbt = warehouse.dbt

    def record_dbt(*args, **kwargs):
        result = original_dbt(*args, **kwargs)
        command = list(args[1])
        artifact_path = kwargs.get("artifacts")
        result_nodes = []
        if artifact_path is not None:
            artifacts = json.loads(store.path(artifact_path).read_text())
            run_results = artifacts.get("run_results.json", {}).get("content", {})
            result_nodes = [
                item.get("unique_id")
                for item in run_results.get("results", [])
                if isinstance(item, dict)
            ]
        dbt_calls.append((command, result_nodes))
        return result

    monkeypatch.setattr(warehouse, "dbt", record_dbt)
    original_install = warehouse._install_build
    source_hashes_at_adoption = []

    def record_install(*args, **kwargs):
        before = (
            warehouse.file_digest(source_db, store.limits.max_database_bytes),
            warehouse.file_digest(source_wal, store.limits.max_database_bytes)
            if source_wal.exists()
            else None,
        )
        receipt = original_install(*args, **kwargs)
        after = (
            warehouse.file_digest(source_db, store.limits.max_database_bytes),
            warehouse.file_digest(source_wal, store.limits.max_database_bytes)
            if source_wal.exists()
            else None,
        )
        source_hashes_at_adoption.append((before, after, source_db.exists()))
        return receipt

    monkeypatch.setattr(warehouse, "_install_build", record_install)
    receipt = warehouse.build_historical(store, capture_id, resume_historical_stage=stage_name)

    assert receipt["observation_count"] > 0
    assert len(source_hashes_at_adoption) == 1
    before, after, source_survived_install = source_hashes_at_adoption[0]
    assert before == after == (source_db_hash, source_wal_hash)
    assert source_survived_install
    assert not stage.exists()
    prepared_run_results = [results for command, results in dbt_calls if command[0] == "run"]
    assert len(prepared_run_results) == 1
    assert not any("._historical_current_" in unique_id for unique_id in prepared_run_results[0])
    assert not any(
        unique_id == "model.kalshi_events.relationships"
        for _command, results in dbt_calls
        for unique_id in results
    )
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt


@pytest.mark.parametrize("corrupt", [False, True])
def test_explicit_resume_missing_or_corrupt_stage_fails_without_loader_fallback(
    store: Store, monkeypatch, corrupt
):
    state = acquisition.capture(store, client_factory=_factory({}))
    stage_name = "build-historical-" + "d" * 32
    if corrupt:
        stage = store.path(f"tmp/{stage_name}")
        stage.mkdir(parents=True)
        (stage / "warehouse.duckdb").write_bytes(b"not a DuckDB database")

    monkeypatch.setattr(
        warehouse,
        "_land_historical_rows",
        lambda *args, **kwargs: pytest.fail("invalid explicit stage must not reload"),
    )
    monkeypatch.setattr(
        warehouse,
        "_land",
        lambda *args, **kwargs: pytest.fail("invalid explicit stage must not use general loader"),
    )
    with pytest.raises(CatalogueError):
        warehouse.build_historical(store, state["capture_id"], resume_historical_stage=stage_name)
