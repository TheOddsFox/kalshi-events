"""Pinned baseline and lineage checks for opt-in composed catalogue releases."""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from kalshi_events import metrics
from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    Store,
    decode,
    durable_mkdir,
    fsync_directory,
    interrupt_at_deadline,
    json_bytes,
    now,
    path_is_link,
    regular_bytes,
    validate_series_scope,
)

MAX_DELTAS = 8
OVERLAP = timedelta(minutes=5)
SERIES_CONTRACT = "oddsfox.kalshi.series.v1"
CATALOGUE_V2_CONTRACT = "oddsfox.kalshi.catalogue.v2"


def pointer_profile(store: Store) -> tuple[str, int, str]:
    """Return the sole publication pointer and contract for this operator root."""
    if store.scope is not None:
        return "current-series.json", 2, SERIES_CONTRACT
    return "current-v2.json", 2, CATALOGUE_V2_CONTRACT


def publication_pointers(store: Store) -> tuple[tuple[int, str], ...]:
    if store.scope is not None:
        return ((2, "current-series.json"),)
    return ((1, "current.json"), (2, "current-v2.json"))


def _utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise CatalogueError(f"{label} must be a UTC timestamp")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CatalogueError(f"{label} must be a UTC timestamp") from exc
    if instant.utcoffset() != timedelta(0):
        raise CatalogueError(f"{label} must be a UTC timestamp")
    return instant.astimezone(UTC)


def root_id(store: Store, *, create: bool = False) -> str:
    """Read the stable logical root identity, creating it only for v2 work."""
    if store.storage_version not in {2, 3, 4, 5} or (create and store.storage_version != 5):
        raise CatalogueError("incremental catalogue work requires an upgraded storage-v5 root")
    if create and not store.writer_locked_by_current_thread:
        raise CatalogueError("root identity creation requires the canonical writer lock")
    path = store.path("root-id.json")
    if not path.exists():
        if not create:
            raise CatalogueError(
                "operator root has no v2 identity; start v2 work before copying it"
            )
        value = uuid.uuid4().hex
        store.write("root-id.json", {"version": 1, "root_id": value})
        return value
    data = decode(regular_bytes(path, 1024**2))
    value = data.get("root_id") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or set(data) != {"version", "root_id"}
        or data.get("version") != 1
        or not isinstance(value, str)
        or len(value) != 32
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise CatalogueError("invalid operator root identity")
    return value


def baseline(
    store: Store, *, started_at: str | None = None, create_root_id: bool = True
) -> dict[str, Any]:
    """Return the currently effective verified publication and its sealed next window."""
    from kalshi_events import publish

    identity = root_id(store, create=create_root_id)
    if store.scope is not None:
        pointer_name, version, contract = pointer_profile(store)
        if not store.path(pointer_name).exists():
            raise CatalogueError("series refresh requires a verified series baseline")
        current = publish.current(store, catalogue_version=2)
        if (
            current.get("root_id") != identity
            or current.get("contract") != contract
            or current.get("series_scope") != store.scope
        ):
            raise CatalogueError("series baseline belongs to a different root or scope")
        depth = current.get("delta_depth")
        if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= MAX_DELTAS:
            raise CatalogueError("invalid series baseline delta depth")
        watermark = _utc(current.get("watermark_started_at"), "published watermark")
    elif store.path("current-v2.json").exists():
        current = publish.current(store, catalogue_version=2)
        if current.get("root_id") != identity:
            raise CatalogueError("v2 baseline belongs to a different logical operator root")
        depth = current.get("delta_depth")
        if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= MAX_DELTAS:
            raise CatalogueError("invalid v2 baseline delta depth")
        watermark = _utc(current.get("watermark_started_at"), "published watermark")
        version = 2
        contract = CATALOGUE_V2_CONTRACT
    else:
        if store.path("current.json").exists():
            current = publish.current(store, catalogue_version=1)
        else:
            raise CatalogueError("incremental capture requires a verified v1 or v2 publication")
        from kalshi_events.capture import read_capture

        state = read_capture(store, current["capture_id"])
        watermark = _utc(state.get("started_at"), "baseline capture start")
        depth = 0
        version = 1
        contract = "oddsfox.kalshi.catalogue.v1"
    if depth >= MAX_DELTAS:
        raise CatalogueError("eight deltas already depend on this baseline; publish a full rebase")
    started_at = started_at or now()
    sealed_start = _utc(started_at, "incremental capture start")
    if sealed_start < watermark:
        raise CatalogueError("incremental capture start precedes the published watermark")
    overlap_start = watermark - OVERLAP
    pointer_name = (
        pointer_profile(store)[0]
        if store.scope is not None
        else ("current-v2.json" if version == 2 else "current.json")
    )
    pointer = decode(regular_bytes(store.path(pointer_name), 1024**2))
    if pointer.get("source_release_id") != current["source_release_id"]:
        raise CatalogueError("publication pointer changed while pinning incremental baseline")
    result = {
        "root_id": identity,
        "baseline": {
            "catalogue_version": version,
            "source_release_id": current["source_release_id"],
            "manifest_sha256": pointer.get("manifest_sha256"),
            "capture_id": current["capture_id"],
            "root_id": identity,
            "delta_depth": depth,
        },
        "watermark_started_at": sealed_start.isoformat().replace("+00:00", "Z"),
        "previous_watermark": watermark.isoformat().replace("+00:00", "Z"),
    }
    if store.scope is not None:
        result["baseline"].update(
            release_contract=contract,
            series_scope=dict(store.scope),
        )
    else:
        result["query_from"] = int(overlap_start.timestamp())
    verify_pin(store, result)
    return result


def verify_pin(store: Store, state: dict[str, Any]) -> dict[str, Any]:
    """Fail closed if the pinned publication or logical root has changed."""
    from kalshi_events import publish

    pin = state.get("baseline")
    if not isinstance(pin, dict) or pin.get("root_id") != root_id(store):
        raise CatalogueError("incremental baseline root identity mismatch")
    pin_scope = None
    if store.scope is not None:
        pin_scope = validate_series_scope(pin.get("series_scope"))
        if pin_scope != store.scope:
            raise CatalogueError("pinned series baseline belongs to a different contract or scope")
    version = pin.get("catalogue_version")
    release_id = pin.get("source_release_id")
    if not isinstance(release_id, str) or len(release_id) != 64:
        raise CatalogueError("invalid incremental baseline release identity")
    directory = store.path(f"releases/{release_id}")
    current = publish.verify_release(directory, store.limits.max_root_bytes)
    manifest_body = regular_bytes(directory / "manifest.json", 1024**2)
    if (
        current.get("source_release_id") != pin.get("source_release_id")
        or hashlib.sha256(manifest_body).hexdigest() != pin.get("manifest_sha256")
        or current.get("capture_id") != pin.get("capture_id")
        or current.get("root_id", pin.get("root_id")) != pin.get("root_id")
    ):
        raise CatalogueError("pinned incremental baseline publication changed")
    if store.scope is not None:
        if (
            pin.get("catalogue_version") != 2
            or pin.get("release_contract") != SERIES_CONTRACT
            or pin_scope != store.scope
            or current.get("contract") != SERIES_CONTRACT
            or current.get("series_scope") != store.scope
        ):
            raise CatalogueError("pinned series baseline belongs to a different contract or scope")
    elif (
        pin.get("release_contract") == SERIES_CONTRACT or current.get("contract") == SERIES_CONTRACT
    ):
        raise CatalogueError("series publication cannot be used as a catalogue baseline")
    expected_depth = 0 if version == 1 else current.get("delta_depth")
    if pin.get("delta_depth") != expected_depth:
        raise CatalogueError("incremental baseline depth differs from its pinned publication")
    if version == 2:
        _, ancestors, _ = _lineage(
            store,
            release_id,
            manifest_sha256=pin["manifest_sha256"],
            expected_root=pin["root_id"],
            expected_scope=store.scope,
        )
        if pin["capture_id"] not in ancestors:
            raise CatalogueError("pinned v2 release ancestry omits its capture")
    with _baseline_data(store, pin["capture_id"], version, pin):
        pass
    return current


def build_directory(capture_id: str, version: int) -> str:
    return f"builds-v2/{capture_id}" if version == 2 else f"builds/{capture_id}"


def _rows(connection: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    result = connection.execute(f"select * from main.{table}")
    names = [column[0] for column in result.description]
    return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


@contextmanager
def _baseline_data_impl(
    store: Store, capture_id: str, version: int, pin: dict[str, Any] | None = None
) -> Iterator[duckdb.DuckDBPyConnection]:
    from kalshi_events.warehouse import certified

    receipt = certified(store, capture_id, catalogue_version=version)
    if not isinstance(pin, dict):
        raise CatalogueError("baseline semantic verification requires its release pin")
    from kalshi_events import publish

    directory = store.path(f"releases/{pin['source_release_id']}")
    manifest = publish.verify_release(directory, store.limits.max_root_bytes)
    if hashlib.sha256(regular_bytes(directory / "manifest.json", 1024**2)).hexdigest() != pin.get(
        "manifest_sha256"
    ):
        raise CatalogueError("pinned release manifest changed")
    relative = build_directory(capture_id, version) + "/warehouse.duckdb"
    database = store.path(relative)
    if store.path(relative + ".wal").exists():
        raise CatalogueError("incremental baseline warehouse has an uncheckpointed WAL")
    usage = store.usage()
    spill_budget = min(
        store.limits.max_temp_bytes - usage["temporary_bytes"],
        store.limits.max_root_bytes - usage["retained_bytes"],
    )
    if spill_budget < 1:
        raise BudgetExceeded("no bounded temporary storage is available for baseline verification")
    store.reserve_storage(spill_budget)
    spill_directory = store.path(f"tmp/baseline-verify-{uuid.uuid4().hex}")
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        spill_directory.mkdir(parents=True)
        store.check_deadline()
        connection = duckdb.connect(
            str(database),
            read_only=True,
            config={
                "memory_limit": "512MB",
                "temp_directory": str(spill_directory),
                "max_temp_directory_size": f"{spill_budget}B",
            },
        )
        from kalshi_events.warehouse import RELATIONS, membership_fingerprint

        warehouse_hash_fast_path = version == 2 and manifest.get("warehouse_sha256") == receipt.get(
            "warehouse_sha256"
        )
        with interrupt_at_deadline(store, connection.interrupt):
            try:
                if not warehouse_hash_fast_path:
                    metrics.add("baseline_semantic_comparisons")
                    for relation, table in RELATIONS.items():
                        store.check_deadline()
                        parquet = str(directory / f"{relation}.parquet")
                        parquet_schema = connection.execute(
                            "describe select * from read_parquet(?)", [parquet]
                        ).fetchall()
                        warehouse_schema = connection.execute(
                            f"describe select * from main.{table}"
                        ).fetchall()
                        if [(row[0], row[1]) for row in parquet_schema] != [
                            (row[0], row[1]) for row in warehouse_schema
                        ]:
                            raise CatalogueError(
                                f"baseline {relation} schema differs from its pinned release"
                            )
                        for left, right, label in (
                            (
                                "select * from read_parquet(?)",
                                f"select * from main.{table}",
                                "release",
                            ),
                            (
                                f"select * from main.{table}",
                                "select * from read_parquet(?)",
                                "warehouse",
                            ),
                        ):
                            store.check_deadline()
                            query = f"select 1 from (({left}) except all ({right})) limit 1"
                            if connection.execute(query, [parquet]).fetchone():
                                raise CatalogueError(
                                    f"baseline {relation} {label} rows differ from its pinned release"
                                )
                if version == 2 and not warehouse_hash_fast_path:
                    expected = manifest.get("membership_sha256")
                    if not isinstance(expected, str):
                        raise CatalogueError("pinned v2 publication lacks a membership fingerprint")
                    if membership_fingerprint(store, capture_id, connection=connection) != expected:
                        raise CatalogueError(
                            "baseline warehouse membership differs from its pinned release"
                        )
                yield connection
            except duckdb.InterruptException as exc:
                raise BudgetExceeded("operation deadline exhausted") from exc
    finally:
        if connection is not None:
            connection.close()
        shutil.rmtree(spill_directory, ignore_errors=True)
        store.sync("tmp")
        store.release_storage(spill_budget)


@contextmanager
def _baseline_data(
    store: Store, capture_id: str, version: int, pin: dict[str, Any] | None = None
) -> Iterator[duckdb.DuckDBPyConnection]:
    with (
        metrics.phase("baseline_verification"),
        _baseline_data_impl(store, capture_id, version, pin) as connection,
    ):
        yield connection


def baseline_membership(store: Store, pin: dict[str, Any]) -> dict[str, set[str]]:
    """Read scope membership from a freshly verified published baseline."""
    return _baseline_snapshot(store, pin)["membership"]


def _baseline_snapshot(store: Store, pin: dict[str, Any]) -> dict[str, Any]:
    """Read compact membership and parent maps alongside one baseline verification."""
    capture_id = pin["capture_id"]
    version = pin["catalogue_version"]
    with _baseline_data(store, capture_id, version, pin) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "select table_name as name from information_schema.tables where table_schema='landing'"
            ).fetchall()
        }
        if version == 2 and "membership" in tables:
            rows = connection.execute(
                "select scope, entity_ticker from landing.membership"
            ).fetchall()
            result: dict[str, set[str]] = {
                scope: set()
                for scope in (
                    "series",
                    "events_live",
                    "events_combo",
                    "markets_live",
                    "markets_combo",
                    "markets_history",
                )
            }
            for scope, entity in rows:
                result.setdefault(scope, set()).add(entity)
            markets = {
                market: (received, observation_id, event)
                for market, event, received, observation_id in connection.execute(
                    """select entity_ticker, event_ticker, received_at, observation_id
                       from landing.observations
                       where entity_kind='markets' and source_kind='direct'
                       qualify row_number() over (
                         partition by entity_ticker order by received_at desc, observation_id desc
                       ) = 1"""
                ).fetchall()
            }
            events = {
                event: (received, observation_id, series)
                for event, series, received, observation_id in connection.execute(
                    """select entity_ticker, series_ticker, received_at, observation_id
                       from landing.observations
                       where entity_kind='events' and source_kind='direct'
                       qualify row_number() over (
                         partition by entity_ticker order by received_at desc, observation_id desc
                       ) = 1"""
                ).fetchall()
            }
            return {"membership": result, "markets": markets, "events": events}
        if "observations" not in tables:
            raise CatalogueError("legacy baseline lacks membership source evidence")
        current_series = {
            row[0]
            for row in connection.execute(
                "select series_ticker from main.current_series"
            ).fetchall()
        }
        current_events = {
            row[0]
            for row in connection.execute("select event_ticker from main.current_events").fetchall()
        }
        current_markets = {
            market: (bool(collection) or bool(legs))
            for market, collection, legs in connection.execute(
                """select market_ticker,
                          json_extract_string(payload_json, '$.mve_collection_ticker'),
                          json_array_length(json_extract(payload_json, '$.mve_selected_legs'))
                   from main.current_markets"""
            ).fetchall()
        }
        proofs = connection.execute(
            """select distinct entity_kind, entity_ticker, source_endpoint
               from landing.observations where source_kind='direct'"""
        ).fetchall()
        result = {
            key: set()
            for key in (
                "series",
                "events_live",
                "events_combo",
                "markets_live",
                "markets_combo",
                "markets_history",
            )
        }
        for kind, identity, endpoint in proofs:
            if kind == "series" and endpoint == "/series" and identity in current_series:
                result["series"].add(identity)
            elif kind == "events" and identity in current_events:
                if endpoint == "/events":
                    result["events_live"].add(identity)
                elif endpoint == "/events/multivariate":
                    result["events_combo"].add(identity)
            elif kind == "markets" and identity in current_markets:
                combo = current_markets[identity]
                if endpoint == "/markets":
                    result["markets_combo" if combo else "markets_live"].add(identity)
                elif endpoint == "/historical/markets":
                    result["markets_history"].add(identity)
        market_parents = {
            market: (received, observation_id, event)
            for market, event, received, observation_id in connection.execute(
                """select entity_ticker, event_ticker, received_at, observation_id
                   from landing.observations
                   where entity_kind='markets' and source_kind='direct'
                   qualify row_number() over (
                     partition by entity_ticker order by received_at desc, observation_id desc
                   ) = 1"""
            ).fetchall()
        }
        event_parents = {
            event: (received, observation_id, series)
            for event, series, received, observation_id in connection.execute(
                """select entity_ticker, series_ticker, received_at, observation_id
                   from landing.observations
                   where entity_kind='events' and source_kind='direct'
                   qualify row_number() over (
                     partition by entity_ticker order by received_at desc, observation_id desc
                   ) = 1"""
            ).fetchall()
        }
        return {"membership": result, "markets": market_parents, "events": event_parents}


def parent_lookup_tasks(
    store: Store,
    state: dict[str, Any],
    identities: Any,
) -> list[dict[str, Any]]:
    """Find active parent identities that require a fresh point lookup in this capture."""
    effective_events, effective_lookups = identities.effective_parent_evidence(state)
    scope_values: dict[str, set[str]] = {}
    final_scope_values: dict[str, set[str]] = {}
    for index, scope in state.get("task_scopes", {}).items():
        work = state["tasks"][int(index)]
        generations = identities.scope_membership.get(scope, {})
        final_values = set(generations.get(work["restarts"], set()))
        if state.get("incremental"):
            values = final_values
        else:
            # Preserve v1's union-of-observed rows while also checking the final
            # traversal generation for the closure required by a v2 rebase.
            values = set().union(*generations.values()) if generations else set()
        if scope == "markets_live" and not work.get("params", {}).get("mve_filter"):
            combo_generations = identities.scope_membership.get("markets_combo", {})
            if state.get("incremental"):
                values = set(values) | combo_generations.get(work["restarts"], set())
                final_values |= combo_generations.get(work["restarts"], set())
            elif combo_generations:
                values = set(values) | set().union(*combo_generations.values())
                final_values |= combo_generations.get(work["restarts"], set())
        scope_values.setdefault(scope, set()).update(values)
        final_scope_values.setdefault(scope, set()).update(final_values)
        if scope == "markets_live" and not work.get("params", {}).get("mve_filter"):
            scope_values.setdefault("markets_combo", set()).update(
                identities.scope_membership.get("markets_combo", {}).get(work["restarts"], set())
                if state.get("incremental")
                else set().union(*identities.scope_membership.get("markets_combo", {}).values())
                if identities.scope_membership.get("markets_combo")
                else set()
            )
            final_scope_values.setdefault("markets_combo", set()).update(
                combo_generations.get(work["restarts"], set())
            )

    if state.get("incremental"):
        pin = state["baseline"]
        cache = getattr(identities, "_baseline_snapshot", None)
        if not isinstance(cache, tuple) or cache[0] != pin.get("source_release_id"):
            cache = (pin.get("source_release_id"), _baseline_snapshot(store, pin))
            identities._baseline_snapshot = cache
        snapshot = cache[1]
        base = snapshot["membership"]
        if state.get("scope") is not None:
            effective = {scope: set(values) for scope, values in scope_values.items()}
            for scope in base:
                effective.setdefault(scope, set())
        else:
            effective = {scope: set(values) for scope, values in base.items()}
            for scope, values in scope_values.items():
                if scope in {"events_live", "markets_live"}:
                    effective.setdefault(scope, set()).update(values)
                else:
                    effective[scope] = set(values)
        baseline_markets = snapshot["markets"]
        baseline_events = snapshot["events"]
    else:
        effective = scope_values
        baseline_markets = {}
        baseline_events = {}

    market_members = set().union(
        effective.get("markets_live", set()),
        effective.get("markets_combo", set()),
        effective.get("markets_history", set()),
    )
    event_members = set(effective.get("events_live", set())) | set(
        effective.get("events_combo", set())
    )
    series_members = set(effective.get("series", set()))
    market_winners = {
        market: winner for market, winner in baseline_markets.items() if market in market_members
    }
    for market, winner in identities.markets.items():
        if market in market_members and (
            market not in market_winners or winner[:2] > market_winners[market][:2]
        ):
            market_winners[market] = winner
    lookup_events = {
        identity
        for (kind, identity), outcome in effective_lookups.items()
        if kind == "events" and outcome[0] == 200
    }
    active_events = event_members | lookup_events
    event_winners = {
        event: winner for event, winner in baseline_events.items() if event in active_events
    }
    for event, winner in effective_events.items():
        if event in active_events and (
            event not in event_winners or winner[:2] > event_winners[event][:2]
        ):
            event_winners[event] = winner
    event_parents = {event: winner[2] for event, winner in event_winners.items()}
    missing_events = {
        parent
        for winner in market_winners.values()
        for parent in (winner[2],)
        if parent and parent not in event_members and ("events", parent) not in effective_lookups
    }
    missing_series = {
        parent
        for event, parent in event_parents.items()
        if event in active_events
        and parent
        and parent not in series_members
        and ("series", parent) not in effective_lookups
    }
    if state.get("scope") is not None:
        for winner in market_winners.values():
            parent = winner[2]
            outcome = effective_lookups.get(("events", parent)) if parent else None
            if parent and parent not in event_members and outcome is not None and outcome[0] != 200:
                raise CatalogueError("series market winner lacks an authoritative event parent")
        for event, parent in event_parents.items():
            outcome = effective_lookups.get(("series", parent)) if parent else None
            if (
                event in active_events
                and parent
                and parent not in series_members
                and outcome is not None
                and outcome[0] != 200
            ):
                raise CatalogueError("series event winner lacks an authoritative series parent")
    if not state.get("incremental"):
        final_market_members = set().union(
            final_scope_values.get("markets_live", set()),
            final_scope_values.get("markets_combo", set()),
            final_scope_values.get("markets_history", set()),
        )
        final_event_members = final_scope_values.get("events_live", set()) | final_scope_values.get(
            "events_combo", set()
        )
        final_series_members = final_scope_values.get("series", set())
        final_market_parents = {
            market: winner[2]
            for market, winner in identities.markets.items()
            if market in final_market_members
        }
        missing_events.update(
            parent
            for parent in final_market_parents.values()
            if parent
            and parent not in final_event_members
            and ("events", parent) not in effective_lookups
        )
        final_active_events = final_event_members | {
            identity
            for (kind, identity), outcome in effective_lookups.items()
            if kind == "events" and outcome[0] == 200
        }
        final_event_parents = {
            event: winner[2]
            for event, winner in effective_events.items()
            if event in final_active_events
        }
        missing_series.update(
            parent
            for event, parent in final_event_parents.items()
            if parent
            and parent not in final_series_members
            and ("series", parent) not in effective_lookups
        )
    from kalshi_events.capture import parent_lookup_plan

    return parent_lookup_plan(state, missing_events, missing_series)


def verify_predecessor(store: Store, state: dict[str, Any]) -> None:
    """A resumed delta cannot publish over a newer or different branch."""
    from kalshi_events import publish

    pin = state.get("baseline")
    if not isinstance(pin, dict):
        raise CatalogueError("incremental capture lacks a baseline pin")
    pointer_name, effective_version, expected_contract = pointer_profile(store)
    if store.scope is None:
        effective_version = 2 if store.path("current-v2.json").exists() else 1
        pointer_name = "current-v2.json" if effective_version == 2 else "current.json"
        expected_contract = (
            CATALOGUE_V2_CONTRACT if effective_version == 2 else "oddsfox.kalshi.catalogue.v1"
        )
    elif not store.path(pointer_name).exists():
        raise CatalogueError("series predecessor pointer is missing")
    if effective_version != pin.get("catalogue_version"):
        raise CatalogueError(
            "stale incremental baseline; a newer "
            f"{'scoped' if store.scope is not None else 'catalogue'} predecessor exists"
        )
    current = publish.current(store, catalogue_version=effective_version)
    pointer = decode(regular_bytes(store.path(pointer_name), 1024**2))
    if (
        current.get("source_release_id") != pin.get("source_release_id")
        or pointer.get("manifest_sha256") != pin.get("manifest_sha256")
        or current.get("capture_id") != pin.get("capture_id")
        or current.get("contract") != expected_contract
        or (store.scope is not None and current.get("series_scope") != store.scope)
        or (effective_version == 2 and current.get("root_id") != pin.get("root_id"))
    ):
        raise CatalogueError("stale incremental baseline; publish from the current predecessor")


def verify_full_rebase(store: Store, state: dict[str, Any]) -> None:
    """Reject a full-v2 publication that would reset an existing lineage unsafely."""
    from kalshi_events import publish

    pointer_name, _version, _contract = pointer_profile(store)
    if store.scope is None:
        pointer_name = "current-v2.json"
    pointer_path = store.path(pointer_name)
    if not pointer_path.exists():
        return
    current = publish.current(store, catalogue_version=2)
    candidate_id = state.get("capture_id")
    idempotent_current = candidate_id == current.get("capture_id") and not state.get("incremental")
    watermark = _utc(current.get("watermark_started_at"), "published watermark")
    candidate_start = _utc(state.get("started_at"), "full capture start")
    if candidate_start < watermark:
        raise CatalogueError(
            "full rebase predates the current watermark; capture a new full baseline"
        )

    release_id = current["source_release_id"]
    root = current.get("root_id")
    seen: set[str] = set()
    while release_id is not None:
        if release_id in seen:
            raise CatalogueError("cyclic v2 release ancestry")
        seen.add(release_id)
        directory = store.path(f"releases/{release_id}")
        manifest = publish.verify_release(directory, store.limits.max_root_bytes)
        if manifest.get("source_release_id") != release_id:
            raise CatalogueError("v2 ancestry has a mismatched release identity")
        if manifest.get("contract") == "oddsfox.kalshi.catalogue.v1":
            if store.scope is not None:
                raise CatalogueError("series lineage cannot use a catalogue-v1 baseline")
            if manifest.get("capture_id") == candidate_id:
                raise CatalogueError("cannot republish an ancestor full capture as a new rebase")
            break
        expected_contract = SERIES_CONTRACT if store.scope is not None else CATALOGUE_V2_CONTRACT
        if manifest.get("contract") != expected_contract:
            raise CatalogueError("publication ancestry crosses contracts or scopes")
        if manifest.get("root_id") != root or (
            store.scope is not None and manifest.get("series_scope") != store.scope
        ):
            raise CatalogueError("v2 ancestry crosses roots or has a mismatched series scope")
        if manifest.get("capture_id") == candidate_id and not idempotent_current:
            raise CatalogueError("cannot republish an ancestor full capture as a new rebase")
        parent_id = manifest.get("baseline_release_id")
        parent_sha = manifest.get("baseline_manifest_sha256")
        if parent_id is None:
            if parent_sha is not None or manifest.get("baseline_capture_id") is not None:
                raise CatalogueError("invalid v2 root baseline ancestry")
            break
        if (
            not isinstance(parent_id, str)
            or len(parent_id) != 64
            or not isinstance(parent_sha, str)
            or len(parent_sha) != 64
        ):
            raise CatalogueError("invalid v2 release ancestry pin")
        parent_path = store.path(f"releases/{parent_id}/manifest.json")
        if hashlib.sha256(regular_bytes(parent_path, 1024**2)).hexdigest() != parent_sha:
            raise CatalogueError("v2 release ancestry manifest pin mismatch")
        parent = publish.verify_release(parent_path.parent, store.limits.max_root_bytes)
        if parent.get("capture_id") != manifest.get("baseline_capture_id"):
            raise CatalogueError("v2 release ancestry capture mismatch")
        release_id = parent_id
    if idempotent_current:
        if current.get("delta_depth") != 0:
            raise CatalogueError("cannot republish a current delta as a full rebase")
        return
    if candidate_id == current.get("capture_id"):
        raise CatalogueError("cannot republish an ancestor full capture as a new rebase")


def release_fields(store: Store, state: dict[str, Any]) -> dict[str, Any]:
    """Return immutable lineage fields for a catalogue-v2 release manifest.

    A full capture establishes a root baseline. An incremental capture names its exact
    predecessor and advances only to the start watermark sealed before acquisition.
    """
    identity = state.get("root_id")
    if identity is None:
        # A previously captured full-v1 artifact has no root identity. Creating the
        # logical ID only on first v2 work preserves its old evidence identities.
        identity = root_id(store, create=True)
    if not isinstance(identity, str) or identity != root_id(store):
        raise CatalogueError("capture root identity does not match the operator root")
    from kalshi_events.warehouse import membership_fingerprint

    common = {
        "membership_sha256": membership_fingerprint(store, state["capture_id"]),
    }
    series_fields = (
        {
            "coverage_scope": "series",
            "series_ticker": store.scope["series_ticker"],
            "include_multivariate": False,
        }
        if store.scope is not None
        else {}
    )
    if not state.get("incremental"):
        return {
            **common,
            **series_fields,
            "root_id": identity,
            "baseline_release_id": None,
            "baseline_manifest_sha256": None,
            "baseline_capture_id": None,
            "previous_watermark": None,
            "watermark_started_at": state["started_at"],
            "delta_depth": 0,
        }
    verify_pin(store, state)
    return {
        **common,
        **series_fields,
        "root_id": identity,
        "baseline_release_id": state["baseline"]["source_release_id"],
        "baseline_manifest_sha256": state["baseline"]["manifest_sha256"],
        "baseline_capture_id": state["baseline"]["capture_id"],
        "previous_watermark": state["previous_watermark"],
        "watermark_started_at": state["watermark_started_at"],
        "delta_depth": state["baseline"]["delta_depth"] + 1,
    }


def _lineage_impl(
    store: Store,
    release_id: str,
    *,
    manifest_sha256: str | None = None,
    expected_root: str | None = None,
    expected_scope: dict[str, Any] | None = None,
    _context: Any = None,
) -> tuple[set[str], set[str], datetime | None]:
    from kalshi_events import publish

    releases: set[str] = set()
    captures: set[str] = set()
    evidence: dict[str, tuple[int, dict[str, Any]]] = {}
    capture_states: dict[str, dict[str, Any]] = {}
    root = expected_root
    scope = store.scope if expected_scope is None and store.scope is not None else expected_scope
    expected_contract = SERIES_CONTRACT if scope is not None else None
    baseline_started: datetime | None = None
    child_depth: int | None = None
    while release_id:
        if release_id in releases:
            raise CatalogueError("cyclic release ancestry")
        directory = store.path(f"releases/{release_id}")
        manifest_body = regular_bytes(directory / "manifest.json", 1024**2)
        if manifest_sha256 and hashlib.sha256(manifest_body).hexdigest() != manifest_sha256:
            raise CatalogueError("release ancestry manifest pin mismatch")
        manifest = publish.verify_release(directory, store.limits.max_root_bytes)
        if manifest.get("source_release_id") != release_id:
            raise CatalogueError("release ancestry identity mismatch")
        releases.add(release_id)
        capture_id = manifest.get("capture_id")
        if not isinstance(capture_id, str):
            raise CatalogueError("release ancestry lacks a capture identity")
        captures.add(capture_id)
        contract = manifest.get("contract")
        if contract == "oddsfox.kalshi.catalogue.v1":
            if scope is not None:
                raise CatalogueError("series ancestry cannot contain a catalogue release")
            if child_depth not in {None, 1}:
                raise CatalogueError("v2 lineage depth does not match its v1 bootstrap")
            from kalshi_events.capture import read_capture

            cached = _context.captures.get((capture_id, 1)) if _context is not None else None
            baseline_state = (
                cached.proof.state if cached is not None else read_capture(store, capture_id)
            )
            capture_states[capture_id] = baseline_state
            baseline_started = _utc(baseline_state.get("started_at"), "v1 baseline start")
            evidence[capture_id] = (1, manifest)
            break
        if contract not in {CATALOGUE_V2_CONTRACT, SERIES_CONTRACT}:
            raise CatalogueError("unsupported release in ancestry")
        if scope is not None:
            if contract != expected_contract or manifest.get("series_scope") != scope:
                raise CatalogueError("series ancestry crosses release contracts or scopes")
        elif contract == SERIES_CONTRACT:
            raise CatalogueError("series ancestry cannot be used as a catalogue release")
        evidence[capture_id] = (2, manifest)
        manifest_root = manifest.get("root_id")
        if not isinstance(manifest_root, str) or (root and manifest_root != root):
            raise CatalogueError("release ancestry crosses logical operator roots")
        root = manifest_root
        depth = manifest.get("delta_depth")
        if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= MAX_DELTAS:
            raise CatalogueError("invalid v2 lineage depth")
        if child_depth is not None and depth != child_depth - 1:
            raise CatalogueError("v2 lineage depth sequence is invalid")
        child_depth = depth
        parent_id = manifest.get("baseline_release_id")
        parent_sha = manifest.get("baseline_manifest_sha256")
        parent_capture = manifest.get("baseline_capture_id")
        if parent_id is None:
            if depth != 0 or parent_sha is not None or parent_capture is not None:
                raise CatalogueError("invalid v2 full baseline ancestry")
            from kalshi_events.capture import read_capture

            cached = _context.captures.get((capture_id, 2)) if _context is not None else None
            baseline_state = (
                cached.proof.state if cached is not None else read_capture(store, capture_id)
            )
            if scope is not None and baseline_state.get("scope") != scope:
                raise CatalogueError("series baseline capture scope differs from its release")
            capture_states[capture_id] = baseline_state
            baseline_started = _utc(baseline_state.get("started_at"), "full baseline start")
            break
        if (
            depth == 0
            or not isinstance(parent_id, str)
            or len(parent_id) != 64
            or not isinstance(parent_sha, str)
            or len(parent_sha) != 64
            or not isinstance(parent_capture, str)
        ):
            raise CatalogueError("invalid v2 predecessor pin")
        parent_path = store.path(f"releases/{parent_id}/manifest.json")
        if hashlib.sha256(regular_bytes(parent_path, 1024**2)).hexdigest() != parent_sha:
            raise CatalogueError("v2 predecessor manifest checksum mismatch")
        parent = publish.verify_release(parent_path.parent, store.limits.max_root_bytes)
        if parent.get("capture_id") != parent_capture:
            raise CatalogueError("v2 predecessor capture identity mismatch")
        child_depth = depth
        release_id, manifest_sha256 = parent_id, parent_sha
    # Release checksums protect exported files, while each capture's own build
    # receipt protects its raw evidence inventory. Verify every reachable
    # capture once, including grandparents, before accepting the lineage.
    from kalshi_events.capture import page_receipts, read_capture
    from kalshi_events.warehouse import certified, membership_fingerprint

    for capture_id, (version, manifest) in evidence.items():
        cached = _context.captures.get((capture_id, version)) if _context is not None else None
        if cached is None:
            capture_state = capture_states.get(capture_id)
            if capture_state is None:
                capture_state = read_capture(store, capture_id)
            page_rows = page_receipts(store, capture_state)
        else:
            capture_state = cached.proof.state
            page_rows = list(cached.proof.page_rows)
        capture_states[capture_id] = capture_state
        certified(
            store,
            capture_id,
            catalogue_version=version,
            _state=capture_state,
            _page_rows=page_rows,
            _context=_context,
        )
        if version == 2:
            expected_membership = manifest.get("membership_sha256")
            if (
                not isinstance(expected_membership, str)
                or membership_fingerprint(store, capture_id) != expected_membership
            ):
                raise CatalogueError("release ancestry membership fingerprint mismatch")
    return releases, captures, baseline_started


def _lineage(
    store: Store,
    release_id: str,
    *,
    manifest_sha256: str | None = None,
    expected_root: str | None = None,
    expected_scope: dict[str, Any] | None = None,
    _context: Any = None,
) -> tuple[set[str], set[str], datetime | None]:
    with metrics.phase("lineage_verification"):
        return _lineage_impl(
            store,
            release_id,
            manifest_sha256=manifest_sha256,
            expected_root=expected_root,
            expected_scope=expected_scope,
            _context=_context,
        )


def _validate_retirement_artifact(store: Store, path: Path, root: str, identity: str) -> str | None:
    """Bind a source or tombstone directory to its canonical artifact identity."""
    if not path.exists():
        return None
    if path_is_link(path) or not path.is_dir():
        raise CatalogueError("retirement plan artifact is not a canonical directory")
    if root == "captures":
        value = decode(regular_bytes(path / "state.json", store.limits.max_root_bytes))
        if value.get("capture_id") != identity:
            raise CatalogueError("retirement capture identity mismatch")
        return identity
    filename = {
        "raw": "manifest.json",
        "builds": "build.json",
        "builds-v2": "build.json",
        "releases": "manifest.json",
    }[root]
    value = decode(regular_bytes(path / filename, store.limits.max_root_bytes))
    actual = (
        value.get("source_release_id")
        if root == "releases"
        else value.get("page_id")
        if root == "raw"
        else value.get("capture_id")
    )
    if actual != identity:
        raise CatalogueError("retirement artifact identity mismatch")
    return value.get("capture_id") if root == "raw" else identity


def _finish_retirement_plan(store: Store) -> None:
    """Resume only a previously validated, durable retirement plan."""
    if not store.writer_locked_by_current_thread:
        raise CatalogueError("retirement recovery requires the canonical writer lock")
    plan_path = store.path("prune-plan.json")
    progress_path = store.path("prune-progress.jsonl")
    trash = store.path("prune-trash")
    if not plan_path.exists():
        if trash.exists() and any(trash.iterdir()):
            raise CatalogueError("unplanned prune-trash contents; refusing broad cleanup")
        if progress_path.exists():
            progress_path.unlink()
            fsync_directory(store.root)
        return
    plan_body = regular_bytes(plan_path, store.limits.max_root_bytes)
    plan = decode(plan_body)
    if (
        not isinstance(plan, dict)
        or set(plan)
        not in ({"version", "kind", "entries"}, {"version", "kind", "capture_id", "entries"})
        or isinstance(plan.get("version"), bool)
        or plan.get("version") != 1
        or plan.get("kind") not in {"prune", "discard"}
        or (plan.get("kind") == "discard") != ("capture_id" in plan)
    ):
        raise CatalogueError("invalid durable prune plan")
    entries = plan["entries"]
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        raise CatalogueError("invalid durable prune plan entries")
    if plan.get("kind") == "discard" and (
        not isinstance(plan.get("capture_id"), str)
        or f"captures/{plan['capture_id']}"
        not in {entry.get("source") for entry in entries if isinstance(entry, dict)}
    ):
        raise CatalogueError("discard plan omits its incomplete capture")
    retired_capture_ids = {
        Path(entry["source"]).parts[1]
        for entry in entries
        if isinstance(entry, dict)
        and isinstance(entry.get("source"), str)
        and len(Path(entry["source"]).parts) == 2
        and Path(entry["source"]).parts[0] == "captures"
    }
    if plan.get("kind") == "discard":
        retired_capture_ids.add(plan["capture_id"])
    allowed_roots = {"captures", "raw", "builds", "builds-v2", "releases"}
    targets: set[str] = set()
    sources: set[str] = set()
    plan_sha256 = hashlib.sha256(plan_body).hexdigest()
    progress: dict[int, dict[str, Any]] = {}
    if progress_path.exists():
        progress_body = regular_bytes(progress_path, store.limits.max_root_bytes)
        lines = progress_body.splitlines(keepends=True)
        if lines and not lines[-1].endswith(b"\n"):
            committed = b"".join(lines[:-1])
            with progress_path.open("r+b") as stream:
                stream.truncate(len(committed))
                stream.flush()
                os.fsync(stream.fileno())
            store.sync("prune-progress.jsonl")
            lines = lines[:-1]
        for index, line in enumerate(lines):
            record = decode(line)
            if (
                not isinstance(record, dict)
                or set(record)
                not in (
                    {"version", "plan_sha256", "index", "source", "target"},
                    {"version", "plan_sha256", "index", "source", "target", "capture_id"},
                    {"version", "plan_sha256", "index", "source", "target", "incomplete"},
                    {
                        "version",
                        "plan_sha256",
                        "index",
                        "source",
                        "target",
                        "capture_id",
                        "incomplete",
                    },
                )
                or isinstance(record.get("version"), bool)
                or record.get("version") != 1
                or record.get("plan_sha256") != plan_sha256
                or isinstance(record.get("index"), bool)
                or record.get("index") != index
                or index >= len(entries)
                or record.get("source") != entries[index].get("source")
                or record.get("target") != entries[index].get("target")
                or ("capture_id" in record and not isinstance(record["capture_id"], str))
                or ("incomplete" in record and record["incomplete"] is not True)
                or ("capture_id" in record and Path(record["source"]).parts[0] != "raw")
                or (
                    "incomplete" in record
                    and (
                        plan.get("kind") != "discard"
                        or Path(record["source"]).parts != ("captures", plan.get("capture_id"))
                    )
                )
            ):
                raise CatalogueError("durable retirement progress does not match its plan")
            progress[index] = record
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"source", "target"}:
            raise CatalogueError("invalid durable prune plan entry")
        source, target = entry["source"], entry["target"]
        source_parts = Path(source).parts if isinstance(source, str) else ()
        target_parts = Path(target).parts if isinstance(target, str) else ()
        source_id = source_parts[1] if len(source_parts) == 2 else ""
        expected_id_length = 64 if source_parts and source_parts[0] == "releases" else 32
        target_leaf = target_parts[1] if len(target_parts) == 2 else ""
        target_prefix, _, target_suffix = target_leaf.partition("-")
        if (
            not isinstance(source, str)
            or Path(source).is_absolute()
            or ".." in Path(source).parts
            or source != Path(source).as_posix()
            or len(Path(source).parts) != 2
            or Path(source).parts[0] not in allowed_roots
            or len(source_id) != expected_id_length
            or any(character not in "0123456789abcdef" for character in source_id)
            or source in sources
            or not isinstance(target, str)
            or Path(target).is_absolute()
            or ".." in Path(target).parts
            or target != Path(target).as_posix()
            or len(Path(target).parts) != 2
            or Path(target).parts[0] != "prune-trash"
            or len(target_prefix) != 32
            or any(character not in "0123456789abcdef" for character in target_prefix)
            or target_suffix != f"{source_parts[0]}-{source_id}"
            or target in targets
        ):
            raise CatalogueError("unsafe durable prune plan path")
        sources.add(source)
        targets.add(target)

    if plan.get("kind") == "discard":
        from kalshi_events.capture import read_capture

        victim = plan["capture_id"]
        capture_source_count = 0
        for index, entry in enumerate(entries):
            root, identity = Path(entry["source"]).parts
            if root == "captures" and identity == victim:
                capture_source_count += 1
                source_path = store.path(entry["source"])
                if source_path.exists():
                    capture_state = read_capture(store, victim)
                    if capture_state.get("complete"):
                        raise CatalogueError("discard plan victim is not an incomplete capture")
                elif progress.get(index, {}).get("incomplete") is not True:
                    raise CatalogueError("retired discard victim lacks incomplete-state proof")
                continue
            if root != "raw":
                raise CatalogueError("discard plan contains an unrelated artifact")
            source_path = store.path(entry["source"])
            if source_path.exists():
                capture_id = _validate_retirement_artifact(store, source_path, root, identity)
            else:
                capture_id = progress.get(index, {}).get("capture_id")
            if capture_id != victim:
                raise CatalogueError("discard plan contains unrelated raw evidence")
        if capture_source_count != 1:
            raise CatalogueError("discard plan must contain exactly one incomplete capture")

    # A pending plan is only safe to resume while none of its sources has
    # become reachable from a publication or an incomplete capture.
    from kalshi_events import publish
    from kalshi_events.capture import read_capture

    protected_captures: set[str] = set()
    protected_releases: set[str] = set()
    for version, pointer_name in publication_pointers(store):
        if store.path(pointer_name).exists():
            current = publish.current(store, catalogue_version=version)
            release_ids, capture_ids, _ = _lineage(
                store,
                current["source_release_id"],
                expected_root=current.get("root_id") if version == 2 else None,
            )
            protected_releases.update(release_ids)
            protected_captures.update(capture_ids)
    captures_root = store.path("captures")
    discarded_capture = plan.get("capture_id") if plan.get("kind") == "discard" else None
    for path in sorted(captures_root.iterdir()) if captures_root.exists() else []:
        if not path.is_dir() or path_is_link(path):
            continue
        pending = read_capture(store, path.name)
        if pending.get("complete"):
            continue
        if path.name != discarded_capture:
            protected_captures.add(path.name)
        pin = pending.get("baseline")
        if isinstance(pin, dict):
            ids, captures, _ = _lineage(
                store,
                pin["source_release_id"],
                manifest_sha256=pin["manifest_sha256"],
                expected_root=pin["root_id"],
            )
            protected_releases.update(ids)
            protected_captures.update(captures)
    for index, entry in enumerate(entries):
        parts = Path(entry["source"]).parts
        if parts[0] in {"captures", "builds", "builds-v2"} and parts[1] in protected_captures:
            raise CatalogueError("pending prune plan now intersects a publication or recovery pin")
        if (
            parts[0] == "captures"
            and parts[1] == discarded_capture
            and discarded_capture in protected_captures
        ):
            raise CatalogueError("discard target is now pinned by another recovery path")
        if parts[0] == "releases" and parts[1] in protected_releases:
            raise CatalogueError("pending prune plan now intersects a publication or recovery pin")
        if parts[0] == "raw":
            candidate = store.path(entry["source"])
            if not candidate.exists():
                candidate = store.path(entry["target"])
            manifest_path = candidate / "manifest.json"
            if manifest_path.exists():
                raw_manifest = decode(regular_bytes(manifest_path, 1024**2))
                if raw_manifest.get("capture_id") in protected_captures:
                    raise CatalogueError(
                        "pending prune plan now intersects an incomplete evidence pin"
                    )
            elif progress.get(index, {}).get("capture_id") in protected_captures:
                raise CatalogueError("pending prune plan now intersects an incomplete evidence pin")
    # Validate the complete retirement graph before changing any source path.
    # Otherwise a malformed later entry could leave earlier entries deleted
    # and wedge every future writer on the still-present plan.
    for index, entry in enumerate(entries):
        source = store.path(entry["source"])
        target = store.path(entry["target"])
        root, identity = Path(entry["source"]).parts
        started = index in progress
        if source.exists():
            _validate_retirement_artifact(store, source, root, identity)
            if target.exists():
                raise CatalogueError("prune plan source and tombstone both exist")
        elif target.exists():
            if not started or path_is_link(target) or not target.is_dir():
                raise CatalogueError("untracked prune tombstone; refusing cleanup")
        elif not started:
            raise CatalogueError("untracked missing prune source; refusing cleanup")

    if path_is_link(trash) or (trash.exists() and not trash.is_dir()):
        raise CatalogueError("prune-trash is not a canonical directory")
    if trash.exists():
        for child in trash.iterdir():
            relative = f"prune-trash/{child.name}"
            if relative not in targets:
                raise CatalogueError("unplanned prune-trash contents; refusing broad cleanup")
            if path_is_link(child) or not child.is_dir():
                raise CatalogueError("retirement tombstone is not a directory")

    durable_mkdir(trash)
    for index, entry in enumerate(entries):
        source = store.path(entry["source"])
        target = store.path(entry["target"])
        root, identity = Path(entry["source"]).parts
        started = index in progress
        if source.exists():
            capture_id = _validate_retirement_artifact(store, source, root, identity)
        else:
            capture_id = progress.get(index, {}).get("capture_id")
        if source.exists() and target.exists():
            raise CatalogueError("prune plan source and tombstone both exist")
        if target.exists() and not started:
            raise CatalogueError("untracked prune tombstone; refusing cleanup")
        if not source.exists() and not target.exists() and not started:
            raise CatalogueError("untracked missing prune source; refusing cleanup")
        if not started:
            record: dict[str, Any] = {
                "version": 1,
                "plan_sha256": plan_sha256,
                "index": index,
                "source": entry["source"],
                "target": entry["target"],
            }
            if root == "raw":
                if not isinstance(capture_id, str):
                    raise CatalogueError("raw retirement source lacks a capture identity")
                record["capture_id"] = capture_id
            if plan.get("kind") == "discard" and root == "captures":
                from kalshi_events.capture import read_capture

                capture_state = read_capture(store, identity)
                if capture_state.get("complete"):
                    raise CatalogueError("discard plan victim is not an incomplete capture")
                record["incomplete"] = True
            store.append("prune-progress.jsonl", json_bytes(record))
            progress[index] = record
        if source.exists():
            os.rename(source, target)
            fsync_directory(source.parent)
            fsync_directory(trash)
        if target.exists():
            if path_is_link(target):
                raise CatalogueError("retirement tombstone became a symlink")
            if target.is_dir() and not path_is_link(target):
                shutil.rmtree(target)
            else:
                target.unlink()
            fsync_directory(trash)
    if any(trash.iterdir()):
        raise CatalogueError("prune plan left unreferenced tombstones")
    # Keep the durable plan until every derived recovery cache is removed. If
    # cleanup is interrupted, the next writer can derive the same capture IDs
    # from the still-present plan and retry idempotently.
    from kalshi_events.recovery_index import remove_capture_cache

    for identity in sorted(retired_capture_ids):
        remove_capture_cache(store, identity)
    plan_path.unlink()
    fsync_directory(store.root)
    progress_path.unlink(missing_ok=True)
    fsync_directory(store.root)
    for relative in ("captures", "raw", "builds", "builds-v2", "releases", "prune-trash"):
        if store.path(relative).exists():
            store.sync(relative)


def finish_pending_retirement(store: Store) -> None:
    """Complete a validated interrupted prune before new catalogue work starts."""
    with store.writer_guard():
        store.require_writable()
        _finish_retirement_plan(store)


def prune(
    store: Store,
    state: dict[str, Any],
    release_id: str,
    catalogue_version: int,
    *,
    _context: Any = None,
) -> list[str]:
    with store.writer_guard():
        if _context is None:
            from kalshi_events.warehouse import operation_context

            with operation_context(store) as context:
                return _prune_locked(store, state, release_id, catalogue_version, context=context)
        return _prune_locked(store, state, release_id, catalogue_version, context=_context)


def _prune_locked(
    store: Store,
    state: dict[str, Any],
    release_id: str,
    catalogue_version: int,
    *,
    context: Any,
) -> list[str]:
    """Retire only validated, unreachable evidence after successful publication."""
    from kalshi_events import publish
    from kalshi_events.capture import read_capture

    store.require_writable()
    if not store.writer_locked_by_current_thread:
        raise CatalogueError("pruning requires the canonical writer lock")
    if catalogue_version not in {1, 2}:
        raise CatalogueError("catalogue version must be 1 or 2")
    published = publish.current(store, catalogue_version=catalogue_version)
    if published.get("source_release_id") != release_id or published.get("capture_id") != state.get(
        "capture_id"
    ):
        raise CatalogueError("cannot prune before the requested release is current")
    _finish_retirement_plan(store)
    trash = store.path("prune-trash")
    durable_mkdir(trash)

    pointer_manifests: list[dict[str, Any]] = []
    for version, pointer_name in publication_pointers(store):
        if store.path(pointer_name).exists():
            pointer_manifests.append(publish.current(store, catalogue_version=version))

    keep_releases: set[str] = set()
    keep_captures: set[str] = {state["capture_id"]}
    full_cutoffs: list[datetime] = []
    for manifest in pointer_manifests:
        version = 2 if manifest.get("contract") in {CATALOGUE_V2_CONTRACT, SERIES_CONTRACT} else 1
        ids, captures, baseline_started = _lineage(
            store,
            manifest["source_release_id"],
            manifest_sha256=hashlib.sha256(
                regular_bytes(
                    store.path(f"releases/{manifest['source_release_id']}/manifest.json"),
                    1024**2,
                )
            ).hexdigest(),
            expected_root=manifest.get("root_id") if version == 2 else None,
            _context=context,
        )
        keep_releases.update(ids)
        keep_captures.update(captures)
        if baseline_started is not None:
            full_cutoffs.append(baseline_started)
    if not pointer_manifests:
        raise CatalogueError("cannot prune without a verified publication pointer")
    cutoff = max(full_cutoffs) if full_cutoffs else _utc(state["started_at"], "capture start")

    captures_dir = store.path("captures")
    capture_states: dict[str, dict[str, Any]] = {}
    for path in sorted(captures_dir.iterdir()) if captures_dir.exists() else []:
        if not path.is_dir() or path_is_link(path):
            continue
        cached = next(
            (
                context.captures.get((path.name, version))
                for version in (1, 2)
                if context.captures.get((path.name, version)) is not None
            ),
            None,
        )
        capture_state = cached.proof.state if cached is not None else read_capture(store, path.name)
        capture_states[path.name] = capture_state
        if not capture_state.get("complete"):
            keep_captures.add(path.name)
            if capture_state.get("incremental"):
                pin = capture_state.get("baseline")
                if not isinstance(pin, dict):
                    raise CatalogueError("incomplete incremental capture lacks its baseline pin")
                verify_pin(store, capture_state)
                ids, ancestors, baseline_started = _lineage(
                    store,
                    pin["source_release_id"],
                    manifest_sha256=pin["manifest_sha256"],
                    expected_root=pin["root_id"],
                    _context=context,
                )
                keep_releases.update(ids)
                keep_captures.update(ancestors)
                if baseline_started is not None:
                    full_cutoffs.append(baseline_started)
        elif _utc(capture_state["started_at"], "capture start") >= cutoff:
            keep_captures.add(path.name)
            if capture_state.get("incremental"):
                pin = capture_state.get("baseline")
                if not isinstance(pin, dict):
                    raise CatalogueError("completed incremental capture lacks its baseline pin")
                verify_pin(store, capture_state)
                ids, ancestors, baseline_started = _lineage(
                    store,
                    pin["source_release_id"],
                    manifest_sha256=pin["manifest_sha256"],
                    expected_root=pin["root_id"],
                    _context=context,
                )
                keep_releases.update(ids)
                keep_captures.update(ancestors)
                if baseline_started is not None:
                    full_cutoffs.append(baseline_started)

    releases_dir = store.path("releases")
    for directory in sorted(releases_dir.iterdir()) if releases_dir.exists() else []:
        if directory.name in keep_releases:
            continue
        if (
            not directory.is_dir()
            or path_is_link(directory)
            or not (directory / "manifest.json").is_file()
        ):
            continue
        manifest = publish.verify_release(directory, store.limits.max_root_bytes)
        if manifest.get("capture_id") in keep_captures:
            ids, ancestors, baseline_started = _lineage(
                store,
                directory.name,
                expected_root=manifest.get("root_id")
                if manifest.get("contract") in {CATALOGUE_V2_CONTRACT, SERIES_CONTRACT}
                else None,
                _context=context,
            )
            keep_releases.update(ids)
            keep_captures.update(ancestors)
            if baseline_started is not None:
                full_cutoffs.append(baseline_started)

    raw_dirs: set[Path] = set()
    raw_root = store.path("raw")
    verified_raw_owners = {
        row["relative"]: capture_id
        for (capture_id, _version), cached in context.captures.items()
        for row in cached.proof.page_rows
    }
    for manifest_path in raw_root.glob("*/manifest.json") if raw_root.exists() else []:
        relative = manifest_path.relative_to(store.root).as_posix()
        identity = verified_raw_owners.get(relative)
        if identity is None:
            manifest = decode(regular_bytes(manifest_path, 1024**2))
            identity = manifest.get("capture_id")
        if identity in capture_states and identity not in keep_captures:
            raw_dirs.add(manifest_path.parent)

    # Targeted metadata evidence is outside catalogue retention, even if an
    # older publication predates that targeted capture.
    targeted = {
        identity
        for identity, capture_state in capture_states.items()
        if capture_state.get("selected") is not None
    }
    keep_captures.update(targeted)
    raw_dirs = {
        path
        for path in raw_dirs
        if decode(regular_bytes(path / "manifest.json", 1024**2)).get("capture_id") not in targeted
    }

    remove: list[Path] = []
    for identity in sorted(capture_states):
        if identity not in keep_captures:
            path = store.path(f"captures/{identity}")
            if path.exists():
                remove.append(path)
    for dirname in ("builds", "builds-v2"):
        base = store.path(dirname)
        for path in sorted(base.iterdir()) if base.exists() else []:
            if (
                path.is_dir()
                and not path_is_link(path)
                and path.name in capture_states
                and path.name not in keep_captures
            ):
                remove.append(path)
    for path in sorted(raw_dirs):
        if path.exists():
            remove.append(path)
    for path in sorted(releases_dir.iterdir()) if releases_dir.exists() else []:
        if (
            path.is_dir()
            and not path_is_link(path)
            and path.name not in keep_releases
            and (path / "manifest.json").is_file()
        ):
            remove.append(path)

    capture_paths = {path.name: path for path in remove if path.parent == captures_dir}
    children: dict[str, set[str]] = {}
    for identity in capture_paths:
        pin = capture_states[identity].get("baseline")
        parent = pin.get("capture_id") if isinstance(pin, dict) else None
        if parent in capture_paths:
            children.setdefault(parent, set()).add(identity)
    ordered_captures: list[Path] = []
    visited: set[str] = set()

    def visit(identity: str) -> None:
        if identity in visited:
            return
        visited.add(identity)
        for child in sorted(children.get(identity, set())):
            visit(child)
        ordered_captures.append(capture_paths[identity])

    for identity in sorted(capture_paths):
        visit(identity)
    remove = [*ordered_captures, *(path for path in remove if path.parent != captures_dir)]

    # The complete deletion plan is durable before any selector-visible path moves.
    removed_captures = [path.name for path in remove if path.parent == captures_dir]
    entries = []
    for path in remove:
        if not path.exists():
            continue
        relative = path.relative_to(store.root).as_posix()
        entries.append(
            {
                "source": relative,
                "target": f"prune-trash/{uuid.uuid4().hex}-{path.parent.name}-{path.name}",
            }
        )
    from kalshi_events.recovery_index import remove_capture_cache

    if not entries:
        # The current capture is retained by the verified publication, so its
        # identity remains discoverable if the process stops before cleanup.
        remove_capture_cache(store, state["capture_id"])
        return removed_captures
    # No cached proof is used after retention has selected the complete removal set.
    context.captures.clear()
    store.write("prune-plan.json", {"version": 1, "kind": "prune", "entries": entries})
    fsync_directory(store.root)
    _finish_retirement_plan(store)
    # This capture remains reachable from the publication after the retirement
    # plan is removed, so a later prune can retry if cleanup is interrupted.
    remove_capture_cache(store, state["capture_id"])
    return removed_captures


def discard(store: Store, capture_id: str) -> dict[str, Any]:
    with store.writer_guard():
        return _discard_locked(store, capture_id)


def _discard_locked(store: Store, capture_id: str) -> dict[str, Any]:
    """Explicitly discard an unreferenced incomplete capture and its raw receipts."""
    from kalshi_events import publish
    from kalshi_events.capture import read_capture

    store.require_writable()
    _finish_retirement_plan(store)
    capture_path = store.path(f"captures/{capture_id}")
    if not capture_path.exists():
        raise CatalogueError("incomplete capture does not exist")
    state = read_capture(store, capture_id)
    if state.get("complete"):
        raise CatalogueError("only incomplete captures can be discarded")

    protected: set[str] = set()
    release_ids: set[str] = set()
    for version, pointer_name in publication_pointers(store):
        if store.path(pointer_name).exists():
            current = publish.current(store, catalogue_version=version)
            ids, captures, _ = _lineage(
                store,
                current["source_release_id"],
                expected_root=current.get("root_id") if version == 2 else None,
            )
            release_ids.update(ids)
            protected.update(captures)

    captures = store.path("captures")
    for path in sorted(captures.iterdir()) if captures.exists() else []:
        if path.name == capture_id or not path.is_dir() or path_is_link(path):
            continue
        descendant = read_capture(store, path.name)
        pin = descendant.get("baseline")
        if not isinstance(pin, dict):
            continue
        if pin.get("capture_id") == capture_id:
            raise CatalogueError("incomplete capture is pinned by a descendant capture")
        ids, ancestors, _ = _lineage(
            store,
            pin["source_release_id"],
            manifest_sha256=pin["manifest_sha256"],
            expected_root=pin["root_id"],
        )
        release_ids.update(ids)
        protected.update(ancestors)

    releases = store.path("releases")
    for path in sorted(releases.iterdir()) if releases.exists() else []:
        if path.name in release_ids or not path.is_dir() or path_is_link(path):
            continue
        if not (path / "manifest.json").is_file():
            continue
        manifest = publish.verify_release(path, store.limits.max_root_bytes)
        if manifest.get("contract") in {CATALOGUE_V2_CONTRACT, SERIES_CONTRACT}:
            _, ancestors, _ = _lineage(
                store,
                path.name,
                expected_root=manifest.get("root_id"),
            )
        else:
            ancestors = {manifest.get("capture_id")}
        if capture_id in ancestors:
            raise CatalogueError("incomplete capture is referenced by a release descendant")

    if capture_id in protected:
        raise CatalogueError("incomplete capture is reachable from a published release")
    trash = store.path("prune-trash")
    durable_mkdir(trash)
    raw_dirs = []
    for manifest_path in store.path("raw").glob("*/manifest.json"):
        manifest = decode(regular_bytes(manifest_path, 1024**2))
        if manifest.get("capture_id") == capture_id:
            raw_dirs.append(manifest_path.parent)
    paths = [capture_path, *sorted(set(raw_dirs))]
    entries = []
    for path in paths:
        if not path.exists():
            continue
        entries.append(
            {
                "source": path.relative_to(store.root).as_posix(),
                "target": f"prune-trash/{uuid.uuid4().hex}-{path.parent.name}-{path.name}",
            }
        )
    store.write(
        "prune-plan.json",
        {"version": 1, "kind": "discard", "capture_id": capture_id, "entries": entries},
    )
    fsync_directory(store.root)
    _finish_retirement_plan(store)
    store.sync("captures")
    store.sync("raw")
    store.sync("prune-trash")
    return {"capture_id": capture_id, "discarded": True}
