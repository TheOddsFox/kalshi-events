"""Lossless native identities and observation-era relationships; never ticker inference."""

from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Iterable, Iterator
from contextlib import suppress
from typing import Any

from kalshi_events.common import (
    BudgetExceeded,
    CatalogueError,
    digest,
    durable_mkdir,
    json_bytes,
    ticker,
    validate_series_scope,
)

ENTITY_KEYS = {"series": "series_ticker", "events": "event_ticker", "markets": "market_ticker"}

PARENT_INDEX_SCHEMA = """create table winners(kind text,ticker text,received text,observation_id text,parent text,
       primary key(kind,ticker)) without rowid;
   create index winners_parent on winners(kind,parent);
   create table membership(scope text,generation integer,ticker text,
       primary key(scope,generation,ticker)) without rowid;
   create table lookups(kind text,ticker text,status integer,page_id text,request_id text,
       primary key(kind,ticker)) without rowid;
   create table candidates(batch_id text,generation integer,ticker text,received text,
       observation_id text,parent text,page_id text,request_id text,
       primary key(batch_id,generation,ticker)) without rowid;
   create index candidates_ticker on candidates(ticker);
   create table terminal(batch_id text,generation integer,
       primary key(batch_id,generation)) without rowid;"""


def records(payload: Any, key: str, *, singular: bool = False) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise CatalogueError("source envelope must be an object")
    value = payload.get(key)
    if singular:
        if not isinstance(value, dict):
            raise CatalogueError(f"source envelope lacks {key} object")
        value = [value]
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise CatalogueError(f"source envelope lacks {key} object array")
    return value


def observation(
    kind: str, native: dict[str, Any], provenance: dict[str, Any], pointer: str
) -> dict[str, Any]:
    identity, parent = native_identity(kind, native)
    key = ENTITY_KEYS[kind]
    row = {
        **native,
        key: ticker(identity),
        **provenance,
        "observation_id": digest(json_bytes([provenance["page_id"], pointer])),
        "provider_updated_at": native.get("updated_time", native.get("last_updated_ts")),
        "payload_json": json_bytes(native).decode().strip(),
    }
    if kind == "events":
        row["series_ticker"] = parent
    if kind == "markets":
        row["event_ticker"] = parent
    return row


def native_identity(kind: str, native: dict[str, Any]) -> tuple[str, Any]:
    """Validate native identity and relationship fields without serializing payloads."""
    identity = native.get("event_ticker") if kind == "events" else native.get("ticker")
    identity = ticker(identity)
    parent = None
    if kind == "events":
        parent = native.get("series_ticker")
        if parent:
            ticker(parent)
    elif kind == "markets":
        parent = ticker(native.get("event_ticker"))
        legs = native.get("mve_selected_legs", [])
        if not isinstance(legs, list) or any(not isinstance(leg, dict) for leg in legs):
            raise CatalogueError("malformed native combo legs")
        for leg in legs:
            ticker(leg.get("event_ticker"))
            ticker(leg.get("market_ticker"))
            if leg.get("side") not in {"yes", "no"}:
                raise CatalogueError("malformed native combo side")
    return identity, parent


class ParentIndex:
    """Compact winner identities needed to resolve capture-time parent lookups."""

    def __init__(self) -> None:
        self.series: dict[str, tuple[str, str, None]] = {}
        self.events: dict[str, tuple[str, str, str | None]] = {}
        self.markets: dict[str, tuple[str, str, str]] = {}
        self.processed: set[str] = set()
        self.scope_membership: dict[str, dict[int, set[str]]] = {}
        self.parent_lookups: dict[tuple[str, str], tuple[int, str, str]] = {}
        self.parent_batch_candidates: dict[
            tuple[str, int], dict[str, tuple[tuple[str, str, str | None], str, str]]
        ] = {}
        self.parent_batch_terminal: set[tuple[str, int]] = set()

    @staticmethod
    def _put(target: dict[str, tuple], identity: str, winner: tuple) -> None:
        previous = target.get(identity)
        if previous is None or winner[:2] > previous[:2]:
            target[identity] = winner

    def add_page(self, manifest: dict[str, Any], payload: Any) -> None:
        kind = manifest.get("entity_kind")
        series_scope = manifest.get("series_scope")
        scoped_series = (
            validate_series_scope(series_scope)["series_ticker"]
            if series_scope is not None
            else None
        )
        if manifest.get("singular") and kind in {"events", "series"}:
            endpoint = manifest.get("endpoint", "")
            prefix = "/events/" if kind == "events" else "/series/"
            if endpoint.startswith(prefix):
                identity = endpoint[len(prefix) :]
                self.parent_lookups[(kind, identity)] = (
                    manifest.get("status", 0),
                    manifest.get("page_id", ""),
                    manifest.get("request_id", ""),
                )
        if manifest.get("status") != 200:
            return
        if kind not in ENTITY_KEYS:
            return
        envelope = {"series": "series", "events": "events", "markets": "markets"}[kind]
        if manifest.get("singular"):
            envelope = {"series": "series", "events": "event", "markets": "market"}[kind]
        provenance = {
            "page_id": manifest["page_id"],
            "received_at": manifest["received_at"],
        }
        rows = records(payload, envelope, singular=manifest.get("singular", False))
        parent_batch = manifest.get("parent_batch")
        batch_id = manifest.get("parent_batch_id")
        generation = manifest.get("task_generation")
        allowed_batch: set[str] | None = None
        if parent_batch is not None:
            selection = manifest.get("parent_batch_tickers")
            if (
                parent_batch not in {"ordinary", "multivariate"}
                or not isinstance(batch_id, str)
                or len(batch_id) != 64
                or not isinstance(generation, int)
                or isinstance(generation, bool)
                or not isinstance(selection, str)
            ):
                raise CatalogueError("malformed event parent batch receipt context")
            batch_values = selection.split(",")
            if (
                not batch_values
                or len(batch_values) > 100
                or len(batch_values) != len(set(batch_values))
            ):
                raise CatalogueError("malformed event parent batch selection")
            allowed_batch = {ticker(value) for value in batch_values}
        for index, native in enumerate(rows):
            identity, parent = native_identity(kind, native)
            if allowed_batch is not None:
                if kind != "events" or identity not in allowed_batch:
                    raise CatalogueError("event parent batch returned an unrequested identity")
                candidate_key = (batch_id, generation)
                candidates = self.parent_batch_candidates.setdefault(candidate_key, {})
                candidate = (
                    (
                        provenance["received_at"],
                        digest(json_bytes([provenance["page_id"], f"/{envelope}/{index}"])),
                        parent,
                    ),
                    provenance["page_id"],
                    manifest["request_id"],
                )
                previous = candidates.get(identity)
                if previous is None or candidate[0][:2] > previous[0][:2]:
                    candidates[identity] = candidate
            if scoped_series is not None:
                if kind == "series" and identity != scoped_series:
                    raise CatalogueError("series definition differs from the scoped ticker")
                if kind == "events":
                    if parent != scoped_series:
                        raise CatalogueError("event is outside the scoped series")
                    if native.get("mve_collection_ticker") or native.get("mve_selected_legs"):
                        raise CatalogueError("series traversal returned a multivariate event")
                if kind == "markets" and (
                    native.get("mve_collection_ticker") or native.get("mve_selected_legs")
                ):
                    raise CatalogueError("series traversal returned a combo market")
            scope = manifest.get("task_scope")
            generation = manifest.get("task_generation", 0)
            if (
                isinstance(scope, str)
                and isinstance(generation, int)
                and not isinstance(generation, bool)
                and (
                    not manifest.get("singular")
                    or (kind == "series" and scope == "series" and identity == scoped_series)
                )
                and parent_batch is None
            ):
                if (
                    kind == "markets"
                    and scope == "markets_live"
                    and (
                        native.get("mve_collection_ticker") or bool(native.get("mve_selected_legs"))
                    )
                ):
                    membership_scope = "markets_combo"
                else:
                    membership_scope = scope
                self.scope_membership.setdefault(membership_scope, {}).setdefault(
                    generation, set()
                ).add(identity)
            winner = (
                provenance["received_at"],
                digest(json_bytes([provenance["page_id"], f"/{envelope}/{index}"])),
                parent,
            )
            if kind == "series":
                self._put(self.series, identity, winner[:2] + (None,))
            elif kind == "events":
                # Native warehouse winners span every observation generation. Batch
                # eligibility is tracked separately and promoted only after the final
                # pagination generation reaches its terminal page.
                self._put(self.events, identity, winner)
                if "markets" in native:
                    for market in records(native, "markets"):
                        if market.get("event_ticker") != identity:
                            raise CatalogueError("nested market parent contradicts event")
                        if scoped_series is not None and (
                            market.get("mve_collection_ticker") or market.get("mve_selected_legs")
                        ):
                            raise CatalogueError("series traversal returned a nested combo market")
                        native_identity("markets", market)
            else:
                self._put(self.markets, identity, winner)
        if allowed_batch is not None and payload.get("cursor") == "":
            self.parent_batch_terminal.add((batch_id, generation))

    def effective_parent_evidence(
        self, state: dict[str, Any]
    ) -> tuple[dict[str, tuple[str, str, str | None]], dict[tuple[str, str], tuple[int, str, str]]]:
        """Add only terminal, final-generation batch results to authoritative parent views."""
        events = dict(self.events)
        lookups = dict(self.parent_lookups)
        for work in state.get("tasks", []):
            role = work.get("parent_batch")
            if role is None or not work.get("done"):
                continue
            batch_id = digest(
                json_bytes(
                    [
                        work.get("endpoint"),
                        work.get("params"),
                        work.get("kind"),
                        work.get("singular"),
                        work.get("requested"),
                    ]
                )
            )
            generation = work.get("restarts", 0)
            key = (batch_id, generation)
            if key not in self.parent_batch_terminal:
                continue
            for identity, (winner, page_id, request_id) in self.parent_batch_candidates.get(
                key, {}
            ).items():
                self._put(events, identity, winner)
                direct = lookups.get(("events", identity))
                if direct is None or direct[1:] == (page_id, request_id):
                    lookups[("events", identity)] = (200, page_id, request_id)
        return events, lookups


class DiskParentIndex:
    """Bounded-memory SQLite inventory used while replaying full catalogue captures."""

    def __init__(self, store: Any):
        self.store = store
        self.processed: set[str] = set()
        self.defer_page_merges = False
        tmp = store.path("tmp")
        durable_mkdir(tmp)
        for stale in tmp.glob("parent-index-*"):
            store.remove(stale.relative_to(store.root).as_posix())
        directory = f"tmp/parent-index-{os.getpid()}-{os.urandom(6).hex()}"
        self.relative = f"{directory}/index.sqlite"
        self.path = store.path(self.relative)
        durable_mkdir(self.path.parent)
        self.db = None
        try:
            self.db = sqlite3.connect(self.path)
            self._initialize_db()
        except BaseException as exc:
            if self.db is not None:
                self.db.close()
                self.db = None
            store.remove(directory)
            if isinstance(exc, sqlite3.OperationalError) and "full" in str(exc).lower():
                raise BudgetExceeded(
                    "temporary storage exhausted while initializing verified-evidence index"
                ) from exc
            raise

    def _initialize_db(self) -> None:
        self.db.execute("pragma cache_size=-8192")
        self.db.execute("pragma temp_store=FILE")
        self.db.execute("pragma automatic_index=OFF")
        self.db.execute("pragma mmap_size=0")
        # This inventory is disposable and rebuilt from verified receipts on every operation.
        self.db.execute("pragma journal_mode=OFF")
        self.db.execute("pragma synchronous=OFF")
        usage = self.store.usage()
        remaining = min(
            self.store.limits.max_temp_bytes - usage["temporary_bytes"],
            self.store.limits.max_root_bytes - usage["retained_bytes"],
        )
        self.db.execute(f"pragma max_page_count={max(1, remaining // 4096)}")
        self.db.executescript(PARENT_INDEX_SCHEMA)
        deadline = self.store.started + self.store.limits.max_duration_seconds
        self._deadline_hit = False

        def interrupt_at_deadline() -> int:
            if time.monotonic() >= deadline:
                self._deadline_hit = True
                return 1
            return 0

        self.db.set_progress_handler(interrupt_at_deadline, 10_000)
        self.db.commit()
        self._sync()

    def _translate_sql_deadline(self, exc: sqlite3.OperationalError) -> None:
        if self._deadline_hit:
            raise BudgetExceeded(
                "operation deadline exhausted while indexing verified evidence"
            ) from exc
        if "full" in str(exc).lower():
            raise BudgetExceeded(
                "temporary storage exhausted while indexing verified evidence"
            ) from exc
        raise exc

    def _sync(self) -> None:
        self.db.commit()
        self.store.sync_file(self.relative)
        self.store.check_storage()
        self.store.check_deadline()

    def close(self) -> None:
        db = getattr(self, "db", None)
        if db is None:
            return
        self.db = None
        db.close()
        self.store.remove(self.path.parent.relative_to(self.store.root).as_posix())

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    def add_page(self, manifest: dict[str, Any], payload: Any) -> None:
        page = ParentIndex()
        page.add_page(manifest, payload)
        self.merge(page)

    def merge(self, page: ParentIndex) -> None:
        try:
            self._merge(page)
        except sqlite3.OperationalError as exc:
            self._translate_sql_deadline(exc)

    def _merge(self, page: ParentIndex) -> None:
        for kind in ENTITY_KEYS:
            for identity, winner in getattr(page, kind).items():
                parent = winner[2] if kind == "events" or kind == "markets" else None
                if kind == "events" and not parent:
                    parent = None
                self.db.execute(
                    """insert into winners values(?,?,?,?,?) on conflict(kind,ticker) do update set
                       received=excluded.received,observation_id=excluded.observation_id,parent=excluded.parent
                       where (excluded.received,excluded.observation_id) >
                             (winners.received,winners.observation_id)""",
                    [kind, identity, winner[0], winner[1], parent],
                )
        for scope, generations in page.scope_membership.items():
            for generation, identities in generations.items():
                self.db.executemany(
                    "insert or ignore into membership values(?,?,?)",
                    ((scope, generation, identity) for identity in identities),
                )
        for (kind, identity), (status, page_id, request_id) in page.parent_lookups.items():
            self.db.execute(
                """insert into lookups values(?,?,?,?,?) on conflict(kind,ticker) do update set
                   status=excluded.status,page_id=excluded.page_id,request_id=excluded.request_id""",
                [kind, identity, status, page_id, request_id],
            )
        for (batch_id, generation), candidates in page.parent_batch_candidates.items():
            for identity, (winner, page_id, request_id) in candidates.items():
                self.db.execute(
                    """insert into candidates values(?,?,?,?,?,?,?,?) on conflict(batch_id,generation,ticker)
                       do update set received=excluded.received,observation_id=excluded.observation_id,
                       parent=excluded.parent,page_id=excluded.page_id,request_id=excluded.request_id
                       where (excluded.received,excluded.observation_id) >
                             (candidates.received,candidates.observation_id)""",
                    [
                        batch_id,
                        generation,
                        identity,
                        winner[0],
                        winner[1],
                        winner[2] or None,
                        page_id,
                        request_id,
                    ],
                )
        self.db.executemany(
            "insert or ignore into terminal values(?,?)", page.parent_batch_terminal
        )
        self._sync()

    def stream_winners(self, kind: str) -> Iterator[tuple[str, str, str, str | None]]:
        yield from self.db.execute(
            "select ticker,received,observation_id,parent from winners where kind=? order by ticker",
            [kind],
        )

    @staticmethod
    def _task_scopes(state: dict[str, Any]) -> dict[str, str]:
        scopes = state.get("task_scopes")
        if isinstance(scopes, dict) and scopes:
            return scopes
        inferred = {}
        for index, work in enumerate(state.get("tasks", [])):
            if (
                not isinstance(work, dict)
                or work.get("singular")
                or work.get("parent_batch") is not None
                or "tickers" in work.get("params", {})
            ):
                continue
            endpoint = work.get("endpoint")
            kind = work.get("kind")
            scope = None
            if endpoint == "/series" and kind == "series":
                scope = "series"
            elif endpoint == "/events" and kind == "events":
                scope = "events_live"
            elif endpoint == "/events/multivariate" and kind == "events":
                scope = "events_combo"
            elif endpoint == "/markets" and kind == "markets":
                scope = (
                    "markets_combo"
                    if work.get("params", {}).get("mve_filter") == "only"
                    else "markets_live"
                )
            elif endpoint == "/historical/markets" and kind == "markets":
                scope = "markets_history"
            if scope is not None:
                inferred[str(index)] = scope
        return inferred

    def stream_membership(self, state: dict[str, Any]) -> Iterator[tuple[str, str]]:
        try:
            yield from self._stream_membership(state)
        except sqlite3.OperationalError as exc:
            self._translate_sql_deadline(exc)

    def _stream_membership(self, state: dict[str, Any]) -> Iterator[tuple[str, str]]:
        selected: dict[str, set[int]] = {}
        scopes = self._task_scopes(state)
        for index, scope in sorted(scopes.items(), key=lambda row: int(row[0])):
            generation = int(state["tasks"][int(index)].get("restarts", 0))
            work = state["tasks"][int(index)]
            selected.setdefault(scope, set()).add(generation)
            if scope == "markets_live" and not work.get("params", {}).get("mve_filter"):
                selected.setdefault("markets_combo", set()).add(generation)
        self.db.execute("drop table if exists selected_membership")
        self.db.execute(
            "create table selected_membership(scope text,ticker text,primary key(scope,ticker)) without rowid"
        )
        for selected_scope, generations in selected.items():
            for generation in sorted(generations):
                self.db.execute(
                    "insert or ignore into selected_membership select scope,ticker from membership where scope=? and generation=?",
                    [selected_scope, generation],
                )
        self._sync()
        yield from self.db.execute(
            "select scope,ticker from selected_membership order by scope,ticker"
        )

    def _effective_lookups(self, state: dict[str, Any]) -> None:
        self.db.execute("drop table if exists effective_lookups")
        self.db.execute(
            "create table effective_lookups(kind text,ticker text,status int,page_id text,primary key(kind,ticker)) without rowid"
        )
        self.db.execute(
            "insert into effective_lookups select kind,ticker,status,page_id from lookups"
        )
        for work in state.get("tasks", []):
            if work.get("parent_batch") is None or not work.get("done"):
                continue
            batch_id = digest(
                json_bytes(
                    [
                        work.get("endpoint"),
                        work.get("params"),
                        work.get("kind"),
                        work.get("singular"),
                        work.get("requested"),
                    ]
                )
            )
            generation = int(work.get("restarts", 0))
            self.db.execute(
                """insert or ignore into effective_lookups
                   select 'events',c.ticker,200,c.page_id from candidates c join terminal t using(batch_id,generation)
                   left join lookups l on l.kind='events' and l.ticker=c.ticker
                   where c.batch_id=? and c.generation=? and
                     (l.ticker is null or (l.page_id=c.page_id and l.request_id=c.request_id))""",
                [batch_id, generation],
            )
        self._sync()

    def stream_eligibility(self, state: dict[str, Any]) -> Iterator[tuple[str, str, int, str]]:
        try:
            yield from self._stream_eligibility(state)
        except sqlite3.OperationalError as exc:
            self._translate_sql_deadline(exc)

    def _stream_eligibility(self, state: dict[str, Any]) -> Iterator[tuple[str, str, int, str]]:
        self._effective_lookups(state)
        yield from self.db.execute(
            "select kind,ticker,status,page_id from effective_lookups order by kind,ticker"
        )

    def missing_parent_batches(self, state: dict[str, Any]) -> Iterator[tuple[str, list[str]]]:
        try:
            yield from self._missing_parent_batches(state)
        except sqlite3.OperationalError as exc:
            self._translate_sql_deadline(exc)

    def _missing_parent_batches(self, state: dict[str, Any]) -> Iterator[tuple[str, list[str]]]:
        """Stream sorted groups; table joins replace full-catalogue Python dictionaries and sets."""
        self._effective_lookups(state)
        self.db.execute("drop table if exists required_parents")
        self.db.execute(
            "create table required_parents(kind text,ticker text,primary key(kind,ticker)) without rowid"
        )
        self.db.execute("drop table if exists listed_union")
        self.db.execute(
            "create table listed_union(scope text,ticker text,primary key(scope,ticker)) without rowid"
        )
        self.db.execute("insert or ignore into listed_union select scope,ticker from membership")
        self.db.execute("drop table if exists listed_final")
        self.db.execute(
            "create table listed_final(scope text,ticker text,primary key(scope,ticker)) without rowid"
        )
        for index, scope in self._task_scopes(state).items():
            generation = int(state["tasks"][int(index)].get("restarts", 0))
            self.db.execute(
                "insert or ignore into listed_final select scope,ticker from membership where scope=? and generation=?",
                [scope, generation],
            )
            work = state["tasks"][int(index)]
            if scope == "markets_live" and not work.get("params", {}).get("mve_filter"):
                self.db.execute(
                    "insert or ignore into listed_final select scope,ticker from membership where scope='markets_combo' and generation=?",
                    [generation],
                )
        # The original rules combine all-observed parent gaps with separate union and
        # final-generation membership closures. Keep those sets separate in SQL.
        self.db.execute("""insert or ignore into required_parents
            select 'events',m.parent from winners m where m.kind='markets' and m.parent is not null
              and not exists(select 1 from winners e where e.kind='events' and e.ticker=m.parent)
              and not exists(select 1 from effective_lookups l where l.kind='events' and l.ticker=m.parent)
            union all select 'events',c.ticker from candidates c
              join winners m on m.kind='markets' and m.parent=c.ticker
              where not exists(select 1 from effective_lookups l where l.kind='events' and l.ticker=c.ticker)
            union all select 'series',e.parent from winners e where e.kind='events' and e.parent is not null and e.parent<>''
              and not exists(select 1 from winners s where s.kind='series' and s.ticker=e.parent)
              and not exists(select 1 from effective_lookups l where l.kind='series' and l.ticker=e.parent)""")
        self.db.execute("""insert or ignore into required_parents
            select 'events',m.parent from winners m
              join listed_union mm on mm.ticker=m.ticker and mm.scope in ('markets_live','markets_combo','markets_history')
              left join listed_union em on em.ticker=m.parent and em.scope in ('events_live','events_combo')
              left join effective_lookups l on l.kind='events' and l.ticker=m.parent
              where m.kind='markets' and m.parent is not null and em.ticker is null and l.ticker is null
            union all select 'events',m.parent from winners m
              join listed_final mm on mm.ticker=m.ticker and mm.scope in ('markets_live','markets_combo','markets_history')
              left join listed_final em on em.ticker=m.parent and em.scope in ('events_live','events_combo')
              left join effective_lookups l on l.kind='events' and l.ticker=m.parent
              where m.kind='markets' and m.parent is not null and em.ticker is null and l.ticker is null
            union all select 'series',e.parent from winners e
              join listed_union em on em.ticker=e.ticker and em.scope in ('events_live','events_combo')
              left join listed_union sm on sm.scope='series' and sm.ticker=e.parent
              left join effective_lookups l on l.kind='series' and l.ticker=e.parent
              where e.kind='events' and e.parent is not null and e.parent<>'' and sm.ticker is null and l.ticker is null
            union all select 'series',e.parent from winners e
              join listed_final em on em.ticker=e.ticker and em.scope in ('events_live','events_combo')
              left join listed_final sm on sm.scope='series' and sm.ticker=e.parent
              left join effective_lookups l on l.kind='series' and l.ticker=e.parent
              where e.kind='events' and e.parent is not null and e.parent<>'' and sm.ticker is null and l.ticker is null
            union all select 'series',e.parent from winners e
              join effective_lookups l on l.kind='events' and l.ticker=e.ticker and l.status=200
              left join listed_union sm on sm.scope='series' and sm.ticker=e.parent
              left join effective_lookups sl on sl.kind='series' and sl.ticker=e.parent
              where e.kind='events' and e.parent is not null and e.parent<>'' and sm.ticker is null and sl.ticker is null""")
        self.db.execute("""insert or ignore into required_parents
            select 'series',e.parent from winners e
              join effective_lookups l on l.kind='events' and l.ticker=e.ticker and l.status=200
              left join listed_final sm on sm.scope='series' and sm.ticker=e.parent
              left join effective_lookups sl on sl.kind='series' and sl.ticker=e.parent
              where e.kind='events' and e.parent is not null and e.parent<>'' and sm.ticker is null and sl.ticker is null""")
        self._sync()
        for kind in ("events", "series"):
            cursor = self.db.execute(
                "select ticker from required_parents where kind=? order by ticker", [kind]
            )
            batch: list[str] = []
            for (identity,) in cursor:
                batch.append(identity)
                if len(batch) == 100:
                    yield kind, batch
                    batch = []
            if batch:
                yield kind, batch


def observations(pages: Iterable[tuple[dict[str, Any], Any]]) -> Iterator[dict[str, Any]]:
    for manifest, payload in pages:
        if manifest["status"] != 200:
            continue
        kind = manifest.get("entity_kind")
        if kind not in ENTITY_KEYS:
            continue
        provenance = {
            "capture_id": manifest["capture_id"],
            "page_id": manifest["page_id"],
            "received_at": manifest["received_at"],
            "source_endpoint": manifest["endpoint"],
            "payload_sha256": manifest["sha256"],
            "source_kind": "direct",
        }
        envelope = {"series": "series", "events": "events", "markets": "markets"}[kind]
        if manifest.get("singular"):
            envelope = {"series": "series", "events": "event", "markets": "market"}[kind]
        for index, native in enumerate(
            records(payload, envelope, singular=manifest.get("singular", False))
        ):
            row = observation(kind, native, provenance, f"/{envelope}/{index}")
            yield {"entity_kind": kind, **row}
            if kind == "events" and "markets" in native:
                for n, market in enumerate(records(native, "markets")):
                    if market.get("event_ticker") != row["event_ticker"]:
                        raise CatalogueError("nested market parent contradicts event")
                    yield {
                        "entity_kind": "markets",
                        **observation(
                            "markets",
                            market,
                            {**provenance, "source_kind": "event_reference"},
                            f"/{envelope}/{index}/markets/{n}",
                        ),
                    }


def project_pages(
    pages: Iterable[tuple[dict[str, Any], Any]], *, selected: set[str] | None = None
) -> dict[str, list[dict[str, Any]]]:
    history = (
        row
        for row in observations(pages)
        if selected is None or row["entity_kind"] != "markets" or row["market_ticker"] in selected
    )
    unique = {row["observation_id"]: row for row in history}
    history = sorted(unique.values(), key=lambda row: (row["received_at"], row["observation_id"]))
    result: dict[str, list[dict[str, Any]]] = {kind: [] for kind in ENTITY_KEYS}
    for kind, key in ENTITY_KEYS.items():
        latest = {}
        for row in history:
            if row["entity_kind"] == kind and (kind != "markets" or row["source_kind"] == "direct"):
                latest[row[key]] = {k: v for k, v in row.items() if k != "entity_kind"}
        result[kind] = [latest[k] for k in sorted(latest)]
    series = {row["series_ticker"] for row in result["series"]}
    events = {row["event_ticker"]: row for row in result["events"]}
    relationships = []
    for event in result["events"]:
        resolved = event.get("series_ticker") in series
        relationships.append(
            {
                "parent_kind": "series",
                "parent_ticker": event.get("series_ticker"),
                "child_kind": "events",
                "child_ticker": event["event_ticker"],
                "resolved": resolved,
            }
        )
    combo_legs = []
    markets = {row["market_ticker"]: row for row in result["markets"]}
    for market in result["markets"]:
        parent = events.get(market["event_ticker"])
        market["series_ticker"] = parent.get("series_ticker") if parent else None
        market["usable"] = bool(parent and market["series_ticker"] in series)
        relationships.append(
            {
                "parent_kind": "events",
                "parent_ticker": market["event_ticker"],
                "child_kind": "markets",
                "child_ticker": market["market_ticker"],
                "resolved": market["usable"],
            }
        )
        for index, leg in enumerate(market.get("mve_selected_legs", [])):
            combo_legs.append(
                {
                    **leg,
                    "combo_market_ticker": market["market_ticker"],
                    "leg_index": index,
                    "resolved": leg["market_ticker"] in markets
                    and markets[leg["market_ticker"]]["event_ticker"] == leg["event_ticker"]
                    and leg["event_ticker"] in events,
                }
            )
    return {
        **result,
        "observation_history": history,
        "relationships": relationships,
        "combo_legs": combo_legs,
    }
