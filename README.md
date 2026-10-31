# kalshi-events

Independent, operator-invoked historical catalogue of public Kalshi event contracts. New full captures acquire series, ordinary events and all historical markets, including historical combos. They exclude live-market and multivariate-event traversals. Rules, price grids, lifecycle and settlement definitions, relationships and observation history retain their native values. Legacy exchange-wide artifacts remain readable; targeted metadata and dedicated-series workflows retain their own acquisition plans. `kalshi-market-data` owns prices, candles, trades and books. The two applications exchange copied immutable files; they do not share a warehouse or import each other's packages.

See [native contracts and storage/publication formats](docs/contracts.md) for compatibility, recovery and incremental freshness boundaries.

See [historical production completion](docs/remaining-work.md) for verified defects,
capacity and storage blockers, and the remaining acceptance gates.

Python 3.12 and uv are required:

```sh
uv sync --locked
uv run kalshi-events --root ./data/operator capture
# Exit 0 with "complete": true means the catalogue capture is ready. Exit 3 with
# "complete": false means the invocation hit a budget; repeat with the reported capture ID.
uv run kalshi-events --root ./data/operator capture --capture-id CAPTURE_ID
uv run kalshi-events --root ./data/operator build --capture-id CAPTURE_ID
uv run kalshi-events --root ./data/operator publish --capture-id CAPTURE_ID
uv run kalshi-events --root ./data/operator current
```

In a source checkout, the operator root defaults to the canonical checkout’s `data/operator`, independent of the current working directory. `--root` and `KALSHI_EVENTS_ROOT` can select another strict descendant of that same `data/` directory; external write destinations are rejected. It holds raw evidence, captures, builds, releases and the versioned publication pointers. Only one writer may hold the canonical root at a time. Symlinked roots, files and destinations are refused.

New ordinary and scoped roots use storage version 5. Existing storage versions 1–4 remain readable; stop writers and explicitly run `kalshi-events --root ./data/operator upgrade-root` before further writes. Upgrade changes only the closed-schema marker under the writer lock, preserving the logical root ID and exact series scope. Existing incomplete captures retain their sealed acquisition settings and recovery protocol. Older binaries refuse an upgraded root, including after waiting for its writer lock; binary rollback requires a separate legacy root and retained verified publication. Never downgrade the marker. Storage version 5 does not introduce a new catalogue or metadata contract.

## Data location

Keep persistent datasets, raw evidence, warehouses, releases, metadata bundles, diagnostic samples, metrics and benchmark reports beneath this checkout’s ignored `data/` directory. Source-checkout commands enforce that boundary, including explicitly supplied output paths. The `data/` container itself is not an operator root. Symlink ancestors, traversal outside the boundary and writes into source directories remain forbidden. Use separate child directories for operator roots, probes, metadata and reports; metrics must still stay outside the selected operator root and bundle.

`data/` is persistent user data, not a disposable build cache. It is excluded from Git and package distributions. Repository cleanup must preserve it. Code runtimes and virtual environments remain separate. Standalone installed consumers retain their caller-owned path behavior when no checkout context is present; read-only verification can inspect externally supplied artifacts. Low-level `Store` callers remain responsible for selecting their own paths.

Invoke frozen runtime tools from the canonical checkout, or set `KALSHI_EVENTS_PROJECT_ROOT` to its absolute path, so they use this project’s data boundary rather than the frozen code directory. The binding must identify a real checkout of this project; it does not permit an arbitrary data directory. Do not move a directory while a worker is using it. Existing active jobs retain their original paths until they stop; migrate complete trees under their canonical locks, preserving hard links, evidence bytes and original limits. Dated report paths remain historical provenance; consult `data/relocation-20261029/completed-migration-map.json` for completed moves.

## DuckDB 2 runtime and database files

The locked producer runtime pins DuckDB 2.0.0, dlt 1.31.0, dbt-duckdb 1.11.1 and dbt-core 1.12.5. PyArrow is constrained to `>=17` and the current lock resolves 26.0.0. The dlt integration is validated for this application's narrow Arrow landing path; it is not a claim that dlt 1.31 generally supports DuckDB 2. See the [DuckDB release calendar](https://duckdb.org/release_calendar), [DuckDB 2 migration guide](https://duckdb.org/docs/2.0/operations_manual/migration), [Python migration guide](https://duckdb.org/docs/2.0/clients/python/migration) and [dlt PR 4601](https://github.com/dlt-hub/dlt/pull/4601).

New database files created by DuckDB 2 use its native v2.0 format, which DuckDB 1.x, including 1.5.6, cannot open. DuckDB 2 reads existing 1.x databases without automatically converting their files. A new build that needs a native database from a certified legacy warehouse copies the source read-only into fresh staging with `COPY FROM DATABASE`, then verifies the converted contents; the source warehouse and release stay unchanged. Certified native DuckDB 2 databases use the byte-copy path. A completed certified legacy build is reused on resume and is not converted automatically. Request a new build or rebuild from its existing capture to produce a native database and repeat certification. This physical storage change does not alter the application root's storage version 5 marker. Public Parquet exports retain their existing nullable fields and ordering. The public Parquet handoff remains separate from direct database access: consumers opening `production.duckdb` need DuckDB 2.0 or newer.

Conversion uses the operation's existing storage allowances. DuckDB's `max_temp_directory_size` caps spill files, not the destination database or WAL; output growth is checked after `COPY`, so configured root, temporary or database soft limits can be exceeded before cleanup. No hard conversion quota is added; the separate dlt post-load quota work remains tracked by `WORK-42`.

An isolated, locked DuckDB 2.0 runtime has been installed and verified. Installation alone does not establish a completed catalogue or production activation. Launch requires explicit operator authorization, fresh recovery checks, no active writer, and DuckDB 2-compatible direct-database consumers before native activation. Keep the matching legacy runtime, publication and evidence available for rollback; never open a DuckDB 2-native file with a 1.x reader. See the [measured results and rollout status](docs/performance.md#duckdb-2-storage-compatibility-and-rollout).

## Capture

The public transport uses only `https://external-api.kalshi.com/trade-api/v2`. Requests use a shared limiter at a 5 requests/second ceiling by default. This is a configured client ceiling, not a verified anonymous-provider allowance. Set `--max-rate` to choose another positive initial ceiling. A fair scheduler keeps at most eight futures submitted and one request outstanding per traversal. The default historical plan uses 200 ordinary events/page and 1000 historical markets/page. `capture --catalogue-profile exchange` explicitly selects the legacy broad plan; metadata selections and dedicated series captures retain their documented behavior.

Each attempt writes its raw HTTP response bytes and per-attempt accounting before its page is applied. A request that fails with 429, 500, 502, 503 or 504 is retried up to four attempts in total, with equal-jitter exponential backoff capped at 30 seconds. A `Retry-After` header, as delta-seconds or an HTTP date, is always honored and recorded on the attempt receipt. Workers recheck shared cooldowns after waiting, so a later deferral also delays workers already waiting for a request slot. Resume recovers verified completed receipts without reissuing their requests.

Capture stops at its request, download, response, duration or storage allowance and reports `complete: false` with exit code 3. Completed raw evidence is kept, and the capture resumes from there. Invalidated cursors restart their traversal. New captures checkpoint historical cutoffs at traversal start, after resume/reset, every 100 committed data pages or 300 seconds at the scheduler boundary, and before terminal completion. Retries or limiter waits can extend the elapsed time to the actual request; the checkpoints do not promise a hard 300-second freshness interval or atomic snapshot. Missing parents are retained as unresolved relationships and are never derived from ticker prefixes.

Every capture seals its response ceiling at creation, so later reads with different flags still verify the bytes it recorded.

New captures request gzip by default; `--compression identity` opts into identity. An identity response is valid when gzip was requested. Raw evidence retains exact encoded bytes; gzip receipts also certify decoded size and checksum. Both representations have independent ceilings: `--max-decoded-response-bytes` defaults to the chosen `--max-response-bytes`. Resume retains the original compression and ceilings. Truncated or corrupt gzip, additional members, trailing bytes and excessive expansion fail closed.

New protocol-5 captures default to `--rate-policy decrease-on-429`; `--rate-policy fixed` disables adaptation. The shared limiter halves its effective ceiling once for each current-generation 429 response, never below `min(initial ceiling, 0.25 requests/second)`, honors `Retry-After`, and never raises the rate during an invocation. Waiters recheck shared spacing and cooldowns. Older captures retain their sealed policy. A manual resume starts at the supplied `--max-rate` ceiling and can decrease again; it does not persist the previous invocation's effective rate. Operators select the initial ceiling and policy; only the specified downshift is automatic.

### Optional transport diagnostics

Metrics reports must stay outside the operator root, immutable release directories and metadata bundle destinations. The CLI rejects overlapping paths before creating a report or starting acquisition, including when the bundle destination does not exist yet.

With `--metrics-file FILE`, the CLI also writes a sibling `FILE.progress.json`. This version-1 companion contains only `version`, a fixed `phase`, monotonic `elapsed_seconds`, UTC `updated_at`, and fixed aggregate `counters`: `journal_records`, `total_pages`, `indexed_pages`, `backlog_pages`, `reused_pages`, `verified_pages`, `first_data_request_elapsed_seconds` and `first_data_page_elapsed_seconds`. Elapsed values are measured in the refresh worker and exclude supervisor setup and process startup. The first-request value is captured immediately before the first non-cutoff HTTP attempt, after limiter and cooldown waits; the first-page value is captured after its data page is committed. Initial and final snapshots are written, phase changes flush immediately, and same-phase progress calls write only when at least 15 seconds have elapsed since the previous progress write. The file contains no tickers or request traces. The final metrics report includes `recovery_cache_enabled`, `recovery_cache_reused` and `recovery_cache_invalidated` aggregate counters. A production session report records its companion path as `metrics_progress_file` and records supervisor preflight and accounting durations separately.

Capability and rate checks are manual, bounded operations. Use a new disposable root and a known market ticker with a new metrics report path:

```sh
mkdir -p ./data/probes/manual
uv run kalshi-events --root ./data/probes/manual/root \
  --metrics-file ./data/probes/manual/metrics.json capture \
  --market-ticker KNOWN_MARKET_TICKER --compression gzip --max-rate 5 \
  --max-requests 12 --max-download-bytes 8388608 \
  --max-response-bytes 4194304 --max-decoded-response-bytes 4194304 \
  --max-duration-seconds 30 --max-root-bytes 67108864 --max-temp-bytes 33554432
```

Inspect the probe's raw receipts: version 2 with `content_encoding: "gzip"` proves compressed evidence was served; version 1 means identity fallback. Compare encoded bytes, request time, rate waits and retry waits in the external metrics report. To investigate a different rate, explicitly repeat with another new root/report and a chosen positive `--max-rate`, keeping other limits fixed. A small probe can expose throttling but cannot establish sustained catalogue throughput. Commands never run live capacity probes or raise the rate automatically. New captures begin at the selected ceiling (5 requests/second by default) and may only decrease it after HTTP 429. A short 2026-10-20 probe observed gzip on events and markets, with identity fallback on cutoff responses. This does not establish support on every endpoint or sustained higher-rate capability.

### Serial acquisition comparisons

Run the manual probe script from the checkout’s locked environment with a new destination beneath `data/`. Create its parent first:

```sh
mkdir -p ./data/probes
uv run --locked python scripts/probe-acquisition.py --output ./data/probes/acquisition-probe
uv run --locked python scripts/probe-acquisition.py --output ./data/probes/rate-probe --rates 5 10 20
```

The default compares identity and requested gzip at 5 requests/second. Explicit rates replace that list; at most three distinct finite positive rates up to 20 are accepted. Samples run serially against fresh roots and never build or publish. Each sample allows 12 attempts, 30 seconds and 64 MiB encoded downloads, with independent 32 MiB encoded and 64 MiB decoded response ceilings. The screening run retains its 72-attempt, 256-MiB and 180-second total supervision/accounting allowance, plus at most five seconds for termination and reaping. Aggregate retained and temporary allowances are 2 GiB and 1 GiB under existing Store accounting semantics. Remaining allowances can shorten later samples.

The output separates roots, reports and logs and preserves failed evidence. Timeouts or uncertain accounting stop subsequent samples. A request-budget stop with verified accounting is an expected partial sample. Requested gzip may produce identity fallback. The report distinguishes attempts, completed pages, errors/retries, bytes, observed encodings and timing. Samples may retrieve different changing source data; their byte ratios are not controlled compression benchmarks. Both screening and throughput probe samples explicitly use fixed pacing. No winning rate is selected and no rate is raised automatically; ordinary protocol-5 captures can still lower their rate after HTTP 429. Short probes establish capability observations, not sustained full-crawl throughput.

For longer, explicitly bounded comparisons, select the throughput profile:

```sh
uv run --locked python scripts/probe-acquisition.py --profile throughput \
  --output ./data/probes/throughput-probe --rates 10 15 20 \
  --series-ticker KXMIDTERMVOTETURN --series-ticker KXMIDTERMMOV \
  --collection-ticker KXMVECROSSCATEGORY-R
```

Omit selectors for global samples only. Otherwise supply two to eight distinct ordinary-series or multivariate-collection partitions. Rates default to `[5]`; explicitly supplied rates run in ascending order. The sequence is identity/gzip at the lowest rate, one serial and one parallel selected-event sample when requested, then identity/gzip at remaining rates. Global samples use the production capture command. Partition samples acquire diagnostic raw evidence only: they cannot build or publish a catalogue and cannot resume.

The throughput profile permits 2,000 attempts, 4 GiB encoded downloads and 15 minutes for the entire invocation, including startup, preflight and reporting. Collection preflight permits 100 attempts/64 MiB/30 seconds; each global sample 250 attempts/512 MiB/60 seconds; each partition sample 200 attempts/512 MiB/60 seconds. Five seconds within each child allowance are reserved for finalization. Termination and reaping have at most five additional seconds. Responses retain independent 32 MiB encoded and 64 MiB decoded ceilings. Aggregate Store-accounted retained/temporary limits are 8 GiB/2 GiB; preceding consumption reduces subsequent allowances.

Reports preserve partial evidence and stop remaining scenarios after any HTTP 429, timeout, uncertain accounting, invariant failure or unexpected command failure. Fewer than 100 validated data pages or ten seconds of acquisition is insufficient for a throughput conclusion. Even larger samples establish only short-window observations. Collection membership relies on the provider's filter, checked against its collection definition's series; it is not an independently certified catalogue scope. See [performance guidance](docs/performance.md) for interpretation and actual results.

## Build and publish

The default full-capture scope is historical: series, ordinary events and all historical
markets, including combos. `build` and `publish` automatically select the historical profile
for a new sealed historical capture. The shared Arrow/dbt path preserves native payloads,
exact decimals, provenance, observation history and deterministic exports. Missing parents
and combo references remain visibly unresolved; no historical market is removed for that
reason.

Historical builds use `builds-historical/`, publish `oddsfox.kalshi.historical.v1`, and promote
`current-historical.json`. Their coverage binds included and omitted scopes, selected task
indices/generations, the source capture's actual completion status, root identity and
warehouse checksum. The database's `main.catalogue_coverage` identifies this scope.
Historical output cannot serve as a legacy exchange-wide incremental baseline. `current`,
`replay` and `rebuild` prefer the historical pointer when present; explicit
`--catalogue-version 1|2` continues to select legacy publication tracks.

To finish completed historical scopes from a stopped legacy capture without networking:

```sh
uv run kalshi-events --root ./data/operator refresh \
  --historical --capture-id SAVED_CAPTURE_ID
# Individual stages are also available; these never resume acquisition.
uv run kalshi-events --root ./data/operator build \
  --historical --capture-id SAVED_CAPTURE_ID
uv run kalshi-events --root ./data/operator publish \
  --historical --capture-id SAVED_CAPTURE_ID
uv run kalshi-events --root ./data/operator current --historical
```

This explicit offline path requires completed series, ordinary-event and historical-market
traversals, including terminal cutoff evidence. It validates the full journal and selected
raw receipts, without changing the old capture's globally incomplete state. Excluded bodies
are not certified by a historical build. The output is complete for its declared scope,
not the full exchange or an atomic snapshot. Targeted captures cannot enter this path.
A failed build or verification preserves prior publications.

Legacy exchange-profile captures still use certified catalogue v1/v2 builds and their
`current.json`/`current-v2.json` pointers. Existing releases and schemas are not rewritten.
Historical publication and incomplete captures protect their required evidence; discard
refuses referenced sources. Intentional retirement of excluded data ends ordinary resume
for that source and leaves an explicit `retired-scopes.json` guard; it is not normal pruning.

Native tickers preserve their exact identifiers, including punctuation. Singular lookup URLs encode each ticker as one path segment. Trailing U+0020 spaces remain part of the native identity and become `%20` at the end of a lookup URL. Comma-containing tickers such as `JOBLESS-22SEP10-C220,000` use singular selected-market and parent lookups; other selected markets retain automatic bounded batching. Pass a leading-hyphen ticker with an equals sign, for example `--market-ticker=-23MAR-T3`.

Native payloads remain in `payload_json`. Commonly queried prices and fractional quantities are exact `DECIMAL(38,18)` columns. Current rows are the latest observed native entities, ordered by the UTC receipt instant and then observation identity. Comparison does not depend on fractional-second width or equivalent UTC-offset spellings; stored receipt timestamps and payloads retain their original text. New timestamps use six fractional-second digits. Provider updates do not establish historical rule applicability.

The release contains `series.parquet`, `events.parquet`, `markets.parquet`, `relationships.parquet`, `combo_legs.parquet`, `observation_history.parquet`, `coverage.parquet` and a checksummed manifest. Coverage records the observed traversal interval and cutoff evidence. These endpoints do not provide an atomic exchange snapshot. Markets moving between live and historical tiers can change during the observation interval; live and historical traversals overlap, and repeated direct observations are retained.

## Legacy exchange-wide incremental refresh

This section applies only to the explicit legacy exchange profile and existing catalogue v1/v2 publications. Historical publications are not incremental baselines; use an explicit historical `refresh --full` for a new reconciliation. To create a legacy version 2 baseline from a complete exchange-profile capture, publish it with the version selector; this builds the version 2 warehouse when needed. A preceding version 1 build/publication is necessary only when both publication tracks are wanted. Version 2 uses a separate `builds-v2` warehouse and `current-v2.json`; version 1 uses `builds` and `current.json`.

```sh
uv run kalshi-events --root ./data/operator publish \
  --capture-id FULL_CAPTURE_ID --catalogue-version 2
uv run kalshi-events --root ./data/operator capture --incremental
# Resume an incomplete refresh with its reported ID; its original baseline/window stay sealed.
uv run kalshi-events --root ./data/operator capture --capture-id DELTA_CAPTURE_ID
uv run kalshi-events --root ./data/operator build --capture-id DELTA_CAPTURE_ID
uv run kalshi-events --root ./data/operator publish \
  --capture-id DELTA_CAPTURE_ID --catalogue-version 2
uv run kalshi-events --root ./data/operator current --catalogue-version 2
```

An incremental capture pins the certified v2 publication when one exists, otherwise the certified v1 publication. A corrupt v2 publication fails closed. Composed builds and publications require explicit capture IDs. Legacy default selection continues to choose a full capture, and metadata v1 export rejects an explicit incremental capture.

Ordinary events and supported non-combo live markets use metadata-update filters from the previous published capture's start time minus five minutes. Series, combo events, combo markets and historical markets are fully reconciled on every refresh. A refresh seals its start before requesting data and advances the published watermark to that time only after verification and promotion. Provider timestamps never determine the winning observation.

This mode permits dated price, volume and open-interest values. Carried rows retain their actual observation timestamps and source capture/receipt identities. Metadata-update filters are not a universal change feed: late updates outside the overlap, and removal from a scope queried only for changes, may require a full rebase. Coverage describes the acquisition interval rather than a fresh atomic exchange snapshot.

Membership is separate from historical observations. A complete reconciliation replaces its scope's membership, while omission from a delta removes nothing. Live and historical membership are combined before resolving current markets. Removed entities retain their history and can reappear. Parent lookups resolve declared relationships without turning lookup-only entities into permanent list members.

Each baseline supports at most eight deltas. Retained baseline and delta warehouses, including WAL and staging copies, count toward the storage limits; size those limits for the retained chain before refreshing. To rebase the legacy track, explicitly acquire an exchange-profile capture with `capture --catalogue-profile exchange`, then publish that capture with `--catalogue-version 2`. A refresh whose pinned predecessor has been superseded cannot publish; acquire a new refresh against the current publication. Incomplete captures remain recovery pins indefinitely. Explicitly abandon an unreferenced incomplete capture with:

```sh
uv run kalshi-events --root ./data/operator discard --capture-id CAPTURE_ID
```

Discard refuses complete captures and captures referenced by a publication or descendant. Subsequent verified publication can prune obsolete dependencies. Missing, cyclic, corrupted or cross-root ancestry prevents composition and publication. Never manually delete pinned captures, raw evidence, builds or releases.

## One-command catalogue refresh

The operator-invoked `refresh --full` coordinates the historical acquisition plan, shared
build, complete export, fresh verification, historical pointer promotion and cleanup under
one writer lock and invocation deadline:

```sh
uv run kalshi-events --root ./data/operator refresh --full
# Resume a new historical capture's sealed acquisition plan:
uv run kalshi-events --root ./data/operator refresh --capture-id CAPTURE_ID
# Complete saved historical scopes of an old broad capture without acquisition:
uv run kalshi-events --root ./data/operator refresh \
  --historical --capture-id SAVED_CAPTURE_ID
```

`--full` and `--capture-id` conflict. New full captures request gzip and use the
5 requests/second initial ceiling with downward-only adjustment after HTTP 429. Resource,
compression and rate-policy options remain available; resumed captures retain sealed
settings. Historical publications currently require explicit full reconciliations; bare
`refresh` will not silently turn a historical baseline into an exchange-wide delta or rebase.
Existing legacy v1/v2 incremental workflows keep their separate baseline rules.

The JSON result distinguishes workflow `complete`, real `capture_complete`, stage,
publication state and available accounting. Historical results additionally identify
`coverage_scope`, `scope_complete` and `source_capture_complete`. A saved historical build
can finish with `complete: true`, `scope_complete: true` and `capture_complete: false`:
that does not mark the old broad capture complete. Its zero requests/download bytes describe
only this offline invocation, never a reset of cumulative production consumption.
Unknown accounting remains null. Publication state `unknown` may mean a pointer was promoted
before cleanup failed; inspect and retry the same explicit offline command rather than
assuming rollback or resuming network acquisition. Budgets return exit 3, invalid input or
invariant failures exit 1, and only verified completion exits 0.

For 1–100 selected markets, use `metadata refresh`; it retains its explicit selection scope.

## Supervised production catalogue

Use the manual production runner to establish one consumer database on this MacBook. Run the following commands from the checkout root with Python 3.12 in the locked project environment (`uv run --locked python` or the activated virtual environment). Retained datasets and run logs must stay under the ignored `data/` directory; use an explicit `--root` for the particular operator root when several datasets share that container. Production directories inside the checkout must be strict descendants of `data/`; the source tree and the `data/` container itself are rejected. Source-checkout commands reject external write destinations, and symlink paths remain forbidden. The `data/` directory holds persistent operator evidence, not disposable build output; preserve it during repository cleanup. Relocation inventories are stored with the data; archived reports retain their original paths as provenance.

```sh
python scripts/run-production.py \
  --directory "./data/kalshi-production" --full
# Resume only the exact reported capture after reviewing the stop reason.
python scripts/run-production.py \
  --directory "./data/kalshi-production" \
  --capture-id CAPTURE_ID
# Finish only the completed historical scopes of a saved legacy capture, without HTTP:
python scripts/run-production.py \
  --directory "./data/kalshi-production" \
  --historical --capture-id CAPTURE_ID
# Only after a supervisor reports unknown child accounting from abrupt termination:
python scripts/run-production.py \
  --directory "./data/kalshi-production" \
  --capture-id CAPTURE_ID --reconcile-accounting
# Explicit recovery after a Mac reboot, preserving every existing wall deadline/cap.
python scripts/run-production.py \
  --directory "./data/kalshi-production" \
  --capture-id CAPTURE_ID --reconcile-accounting --recover-reboot \
  --recovery-volume-uuid EXPECTED_VOLUME_UUID
# Consume the report's one-time 48-hour window for this exact stopped capture.
python scripts/run-production.py \
  --directory "./data/kalshi-production" \
  --capture-id CAPTURE_ID --reconcile-accounting --extend-session
# Recover activation of the current historical publication without acquisition.
python scripts/run-production.py \
  --directory "./data/kalshi-production" --activate-current --historical
```

`--reconcile-accounting` is an explicit recovery path for a stopped session whose child died before returning its counters. Under the canonical writer lock, it validates every receipt and binds each unfinished request to the exact capture task. If all durable attempts are finalized, it restores their validated exact totals. Otherwise it charges the full sealed response ceiling plus one 64 KiB HTTP read chunk per unfinished request against the original aggregate limit. The pinned HTTP core reads at most 64 KiB per chunk. Before recovery proceeds, the supervisor enforces the approved legacy-to-current or current-to-current lock/package transition, HTTP transport-source fingerprint and 64 KiB read bound described in the [recovery contract](docs/contracts.md#production-database-entry-point). Only the documented historical legacy reports may omit selected saved package fields. The prior system snapshot is retained as `original_system`, while `system` records the recovery environment. In the latter case exact downloaded bytes remain unknown and the report records the conservative bound; unfinished receipts are never edited. Recovery accepts only the stopped session's exact capture, pinned HTTP runtime, boot identity (unless explicitly recovered below), effective deadlines and resource ceilings, and only after supported child-counter stops (including a resumed child missing invocation counters), compressed-memory, process RSS, low-disk-space, or persistent process-group RSS sampling timeouts. The exact legacy system-wide `ps` timeout report is accepted for existing sessions. Current free-space and all other resource checks still run before networking; ledger mismatches, receipt or integrity failures, mount changes, malformed resource output, and unreaped process groups remain ineligible. It records the recovery revision while preserving the original revision and source. The current supervisor uses macOS pressure state rather than the former 20 GiB logical-compressor cutoff; legacy stops remain identifiable in the report history. A retry after any new abrupt stop needs another explicit reconciliation.

Memory supervision samples every 15 seconds. Before processing starts, telemetry must be available and macOS pressure must be NORMAL. During work, CRITICAL pressure stops at the first observed sample; WARN pressure stops after 60 continuous seconds, resetting on NORMAL or a sample gap above 22.5 seconds. The existing process-group RSS limit remains greater than 24 GiB for 60 seconds. The complete telemetry sample is bounded to five seconds and the invocation deadline. The pressure reader uses a capability-checked private macOS sysctl, with userspace values 1=NORMAL, 2=WARN and 4=CRITICAL; unsupported output fails closed.

Reports distinguish logical compressor contents (`system_compressed_bytes`, retaining its legacy meaning), physical compressor occupancy, worker-group RSS, worker-group physical footprint and supervisor physical footprint. System measurements include other applications; they do not establish that Kalshi caused pressure. Physical footprint is attribution telemetry, not an additional stop threshold. Only the latest 256 resource samples remain inline, with total/dropped counts and cumulative peaks. Current errors and phases describe the current attempt; the previous stop remains recorded separately. Historical-only invocations report zero API activity separately from lifetime acquisition accounting, whose exact downloaded bytes may remain unavailable.

Historical retry preflight recovers build-replacement state, then reclaims only validated abandoned `tmp/build-historical-<UUID>` staging directories under the canonical writer lock before applying storage admission limits. The independent 150 GiB free-space floor still applies before processing starts; if free space is below that floor, free capacity separately before retrying. Raw evidence, published builds, replacement backups and recovery caches are protected. Partial Parquet files and disposable indexes are not completed warehouses or reusable certification. Progress reports show verified receipts, emitted observations and yielded batches while landing; projection, dbt, export and verification remain separate stages. DuckDB loading and projection use 4 GB managed-memory and four-thread settings; Python and Arrow memory are supervised separately.

An eligible stopped historical capture can explicitly transition to the `expanded` storage profile with `--historical --capture-id ID --historical-resource-profile expanded`. The profile is sealed in append-only session policy history; later historical resumes inherit it, and conflicting selections are rejected. Policy version 1 records monitoring-only enforcement and acknowledged overshoot. Database and temporary limits become 128 GiB and 192 GiB. The saved policy records 96 GiB and 144 GiB forecast-admission thresholds; runtime baseline checks and cooperative stops use 112 GiB and 168 GiB, respectively. These one-second checks can overshoot and are not filesystem hard quotas. Root (350 GiB), aggregate (400 GiB), free-space (150 GiB), RSS (24 GiB), memory-pressure, thread, and managed-memory policies stay unchanged. Activation-only commands cannot select `expanded`; use the exact capture's historical resume to carry that policy through activation.

`--recover-reboot` requires `--capture-id`, `--reconcile-accounting` and `--recovery-volume-uuid EXPECTED_VOLUME_UUID`, cannot be combined with `--extend-session`, and never grants more time. The expected UUID is checked against macOS volume metadata, root ownership and the mounted filesystem. Legacy reports did not store a volume UUID: this explicitly selects the current volume and records that historical physical-volume identity cannot be proven. A previously recorded UUID must match. After validating the stopped session, root and filesystem, it records the previous boot/clock mapping and reconstructs monotonic deadlines from the unchanged wall-clock deadlines. Fresh accounting and resource checks must pass before networking. Expired deadlines, backward wall clocks and unreconciled child markers remain fatal. Ordinary resume still rejects a changed boot.

`--extend-session` may be used once per report and only with `--capture-id` and `--reconcile-accounting`. It records the old and new deadlines and current source revision, then consumes a fixed new 48-hour window before offline accounting preflight; that preflight uses the new window, and a failed preflight does not return the grant. The original `started_*` values remain as provenance. Ordinary resume keeps the current effective clock. The 850,000-attempt, 256 GiB download, storage and response limits remain cumulative across the extension.

Legacy exchange-profile protocol-5 full captures may keep a private SQLite parent index across invocations, in the operator's accounted temporary storage, with an 8 MiB page cache and memory mapping disabled. The persistent cache is enabled only when the root already has a validated logical root ID; without one, acquisition-first recovery still applies using the disposable index, and no root ID is created for this optimization. Cache checkpoints bind the root ID, capture ID and settings, acquisition protocol, normalization version and exact journal anchor, and represent an ordered prefix of verified journal pages. On resume, the runner validates the snapshot and journal, prioritizes available acquisition requests, then indexes older committed receipts in journal order when the scheduler has room. This ordering depends on a validated snapshot; if it is absent, recovery may need to replay the full authoritative journal before networking begins. The index has a separate soft checkpoint trigger at 100 indexed pages or five seconds, checked between pages; one large receipt can exceed five seconds.

The index is an auxiliary hint, never evidence or certification. A missing, stale, ahead, incompatible or corrupt cache can be discarded and rebuilt from validated receipts; malformed settings, journal state, raw evidence or receipts still fail closed. If parent-directory permissions prevent creating a private cache, acquisition uses the disposable index without changing those permissions. Existing unsafe cache contents still fail closed. Completion requires a clean index built from evidence verified in the current invocation and a freshly verified parent closure. A warm cache is reset before that pass rather than upserted; a cold clean accumulator may be reused. Legacy, targeted, scoped-series and incremental captures keep their existing indexing behavior. Published builds remain independently verified.

A paired 100-page SSD benchmark measured earlier first-request and first-commit medians on warm recovery, while cold recovery improved the first-request median and had a slightly slower first-commit median. These small-fixture results do not establish the five-minute target or full-scale performance; see [recovery measurements and open gates](docs/performance.md#supervised-production-acceptance).

The directory contains the producer-owned `operator/` root, external session evidence in `runs/`, and the consumer entry point `production.duckdb`. New full runs use the historical contract, retaining series, ordinary events and every historical market including combos, while excluding live-market and multivariate-event traversals. Legacy run reports and v2 publications retain their original scope. Use `--historical --capture-id ID` to finish saved historical scopes offline and `--activate-current --historical` to recover historical activation. It requests gzip at a five-request/second ceiling with downward adjustment after HTTP 429. It never resumes automatically or publishes incomplete acquisition.

Each approved window lasts at most 48 hours, including preflight and reporting. Refresh has a 46-hour cutoff; independent verification and activation must finish before hour 47:50. Active stages continuously check their effective UTC and monotonic deadlines: forced sleep or a forward wall-clock change cannot extend the wall allowance, and a backward change cannot extend the monotonic allowance. The ceilings are 850,000 attempts, 256 GiB encoded downloads, 400 GiB aggregate storage, 350 GiB operator-root storage, and 64 GiB each for temporary and database storage under the default profile. The expanded historical profile has 128 GiB database and 192 GiB temporary limits, 96 GiB and 144 GiB admission thresholds, and 112 GiB and 168 GiB sampled stop thresholds. At least 150 GiB must remain free. Per-response ceilings are 384 MiB encoded and 768 MiB decoded. Resource checks run every 15 seconds; summed descendant RSS over 24 GiB for one minute stops the run. This conservative RSS sum can count shared pages more than once. Process RSS is queried only for the supervised process group. One `ps` timeout is retried once within the same five-second probe budget; if RSS remains unavailable, the supervisor stops and reaps the child without recording an RSS sample. The report records `resource accounting failed: process-group RSS sampling timed out`; resuming that exact capture requires explicit accounting reconciliation. A transient timeout that succeeds on retry does not stop the run. Malformed output or an empty result for a live group remains ineligible. Storage stops are one-second cooperative checks that can overshoot; they are not hard quotas. Existing dlt post-load quota limitations still apply.

Progress snapshots do not restart a stopped worker or extend any deadline. Ordinary resume remains an explicit operator command for the exact reported capture and retains the current 48-hour session clock and cumulative accounting.

An explicitly authorized offline completion can supply
`--historical --capture-id ID --offline-duration-seconds SECONDS` (at most 86,400 seconds).
This records one separate historical-only window bound to the capture, root and frozen
revision. It may outlive the old networking period, but never changes that period, consumed
request/download totals or extension history. Repeating the command reuses the saved offline
deadline rather than granting more time. The window covers saved-evidence processing,
verification and activation only; it cannot enable HTTP requests or ordinary resume.
Storage, memory, free-space and termination safeguards still apply. Without this explicit
option, the existing approved deadline remains in force.

After a window expires, a separately authorized continuation must explicitly renew it:

```sh
python scripts/run-production.py --directory ./data/kalshi-production \
  --historical --capture-id CAPTURE_ID \
  --offline-duration-seconds 86400 --renew-offline-window
```

Renewal requires a stopped session with no unreconciled child, the same boot and volume,
and matching production-owner and operator-root identities. Both clocks must show that
the prior window expired. The old window remains unchanged in
`offline_historical_window_history`; the new window binds the current verified source
revision. New grants use operational-window version 2 with a 24-hour maximum. Existing
version 1 grants retain their four-hour maximum, including when checked in history;
requesting a longer duration on an ordinary retry never extends them. This window version
is separate from root storage, acquisition and release versions.
Renewal cannot reset acquisition
accounting, resource ceilings or consumed extensions, and cannot enable API requests.

To replace an existing window before both clocks expire, use the same command with
`--supersede-offline-window` instead of `--renew-offline-window`. This flag implies
renewal; it keeps the old window unchanged in history and records the exact successor
start that superseded it. The validator accepts overlapping windows only when that
record matches both grants. The stopped-session, child, boot, volume, owner and root
checks still apply, and cumulative acquisition limits remain unchanged.
It grants processing time, not a promise that a large build will finish within 24 hours.

`production.duckdb` is an atomically activated hard link to the certified warehouse, sharing its physical storage. It appears only after the workflow completes, standalone release verification passes, and fresh certification binds the database checksum to the expected publication. Open it read-only:

```python
import duckdb

connection = duckdb.connect(
    "./data/kalshi-production/production.duckdb",
    read_only=True,
)
```

An existing reader retains its opened snapshot. DuckDB can reuse that database for additional connections to the same path in the same process: close every connection to the path before reopening, or use a fresh process to see the activated generation. A writable consumer can damage the shared certified file. Keep the hard link outside `operator/`, retain evidence and recovery pins, and never edit warehouse files. The entry point survives pruning of its original pathname, but does not pin the older raw-evidence closure.

Publication and production activation are separate results. Failures before activation replacement preserve the previous production database. A failure after replacement can leave the new file present with activation still uncertain; the report does not imply rollback. Offline activation finishes the existing verified publication's pending retention cleanup before replacing the entry point. An ordinary resume retains the effective approved deadline and subtracts previously accounted requests/downloads. Only the explicitly invoked one-time `--extend-session` flow grants a new 48-hour window; cumulative resource ceilings remain unchanged. Expired or uncertain acquisition accounting does not authorize more networking. Review the external report and use its exact recovery command. Existing catalogue commands do not update `production.duckdb`; explicitly use `--activate-current` after a later manual publication. Stop the supervisor with SIGTERM or Ctrl-C so it can terminate and reap its workers; do not use SIGKILL for an orderly stop. See the [production contract](docs/contracts.md#production-database-entry-point) and [acceptance guidance](docs/performance.md#supervised-production-acceptance).

## Complete ordinary-series ingestion

Use a new dedicated root for one ordinary series, such as `KXCPI`:

```sh
uv run kalshi-events --root ./data/series/KXCPI series refresh --series-ticker KXCPI --full
uv run kalshi-events --root ./data/series/KXCPI series refresh
uv run kalshi-events --root ./data/series/KXCPI series refresh --capture-id CAPTURE_ID
uv run kalshi-events --root ./data/series/KXCPI series current
uv run kalshi-events --root ./data/series/KXCPI series replay --capture-id CAPTURE_ID
uv run kalshi-events --root ./data/series/KXCPI series rebuild
uv run kalshi-events verify --release ./data/series/KXCPI/releases/RELEASE_ID
```

The first command requires a series ticker and explicit `--full`. The root then fixes that scope permanently; later commands derive it and reject a conflicting ticker. New series roots use storage version 5 with the exact immutable scope descriptor and cannot convert populated catalogue roots. Catalogue and metadata acquisition/export commands reject series roots; `status` and `discard --capture-id ID` remain available.

Every series refresh follows all pages of the series definition, ordinary events, live-tier markets and historical markets, without status or metadata-update filters. It excludes combos and does not acquire candles, trades or order books. Authoritative event definitions establish market membership; unresolved or contradictory parents prevent publication. Earlier cursor generations remain history, while only the final completed generation determines membership.

Refresh copies the certified predecessor warehouse, appends new observations idempotently and reconciles every membership scope. Departed entities leave current output while their observations remain in protected ancestry. Replaying a receipt adds no duplicate; fetching an unchanged object again records its new observation time and provenance. Prices, volume and open interest are observed values, not a trading time series. After eight refresh captures, run `series refresh --full` explicitly to rebase. No automatic rebase occurs.

Series publication uses `oddsfox.kalshi.series.v1` and `current-series.json`, with `coverage_scope: "series"`, the selected `series_ticker` and `include_multivariate: false`. It reuses the internal v2 warehouse machinery without creating catalogue v3 or changing either catalogue pointer. There is no catalogue-v1 baseline fallback. Published ancestry and incomplete captures remain pinned; successful publication after an explicit rebase can prune obsolete unreferenced evidence.

The resource flags, compression choices, global `--metrics-file`, refresh JSON stages, nullable accounting/resumability and exit codes match `refresh` above; series results also include `series_scope`. One lock and deadline span acquisition through cleanup. Resume the exact reported ID after a budget stop, including when acquisition completed but publication did not. Completed resume avoids networking and redundant landing, but freshly verifies publication. `series replay` and `series rebuild` use committed evidence without publishing automatically. If interrupted before the initial root marker was installed, preserve the unowned directory and choose a fresh directory; never edit markers or recovery files by hand.

“Complete” certifies traversals of the declared scope, not every historically existing record or an atomic exchange snapshot. Live/historical handoffs can race with pagination; `atomic_snapshot` remains false. See the [series contract and recovery rules](docs/contracts.md#ordinary-series-releases) and [measured acceptance](docs/performance.md#kxcpi-series-acceptance).

## Targeted metadata handoff

Select 1–100 distinct explicit market tickers. New captures query bounded batches of comma-free selected live market tickers; comma-containing tickers use individual live lookups. An omitted ticker receives an authoritative individual live lookup followed by a historical lookup when necessary; list omission alone never proves absence. Eligible comma-free event parents use bounded batches with authoritative individual fallback; series parents and comma-containing parent IDs use individual lookups. A completed selection can be absent or unresolved; its coverage remains visible and no stale record is substituted.

Batches contain at most 100 tickers and 6,144 encoded initial URL bytes, with pagination handled separately. New event-parent lookups use `exclude_markets=true` to avoid downloading unrelated children. Direct market coverage is unchanged; new captures no longer acquire those incidental child references through parent hydration. Older captures retain their original requests when resumed. Newly batched observations retain the actual list-request provenance and therefore have different receipt identities from individual requests.

```sh
uv run kalshi-events --root ./data/operator metadata refresh \
  --market-ticker KXEXAMPLE-26-YES --output ./data/metadata/metadata-bundle
uv run kalshi-events --root ./data/operator metadata export \
  --market-ticker KXEXAMPLE-26-YES --capture-id CAPTURE_ID \
  --output ./data/metadata/offline-bundle
uv run kalshi-events metadata verify --output ./data/metadata/offline-bundle
```

`oddsfox.kalshi.metadata.v1` contains the object arrays `series.json`, `events.json`, `markets.json`, `coverage.json` and `manifest.json`. The manifest carries `contract`, `source_release_id`, UTC `received_at`, `coverage_scope: "targeted"`, and `files` entries with exact `size` and `sha256`. Its release identity is SHA-256 of the sorted, compact UTF-8 JSON file inventory, without a trailing newline. Native fields are preserved alongside normalized tickers, receipt and observation identities. Market `usable` requires authoritative event and series parents; coverage is `found`, `absent`, `unresolved` or `unusable`. Bundles, including their manifests, are limited to 128 MiB and destinations must be new directories.

Refresh and export print one JSON result with `output`, `manifest_sha256`, `source_release_id`, `requests`, `downloaded_bytes` and `accounting_complete`. A consumer can pass `--max-requests`, `--max-download-bytes`, `--max-response-bytes`, `--max-duration-seconds` and `--max-output-bytes`. Failed acquisition never exports a previous selection. Retained evidence and producer storage remain in the persistent producer root; copied bundles are consumer owned.

## Resources and recovery

Defaults: 25,000 requests, 4 GiB downloaded, 384 MiB per response, four hours, 64 GiB retained root, 8 GiB temporary files and 2 GiB database including WAL. Every invocation accepts the corresponding `--max-…` flags, plus `--max-rate` for the request pace. The DuckDB and dbt build runs with 4 threads and a 4 GB memory limit. Storage counters are updated incrementally, with one full reconcile at the start and one at the end of each invocation. Exhausted storage requires relief before another run.

Atomic state and receipt replacements require room for the complete new temporary file alongside the original. Metadata writes serialize capacity checks, writes and accounting, preventing concurrent workers from spending the same headroom. Newly created evidence, build and publication directories synchronize their parent entries before durable progress; retrying an interrupted directory creation repeats the uncertain parent synchronization.

Known storage limitation: dlt landing measures its workspace and database after `pipeline.run()` returns. It can exceed the selected root, temporary or database allowance before reporting exhaustion. Those settings currently provide post-load detection for this phase, not a hard write quota. Hard enforcement requires a validated storage boundary; file rotation and periodic checks do not establish that guarantee.

```sh
uv run kalshi-events --root ./data/operator status
uv run kalshi-events --root ./data/operator replay
uv run kalshi-events --root ./data/operator rebuild
uv run kalshi-events verify --release ./data/operator/releases/RELEASE_ID
```

`replay` rebuilds the warehouse for the capture named by the current release, from committed raw evidence, without networking. `rebuild` does the same for that current capture and never changes published releases. Use `--catalogue-version 2` for the v2 pointer. Neither command publishes automatically. There is no backup or restore command. To move a publication, copy the operator root while no writer is running, or copy the immutable release directory that `verify` accepts.

New captures commit each page to a checksummed, sequenced journal and snapshot every 100 transitions, at completion and at an orderly budget stop. Each page transition is verified and derived before its durable append, then applied; a failure after append stops further commits until recovery. The scheduler bounds submitted work to eight futures and requeues traversals fairly. Resume verifies the chain and replays durable progress after the snapshot; committed orphan receipts are reused. Only writer-locked resume may remove an unfinished trailing journal record. Committed corruption fails closed. Read-only commands never repair files, and incomplete legacy captures continue using their original checkpoint protocol. Do not edit recovery files by hand. The protocol-5 full-capture index has a separate soft checkpoint schedule; it does not change journal snapshot frequency or durability order.

Build replacement for either catalogue version retains a certified backup until the replacement verifies. After interrupted build promotion, rerun replay or rebuild with the same catalogue version. Delta rebuilds recover their version 2 replacement before allocating another staging warehouse, so a redundant backup does not consume the retry's database allowance. An interrupted publication may leave a verified immutable release before its pointer is promoted; retry publication with the same capture ID. Cleanup records durable ownership and progress before retiring files, and the next writer resumes that plan before new work. Read-only inspection never repairs or deletes files. Preserve the root and investigate a corrupt plan or unknown cleanup artifact rather than deleting recovery files manually.

Tests use synthetic source responses. Loopback HTTP is available only with both `KALSHI_EVENTS_ALLOW_LOOPBACK=1` and an explicit `KALSHI_EVENTS_API_URL=http://127.0.0.1:PORT/trade-api/v2`. No authenticated trading, WebSocket recorder or activated schedule is included. Dagster remains optional and unscheduled; it is not installed for this operator-invoked release.

Run `scripts/verify-fast` during development and `scripts/verify` before completion. Both installed wheels and source checkouts include dbt resources; runtime data, credentials, local Pad state and warehouse files are excluded from distributions.

Default pytest scratch stays under `data/testing` with pytest's normal temporary-directory retention. An unsafe checkout data path fails before tests run; extracted-source tests launched from the checkout use the same boundary.

The supported execution environment is the MacBook with Python 3.12 and POSIX file/directory synchronization. Keep full-run operator roots and their temporary files beneath this checkout’s ignored `data/` directory on the external SSD. New benchmark/probe outputs must also start beneath `data/`; they no longer require an external destination. Existing active external roots move only after their workers stop. Run the canonical verification commands above before a full catalogue pass. The bounded full-catalogue acceptance attempt described below stopped during acquisition; a successful eight-hour acceptance has not completed.

## Offline benchmarking

See [performance measurements](docs/performance.md) for frozen fixtures, paired baseline comparisons, resource counters, standalone verification and the costs that remain after incremental acquisition. Benchmark roots and reports stay outside Git and distributions. Offline results do not establish live catalogue duration or a higher supported public request rate.

## Live acceptance

No exchange-wide catalogue capture has completed. The 2026-10-17 probe and separate KXCPI series acceptance are not exchange-wide acceptance; the latter is recorded in [performance guidance](docs/performance.md#kxcpi-series-acceptance). A full-catalogue run on 2026-10-22 first stopped after about 54 minutes because the ticker validator rejected `KXCITIESWEATHER-25MAR04(NY)(PHIL)`. A continuation recovered four orphaned receipts and made 409 requests before stopping at `GDP-232022 Q4`, whose interior space was then rejected. After adding interior ASCII-space support, a final continuation exercised both identity fixes and stopped at the operation deadline, remaining incomplete. No release was published or independently verified. Capture `db8a7517031e458aa2815355d067c2a4` was resumable when the run stopped, but the original acceptance window has since expired; any future acceptance needs a new bounded session. Resume is never automatic. These attempts do not provide a complete-catalogue runtime estimate. The final external report is at `/Volumes/Mac SSD/oddsfox-data/kalshi-events-costs/acceptance-20261022T143236Z/continuation3/reports/final-acceptance-report.md`.

The first invocation made 15,752 attempts and transferred 3,277,360,442 encoded and 28,674,089,430 decoded bytes. The second made 409 requests and recovered four orphaned receipts. The final continuation's command accounting was complete: 57,613 requests and 14,212,744,364 encoded bytes, with 126,266,582,928 decoded bytes. Cumulative accounting (audited prior consumption plus the final invocation) is 73,774 requests and 17,563,176,845 encoded bytes; cumulative HTTP decoded metrics are 155,559,467,241 bytes (about 8.86:1). Across the final continuation, no HTTP 429 occurred, and the effective ceiling stayed at 5 requests/second. Events and series traversals completed; multivariate events, live markets and historical markets did not. Entity totals, a build, publication, standalone verification and a reliable end-to-end runtime are unavailable.

An earlier probe ran for 20.1 minutes and stored 6.90 GiB (7,409,637,631 bytes) across 7,802 pages. Series finished at 15,187. The other traversals were still returning full pages: 484,600 events, 590,400 multivariate events, 903,000 live markets, and 761,000 historical markets. Sharing the configured 10 requests per second, those four moved at about 42, 34, 22, and 26 minutes per extra million. Disk use was about 21 GiB per hour. The cursors do not report a remainder, so this probe has no end time.

One page exceeded the old 16 MiB response default. The probe sealed a 256 MiB ceiling and kept receiving pages. The default is now 384 MiB, 1.5 times that ceiling. `max_root_bytes` stays 64 GiB because 1.5 times the 6.90 GiB probe is 10.4 GiB. A finished capture can pass 64 GiB; this probe never reached the end.

A targeted refresh of `KXNHLGOAL-26OCT17DALCHI-CHIRGREENE20-2` returned 4 requests and a bundle. `kalshi-market-data run` accepted that producer for candles from 2026-10-17T00:00:00Z through 2026-10-18T00:00:00Z. The consumer run completed with release `e2777881be74a464b301f04894a7574154a90f4f119b5f17fee490815f851e34`.

## Sources and attribution

Native endpoint, cursor and immutable evidence patterns were adapted from `oddsfox-pipeline`'s Kalshi client and `polymarket-events`' bounded catalogue workflow. Neither donor is imported at runtime or altered by this extraction. Provider contracts: [markets](https://docs.kalshi.com/api-reference/market/get-markets), [multivariate events](https://docs.kalshi.com/api-reference/events/get-multivariate-events), [series](https://docs.kalshi.com/api-reference/market/get-series-list), [historical tiers and cutoff](https://docs.kalshi.com/getting_started/historical_data).

After `scripts/verify` builds distributions, run `uv run --locked python scripts/check-distributions.py` and `uv run --locked python scripts/check-installed.py`. GitHub Actions CI is disabled; run these verification commands manually. The installed check creates a separate locked environment, installs the checksummed wheel, verifies its site-packages import origin and parses its packaged dbt project from a temporary working directory.

The consumer repository owns the full installed handoff harness. Given two separate installed wheel environments, run:

```sh
python /absolute/kalshi-market-data/scripts/check-handoff.py \
  --events-executable /absolute/events-venv/bin/kalshi-events \
  --market-data-executable /absolute/data-venv/bin/kalshi-market-data \
  --python /absolute/data-venv/bin/python \
  --report ./data/verification/acceptance.json
```

This harness uses a synthetic loopback exchange, proves capture through publication and metadata transfer, exercises all market-data feeds and intervals, checks persistent refresh/replay/recovery, and performs no live acquisition.

For acquisition protocol 3, its synthetic exchange must honor `/markets?tickers=...` and singular-event `exclude_markets=true`. Older consumer fixtures ignore these filters and return unrequested markets, which the producer correctly rejects. The installed handoff for this change passed with a private fixture copy implementing those two filters; the consumer repository and installed applications were unchanged.


### Compact saved-evidence historical builds

Fresh historical builds use native Arrow-to-DuckDB inserts instead of dlt merge staging. They retain the 13 logical landing columns, exact payload text and every distinct observation; other workflows retain their existing dlt loader. Payloads use native DuckDB Zstandard compression and are stored once. Historical history/current relations expose compatible views over canonical data and narrow winner selections. The resulting database remains self-contained for ordinary DuckDB 2 read-only consumers.

New historical Parquet exports use Zstandard level 6. Their schemas, values and deterministic row order are preserved; compressed file checksums and release IDs change. Existing releases and other catalogue/metadata/series export settings remain unchanged. Raw receipt bytes are never repacked.

The production runner supports `--reconcile-stopped-worker` only for an exact historical capture. Use it to validate a stopped worker's absence under the supervisor and writer locks before clearing a stale process marker; never edit session fields by hand. A live, reused or ambiguous process identity blocks recovery. Primary stop reasons and uncertain shutdown outcomes remain in history.

The default historical completion profile keeps 64 GiB database and temporary ceilings with 56 GiB cooperative stop thresholds. An explicit `--historical-resource-profile expanded` transition for a stopped historical capture raises those limits to 128 GiB database and 192 GiB temporary. Its report records 96/144 GiB forecast-admission thresholds; runtime baseline checks and stop thresholds are 112/168 GiB. The selected profile and prior limits are retained in append-only session history. Active measurements include database, WAL and spill, not just output after a load returns. Sampling and cooperative checks can overshoot; they do not provide filesystem-enforced quotas (WORK-42 remains open). A staged database counts toward both database and temporary usage.

One admitted offline attempt must preserve all cumulative acquisition accounting, use only saved evidence, and retain the 400 GiB session, 350 GiB root, 150 GiB free-space and existing memory safeguards. Reserve the final two hours of a 24-hour attempt for independent verification, activation and reporting. If the capacity forecast fails, do not launch or silently raise limits. Partial extraction is not a production publication.


After final source verification and a passing capacity admission, reconcile an explicitly authorized stopped historical attempt first:

```sh
python scripts/run-production.py \
  --directory ./data/kalshi-production \
  --historical --capture-id CAPTURE_ID \
  --reconcile-stopped-worker
```

Reconciliation starts no worker and grants no processing time. After it succeeds, start the separately authorized bounded resume:

```sh
python scripts/run-production.py \
  --directory ./data/kalshi-production \
  --historical --capture-id CAPTURE_ID \
  --supersede-offline-window \
  --offline-duration-seconds 86280
```

This grants at most 23 hours 58 minutes inside an outer 24-hour session, preserving startup/reporting headroom. It never resets acquisition consumption or automatically retries. The October 30 compression experiment did **not** pass capacity admission; do not run this command for that capture until the unresolved capacity gate is satisfied.
