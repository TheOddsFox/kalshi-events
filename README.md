# kalshi-events

Independent, operator-invoked catalogue of public Kalshi event contracts. This repository owns native series, ordinary and multivariate events, live and historical markets, rules, price grids, lifecycle and settlement definitions, relationships and observation history. `kalshi-market-data` owns prices, candles, trades and books. The two applications exchange copied immutable files; they do not share a warehouse or import each other's packages.

Python 3.12 and uv are required:

```sh
uv sync --locked
uv run kalshi-events --root /absolute/operator/kalshi-events capture
# If complete=false, repeat with the reported capture ID to resume bounded work.
uv run kalshi-events --root /absolute/operator/kalshi-events capture --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events build --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events publish
uv run kalshi-events --root /absolute/operator/kalshi-events current
```

Capture writes immutable raw HTTP response bytes and per-attempt accounting before committing each cursor checkpoint. Request identity is durable before networking; resume recovers verified completed receipts even when a crash precedes their cursor checkpoint. Failed partial reads stay in attempt accounting and can be retried. The current invocation stops at its request, download, response, duration or storage allowance. Complete the requested traversal before building or publishing. Invalidated cursors restart their traversal; already committed evidence remains available. Ordinary events, multivariate events, live markets and historical markets are traversed independently. Missing parents are retained as unresolved relationships, and never derived from ticker prefixes.

`build` lands verified observations with dlt, runs packaged dbt models and tests in DuckDB, then certifies the raw inventory, database and dbt artifacts. `publish` verifies that certification and writes the immutable `oddsfox.kalshi.catalogue.v1` release before atomically promoting `current.json`. An incomplete capture or failed build does not promote a publication. Native payloads remain in `payload_json`; commonly queried prices and fractional quantities are exact `DECIMAL(38,18)` columns. Current rows are the latest observed native entities, ordered deterministically by receipt time and observation identity; observation history retains every capture. Provider updates do not establish historical rule applicability.

The release contains `series.parquet`, `events.parquet`, `markets.parquet`, `relationships.parquet`, `combo_legs.parquet`, `observation_history.parquet`, `coverage.parquet` and a checksummed manifest. Coverage records the observed traversal interval and cutoff evidence. These endpoints do not provide an atomic exchange snapshot. Markets moving between live and historical tiers can change during the observation interval; the collector reads live before historical and retains overlap observations.

## Targeted metadata handoff

Select 1–100 distinct explicit market tickers. Direct lookups check the historical endpoint when a live lookup is absent, then fetch the native authoritative event and series. A completed selection can be absent or unresolved; its coverage remains visible and no stale record is substituted.

```sh
uv run kalshi-events --root /absolute/operator/kalshi-events metadata refresh \
  --market-ticker KXEXAMPLE-26-YES --output /absolute/new/metadata-bundle
uv run kalshi-events --root /absolute/operator/kalshi-events metadata export \
  --market-ticker KXEXAMPLE-26-YES --capture-id CAPTURE_ID \
  --output /absolute/new/offline-bundle
uv run kalshi-events metadata verify --output /absolute/new/offline-bundle
```

`oddsfox.kalshi.metadata.v1` contains the object arrays `series.json`, `events.json`, `markets.json`, `coverage.json` and `manifest.json`. The manifest carries `contract`, `source_release_id`, UTC `received_at`, `coverage_scope: "targeted"`, and `files` entries with exact `size` and `sha256`. Its release identity is SHA-256 of the sorted, compact UTF-8 JSON file inventory, without a trailing newline. Native fields are preserved alongside normalized tickers, receipt and observation identities. Market `usable` requires authoritative event and series parents; coverage is `found`, `absent`, `unresolved` or `unusable`. Bundles, including their manifests, are limited to 128 MiB and destinations must be new directories.

Refresh/export print one JSON result with `output`, `manifest_sha256`, `source_release_id`, `requests`, `downloaded_bytes` and `accounting_complete`. A consumer can pass `--max-requests`, `--max-download-bytes`, `--max-response-bytes`, `--max-duration-seconds` and `--max-output-bytes`. Failed acquisition never exports a previous selection. Retained evidence and producer storage remain in the persistent producer root; copied bundles are consumer owned.

## Resources and recovery

All mutations take one OS lock on the canonical root. Symlinked roots, files and destinations are refused. The public transport uses only `https://external-api.kalshi.com/trade-api/v2`, with one worker, two attempts per second, bounded streaming, four counted attempts at most per request, explicit deadlines, no redirects, credentials or environment proxies. Each attempt reserves the smaller of its response ceiling and the remaining download allowance before networking; incomplete attempt receipts retain conservative byte reservations. Rejected streams account for every received chunk and cannot publish a page.

Defaults: 25,000 attempts, 4 GiB downloaded, 16 MiB per response, four hours, 64 GiB retained root, 8 GiB temporary files and 2 GiB database including WAL. Every invocation accepts the corresponding `--max-…` flags. Storage is checked before writes and after third-party warehouse work; exhausted roots require storage relief before another run. Raw evidence is retained, with no automatic destructive pruning.

```sh
uv run kalshi-events --root /absolute/operator/kalshi-events status
uv run kalshi-events --root /absolute/operator/kalshi-events replay --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events backup --output /absolute/new/backup
uv run kalshi-events --root /absolute/new/restored-root restore --backup /absolute/new/backup
uv run kalshi-events --root /absolute/new/restored-root rebuild
uv run kalshi-events verify --release /absolute/operator/kalshi-events/releases/RELEASE_ID
```

Replay re-loads committed raw evidence idempotently and certifies the warehouse without networking. Backup verifies complete captures and the current publication, creates a checksummed disjoint snapshot, and restore only accepts a new root. Rebuild regenerates the warehouse from complete raw captures in staging, retaining the current release pointer on failure. Build/replay do not publish automatically.

Tests use synthetic source responses. Loopback HTTP is available only with both `KALSHI_EVENTS_ALLOW_LOOPBACK=1` and an explicit `KALSHI_EVENTS_API_URL=http://127.0.0.1:PORT/trade-api/v2`. No live acquisition, authenticated trading, WebSocket recorder or activated schedule is included. Dagster remains optional and unscheduled; it is not installed for this operator-invoked first release.

Run `scripts/verify-fast` during development and `scripts/verify` before completion. Both installed wheels and source checkouts include dbt resources; runtime data, credentials, local Pad state and warehouse files are excluded from distributions.

## Sources and attribution

Native endpoint, cursor and immutable evidence patterns were adapted from `oddsfox-pipeline`'s Kalshi client and `polymarket-events`' bounded catalogue workflow. Neither donor is imported at runtime or altered by this extraction. Provider contracts: [markets](https://docs.kalshi.com/api-reference/market/get-markets), [multivariate events](https://docs.kalshi.com/api-reference/events/get-multivariate-events), [series](https://docs.kalshi.com/api-reference/market/get-series-list), [historical tiers and cutoff](https://docs.kalshi.com/getting_started/historical_data).

After `scripts/verify` builds distributions, run `uv run --locked python scripts/check-distributions.py` and `uv run --locked python scripts/check-installed.py`. CI runs both checks. The installed check creates a separate locked environment, installs the checksummed wheel, verifies its site-packages import origin and parses its packaged dbt project from a temporary working directory.

The consumer repository owns the full installed handoff harness. Given two separate installed wheel environments, run:

```sh
python /absolute/kalshi-market-data/scripts/check-handoff.py \
  --events-executable /absolute/events-venv/bin/kalshi-events \
  --market-data-executable /absolute/data-venv/bin/kalshi-market-data \
  --python /absolute/data-venv/bin/python \
  --report /absolute/new/acceptance.json
```

This harness uses a synthetic loopback exchange, proves capture through publication and metadata transfer, exercises all market-data feeds and intervals, checks persistent refresh/replay/recovery, and performs no live acquisition.
