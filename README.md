# kalshi-events

Independent, operator-invoked catalogue of public Kalshi event contracts. This repository owns native series, ordinary and multivariate events, live and historical markets, rules, price grids, lifecycle and settlement definitions, relationships and observation history. `kalshi-market-data` owns prices, candles, trades and books. The two applications exchange copied immutable files; they do not share a warehouse or import each other's packages.

See [native contracts and proposed storage/publication extensions](docs/contracts.md) for compatibility, recovery and incremental freshness boundaries. Proposed interfaces are marked there until their implementation lands.

Python 3.12 and uv are required:

```sh
uv sync --locked
uv run kalshi-events --root /absolute/operator/kalshi-events capture
# Exit 0 with "complete": true means the catalogue capture is ready. Exit 3 with
# "complete": false means the invocation hit a budget; repeat with the reported capture ID.
uv run kalshi-events --root /absolute/operator/kalshi-events capture --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events build --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events publish --capture-id CAPTURE_ID
uv run kalshi-events --root /absolute/operator/kalshi-events current
```

The operator root is the persistent directory passed with `--root`, or `KALSHI_EVENTS_ROOT`, or `data`. It holds raw evidence, captures, builds, releases and `current.json`. Only one writer may hold the canonical root at a time. Symlinked roots, files and destinations are refused.

## Capture

The public transport uses only `https://external-api.kalshi.com/trade-api/v2`. Requests are paced by one shared limiter at 10 requests per second by default. Set `--max-rate` to a positive number to change that rate; zero or negative values are rejected. All five traversals (series, ordinary events, multivariate events, live markets and historical markets) run concurrently under that one limiter. Events and multivariate events use pages of 200; live and historical markets use pages of 1000.

Each attempt writes its raw HTTP response bytes and per-attempt accounting before its page is applied. A request that fails with 429, 500, 502, 503 or 504 is retried up to four attempts in total, with equal-jitter exponential backoff capped at 30 seconds. A `Retry-After` header, as delta-seconds or an HTTP date, is always honored and recorded on the attempt receipt. Resume recovers verified completed receipts without reissuing their requests.

Capture stops at its request, download, response, duration or storage allowance and reports `complete: false` with exit code 3. Completed raw evidence is kept, and the capture resumes from there. Invalidated cursors restart their traversal. Each historical read is preceded by a cutoff read that is recorded as its own receipt. Missing parents are retained as unresolved relationships and are never derived from ticker prefixes.

Every capture seals its response ceiling at creation, so later reads with different flags still verify the bytes it recorded.

## Build and publish

Builds are latest-only. `build --capture-id` lands a fresh warehouse in staging from that one complete catalogue capture, runs packaged dbt models and tests in DuckDB, and certifies the raw inventory, database and dbt artifacts before renaming the staged build into place. Without `--capture-id`, the newest complete non-targeted catalogue capture is used. Targeted metadata captures never enter the catalogue warehouse. A failed build leaves prior builds and releases untouched.

`publish` promotes only a certified, complete catalogue build. It writes the immutable `oddsfox.kalshi.catalogue.v1` release and atomically promotes `current.json`. After publication and verification both succeed, older catalogue captures, their raw evidence, their builds and superseded releases are pruned. The current release, its capture and its build always remain. A failed publish or verification prunes nothing.

Native payloads remain in `payload_json`. Commonly queried prices and fractional quantities are exact `DECIMAL(38,18)` columns. Current rows are the latest observed native entities, ordered deterministically by receipt time and observation identity. Provider updates do not establish historical rule applicability.

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

Refresh and export print one JSON result with `output`, `manifest_sha256`, `source_release_id`, `requests`, `downloaded_bytes` and `accounting_complete`. A consumer can pass `--max-requests`, `--max-download-bytes`, `--max-response-bytes`, `--max-duration-seconds` and `--max-output-bytes`. Failed acquisition never exports a previous selection. Retained evidence and producer storage remain in the persistent producer root; copied bundles are consumer owned.

## Resources and recovery

Defaults: 25,000 requests, 4 GiB downloaded, 384 MiB per response, four hours, 64 GiB retained root, 8 GiB temporary files and 2 GiB database including WAL. Every invocation accepts the corresponding `--max-…` flags, plus `--max-rate` for the request pace. The DuckDB and dbt build runs with 4 threads and a 4 GB memory limit. Storage counters are updated incrementally, with one full reconcile at the start and one at the end of each invocation. Exhausted storage requires relief before another run.

```sh
uv run kalshi-events --root /absolute/operator/kalshi-events status
uv run kalshi-events --root /absolute/operator/kalshi-events replay
uv run kalshi-events --root /absolute/operator/kalshi-events rebuild
uv run kalshi-events verify --release /absolute/operator/kalshi-events/releases/RELEASE_ID
```

`replay` rebuilds the warehouse for the capture named by the current release, from committed raw evidence, without networking. `rebuild` does the same for that current capture and never changes published releases. Neither command publishes automatically. There is no backup or restore command. To move a publication, copy the operator root while no writer is running, or copy the immutable release directory that `verify` accepts.

Tests use synthetic source responses. Loopback HTTP is available only with both `KALSHI_EVENTS_ALLOW_LOOPBACK=1` and an explicit `KALSHI_EVENTS_API_URL=http://127.0.0.1:PORT/trade-api/v2`. No authenticated trading, WebSocket recorder or activated schedule is included. Dagster remains optional and unscheduled; it is not installed for this operator-invoked release.

Run `scripts/verify-fast` during development and `scripts/verify` before completion. Both installed wheels and source checkouts include dbt resources; runtime data, credentials, local Pad state and warehouse files are excluded from distributions.

## Live acceptance

No capture has completed. The 2026-10-17 probe is not acceptance.

A new capture ran for 20.1 minutes and stored 6.90 GiB (7,409,637,631 bytes) across 7,802 pages. Series finished at 15,187. The other traversals were still returning full pages: 484,600 events, 590,400 multivariate events, 903,000 live markets, and 761,000 historical markets. Sharing 10 requests per second, those four moved at about 42, 34, 22, and 26 minutes per extra million. Disk use was about 21 GiB per hour. The cursors do not report a remainder, so this probe has no end time.

One page exceeded the old 16 MiB response default. The probe sealed a 256 MiB ceiling and kept receiving pages. The default is now 384 MiB, 1.5 times that ceiling. `max_root_bytes` stays 64 GiB because 1.5 times the 6.90 GiB probe is 10.4 GiB. A finished capture can pass 64 GiB; this probe never reached the end.

A targeted refresh of `KXNHLGOAL-26OCT17DALCHI-CHIRGREENE20-2` returned 4 requests and a bundle. `kalshi-market-data run` accepted that producer for candles from 2026-10-17T00:00:00Z through 2026-10-18T00:00:00Z. The consumer run completed with release `e2777881be74a464b301f04894a7574154a90f4f119b5f17fee490815f851e34`.

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
