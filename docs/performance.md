# Offline performance measurements

The benchmark generates deterministic public-API-shaped responses without networking. It runs capture, build and publication with the same fixture, interpreter, dependencies, limits and filesystem for each revision. It checks output equivalence before accepting a timing result.

The frozen full fixture contains 197 markets and three parent observations. The scale fixture contains 249,997 markets and three parents, including 249 combo markets. Both use 200-market response pages, exact decimal strings, native combo legs, repeated parents, rules and nested price ranges. This synthetic pagination is a benchmark input, not an estimate of exchange page counts or live runtime. A separate 1,203-market test exercises seven pages and a combo leg without changing the frozen fixtures.

The original unfiltered mock market traversal rebuilt the complete market-index list on every page. Its quadratic fixture work can dominate large capture timings. The current harness slices a lazy range and preserves the same pages and payloads. Historical results below describe their recorded harness bytes; this correction does not demonstrate a production-pipeline speedup. Record the harness revision when comparing runs.

## Reproduce a comparison

Use one locked environment and two immutable source archives. Keep both archives and all generated roots/reports on the same filesystem, outside Git. Do not run another benchmark or test suite concurrently.

Run both source archives on the MacBook using the same interpreter, locked dependencies and external SSD. Preserve file and directory synchronization inside the measured work.

```sh
uv sync --locked
bench_dir=$(mktemp -d /tmp/kalshi-events-benchmark-XXXXXX)
mkdir "$bench_dir/baseline" "$bench_dir/candidate"
git archive 21edf14a486addfa743c524a645857ed711b640f | tar -x -C "$bench_dir/baseline"
git archive HEAD | tar -x -C "$bench_dir/candidate"
uv run --locked python scripts/benchmark-pipeline.py \
  --source-root "$bench_dir/baseline" \
  --compare-root "$bench_dir/candidate" \
  --source-revision 21edf14a486addfa743c524a645857ed711b640f \
  --compare-revision "$(git rev-parse HEAD)" \
  --profile full --warmups 1 --runs 3 \
  --report-dir "$bench_dir/full-small"
```

Repeat with a new report directory and `--scale` for approximately 250,000 observations. Use `--profile targeted` for metadata acquisition/export and `--profile resume` for an orderly request-budget stop followed by recovery. Each profile requires a new report directory. `--sample-storage` adds an untimed run that samples actual on-disk usage; its observed peaks are lower bounds between samples.

Benchmark workers use a 180-second deadline budget, reserving up to five seconds for termination and reaping. A timed-out worker does not produce an accepted timing result. If descendants cannot be reaped, the benchmark reports failure and preserves their disposable root.

Existing full, targeted and resume profiles retain their original request protocol and frozen receipt identities. They compare the same evidence through the dictionary and Arrow loaders. Existing composed-refresh profiles retain their workload definitions and request protocol; the mock receipt-ID correction described below applies to that family. `--profile selected-stress` instead compares changed acquisition requests: 100 ordered selections share parents, with live, historical, absent and unresolved cases, a maximum-length ticker, and a parent containing 2,000 unrelated child markets. Native values and coverage must agree; each side's actual receipt provenance is independently validated. Only incidental child arrays in parent-event payloads are excluded from that native comparison. Receipt and release identities are expected to differ.

The `--fixture combo-heavy` variant is a separate combo-heavy payload workload for `full`, `no-change`, `sparse-change` and `all-change`. To prepare same-receipt v2 build input, create a fresh storage-version-5 capture-only root outside the checkout with `--fixture combo-heavy --profile full --prepare-frozen-capture PATH`; record its capture ID and preserve the root. Then run `--profile frozen-v2-build --frozen-root PATH --capture-id ID` against two implementations that both support storage version 5. The seed is built and published directly as v2; it does not require a v1 publication first. The frozen-v2 profile deliberately rejects pre-v5 runtimes, so use the `ddc19b2` storage-v5-compatible reference for the same-receipt comparison. Use `ed133d9` for changed-acquisition/full-pipeline comparisons, not to consume this frozen-v2 seed. Compare verified public artifacts and release identities; the physical DuckDB file hash is not a public identity and is not an equality requirement.

Example preparation and comparison commands (replace paths and the capture ID with the generated values):

```sh
uv run --locked python scripts/benchmark-pipeline.py \
  --fixture combo-heavy --profile full \
  --prepare-frozen-capture "$bench_dir/frozen-v2-root" \
  --report-dir "$bench_dir/prepare-frozen-v2"

uv run --locked python scripts/benchmark-pipeline.py \
  --source-root "$bench_dir/ddc19b2" --compare-root "$PWD" \
  --source-revision ddc19b2 --compare-revision protocol5 \
  --profile frozen-v2-build --frozen-root "$bench_dir/frozen-v2-root" \
  --capture-id CAPTURE_ID --warmups 1 --runs 3 \
  --report-dir "$bench_dir/frozen-v2-comparison"
```

Use `--profile selected-single` for one present selection and `--profile selected-fallback` for a historical plus an absent selection. These exercise the new acquisition protocol, unlike the frozen `targeted` profile. Reports separate singular fallback calls from the initial batch request.

`--profile history-sparse` retains at least 250,000 observations over 10,000 market identities, then refreshes approximately 1% of keys. Repeated baseline pagination is intentional history stress, not an estimate of exchange behavior. Complete reconciliation membership stays stable. Both revisions run an incremental refresh from clones of the same certified reference baseline; the candidate clone's explicit root upgrade is measured outside refresh timing. The benchmark checks the actual affected-market table, carried timestamps and provenance, retained history and public output bytes. `--profile baseline-v2-setup` separately measures direct v2-only bootstrap; it does not replace the existing setup profile that publishes both versions.

Refresh comparisons use `--profile no-change`, `sparse-change` or `all-change`. Each revision prepares one T0 baseline, freezes its root identity and physical database bytes, then clones that seed for every warm-up and measured run. Each run measures T1 acquisition/build/publication in a fresh process. The reference performs a full v1 refresh; the candidate performs a composed v2 refresh. Reports separate the shared baseline setup and per-run clone costs. Both sides use the same 4 GiB download allowance, sufficient for the extra concurrent reconciliation traversal. Frozen full, targeted and resume profiles retain their original 2 GiB allowance. The fixture rate is explicitly 1,000 requests/second on both sides; it is not a live rate recommendation.

Trading values stay fixed in refresh fixtures. Current relation comparison excludes only source observation/capture identifiers and observation time; separate assertions check carried T0 and refreshed T1 provenance, exact baseline history retention and expected new observations. Composed history intentionally contains more observations than a fresh full release. These comparisons do not establish universal change-feed completeness or current trading-value equivalence.

## Interpret reports

`pipeline_elapsed_seconds` measures native capture/build/publication after imports, excluding separately prepared baseline setup, output comparison and standalone verification. `worker_wall_seconds` includes process startup and post-run checks. Phase timings are inclusive and can overlap; do not add them. Compare medians of at least three paired runs after one warm-up, and inspect individual pairs for variance.

`dlt_phase_seconds` records dlt's own extract, normalize and load trace spans after the existing `pipeline.run()` call. Those spans are nested within landing and are not additional pipeline time. Observation production, evidence checks, synchronization and storage checks remain part of the landing measurement. The observer does not change production configuration or execution.

Reports include fixture/revision identities, package versions, interpreter, filesystem device, limits, requests, bytes, reads, JSON decodes, hashing, synchronization, Store-accounted peaks and separate parent/child peak RSS. Protocol-5 reports also include submitted-future peaks and counts, task enqueues, and prepared/applied page transitions. These mechanism counters are not duration estimates. Hash counters cover instrumented `digest`/`file_digest` helpers, including normalization and metadata aliases, plus explicitly instrumented transport and journal hashes. Uninstrumented lineage and membership hashes and third-party internals are outside those totals; available hashing phases are reported separately. Instrumentation is enabled on both revisions, so timings include recorder overhead. Store-accounted retained bytes include all files under the operator root; temporary bytes are a subset. Child RSS is the operating system's child-process high-water measurement, not simultaneous combined memory. Synchronization counters cover instrumented Python calls (inferred for archived code), including attempted failed calls in updated code; DuckDB/C++ internal synchronization is outside those counters. Unavailable measurements remain unavailable; zero is a measurement only when the report explicitly records it.

Reports record the POSIX file/directory synchronization requirement and SHA-256 inventories of actual Python/dbt source files. JSON metadata is read as UTF-8.

The benchmark JSON total covers first-party in-process parsing through `common.decode` and the warehouse's direct dbt-artifact `json.loads` calls. The artifact counter is a subset of that total; do not add it again. Third-party and subprocess JSON parsing is outside this counter. Raw-body parses and other metadata/journal/artifact parses are reported separately. `metadata_scan` and `metadata_scans` measure storage inventory walks/reconciliations, separately from JSON parsing. Standalone capture/release or metadata verification has its own measurements and is excluded from pipeline counters. Archived code lacks some native phase hooks; unavailable phases are marked explicitly instead of reported as zero-cost work. Identity fixtures do not measure gzip decompression performance.

Full v1 output checks compare Parquet bytes, release identity and deterministic manifest/pointer identities. Composed runs also require stable Parquet/release identities from the frozen baseline; their manifest and pointer bytes may vary with the certified physical warehouse hash hint. Targeted checks compare the complete metadata bundle and independently verify it. Mutation between commands must still fail fresh evidence verification; operation-local reuse is not a persistent trust cache.

## Acquisition diagnostics and coordinated refresh

The additive `refresh` workflow uses existing capture and v2 publication routines. It removes the need to manually coordinate stages and avoids creating a redundant v1 baseline build. Default invocation uses incremental acquisition only after a verified baseline exists; `refresh --full` explicitly establishes or rebases that baseline. This workflow does not establish a faster initial full crawl. Targeted `metadata refresh` remains preferable when only selected market definitions are required.

Opt-in metrics retain their existing version and fields and add bounded endpoint-family aggregates. Singular ticker paths become placeholders; reports contain no unbounded request traces or native payloads. Acquisition counters distinguish attempts and HTTP statuses, requested/observed encoding, bytes, committed pages and recovered pages. Latency aggregates retain count, total and maximum rather than individual request records.

Header and response-iterator waits measure time observed by the client, including connection and scheduling effects. Iterator advancement is timed separately from hashing, decompression and spool writing. The existing inclusive request phase remains available. Additional phases separate rate/retry waits, receipt finalization, journal appends, snapshot replacements, validation/application, scheduler waits, spool writes and file/directory synchronization. Main-thread page-processing CPU uses a thread CPU clock; peak concurrent requests is reported separately. Failed operations remain counted and timed. Synchronization timings cover instrumented Python operations, not every native library or database flush.

Worker times overlap and phases nest: neither their sum nor HTTP elapsed time alone proves the critical path or provider processing cost. Metrics stay outside evidence and cannot change raw receipts, release identities or existing command JSON.

The manual `scripts/probe-acquisition.py` runner compares identity and requested gzip serially using fresh roots. Screening and throughput worker commands explicitly use fixed pacing; only explicitly supplied rates are tried, and the default is 5 requests/second. Fixed per-sample and aggregate budgets, preserved partial reports and uncertain-accounting stops keep this diagnostic separate from production operation. See README for exact limits. Live source content can differ between samples, so a lower byte count or shorter sample is not an equal-work speedup. Identity fallback is a valid observed result.

The optional `--profile throughput` extends those observations without changing the production traversal. Its global samples still execute the ordinary capture CLI, including cutoff, journals, parent hydration and synchronization. Explicit event selectors append serial/parallel diagnostic samples within the same invocation. Those samples omit production capture journals, hydration, warehouses and publication; compare serial versus parallel only within that diagnostic engine. Their time cannot be credited as an end-to-end production speedup.

Throughput reports separate validated HTTP-200 data pages from cutoff/control/error responses and distinguish diagnostic verified pages from journal-committed production pages. They include requested/observed encoding, attempts and page rates, acquisition/subprocess/reporting durations, existing phase timings, synchronization, Store-accounted peaks and RSS. Unknown values remain unavailable. A sample with fewer than 100 validated data pages or ten seconds of acquisition is insufficient for a throughput conclusion. Longer samples still cannot establish sustained public capacity, a safe default rate or global equivalence. Kalshi's [published rate tiers](https://docs.kalshi.com/getting_started/rate_limits) describe authenticated token budgets, not this public client's unauthenticated allowance.

Partitioning can overlap independent cursor chains but cannot divide one hot partition. A retained earlier sample placed its first 2,000 multivariate events in one series and 9,684 of its first 10,000 live markets in one collection. These are prefix observations, not a full distribution census. Complete combo/history reconciliation, parent resolution, full integrity checks and deterministic exports remain production costs. For subsequent catalogue runs, use explicit v2 incremental refreshes and full rebases as described above; this pilot does not change their coverage or freshness semantics.

### 2026-10-22 bounded catalogue experiment

Reference `6b13f70` was archived externally with interpreter, locked dependency versions, machine/filesystem details and source/fixture hashes. Candidate acquisition code was frozen at `6091a61`. Generated evidence is retained outside the checkout under `/Volumes/Mac SSD/oddsfox-data/kalshi-events-throughput/implementation-20261022T073032Z/`; it is not distributed.

The offline comparison used the same diagnostic engine, frozen gzip input, external SSD, 20-request/second ceiling and limits for both worker counts. Each fixture had one warm-up per mode and three alternating measured pairs. All observation multisets matched, including duplicate payloads and corrections. Median seconds were:

| Fixture | One worker | Eight workers |
|---|---:|---:|
| Balanced | 0.389 | 0.386 |
| Many small | 0.379 | 0.381 |
| Empty | 0.169 | 0.167 |
| Skewed | 0.489 | 0.487 |
| Dominant partition | 0.595 | 0.603 |
| Duplicate observations | 0.605 | 0.594 |
| Wide payloads | 0.386 | 0.383 |

These tiny MockTransport fixtures have no artificial network latency and are dominated by pacing. Differences range from about -1.9% to +1.3%; they do not establish a meaningful speedup or a reproducible regression above 10%. The machine was not reserved for these measurements; clean scale comparisons remain open for a quiet MacBook session. Mechanical barrier tests independently prove concurrent requests, one outstanding request per partition and a shared limiter/budget. Full source, per-run phase/counter/RSS/storage reports and fixture identities are in `offline-pairs-final-6091a61/report.json` in the external evidence directory.

A separate balanced-fixture metrics-on/off comparison used one warm-up per mode and three alternating pairs with identical inventories. Median acquisition was 0.385 seconds without metrics and 0.379 seconds with metrics; this small variation does not show measurable overhead or a speedup. Its report is `metrics-overhead-6091a61/report.json` in the same external directory.

The manually invoked live session used the exact approved ordinary scopes `KXMIDTERMVOTETURN`, `KXMIDTERMMOV` and collection `KXMVECROSSCATEGORY-R`, with explicit rates 10, 15 and 20. Preflight successfully bound the collection to series `KXMVECROSSCATEGORY`; that definition was served as gzip (31,531 encoded bytes, 346,041 decoded bytes). The first global identity sample observed HTTP 429 and the supervisor stopped all remaining scenarios. Gzip global samples, partition comparisons and higher rates were therefore **not run**.

The whole session took 37.146 seconds, with 251 attempts including preflight and 222,833,910 encoded bytes. The global sample took 35.789 seconds of acquisition: 250 attempts, 238 HTTP 200 responses, 12 HTTP 429 responses/retries, 218 validated data pages and 20 cutoff pages. It reached 6.99 attempts/second and 6.09 data pages/second, with peak HTTP concurrency five. Every global response used identity encoding. The capture remained incomplete at its request allowance; this was not a complete catalogue or a sustained-capacity measurement.

Global accumulated timings included header waits 36.636 seconds, response-iterator waits 32.659, limiter waits 57.845 and retry waits 9.080; these overlap across workers and cannot be summed or treated as wall time. Spool writes took 0.324 seconds and file synchronization 0.553 seconds; 3,005 total sync calls were recorded. Main-thread page CPU was 1.528 seconds. Reported self peak RSS was 322,732,032 bytes. Aggregate Store-accounted retained storage ended at 223,128,882 bytes, temporary high-water was 20,104,540 bytes and no temporary bytes remained. This sample provides no evidence that SSD bandwidth is the limiting factor, and no basis for increasing the default public rate.

The experiment completed its required throttling stop, preserving partial evidence and verified accounting. Compression and partition throughput comparisons remain inconclusive. No new full-crawl or end-to-end acceleration claim follows from these results. Long canonical suites remain separate manual gates on the MacBook; GitHub CI stays removed.

Clean full/scale performance targets and full recovery verification remain open on the MacBook.

A manually invoked public probe on 2026-10-20 used the new script at the unchanged 10 requests/second with fresh roots on the MacBook's temporary filesystem. Identity and requested-gzip samples made three and four attempts respectively, all HTTP 200, with no retries or throttling observed. Both stopped at in-flight download reservations before their 12-attempt limit; they were partial captures, not complete catalogues. Aggregate recorded downloads were 716,121 bytes. Gzip was actually served on `/events` and `/markets`; cutoff responses used identity. The samples read different endpoint mixes and are not an equal-work compression or throughput benchmark. They do not measure the external SSD/cable or establish a safe higher public rate. Generated roots, receipts, metrics and reports remain outside Git and distributions.

### 2026-10-22 bounded full-runtime investigation

A separate session at `f20f9c13ab4a8054ea22d1283e6413bc7ae570a0` used the MacBook, locked Python 3.12 environment and external SSD. Measurement work ended after 44 minutes; analysis and independent review finished within 55 minutes. Generated roots, logs, scripts and reports remain outside Git and distributions under the operator's `kalshi-events-runtime/session-20261022T091527Z/` directory.

The sustained production capture ran for 887.989 seconds at an explicit five requests/second. It accounted for 4,322 attempts and 5,364,842,001 encoded bytes: 4,321 HTTP 200 responses plus one finalized zero-download `ReadTimeout` at the deadline. Accounting remained complete. No HTTP 429 occurred in this sample. Achieved throughput was 4.867 attempts/second, 97.3% of the configured ceiling. Series completed at 15,336 records; the other traversals remained incomplete after 260,400 ordinary events, 247,000 multivariate events, 754,000 live markets and 514,000 historical markets. These are interval observations, not an atomic population census or an estimate of the unseen remainder.

Short comparisons then used fresh roots:

| Requested rate | Encoding | Validated data pages | Acquisition seconds | Encoded / decoded bytes | HTTP 429 |
|---|---|---:|---:|---:|---:|
| 5/s | Identity | 219 | 51.369 | 356,291,719 / 356,291,719 | 0 |
| 5/s | Requested gzip | 219 | 51.340 | 47,242,465 / 409,187,786 | 0 |
| 10/s | Identity | 210 | 35.298 | 283,685,049 / 283,685,049 | 13 |

The requested-gzip sample served 219 gzip data responses and 31 identity cutoff responses. Its own encoded/decoded totals demonstrate wire compression; different source content and endpoint mixes prevent a controlled comparison with the identity sample. Gzip barely changed throughput at the five-request ceiling. The 10/s sample triggered the required stop; its gzip counterpart was not run. This does not establish a sustained public allowance or change the default rate.

Sustained capture recorded 45.4 seconds of main-thread page CPU, 20.2 seconds of file synchronization, 0.093 seconds of directory synchronization and 848 MiB peak self RSS. Encoded arrival averaged 5.76 MiB/s. Together with near-ceiling request throughput, these support request pacing and HTTP/cursor waits as the primary acquisition constraint, rather than SSD bandwidth. Timers overlap and cannot be added into wall time. Unrelated applications were active; system-wide disk bursts during local processing cannot all be attributed to this pipeline.

The full synthetic benchmark completed one warm-up and three measured runs at each of 10k, 100k and 250k markets, with stable public artifacts. Its original million-market warm-up hit the 180-second cap. Review identified the quadratic whole-index construction described above; that timeout cannot establish production backend cost. The repository was not patched during the measurement session.

To isolate downstream work, four disposable copies of that completed million-market capture ran the existing explicit v1 build, publication and standalone verification. One warm-up and all three measured runs passed with identical public release identities. The measured build-plus-publication paired median was 50.934 seconds; adding standalone verification within each pair gave 55.505 seconds, with a 54.360–68.986-second range. Evidence-copy costs and failed setup attempts were excluded. The synthetic acquisition protocol was frozen at version 2; the published contract was catalogue v1. These numbers do not measure initial acquisition or additional v2 certification work.

The real sample was 91.2% combo markets in the live tier and 97.5% in history, containing 12,165,500 leg observations. The synthetic fixture has about 0.1% combo markets with one leg each. Real combo-leg joins, integrity checks and exports therefore remain an uncalibrated downstream cost; the roughly 51-second synthetic result is not a full-build forecast for the real catalogue.

For illustration only, assuming equal populations in each of the four large traversals, the observed rate/latency model and an unchanged four-way progress model imply these acquisition scenarios:

| Assumed total across four traversals | Population per traversal | Modelled acquisition |
|---|---:|---:|
| 4 million | 1 million | 45–60 minutes |
| 10 million | 2.5 million | 111–150 minutes |
| 25 million | 6.25 million | 278–374 minutes |

These are explicit assumptions, not inferred exchange totals, confidence intervals or upper bounds. The models include historical cutoff requests but exclude unknown parent hydration. At the measured rate, 10,000 additional lookup attempts would consume about 34 minutes of request budget. The incomplete prefix's unresolved parent references do not predict final lookup counts because later pages can resolve them. Full end-to-end runtime remains uncalibrated until population/tail behavior and representative combo-heavy downstream work are measured.

Aggregate live accounting was 5,072 attempts and 6,052,061,234 encoded bytes, below the session's 10,000-attempt/20-GiB ceilings. Final retained test data was about 14.7 GiB. Raw response hashes were independently checked, partial evidence was preserved and measurement processes were reaped. No full catalogue publication, full verification suite, higher persistent rate or production optimization was claimed.

### Small-fixture diagnostic overhead checks

On 2026-10-20, immutable source archives of `8c13274` and `f2c6a61` were compared with the existing frozen benchmark harness, locked dependencies, one warm-up and three alternating measured pairs on the MacBook temporary filesystem. Both revisions used the same fixture, limits and 1,000-request/second synthetic pace. These are small offline observations, not live or scale performance acceptance. Diagnostics were enabled; this measures their added hooks alongside unchanged pipeline work.

| Fixture | Reference median | Candidate median | Observed change |
|---|---:|---:|---:|
| Full, 200 observations | 6.959 s | 7.559 s | +8.6% |
| Frozen targeted metadata, first matrix | 72.226 ms | 92.661 ms | +28.3% |
| Frozen targeted metadata, investigation repeat | 58.558 ms | 59.864 ms | +2.2% |

Every measured pair preserved public artifact bytes and release identities; identities were stable within each revision. Full-mode counts remained seven requests, 74 first-party JSON decodes and 146 instrumented sync calls. Targeted counts remained four requests, 33 decodes and 90 sync calls. No verification or synchronization was omitted.

The first targeted result exceeded the 10% investigation threshold. Repeating the same three-pair protocol without overlapping agent checks did not reproduce that median regression: reference run times were 57.936–58.581 ms and candidate times 56.872–86.152 ms. Capture medians were 46.990 versus 49.010 ms and raw-write medians 7.680 versus 8.442 ms. The first matrix varied more widely, including reference runs from 61.284–95.034 ms. These short timings do not establish the cause of the variation or a significant speedup.

The full-small result is also noisy: one 1.15-second compatibility test overlapped part of that matrix, and dbt medians changed from 6.101 to 6.753 seconds despite unchanged dbt/warehouse implementation. Treat its artifact equivalence as correctness evidence and its timing as inconclusive. No agent tests overlapped either targeted matrix; unrelated host activity was not controlled. Long full/scale comparisons and clean timing targets remain open for a quiet MacBook session.

## KXCPI series acceptance

On 2026-10-20, a supervised MacBook session completed a fresh `KXCPI` baseline, one reconciled refresh and standalone release verification on the external SSD. Python was 3.12.13 on arm64 macOS 27.0.1, with httpx 0.28.1, dlt 1.31.0, DuckDB 1.5.6, PyArrow 26.0.0 and dbt-core 1.12.5. Identity encoding and the default 10 requests/second were retained.

The first baseline attempt stopped before publication: the historical endpoint returned HTTP 400 when `series_ticker` and `mve_filter=exclude` were combined. Its six receipts were all accounting-complete: 140,301 encoded bytes and 151,650 retained bytes. The historical query was corrected to use the series filter alone, retaining strict ordinary-series/combo and authoritative-parent validation. A regression and independent checks passed before continuation. The failed pre-release root and its report remain preserved separately; its sealed earlier task plan is not a recovery input for the final implementation.

Continuation used a fresh root within the **original** 30-minute session, subtracting those attempts, downloads and retained bytes. The supervisor passed remaining allowances into each child and could terminate the process group and reap it within five additional seconds. Limits were 1,000 aggregate attempts, 512 MiB encoded downloads, 32 MiB encoded / 64 MiB decoded per response, 8 GiB aggregate retained-root storage, 4 GiB temporary storage and 2 GiB database storage under existing accounting. No budget was reset. The three successful commands finished at 256.66 seconds from the original session start, including diagnosis, correction and offline checks. A separate retained-ancestry audit finished at 300.34 seconds. No timeout or termination was needed.

The supervisor invoked the active interpreter with `--root ROOT --metrics-file EXTERNAL_FILE series refresh --series-ticker KXCPI --full`, then `series refresh`, then root-independent `verify --release RELEASE_DIRECTORY`, passing the remaining resource flags each time. Roots, metrics, logs and reports are retained outside Git under the operator's `kalshi-events-kxcpi/acceptance-20261020T184727Z` directory. Acquisition totals include the failed attempt: **18 attempts and 2,285,731 encoded bytes**, with confirmed accounting. Each successful capture used six requests and 1,072,715 encoded bytes: one series, one event page, one live-market page, one historical-market page and two cutoff receipts. All successful responses used identity encoding and HTTP 200; no retries or throttling were observed.

| Certified output | Baseline | Refresh |
|---|---:|---:|
| Series | 1 | 1 |
| Ordinary events | 67 | 67 |
| Current markets | 552 | 552 |
| Live-tier membership | 59 | 59 |
| Historical membership | 493 | 493 |
| Observation history | 620 | 1,240 |
| Relationships | 619 | 619 |
| Combo legs | 0 | 0 |

Both releases contain only `KXCPI` events and usable markets. The second capture acquired new observations for the same scope instead of relabeling baseline evidence. Standalone verification accepted refresh release `33e21c1951312b28fac6b3814478e47b47b8292bc57e2b1cb9af5542633baccf`; an additional fresh audit verified its pinned baseline, both captures' six raw pages and the absence of catalogue pointers. The baseline and refresh warehouses remained present and independently certified.

| Measured seconds | Baseline | Refresh |
|---|---:|---:|
| Whole invocation, including child startup | 8.849 | 7.641 |
| Acquisition | 4.183 | 2.830 |
| Landing | 0.317 | 0.302 |
| Current-projection preparation | 0.296 | 0.535 |
| dbt build and integrity checks | 3.377 | 3.090 |
| Complete export | 0.017 | 0.017 |
| Release verification hooks | 0.012 | 0.059 |
| Promotion | 0.004 | 0.003 |
| Pruning | 0.032 | 0.110 |
| Client-observed header waits | 4.727 | 4.319 |
| Response-iterator waits | 3.613 | 1.975 |
| Rate-limiter waits | 1.037 | 1.044 |
| File synchronization | 0.011 | 0.011 |
| Directory synchronization | 0.000264 | 0.000216 |

These phases nest and worker waits overlap; they must not be summed into wall time or labeled server-processing time. Landing, projection preparation and dbt are measured build components, not a separately timed whole-build span. Standalone verification took 0.463 seconds including startup (0.003 seconds in its verification hook). The baseline and refresh recorded 139 and 127 instrumented synchronization calls respectively. Peak HTTP concurrency was five. Store-accounted retained high-water marks were 17,125,554 and 32,468,553 bytes; adding the preserved failed root gives an aggregate observed retained peak of 32,620,203 bytes. Temporary high-water marks were 8,020,580 and 10,901,614 bytes. The two closed databases occupy 7,090,176 and 9,973,760 bytes; these are final sizes, not measured database peaks. The 4 GiB reservation high-water mark is an allowance, not additional disk usage. Self peak RSS was 221,609,984 / 240,336,896 bytes and separate child peak RSS was 276,791,296 / 287,457,280 bytes.

This is one live acceptance sample, not a controlled performance comparison, a durable snapshot of the exchange or an estimate of global crawl duration. Every series refresh still acquires the entire declared scope, exports complete history and runs full verification. Hard dlt quota enforcement remains separate work; full catalogue execution remains an open MacBook gate.

Small offline correctness comparisons against `3f1a946` also passed for the frozen full, targeted, resume and direct-v2-baseline profiles: public Parquet/bundle bytes and release identities matched. Full and resume also matched manifest and pointer bytes; targeted matched the complete bundle including its manifest and creates no publication pointer. Direct-v2 setup compared logical lineage but did not compare manifest/pointer bytes containing the physical warehouse hint. Each profile used one pair without warm-up; **no timing claim** follows from those checks. Independent tests additionally compared scoped incremental relations against a full dbt projection over identical retained observations and membership.

## Current changes: contended measurements against 21edf14

The implementation at `522af66a877fb75a1c61757ec1af0067a83f653e` was compared with `21edf14a486addfa743c524a645857ed711b640f` on 2026-10-18 using Python 3.12.13, dlt 1.31.0, PyArrow 26.0.0, DuckDB 1.5.6 and dbt-core 1.12.5 on arm64 macOS 27.0.1. Each reported comparison uses one warm-up and three alternating measured pairs with the same fixtures, limits, interpreter and filesystem. The runtime inventory SHA-256 is `57279c7f7612bea9690cb1fff7dd34cdbbc656584c89e47c3acff5b447dbaa08`.

This historical matrix has not been repeated for later revisions. Full validation, recovery checks and clean paired comparisons remain open on the MacBook.

A subsequent review fix preserves authoritative absent coverage after a batch cursor reset when retained observations have unavailable parents. The matrix below was not rerun for that fix; its runtime inventory describes `522af66`, not later revisions. Clean timing targets remain open.

**These are contended observations, not validated timing targets.** Unrelated CPU-heavy jobs were active on the host. The operator selected finishing the PR with these results and leaving clean timing targets open. For example, scale landing ranges from 12.53–20.48 s on the reference and 8.74–38.21 s on the candidate. Do not infer a causal speedup or regression from those medians alone.

All 12 profile correctness gates pass across three measured pairs, including identity stability within each revision. The timing table reports medians, not promises. The three small refresh profiles compare a reference full refresh with a candidate composed refresh; only history-sparse compares incremental refresh with incremental refresh from the same baseline.

| Operation | Reference | Candidate | Observed change |
|---|---:|---:|---:|
| Full, 200 observations | 5.782 s | 3.828 s | -33.8% |
| Full, 250,000 observations | 32.259 s | 38.057 s | +18.0% |
| Frozen targeted metadata | 34.280 ms | 35.578 ms | +3.8% |
| Stop and resume, 200 observations | 3.888 s | 4.676 s | +20.3% |
| Mixed 100-selection metadata | 625.702 ms | 96.710 ms | -84.5% |
| One present selection | 62.976 ms | 44.466 ms | -29.4% |
| Historical and absent selections | 78.519 ms | 67.177 ms | -14.4% |
| No-change refresh | 2.416 s | 2.800 s | +15.9% |
| Sparse-change refresh | 2.485 s | 2.835 s | +14.1% |
| All-change refresh | 4.350 s | 4.861 s | +11.7% |
| Direct v2-only baseline setup | 3.008 s | 3.854 s | +28.1% |
| History-heavy sparse incremental refresh | 24.752 s | 23.463 s | -5.2% |

The above-10% increases in scale, resume, small refreshes and direct v2 setup remain unresolved timing results. Phase inspection found increases spread across capture, extraction, projection and dbt rather than a single isolated regression. Scale pair times vary from 31.97–45.46 s on the reference and 33.32–87.59 s on the candidate; unrelated CPU-heavy work was confirmed on the host. This investigation does not establish that contention explains each increase. Rerun the affected profiles on a quiet machine before accepting their performance.

The 100-selection mixed fixture uses 109 total requests on the reference and 12 on the candidate, with identical native definitions and ordered coverage and independently verified actual provenance. Encoded bytes fall from 123,329 to 26,362. A separate regression test proves 100 short present selections use one market-list request instead of 100 individual market requests, excluding unchanged parent/cutoff work; 100 maximum-length selections exercise URL-bounded partitioning. The one-selection fixture uses four requests on both revisions, with 96,481 versus 484 encoded bytes because the parent omits its 2,000 irrelevant child markets.

Historical/missing-only selection uses **one additional batch request**: ten total requests versus nine, including four singular live/historical calls on both sides. List omission still cannot certify absence. Its observed total time must not hide that extra request cost at the normal 10 requests/second limit.

The existing no-change, sparse-change and all-change fixture family now uses valid 32-character raw receipt directory IDs, including its baseline setup. The archived harness's 64-character mock IDs were rejected by `21edf14`'s prune validator. This correction preserves requests, payloads and limits, uses the same deterministic generator on both revisions, and does not bypass pruning. Reports identify the corrected format. Full, targeted and resume frozen identities remain unchanged.

Scale phase medians below are inclusive and overlap. The complete pipeline median is 32.259 s versus 38.057 s, an observed 18.0% increase. Arrow normalization is lower in this run, but extraction and total landing are higher; a quiet rerun is needed to distinguish implementation effects from host contention. Both the 25% multi-selection target and the 20% landing target remain open as clean performance gates.

| Scale measurement | Reference | Candidate |
|---|---:|---:|
| Requests | 1,256 | 1,256 |
| Encoded / decoded bytes | 94,555,150 / 94,555,150 | 94,555,150 / 94,555,150 |
| Raw body reads / JSON decodes | 5,024 / 2,514 | 5,024 / 2,514 |
| Total counted first-party JSON decodes | 8,841 | 8,841 |
| Instrumented hashes / Python sync calls | 519,101 / 15,194 | 519,101 / 15,194 |
| Instrumented hash bytes | 1,509,931,361 | 1,516,223,341 |
| Storage inventory scans | 15 | 15 |
| Capture | 12.2566 s | 14.4016 s |
| Parent hydration, nested | 0.1369 s | 0.2499 s |
| Raw writes, nested | 1.6136 s | 1.8952 s |
| Rate waits, nested | 0.0170 s | 0.0217 s |
| Landing | 12.6704 s | 15.8557 s |
| dlt extract / normalize / load, nested | 8.0845 / 3.2185 / 1.6242 s | 13.4991 / 0.4921 / 2.2436 s |
| dbt | 4.7160 s | 6.4661 s |
| Encoded hashing | 0.1994 s | 0.2826 s |
| Helper hashing | 1.1167 s | 1.5656 s |
| JSON parsing | 0.8704 s | 1.7144 s |
| Storage metadata scans | 0.3019 s | 0.3552 s |
| Complete Parquet export | 0.3856 s | 0.4310 s |
| In-pipeline verification | 0.1506 s | 0.1695 s |
| Pointer promotion | 0.0009 s | 0.0008 s |
| Pruning | 0.8009 s | 0.9492 s |
| Standalone verification, excluded above | 1.2239 s | 1.3816 s |

No retries occurred; identity encoding was used, so gzip decompression was not measured. Arrow changes dlt's internal path rather than reducing these first-party JSON passes. Full integrity checks, dependency discovery, retained-history certification and complete exports still scan substantial data.

| Scale high-water measurement | Reference | Candidate |
|---|---:|---:|
| Parent RSS, maximum across runs | 1329.0 MiB | 1403.7 MiB |
| Child RSS, maximum across runs | 1340.9 MiB | 1367.7 MiB |
| Store-accounted retained, median | 471.1 MiB | 474.0 MiB |
| Store-accounted temporary, median | 189.8 MiB | 191.2 MiB |
| Untimed sampled actual retained, lower bound | 327.9 MiB | 327.6 MiB |
| Untimed sampled actual temporary, lower bound | 188.7 MiB | 188.4 MiB |

The equal-work history-sparse comparison starts from the same certified, checkpointed baseline with 250,004 observations over 10,000 market identities. Each refresh adds 113 observations and affects 110 market keys (1.1%), leaving 250,117 observations. Every measured pair preserves exact public Parquet bytes, release identity, logical lineage and carried T0/refreshed T1 timestamps. Both sides make eight requests and receive 45,477 encoded bytes. Baseline cloning and the candidate's explicit root upgrade are outside refresh timing.

Its median current-projection phase is **3.801 s versus 5.758 s**, so this run does not demonstrate a projection-time gain despite the narrower window inputs verified by the query-plan tests. Baseline verification is 7.414 s versus 6.379 s, lineage verification 8.173 s versus 6.643 s, pruning 4.565 s versus 4.257 s, and export 0.192 s versus 0.154 s. These inclusive phases overlap. The total observed 5.2% reduction cannot be attributed to the SQL change from these contended measurements. Retained-evidence certification and complete exports remain material costs.

Reservations are separate allowances, not additional disk consumption. The Arrow batch limits do not establish a global RSS limit or a hard dlt write quota. Generated roots, profiles, inventories and reports remain outside Git and distributions.

## Earlier measured results

These measurements describe the runtime shipped in `9ea9c6bd272624118e2780f1a6dec5708b6e209a`. Subsequent retry, atomic-write accounting and directory-durability fixes add synchronization and change request scheduling; this matrix has not been rerun for those changes.

Measured on 2026-10-18 in Europe/Zurich (2026-10-17 UTC) with Python 3.12.13 on arm64 macOS 27.0.1, the locked dependencies and one local filesystem. Both revisions used eight acquisition workers, four dbt threads and the same limits for each profile. Each comparison used one warm-up and three alternating measured pairs. The baseline is `0f53d0cda7d24c8e5268e90bc0e4f12d3558645a`; the frozen runtime/harness inventory hash is `2801cb946504b835448f838770afeb577ee0c328a9419220f0ee9392ed4b8dc4`. This matrix includes the review fixes and corrected direct-artifact JSON observer. It supersedes the earlier helper-only counts, which omitted two dbt-artifact parses per build. Every writer guard still reads the root marker freshly; exact canonical bytes now validate without reparsing, with the existing JSON fallback for other representations. Raw fixtures, inventories and reports remain outside Git.

Times below are native pipeline medians; percentages compare the two medians. Refresh rows compare a full T1 refresh with a composed T1 refresh from the frozen T0 baseline.

| Operation | Baseline | Candidate | Change |
|---|---:|---:|---:|
| Full, 200 observations | 2.214 s | 2.284 s | +3.1% |
| Full, 250,000 observations | 22.836 s | 20.034 s | -12.3% |
| Targeted metadata | 30.426 ms | 25.791 ms | -15.2% |
| Stop and resume, 200 observations | 2.243 s | 2.322 s | +3.5% |
| No-change composed refresh | 2.244 s | 2.670 s | +19.0% |
| Sparse-change composed refresh | 2.239 s | 2.664 s | +19.0% |
| All-change composed refresh | 2.243 s | 2.699 s | +20.4% |

Every full/resume pair matched all Parquet bytes, release IDs, manifests and pointers; targeted bundles matched exactly. All three composed fixtures matched reference current rows while preserving original provenance and the baseline history. Their histories contain 201 baseline observations plus 3, 5 and 201 new observations respectively.

The 25% scale time target is **unmet**: the measured gain is 12.3%. The counted first-party parsing gate passes: full-small decodes fall from 160 to 74 (53.75%); scale decodes fall from 26,389 to 8,841 (66.5%). Each full build includes two direct dbt-artifact parses on both sides. Full-small and resume regress by 3.1% and 3.5%, below the 10% investigation threshold; targeted metadata improves by 15.2%.

The small composed cases are 19.0–20.4% slower. They still make eight reconciliation requests versus seven for a full refresh, retain the baseline history, and perform baseline/lineage certification. Their dbt medians are about 2.05–2.07 s versus 1.97–1.98 s for full refresh; baseline verification is about 0.10 s and lineage verification about 0.13 s, with nested phases. Current projection takes 62–100 ms, verification about 40 ms and pruning about 79 ms. Measured candidate setup publishes v1 and then a full v2 baseline. This one-time setup costs about 4.88–4.90 s versus 2.55–2.56 s for v1; cloning costs about 13 ms versus 10 ms, outside refresh timing. This fixture does not establish a scale incremental speedup. Full capture remains the default.

Scale counters and phase medians:

| Measurement | Baseline | Candidate |
|---|---:|---:|
| Requests | 1,256 | 1,256 |
| Encoded bytes | 94,555,150 | 94,555,150 |
| Decoded bytes | 94,555,150 | 94,555,150 |
| Raw body reads, including hashing | 11,304 | 5,024 |
| Raw body JSON decodes | 8,792 | 2,514 |
| Metadata/journal/artifact JSON decodes | 17,597 | 6,327 |
| Instrumented hash calls | 518,869 | 519,101 |
| Instrumented hash bytes | 1,482,798,723 | 1,498,394,645 |
| Python sync calls | 13,833 | 8,864 |
| Storage inventory scans | 15 | 15 |
| Capture, inclusive | 7.9631 s | 7.0783 s |
| Parent resolution, nested in capture | 2.2176 s | 0.0991 s |
| Raw writes, nested in capture | 1.0833 s | 0.5534 s |
| Rate waits, nested in capture | 0.0237 s | 0.0190 s |
| Landing | 7.8978 s | 8.1026 s |
| dbt | 2.9833 s | 3.0504 s |
| Encoded hashing | 0.2545 s | 0.1297 s |
| Helper hashing | 0.7726 s | 0.7509 s |
| JSON parsing | 1.6547 s | 0.5400 s |
| Storage metadata scans | 0.1874 s | 0.1906 s |
| Parquet export | Unavailable | 0.2615 s |
| In-pipeline verification | 0.0376 s | 0.0979 s |
| Pointer promotion | 0.0003 s | 0.0004 s |
| Pruning | 0.0016 s | 0.5075 s |
| Standalone verification, excluded above | 0.7776 s | 0.7761 s |

Identity responses were used throughout; gzip decompression was not measured and no retries occurred. Archived export timing is unavailable separately. Inclusive phase timings overlap and must not be summed.

| Scale high-water measurement | Baseline | Candidate |
|---|---:|---:|
| Parent RSS, maximum across runs | 1233.2 MiB | 1319.8 MiB |
| Child RSS, maximum across runs | 1359.0 MiB | 1378.2 MiB |
| Store-accounted root | 332.9 MiB | 466.1 MiB |
| Store-accounted temporary subset | 193.7 MiB | 187.3 MiB |
| Untimed sampled actual root, lower bound | 330.4 MiB | 333.4 MiB |
| Untimed sampled temporary subset, lower bound | 191.6 MiB | 194.2 MiB |

These measurements show fewer response reads/parses and synchronization calls, with higher parent RSS and similar sampled disk use. Outstanding candidate reservations peak at 2304.0 MiB; reservations are allowances, not additional on-disk bytes, and are separate from the storage table. Landing, capture, full integrity checks and complete export remain substantial costs; the missed time target remains an open performance result.

## Protocol-5 development measurements (2026-10-22)

The bounded development session compared archived `ed133d9` with protocol 5 on the MacBook's external SSD. It closed after **25m38s total**, including setup, measurements, report preparation and independent review, within the one-hour allowance. The detailed report and run summaries are retained outside the repository in `/Volumes/Mac SSD/oddsfox-data/kalshi-events-costs/session-20261022T105746Z/`.

The corrected combo-heavy fixtures ran one warm-up and three alternating measured pairs. At 1,000 markets the median full pipeline was 5.566s for the candidate versus 5.323s for the reference (+4.6%); paired ratios straddled parity, so this is not a repeatable regression. At 10,000 markets the medians were 14.590s versus 16.829s (-13.3%), but the reference range was 10.965–51.323s and includes a large outlier. This is inconclusive and not a speed claim. The 25% target remains unproven.

The 10,000-market run used 106 candidate requests versus 154 reference requests, with about 52.34 MB encoded on each side. All 48 fewer requests are accounted for by the historical cutoff schedule: the reference made 51 cutoff requests and protocol 5 made three. The candidate requested gzip but the fixture returned identity for every response. The 1,000-market run used 16 versus 19 requests for the same cutoff reason. These fixtures contain only a few already-known parents, so they do not measure the runtime savings from batched parent hydration. The public API's sustained allowance, gzip savings on a complete run, and full-catalogue duration remain unknown.

Same-receipt v2 build/publish comparisons used a frozen storage-v5 capture-only root and compared the storage-v5-compatible `ddc19b2` reference runtime with the current protocol-5 runtime. The `ed133d9` reference predates this seed format and was used for changed-acquisition/full-pipeline comparisons only, not this same-receipt build comparison. Public Parquet bytes and release identities matched in all three pairs. Build/publish medians were 6.661s candidate versus 6.512s reference (+2.3%), within observed run-to-run spread. The seed established a v2-only baseline directly; it did not create an unnecessary v1 publication first. Physical DuckDB database hashes are not public release identities and were not required to match; the verified public outputs and release identities were compared. A separate frozen full fixture retained exact artifact, manifest, pointer and release identity. No production SQL rewrite was shipped: the measured combo-leg query was about 60ms within roughly 3.9s of dbt work, and the required 20% affected-phase improvement and regression comparisons were not established. Sparse/all-change exploratory runs did not compare equal work and are not speed claims.

These development measurements support fewer historical cutoff requests and preserve same-receipt output, but they do not establish a significant end-to-end acceleration. A bounded full-catalogue attempt is recorded in the following section; the eight-hour acceptance has not completed successfully, and verified publication plus standalone release verification remain outstanding. Its acceptance conditions remain: finish within the bounded session, complete acquisition, verify/promote a v2 release, then independently verify that release. Until then, do not treat synthetic throughput as a catalogue runtime estimate. The dlt post-load quota limitation tracked separately under `WORK-42` also remains open.

## Full-catalogue acceptance attempt (2026-10-22)

A bounded live attempt stopped during acquisition after the provider returned `KXCITIESWEATHER-25MAR04(NY)(PHIL)`, which the then-current shared ticker validator rejected. The event's identifier is valid provider data; the OpenAPI schema declares it as a string without a restrictive pattern. An authorized continuation of the same capture recovered four orphaned receipts and issued 409 requests without a 429 before stopping on a second valid provider identity, `GDP-232022 Q4`, whose interior ASCII space was also rejected. The current validator permits parentheses and interior U+0020 spaces while preserving exact strings, its first-character and length checks, and rejection of leading/trailing spaces, controls, Unicode whitespace and delimiters. A final continuation exercised both fixes without another ticker-validation error.

The initial supervised interval from setup through supervisor stop lasted 4,383 seconds; its refresh child ran about 54 minutes. The initial run made 15,752 HTTP attempts and committed 15,748 page transitions, transferring 3,277,360,442 encoded bytes and 28,674,089,430 decoded bytes (about 8.75:1). It requested gzip with a 5 requests/second ceiling; metrics observed gzip for 15,715 responses and identity fallback for 37 cutoff responses. All reported statuses were 200, with no 429 responses or rate changes. The initial run averaged about 4.85 attempts and committed pages per second. The later 409 requests are separate from these initial totals.

The initial attempt completed series and the initial cutoff, but not ordinary events, multivariate events, live markets or historical markets. No entity totals were measured; the evidence was not landed. The command's accounting was incomplete, so initial request and byte counts come from the separate metrics report. Neither this run nor its continuation produced a build, publication, pruning or standalone verification.

The final continuation stopped at 22:17:38 UTC, two seconds after its original workflow cutoff at 22:17:36 UTC. The session began at 14:32:36 UTC, with an overall report deadline of 22:32:36 UTC. Supervision timed out the child on the operation deadline, sent SIGTERM, confirmed cleanup and reaped its process groups. The CLI reported `operation deadline exhausted`, `capture_complete: false`, `resumable: true`, `publication_state: not_attempted`, and complete accounting: 57,613 requests and 14,212,744,364 encoded bytes. Its 57,615 committed transitions include four recovered orphan receipts, leaving 57,611 new-response commits; 57,612 statuses were HTTP 200 and one attempt had no recorded status. No 429 occurred; the effective rate stayed at 5 requests/second. The final continuation transferred 126,266,582,928 decoded bytes. Cumulative accounting (audited prior consumption plus the final invocation) is 73,774 requests and 17,563,176,845 encoded bytes; cumulative HTTP decoded metrics are 155,559,467,241 bytes (about 8.86:1). Events and series completed; multivariate events, live markets and historical markets remained unfinished. No entity totals or full-catalogue runtime estimate resulted.

Final-continuation metrics report 18,005.714 seconds of capture phase time, 15,307,358,208 bytes process self peak RSS (about 14.26 GiB), 17,684,147,509 bytes Store-accounted retained high-water (about 16.47 GiB), and 5,163,485 bytes temporary high-water. Parsing took 3,330.739 seconds and main-thread page CPU 6,494.932 seconds; client-observed header waits totaled 18,484.736 worker-seconds and rate waits 21,389.262 worker-seconds. These timers overlap and are not additive wall time. They do not isolate server time or prove CPU, SSD or one other subsystem as the dominant bottleneck. The observed rate of about 3.2 attempts/second came from this interrupted and externally paused run, not a sustained provider-capacity measurement. Owned processes were observed externally suspended around 21:24 UTC and restored around 21:25 UTC; the cause is unknown and is not attributed to Kalshi or other applications.

Read-only checks validated journal/snapshot state within the limits recorded in the continuation report. The capture was resumable when the run stopped; preserve the root, and do not resume automatically. The original acceptance window has since expired, so any future acceptance requires a new bounded session, offline recovery checks, and accounting that subtracts prior request/download use. The initial report is at `/Volumes/Mac SSD/oddsfox-data/kalshi-events-costs/acceptance-20261022T143236Z/reports/acceptance-report.md`; the final continuation report is at `/Volumes/Mac SSD/oddsfox-data/kalshi-events-costs/acceptance-20261022T143236Z/continuation3/reports/final-acceptance-report.md`.

## Supervised production acceptance

The supported `scripts/run-production.py` runner provides a bounded production attempt, not a measured runtime guarantee. The planning allowance is 24–48 hours with substantial uncertainty: no exchange-wide catalogue has completed and the real combo-heavy downstream cost remains unknown. A 48-hour budget stop is incomplete acceptance, even if acquisition or operator publication completed.

The session begins after code verification and records the frozen revision, installed environment, machine/filesystem, initial free space and competing resource use. Refresh is bounded by hour 46; standalone verification and production activation must finish by hour 47:50, leaving ten minutes for reporting. All subprocesses share the remaining session allowance, with at most five additional seconds for termination and reaping. The runner retains gzip and the five-request/second ceiling with downward-only throttling adaptation; it performs no additional comparison probes or automatic networking recovery.

Limits are 850,000 attempts, 256 GiB encoded downloads, 400 GiB aggregate retained session storage, a 350 GiB operator-root ceiling, and 64 GiB temporary/database sublimits. At least 150 GiB of SSD space must remain free. Each response is bounded at 384 MiB encoded and 768 MiB decoded. Every 15 seconds the supervisor samples free space and process resources; summed descendant RSS above 24 GiB for 60 seconds stops work. Shared pages can be counted repeatedly in that conservative RSS sum. These checks do not turn dlt's post-load accounting into a hard write quota.

External reports distinguish capture, publication and production activation, including unknown outcomes. Record requests, retries, statuses, encoded/decoded bytes, endpoint progress, effective rate, phase durations, CPU/RSS, storage peaks, synchronization and stop reasons. Concurrent worker and nested phase timers overlap; do not add them to estimate wall time. Retain capture-specific recovery instructions, checksums and the observed source interval.

Acceptance requires a completed full capture, certified catalogue v2 publication, successful independent release verification and readback of the activated `production.duckdb`. Until those gates pass, no full-catalogue production dataset or performance percentage is claimed. The previous eight-hour attempt remains historical evidence rather than successful production acceptance.

## Acceptance and remaining costs

The parsing gate requires at least 50% fewer counted first-party in-process JSON decodes on the fixed full fixture, using the scope defined above. It does not establish a reduction across unobserved third-party or subprocess parsers. The scale target is at least 25% lower median pipeline time with equal work and limits. A missed time target remains an open performance result. Reproducible small or targeted median regressions above 10% require investigation.

Series, combo events, combo markets and historical markets still require full reconciliation during a composed refresh. Publication still exports and checksums every relation, including retained history; independent verification still reads its evidence. These costs can dominate after ordinary metadata acquisition becomes incremental. A chain is limited to eight deltas before a verified full rebase.

Live catalogue duration depends on exchange population, pagination, response size, rate limits, retries, storage and machine resources. These offline fixtures do not establish live compression support, higher sustained public throughput or production speedup. The separately recorded short probe establishes only the endpoint-specific gzip observations described above. See [format and freshness contracts](contracts.md) for rollout, recovery and carried-value semantics.
