# Offline performance measurements

The benchmark generates deterministic public-API-shaped responses without networking. It runs capture, build and publication with the same fixture, interpreter, dependencies, limits and filesystem for each revision. It checks output equivalence before accepting a timing result.

The frozen full fixture contains 197 markets and three parent observations. The scale fixture contains 249,997 markets and three parents, including 249 combo markets. Both use 200-market response pages, exact decimal strings, native combo legs, repeated parents, rules and nested price ranges. This synthetic pagination is a benchmark input, not an estimate of exchange page counts or live runtime. A separate 1,203-market test exercises seven pages and a combo leg without changing the frozen fixtures.

## Reproduce a comparison

Use one locked environment and two immutable source archives. Keep both archives and all generated roots/reports on the same filesystem, outside Git. Do not run another benchmark or test suite concurrently.

For native Windows Python use the [Windows handoff](windows.md). Both source archives need the same compatibility patch: unmodified `21edf14` imports Unix-only `resource` and uses POSIX directory synchronization. Windows volume-wide flushing is required and remains inside the timed work; do not compare it with a POSIX baseline or omit synchronization to produce a timing result.

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

Existing full, targeted and resume profiles retain their original request protocol and frozen receipt identities. They compare the same evidence through the dictionary and Arrow loaders. Existing composed-refresh profiles retain their workload definitions and request protocol; the mock receipt-ID correction described below applies to that family. `--profile selected-stress` instead compares changed acquisition requests: 100 ordered selections share parents, with live, historical, absent and unresolved cases, a maximum-length ticker, and a parent containing 2,000 unrelated child markets. Native values and coverage must agree; each side's actual receipt provenance is independently validated. Only incidental child arrays in parent-event payloads are excluded from that native comparison. Receipt and release identities are expected to differ.

Use `--profile selected-single` for one present selection and `--profile selected-fallback` for a historical plus an absent selection. These exercise the new acquisition protocol, unlike the frozen `targeted` profile. Reports separate singular fallback calls from the initial batch request.

`--profile history-sparse` retains at least 250,000 observations over 10,000 market identities, then refreshes approximately 1% of keys. Repeated baseline pagination is intentional history stress, not an estimate of exchange behavior. Complete reconciliation membership stays stable. Both revisions run an incremental refresh from clones of the same certified reference baseline; the candidate clone's explicit root upgrade is measured outside refresh timing. The benchmark checks the actual affected-market table, carried timestamps and provenance, retained history and public output bytes. `--profile baseline-v2-setup` separately measures direct v2-only bootstrap; it does not replace the existing setup profile that publishes both versions.

Refresh comparisons use `--profile no-change`, `sparse-change` or `all-change`. Each revision prepares one T0 baseline, freezes its root identity and physical database bytes, then clones that seed for every warm-up and measured run. Each run measures T1 acquisition/build/publication in a fresh process. The reference performs a full v1 refresh; the candidate performs a composed v2 refresh. Reports separate the shared baseline setup and per-run clone costs. Both sides use the same 4 GiB download allowance, sufficient for the extra concurrent reconciliation traversal. Frozen full, targeted and resume profiles retain their original 2 GiB allowance. The fixture rate is explicitly 1,000 requests/second on both sides; it is not a live rate recommendation.

Trading values stay fixed in refresh fixtures. Current relation comparison excludes only source observation/capture identifiers and observation time; separate assertions check carried T0 and refreshed T1 provenance, exact baseline history retention and expected new observations. Composed history intentionally contains more observations than a fresh full release. These comparisons do not establish universal change-feed completeness or current trading-value equivalence.

## Interpret reports

`pipeline_elapsed_seconds` measures native capture/build/publication after imports, excluding separately prepared baseline setup, output comparison and standalone verification. `worker_wall_seconds` includes process startup and post-run checks. Phase timings are inclusive and can overlap; do not add them. Compare medians of at least three paired runs after one warm-up, and inspect individual pairs for variance.

`dlt_phase_seconds` records dlt's own extract, normalize and load trace spans after the existing `pipeline.run()` call. Those spans are nested within landing and are not additional pipeline time. Observation production, evidence checks, synchronization and storage checks remain part of the landing measurement. The observer does not change production configuration or execution.

Reports include fixture/revision identities, package versions, interpreter, filesystem device, limits, requests, bytes, reads, JSON decodes, hashing, synchronization, store-accounted peaks and separate parent/child peak RSS. Hash counters cover instrumented `digest`/`file_digest` helpers, including normalization and metadata aliases, plus explicitly instrumented transport and journal hashes. Uninstrumented lineage and membership hashes and third-party internals are outside those totals; available hashing phases are reported separately. Instrumentation is enabled on both revisions, so timings include recorder overhead. Store-accounted retained bytes include all files under the operator root; temporary bytes are a subset. Child RSS is the operating system's child-process high-water measurement, not simultaneous combined memory. Synchronization counters cover instrumented Python calls (inferred for archived code), including attempted failed calls in updated code; DuckDB/C++ internal synchronization is outside those counters.

Native Windows reports self lifetime peak working-set bytes from `K32GetProcessMemoryInfo` and `children: null`: child peak RSS is unavailable, not zero. `volume_flush_calls` counts volume-wide barriers separately, in addition to `fsync_calls`; `volume_flush` measures their elapsed time. Reports also record the platform durability requirement and SHA-256 inventories of actual Python/dbt source files, including compatibility changes. JSON metadata is read as UTF-8 on every platform.

The benchmark JSON total covers first-party in-process parsing through `common.decode` and the warehouse's direct dbt-artifact `json.loads` calls. The artifact counter is a subset of that total; do not add it again. Third-party and subprocess JSON parsing is outside this counter. Raw-body parses and other metadata/journal/artifact parses are reported separately. `metadata_scan` and `metadata_scans` measure storage inventory walks/reconciliations, separately from JSON parsing. Standalone capture/release or metadata verification has its own measurements and is excluded from pipeline counters. Archived code lacks some native phase hooks; unavailable phases are marked explicitly instead of reported as zero-cost work. Identity fixtures do not measure gzip decompression performance.

Full v1 output checks compare Parquet bytes, release identity and deterministic manifest/pointer identities. Composed runs also require stable Parquet/release identities from the frozen baseline; their manifest and pointer bytes may vary with the certified physical warehouse hash hint. Targeted checks compare the complete metadata bundle and independently verify it. Mutation between commands must still fail fresh evidence verification; operation-local reuse is not a persistent trust cache.

## Acquisition diagnostics and coordinated refresh

The additive `refresh` workflow uses existing capture and v2 publication routines. It removes the need to manually coordinate stages and avoids creating a redundant v1 baseline build. Default invocation uses incremental acquisition only after a verified baseline exists; `refresh --full` explicitly establishes or rebases that baseline. This workflow does not establish a faster initial full crawl. Targeted `metadata refresh` remains preferable when only selected market definitions are required.

Opt-in metrics retain their existing version and fields and add bounded endpoint-family aggregates. Singular ticker paths become placeholders; reports contain no unbounded request traces or native payloads. Acquisition counters distinguish attempts and HTTP statuses, requested/observed encoding, bytes, committed pages and recovered pages. Latency aggregates retain count, total and maximum rather than individual request records.

Header and response-iterator waits measure time observed by the client, including connection and scheduling effects. Iterator advancement is timed separately from hashing, decompression and spool writing. The existing inclusive request phase remains available. Additional phases separate rate/retry waits, receipt finalization, journal appends, snapshot replacements, validation/application, scheduler waits, spool writes and file/directory synchronization. Main-thread page-processing CPU uses a thread CPU clock; peak concurrent requests is reported separately. Failed operations remain counted and timed. Synchronization timings cover instrumented Python operations, not every native library or database flush.

Worker times overlap and phases nest: neither their sum nor HTTP elapsed time alone proves the critical path or provider processing cost. Windows volume barriers retain separate measurements and unavailable child RSS remains null. Metrics stay outside evidence and cannot change raw receipts, release identities or existing command JSON.

The manual `scripts/probe-acquisition.py` runner compares identity and requested gzip serially using fresh roots. Only explicitly supplied rates are tried; the default remains 10 requests/second. Fixed per-sample and aggregate budgets, preserved partial reports and uncertain-accounting stops keep this diagnostic separate from production operation. See README for exact limits. Live source content can differ between samples, so a lower byte count or shorter sample is not an equal-work speedup. Identity fallback is a valid observed result.

Clean full/scale performance targets and actual native Windows recovery remain open.

A manually invoked public probe on 2026-10-20 used the new script at the unchanged 10 requests/second with fresh roots on the MacBook's temporary filesystem. Identity and requested-gzip samples made three and four attempts respectively, all HTTP 200, with no retries or throttling observed. Both stopped at in-flight download reservations before their 12-attempt limit; they were partial captures, not complete catalogues. Aggregate recorded downloads were 716,121 bytes. Gzip was actually served on `/events` and `/markets`; cutoff responses used identity. The samples read different endpoint mixes and are not an equal-work compression or throughput benchmark. They do not measure the external SSD/cable or establish a safe higher public rate. Generated roots, receipts, metrics and reports remain outside Git and distributions.

### Small-fixture diagnostic overhead checks

On 2026-10-20, immutable source archives of `8c13274` and `f2c6a61` were compared with the existing frozen benchmark harness, locked dependencies, one warm-up and three alternating measured pairs on the MacBook temporary filesystem. Both revisions used the same fixture, limits and 1,000-request/second synthetic pace. These are small offline observations, not live or scale performance acceptance. Diagnostics were enabled; this measures their added hooks alongside unchanged pipeline work.

| Fixture | Reference median | Candidate median | Observed change |
|---|---:|---:|---:|
| Full, 200 observations | 6.959 s | 7.559 s | +8.6% |
| Frozen targeted metadata, first matrix | 72.226 ms | 92.661 ms | +28.3% |
| Frozen targeted metadata, investigation repeat | 58.558 ms | 59.864 ms | +2.2% |

Every measured pair preserved public artifact bytes and release identities; identities were stable within each revision. Full-mode counts remained seven requests, 74 first-party JSON decodes and 146 instrumented sync calls. Targeted counts remained four requests, 33 decodes and 90 sync calls. No verification or synchronization was omitted.

The first targeted result exceeded the 10% investigation threshold. Repeating the same three-pair protocol without overlapping agent checks did not reproduce that median regression: reference run times were 57.936–58.581 ms and candidate times 56.872–86.152 ms. Capture medians were 46.990 versus 49.010 ms and raw-write medians 7.680 versus 8.442 ms. The first matrix varied more widely, including reference runs from 61.284–95.034 ms. These short timings do not establish the cause of the variation or a significant speedup.

The full-small result is also noisy: one 1.15-second compatibility test overlapped part of that matrix, and dbt medians changed from 6.101 to 6.753 seconds despite unchanged dbt/warehouse implementation. Treat its artifact equivalence as correctness evidence and its timing as inconclusive. No agent tests overlapped either targeted matrix; unrelated host activity was not controlled. Long full/scale comparisons and clean timing targets remain deferred to the workstation.

## Current changes: contended measurements against 21edf14

The implementation at `522af66a877fb75a1c61757ec1af0067a83f653e` was compared with `21edf14a486addfa743c524a645857ed711b640f` on 2026-10-18 using Python 3.12.13, dlt 1.31.0, PyArrow 26.0.0, DuckDB 1.5.6 and dbt-core 1.12.5 on arm64 macOS 27.0.1. Each reported comparison uses one warm-up and three alternating measured pairs with the same fixtures, limits, interpreter and filesystem. The runtime inventory SHA-256 is `57279c7f7612bea9690cb1fff7dd34cdbbc656584c89e47c3acff5b447dbaa08`.

Native Windows compatibility was added after this matrix; no measurements or full runs were repeated locally for it. Windows full validation, recovery checks and clean paired comparisons are deferred to the elevated local-NTFS workstation.

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

## Acceptance and remaining costs

The parsing gate requires at least 50% fewer counted first-party in-process JSON decodes on the fixed full fixture, using the scope defined above. It does not establish a reduction across unobserved third-party or subprocess parsers. The scale target is at least 25% lower median pipeline time with equal work and limits. A missed time target remains an open performance result. Reproducible small or targeted median regressions above 10% require investigation.

Series, combo events, combo markets and historical markets still require full reconciliation during a composed refresh. Publication still exports and checksums every relation, including retained history; independent verification still reads its evidence. These costs can dominate after ordinary metadata acquisition becomes incremental. A chain is limited to eight deltas before a verified full rebase.

Live catalogue duration depends on exchange population, pagination, response size, rate limits, retries, storage and machine resources. These offline fixtures do not establish live compression support, higher sustained public throughput or production speedup. The separately recorded short probe establishes only the endpoint-specific gzip observations described above. See [format and freshness contracts](contracts.md) for rollout, recovery and carried-value semantics.
