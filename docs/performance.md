# Offline performance measurements

The benchmark generates deterministic public-API-shaped responses without networking. It runs capture, build and publication with the same fixture, interpreter, dependencies, limits and filesystem for each revision. It checks output equivalence before accepting a timing result.

The frozen full fixture contains 197 markets and three parent observations. The scale fixture contains 249,997 markets and three parents, including 249 combo markets. Both use 200-market response pages, exact decimal strings, native combo legs, repeated parents, rules and nested price ranges. This synthetic pagination is a benchmark input, not an estimate of exchange page counts or live runtime. A separate 1,203-market test exercises seven pages and a combo leg without changing the frozen fixtures.

## Reproduce a comparison

Use one locked environment and two immutable source archives. Keep both archives and all generated roots/reports on the same filesystem, outside Git. Do not run another benchmark or test suite concurrently.

```sh
uv sync --locked
bench_dir=$(mktemp -d /tmp/kalshi-events-benchmark-XXXXXX)
mkdir "$bench_dir/baseline" "$bench_dir/candidate"
git archive 0f53d0cda7d24c8e5268e90bc0e4f12d3558645a | tar -x -C "$bench_dir/baseline"
git archive HEAD | tar -x -C "$bench_dir/candidate"
uv run --locked python scripts/benchmark-pipeline.py \
  --source-root "$bench_dir/baseline" \
  --compare-root "$bench_dir/candidate" \
  --source-revision 0f53d0cda7d24c8e5268e90bc0e4f12d3558645a \
  --compare-revision "$(git rev-parse HEAD)" \
  --profile full --warmups 1 --runs 3 \
  --report-dir "$bench_dir/full-small"
```

Repeat with a new report directory and `--scale` for approximately 250,000 observations. Use `--profile targeted` for metadata acquisition/export and `--profile resume` for an orderly request-budget stop followed by recovery. Each profile requires a new report directory. `--sample-storage` adds an untimed run that samples actual on-disk usage; its observed peaks are lower bounds between samples.

Refresh comparisons use `--profile no-change`, `sparse-change` or `all-change`. Each revision prepares one T0 baseline, freezes its root identity and physical database bytes, then clones that seed for every warm-up and measured run. Each run measures T1 acquisition/build/publication in a fresh process. The reference performs a full v1 refresh; the candidate performs a composed v2 refresh. Reports separate the shared baseline setup and per-run clone costs. Both sides use the same 4 GiB download allowance, sufficient for the extra concurrent reconciliation traversal. Frozen full, targeted and resume profiles retain their original 2 GiB allowance. The fixture rate is explicitly 1,000 requests/second on both sides; it is not a live rate recommendation.

Trading values stay fixed in refresh fixtures. Current relation comparison excludes only source observation/capture identifiers and observation time; separate assertions check carried T0 and refreshed T1 provenance, exact baseline history retention and expected new observations. Composed history intentionally contains more observations than a fresh full release. These comparisons do not establish universal change-feed completeness or current trading-value equivalence.

## Interpret reports

`pipeline_elapsed_seconds` measures native capture/build/publication after imports, excluding separately prepared baseline setup, output comparison and standalone verification. `worker_wall_seconds` includes process startup and post-run checks. Phase timings are inclusive and can overlap; do not add them. Compare medians of at least three paired runs after one warm-up, and inspect individual pairs for variance.

Reports include fixture/revision identities, package versions, interpreter, filesystem device, limits, requests, bytes, reads, JSON decodes, hashing, synchronization, store-accounted peaks and separate parent/child peak RSS. Hash counters cover instrumented `digest`/`file_digest` helpers, including normalization and metadata aliases, plus explicitly instrumented transport and journal hashes. Uninstrumented lineage and membership hashes and third-party internals are outside those totals; available hashing phases are reported separately. Instrumentation is enabled on both revisions, so timings include recorder overhead. Store-accounted retained bytes include all files under the operator root; temporary bytes are a subset. Child RSS is the operating system's child-process high-water measurement, not simultaneous combined memory. Synchronization counters cover instrumented Python calls (inferred for archived code), including attempted failed calls in updated code; DuckDB/C++ internal synchronization is outside those counters.

The benchmark JSON total covers first-party in-process parsing through `common.decode` and the warehouse's direct dbt-artifact `json.loads` calls. The artifact counter is a subset of that total; do not add it again. Third-party and subprocess JSON parsing is outside this counter. Raw-body parses and other metadata/journal/artifact parses are reported separately. `metadata_scan` and `metadata_scans` measure storage inventory walks/reconciliations, separately from JSON parsing. Standalone capture/release or metadata verification has its own measurements and is excluded from pipeline counters. Archived code lacks some native phase hooks; unavailable phases are marked explicitly instead of reported as zero-cost work. Identity fixtures do not measure gzip decompression performance.

Full v1 output checks compare Parquet bytes, release identity and deterministic manifest/pointer identities. Composed runs also require stable Parquet/release identities from the frozen baseline; their manifest and pointer bytes may vary with the certified physical warehouse hash hint. Targeted checks compare the complete metadata bundle and independently verify it. Mutation between commands must still fail fresh evidence verification; operation-local reuse is not a persistent trust cache.

## Measured results

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

Live catalogue duration depends on exchange population, pagination, response size, rate limits, retries, storage and machine resources. No live compression support, higher sustained public throughput or production speedup is established by these offline fixtures. See [format and freshness contracts](contracts.md) for rollout, recovery and carried-value semantics.
