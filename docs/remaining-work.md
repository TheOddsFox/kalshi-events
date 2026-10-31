# Historical production completion

This inventory starts from revision `9f33ae8`. Code delivery, offline test results,
capacity admission and production activation are separate outcomes. No complete
historical production database has been activated by this investigation.

## Verified defects

| Area | Failure | Required proof |
| --- | --- | --- |
| Retirement accounting | Deleting root-level prune metadata can leave the active Store's retained-byte inventory stale. | Both interrupted and completed retirement release their accounted bytes, with safe retry after failure. |
| Recovery benchmark | Its commit observer rejects the current `first_data_timing` keyword. | The actual resume worker runs against current and reference capture APIs. |
| Diagnostic paths | The historical benchmark creates output before rejecting an unsafe operator-root alias. | Invalid, overlapping and nested-checkout paths cause no output writes. |
| Orphan recovery | An invalid successful source response can be adopted repeatedly, stranding its unfinished request. | Preflight permits retry of typed source-body failures while retaining raw evidence and failing closed on integrity, context and scope contradictions. |
| Receipt ordering | Whole-second receipt text can sort after a chronologically newer fractional-second receipt. | Python, SQLite and SQL compare exact UTC instants while retaining original timestamp text; old hints and baseline projections cannot silently retain stale winners. |
| Operational guidance | The performance guide still described the removed logical-compressor cutoff as current policy. | Guidance matches NORMAL/WARN/CRITICAL monitoring and the independent RSS guard. |
| Protected index accounting | The historical benchmark counted only each parent index's `index.sqlite`, omitting recovery indexes, SQLite sidecars and other index metadata. | Sum logical `st_size` per regular-file path under immediate `tmp/parent-index-*` and `tmp/recovery-index` trees; exclude unrelated temporary trees and fail closed on symlinks, special files or an over-bound inventory. |

The changes address these defects with targeted regressions. Final verification
and representative recovery measurements remain separate acceptance gates.
Source-body errors are classified only at body-validation sites; general
evidence-validation errors must never be suppressed to retry them as source errors.

## Production blockers

The approved monitored historical attempt now permits an explicitly selected
expanded profile: 128 GiB database and 192 GiB temporary ceilings, 112/168 GiB
protective stops, and 96/144 GiB capacity-admission forecasts respectively.
Root/aggregate limits remain 350/400 GiB and minimum SSD free space remains
150 GiB. No new volumes or mounts are permitted. For this attempt only, the
operator accepted monitored write overshoot instead of requiring a hard
filesystem boundary. Genuine hard-enforcement work remains unresolved.

The revised bounded investigation counted all 83,397 selected data receipts:
79,762,323 observations (78,835,045 historical markets, 911,884 events and
15,394 series). This establishes the saved population, not a finished warehouse.
A broader 219,523-observation fixture passed native/reference schema and
two-way row comparisons after correcting dbt spill configuration. Both loaders
previously failed when the adapter reapplied temporary-directory settings after
spill; those settings now use supported connection-time configuration.

Admission remains withheld. The revised endpoint-weighted database scenario is
52.64 GiB (63.16 GiB with a 20% sample-mix allowance), but a sampled-tail stress
is 102.08 GiB, above the expanded 96 GiB admission threshold. These are scenarios,
not formal upper bounds. End-of-phase temporary snapshots are not high-water
measurements; full-scale spill and aggregate occupancy remain unresolved.
No production processing attempt or new offline grant was started.

The current sampled lossless layout is not admitted. The previous endpoint-weighted
point forecasts were about 58.3 GiB of database and 91.2 GiB of temporary storage.
They exceed the 48 GiB admission threshold protecting each 64 GiB category.
Wide/high-entropy content, repeated entities, full-scale spill and a fresh root
baseline still need representative evidence. Sensitivity scenarios are not formal
upper bounds. Smaller sampled databases do not establish full-scale feasibility.

The shared APFS operator location has no validated write-enforcing boundary for
aggregate logical file sizes. Polling, per-batch checks, spill limits, allocated-block
quotas and per-file limits do not by themselves provide that guarantee. Storage
provisioning is excluded from the approved attempt. The monitored exception
does not establish a hard logical-byte or allocated-space guarantee.

A fresh seven-observation dlt reproduction with a 1 MiB database allowance wrote
1,847,296 bytes before reporting `database storage exhausted`; the same load
succeeded with a 16 MiB allowance. This confirms post-write detection on the
current path, not enforcement. It does not establish a defect in the separate
native historical loader's row semantics.

The October 31 read-only root inventory found 24,463,120,641 bytes of raw evidence,
152,398,958 bytes of capture metadata, and 356,812,974,170 bytes under temporary
storage. Those temporary bytes include the failed database, loader Parquet, spill
and indexes. No cleanup was performed. Do not use the older assumed 35 GB root
baseline to admit a new run; cleanup and fresh accounting remain required.

## Acceptance still required

- Representative warm, cold and hydration-only recovery comparisons, separating
  worker startup from supervisor accounting and preflight. The five-minute target
  is unproven until the complete path is measured.
- Final-source verification, distribution checks, isolated installed-wheel checks,
  and independent correctness, semantic-data and filesystem/security reviews.
- One admitted saved-evidence-only historical attempt, then standalone release
  verification, atomic activation and fresh-process read-only database queries.

Historical-only completion supersedes acquisition of live markets and multivariate
events. Existing legacy artifacts remain readable; excluded acquisition was never
completed and must not be reported as successful. All historical markets, distinct
observations and exact payload text remain required, including unresolved parents
and combo references under the historical contract.

Persistent investigation artifacts remain under ignored `data/`. No automatic API
acquisition, restart, retry, capacity increase or GitHub CI restoration is implied.
