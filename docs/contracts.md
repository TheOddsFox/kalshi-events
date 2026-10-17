# Native contract invariants

Catalogue entity keys are native `series_ticker`, `event_ticker` and `market_ticker`; relationships use declared parents. Settlement status and results are copied without conflating native states. Raw response bytes are authoritative acquisition evidence. JSON payloads preserve exact numeric values as strings when parsing JSON decimals; financial projections use exact decimals, and definitions include rules, strikes, price grids, settlement sources and native combo legs.

Catalogue current tables include `payload_json` for the complete observed object plus normalized identity/provenance. Unresolved parents remain visible through left joins. `combo_legs` has one row per native selected leg, keyed by combo market and leg index, and retains native side and settlement value. Observation history has one row per observed page item and JSON pointer, deduplicated by observation identity on replay. A catalogue v1 release is built from exactly one complete catalogue capture.

Event-embedded markets are observation references only. Direct live and historical market endpoints determine current market inventory; a nested event summary cannot override a direct market definition. A catalogue release covers completed requested traversals over an observation interval, not an atomic exchange snapshot or a proof of permanent absence.

Targeted bundles contain only found usable selected markets and their authoritative event/series parents. Every requested ticker appears exactly once in coverage; absent, malformed, unresolved and failed are distinct states. A malformed or failed response prevents completion, while honest absence/unresolved parent results may complete with non-found coverage. No producer warehouse is needed to verify or consume a copied bundle.

Release identity is the SHA-256 of compact sorted JSON (`separators=(",", ":")`, no trailing newline) of literal filename inventories. Inventory entry fields are `sha256` and integer `size`; filenames and digests are ASCII. File SHA-256 includes every byte, including JSON files' final newline. Catalogue/metadata manifests have separate versioned contract identifiers.

## Storage and publication formats

These are separate compatibility boundaries for compressed receipts, journal recovery and composed catalogue releases. Existing catalogue v1 and metadata v1 behavior remains supported.

### Operator roots and rollback

The legacy root marker is exactly `{"owner":"oddsfox.kalshi.events.v1"}`. New operator roots use `{"owner":"oddsfox.kalshi.events.v1","storage_version":2}` in `root.json`. The changed marker makes existing binaries, which require exact equality with the legacy marker, reject an upgraded root before writing.

Updated binaries read legacy roots and their existing artifacts. Creating new-format artifacts in a legacy root requires the explicit `upgrade-root` command. Upgrade takes the canonical writer lock, checks the marker again under that lock, and atomically replaces only `root.json`; it does not rewrite evidence, captures, journals, builds or publications. An incomplete legacy capture may resume using its original checkpoint protocol. Upgrading its root must not switch that capture to a new journal protocol partway through recovery.

The first v2 catalogue operation creates `root-id.json`, a stable logical root identity used by composed lineage. A whole-root copy retains that identity; copying unrelated composed artifacts into another root does not establish valid ancestry. The storage marker itself remains unchanged.

Stop writers before upgrading an existing root. Initial rollout uses a separate operator root. Binary rollback uses a separate legacy root and a retained verified v1 publication. Never downgrade the marker or allow an old binary to write an upgraded root.

### Raw HTTP receipts

Raw evidence preserves the exact encoded HTTP entity bytes, excluding HTTP transfer framing. Identity encoding remains the default; `--compression identity|gzip` makes gzip an explicit operator choice. An identity response is valid when gzip was requested. Only identity and a single gzip member are supported. Unsupported encodings, incomplete gzip streams, checksum failures, additional members and trailing bytes are rejected as unusable evidence.

Receipt v1 remains readable with its identity-body `size` and `sha256`. Compressed evidence uses receipt v2 and records its content encoding plus the encoded and decoded sizes and SHA-256 checksums. Readers verify both representations before accepting a compressed payload. Encoding does not change the decoded JSON semantics, decimal preservation or observation identity rules.

Each capture seals independent encoded and decoded response ceilings at creation; both initially use the existing response-limit setting. A small encoded body does not excuse excessive decoded expansion. Download accounting measures actual encoded bytes received, including unsuccessful attempts and retries. Streaming and decompression retain aggregate in-flight reservations, disk limits, deadlines, origin validation, the shared request limiter and existing retry guarantees. The normal rate remains 10 requests per second; capability and rate diagnostics are bounded, explicitly invoked operations.

Bodies stream into atomic temporary evidence files while being hashed. Durability order is raw body, usable receipt, durable journal transition, then applied capture state. Failed or partial attempts cannot become usable receipts. Recovery includes partial temporary files and interrupted reservations in storage accounting.

### Journal recovery

New captures use a versioned append-only journal. Records have monotonic sequence numbers and chained integrity checks; an atomic snapshot records its replay offset. Every committed page retains a durable append. Snapshots occur every 100 transitions, at completion and at an orderly budget stop. This amortizes snapshots without removing file or directory synchronization guarantees.

The journal covers all recovery-relevant state: sealed settings, pagination cursors and loop detection, cursor resets and generations, historical-cutoff phases, discovered parent tasks, coverage and completion. Replay after the snapshot offset is idempotent. A committed raw receipt orphaned before its transition is recovered without repeating its request.

Only an incomplete trailing record lacking its commit delimiter may be discarded. Interior corruption, invalid checksums, missing sequences and contradictory transitions fail closed. Legacy captures retain their original checkpoint and page-journal protocol, including after a root upgrade.

The v2 snapshot remains `captures/<capture_id>/state.json`. Its envelope contains `version`, `capture_id`, logical `state`, `seen_cursors`, `state_sha256`, `snapshot_sha256` and `_journal` (`seq`, `offset`, `sha256`, `transition_count`). The journal checkpoint binds a digest of both logical state and cursor history; recomputing a snapshot's own checksum cannot replace that anchored state. The offset is the byte position after the anchored record's newline.

`pages.jsonl` records contain `version`, `capture_id`, `seq`, `prev`, `op`, `payload` and `sha256`. Each checksum covers canonical JSON excluding its own `sha256` field. The first `start` record has sequence 1 and a previous checksum of 64 zeroes. Page and bounded control transitions increment the transition count; checkpoint records do not. Readers validate the complete chain and snapshot anchor before accepting recovered state, then apply only the suffix after the snapshot. A valid genesis permits recovery when the first snapshot was never written. An existing invalid snapshot is rejected.

Read-only inspection never repairs evidence. Resume takes the canonical writer lock before truncating an undelimited trailing record and syncs the truncation. Never edit a journal or snapshot by hand; preserve the root and investigate a committed-record integrity error instead of discarding it.

### Catalogue versions and selection

Catalogue v1 remains `oddsfox.kalshi.catalogue.v1`, published through `current.json`. Full capture remains the default. Default `load`, `build`, `publish` and metadata capture selection excludes incremental captures, even if a delta is newer. Targeted captures remain separate from catalogue captures.

Opt-in `capture --incremental` pins a certified v2 publication when present, otherwise a certified v1 baseline. Building or publishing a composed capture requires its explicit capture ID. Composed output uses `oddsfox.kalshi.catalogue.v2` and `current-v2.json`; promotion leaves `current.json` untouched. A verified full capture published as version 2 establishes a new baseline.

`current`, `replay`, `rebuild` and `publish` accept `--catalogue-version 1|2`, defaulting to 1. `verify --release` dispatches from the supplied manifest contract. Metadata v1 export rejects incremental captures instead of exporting only their changed subset. Full exports, checksums and independent release verification remain required for both catalogue versions. A pre-existing release destination must match its contract and capture identity as well as its verified files.

A v2 manifest additionally carries `root_id`, `baseline_release_id`, `baseline_manifest_sha256`, `baseline_capture_id`, `previous_watermark`, `watermark_started_at`, `delta_depth` and `membership_sha256`. Baseline fields name the immediate pinned predecessor for a delta and are null for a full rebase. Full depth is zero; each delta increments depth. The membership digest commits sorted scope/entity-kind/native-ticker entries, including membership not visible in the current relations. These fields are also included in v2 coverage, binding them into the release identity. Publication pointers contain the release ID and manifest checksum.

### Incremental acquisition and freshness

Each incremental capture seals a UTC start watermark before its first request. Its ordinary-event and supported non-combo market metadata queries begin at the previous published watermark minus a fixed 300-second overlap. Series, combo events, combo markets and historical markets are fully reconciled on every composed refresh. The watermark advances to the sealed start only after verified publication. Resume retains the original baseline, watermark and query window even if another publication appears.

The first delta from a v1 baseline seeds its previous watermark from that full capture's UTC `started_at`. A full v2 rebase uses the same seed. Completion or manifest timestamps would miss changes made during the baseline traversal.

Overlap deduplicates an observation already identified by the same receipt and observation identity. Distinct receipts remain distinct historical observations; equal ticker or payload values alone are not a deduplication key. Every historical-cutoff receipt remains part of the evidence history, including cutoffs before subsequent historical reads.

Metadata-update filters are not a universal change feed. Trading-only values such as price, volume and open interest may be carried from the baseline with their actual observation timestamps. Updates arriving outside the overlap may require full reconciliation or rebase. Coverage describes the acquired scopes and interval; it does not claim a fresh atomic snapshot of all values.

### Membership, projections and provenance

A composed build copies the certified, checkpointed baseline warehouse into staging. The published baseline is immutable. New observations land idempotently; affected current entities, relationships and dependent combo legs are updated, and full integrity tests still run.

Membership is an explicit input to current projections, distinct from observation history and parent-resolution lookups. A completed full reconciliation replaces only its own scope's membership. If a traversal resets its cursor, only its final completed cursor generation establishes replacement membership; earlier receipts remain observations and evidence. An incomplete traversal cannot remove members. Omission from a delta removes nothing.

Applicable live and historical market memberships are combined before resolving current markets. Removal from current membership retains observation history; reappearance can restore membership. Parent hydration resolves declared relationships without claiming that a lookup enumerated an entire reconciliation scope. Reassignment updates the affected relationship and dependent combo legs.

Current events include listed event membership plus authoritative parents of active markets positively resolved during this capture. Current series include listed series membership plus authoritative parents of active events positively resolved during this capture. A prior observation alone is not a successful fresh parent lookup; a 404 does not restore parent eligibility. Lookup-only parents reachable solely from inactive historical rows do not become permanent members. Removing historical membership does not remove a market still retained by live membership; delta omission supplies no live tombstone, so authoritative removal in that case requires a full rebase.

Current winners remain `received_at DESC, observation_id DESC`. Provider timestamps never silently replace that order. Direct market definitions remain authoritative over event-embedded references. Carried and new rows retain their original source capture, receipt and observation provenance; carried evidence is never relabeled as newly acquired.

Full v2 baselines project the final completed cursor generation's memberships into their own certified warehouse under `builds-v2/<capture_id>`; the v1 warehouse remains unchanged. When a legacy full capture lacks fresh parent-resolution evidence needed by that projection, a new full capture is required. A direct v1 bootstrap preserves the pinned v1 current ordinary events and live non-combo markets with successful non-singular list evidence, including earlier cursor generations; singular hydration and history-only markets cannot seed those scopes. This preserves the selected baseline's semantics until a scope is explicitly reconciled.

Baseline copying requires a closed, checkpointed certified warehouse. Rebuilding the same baseline may change database bytes; accepting that rebuild still requires fresh certification, schema and row-multiset equivalence against the pinned immutable release, and equivalent membership state. Published source databases are never opened for writable checkpointing. Carrying a baseline does not relabel its evidence as newly acquired.

### Ancestry, retention and discard

A composed baseline permits at most eight delta captures. Another delta requires a verified full rebase. Missing, cyclic, corrupted or cross-root ancestry is rejected before build or publication.

Retention preserves the complete evidence closure reachable from both publication pointers and incomplete captures. Incomplete captures stay pinned indefinitely unless explicitly discarded. Retained completed compositions also keep their referenced ancestry. Completed captures at or after the relevant full baseline start remain candidates for publication; an older unreachable group may be pruned together, with children retired before parents. No incomplete recovery pin expires merely because time elapsed.

Explicit discard is allowed only for an incomplete capture that no publication or descendant references. Pruning computes the retained dependency closure before deleting anything, and removes obsolete dependencies only after successful verification and promotion. A failed build, verification or promotion cannot prune evidence needed by either publication or recovery.

### Deterministic output and verification

Certified output proves the export keys unique: native ticker for current series/events/markets; observation ID for history; combo market ticker and leg index for combo legs; parent kind/ticker and child kind/ticker for relationships. Publication orders by those explicit keys, preserving existing lexicographic order where they are already a unique prefix. Payloads, decimal types, relation schemas and Parquet writer settings remain unchanged for compatible full-mode output.

Within one command, evidence validation, hashing, decoding, landing and inventory collection may share verified input bytes. This is not a cross-command trust cache: publish freshly verifies build evidence and checksums, while standalone verification remains independent and complete. Mutation between commands must prevent publication. Metrics and benchmark reports stay outside command-result JSON, evidence, release identities, Git and distributions.
