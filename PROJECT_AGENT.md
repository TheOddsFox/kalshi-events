# Project agent notes

Public Kalshi event-contract catalogue. Own native series/events/markets, definitions, lifecycle, settlement and metadata handoffs. Stack: Python3.12, uv, dlt, dbt, DuckDB; Dagster is optional and unscheduled.

## Invariants

- Keep oddsfox-pipeline unchanged; no runtime imports, shared warehouse or state migration.
- Preserve raw evidence, native tickers, exact decimals and honest per-selection coverage.
- Bounded requests, downloads, responses, duration and storage; commit raw before checkpoints.
- One canonical writer lock per root; failed/incomplete work cannot promote publication.
- Tests are offline; persistent project datasets, operator roots, metrics, metadata and diagnostic/benchmark outputs stay beneath the canonical checkout’s ignored `data/` directory. Preserve it during cleanup. Runtime environments and isolated installed-consumer checks remain separate; data, credentials and Pad content stay outside Git and distributions.
- No trading, authenticated WebSocket recording, activated schedules, commit or push.

## Verification

Use scripts/verify-fast and scripts/verify, installed wheels outside checkouts and synthetic source fixtures.
