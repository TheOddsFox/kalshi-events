# Changelog

## 1.0.0

Captures now run at a configurable 10 requests per second by default, shared by all five traversals, and a budget stop exits with code 3 and `complete: false` so the capture can resume. Each build and release comes from exactly one complete catalogue capture, and superseded captures, their raw evidence, builds and releases are pruned only after the new release verifies. The backup and restore commands are removed; `rebuild` and `replay` operate on the current capture.
