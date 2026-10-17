# Changelog

## 1.0.0

Captures now run at a configurable 10 requests per second by default, shared by all five traversals, and a budget stop exits with code 3 and `complete: false` so the capture can resume. Each build and release comes from exactly one complete catalogue capture, and superseded captures, their raw evidence, builds and releases are pruned only after the new release verifies. The backup and restore commands are removed; `rebuild` and `replay` operate on the current capture.

Captures made before 1.0.0 are not readable; a new operator root is required. Reading a stored page checks its size and then reads the file, so a large byte ceiling does not allocate that ceiling.
