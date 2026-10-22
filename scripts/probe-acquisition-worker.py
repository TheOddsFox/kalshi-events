#!/usr/bin/env python3
"""Private single-process worker used by the throughput acquisition probe."""

from __future__ import annotations

import sys
from pathlib import Path

if __name__ == "__main__":
    SOURCE_ROOT = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(SOURCE_ROOT / "src"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from acquisition_probe import worker_main

    raise SystemExit(worker_main(sys.argv[1:]))
