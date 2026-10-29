#!/usr/bin/env python3
"""CS3103 Assignment 4 - LiveRecall server entry point.

Required evaluator command (run from submission_root/):

    python solution_server.py --port 8765

This file is deliberately a thin launcher. The implementation lives in the
``app`` package next to it:

    app/protocol.py         typed wire messages
    app/caption_store.py    revision/dedup state + bounded recovery history
    app/outbound_buffer.py  per-client bounded, coalescing send buffer
    app/source.py           the caption-source connection
    app/client.py           one browser / load-test client connection
    app/server.py           coordinator, and the --port CLI

See diagrams/server_oop_design.md for the design these classes implement.
"""

from __future__ import annotations

import sys
from pathlib import Path

# The public harness starts this script from two different working directories
# (test/submission_check.py uses submission_root/, test/run_demo.py uses the
# repository root), so the package is resolved from this file's own location
# rather than from the cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.server import Server  # noqa: E402  (import follows the sys.path guard)

if __name__ == "__main__":
    Server.main()
