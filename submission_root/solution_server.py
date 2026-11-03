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

from app.server import Server

if __name__ == "__main__":
    Server.main()
