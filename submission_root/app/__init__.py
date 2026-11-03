"""LiveRecall server package.

The required evaluator entry point stays at ``submission_root/solution_server.py``;
it is a thin launcher that delegates to :class:`app.server.Server`.

Layout:
    protocol.py         typed wire messages (no behavior beyond parse/serialize)
    caption_store.py    revision/dedup state + bounded recovery history (pure logic)
    outbound_buffer.py  per-client bounded, coalescing send buffer (pure logic)
    source.py           the one caption-source connection
    client.py           Client lifecycle, LagPolicy, and ReplayCursor
    server.py           coordinator + CLI entry point
"""
