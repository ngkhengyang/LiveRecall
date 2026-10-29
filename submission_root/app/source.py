"""The one caption-source connection.

A thin connection/protocol-dispatch wrapper. It owns no revision logic and no
message validation of its own -- that lives in ``protocol.py``'s ``from_dict``
methods and in ``CaptionStore`` -- so there is exactly one place where the
revision rules are written down.

Nothing in this read loop ever awaits a client socket:
``ingest_caption_update -> broadcast -> Client.enqueue`` is fully synchronous
and only touches in-memory buffers. That is what keeps the source's ingest
rate independent of the slowest connected client.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .protocol import CaptionUpdate, SourceDone, SourceHello

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from .server import Server


class Source:
    def __init__(self, ws, hello: SourceHello, server: "Server") -> None:
        self.ws = ws
        self.hello = hello
        self.server = server

    async def run(self) -> None:
        print(f"source connected: session={self.hello.session}", flush=True)
        try:
            async for raw in self.ws:
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    # Malformed JSON must never take the server down.
                    continue
                if not isinstance(payload, dict):
                    continue

                kind = payload.get("type")
                if kind == "CAPTION_UPDATE":
                    update = CaptionUpdate.from_dict(payload)
                    if update is not None:
                        self.server.ingest_caption_update(update)
                elif kind == "SOURCE_DONE":
                    done = SourceDone.from_dict(payload)
                    segments = done.segments if done else None
                    # Test-control only; the protocol does not require it to be
                    # forwarded to clients.
                    print(f"source finished: segments={segments}", flush=True)
                else:
                    # Forward compatibility: ignore message types and extra
                    # fields this server does not know about.
                    continue
        finally:
            print("source disconnected", flush=True)
