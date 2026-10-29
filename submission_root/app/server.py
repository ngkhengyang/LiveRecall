"""Coordinator for the LiveRecall server, and the process entry point.

Holds the single ``CaptionStore`` and the set of connected clients, accepts new
connections and routes each to a ``Source`` or a ``Client``. It owns no socket
of its own: cleanup is driven by each ``Client`` calling ``unregister_client``
from its own ``finally`` block.
"""

from __future__ import annotations

import argparse
import asyncio
import json

import websockets
from websockets.exceptions import ConnectionClosed

from .caption_store import CaptionStore
from .client import Client
from .protocol import CaptionUpdate, ClientHello, SourceHello
from .source import Source


class Server:
    HELLO_TIMEOUT_SECONDS = 5
    # 1008 "Policy Violation": the connection did not open with a valid hello.
    BAD_HELLO = 1008

    def __init__(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        self._host = host
        self._port = port
        self._store = CaptionStore()
        self._clients: set[Client] = set()

    # -- lifecycle ------------------------------------------------------

    async def run(self) -> None:
        async with websockets.serve(
            self._accept,
            self._host,
            self._port,
            max_size=2**20,
            ping_interval=20,
            ping_timeout=20,
        ):
            print(
                f"LiveRecall server listening on ws://{self._host}:{self._port}",
                flush=True,
            )
            await asyncio.Future()

    async def _accept(self, ws) -> None:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=self.HELLO_TIMEOUT_SECONDS)
            payload = json.loads(raw)
        except (asyncio.TimeoutError, json.JSONDecodeError, ConnectionClosed):
            await ws.close(self.BAD_HELLO, "Expected a valid hello message")
            return

        if not isinstance(payload, dict):
            await ws.close(self.BAD_HELLO, "Expected a JSON object")
            return

        # The one unavoidable raw-dict read in the system: a tagged-union
        # protocol has to look at the tag before it knows which type to parse.
        # Everything downstream of this peek is a typed message object.
        kind = payload.get("type")
        if kind == "SOURCE_HELLO":
            hello = SourceHello.from_dict(payload)
            if hello is None:
                await ws.close(self.BAD_HELLO, "Malformed SOURCE_HELLO")
                return
            await Source(ws, hello, self).run()
        elif kind == "CLIENT_HELLO":
            client_hello = ClientHello.from_dict(payload)
            if client_hello is None:
                await ws.close(self.BAD_HELLO, "Malformed CLIENT_HELLO")
                return
            await Client(ws, client_hello, self).run()
        else:
            await ws.close(self.BAD_HELLO, "Expected SOURCE_HELLO or CLIENT_HELLO")

    # -- client registry ------------------------------------------------

    def register_client(self, client: Client) -> None:
        self._clients.add(client)

    def unregister_client(self, client: Client) -> None:
        self._clients.discard(client)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # -- caption flow ---------------------------------------------------

    def compute_history(
        self, last_segment: int, last_revision: int
    ) -> list[CaptionUpdate]:
        return self._store.compute_history(last_segment, last_revision)

    def ingest_caption_update(self, update: CaptionUpdate) -> None:
        """Called by ``Source``. Broadcasts only genuinely new state."""
        accepted = self._store.validate_and_apply(update)
        if accepted is not None:
            self.broadcast(accepted)

    def broadcast(self, update: CaptionUpdate) -> None:
        """Hand one update to every client without touching the network.

        This is why slow-client isolation holds: ``enqueue`` only writes to an
        in-memory buffer, so no client's receive rate can delay delivery to any
        other client, and the source's read loop never blocks here. It is also
        why this loop is safe without a snapshot copy -- there is no await
        inside it, so no client can unregister midway through.
        """
        for client in self._clients:
            client.enqueue(update)

    # -- entry point ----------------------------------------------------

    @classmethod
    def main(cls, argv: list[str] | None = None) -> None:
        parser = argparse.ArgumentParser(description="LiveRecall server")
        parser.add_argument("--port", type=int, default=8765)
        parser.add_argument(
            "--host",
            default="0.0.0.0",
            help="Bind address; 0.0.0.0 also exposes the server to other VM interfaces",
        )
        args = parser.parse_args(argv)
        try:
            asyncio.run(cls(args.host, args.port).run())
        except KeyboardInterrupt:
            pass
