"""Protocol regressions for individual live frames during shared-log replay."""

import asyncio
import json
import unittest

import websockets

from test_delivery import FakeSocket, cancel_and_join, caption, eventually
from app.client import Client
from app.protocol import ClientHello
from app.server import Server


class LiveDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_replay_preserves_finals_and_latest_active_with_fairness(self):
        server = Server()
        client = Client(FakeSocket(), ClientHello(), server)
        server.register_client(client)
        client._catchup_until = asyncio.get_running_loop().time() + 10
        client._buffer._clock = lambda: 0.0
        for segment in range(1, 9):
            for revision in range(1, 4):
                server.ingest_caption_update(caption(segment, revision, revision == 3))
        server.ingest_caption_update(caption(9, 1, False))
        server.ingest_caption_update(caption(9, 2, False))
        client._buffer._clock = lambda: 2.0
        delivered = []
        for _ in range(9):
            delivery, through_sequence = client._next_delivery()
            delivered.append(delivery)
            client._last_sent_final_sequence = through_sequence
        self.assertTrue(
            all(item.to_dict()["type"] == "CAPTION_UPDATE" for item in delivered)
        )
        self.assertEqual(
            [item.segment for item in delivered if item.final], list(range(1, 9))
        )
        self.assertEqual((delivered[4].segment, delivered[4].revision), (9, 2))
        self.assertFalse(client._buffer.has_pending())

    async def test_probe_windows_send_individual_finals_without_history(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        server.register_client(client)
        client._catchup_until = asyncio.get_running_loop().time() + 10
        holds = []

        async def hold():
            holds.append(len(ws.messages))

        client._hold = hold
        for segment in range(1, 131):
            server.ingest_caption_update(caption(segment))
        sender = asyncio.create_task(client._sender_loop())
        try:
            await eventually(lambda: client._last_sent_final_sequence == 130, 10)
            self.assertEqual([count for count in holds if count < 130], [0, 64, 128])
            self.assertTrue(
                all(item["type"] == "CAPTION_UPDATE" for item in ws.messages)
            )
            self.assertEqual(
                [item["segment"] for item in ws.messages], list(range(1, 131))
            )
        finally:
            await cancel_and_join(sender)

    async def test_wire_history_only_on_hello_even_under_queue_pressure(self):
        server = Server()
        server.ingest_caption_update(caption(1))
        async with websockets.serve(
            server._accept, "127.0.0.1", 0, ping_interval=None
        ) as listener:
            port = listener.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send(ClientHello(history_seconds=60).to_json())
                history = json.loads(await asyncio.wait_for(ws.recv(), 1))
                self.assertEqual(history["type"], "HISTORY")
                self.assertEqual([item["segment"] for item in history["items"]], [1])
                # No await while filling: exercise actual pending-queue pressure.
                for segment in range(2, 102):
                    for revision in range(1, 4):
                        server.ingest_caption_update(
                            caption(segment, revision, revision == 3)
                        )
                for segment in range(2, 102):
                    message = json.loads(await asyncio.wait_for(ws.recv(), 1))
                    self.assertEqual(message["type"], "CAPTION_UPDATE")
                    self.assertEqual(message["segment"], segment)
                    self.assertTrue(message["final"])


if __name__ == "__main__":
    unittest.main()
