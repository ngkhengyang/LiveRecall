"""Regression checks for live delivery without unsolicited HISTORY."""

import asyncio
import contextlib
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "submission_root"))

import websockets

from app.client import Client
from app.protocol import CaptionUpdate, ClientHello
from app.server import Server


def caption(segment, revision=1, final=False):
    return CaptionUpdate("L1", segment, revision, 1000.0, final, "caption")


class LiveDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_delivery_preserves_all_revisions(self):
        client = Client(None, ClientHello(), None)
        for revision in range(1, 4):
            client.enqueue(caption(1, revision, revision == 3))
        self.assertEqual(
            [client._next_delivery().revision for _ in range(3)], [1, 2, 3]
        )

    async def test_catchup_preserves_finals_and_latest_active_with_fairness(self):
        client = Client(None, ClientHello(), None)
        client._catchup_until = asyncio.get_running_loop().time() + 10
        client._buffer._clock = lambda: 0.0
        for segment in range(1, 9):
            for revision in range(1, 4):
                client.enqueue(caption(segment, revision, revision == 3))
        client.enqueue(caption(9, 1))
        client.enqueue(caption(9, 2))
        client._buffer._clock = lambda: 2.0
        delivered = [client._next_delivery() for _ in range(9)]
        self.assertTrue(all(isinstance(item, CaptionUpdate) for item in delivered))
        self.assertEqual(
            [item.segment for item in delivered if item.final], list(range(1, 9))
        )
        self.assertEqual((delivered[4].segment, delivered[4].revision), (9, 2))
        self.assertFalse(client._buffer.has_pending())
        client._catchup_until = 0
        for revision in (1, 2):
            client.enqueue(caption(10, revision))
        self.assertEqual([client._next_delivery().revision for _ in range(2)], [1, 2])

    async def test_sender_holds_once_for_a_backlog_not_once_per_final(self):
        sent = []
        holds = []
        complete = asyncio.Event()

        class Socket:
            async def send(self, raw):
                sent.append(json.loads(raw))
                if len(sent) == 20:
                    complete.set()

        client = Client(Socket(), ClientHello(), None)
        client._catchup_until = asyncio.get_running_loop().time() + 10

        async def hold():
            holds.append(True)

        client._hold = hold
        for segment in range(1, 21):
            client.enqueue(caption(segment, final=True))
        sender = asyncio.create_task(client._sender_loop())
        try:
            await asyncio.wait_for(complete.wait(), 1)
            self.assertEqual(len(holds), 1)
            self.assertTrue(all(item["type"] == "CAPTION_UPDATE" for item in sent))
            self.assertEqual([item["segment"] for item in sent], list(range(1, 21)))
        finally:
            sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender

    async def test_overdue_pong_keeps_coalescing_without_forced_reconnect(self):
        pong = asyncio.get_running_loop().create_future()

        class Socket:
            pings = 0

            async def ping(self):
                self.pings += 1
                return pong

            async def close(self, *args):
                raise AssertionError("Lag alone must not force history recovery")

        client = Client(Socket(), ClientHello(), None)
        client.BEHIND_AFTER_SECONDS = 0.005
        client.MAX_HOLD_SECONDS = 0.005
        client.CATCHUP_STICKY_SECONDS = 0.02
        probe = asyncio.create_task(client._probe_loop())
        try:
            await asyncio.sleep(0.08)
            self.assertFalse(probe.done())
            self.assertTrue(client._catchup)
            self.assertEqual(client.ws.pings, 1)
            pong.set_result(0.2)
            await asyncio.wait_for(client._probe_tick.wait(), 1)
            self.assertGreater(
                client._catchup_until - asyncio.get_running_loop().time(), 0.1
            )
        finally:
            probe.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await probe

    async def test_wire_history_only_on_hello_even_under_queue_pressure(self):
        server = Server()
        server.ingest_caption_update(caption(1, final=True))
        async with websockets.serve(server._accept, "127.0.0.1", 0) as listener:
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
