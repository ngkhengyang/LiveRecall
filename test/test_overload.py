"""Regression tests for bounded delivery and final batching.

Run: python -m unittest discover -s test -p test_overload.py
The supplied instructor workload and measurement programs are unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "submission_root"))

import websockets

from app.client import Client
from app.outbound_buffer import OutboundBuffer
from app.protocol import CaptionUpdate, ClientHello, History
from app.server import Server


def caption(segment, revision=1, final=False, text="caption", source_ts=None):
    return CaptionUpdate(
        "L1",
        segment,
        revision,
        time.time() if source_ts is None else source_ts,
        final,
        text,
    )


class BufferTests(unittest.TestCase):
    def test_distinct_active_segments_are_not_evicted_at_soft_limit(self):
        buffer = OutboundBuffer()
        for segment in range(1, 301):
            buffer.offer(caption(segment))
            buffer.offer(caption(segment, 7))
        self.assertFalse(buffer.should_close)
        received = []
        while buffer.has_pending():
            received.append(buffer.next_update())
        self.assertEqual([u.segment for u in received], list(range(1, 301)))
        self.assertTrue(all(u.revision == 7 for u in received))
        self.assertEqual(buffer.pending_bytes, 0)

    def test_full_final_backlog_preserves_new_active_segment(self):
        buffer = OutboundBuffer()
        for segment in range(1, 257):
            buffer.offer(caption(segment, final=True))
        buffer.offer(caption(257))
        finals = []
        while batch := buffer.take_final_batch():
            self.assertTrue(all(u.final for u in batch))
            finals.extend(batch)
        self.assertEqual([u.segment for u in finals], list(range(1, 257)))
        self.assertEqual(buffer.next_update().segment, 257)
        self.assertFalse(buffer.should_close)

    def test_old_pending_final_is_not_filtered_by_recovery_window(self):
        clock = [0.0]
        buffer = OutboundBuffer(clock=lambda: clock[0])
        buffer.offer(caption(1, final=True, source_ts=0))
        clock[0] = 200
        self.assertEqual([u.segment for u in buffer.take_final_batch()], [1])

    def test_batches_are_ordered_final_only_and_frame_bounded(self):
        buffer = OutboundBuffer()
        buffer.MAX_BATCH_ITEMS = 3
        buffer.MAX_BATCH_BYTES = 600
        for segment in (9, 1, 7, 3, 5):
            buffer.offer(caption(segment, final=True, text="\u4e2d" * 20))
        buffer.offer(caption(10))
        delivered = []
        while batch := buffer.take_final_batch():
            self.assertLessEqual(len(batch), 3)
            self.assertLessEqual(len(History(batch).to_json().encode()), 600)
            self.assertTrue(all(u.final for u in batch))
            delivered.extend(u.segment for u in batch)
        self.assertEqual(delivered, [1, 3, 5, 7, 9])
        self.assertEqual(buffer.next_update().segment, 10)

    def test_large_final_falls_back_to_individual_delivery(self):
        buffer = OutboundBuffer()
        buffer.MAX_BATCH_BYTES = 200
        update = caption(1, final=True, text="x" * 300)
        buffer.offer(update)
        self.assertEqual(buffer.take_final_batch(), [])
        self.assertEqual(buffer.next_update(prefer_final=True), update)

    def test_hard_limits_fail_explicitly_without_evicting_pending_state(self):
        for final in (False, True):
            with self.subTest(final=final):
                buffer = OutboundBuffer()
                buffer.MAX_PENDING_ACTIVE_SEGMENTS = 2
                buffer.MAX_PENDING_FINALS = 2
                buffer.offer(caption(1, final=final))
                buffer.offer(caption(2, final=final))
                buffer.offer(caption(3, final=final))
                buffer.offer(caption(4, final=final))
                self.assertTrue(buffer.should_close)
                self.assertEqual(buffer.rejected, 2)
                self.assertEqual(buffer.pending, 2)
                self.assertEqual(buffer.next_update().segment, 1)
                self.assertEqual(buffer.next_update().segment, 2)

    def test_byte_limit_rejects_growth_without_replacing_existing_state(self):
        buffer = OutboundBuffer()
        original = caption(1)
        buffer.MAX_PENDING_BYTES = len(original.to_json())
        buffer.offer(original)
        buffer.offer(caption(1, 2, text="x" * 200))
        self.assertTrue(buffer.should_close)
        self.assertEqual(buffer.pending_bytes, len(original.to_json()))
        self.assertEqual(buffer.next_update(), original)

    def test_finalizing_active_segment_updates_capacity_accounting(self):
        buffer = OutboundBuffer()
        buffer.MAX_PENDING_ACTIVE_SEGMENTS = 1
        buffer.offer(caption(1))
        buffer.offer(caption(1, 2, final=True))
        buffer.offer(caption(2))
        self.assertFalse(buffer.should_close)
        self.assertEqual([u.segment for u in buffer.take_final_batch()], [1])
        self.assertEqual(buffer.next_update().segment, 2)
        self.assertEqual(buffer.pending_bytes, 0)


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_delivery_keeps_full_revision_sequence(self):
        client = Client(None, ClientHello(), None)
        for revision in range(1, 8):
            update = caption(1, revision, final=revision == 7)
            client.enqueue(update)
            self.assertEqual(client._next_delivery(), update)

    async def test_active_segment_is_not_starved_by_continuing_final_batches(self):
        clock = [0.0]
        client = Client(None, ClientHello(), None)
        client._buffer = OutboundBuffer(clock=lambda: clock[0])
        client._catchup_until = asyncio.get_running_loop().time() + 60
        client.enqueue(caption(1))
        for segment in range(2, 402):
            client.enqueue(caption(segment, final=True))
        clock[0] = 2
        for _ in range(4):
            self.assertIsInstance(client._next_delivery(), History)
        active = client._next_delivery()
        self.assertIsInstance(active, CaptionUpdate)
        self.assertEqual(active.segment, 1)

    async def test_in_flight_batch_and_arriving_final_have_single_ownership(self):
        entered, release = asyncio.Event(), asyncio.Event()
        messages = []

        class Socket:
            async def send(self, raw):
                messages.append(json.loads(raw))
                if len(messages) == 1:
                    entered.set()
                    await release.wait()

        client = Client(Socket(), ClientHello(), None)
        for segment in range(1, 257):
            client.enqueue(caption(segment, final=True))
        client.enqueue(caption(257))
        task = asyncio.create_task(client._sender_loop())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.assertIsInstance(client._in_flight, History)
            self.assertEqual(len(client._in_flight.items), 64)
            client.enqueue(caption(257, 2, final=True))
            release.set()
            for _ in range(20):
                if not client._buffer.has_pending() and client._in_flight is None:
                    break
                await asyncio.sleep(0)
            delivered = []
            for msg in messages:
                delivered.extend(msg["items"] if msg["type"] == "HISTORY" else [msg])
            self.assertEqual([u["segment"] for u in delivered], list(range(1, 258)))
            self.assertTrue(all(u["final"] for u in delivered))
            self.assertFalse(client._buffer.has_pending())
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_websocket_backlog_batches_finals_and_preserves_active_state(self):
        server = Server()
        async with websockets.serve(server._accept, "127.0.0.1", 0) as listener:
            port = listener.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                await ws.send(ClientHello().to_json())
                initial = json.loads(await asyncio.wait_for(ws.recv(), 1))
                self.assertEqual(initial, {"type": "HISTORY", "items": []})
                # No await between ingests: every update is queued before the
                # sender drains, reproducing temporary application backlog.
                for segment in range(1, 301):
                    server.ingest_caption_update(caption(segment, final=True))
                server.ingest_caption_update(caption(301))
                received, batches = [], 0
                while len(received) < 301:
                    message = json.loads(await asyncio.wait_for(ws.recv(), 2))
                    if message["type"] == "HISTORY":
                        batches += 1
                        self.assertTrue(all(u["final"] for u in message["items"]))
                        received.extend(message["items"])
                    else:
                        received.append(message)
                self.assertGreater(batches, 0)
                self.assertEqual([u["segment"] for u in received], list(range(1, 302)))
                self.assertFalse(received[-1]["final"])


if __name__ == "__main__":
    unittest.main()
