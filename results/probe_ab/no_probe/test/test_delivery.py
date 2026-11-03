"""Delivery regressions; run with python -m unittest discover -s test -p 'test_*.py'.

Set LIVERECALL_LONG_STALL=1 to include the real 65-second transport-stall case,
or run it alone with:
    LIVERECALL_LONG_STALL=1 PYTHONPATH=test python -m unittest \
        test_delivery.LongTransportStallTests -v
These tests leave the instructor's source, clients, and measurement logic alone.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
import socket
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "submission_root"))

import websockets

from app.caption_store import CaptionStore, ReplayExpired
from app.client import Client
from app.outbound_buffer import OutboundBuffer
from app.protocol import CaptionUpdate, ClientHello, History
from app.server import Server


def caption(segment, revision=1, final=True, text="caption"):
    return CaptionUpdate("L1", segment, revision, time.time(), final, text)


def final_items(message):
    if message["type"] == "HISTORY":
        return message["items"]
    return [message] if message.get("final") else []


async def eventually(predicate, timeout=1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("test condition did not become true")
        await asyncio.sleep(0.001)


async def cancel_and_join(task):
    task.cancel()
    try:
        await asyncio.wait_for(task, 0.5)
    except asyncio.CancelledError:
        pass
    except asyncio.TimeoutError:
        raise AssertionError("background task ignored cancellation")


class FakeSocket:
    def __init__(self):
        self.messages = []
        self.closed = asyncio.Event()
        self.close_calls = []
        self.before_send = None
        self.ping_started = asyncio.Event()
        self.ping_gate = None
        self.ping_calls = 0
        self.active_pings = 0
        self.max_active_pings = 0
        self.ping_cancelled = asyncio.Event()
        self.pong_future = None
        self.abort_calls = 0
        self.transport = self

    async def send(self, raw):
        message = json.loads(raw)
        if self.before_send is not None:
            await self.before_send(message)
        self.messages.append(message)

    async def ping(self):
        self.ping_calls += 1
        self.active_pings += 1
        self.max_active_pings = max(self.max_active_pings, self.active_pings)
        self.ping_started.set()
        try:
            if self.ping_gate is not None:
                await self.ping_gate.wait()
            future = self.pong_future
            if future is None:
                future = asyncio.get_running_loop().create_future()
                future.set_result(0.001)
            return future
        except asyncio.CancelledError:
            self.ping_cancelled.set()
            raise
        finally:
            self.active_pings -= 1

    async def close(self, code=1000, reason=""):
        self.close_calls.append((code, reason))
        self.closed.set()

    async def wait_closed(self):
        await self.closed.wait()

    def abort(self):
        self.abort_calls += 1
        self.closed.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self.closed.wait()
        raise StopAsyncIteration


class FinalLogTests(unittest.TestCase):
    def setUp(self):
        self.now = [100.0]
        self.store = CaptionStore(clock=lambda: self.now[0])

    def test_delivery_retains_owed_final_outside_requested_context(self):
        update = caption(1)
        self.store.validate_and_apply(update)
        self.now[0] += 65
        self.assertEqual(self.store.compute_history(0, 0, 60), [])
        items, cursor = self.store.read_final_batch(0, 64, 256 * 1024)
        self.assertEqual(items, [update])
        self.assertEqual(cursor, 1)

    def test_empty_context_snapshot_still_has_subscription_boundary(self):
        self.store.validate_and_apply(caption(1))
        items, boundary = self.store.history_snapshot(0, 0, 0)
        self.assertEqual(items, [])
        self.assertEqual(boundary, 1)
        self.store.validate_and_apply(caption(2))
        items, cursor = self.store.read_final_batch(boundary, 64, 256 * 1024)
        self.assertEqual([item.segment for item in items], [2])
        self.assertEqual(cursor, 2)

    def test_expired_owed_prefix_is_not_reported_as_caught_up(self):
        self.store.validate_and_apply(caption(1))
        self.now[0] += self.store.HISTORY_HORIZON_SECONDS + 1
        with self.assertRaises(ReplayExpired):
            self.store.read_final_batch(0, 64, 256 * 1024)
        self.assertEqual(self.store.history_size, 0)
        self.assertEqual(self.store.read_final_batch(1, 64, 256 * 1024), ([], 1))
        self.store.validate_and_apply(caption(2))
        with self.assertRaises(ReplayExpired):
            self.store.read_final_batch(0, 64, 256 * 1024)

    def test_final_sequences_ignore_duplicate_and_unfinished_retirement(self):
        self.store.MAX_ACTIVE_SEGMENTS = 1
        self.store.validate_and_apply(caption(1, final=False))
        self.store.validate_and_apply(caption(2, final=False))
        self.assertEqual(self.store.read_final_batch(0, 64, 256 * 1024), ([], 0))
        update = caption(3)
        self.store.validate_and_apply(update)
        self.assertIsNone(self.store.validate_and_apply(update))
        self.assertIsNone(self.store.validate_and_apply(caption(3, 2)))
        self.assertEqual(self.store.final_tail, 1)
        self.assertEqual(self.store.final_sequence(update), 1)

    def test_batches_limit_items_and_serialized_bytes(self):
        updates = [caption(segment, text="x" * 150) for segment in range(1, 6)]
        for update in updates:
            self.store.validate_and_apply(update)
        items, cursor = self.store.read_final_batch(0, 2, 100000)
        self.assertEqual(items, updates[:2])
        self.assertEqual(cursor, 2)
        budget = len(History(updates[:2]).to_json())
        items, cursor = self.store.read_final_batch(0, 64, budget)
        self.assertEqual(items, updates[:2])
        self.assertEqual(len(History(items).to_json()), budget)
        self.assertEqual(cursor, 2)

    def test_single_large_final_remains_deliverable(self):
        update = caption(1, text="x" * 1000)
        self.store.validate_and_apply(update)
        self.assertEqual(self.store.read_final_batch(0, 64, 200), ([update], 1))


class PendingReplayTests(unittest.TestCase):
    def test_switch_to_replay_preserves_only_latest_active_state(self):
        buffer = OutboundBuffer()
        buffer.offer(caption(1, 1, False))
        buffer.offer(caption(1, 2, True))
        buffer.offer(caption(2, 1, False))
        latest = caption(2, 2, False)
        buffer.offer(latest)
        buffer.begin_replay()
        self.assertEqual(buffer.pending, 1)
        self.assertEqual(buffer.next_update(), latest)
        self.assertFalse(buffer.has_pending())

    def test_replay_transition_recovers_from_final_queue_overflow(self):
        buffer = OutboundBuffer()
        buffer.MAX_PENDING_FINALS = 1
        buffer.offer(caption(1))
        buffer.offer(caption(2))
        self.assertTrue(buffer.should_close)
        buffer.begin_replay()
        self.assertFalse(buffer.should_close)
        self.assertEqual(buffer.pending_bytes, 0)
        buffer.offer(caption(3, final=False))
        buffer.discard_segment(3)
        self.assertEqual(buffer.pending, 0)
        self.assertEqual(buffer.pending_bytes, 0)


class ClientReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_failed_normal_trials_back_off_but_healthy_trial_resets(
        self,
    ):
        client = Client(FakeSocket(), ClientHello(), Server())
        client._last_lag_delay = 65
        client._start_lag(100)
        self.assertEqual(client._catchup_lease, client.CATCHUP_STICKY_SECONDS)
        for started in range(200, 210):
            client._normal_trial_started = started
            client._last_lag_delay = 6
            client._start_lag(started + 8)
        self.assertEqual(client._catchup_lease, client.MAX_CATCHUP_SECONDS)
        client._normal_trial_started = 300
        client._start_lag(400)
        self.assertEqual(client._catchup_lease, client.CATCHUP_STICKY_SECONDS)

    async def stop(self, ws, task):
        ws.closed.set()
        try:
            await asyncio.wait_for(task, 1)
        except asyncio.TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self.fail("client did not clean up after connection closed")

    async def test_normal_burst_delivers_every_revision_in_order(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        server.register_client(client)
        for revision in range(1, 101):
            server.ingest_caption_update(caption(1, revision, revision == 100))
        self.assertFalse(client._replaying)
        sender = asyncio.create_task(client._sender_loop())
        try:
            await eventually(lambda: client._last_sent_final_sequence == 1, 10)
            self.assertEqual(
                [message["revision"] for message in ws.messages], list(range(1, 101))
            )
            self.assertEqual(ws.close_calls, [])
        finally:
            await cancel_and_join(sender)
            server.unregister_client(client)

    async def test_entering_replay_during_final_send_does_not_repeat_it(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        first_started, release_first = asyncio.Event(), asyncio.Event()

        async def before_send(message):
            if message.get("segment") == 1:
                first_started.set()
                await release_first.wait()

        ws.before_send = before_send
        server.register_client(client)
        server.ingest_caption_update(caption(1))
        sender = asyncio.create_task(client._sender_loop())
        try:
            await asyncio.wait_for(first_started.wait(), 1)
            self.assertEqual(client._last_sent_final_sequence, 0)
            server.ingest_caption_update(caption(2))
            client._enter_replay()
            release_first.set()
            await eventually(lambda: client._last_sent_final_sequence == 2)
            received = [
                item["segment"]
                for message in ws.messages
                for item in final_items(message)
            ]
            self.assertEqual(received, [1, 2])
            self.assertEqual(ws.close_calls, [])
        finally:
            release_first.set()
            await cancel_and_join(sender)
            server.unregister_client(client)

    async def test_expired_delivery_prefix_closes_instead_of_skipping_finals(self):
        now = [100.0]
        server, ws = Server(), FakeSocket()
        server._store = CaptionStore(clock=lambda: now[0])
        client = Client(ws, ClientHello(), server)
        server.register_client(client)
        server.ingest_caption_update(caption(1))
        client._enter_replay()
        now[0] += server._store.HISTORY_HORIZON_SECONDS + 1
        try:
            await asyncio.wait_for(client._sender_loop(), 1)
            self.assertEqual(ws.messages, [])
            self.assertEqual(client._last_sent_final_sequence, 0)
            self.assertEqual(len(ws.close_calls), 1)
            self.assertIn("retention", ws.close_calls[0][1])
        finally:
            server.unregister_client(client)

    async def test_final_arrival_removes_pending_active_revision_during_replay(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        server.register_client(client)
        server.ingest_caption_update(caption(1, 1, False))
        client._enter_replay()
        server.ingest_caption_update(caption(1, 2, True))
        self.assertEqual(client._buffer.pending, 0)
        sender = asyncio.create_task(client._sender_loop())
        try:
            await eventually(lambda: client._last_sent_final_sequence == 1)
            self.assertEqual(
                [(message["revision"], message["final"]) for message in ws.messages],
                [(2, True)],
            )
        finally:
            await cancel_and_join(sender)
            server.unregister_client(client)

    async def test_caught_up_client_returns_to_full_revision_delivery(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        client._buffer.MAX_PENDING_UPDATES = 4
        server.register_client(client)
        for segment in range(1, 6):
            server.ingest_caption_update(caption(segment))
        sender = asyncio.create_task(client._sender_loop())
        try:
            await eventually(
                lambda: client._last_sent_final_sequence == 5 and not client._replaying
            )
            for revision in (1, 2, 3):
                server.ingest_caption_update(caption(6, revision, revision == 3))
            await eventually(lambda: client._last_sent_final_sequence == 6)
            revisions = [
                message["revision"]
                for message in ws.messages
                if message.get("segment") == 6
            ]
            self.assertEqual(revisions, [1, 2, 3])
        finally:
            await cancel_and_join(sender)
            server.unregister_client(client)

    async def test_zero_context_does_not_replay_unrequested_old_finals(self):
        server, ws = Server(), FakeSocket()
        server.ingest_caption_update(caption(1))
        client = Client(ws, ClientHello(history_seconds=0), server)
        task = asyncio.create_task(client.run())
        try:
            await eventually(lambda: len(ws.messages) == 1)
            self.assertEqual(ws.messages[0], {"type": "HISTORY", "items": []})
            client._catchup_until = asyncio.get_running_loop().time() + 0.1
            server.ingest_caption_update(caption(2))
            await eventually(lambda: client._last_sent_final_sequence == 2)
            received = [
                item["segment"] for msg in ws.messages for item in final_items(msg)
            ]
            self.assertEqual(received, [2])
        finally:
            await self.stop(ws, task)

    async def test_arrival_during_initial_history_is_not_skipped_or_duplicated(self):
        server, ws = Server(), FakeSocket()
        server.ingest_caption_update(caption(1))
        history_started, release_history = asyncio.Event(), asyncio.Event()
        final_started, release_final = asyncio.Event(), asyncio.Event()

        async def before_send(message):
            if message["type"] == "HISTORY" and not history_started.is_set():
                history_started.set()
                await release_history.wait()
            else:
                final_started.set()
                await release_final.wait()

        ws.before_send = before_send
        client = Client(ws, ClientHello(history_seconds=60), server)
        client._buffer.MAX_PENDING_UPDATES = 4
        task = asyncio.create_task(client.run())
        try:
            await asyncio.wait_for(history_started.wait(), 1)
            for segment in range(2, 8):
                server.ingest_caption_update(caption(segment))
            self.assertTrue(client._replaying)
            release_history.set()
            await asyncio.wait_for(final_started.wait(), 1)
            self.assertEqual(client._last_sent_final_sequence, 1)
            release_final.set()
            await eventually(lambda: client._last_sent_final_sequence == 7)
            received = [
                item["segment"] for msg in ws.messages for item in final_items(msg)
            ]
            self.assertEqual(received, list(range(1, 8)))
        finally:
            release_history.set()
            release_final.set()
            await self.stop(ws, task)

    async def test_final_pressure_switches_to_shared_log_without_loss(self):
        server, ws = Server(), FakeSocket()
        client = Client(ws, ClientHello(), server)
        client._buffer.MAX_PENDING_UPDATES = 4
        server.register_client(client)
        for segment in range(1, 101):
            server.ingest_caption_update(caption(segment))
        self.assertTrue(client._replaying)
        self.assertFalse(client._buffer.should_close)
        self.assertLessEqual(client._buffer.pending, 4)
        sender = asyncio.create_task(client._sender_loop())
        try:
            await eventually(lambda: client._last_sent_final_sequence == 100, 10)
            received = [
                item["segment"] for msg in ws.messages for item in final_items(msg)
            ]
            self.assertEqual(received, list(range(1, 101)))
            self.assertEqual(ws.close_calls, [])
        finally:
            await cancel_and_join(sender)
            server.unregister_client(client)

    async def test_probe_timeout_covers_ping_write_and_cleans_up_one_task(self):
        ws = FakeSocket()
        ws.ping_gate = asyncio.Event()
        client = Client(ws, ClientHello(), Server())
        client.BEHIND_AFTER_SECONDS = 0.02
        client.REPLAY_AFTER_SECONDS = 0.05
        client.MAX_STALL_SECONDS = 0.5
        client.PROBE_INTERVAL_SECONDS = 0.01
        task = asyncio.create_task(client._probe_loop())
        try:
            await asyncio.wait_for(ws.ping_started.wait(), 1)
            await eventually(lambda: client._replaying)
            self.assertEqual(ws.close_calls, [])
            await asyncio.wait_for(task, 1)
            self.assertEqual(ws.ping_calls, 1)
            self.assertEqual(ws.max_active_pings, 1)
            self.assertTrue(ws.ping_cancelled.is_set())
            self.assertEqual(len(ws.close_calls), 1)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_pong_deadline_and_stalled_close_are_both_bounded(self):
        ws = FakeSocket()
        ws.pong_future = asyncio.get_running_loop().create_future()

        async def stalled_close(code, reason):
            ws.close_calls.append((code, reason))
            await asyncio.Event().wait()

        ws.close = stalled_close
        client = Client(ws, ClientHello(), Server())
        client.BEHIND_AFTER_SECONDS = 0.02
        client.REPLAY_AFTER_SECONDS = 0.05
        client.MAX_STALL_SECONDS = 0.5
        client.PROBE_INTERVAL_SECONDS = 0.01
        client.CLOSE_TIMEOUT_SECONDS = 0.02
        await asyncio.wait_for(client._probe_loop(), 1.5)
        self.assertEqual(ws.ping_calls, 1)
        self.assertTrue(ws.pong_future.cancelled())
        self.assertIsNone(client._pending_probe)
        self.assertEqual(len(ws.close_calls), 1)
        self.assertEqual(ws.abort_calls, 1)


@unittest.skipUnless(
    os.environ.get("LIVERECALL_LONG_STALL") == "1", "65-second socket test is opt-in"
)
class LongTransportStallTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_finals_arrive_on_same_connection_after_65_second_stall(self):
        # Match the production event loop. Debug traceback collection on a
        # mounted filesystem would distort this test's real retention budget.
        asyncio.get_running_loop().set_debug(False)
        server = Server()
        receiver = source = None
        async with websockets.serve(
            server._accept, "127.0.0.1", 0, ping_interval=None, max_size=2**20
        ) as listener:
            port = listener.sockets[0].getsockname()[1]
            uri = f"ws://127.0.0.1:{port}"
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                sock.setblocking(False)
                await asyncio.get_running_loop().sock_connect(sock, ("127.0.0.1", port))
                receiver = await websockets.connect(
                    uri, sock=sock, compression=None, max_queue=16, ping_interval=None
                )
                await receiver.send(
                    ClientHello(client_id=17, history_seconds=60).to_json()
                )
                self.assertEqual(json.loads(await receiver.recv())["type"], "HISTORY")
                connection = next(iter(server._clients))
                source = await websockets.connect(uri, compression=None)
                await source.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
                previous_rtt = connection._rtt
                await eventually(
                    lambda: connection._rtt > 0 and connection._rtt != previous_rtt, 3
                )
                connection.ws.transport.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_SNDBUF, 4096
                )
                receiver.transport.pause_reading()
                stalled_at = asyncio.get_running_loop().time()
                for segment in range(1, 801):
                    await source.send(caption(segment, text="x" * 9000).to_json())
                await source.close()
                source = None
                await eventually(lambda: connection._replaying, 3)
                self.assertFalse(connection._buffer.should_close)
                self.assertLessEqual(
                    connection._buffer.pending_bytes,
                    connection._buffer.MAX_PENDING_BYTES,
                )
                await asyncio.sleep(
                    max(0, 65 - (asyncio.get_running_loop().time() - stalled_at))
                )
                self.assertIn(connection, server._clients)
                receiver.transport.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_RCVBUF, 2**20
                )
                connection.ws.transport.get_extra_info("socket").setsockopt(
                    socket.SOL_SOCKET, socket.SO_SNDBUF, 2**20
                )
                receiver.transport.resume_reading()
                received = []
                deadline = asyncio.get_running_loop().time() + 20
                while len(received) < 800:
                    message = json.loads(
                        await asyncio.wait_for(
                            receiver.recv(),
                            max(0.01, deadline - asyncio.get_running_loop().time()),
                        )
                    )
                    received.extend(item["segment"] for item in final_items(message))
                self.assertEqual(received, list(range(1, 801)))
                self.assertIn(connection, server._clients)
                self.assertLessEqual(
                    connection._buffer.pending_bytes,
                    connection._buffer.MAX_PENDING_BYTES,
                )
                await eventually(lambda: not connection._replaying, 5)
                source = await websockets.connect(uri, compression=None)
                await source.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
                for revision in (1, 2, 3):
                    await source.send(caption(801, revision, revision == 3).to_json())
                resumed = [
                    json.loads(await asyncio.wait_for(receiver.recv(), 3))
                    for _ in range(3)
                ]
                self.assertEqual([item["revision"] for item in resumed], [1, 2, 3])
                self.assertTrue(
                    all(item["type"] == "CAPTION_UPDATE" for item in resumed)
                )
            finally:
                for ws in (source, receiver):
                    if ws is not None:
                        ws.transport.abort()


if __name__ == "__main__":
    unittest.main()
