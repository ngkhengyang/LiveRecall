"""One receiver's lifecycle, lag policy, and shared-log delivery.

Normal delivery preserves revisions in FIFO order. A late probe or queue
pressure switches the receiver to replay: finals stay in the shared log and
only the latest active state stays in its local buffer. One sender commits
the final cursor after each successful send.
"""

from __future__ import annotations

import asyncio
import contextlib
from enum import Enum
from typing import TYPE_CHECKING, Callable

from websockets.exceptions import ConnectionClosed

from .caption_store import ReplayExpired
from .outbound_buffer import OutboundBuffer
from .protocol import CaptionUpdate, ClientHello, History

if TYPE_CHECKING:
    from .server import Server


class Mode(Enum):
    NORMAL = "normal"
    REPLAY = "replay"


class LagPolicy:
    """RTT-based lag detection and a bounded lease against mode oscillation.

    Timestamps are supplied by the caller so policy decisions need no socket
    or event loop. A pong indicates transport progress, not application ACKs.
    """

    BEHIND_AFTER_SECONDS = 0.5
    BEHIND_RTT_MULTIPLIER = 6.0
    RTT_SMOOTHING = 0.25
    CATCHUP_STICKY_SECONDS = 3.0
    MAX_CATCHUP_SECONDS = 30.0

    def __init__(self) -> None:
        self.rtt = 0.0
        self.catchup_until = 0.0
        self.catchup_lease = self.CATCHUP_STICKY_SECONDS
        self.last_lag_delay = 0.0
        self.normal_trial_started: float | None = None

    def behind_threshold(self) -> float:
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self.rtt)

    def in_catchup(self, now: float) -> bool:
        return now < self.catchup_until

    def lag_started(self, now: float) -> None:
        """Lengthen the lease only when a recent normal trial failed."""
        if self.normal_trial_started is not None and self.last_lag_delay > 0:
            probation = min(
                self.MAX_CATCHUP_SECONDS,
                max(4 * self.CATCHUP_STICKY_SECONDS, 2 * self.last_lag_delay),
            )
            if now - self.normal_trial_started < probation:
                self.catchup_lease = min(
                    self.MAX_CATCHUP_SECONDS, 2 * self.catchup_lease
                )
            else:
                self.catchup_lease = self.CATCHUP_STICKY_SECONDS
        self.normal_trial_started = None

    def still_lagging(self, now: float) -> None:
        self.catchup_until = now + self.catchup_lease

    def probe_finished(
        self, started: float, now: float, latency: object, was_late: bool
    ) -> None:
        if was_late:
            self.last_lag_delay = now - started
        if isinstance(latency, (int, float)):
            # One congestion spike must not establish an enormous RTT baseline.
            sample = min(latency, self.behind_threshold())
            self.rtt += self.RTT_SMOOTHING * (sample - self.rtt)

    def start_normal_trial(self, now: float) -> None:
        self.normal_trial_started = now

    def stop(self) -> None:
        self.catchup_until = 0.0


ReadNextFinal = Callable[[int], tuple[CaptionUpdate, int] | None]


class ReplayCursor:
    """Select replay work while leaving cursor commits to the sender."""

    MAX_FINALS_BEFORE_ACTIVE = 4
    PROVISIONAL_GRACE_SECONDS = 1.0

    def __init__(self) -> None:
        self.position = 0
        self.finals_since_active = 0

    def next_replay(
        self, buffer: OutboundBuffer, read_next_final: ReadNextFinal
    ) -> tuple[CaptionUpdate, int] | None:
        if self.finals_since_active >= self.MAX_FINALS_BEFORE_ACTIVE:
            active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
            if active is not None:
                self.finals_since_active = 0
                return active, self.position
        selected = read_next_final(self.position)
        if selected is not None:
            self.finals_since_active += 1
            return selected
        active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
        if active is not None:
            self.finals_since_active = 0
            return active, self.position
        return None


class Client:
    """Own one receiver's buffer, sender, probes, and mode transitions."""

    TOO_FAR_BEHIND = 1013
    MAX_STALL_SECONDS = 75.0
    CLOSE_TIMEOUT_SECONDS = 2.0
    PROBE_INTERVAL_SECONDS = 0.25
    MAX_BURST_ITEMS = 64
    MAX_BURST_BYTES = 256 * 1024

    def __init__(self, ws, hello: ClientHello, server: "Server") -> None:
        self.ws = ws
        self.hello = hello
        self.server = server
        self._buffer = OutboundBuffer()
        self._lag = LagPolicy()
        self._cursor = ReplayCursor()
        self._mode = Mode.NORMAL
        self._wake = asyncio.Event()
        self._probe_tick = asyncio.Event()
        self._pending_probe: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []
        self._held = 0

    @property
    def _catchup(self) -> bool:
        return self._lag.in_catchup(asyncio.get_running_loop().time())

    async def run(self) -> None:
        self.server.register_client(self)
        try:
            # The helper captures its snapshot before its first network await.
            # Its caption references expire before the long-lived receive loop.
            snapshot_tail = await self._send_initial_history()
            if snapshot_tail is None:
                return
            self._cursor.position = snapshot_tail
            # Ablation: never start the probe loop.
            self._tasks = [asyncio.create_task(self._sender_loop())]
            async for _ in self.ws:
                pass
        except ConnectionClosed:
            pass
        finally:
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
                    await task
            self.server.unregister_client(self)
            print(
                f"client disconnected: id={self.hello.client_id}; "
                f"coalesced={self._buffer.coalesced}; rejected={self._buffer.rejected}; "
                f"held={self._held}; clients={self.server.client_count}",
                flush=True,
            )

    async def _send_initial_history(self) -> int | None:
        """Send the complete hello snapshot, then release its caption references."""
        items, snapshot_tail = self.server.history_snapshot(
            self.hello.last_segment,
            self.hello.last_revision,
            self.hello.history_seconds,
        )
        print(
            f"client connected: id={self.hello.client_id}; "
            f"resume=({self.hello.last_segment},{self.hello.last_revision}); "
            f"history={len(items)}; clients={self.server.client_count}",
            flush=True,
        )
        for history in History.batches(items):
            if not await self._send(history):
                return None
        return snapshot_tail

    def enqueue(self, update: CaptionUpdate) -> None:
        """Accept broadcast work synchronously; never wait for this socket."""
        if self._mode is Mode.REPLAY:
            if update.final:
                self._buffer.discard_segment(update.segment)
            else:
                self._buffer.offer(update)
        else:
            self._buffer.offer(update)
            if self._buffer.pressured or self._buffer.should_close:
                self._escalate_to_replay(update)
        self._wake.set()

    async def _sender_loop(self) -> None:
        try:
            while True:
                await self._wake.wait()
                self._wake.clear()
                sent_items = 0
                sent_bytes = 0
                while self._buffer.has_pending() or self._mode is Mode.REPLAY:
                    if self._buffer.should_close:
                        break
                    if self._window_full(sent_items, sent_bytes):
                        await self._hold()
                        sent_items = 0
                        sent_bytes = 0
                        if self._buffer.should_close:
                            break
                    selected = self._next_delivery()
                    if selected is None:
                        break
                    delivery, through_sequence = selected
                    if not await self._send(delivery):
                        return
                    # The local delivery owns the in-flight message. A successful
                    # send commits transport progress, not an application ACK.
                    self._cursor.position = through_sequence
                    sent_items += 1
                    sent_bytes += len(delivery.to_json())
                if self._buffer.should_close:
                    await self._close("latest active state exceeds delivery capacity")
                    return
        except ReplayExpired:
            await self._close("outstanding finals exceeded shared history retention")
        except ConnectionClosed:
            return

    def _next_delivery(self) -> tuple[CaptionUpdate, int] | None:
        if self._mode is Mode.REPLAY:
            selected = self._cursor.next_replay(
                self._buffer, self.server.read_next_final
            )
            if selected is not None:
                return selected
            if not self._leave_replay_if_caught_up():
                return None
        delivery = self._buffer.next_update()
        if delivery is None:
            return None
        through_sequence = self._cursor.position
        if delivery.final:
            through_sequence = self.server.final_sequence(delivery)
        else:
            self._cursor.finals_since_active = 0
        return delivery, through_sequence

    def _enter_replay(self) -> None:
        if self._mode is not Mode.REPLAY:
            self._mode = Mode.REPLAY
            self._buffer.drop_finals_keep_latest_active()
            print(
                f"client catch-up: id={self.hello.client_id}; replaying shared finals",
                flush=True,
            )
        self._wake.set()

    def _escalate_to_replay(self, update: CaptionUpdate) -> None:
        refused = self._buffer.should_close
        self._enter_replay()
        if update.final:
            # A refused final may have left its older draft in the buffer.
            self._buffer.discard_segment(update.segment)
        elif refused:
            self._buffer.offer(update)

    def _leave_replay_if_caught_up(self) -> bool:
        """Called only after the log has no next final and no ready active item."""
        if self._catchup:
            return False
        self._mode = Mode.NORMAL
        self._buffer.set_coalescing(False)
        self._cursor.finals_since_active = 0
        self._lag.start_normal_trial(asyncio.get_running_loop().time())
        return True

    async def _probe_loop(self) -> None:
        """Keep one round trip; the first late deadline enters replay."""
        try:
            while True:
                loop = asyncio.get_running_loop()
                started = loop.time()
                deadline = started + self.MAX_STALL_SECONDS
                self._pending_probe = asyncio.create_task(self._round_trip())
                was_late = False
                try:
                    latency = await _wait_task(
                        self._pending_probe,
                        min(self._lag.behind_threshold(), self.MAX_STALL_SECONDS),
                    )
                except asyncio.TimeoutError:
                    was_late = True
                    self._lag.lag_started(loop.time())
                    self._enter_replay()
                    while True:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            await self._close("probe exceeded stall budget")
                            return
                        self._lag.still_lagging(loop.time())
                        try:
                            latency = await _wait_task(
                                self._pending_probe,
                                min(self.PROBE_INTERVAL_SECONDS, remaining),
                            )
                            break
                        except asyncio.TimeoutError:
                            pass
                self._pending_probe = None
                self._lag.probe_finished(started, loop.time(), latency, was_late)
                self._probe_tick.set()
                self._wake.set()
                await asyncio.sleep(self.PROBE_INTERVAL_SECONDS)
        except ConnectionClosed:
            return
        finally:
            if self._pending_probe is not None:
                await _stop_task(self._pending_probe)
                self._pending_probe = None
            self._lag.stop()
            self._probe_tick.set()

    async def _round_trip(self) -> float:
        pong = await self.ws.ping()
        return await pong

    def _window_full(self, sent_items: int, sent_bytes: int) -> bool:
        return self._catchup and (
            sent_items == 0
            or sent_items >= self.MAX_BURST_ITEMS
            or sent_bytes >= self.MAX_BURST_BYTES
        )

    async def _hold(self) -> None:
        self._held += 1
        self._probe_tick.clear()
        await self._probe_tick.wait()

    async def _send(self, delivery: CaptionUpdate | History) -> bool:
        sending = asyncio.create_task(self.ws.send(delivery.to_json()))
        try:
            await _wait_task(sending, self.MAX_STALL_SECONDS)
            return True
        except asyncio.TimeoutError:
            await self._close("socket send exceeded stall budget")
            return False
        finally:
            await _stop_task(sending)

    async def _close(self, reason: str) -> None:
        closing = asyncio.create_task(self.ws.close(self.TOO_FAR_BEHIND, reason))
        try:
            await _wait_task(closing, self.CLOSE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            transport = getattr(self.ws, "transport", None)
            if transport is not None:
                transport.abort()
        except ConnectionClosed:
            pass
        finally:
            await _stop_task(closing)


async def _wait_task(task: asyncio.Task, timeout: float):
    """Preserve caller cancellation and keep timed-out probes alive."""
    done, _ = await asyncio.wait((task,), timeout=max(0.0, timeout))
    if not done:
        raise asyncio.TimeoutError
    return task.result()


async def _stop_task(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
        await task
