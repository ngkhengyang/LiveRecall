"""One browser / load-test client connection.

A thin connection wrapper around one :class:`OutboundBuffer`, plus the tasks
the design calls for: the ``run`` loop that watches for disconnect, an
independent ``_sender_loop`` that drains the buffer, and a ``_probe_loop`` that
measures whether this client is actually consuming what it is sent.

Keeping the sender separate from ``run`` is what lets ``Server.broadcast`` hand
this client work without ever awaiting its socket.

Why the probe loop exists
-------------------------
A slow reader produces no send-side signal at all. Measured on loopback with
the public workload, ``transport.get_write_buffer_size()`` stays at 0 for the
entire run and ``ws.send`` never blocks: the whole caption stream is small
enough to sit in kernel socket buffers, so the backlog is invisible and the
outbound buffer never fills. A design that waits for ``send`` to push back
therefore never coalesces anything, and a slow client spends its limited read
budget on obsolete provisional revisions instead of finals.

WebSocket ping/pong does expose it. The client library stops reading from the
transport once its own inbound queue is full, so its pong is delayed by exactly
the condition we care about -- the application is not keeping up. The same
measurement showed pong latency moving from 0.5 ms to repeated multi-second
timeouts as soon as the reader fell behind.

So this client pings continuously, and while a pong is overdue the sender holds
off instead of pushing more. That hold is what lets superseded provisional
revisions collapse in :class:`OutboundBuffer` rather than queue downstream, and
it paces delivery to what the client is demonstrably consuming. A client that
is keeping up never waits, so normal clients pay nothing for it.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
from typing import TYPE_CHECKING

from websockets.exceptions import ConnectionClosed

from .outbound_buffer import OutboundBuffer
from .protocol import CaptionUpdate, ClientHello, History, Pad

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from .server import Server


ABLATIONS = {
    name.strip()
    for name in os.environ.get("LIVERECALL_ABLATE", "").split(",")
    if name.strip()
}
if "all" in ABLATIONS:
    ABLATIONS.update(("probe", "grace", "resync"))


class Client:
    # Matches the 1013 "Try Again Later" close used when a reader falls too far
    # behind to stay inside its buffer bound.
    TOO_FAR_BEHIND = 1013

    # Gap between ordinary backlog pings.
    PROBE_INTERVAL_SECONDS = 0.25
    # Floors for the two verdicts below. Both are also scaled against this
    # connection's own measured round trip, so a link with real delay and
    # jitter is judged against what is normal *for that link* rather than
    # against a loopback constant. The supplied impairment profile alone is
    # 120 ms +/- 30 ms with 3% loss, which a fixed threshold would read as a
    # stalled reader.
    BEHIND_AFTER_SECONDS = 0.5
    # How long one overdue pong keeps this client in catch-up mode. A reader
    # that is behind drains just enough to answer a ping and then falls back,
    # so without a sticky window the sender resumes flooding the moment a
    # single pong lands and the client spends its read budget on revisions
    # that are obsolete by the time it reaches them.
    CATCHUP_STICKY_SECONDS = 3.0
    # Lack of any acknowledged progress, rather than ordinary slowness, is
    # grounds for resync. Leave time for a full receive queue to drain first.
    RESYNC_AFTER_SECONDS = 30.0
    BEHIND_RTT_MULTIPLIER = 6.0
    RESYNC_RTT_MULTIPLIER = 18.0
    RTT_SMOOTHING = 0.25
    RATE_SMOOTHING = 0.25
    PACING_MARGIN = 1.1
    PACING_BACKOFF = 1.25
    # A pong acknowledges the library's read, not application consumption.
    # Reserve half the observed stall again for the unknown queue tail; this
    # estimate uses no assumed peer queue size or source revision frequency.
    DRAIN_MARGIN = 0.5
    # Prompt pongs alone cannot prove consumption. Ignored pads after a real
    # paced frame expose its processing delay without adding caption work.
    CONSUMPTION_PROBE_INTERVAL = 2.0
    CONSUMPTION_PROBE_MAX_INTERVAL = 4.0
    # Exceed the typical receive-queue high watermark (websockets default 16).
    # Without such a queue, prompt pongs fall back to overdue-pong re-detection.
    PROBE_PAD_FRAMES = 24
    # The padprobe experiment saw 2 ms false pongs at zero gap, but 506 ms
    # at 20 ms: let pads pause the peer's reads before the ping arrives.
    PROBE_PING_GAP_SECONDS = 0.02
    PROBE_PING_GAP_RTT_MULTIPLIER = 4.0
    OFFERED_RATE_WINDOW_SECONDS = 2.0
    RECOVERY_UTILISATION = 0.5
    RECOVERY_CONFIRMATIONS = 3
    # While behind, hold a lone provisional revision this long before sending
    # it. The deterministic source finalizes a segment in about 0.4 s, so the
    # revision is normally replaced in place by its own final and one message
    # carries what three would have.
    PROVISIONAL_GRACE_SECONDS = 1.0

    def __init__(self, ws, hello: ClientHello, server: "Server") -> None:
        self.ws = ws
        self.hello = hello
        self.server = server
        self._buffer = OutboundBuffer()
        self._wake = asyncio.Event()
        self._probe_tick = asyncio.Event()
        self._catchup_until = 0.0
        self._rtt = 0.0
        self._sent_frames = 0
        self._pong_overdue = False
        self._rate = 0.0
        self._pace_interval = 0.0
        self._next_send_at = 0.0
        self._unread_frames = 0.0
        self._unread_at = 0.0
        self._offered_rate = 0.0
        self._offered_at = 0.0
        self._consumption_probe_at = 0.0
        self._consumption_probe_interval = self.CONSUMPTION_PROBE_INTERVAL
        self._consumption_probe_done = asyncio.Event()
        self._consumption_probe_done.set()
        self._backlog_probe_done = asyncio.Event()
        self._backlog_probe_done.set()
        self._consumption_probe_task: asyncio.Task | None = None
        self._recovery_confirmations = 0
        self._held = 0
        self._batches_sent = 0
        self._batched_finals = 0
        self._in_flight: CaptionUpdate | History | None = None
        self._tasks: list[asyncio.Task] = []

    def _behind_threshold(self) -> float:
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self._rtt)

    def _resync_threshold(self) -> float:
        return max(
            self.RESYNC_AFTER_SECONDS,
            self.RESYNC_RTT_MULTIPLIER * self._rtt,
            self.RESYNC_RTT_MULTIPLIER * self._pace_interval,
        )

    @property
    def _catchup(self) -> bool:
        """True while a recently overdue pong still governs this client."""
        return (
            self._pong_overdue
            or self._pace_interval > 0.0
            or asyncio.get_running_loop().time() < self._catchup_until
        )

    async def run(self) -> None:
        # Step order here is load-bearing; see the note at the end of this
        # method for why registration must precede the history snapshot with no
        # await in between.
        self.server.register_client(self)
        items = self.server.compute_history(
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

        try:
            # HISTORY is always the first frame this client sees: the sender
            # task that could emit live updates does not exist until after it
            # has been sent.
            await self.ws.send(History(items).to_json())
            self._sent_frames += 1
            self._tasks = [asyncio.create_task(self._sender_loop())]
            if "probe" not in ABLATIONS:
                self._tasks.append(asyncio.create_task(self._probe_loop()))
            # Clients send nothing after the hello. This loop exists to notice
            # the disconnect and to let the library's keepalive run.
            async for _ in self.ws:
                pass
        except ConnectionClosed:
            pass
        finally:
            tasks = tuple(self._tasks)
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
                    await task
            self.server.unregister_client(self)
            print(
                f"client disconnected: id={self.hello.client_id}; "
                f"coalesced={self._buffer.coalesced}; rejected={self._buffer.rejected}; "
                f"held={self._held}; batches={self._batches_sent}; "
                f"batched_finals={self._batched_finals}; clients={self.server.client_count}",
                flush=True,
            )

        # Why register_client runs before compute_history, synchronously:
        #
        # If the snapshot were taken first, a caption finalized in the gap
        # would be broadcast while this client was not yet in the registry --
        # enqueue would simply never be called for it, leaving a silent hole
        # that no later message repairs.
        #
        # The opposite risk, a final appearing in both the snapshot and a live
        # update, cannot happen: CaptionStore's final-durability rule appends
        # each segment to the history exactly once and nothing re-broadcasts an
        # already-final segment. So a given final is either in the history when
        # compute_history reads it (delivered via HISTORY) or becomes final
        # strictly afterwards (delivered live) -- never both.
        #
        # Both calls being synchronous closes the remaining window: within one
        # event loop nothing, including the source's read loop, can interleave
        # between two back-to-back non-awaiting calls.

    def enqueue(self, update: CaptionUpdate) -> None:
        """Hand this client one update. Never blocks, never awaits.

        Called from ``Server.broadcast``. It only mutates in-memory buffer
        state, so a stalled reader here cannot delay any other client.
        """
        now = asyncio.get_running_loop().time()
        self._offered_rate *= math.exp(
            -(now - self._offered_at) / self.OFFERED_RATE_WINDOW_SECONDS
        )
        self._offered_rate += 1.0 / self.OFFERED_RATE_WINDOW_SECONDS
        self._offered_at = now
        self._buffer.offer(update)
        self._wake.set()
        if self._buffer.should_close:
            self._probe_tick.set()

    # -- tasks ----------------------------------------------------------

    async def _sender_loop(self) -> None:
        try:
            while True:
                await self._wake.wait()
                # Cleared before draining. enqueue mutates the buffer before it
                # sets the event, so an enqueue racing this clear is still seen
                # by the has_pending check below.
                self._wake.clear()

                while self._buffer.has_pending():
                    if self._buffer.should_close:
                        break
                    if self._catchup:
                        # Keep ownership in the buffer until a consumption
                        # slot opens, so finals can join the same batch.
                        await self._hold()
                        if self._buffer.should_close or not self._buffer.has_pending():
                            break
                    delivery = self._next_delivery()
                    if delivery is None:
                        # Only a still-fresh provisional is waiting. Leave it
                        # to be superseded; the probe loop re-wakes us so it
                        # still goes out once it ages past the grace.
                        break
                    # Dequeue transfers ownership to this in-flight message;
                    # completing send is not an application-level client ACK.
                    self._in_flight = delivery
                    sent_at = asyncio.get_running_loop().time()
                    await self.ws.send(delivery.to_json())
                    self._sent_frames += 1
                    if self._pace_interval:
                        now = asyncio.get_running_loop().time()
                        self._drain_estimate(now)
                        self._unread_frames += 1.0
                        self._next_send_at = now + self._pace_interval
                        await self._consumption_probe(sent_at)
                    self._in_flight = None
                    if isinstance(delivery, History):
                        self._batches_sent += 1
                        self._batched_finals += len(delivery.items)

                if self._buffer.should_close:
                    await self.ws.close(
                        self.TOO_FAR_BEHIND, "pending delivery capacity exhausted"
                    )
                    return
        except ConnectionClosed:
            return

    def _next_delivery(self) -> CaptionUpdate | History | None:
        """Batch unsent finals only during catch-up or actual queue pressure."""
        catching_up = self._catchup or self._buffer.pressured
        grace = 0.0 if "grace" in ABLATIONS else self.PROVISIONAL_GRACE_SECONDS
        if catching_up:
            # Spend each slow-reader slot on all available finals before a
            # provisional; the newest active revision remains in the buffer.
            items = self._buffer.take_final_batch()
            if items:
                return History(items)
        delivery = self._buffer.next_update(
            prefer_final=catching_up,
            provisional_grace=grace,
        )
        return delivery

    def _drain_estimate(self, now: float) -> None:
        self._unread_frames = max(
            0.0, self._unread_frames - self._rate * (now - self._unread_at)
        )
        self._unread_at = now

    async def _hold(self) -> None:
        """Wait for a usable slot; an overdue pong never permits a send."""
        self._held += 1
        loop = asyncio.get_running_loop()
        while not self._buffer.should_close:
            self._probe_tick.clear()
            now = loop.time()
            self._drain_estimate(now)
            delay = max(
                self._next_send_at - now,
                self._unread_frames / self._rate if self._rate else 0.0,
            )
            if not self._pong_overdue and delay <= 0.0:
                return
            if self._pong_overdue:
                await self._probe_tick.wait()
            else:
                # A timer opens a slot, never overrides an overdue pong. A
                # probe waking us early simply rechecks the current estimate.
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._probe_tick.wait(), timeout=delay)

    async def _consumption_probe(self, sent_at: float) -> None:
        """Measure consumption after a real paced frame, with bounded padding."""
        if (
            "probe" in ABLATIONS
            or not self._pace_interval
            or self._pong_overdue
            or not self._backlog_probe_done.is_set()
            or (
                self._consumption_probe_task is not None
                and not self._consumption_probe_task.done()
            )
            or sent_at - self._consumption_probe_at < self._consumption_probe_interval
        ):
            return
        self._consumption_probe_at = sent_at
        # Start only between backlog pings, and fence them until the probe finishes.
        self._consumption_probe_done.clear()
        task = None
        try:
            pad = Pad().to_json()
            for _ in range(self.PROBE_PAD_FRAMES):
                await self.ws.send(pad)
            # Only padding stays inline, so a probe's gap and pong never hold finals.
            task = asyncio.create_task(
                self._finish_consumption_probe(sent_at, self._sent_frames)
            )
            self._consumption_probe_task = task
            self._tasks.append(task)
            task.add_done_callback(self._tasks.remove)
            task.add_done_callback(lambda _: self._consumption_probe_done.set())
        finally:
            if task is None:
                self._consumption_probe_done.set()

    async def _finish_consumption_probe(self, sent_at: float, sent_frames: int) -> None:
        """Include the separation gap in the real frame's consumption delay."""
        try:
            await asyncio.sleep(
                max(
                    self.PROBE_PING_GAP_SECONDS,
                    self.PROBE_PING_GAP_RTT_MULTIPLIER * self._rtt,
                )
            )
            # A later real frame changes the queue under measurement. Require
            # a fresh run of confirmations rather than trusting this sample.
            if self._sent_frames != sent_frames or self._pong_overdue:
                self._recovery_confirmations = 0
                return
            pong = await self.ws.ping()
            await asyncio.wait_for(
                asyncio.shield(pong), timeout=self._behind_threshold()
            )
        except asyncio.TimeoutError:
            self._consumption_probe_interval = min(
                self.CONSUMPTION_PROBE_MAX_INTERVAL,
                2.0 * self._consumption_probe_interval,
            )
            self._recovery_confirmations = 0
            return
        except ConnectionClosed:
            self._recovery_confirmations = 0
            return
        now = asyncio.get_running_loop().time()
        offered_rate = self._offered_rate * math.exp(
            -(now - self._offered_at) / self.OFFERED_RATE_WINDOW_SECONDS
        )
        if (
            not self._pong_overdue
            and offered_rate > 0.0
            and (now - sent_at) * offered_rate < self.RECOVERY_UTILISATION
        ):
            self._consumption_probe_interval = self.CONSUMPTION_PROBE_INTERVAL
            self._recovery_confirmations += 1
        else:
            self._consumption_probe_interval = min(
                self.CONSUMPTION_PROBE_MAX_INTERVAL,
                2.0 * self._consumption_probe_interval,
            )
            self._recovery_confirmations = 0
            # One real frame's delay measures consumption; pads carry no work.
            self._drain_estimate(now)
            sample = 1.0 / (now - sent_at)
            if self._rate:
                self._rate += self.RATE_SMOOTHING * (sample - self._rate)
            else:
                self._rate = sample
            self._pace_interval = self.PACING_MARGIN / self._rate
            self._probe_tick.set()
        if self._recovery_confirmations >= self.RECOVERY_CONFIRMATIONS:
            self._pace_interval = 0.0
            self._rate = 0.0
            self._next_send_at = 0.0
            self._unread_frames = 0.0
            self._unread_at = now
            self._catchup_until = 0.0
            self._recovery_confirmations = 0
            self._probe_tick.set()
            self._wake.set()

    async def _probe_loop(self) -> None:
        """Keep one ordinary backlog ping in flight and publish the verdict.

        Delayed acknowledgements measure throughput while backlogged. Prompt
        acknowledgements during pacing are demand-limited, so treating their
        frame deltas as consumption would mistake an idle source for a slow
        reader. Keep only one measurement anchor, never a lecture-long log.
        """
        loop = asyncio.get_running_loop()
        measured_at = loop.time()
        acknowledged_frames = self._sent_frames
        consumption_probe_at = self._consumption_probe_at
        try:
            while True:
                await self._consumption_probe_done.wait()
                if consumption_probe_at != self._consumption_probe_at:
                    # Padding time must not enter an ordinary throughput sample.
                    consumption_probe_at = self._consumption_probe_at
                    measured_at = loop.time()
                    acknowledged_frames = self._sent_frames
                self._backlog_probe_done.clear()
                sent_frames = self._sent_frames
                ping_at = loop.time()
                pong = await self.ws.ping()
                overdue = False
                try:
                    latency = await asyncio.wait_for(
                        asyncio.shield(pong), timeout=self._behind_threshold()
                    )
                    # Answered promptly: fold it into this link's baseline.
                    if isinstance(latency, (int, float)):
                        self._rtt += self.RTT_SMOOTHING * (latency - self._rtt)
                except asyncio.TimeoutError:
                    # Overdue: stop pushing, then keep waiting on this pong
                    # rather than issuing another ping behind it.
                    overdue = True
                    self._pong_overdue = True
                    self._recovery_confirmations = 0
                    self._catchup_until = loop.time() + self.CATCHUP_STICKY_SECONDS
                    self._pace_interval *= self.PACING_BACKOFF
                    self._wake.set()
                    self._probe_tick.set()
                    try:
                        if "resync" in ABLATIONS:
                            await asyncio.shield(pong)
                        else:
                            await asyncio.wait_for(
                                asyncio.shield(pong),
                                timeout=max(
                                    0.0,
                                    self._resync_threshold() - (loop.time() - ping_at),
                                ),
                            )
                    except asyncio.TimeoutError:
                        print(
                            f"client resync: id={self.hello.client_id}; "
                            "reader stalled, closing for history catch-up",
                            flush=True,
                        )
                        await self.ws.close(
                            self.TOO_FAR_BEHIND, "resynchronize via history"
                        )
                        return
                now = loop.time()
                self._drain_estimate(now)
                if overdue:
                    stall = max(0.0, now - ping_at - self._rtt)
                    frames = sent_frames - acknowledged_frames
                    if frames > 0:
                        # Include a tail reserve in the first sample because
                        # even the previous prompt pong may have left frames
                        # unread. Later samples span consecutive delayed ACKs.
                        elapsed = now - measured_at
                        if not self._rate:
                            elapsed += stall * self.DRAIN_MARGIN
                        sample = frames / elapsed
                        if self._rate:
                            self._rate += self.RATE_SMOOTHING * (sample - self._rate)
                        else:
                            self._rate = sample
                    if self._rate:
                        self._pace_interval = max(
                            self._pace_interval, self.PACING_MARGIN / self._rate
                        )
                        self._unread_frames = max(
                            self._unread_frames,
                            self._rate * stall * self.DRAIN_MARGIN
                            + self._sent_frames
                            - sent_frames,
                        )
                    else:
                        # No data crossed the probe: this was link delay, not
                        # evidence from which to invent a consumption rate.
                        self._next_send_at = now + stall * self.DRAIN_MARGIN
                    measured_at = now
                    acknowledged_frames = sent_frames
                    self._catchup_until = now + self.CATCHUP_STICKY_SECONDS
                self._pong_overdue = False
                self._probe_tick.set()
                self._wake.set()
                self._backlog_probe_done.set()
                await asyncio.sleep(self.PROBE_INTERVAL_SECONDS)
        except ConnectionClosed:
            return
        except asyncio.CancelledError:
            raise
        finally:
            # Never leave the sender blocked on a connection that is going away.
            self._backlog_probe_done.set()
            self._catchup_until = 0.0
            self._pong_overdue = False
            self._pace_interval = 0.0
            self._probe_tick.set()
