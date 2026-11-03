"""One receiver, with independent sending and bounded connected catch-up.

Normal delivery preserves every revision. Behind receivers replay finals from
the shared rolling log and keep only latest active state locally. One sender
owns the in-flight message and advances a log cursor only after sending it.
Ping timing is an indirect lag signal, not an application acknowledgement.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from websockets.exceptions import ConnectionClosed

from .caption_store import ReplayExpired
from .outbound_buffer import OutboundBuffer
from .protocol import CaptionUpdate, ClientHello, History

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from .server import Server


class Client:
    # Matches the 1013 "Try Again Later" close used when a reader falls too far
    # behind to stay inside its buffer bound.
    TOO_FAR_BEHIND = 1013

    # Gap between consumption probes.
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
    MAX_CATCHUP_SECONDS = 30.0
    # Persistent lag escalates to shared-log replay, not a routine reconnect.
    REPLAY_AFTER_SECONDS = 1.5
    BEHIND_RTT_MULTIPLIER = 6.0
    REPLAY_RTT_MULTIPLIER = 18.0
    RTT_SMOOTHING = 0.25
    # Finite stall budget, below the shared log's 90-second retention horizon.
    # Includes ping initiation and socket sends, not only the pong wait.
    MAX_STALL_SECONDS = 75.0
    CLOSE_TIMEOUT_SECONDS = 2.0
    # While behind, hold a lone provisional revision this long before sending
    # it. The deterministic source finalizes a segment in about 0.4 s, so the
    # revision is normally replaced in place by its own final and one message
    # carries what three would have.
    PROVISIONAL_GRACE_SECONDS = 1.0
    MAX_FINALS_BEFORE_ACTIVE = 4

    def __init__(self, ws, hello: ClientHello, server: "Server") -> None:
        self.ws = ws
        self.hello = hello
        self.server = server
        self._buffer = OutboundBuffer()
        self._wake = asyncio.Event()
        self._probe_tick = asyncio.Event()
        self._catchup_until = 0.0
        self._rtt = 0.0
        self._held = 0
        self._finals_since_active = 0
        self._in_flight: CaptionUpdate | None = None
        self._tasks: list[asyncio.Task] = []
        self._pending_probe: asyncio.Task | None = None
        self._last_sent_final_sequence = 0
        self._replaying = False
        self._catchup_lease = self.CATCHUP_STICKY_SECONDS
        self._last_lag_delay = 0.0
        self._normal_trial_started: float | None = None

    def _behind_threshold(self) -> float:
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self._rtt)

    def _replay_threshold(self) -> float:
        return max(self.REPLAY_AFTER_SECONDS, self.REPLAY_RTT_MULTIPLIER * self._rtt)

    @property
    def _catchup(self) -> bool:
        """True while a recently overdue pong still governs this client."""
        return asyncio.get_running_loop().time() < self._catchup_until

    async def run(self) -> None:
        # Step order here is load-bearing; see the note at the end of this
        # method for why registration must precede the history snapshot with no
        # await in between.
        self.server.register_client(self)
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

        try:
            # Complete the bounded HISTORY snapshot before starting the live
            # sender. New arrivals remain pending behind the captured boundary.
            for history in History.batches(items):
                if not await self._send(history):
                    return
            # Use the captured tail, even for empty/zero-second history. Finals
            # arriving during the send are strictly after this boundary.
            self._last_sent_final_sequence = snapshot_tail
            self._tasks = [
                asyncio.create_task(self._sender_loop()),
                asyncio.create_task(self._probe_loop()),
            ]
            # Clients send nothing after the hello. This loop exists to notice
            # the disconnect and to let the library's keepalive run.
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
        if self._replaying:
            if update.final:
                self._buffer.discard_segment(update.segment)
            else:
                self._buffer.offer(update)
        else:
            self._buffer.set_overloaded(self._catchup)
            self._buffer.offer(update)
            if self._buffer.pressured or self._buffer.should_close:
                refused = self._buffer.should_close
                self._enter_replay()
                if update.final:
                    self._buffer.discard_segment(update.segment)
                elif refused:
                    self._buffer.offer(update)
        self._wake.set()

    def _enter_replay(self) -> None:
        if not self._replaying:
            self._replaying = True
            self._buffer.begin_replay()
            print(
                f"client catch-up: id={self.hello.client_id}; replaying shared finals",
                flush=True,
            )
        self._wake.set()

    # -- tasks ----------------------------------------------------------

    async def _sender_loop(self) -> None:
        try:
            while True:
                await self._wake.wait()
                # Cleared before draining. enqueue mutates the buffer before it
                # sets the event, so an enqueue racing this clear is still seen
                # by the has_pending check below.
                self._wake.clear()
                sent_items = 0
                sent_bytes = 0

                while self._buffer.has_pending() or self._replaying:
                    if self._buffer.should_close:
                        break
                    if self._catchup and (
                        sent_items == 0
                        or sent_items >= self._buffer.MAX_BATCH_ITEMS
                        or sent_bytes >= self._buffer.MAX_BATCH_BYTES
                    ):
                        # Preserve the bounded probe window, using individual live frames.
                        await self._hold()
                        sent_items = 0
                        sent_bytes = 0
                        if self._buffer.should_close:
                            break
                    selected = self._next_delivery()
                    if selected is None:
                        # Only a still-fresh provisional is waiting. Leave it
                        # to be superseded; the probe loop re-wakes us so it
                        # still goes out once it ages past the grace.
                        break
                    delivery, through_sequence = selected
                    # Dequeue transfers ownership to this in-flight message;
                    # completing send is not an application-level client ACK.
                    self._in_flight = delivery
                    if not await self._send(delivery):
                        return
                    self._last_sent_final_sequence = through_sequence
                    self._in_flight = None
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
        """Select one message and its cursor; only the sender commits progress."""
        if not self._replaying and (self._catchup or self._buffer.pressured):
            self._enter_replay()
        if self._replaying:
            # Continuing finals must not starve active segments.
            if self._finals_since_active >= self.MAX_FINALS_BEFORE_ACTIVE:
                active = self._buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
                if active is not None:
                    self._finals_since_active = 0
                    return active, self._last_sent_final_sequence
            items, through_sequence = self.server.read_final_batch(
                self._last_sent_final_sequence,
                1,
                self._buffer.MAX_BATCH_BYTES,
            )
            if items:
                self._finals_since_active += 1
                return items[0], through_sequence
            active = self._buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
            if active is not None:
                self._finals_since_active = 0
                return active, self._last_sent_final_sequence
            # Query and transition contain no await: new arrivals cannot land
            # between the caught-up check and resuming normal FIFO delivery.
            if not self._catchup and not self._buffer.has_pending():
                self._replaying = False
                self._buffer.set_overloaded(False)
                self._finals_since_active = 0
                self._normal_trial_started = asyncio.get_running_loop().time()
            return None
        delivery = self._buffer.next_update()
        if delivery is None:
            return None
        through_sequence = self._last_sent_final_sequence
        if delivery.final:
            through_sequence = self.server.final_sequence(delivery)
        else:
            self._finals_since_active = 0
        return delivery, through_sequence

    async def _send(self, delivery: CaptionUpdate | History) -> bool:
        sending = asyncio.create_task(self.ws.send(delivery.to_json()))
        try:
            await self._wait_task(sending, self.MAX_STALL_SECONDS)
            return True
        except asyncio.TimeoutError:
            await self._close("socket send exceeded stall budget")
            return False
        finally:
            await self._stop_task(sending)

    async def _close(self, reason: str) -> None:
        """Bound the closing handshake too; a dead peer may never read it."""
        closing = asyncio.create_task(self.ws.close(self.TOO_FAR_BEHIND, reason))
        try:
            await self._wait_task(closing, self.CLOSE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            transport = getattr(self.ws, "transport", None)
            if transport is not None:
                transport.abort()
        except ConnectionClosed:
            pass
        finally:
            await self._stop_task(closing)

    @staticmethod
    async def _wait_task(task: asyncio.Task, timeout: float):
        # asyncio.wait preserves caller cancellation even when the child finishes
        # simultaneously (unlike wait_for on supported Python 3.10 versions).
        # It also leaves a timed-out probe alive for the next deadline.
        done, _ = await asyncio.wait((task,), timeout=max(0.0, timeout))
        if not done:
            raise asyncio.TimeoutError
        return task.result()

    @staticmethod
    async def _stop_task(task: asyncio.Task) -> None:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
            await task

    async def _hold(self) -> None:
        """Pace replay to completed probes; their stall deadline bounds this wait."""
        self._held += 1
        self._probe_tick.clear()
        await self._probe_tick.wait()

    async def _probe_loop(self) -> None:
        """Time the entire probe and keep one outstanding round trip.

        Early deadlines enable connected catch-up. The terminal deadline is a
        finite resource/failure policy, independent of whether ping() itself
        has returned its pong future yet.
        """
        try:
            while True:
                loop = asyncio.get_running_loop()
                started = loop.time()
                deadline = started + self.MAX_STALL_SECONDS
                replay_at = started + self._replay_threshold()
                self._pending_probe = asyncio.create_task(self._round_trip())
                was_late = False
                try:
                    latency = await self._wait_task(
                        self._pending_probe,
                        min(self._behind_threshold(), self.MAX_STALL_SECONDS),
                    )
                except asyncio.TimeoutError:
                    was_late = True
                    self._start_lag(loop.time())
                    while True:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            await self._close("probe exceeded stall budget")
                            return
                        self._catchup_until = loop.time() + self._catchup_lease
                        self._wake.set()
                        if loop.time() >= replay_at:
                            self._enter_replay()
                        try:
                            latency = await self._wait_task(
                                self._pending_probe,
                                min(self.PROBE_INTERVAL_SECONDS, remaining),
                            )
                            break
                        except asyncio.TimeoutError:
                            pass
                self._pending_probe = None
                if was_late:
                    self._last_lag_delay = loop.time() - started
                if isinstance(latency, (int, float)):
                    # Learn from late successes too, but cap one sample so a
                    # congestion spike cannot become an enormous RTT baseline.
                    sample = min(latency, self._behind_threshold())
                    self._rtt += self.RTT_SMOOTHING * (sample - self._rtt)
                self._probe_tick.set()
                self._wake.set()
                await asyncio.sleep(self.PROBE_INTERVAL_SECONDS)
        except ConnectionClosed:
            return
        except asyncio.CancelledError:
            raise
        finally:
            if self._pending_probe is not None:
                await self._stop_task(self._pending_probe)
                self._pending_probe = None
            # Never leave the sender blocked on a connection that is going away.
            self._catchup_until = 0.0
            self._probe_tick.set()

    def _start_lag(self, now: float) -> None:
        """Back off if a full-update trial quickly fills the receiver again.

        Prompt pongs during final-only replay don't prove capacity for every
        revision. Repeated failed normal trials earn a longer, bounded replay
        lease; a sustained healthy normal trial resets it. An isolated long
        stall doesn't by itself increase the lease.
        """
        if self._normal_trial_started is not None and self._last_lag_delay > 0:
            probation = min(
                self.MAX_CATCHUP_SECONDS,
                max(4 * self.CATCHUP_STICKY_SECONDS, 2 * self._last_lag_delay),
            )
            if now - self._normal_trial_started < probation:
                self._catchup_lease = min(
                    self.MAX_CATCHUP_SECONDS, 2 * self._catchup_lease
                )
            else:
                self._catchup_lease = self.CATCHUP_STICKY_SECONDS
        self._normal_trial_started = None

    async def _round_trip(self) -> float:
        pong = await self.ws.ping()
        return await pong
