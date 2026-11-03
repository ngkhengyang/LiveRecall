"""One receiver, with independent sending and bounded connected catch-up.

REORGANISED COPY of ``submission_root/app/client.py`` (two-file layout).
Behaviour is identical; responsibilities are split into small classes.

Layout (top to bottom)
----------------------
``Mode``          the client's three delivery modes and how it moves between them.
``LagPolicy``     pure: RTT estimate, "pong is late" and "go to replay"
                  thresholds, catch-up window and its adaptive back-off.
``ReplayCursor``  pure: position in the shared final log, and which message to
                  send next while replaying.
``Client``        lifecycle, the golden path, mode changes, the probe task.
module helpers    ``_wait_task`` / ``_stop_task``: every network wait has a
                  deadline.

"Pure" means no ``await`` and no socket: it takes timestamps or objects and
returns decisions, so it can be tested without a connection.

Invariants (each lives inside one synchronous method):
- No ``await`` between ``register_client`` and ``history_snapshot``
  (``Client.run``): no final is lost or duplicated across HISTORY and live.
- No ``await`` between the caught-up check and returning to NORMAL
  (``Client._leave_replay_if_caught_up``): nothing slips in between.
- The cursor moves only after a successful send (``Client._sender_loop``).
- Ping timing is a lag signal, not an application acknowledgement.
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

if TYPE_CHECKING:  # pragma: no cover - import cycle guard, typing only
    from .server import Server


# ==========================================================================
# Mode
# ==========================================================================


class Mode(Enum):
    """How a client is being served.

    NORMAL      Healthy link. Buffer is FIFO, every revision is sent.
    COALESCING  A pong was late. The buffer keeps only the latest entry per
                segment and sending is paced to completed probes. Derived
                from NORMAL plus the catch-up window; not stored.
    REPLAY      Finals come from the shared CaptionStore log via the
                cursor; the buffer holds only latest active state.

    Transitions (all through ``Client._enter_replay`` and
    ``Client._leave_replay_if_caught_up``):

    NORMAL/COALESCING -> REPLAY
        - the buffer is pressured or full after an offer   (enqueue)
        - the sender sees catch-up or pressure              (_next_delivery)
        - a late pong outlasts the replay threshold         (_probe_loop)
    REPLAY -> NORMAL
        - the log is drained, the buffer is empty and there is no lag
    any -> closed (1013)
        - a send or probe stalls past 75 s, the cursor falls out of the
          90 s log, or latest active state alone exceeds the buffer limits
    """

    NORMAL = "normal"
    COALESCING = "coalescing"
    REPLAY = "replay"


# ==========================================================================
# LagPolicy (pure)
# ==========================================================================


class LagPolicy:
    """Decides when a client is behind, from ping/pong timing alone.

    Both thresholds have a floor and scale with this link's own RTT, so the
    supplied 120 ms +/- 30 ms netem profile with 3% loss is not read as a
    stalled reader.
    """

    BEHIND_AFTER_SECONDS = 0.5
    BEHIND_RTT_MULTIPLIER = 6.0
    REPLAY_AFTER_SECONDS = 1.5
    REPLAY_RTT_MULTIPLIER = 18.0
    RTT_SMOOTHING = 0.25
    # How long one overdue pong keeps the client in catch-up. A behind reader
    # drains just enough to answer a ping and falls back, so without a sticky
    # window the sender would resume flooding after a single pong.
    CATCHUP_STICKY_SECONDS = 3.0
    MAX_CATCHUP_SECONDS = 30.0

    def __init__(self) -> None:
        """Called by: Client.__init__ (one LagPolicy per client).
        Does: Starts with no RTT, no catch-up window and the 3 s lease.
        """
        self.rtt = 0.0
        self.catchup_until = 0.0
        self.catchup_lease = self.CATCHUP_STICKY_SECONDS
        self.last_lag_delay = 0.0
        self.normal_trial_started: float | None = None

    def behind_threshold(self) -> float:
        """Called by: Client._probe_loop (start of each probe),
            LagPolicy.probe_finished.
        Does: Returns how late a pong may be before the client counts as behind.

        A pong later than this means the client is behind.
        """
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self.rtt)

    def replay_threshold(self) -> float:
        """Called by: Client._probe_loop (start of each probe).
        Does: Returns how long lag must last before finals switch to log replay.

        Lag lasting longer than this switches finals to log replay.
        """
        return max(self.REPLAY_AFTER_SECONDS, self.REPLAY_RTT_MULTIPLIER * self.rtt)

    def in_catchup(self, now: float) -> bool:
        """Called by: Client._catchup (every lag-dependent decision).
        Does: Says whether the catch-up window is still open at `now`.
        """
        return now < self.catchup_until

    def lag_started(self, now: float) -> None:
        """Called by: Client._probe_loop, once when a pong first becomes late (and
            Client._start_lag in tests).
        Does: Doubles or resets the catch-up lease depending on how soon the lag came
            back.

        A pong just became late. Back off if a normal trial failed quickly.

        Prompt pongs during final-only replay don't prove capacity for every
        revision. Repeated failed normal trials earn a longer, bounded
        lease; a sustained healthy normal trial resets it. An isolated long
        stall doesn't by itself increase the lease.
        """
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
        """Called by: Client._probe_loop, every 0.25 s while the pong stays late.
        Does: Extends the catch-up window by one lease from `now`.

        The pong is still late: keep catch-up on for another lease.
        """
        self.catchup_until = now + self.catchup_lease

    def probe_finished(
        self, started: float, now: float, latency: object, was_late: bool
    ) -> None:
        """Called by: Client._probe_loop, after every completed probe.
        Does: Records how long a late probe took and updates the smoothed RTT.

        Record how long a late probe took, and learn the RTT.
        """
        if was_late:
            self.last_lag_delay = now - started
        if isinstance(latency, (int, float)):
            # Learn from late successes too, but cap one sample so a
            # congestion spike cannot become an enormous RTT baseline.
            sample = min(latency, self.behind_threshold())
            self.rtt += self.RTT_SMOOTHING * (sample - self.rtt)

    def start_normal_trial(self, now: float) -> None:
        """Called by: Client._leave_replay_if_caught_up.
        Does: Remembers when the client returned to NORMAL, for the back-off rule.

        The client just left replay; a quick relapse will lengthen the lease.
        """
        self.normal_trial_started = now

    def stop(self) -> None:
        """Called by: Client._probe_loop (finally), when the probe task ends.
        Does: Closes the catch-up window so the sender is never left pacing.

        The connection is going away; never leave the sender in catch-up.
        """
        self.catchup_until = 0.0


# ==========================================================================
# ReplayCursor (pure)
# ==========================================================================

# read_final_batch(after_sequence, max_items, max_bytes) -> (items, through)
ReadFinalBatch = Callable[[int, int, int], "tuple[list[CaptionUpdate], int]"]


class ReplayCursor:
    """The client's position in the shared final log.

    ``position`` is the sequence of the last final sent (or covered by
    HISTORY). Only the sender moves it, and only after a successful send.
    """

    # Send the latest active segment after at most this many replayed finals,
    # so catching up on old finals never freezes the live line.
    MAX_FINALS_BEFORE_ACTIVE = 4
    # Hold a lone provisional this long; the source usually finalizes a
    # segment in ~0.4 s, so one message carries what three would have.
    PROVISIONAL_GRACE_SECONDS = 1.0

    def __init__(self) -> None:
        """Called by: Client.__init__ (one cursor per client).
        Does: Starts at log position 0 with no finals sent since the last active.
        """
        self.position = 0
        self.finals_since_active = 0

    def next_replay(
        self, buffer: OutboundBuffer, read_final_batch: ReadFinalBatch
    ) -> tuple[CaptionUpdate, int] | None:
        """Called by: Client._next_delivery, once per message while in REPLAY.
        Does: Picks the next replay message: the latest active after every 4 finals,
            else the next final from the log, else the latest active, else nothing.

        The next message while replaying, with the position it reaches.

        Returns None when there is nothing to send right now (log drained and
        no ready active segment). Raises ReplayExpired if the position has
        fallen out of the log.
        """
        if self.finals_since_active >= self.MAX_FINALS_BEFORE_ACTIVE:
            active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
            if active is not None:
                self.finals_since_active = 0
                return active, self.position
        items, through_sequence = read_final_batch(
            self.position, 1, buffer.MAX_BATCH_BYTES
        )
        if items:
            self.finals_since_active += 1
            return items[0], through_sequence
        active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
        if active is not None:
            self.finals_since_active = 0
            return active, self.position
        return None


# ==========================================================================
# Client
# ==========================================================================


class Client:
    # Close code for a reader too far behind to keep up (Try Again Later).
    TOO_FAR_BEHIND = 1013
    # Stall budget per send or probe, below the shared log's 90-second
    # retention so a closed client can still recover by reconnecting.
    MAX_STALL_SECONDS = 75.0
    CLOSE_TIMEOUT_SECONDS = 2.0
    PROBE_INTERVAL_SECONDS = 0.25

    def __init__(self, ws, hello: ClientHello, server: "Server") -> None:
        """Called by: Server._accept, when a CLIENT_HELLO arrives.
        Does: Creates this client's OutboundBuffer, LagPolicy, ReplayCursor and events;
            starts in NORMAL.
        """
        self.ws = ws
        self.hello = hello
        self.server = server
        self._buffer = OutboundBuffer()
        self._lag = LagPolicy()
        self._cursor = ReplayCursor()
        self._mode = Mode.NORMAL  # only NORMAL or REPLAY is stored
        self._wake = asyncio.Event()  # set whenever the sender has work
        self._probe_tick = asyncio.Event()  # set after each completed probe
        self._in_flight: CaptionUpdate | None = None
        self._pending_probe: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []
        self._held = 0

    @property
    def mode(self) -> Mode:
        """Called by: Readers and tests (not used by the delivery code).
        Does: Reports NORMAL, COALESCING (NORMAL with an open catch-up window) or
            REPLAY.
        """
        if self._mode is Mode.NORMAL and self._catchup:
            return Mode.COALESCING
        return self._mode

    @property
    def _catchup(self) -> bool:
        """Called by: enqueue, _next_delivery, _window_full, _leave_replay_if_caught_up,
            mode.
        Does: Asks LagPolicy whether the catch-up window is open right now.

        True while a recently overdue pong still governs this client.
        """
        return self._lag.in_catchup(asyncio.get_running_loop().time())

    # ======================================================================
    # GOLDEN PATH
    # ======================================================================

    async def run(self) -> None:
        """Called by: Server._accept (awaited), once per connection.
        Does: Registers, sends HISTORY, sets the cursor, starts the sender and probe
            tasks, waits for disconnect, cleans up.

        Connect, send HISTORY, start the sender and probe, await disconnect.
        """
        # Register before the snapshot with no await in between. If the
        # snapshot came first, a final produced in the gap would never reach
        # this client. A final can't be in both: CaptionStore appends each
        # final to the log exactly once and never re-broadcasts it.
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
            # HISTORY goes out before the sender starts, so no live update
            # can precede it.
            for history in History.batches(items):
                if not await self._send(history):
                    return
            # Use the captured tail, even for empty history. Finals arriving
            # during the send are strictly after this boundary.
            self._cursor.position = snapshot_tail
            self._tasks = [
                asyncio.create_task(self._sender_loop()),
                asyncio.create_task(self._probe_loop()),
            ]
            # Clients send nothing after the hello; this notices the disconnect.
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

    def enqueue(self, update: CaptionUpdate) -> None:
        """Called by: Server.broadcast, for every caption the store accepts.
        Does: Queues the update in the buffer (or drops a final in REPLAY), escalates to
            replay on pressure, wakes the sender.

        Hand this client one update. Never blocks, never awaits.

        Called from ``Server.broadcast``. It only touches in-memory buffer
        state, so a stalled reader cannot delay any other client.
        """
        if self._mode is Mode.REPLAY:
            # The log owns finals; keep only latest active state.
            if update.final:
                self._buffer.discard_segment(update.segment)
            else:
                self._buffer.offer(update)
        else:
            self._buffer.set_coalescing(self._catchup)  # False on the golden path
            self._buffer.offer(update)
            if self._buffer.pressured or self._buffer.should_close:
                self._escalate_to_replay(update)
        self._wake.set()

    async def _sender_loop(self) -> None:
        """Called by: Task started by run; runs for the whole connection.
        Does: Waits for _wake, then picks, sends and commits messages one at a time,
            pacing during catch-up.

        The only task that sends live messages and moves the cursor.
        """
        try:
            while True:
                await self._wake.wait()
                # Cleared before draining. enqueue changes the buffer before it
                # sets the event, so a racing enqueue is still seen below.
                self._wake.clear()
                sent_items = 0
                sent_bytes = 0
                while self._buffer.has_pending() or self._mode is Mode.REPLAY:
                    if self._buffer.should_close:
                        break
                    if self._window_full(sent_items, sent_bytes):
                        await self._hold()  # catch-up: one window per probe
                        sent_items = 0
                        sent_bytes = 0
                        if self._buffer.should_close:
                            break
                    selected = self._next_delivery()
                    if selected is None:
                        # Only a still-fresh provisional is waiting. The probe
                        # loop re-wakes us so it goes out once past the grace.
                        break
                    delivery, through_sequence = selected
                    # Ownership moves to this in-flight message; a completed
                    # send is not an application-level ACK.
                    self._in_flight = delivery
                    if not await self._send(delivery):
                        return
                    self._cursor.position = through_sequence  # only after the send
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
        """Called by: _sender_loop, once per message.
        Does: Enters replay if needed, then picks the next message from ReplayCursor
            (REPLAY) or the buffer (FIFO).

        Pick one message and the cursor it reaches; the sender commits it.
        """
        if self._mode is Mode.NORMAL and (self._catchup or self._buffer.pressured):
            self._enter_replay()
        if self._mode is Mode.REPLAY:
            selected = self._cursor.next_replay(
                self._buffer, self.server.read_final_batch
            )
            if selected is None:
                self._leave_replay_if_caught_up()
            return selected
        # Golden path: strict FIFO, every revision.
        delivery = self._buffer.next_update()
        if delivery is None:
            return None
        through_sequence = self._cursor.position
        if delivery.final:
            through_sequence = self.server.final_sequence(delivery)
        else:
            self._cursor.finals_since_active = 0
        return delivery, through_sequence

    async def _send(self, delivery: CaptionUpdate | History) -> bool:
        """Called by: run (HISTORY frames), _sender_loop (live frames).
        Does: Writes one frame to the socket within 75 s, or closes the client.

        One socket write, bounded by MAX_STALL_SECONDS.
        """
        sending = asyncio.create_task(self.ws.send(delivery.to_json()))
        try:
            await _wait_task(sending, self.MAX_STALL_SECONDS)
            return True
        except asyncio.TimeoutError:
            await self._close("socket send exceeded stall budget")
            return False
        finally:
            await _stop_task(sending)

    # ======================================================================
    # Mode changes (see Mode for the full transition table)
    # ======================================================================

    def _enter_replay(self) -> None:
        """Called by: _escalate_to_replay, _next_delivery, _probe_loop.
        Does: Switches to REPLAY (once) and drops the buffer's finals; the shared log
            delivers them now.
        """
        if self._mode is not Mode.REPLAY:
            self._mode = Mode.REPLAY
            self._buffer.drop_finals_keep_latest_active()
            print(
                f"client catch-up: id={self.hello.client_id}; replaying shared finals",
                flush=True,
            )
        self._wake.set()

    def _escalate_to_replay(self, update: CaptionUpdate) -> None:
        """Called by: enqueue, when offer left the buffer pressured or full.
        Does: Enters replay, then drops the update if it is a final or re-offers it if
            it was refused.

        enqueue found the buffer pressured or full after offering ``update``.
        """
        refused = self._buffer.should_close
        self._enter_replay()
        if update.final:
            self._buffer.discard_segment(update.segment)  # the log has it
        elif refused:
            self._buffer.offer(update)  # retry now that finals left the buffer

    def _leave_replay_if_caught_up(self) -> None:
        """Called by: _next_delivery, when next_replay has nothing to send.
        Does: Returns to NORMAL if there is no lag and the buffer is empty, with no
            await in between.

        REPLAY -> NORMAL. No await between the check and the switch.
        """
        if not self._catchup and not self._buffer.has_pending():
            self._mode = Mode.NORMAL
            self._buffer.set_coalescing(False)
            self._cursor.finals_since_active = 0
            self._lag.start_normal_trial(asyncio.get_running_loop().time())

    # ======================================================================
    # Probe task (feeds LagPolicy) and catch-up pacing
    # ======================================================================

    async def _probe_loop(self) -> None:
        """Called by: Task started by run; runs for the whole connection.
        Does: Pings every 0.25 s, feeds the timing to LagPolicy, enters replay on
            lasting lag, closes after 75 s.

        Time each whole probe and keep one round trip outstanding.

        Early deadlines turn on catch-up. The 75 s deadline is a terminal
        failure policy, independent of whether ping() has returned yet.
        """
        try:
            while True:
                loop = asyncio.get_running_loop()
                started = loop.time()
                deadline = started + self.MAX_STALL_SECONDS
                replay_at = started + self._lag.replay_threshold()
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
                    while True:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            await self._close("probe exceeded stall budget")
                            return
                        self._lag.still_lagging(loop.time())
                        self._wake.set()
                        if loop.time() >= replay_at:
                            self._enter_replay()
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
        except asyncio.CancelledError:
            raise
        finally:
            if self._pending_probe is not None:
                await _stop_task(self._pending_probe)
                self._pending_probe = None
            self._lag.stop()
            self._probe_tick.set()  # never leave the sender blocked in _hold

    async def _round_trip(self) -> float:
        """Called by: _probe_loop, once per probe.
        Does: Sends one ping and waits for its pong; returns the latency.
        """
        pong = await self.ws.ping()
        return await pong

    def _window_full(self, sent_items: int, sent_bytes: int) -> bool:
        """Called by: _sender_loop, before each message.
        Does: During catch-up, says whether to pause before the next window of 64 items
            / 256 KiB.

        During catch-up, pause before each window of 64 items / 256 KiB.
        """
        return self._catchup and (
            sent_items == 0
            or sent_items >= self._buffer.MAX_BATCH_ITEMS
            or sent_bytes >= self._buffer.MAX_BATCH_BYTES
        )

    async def _hold(self) -> None:
        """Called by: _sender_loop, during catch-up (tests may replace it).
        Does: Waits until the probe task completes its next probe.

        Wait for the next completed probe; its stall deadline bounds this.
        """
        self._held += 1
        self._probe_tick.clear()
        await self._probe_tick.wait()

    # ======================================================================
    # Closing
    # ======================================================================

    async def _close(self, reason: str) -> None:
        """Called by: _sender_loop, _send, _probe_loop.
        Does: Closes the socket with 1013, aborting the transport if the handshake takes
            over 2 s.

        Close with 1013, bounding the handshake; a dead peer may never read it.
        """
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

    # ======================================================================
    # Test compatibility: old names used by test/test_delivery.py and
    # test/test_live_delivery.py. Remove once the tests use the new objects.
    # ======================================================================

    # Lag constants now live on LagPolicy; tests read and override them on
    # the client instance, so forward both reads and writes.
    def _forward_to_lag(name: str) -> property:  # noqa: N805 - class-body helper
        """Called by: The class body below, at import time (test compatibility).
        Does: Builds a property that reads and writes a constant on this client's
            LagPolicy.
        """
        return property(
            lambda self: getattr(self._lag, name),
            lambda self, value: setattr(self._lag, name, value),
        )

    BEHIND_AFTER_SECONDS = _forward_to_lag("BEHIND_AFTER_SECONDS")
    REPLAY_AFTER_SECONDS = _forward_to_lag("REPLAY_AFTER_SECONDS")
    CATCHUP_STICKY_SECONDS = _forward_to_lag("CATCHUP_STICKY_SECONDS")
    MAX_CATCHUP_SECONDS = _forward_to_lag("MAX_CATCHUP_SECONDS")
    del _forward_to_lag

    @property
    def _replaying(self) -> bool:
        """Called by: Tests only (old name).
        Does: True while the client is in REPLAY.
        """
        return self._mode is Mode.REPLAY

    @property
    def _last_sent_final_sequence(self) -> int:
        """Called by: Tests only (old name).
        Does: Reads or sets ReplayCursor.position.
        """
        return self._cursor.position

    @_last_sent_final_sequence.setter
    def _last_sent_final_sequence(self, value: int) -> None:
        self._cursor.position = value

    @property
    def _catchup_until(self) -> float:
        """Called by: Tests only (old name).
        Does: Reads or sets LagPolicy.catchup_until.
        """
        return self._lag.catchup_until

    @_catchup_until.setter
    def _catchup_until(self, value: float) -> None:
        self._lag.catchup_until = value

    @property
    def _catchup_lease(self) -> float:
        """Called by: Tests only (old name).
        Does: Reads LagPolicy.catchup_lease.
        """
        return self._lag.catchup_lease

    @property
    def _last_lag_delay(self) -> float:
        """Called by: Tests only (old name).
        Does: Reads or sets LagPolicy.last_lag_delay.
        """
        return self._lag.last_lag_delay

    @_last_lag_delay.setter
    def _last_lag_delay(self, value: float) -> None:
        self._lag.last_lag_delay = value

    @property
    def _normal_trial_started(self) -> float | None:
        """Called by: Tests only (old name).
        Does: Reads or sets LagPolicy.normal_trial_started.
        """
        return self._lag.normal_trial_started

    @_normal_trial_started.setter
    def _normal_trial_started(self, value: float | None) -> None:
        self._lag.normal_trial_started = value

    def _start_lag(self, now: float) -> None:
        """Called by: Tests only (old name).
        Does: Calls LagPolicy.lag_started.
        """
        self._lag.lag_started(now)


# ==========================================================================
# Deadline helpers for every network wait
# ==========================================================================


async def _wait_task(task: asyncio.Task, timeout: float):
    """Called by: Client._send, Client._close, Client._probe_loop.
    Does: Waits for a task up to a deadline; raises TimeoutError but leaves the task
        running.

    Wait for ``task`` up to ``timeout``; raise TimeoutError but leave it running.

    asyncio.wait preserves caller cancellation even when the child finishes
    simultaneously (unlike wait_for on supported Python 3.10 versions), and
    leaves a timed-out probe alive for the next deadline.
    """
    done, _ = await asyncio.wait((task,), timeout=max(0.0, timeout))
    if not done:
        raise asyncio.TimeoutError
    return task.result()


async def _stop_task(task: asyncio.Task) -> None:
    """Called by: Client._send, Client._close, Client._probe_loop.
    Does: Cancels a task if it is still running and waits for it to finish.

    Cancel ``task`` if still running and wait for it to finish.
    """
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
        await task
