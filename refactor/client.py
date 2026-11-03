"""One receiver's lifecycle, lag policy, and shared-log delivery.

Normal delivery preserves revisions in FIFO order. A late probe or queue
pressure switches the receiver to replay: finals stay in the shared log and
only the latest active state stays in its local buffer. One sender commits
the final cursor after each successful send.

GOLDEN PATH (normal mode, healthy connection)
=============================================

    Server._accept()                         # CLIENT_HELLO received
      -> Client(ws, hello, server).run()
           -> server.register_client(self)
           -> _send_initial_history()        # HISTORY batches, sets cursor
           -> start tasks: _sender_loop(), _probe_loop()
           -> wait for socket to close

    Server.broadcast(update)                 # for every new caption
      -> Client.enqueue(update)
           -> OutboundBuffer.offer(update)
           -> wake sender

    Client._sender_loop()                    # one per client
      -> _next_delivery()
           -> OutboundBuffer.next_update()
      -> _send(update)                       # ws.send with stall timeout
      -> advance ReplayCursor.position

    Client._probe_loop()                     # in parallel
      -> _round_trip()                       # ws.ping / pong
      -> LagPolicy.probe_finished()          # update smoothed RTT

SLOW PATH (receiver falls behind)
=================================

    probe late OR buffer pressured  -> _enter_replay()
    replay mode: _next_delivery()   -> ReplayCursor.next_replay()
                                       (finals from shared log + latest drafts)
    log drained & lease expired     -> _leave_replay_if_caught_up() -> NORMAL
    stall budget / hard limit       -> _close()

FILE LAYOUT
===========

    1. Client                        - the per-receiver connection
       1.1 Settings and state
       1.2 Golden path: lifecycle    - run, initial history
       1.3 Golden path: delivery     - enqueue, sender loop, next delivery, send
       1.4 Replay mode transitions   - enter / escalate / leave
       1.5 Lag probing               - probe loop, round trip
       1.6 Catch-up flow control     - window, hold
       1.7 Shutdown                  - close
    2. Mode                          - NORMAL / REPLAY
    3. LagPolicy                     - RTT and catch-up lease maths
    4. ReplayCursor                  - picks next item while replaying
    5. Task helpers                  - bounded waits and cancellation
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


# =============================================================================
# 1. Client
# =============================================================================


class Client:
    """Own one receiver's buffer, sender, probes, and mode transitions.

    Created by ``Server._accept`` when a ``CLIENT_HELLO`` arrives.
    """

    # -------------------------------------------------------------------------
    # 1.1 Settings and state
    # -------------------------------------------------------------------------

    TOO_FAR_BEHIND = 1013
    MAX_STALL_SECONDS = 75.0
    CLOSE_TIMEOUT_SECONDS = 2.0
    PROBE_INTERVAL_SECONDS = 0.25
    MAX_BURST_ITEMS = 64
    MAX_BURST_BYTES = 256 * 1024

    def __init__(self, ws, hello: ClientHello, server: "Server") -> None:
        """Set up per-receiver state.

        What: stores the socket, hello and server, and creates the outbound
        buffer, lag policy, replay cursor, and wake/probe events.
        Called by: ``Server._accept``.
        """
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
        """Whether the catch-up lease is still active.

        What: asks ``LagPolicy.in_catchup`` using the event-loop clock.
        Called by: ``_window_full``, ``_leave_replay_if_caught_up``.
        """
        return self._lag.in_catchup(asyncio.get_running_loop().time())

    # -------------------------------------------------------------------------
    # 1.2 Golden path: lifecycle
    # -------------------------------------------------------------------------

    async def run(self) -> None:
        """Run this receiver from hello to disconnect.

        What: registers with the server, sends initial history, starts the
        sender and probe tasks, waits for the socket to close, then cancels
        tasks, unregisters, and logs stats.
        Called by: ``Server._accept``.
        """
        self.server.register_client(self)
        try:
            # The helper captures its snapshot before its first network await.
            # Its caption references expire before the long-lived receive loop.
            snapshot_tail = await self._send_initial_history()
            if snapshot_tail is None:
                return
            self._cursor.position = snapshot_tail
            self._tasks = [
                asyncio.create_task(self._sender_loop()),
                asyncio.create_task(self._probe_loop()),
            ]
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
        """Send the complete hello snapshot, then release its caption references.

        What: takes a history snapshot from the server for the hello's resume
        point, sends it in ``History`` batches, and returns the final-log
        sequence the snapshot ends at (``None`` if a send failed).
        Called by: ``run``.
        """
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

    # -------------------------------------------------------------------------
    # 1.3 Golden path: delivery
    # -------------------------------------------------------------------------

    def enqueue(self, update: CaptionUpdate) -> None:
        """Accept broadcast work synchronously; never wait for this socket.

        What: in normal mode, offers the update to the buffer and escalates
        to replay under pressure; in replay mode, keeps only drafts (finals
        will come from the shared log). Then wakes the sender.
        Called by: ``Server.broadcast``.
        """
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
        """The single writer for this socket.

        What: on each wake, repeatedly picks the next delivery, sends it, and
        commits the replay cursor. Pauses between bursts while catching up;
        closes the socket on hard-limit or retention failure.
        Called by: ``run`` (as a background task).
        """
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
        """Choose what to send next and the final-log position it covers.

        What: in replay mode, asks ``ReplayCursor.next_replay``; if nothing is
        left, tries to return to normal mode. In normal mode, pops the
        buffer's oldest update. Returns ``(update, through_sequence)``.
        Called by: ``_sender_loop``.
        """
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

    async def _send(self, delivery: CaptionUpdate | History) -> bool:
        """Send one message with a stall timeout.

        What: writes ``delivery`` to the socket; if it takes longer than
        ``MAX_STALL_SECONDS``, closes the connection and returns ``False``.
        Called by: ``_send_initial_history``, ``_sender_loop``.
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

    # -------------------------------------------------------------------------
    # 1.4 Replay mode transitions
    # -------------------------------------------------------------------------

    def _enter_replay(self) -> None:
        """Switch to replay mode.

        What: sets mode to REPLAY, drops queued finals from the buffer (they
        will be re-read from the shared log), and wakes the sender.
        Called by: ``_probe_loop`` (late probe), ``_escalate_to_replay``.
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
        """Enter replay because the buffer is under pressure.

        What: enters replay, then makes sure the triggering update ends up in
        the right place (finals dropped from buffer, refused drafts re-offered).
        Called by: ``enqueue``.
        """
        refused = self._buffer.should_close
        self._enter_replay()
        if update.final:
            # A refused final may have left its older draft in the buffer.
            self._buffer.discard_segment(update.segment)
        elif refused:
            self._buffer.offer(update)

    def _leave_replay_if_caught_up(self) -> bool:
        """Called only after the log has no next final and no ready active item.

        What: if the catch-up lease has expired, returns to NORMAL mode,
        turns off coalescing, and starts a normal-mode trial. Returns whether
        it switched.
        Called by: ``_next_delivery``.
        """
        if self._catchup:
            return False
        self._mode = Mode.NORMAL
        self._buffer.set_coalescing(False)
        self._cursor.finals_since_active = 0
        self._lag.start_normal_trial(asyncio.get_running_loop().time())
        return True

    # -------------------------------------------------------------------------
    # 1.5 Lag probing
    # -------------------------------------------------------------------------

    async def _probe_loop(self) -> None:
        """Keep one round trip; the first late deadline enters replay.

        What: repeatedly pings the receiver. If the pong is later than the
        behind threshold, enters replay and keeps extending the catch-up
        lease until the pong arrives or the stall budget runs out (then
        closes). Each finished probe updates RTT and releases a held sender.
        Called by: ``run`` (as a background task).
        """
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
        """One WebSocket ping/pong.

        What: sends a ping and returns the pong latency.
        Called by: ``_probe_loop`` (wrapped in a task).
        """
        pong = await self.ws.ping()
        return await pong

    # -------------------------------------------------------------------------
    # 1.6 Catch-up flow control
    # -------------------------------------------------------------------------

    def _window_full(self, sent_items: int, sent_bytes: int) -> bool:
        """Whether the sender should pause for the next probe.

        What: during catch-up, true at the start of a burst or once the burst
        reaches ``MAX_BURST_ITEMS`` / ``MAX_BURST_BYTES``; always false
        otherwise.
        Called by: ``_sender_loop``.
        """
        return self._catchup and (
            sent_items == 0
            or sent_items >= self.MAX_BURST_ITEMS
            or sent_bytes >= self.MAX_BURST_BYTES
        )

    async def _hold(self) -> None:
        """Wait for the next probe to finish.

        What: counts the hold and blocks until ``_probe_loop`` signals a
        completed probe (so bursts are paced by transport progress).
        Called by: ``_sender_loop``.
        """
        self._held += 1
        self._probe_tick.clear()
        await self._probe_tick.wait()

    # -------------------------------------------------------------------------
    # 1.7 Shutdown
    # -------------------------------------------------------------------------

    async def _close(self, reason: str) -> None:
        """Close the socket with code 1013, aborting if close stalls.

        What: sends a WebSocket close with ``reason``; if that does not finish
        within ``CLOSE_TIMEOUT_SECONDS``, aborts the transport.
        Called by: ``_sender_loop``, ``_probe_loop``, ``_send``.
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


# =============================================================================
# 2. Mode
# =============================================================================


class Mode(Enum):
    """Delivery mode of a Client.

    NORMAL: FIFO delivery of every revision from the local buffer.
    REPLAY: finals read from the shared log, only latest drafts kept locally.
    Used by: ``Client.__init__``, ``enqueue``, ``_sender_loop``,
    ``_next_delivery``, ``_enter_replay``, ``_leave_replay_if_caught_up``.
    """

    NORMAL = "normal"
    REPLAY = "replay"


# =============================================================================
# 3. LagPolicy
# =============================================================================


class LagPolicy:
    """RTT-based lag detection and a bounded lease against mode oscillation.

    Timestamps are supplied by the caller so policy decisions need no socket
    or event loop. A pong indicates transport progress, not application ACKs.
    Created by: ``Client.__init__``.
    """

    BEHIND_AFTER_SECONDS = 0.5
    BEHIND_RTT_MULTIPLIER = 6.0
    RTT_SMOOTHING = 0.25
    CATCHUP_STICKY_SECONDS = 3.0
    MAX_CATCHUP_SECONDS = 30.0

    def __init__(self) -> None:
        """Start with zero RTT and no active catch-up lease.

        What: initialises RTT, lease end time, lease length, and trial state.
        Called by: ``Client.__init__``.
        """
        self.rtt = 0.0
        self.catchup_until = 0.0
        self.catchup_lease = self.CATCHUP_STICKY_SECONDS
        self.last_lag_delay = 0.0
        self.normal_trial_started: float | None = None

    def behind_threshold(self) -> float:
        """How long a probe may take before the client counts as behind.

        What: ``max(BEHIND_AFTER_SECONDS, BEHIND_RTT_MULTIPLIER * rtt)``.
        Called by: ``Client._probe_loop``, ``probe_finished``.
        """
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self.rtt)

    def in_catchup(self, now: float) -> bool:
        """Whether the catch-up lease covers ``now``.

        What: true while ``now < catchup_until``.
        Called by: ``Client._catchup``.
        """
        return now < self.catchup_until

    def lag_started(self, now: float) -> None:
        """Lengthen the lease only when a recent normal trial failed.

        What: if lag returned soon after leaving replay, doubles the lease
        (capped); otherwise resets it. Ends the current normal trial.
        Called by: ``Client._probe_loop`` (first late deadline).
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
        """Extend the catch-up lease from ``now``.

        What: sets ``catchup_until = now + catchup_lease``.
        Called by: ``Client._probe_loop`` (while waiting on a late pong).
        """
        self.catchup_until = now + self.catchup_lease

    def probe_finished(
        self, started: float, now: float, latency: object, was_late: bool
    ) -> None:
        """Record a completed probe.

        What: remembers how late a late probe was, and folds the (capped)
        latency sample into the smoothed RTT.
        Called by: ``Client._probe_loop``.
        """
        if was_late:
            self.last_lag_delay = now - started
        if isinstance(latency, (int, float)):
            # One congestion spike must not establish an enormous RTT baseline.
            sample = min(latency, self.behind_threshold())
            self.rtt += self.RTT_SMOOTHING * (sample - self.rtt)

    def start_normal_trial(self, now: float) -> None:
        """Mark when the client returned to normal mode.

        What: stores ``now`` so ``lag_started`` can tell if the trial failed.
        Called by: ``Client._leave_replay_if_caught_up``.
        """
        self.normal_trial_started = now

    def stop(self) -> None:
        """End any catch-up lease.

        What: sets ``catchup_until`` to 0 so the client is not in catch-up.
        Called by: ``Client._probe_loop`` (finally block on exit).
        """
        self.catchup_until = 0.0


# =============================================================================
# 4. ReplayCursor
# =============================================================================


ReadNextFinal = Callable[[int], tuple[CaptionUpdate, int] | None]


class ReplayCursor:
    """Select replay work while leaving cursor commits to the sender.

    Created by: ``Client.__init__``. ``position`` is set by ``Client.run`` and
    ``Client._sender_loop``; ``finals_since_active`` is reset by
    ``Client._next_delivery`` and ``Client._leave_replay_if_caught_up``.
    """

    MAX_FINALS_BEFORE_ACTIVE = 4
    PROVISIONAL_GRACE_SECONDS = 1.0

    def __init__(self) -> None:
        """Start at final-log position 0 with no finals sent since a draft.

        What: initialises ``position`` and ``finals_since_active``.
        Called by: ``Client.__init__``.
        """
        self.position = 0
        self.finals_since_active = 0

    def next_replay(
        self, buffer: OutboundBuffer, read_next_final: ReadNextFinal
    ) -> tuple[CaptionUpdate, int] | None:
        """Pick the next replay item, interleaving drafts fairly with finals.

        What: after ``MAX_FINALS_BEFORE_ACTIVE`` finals, prefers a ready draft
        from the buffer; otherwise reads the next final from the shared log;
        falls back to a ready draft. Returns ``(update, through_sequence)``
        or ``None`` when nothing is ready.
        Called by: ``Client._next_delivery`` (replay mode), with
        ``read_next_final = Server.read_next_final``.
        """
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


# =============================================================================
# 5. Task helpers
# =============================================================================


async def _wait_task(task: asyncio.Task, timeout: float):
    """Preserve caller cancellation and keep timed-out probes alive.

    What: waits up to ``timeout`` for ``task`` without cancelling it on
    timeout; raises ``asyncio.TimeoutError`` if not done, else returns its
    result.
    Called by: ``Client._probe_loop``, ``Client._send``, ``Client._close``.
    """
    done, _ = await asyncio.wait((task,), timeout=max(0.0, timeout))
    if not done:
        raise asyncio.TimeoutError
    return task.result()


async def _stop_task(task: asyncio.Task) -> None:
    """Cancel a task if still running and wait for it to finish.

    What: cancels ``task`` (if not done) and awaits it, swallowing
    cancellation and connection-closed errors.
    Called by: ``Client._probe_loop``, ``Client._send``, ``Client._close``.
    """
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
        await task
