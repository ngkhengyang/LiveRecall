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
#
# One Client per connected viewer. It owns that viewer's mailbox
# (OutboundBuffer) and runs two background tasks side by side:
#   - _sender_loop: the only code that writes captions to the socket.
#   - _probe_loop:  pings the viewer to detect when it falls behind.
# The server talks to it through just two entry points: run() when the viewer
# connects, and enqueue() for every new caption.
# =============================================================================


class Client:
    """Own one receiver's buffer, sender, probes, and mode transitions.

    Created by ``Server._accept`` when a ``CLIENT_HELLO`` arrives.
    """

    # -------------------------------------------------------------------------
    # 1.1 Settings and state
    #
    # Tunable limits and the per-viewer objects:
    #   TOO_FAR_BEHIND         close code (1013 "try again later") for giving up.
    #   MAX_STALL_SECONDS      longest a send or a ping may hang before closing.
    #   CLOSE_TIMEOUT_SECONDS  longest a close may hang before aborting TCP.
    #   PROBE_INTERVAL_SECONDS gap between pings.
    #   MAX_BURST_ITEMS/BYTES  burst size during catch-up (see 1.6).
    # State: _buffer (mailbox), _lag (timing maths), _cursor (replay bookmark),
    # _mode (NORMAL/REPLAY), _wake (wakes the sender), _probe_tick (releases a
    # held sender), _held (diagnostic count of catch-up pauses).
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
        # --- Connection inputs ------------------------------------------------
        self.ws = ws
        self.hello = hello
        self.server = server

        # --- Helpers: mailbox, timing maths, replay bookmark ---------------
        self._buffer = OutboundBuffer()
        self._lag = LagPolicy()
        self._cursor = ReplayCursor()
        self._mode = Mode.NORMAL

        # --- Signals between tasks -----------------------------------------
        # _wake: "there is something to send" (set by enqueue / probe loop).
        # _probe_tick: "a probe finished" (releases a sender in _hold).
        self._wake = asyncio.Event()
        self._probe_tick = asyncio.Event()

        # --- Background tasks and diagnostics ------------------------------
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
    #
    # A connection from start to finish:
    #   register -> send initial history -> start sender + probe tasks
    #   -> wait until the viewer disconnects -> cancel tasks -> unregister.
    # The initial history ends at a position in the shared list of finals;
    # the replay cursor starts there so no final is skipped or sent twice.
    # -------------------------------------------------------------------------

    async def run(self) -> None:
        """Run this receiver from hello to disconnect.

        What: registers with the server, sends initial history, starts the
        sender and probe tasks, waits for the socket to close, then cancels
        tasks, unregisters, and logs stats.
        Called by: ``Server._accept``.
        """
        # --- 1. Join the broadcast list (enqueue() starts receiving updates) --
        self.server.register_client(self)
        try:
            # --- 2. Send recent context; stop if the socket already failed ---
            # The helper captures its snapshot before its first network await.
            # Its caption references expire before the long-lived receive loop.
            snapshot_tail = await self._send_initial_history()
            if snapshot_tail is None:
                return

            # --- 3. Start the replay bookmark where the history ended -------
            self._cursor.position = snapshot_tail

            # --- 4. Launch the sender and the lag prober in the background --
            # Ablation: never start the probe loop.
            self._tasks = [asyncio.create_task(self._sender_loop())]

            # --- 5. Wait until the viewer disconnects (incoming messages are
            #        ignored; iterating just keeps the connection serviced) ---
            async for _ in self.ws:
                pass
        except ConnectionClosed:
            pass
        finally:
            # --- 6. Cleanup: cancel both tasks and wait for them to finish ---
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
                    await task

            # --- 7. Leave the broadcast list and log this viewer's stats -----
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
        # --- 1. Snapshot recent captions after the viewer's resume point,
        #        plus where that snapshot ends in the shared list of finals ---
        items, snapshot_tail = self.server.history_snapshot(
            self.hello.last_segment,
            self.hello.last_revision,
            self.hello.history_seconds,
        )

        # --- 2. Log the new connection -----------------------------------
        print(
            f"client connected: id={self.hello.client_id}; "
            f"resume=({self.hello.last_segment},{self.hello.last_revision}); "
            f"history={len(items)}; clients={self.server.client_count}",
            flush=True,
        )

        # --- 3. Send the snapshot in size-limited HISTORY batches --------
        for history in History.batches(items):
            if not await self._send(history):
                return None

        # --- 4. Tell run() where the cursor should start -------------------
        return snapshot_tail

    # -------------------------------------------------------------------------
    # 1.3 Golden path: delivery
    #
    # How one caption reaches the viewer:
    #   enqueue()        put it in the mailbox (never waits on the network),
    #                    wake the sender.
    #   _sender_loop()   loop: choose next -> send -> advance the cursor.
    #   _next_delivery() NORMAL: oldest item in the mailbox.
    #                    REPLAY: whatever ReplayCursor picks.
    #   _send()          socket write with a stall timeout.
    # The cursor only moves after a send succeeds, so a final is never skipped.
    # -------------------------------------------------------------------------

    def enqueue(self, update: CaptionUpdate) -> None:
        """Accept broadcast work synchronously; never wait for this socket.

        What: in normal mode, offers the update to the buffer and escalates
        to replay under pressure; in replay mode, keeps only drafts (finals
        will come from the shared log). Then wakes the sender.
        Called by: ``Server.broadcast``.
        """
        # --- REPLAY: finals come from the shared history, so drop the
        #     segment's draft; drafts replace the previous one ---------------
        if self._mode is Mode.REPLAY:
            if update.final:
                self._buffer.discard_segment(update.segment)
            else:
                self._buffer.offer(update)

        # --- NORMAL: queue it; if the mailbox is too full, switch to replay -
        else:
            self._buffer.offer(update)
            if self._buffer.pressured or self._buffer.should_close:
                self._escalate_to_replay(update)

        # --- Either way, wake the sender -----------------------------------
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
                # --- 1. Sleep until enqueue() or the probe loop wakes us -----
                await self._wake.wait()
                self._wake.clear()

                # --- 2. Start a fresh burst --------------------------------
                sent_items = 0
                sent_bytes = 0

                # --- 3. Drain: keep going while the mailbox has items, or
                #        while replaying (the shared history may have more) --
                while self._buffer.has_pending() or self._mode is Mode.REPLAY:
                    # 3a. Mailbox hit a hard limit: stop and close below.
                    if self._buffer.should_close:
                        break

                    # 3b. Catch-up pacing: burst is full, so wait for a pong.
                    if self._window_full(sent_items, sent_bytes):
                        await self._hold()
                        sent_items = 0
                        sent_bytes = 0
                        if self._buffer.should_close:
                            break

                    # 3c. Choose what to send; nothing ready -> back to sleep.
                    selected = self._next_delivery()
                    if selected is None:
                        break
                    delivery, through_sequence = selected

                    # 3d. Send it; a stalled send already closed the socket.
                    if not await self._send(delivery):
                        return

                    # 3e. Sent: move the bookmark and count the burst.
                    # The local delivery owns the in-flight message. A successful
                    # send commits transport progress, not an application ACK.
                    self._cursor.position = through_sequence
                    sent_items += 1
                    sent_bytes += len(delivery.to_json())

                # --- 4. Hard limit was hit: give up on this viewer ----------
                if self._buffer.should_close:
                    await self._close("latest active state exceeds delivery capacity")
                    return

        # --- Finals this viewer still needed were removed from the shared
        #     history: it can never catch up, so close -----------------------
        except ReplayExpired:
            await self._close("outstanding finals exceeded shared history retention")
        # --- Viewer already gone: just stop ---------------------------------
        except ConnectionClosed:
            return

    def _next_delivery(self) -> tuple[CaptionUpdate, int] | None:
        """Choose what to send next and the final-log position it covers.

        What: in replay mode, asks ``ReplayCursor.next_replay``; if nothing is
        left, tries to return to normal mode. In normal mode, pops the
        buffer's oldest update. Returns ``(update, through_sequence)``.
        Called by: ``_sender_loop``.
        """
        # --- 1. REPLAY: let the cursor pick (next final or a waiting draft) --
        if self._mode is Mode.REPLAY:
            selected = self._cursor.next_replay(
                self._buffer, self.server.read_next_final
            )
            if selected is not None:
                return selected
            # Nothing left to replay. Still in the lease -> wait; otherwise
            # switch to NORMAL and fall through.
            if not self._leave_replay_if_caught_up():
                return None

        # --- 2. NORMAL: take the oldest item from the mailbox --------------
        delivery = self._buffer.next_update()
        if delivery is None:
            return None

        # --- 3. Work out the bookmark after sending it: a final moves it to
        #        that final's position; a draft leaves it unchanged ----------
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
        # Run the write as a task so it can be timed out.
        sending = asyncio.create_task(self.ws.send(delivery.to_json()))
        try:
            # Finished within the stall budget: success.
            await _wait_task(sending, self.MAX_STALL_SECONDS)
            return True
        except asyncio.TimeoutError:
            # Stalled too long: the viewer is unreachable, so close.
            await self._close("socket send exceeded stall budget")
            return False
        finally:
            # Never leave the write task running.
            await _stop_task(sending)

    # -------------------------------------------------------------------------
    # 1.4 Replay mode transitions
    #
    # Replay is the mode for a viewer that has fallen behind. Finals stop
    # going through the mailbox and are read back from the server's shared
    # history, starting at the cursor. Only the latest draft for each segment
    # stays in the mailbox, so a slow viewer uses little memory and loses
    # no finals.
    #   _enter_replay()              switch on (late ping or full mailbox).
    #   _escalate_to_replay()        switch on from enqueue() when the mailbox
    #                                is full, then file the triggering update.
    #   _leave_replay_if_caught_up() switch off once nothing is left to replay
    #                                and the catch-up lease has expired.
    # -------------------------------------------------------------------------

    def _enter_replay(self) -> None:
        """Switch to replay mode.

        What: sets mode to REPLAY, drops queued finals from the buffer (they
        will be re-read from the shared log), and wakes the sender.
        Called by: ``_probe_loop`` (late probe), ``_escalate_to_replay``.
        """
        # Only act on the first switch; repeated calls just wake the sender.
        if self._mode is not Mode.REPLAY:
            # Switch mode and hand queued finals over to the shared history.
            self._mode = Mode.REPLAY
            self._buffer.drop_finals_keep_latest_active()
            print(
                f"client catch-up: id={self.hello.client_id}; replaying shared finals",
                flush=True,
            )
        # Wake the sender so it starts replaying right away.
        self._wake.set()

    def _escalate_to_replay(self, update: CaptionUpdate) -> None:
        """Enter replay because the buffer is under pressure.

        What: enters replay, then makes sure the triggering update ends up in
        the right place (finals dropped from buffer, refused drafts re-offered).
        Called by: ``enqueue``.
        """
        # Remember whether offer() refused this update (entering replay
        # resets the flag).
        refused = self._buffer.should_close
        self._enter_replay()

        # Final: the shared history has it, so drop any leftover draft.
        if update.final:
            # A refused final may have left its older draft in the buffer.
            self._buffer.discard_segment(update.segment)
        # Refused draft: now there is room, so queue it again.
        elif refused:
            self._buffer.offer(update)

    def _leave_replay_if_caught_up(self) -> bool:
        """Called only after the log has no next final and no ready active item.

        What: if the catch-up lease has expired, returns to NORMAL mode,
        turns off coalescing, and starts a normal-mode trial. Returns whether
        it switched.
        Called by: ``_next_delivery``.
        """
        # Catch-up lease still running: stay in replay for now.
        if self._catchup:
            return False

        # Switch back: keep every revision again, reset the fairness counter.
        self._mode = Mode.NORMAL
        self._buffer.set_coalescing(False)
        self._cursor.finals_since_active = 0

        # Start a normal trial; if lag returns soon, LagPolicy lengthens the
        # next lease.
        self._lag.start_normal_trial(asyncio.get_running_loop().time())
        return True

    # -------------------------------------------------------------------------
    # 1.5 Lag probing
    #
    # Detects that the viewer has fallen behind. A successful ws.send() only
    # means the bytes reached the OS buffer, so it says nothing about the
    # viewer. A ping, though, goes through the same connection as the
    # captions, so its pong cannot return until earlier data has gone through.
    #   pong on time  -> healthy, update the smoothed RTT.
    #   pong late     -> enter replay, keep extending the catch-up lease.
    #   no pong 75 s  -> close the connection.
    # Every finished probe also sets _probe_tick, which releases a sender
    # paused in _hold() (see 1.6).
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
                # --- 1. Send one ping; note when and the hard deadline ------
                loop = asyncio.get_running_loop()
                started = loop.time()
                deadline = started + self.MAX_STALL_SECONDS
                self._pending_probe = asyncio.create_task(self._round_trip())
                was_late = False

                try:
                    # --- 2. Wait for the pong up to the "behind" threshold --
                    latency = await _wait_task(
                        self._pending_probe,
                        min(self._lag.behind_threshold(), self.MAX_STALL_SECONDS),
                    )
                except asyncio.TimeoutError:
                    # --- 3. Pong is late: the viewer is behind --------------
                    was_late = True
                    self._lag.lag_started(loop.time())
                    self._enter_replay()

                    # --- 4. Keep waiting for the SAME pong in short steps,
                    #        extending the catch-up lease each time ----------
                    while True:
                        remaining = deadline - loop.time()
                        # 4a. Out of stall budget: give up on this viewer.
                        if remaining <= 0:
                            await self._close("probe exceeded stall budget")
                            return
                        # 4b. Still lagging: push the lease forward.
                        self._lag.still_lagging(loop.time())
                        # 4c. Wait a bit more; pong arrived -> leave the loop.
                        try:
                            latency = await _wait_task(
                                self._pending_probe,
                                min(self.PROBE_INTERVAL_SECONDS, remaining),
                            )
                            break
                        except asyncio.TimeoutError:
                            pass

                # --- 5. Pong received: update RTT and release the sender ----
                self._pending_probe = None
                self._lag.probe_finished(started, loop.time(), latency, was_late)
                self._probe_tick.set()
                self._wake.set()

                # --- 6. Pause before the next ping --------------------------
                await asyncio.sleep(self.PROBE_INTERVAL_SECONDS)
        except ConnectionClosed:
            return
        finally:
            # --- Cleanup: stop any ping in flight, end catch-up, and release
            #     a sender that may be waiting in _hold() --------------------
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
        # Send the ping; this returns a future that completes on the pong.
        pong = await self.ws.ping()
        # Wait for the pong and return its latency.
        return await pong

    # -------------------------------------------------------------------------
    # 1.6 Catch-up flow control
    #
    # Controls how fast to send while recovering. Sending a large backlog at
    # once would refill the network buffers and stall the viewer again. So,
    # only during catch-up:
    #   send a burst (<= 64 items or 256 KB) -> _hold() until the next pong
    #   -> next burst -> ...
    # Each pong confirms that the previous burst has gone through. Outside
    # catch-up, _window_full() is always False and the sender never pauses.
    # -------------------------------------------------------------------------

    def _window_full(self, sent_items: int, sent_bytes: int) -> bool:
        """Whether the sender should pause for the next probe.

        What: during catch-up, true at the start of a burst or once the burst
        reaches ``MAX_BURST_ITEMS`` / ``MAX_BURST_BYTES``; always false
        otherwise.
        Called by: ``_sender_loop``.
        """
        # Only pace during catch-up. Then pause before the first item of a
        # burst, or once the burst reaches its item or byte limit.
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
        # Count the pause (shown in the disconnect log).
        self._held += 1
        # Clear first so only a probe that finishes AFTER now releases us.
        self._probe_tick.clear()
        await self._probe_tick.wait()

    # -------------------------------------------------------------------------
    # 1.7 Shutdown
    #
    # Gives up on a viewer that cannot be saved (send or ping stalled too long,
    # mailbox hit a hard limit, or its finals were removed from the shared
    # history). Sends close code 1013 with a reason; if the close itself
    # hangs, aborts the TCP connection.
    # -------------------------------------------------------------------------

    async def _close(self, reason: str) -> None:
        """Close the socket with code 1013, aborting if close stalls.

        What: sends a WebSocket close with ``reason``; if that does not finish
        within ``CLOSE_TIMEOUT_SECONDS``, aborts the transport.
        Called by: ``_sender_loop``, ``_probe_loop``, ``_send``.
        """
        # Start a polite WebSocket close (code 1013 + reason).
        closing = asyncio.create_task(self.ws.close(self.TOO_FAR_BEHIND, reason))
        try:
            # Give it a short time to finish.
            await _wait_task(closing, self.CLOSE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            # Close hung (peer not reading): kill the TCP connection.
            transport = getattr(self.ws, "transport", None)
            if transport is not None:
                transport.abort()
        except ConnectionClosed:
            # Already closed: nothing to do.
            pass
        finally:
            # Never leave the close task running.
            await _stop_task(closing)


# =============================================================================
# 2. Mode
#
# The two delivery modes. NORMAL sends every revision from the mailbox in
# order. REPLAY reads finals from the shared history and keeps only the
# latest drafts locally (see 1.4).
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
#
# The timing maths behind lag probing. It does no networking: callers pass in
# timestamps, so it is easy to reason about.
#   rtt            smoothed round-trip time; makes the "behind" threshold
#                  adapt to each viewer (max(0.5 s, 6 x rtt)).
#   catchup_until  end of the catch-up lease. Stops the client from flipping
#                  between NORMAL and REPLAY every few hundred ms.
#   backoff        if lag returns soon after going back to NORMAL (a failed
#                  normal trial), the lease doubles, up to 30 s.
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
        # Only judge a normal trial if one is running and lag was seen before.
        if self.normal_trial_started is not None and self.last_lag_delay > 0:
            # Probation: how long NORMAL must last to count as a success
            # (at least 12 s or twice the last lag, at most 30 s).
            probation = min(
                self.MAX_CATCHUP_SECONDS,
                max(4 * self.CATCHUP_STICKY_SECONDS, 2 * self.last_lag_delay),
            )
            # Lag came back too soon: the trial failed, so double the lease.
            if now - self.normal_trial_started < probation:
                self.catchup_lease = min(
                    self.MAX_CATCHUP_SECONDS, 2 * self.catchup_lease
                )
            # Trial lasted long enough: reset the lease to the default.
            else:
                self.catchup_lease = self.CATCHUP_STICKY_SECONDS
        # Either way the trial is over.
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
        # Remember how long a late probe took (used by lag_started's probation).
        if was_late:
            self.last_lag_delay = now - started
        # Fold the latency into the smoothed RTT (moving 25% toward the sample).
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
#
# The bookmark and selector used during REPLAY. `position` is how far this
# viewer has got through the shared list of finals. next_replay() reads the
# next final from there, but after every 4 finals it slips in a waiting draft
# so the viewer's live text does not freeze while it catches up. It only
# chooses; the sender moves `position` after a successful send.
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
        # --- 1. Fairness: after 4 finals in a row, send a waiting draft so
        #        the live text does not freeze --------------------------------
        if self.finals_since_active >= self.MAX_FINALS_BEFORE_ACTIVE:
            active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
            if active is not None:
                self.finals_since_active = 0
                return active, self.position

        # --- 2. Normal replay step: the next final after the bookmark -------
        selected = read_next_final(self.position)
        if selected is not None:
            self.finals_since_active += 1
            return selected

        # --- 3. No finals left: send a waiting draft if one is ready -------
        active = buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
        if active is not None:
            self.finals_since_active = 0
            return active, self.position

        # --- 4. Nothing ready -------------------------------------------
        return None


# =============================================================================
# 5. Task helpers
#
# Small asyncio utilities for socket operations that may hang.
#   _wait_task  waits with a timeout but does NOT cancel on timeout, so a
#               late ping can keep running and still be awaited later.
#   _stop_task  cancels a task if needed and waits for it, ignoring errors.
# =============================================================================


async def _wait_task(task: asyncio.Task, timeout: float):
    """Preserve caller cancellation and keep timed-out probes alive.

    What: waits up to ``timeout`` for ``task`` without cancelling it on
    timeout; raises ``asyncio.TimeoutError`` if not done, else returns its
    result.
    Called by: ``Client._probe_loop``, ``Client._send``, ``Client._close``.
    """
    # asyncio.wait (unlike wait_for) does not cancel the task on timeout.
    done, _ = await asyncio.wait((task,), timeout=max(0.0, timeout))
    # Still running: report a timeout but leave the task alive.
    if not done:
        raise asyncio.TimeoutError
    # Finished: return its result (or re-raise its exception).
    return task.result()


async def _stop_task(task: asyncio.Task) -> None:
    """Cancel a task if still running and wait for it to finish.

    What: cancels ``task`` (if not done) and awaits it, swallowing
    cancellation and connection-closed errors.
    Called by: ``Client._probe_loop``, ``Client._send``, ``Client._close``.
    """
    # Ask it to stop if it is still running.
    if not task.done():
        task.cancel()
    # Wait for it to actually finish, ignoring the expected errors.
    with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
        await task
