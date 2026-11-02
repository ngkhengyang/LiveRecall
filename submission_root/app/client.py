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
from typing import TYPE_CHECKING

from websockets.exceptions import ConnectionClosed

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
    # The lag threshold is also scaled against this
    # connection's own measured round trip, so a link with real delay and
    # jitter is judged against what is normal *for that link* rather than
    # against a loopback constant. The supplied impairment profile alone is
    # 120 ms +/- 30 ms with 3% loss, which a fixed threshold would read as a
    # stalled reader.
    BEHIND_AFTER_SECONDS = 0.5
    # Upper bound on one hold. A client that never recovers still makes
    # forward progress on finals instead of stalling outright.
    MAX_HOLD_SECONDS = 0.5
    # Minimum catch-up time after an overdue pong. A reader
    # that is behind drains just enough to answer a ping and then falls back,
    # so without a sticky window the sender resumes flooding the moment a
    # single pong lands and the client spends its read budget on revisions
    # that are obsolete by the time it reaches them.
    CATCHUP_STICKY_SECONDS = 3.0
    BEHIND_RTT_MULTIPLIER = 6.0
    RTT_SMOOTHING = 0.25
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

    def _behind_threshold(self) -> float:
        return max(self.BEHIND_AFTER_SECONDS, self.BEHIND_RTT_MULTIPLIER * self._rtt)

    @property
    def _catchup(self) -> bool:
        """True while a recently overdue pong still governs this client."""
        return asyncio.get_running_loop().time() < self._catchup_until

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
        self._buffer.set_overloaded(self._catchup)
        self._buffer.offer(update)
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
                if self._catchup:
                    # Hold once per drain, so individual finals have no fixed rate cap.
                    await self._hold()

                while self._buffer.has_pending():
                    if self._buffer.should_close:
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
                    await self.ws.send(delivery.to_json())
                    self._in_flight = None

                if self._buffer.should_close:
                    await self.ws.close(
                        self.TOO_FAR_BEHIND, "pending delivery capacity exhausted"
                    )
                    return
        except ConnectionClosed:
            return

    def _next_delivery(self) -> CaptionUpdate | None:
        """Prioritize finals during catch-up; live traffic is CAPTION_UPDATE only."""
        # Lag can change while updates are already queued. Compact that backlog
        # on entry to catch-up, and preserve new revisions once lag clears.
        self._buffer.set_overloaded(self._catchup)
        catching_up = self._catchup or self._buffer.pressured
        if catching_up and self._finals_since_active >= self.MAX_FINALS_BEFORE_ACTIVE:
            active = self._buffer.take_active_update(self.PROVISIONAL_GRACE_SECONDS)
            if active is not None:
                self._finals_since_active = 0
                return active
        delivery = self._buffer.next_update(
            prefer_final=catching_up,
            provisional_grace=self.PROVISIONAL_GRACE_SECONDS,
        )
        if delivery is not None:
            if catching_up and delivery.final:
                self._finals_since_active += 1
            else:
                self._finals_since_active = 0
        return delivery

    async def _hold(self) -> None:
        """Wait for the next completed probe, or ``MAX_HOLD_SECONDS``."""
        self._held += 1
        self._probe_tick.clear()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._probe_tick.wait(), timeout=self.MAX_HOLD_SECONDS
            )

    async def _probe_loop(self) -> None:
        """Keep exactly one ping in flight and publish the verdict.

        One overdue pong keeps catch-up active until it arrives, followed by
        a drain window based on its delay. Timely probes let the window expire.
        This task keeps only one consumption probe outstanding.
        """
        try:
            while True:
                pong = await self.ws.ping()
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
                    while True:
                        self._catchup_until = (
                            asyncio.get_running_loop().time()
                            + self.CATCHUP_STICKY_SECONDS
                        )
                        self._wake.set()
                        try:
                            latency = await asyncio.wait_for(
                                asyncio.shield(pong), timeout=self.MAX_HOLD_SECONDS
                            )
                            # Let downstream backlog drain before resuming revisions.
                            if isinstance(latency, (int, float)):
                                self._catchup_until = (
                                    asyncio.get_running_loop().time()
                                    + max(self.CATCHUP_STICKY_SECONDS, latency)
                                )
                            break
                        except asyncio.TimeoutError:
                            continue
                self._probe_tick.set()
                self._wake.set()
                await asyncio.sleep(self.PROBE_INTERVAL_SECONDS)
        except ConnectionClosed:
            return
        except asyncio.CancelledError:
            raise
        finally:
            # Never leave the sender blocked on a connection that is going away.
            self._catchup_until = 0.0
            self._probe_tick.set()
