"""Bounded, coalescing outbound buffer for one client.

Pure logic: no socket, no asyncio, no ``await``. It is handed ``CaptionUpdate``
objects and asked for the next one to send; it never sends anything itself.
This is where the overload policy lives, so it is deliberately testable in
isolation -- feed it updates, ask for what comes out, assert on the order.

Why ``dict[int, CaptionUpdate]`` keyed by segment is the right shape:

* Python dicts preserve insertion order, and re-assigning an existing key does
  **not** move it. So ``offer`` simultaneously replaces a superseded
  provisional revision, keeps that segment's original FIFO slot, and needs no
  separate index bookkeeping.
* It never has to compare revision numbers. ``CaptionStore`` has already
  enforced forward-only revisions and final durability before anything reaches
  ``offer``, so an unconditional overwrite is always correct.
* Coalescing is therefore **emergent, not heuristic**. When the owning client
  is keeping up, each entry is drained before the next revision for that
  segment arrives, so every revision is delivered individually and update
  coverage stays at 100%. Revisions only collapse when delivery is genuinely
  being held back -- the real-backlog condition -- with no "is this client
  slow" branch inside this class to get wrong.
* Finals are never silently dropped. Once an entry is final nothing rewrites
  that key, so it stays until it is sent. Draining is FIFO with no artificial
  pacing, so a client converges immediately once it catches up.
"""

from __future__ import annotations

import time
from typing import Callable

from .protocol import CaptionUpdate


class OutboundBuffer:
    """Pending outbound state for one client, bounded by segment count."""

    # The explicit bound behind "boundedness is part of correctness". At the
    # public generator's pace this is several minutes of backlog, so a healthy
    # client never approaches it; only a genuinely stuck reader does.
    MAX_PENDING_SEGMENTS = 256

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._should_close = False
        self._coalesced = 0
        self._dropped = 0

    # -- state ----------------------------------------------------------

    @property
    def should_close(self) -> bool:
        """True when the only way to stay bounded would be to discard a final.

        The owning ``Client`` closes the connection instead. Every final still
        lives in the server's history, so the client's own reconnect path
        re-syncs it through HISTORY -- which converges far faster than draining
        a long stale backlog, and never loses durable state.
        """
        return self._should_close

    @property
    def pending(self) -> int:
        return len(self._pending)

    @property
    def coalesced(self) -> int:
        """Provisional revisions superseded in place before they were sent."""
        return self._coalesced

    @property
    def dropped(self) -> int:
        """Obsolete provisional revisions evicted to stay inside the bound."""
        return self._dropped

    def has_pending(self) -> bool:
        return bool(self._pending)

    # -- queue ----------------------------------------------------------

    def offer(self, update: CaptionUpdate) -> None:
        if self._should_close:
            # The connection is already being torn down. Accepting more work
            # would let the buffer grow past its bound while the sender task
            # is still blocked inside a send to this stuck reader. Everything
            # dropped here is durable in the server's history and comes back
            # through HISTORY on the client's reconnect.
            return
        if update.segment in self._pending:
            self._coalesced += 1
        else:
            self._queued_at[update.segment] = self._clock()
        self._pending[update.segment] = update
        if len(self._pending) > self.MAX_PENDING_SEGMENTS:
            self._enforce_bound()

    def next_update(
        self, prefer_final: bool = False, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Take the next update to send, or ``None`` if nothing should go yet.

        Default behavior is plain FIFO: the oldest pending segment, whatever
        its state. That is what a client keeping up always gets.

        ``prefer_final`` is used only while the owning client is measurably
        behind, which is exactly the condition the protocol allows obsolete
        provisional revisions to be suppressed in. It takes the oldest final
        ahead of any provisional, so a client with a limited read budget
        spends it on durable state.

        ``provisional_grace`` then holds a lone provisional back for that many
        seconds after it was first queued. Under the deterministic source a
        segment finalizes well inside that window, so the provisional is
        normally replaced in place by its own final and one message carries
        what three would have. Nothing is discarded: a segment that genuinely
        stays open past the grace is still the latest state for that segment
        and is released on the next call, which is what "preserve the newest
        relevant revision of an active segment" requires.
        """
        if not self._pending:
            return None

        oldest = next(iter(self._pending))
        if not prefer_final or self._pending[oldest].final:
            return self._take(oldest)

        for segment, update in self._pending.items():
            if update.final:
                return self._take(segment)

        # Only provisional revisions are waiting.
        if self._clock() - self._queued_at.get(oldest, 0.0) < provisional_grace:
            return None
        return self._take(oldest)

    def _take(self, segment: int) -> CaptionUpdate:
        self._queued_at.pop(segment, None)
        return self._pending.pop(segment)

    # -- bound ----------------------------------------------------------

    def _enforce_bound(self) -> None:
        victim = None
        for segment, update in self._pending.items():
            if not update.final:
                victim = segment
                break

        if victim is None:
            # Every pending entry is final: this client is catastrophically
            # behind on durable state, not merely on provisional chatter.
            # Dropping a final here would break correctness, so close instead.
            self._should_close = True
            return

        self._take(victim)
        self._dropped += 1
