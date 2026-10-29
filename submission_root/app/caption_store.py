"""Revision state and bounded recovery history.

Pure logic: no socket, no asyncio, no ``await`` anywhere in this module. The
``Server`` owns exactly one instance and is the only thing that talks to it,
which makes the revision/dedup rules unit-testable without a running server.

Two invariants live here, and the rest of the system depends on both:

1. **A final caption is durable.** Once a segment is final, nothing overwrites
   it -- not a higher revision, not a replay, nothing.
2. **Revisions only move forward.** A revision less than or equal to the one
   already held for a segment is rejected, which covers duplicates and
   out-of-order/stale arrivals in one rule.

Because of (1), a given segment is appended to the history exactly once, which
is what lets a reconnecting client receive HISTORY and live updates without
ever seeing the same final twice.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass

from .protocol import CaptionUpdate


@dataclass(frozen=True)
class SegmentState:
    """The winning state for one segment, plus when the server accepted it.

    Composed from a ``CaptionUpdate`` rather than subclassing it. Private to
    this module: everything outside ``CaptionStore`` only ever sees the plain
    ``CaptionUpdate``.
    """

    update: CaptionUpdate
    received_at: float

    @property
    def segment(self) -> int:
        return self.update.segment

    @property
    def revision(self) -> int:
        return self.update.revision

    @property
    def final(self) -> bool:
        return self.update.final


class CaptionStore:
    """Current state per segment plus a bounded window of finalized captions."""

    # Recovery requests ask for at most 60 s of context, so a 90 s window is
    # comfortably sufficient while still being a hard, stated bound. The item
    # cap is the backstop for a source that emits far faster than the public
    # generator's ~0.75 s/segment.
    HISTORY_HORIZON_SECONDS = 90.0
    HISTORY_MAX_ITEMS = 2000

    # Backstop for a pathological source that opens segments without ever
    # finalizing them: such segments never enter the history, so the time and
    # item bounds above would not reclaim them.
    MAX_TRACKED_SEGMENTS = 4000

    def __init__(self) -> None:
        self._segments: dict[int, SegmentState] = {}
        self._final_history: deque[SegmentState] = deque()
        # Segments below this watermark have been evicted from the bounded
        # history. Rejecting them keeps a stale revision for a long-forgotten
        # segment from being re-accepted once its state has been reclaimed.
        self._retired_before = 0

    # -- ingest ---------------------------------------------------------

    def validate_and_apply(self, update: CaptionUpdate) -> CaptionUpdate | None:
        """Accept ``update`` and return it, or return ``None`` if it is not new.

        Returning ``None`` means "do not broadcast this" -- it is a duplicate,
        a stale revision, or an attempt to revise an already-final segment.
        """
        if update.segment < self._retired_before:
            return None

        existing = self._segments.get(update.segment)
        if existing is not None:
            if existing.final:
                return None
            if update.revision <= existing.revision:
                return None

        state = SegmentState(update=update, received_at=time.monotonic())
        self._segments[update.segment] = state

        if update.final:
            self._final_history.append(state)
            self._trim_history()
        self._trim_segments()
        return update

    # -- recovery -------------------------------------------------------

    def compute_history(
        self, last_segment: int, last_revision: int
    ) -> list[CaptionUpdate]:
        """Finalized captions newer than the client's latest finalized state.

        Ordered oldest to newest (the deque is in append order). Only finals
        are ever returned, which is what the resume contract requires.

        The client's requested ``history_seconds`` is deliberately *not* used
        to shrink this result. The bounded window is already the cap, and a
        client can only be harmed by receiving fewer finals than it missed:
        anything it already holds is excluded by the ``(segment, revision)``
        comparison below, so sending the whole window costs a late joiner
        nothing and maximizes the finals it recovers.
        """
        marker = (last_segment, last_revision)
        return [
            state.update
            for state in self._final_history
            if (state.segment, state.revision) > marker
        ]

    # -- bounds ---------------------------------------------------------

    def _trim_history(self) -> None:
        cutoff = time.monotonic() - self.HISTORY_HORIZON_SECONDS
        while self._final_history and (
            len(self._final_history) > self.HISTORY_MAX_ITEMS
            or self._final_history[0].received_at < cutoff
        ):
            evicted = self._final_history.popleft()
            self._retire(evicted.segment)

    def _trim_segments(self) -> None:
        while len(self._segments) > self.MAX_TRACKED_SEGMENTS:
            self._retire(min(self._segments))

    def _retire(self, segment: int) -> None:
        self._segments.pop(segment, None)
        self._retired_before = max(self._retired_before, segment + 1)

    # -- observability --------------------------------------------------

    @property
    def tracked_segments(self) -> int:
        return len(self._segments)

    @property
    def history_size(self) -> int:
        return len(self._final_history)
