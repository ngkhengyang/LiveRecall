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
from typing import Callable

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

    # Bound final history by age, not count: a burst must not evict finals
    # still needed for a 60 s recovery request. Memory scales with the source
    # rate within this fixed window, rather than the full lecture duration.
    HISTORY_HORIZON_SECONDS = 90.0

    # Backstop for a pathological source that opens segments without ever
    # finalizing them: such segments never enter the history, so its age
    # cutoff would not reclaim them.
    MAX_ACTIVE_SEGMENTS = 4000

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._segments: dict[int, SegmentState] = {}
        self._active_segments: set[int] = set()
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
        self._trim_history()
        if update.segment < self._retired_before:
            return None

        existing = self._segments.get(update.segment)
        if existing is not None:
            if existing.final:
                return None
            if update.revision <= existing.revision:
                return None

        state = SegmentState(update=update, received_at=self._clock())
        self._segments[update.segment] = state

        if update.final:
            self._active_segments.discard(update.segment)
            self._final_history.append(state)
            self._trim_history()
        else:
            self._active_segments.add(update.segment)
        self._trim_segments()
        return update

    # -- recovery -------------------------------------------------------

    def compute_history(
        self, last_segment: int, last_revision: int, history_seconds: float = 60.0
    ) -> list[CaptionUpdate]:
        """Finalized captions newer than the client's latest finalized state.

        Ordered oldest to newest (the deque is in append order). Only finals
        are ever returned, which is what the resume contract requires.

        Expire history even when the source is idle. The requested window is
        measured since server acceptance using the same monotonic clock as
        retention, independently of source/client wall-clock skew. The 90 s
        storage margin does not enlarge the client's requested 0--60 s view.
        """
        self._trim_history()
        if history_seconds <= 0:
            return []
        cutoff = self._clock() - min(history_seconds, 60.0)
        marker = (last_segment, last_revision)
        return [
            state.update
            for state in self._final_history
            if state.received_at >= cutoff and (state.segment, state.revision) > marker
        ]

    # -- bounds ---------------------------------------------------------

    def _trim_history(self) -> None:
        cutoff = self._clock() - self.HISTORY_HORIZON_SECONDS
        while self._final_history and self._final_history[0].received_at < cutoff:
            evicted = self._final_history.popleft()
            self._retire(evicted.segment)

    def _trim_segments(self) -> None:
        # Only unfinished segments need a count backstop. Finals are reclaimed
        # by _trim_history so the required recovery window remains intact.
        while len(self._active_segments) > self.MAX_ACTIVE_SEGMENTS:
            self._retire(min(self._active_segments))

    def _retire(self, segment: int) -> None:
        self._segments.pop(segment, None)
        self._active_segments.discard(segment)
        self._retired_before = max(self._retired_before, segment + 1)

    # -- observability --------------------------------------------------

    @property
    def tracked_segments(self) -> int:
        return len(self._segments)

    @property
    def history_size(self) -> int:
        return len(self._final_history)
