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
from dataclasses import dataclass
from typing import Callable

from .protocol import CaptionUpdate


class ReplayExpired(Exception):
    """A connected receiver's outstanding finals exceeded shared retention."""


@dataclass(frozen=True)
class SegmentState:
    """The winning state for one segment, plus when the server accepted it.

    Composed from a ``CaptionUpdate`` rather than subclassing it. Private to
    this module: everything outside ``CaptionStore`` only ever sees the plain
    ``CaptionUpdate``.
    """

    update: CaptionUpdate
    received_at: float
    final_sequence: int = 0

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
        # Accepted finals have contiguous sequence numbers independent of the
        # source's segment IDs. Dict insertion order preserves acceptance order;
        # sequence lookup lets each receiver read its next outstanding final.
        self._final_history: dict[int, SegmentState] = {}
        self._final_tail = 0
        self._evicted_final_sequence = 0
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

        if update.final:
            self._final_tail += 1
        state = SegmentState(
            update=update,
            received_at=self._clock(),
            final_sequence=self._final_tail if update.final else 0,
        )
        self._segments[update.segment] = state

        if update.final:
            self._active_segments.discard(update.segment)
            self._final_history[state.final_sequence] = state
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

        Ordered oldest to newest (the log is in acceptance order). Only finals
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
            for state in self._final_history.values()
            if state.received_at >= cutoff and (state.segment, state.revision) > marker
        ]

    def history_snapshot(
        self, last_segment: int, last_revision: int, history_seconds: float = 60.0
    ) -> tuple[list[CaptionUpdate], int]:
        """Capture requested context and its live-subscription boundary together.

        All methods here are synchronous. A caller registering immediately
        before this call can send HISTORY, then replay only finals accepted
        after this watermark. An empty or zero-second context request must not
        cause older, intentionally unrequested finals to replay as live work.
        """
        items = self.compute_history(last_segment, last_revision, history_seconds)
        return items, self._final_tail

    @property
    def final_tail(self) -> int:
        """Sequence of the last accepted final, including already expired ones."""
        return self._final_tail

    def final_sequence(self, update: CaptionUpdate) -> int:
        """Find a retained accepted final's sequence before starting its send.

        The sender can retain this small sequence alongside an in-flight
        message and commit its cursor only after sending succeeds. Looking the
        sequence up after a long send may fail because retention kept advancing.
        """
        self._trim_history()
        state = self._segments.get(update.segment)
        if state is None or not state.final or state.update != update:
            raise ReplayExpired(f"Final for segment {update.segment} is not retained")
        return state.final_sequence

    def read_next_final(self, after_sequence: int) -> tuple[CaptionUpdate, int] | None:
        """Read the next outstanding final without advancing a receiver's cursor.

        This is continuous delivery from the shared 90-second log, independent
        of the requested reconnect-context window. Expired outstanding prefixes
        raise even when the log is empty; they must never be skipped silently.
        The caller sends one CAPTION_UPDATE and commits this sequence only
        after success. Hello recovery framing and live burst limits belong
        to their respective send paths, not this log lookup.
        """
        self._trim_history()
        if after_sequence < self._evicted_final_sequence:
            raise ReplayExpired(
                f"Final replay after sequence {after_sequence} expired through "
                f"{self._evicted_final_sequence}"
            )
        if after_sequence >= self._final_tail:
            return None
        sequence = after_sequence + 1
        return self._final_history[sequence].update, sequence

    # -- bounds ---------------------------------------------------------

    def _trim_history(self) -> None:
        cutoff = self._clock() - self.HISTORY_HORIZON_SECONDS
        while self._final_history:
            sequence = self._evicted_final_sequence + 1
            evicted = self._final_history[sequence]
            if evicted.received_at >= cutoff:
                break
            del self._final_history[sequence]
            self._evicted_final_sequence = sequence
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
