"""One receiver's bounded FIFO queue and coalesced latest caption state.

The owner decides when to replay finals from shared history. This buffer only
handles admission, ordering, replacement, and removal of pending captions.
Dequeuing transfers ownership to the sender; it is not a delivery acknowledgement.
"""

from __future__ import annotations

import time
from typing import Callable, NamedTuple

from .protocol import CaptionUpdate


class Usage(NamedTuple):
    """Projected queue usage after admitting an update."""

    active: int
    pending: int
    bytes: int


class OutboundBuffer:
    """Preserve FIFO revisions normally and latest segment state under pressure.

    The soft entry limit asks the owner to switch finals to shared-log replay.
    Active entries and serialized bytes remain bounded after compaction.
    """

    MAX_PENDING_UPDATES = 256
    MAX_PENDING_ACTIVE_SEGMENTS = 4000
    MAX_PENDING_BYTES = 4 * 1024 * 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        # Distinct keys retain every revision in normal FIFO mode.
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._next_key = 0
        # Coalescing uses one queue entry per segment.
        self._by_segment: dict[int, int] = {}
        self._coalescing_requested = False
        self._pressure_active = False
        self._active_count = 0
        self._pending_bytes = 0
        self._should_close = False
        self._coalesced = 0
        self._rejected = 0

    @property
    def pending(self) -> int:
        return len(self._pending)

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    @property
    def pressured(self) -> bool:
        """Whether the soft limit was reached and the backlog has not drained."""
        return self._pressure_active

    @property
    def should_close(self) -> bool:
        """Whether a hard limit refused an update."""
        return self._should_close

    @property
    def coalesced(self) -> int:
        return self._coalesced

    @property
    def rejected(self) -> int:
        return self._rejected

    @property
    def _coalescing(self) -> bool:
        return self._coalescing_requested or self._pressure_active

    def offer(self, update: CaptionUpdate) -> None:
        """Admit one update, compacting before any hard-limit refusal."""
        if self._should_close:
            self._rejected += 1
            return

        key = self._by_segment.get(update.segment) if self._coalescing else None
        usage = self._usage_after(update, key)
        if not self._pressure_active and (
            usage.pending >= self.MAX_PENDING_UPDATES
            or usage.bytes > self.MAX_PENDING_BYTES
        ):
            was_coalescing = self._coalescing
            self._pressure_active = True
            if not was_coalescing:
                self._compact()
                key = self._by_segment.get(update.segment)
                usage = self._usage_after(update, key)

        if not self._within_hard_limits(usage):
            self._should_close = True
            self._rejected += 1
            return
        self._store(update, key, usage)

    def next_update(self) -> CaptionUpdate | None:
        """Remove the oldest pending update."""
        if not self._pending:
            return None
        return self._take(next(iter(self._pending)))

    def has_pending(self) -> bool:
        return bool(self._pending)

    def set_coalescing(self, on: bool) -> None:
        """Apply the owner's policy while preserving independent pressure state."""
        was_coalescing = self._coalescing
        self._coalescing_requested = on
        if self._coalescing and not was_coalescing:
            self._compact()
        elif not self._coalescing:
            self._by_segment.clear()

    def drop_finals_keep_latest_active(self) -> None:
        """Transfer queued finals to shared replay and retain latest drafts."""
        self.set_coalescing(True)
        for key, update in list(self._pending.items()):
            if update.final:
                self._take(key)
        self._pressure_active = False
        self._should_close = False

    def discard_segment(self, segment: int) -> None:
        """Remove a coalesced draft once its final is available in shared history."""
        key = self._by_segment.get(segment)
        if key is not None:
            self._take(key)

    def take_active_update(
        self, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Remove the oldest draft once its original enqueue time passes grace."""
        for key, update in self._pending.items():
            if not update.final:
                if self._clock() - self._queued_at[key] < provisional_grace:
                    return None
                return self._take(key)
        return None

    def _usage_after(self, update: CaptionUpdate, key: int | None) -> Usage:
        existing = self._pending[key] if key is not None else None
        active = self._active_count + int(not update.final)
        pending = self.pending + 1
        # Canonical caption JSON is ASCII, so character and UTF-8 byte counts agree.
        size = self._pending_bytes + len(update.to_json())
        if existing is not None:
            active -= int(not existing.final)
            pending -= 1
            size -= len(existing.to_json())
        return Usage(active, pending, size)

    def _within_hard_limits(self, usage: Usage) -> bool:
        return (
            usage.active <= self.MAX_PENDING_ACTIVE_SEGMENTS
            and usage.bytes <= self.MAX_PENDING_BYTES
        )

    def _store(self, update: CaptionUpdate, key: int | None, usage: Usage) -> None:
        if key is not None:
            self._coalesced += 1
        else:
            key = self._next_key
            self._next_key += 1
            self._queued_at[key] = self._clock()
        self._pending[key] = update
        if self._coalescing:
            self._by_segment[update.segment] = key
        self._active_count = usage.active
        self._pending_bytes = usage.bytes

    def _take(self, key: int) -> CaptionUpdate:
        update = self._pending.pop(key)
        self._queued_at.pop(key)
        self._by_segment.pop(update.segment, None)
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())
        if not self._pending:
            self._pressure_active = False
        return update

    def _compact(self) -> None:
        """Keep latest values at first queue positions with their original ages."""
        pending: dict[int, CaptionUpdate] = {}
        by_segment: dict[int, int] = {}
        for key, update in self._pending.items():
            retained_key = by_segment.setdefault(update.segment, key)
            pending[retained_key] = update

        queued_at: dict[int, float] = {}
        active = size = 0
        for key, update in pending.items():
            queued_at[key] = self._queued_at[key]
            active += int(not update.final)
            size += len(update.to_json())
        self._coalesced += len(self._pending) - len(pending)
        self._pending = pending
        self._by_segment = by_segment
        self._queued_at = queued_at
        self._active_count = active
        self._pending_bytes = size
