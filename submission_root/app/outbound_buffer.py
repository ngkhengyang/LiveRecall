"""Bounded pending delivery state for one client.

There is one entry per segment, so replacing an entry coalesces a superseded
revision. Every remaining entry is useful state: capacity pressure must never
evict it just because it is provisional. Dequeuing transfers ownership to the
sender; it does not acknowledge application-level receipt.
"""

from __future__ import annotations

import time
from typing import Callable

from .protocol import CaptionUpdate, History


class OutboundBuffer:
    """Independent limits for pending finals, active segments, and wire bytes.

    The 256-entry threshold activates batching, not eviction. Exceeding a hard
    limit refuses new work and requests an explicit connection failure. This
    is the sustained-overload limit, not a claim that reconnect can recover
    arbitrary old finals outside the shared history window.
    """

    MAX_PENDING_SEGMENTS = 256  # Soft threshold for catch-up delivery.
    MAX_PENDING_FINALS = 4096
    MAX_PENDING_ACTIVE_SEGMENTS = 4000
    MAX_PENDING_BYTES = 4 * 1024 * 1024
    MAX_BATCH_ITEMS = 64
    MAX_BATCH_BYTES = 256 * 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._active_count = 0
        self._pending_bytes = 0
        self._should_close = False
        self._coalesced = 0
        self._rejected = 0

    @property
    def should_close(self) -> bool:
        return self._should_close

    @property
    def pending(self) -> int:
        return len(self._pending)

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    @property
    def pressured(self) -> bool:
        return self.pending >= self.MAX_PENDING_SEGMENTS

    @property
    def coalesced(self) -> int:
        return self._coalesced

    @property
    def rejected(self) -> int:
        """Updates refused after a hard limit, reported when closing."""
        return self._rejected

    def has_pending(self) -> bool:
        return bool(self._pending)

    def offer(self, update: CaptionUpdate) -> None:
        if self._should_close:
            self._rejected += 1
            return

        existing = self._pending.get(update.segment)
        active_count = self._active_count - int(
            existing is not None and not existing.final
        )
        active_count += int(not update.final)
        pending_count = self.pending + int(existing is None)
        # to_json uses ensure_ascii=True, so string length equals UTF-8 size.
        pending_bytes = self._pending_bytes + len(update.to_json())
        if existing is not None:
            pending_bytes -= len(existing.to_json())
        if (
            active_count > self.MAX_PENDING_ACTIVE_SEGMENTS
            or pending_count - active_count > self.MAX_PENDING_FINALS
            or pending_bytes > self.MAX_PENDING_BYTES
        ):
            self._should_close = True
            self._rejected += 1
            return

        if existing is not None:
            self._coalesced += 1
        else:
            self._queued_at[update.segment] = self._clock()
        self._pending[update.segment] = update
        self._active_count = active_count
        self._pending_bytes = pending_bytes

    def take_final_batch(self) -> list[CaptionUpdate]:
        """Transfer unsent finals to a bounded HISTORY batch, oldest first.

        This is pending delivery, not time-filtered recovery: even a final
        queued more than 60 seconds ago stays eligible. A final too large for
        a batch remains pending for ordinary CAPTION_UPDATE delivery.
        """
        items = []
        size = len(History().to_json())
        for segment in sorted(self._pending):
            update = self._pending[segment]
            if not update.final:
                continue
            item_size = len(update.to_json()) + (2 if items else 0)
            if size + item_size > self.MAX_BATCH_BYTES:
                break
            items.append(update)
            size += item_size
            if len(items) >= self.MAX_BATCH_ITEMS:
                break
        for update in items:
            self._take(update.segment)
        return items

    def take_active_update(
        self, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Take the oldest ready active segment, allowing finals to coexist."""
        for segment, update in self._pending.items():
            if not update.final:
                if self._clock() - self._queued_at[segment] < provisional_grace:
                    return None
                return self._take(segment)
        return None

    def next_update(
        self, prefer_final: bool = False, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Normal FIFO, or final-first selection while catching up."""
        if not self._pending:
            return None
        oldest = next(iter(self._pending))
        if not prefer_final or self._pending[oldest].final:
            return self._take(oldest)
        for segment, update in self._pending.items():
            if update.final:
                return self._take(segment)
        return self.take_active_update(provisional_grace)

    def _take(self, segment: int) -> CaptionUpdate:
        update = self._pending.pop(segment)
        self._queued_at.pop(segment)
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())
        return update
