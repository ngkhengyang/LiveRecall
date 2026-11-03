"""Bounded pending delivery state for one client.

Normal delivery preserves every revision in FIFO order. Only detected client
lag or queue pressure enables coalescing to one useful entry per segment.
Capacity pressure must never evict a segment's latest state just because it is
provisional. Dequeuing transfers ownership to the sender; it does not
acknowledge application-level receipt.
"""

from __future__ import annotations

import time
from typing import Callable

from .protocol import CaptionUpdate, History


class OutboundBuffer:
    """Independent limits for pending finals, active segments, and wire bytes.

    A 256-update backlog or byte pressure activates coalescing and batching
    until the queue drains. Client lag can also enable coalescing explicitly.
    Exceeding a hard limit after compaction refuses new buffered work. The
    owning Client can switch finals to shared-log replay instead of closing.
    Latest active state still has to fit within the per-client limits.
    """

    MAX_PENDING_UPDATES = 256  # Soft threshold for semantic catch-up delivery.
    MAX_PENDING_FINALS = 4096
    MAX_PENDING_ACTIVE_SEGMENTS = 4000
    MAX_PENDING_BYTES = 4 * 1024 * 1024
    MAX_BATCH_ITEMS = 64
    MAX_BATCH_BYTES = 256 * 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        # Monotonic queue keys preserve global arrival order, including when
        # several revisions of the same segment are pending simultaneously.
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._next_key = 0
        # This index is used only while coalescing, with one entry per segment.
        self._by_segment: dict[int, int] = {}
        self._overloaded = False
        self._pressure_active = False
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
        # Keep catch-up active through compaction until this backlog drains.
        return self._pressure_active

    @property
    def _coalescing(self) -> bool:
        return self._overloaded or self._pressure_active

    def set_overloaded(self, overloaded: bool) -> None:
        """Apply the client's lag verdict without discarding normal revisions."""
        was_coalescing = self._coalescing
        self._overloaded = overloaded
        if self._coalescing and not was_coalescing:
            self._compact()
        elif not self._coalescing:
            self._by_segment.clear()

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

        update_bytes = len(update.to_json())
        key = self._by_segment.get(update.segment) if self._coalescing else None
        existing = self._pending[key] if key is not None else None
        replaced_bytes = len(existing.to_json()) if existing is not None else 0
        projected_bytes = self._pending_bytes + update_bytes - replaced_bytes
        if not self._pressure_active and (
            self.pending + int(existing is None) >= self.MAX_PENDING_UPDATES
            or projected_bytes > self.MAX_PENDING_BYTES
        ):
            self._pressure_active = True
            self._compact()
            key = self._by_segment.get(update.segment)
            existing = self._pending[key] if key is not None else None

        active_count = self._active_count - int(
            existing is not None and not existing.final
        )
        active_count += int(not update.final)
        pending_count = self.pending + int(existing is None)
        # to_json uses ensure_ascii=True, so string length equals UTF-8 size.
        pending_bytes = self._pending_bytes + update_bytes
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
            key = self._next_key
            self._next_key += 1
            self._queued_at[key] = self._clock()
        self._pending[key] = update
        if self._coalescing:
            self._by_segment[update.segment] = key
        self._active_count = active_count
        self._pending_bytes = pending_bytes

    def begin_replay(self) -> None:
        """Keep latest active state; the shared log now owns pending finals.

        The sender may still own an in-flight message. It must commit that
        message's delivery cursor before fetching the next shared-log batch.
        """
        self._overloaded = True
        self._compact()
        for key, update in list(self._pending.items()):
            if update.final:
                self._take(key)
        self._pressure_active = False
        self._should_close = False

    def discard_segment(self, segment: int) -> None:
        """Remove an active entry once its final is available in the log."""
        key = self._by_segment.get(segment)
        if key is not None:
            self._take(key)

    def _compact(self) -> None:
        """Keep each segment's latest state in its original queue position.

        CaptionStore has already rejected stale revisions and changes to a
        final. Thus each segment's last queued update is its useful state:
        either its durable final or its latest provisional. Preserve the first
        enqueue time so repeated replacement cannot restart the active grace.
        """
        pending: dict[int, CaptionUpdate] = {}
        by_segment: dict[int, int] = {}
        for key, update in self._pending.items():
            retained_key = by_segment.setdefault(update.segment, key)
            pending[retained_key] = update
        self._coalesced += len(self._pending) - len(pending)
        self._pending = pending
        self._by_segment = by_segment
        self._queued_at = {key: self._queued_at[key] for key in pending}
        self._active_count = sum(not update.final for update in pending.values())
        self._pending_bytes = sum(len(update.to_json()) for update in pending.values())

    def take_final_batch(self) -> list[CaptionUpdate]:
        """Transfer unsent finals to a bounded HISTORY batch, oldest first.

        This is pending delivery, not time-filtered recovery: even a final
        queued more than 60 seconds ago stays eligible. A final too large for
        a batch remains pending for ordinary CAPTION_UPDATE delivery. Batching
        is disabled during normal delivery to preserve revision order.
        """
        if not self._coalescing:
            return []
        items = []
        keys = []
        size = len(History().to_json())
        for key, update in sorted(
            self._pending.items(), key=lambda item: item[1].segment
        ):
            if not update.final:
                continue
            item_size = len(update.to_json()) + (2 if items else 0)
            if size + item_size > self.MAX_BATCH_BYTES:
                break
            items.append(update)
            keys.append(key)
            size += item_size
            if len(items) >= self.MAX_BATCH_ITEMS:
                break
        for key in keys:
            self._take(key)
        return items

    def take_active_update(
        self, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Take the oldest ready active segment, allowing finals to coexist."""
        for key, update in self._pending.items():
            if not update.final:
                if self._clock() - self._queued_at[key] < provisional_grace:
                    return None
                return self._take(key)
        return None

    def next_update(
        self, prefer_final: bool = False, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Normal FIFO, or final-first selection while catching up."""
        if not self._pending:
            return None
        oldest = next(iter(self._pending))
        if not (prefer_final and self._coalescing) or self._pending[oldest].final:
            return self._take(oldest)
        for key, update in self._pending.items():
            if update.final:
                return self._take(key)
        return self.take_active_update(provisional_grace)

    def _take(self, key: int) -> CaptionUpdate:
        update = self._pending.pop(key)
        self._queued_at.pop(key)
        self._by_segment.pop(update.segment, None)
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())
        if not self._pending:
            self._pressure_active = False
        return update
