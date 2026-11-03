"""One receiver's bounded FIFO queue and coalesced latest caption state.

The owner (``Client``) decides when to replay finals from shared history. This
buffer only handles admission, ordering, replacement, and removal of pending
captions. Dequeuing transfers ownership to the sender; it is not a delivery
acknowledgement.

GOLDEN PATH (normal mode, no pressure)
======================================

    Server.broadcast(update)
      -> Client.enqueue(update)
           -> OutboundBuffer.offer(update)          # admit to FIFO
                -> _usage_after()                   # project new totals
                -> _within_hard_limits()            # still under caps?
                -> _store()                         # append with new key
    Client._sender_loop()
      -> Client._next_delivery()
           -> OutboundBuffer.has_pending()
           -> OutboundBuffer.next_update()          # pop oldest
                -> _take()                          # remove + fix counters

UNDER PRESSURE / REPLAY
=======================

    offer() hits the soft limit  -> _pressure_active = True -> _compact()
    Client._enter_replay()       -> drop_finals_keep_latest_active()
    Client.enqueue(final)        -> discard_segment()
    ReplayCursor.next_replay()   -> take_active_update()
    Client._leave_replay_...()   -> set_coalescing(False)

FILE LAYOUT
===========

    1. Usage                     - projected-usage record
    2. OutboundBuffer
       2.1 Limits and state
       2.2 Status properties     - read by Client
       2.3 Golden path           - offer / next_update / has_pending
       2.4 Replay support        - coalescing, dropping finals, drafts
       2.5 Internal bookkeeping  - usage maths, store, take, compact
"""

from __future__ import annotations

import time
from typing import Callable, NamedTuple

from .protocol import CaptionUpdate


# =============================================================================
# 1. Usage
# =============================================================================


class Usage(NamedTuple):
    """Projected queue usage after admitting an update.

    Produced by ``OutboundBuffer._usage_after`` and consumed by
    ``offer``, ``_within_hard_limits`` and ``_store``.
    """

    active: int
    pending: int
    bytes: int


# =============================================================================
# 2. OutboundBuffer
# =============================================================================


class OutboundBuffer:
    """Preserve FIFO revisions normally and latest segment state under pressure.

    The soft entry limit asks the owner to switch finals to shared-log replay.
    Active entries and serialized bytes remain bounded after compaction.

    Created by ``Client.__init__`` (one buffer per connected receiver).
    """

    # -------------------------------------------------------------------------
    # 2.1 Limits and state
    # -------------------------------------------------------------------------

    MAX_PENDING_UPDATES = 256
    MAX_PENDING_ACTIVE_SEGMENTS = 4000
    MAX_PENDING_BYTES = 4 * 1024 * 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        """Set up an empty queue.

        What: initialises the FIFO storage, coalescing index, counters, and
        the pressure / close flags.
        Called by: ``Client.__init__``.
        """
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

    # -------------------------------------------------------------------------
    # 2.2 Status properties (read by Client)
    # -------------------------------------------------------------------------

    @property
    def pending(self) -> int:
        """Number of queued updates.

        What: returns how many entries are currently in the queue.
        Called by: ``_usage_after`` (to project the post-admission count).
        """
        return len(self._pending)

    @property
    def pressured(self) -> bool:
        """Whether the soft limit was reached and the backlog has not drained.

        What: true once ``offer`` crossed the soft entry/byte limit; cleared by
        ``_take`` when the queue empties or by ``drop_finals_keep_latest_active``.
        Called by: ``Client.enqueue`` (decides whether to escalate to replay).
        """
        return self._pressure_active

    @property
    def should_close(self) -> bool:
        """Whether a hard limit refused an update.

        What: true once ``offer`` rejected an update for exceeding a hard cap.
        Called by: ``offer``, ``Client.enqueue``, ``Client._escalate_to_replay``,
        ``Client._sender_loop``.
        """
        return self._should_close

    @property
    def coalesced(self) -> int:
        """Count of updates replaced in place by a newer revision.

        What: diagnostic counter incremented by ``_store`` and ``_compact``.
        Called by: ``Client.run`` (disconnect log line).
        """
        return self._coalesced

    @property
    def rejected(self) -> int:
        """Count of updates refused by ``offer``.

        What: diagnostic counter incremented when a hard limit is hit.
        Called by: ``Client.run`` (disconnect log line).
        """
        return self._rejected

    @property
    def _coalescing(self) -> bool:
        """Whether new updates replace the queued entry for the same segment.

        What: true if the owner requested coalescing (replay mode) or the
        buffer is under soft-limit pressure.
        Called by: ``offer``, ``set_coalescing``, ``_store``.
        """
        return self._coalescing_requested or self._pressure_active

    # -------------------------------------------------------------------------
    # 2.3 Golden path: admit -> dequeue
    # -------------------------------------------------------------------------

    def offer(self, update: CaptionUpdate) -> None:
        """Admit one update, compacting before any hard-limit refusal.

        What: appends ``update`` to the FIFO (or replaces the segment's entry
        when coalescing). Crossing the soft limit turns on pressure and
        compacts; crossing a hard limit sets ``should_close`` and rejects.
        Called by: ``Client.enqueue``, ``Client._escalate_to_replay``.
        """
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
        """Remove the oldest pending update.

        What: pops and returns the head of the FIFO, or ``None`` if empty.
        Called by: ``Client._next_delivery`` (normal-mode delivery).
        """
        if not self._pending:
            return None
        return self._take(next(iter(self._pending)))

    def has_pending(self) -> bool:
        """Whether anything is queued.

        What: true if the FIFO is non-empty.
        Called by: ``Client._sender_loop`` (inner loop condition).
        """
        return bool(self._pending)

    # -------------------------------------------------------------------------
    # 2.4 Replay support (driven by Client / ReplayCursor)
    # -------------------------------------------------------------------------

    def set_coalescing(self, on: bool) -> None:
        """Apply the owner's policy while preserving independent pressure state.

        What: turns owner-requested coalescing on/off. Turning it on compacts
        the queue; turning it fully off clears the segment index.
        Called by: ``drop_finals_keep_latest_active`` (on),
        ``Client._leave_replay_if_caught_up`` (off).
        """
        was_coalescing = self._coalescing
        self._coalescing_requested = on
        if self._coalescing and not was_coalescing:
            self._compact()
        elif not self._coalescing:
            self._by_segment.clear()

    def drop_finals_keep_latest_active(self) -> None:
        """Transfer queued finals to shared replay and retain latest drafts.

        What: enables coalescing, removes every queued final (they will be
        replayed from the shared log), and resets pressure / close flags.
        Called by: ``Client._enter_replay``.
        """
        self.set_coalescing(True)
        for key, update in list(self._pending.items()):
            if update.final:
                self._take(key)
        self._pressure_active = False
        self._should_close = False

    def discard_segment(self, segment: int) -> None:
        """Remove a coalesced draft once its final is available in shared history.

        What: drops the queued entry for ``segment`` if the index has one.
        Called by: ``Client.enqueue`` (final arrives in replay mode),
        ``Client._escalate_to_replay``.
        """
        key = self._by_segment.get(segment)
        if key is not None:
            self._take(key)

    def take_active_update(
        self, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Remove the oldest draft once its original enqueue time passes grace.

        What: pops the oldest non-final entry if it has been queued for at
        least ``provisional_grace`` seconds; otherwise returns ``None``.
        Called by: ``ReplayCursor.next_replay``.
        """
        for key, update in self._pending.items():
            if not update.final:
                if self._clock() - self._queued_at[key] < provisional_grace:
                    return None
                return self._take(key)
        return None

    # -------------------------------------------------------------------------
    # 2.5 Internal bookkeeping
    # -------------------------------------------------------------------------

    def _usage_after(self, update: CaptionUpdate, key: int | None) -> Usage:
        """Project queue usage if ``update`` were admitted.

        What: computes active-draft count, entry count, and serialized bytes
        after adding ``update`` (or replacing the entry at ``key``).
        Called by: ``offer``.
        """
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
        """Check projected usage against the hard caps.

        What: true if active drafts and bytes stay within their maximums.
        Called by: ``offer``.
        """
        return (
            usage.active <= self.MAX_PENDING_ACTIVE_SEGMENTS
            and usage.bytes <= self.MAX_PENDING_BYTES
        )

    def _store(self, update: CaptionUpdate, key: int | None, usage: Usage) -> None:
        """Write an admitted update into the queue.

        What: replaces the entry at ``key`` (coalesce) or appends under a new
        key, records enqueue time, updates the segment index and counters.
        Called by: ``offer``.
        """
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
        """Remove one entry and keep counters consistent.

        What: pops the entry at ``key``, updates indexes and totals, and
        clears pressure once the queue is empty.
        Called by: ``next_update``, ``drop_finals_keep_latest_active``,
        ``discard_segment``, ``take_active_update``.
        """
        update = self._pending.pop(key)
        self._queued_at.pop(key)
        self._by_segment.pop(update.segment, None)
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())
        if not self._pending:
            self._pressure_active = False
        return update

    def _compact(self) -> None:
        """Keep latest values at first queue positions with their original ages.

        What: collapses the queue to one entry per segment (latest revision
        kept at the segment's earliest position) and recomputes totals.
        Called by: ``offer`` (on entering pressure), ``set_coalescing`` (on).
        """
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
