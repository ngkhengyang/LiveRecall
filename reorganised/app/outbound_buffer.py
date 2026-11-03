"""Bounded pending delivery state for one client.

REORGANISED COPY of ``submission_root/app/outbound_buffer.py`` (two-file
layout). Behaviour is identical; the buffer now speaks only in queue terms
and knows nothing about the Client's modes.

Layout
------
``Usage``          pending entries, active entries and bytes, as one value.
``OutboundBuffer``
    GOLDEN PATH    ``offer`` appends; ``next_update`` pops the oldest (FIFO).
    LIMITS         soft limit (256 updates / 4 MiB) turns on pressure; hard
                   limits (4000 active / 4096 finals / 4 MiB) refuse work.
    COALESCING     under lag or pressure, keep the latest entry per segment
                   at that segment's original queue position.
    LATEST-ACTIVE  operations the Client uses while finals come from the
                   shared log instead of this buffer.

Invariants:
- Capacity pressure never evicts a segment's latest state just because it is
  provisional.
- Dequeuing transfers ownership to the sender; it does not acknowledge
  application-level receipt.
"""

from __future__ import annotations

import time
from typing import Callable, NamedTuple

from .protocol import CaptionUpdate


class Usage(NamedTuple):
    """What the buffer holds, or would hold after one more ``offer``."""

    active: int  # provisional entries (segments not yet final)
    pending: int  # all entries
    bytes: int  # wire bytes of all entries

    @property
    def finals(self) -> int:
        """Called by: OutboundBuffer._within_hard_limits.
        Does: Number of final entries (all entries minus active ones).
        """
        return self.pending - self.active


class OutboundBuffer:
    """One client's pending queue: FIFO normally, coalescing under stress."""

    # Limits are class attributes so a test (or a caller) can override them
    # per instance.
    MAX_PENDING_UPDATES = 256  # soft: reaching it turns on pressure
    MAX_PENDING_FINALS = 4096  # hard
    MAX_PENDING_ACTIVE_SEGMENTS = 4000  # hard
    MAX_PENDING_BYTES = 4 * 1024 * 1024  # soft and hard
    # Read by Client to size one catch-up window; not used inside this class.
    MAX_BATCH_ITEMS = 64
    MAX_BATCH_BYTES = 256 * 1024

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        """Called by: Client.__init__ (one buffer per client), tests.
        Does: Starts with an empty queue, no pressure, no coalescing and zero counters.
        """
        self._clock = clock
        # Queue. Monotonic keys preserve arrival order, including several
        # pending revisions of the same segment.
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._next_key = 0
        self._active_count = 0
        self._pending_bytes = 0
        # Limits.
        self._pressure_active = False
        self._should_close = False
        # Coalescing: segment -> key, maintained only while coalescing.
        self._by_segment: dict[int, int] = {}
        self._lag_coalescing = False
        # Counters reported when the client disconnects.
        self._coalesced = 0
        self._rejected = 0

    # ======================================================================
    # GOLDEN PATH
    # ======================================================================

    def offer(self, update: CaptionUpdate) -> None:
        """Called by: Client.enqueue, Client._escalate_to_replay.
        Does: Queues one update: append (golden path) or replace (coalescing), after the
            soft and hard limit checks.

        Queue one update. On the golden path this is just ``_store``.
        """
        if self._should_close:  # a hard limit was already hit
            self._rejected += 1
            return
        key = self._key_to_replace(update)  # None unless coalescing
        key = self._apply_soft_limit(update, key)
        usage = self._usage_after(update, key)
        if not self._within_hard_limits(usage):
            self._should_close = True  # refuse; the Client decides what next
            self._rejected += 1
            return
        self._store(update, key, usage)

    def next_update(self) -> CaptionUpdate | None:
        """Called by: Client._next_delivery, once per message in normal mode.
        Does: Removes and returns the oldest pending update (strict FIFO).

        Pop the oldest pending update (strict FIFO).
        """
        if not self._pending:
            return None
        return self._take(next(iter(self._pending)))

    def has_pending(self) -> bool:
        """Called by: Client._sender_loop, Client._leave_replay_if_caught_up.
        Does: Says whether anything is queued.
        """
        return bool(self._pending)

    @property
    def pending(self) -> int:
        """Called by: OutboundBuffer._usage_after, tests.
        Does: Number of queued entries.
        """
        return len(self._pending)

    @property
    def pending_bytes(self) -> int:
        """Called by: Tests.
        Does: Wire bytes of all queued entries.
        """
        return self._pending_bytes

    def _usage_after(self, update: CaptionUpdate, key: int | None) -> Usage:
        """Called by: offer, _apply_soft_limit.
        Does: Computes the Usage the buffer would have after storing an update.

        Usage if ``update`` were stored, replacing ``key``'s entry if given.
        """
        existing = self._pending[key] if key is not None else None
        active = self._active_count + int(not update.final)
        pending = self.pending + 1
        # to_json uses ensure_ascii=True, so string length equals UTF-8 size.
        size = self._pending_bytes + len(update.to_json())
        if existing is not None:
            active -= int(not existing.final)
            pending -= 1
            size -= len(existing.to_json())
        return Usage(active, pending, size)

    def _store(self, update: CaptionUpdate, key: int | None, usage: Usage) -> None:
        """Called by: offer, once the limit checks pass.
        Does: Replaces the segment's entry in place (coalescing) or appends under a new
            key (golden path).
        """
        if key is not None:
            self._coalesced += 1  # coalescing: replace the entry in place
        else:
            key = self._next_key  # golden path: append under a new key
            self._next_key += 1
            self._queued_at[key] = self._clock()
        self._pending[key] = update
        if self._coalescing:
            self._by_segment[update.segment] = key
        self._active_count = usage.active
        self._pending_bytes = usage.bytes

    def _take(self, key: int) -> CaptionUpdate:
        """Called by: next_update, drop_finals_keep_latest_active, discard_segment,
            take_active_update.
        Does: Removes one entry and keeps every counter correct; clears pressure when
            the queue empties.

        The single removal path; keeps every counter consistent.
        """
        update = self._pending.pop(key)
        self._queued_at.pop(key)
        self._by_segment.pop(update.segment, None)
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())
        if not self._pending:
            self._pressure_active = False  # backlog drained
        return update

    # ======================================================================
    # LIMITS
    # ======================================================================

    @property
    def pressured(self) -> bool:
        """Called by: Client.enqueue, Client._next_delivery.
        Does: True from the soft limit until the backlog fully drains.
        """
        return self._pressure_active

    @property
    def should_close(self) -> bool:
        """Called by: Client.enqueue, Client._escalate_to_replay, Client._sender_loop.
        Does: True once a hard limit refused an update.
        """
        return self._should_close

    def _apply_soft_limit(self, update: CaptionUpdate, key: int | None) -> int | None:
        """Called by: offer, while pressure is off.
        Does: At 256 entries or over 4 MiB, turns pressure on and compacts; returns the
            key to use.

        Turn on pressure (and coalesce) at the soft limit; return the key to use.
        """
        if self._pressure_active:
            return key
        usage = self._usage_after(update, key)
        if (
            usage.pending >= self.MAX_PENDING_UPDATES
            or usage.bytes > self.MAX_PENDING_BYTES
        ):
            self._pressure_active = True
            self._compact()
            return self._by_segment.get(update.segment)
        return key

    def _within_hard_limits(self, usage: Usage) -> bool:
        """Called by: offer.
        Does: Checks 4000 active, 4096 finals and 4 MiB.
        """
        return (
            usage.active <= self.MAX_PENDING_ACTIVE_SEGMENTS
            and usage.finals <= self.MAX_PENDING_FINALS
            and usage.bytes <= self.MAX_PENDING_BYTES
        )

    # ======================================================================
    # COALESCING
    # ======================================================================

    @property
    def _coalescing(self) -> bool:
        """Called by: _store, set_coalescing, _key_to_replace.
        Does: True under client lag or pressure.
        """
        return self._lag_coalescing or self._pressure_active

    def set_coalescing(self, on: bool) -> None:
        """Called by: Client.enqueue (before every normal-mode offer),
            Client._leave_replay_if_caught_up.
        Does: Turns lag coalescing on (compacting once) or off (dropping the segment
            index).

        Coalesce because the client is lagging (pressure coalesces by itself).

        Called before every normal-mode offer. On the golden path it is
        ``False`` and changes nothing.
        """
        was_coalescing = self._coalescing
        self._lag_coalescing = on
        if self._coalescing and not was_coalescing:
            self._compact()
        elif not self._coalescing:
            self._by_segment.clear()

    def _key_to_replace(self, update: CaptionUpdate) -> int | None:
        """Called by: offer.
        Does: Finds this segment's pending entry, if coalescing.

        This segment's pending entry, if coalescing.
        """
        return self._by_segment.get(update.segment) if self._coalescing else None

    def _compact(self) -> None:
        """Called by: set_coalescing, _apply_soft_limit, drop_finals_keep_latest_active.
        Does: Keeps each segment's latest entry at its original position and recounts.

        Keep each segment's latest state in its original queue position.

        CaptionStore has already rejected stale revisions and changes to a
        final, so each segment's last queued update is its useful state:
        its durable final or its latest provisional. The first enqueue time
        is kept so repeated replacement cannot restart the provisional grace.
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

    # ======================================================================
    # LATEST-ACTIVE (finals are delivered from the shared log instead)
    # ======================================================================

    def drop_finals_keep_latest_active(self) -> None:
        """Called by: Client._enter_replay (and begin_replay in tests).
        Does: Compacts, removes every pending final and clears the limit flags.

        Coalesce, drop every pending final, and clear the limit flags.

        The sender may still own an in-flight message. It must commit that
        message's delivery cursor before fetching the next shared-log batch.
        """
        self._lag_coalescing = True
        self._compact()
        for key, update in list(self._pending.items()):
            if update.final:
                self._take(key)
        self._pressure_active = False
        self._should_close = False

    def discard_segment(self, segment: int) -> None:
        """Called by: Client.enqueue (in REPLAY), Client._escalate_to_replay.
        Does: Removes a segment's active entry once its final is in the log.

        Remove a segment's active entry once its final is in the log.
        """
        key = self._by_segment.get(segment)
        if key is not None:
            self._take(key)

    def take_active_update(
        self, provisional_grace: float = 0.0
    ) -> CaptionUpdate | None:
        """Called by: ReplayCursor.next_replay.
        Does: Removes and returns the oldest provisional, unless it is younger than the
            grace.

        The oldest provisional, unless it is younger than the grace.
        """
        for key, update in self._pending.items():
            if not update.final:
                if self._clock() - self._queued_at[key] < provisional_grace:
                    return None
                return self._take(key)
        return None

    # ======================================================================
    # Counters reported on disconnect
    # ======================================================================

    @property
    def coalesced(self) -> int:
        """Called by: Client.run (printed on disconnect).
        Does: Number of updates replaced or compacted away.
        """
        return self._coalesced

    @property
    def rejected(self) -> int:
        """Called by: Client.run (printed on disconnect).
        Does: Number of updates refused after a hard limit.

        Updates refused after a hard limit.
        """
        return self._rejected

    # ======================================================================
    # Test compatibility: the old name used by test/test_delivery.py.
    # Remove once the tests call drop_finals_keep_latest_active().
    # ======================================================================

    begin_replay = drop_finals_keep_latest_active
