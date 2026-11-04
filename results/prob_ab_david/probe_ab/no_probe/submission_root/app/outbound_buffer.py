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
#
# A small record of what the queue totals would be if an update were added:
# number of drafts, number of entries, and serialized bytes. offer() computes
# this first so it can check limits before changing anything.
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
#
# One viewer's mailbox. The server puts captions in (via Client.enqueue) and
# the sender takes them out (via Client._next_delivery). It handles three
# situations:
#   normal    plain queue; every revision kept in order.
#   pressure  queue too big: keep only the latest revision of each segment
#             (coalescing) and tell the Client to switch to replay.
#   replay    finals live in the shared history; this buffer keeps only the
#             latest draft for each segment.
# =============================================================================


class OutboundBuffer:
    """Preserve FIFO revisions normally and latest segment state under pressure.

    The soft entry limit asks the owner to switch finals to shared-log replay.
    Active entries and serialized bytes remain bounded after compaction.

    Created by ``Client.__init__`` (one buffer per connected receiver).
    """

    # -------------------------------------------------------------------------
    # 2.1 Limits and state
    #
    # Limits:
    #   MAX_PENDING_UPDATES          soft: start coalescing and ask for replay.
    #   MAX_PENDING_BYTES            soft and hard: crossing it starts
    #                                coalescing; still over it -> refuse.
    #   MAX_PENDING_ACTIVE_SEGMENTS  hard: too many drafts -> refuse.
    # Refusing sets should_close, and the Client then closes the connection.
    # State:
    #   _pending     the queue (key -> update), in insertion order.
    #   _queued_at   when each key was first queued (for the draft grace period).
    #   _by_segment  segment -> key, used only while coalescing.
    #   _pressure_active / _coalescing_requested  the two reasons to coalesce.
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
        # --- Clock (replaceable so timing can be faked) -----------------------
        self._clock = clock

        # --- The queue itself ---------------------------------------------
        # Distinct keys retain every revision in normal FIFO mode.
        self._pending: dict[int, CaptionUpdate] = {}
        self._queued_at: dict[int, float] = {}
        self._next_key = 0

        # --- Coalescing index and the two reasons to coalesce -------------
        # Coalescing uses one queue entry per segment.
        self._by_segment: dict[int, int] = {}
        self._coalescing_requested = False
        self._pressure_active = False

        # --- Running totals (kept in sync by _store / _take / _compact) ---
        self._active_count = 0
        self._pending_bytes = 0

        # --- Hard-limit flag and diagnostic counters ------------------------
        self._should_close = False
        self._coalesced = 0
        self._rejected = 0

    # -------------------------------------------------------------------------
    # 2.2 Status properties (read by Client)
    #
    # Read-only views the Client uses to make decisions:
    #   pressured     -> switch to replay.
    #   should_close  -> give up on the viewer.
    #   coalesced / rejected -> counters printed when the viewer disconnects.
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
    #
    # The normal flow:
    #   offer()        add to the back (or replace the segment's entry while
    #                  coalescing), checking the limits first.
    #   next_update()  take from the front.
    #   has_pending()  is there anything to send?
    # -------------------------------------------------------------------------

    def offer(self, update: CaptionUpdate) -> None:
        """Admit one update, compacting before any hard-limit refusal.

        What: appends ``update`` to the FIFO (or replaces the segment's entry
        when coalescing). Crossing the soft limit turns on pressure and
        compacts; crossing a hard limit sets ``should_close`` and rejects.
        Called by: ``Client.enqueue``, ``Client._escalate_to_replay``.
        """
        # --- 1. Already over a hard limit: refuse everything ----------------
        if self._should_close:
            self._rejected += 1
            return

        # --- 2. Find the slot: an existing entry for this segment (when
        #        coalescing) or None for a new entry; project the totals ------
        key = self._by_segment.get(update.segment) if self._coalescing else None
        usage = self._usage_after(update, key)

        # --- 3. Soft limit crossed for the first time: turn on pressure,
        #        collapse the queue, and recompute the slot and totals --------
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

        # --- 4. Still over a hard limit after compaction: refuse and flag ---
        if not self._within_hard_limits(usage):
            self._should_close = True
            self._rejected += 1
            return

        # --- 5. Fits: write it into the queue -----------------------------
        self._store(update, key, usage)

    def next_update(self) -> CaptionUpdate | None:
        """Remove the oldest pending update.

        What: pops and returns the head of the FIFO, or ``None`` if empty.
        Called by: ``Client._next_delivery`` (normal-mode delivery).
        """
        # Empty queue: nothing to send.
        if not self._pending:
            return None
        # Dicts keep insertion order, so the first key is the oldest entry.
        return self._take(next(iter(self._pending)))

    def has_pending(self) -> bool:
        """Whether anything is queued.

        What: true if the FIFO is non-empty.
        Called by: ``Client._sender_loop`` (inner loop condition).
        """
        return bool(self._pending)

    # -------------------------------------------------------------------------
    # 2.4 Replay support (driven by Client / ReplayCursor)
    #
    # Used only around replay mode:
    #   drop_finals_keep_latest_active()  entering replay: remove finals
    #                                     (the shared history has them).
    #   discard_segment()                 a segment's final arrived: its draft
    #                                     is no longer needed.
    #   take_active_update()              ReplayCursor slips in a draft once it
    #                                     has waited past the grace period.
    #   set_coalescing()                  turn coalescing on/off (off when
    #                                     leaving replay).
    # -------------------------------------------------------------------------

    def set_coalescing(self, on: bool) -> None:
        """Apply the owner's policy while preserving independent pressure state.

        What: turns owner-requested coalescing on/off. Turning it on compacts
        the queue; turning it fully off clears the segment index.
        Called by: ``drop_finals_keep_latest_active`` (on),
        ``Client._leave_replay_if_caught_up`` (off).
        """
        # Remember the old state, then record the owner's request.
        was_coalescing = self._coalescing
        self._coalescing_requested = on
        # Just switched on: collapse to one entry per segment now.
        if self._coalescing and not was_coalescing:
            self._compact()
        # Fully off (no request and no pressure): forget the segment index so
        # new revisions are queued separately again.
        elif not self._coalescing:
            self._by_segment.clear()

    def drop_finals_keep_latest_active(self) -> None:
        """Transfer queued finals to shared replay and retain latest drafts.

        What: enables coalescing, removes every queued final (they will be
        replayed from the shared log), and resets pressure / close flags.
        Called by: ``Client._enter_replay``.
        """
        # From now on keep only the latest draft for each segment.
        self.set_coalescing(True)
        # Remove every final: the shared history will replay them.
        for key, update in list(self._pending.items()):
            if update.final:
                self._take(key)
        # Fresh start: the backlog that caused pressure or refusal is gone.
        self._pressure_active = False
        self._should_close = False

    def discard_segment(self, segment: int) -> None:
        """Remove a coalesced draft once its final is available in shared history.

        What: drops the queued entry for ``segment`` if the index has one.
        Called by: ``Client.enqueue`` (final arrives in replay mode),
        ``Client._escalate_to_replay``.
        """
        # Look up the segment's entry (only indexed while coalescing).
        key = self._by_segment.get(segment)
        # Remove it if present; otherwise there is nothing to discard.
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
        # Walk the queue oldest-first looking for the first draft.
        for key, update in self._pending.items():
            if not update.final:
                # Too fresh: it may still be replaced soon, so wait.
                if self._clock() - self._queued_at[key] < provisional_grace:
                    return None
                # Old enough: hand it to the replay cursor.
                return self._take(key)
        # No drafts queued.
        return None

    # -------------------------------------------------------------------------
    # 2.5 Internal bookkeeping
    #
    # Private helpers that keep the queue and its counters consistent. You
    # can skip these on a first read.
    #   _usage_after / _within_hard_limits  work out and check the totals.
    #   _store / _take                      add or remove one entry and update
    #                                       the counters.
    #   _compact                            collapse to one entry per segment
    #                                       (latest revision, earliest position).
    # -------------------------------------------------------------------------

    def _usage_after(self, update: CaptionUpdate, key: int | None) -> Usage:
        """Project queue usage if ``update`` were admitted.

        What: computes active-draft count, entry count, and serialized bytes
        after adding ``update`` (or replacing the entry at ``key``).
        Called by: ``offer``.
        """
        # The entry this update would replace, if coalescing found one.
        existing = self._pending[key] if key is not None else None

        # Start from current totals plus the new update.
        active = self._active_count + int(not update.final)
        pending = self.pending + 1
        # Canonical caption JSON is ASCII, so character and UTF-8 byte counts agree.
        size = self._pending_bytes + len(update.to_json())

        # Replacing an entry: subtract what it contributed.
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
        # Replace in place (keeps queue position and original enqueue time),
        # or allocate a new key at the back of the queue.
        if key is not None:
            self._coalesced += 1
        else:
            key = self._next_key
            self._next_key += 1
            self._queued_at[key] = self._clock()

        # Write the update and index it by segment while coalescing.
        self._pending[key] = update
        if self._coalescing:
            self._by_segment[update.segment] = key

        # Commit the totals already worked out by _usage_after.
        self._active_count = usage.active
        self._pending_bytes = usage.bytes

    def _take(self, key: int) -> CaptionUpdate:
        """Remove one entry and keep counters consistent.

        What: pops the entry at ``key``, updates indexes and totals, and
        clears pressure once the queue is empty.
        Called by: ``next_update``, ``drop_finals_keep_latest_active``,
        ``discard_segment``, ``take_active_update``.
        """
        # Remove the entry from the queue and both indexes.
        update = self._pending.pop(key)
        self._queued_at.pop(key)
        self._by_segment.pop(update.segment, None)

        # Subtract its contribution from the running totals.
        self._active_count -= int(not update.final)
        self._pending_bytes -= len(update.to_json())

        # Backlog fully drained: pressure is over.
        if not self._pending:
            self._pressure_active = False
        return update

    def _compact(self) -> None:
        """Keep latest values at first queue positions with their original ages.

        What: collapses the queue to one entry per segment (latest revision
        kept at the segment's earliest position) and recomputes totals.
        Called by: ``offer`` (on entering pressure), ``set_coalescing`` (on).
        """
        # --- 1. Collapse: each segment keeps its FIRST key (queue position)
        #        but the LAST update seen (latest revision) ------------------
        pending: dict[int, CaptionUpdate] = {}
        by_segment: dict[int, int] = {}
        for key, update in self._pending.items():
            retained_key = by_segment.setdefault(update.segment, key)
            pending[retained_key] = update

        # --- 2. Rebuild enqueue times and totals for the surviving entries --
        queued_at: dict[int, float] = {}
        active = size = 0
        for key, update in pending.items():
            queued_at[key] = self._queued_at[key]
            active += int(not update.final)
            size += len(update.to_json())

        # --- 3. Count what was merged away and swap in the new state -------
        self._coalesced += len(self._pending) - len(pending)
        self._pending = pending
        self._by_segment = by_segment
        self._queued_at = queued_at
        self._active_count = active
        self._pending_bytes = size
