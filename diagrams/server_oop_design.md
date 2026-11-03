# OOP design: Server / Source / Client for solution_server.py

**Status: current architecture with a separate historical-results section.** The
component and API descriptions match `submission_root/app/`'s connected shared-log
replay design. Earlier reconnect-policy measurements are retained below as historical
notes; they are not verification or benchmark results for the current implementation.

## Context

The starter `submission_root/solution_server.py` was the intentionally-naive baseline: a
flat set of free functions (`handler`, `handle_source`, `handle_client`,
`broadcast_naively`), raw `dict`/`json.dumps` traffic, and a single global `CLIENTS` set.
It had no revision state, no history, no per-client buffering — it awaited every client
send inline, so one slow reader stalled everyone, and it answered every `CLIENT_HELLO`
with an empty `HISTORY`.

What replaced it is a fully object-oriented design: three actor classes — `Server`
(coordinator), `Source` (wraps the one caption-source connection, delegates to Server),
`Client` (wraps one browser/load-client connection) — plus the wire protocol itself
modeled as typed message classes instead of raw dicts, and the CLI entry point folded
into `Server` rather than left as free module-level functions. No global mutable state
and no bare `dict` passed between layers anywhere.

`solution_server.py` is now a ~20-line launcher that exists only to satisfy the required
`python solution_server.py --port 8765` command. It inserts its own directory on
`sys.path` before importing `app.server`, because the public harness starts it from two
different working directories: `test/run_demo.py` uses the repository root,
`test/submission_check.py` uses `submission_root/`.

Source and Client deliberately do **not** share a base class (decided explicitly — see
"Design decisions" below): they're independent, each a full `run()` of their own, trading
a little duplicated read-loop boilerplate for zero risk of one's logic leaking into the
other through a shared parent.

The two hardest pieces of logic — revision/dedup/history validation, and per-client
buffering/coalescing — are pulled out into their own **pure, non-actor logic classes**
(`CaptionStore`, `OutboundBuffer`) with no networking or asyncio dependency at all. This
makes them unit-testable in complete isolation and lets two people own a "state-side" and
a "backpressure-side" track each without touching the same files or needing a live
end-to-end system to test their own logic.

## Design decisions already made

- **No inheritance between Source and Client.** Considered a shared `Connection` base
  class (template-method pattern); rejected in favor of full independence.
- **Composition over inheritance everywhere else too** — e.g. `CaptionStore`'s internal
  `SegmentState` wraps a `CaptionUpdate` rather than subclassing it.
- **Single-session only.** The mandatory tests use exactly one source and one session
  (`"L1"`) at a time; don't key `CaptionStore` by session id. (Cheap to generalize later
  — not worth building now.)
- **Strict parsing for captions, lenient parsing for client hellos.** A malformed
  `CAPTION_UPDATE` is rejected outright, because guessing at a segment or revision number
  would corrupt the revision state everything else depends on. A `CLIENT_HELLO` with a
  missing or odd optional field is repaired to defaults instead, because closing a
  student's browser connection over a cosmetic field would be a worse failure than
  assuming `history_seconds=30`.

## Class / module map

```
protocol.py  (typed message classes — no behavior beyond parse/serialize)
    SourceHello, ClientHello, CaptionUpdate, History, SourceDone

Server  --creates-->  Source   (one, for the single caption-source connection)
Server  --creates-->  Client   (one per connected browser / load-test client)
Server  --owns------>  CaptionStore   (one instance; pure logic, no networking)
Client  --owns------>  OutboundBuffer (one instance per client; pure logic, no networking)

Source  --delegates to-->  Server.ingest_caption_update(update: CaptionUpdate)
Server  --delegates to-->  CaptionStore.validate_and_apply(update) / .history_snapshot(...)
Server  --pushes into-->   Client.enqueue(update: CaptionUpdate)   (never awaits a socket)
Client  --delegates to-->  OutboundBuffer.offer(update) / .next_update(...)
Client  --reads via Server--> CaptionStore.read_final_batch(...)  (shared replay log)
```

`CaptionUpdate` is the one message type that flows through almost the entire system
(Source parses it in, Server/CaptionStore validate it, Client/OutboundBuffer buffer and
send it back out) — it's the closest thing this design has to a shared vocabulary, and
it's a plain data class, not a behavior-bearing one.

## `protocol.py` — typed wire messages

Every message on the wire is a small frozen dataclass instead of a raw dict, each knowing
how to validate itself out of JSON and serialize itself back:

```python
@dataclass(frozen=True)
class CaptionUpdate:
    session: str
    segment: int
    revision: int
    source_ts: float
    final: bool
    text: str
```

(plus `SourceHello`, `ClientHello`, `History`, `SourceDone`.)

Each of `SourceHello`, `ClientHello`, `CaptionUpdate`, `SourceDone` exposes:
- `@classmethod def from_dict(cls, raw: dict) -> "Self | None"` — field presence/type
  validation; returns `None` on anything malformed/missing rather than raising. Note the
  `_as_int` helper rejects `bool` explicitly: `isinstance(True, int)` is `True` in Python,
  so `{"segment": true}` would otherwise be accepted as segment 1.
- Unknown extra JSON fields are ignored, as §4.1 of the spec requires.

Every message type (including `History`, which is only ever server-constructed, never
parsed) exposes `to_dict()` (adds the literal `"type"` tag) and `to_json()` — so no call
site ever hand-assembles a dict to send.

**Added during implementation: `CaptionUpdate.to_json()` memoizes its string.** One
update object is fanned out to every connected client, so without this the server runs
`json.dumps` N times on identical data. The cache is written with `object.__setattr__` on
the frozen instance and lives outside the dataclass fields, so it never affects equality
or `repr`. This is a clean, cheap item for the report's required before/after comparison.

**Message dispatch**: `Server._accept` inspects `raw.get("type")` before choosing an
opening-hello class; `Source.run` also dispatches producer message tags. After parsing,
the state and delivery layers exchange typed objects.

## `CaptionStore` (pure logic — owned by `Server`)

No `ws`, no `asyncio`, no `await` anywhere in this class. Constructed once by `Server`.

**Attributes**
- `self._segments: dict[int, SegmentState]` — current winning state per segment.
- `self._final_history: dict[int, SegmentState]` — insertion-ordered, keyed by a
  contiguous accepted-final sequence; shared by context recovery and connected replay.
- `self._final_tail` — last accepted final sequence, even if that final has expired.
- `self._evicted_final_sequence` — last expired final sequence; detects missing replay
  prefixes even when the log is empty.
- `self._retired_before: int` — source-segment retirement watermark; separate from the
  replay watermark because unfinished-segment eviction also advances it.
- `self._active_segments: set[int]` — unfinished IDs only; finals are not subject
  to this count backstop.
- Class constants: `HISTORY_HORIZON_SECONDS = 90.0`, `MAX_ACTIVE_SEGMENTS = 4000`.

**`SegmentState`** — *composed* from a `CaptionUpdate` rather than subclassing it:
`@dataclass(frozen=True) class SegmentState: update: CaptionUpdate; received_at: float;
final_sequence: int = 0`,
with `segment`/`revision`/`final` convenience properties. Never leaves `CaptionStore`.

**Public methods**
- `validate_and_apply(update) -> CaptionUpdate | None`:
  1. reject if `update.segment < self._retired_before`;
  2. reject if the segment is already `.final` — final is durable, nothing overwrites it
     regardless of the incoming revision number;
  3. reject if `update.revision <= existing.revision` (covers duplicates and stale/
     out-of-order in one rule);
  4. otherwise store, and if final allocate the next sequence, append to
     `_final_history`, and trim. Returns the accepted update.
- `compute_history(last_segment, last_revision, history_seconds=60) -> list[CaptionUpdate]`
  — finalized entries after the cursor inside the requested window, oldest-first.
- `history_snapshot(...) -> (items, tail)` — requested context and its final-log
  boundary captured synchronously. The captured tail must not be replaced by a tail
  read after awaiting the HISTORY send.
- `final_tail` — current accepted-final sequence watermark.
- `final_sequence(update) -> int` — resolves a retained matching final before its
  send; absence or mismatch raises `ReplayExpired`.
- `read_final_batch(after_sequence, max_items, max_bytes) -> (items, next_sequence)`
  — bounded continuous delivery after the cursor, independent of the requested
  context window. It accounts for HISTORY framing/separators and returns a single
  oversized item alone for ordinary CAPTION_UPDATE fallback. An expired prefix raises
  `ReplayExpired` rather than silently skipping ahead. Indexed sequence reads avoid
  copying a complete per-client history snapshot for every batch.

**Corrected during region A review: honor `history_seconds`.** Client parsing clamps
the requested window to 0--60 seconds, and the client handler forwards it to the store.
The previous implementation returned the whole retained window, spending bandwidth
on unrequested context. Recovery now filters by both cursor and elapsed time since
server acceptance. Storage remains 90 seconds, but replies respect the requested
window. History is trimmed on recovery, replay reads and every update, including
provisional updates, so an idle source cannot leave expired history in the next response.
A zero-second request returns empty history; missing/invalid requests default to 30.
Connected replay can still deliver a 65-second-old owed final while it remains in the
90-second shared log. This does not enlarge a genuine reconnect's requested context.

**Changed during implementation — bounding `_segments`.** The original spec bounded
`_final_history` but left `_segments` growing one entry per segment forever, which fails
"boundedness is part of correctness" over a long lecture. `_trim_history` now also drops
the evicted segment's `_segments` entry and advances `_retired_before`, so a pruned
segment's stale revision can't be re-accepted once its state is reclaimed.
`_trim_segments` caps only the unfinished IDs in `_active_segments`; those never
enter final history and therefore need their own count backstop. Accepted finals
leave the active set and remain until the 90-second history cutoff. The previous
2,000-final and 4,000-total count caps could truncate a requested 60-second window
under bursts, so they no longer evict finals. Final-state memory scales with
caption volume in the fixed retention window, not with total lecture duration.
The browser likewise expires finals after 150 seconds and caps only unfinished
segments at 400; its 80-row live display limit does not truncate local recall.

## `OutboundBuffer` (pure logic — owned by each `Client`)

No `ws`, no `asyncio`, no `await`. Handed `CaptionUpdate`s and asked for the next one to
send; it never sends anything itself. Takes an injectable `clock` so the grace-window
behavior below is testable without real time passing.

**Attributes**
- `self._pending: dict[int, CaptionUpdate]` — keyed by an increasing queue ID,
  insertion-ordered. Multiple revisions of one segment coexist in normal mode.
- `self._queued_at: dict[int, float]` — the enqueue time for each queue ID.
- `self._by_segment: dict[int, int]` — segment-to-queue-ID index used only while
  coalescing; each segment then has exactly one pending entry.
- `MAX_PENDING_UPDATES = 256` — soft backlog threshold activating compaction and
  shared-log catch-up until the queue drains.
- Buffer admission bounds: 4096 pending finals, 4000 active segments and 4 MiB pending
  JSON. Client-side replay moves final ownership to the shared log; latest active
  state remains locally bounded.
- `self._should_close`, exposed as `should_close`, flags admission failure. `Client`
  first attempts shared-log replay; persistent active-state overflow remains terminal.

**Public methods**
- `set_overloaded(bool)` — applies the client's lag verdict, compacting an existing
  backlog when lag starts. When lag clears, new revisions are preserved individually
  unless queue pressure remains active.
- `offer(update)` — append every revision normally; replace a superseded revision
  only during overload. Check hard limits after any pressure-triggered compaction.
- `begin_replay()` — compact, remove pending finals now owned by the shared log,
  clear queue-pressure/admission failure, and retain only latest active state. The
  sender separately retains any in-flight message until it finishes.
- `discard_segment(segment)` — remove queued active state once its final is in the log.
- `has_pending()`, and the `pending` / `pending_bytes` / `coalesced` / `rejected`
  counters (coalesced/rejected counts are printed on disconnect).
- `next_update(prefer_final=False, provisional_grace=0.0) -> CaptionUpdate | None` —
  normal FIFO or coalesced selection; replay uses `take_active_update(grace)` locally
  and reads one final at a time through `Server.read_final_batch`.

**Private helper**
- `_compact()` — discard only superseded provisional revisions, retaining each
  segment's final or newest active revision at its original position and enqueue age.

**Why the normal and overload paths differ**:
- Normal arrivals get distinct queue IDs, so a fast burst cannot overwrite unsent
  revisions before the sender runs. Global arrival order is preserved.
- During overload, the segment index allows replacement in constant time without
  moving the retained queue slot or restarting its provisional grace period.
- `CaptionStore` validates revision monotonicity and final durability before enqueue,
  so compaction can retain the last accepted state of each segment safely.
- Soft pressure switches delivery to shared-log replay instead of treating a full
  local final queue as a reason to disconnect. Only latest active state remains local.
- Once the replay cursor reaches the shared tail, active work drains and lag clears,
  normal full-update delivery resumes without yielding during the transition.
- A replay-horizon gap, active-state exhaustion or terminal stall is an explicit
  capacity/failure boundary. Actual reconnect remains recent-context recovery;
  persistent reconnect sessions and application ACKs are not implemented.

## `Server`

**Constructor**: `host: str = "0.0.0.0"`, `port: int = 8765`. Builds one `CaptionStore`.

**Public methods**
- `async run(self)` — `websockets.serve(self._accept, host, port, max_size=2**20,
  ping_interval=None)`, awaits forever. Built-in heartbeat is disabled because each
  receiver's custom probe owns the bounded heartbeat and catch-up policy.
- `async _accept(self, ws)` — reads the first message with a 5 s timeout, JSON-decodes,
  peeks `raw.get("type")`, constructs and runs a `Source` or a `Client`; anything else
  (bad JSON, timeout, non-object, unknown type, failed `from_dict`) closes with 1008.
- `register_client` / `unregister_client` / `client_count`.
- `compute_history`, `history_snapshot`, `final_tail`, `final_sequence`,
  `read_final_batch` — delegate directly to the shared store.
- `ingest_caption_update(update)` — validate, and broadcast only if accepted.
- `broadcast(update)` — `for client in self._clients: client.enqueue(update)`. Never
  awaits a socket. This one line is *why* slow-client isolation holds. It is also why no
  snapshot copy is needed: there is no `await` inside the loop, so no client can
  unregister midway through it.

**Entry point — folded into `Server`**: `@classmethod def main(cls, argv=None)` owns
`argparse` (`--port`, plus `--host` defaulting to `0.0.0.0`) and `asyncio.run`.
`solution_server.py` contains nothing but the `sys.path` guard and
`if __name__ == "__main__": Server.main()`.

## `Source`

Thin connection/protocol-dispatch wrapper. Owns no revision logic and no message
validation of its own. `async for raw in self.ws` → `json.loads` (skip on
`JSONDecodeError`, never crash) → `CAPTION_UPDATE` → `CaptionUpdate.from_dict` →
`server.ingest_caption_update`; `SOURCE_DONE` is logged and not forwarded; unknown types
are ignored for forward compatibility.

**Tasks owned**: exactly its own `run()` coroutine. Because
`ingest_caption_update → broadcast → client.enqueue` never `await`s, Source's read loop
is never blocked by any client's network speed.

## `Client`

**Constructor / attributes**: `ws`, `server`, `hello: ClientHello`, one `OutboundBuffer`,
`_wake` and `_probe_tick` events, `_catchup_until`, smoothed `_rtt`, one
`_pending_probe` task, `_replaying`, and `_last_sent_final_sequence`. `_in_flight`
belongs to the single sender; its cursor is committed only after a successful send.

**`async run(self)`**:
1. `self.server.register_client(self)` — **before** step 2, not after
2. `(items, snapshot_tail) = server.history_snapshot(...)`, with no intervening await
3. `await self._send(History(items))`; return if the bounded send fails
4. initialize the delivery cursor to **the captured** `snapshot_tail`, then start
   `_sender_loop()` and `_probe_loop()`; HISTORY is always first
5. `async for _ in self.ws: pass` — detects disconnect
6. `finally:` cancel both tasks, await them, `unregister_client`

**`enqueue(update)`** never awaits. In normal mode it offers to the FIFO and enters
replay on pressure/admission failure. In replay, a new final only removes that segment's
active entry and wakes the sender; the shared log owns its final. Provisionals update
the bounded latest-active buffer. A refused provisional is retried after the replay
transition releases local final entries.

**`_next_delivery()`** selects a message together with its sequence watermark. Normal
delivery takes a FIFO update and resolves a final's sequence before sending. Catch-up
reads one final after the committed cursor and sends it as `CAPTION_UPDATE`.
`HISTORY` is sent only in response to `CLIENT_HELLO`. After four consecutive finals,
a ready active update gets a turn. Active state has a one-second provisional grace.
When the log is caught up, active work is empty and lag clears, the sender atomically
returns to normal mode. `ReplayExpired` explicitly fails a missing retained prefix.

**`_sender_loop()`** remains the only caption sender. A mode change during an in-flight
send cannot start a competing replay. It awaits `_send`, commits the selected cursor
on success, and only then selects another update. Send completion is not an application
ACK; this cursor is scoped to the current ordered WebSocket connection.

**`_probe_loop()`** owns one `_round_trip()` task, including both `await ws.ping()` and
its matching pong wait. `asyncio.wait` applies deadlines without cancelling the child
on timeout and preserves caller cancellation; owned tasks are cancelled and joined on
cleanup. An overdue round trip at `max(0.5 s, 6 * RTT)` renews three seconds of catch-up,
which can already activate replay through the sender. The probe forces replay at
`max(1.5 s, 18 * RTT)`. These are catch-up deadlines, not reconnect deadlines. Timely
and late successful pongs update RTT with weight 0.25, capping each sample at the
current behind threshold. The probe pauses 250 ms after completion. Pong timing is
an indirect connection-lag signal, not proof of application consumption.

**Finite failure policy**: each complete probe and socket send has a 75-second stall
budget; `_send()` also covers initial HISTORY. `_close()` allows two seconds for the
closing handshake, then aborts the transport. This bounds blocked ping initiation,
blocked sends and closing, not just the pong wait. A sender in catch-up waits for
the next completed probe before each window of up to 64 updates or until sent
bytes reach 256 KiB (one caption can cross that byte threshold), rather than
feeding a stalled transport periodically;
the probe's terminal deadline and cleanup release bound that wait. Outstanding finals can outlive
the requested 60-second context window while retained in the shared 90-second log,
but horizon expiry, persistent active-state overflow and terminal stalls remain
explicit failures. Genuine reconnect has no persistent delivery session and uses the
required recent-context cursor; application ACK/credit flow control is not implemented.

Current catch-up uses an adaptive lease: it starts at 3 seconds, doubles
on quickly recurring lag after a full-update trial, and is capped at 30
seconds. A sustained healthy normal trial resets it. A single long probe
does not itself increase the lease. This avoids repeated full-stream
flooding of persistently slow readers; provisional coverage can take longer
to resume after repeated lag. The controller uses monotonic timings.

**Tasks owned**: `run()` (receive), `_sender_loop()` (normal/replay delivery),
`_probe_loop()` (bounded heartbeat), and their short-lived send/probe/close tasks.

## The one race condition that must be gotten right: reconnect vs. a live final

**Rule: `register_client` must execute before `history_snapshot`, with no `await` between
them** (`Client.run()` steps 1 and 2, back-to-back, both synchronous).

- If registration happened *after* the history snapshot, a final arriving at the Server in
  that window would be broadcast while this client isn't yet in `_clients` — `enqueue` is
  simply never called for it, a silent gap.
- The reverse risk — a final landing in *both* the HISTORY snapshot and a subsequent live
  `enqueue` — can't happen: `validate_and_apply`'s final-durability rule guarantees a
  segment is written to `_final_history` at most once. Snapshot context is sent before
  live delivery, and the committed captured tail excludes those snapshot finals from
  connected replay. Arrivals during the HISTORY send have larger final sequences and
  remain eligible. Even empty or zero-second context must use the captured tail so old
  unrequested finals are not replayed.
- Keeping both calls synchronous closes the remaining theoretical window: within one
  asyncio event loop, nothing (including `Source.run → broadcast`) can interleave between
  two back-to-back non-awaiting calls.

The harness classifies a repeated `(segment, revision)` as duplicate and a lower one as
stale. Current correctness and performance must be verified against the current code;
the earlier public-run numbers below belong to the superseded reconnect policy.

---

## Historical measurements: superseded reconnect policy

The following numbers were recorded in earlier design notes for a different delivery
policy. That implementation closed lagging connections at an adaptive 1.5-second-floor
resync deadline and relied on recent HISTORY recovery. It also used different local
buffer limits and probe timeout handling. **These are historical observations, not
results for the current connected shared-log replay implementation.** No current
performance claim should be inferred from them; the report requires new independent
runs with implementation commit and evidence paths.

Earlier experiments found that the small public stream could fit in downstream buffers
without visible server write pressure. Pong delays then provided an indirect lag signal
for the Python slow-reader clients. This observation motivated detection and pacing,
but it did not prove application consumption or apply to every transport workload.
Current probes time ping initiation too, because `ping()` itself may block on writing.

Recorded historical T3 optimization comparison (100 clients, 10% slow, 500 ms reads,
60 seconds), retained to explain the abandoned reconnect-based trade-off:

| Previous-policy change | Recorded final completeness |
|---|---|
| original design (no probe) | 95.13% |
| + detect and hold | 99.07% |
| + hysteresis, finals-first, grace, pessimistic start | 98.75–98.93% |
| + sticky catch-up alone | 96.18% |
| + reconnect resync at 4.0 s | 98.80% |
| + reconnect resync at 1.5 s | 99.99% |
| + adaptive RTT thresholds | 99.91–99.93% |

The old 1.5-second reconnect floor is now an early replay threshold, not the terminal
stall deadline. Its recorded score cannot be attributed to the current policy.

Recorded historical public runs (one run each, not the required three-run medians):

| Previous-policy test | Duplicates | Stale | Final completeness | Coverage | P50 | P95 |
|---|---|---|---|---|---|---|
| T1 basic (20 clients) | 0 | 0 | 100% | 100% | 4.7 ms | 7.4 ms |
| T2 scale (100 clients) | 0 | 0 | 100% | 100% | 13.5 ms | 24.7 ms |
| T3 slow (10% at 500 ms) | 0 | 0 | 99.91% | 94.9% | 13.3 ms | 439 ms |
| T4 reconnect (20%) | 0 | 0 | 100% | 97.4% | 13.7 ms | 26.2 ms |
| late join (20%) | 0 | 0 | 100% | 93.3% | 3.8 ms | 6.2 ms |

The earlier notes recorded no T5 run. Establish current T1–T5 coverage and before/after
results from new artifacts rather than carrying these values into the report.

## How this maps onto a 4-person split

- **Day 1, together**: `protocol.py` (all five message classes) + `Server._accept`'s
  dispatch skeleton. Both backend people need these first.
- **Person A — "inbound/state" track**: `CaptionStore` (+ `SegmentState`) and `Source`.
  Fully unit-testable without a running server.
- **Person B — "outbound/backpressure" track**: `OutboundBuffer` and `Client`. This is the
  single highest-value piece for hidden-performance marks (slow-reader +
  constrained-bandwidth challenges, 12/30), and — as the section above shows — the one
  that cannot be validated by reasoning alone. Budget time for measurement, not just code.
- **Person C — browser client** (`client/app.js`): connection manager, reconnect/resume,
  local caption state + dedup, recall UI (15/30/60 s). The client mirrors the server's
  revision rule exactly (`CaptionState.accept`), which is what makes "no duplicate finals"
  true on both sides.
- **Person D — integration & experiments**: the `test/` harness, `tc`/`netem` profiles,
  metrics (T1–T5), before/after comparison, report assembly.

## Verification

`test/test_delivery.py` contains regression scenarios for shared-log expiry and batch
bounds, empty-context subscription boundaries, final pressure switching to replay,
in-flight cursor ownership, normal revision coverage and restoration after catch-up,
initial-HISTORY races, and bounded ping/pong/close cleanup. It includes an opt-in real
65-second transport-stall scenario. Read its run instructions and record actual outcomes
for the implementation commit being evaluated; the existence of these tests is not a
claim that current public or hidden workloads have passed.

`test/test_live_delivery.py` additionally verifies individual live frames,
latest-active fairness, bounded probe windows and exactly one initial HISTORY
under real WebSocket queue pressure.

Use `test/submission_check.py` for entry-point compatibility and the unchanged public
harness for T1–T5. Run each reported experiment at least three independent times and
report medians, normal/slow splits, completeness, freshness, coverage and recovery.
Check network impairment and memory limits in the required Ubuntu environment. Browser
revision replacement, local recall, late join and actual reconnect remain separate
application-demonstration checks.
