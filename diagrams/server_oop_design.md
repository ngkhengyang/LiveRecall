# OOP design: Server / Source / Client for solution_server.py

**Status: as-built.** This was written as a design spec before implementation; it now
describes the code that is in `submission_root/app/`. Sections marked
*"changed during implementation"* record where measurement contradicted the original
design — those are the parts worth reading before the viva, because they are the ones
we can defend with evidence rather than intent.

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
Server  --delegates to-->  CaptionStore.validate_and_apply(update) / .compute_history(...)
Server  --pushes into-->   Client.enqueue(update: CaptionUpdate)   (never awaits a socket)
Client  --delegates to-->  OutboundBuffer.offer(update) / .next_update(...)
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

**The one unavoidable raw-dict touch**: `Server._accept` must peek `raw.get("type")` to
know *which* class's `from_dict` to call for the opening hello — inherent to any JSON
tagged-union protocol, OOP or not. Everything past that first peek is a typed object.

## `CaptionStore` (pure logic — owned by `Server`)

No `ws`, no `asyncio`, no `await` anywhere in this class. Constructed once by `Server`.

**Attributes**
- `self._segments: dict[int, SegmentState]` — current winning state per segment.
- `self._final_history: collections.deque[SegmentState]` — bounded, ordered
  oldest→newest; the recovery/HISTORY source.
- `self._retired_before: int` — watermark; see boundedness below.
- Class constants: `HISTORY_HORIZON_SECONDS = 90.0`, `HISTORY_MAX_ITEMS = 2000`,
  `MAX_TRACKED_SEGMENTS = 4000`.

**`SegmentState`** — *composed* from a `CaptionUpdate` rather than subclassing it:
`@dataclass(frozen=True) class SegmentState: update: CaptionUpdate; received_at: float`,
with `segment`/`revision`/`final` convenience properties. Never leaves `CaptionStore`.

**Public methods**
- `validate_and_apply(update) -> CaptionUpdate | None`:
  1. reject if `update.segment < self._retired_before`;
  2. reject if the segment is already `.final` — final is durable, nothing overwrites it
     regardless of the incoming revision number;
  3. reject if `update.revision <= existing.revision` (covers duplicates and stale/
     out-of-order in one rule);
  4. otherwise store, and if final append to `_final_history` and trim. Returns the
     update as the caller's confirmation that it was accepted.
- `compute_history(last_segment, last_revision) -> list[CaptionUpdate]` — every entry
  with `(segment, revision) > (last_segment, last_revision)`, oldest-first.

**Decision: `history_seconds` is deliberately ignored.** The bounded window is already
the cap, and a client can only be harmed by receiving fewer finals than it missed —
anything it already holds is excluded by the tuple comparison. Serving the whole window
costs a late joiner nothing and maximizes Final Caption Completeness, which is the hard
correctness gate. Expect this as a viva question.

**Changed during implementation — bounding `_segments`.** The original spec bounded
`_final_history` but left `_segments` growing one entry per segment forever, which fails
"boundedness is part of correctness" over a long lecture. `_trim_history` now also drops
the evicted segment's `_segments` entry and advances `_retired_before`, so a pruned
segment's stale revision can't be re-accepted once its state is reclaimed.
`_trim_segments` is a second backstop for a source that opens segments without ever
finalizing them — those never enter the history, so the time and item bounds alone would
not reclaim them.

## `OutboundBuffer` (pure logic — owned by each `Client`)

No `ws`, no `asyncio`, no `await`. Handed `CaptionUpdate`s and asked for the next one to
send; it never sends anything itself. Takes an injectable `clock` so the grace-window
behavior below is testable without real time passing.

**Attributes**
- `self._pending: dict[int, CaptionUpdate]` — keyed by segment, insertion-ordered.
- `self._queued_at: dict[int, float]` — when each segment first entered the buffer.
- `MAX_PENDING_SEGMENTS = 256` — the explicit bound backing "boundedness is part of
  correctness".
- `self._should_close`, exposed as the public read-only `should_close` property.

**Public methods**
- `offer(update)` — `self._pending[update.segment] = update`, then enforce the bound.
- `has_pending()`, and the `pending` / `coalesced` / `dropped` counters (printed on
  disconnect; they are how we observed the behavior described below).
- `next_update(prefer_final=False, provisional_grace=0.0) -> CaptionUpdate | None` —
  see "Slow readers" for why the two arguments exist.

**Private helper**
- `_enforce_bound()` — evict the oldest **non-final** entry. If every pending entry is
  already final, do **not** drop a final to make room — set `_should_close` instead,
  forcing a reconnect that re-syncs through `compute_history`.

**Why `dict[int, CaptionUpdate]` is the right shape** (over a list+index-map or
`asyncio.Queue`):
- Python dicts preserve insertion order, and **re-assigning an existing key does not move
  its position**. So `offer()` simultaneously (a) coalesces a superseded provisional for a
  segment already queued, (b) preserves its original arrival-order slot for FIFO draining,
  and (c) needs no separate cleanup/index bookkeeping.
- It never needs to compare revision numbers — `CaptionStore` has already validated
  monotonicity and final-durability before anything reaches `offer()`, so an
  unconditional overwrite is always correct.
- Coalescing is **emergent, not heuristic**: when the client is keeping up, each entry is
  drained before the next revision for that segment arrives, so every revision is
  delivered individually and Normal Update Coverage stays at 100%. There is no "is this
  client slow" branch *inside this class* to get wrong.
- Finals are never silently dropped: once an entry is final, nothing ever writes that key
  again, so it just waits its turn.

**Changed during implementation — `offer()` stops accepting work after `should_close`.**
Originally the bound was only enforced inside `_enforce_bound`, so once the close flag
was set the dict kept growing while the sender task was still blocked. Unit test: with
`MAX_PENDING_SEGMENTS = 3` and 200 offered finals, `pending` settled at 4 rather than
climbing. Everything dropped there is durable in the server's history.

## `Server`

**Constructor**: `host: str = "0.0.0.0"`, `port: int = 8765`. Builds one `CaptionStore`.

**Public methods**
- `async run(self)` — `websockets.serve(self._accept, host, port, max_size=2**20,
  ping_interval=20, ping_timeout=20)`, awaits forever.
- `async _accept(self, ws)` — reads the first message with a 5 s timeout, JSON-decodes,
  peeks `raw.get("type")`, constructs and runs a `Source` or a `Client`; anything else
  (bad JSON, timeout, non-object, unknown type, failed `from_dict`) closes with 1008.
- `register_client` / `unregister_client` / `client_count`.
- `compute_history(...)` — delegates straight to the store.
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
a `_wake` event, a `_probe_tick` event, and the catch-up state (`_catchup_until`, `_rtt`).

**`async run(self)`**:
1. `self.server.register_client(self)` — **before** step 2, not after
2. `items = self.server.compute_history(hello.last_segment, hello.last_revision)`
3. `await self.ws.send(History(items).to_json())`
4. start `_sender_loop()` **and** `_probe_loop()` — after the HISTORY send, so HISTORY is
   always the first frame
5. `async for _ in self.ws: pass` — detects disconnect, lets keepalive run
6. `finally:` cancel both tasks, await them, `unregister_client`

**`enqueue(update)`** — `buffer.offer(update)` then `_wake.set()`. Never blocks, never
awaits.

**Tasks owned**: `run()` (receive/keepalive), `_sender_loop()` (drains the buffer), and
`_probe_loop()` (measures consumption). The third task was **not** in the original design
— see below.

## The one race condition that must be gotten right: reconnect vs. a live final

**Rule: `register_client` must execute before `compute_history`, with no `await` between
them** (`Client.run()` steps 1 and 2, back-to-back, both synchronous).

- If registration happened *after* the history snapshot, a final arriving at the Server in
  that window would be broadcast while this client isn't yet in `_clients` — `enqueue` is
  simply never called for it, a silent gap.
- The reverse risk — a final landing in *both* the HISTORY snapshot and a subsequent live
  `enqueue` — can't happen: `validate_and_apply`'s final-durability rule guarantees a
  segment is written to `_final_history` at most once, and nothing ever re-broadcasts an
  already-finalized segment. So a given final is either already in `_final_history` when
  `compute_history` reads it (delivered via HISTORY only) or becomes final strictly
  afterward (delivered live only) — never both.
- Keeping both calls synchronous closes the remaining theoretical window: within one
  asyncio event loop, nothing (including `Source.run → broadcast`) can interleave between
  two back-to-back non-awaiting calls.

This is directly confirmed by the harness: `test/load_clients.py` classifies a repeated
`(segment, revision)` as `duplicate` and a lower one as `stale`, and every public run
reports **0 duplicates, 0 stale**, including T4 with 20% of clients disconnecting.

---

# Slow readers: what measurement changed

**This section did not exist in the original design, and it is the single biggest
correction measurement forced on us.** Everything above was written assuming that a slow
reader eventually makes `ws.send()` push back, filling `OutboundBuffer` and triggering
coalescing. That assumption is false, and the original design scored **95.13% Final
Caption Completeness on T3 — below the 99% correctness gate**, which alone would cap the
hidden-performance section at 15/30.

## Finding 1: there is no send-side backpressure signal at all

With one slow reader (500 ms between reads) and the public workload:

- `ws.transport.get_write_buffer_size()` stayed at **0** for the entire 60 s run.
- The server's own counters reported **`coalesced=0, dropped=0`** — `OutboundBuffer` was
  never non-empty, so the coalescing logic above never executed once.

The cause is simply volume: 76 segments × 3 revisions × ~150 bytes is about **34 KB**,
which fits entirely inside kernel socket buffers plus the client library's inbound queue.
TCP never pushes back, so `send()` never blocks. The backlog is real but completely
invisible to the application. A design that waits for `send()` to signal pressure will
coalesce nothing, and the slow client spends its limited read budget on provisional
revisions that were obsolete before it got to them: it read 119 messages and only **39**
of them were finals.

## Finding 2: ping/pong does expose it

The client library stops reading from its transport once its own inbound queue is full,
so pongs stall under exactly the condition we care about. Measured on the same run:

```
t= 7.5s  write_buf=0   ping_rtt=0.5 ms
t=18.1s  write_buf=0   ping_rtt=timeout (>3 s)
t=26.1s  write_buf=0   ping_rtt=442.8 ms
t=36.6s  write_buf=0   ping_rtt=timeout (>3 s)
```

`write_buf` is flat at 0 throughout; RTT moves three orders of magnitude. That is the
signal.

## Finding 3: there is a ~17 second blind window

The client's queue only fills at the rate production exceeds consumption (4/s vs 2/s), so
it takes roughly 17 s before pongs stall. Tracing what the client actually received, it
got all three revisions of every segment from t=0.5 s to t=17.1 s — the full uncoalesced
stream — before any mechanism could engage. Starting clients pessimistically does **not**
fix this: a slow client's pongs are genuinely fast until its queue fills, so it is
indistinguishable from a healthy one during that window.

## What the `Client` does about it

Four mechanisms, in the order they engage:

1. **Detect** — `_probe_loop()` keeps exactly one ping in flight (`PROBE_INTERVAL_SECONDS
   = 0.25`). One overdue pong raises catch-up for `CATCHUP_STICKY_SECONDS = 3.0`. Only
   this task sends pings, so there is never more than one outstanding no matter how far
   behind the client gets.
2. **Hold** — while in catch-up, `_hold()` waits for the next completed probe (capped by
   `MAX_HOLD_SECONDS = 0.5`) *before each send*. **This is what creates the backlog**, and
   therefore what finally makes the coalescing in `OutboundBuffer` execute. A client that
   is keeping up never holds, so normal clients pay nothing.
3. **Prioritize** — `next_update(prefer_final=True,
   provisional_grace=PROVISIONAL_GRACE_SECONDS = 1.0)` takes the
   oldest *final* ahead of any provisional, and withholds a lone provisional for 1 s.
   Under the deterministic source a segment finalizes in ~0.4 s, so the provisional is
   normally replaced in place by its own final and one message carries what three would
   have. Nothing is discarded: a segment that genuinely stays open past the grace is
   released on the next call, satisfying "preserve the newest relevant revision of an
   active segment".
4. **Resync** — a pong still outstanding past `_resync_threshold()` means the reader has
   stopped draining entirely. Close with 1013; the client's own reconnect returns every
   missed final in **one** HISTORY message. For a reader consuming two messages a second,
   that is the difference between converging and never catching up — it is also exactly
   the spec's "rapid convergence after overload", and `test/load_clients.py` reconnects
   automatically.

Thresholds for (1) and (4) are scaled against each connection's own smoothed RTT
(`BEHIND_RTT_MULTIPLIER = 6`, `RESYNC_RTT_MULTIPLIER = 18`, with floors
`BEHIND_AFTER_SECONDS = 0.5` and `RESYNC_AFTER_SECONDS = 1.5`), so a
link with real delay is judged against what is normal *for that link*. The supplied
`network_profiles/delay_loss.sh` alone is 120 ms ± 30 ms with 3% loss; a fixed 1.5 s
threshold would read that as a stalled reader. Measured thresholds:

| link | baseline RTT | "behind" after | "resync" after |
|---|---|---|---|
| loopback | 0.5 ms | 500 ms (floor) | 1500 ms (floor) |
| netem 120 ms ± 30 ms | 250 ms | 1500 ms | 4500 ms |

## Honest attribution — what actually moved the number

Each step was measured on T3 (100 clients, 10% slow at 500 ms, 60 s). Not every idea paid
off, and the table matters more than the narrative:

| change | Final completeness |
|---|---|
| original design (no probe) | 95.13% |
| + detect & hold (1, 2) | 99.07% |
| + hysteresis, finals-first, grace, pessimistic start | 98.75 – 98.93% |
| + sticky catch-up alone | **96.18%** — worse |
| + resync at 4.0 s | 98.80% |
| + resync at 1.5 s | **99.99%** |
| + adaptive RTT thresholds | 99.91 – 99.93% |

Two changes carried it: **detect+hold** (95 → 99.07) and **resync** (99.07 → 99.9+). The
middle block roughly cancelled out — finals-first and the grace window are defensible on
their own terms and cost nothing, but they did not earn the number. Sticky catch-up
*hurt* in isolation, and only became useful once resync existed to stop a stalled reader
draining a stale queue.

**Known risk:** `RESYNC_AFTER_SECONDS` is the most sensitive constant in the system —
4.0 s gives 98.8%, 1.5 s gives 99.99%. It is the one value fitted against the public test,
so it should be re-checked rather than trusted if the hidden workload behaves differently.

---

## Measured results (public tests, 1 run each; use `--runs 3` for the report)

| | Duplicates | Stale | Final completeness | Normal Update Coverage | P50 | P95 |
|---|---|---|---|---|---|---|
| T1 basic (20 clients) | 0 | 0 | 100% | 100% | 4.7 ms | 7.4 ms |
| T2 scale (100 clients) | 0 | 0 | 100% | 100% | 13.5 ms | 24.7 ms |
| T3 slow (10% @ 500 ms) | 0 | 0 | 99.91% | 94.9% | 13.3 ms | 439 ms |
| T4 reconnect (20%) | 0 | 0 | 100% | 97.4% | 13.7 ms | 26.2 ms |
| late join (20%) | 0 | 0 | 100% | 93.3% | 3.8 ms | 6.2 ms |

Slow-reader isolation on T3: normal clients hold **P95 ≈ 21 ms, 100% fresh** while slow
clients degrade — which is the point of the mechanism. The sub-100% coverage figures on
T3/T4/late-join are the revisions a disconnected or overloaded client provably could not
have received, which the spec permits; coverage is 100% whenever no client is impaired.

T5 (`tc/netem`) has **not** been run — it needs `sudo` on a real Ubuntu VM, and WSL is not
the reference grading environment. Run `make t5-network-enable` → `make t5-network` →
`make t5-network-reset` there.

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

Design-level, by hand against `docs/PROTOCOL_EXAMPLES.md` (normal connection,
disconnect/reconnect, late join): no scenario produces a duplicate or missed final;
nothing reads `CaptionStore`/`OutboundBuffer` except through its owning actor; every bound
is named, finite and justified.

As implemented:
- `CaptionStore` and `OutboundBuffer` have standalone assertion suites covering
  duplicate/stale/final-durability, history trimming and segment retirement, coalescing
  with FIFO-slot preservation, provisional eviction, finals-first draining, and the grace
  window — all with an injected clock and no asyncio.
- `protocol.py` round-trips and rejects malformed input (missing field, wrong type,
  `bool`-as-int, non-positive segment/revision).
- `client/app.js` is exercised under a DOM stub: three revisions render as one line, a
  final is never overwritten, and the 15/30/60 s windows resolve from local state with
  zero additional server requests.
- End-to-end: `test/run_demo.py` for T1–T4 plus a late-join run, and
  `test/submission_check.py` for the required startup path.
