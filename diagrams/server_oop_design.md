# OOP design: Server / Source / Client for solution_server.py

## Context

The current `submission_root/solution_server.py` is the intentionally-naive baseline: a flat set of free functions (`handler`, `handle_source`, `handle_client`, `broadcast_naively`), raw `dict`/`json.dumps` traffic, and a single global `CLIENTS` set. It has no revision state, no history, no per-client buffering — all of the graded networking mechanisms (revision/dedup, bounded recovery history, per-client backpressure, slow-client isolation) are still to be built.

This is a fully object-oriented design: three actor classes — `Server` (coordinator), `Source` (wraps the one caption-source connection, delegates to Server), `Client` (wraps one browser/load-client connection) — plus the wire protocol itself modeled as typed message classes instead of raw dicts, and the CLI entry point folded into `Server` rather than left as free module-level functions. No global mutable state and no bare `dict` passed between layers anywhere.

Source and Client deliberately do **not** share a base class (decided explicitly — see "Design decisions" below): they're independent, each a full `run()` of their own, trading a little duplicated read-loop boilerplate for zero risk of one's logic leaking into the other through a shared parent.

The two hardest pieces of logic — revision/dedup/history validation, and per-client buffering/coalescing — are pulled out into their own **pure, non-actor logic classes** (`CaptionStore`, `OutboundBuffer`) with no networking or asyncio dependency at all. This makes them unit-testable in complete isolation and lets two people own a "state-side" and a "backpressure-side" track each without touching the same files or needing a live end-to-end system to test their own logic.

This document is a **design spec** — meant to be handed directly to the team before implementation starts.

## Design decisions already made

- **No inheritance between Source and Client.** Considered a shared `Connection` base class (template-method pattern); rejected in favor of full independence.
- **Composition over inheritance everywhere else too** — e.g. `CaptionStore`'s internal `SegmentState` wraps a `CaptionUpdate` message rather than subclassing it.
- **Single-session only.** The mandatory tests use exactly one source and one session (`"L1"`) at a time; don't key `CaptionStore` by session id. (Cheap to generalize later — not worth building now.)

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
Client  --delegates to-->  OutboundBuffer.offer(update) / .drain_next()
```

`CaptionUpdate` is the one message type that flows through almost the entire system (Source parses it in, Server/CaptionStore validate it, Client/OutboundBuffer buffer and send it back out) — it's the closest thing this design has to a shared vocabulary, and it's a plain data class, not a behavior-bearing one.

## `protocol.py` — typed wire messages (write this first, together)

Every message on the wire becomes a small frozen dataclass instead of a raw dict, each knowing how to validate itself out of JSON and serialize itself back:

```python
@dataclass(frozen=True)
class SourceHello:
    session: str

@dataclass(frozen=True)
class ClientHello:
    client_id: int
    session: str
    last_segment: int
    last_revision: int
    history_seconds: float

@dataclass(frozen=True)
class CaptionUpdate:
    session: str
    segment: int
    revision: int
    source_ts: float
    final: bool
    text: str

@dataclass(frozen=True)
class History:
    items: list[CaptionUpdate]

@dataclass(frozen=True)
class SourceDone:
    segments: int | None = None
```

Each of `SourceHello`, `ClientHello`, `CaptionUpdate`, `SourceDone` exposes:
- `@classmethod def from_dict(cls, raw: dict) -> "Self | None"` — field presence/type validation (e.g. `CaptionUpdate.from_dict` enforces `segment: int > 0`, `revision: int > 0`, `final: bool`, `text: str`, `source_ts` numeric); returns `None` on anything malformed/missing rather than raising. This is what replaces the baseline's ad hoc dict `.get()` calls, and what Source and Server use instead of hand-parsing.

Every message type (including `History`, which is only ever server-constructed, never parsed) exposes:
- `def to_dict(self) -> dict` (adds the literal `"type"` tag) and `def to_json(self) -> str` — so no call site ever hand-assembles a dict to send; it's always `await ws.send(update.to_json())`.

**The one unavoidable raw-dict touch**: `Server._accept` must peek `raw.get("type")` to know *which* class's `from_dict` to call for the opening hello — inherent to any JSON tagged-union protocol, OOP or not. Everything past that first peek is a typed object, never a bare dict again, anywhere in the system.

This module is a shared dependency of both backend tracks (Source needs `CaptionUpdate`/`SourceDone`; Client/Server need `ClientHello`/`History`/`CaptionUpdate`), so it should be pair-written on day 1 rather than assigned to one person — see the team-split section.

## `CaptionStore` (pure logic — owned by `Server`)

No `ws`, no `asyncio`, no `await` anywhere in this class. Constructed once by `Server` at startup.

**Attributes**
- `self._segments: dict[int, SegmentState]` — current winning state per segment, for O(1) validate-against-latest lookups. `SegmentState` is private to this class (see below).
- `self._final_history: collections.deque[SegmentState]` — bounded, ordered oldest→newest; the recovery/HISTORY source.
- `self._history_horizon_seconds = 90.0` (comfortably over the 60s spec max), `self._history_max_items = 2000`.

**`SegmentState`** — a private helper, *composed* from a `CaptionUpdate` rather than subclassing it (consistent with the no-inheritance decision): `@dataclass(frozen=True) class SegmentState: update: CaptionUpdate; received_at: float`, with `segment`/`revision`/`final` convenience properties delegating to `self.update`. Never leaves `CaptionStore` — everything outside this class only ever sees `CaptionUpdate`.

**Public methods**
- `def validate_and_apply(self, update: CaptionUpdate) -> CaptionUpdate | None`:
  1. if `update.segment` already in `self._segments` and that entry is `.final` → reject (`None`). Final is durable — nothing ever overwrites it, regardless of the incoming revision number.
  2. if `update.segment` already present and `update.revision <= existing.revision` → reject (covers both duplicates and stale/out-of-order).
  3. otherwise wrap `SegmentState(update, received_at=time.monotonic())`, store it in `self._segments[update.segment]`, and if `update.final`, append to `self._final_history` and call `_trim_history()`. Return `update` itself (the caller's confirmation that it was accepted).
- `def compute_history(self, last_segment, last_revision) -> list[CaptionUpdate]` — returns the `.update` of each entry in `_final_history` with `(segment, revision) > (last_segment, last_revision)`, already oldest-first (deque is append-order).

**Private helper**
- `def _trim_history(self)` — pop from the left of `_final_history` while over `history_max_items` or the oldest entry is older than `history_horizon_seconds`.

**Why it's not a 4th actor**: it never owns a socket, a task, or a connection lifecycle — it's a data structure with rules attached. `Server` composing one is internal composition, not a 4th top-level class.

## `OutboundBuffer` (pure logic — owned by each `Client`)

No `ws`, no `asyncio.create_task`, no `await` anywhere in this class — it's handed `CaptionUpdate`s and asked for the next one to send; it never sends anything itself.

**Attributes**
- `self._pending: dict[int, CaptionUpdate]` — keyed by segment number, insertion-ordered.
- `MAX_PENDING_SEGMENTS` class constant (e.g. 256) — the explicit bound backing "boundedness is part of correctness."
- `self._force_close: bool = False` — set when the bound is exceeded by finals alone (see below); `Client` checks this after each `offer()`.

**Public methods**
- `def offer(self, update: CaptionUpdate) -> None`:
  ```python
  self._pending[update.segment] = update   # insert, or overwrite in place if segment already pending
  self._enforce_bound()
  ```
- `def has_pending(self) -> bool` — `bool(self._pending)`.
- `def drain_next(self) -> CaptionUpdate` — pops and returns the oldest entry (`next(iter(self._pending.items()))`, then `del`).

**Private helper**
- `def _enforce_bound(self)` — if `len(self._pending) > MAX_PENDING_SEGMENTS`: evict the oldest **non-final** entry first (walk `_pending` in insertion order, drop the first one with `final=False`). If every pending entry is already final (this client is catastrophically behind on finals specifically, not just provisionals), do **not** silently drop a final to make room — instead set `self._force_close = True`, forcing the browser's existing auto-reconnect path to re-sync through `compute_history` instead of the server growing unbounded or discarding a final.

**Why `dict[int, CaptionUpdate]` is the right shape** (over a list+index-map or `asyncio.Queue`):
- Python dicts preserve insertion order, and **re-assigning an existing key does not move its position**. So `offer()` simultaneously (a) coalesces a superseded provisional for a segment already queued, (b) preserves its original arrival-order slot for FIFO draining, and (c) needs no separate cleanup/index bookkeeping.
- `OutboundBuffer` never needs to compare revision numbers itself — `CaptionStore` has already validated monotonicity and final-durability before anything reaches `offer()`, so an unconditional overwrite is always correct.
- Coalescing becomes **emergent, not heuristic**: under normal pace, `drain_next()` empties each entry before the next revision for that segment arrives, so every revision is delivered individually (keeps "Normal Update Coverage" high for non-overloaded clients — no explicit "is this client slow" branch to get wrong). Coalescing only happens when draining is genuinely behind enough that two updates for the same open segment both land before either is drained — exactly the "genuine backlog" condition the spec requires, for free.
- Finals are never silently dropped: once an entry is final, nothing ever writes that key again (`CaptionStore`'s dedup rule guarantees it), so it just waits its FIFO turn. Rapid convergence after overload clears falls out too, since draining is strictly FIFO with no artificial pacing.

**Testability**: fully unit-testable with no asyncio and no websocket — feed it a sequence of `CaptionUpdate`s via `offer()`, call `drain_next()`, assert on exactly what comes back and in what order. The 12 hardest hidden-performance marks (slow-reader + constrained-bandwidth challenges) live here, so this deserves dedicated, isolated test coverage before it's wired into a real `Client`.

## `Server`

**Constructor**: `host: str`, `port: int`. Builds one `self._store = CaptionStore()`.

**Attributes**
- `self._store: CaptionStore`
- `self._clients: set[Client]`

**Public methods**
- `async def run(self)` — builds `websockets.serve(self._accept, self._host, self._port, ...)`, awaits forever.
- `async def _accept(self, ws)` — reads the first message with a timeout (`asyncio.wait_for(ws.recv(), 5)`), JSON-decodes it, peeks `raw.get("type")`: on `"SOURCE_HELLO"` calls `SourceHello.from_dict(raw)` and constructs+runs a `Source`; on `"CLIENT_HELLO"` calls `ClientHello.from_dict(raw)` and constructs+runs a `Client`; anything else (bad JSON, timeout, unknown type, failed `from_dict`) closes with code 1008.
- `def register_client(self, client: Client)` / `def unregister_client(self, client: Client)` — plain synchronous set add/discard.
- `def compute_history(self, last_segment, last_revision) -> list[CaptionUpdate]` — delegates straight to `self._store.compute_history(...)`.
- `def ingest_caption_update(self, update: CaptionUpdate) -> None` — called by `Source`. Calls `self._store.validate_and_apply(update)`; if it returns non-`None`, calls `self.broadcast(update)`.
- `def broadcast(self, update: CaptionUpdate) -> None` — `for client in self._clients: client.enqueue(update)`. Never awaits a socket. This one line is *why* slow-client isolation holds: one client's backpressure can never block delivery to anyone else, because `enqueue` only touches in-memory buffer state, not the network.

**Entry point — folded into `Server`, no free-floating `main()`**:
```python
@classmethod
def main(cls, argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    try:
        asyncio.run(cls("0.0.0.0", args.port).run())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    Server.main()
```
This is the only module-level code left. A module needs *some* entry point; wrapping `if __name__ == "__main__":` itself in a class would be over-engineering, not "more OOP."

**Tasks owned**: the implicit per-connection task `websockets.serve` spawns to run `_accept`. No periodic background task needed — trimming on every final-ingest (inside `CaptionStore`) is enough to keep memory bounded for the graded scenarios.

**Cleanup**: Server holds no sockets itself — it only reacts to `unregister_client`, called by `Client` in its own `finally` block.

## `Source`

Thin connection/protocol-dispatch wrapper. Owns no revision logic, no message-validation logic of its own (that now lives in `protocol.py`'s `from_dict` methods).

**Constructor**: `ws`, `hello: SourceHello`, `server: Server`.

**Public method**
- `async def run(self)`:
  ```python
  try:
      async for raw in self.ws:
          try:
              msg = json.loads(raw)
          except json.JSONDecodeError:
              continue                      # malformed JSON must not crash the server
          t = msg.get("type")
          if t == "CAPTION_UPDATE":
              update = CaptionUpdate.from_dict(msg)
              if update is not None:
                  self.server.ingest_caption_update(update)
          elif t == "SOURCE_DONE":
              pass                          # test-control only, no forwarding required
          else:
              continue                      # ignore unrecognized types, forward-compat
  finally:
      pass   # nothing to unregister; Server has no per-source bookkeeping that matters
  ```

**Tasks owned**: exactly its own `run()` coroutine. It spawns nothing else, and because `ingest_caption_update → broadcast → client.enqueue` never `await`s, Source's read loop is never blocked by any client's network speed.

**Cleanup**: nothing to release beyond letting `websockets` close the socket when `run()` returns.

## `Client`

Thin connection wrapper around one `OutboundBuffer`.

**Constructor / attributes**
- `ws`, `server`, `hello: ClientHello` (carries `client_id`, `session`, `last_segment`, `last_revision`, `history_seconds` — read once to build the HISTORY reply, then not retained as ongoing state).
- `self._buffer = OutboundBuffer()`.
- `self._wake = asyncio.Event()` — signals the sender task that new work exists.
- `self._sender_task: asyncio.Task | None`.

**Public methods**
- `async def run(self)`:
  1. `self.server.register_client(self)` — **before** step 2, not after (see race section below).
  2. `items = self.server.compute_history(self.hello.last_segment, self.hello.last_revision)`; `await self.ws.send(History(items).to_json())`.
  3. `self._sender_task = asyncio.create_task(self._sender_loop())`.
  4. `async for _ in self.ws: pass` — browser clients send nothing further after hello; this loop just detects disconnect (and lets `websockets`' built-in ping/pong keep the connection alive).
  5. `finally:` cancel `self._sender_task`, await it, `self.server.unregister_client(self)`.
- `def enqueue(self, update: CaptionUpdate) -> None` — called by `Server.broadcast`, must never block or await:
  ```python
  self._buffer.offer(update)
  self._wake.set()
  ```

**Private helper**
- `async def _sender_loop(self)`:
  ```python
  while True:
      await self._wake.wait(); self._wake.clear()
      while self._buffer.has_pending():
          update = self._buffer.drain_next()
          try:
              await self.ws.send(update.to_json())
          except ConnectionClosed:
              return
      if self._buffer._force_close:
          await self.ws.close(1013, "client too far behind")
          return
  ```

**Tasks owned**: `run()` (receive/keepalive loop) **and** `_sender_loop()` (the independent sender task) — exactly the split the assignment calls for.

**Cleanup**: cancel sender task, let `self._buffer` be GC'd with the object, `unregister_client`.

## The one race condition that must be gotten right: reconnect vs. a live final

**Rule: `register_client` must execute before `compute_history`, with no `await` between them** (`Client.run()` steps 1 and 2, back-to-back, both synchronous).

- If registration happened *after* the history snapshot, a final arriving at the Server in that window would be broadcast while this client isn't yet in `_clients` — `enqueue` is simply never called for it, a silent gap.
- The reverse risk — a final landing in *both* the HISTORY snapshot and a subsequent live `enqueue` — can't happen: `CaptionStore.validate_and_apply`'s final-durability rule guarantees a segment is written to `_final_history` at most once, and nothing ever re-broadcasts an already-finalized segment. So a given final is either already in `_final_history` when `compute_history` reads it (delivered via HISTORY only) or becomes final strictly afterward (delivered live only) — never both.
- Keeping both calls synchronous (no `await` between them) closes the remaining theoretical window: within one asyncio event loop, nothing else (including `Source.run → broadcast`) can interleave between two back-to-back non-awaiting calls.

This is also a strong viva answer in waiting — it directly matches the spec's example question "What exactly happens when a client reconnects after 10 seconds offline?"

## How this maps onto a 4-person split

- **Day 1, together**: `protocol.py` (all five message classes) + `Server._accept`'s dispatch skeleton. Both backend people need these before they can write anything else — pair-program this in the first hour or two rather than assigning it to one person.
- **Person A — "inbound/state" track**: `CaptionStore` (+ its private `SegmentState`) and `Source`. Fully unit-testable without a running server — feed `validate_and_apply` synthetic `CaptionUpdate` objects and assert accept/reject. `Source` itself is a fast follow-on once `CaptionStore`'s interface is fixed.
- **Person B — "outbound/backpressure" track**: `OutboundBuffer` and `Client`. Also fully unit-testable in isolation (feed `offer()` a sequence of `CaptionUpdate`s, assert `drain_next()` order/coalescing/bound behavior) before any real socket is involved. This is the single highest-value piece for hidden-performance marks (slow-reader + constrained-bandwidth challenges, 12/30 marks).
- **Person C — browser client** (`client/app.js`): connection manager, reconnect/resume, local caption state + dedup, recall UI (15/30/60s). Fully independent — only needs the already-fixed wire protocol, can develop against the current naive baseline server from day 1.
- **Person D — integration & experiments**: `test/caption_source.py`/`load_clients.py`/`run_demo.py` harness, `tc`/`netem` profiles, metrics (T1–T5 from the spec), before/after comparison, report assembly.

## Verification (design-review level, before implementation)

Walk this design against the three documented scenarios in `docs/PROTOCOL_EXAMPLES.md` (normal connection, disconnect/reconnect, late join) by hand, tracing exactly which methods fire in which order, and confirm:
- No scenario produces a duplicate final or a missed final.
- No scenario requires `Source` or `Client` to read/write `CaptionStore`/`OutboundBuffer` except through their owning actor (`Server`/`Client` respectively), and neither ever constructs a raw dict to send or receive.
- The buffer bound (`MAX_PENDING_SEGMENTS`) and history bound (`history_max_items`/`history_horizon_seconds`) are each named, finite, and justified — not implicit.

Once implementation begins:
- `protocol.py` gets small round-trip tests (`to_dict`/`from_dict`/`to_json`) plus malformed-input rejection tests (missing field, wrong type, negative segment/revision) before anything else is built on top of it.
- `CaptionStore` and `OutboundBuffer` each get a standalone pytest module before they're ever wired into `Server`/`Client` — the fastest way to catch revision/dedup and coalescing bugs without the async server running at all.
- End-to-end: `test/caption_source.py` + `test/load_clients.py` via `run_demo.py` (T1–T5 from the spec), then `test/analyse_results.py` for the metrics.
