# LiveRecall Technical Report

Team: TODO | Members: TODO | Implementation commit: TODO

## 1. Architecture and protocol

LiveRecall distributes the supplied deterministic caption stream to student
browsers over WebSockets. The Python asyncio server separates authoritative
caption state from independent delivery to each receiver:

```text
caption_source.py --WebSocket--> Source --> Server --> CaptionStore
                                             |        latest revisions
                                    enqueue  |        shared final log
                                             v              |
                                  per-client buffer         | replay
                                             |              v
                                             +---------> sender --WS--> browser
```

`submission_root/solution_server.py` preserves the required
`python solution_server.py --port <port>` interface, defaulting to
`0.0.0.0:8765`. Within `app/`, `server.py` coordinates connections and the shared
store; `source.py` parses producer input; `caption_store.py` owns revision and
history state; and `outbound_buffer.py` holds pending work. Each `Client` in
`client.py` owns its sender, lag probe and replay cursor. `client/app.js` and
`client/index.html` provide browser revision replacement, recall and reconnect.

The first JSON message selects `SOURCE_HELLO` or `CLIENT_HELLO`; failure to
provide a valid hello within five seconds closes the connection with code 1008.
Only one source may be active. `protocol.py` requires positive integer segment
and revision IDs, finite nonnegative `source_ts`, Boolean `final`, string `text`
and a nonempty session. Updates must match the source's declared session.
Malformed source updates and unknown message types are ignored; extra fields
are tolerated. `SOURCE_DONE` is test control and is not forwarded.

The store accepts updates before broadcast. Ingestion and enqueueing perform no
socket waits; a blocked sender therefore does not directly stall other clients.
Fan-out remains proportional to client count, while cached JSON avoids repeated
serialization. The design assumes one lecture with increasing segment IDs and
ordered completion. State survives source disconnection; a new lecture requires
a server restart and browser reload because sessions share one store.

## 2. Revision and recovery semantics

`CaptionStore` retains the highest accepted revision per segment. Equal or
lower revisions are rejected; revision numbers may skip. A final segment rejects
every subsequent update, including higher revisions, so each final enters
history once. The browser applies the same gate to live and recovered captions,
replacing one row per segment and distinguishing provisional text from finals.

Finals receive contiguous internal sequence numbers in a shared log retained
for 90 seconds after server acceptance, measured with a monotonic clock. Expiry
reclaims segment state; a retirement watermark rejects obsolete IDs. Unfinished
state is capped at 4,000 segments. Memory therefore depends on caption rate and
size within retention, rather than total lecture duration.

`CLIENT_HELLO` supplies the latest finalized `(last_segment, last_revision)` and
a history window capped at 60 seconds; a late join uses `(0, 0)`. Recovery selects
only finals newer than that tuple and inside the requested server-acceptance
window, oldest first. Registration and history snapshot capture occur without
an intervening `await`, recording the final-log tail at the subscription boundary.
All `HISTORY` batches precede live delivery. Each targets at most 64 captions
and 256 KiB including JSON framing; an oversized caption travels alone and must
fit the peer's message limit. Empty context still produces one empty `HISTORY`.

After successful history transmission, the sender starts at the captured tail.
Arrivals during transmission enter live delivery; older unrequested finals are
excluded even for a zero-second window. Existing unfinished captions are not
snapshotted: a newcomer sees them on their next update. Connected replay can
retrieve outstanding finals throughout the 90-second retention window. Its
cursor advances after successful sends, without application acknowledgements;
a reconnect creates a fresh delivery cursor from the requested recent context.

The browser retains finals for 150 seconds by `source_ts`, with an independent
80-row display limit and 400-segment unfinished-state cap. Local recall offers
15/30/60 seconds, defaulting to 30, without network requests. Reconnect preserves
in-memory state, retries from 500 ms with exponential backoff capped at five
seconds, and requests 60 seconds after the finalized cursor. Reload becomes a
late join. Timestamp-based recall requires aligned source and browser clocks.

## 3. Backpressure and overload policy

Normal delivery preserves accepted revisions in FIFO order. Reaching 256 pending
updates or exceeding the 4-MiB byte budget triggers compaction and replay on the
same connection. Queued finals are then read from the shared log after the
committed cursor; each unfinished segment retains only its latest revision,
preserving its original queue age. A new final removes its queued provisional
state. Final admission failures first attempt replay; active state must still
fit within 4,000 entries and 4 MiB of pending JSON. One in-flight message and
transport buffers are outside these limits.

```text
NORMAL (every revision FIFO) --queue pressure or late probe--> REPLAY
       ^                                          shared finals + latest drafts
       +--no owed final/ready draft; lease expired-------------+
Either mode --capacity / retention / terminal stall failure--> CLOSE (1013)
```

Replay sends individual `CAPTION_UPDATE` messages; `HISTORY` is reserved for
hello recovery. After four consecutive finals, an eligible provisional update
gets a turn. Provisional state becomes eligible after one second. This trades
intermediate revision coverage for bounded queues and priority for finals.

One probe runs at a time, with 250 ms between completed probes. Its deadline
covers ping initiation and pong receipt, including blocked writes. A delay above
`max(0.5 s, 6 × smoothed RTT)` immediately enters replay and starts a three-second
catch-up lease, extended while the probe remains late. Failed returns to normal
delivery double the lease up to 30 seconds; a sustained healthy trial resets it.
RTT samples are capped at the lag threshold and smoothed with weight 0.25.

During the lease, each burst waits for a newly completed probe and sends up to
64 updates or until bytes reach 256 KiB; one caption may cross the byte target.
Queue-triggered replay alone does not impose this pacing. When no owed final
or eligible provisional remains and the lease expires, FIFO delivery resumes.
Pongs and successful sends indicate transport progress, without proving browser
consumption; the library's separate heartbeat is disabled.

A send or complete probe stalled for 75 seconds, expired outstanding finals
(`ReplayExpired`), or exhausted active capacity closes only that receiver with
code 1013. Closing has a two-second deadline before transport abort. Disconnect
cleanup cancels and joins sender/probe tasks. Finite retention cannot guarantee
every final under sustained overload; reconnect recovers only requested context.

## 4. Public experiments and results

### 4.1 Reproducible setup

TODO: Record Ubuntu version, CPU/RAM, Python and dependency versions, implementation commit, server configuration, caption-source parameters, freshness threshold and exact commands/output paths. Run the server and simulated clients in the same Ubuntu environment for the required networking experiments.

Use the supplied harness without changing its measurement logic. Run each required test at least three independent times and report the median. Follow [PUBLIC_TESTS.md](PUBLIC_TESTS.md) for commands; record the exact `tc/netem` settings for T5 and reset the impairment afterwards.

| Test | Required scenario | Configuration / evidence path |
|---|---|---|
| T1 | Basic correctness, 20 clients, normal local network | TODO |
| T2 | Public scale, 100 clients, normal local network | TODO |
| T3 | 100 clients, 10% slow readers | TODO |
| T4 | 100 clients, 20% temporarily disconnected | TODO |
| T5 | Ubuntu `tc/netem` delay, jitter and loss | TODO |

### 4.2 Measured results

Caption Age is client receive time minus `source_ts`, excluding browser rendering;
report its P50/P95/P99. Final completeness is correctly received unique finals /
generated unique finals. Fresh Caption Ratio is the fraction of received updates
within threshold T (public default: 500 ms); Fresh Final Caption Ratio uses generated
finals as its denominator. Normal Update Coverage measures source revisions
received during unconstrained operation. Recovery time runs from successful
reconnect until all required recent recovery state has arrived.

| Test / client group | P50 age (ms) | P95 age (ms) | P99 age (ms) | Final completeness (%) |
|---|---:|---:|---:|---:|
| T1 | TODO | TODO | TODO | TODO |
| T2 | TODO | TODO | TODO | TODO |
| T3 normal | TODO | TODO | TODO | TODO |
| T3 slow | TODO | TODO | TODO | TODO |
| T4 | TODO | TODO | TODO | TODO |
| T5 | TODO | TODO | TODO | TODO |

| Test / client group | Fresh Caption Ratio (%) | Fresh Final Caption Ratio (%) | Normal Update Coverage (%) |
|---|---:|---:|---:|
| T1 | TODO | TODO | TODO |
| T2 | TODO | TODO | TODO |
| T3 normal | TODO | TODO | TODO |
| T3 slow | TODO | TODO | TODO |
| T4 | TODO | TODO | TODO |
| T5 | TODO | TODO | TODO |


TODO: Interpret the results: correctness under T1, scaling under T2, normal-versus-slow isolation under T3, recovery under T4, and TCP retransmission/head-of-line effects on tail latency under T5. Explain unexpected observations and any failed runs.

## 5. Before/after optimization comparison

TODO: Select at least one public stress test and compare the final design with an earlier/basic configuration or a major optimization disabled. Identify both configurations and hold workload, environment and measurement settings constant. Use at least three independent runs per configuration and report medians.

| Metric (include unit) | Baseline / optimization disabled | Final / optimization enabled | Change |
|---|---:|---:|---:|
| TODO: primary performance metric | TODO | TODO | TODO |
| TODO: final completeness | TODO | TODO | TODO |
| TODO: coverage or resource metric | TODO | TODO | TODO |

TODO: Explain the mechanism behind the measured difference and its cost. Quantify the improvement or regression without sacrificing correctness or concealing reduced coverage.

## 6. Discussion, limitations and future work

TODO: Discuss failures and engineering trade-offs, including any tuning that did not help. Relate claims to the measurements and distinguish observed behavior from untested expectations.

TODO: State known limits: recovery horizon, sustained overload, tested population/workloads, environment differences, and any incomplete application behavior. Do not claim hidden-workload performance from public tests alone.

TODO: Give a short, concrete future-work/FYP direction and explain how it builds on the results.

## References

- CS3103 Assignment 4 specification, Sections 8-12 (experiments, metrics, environment, report requirements).
- TODO: Cite external technical sources actually used.

## Appendices

TODO: Put exact commands, per-run outputs, detailed configurations, and any code
walkthrough here or link to repository evidence. Keep the required design,
aggregate results, comparison, and analysis in the main 4-5 pages.

## Draft completion checklist (remove before export)

The assignment specifies **4-5 main pages, excluding references/appendices**;
it does not prescribe font size or margins. Use a readable export (for example,
A4, 11 pt text, 20 mm margins) and verify the rendered PDF after completing all
TODOs. Do not add a separate cover page. The following budgets are drafting
targets, not assignment rules; they include space for headings and tables.

| Main section | Page allowance | Prose target |
|---|---:|---:|
| 1. Architecture and protocol | 0.90 | 350 words |
| 2. Revision and recovery | 0.65 | 300 words |
| 3. Backpressure and overload | 0.65 | 300 words |
| 4. Experiments and results | 1.50 | 550 words plus the compact results tables |
| 5. Optimization comparison | 0.55 | 225 words plus one comparison table |
| 6. Discussion and future work | 0.50 | 175 words |
| Total | 4.75 | About 1,900 words plus tables |

Replace TODO instructions with final prose and measurements within these budgets.
Keep architecture concise; a method-by-method walkthrough belongs in an appendix.
Leave the remaining 0.25 page for the title, team line, and layout variation.
An A4/11 pt/20 mm-margin layout projection expanded to these prose targets, with
the existing tables, rendered to five main pages. This is a planning check;
verify again after replacing placeholders and adding any figures. Replace a
table with a figure where useful instead of continually adding material.

- [ ] All required sections are supported by the submitted implementation and evidence; no TODOs remain.
- [ ] Sections 2-3 explain actual rules, numeric bounds, and failure/recovery limits.
- [ ] Results cover all required metrics, recovery time, isolation, and bounded resource usage.
- [ ] The separate deterministic browser demo covers revision replacement, local recall, reconnect, and late join.
- [ ] T1–T5 each have at least three independent runs; medians and units are clear.
- [ ] At least one stress-test before/after comparison is quantified and reproducible.
- [ ] The main report is 4–5 pages after PDF export, excluding references/appendices.
- [ ] The reviewed PDF is saved as `submission_root/report.pdf`.
- [ ] README, AI usage, contributions and the separate 5–7-minute demo video are completed.
