# LiveRecall Technical Report

Team: TODO | Members: TODO | Implementation commit: TODO

## 1. Architecture and protocol

LiveRecall distributes a changing caption stream from one source to many
browsers over WebSockets. The backend server decides which revisions are valid and
manages delivery separately for each receiver according to its backlog and
connection progress.

```text
source --> Source --> Server --> CaptionStore (latest state + final log)
                         |                    |
                    accepted updates     replay finals
                         v                    v
                    Client: buffer + delivery cursor --> sender --> browser
```

`CaptionStore` is the shared authority for revision acceptance and recent finals.
Each `Client` owns an `OutboundBuffer`, one sender, a final-log cursor, and a
probe task. Accepting and enqueueing an update never waits for a receiver's
socket, so a blocked sender does not directly block delivery to other clients.
Broadcast still requires work proportional to the number of clients.

`SOURCE_HELLO` and `CLIENT_HELLO` establish the connection role. Sources send
`CAPTION_UPDATE` messages; receivers get `HISTORY` for initial recovery or connection followed
by live `CAPTION_UPDATE` messages. Malformed source updates are ignored before
state changes or broadcast, and `SOURCE_DONE` is not forwarded. Only one source
is active at a time. The implementation assumes the supplied source's increasing
segment and finalization order. State survives source disconnection in one shared
namespace; starting a new lecture requires a server restart and browser reload.

## 2. Revision and recovery semantics

**Revision replacement.** A segment is one caption whose text may change through
several provisional revisions. While unfinished, it accepts only a higher
revision; equal or lower revisions are duplicates or stale state. Once final,
it rejects every further update, including higher revisions.

Revision numbers may skip values. The browser applies this same gate to live
and recovered captions, rewriting one row per segment and distinguishing drafts
from finals visually. Repeated messages therefore cannot overwrite newer held
state or append duplicate final rows.

**Incremental recovery.** `CLIENT_HELLO` reports the latest finalized
`(last_segment, last_revision)` already held by the browser. A provisional
revision does not advance this marker: its final may still be missing. The
server returns only finals beyond the marker and within the requested recent
window, capped at 60 seconds. A late join uses `(0, 0)`. Ordering these finals
by acceptance gives oldest-first recovery under the sequential source. This
marker assumes ordered completion; it cannot represent holes below a later final.

Registration and snapshot capture happen without yielding, recording both the
requested history and the current end of the final log. All `HISTORY` batches
are sent before live delivery starts at that boundary. Finals accepted during
history transmission lie after the boundary and remain owed. Batches target
64 captions and 256 KiB; an oversized caption travels alone and must fit the
peer's message limit. Existing unfinished captions are absent from the snapshot
and appear only on their next update.

**Recovery limits and local recall.** Finals enter a shared log once, with
internal sequence numbers marking delivery progress. They are retained for
90 seconds after server acceptance and pruned during store operations. Connected
replay can use that full window, whereas reconnect creates a fresh cursor after
its requested context: older excluded finals are not replayed later. Retired
segment IDs are rejected to prevent stale state returning after pruning. Server
unfinished state is capped at 4,000 segments by retiring the lowest IDs.

The browser preserves state across automatic reconnects and requests 60 seconds
after its finalized marker; reloading instead becomes a late join. Its local
15/30/60-second recall, defaulting to 30, filters already-held finals without a
server request. On accepted updates it prunes finals older than 150 seconds and
caps unfinished state at 400 segments. The 80-row live display limit does not
limit recall storage.

## 3. Backpressure and overload policy

**Normal delivery preserves every accepted revision.** Each receiver has a FIFO,
so a healthy browser sees drafts develop into their final text. Keeping every
revision indefinitely when a receiver falls behind would instead accumulate
obsolete text, memory use, and delivery delay. That receiver switches to replay
on the same connection, while healthy receivers keep their own delivery policy.

```text
NORMAL -- queue pressure / late probe --> REPLAY
   ^                                       |
   +--------- caught up; lease expired -----+
capacity / retention / stall failure --> close affected receiver (1013)
```

**Replay preserves useful state.** Reaching 256 pending updates or exceeding
4 MiB triggers compaction and replay. Queued finals leave the local buffer and
are read in order from the shared log after the receiver's committed cursor.
Each unfinished segment keeps only its newest queued revision, preserving its
original queue age; a final removes its queued draft. Thus pending drafts r1,
r2, and r3 for one segment collapse to r3, and none remains queued once the
final arrives. Intermediate drafts are sacrificed, while outstanding finals
remain available within retention. Replay sends individual `CAPTION_UPDATE`
messages; `HISTORY` is reserved for hello recovery.

Finals normally take priority. After at least four consecutive finals, a draft
queued for one second gets a turn; eligible drafts can also be sent when no
final remains. This grace period allows rapidly superseded drafts to disappear
while giving longer-lived unfinished captions an opportunity to progress. The
final cursor advances only after a successful send, so selecting a final does
not prematurely mark it as sent.

**Lag controls pacing and return to normal.** One probe measures ping initiation
and pong receipt, including blocked writes. A delay beyond
`max(0.5 s, 6 × smoothed round-trip time)` activates replay and a three-second
catch-up lease. This keeps delivery in replay even if the backlog drains, and
is extended while the probe remains late. During the lease, each burst waits
for a newly completed probe, then sends up to 64 updates or until bytes reach
256 KiB; one caption may cross the byte target. Queue pressure alone activates
replay without this pacing. Neither pongs nor successful sends prove browser
acceptance or rendering.

When no outstanding final or eligible draft remains and the lease expires,
coalescing stops and FIFO resumes, including any younger queued drafts. If lag
returns soon after this normal trial, the next lease doubles, up to 30 seconds.
After a sufficiently long normal trial, the next lag event resets it to three
seconds. This discourages repeated overload but can delay restoration of full
revision coverage after the receiver improves.

**Finite capacity has an explicit failure policy.** Normal admission failure
first attempts replay. Remaining unfinished work must fit within 4,000 entries
and 4 MiB of pending JSON. Exceeding that capacity, losing an outstanding final
to 90-second retention (`ReplayExpired`), or stalling an individual send or
complete probe for 75 seconds initiates a 1013 close for that receiver. Closing
gets two seconds before transport abort. Queue bounds exclude in-flight work,
recovery snapshots, and transport buffers; shared-log memory depends on caption
rate and size within retention. Sustained overload can therefore prevent final
completeness, and reconnect restores only the requested recent context.

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
the public analyser computes its percentiles over accepted live updates. Final
completeness counts correctly received unique finals, including recovered history,
against expected finals across clients. The following table reproduces the
supplied T1–T5 results summary. Matching per-run evidence and confirmation of the
run count and median aggregation remain to be attached.

| Test / client group | P50 age (ms) | P95 age (ms) | P99 age (ms) | Final completeness (%) |
|---|---:|---:|---:|---:|
| T1 | 3.88 | 6.78 | 8.95 | 100.00 |
| T2 | 15.84 | 26.19 | 37.51 | 100.00 |
| T3 overall | 15.56 | 2,303.55 | 7,874.86 | 99.41 |
| T3 normal readers | 15.31 | 25.29 | 38.77 | 100.00 |
| T3 slow readers | 6,316.66 | 8,573.00 | 8,879.90 | 94.08 |
| T4 | 13.02 | 24.41 | 33.92 | 100.00 |
| T5 | 261.29 | 677.31 | 825.90 | 99.98 |

Fresh Caption Ratio is the fraction of accepted live updates within threshold T
(public default: 500 ms). Fresh Final Caption Ratio counts unique finals received
within T against generated finals, including finals recovered through history.
Normal Update Coverage measures source revisions received during unconstrained
operation. These ratios cannot be reconstructed from three age percentiles and
completeness alone; their values remain to be obtained from the matching runs.

| Test / client group | Fresh Caption Ratio (%) | Fresh Final Caption Ratio (%) | Normal Update Coverage (%) |
|---|---:|---:|---:|
| T1 | TODO | TODO | TODO |
| T2 | TODO | TODO | TODO |
| T3 overall | TODO | TODO | TODO |
| T3 normal readers | TODO | TODO | TODO |
| T3 slow readers | TODO | TODO | TODO |
| T4 | TODO | TODO | TODO |
| T5 | TODO | TODO | TODO |

TODO: Add T4 recovery time, measured from successful reconnect until the required
recent recovery state is received, plus duplicate/stale counts and resource-use
evidence. Record the harness's HISTORY-response timing separately if it does not
measure arrival of the complete recovery state.

### 4.3 Interpretation

**T1–T2: normal delivery and scale.** T1 achieves 100% final completeness with
P99 age of 8.95 ms. At the required 100-client scale in T2, P99 increases to
37.51 ms while completeness remains 100%. This supports timely final delivery
under the tested normal workload. Full provisional revision coverage and absence
of duplicate/stale updates still require their corresponding counters.

**T3: isolation succeeds, but slow-reader delivery remains limited.** Normal
readers retain 100% completeness, with P95/P99 ages of 25.29/38.77 ms, close to
T2's 26.19/37.51 ms. This supports isolation from slow receivers. Slow readers,
however, have median age of 6.32 seconds, P95 age of 8.57 seconds, and only
94.08% final completeness. The overall 99.41% completeness hides this group-level
shortfall. Coalescing can reduce obsolete draft traffic, but 5.92% of expected
finals remain unaccounted for in the slow-reader measurement. These results do
not establish timely or complete catch-up for that group. Per-client logs are
needed to distinguish finals still delayed at the measurement deadline from
other delivery failures.

**T4: final completeness survives temporary disconnection.** The reconnect
scenario reports 100% final completeness and P99 live-update age of 33.92 ms.
This supports recovery of missed finals within the tested run. The live-update
percentiles exclude recovered history, so 33.92 ms is not a recovery-time
measurement. Recovery timing and duplicate/stale counters are still needed to
assess how quickly and cleanly clients converge after reconnecting.

**T5: impairment increases delay and exposes a small completeness shortfall.**
P50 rises to 261.29 ms and P95/P99 to 677.31/825.90 ms, exceeding the 500-ms
freshness threshold in the tail. This is consistent with added network delay
and jitter, together with TCP retransmission and ordered delivery delaying later
data after packet loss. The summary cannot separate these contributions.
Completeness is 99.98%, leaving a 0.02-percentage-point shortfall to investigate
using the matching logs and observation deadline. Exact impairment settings and
freshness ratios are required to make the result reproducible and quantify its
effect on useful caption delivery.

## 5. Before/after optimization comparison

TODO: Select at least one public stress test and compare the final design with an earlier/basic configuration or a major optimization disabled. Identify both configurations and hold workload, environment and measurement settings constant. Use at least three independent runs per configuration and report medians.

| Metric (include unit) | Baseline / optimization disabled | Final / optimization enabled | Change |
|---|---:|---:|---:|
| TODO: primary performance metric | TODO | TODO | TODO |
| TODO: final completeness | TODO | TODO | TODO |
| TODO: coverage or resource metric | TODO | TODO | TODO |

TODO: Explain the mechanism behind the measured difference and its cost. Quantify the improvement or regression without sacrificing correctness or concealing reduced coverage.

## 6. Discussion, limitations and future work

**Failures and trade-offs.** T3 normal readers achieve 100% final completeness
and 25.29-ms P95 age, while slow readers reach only 94.08% completeness and
8.57-second P95 age. Isolation therefore succeeds under this workload, but
slow-reader delivery remains delayed and incomplete. T5 also exceeds the
500-ms freshness threshold at P95 and falls slightly short of complete final
delivery. Coalescing reduces obsolete draft traffic at the cost of intermediate
revision coverage. Longer catch-up leases discourage repeated overload but can
postpone the return to full revisions after a receiver improves. Burst pacing
and lease timing are possible contributors to delay; matching traces are needed
to establish why finals remained unreceived at the measurement deadline.

**Limitations.** Lag detection measures transport responsiveness without
measuring the receiver's caption-processing rate. Healthy pongs can therefore
coexist with an application backlog, and fixed bursts may supply updates faster
than the receiver consumes them. Coalescing affects only the server's pending
buffer: obsolete drafts already handed to socket or receiver buffers cannot be
withdrawn and can delay newer finals. Recovery after a bottleneck consequently
depends on how much data was sent before replay began. Memory use also remains
workload-dependent: the shared final log has an age limit but no byte cap, so
caption bursts or large texts can increase memory consumption despite the
per-client queue limits.

**Future work.** Our objective is 100% final completeness in T3 and a faster
return to full revision delivery after a temporary slowdown, within the existing
queue bounds. We will first trace unreceived finals, then compare burst limits
and catch-up lease policies in repeated, matched T3 and slowdown/recovery tests.
Completeness and caption age will be compared at the same observation deadline.
After restoring normal reading speed, we will measure the time until all newly
generated revisions are received consistently. Normal-client latency, peak queue
usage, and process memory will be monitored to check that improved slow-client
delivery preserves isolation and bounded resource use.

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
