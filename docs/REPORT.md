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

T1-T4 use the Ubuntu rerun in `results/probe_ab_t1_t4_ubuntu_20261104/`;
T5 reuses the earlier same-day results in `results/probe_ab_t5_ubuntu_20261104/`.
Both sets ran on 4 November 2026 in Ubuntu 24.04.5 LTS, Linux 6.8.0-147-generic
ARM64, with two vCPUs, approximately 4 GiB RAM, Python 3.12.3 and websockets
16.1.1. Application and public-harness hashes match implementation `81608f5`.
The source uses nominal 200-ms revision delays and 350-ms segment gaps; other
settings are seed 3103, a one-second source start delay, 30-second history,
500-ms freshness threshold and port 8811. No extra drain interval was added.

The supplied measurement logic was unchanged. Each test used three independent
runs per configuration, restarting the server each time and reporting the median
of per-run metrics. **Section 4 uses with-probe results only; Section 5 compares
with and without probes.** Server, source and simulated clients ran in the same
Ubuntu VM. T1-T4 used `noqueue` loopback; T5 used 120 ms delay, 30 ms
normal-distribution jitter and 3% loss, with a verified reset afterwards.

| Test | Required scenario | Configuration / evidence path |
|---|---|---|
| T1 | 20 clients, 20 s; 22 expected segments | `results/probe_ab_t1_t4_ubuntu_20261104/` |
| T2 | 100 clients, 40 s; 49 expected segments | Same T1-T4 directory |
| T3 | 100 clients, 60 s, 10% slow; 500 ms sleep/message; 76 expected segments | Same T1-T4 directory |
| T4 | 100 clients, 60 s, 20% disconnect at 15 s for 5 s; 76 expected segments | Same T1-T4 directory |
| T5 | 100 clients, 40 s; 49 expected segments; loopback netem | `results/probe_ab_t5_ubuntu_20261104/` |

### 4.2 Measured results

Caption Age is client receive time minus `source_ts`, excluding browser rendering;
the public analyser computes its percentiles over accepted live updates. Final
completeness counts correctly received unique finals, including recovered history,
against expected finals across clients. The following table gives **with-probe
three-run medians**. T3 group metrics are derived from the CSV role flags;
other aggregate values reproduce the saved JSON summaries.

| Test / client group | P50 age (ms) | P95 age (ms) | P99 age (ms) | Final completeness (%) |
|---|---:|---:|---:|---:|
| T1 | 3.95 | 6.57 | 8.29 | 100.00 |
| T2 | 13.84 | 21.87 | 28.60 | 91.84 |
| T3 overall | 13.08 | 2,787.66 | 8,241.15 | 90.13 |
| T3 normal readers | 12.75 | 21.01 | 30.85 | 90.79 |
| T3 slow readers | 6,515.51 | 8,876.66 | 9,169.73 | 84.21 |
| T4 | 12.20 | 19.53 | 25.05 | 89.47 |
| T5 (reused) | 255.26 | 702.30 | 1,057.88 | 100.00 |

Fresh Caption Ratio is the fraction of accepted live updates within threshold T
(public default: 500 ms). Fresh Final Caption Ratio counts unique finals received
within T against expected finals, including finals recovered through history.
The analyser's Normal Update Coverage includes all client roles; the table labels
this live revision coverage and separately shows the T3 reader groups.

| Test / client group | Fresh Caption Ratio (%) | Fresh Final Caption Ratio (%) | Live revision coverage (%) |
|---|---:|---:|---:|
| T1 | 100.00 | 100.00 | 100.00 |
| T2 | 100.00 | 91.84 | 91.84 |
| T3 overall | 94.48 | 81.71 | 86.58 |
| T3 normal readers | 100.00 | 90.79 | 90.79 |
| T3 slow readers | 1.80 | 0.00 | 48.68 |
| T4 | 100.00 | 88.16 | 87.98 |
| T5 (reused) | 88.98 | 87.12 | 98.53 |

All 15 selected with-probe runs recorded zero duplicate/stale captions, invalid
non-final HISTORY items and connection errors. T4's 20 forced-disconnect clients
reconnected and received HISTORY in every run. Median HISTORY-response P95 was
4.51 ms; this is not a separate measurement of complete-state recovery time.
No RSS/CPU or queue-high-water trace was collected.

### 4.3 Interpretation

**Source timing limits completeness interpretation.** The runner estimates
segment count from nominal 0.75 s/segment, while clients stop at a fixed deadline.
An observer reading existing logs measured source completion at 65.95-68.70 s
after client start in ten T3/T4 runs, beyond their 60-s windows. SOURCE_DONE
follows the final segment gap and socket close; exact final-emission times were
not captured. Finals absent from every client may have been generated after
clients stopped, while the analyser still counts them as expected. These are
unchanged public measurements, not proof that all missing finals were server
losses. Missing deliveries also do not enter live-age percentiles.

**T1-T2: normal delivery and scale.** T1 received every expected final and
revision, with P99 age 8.29 ms. T2's P99 was 28.60 ms and median completeness
91.84%. Its first run received 4,400/4,900 finals; finals 45-49 were absent from
all clients. Source timing must be resolved before attributing this shortfall
to the server's delivery policy.

**T3: normal readers stay responsive, but slow readers lag.** Normal-reader
P95/P99 was 21.01/30.85 ms, close to T2's 21.87/28.60 ms. Each normal reader
received finals 1-69 in every run, giving 90.79% completeness against 76 expected.
Slow readers received 640, 639 and 660 of 760 expected pairs, with median
completeness 84.21% and P95 age 8.88 s. In run 3, slow readers missed finals
67-69 that normal readers received; finals 70-76 were absent from everyone.
This identifies a slow-reader backlog separately from the source-timing limit.

**T4: reconnect works, but complete final delivery is not established.** Every
forced-disconnect client reconnected and received HISTORY, with median response
P95 4.51 ms. Each run received 6,800/7,600 finals; finals 69-76 were absent from
all clients. Live-update P99 was 25.05 ms. HISTORY-response timing and zero
duplicate/stale counts support reconnect behavior but do not establish complete
recovery by the deadline.

**T5: impairment increases age while all expected finals arrive.** The reused
runs each received 4,900/4,900 final/client pairs. P50 was 255.26 ms and P95/P99
702.30/1,057.88 ms; fresh-final ratio was 87.12% at 500 ms. This is consistent
with delay/jitter and TCP retransmission/ordered-delivery effects, without
isolating their contributions. Loopback netem affects both source and receiver
traffic. Live revision coverage was 98.53%.

## 5. Before/after optimization comparison

The final with-probe design is compared with a copied client whose probe task
is not started. Queue-pressure coalescing and the send timeout remain. Disabling
the probe removes lag detection, probe-driven wakeups/pacing and probe stall
detection together. Both configurations use the same public parameters and
three independent runs; changes below are with-probe minus no-probe, using
unrounded medians. T3 uses the fresh rerun and T5 the reused same-day evidence.

| Metric (include unit) | Baseline / probe disabled | Final / probe enabled | Change |
|---|---:|---:|---:|
| T3 final completeness (%) | 83.03 | 90.13 | +7.11 pp |
| T3 P99 age (ms) | 21,644.60 | 8,241.15 | -13,403.45 |
| T3 slow-reader P95 age (ms) | 24,919.67 | 8,876.66 | -16,043.01 |
| T3 normal-reader P95 age (ms) | 16.31 | 21.01 | +4.70 |
| T3 live revision coverage (%) | 83.77 | 86.58 | +2.81 pp |
| T5 final completeness (%) | 99.90 | 100.00 | +0.10 pp |
| T5 live revision coverage (%) | 99.82 | 98.53 | -1.29 pp |
| T5 P95 age (ms) | 714.75 | 702.30 | -12.46 |
| T5 P99 age (ms) | 1,040.24 | 1,057.88 | +17.64 |

With probes, T3 median slow-reader P95 was 64.4% lower and overall P99 61.9%
lower, while normal-reader P95 was 4.70 ms higher. Earlier lag detection and
coalescing can avoid sending obsolete drafts, consistent with reduced backlog.
All with-probe runs preceded no-probe runs, however, and source pacing differed:
one observed T3 with-probe source completed at 65.95 s, versus 68.51-68.70 s
without probes. Different work generated before the deadline confounds the
completeness/coverage comparison; +7.11 pp is an observed difference, not an
isolated server-delivery improvement.

T5 received 4,900 finals per with-probe run versus 4,900, 4,895 and 4,822 without
probes; missing finals were segment 49 at the deadline. The trade-off was 1.29 pp
lower live revision coverage. P99 rose slightly, latency ranges overlapped and
random loss sequences differed, so these runs do not establish a general latency
speedup. Eventual delivery beyond the observation deadline was not measured.

## 6. Discussion, limitations and future work

**Failures and trade-offs.** T3 normal-reader P95 was 21.01 ms, while slow
readers had 84.21% median completeness and 8.88-s P95 age. Normal readers stayed
responsive, but source completion beyond client deadlines confounds final
completeness; the separate slow-reader backlog remains unresolved. T5 received
all expected finals but exceeded the 500-ms freshness threshold at P95.
Coalescing reduces obsolete draft traffic at the cost of intermediate revision
coverage. Longer catch-up leases discourage repeated overload but can postpone
the return to full revisions after a receiver improves. Exact source-emission
and delivery traces are needed to distinguish late generation from pending
final delivery at the measurement deadline.

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
- [Fresh Ubuntu T1-T4 evidence](../results/probe_ab_t1_t4_ubuntu_20261104/SUMMARY.md) and [reused Ubuntu T5 evidence](../results/probe_ab_t5_ubuntu_20261104/SUMMARY.md), including each configuration and both arms' CSV/JSON files.
- TODO: Cite external technical sources actually used.

## Appendices

[Combined results and run caveats](../results/TEST_RESULTS_20261104.md) link to
per-run values, source timing observations, environment details and verification
of all 30 CSV/JSON pairs. The original evidence remains preserved.

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
