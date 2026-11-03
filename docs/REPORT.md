# LiveRecall Technical Report

Team and members: not supplied. Evaluation: 2 November 2026 (Singapore).
Server and runner implementation: `6eaced4`; verification began from `0c4532c`
with the startup/ablation repairs described below. The evidence records source hashes.

## 1. Architecture and protocol

LiveRecall distributes the deterministic caption source to WebSocket clients.
`solution_server.py` preserves the required `--port` interface and starts an
asyncio server on `0.0.0.0:8765`. `Server` coordinates one authoritative
`CaptionStore`, one active source, and independent client connections:

```text
caption_source.py -> Source -> Server -> CaptionStore
                                |
                         per-client OutboundBuffer
                                |
                         independent sender -> client
```

The first JSON object chooses `SOURCE_HELLO` or `CLIENT_HELLO`; malformed JSON,
unknown roles, and an invalid source hello are closed with code 1008. A missing
hello times out after five seconds. Client hello fields are lenient: invalid or
missing values receive defaults, and requested history is capped at 60 seconds.
Caption validation requires positive integer segment/revision IDs, a finite
nonnegative timestamp, Boolean final status, string text, and a nonempty session
matching the producer's session. Unknown source messages and malformed updates
are ignored; `SOURCE_DONE` is test control and is not broadcast.

Ingestion and fan-out contain no socket send or `await`. Each client owns its
sender, so one stalled socket does not directly block other clients. Fan-out
still costs work proportional to client count. Immutable update objects cache
their serialized JSON. The browser replaces one row per segment and applies
live and history updates through the same revision gate. Starting a different
lecture requires restarting the server and reloading browsers: the store is
shared rather than partitioned by session.

## 2. Revision and recovery semantics

The store accepts strictly increasing revisions and rejects duplicates, stale
updates, and every revision after a final. Finalization appends the winning
caption to history once. Final history expires 90 seconds after server acceptance,
using a monotonic clock; recovery returns only finals within the requested
0–60 seconds. Expiry also runs on recovery requests after the source stops.
Unfinished state is capped at 4,000 segments. A retirement watermark rejects
reclaimed IDs, relying on the supplied producer's increasing segment order.

The client sends its latest finalized `(segment, revision)` cursor. History
contains strictly greater tuples in acceptance order. Registration and the
history snapshot are synchronous; the initial `HISTORY` is sent before the
sender starts. Updates arriving during that send enter the live buffer, closing
the snapshot/subscription gap. Recovery outside the requested window is
unavailable, and a cursor cannot represent arbitrary holes behind its maximum.

The browser retains finals for 150 seconds by source timestamp, independently
of its 80-row display limit, and caps unfinished state at 400 segments. Recall
filters local finals into 15/30/60-second windows. Reconnect delay doubles from
500 ms to five seconds, preserving in-memory revision state and requesting
60 seconds of history. Reload becomes a late join. Browser recall and measured
caption age assume aligned source/client wall clocks.

## 3. Backpressure and overload policy

Pending state is keyed by segment, coalescing superseded provisional revisions.
A soft threshold of 256 pending segments activates catch-up delivery.
Hard limits are 4,096 pending finals, 4,000 active segments, and 4 MiB of serialized
pending state per client. Exceeding a limit refuses new work and closes with
1013; existing pending entries remain intact. The in-flight message, object
overhead, and WebSocket/TCP buffers lie outside this byte count.

A probe task keeps one ping outstanding, with 250 ms between completed probes.
Prompt pongs update smoothed RTT with weight 0.25. A pong overdue by
`max(0.5 s, 6 × RTT)` activates three seconds of catch-up. Sending then waits for
a completed probe or at most 0.5 seconds. Catch-up or queue pressure batches
pending finals into `HISTORY` frames, capped at 64 items and 256 KiB. Pending
finals remain eligible even after the recovery horizon. A provisional-only
backlog normally waits one second for replacement, with an active-update
opportunity after four final batches. Ordinary FIFO delivery resumes afterwards.

A pong overdue by `max(1.5 s, 18 × RTT)` closes the client for history resync.
Pong delay is an indirect consumption signal and may also reflect network
latency. Dequeueing and successful `send()` do not acknowledge application
receipt. These mechanisms bound application backlog and preserve recent
recovery state; they do not guarantee complete delivery during sustained overload.

## 4. Experiments and results

### 4.1 Setup and verification

All new measurements used Ubuntu 22.04.5 under WSL2, kernel
`5.15.167.4-microsoft-standard-WSL2`, an Intel Core i7-1255U, 12 exposed logical
CPUs, and 7,795 MiB RAM. Python was 3.10.12 and `websockets` 16.1.1. Server,
source, and simulated clients ran on the same host over loopback. Browser
rendering was not measured. Freshness threshold was 500 ms; client role seed
was 3103. The source started after one second and emitted three revisions per
segment with 0.20-second revision spacing and 0.35-second inter-segment gaps.

The initial checkout failed before listening: `server.py` imported missing
`ABLATIONS` from `client.py`. Restoring that configuration and its probe, grace,
and resync branches repaired startup and the advertised comparison arms.
Eight regression tests and all six H10 protocol checks then passed. The H10
checks cover extra fields, `SOURCE_DONE`, malformed JSON/unknown roles,
duplicates, stale/post-final updates, and finalized-cursor recovery; they do
not require strict rejection of malformed client-hello fields.

T1–T4 and the unimpaired T5 control each received three independent runs with
a fresh server. Their durations were 20/40/60/60/40 seconds and client counts
20/100/100/100/100. T3 used 10% readers sleeping 500 ms per caption frame; T4
disconnected 20% at 15 seconds for five seconds. Generated finals per run were
22/49/76/76/49. The supplied analyser's measurement logic was unchanged.
Values below are medians of the three per-run statistics, not pooled percentiles.

### 4.2 Public measurements

| Test / group | P50 ms | P95 ms | P99 ms | Complete % | Fresh % | Fresh final % | Coverage % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| T1 | 1.90 | 3.93 | 4.62 | 100.00 | 100.00 | 100.00 | 100.00 |
| T2 | 5.33 | 8.97 | 13.17 | 100.00 | 100.00 | 100.00 | 100.00 |
| T3 | 5.48 | 43.13 | 6950.62 | 99.87 | 95.20 | 90.39 | 94.78 |
| T3 normal | 5.33 | 11.43 | 20.99 | 100.00 | 100.00 | 100.00 | 100.00 |
| T3 slow | 4245.58 | 8268.07 | 8592.06 | 98.68 | 4.77 | 3.95 | 47.81 |
| T4 | 5.27 | 8.14 | 10.58 | 100.00 | 100.00 | 98.42 | 98.33 |
| T5 control | 5.14 | 7.46 | 11.61 | 100.00 | 100.00 | 100.00 | 100.00 |

Caption age is receive time minus the individual revision's `source_ts`.
Completeness counts unique finals delivered live or through history against
generated finals for all clients. Fresh update ratio uses accepted live updates;
fresh final ratio uses all generated finals, including missed finals in its
denominator. Coverage is unique live revisions/generated revisions; recovered
history does not restore this metric. T3 role metrics were derived from the same
CSV rows and denominators, including a separate P99 and completeness per group.

T1 and T2 delivered every generated final and revision with zero connection errors.
Increasing from 20 to 100 clients raised P95 from 3.93 to 8.97 ms, well below 500 ms on
this host. T3 normal readers retained 100% coverage and freshness with P95 11.43 ms;
slow readers had P95 8268.07 ms, 3.95% fresh finals, and 98.68% completeness. The three
T3 runs missed 10, 0, 10 of 7,600 expected client-finals each by the deadline. Thus
normal-reader isolation was observed, while slow-reader freshness and complete deadline
delivery were not achieved. Median T3 connection errors were 30 (normal/slow: 0/30); the
load client also counts server-triggered resync closures as errors, and quiet public
logs do not identify each closure's cause. All public runs recorded zero duplicates,
stale deliveries, and non-final history items.

T4 recovered all finals with first reconnect-HISTORY P95 4.32 ms; all 20 reconnect
responses per run contained history. Its fresh final ratio was 98.42% because
outage-delayed finals still count against the 500 ms deadline. Median per-run peak
server RSS ranged from 24.49 to 32.27 MiB across public scenarios; T3 used 32.27 MiB and
4.39% of one CPU on average.

**T5 impairment is unverified.** Outside the sandbox, `sudo -n tc qdisc show dev lo`
failed with “a password is required.” The runner explicitly skipped all five
impaired public profiles. The required delay/loss profile would apply
`netem delay 120ms 30ms distribution normal loss 3%` on `lo`, then reset the qdisc.
The control above is an ordinary local run. No delay/loss tail ratio or empirical
TCP retransmission conclusion can be drawn from it.

### 4.3 Additional experiment coverage

The fresh quick run produced 65 scenario/arm medians, including reused final arms, and
explicitly skipped 22 arms across 14 scenarios requiring tc. Checks recorded 252 PASS,
10 FAIL, 0 ERROR, and 31 SKIP; counts are individual checks, not scenario pass rates.
Failed checks affected `H2_s10_m1000`, `H2_s10_m500`, `H2_s30_m1000`, `H2_s30_m250`,
`H2_s30_m500`, `H2_s50_m1000`, `H2_s50_m500`, `H9`. Failures concerned slow-reader
delivery, relative isolation, and the short H9 memory slope. Exact checks and values are
listed in the appendix.

The catalog named `hidden` contains our own engineering stress tests, not the
evaluator's hidden tests. Quick mode normally uses 20 seconds and at most
50 clients, with scale cases at 100/200/200 and H9 at 40 seconds; it also shortens
disconnect, join, and impairment timing. In particular, a quick “70-second”
disconnect does not test a real 70-second outage. These are workflow checks;
they do not establish 1,000-client performance, five-minute memory stability,
or the full recovery-horizon boundary. Missing observations remain unknown.

## 5. Before/after comparison

The full T4 workload compared `experiments/naive_server.py` with the final design,
using three independent runs each on the same host. Both used 100 clients,
60 seconds, 20% disconnected at 15 seconds for five seconds, the same source,
and the supplied analyser. The naive server directly broadcasts updates and
returns empty history; the final design retains and serves missed finals.

| Metric | Naive baseline | Final design | Final − baseline |
| --- | --- | --- | --- |
| Final completeness (%) | 98.17 | 100.00 | +1.83 pp |
| Fresh finals (%) | 98.17 | 98.42 | +0.25 pp |
| Live revision coverage (%) | 98.33 | 98.33 | +0.00 pp |
| Caption age P95 (ms) | 27.87 | 8.14 | -19.73 ms |
| First reconnect HISTORY P95 (ms) | 11.86 | 4.32 | -7.54 ms |
| Recovered history items/run | 0.00 | 120.00 | +120.00 items |
| Peak server RSS (MiB) | 31.05 | 32.10 | +1.04 MiB |

Completeness improved by 1.83 percentage points, with 120 missed finals returned per run
instead of zero. The naive server's 11.86 ms history response was empty for every
reconnect; response speed alone therefore does not establish recovery. Fresh finals and
live coverage cannot count outage-delayed history as fresh/live; their values should be
read alongside completeness. The final server's median peak RSS changed by +1.04 MiB.

The final design pays for revision state, history, per-client buffers, probes,
and independent senders. This comparison measures the combined architecture;
it does not isolate any individual optimization. The quick suite additionally
executes `no-probe`, `no-grace`, `no-resync`, and `no-all` arms. Those single,
short runs support exploratory comparisons only. Disabling probes also removes
the probe-triggered grace and resync path, so these factors are not independent.

For example, quick BA_T3 final completeness was 100.00%, compared with 95.45% without
probes and 96.36% without resync. The full 60-second T3 nevertheless reached only
99.87%; the quick run alone would have hidden its deadline-delivery gap.

## 6. Discussion, limitations and future work

The implementation now starts and the local experiment workflows execute, but
correct protocol handling alone does not establish freshness or completeness
for slow readers. Reported live latency excludes history items, which can make
coalescing appear faster while reducing observed live coverage. `HISTORY`
batching spends one simulated read delay per frame rather than per item; this
models frame-limited readers and is not a model of per-caption rendering cost.

The supplied analyser times every later `HISTORY` as a recovery response, so
catch-up batches on reconnecting/late clients can contaminate that statistic.
The full T4 comparison has no slow-reader role and its first reconnect history
was also checked separately. Process RSS is sampled every 0.5 seconds and
is neither a total system memory bound nor proof of long-term stability.
The separate browser demonstration and PDF layout were not exercised by these
network experiments. T5 and the full stress matrix remain outstanding.

Future work should add application acknowledgements for finalized state and
separate initial recovery from later catch-up batches in telemetry, then repeat
the full slow-reader/soak matrix and same-runner netem control comparisons.
That would test whether resync closes real delivery gaps and whether the
RTT-based lag thresholds remain useful under actual network delay.

## References

- Assignment experiment requirements reproduced in [PUBLIC_TESTS.md](PUBLIC_TESTS.md).
- Experiment definitions and execution details: [experiments/README.md](../experiments/README.md),
  [scenarios.py](../experiments/scenarios.py), and [run_matrix.py](../experiments/run_matrix.py).
- Measurement definitions: [analyse_results.py](../test/analyse_results.py)
  and [load_clients.py](../test/load_clients.py). No external sources were used
  to substitute for measurements.

## Appendix: reproduction and evidence

Run from the repository root. Local WebSocket tests need permission to open
loopback sockets; this execution required leaving the tool's socket-restricted
sandbox. Each output directory is independent of the older mixed results in
`output/experiments`.

```bash
./venv/bin/python test/submission_check.py
./venv/bin/python -m unittest discover -s experiments -p 'test_regressions.py'
./venv/bin/python experiments/run_matrix.py --suite public --no-tc --runs 3 \
  --out output/verification-20261102/public
./venv/bin/python experiments/run_matrix.py --suite before_after --only BA_T4 \
  --arms naive --no-tc --runs 3 --out output/verification-20261102/comparison
./venv/bin/python experiments/run_matrix.py --suite all --no-tc --quick --runs 1 \
  --out output/verification-20261102/smoke
./venv/bin/python experiments/report.py --out output/verification-20261102/public
./venv/bin/python experiments/report.py --out output/verification-20261102/comparison
./venv/bin/python experiments/report.py --out output/verification-20261102/smoke
```

Per-run CSV, JSON, logs, and sampled RSS are in
[`output/verification-20261102`](../output/verification-20261102).
The checked-in [measurement archive](experiment-results-20261102.json) preserves
the environment, commands, implementation hashes, full-run summaries, quick
medians, verdicts, and supplementary role/recovery calculations because raw
`output/` files are ignored by Git. New runner output records effective settings
and an implementation digest; reuse requires matching settings and run counts.
Unlogged counters remain null, and rejected updates and final batches are
reported using the current implementation's counter names.

### Quick-run checks that failed

| Scenario | Failed engineering target | Value | Target |
| --- | --- | --- | --- |
| H2_s10_m500 | slow_clients.fresh_final_pct >= 50 | 0.00 | 50.00 |
| H2_s10_m1000 | final_completeness_pct >= 99 | 92.73 | 99.00 |
| H2_s30_m250 | normal_clients.p95_ms <= ref:H_base.normal_clients.p95_ms*1.5 | 23.27 | 16.82 |
| H2_s30_m500 | normal_clients.p95_ms <= ref:H_base.normal_clients.p95_ms*1.5 | 18.42 | 16.82 |
| H2_s30_m500 | slow_clients.fresh_final_pct >= 50 | 0.00 | 50.00 |
| H2_s30_m1000 | final_completeness_pct >= 99 | 78.18 | 99.00 |
| H2_s50_m500 | slow_clients.fresh_final_pct >= 50 | 0.00 | 50.00 |
| H2_s50_m1000 | normal_clients.p95_ms <= ref:H_base.normal_clients.p95_ms*1.5 | 27.58 | 16.82 |
| H2_s50_m1000 | final_completeness_pct >= 99 | 63.64 | 99.00 |
| H9 | rss_slope_mb_per_min <= 1 | 2.77 | 1.00 |

The team/member identity has not been supplied. Final submission still needs
the impaired-network evidence, review of the rendered 4–5-page main report,
`submission_root/report.pdf`, submission documents, and the separate demo video.
