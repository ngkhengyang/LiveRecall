# OBSERVE.md — LiveRecall server fixes (2026-11-02)

Manager: Claude (Opus 5.5). Worker: Codex CLI `gpt-6.1-sol`, reasoning effort `medium`.
Scope: `submission_root/solution_server.py` and the `submission_root/app/` package it launches. The test harness, experiments and client are untouched.

## 0. Setup observations

- **Codex invocation.** The WSL `codex` shim is broken (`Missing optional dependency @openai/codex-linux-x64`). The Windows install works through `cmd.exe /c codex …` (codex-cli 0.166.0). Codex therefore edits the repo through `C:\Users\games\Desktop\LiveRecall`, while all tests run in WSL with `./venv/bin/python`.
- **tc / sudo.** Passwordless sudo for `tc` works. However, this WSL2 kernel (`5.15.167.4-microsoft-standard-WSL2`) has neither `netem` nor `tbf`: `tc qdisc add dev lo root netem …` fails with "Specified qdisc kind is unknown", and `modprobe sch_netem` reports the module is not found. All tc scenarios (T5, H3, H7, H8_bandwidth, BA_bandwidth) therefore cannot run on this machine and remain SKIP. They need a stock Ubuntu VM (see `docs/UBUNTU_SETUP.md`, Multipass).
- **Starting point.** The run in `docs/experiment-results-20261102.json` (quick, 1 run) has 10 FAILs, all in Challenge 1 (H2 slow-reader) and H9 (RSS slope). The public T1–T4 checks all pass.

## 1. How the slow-reader test actually behaves (facts the fix depends on)

These come from reading the harness, not from changing it:

- `test/load_clients.py` sleeps `slow_ms` after **each** `CAPTION_UPDATE` frame, and once after each **non-empty** `HISTORY` frame regardless of how many items it carries. Unknown message types cost no sleep. A HISTORY item is scored as `recovered` and counts toward final completeness and fresh-final (`test/analyse_results.py:66-136`). So one batched frame can deliver many finals for the price of one read.
- The client uses websockets 16.1.1 defaults: `max_queue=16`, with a low watermark of 4. The library pauses transport reads only above 16 queued frames, and one `data_received` chunk can enqueue far more than 16. Until then pings are answered immediately, even while the app is asleep. The server's only consumption signal, a delayed pong, therefore appears only once there is already a backlog of 16+ frames, and that backlog grows with whatever the server keeps sending.
- The source stops about 2 s before the clients do (`test/run_demo.py:55-58`). Any frames still queued at a slow client when the run ends are never read, so a deep backlog at the end shows up directly as lost finals (the completeness FAILs).
- The source finalizes a segment roughly every 0.75 s with 3 revisions, about 4 frames/s per client at full fidelity. A 500 ms reader consumes 2 frames/s and a 1000 ms reader 1 frame/s.

## 2. Baseline (full durations, no tc)

Command: `run_matrix --suite hidden --only H_base,H2,H9 --no-tc --runs 1 --out output/observe-baseline`. Result: 124 PASS, 6 FAIL.

| Scenario | Check | Baseline | Threshold | Verdict |
|---|---|---|---|---|
| H2_s10_m500 | slow_clients.fresh_final_pct | 3.95 % | ≥ 50 | **FAIL** |
| H2_s30_m500 | slow_clients.fresh_final_pct | 2.15 % | ≥ 50 | **FAIL** |
| H2_s50_m500 | slow_clients.fresh_final_pct | 2.63 % | ≥ 50 | **FAIL** |
| H2_s30_m1000 | final_completeness_pct | 98.42 % | ≥ 99 | **FAIL** |
| H2_s50_m1000 | final_completeness_pct | 96.71 % | ≥ 99 | **FAIL** |
| H9 (300 s, 30 % slow) | rss_slope_mb_per_min | 2.74 (peak 48.4 MB) | ≤ 1 | **FAIL** |
| H2_s10_m1000 | final_completeness_pct | 99.47 % | ≥ 99 | PASS (marginal) |
| H2_* (all) | normal_clients.p95_ms vs 1.5×H_base (13.3 ms) | 7.5–9.3 ms | ≤ 13.3 | PASS |

**Confirmed:** 6 failures.
- At 500 ms, slow readers almost never get a final within 500 ms.
- At 1000 ms, finals are lost when 30–50 % of readers are slow.
- H9 memory grows by about 2.7 MB/min over 300 s.

**Dismissed:** the 3 quick-run `normal_clients.p95_ms` FAILs do not reproduce at full duration (all 7.5–9.3 ms, against a limit of 13.3 ms). Slow-client isolation for normal clients already holds.


### H9 diagnosis (manager, before any change)
- `output/observe-baseline/H9/final_run01.rss.csv` shows RSS climbing from 30.9 MB to 48.1 MB over about 170 s, then staying **flat** at 48.1–48.4 MB until 300 s. Memory is bounded; the failure is a warm-up ramp.
- `experiments/run_matrix.py:147-160` fits the slope over the last 75 % of samples (t ≥ 75 s). The 75–170 s ramp therefore lands inside the fit and reads as 2.74 MB/min.
- The server log shows **480 forced resyncs in 300 s** for 30 slow clients: each slow reader is closed and reconnected about 16 times. That connection churn, plus the backlogs it creates, is the most likely driver of the ramp.

## 3. Iteration 1 — measured-consumption pacing (Codex)

**Brief:** `output/codex/brief1.md`. It covers the harness facts from §1, the root cause, the suggested pacing design, the ablation names to keep, and the regression-test API.
**Codex invocation note:** the first attempt failed because `cmd.exe` split the quoted prompt ("unrecognized subcommand 'the'"). The fix was to pipe the prompt on stdin (`codex exec … -`).

### What was changed
| File | Change (before → after) | Why |
|---|---|---|
| `app/client.py` `_hold` | Before: waited for a probe **or** `MAX_HOLD_SECONDS=0.5`, then sent anyway. After: no send at all while a pong is overdue; otherwise wait until `max(next paced slot, estimated unread frames / measured rate)`. `MAX_HOLD_SECONDS` removed. | The 0.5 s timeout sent ≈2 frames/s, exactly a 500 ms reader's consumption rate, so the ≥16-frame backlog built before detection never drained. Every final then sat behind seconds of stale frames (fresh-final ≈3 %). |
| `app/client.py` `_probe_loop` | New: counts frames sent (`_sent_frames`) and records the count when each ping goes out. Each overdue pong yields a sample of frames acknowledged / elapsed time. These feed an EWMA consumption rate (`RATE_SMOOTHING=0.25`), a pacing interval of `PACING_MARGIN=1.1` / rate, and an unread-frame estimate (`DRAIN_MARGIN=0.5` × stall reserved for the unknown queue tail). A renewed overdue pong multiplies the interval by `PACING_BACKOFF=1.25`. After `RECOVERY_AFTER_SECONDS=30` of prompt pongs the interval shrinks by `RECOVERY_FACTOR=0.9`, until the client returns to normal mode. | Pong delay is the only consumption signal the client exposes, so rate and backlog are derived from it. Nothing is hard-coded to the source rate or slow_ms (PDF §5: do not assume hidden parameters). |
| `app/client.py` `_catchup` | Now also true while a pong is overdue or pacing is active, not only in the 3 s sticky window. | Paced clients must keep using final batches and provisional grace for as long as they are paced. |
| `app/client.py` `_next_delivery` | Every catch-up slot first takes **all** pending finals as one HISTORY batch. The "4 final batches then 1 active" rotation (`MAX_FINAL_BATCHES_BEFORE_ACTIVE`) was removed. | A HISTORY frame costs the slow reader one sleep however many finals it carries, so each slot should deliver every pending final (PDF #11). The newest provisional stays coalesced in the buffer. |
| `app/client.py` resync | `RESYNC_AFTER_SECONDS` 1.5 → 30, and the threshold also scales with 18 × pace interval. | Closing a reader that is slow but still draining lost finals still in flight, and caused the H9 reconnect churn. Resync is now a last resort for a reader that has stopped entirely. The `resync` ablation still disables it. |
| `app/protocol.py` | Added `CaptionUpdate.wire_size`, a cached `len(to_json())`. | `OutboundBuffer` re-measured each update's size on every offer and take, for every client. |
| `app/outbound_buffer.py` | Uses `wire_size` instead of `len(update.to_json())`. | Saves CPU on the shared event loop. |

### Manager review
- Invariants checked by reading the diff: HISTORY is still the first frame (sent before the sender task exists); finals leave the buffer exactly once (`_take` pops them); per-segment coalescing and the hard limits are unchanged; normal clients never enter `_hold` because `_catchup` stays false with no overdue pong and no pacing.
- `experiments/test_regressions.py`: 8/8 OK. H10 protocol invariants: PASS.

### Iteration 1 result (`output/observe-iter1`, H_base + H2 + H9, full durations)
**130 PASS / 0 FAIL.**

| Scenario | Check | Baseline → Iter 1 |
|---|---|---|
| H2_s10/s30/s50_m500 | slow fresh-final % | 3.95 / 2.15 / 2.63 → **69.7 / 69.7 / 69.7** |
| H2_s10/s30/s50_m1000 | final completeness % | 99.47 / 98.42 / 96.71 → **100 / 100 / 100** |
| H2_*_m1000 | slow fresh-final % (no target) | ≈2.6 → ≈22 |
| H2_* | normal-client p95 | 7.5–9.3 ms → 7.3–9.4 ms (unchanged, isolation preserved) |
| H9 | RSS slope | 2.74 → **0.0 MB/min** (flat at 31.1 MB from 15 s; was ramping to 48 MB) |
| H9 | forced resyncs | 480 → **0** |
| H9 | final completeness % | 99.77 → 99.24 (still PASS, but lower: see below) |

Removing the forced resyncs fixed H9 memory too. The ramp came from connection churn, not from history growth, and needed no separate change. This confirms the diagnosis above.

**Regression found by manager analysis:** H9 completeness dropped. All 30 slow clients missed the last ~10 finals (segments 387–396). Bucketing slow-client final latency by time:
- 0–20 s: 4.4 s (initial backlog before detection, unavoidable);
- 20–270 s: 6–8 ms;
- 280–300 s: 1.2 s.

**Cause:** Codex's recovery rule shrank the pace interval by 0.9× every 30 s of prompt pongs. After about 8 steps (≈270 s) it fell below 0.25 s and the client was dropped back to unpaced full-fidelity mode. The flood rebuilt a backlog that the run's end then cut off. Prompt pongs are not proof of capacity, because the client library absorbs 16+ frames before delaying a pong. While paced, the sender was demand-limited, so the shrinking interval was never actually exercised before the jump. Iteration 1 is **kept** as the base; the final gate was stopped and iteration 2 targets recovery.

## 4. Iteration 2 — evidence-based recovery (Codex)
**Brief:** `output/codex/brief2.md`. Shrink the interval only when it was binding. Raise offered load gradually through a paced phase with provisionals allowed, before any return to unpaced mode. On an overdue pong, revert to the last good interval and back off exponentially (30 → 60 → 120 s …).

### What was changed (iteration 2)
| File | Change | Why |
|---|---|---|
| `app/client.py` | Recovery became a 3-step state machine. (1) Pace at the same interval with provisional grace off. (2) Shrink the interval ×0.9 only if ≥25 % of ≥32 sends actually waited on the slot. (3) Return to unpaced mode only when nothing was waited on or coalesced and the interval is shorter than the offered inter-arrival. An overdue pong restores the last good interval and doubles the probe cooldown (30 s → … → 480 s cap). New constants: `RECOVERY_MAX_SECONDS=480`, `RECOVERY_MIN_SENDS=32`, `RECOVERY_BINDING_SHARE=0.25`, `RECOVERY_WAIT_SHARE=0.25`. | To stop iteration 1's blind jump back to full fidelity. |

### Iteration 2 result (`output/observe-iter2`): **REJECTED**, code restored to iteration 1
- 132 PASS / 0 FAIL, and H9 completeness back to 100 %. **But** slow-client median final latency per 20 s in H9 became 7 → 210 → 317 → 263 → 404 → 957 → 2818 → 5306 → 9 → 7 → 6 → 1232 → 3717 → 5943 ms, against iteration 1's steady 6–8 ms.
- H2 m500 slow fresh-final fell from 69.7 % to 63.6–66.6 %.
- **Why it failed:** every probe above the reader's real capacity (e.g. 0.55 s → 0.495 s → 0.446 s for a 2 frames/s reader) builds a backlog slowly. The client library absorbs 16+ frames before delaying a single pong, so detection comes only after seconds of staleness. With the only signal this client exposes, capacity probing cannot be made cheap. Iteration 2's H9 completeness pass was down to when the probe happened to fall, not to the design.

## 5. Iteration 3 — no automatic recovery (Codex)
**Brief:** `output/codex/brief3.md`. Manager reset `client.py` to the iteration-1 version (snapshot `output/codex/app_iter1`), then Codex removed only the recovery branch.

### What was changed
| File | Change (before → after) | Why |
|---|---|---|
| `app/client.py` `_probe_loop` | Deleted the `elif self._pace_interval and now >= self._recover_at` branch (×0.9 every 30 s, unpaced below 0.25 s). | This was the cause of iteration 1's tail-final loss in H9. |
| `app/client.py` constants/fields | Removed `RECOVERY_AFTER_SECONDS`, `RECOVERY_FACTOR`, `_recover_at`. Added a rationale comment. | Dead after the deletion. |
| Behaviour | A paced connection stays paced for its lifetime. Rate and interval still update on every new overdue-pong sample (EWMA, ×1.25 backoff). A reconnect starts a fresh, unpaced `Client`. | A paced reader still gets every final immediately, as a HISTORY batch in the next slot. Only provisionals are coalesced behind the 1 s grace, which PDF #11 permits. |

Manager review: the diff is a pure deletion plus a comment. Regression tests 8/8.

### Iteration 3 result: full final gate (`output/observe-final-public`, `output/observe-final`, `output/observe-final-ba`)
_(These output directories were later overwritten by the final gate on iteration 6, section 9; the iteration-3 numbers below are as recorded at the time.)_
- `make check`: PASS. Regressions: 8/8.
- Public T1–T4 ×3: **9 PASS**. The 5 SKIPs are T5 tc profiles (no netem/tbf in this kernel).
- Hidden (all, no tc): **247 PASS, 1 FAIL**. The non-tc SKIPs are metrics that don't apply to their scenario: H4_p100 has no "unaffected normal" clients, H4_d70's 70 s disconnect outlasts the 60 s recovery window (see §9), and H10/H11 are edge/aggregate modes. They are not failures.
- **New FAIL: `H8_slow` `tail_update_coverage_pct` = 70.0 (threshold ≥ 95).** H8_slow makes 30 % of readers slow from 10 s to 30 s, then fast again. The metric counts live CAPTION_UPDATE revisions from the last 10 s. Under iteration 3 the recovered readers stay paced forever and receive finals only as HISTORY batches, so they add 0 → exactly 70 %. PDF §4.3 ("under normal conditions, clients should receive the full sequence of caption revisions") means a recovered reader **must** return to full fidelity. Removing recovery outright was therefore wrong; what's needed is a cheap, *correct* recovery signal.

## 6. Iteration 4 — pad-frame consumption probe (Codex)
**Brief:** `output/codex/brief4.md`.

**Key insight (manager):** pings alone can't tell an app that is asleep from one that is consuming, because the client library keeps answering pings until its 16-frame queue is full. Padding can force that condition on purpose. Right after a real paced frame, send 64 tiny frames of an ignored type and then a ping:
- If the app is asleep processing the real frame, the pads overflow the library queue, reading pauses, and the pong waits until the app wakes. The pong delay then roughly equals the per-frame processing time.
- If the app is consuming promptly, it pops the pads instantly and the pong returns in milliseconds.
- Unknown types cost the test client no sleep (`test/load_clients.py`), and the browser client ignores them (no `else` branch in its message handler), so the probe adds no staleness.

### What was changed
| File | Change | Why |
|---|---|---|
| `app/protocol.py` | New `Pad` message `{"type":"PAD"}` (protocol extension; clients ignore it). | Probe filler. The PDF allows protocol extensions that keep the required messages intact. |
| `app/client.py` `_consumption_probe` (new) | For **paced clients only**, at most every `CONSUMPTION_PROBE_INTERVAL=2 s` and right after a real paced frame: send `PROBE_PAD_FRAMES=64` pads, then a ping. Utilisation is computed as probe delay × offered update rate. Below `RECOVERY_UTILISATION=0.5` on `RECOVERY_CONFIRMATIONS=2` consecutive probes, pacing is cleared and the client returns to full fidelity. An overdue pong or a probe timeout resets the confirmations. | Detects a recovered reader directly without ever offering it more load than it has shown it can take. Probes are only ~1 KB each and never run for normal clients. |
| `app/client.py` `enqueue` | Keeps an EWMA of the offered update rate (`OFFERED_RATE_WINDOW_SECONDS=2`). | Utilisation needs the full-fidelity offered load, not the paced send rate. |
| `app/client.py` | The iteration-3 "no automatic recovery" comment was replaced with the new rationale. | |

Manager review: the probe runs only when `_pace_interval` is set and no pong is overdue. The `probe` ablation disables it. The pad count is a named constant whose comment explains the dependency on the library's queue high watermark. If a client has no such queue, the probe returns a prompt pong; the client then exits pacing and, if it is really slow, the existing overdue detection re-paces it. One cost: the paced sender awaits the probe pong (≤ 0.5 s) every 2 s. That is the same order as its pacing interval, and finals queued meanwhile simply join the next HISTORY batch.

### Iteration 4 result (`output/observe-iter4`, H8_slow + H_base + H2 + H9 + H10): **partially REJECTED**
- **H8_slow: now PASS** (tail_update_coverage ≥ 95, converge 0.37 s). Recovered readers return to full fidelity, so the recovery idea works.
- **Regression:** 135 PASS, 5 FAIL. H2 m500 slow fresh-final 69.7 → 39.6 / 48.8 / 49.7 % (FAIL). H2 m1000 completeness 100 → 98.8 / 98.0 % (FAIL). H9 slow-client median final latency ~500 ms for the whole run (was 7 ms).
- **Cause (verified by the manager in a standalone experiment, `output/codex/padprobe.py`):** a websockets 16 reader sleeping 500 ms per CAPTION_UPDATE received 1 real frame + 64 pads + ping:

  | gap between pads and ping | pong delay |
  |---|---|
  | 0 ms | **2 ms** (false "fast") |
  | 20 ms | 506 ms |
  | 50 ms | 503 ms |

  Pads and ping written back-to-back arrive in one TCP read. The library parses every frame in a single `data_received()` before `pause_reading` takes effect, so the ping is answered while the app sleeps. Steadily slow readers were therefore repeatedly declared "recovered", flooded, and re-paced.
- Secondary issue: the sender awaited the probe pong inline (≤ 0.5 s), which held back finals.

## 7. Iteration 5 — probe gap + non-blocking probe (Codex)
**Brief:** `output/codex/brief5.md`.

### What was changed
| File | Change (before → after) | Why |
|---|---|---|
| `app/client.py` `_consumption_probe` | Pads are still sent inline right after the real frame. The ping now goes out only after a gap of `max(PROBE_PING_GAP_SECONDS=0.02, PROBE_PING_GAP_RTT_MULTIPLIER=4 × RTT)`. | The gap lets the pads be parsed and reading pause before the ping arrives. This is the measured fix for the false "fast" verdicts. |
| `app/client.py` `_finish_consumption_probe` (new) | The gap, ping, timeout and verdict moved into one tracked background task (at most one outstanding, cancelled on close). The probe is abandoned if another real frame was sent during the gap. | Finals are no longer held up behind a probe pong, and a measurement disturbed by a new frame is discarded. |
| `RECOVERY_CONFIRMATIONS` | 2 → 3 | More conservative against any residual false positive. |
| `run()` cleanup | Iterates a snapshot of `_tasks`. | Probe tasks remove themselves from `_tasks` when done. |

### Iteration 5 result (`output/observe-iter5`): 136 PASS, 4 FAIL, **kept as base but not final**
- Fixed: H2 m1000 completeness 99.2–99.8 % (PASS); H2 m500 slow fresh-final 50.5 / 54.1 / 55.3 % (PASS, but thin). H8_slow still PASS: during the slow phase (10–30 s) slow-client latency peaks at 6.6 s, then drops to 12–16 ms once readers recover.
- No more false recoveries: slow readers in H9 receive 0 provisional frames after minute 1.
- **New FAILs:** H2_s50_* normal-client p95 = 16–24 ms (threshold 13.8; iteration 3: 7–9 ms).
- H9 slow-client median final latency still climbs 45 → ~540 ms. Slow clients receive ≈1,700 HISTORY frames/min, against 2,400 in iteration 3.
- **Diagnosis (manager):** (a) The pads deliberately pause the reader, so a regular backlog ping sent after them is delayed about P ≈ `_behind_threshold()`. It is then counted as overdue, which blocks all sends, applies ×1.25 backoff every probe, and skews the rate. Fewer frames means more batching and more latency. (b) Probe cost: 50 slow clients × 64 pads / 2 s ≈ 1,600 extra sends/s on the shared event loop. That explains the normal-client p95 regression.

## 8. Iteration 6 — fence the probes, cheaper probe (Codex)
**Brief:** `output/codex/brief6.md`.

### What was changed
| File | Change (before → after) | Why |
|---|---|---|
| `app/client.py` `_probe_loop` | Waits on `_consumption_probe_done` before each ordinary ping, and resets its throughput anchor after a consumption probe. A consumption probe starts only when no ordinary ping is in flight (`_backlog_probe_done`). | Our own pads' pause must never be read as an overdue reader. This fixes the spurious backoff and send blocking. |
| `_finish_consumption_probe` | A "slow" verdict refreshes `_rate` toward 1 / (measured per-frame delay) via the existing EWMA, and sets `_pace_interval = PACING_MARGIN / rate`. | The probe directly measures processing time P, which is better evidence than delayed-ACK throughput. |
| `PROBE_PAD_FRAMES` | 64 → 24 | Still above the websockets default high watermark of 16, at ~⅓ of the cost. |
| Probe interval | Fixed 2 s → doubles per slow verdict up to `CONSUMPTION_PROBE_MAX_INTERVAL=4 s`; resets to 2 s on a fast verdict. | Halves probe cost for steadily slow readers while keeping H8_slow recovery within ~12 s. |

**Codex hygiene issue:** Codex (running on Windows) rewrote `client.py` with CRLF line endings, unlike the LF used everywhere else in the package. The manager normalised the file back to LF with `sed -i 's/\r$//'`.

### Iteration 6 result (`output/observe-iter6`): **140 PASS / 0 FAIL — ACCEPTED**
(The first attempt was interrupted when the session ended and was re-run from scratch.)

| Check | Iter 3 | Iter 5 | **Iter 6** |
|---|---|---|---|
| H2 m500 slow fresh-final % (s10/s30/s50) | 69.7 / 69.7 / 69.7 | 50.5 / 54.1 / 55.3 | **69.7 / 69.7 / 68.4** |
| H2 m1000 completeness % | 100 / 100 / 100 | 99.8 / 99.6 / 99.2 | **100 / 100 / 100** |
| H8_slow tail_update_coverage ≥ 95 | **FAIL (70)** | PASS | **PASS** |
| H9 slow-client median final latency (20–300 s) | 6–8 ms | 45 → 540 ms | **16–18 ms, flat** |
| H9 slow HISTORY frames/min | 2,400 | ≈1,700 | **2,370–2,400** |
| H9 RSS slope / completeness | 0.09 / 100 % | 0.0 / 99.93 % | **0.03 / 99.89 %** |

Fencing the probes restored iteration 3's slow-reader performance, and H8_slow recovery was kept.

**Normal-client p95 check (manager re-test, `output/observe-iter6-p95`, 3 runs):** H_base 22.5 / 21.6 / 22.4 ms; H2_s50_m250 18.6 / 16.9 / 17.4 ms; H2_s50_m500 21.6 / 22.8 / 21.7 ms.
- H_base never exercises pacing or probes (no slow clients) and runs the same code path in every iteration. Its rise from ≈9 ms in earlier runs to ≈22 ms after the session restart is therefore environmental (host state), not caused by the changes. WSL load average at the time was 0.3; Windows-side load is not visible from WSL.
- With 50 % slow readers, normal clients see the **same** p95 as with none, so slow-client isolation (PDF #10) holds. The check is relative to H_base within the same run, which is what it is designed to measure.

## 9. Final gate (iteration 6 code)

Commands (WSL, no tc): `make check`; `experiments/test_protocol_edge.py`; `run_matrix --suite public --no-tc --runs 3 --out output/observe-final-public`; `--suite hidden --no-tc --runs 1 --out output/observe-final`; `--suite before_after --no-tc --runs 1 --out output/observe-final-ba`. Regression tests (`experiments/test_regressions.py`): 8/8.

| Gate | PASS | FAIL | SKIP | Notes |
|---|---|---|---|---|
| `make check` | ✓ | – | – | Connection + CLIENT_HELLO smoke check. |
| Protocol edge (`duplicate`, `stale_and_post_final`, `resume`) | 3 | 0 | 0 | |
| Public T1–T4, 3 runs | 9 | 0 | 5 | All 5 SKIPs are T5 tc profiles. |
| Hidden suite, all scenarios | **248** | **0** | 22 | 11 are tc scenarios; 11 are metrics that don't apply (below). |
| Before/after | 0 | 0 | 6 | All BA scenarios need tc. |

**Non-tc SKIPs are by design, not hidden failures:**
- `H4_*_d70` completeness is marked `informational`. The client is offline 70 s, but the resume contract only covers 60 s of history (`CaptionStore.compute_history`), so ~10 s of finals can't be recovered.
  - I re-ran H4_p50_d70 and H4_p100_d70 on the **unmodified HEAD** code in a separate worktree. It gave 80.51 % / 61.03 %; the final code gives 80.54 % / 60.70 %. The changes have no effect on this.
- `H4_p100_*` `normal_unaffected_p95_ms`: every client disconnects, so the set of unaffected clients is empty.
- `H10`: a protocol-edge scenario with no load metrics.

### Before → after for the originally failing checks (full durations)

| Scenario | Check | Baseline | **Final** | Threshold |
|---|---|---|---|---|
| H2_s10_m500 | slow fresh_final_pct | 3.95 % | **69.74 %** | ≥ 50 |
| H2_s30_m500 | slow fresh_final_pct | 2.15 % | **69.69 %** | ≥ 50 |
| H2_s50_m500 | slow fresh_final_pct | 2.63 % | **69.71 %** | ≥ 50 |
| H2_s30_m1000 | final_completeness_pct | 98.42 % | **100 %** | ≥ 99 |
| H2_s50_m1000 | final_completeness_pct | 96.71 % | **100 %** | ≥ 99 |
| H2_s10_m1000 | final_completeness_pct | 99.47 % | **100 %** | ≥ 99 |
| H9 (300 s) | rss_slope_mb_per_min | 2.74 | **0.0** | ≤ 1 |
| H9 (300 s) | final_completeness_pct | 99.77 % | **100 %** | ≥ 99 |
| H8_slow | tail_update_coverage_pct | 100 (baseline) / 70 (iter 3) | **100** | ≥ 95 |
| H2_* | normal p95 vs 1.5 × H_base | ≤ 9.3 ms vs 13.3 ms | **13.9–24.8 ms vs 38.8 ms** | ≤ 1.5 × H_base |

In this run H_base p95 was ≈ 25.9 ms, the same environmental shift noted in §8. Every H2 variant, including 50 % slow readers, is at or below H_base itself.

## 10. Final implementation and why it was selected

All behavioural changes are in `submission_root/app/client.py` (`Client`), plus two small supporting changes:
- `protocol.py`: `CaptionUpdate.wire_size` (cached serialised length) and a `Pad` frame.
- `outbound_buffer.py`: uses `wire_size`.

`solution_server.py`, `CaptionStore` and `OutboundBuffer`'s coalescing and bounds are unchanged.

### 10.1 How it works

1. **Normal clients: the unchanged zero-wait path.**
   - Every revision is sent as soon as it arrives. `OutboundBuffer` coalesces only what is still unsent.
   - A backlog ping runs every `PROBE_INTERVAL_SECONDS` (0.25 s); that is the only extra work.
2. **Detection.**
   - A pong is overdue after `_behind_threshold()` = max(0.5 s, 6 × RTT EWMA).
   - That sets `_pong_overdue` and starts catch-up (sticky for 3 s).
   - The reader's consumption rate is sampled as frames acknowledged ÷ elapsed time (EWMA 0.25), and the pacing interval backs off ×1.25.
3. **Measured-consumption pacing (`_hold`, `_sender_loop`).**
   - While a pong is overdue, **nothing** is sent. Pending state keeps coalescing in the bounded per-segment `OutboundBuffer`.
   - After that, the server sends at most one frame per `PACING_MARGIN / rate` (1.1× the measured per-frame time), and only once the estimated unread frames (`unread / rate`) have drained.
4. **Maximum value per frame (`_next_delivery`).**
   - In catch-up, each slot carries **all pending finals as one `HISTORY` batch** (oldest first, existing byte/item caps).
   - Otherwise the slot carries the newest revision of the active segment, with the existing 1 s provisional grace so a provisional that is about to be finalised is superseded rather than sent.
   - Finals are never dropped. Only obsolete provisionals are coalesced (§4.3).
5. **Recovery: the pad-frame consumption probe (`_consumption_probe`, `_finish_consumption_probe`).**
   - Paced clients only, at most every 2 s (backing off to 4 s).
   - Right after a real paced frame, the server sends 24 tiny `{"type":"PAD"}` frames. Clients ignore unknown types at zero cost.
   - It then waits a gap of max(20 ms, 4 × RTT) and pings.
   - If the app is asleep on the real frame, the pads push the client library's receive queue past its high watermark (16), reading stops, and the pong is delayed by about the app's per-frame processing time P. If the app is consuming, the pong returns in about one RTT.
   - Utilisation is P × the offered update rate (EWMA over a 2 s window). Three consecutive probes below 0.5 return the connection to unpaced, full-fidelity mode.
   - A "slow" verdict refreshes the rate estimate from 1/P.
6. **Fencing.**
   - The backlog ping and the consumption probe never overlap (`_backlog_probe_done` / `_consumption_probe_done`), so the pause caused by our own pads is never read as an overdue reader.
   - A probe is abandoned if another real frame is sent during its gap.
7. **Resync close is the last resort.**
   - A connection is closed with 1013 only after no progress for max(30 s, 18 × RTT).
   - The `resync` ablation still disables it.
   - The ablation names `probe`, `grace`, `resync` and `all` are unchanged.
8. **Bounds.**
   - Per-connection state is a fixed set of scalars and EWMAs. There are no growing lists.
   - At most one probe task per connection is tracked in `_tasks` and removed when it finishes.
   - The per-client `OutboundBuffer` keeps its existing hard limits.

### 10.2 Why each choice was selected (evidence)

| Choice | Alternatives tried / considered | Evidence | PDF requirement |
|---|---|---|---|
| Stop sending while a pong is overdue, then pace at the measured rate | Original `_hold()` with a 0.5 s timeout (baseline) | The baseline timeout sent ≈2 frames/s, exactly a 500 ms reader's capacity, so the 16+-frame backlog formed before detection never drained. Fresh finals: 2–4 % → 69.7 %. | #10, #11 |
| Batch all pending finals into one HISTORY frame per slot | One frame per final | The reader pays one sleep per frame regardless of items, so batching delivers every final within the reader's capacity. m1000 completeness: 96.7–99.5 % → 100 %. | #11, §4.3 (finals durable) |
| Resync only after ≥ 30 s without progress | 1.5 s resync (baseline) | The baseline did 480 forced resyncs in 300 s in H9. Each one lost in-flight finals and churned memory. H9 RSS slope: 2.74 → 0.0–0.09 MB/min. | #8, #10 |
| Pad-frame consumption probe for recovery | (a) ×0.9 time decay (iter 1), (b) gradual rate probing (iter 2), (c) no recovery (iter 3) | (a) Flooded readers at ~270 s and lost the last ~10 finals per slow client (H9 completeness 99.24 %). (b) Rejected: any load above capacity silently fills the 16-frame client queue first. (c) H8_slow coverage 70 % (recovered readers never regain full fidelity). The probe measures consumption directly without overloading the reader: H8_slow coverage 100 %, and steady slow readers in H9 get 0 false recoveries. | §4.3 ("normal conditions → full sequence"), #10 |
| Probe gap, fencing, 24 pads, 2→4 s backoff | 64 pads, no gap, unfenced (iters 4–5) | Without a gap, pads and ping arrived in one TCP read, giving false "fast" verdicts (confirmed by the `padprobe.py` experiment). Without fencing, our own pause showed up as overdue pongs, which caused spurious backoff and H9 slow latency rising 45 → 540 ms. 64 pads × 50 clients cost the shared loop ≈1,600 sends/s and inflated normal-client p95. Iteration 6 fixed all three, with 140/0 on the targeted set. | #10 |
| Cached `wire_size` | Re-serialising per `offer`/`_take` | Removes repeated `to_json()` across 100+ clients per update on the shared event loop; zero semantic change. | #10 (CPU isolation) |

**Trade-offs accepted:**
- A paced reader receives coalesced provisionals rather than every revision. §4.3 allows this under overload.
- Recovery takes ≥ 3 probes (≈ 6–12 s) after a reader speeds up. H8_slow shows that is well within the 10 s tail window.
- The probe signal depends on the client library having a bounded receive queue (websockets default 16; browsers apply TCP backpressure). If a client has no such queue, pongs come back promptly, the connection may return to unpaced mode, and overdue-pong detection then simply re-paces it. That costs a few seconds of backlog, not lost finals.
- A slow reader costs about 24 small frames every 2–4 s, and only while it is paced.

## 11. Observations about managing Codex

- **Invocation:** only the Windows install worked. Quoted prompts passed through `cmd.exe` were split ("unrecognized subcommand 'the'"); piping the brief on stdin (`codex exec … -`) fixed it. Long briefs were kept in `output/codex/brief*.md`.
- **Codex was good at:** turning a precise design brief into consistent, well-commented code that kept the ablation names, private names used by `test_regressions.py`, and invariants.
- **Codex needed the manager for:**
  - Diagnosis from measurements. Every root cause (hold timeout ≈ reader capacity, the 16-frame queue hiding backlog, decay flooding at 270 s, the pad/ping single-read false positive, self-inflicted overdue pongs) was found by the manager from metrics, logs and harness source. Codex never ran the WSL harness.
  - Rejecting plausible but wrong designs (iterations 2 and 3).
- **Hygiene:**
  - Codex rewrote `client.py` with CRLF line endings once; the manager normalised them.
  - A manager mistake is also worth recording: `pkill -f solution_server.py` matched the manager's own shell and killed it. Server processes should be stopped by PID, not by pattern.

## 12. Caveats

- **tc scenarios not verified here.** The WSL2 kernel has no `sch_netem`/`tbf`, so T5, H3, H7, H8_bandwidth and BA_* SKIP. They need a stock Ubuntu VM (`docs/UBUNTU_SETUP.md`).
- **Absolute p95 shifted with host state.** H_base p95 went from ≈9 ms to ≈22–26 ms across sessions on identical code paths. The isolation check is relative to H_base in the same run, and it passes with margin.
- **1 run per hidden scenario.** H2 m500 fresh-final sits at ~70 % against a 50 % threshold, which leaves a reasonable margin. Before submission, the public suite was run 3 times.
