# Updated public test results — 4 November 2026

**One-core follow-up:** T2–T4 were subsequently rerun three times per configuration with the Ubuntu VM set to one virtual CPU. All 18 runs completed successfully; one core did not resolve source generation overrunning the client deadline. See the [one-core results and two-core comparison](probe_ab_t2_t4_1core_ubuntu_20261104/COMPARISON.md). The two-core results below remain the original evidence set.

Branch: `tests/refresh-t1-t4-ubuntu-20261104`. Application, test harness, and experiment sources match commit `81608f5`.

T1–T4 were rerun in Ubuntu with three independent runs per configuration (24 new runs). T5 reuses the six Ubuntu runs recorded earlier on 4 November 2026: every recorded application, browser, harness, experiment, Makefile, and network-profile SHA-256 hash matches this rerun and the current source. T5 was not rerun. Older evidence is preserved.

## Evidence

- [New T1–T4 full comparison](probe_ab_t1_t4_ubuntu_20261104/SUMMARY.md), [configuration and environment](probe_ab_t1_t4_ubuntu_20261104/configuration.json), and raw CSV/JSON/logs in each arm’s `output/` directory.
- [Existing T5 full comparison](probe_ab_t5_ubuntu_20261104/SUMMARY.md), [configuration](probe_ab_t5_ubuntu_20261104/configuration.json), raw CSV/JSON, and netem setup/counter/reset evidence.
- [Verification of all 30 raw CSV/JSON pairs, source identity, and reconnects](probe_ab_t1_t4_ubuntu_20261104/verification.json).
- [T3 per-run normal/slow group metrics and missing-final segment counts](probe_ab_t1_t4_ubuntu_20261104/t3_group_metrics.json).

## Conditions and method

Ubuntu 24.04.5 LTS, `Linux-6.8.0-147-generic-aarch64-with-glibc2.39`, 2 virtual CPUs, approximately 4 GiB RAM; Python 3.12.3, websockets 16.1.1. New run start (UTC): `2026-11-04T03:09:01.910912+00:00`. T1–T4 loopback qdisc was `noqueue` before and after the run. T5 used loopback netem with 120 ms delay, 30 ms normal-distribution jitter, and 3% packet loss, followed by a verified reset to `noqueue`.

| Test | Clients | Duration/run | Source segments | Condition |
|---|---:|---:|---:|---|
| T1 | 20 | 20 s | 22 | Basic correctness |
| T2 | 100 | 40 s | 49 | Public scale |
| T3 | 100 | 60 s | 76 | 10% slow readers, 500 ms sleep/message |
| T4 | 100 | 60 s | 76 | 20% disconnect at 15 s for 5 s |
| T5 (reused) | 100 | 40 s | 49 | Netem delay, jitter, and loss |

Each server restarts for each run. For each test, all three with-probe runs precede the three no-probe runs. No-probe omits only the probe-task start in the copied client. This compares the combined probe-driven delivery policy. Values below are medians of per-run metrics, not pooled-message percentiles. Freshness uses a 500 ms threshold. Completeness counts unique final/client pairs received live or recovered through HISTORY by the measurement deadline. Coverage counts unique live segment/revision/client triples; skipped intermediate revisions reduce it. No extra drain interval was added.

The outer host Multipass command returned status 1 after the complete summary; all eight recorded per-test/arm runner exits were zero. A separate VM check confirmed all result files were present, no test/server process remained, and loopback was `noqueue`. The cause of the outer wrapper status was not established. The source archive also transferred macOS `._` metadata sidecars: the raw configuration retains their hashes, which were checked against the archive separately; all application and harness hashes match T5 and the base commit.

## Source timing limitation

The public runner estimates the source segment count using nominal 200 ms revision gaps and 350 ms segment gaps. Observed source timestamps can show longer gaps in this Ubuntu run, and some terminal segments may be absent from every client. The runner waits for the source process to finish but clients use a fixed deadline; the public runner does not timestamp source completion. Consequently, absent terminal finals do not by themselves prove server delivery loss: they may not have been generated before the clients stopped. Per-run observed gaps, latest segments, and universally absent finals are saved in [verification.json](probe_ab_t1_t4_ubuntu_20261104/verification.json) under `source_timing_observations`. These are results from the unmodified public workload; no source-count, duration, or drain adjustment was made.

## Current design (with probes)

| Test | P50 age (ms) | P95 age (ms) | P99 age (ms) | Final completeness (%) | Fresh captions (%) | Fresh finals (%) | Live revision coverage (%) |
|---|---:|---:|---:|---:|---:|---:|---:|
| T1 | 3.95 | 6.57 | 8.29 | 100.00 | 100.00 | 100.00 | 100.00 |
| T2 | 13.84 | 21.87 | 28.60 | 91.84 | 100.00 | 91.84 | 91.84 |
| T3 | 13.08 | 2787.66 | 8241.15 | 90.13 | 94.48 | 81.71 | 86.58 |
| T4 | 12.20 | 19.53 | 25.05 | 89.47 | 100.00 | 88.16 | 87.98 |
| T5 (reused) | 255.26 | 702.30 | 1057.88 | 100.00 | 88.98 | 87.12 | 98.53 |

## Probe comparison

| Test | Finals with (%) | Finals without (%) | P95 with (ms) | P95 without (ms) | P99 with (ms) | P99 without (ms) | Coverage with (%) | Coverage without (%) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| T1 | 100.00 | 95.45 | 6.57 | 5.32 | 8.29 | 6.10 | 100.00 | 95.45 |
| T2 | 91.84 | 87.76 | 21.87 | 17.19 | 28.60 | 25.50 | 91.84 | 89.12 |
| T3 | 90.13 | 83.03 | 2787.66 | 3506.85 | 8241.15 | 21644.60 | 86.58 | 83.77 |
| T4 | 89.47 | 88.16 | 19.53 | 16.03 | 25.05 | 23.58 | 87.98 | 86.67 |
| T5 (reused) | 100.00 | 99.90 | 702.30 | 714.75 | 1057.88 | 1040.24 | 98.53 | 99.82 |

## T3 reader groups

| Configuration | Group | P50 age (ms) | P95 age (ms) | P99 age (ms) | Final completeness (%) | Live revision coverage (%) |
|---|---|---:|---:|---:|---:|---:|
| With probe | normal | 12.75 | 21.01 | 30.85 | 90.79 | 90.79 |
| With probe | slow | 6515.51 | 8876.66 | 9169.73 | 84.21 | 48.68 |
| Without probe | normal | 11.41 | 16.31 | 22.15 | 86.84 | 87.72 |
| Without probe | slow | 12932.01 | 24919.67 | 25932.77 | 48.68 | 49.12 |

## Per-run final delivery

| Test | With-probe unique finals (runs 1, 2, 3) | Without-probe unique finals (runs 1, 2, 3) | Expected/run |
|---|---|---|---:|
| T1 | 440, 440, 440 | 420, 420, 420 | 440 |
| T2 | 4400, 4500, 4500 | 4400, 4300, 4300 | 4,900 |
| T3 | 6850, 6849, 6870 | 6300, 6310, 6400 | 7,600 |
| T4 | 6800, 6800, 6800 | 6600, 6700, 6700 | 7,600 |
| T5 (reused) | 4900, 4900, 4900 | 4900, 4895, 4822 | 4,900 |

## Reconnect and correctness observations

T4 with-probe HISTORY-response P95 values: 4.51, 12.66, 2.22 ms (median 4.51 ms). All 20 forced-disconnect clients reconnected and received HISTORY in each run in both configurations. This measures time from reconnect to HISTORY response, not time to complete final-state recovery; use the final-delivery counts alongside it.

- Across all 30 runs: 0 duplicate captions.
- Across all 30 runs: 0 stale captions.
- Across all 30 runs: 0 invalid non-final HISTORY items.
- Across all 30 runs: 0 connection errors.

A successful runner exit means the experiment completed; the harness does not assert performance or delivery thresholds. Any incomplete counts above represent finals not observed by the measurement deadline. Latency measures accepted live updates and excludes absent deliveries. Differences between three-run medians are descriptive; fixed arm order, machine load, and differing random network loss limit causal claims. See each full comparison for per-run values and range-based verdicts.

## Reproduction

From the repository root inside Ubuntu, using an environment with the recorded dependencies:

```sh
venv/bin/python -u experiments/probe_ab.py --tests t1,t2,t3,t4 --runs 3 \
  --port 8811 --venv "$PWD/venv" --out output/probe_ab_t1_t4_ubuntu_fresh
```

For a new T5 run, use `make exp-probe-t5 PROBE_OUT=output/probe_ab_t5_ubuntu_fresh` inside Ubuntu with sudo/netem available. Use a fresh output directory to preserve this evidence.

## Observed source completion timing

A separate observer read the existing unbuffered logs every 250 ms during the remaining T3/T4 runs. The table measures observed client-start line to observed SOURCE_DONE line. SOURCE_DONE follows the final segment gap and source WebSocket closure, so this is not an exact final-emission timestamp. Polling and scheduling add observation delay. Existing lines at observer startup were excluded. The 60 s client deadline was unchanged. See [raw timestamp observations](probe_ab_t1_t4_ubuntu_20261104/source_completion_observations.json).

| Test | Configuration | Run | Observed start to SOURCE_DONE (s) | Client duration (s) |
|---|---|---:|---:|---:|
| T3 | with_probe | 3 | 65.95 | 60 |
| T3 | no_probe | 1 | 68.51 | 60 |
| T3 | no_probe | 2 | 68.69 | 60 |
| T3 | no_probe | 3 | 68.70 | 60 |
| T4 | with_probe | 1 | 67.22 | 60 |
| T4 | with_probe | 2 | 67.15 | 60 |
| T4 | with_probe | 3 | 67.00 | 60 |
| T4 | no_probe | 1 | 68.59 | 60 |
| T4 | no_probe | 2 | 68.52 | 60 |
| T4 | no_probe | 3 | 67.74 | 60 |
