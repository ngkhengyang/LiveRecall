# Probe comparison

Medians across independent runs; change is with probe minus without probe.

### T1 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 95.45 | 100.00, 100.00, 100.00 | 95.45, 95.45, 95.45 |
| normal update coverage % | 100.00 | 95.45 | 100.00, 100.00, 100.00 | 95.45, 96.97, 95.45 |
| P50 ms | 3.95 | 3.73 | 4.24, 3.95, 3.95 | 3.90, 3.61, 3.73 |
| P95 ms | 6.57 | 5.32 | 6.87, 6.57, 6.36 | 6.03, 5.07, 5.32 |
| P99 ms | 8.29 | 6.10 | 8.35, 7.11, 8.29 | 20.57, 5.69, 6.10 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 100.00 | 95.45 | 100.00, 100.00, 100.00 | 95.45, 95.45, 95.45 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 6.57 | 5.32 | 6.87, 6.57, 6.36 | 6.03, 5.07, 5.32 |

### T2 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 91.84 | 87.76 | 89.80, 91.84, 91.84 | 89.80, 87.76, 87.76 |
| normal update coverage % | 91.84 | 89.12 | 91.16, 91.84, 92.52 | 89.80, 89.12, 89.12 |
| P50 ms | 13.84 | 12.13 | 14.12, 13.83, 13.84 | 12.31, 12.11, 12.13 |
| P95 ms | 21.87 | 17.19 | 21.94, 21.38, 21.87 | 17.19, 16.64, 17.68 |
| P99 ms | 28.60 | 25.50 | 28.60, 24.52, 34.13 | 25.50, 21.44, 26.70 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 91.84 | 87.76 | 89.80, 91.84, 91.84 | 89.80, 87.76, 87.76 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 21.87 | 17.19 | 21.94, 21.38, 21.87 | 17.19, 16.64, 17.68 |

### T3 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 90.13 | 83.03 | 90.13, 90.12, 90.39 | 82.89, 83.03, 84.21 |
| normal update coverage % | 86.58 | 83.77 | 86.58, 86.54, 86.62 | 83.77, 83.46, 84.25 |
| P50 ms | 13.08 | 11.69 | 12.74, 13.20, 13.08 | 11.69, 11.72, 11.50 |
| P95 ms | 2787.66 | 3506.85 | 2787.66, 2708.83, 3107.52 | 3506.85, 3590.80, 3423.99 |
| P99 ms | 8241.15 | 21644.60 | 8241.15, 8363.63, 8144.52 | 21756.43, 21644.60, 21374.76 |
| fresh % | 94.48 | 94.27 | 94.48, 94.53, 94.43 | 94.35, 94.22, 94.27 |
| fresh-final % | 81.71 | 78.16 | 81.71, 81.71, 81.71 | 78.16, 78.16, 79.34 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 21.01 | 16.31 | 20.27, 21.86, 21.01 | 16.31, 16.21, 16.38 |
| slow-client P95 ms | 8876.66 | 24919.67 | 8876.66, 8991.03, 8799.62 | 25125.46, 24919.67, 24824.26 |

### T4 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 89.47 | 88.16 | 89.47, 89.47, 89.47 | 86.84, 88.16, 88.16 |
| normal update coverage % | 87.98 | 86.67 | 88.86, 87.98, 87.98 | 85.79, 86.67, 86.67 |
| P50 ms | 12.20 | 10.70 | 12.20, 11.94, 12.47 | 10.71, 10.70, 10.67 |
| P95 ms | 19.53 | 16.03 | 19.53, 19.12, 20.47 | 16.03, 15.75, 16.17 |
| P99 ms | 25.05 | 23.58 | 23.81, 30.18, 25.05 | 23.86, 23.58, 21.43 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 88.16 | 86.84 | 88.07, 88.16, 88.16 | 85.53, 86.84, 86.84 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 19.53 | 16.03 | 19.53, 19.12, 20.47 | 16.03, 15.75, 16.17 |
| recovery P95 ms | 4.51 | 4.17 | 4.51, 12.66, 2.22 | 2.43, 4.17, 4.79 |

## Section 5 — T3

Change = with probe minus without probe; percentage changes are percentage points.

| Metric | With probe | Without probe | Change |
|---|---:|---:|---:|
| Final completeness | 90.13 % | 83.03 % | +7.11 pp |
| P99 age | 8241.15 ms | 21644.60 ms | -13403.45 ms |
| Slow-client P95 | 8876.66 ms | 24919.67 ms | -16043.01 ms |
| Normal-client P95 | 21.01 ms | 16.31 ms | +4.70 ms |

## Verdicts

T1: probing changes completeness by +4.55 pp; normal update coverage by +4.55 pp; P50 by +0.22 ms; P95 by +1.25 ms; fresh-final by +4.55 pp; normal-client P95 by +1.25 ms
T2: probing changes normal update coverage by +2.72 pp; P50 by +1.70 ms; P95 by +4.68 ms; normal-client P95 by +4.68 ms
T3: probing changes completeness by +7.11 pp; normal update coverage by +2.81 pp; P50 by +1.39 ms; P95 by -719.19 ms; P99 by -13403.45 ms; fresh by +0.20 pp; fresh-final by +3.55 pp; normal-client P95 by +4.70 ms; slow-client P95 by -16043.01 ms
T4: probing changes completeness by +1.32 pp; normal update coverage by +1.32 pp; P50 by +1.51 ms; P95 by +3.51 ms; fresh-final by +1.32 pp; normal-client P95 by +3.51 ms

Range separation is descriptive, not a significance test; one run cannot estimate spread.

## Run conditions and interpretation

T1–T4 were rerun in Ubuntu 24.04.5 LTS (ARM64, 2 virtual CPUs, approximately 4 GiB RAM), Python 3.12.3 and websockets 16.1.1, using three independent runs per arm. Loopback was verified as `noqueue` before and after. The public workload and application sources match the recorded source hashes and the earlier same-day T5 run.

Source generation ran longer than the fixed client deadline in observed T3/T4 runs. Observed source gaps also exceeded the nominal gaps used to estimate the segment count. Terminal finals absent from every client can therefore reflect generation outside the measurement window. The automatic verdicts above describe separated observed ranges; they do not establish a server-delivery benefit caused by probing. Latency excludes missing deliveries, and a zero runner exit does not assert completeness.

See the [combined T1–T5 results and timing caveats](../TEST_RESULTS_20261104.md), [raw source-completion observations](source_completion_observations.json), and [CSV/JSON verification and per-run source timing](verification.json). Older evidence is preserved, and T5 was reused rather than rerun.
