# Probe comparison

Medians across independent runs; change is with probe minus without probe.

### T1 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| P50 ms | 5.40 | 5.23 | 5.31, 5.40, 5.56 | 5.23, 4.98, 5.36 |
| P95 ms | 11.46 | 12.39 | 12.53, 11.46, 9.93 | 12.39, 10.93, 14.05 |
| P99 ms | 13.46 | 16.69 | 17.35, 12.69, 13.46 | 25.25, 12.24, 16.69 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 11.46 | 12.39 | 12.53, 11.46, 9.93 | 12.39, 10.93, 14.05 |

### T2 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| P50 ms | 23.17 | 22.95 | 23.39, 23.17, 20.13 | 22.47, 22.95, 22.98 |
| P95 ms | 42.88 | 43.79 | 42.88, 45.36, 36.78 | 40.74, 43.79, 53.13 |
| P99 ms | 65.80 | 92.72 | 65.80, 70.07, 44.41 | 49.96, 92.72, 137.55 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 42.88 | 43.79 | 42.88, 45.36, 36.78 | 40.74, 43.79, 53.13 |

### T3 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 99.38 | 95.13 | 99.26, 99.45, 99.38 | 95.13, 95.13, 93.68 |
| normal update coverage % | 95.18 | 95.18 | 95.04, 95.18, 95.18 | 95.18, 95.18, 93.70 |
| P50 ms | 19.84 | 22.48 | 19.91, 18.56, 19.84 | 20.40, 22.48, 24.69 |
| P95 ms | 2249.43 | 2265.91 | 1585.83, 2249.43, 2254.63 | 2265.91, 2275.38, 2103.62 |
| P99 ms | 7821.28 | 23973.97 | 7879.69, 7780.36, 7821.28 | 23981.33, 23973.97, 22422.96 |
| fresh % | 94.65 | 94.65 | 94.79, 94.65, 94.65 | 94.65, 94.65, 93.66 |
| fresh-final % | 90.00 | 90.00 | 90.00, 90.00, 90.00 | 90.00, 90.00, 87.71 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 29.02 | 38.58 | 56.75, 27.82, 29.02 | 28.81, 38.58, 41.04 |
| slow-client P95 ms | 8470.33 | 28015.74 | 8574.87, 8470.33, 8402.84 | 28046.06, 28015.74, 27329.09 |

### T4 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 98.25 | 98.33 | 98.25, 98.33, 98.25 | 98.33, 98.33, 98.29 |
| P50 ms | 17.08 | 16.84 | 17.82, 16.99, 17.08 | 16.84, 16.50, 16.87 |
| P95 ms | 30.02 | 28.09 | 30.45, 26.47, 30.02 | 27.83, 28.09, 28.77 |
| P99 ms | 40.23 | 37.89 | 47.41, 34.90, 40.23 | 36.89, 37.89, 45.00 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 98.42 | 98.42 | 98.42, 98.42, 98.42 | 98.42, 98.42, 98.42 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 30.02 | 28.09 | 30.45, 26.47, 30.02 | 27.83, 28.09, 28.77 |
| recovery P95 ms | 23.29 | 18.49 | 48.73, 20.79, 23.29 | 17.78, 18.49, 34.57 |

T5: SKIPPED (no passwordless sudo / sch_netem)

## Section 5 — T3

Change = with probe minus without probe; percentage changes are percentage points.

| Metric | With probe | Without probe | Change |
|---|---:|---:|---:|
| Final completeness | 99.38 % | 95.13 % | +4.25 pp |
| P99 age | 7821.28 ms | 23973.97 ms | -16152.70 ms |
| Slow-client P95 | 8470.33 ms | 28015.74 ms | -19545.41 ms |
| Normal-client P95 | 29.02 ms | 38.58 ms | -9.56 ms |

## Verdicts

T1: no difference beyond run-to-run spread
T2: no difference beyond run-to-run spread
T3: probing changes completeness by +4.25 pp; P50 by -2.63 ms; P99 by -16152.70 ms; slow-client P95 by -19545.41 ms
T4: probing changes P50 by +0.24 ms
T5: SKIPPED (no passwordless sudo / sch_netem)

Range separation is descriptive, not a significance test; one run cannot estimate spread.
