# Probe comparison

Medians across independent runs; change is with probe minus without probe.

### T1 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| P50 ms | 3.28 | 3.64 | 2.19, 3.28, 4.02 | 4.04, 3.31, 3.64 |
| P95 ms | 6.67 | 7.14 | 5.94, 7.35, 6.67 | 6.30, 7.14, 7.77 |
| P99 ms | 8.11 | 8.97 | 6.49, 10.87, 8.11 | 42.95, 7.83, 8.97 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 6.67 | 7.14 | 5.94, 7.35, 6.67 | 6.30, 7.14, 7.77 |

### T2 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| P50 ms | 15.82 | 14.22 | 16.77, 13.17, 15.82 | 14.98, 14.22, 13.65 |
| P95 ms | 27.85 | 25.49 | 27.89, 26.10, 27.85 | 25.49, 27.37, 23.09 |
| P99 ms | 36.93 | 34.27 | 40.02, 36.93, 32.02 | 34.27, 42.14, 33.65 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 27.85 | 25.49 | 27.89, 26.10, 27.85 | 25.49, 27.37, 23.09 |

### T3 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 99.38 | 95.13 | 99.47, 99.34, 99.38 | 95.13, 95.13, 95.13 |
| normal update coverage % | 95.18 | 95.18 | 95.18, 95.18, 95.18 | 95.18, 95.18, 95.18 |
| P50 ms | 11.99 | 6.35 | 11.99, 13.05, 6.17 | 6.35, 14.59, 6.23 |
| P95 ms | 2248.88 | 2269.12 | 2267.90, 2248.88, 2246.39 | 2258.66, 2282.42, 2269.12 |
| P99 ms | 7786.01 | 24035.07 | 8004.86, 7786.01, 7754.31 | 24025.26, 24052.79, 24035.07 |
| fresh % | 94.65 | 94.65 | 94.65, 94.65, 94.65 | 94.65, 94.65, 94.65 |
| fresh-final % | 90.00 | 90.00 | 90.00, 90.00, 90.00 | 90.00, 90.00, 90.00 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 23.88 | 17.26 | 23.88, 24.78, 17.36 | 17.26, 27.99, 8.25 |
| slow-client P95 ms | 8490.87 | 28093.65 | 8686.73, 8490.87, 8432.97 | 28093.65, 28076.40, 28097.95 |

### T4 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| normal update coverage % | 98.33 | 98.33 | 98.33, 98.33, 98.33 | 98.33, 98.33, 98.25 |
| P50 ms | 5.29 | 11.70 | 5.28, 5.29, 6.10 | 12.51, 11.04, 11.70 |
| P95 ms | 8.80 | 23.13 | 8.75, 8.80, 15.36 | 23.90, 23.13, 23.06 |
| P99 ms | 12.55 | 32.16 | 12.55, 11.93, 23.69 | 32.16, 28.11, 32.46 |
| fresh % | 100.00 | 100.00 | 100.00, 100.00, 100.00 | 100.00, 100.00, 100.00 |
| fresh-final % | 98.42 | 98.42 | 98.42, 98.42, 98.42 | 98.42, 98.42, 98.42 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 8.80 | 23.13 | 8.75, 8.80, 15.36 | 23.90, 23.13, 23.06 |
| recovery P95 ms | 3.31 | 12.17 | 3.51, 3.03, 3.31 | 12.17, 10.29, 13.80 |

### T5 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 99.98 | 100.00, 100.00, 100.00 | 99.98, 99.98, 100.00 |
| normal update coverage % | 98.86 | 99.97 | 99.18, 98.86, 98.76 | 99.89, 99.97, 99.97 |
| P50 ms | 258.43 | 261.47 | 258.43, 262.63, 257.33 | 260.57, 261.47, 269.30 |
| P95 ms | 660.66 | 905.82 | 549.88, 671.33, 660.66 | 905.82, 767.73, 1086.38 |
| P99 ms | 801.68 | 1270.90 | 743.70, 801.68, 848.98 | 1117.19, 1270.90, 1994.12 |
| fresh % | 91.28 | 85.59 | 93.54, 87.54, 91.28 | 85.59, 86.38, 82.10 |
| fresh-final % | 87.39 | 82.16 | 92.67, 87.39, 85.98 | 82.16, 86.59, 74.96 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 660.66 | 905.82 | 549.88, 671.33, 660.66 | 905.82, 767.73, 1086.38 |

## Section 5 — T3

Change = with probe minus without probe; percentage changes are percentage points.

| Metric | With probe | Without probe | Change |
|---|---:|---:|---:|
| Final completeness | 99.38 % | 95.13 % | +4.25 pp |
| P99 age | 7786.01 ms | 24035.07 ms | -16249.06 ms |
| Slow-client P95 | 8490.87 ms | 28093.65 ms | -19602.78 ms |
| Normal-client P95 | 23.88 ms | 17.26 ms | +6.62 ms |

## Verdicts

T1: no difference beyond run-to-run spread
T2: no difference beyond run-to-run spread
T3: probing changes completeness by +4.25 pp; P99 by -16249.06 ms; slow-client P95 by -19602.78 ms
T4: probing changes P50 by -6.41 ms; P95 by -14.33 ms; P99 by -19.61 ms; normal-client P95 by -14.33 ms; recovery P95 by -8.86 ms
T5: probing changes normal update coverage by -1.12 pp; P95 by -245.16 ms; P99 by -469.22 ms; fresh by +5.69 pp; normal-client P95 by -245.16 ms

Range separation is descriptive, not a significance test; one run cannot estimate spread.
