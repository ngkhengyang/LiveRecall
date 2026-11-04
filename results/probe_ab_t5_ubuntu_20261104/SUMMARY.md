# Probe comparison

Medians across independent runs; change is with probe minus without probe.

### T5 (runs: with=3, without=3)

| Metric | With probe median | Without probe median | Per-run with | Per-run without |
|---|---:|---:|---|---|
| completeness % | 100.00 | 99.90 | 100.00, 100.00, 100.00 | 100.00, 99.90, 98.41 |
| normal update coverage % | 98.53 | 99.82 | 98.53, 98.90, 98.48 | 99.98, 99.82, 99.43 |
| P50 ms | 255.26 | 262.88 | 251.99, 260.07, 255.26 | 262.88, 274.51, 256.03 |
| P95 ms | 702.30 | 714.75 | 633.04, 702.30, 710.66 | 714.75, 1169.44, 620.60 |
| P99 ms | 1057.88 | 1040.24 | 815.80, 1069.15, 1057.88 | 1040.24, 1762.28, 942.87 |
| fresh % | 88.98 | 88.66 | 92.03, 88.52, 88.98 | 88.66, 79.34, 92.83 |
| fresh-final % | 87.12 | 86.06 | 89.59, 87.12, 84.63 | 86.06, 78.16, 88.80 |
| duplicates | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| stale | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| connection errors | 0.00 | 0.00 | 0.00, 0.00, 0.00 | 0.00, 0.00, 0.00 |
| normal-client P95 ms | 702.30 | 714.75 | 633.04, 702.30, 710.66 | 714.75, 1169.44, 620.60 |

## Verdicts

T5: probing changes normal update coverage by -1.29 pp

Range separation is descriptive, not a significance test; one run cannot estimate spread.

## Ubuntu run conditions and evidence

Run on 4 November 2026 in Ubuntu 24.04 LTS (ARM64, 2 virtual CPUs,
approximately 4 GiB RAM), Python 3.12.3, websockets 16.1.1. Each configuration
used 100 clients, 40 seconds, 49 source segments, and three independent runs.
The freshness threshold was 500 ms. Exact source hashes and software details
are in `configuration.json`.

Loopback netem used 120 ms delay, 30 ms normal-distribution jitter, and 3% packet
loss. The qdisc JSON files record the applied profile, 8,625 packet drops across
all six runs, and the final reset to `noqueue`. `t5_network_status.json` confirms
successful setup and cleanup.

The with-probe runs each received all 4,900 expected final/client pairs.
Without probes, the counts were 4,900, 4,895, and 4,822. All missing finals were
segment 49: missing at 5 clients in run 2 and 78 in run 3 at the measurement
deadline. No additional drain interval was used; eventual delivery was not measured.
All six runs had zero duplicate/stale captions, invalid non-final history items,
or client connection errors.

The three with-probe runs preceded the three no-probe runs under the same qdisc.
Random loss sequences differed, latency ranges overlap, and the results do not
establish a general latency improvement. Probing reduced median live revision
coverage by 1.29 percentage points. The supplied application and public measurement
programs were unchanged; the no-probe copy only omitted the probe-task start.
