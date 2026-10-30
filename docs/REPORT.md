# LiveRecall Technical Report

Team: TODO | Members: TODO | Implementation commit: TODO

## 1. Architecture and protocol

TODO: Introduce the classroom problem and the design goals: correct revision state, fresh captions, bounded resource usage and useful recovery.


## 2. Revision and recovery semantics

TODO: Explain and justify the implementation rules for:

- Replacing a segment with a newer revision, rejecting stale/duplicate updates and preserving final state.
- Bounding server history while supporting up to 60 seconds of recent recovery; state the actual time/item bounds and their limitations.
- Late join and reconnect: selecting missing finals from the client's finalized cursor, ordering recovery before live delivery and preventing duplicate finals or gaps.
- Browser state: distinguishing provisional/final captions, retaining at least 60 seconds of finalized history and providing local 15/30/60-second recall with a 30-second default.


## 3. Backpressure and overload policy

TODO: Explain the actual per-client buffer and transport-pressure handling, including numerical bounds and the signals that activate overload behavior.

TODO: State which provisional revisions may be discarded, how finals and the latest active state are preserved, and what happens if a client remains unable to keep up. Explain how normal clients remain independent and how full revision delivery resumes when pressure clears.

TODO: Discuss catch-up after a temporary bottleneck and the limits of any disconnect/resynchronization policy. Distinguish what the design intends from what experiments demonstrate.

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

Caption Age is client receive time minus `source_ts`; it excludes browser rendering delay. Final completeness measures correctly received unique finals against generated finals. 

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

TODO: Report duplicate/stale-state counts, reconnect recovery time, and evidence of bounded memory/buffering. Define the recovery-time measurement and identify which client group it covers. Add compact supporting figures/tables only where they clarify the findings.

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



## Appendices 

## Draft completion checklist (remove before export)

- [ ] All required sections are supported by the submitted implementation and evidence.
- [ ] T1–T5 each have at least three independent runs; medians and units are clear.
- [ ] At least one stress-test before/after comparison is quantified and reproducible.
- [ ] The main report is 4–5 pages after PDF export, excluding references/appendices.
- [ ] The reviewed PDF is saved as `submission_root/report.pdf`.
- [ ] README, AI usage, contributions and the separate 5–7-minute demo video are completed.
