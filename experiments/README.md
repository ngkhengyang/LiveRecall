# LiveRecall experiments

Three suites share the supplied analyser and store independent runs plus medians:
- `public`: T1–T5 (10 scenarios, including the T5 control and five profiles).
- `before_after`: four workloads × six arms (24 cases; final results reused when available).
- `hidden`: 38 scenarios, including H_base; H10 has six protocol checks.

The current catalog has 52 scenarios / 87 scenario-arm cases, before reuse.
Thresholds are our own engineering targets, not the hidden evaluator's thresholds.

## H scenario index

List the exact scenario IDs with:
```bash
./venv/bin/python experiments/run_matrix.py --suite hidden --list
```

These are locally defined stress scenarios. Full settings are below; `--quick`
reduces client counts, durations, and event timings.

| Group | What it tests |
|---|---|
| `H_base` | Unimpaired control: 100 clients, 60 seconds |
| `H1_cN` | Scaling to N = 250, 500, or 1,000 clients |
| `H2_sP_mMS` | P = 10/30/50% slow readers, each waiting MS = 100/250/500/1,000 ms per caption frame |
| `H3_RATE` | Constrained bandwidth at 2 Mbit/s, 1 Mbit/s, or 512 kbit/s; requires tc |
| `H4_pP_dD` | P = 50/100% disconnect for D = 2/10/30/70 seconds; 70-second cases are informational |
| `H5_lS` | 20% of clients join S = 10/30 seconds late |
| `H6` | Rapid revisions: 20 ms between revisions, 50 ms between segments |
| `H7_PROFILE` | Freshness under delay/loss network profiles; requires tc |
| `H8_bandwidth`, `H8_slow` | Convergence after temporary bandwidth or slow-reader impairment ends |
| `H9` | Five-minute memory-growth observation with 30% slow readers |
| `H10` | Six protocol and recovery invariant checks |
| `H11` | Combined stress: 500 clients, rapid revisions, delay/loss, slow readers, and reconnects; requires tc |

For example, `H2_s10_m1000` means 10% slow readers waiting 1,000 ms per frame.
Exact settings and engineering targets are in `experiments/scenarios.py`.

On Ubuntu or WSL, from the repo root:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r submission_root/requirements.txt
python experiments/run_matrix.py --list
python experiments/run_matrix.py --suite all --no-tc --quick --runs 1
# Full local workloads (tc scenarios are recorded as SKIP):
python experiments/run_matrix.py --suite all --no-tc --runs 3
```

For a full run on a Linux host with netem and passwordless sudo:
```bash
sudo apt-get install -y iproute2
sudo modprobe sch_netem
python experiments/run_matrix.py --suite all --runs 3
```

WSL kernels may lack netem. Run tc cases on GitHub's Ubuntu runner:
```bash
gh workflow run experiments.yml -f tc_only=true
gh run list --workflow experiments.yml
# Substitute the completed run's ID; keep artifacts from each host separate.
gh run download RUN_ID -n experiments-results -D output/experiments-ci
python experiments/report.py --out output/experiments-ci
```
`--tc-only` selects strictly `needs_tc` scenarios; it excludes T5_none and H_base.
For same-runner T5 control ratios, dispatch with `-f tc_only=false` instead.
Use `--only H2,T3` to select prefixes, or `--suite hidden` for hidden workloads.
Use `--arms final naive` to restrict comparison arms. New results record the
effective settings and an implementation digest in `*_configuration.json`;
final medians are reused only when those settings and the completed run count
match. Keep quick runs, full runs, and different hosts in separate output folders.

Regenerate `SUMMARY.md` and `summary.csv` from stored medians and verdicts:
```bash
python experiments/report.py --out output/experiments
```

Arms: `final`, `naive`, `no-probe`, `no-grace`, `no-resync`, `no-all`.
The runner sets `LIVERECALL_ABLATE` to `probe`, `grace`, `resync`, or `all`
for the corresponding `no-*` arm. The variable also accepts comma-separated
values; unset/empty means normal final behavior. `naive` uses the starter server.
H10 checks unknown fields, SOURCE_DONE, malformed JSON/unknown hello roles,
duplicates, stale/post-final revisions, and resume without replay of the held
final. CLIENT_HELLO fields are deliberately defaulted when malformed; H10 does
not enforce strict field rejection.
Current overload counters are `coalesced`, `rejected`, `held`, `batches`,
`batched_finals`, and `resyncs`. Unlogged counters remain unknown rather than zero.
Bold report rows flag completeness <99%.

Check the experiment regression cases without opening sockets:
```bash
python -m unittest discover -s experiments -p 'test_regressions.py'
```

Browser dashboard (graphs, verdicts, arm comparison, and a launcher for runs):
```bash
make exp-ui        # or: python experiments/dashboard.py  ->  http://127.0.0.1:8100
```
Runs started from the dashboard write to `output/experiments-ui` by default and use port 8790.
