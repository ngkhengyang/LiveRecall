# LiveRecall experiments

Three suites share the supplied analyser and store independent runs plus medians:
- `public`: T1–T5 (10 scenarios, including the T5 control and five profiles).
- `before_after`: four workloads × six arms (24 cases; final results reused when available).
- `hidden`: 38 scenarios, including H_base; H10 has six protocol checks.

The current catalog has 52 scenarios / 87 scenario-arm cases, before reuse.
Thresholds are our own engineering targets, not the hidden evaluator's thresholds.

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

Regenerate `SUMMARY.md` and `summary.csv` from stored medians and verdicts:
```bash
python experiments/report.py --out output/experiments
```

Arms: `final`, `naive`, `no-probe`, `no-grace`, `no-resync`, `no-all`.
The runner sets `LIVERECALL_ABLATE` to `probe`, `grace`, `resync`, or `all`
for the corresponding `no-*` arm. The variable also accepts comma-separated
values; unset/empty means normal final behavior. `naive` uses the starter server.
H10 reports malformed CLIENT_HELLO acceptance as a failure even if fields are
intentionally repaired; it also rejects replay of segment K on resume.
Bold report rows flag completeness <99%.
