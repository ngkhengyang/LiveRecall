# Probe versus no-probe experiment

From the repository root, run:

```sh
make exp-probe        # T1–T5, three independent runs per arm (~25 minutes)
make exp-probe-quick  # T1 only, one run per arm
```

The harness copies `submission_root/` and `test/` into two fresh variants under
`output/probe_ab/`. Only the copied no-probe client changes: its sender starts,
but its probe loop never starts. The patch requires exactly one occurrence of
the current task-start block and fails on source drift. Source files are untouched.
Only the selected output directory's two variant directories are replaced on reruns;
`--out` must resolve strictly inside the repository's `output/` folder.

For each selected test, the with-probe arm runs before the no-probe arm, using
`test/run_demo.py` and the public T1–T4 parameters. Each invocation restarts the
server for each independent run. T5 uses T2 parameters with
`network_profiles/delay_loss.sh lo`. It requires passwordless sudo and netem
(not typical on WSL); otherwise it prints
`T5: SKIPPED (no passwordless sudo / sch_netem)` and continues. The harness always
attempts `network_profiles/reset.sh lo` in a `finally`, including on Ctrl-C.
This modifies the host's loopback qdisc while T5 runs; reset removes that root
qdisc, so run on a host without another loopback network experiment.

Each arm logs to `<variant>/output/<test>.log`. Nonzero runner exits are recorded;
remaining arms and tests continue, and the harness ultimately exits nonzero.
Progress lines and each completed test's median table appear live. At completion,
the command prints all median tables, a ready-to-paste T3 Section 5 table, verdicts,
and saved paths. `SUMMARY.md` and `summary.csv` include per-run values;
`configuration.json` records the test arguments, run count, port, and exit statuses.

Custom runs and summary reprinting:

```sh
./venv/bin/python -u experiments/probe_ab.py --tests t1,t3 --runs 3 --port 8811 --out output/probe_ab_custom
./venv/bin/python -u experiments/probe_ab.py --out output/probe_ab_custom --summary-only
```

The tests run with the repository's `venv` by default. Pass `--venv` with the
full path of any virtual environment to use that one instead:

```sh
./venv/bin/python -u experiments/probe_ab.py --tests t1 --runs 1 --venv /home/me/envs/liverecall
make exp-probe-quick VENV=/mnt/c/Users/games/Desktop/LiveRecall/venvVM
```

Summary-only uses the recorded test selection and run count by default. An explicit
`--tests` selection limits the displayed tests. Single-run results (`t1.json`) and
multiple-run results (`t1_runNN.json`) are both supported. Missing metrics are
omitted when absent in both arms; missing data is never treated as zero. Changes
are with-probe minus no-probe; percentages use percentage points.

No-probe also removes the 75 s terminal stall budget (the server heartbeat is
disabled with `ping_interval=None` in `app/server.py`) and the probe re-wake of a
held provisional. These load tests do not measure those properties, which matter
when interpreting the verdict. Scheduling and machine load can change results;
the earlier prototype found the main effect in T3 (about 99.5% versus 95.1%
completeness), but this harness does not guarantee those values.

Thresholds and verdicts are not asserted: this reports numbers. A difference is
reported only when the arms' observed per-run ranges do not overlap, and incomplete
or failed results receive no verdict. Range separation is descriptive, not a
statistical significance test; one run cannot estimate run-to-run spread.

Run the socket-free patch and deletion checks with:

```sh
./venv/bin/python -m unittest experiments/test_probe_ab.py
```
