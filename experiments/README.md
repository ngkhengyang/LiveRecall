# Probe versus no-probe experiment

From the repository root, run:

```sh
make exp-probe        # Inside Ubuntu: T1–T5, three independent runs per arm
make exp-probe-quick  # T1 only, one run per arm
make exp-probe-t5     # Inside Ubuntu: T5 only, three runs per arm
```

`VENV` selects a Python virtual environment, not a virtual machine. These commands
run on the operating system of the current terminal. Enter Ubuntu first; see
[the VM setup instructions](../docs/UBUNTU_SETUP.md). Run `make install-VM` from
the Ubuntu repository root to create its Python environment and load netem.
Python 3.10+ is required; Ubuntu 24.04 provides Python 3.12. For a custom environment,
use the same `VENV=/absolute/path` on setup and experiment commands.

`make exp-probe-t5` uses the existing `probe_ab.py` workflow and writes to
`output/probe_ab_t5/`. To keep independent experiment sets separate:

```sh
make exp-probe-t5 VENV=/home/ubuntu/envs/liverecall PROBE_OUT=output/probe_ab_t5_ubuntu
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
`network_profiles/delay_loss.sh lo`: 120 ms delay, 30 ms normal-distribution
jitter, and 3% packet loss. Both `make exp-probe` and `make exp-probe-t5` pass
`--require-t5`, so missing Linux/netem/sudo support fails the command rather than
silently skipping T5. In an interactive Ubuntu terminal the harness can request
sudo authentication when T5 starts; unattended runs need existing noninteractive
sudo access. Setup does not change sudoers. Direct Python invocations retain the
optional skip behavior unless `--require-t5` is supplied.

The harness refuses to replace an existing loopback impairment. Once profile
application begins, it always attempts `network_profiles/reset.sh lo` in a
`finally`, including after errors or Ctrl-C. It verifies the kernel state after
reset and fails if netem remains. Run on an Ubuntu VM without another loopback
network experiment active.

Each arm logs to `<variant>/output/<test>.log`. Nonzero runner exits are recorded;
remaining arms and tests continue, and the harness ultimately exits nonzero.
Progress lines and each completed test's median table appear live. At completion,
the command prints all median tables, a ready-to-paste T3 Section 5 table, verdicts,
and saved paths. `SUMMARY.md` and `summary.csv` include per-run values;
`configuration.json` records the test arguments, run count, port, and exit statuses.
It also records OS/Python/dependency details, CPU/RAM information, and hashes of
the application, public harness, and experiment sources. For T5,
`qdisc_before.json`, `qdisc_applied.json`, `qdisc_after_test.json`, and
`qdisc_after_reset.json` show the actual kernel configuration/counters;
`t5_network_status.json` records profile application and cleanup status. Packet
drop counters are cumulative across both configurations. The public generator,
load client, runner, and analyser are not modified by this experiment workflow.

`output/` is Git-ignored. Copy the selected run's summary, configuration, qdisc
evidence, and both configurations' CSV/JSON results into `results/` before
committing evidence for submission. Setup/debug logs and quick-check outputs can
stay in `output/`. Do not overwrite older runs from another implementation or
operating system.

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

No-probe removes ping-based lag detection, the probe's 75-second stall detection,
and probe-driven wakeups/pacing. The separate 75-second socket-send timeout still
applies. The server's built-in heartbeat is disabled with `ping_interval=None` in
`app/server.py`. This compares the combined delivery policy rather than isolating
ping overhead alone. Scheduling and machine load can change results;
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
