# Ubuntu Setup for LiveRecall

Ubuntu Linux is required for the reproducible performance/network-impairment tests because they use Linux `tc/netem`.

## Existing Ubuntu / Mininet VM

You may reuse your earlier Ubuntu environment if these commands work:

```bash
python3 --version
tc -V
```

Install the mandatory packages if needed:

```bash
sudo apt update
sudo apt install -y make python3 python3-venv python3-pip iproute2
```

From the repository root **inside Ubuntu**, create a Python 3.10+ environment,
install the mandatory dependencies, load netem, and check server startup:

```bash
make install-VM
make exp-probe-quick
make exp-probe-t5
```

`install-VM` prepares an existing Ubuntu system; it does not install Multipass or
create/start a VM. All Make targets use `venv/` by default. For a different
environment use the same override throughout, for example
`make install-VM VENV=/home/ubuntu/envs/liverecall` and
`make exp-probe-t5 VENV=/home/ubuntu/envs/liverecall`.

The quick target runs T1 once per configuration. The T5 target runs 100 clients
for 40 seconds, three independent runs with probes and three without, using
the supplied delay/jitter/loss profile. For the complete comparison, run
`make exp-probe` (T1–T5, three runs per configuration). See
[the experiment guide](../experiments/README.md) for evidence files and sudo needs.

## Optional Multipass on macOS or Windows

Official installation page: https://canonical.com/multipass/install

Example after Multipass is installed:

```bash
multipass launch --name liverecall --cpus 2 --memory 4G --disk 15G
multipass shell liverecall
```

Inside Ubuntu:

```bash
sudo apt update
sudo apt install -y make python3 python3-venv python3-pip iproute2
```

Clone or copy the repository into Ubuntu before running the Make commands above.
Create its Python environment in Ubuntu; do not copy a host virtual environment.
For an existing VM, use `multipass start liverecall` and
`multipass shell liverecall` instead of launching another one.

When finished, exit the VM shell and run on the host:

```bash
multipass stop liverecall
```

Run the server and simulated clients inside the same Ubuntu environment for the public performance experiments. The supplied profiles can then be applied to `lo`.

`ffmpeg` and ASR packages are **not** mandatory dependencies. Install them only if your team attempts the optional ASR bonus.
