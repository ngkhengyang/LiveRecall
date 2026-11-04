SHELL := /bin/bash

VENV ?= $(CURDIR)/venv
PYTHON := $(abspath $(VENV))/bin/python
PROBE_OUT ?= output/probe_ab

.PHONY: install install-VM check check-final \
	demo-basic demo-server demo-report demo-browser demo-caption \
	t1-basic t2-scale t3-slow t3-monitor t4-reconnect \
	t5-network t5-network-enable t5-network-reset \
	report


# START_HERE.md
install:
	sudo apt update
	sudo apt install -y python3 python3-venv python3-pip iproute2
	python3 -m venv "$(VENV)"
	"$(PYTHON)" -m pip install -r submission_root/requirements.txt
	"$(PYTHON)" -m pip install -r config/requirements-dev.txt
	"$(abspath $(VENV))/bin/pre-commit" install --config config/.pre-commit-config.yaml
	"$(PYTHON)" test/submission_check.py


install-VM:
	sudo apt update
	sudo apt install -y python3 python3-venv python3-pip iproute2 kmod
	python3 -c 'import sys; assert sys.version_info >= (3, 10), "LiveRecall requires Python 3.10+"'
	python3 -m venv "$(VENV)"
	"$(PYTHON)" -m pip install -r submission_root/requirements.txt
	sudo modprobe sch_netem || { sudo apt install -y "linux-modules-extra-$$(uname -r)"; sudo modprobe sch_netem; }
	"$(PYTHON)" test/submission_check.py


# Basic public experiment (20 clients, 20 seconds, one run).
demo-basic:
	"$(PYTHON)" test/run_demo.py --clients 20 --duration 20 --runs 1

# Server startup command used for local browser demonstrations.
demo-server:
	"$(PYTHON)" submission_root/solution_server.py --port 8765

# Report experiment (100 clients, 40 seconds, three independent runs).
demo-report:
	"$(PYTHON)" test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t2

# Validate the submission structure and WebSocket startup path.
check:
	"$(PYTHON)" test/submission_check.py

# Convert docs/REPORT.md into submission_root/report.pdf.
report:
	"$(PYTHON)" config/build_report.py

# Also require the final submission documents and report.
check-final:
	"$(PYTHON)" test/submission_check.py --final

# PUBLIC_TEST.md
# T1: Basic correctness.
t1-basic:
	"$(PYTHON)" test/run_demo.py --clients 20 --duration 20 --runs 3 --output-stem output/t1

# T2: Public scale.
t2-scale:
	"$(PYTHON)" test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t2

# T3: Slow readers.
t3-slow:
	"$(PYTHON)" test/run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem output/t3

# Optional T3 server memory monitor; run this in another terminal.
t3-monitor:
	"$(PYTHON)" test/monitor_server.py --match solution_server.py --duration 60 --output output/t3_memory.csv

# T4: Reconnect.
t4-reconnect:
	"$(PYTHON)" test/run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem output/t4

# T5: Network impairment. Run t5-network-enable first and t5-network-reset afterwards.
t5-network:
	"$(PYTHON)" test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t5

t5-network-enable:
	sudo bash network_profiles/delay_loss.sh lo

t5-network-reset:
	sudo bash network_profiles/reset.sh lo



# DEMO_GUIDE.md
# Serve the browser demo on http://127.0.0.1:8000.
demo-browser:
	"$(PYTHON)" -m http.server 8000 --directory submission_root/client

# Stream deterministic captions for the long browser demonstration.
demo-caption:
	"$(PYTHON)" test/caption_source.py --duration 120

.PHONY: exp-probe exp-probe-quick exp-probe-t5

# Full path of the virtual environment used by the probe experiment.
# Pass any venv on the command line, e.g.
#   make exp-probe VENV=/mnt/c/Users/games/Desktop/LiveRecall/venvVM
#   make exp-probe-quick VENV=/home/me/envs/liverecall

# Full probe comparison, with progressive and final terminal summaries (~25 min).
exp-probe:
	"$(PYTHON)" -u experiments/probe_ab.py --venv "$(abspath $(VENV))" --out "$(PROBE_OUT)" --require-t5

# One T1 run per arm for a quick harness check.
exp-probe-quick:
	"$(PYTHON)" -u experiments/probe_ab.py --tests t1 --runs 1 --out output/probe_ab_quick --venv "$(abspath $(VENV))"

# Existing probe harness: three T5 runs per arm; fail if netem is unavailable.
exp-probe-t5: PROBE_OUT = output/probe_ab_t5
exp-probe-t5:
	"$(PYTHON)" -u experiments/probe_ab.py --tests t5 --runs 3 --venv "$(abspath $(VENV))" --out "$(PROBE_OUT)" --require-t5
