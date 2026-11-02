SHELL := /bin/bash

.PHONY: install check check-final \
	demo-basic demo-server demo-report demo-browser demo-caption \
	t1-basic t2-scale t3-slow t3-monitor t4-reconnect \
	t5-network t5-network-enable t5-network-reset \
	exp-list exp-quick exp-public exp-hidden exp-before-after exp-all \
	exp-tc exp-edge exp-report exp-ui


# START_HERE.md
install:
	sudo apt update
	sudo apt install -y python3 python3-venv python3-pip iproute2
	python3 -m venv venv
	./venv/bin/python -m pip install -r submission_root/requirements.txt
	./venv/bin/python -m pip install -r config/requirements-dev.txt
	./venv/bin/pre-commit install --config config/.pre-commit-config.yaml
	./venv/bin/python test/submission_check.py

# Basic public experiment (20 clients, 20 seconds, one run).
demo-basic:
	./venv/bin/python test/run_demo.py --clients 20 --duration 20 --runs 1

# Server startup command used for local browser demonstrations.
demo-server:
	./venv/bin/python submission_root/solution_server.py --port 8765

# Report experiment (100 clients, 40 seconds, three independent runs).
demo-report:
	./venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t2

# Validate the submission structure and WebSocket startup path.
check:
	./venv/bin/python test/submission_check.py

# Also require the final submission documents and report.
check-final:
	./venv/bin/python test/submission_check.py --final

# PUBLIC_TEST.md
# T1: Basic correctness.
t1-basic:
	./venv/bin/python test/run_demo.py --clients 20 --duration 20 --runs 3 --output-stem output/t1

# T2: Public scale.
t2-scale:
	./venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t2

# T3: Slow readers.
t3-slow:
	./venv/bin/python test/run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem output/t3

# Optional T3 server memory monitor; run this in another terminal.
t3-monitor:
	./venv/bin/python test/monitor_server.py --match solution_server.py --duration 60 --output output/t3_memory.csv

# T4: Reconnect.
t4-reconnect:
	./venv/bin/python test/run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 --output-stem output/t4

# T5: Network impairment. Run t5-network-enable first and t5-network-reset afterwards.
t5-network:
	./venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem output/t5

t5-network-enable:
	sudo ./network_profiles/delay_loss.sh lo

t5-network-reset:
	sudo ./network_profiles/reset.sh lo



# DEMO_GUIDE.md
# Serve the browser demo on http://127.0.0.1:8000.
demo-browser:
	./venv/bin/python -m http.server 8000 --directory submission_root/client

# Stream deterministic captions for the long browser demonstration.
demo-caption:
	./venv/bin/python test/caption_source.py --duration 120


# experiments/README.md
# Override on the command line, e.g. `make exp-hidden RUNS=1 ONLY=H2,H4`.
RUNS ?= 3
ONLY ?=
EXP_OUT ?= output/experiments
EXP = ./venv/bin/python experiments/run_matrix.py --out $(EXP_OUT) --runs $(RUNS) $(if $(ONLY),--only $(ONLY))

# List every scenario and scenario/arm case.
exp-list:
	./venv/bin/python experiments/run_matrix.py --list

# Smoke test of all suites: shortened workloads, one run, no tc.
exp-quick:
	$(EXP) --suite all --no-tc --quick --runs 1
	$(MAKE) exp-report

# Suites without tc (safe on WSL): full local workloads.
exp-public:
	$(EXP) --suite public --no-tc
	$(MAKE) exp-report

exp-hidden:
	$(EXP) --suite hidden --no-tc
	$(MAKE) exp-report

exp-before-after:
	$(EXP) --suite before_after --no-tc
	$(MAKE) exp-report

exp-all:
	$(EXP) --suite all --no-tc
	$(MAKE) exp-report

# Only netem/tbf scenarios. Needs passwordless sudo and the sch_netem module.
exp-tc:
	sudo modprobe sch_netem
	$(EXP) --suite all --tc-only
	$(MAKE) exp-report

# H10 protocol invariants only.
exp-edge:
	$(EXP) --suite hidden --only H10 --runs 1

# Regenerate SUMMARY.md and summary.csv from stored results.
exp-report:
	./venv/bin/python experiments/report.py --out $(EXP_OUT)

# Web dashboard for graphs, results and launching experiments: http://127.0.0.1:8100
exp-ui:
	./venv/bin/python experiments/dashboard.py
