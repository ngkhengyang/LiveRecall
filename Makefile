SHELL := /bin/bash

.PHONY: install server demo report-demo check check-final

install:
	sudo apt update
	sudo apt install -y python3 python3-venv python3-pip iproute2
	python3 -m venv venv
	./venv/bin/python -m pip install -r submission_root/requirements.txt
	./venv/bin/python -m pip install -r config/requirements-dev.txt
	./venv/bin/pre-commit install --config config/.pre-commit-config.yaml
	./venv/bin/python test/submission_check.py

# Basic public experiment (20 clients, 20 seconds, one run).
demo:
	./venv/bin/python test/run_demo.py --clients 20 --duration 20 --runs 1

# Server startup command used for local browser demonstrations.
server:
	./venv/bin/python submission_root/solution_server.py --port 8765

# Report experiment (100 clients, 40 seconds, three independent runs).
report-demo:
	./venv/bin/python test/run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2

# Validate the submission structure and WebSocket startup path.
check:
	./venv/bin/python test/submission_check.py

# Also require the final submission documents and report.
check-final:
	./venv/bin/python test/submission_check.py --final
