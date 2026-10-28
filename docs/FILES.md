# Package Contents

- `submission_root/solution_server.py` - working naive broadcaster baseline; graded mechanisms intentionally absent.
- `test/caption_source.py` - deterministic revision source; exact-segment and long/demo modes.
- `test/load_clients.py` - scalable clients with role labels, finalized resume state, and history duplicate/stale checks.
- `test/run_demo.py` - independent public runs + median summary.
- `test/analyse_results.py` - correctness/freshness/coverage, slow-reader split, and recovery timing.
- `test/monitor_server.py` - optional Ubuntu RSS memory observation helper.
- `test/sample_transcript.txt` - sample caption text for the deterministic source.
- `submission_root/client/` - working naive visual browser skeleton.
- `network_profiles/` - public `tc/netem` examples.
- `START_HERE.md`, `DEMO_GUIDE.md`, `PUBLIC_TESTS.md`, `PROTOCOL_EXAMPLES.md`, `NETWORKING_PRIMER.md`, `UBUNTU_SETUP.md` - student guides.
- `test/submission_check.py` - startup/WebSocket compatibility checker; `--final` checks final deliverables.
- `README_TEMPLATE.md`, `AI_USAGE_TEMPLATE.md`, `CONTRIBUTIONS_TEMPLATE.md` - submission templates.
- `bonus_asr/` - optional-only bonus guidance.
