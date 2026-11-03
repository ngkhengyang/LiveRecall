#!/usr/bin/env python3
"""Compare the final design with and without its probe loop."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
from importlib import metadata
import json
import math
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
PYTHON = REPO / "venv/bin/python"
ARMS = ("with_probe", "no_probe")
TESTS = {
    "t1": ["--clients", "20", "--duration", "20"],
    "t2": ["--clients", "100", "--duration", "40"],
    "t3": [
        "--clients",
        "100",
        "--duration",
        "60",
        "--slow-percent",
        "10",
        "--slow-ms",
        "500",
    ],
    "t4": [
        "--clients",
        "100",
        "--duration",
        "60",
        "--disconnect-percent",
        "20",
        "--disconnect-at",
        "15",
        "--disconnect-seconds",
        "5",
    ],
    "t5": ["--clients", "100", "--duration", "40"],
}
ANCHOR = """            self._tasks = [
                asyncio.create_task(self._sender_loop()),
                asyncio.create_task(self._probe_loop()),
            ]"""
REPLACEMENT = """            # Ablation: never start the probe loop.
            self._tasks = [asyncio.create_task(self._sender_loop())]"""
METRICS = [
    ("completeness %", "final_completeness_pct"),
    ("normal update coverage %", "normal_update_coverage_pct"),
    ("P50 ms", "p50_ms"),
    ("P95 ms", "p95_ms"),
    ("P99 ms", "p99_ms"),
    ("fresh %", "fresh_pct"),
    ("fresh-final %", "fresh_final_pct"),
    ("duplicates", "duplicates"),
    ("stale", "stale"),
    ("connection errors", "connection_errors"),
    ("normal-client P95 ms", "normal_clients.p95_ms"),
    ("slow-client P95 ms", "slow_clients.p95_ms"),
    ("slow-client fresh-final %", "slow_clients.fresh_final_pct"),
    ("slow-client completeness %", "slow_clients.final_completeness_pct"),
    ("recovery P95 ms", "recovery_p95_ms"),
]
SKIPPED = "T5: SKIPPED (no passwordless sudo / sch_netem)"


def validate_out(path):
    """Require an output directory strictly below the repository output folder."""
    path = path.resolve()
    output = (REPO / "output").resolve()
    if not output.is_relative_to(REPO.resolve()) or output == REPO.resolve():
        raise ValueError("repo output/ must resolve inside the repository")
    if not path.is_relative_to(output) or path == output:
        raise ValueError("--out must resolve strictly inside the repo's output/ folder")
    return path


def remove_variant(path, out):
    """Delete only a variant strictly inside a validated output directory."""
    out = validate_out(out)
    resolved = path.resolve()
    if resolved == out or not resolved.is_relative_to(out):
        raise ValueError(f"Refusing deletion outside --out: {path}")
    if path.name not in ARMS or path.parent.resolve() != out:
        raise ValueError(f"Refusing deletion of a non-variant directory: {path}")
    if path.exists():
        shutil.rmtree(path)


def patch_client(path):
    """Remove exactly one probe-start block, failing on source drift."""
    source = path.read_text(encoding="utf-8")
    count = source.count(ANCHOR)
    if count != 1:
        raise ValueError(
            f"Probe patch anchor must occur exactly once in {path}; found {count}"
        )
    path.write_text(source.replace(ANCHOR, REPLACEMENT), encoding="utf-8")


def build_variants(out):
    """Refresh copies without touching the original submission or tests."""
    for arm in ARMS:
        variant = out / arm
        remove_variant(variant, out)
        for directory in ("submission_root", "test"):
            shutil.copytree(
                REPO / directory,
                variant / directory,
                ignore=shutil.ignore_patterns("__pycache__"),
            )
        (variant / "output").mkdir()
    patch_client(out / "no_probe/submission_root/app/client.py")


def write_configuration(out, args, tests):
    """Save the code identity and invocation needed to reproduce this run."""
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True
    )
    config = {
        "git_commit": commit.stdout.strip() if commit.returncode == 0 else None,
        "client_sha256": {
            arm: hashlib.sha256(
                (out / arm / "submission_root/app/client.py").read_bytes()
            ).hexdigest()
            for arm in ARMS
        },
        "python": sys.version,
        "python_executable": str(PYTHON),
        "websockets": metadata.version("websockets"),
        "test_args": {test: TESTS[test] for test in tests},
        "runs": args.runs,
        "port": args.port,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "outcomes": {},
    }
    save_configuration(out, config)
    return config


def save_configuration(out, config):
    (out / "configuration.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8"
    )


def values(runs, key):
    """Read finite numeric values, including nested role metrics."""
    result = []
    for run in runs:
        value = run
        for part in key.split("."):
            value = value.get(part) if isinstance(value, dict) else None
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        ):
            result.append(value)
    return result


def load_runs(out, test, arm):
    directory = out / arm / "output"
    paths = sorted(directory.glob(f"{test}_run[0-9][0-9]*.json"))
    # run_demo omits the run suffix for --runs 1.
    if not paths and (directory / f"{test}.json").exists():
        paths = [directory / f"{test}.json"]
    return [json.loads(path.read_text(encoding="utf-8")) for path in paths]


def fmt(value):
    return "—" if value is None else f"{value:.2f}"


def median(items):
    return statistics.median(items) if items else None


def table(test, runs):
    lines = [
        f"\n### {test.upper()} (runs: with={len(runs[ARMS[0]])}, without={len(runs[ARMS[1]])})",
        "",
        "| Metric | With probe median | Without probe median | Per-run with | Per-run without |",
        "|---|---:|---:|---|---|",
    ]
    rows = []
    for label, key in METRICS:
        a, b = (values(runs[arm], key) for arm in ARMS)
        if not a and not b:
            continue
        lines.append(
            f"| {label} | {fmt(median(a))} | {fmt(median(b))} | {', '.join(map(fmt, a)) or '—'} | {', '.join(map(fmt, b)) or '—'} |"
        )
        rows.append([test, key, median(a), median(b), json.dumps(a), json.dumps(b)])
    return "\n".join(lines), rows


def separated(a, b):
    return bool(a and b and (max(a) < min(b) or max(b) < min(a)))


def verdict(test, runs, expected):
    if any(len(runs[arm]) != expected for arm in ARMS):
        return f"{test.upper()}: incomplete results; no verdict"
    if expected < 2:
        return f"{test.upper()}: single run per arm; no verdict (use --runs 3)"
    changed = []
    for label, key in METRICS:
        a, b = (values(runs[arm], key) for arm in ARMS)
        if len(a) == expected and len(b) == expected and separated(a, b):
            delta = median(a) - median(b)
            unit = (
                " pp" if key.endswith("_pct") else " ms" if key.endswith("_ms") else ""
            )
            changed.append(
                f"{label.removesuffix(' %').removesuffix(' ms')} by {delta:+.2f}{unit}"
            )
    if changed:
        return f"{test.upper()}: probing changes " + "; ".join(changed)
    return f"{test.upper()}: no difference beyond run-to-run spread"


def section5(runs):
    lines = [
        "\n## Section 5 — T3",
        "",
        "Change = with probe minus without probe; percentage changes are percentage points.",
        "",
        "| Metric | With probe | Without probe | Change |",
        "|---|---:|---:|---:|",
    ]
    for label, key, unit in [
        ("Final completeness", "final_completeness_pct", "%"),
        ("P99 age", "p99_ms", "ms"),
        ("Slow-client P95", "slow_clients.p95_ms", "ms"),
        ("Normal-client P95", "normal_clients.p95_ms", "ms"),
    ]:
        a, b = (median(values(runs[arm], key)) for arm in ARMS)
        change = (
            "—"
            if a is None or b is None
            else f"{a - b:+.2f} {'pp' if unit == '%' else unit}"
        )
        lines.append(f"| {label} | {fmt(a)} {unit} | {fmt(b)} {unit} | {change} |")
    return "\n".join(lines)


def summarise(out, tests, config):
    """Print and save all medians, per-run values, and range-based verdicts."""
    lines = [
        "# Probe comparison",
        "",
        "Medians across independent runs; change is with probe minus without probe.",
    ]
    rows, verdicts = [], []
    for test in tests:
        outcome = config.get("outcomes", {}).get(test, {})
        if outcome.get("skipped"):
            lines.append("\n" + SKIPPED)
            verdicts.append(SKIPPED)
            continue
        runs = {arm: load_runs(out, test, arm) for arm in ARMS}
        content, test_rows = table(test, runs)
        lines.append(content)
        rows.extend(test_rows)
        if any(outcome.get(arm, 0) != 0 for arm in ARMS):
            verdicts.append(f"{test.upper()}: run failed; no verdict")
        else:
            verdicts.append(verdict(test, runs, config["runs"]))
    t3 = {arm: load_runs(out, "t3", arm) for arm in ARMS}
    lines.append(section5(t3))
    if "t3" not in tests or any(len(t3[arm]) != config["runs"] for arm in ARMS):
        lines.append(
            "\nT3 results unavailable or incomplete; rerun T3 before using this table."
        )
    lines.extend(["\n## Verdicts", ""] + verdicts)
    lines.append(
        "\nRange separation is descriptive, not a significance test; one run cannot estimate spread."
    )
    content = "\n".join(lines) + "\n"
    (out / "SUMMARY.md").write_text(content, encoding="utf-8")
    with (out / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "test",
                "metric",
                "with_probe_median",
                "no_probe_median",
                "with_probe_runs",
                "no_probe_runs",
            ]
        )
        writer.writerows(rows)
    print(content, flush=True)
    print(
        f"Saved: {out / 'SUMMARY.md'}\nSaved: {out / 'summary.csv'}\nConfig: {out / 'configuration.json'}",
        flush=True,
    )


def elapsed(seconds):
    seconds = int(seconds)
    return f"{seconds // 60}m{seconds % 60:02d}s"


def run_arms(out, test, args, index, total):
    outcomes = {}
    for offset, arm in enumerate(ARMS, 1):
        variant = out / arm
        print(f"[{index + offset}/{total}] {test.upper()} {arm} … ", end="", flush=True)
        started = time.monotonic()
        with (variant / "output" / f"{test}.log").open("w", encoding="utf-8") as log:
            try:
                result = subprocess.run(
                    [
                        str(PYTHON),
                        "-u",
                        "test/run_demo.py",
                        *TESTS[test],
                        "--runs",
                        str(args.runs),
                        "--quiet-server",
                        "--port",
                        str(args.port),
                        "--output-stem",
                        f"output/{test}",
                    ],
                    cwd=variant,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                outcomes[arm] = result.returncode
            except OSError as exc:
                log.write(str(exc) + "\n")
                outcomes[arm] = 1
        status = (
            "ok"
            if outcomes[arm] == 0
            else f"FAILED exit={outcomes[arm]} (see {variant / 'output' / (test + '.log')})"
        )
        print(f"done in {elapsed(time.monotonic() - started)} ({status})", flush=True)
    print(table(test, {arm: load_runs(out, test, arm) for arm in ARMS})[0], flush=True)
    return outcomes


def network_command(arguments, log):
    try:
        return (
            subprocess.run(
                ["sudo", "-n", *arguments],
                cwd=REPO,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
            ).returncode
            == 0
        )
    except FileNotFoundError as exc:
        log.write(str(exc) + "\n")
        return False


def run_t5(out, args, index, total):
    """Apply netem when available and always attempt its reset, even on Ctrl-C."""
    with (out / "t5_network.log").open("w", encoding="utf-8") as log:
        if not network_command(["tc", "qdisc", "show", "dev", "lo"], log):
            print(SKIPPED, flush=True)
            return {"skipped": True}
        try:
            # delay_loss.sh deletes any existing root qdisc first, so reset afterwards
            # even when applying netem itself fails part-way.
            if not network_command(
                ["bash", str(REPO / "network_profiles/delay_loss.sh"), "lo"], log
            ):
                print(SKIPPED, flush=True)
                return {"skipped": True}
            return run_arms(out, "t5", args, index, total)
        finally:
            if not network_command(
                ["bash", str(REPO / "network_profiles/reset.sh"), "lo"], log
            ):
                print(
                    f"T5: network reset failed; see {out / 't5_network.log'}",
                    flush=True,
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests", default=",".join(TESTS))
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--port", type=int, default=8811)
    parser.add_argument("--out", type=Path, default=Path("output/probe_ab"))
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args()
    tests = args.tests.split(",")
    if (
        not tests
        or any(test not in TESTS for test in tests)
        or len(set(tests)) != len(tests)
    ):
        parser.error(
            "--tests must be a comma-separated list of distinct t1,t2,t3,t4,t5"
        )
    if args.runs < 1 or not 1 <= args.port <= 65535:
        parser.error("--runs must be positive and --port must be in 1..65535")
    out = validate_out(args.out if args.out.is_absolute() else REPO / args.out)
    if args.summary_only:
        config = json.loads((out / "configuration.json").read_text(encoding="utf-8"))
        selected = list(config["test_args"]) if args.tests == ",".join(TESTS) else tests
        summarise(out, selected, config)
        return int(
            any(
                code != 0
                for outcome in config.get("outcomes", {}).values()
                for arm, code in outcome.items()
                if arm in ARMS
            )
        )
    out.mkdir(parents=True, exist_ok=True)
    build_variants(out)
    config = write_configuration(out, args, tests)
    # Report sandbox restrictions directly, before run_demo hides server output.
    try:
        with socket.socket() as listener:
            # Match asyncio's server bind so TIME_WAIT from a previous run passes.
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", args.port))
    except PermissionError as exc:
        raise RuntimeError(
            "Localhost sockets/ports are blocked by the sandbox; stopping."
        ) from exc
    for index, test in enumerate(tests):
        outcome = (
            run_t5(out, args, index * 2, len(tests) * 2)
            if test == "t5"
            else run_arms(out, test, args, index * 2, len(tests) * 2)
        )
        config["outcomes"][test] = outcome
        save_configuration(out, config)
    summarise(out, tests, config)
    return int(
        any(
            code != 0
            for outcome in config["outcomes"].values()
            for arm, code in outcome.items()
            if arm in ARMS
        )
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
