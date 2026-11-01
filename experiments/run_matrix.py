"""Run comparable workloads while retaining the supplied measurement logic.

Public runs remain owned by run_demo; custom workloads import the same analyser
and lifecycle helpers. Missing observations stay null rather than implying zero.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import csv
import json
import math
import operator
import os
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "test"))
import analyse_results
import run_demo

import tc
from scenarios import GLOBAL_CHECKS, QUICK, SCENARIOS

ABLATIONS = {
    "no-probe": "probe",
    "no-grace": "grace",
    "no-resync": "resync",
    "no-all": "all",
}
ROLE_ARGS = (
    "slow_percent",
    "slow_ms",
    "disconnect_percent",
    "disconnect_at",
    "disconnect_seconds",
    "late_percent",
    "late_seconds",
)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def nested(value: dict, path: str) -> Any:
    return run_demo.nested_value(value, path)


def medians(results: list[dict]) -> dict:
    result = {}
    for key in set().union(*(row.keys() for row in results)):
        values = [row[key] for row in results if key in row]
        if all(isinstance(v, dict) for v in values):
            result[key] = medians(values)
        else:
            numbers = [v for v in values if numeric(v)]
            result[key] = statistics.median(numbers) if numbers else None
    return result


def find_server(port: int) -> int | None:
    if not Path("/proc").is_dir():
        return None
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / "cmdline").read_bytes().decode().split("\0")
            if any(Path(a).name == "solution_server.py" for a in args):
                if "--port" in args and args[args.index("--port") + 1] == str(port):
                    return int(entry.name)
        except (OSError, ValueError, IndexError, UnicodeError):
            continue
    return None


class Sampler:
    def __init__(self, pid: Callable[[], int | None]) -> None:
        self.pid = pid
        self.rows: list[tuple[float, float, float]] = []
        self.stop = threading.Event()
        self.started = time.monotonic()
        self.last_alive: int | None = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self) -> None:
        while not self.stop.is_set():
            pid = self.pid()
            if pid is not None:
                try:
                    status = Path(f"/proc/{pid}/status").read_text()
                    stat = (
                        Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                    )
                    rss = int(re.search(r"VmRSS:\s+(\d+)", status)[1]) / 1024
                    cpu = (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")
                    self.rows.append((time.monotonic() - self.started, rss, cpu))
                    self.last_alive = int(stat[0] != "Z")
                except (OSError, ValueError, TypeError, IndexError):
                    pass
            self.stop.wait(0.5)

    def finish(self, path: Path) -> dict:
        self.stop.set()
        self.thread.join()
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["elapsed_s", "rss_mb", "cpu_seconds"])
            writer.writerows(self.rows)
        tail = [
            r
            for r in self.rows
            if r[0] >= (self.rows[-1][0] * 0.25 if self.rows else 0)
        ]
        slope = None
        if len(tail) > 1:
            xmean = statistics.mean(r[0] for r in tail)
            ymean = statistics.mean(r[1] for r in tail)
            denominator = sum((r[0] - xmean) ** 2 for r in tail)
            if denominator:
                slope = (
                    60
                    * sum((r[0] - xmean) * (r[1] - ymean) for r in tail)
                    / denominator
                )
        cpu = None
        if len(self.rows) > 1 and self.rows[-1][0] > self.rows[0][0]:
            cpu = (
                100
                * (self.rows[-1][2] - self.rows[0][2])
                / (self.rows[-1][0] - self.rows[0][0])
            )
        return {
            "peak_rss_mb": max((r[1] for r in self.rows), default=None),
            "rss_slope_mb_per_min": slope,
            "cpu_pct_mean": cpu,
        }


def csv_metrics(path: Path, scenario: dict, segments: int, start_epoch: float) -> dict:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    live = [
        r
        for r in rows
        if r["event_type"] == "CAPTION_UPDATE" and r["status"] == "accepted"
    ]
    first = min(
        (float(r["receive_ts"]) for r in rows if r["receive_ts"]), default=start_epoch
    )
    buckets = defaultdict(list)
    for row in live:
        buckets[int(float(row["receive_ts"]) - first)].append(float(row["latency_ms"]))
    timeline = [
        analyse_results.percentile(buckets[i], 95) if buckets[i] else None
        for i in range(max(buckets, default=-1) + 1)
    ]
    unaffected = [
        float(r["latency_ms"])
        for r in live
        if all(r[k] == "0" for k in ("slow", "disconnecting", "late"))
    ]
    late_ids = {r["client_id"] for r in rows if r["late"] == "1"}
    histories = {}
    for row in rows:
        if row["event_type"] == "HISTORY" and row["client_id"] in late_ids:
            histories.setdefault(row["client_id"], int(row["status"].split()[0]) > 0)
    removal = [t for t, p in scenario["tc"] if t > 0 and p == "reset"]
    removal += [t for t, ms in scenario.get("impairment", []) if ms == 0]
    converge = None
    if removal:
        removed = start_epoch + removal[-1] - first
        for i in range(max(0, math.ceil(removed)), len(timeline) - 3):
            if all(numeric(v) and v <= 500 for v in timeline[i : i + 4]):
                converge = max(0, i - removed)
                break
    normal_ids = {r["client_id"] for r in rows if r["slow"] == "0"}
    normal_updates = {
        (r["client_id"], r["segment"], r["revision"]) for r in live if r["slow"] == "0"
    }
    slow_ids = {r["client_id"] for r in rows if r["slow"] == "1"}
    final_latencies = {}
    for row in rows:
        if (
            row["client_id"] in slow_ids
            and row["final"] == "1"
            and row["status"] in ("accepted", "recovered")
        ):
            key = (row["client_id"], row["segment"])
            final_latencies[key] = min(
                float(row["latency_ms"]), final_latencies.get(key, math.inf)
            )
    end = start_epoch + scenario["duration"]
    # Source timestamps define the denominator, so dropped live revisions still count.
    tail_keys = {
        (r["segment"], r["revision"])
        for r in rows
        if r["source_ts"] and end - 10 <= float(r["source_ts"]) <= end
    }
    client_ids = {r["client_id"] for r in rows}
    tail_delivered = {
        (r["client_id"], r["segment"], r["revision"])
        for r in live
        if (r["segment"], r["revision"]) in tail_keys
    }
    return {
        "timeline_p95_ms": timeline,
        "converge_s": converge,
        "normal_unaffected_p95_ms": (
            analyse_results.percentile(unaffected, 95) if unaffected else None
        ),
        "late_history_nonempty_pct": (
            100 * sum(histories.values()) / len(late_ids) if late_ids else None
        ),
        "normal_clients": {
            "update_coverage_pct": (
                100 * len(normal_updates) / (len(normal_ids) * segments * 3)
                if normal_ids
                else None
            )
        },
        "slow_clients": {
            "fresh_final_pct": (
                100
                * sum(v <= 500 for v in final_latencies.values())
                / (len(slow_ids) * segments)
                if slow_ids
                else None
            )
        },
        "tail_update_coverage_pct": (
            100 * len(tail_delivered) / (len(client_ids) * len(tail_keys))
            if client_ids and tail_keys
            else None
        ),
    }


def merge_metrics(summary: dict, extra: dict) -> None:
    for key, value in extra.items():
        if isinstance(value, dict):
            summary.setdefault(key, {}).update(value)
        else:
            summary[key] = value


def log_metrics(path: Path, quiet: bool) -> dict:
    # run_demo's required --quiet-server redirects server output to /dev/null.
    text = path.read_text(encoding="utf-8", errors="replace")
    return {
        **{
            key: (
                None
                if quiet
                else sum(
                    int(v)
                    for line in text.splitlines()
                    if "client disconnected:" in line
                    for v in re.findall(rf"{key}=(\d+)", line)
                )
            )
            for key in ("coalesced", "dropped", "held")
        },
        "resyncs": None if quiet else text.count("client resync:"),
    }


def source_segments(scenario: dict) -> int:
    args = scenario["source_args"]
    if not args:
        return run_demo.default_source_segments(scenario["duration"], 1.0)

    def value(flag: str, default: float) -> float:
        return float(args[args.index(flag) + 1]) if flag in args else default

    return math.floor(
        max(3, scenario["duration"] - 3)
        / (2 * value("--revision-delay", 0.2) + value("--segment-gap", 0.35))
    )


def window_clients(payload: str) -> None:
    """Change only the imported client's sleep argument during an impairment."""
    import load_clients

    config = json.loads(payload)
    started = time.monotonic()
    events = config.pop("impairment")

    class WindowArgs(argparse.Namespace):
        @property
        def slow_ms(self) -> float:
            elapsed = time.monotonic() - started
            return next((ms for t, ms in reversed(events) if elapsed >= t), 0)

    asyncio.run(load_clients.main_async(WindowArgs(**config)))


def stop_group(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    if sys.platform.startswith("linux"):
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    run_demo.terminate_process(proc, "experiment process")
    if sys.platform.startswith("linux"):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def one_run(scenario: dict, arm: str, stem: Path, port: int) -> dict:
    env = os.environ.copy()
    env.pop("LIVERECALL_ABLATE", None)
    if arm in ABLATIONS:
        env["LIVERECALL_ABLATE"] = ABLATIONS[arm]
    processes = []
    sampler = None
    timeline = tc.Timeline(scenario["tc"])
    summary = None
    rss = {"peak_rss_mb": None, "rss_slope_mb_per_min": None, "cpu_pct_mean": None}
    start_epoch = time.time()
    csv_path = stem.with_suffix(".csv")
    log_path = stem.with_suffix(".log")
    segments = source_segments(scenario)
    group = {"start_new_session": True} if sys.platform.startswith("linux") else {}
    with log_path.open("w", encoding="utf-8") as log:

        def launch(command: list[str]) -> subprocess.Popen:
            proc = subprocess.Popen(
                command,
                cwd=ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                **group,
            )
            processes.append(proc)
            return proc

        try:
            timeline.prepare()
            if scenario["mode"] == "run_demo":
                command = [
                    sys.executable,
                    str(ROOT / "test/run_demo.py"),
                    "--clients",
                    str(scenario["clients"]),
                    "--duration",
                    str(scenario["duration"]),
                    "--runs",
                    "1",
                    "--quiet-server",
                    "--port",
                    str(port),
                    "--output-stem",
                    str(stem),
                ]
                for key in ROLE_ARGS:
                    command += ["--" + key.replace("_", "-"), str(scenario[key])]
                proc = launch(command)
                sampler = Sampler(lambda: find_server(port))
                sampler.thread.start()
                timeline.start(time.monotonic())
                if proc.wait(timeout=scenario["duration"] + 90):
                    raise RuntimeError("run_demo.py failed; see run log")
                summary = json.loads(
                    stem.with_suffix(".json").read_text(encoding="utf-8")
                )
                summary["server_alive"] = sampler.last_alive
            elif scenario["mode"] == "edge":
                csv_path.touch()
                proc = launch(
                    [
                        sys.executable,
                        str(ROOT / "experiments/test_protocol_edge.py"),
                        "--port",
                        str(port),
                        "--json-output",
                        str(stem.with_suffix(".json")),
                    ]
                )
                passed = int(proc.wait(timeout=90) == 0)
                summary = json.loads(
                    stem.with_suffix(".json").read_text(encoding="utf-8")
                )
                summary["edge_pass"] = int(passed and summary["edge_pass"] == 1)
            else:
                server_path = ROOT / (
                    "experiments/naive_server.py"
                    if arm == "naive"
                    else "submission_root/solution_server.py"
                )
                server = launch(
                    [sys.executable, "-u", str(server_path), "--port", str(port)]
                )
                run_demo.wait_for_port(port, server)
                sampler = Sampler(lambda: server.pid)
                sampler.thread.start()
                uri = f"ws://127.0.0.1:{port}"
                command = [
                    sys.executable,
                    str(ROOT / "test/load_clients.py"),
                    "--uri",
                    uri,
                    "--clients",
                    str(scenario["clients"]),
                    "--duration",
                    str(scenario["duration"]),
                    "--output",
                    str(csv_path),
                ]
                for key in ROLE_ARGS:
                    command += ["--" + key.replace("_", "-"), str(scenario[key])]
                if scenario.get("impairment"):
                    config = {k: scenario[k] for k in ROLE_ARGS if k != "slow_ms"}
                    config.update(
                        uri=uri,
                        clients=scenario["clients"],
                        duration=scenario["duration"],
                        output=str(csv_path),
                        session="L1",
                        seed=3103,
                        history_seconds=30,
                        impairment=scenario["impairment"],
                    )
                    command = [
                        sys.executable,
                        "-c",
                        "import sys; sys.path.insert(0, 'experiments'); from run_matrix import window_clients; window_clients(sys.argv[1])",
                        json.dumps(config),
                    ]
                start_mono, start_epoch = time.monotonic(), time.time()
                clients = launch(command)
                timeline.start(start_mono)
                time.sleep(1.0)
                if clients.poll() is not None:
                    raise RuntimeError("load_clients.py exited early")
                source = launch(
                    [
                        sys.executable,
                        str(ROOT / "test/caption_source.py"),
                        "--uri",
                        uri,
                        "--segments",
                        str(segments),
                        "--quiet",
                        *scenario["source_args"],
                    ]
                )
                if source.wait(timeout=scenario["duration"] + 60):
                    raise RuntimeError("caption_source.py failed")
                if clients.wait(timeout=scenario["duration"] + 30):
                    raise RuntimeError("load_clients.py failed")
                summary = analyse_results.analyse_file(
                    csv_path,
                    expected_finals=segments,
                    expected_updates=segments * 3,
                    fresh_threshold_ms=500,
                )
                summary["server_alive"] = int(server.poll() is None)
        finally:
            try:
                for proc in reversed(processes):
                    stop_group(proc)
            finally:
                try:
                    if sampler is not None:
                        rss = sampler.finish(stem.with_suffix(".rss.csv"))
                    else:
                        stem.with_suffix(".rss.csv").write_text(
                            "elapsed_s,rss_mb,cpu_seconds\n", encoding="utf-8"
                        )
                finally:
                    timeline.close()
        if timeline.error:
            raise RuntimeError(f"tc timeline failed: {timeline.error}")
    if summary is None:
        raise RuntimeError("run produced no summary")
    summary.update(rss)
    summary.update(log_metrics(log_path, scenario["mode"] == "run_demo"))
    if csv_path.exists() and scenario["mode"] != "edge":
        merge_metrics(summary, csv_metrics(csv_path, scenario, segments, start_epoch))
    write_json(stem.with_suffix(".json"), summary)
    return summary


def quick_scenario(original: dict) -> dict:
    scenario = copy.deepcopy(original)
    duration = min(
        scenario["duration"],
        QUICK["soak_duration"] if scenario["id"] == "H9" else QUICK["duration"],
    )
    factor = duration / scenario["duration"]
    scenario["duration"] = duration
    scenario["clients"] = min(scenario["clients"], QUICK["clients"])
    if scenario["id"].startswith("H1_c"):
        scenario["clients"] = QUICK["scale_clients"][
            (250, 500, 1000).index(original["clients"])
        ]
    for key in ("disconnect_at", "disconnect_seconds", "late_seconds"):
        scenario[key] *= factor
    scenario["tc"] = [(t * factor, profile) for t, profile in scenario["tc"]]
    if "impairment" in scenario:
        scenario["impairment"] = [(t * factor, ms) for t, ms in scenario["impairment"]]
    scenario["runs"] = QUICK["runs"]
    return scenario


def threshold_value(threshold: Any, scenario_id: str, med: dict) -> Any:
    if not isinstance(threshold, str):
        return threshold
    kind, expression = threshold.split(":", 1)
    pattern = r"(.+)\*([-+\d.]+)" if kind == "ref" else r"(.+)\+([-+\d.]+)"
    match = re.fullmatch(pattern, expression)
    if match is None:
        raise ValueError(f"invalid threshold: {threshold}")
    target, metric = match[1].split(".", 1)
    base = med.get((target, "final") if kind == "ref" else (scenario_id, target), {})
    value = nested(base, metric)
    if not numeric(value):
        return None
    return value * float(match[2]) if kind == "ref" else value + float(match[2])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite", choices=("public", "before_after", "hidden", "all"), default="all"
    )
    parser.add_argument("--only", default="")
    parser.add_argument("--runs", type=int)
    parser.add_argument("--quick", action="store_true")
    tc_selection = parser.add_mutually_exclusive_group()
    tc_selection.add_argument("--no-tc", action="store_true")
    tc_selection.add_argument("--tc-only", action="store_true")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--out", type=Path, default=Path("output/experiments"))
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.runs is not None and args.runs < 1:
        parser.error("--runs must be >= 1")
    selected = [
        s
        for s in SCENARIOS
        if (args.suite == "all" or s["suite"] == args.suite)
        and (not args.only or any(s["id"].startswith(p) for p in args.only.split(",")))
        and (not args.tc_only or s["needs_tc"])
    ]
    if args.list:
        for scenario in selected:
            print(scenario["id"])
        print(
            f"{len(selected)} scenarios; {sum(len(s['arms']) for s in selected)} scenario/arm cases"
        )
        return 0
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    med = {}
    for scenario in SCENARIOS:
        for arm in scenario["arms"]:
            path = args.out / scenario["id"] / f"{arm}_median.json"
            if path.exists():
                med[scenario["id"], arm] = json.loads(path.read_text(encoding="utf-8"))
    verdicts = []

    def record(
        scenario: dict,
        arm: str,
        check: str,
        verdict: str,
        value: Any = None,
        threshold: Any = None,
    ) -> None:
        verdicts.append(
            dict(
                id=scenario["id"],
                suite=scenario["suite"],
                component=scenario["component"],
                arm=arm,
                check=check,
                value=value,
                threshold=threshold,
                verdict=verdict,
            )
        )
        print(f"{scenario['id']:<24} {arm:<10} {verdict:<5} {check}", flush=True)

    tc_ok = None
    interrupted = False
    completed = set()
    try:
        for original in selected:
            scenario = (
                quick_scenario(original) if args.quick else copy.deepcopy(original)
            )
            directory = args.out / scenario["id"]
            directory.mkdir(parents=True, exist_ok=True)
            if scenario["needs_tc"] and tc_ok is None:
                tc_ok = False if args.no_tc else tc.available()
            reason = None
            if scenario["needs_tc"] and not tc_ok:
                reason = "tc unavailable or --no-tc"
            if (
                scenario["mode"] == "edge"
                and not (ROOT / "experiments/test_protocol_edge.py").exists()
            ):
                reason = "protocol edge script not present"
            for arm in scenario["arms"]:
                if reason:
                    record(scenario, arm, reason, "SKIP")
                    continue
                key = (scenario["id"], arm)
                med.pop(key, None)
                median_path = directory / f"{arm}_median.json"
                if scenario["suite"] == "before_after" and arm == "final":
                    reused = med.get((scenario["reuse"], "final"))
                    if reused is not None:
                        med[key] = reused
                        write_json(median_path, reused)
                        record(scenario, arm, f"reused {scenario['reuse']}", "PASS")
                        continue
                results = []
                for run in range(1, (args.runs or scenario["runs"]) + 1):
                    stem = directory / f"{arm}_run{run:02d}"
                    print(f"RUN {scenario['id']} {arm} {run}", flush=True)
                    try:
                        results.append(one_run(scenario, arm, stem, args.port))
                    except Exception as exc:
                        write_json(stem.with_suffix(".json"), {"error": str(exc)})
                        record(scenario, arm, f"run {run}: {exc}", "ERROR")
                if results:
                    med[key] = medians(results)
                    write_json(median_path, med[key])
                    completed.add(key)
                else:
                    median_path.unlink(missing_ok=True)
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; processes and network timelines have been stopped.")
    operations = {"<=": operator.le, ">=": operator.ge, "==": operator.eq}
    for scenario in selected:
        key = (scenario["id"], "final")
        if scenario["suite"] == "before_after" or key not in completed:
            continue
        checks = scenario["checks"] + (
            GLOBAL_CHECKS if scenario["suite"] == "hidden" else []
        )
        if not checks:
            record(scenario, "final", "descriptive metrics collected", "PASS")
        for metric, op, threshold in checks:
            value = nested(med[key], metric)
            resolved = threshold_value(threshold, scenario["id"], med)
            verdict = (
                "SKIP"
                if not numeric(value) or not numeric(resolved)
                else ("PASS" if operations[op](value, resolved) else "FAIL")
            )
            if not numeric(value) and numeric(resolved):
                observation_missing = metric in (
                    "peak_rss_mb",
                    "rss_slope_mb_per_min",
                    "cpu_pct_mean",
                )
                no_unaffected = metric == "normal_unaffected_p95_ms" and (
                    scenario["slow_percent"]
                    + scenario["disconnect_percent"]
                    + scenario["late_percent"]
                    >= 100
                )
                edge_global = scenario["mode"] == "edge" and metric not in (
                    "edge_pass",
                    "server_alive",
                )
                verdict = (
                    "SKIP"
                    if observation_missing or no_unaffected or edge_global
                    else "FAIL"
                )
            if scenario["informational"] and verdict == "FAIL":
                verdict = "SKIP"
            record(
                scenario,
                "final",
                f"{metric} {op} {threshold}",
                verdict,
                value,
                resolved,
            )
    write_json(args.out / "verdicts.json", verdicts)
    informational = {s["id"] for s in selected if s["informational"]}
    return (
        130
        if interrupted
        else int(
            any(
                v["verdict"] in ("FAIL", "ERROR") and v["id"] not in informational
                for v in verdicts
            )
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
