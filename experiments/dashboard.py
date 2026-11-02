"""Local web dashboard for the LiveRecall experiments.

Serves experiments/dashboard/index.html plus a small JSON API that reads stored
results (medians, verdicts, per-run CSVs) and can launch run_matrix.py.

    python experiments/dashboard.py            # http://127.0.0.1:8100
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))
from scenarios import SCENARIOS  # noqa: E402
import testcases  # noqa: E402

OUTPUT = ROOT / "output"
VENV_PYTHON = ROOT / "venv/bin/python"
PYTHON = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
ARMS = ("final", "naive", "no-probe", "no-grace", "no-resync", "no-all")
CACHE: dict = {}


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100
    lo, hi = math.floor(k), math.ceil(k)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def safe_out(name: str) -> Path:
    """Result folders must stay inside output/."""
    path = (ROOT / name).resolve()
    if OUTPUT.resolve() not in path.parents and path != OUTPUT.resolve():
        raise ValueError("out must be inside output/")
    return path


def result_dirs() -> list[str]:
    found = []
    for path in sorted(OUTPUT.glob("**/")):
        if any(path.glob("*/*_median.json")) or (path / "verdicts.json").exists():
            found.append(str(path.relative_to(ROOT)))
    return found


def scenarios_payload() -> list[dict]:
    keys = (
        "id suite component needs_tc mode clients duration slow_percent slow_ms "
        "disconnect_percent disconnect_seconds late_percent late_seconds tc arms "
        "informational checks"
    ).split()
    return [{k: s.get(k) for k in keys} for s in SCENARIOS]


def results_payload(out: Path) -> dict:
    medians: dict = {}
    for scenario in SCENARIOS:
        folder = out / scenario["id"]
        for arm in ARMS:
            path = folder / f"{arm}_median.json"
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                except ValueError:
                    continue
                data.pop("timeline_p95_ms", None)
                data["_runs"] = len(list(folder.glob(f"{arm}_run*.json")))
                medians.setdefault(scenario["id"], {})[arm] = data
    verdicts = []
    path = out / "verdicts.json"
    if path.exists():
        verdicts = json.loads(path.read_text(encoding="utf-8"))
    return {"medians": medians, "verdicts": verdicts}


def run_detail(out: Path, scenario: str, arm: str, run: int) -> dict:
    folder = out / scenario
    csv_path = folder / f"{arm}_run{run:02d}.csv"
    if not csv_path.exists():
        return {"error": f"no such run: {csv_path.name}"}
    key = (str(csv_path), csv_path.stat().st_mtime)
    if key in CACHE:
        return CACHE[key]
    with csv_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    stamps = [float(r["receive_ts"]) for r in rows if r["receive_ts"]]
    stamps += [float(r["source_ts"]) for r in rows if r["source_ts"]]
    t0 = min(stamps, default=0.0)
    live = [
        r
        for r in rows
        if r["event_type"] == "CAPTION_UPDATE" and r["status"] == "accepted"
    ]
    groups = {"normal": {}, "slow": {}}
    for row in live:
        group = "slow" if row["slow"] == "1" else "normal"
        sec = int(float(row["receive_ts"]) - t0)
        groups[group].setdefault(sec, []).append(float(row["latency_ms"]))
    last = max([s for g in groups.values() for s in g], default=-1)
    timeline = {"t": list(range(last + 1))}
    for name, buckets in groups.items():
        timeline[f"{name}_p50"] = [
            percentile(buckets.get(i, []), 50) for i in timeline["t"]
        ]
        timeline[f"{name}_p95"] = [
            percentile(buckets.get(i, []), 95) for i in timeline["t"]
        ]
        timeline[f"{name}_count"] = [len(buckets.get(i, [])) for i in timeline["t"]]
    # Log-spaced bins from 1 ms to 100 s.
    edges = [10 ** (i / 4) for i in range(0, 21)]
    hist = {"edges": edges, "normal": [0] * 20, "slow": [0] * 20}
    for name, buckets in groups.items():
        for values in buckets.values():
            for v in values:
                idx = min(19, max(0, int(math.log10(max(v, 1)) * 4)))
                hist[name][idx] += 1
    events = []
    for row in rows:
        kind = row["event_type"]
        if kind == "FORCED_DISCONNECT" or (
            kind == "CONNECT" and row["status"] == "reconnect"
        ):
            if row["receive_ts"]:
                events.append({"t": float(row["receive_ts"]) - t0, "kind": kind})
    events_by_sec: dict = {}
    for ev in events:
        k = (int(ev["t"]), ev["kind"])
        events_by_sec[k] = events_by_sec.get(k, 0) + 1
    rss = []
    rss_path = folder / f"{arm}_run{run:02d}.rss.csv"
    if rss_path.exists():
        with rss_path.open(newline="", encoding="utf-8") as stream:
            rss = [
                [float(r["elapsed_s"]), float(r["rss_mb"]), float(r["cpu_seconds"])]
                for r in csv.DictReader(stream)
            ]
    stats = {}
    for name, buckets in groups.items():
        values = [v for b in buckets.values() for v in b]
        stats[name] = {
            "n": len(values),
            "p50": percentile(values, 50),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
            "max": max(values, default=None),
        }
    result = {
        "timeline": timeline,
        "hist": hist,
        "events": [
            {"t": k[0], "kind": k[1], "n": n} for k, n in sorted(events_by_sec.items())
        ],
        "rss": rss,
        "stats": stats,
        "runs": sorted(
            int(p.stem.split("_run")[1]) for p in folder.glob(f"{arm}_run*.json")
        ),
    }
    CACHE[key] = result
    return result


class Runner:
    """At most one run_matrix.py child at a time; its log is kept in memory."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.lines: list[str] = []
        self.command: list[str] = []
        self.started = 0.0
        self.finished = 0.0
        self.lock = threading.Lock()

    def start(self, opts: dict) -> dict:
        with self.lock:
            if self.proc and self.proc.poll() is None:
                return {"error": "a run is already in progress"}
            out = safe_out(opts.get("out") or "output/experiments-ui")
            cmd = [PYTHON, str(HERE / "run_matrix.py"), "--out", str(out)]
            cmd += ["--port", str(int(opts.get("port") or 8790))]
            cmd += ["--suite", opts.get("suite", "all")]
            if opts.get("only"):
                cmd += ["--only", opts["only"]]
            if opts.get("runs"):
                cmd += ["--runs", str(int(opts["runs"]))]
            arms = [a for a in opts.get("arms", []) if a in ARMS]
            if arms:
                cmd += ["--arms", *arms]
            if opts.get("quick"):
                cmd.append("--quick")
            if opts.get("tc") == "only":
                cmd.append("--tc-only")
            elif opts.get("tc") != "yes":
                cmd.append("--no-tc")
            self.lines = []
            self.command = cmd
            self.started, self.finished = time.time(), 0.0
            self.proc = subprocess.Popen(
                cmd,
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            threading.Thread(target=self.pump, args=(self.proc,), daemon=True).start()
            return {"ok": True, "out": str(out.relative_to(ROOT))}

    def pump(self, proc: subprocess.Popen) -> None:
        for line in proc.stdout:
            self.lines.append(line.rstrip("\n"))
        proc.wait()
        self.finished = time.time()
        report = [PYTHON, str(HERE / "report.py"), "--out", self.command[3]]
        if proc.returncode in (0, 1):
            subprocess.run(report, cwd=ROOT, capture_output=True)

    def stop(self) -> dict:
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGINT)
            return {"ok": True}
        return {"error": "nothing running"}

    def status(self, since: int) -> dict:
        running = bool(self.proc and self.proc.poll() is None)
        return {
            "running": running,
            "returncode": None if running or not self.proc else self.proc.returncode,
            "command": " ".join(self.command),
            "elapsed": (
                (time.time() - self.started)
                if running
                else (self.finished - self.started if self.started else 0)
            ),
            "total": len(self.lines),
            "lines": self.lines[since:],
        }


RUNNER = Runner()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:  # keep the terminal for run output
        pass

    def send(
        self, body: bytes, ctype: str = "application/json", code: int = 200
    ) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def json(self, value, code: int = 200) -> None:
        self.send(json.dumps(value).encode(), code=code)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        try:
            if url.path in ("/", "/index.html"):
                self.send(
                    (HERE / "dashboard/index.html").read_bytes(),
                    "text/html; charset=utf-8",
                )
            elif url.path == "/api/scenarios":
                self.json(
                    {
                        "scenarios": scenarios_payload(),
                        "dirs": result_dirs(),
                        "tc_note": "tc scenarios need sudo + netem",
                    }
                )
            elif url.path == "/api/testcases":
                self.json({"families": testcases.payload(), "arms": testcases.ARMS})
            elif url.path == "/api/results":
                self.json(results_payload(safe_out(q.get("out", "output/experiments"))))
            elif url.path == "/api/detail":
                self.json(
                    run_detail(
                        safe_out(q["out"]), q["id"], q["arm"], int(q.get("run", 1))
                    )
                )
            elif url.path == "/api/run/status":
                self.json(RUNNER.status(int(q.get("since", 0))))
            else:
                self.send(b"not found", "text/plain", 404)
        except (KeyError, ValueError, OSError) as exc:
            self.json({"error": str(exc)}, 400)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/api/run":
                self.json(RUNNER.start(body))
            elif self.path == "/api/run/stop":
                self.json(RUNNER.stop())
            else:
                self.send(b"not found", "text/plain", 404)
        except (ValueError, OSError) as exc:
            self.json({"error": str(exc)}, 400)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"LiveRecall dashboard: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        if RUNNER.proc and RUNNER.proc.poll() is None:
            RUNNER.stop()


if __name__ == "__main__":
    main()
