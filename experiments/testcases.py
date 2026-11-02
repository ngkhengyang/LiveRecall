"""Single source for test-case descriptions: feeds docs/TESTCASES.md and the dashboard.

    python experiments/testcases.py          # rewrites docs/TESTCASES.md

Parameters are read from scenarios.py, so numbering and workloads cannot drift;
only the prose per family lives here.
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))
from scenarios import SCENARIOS  # noqa: E402

OUT = HERE.parent / "docs" / "TESTCASES.md"

NETEM = {
    "delay_loss": "120 ms delay (±30 ms) plus 3% packet loss",
    "loss_3pct": "3% packet loss",
    "delay_100ms": "100 ms delay (±20 ms)",
    "reorder": "20 ms delay with 20% packet reordering",
    "bandwidth_2mbit": "bandwidth capped at 2 Mbit/s",
}

# prefix -> (title, what it tests, what a good result looks like)
FAMILIES = {
    "T1": (
        "Basic correctness",
        "A small, healthy deployment (20 clients, 20 s). Checks the protocol basics: every final caption arrives, nothing is duplicated or stale, and revisions replace earlier ones.",
        "100% final completeness, 0 duplicates, 0 stale, >=99% update coverage.",
    ),
    "T2": (
        "Public scale",
        "100 clients on a clean network for 40 s. The baseline for latency numbers.",
        "Low P50/P95/P99 caption age, high fresh-caption ratio, completeness near 100%.",
    ),
    "T3": (
        "Slow readers",
        "10% of 100 clients read slowly (500 ms between reads). A slow reader must not make normal readers stale or let server memory grow without bound.",
        "Normal clients keep low latency; RSS stays flat; slow clients still get their finals.",
    ),
    "T4": (
        "Reconnect",
        "20% of clients are disconnected at t=15 s for 5 s and then reconnect. They must recover the finals they missed through HISTORY without duplicates.",
        "100% final completeness, 0 duplicates. A fast but empty HISTORY is not correct recovery.",
    ),
    "T5": (
        "Network impairment (tc/netem)",
        "Same 100-client workload as T2 with a network fault injected on the loopback interface. TCP turns loss into retransmission delay, so expect larger tail latency rather than lost messages. T5_none is the clean control for comparison.",
        "100% final completeness under every profile. Needs Linux with sudo and the sch_netem module; otherwise recorded as SKIP.",
    ),
    "H1": (
        "Scale / normal freshness",
        "Clean network with 250, 500 and 1000 clients. Checks that freshness does not collapse as the number of connections grows.",
        ">=99% normal update coverage, P95 <= 500 ms, no connection errors.",
    ),
    "H_base": (
        "Baseline reference",
        "A plain 100-client run used as the reference that H2 compares normal-client latency against.",
        "Not judged by itself; it is the yardstick.",
    ),
    "H2": (
        "Challenge 1: slow-reader sweep",
        "A matrix of slow-client share (10/30/50%) and slow-read delay (100/250/500/1000 ms). Run for both our server (final) and the starter server (naive) to show the benefit of per-client buffering and coalescing.",
        "Normal-client P95 stays within 1.5x of H_base and under 500 ms; slow clients receive at least as many fresh finals as with the naive server (>=50% when delay <= 500 ms).",
    ),
    "H3": (
        "Challenge 2: constrained bandwidth",
        "Link capped at 2 Mbit, 1 Mbit and 512 kbit with a token-bucket filter. The server must drop or merge stale provisionals so finals still get through. Compared against the naive server.",
        "Fresh-final percentage at least the naive server's (>=80% at 2 Mbit). Needs tc.",
    ),
    "H4": (
        "Reconnect & catch-up sweep",
        "50% or 100% of clients disconnect for 2, 10, 30 or 70 s, then resume. The 70 s case is informational because it exceeds the 30 s history window.",
        "Recovery P95 <= 1 s; clients that never dropped are unaffected (P95 <= 500 ms).",
    ),
    "H5": (
        "Late joiners",
        "20% of clients connect 10 s or 30 s into the stream and must be given recent history on arrival.",
        "Recovery P95 <= 1 s; every late client gets a non-empty history.",
    ),
    "H6": (
        "Fast source",
        "The caption source emits revisions much faster (20 ms between revisions, 50 ms between segments), stressing coalescing while keeping normal clients current.",
        ">=95% update coverage for normal clients.",
    ),
    "H7": (
        "Delay/loss tail freshness",
        "Network delay and loss profiles judged on P95 freshness (limits: delay_loss 520 ms, loss_3pct 400 ms, delay_100ms 500 ms). Needs tc.",
        "P95 under the per-profile limit.",
    ),
    "H8": (
        "Rapid convergence",
        "A fault is applied at t=10 s and removed at t=30 s; the system must return to fresh (<500 ms P95) quickly. H8_bandwidth throttles the link to 512 kbit (needs tc); H8_slow makes 30% of clients slow, then fast again.",
        "Converges within 3 s of the fault clearing; >=95% coverage of updates in the final 10 s.",
    ),
    "H9": (
        "Boundedness soak",
        "5-minute run with 30% slow readers. Memory must not creep up over time.",
        "RSS slope <= 1 MB/min.",
    ),
    "H10": (
        "Protocol invariants",
        "Six checks against a live server using hand-crafted messages: unknown fields tolerated, SOURCE_DONE handling, malformed JSON and unknown hello roles, duplicate revisions, stale or post-final revisions, and resume without replaying the held final.",
        "All edge checks pass (edge_pass = 1).",
    ),
    "H11": (
        "Everything at once",
        "500 clients with delay/loss, 10% slow readers (250 ms), 20% disconnecting for 10 s, and a fast source. Needs tc.",
        "Normal-client P95 <= 1 s, recovery P95 <= 2 s.",
    ),
    "BA": (
        "Before / after (ablations)",
        "Reuses the workload of T3, T4, T5_bandwidth_2mbit and H2_s50_m1000 and runs six arms to quantify what each mechanism buys: final (everything on), naive (starter server), no-probe, no-grace, no-resync, no-all.",
        "Final should beat the ablated arms on the metric each mechanism targets. Tabulated, not pass/fail.",
    ),
}
ARMS = {
    "final": "Our submission with every mechanism enabled.",
    "naive": "The supplied starter server (experiments/naive_server.py).",
    "no-probe": "Final without the ping/pong probe that detects a client that is not reading, so the server never holds back or coalesces for slow readers (LIVERECALL_ABLATE=probe).",
    "no-grace": "Final without the 1 s grace that delays a lone provisional for a lagging client, so it is sent immediately instead of being replaced by its final (LIVERECALL_ABLATE=grace).",
    "no-resync": "Final without the resync that closes a hopelessly behind client so it reconnects and fetches missed finals in one HISTORY message (LIVERECALL_ABLATE=resync).",
    "no-all": "Final with probe, grace and resync all disabled.",
}


def family_of(sid: str) -> str:
    if sid == "H_base":
        return "H_base"
    if sid.startswith("BA_"):
        return "BA"
    head = sid.split("_")[0]
    return head


def workload(s: dict) -> str:
    parts = [f"{s['clients']} clients", f"{s['duration']} s"]
    if s["slow_percent"]:
        parts.append(
            f"{s['slow_percent']:g}% slow readers ({s['slow_ms']:g} ms per read)"
        )
    if s["disconnect_percent"]:
        parts.append(
            f"{s['disconnect_percent']:g}% disconnect at t={s['disconnect_at']:g}s for {s['disconnect_seconds']:g}s"
        )
    if s["late_percent"]:
        parts.append(f"{s['late_percent']:g}% join {s['late_seconds']:g}s late")
    for at, profile in s["tc"]:
        label = NETEM.get(profile, profile)
        parts.append(f"network: {label}" + (f" from t={at}s" if at else ""))
    for at, ms in s.get("impairment", []):
        parts.append(
            f"slow readers {'set to ' + str(ms) + ' ms' if ms else 'restored'} at t={at}s"
        )
    if s["source_args"]:
        parts.append("source " + " ".join(s["source_args"]))
    return ", ".join(parts)


def payload() -> list[dict]:
    families: dict[str, dict] = {}
    for s in SCENARIOS:
        key = family_of(s["id"])
        title, what, good = FAMILIES[key]
        fam = families.setdefault(
            key, dict(id=key, title=title, what=what, good=good, cases=[])
        )
        fam["cases"].append(
            dict(
                id=s["id"],
                suite=s["suite"],
                workload=workload(s),
                needs_tc=s["needs_tc"],
                informational=s["informational"],
                arms=s["arms"],
                runs=s["runs"],
                checks=[f"{m} {op} {v}" for m, op, v in s["checks"]],
            )
        )
    return list(families.values())


def render() -> str:
    lines = [
        "# LiveRecall test-case catalogue",
        "",
        "Generated by `python experiments/testcases.py` from `experiments/scenarios.py`; do not edit by hand.",
        "",
        "## How to read the numbering",
        "",
        "- **T1-T5** are the *public* tests from `docs/PUBLIC_TESTS.md`.",
        "- **H1-H11** are our reconstruction of the *hidden* evaluation categories. Suffixes encode the parameters: "
        "`H2_s30_m500` = 30% slow clients, 500 ms read delay; `H4_p50_d10` = 50% of clients disconnect for 10 s; "
        "`H5_l30` = late joiners arrive after 30 s; `H1_c500` = 500 clients; `H3_1mbit` = 1 Mbit cap.",
        "- **BA_*** are *before/after* ablation studies that reuse a workload above (e.g. `BA_T3` reruns T3).",
        "- **Arms** are the server variants a workload is run against (see the end of this file).",
        "- Targets are our own engineering thresholds, not the hidden grader's. Global checks on every case: "
        "final completeness >= 99%, 0 duplicates, 0 stale, 0 non-final history items, server alive, peak RSS <= 300 MB.",
        "",
        "| Family | Title | Cases |",
        "|---|---|---|",
    ]
    fams = payload()
    for f in fams:
        lines.append(f"| {f['id']} | {f['title']} | {len(f['cases'])} |")
    lines.append(f"\nTotal: {sum(len(f['cases']) for f in fams)} scenarios.\n")
    for f in fams:
        lines += [
            f"## {f['id']} - {f['title']}",
            "",
            f["what"],
            "",
            f"**Good result:** {f['good']}",
            "",
        ]
        lines += ["| Case | Suite | Workload | Arms | Notes |", "|---|---|---|---|---|"]
        for c in f["cases"]:
            notes = ["needs tc/netem"] if c["needs_tc"] else []
            if c["informational"]:
                notes.append("informational")
            lines.append(
                f"| `{c['id']}` | {c['suite']} | {c['workload']} | {', '.join(c['arms'])} | {'; '.join(notes)} |"
            )
        lines.append("")
    lines += ["## Arms", "", "| Arm | Meaning |", "|---|---|"]
    lines += [f"| `{a}` | {d} |" for a, d in ARMS.items()]
    lines += [
        "",
        "## Running",
        "",
        "```bash",
        "python experiments/run_matrix.py --list",
        "python experiments/run_matrix.py --only H2,T3 --runs 3",
        "```",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
