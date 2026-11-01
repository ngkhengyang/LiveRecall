"""Workloads and targets live together so experiments remain reproducible.

Thresholds are local engineering targets, not claims about the hidden grader.
Before/after rows reuse their final workload's median rather than measuring twice.
"""

GLOBAL_CHECKS = [
    ("final_completeness_pct", ">=", 99),
    ("duplicates", "==", 0),
    ("stale", "==", 0),
    ("invalid_history_nonfinal", "==", 0),
    ("server_alive", "==", 1),
    ("peak_rss_mb", "<=", 300),
]

QUICK = {
    "duration": 20,
    "soak_duration": 40,
    "clients": 50,
    "scale_clients": [100, 200, 200],
    "runs": 1,
}

DEFAULTS = dict(
    suite="hidden",
    component="",
    needs_tc=False,
    mode="custom",
    clients=100,
    duration=60,
    slow_percent=0,
    slow_ms=500,
    disconnect_percent=0,
    disconnect_at=15,
    disconnect_seconds=5,
    late_percent=0,
    late_seconds=10,
    source_args=[],
    tc=[],
    arms=["final"],
    runs=3,
    informational=False,
    checks=[],
)
SCENARIOS = [
    dict(
        DEFAULTS,
        id="T1",
        suite="public",
        mode="run_demo",
        clients=20,
        duration=20,
        checks=[
            ("final_completeness_pct", ">=", 100),
            ("duplicates", "==", 0),
            ("stale", "==", 0),
            ("normal_update_coverage_pct", ">=", 99),
        ],
    ),
    dict(DEFAULTS, id="T2", suite="public", mode="run_demo", duration=40),
    dict(DEFAULTS, id="T3", suite="public", mode="run_demo", slow_percent=10),
    dict(
        DEFAULTS,
        id="T4",
        suite="public",
        mode="run_demo",
        disconnect_percent=20,
        checks=[("final_completeness_pct", ">=", 100), ("duplicates", "==", 0)],
    ),
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"T5_{profile}",
        suite="public",
        mode="run_demo",
        duration=40,
        needs_tc=profile != "none",
        tc=[] if profile == "none" else [(0, profile)],
        checks=[("final_completeness_pct", ">=", 100)],
    )
    for profile in (
        "none",
        "delay_loss",
        "loss_3pct",
        "delay_100ms",
        "reorder",
        "bandwidth_2mbit",
    )
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H1_c{n}",
        component="Correctness / normal freshness",
        clients=n,
        checks=[
            ("normal_update_coverage_pct", ">=", 99),
            ("p95_ms", "<=", 500),
            ("connection_errors", "==", 0),
        ],
    )
    for n in (250, 500, 1000)
]
SCENARIOS += [dict(DEFAULTS, id="H_base", component="Correctness / normal freshness")]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H2_s{p}_m{ms}",
        component="Challenge 1 (slow-reader)",
        slow_percent=p,
        slow_ms=ms,
        arms=["final", "naive"],
        checks=[
            ("normal_clients.p95_ms", "<=", "ref:H_base.normal_clients.p95_ms*1.5"),
            ("normal_clients.p95_ms", "<=", 500),
            (
                "slow_clients.fresh_final_pct",
                ">=",
                "arm:naive.slow_clients.fresh_final_pct+0",
            ),
        ]
        # A reader at <=2 msg/s can still take every final (~1.33/s) if
        # provisionals are coalesced; at 1 msg/s it cannot, so no target there.
        + ([("slow_clients.fresh_final_pct", ">=", 50)] if ms <= 500 else []),
    )
    for p in (10, 30, 50)
    for ms in (100, 250, 500, 1000)
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H3_{rate}",
        component="Challenge 2 (constrained bandwidth)",
        needs_tc=True,
        tc=[(0, f"tbf rate {rate} burst 32kbit latency 400ms")],
        arms=["final", "naive"],
        checks=[("fresh_final_pct", ">=", "arm:naive.fresh_final_pct+0")]
        + ([("fresh_final_pct", ">=", 80)] if rate == "2mbit" else []),
    )
    for rate in ("2mbit", "1mbit", "512kbit")
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H4_p{p}_d{d}",
        component="Reconnect & catch-up",
        disconnect_percent=p,
        disconnect_seconds=d,
        duration=max(60, 15 + d + 20),
        informational=d == 70,
        checks=[
            ("recovery_p95_ms", "<=", 1000),
            ("normal_unaffected_p95_ms", "<=", 500),
        ],
    )
    for p in (50, 100)
    for d in (2, 10, 30, 70)
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H5_l{seconds}",
        component="Reconnect & catch-up",
        late_percent=20,
        late_seconds=seconds,
        checks=[
            ("recovery_p95_ms", "<=", 1000),
            ("late_history_nonempty_pct", "==", 100),
        ],
    )
    for seconds in (10, 30)
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id="H6",
        component="Normal coverage + Challenge 1",
        source_args=["--revision-delay", "0.02", "--segment-gap", "0.05"],
        checks=[("normal_clients.update_coverage_pct", ">=", 95)],
    ),
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id=f"H7_{profile}",
        component="Delay/loss tail freshness",
        needs_tc=True,
        tc=[(0, profile)],
        checks=[("p95_ms", "<=", limit)],
    )
    for profile, limit in (
        ("delay_loss", 520),
        ("loss_3pct", 400),
        ("delay_100ms", 500),
    )
]
SCENARIOS += [
    dict(
        DEFAULTS,
        id="H8_bandwidth",
        component="Rapid convergence",
        needs_tc=True,
        tc=[(10, "tbf rate 512kbit burst 32kbit latency 400ms"), (30, "reset")],
        checks=[("converge_s", "<=", 3), ("tail_update_coverage_pct", ">=", 95)],
    ),
    dict(
        DEFAULTS,
        id="H8_slow",
        component="Rapid convergence",
        slow_percent=30,
        impairment=[(10, 500), (30, 0)],
        checks=[("converge_s", "<=", 3), ("tail_update_coverage_pct", ">=", 95)],
    ),
    dict(
        DEFAULTS,
        id="H9",
        component="Boundedness",
        duration=300,
        slow_percent=30,
        checks=[("rss_slope_mb_per_min", "<=", 1)],
    ),
    dict(
        DEFAULTS,
        id="H10",
        component="Correctness / invariants",
        mode="edge",
        runs=1,
        checks=[("edge_pass", "==", 1)],
    ),
    dict(
        DEFAULTS,
        id="H11",
        component="All",
        needs_tc=True,
        clients=500,
        tc=[(0, "delay_loss")],
        slow_percent=10,
        slow_ms=250,
        disconnect_percent=20,
        disconnect_seconds=10,
        source_args=["--revision-delay", "0.02", "--segment-gap", "0.05"],
        checks=[("normal_clients.p95_ms", "<=", 1000), ("recovery_p95_ms", "<=", 2000)],
    ),
]
SCENARIOS += [
    dict(
        next(s for s in SCENARIOS if s["id"] == reference),
        id=f"BA_{name}",
        suite="before_after",
        mode="custom",
        reuse=reference,
        arms=["final", "naive", "no-probe", "no-grace", "no-resync", "no-all"],
        checks=[],
    )
    for name, reference in (
        ("T3", "T3"),
        ("T4", "T4"),
        ("bandwidth_2mbit", "T5_bandwidth_2mbit"),
        ("H2", "H2_s50_m1000"),
    )
]
