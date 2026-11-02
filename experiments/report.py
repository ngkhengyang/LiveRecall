"""Render stored medians and verdicts without rerunning or changing measurements."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from scenarios import SCENARIOS

COLUMNS = [
    ("p50_ms", "P50 ms", 1),
    ("p95_ms", "P95 ms", 1),
    ("p99_ms", "P99 ms", 1),
    ("fresh_pct", "Fresh %", 2),
    ("fresh_final_pct", "Fresh final %", 2),
    ("final_completeness_pct", "Completeness %", 2),
    ("normal_update_coverage_pct", "Coverage %", 2),
    ("normal_clients.p95_ms", "Normal P95 ms", 1),
    ("slow_clients.p95_ms", "Slow P95 ms", 1),
    ("recovery_p95_ms", "Recovery P95 ms", 1),
    ("duplicates", "Dup", 0),
    ("stale", "Stale", 0),
    ("peak_rss_mb", "Peak RSS MB", 1),
]
EXTRA = [
    ("converge_s", "Converge s", 2),
    ("rss_slope_mb_per_min", "RSS slope MB/min", 2),
]
BA_COLUMNS = [COLUMNS[i] for i in (4, 7, 8, 5, 6, 12)] + [
    (key, key.capitalize(), 0)
    for key in ("coalesced", "rejected", "batches", "batched_finals", "resyncs")
]
CATALOG = {s["id"]: s for s in SCENARIOS}


def nested(row: dict, key: str) -> Any:
    value: Any = row
    for part in key.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def fmt(value: Any, places: int = 2, signed: bool = False) -> str:
    if not numeric(value):
        return "-"
    return format(value, f"{'+' if signed else ''}.{places}f")


def overall(rows: list[dict]) -> str:
    """A failed check wins even if another run errored or a metric was skipped."""
    states = {row["verdict"] for row in rows}
    return next(
        (state for state in ("FAIL", "ERROR", "PASS", "SKIP") if state in states), "-"
    )


def gate(row: dict) -> bool:
    value = row.get("final_completeness_pct")
    return numeric(value) and value < 99


def label(test_id: str, arm: str, row: dict) -> str:
    name = f"{test_id}/{arm}"
    return f"**{name}**" if gate(row) else name


def table(headers: list[str], rows: list[list[str]]) -> str:
    def line(cells: list[str]) -> str:
        return (
            "| "
            + " | ".join(str(c).replace("|", "\\|").replace("\n", " ") for c in cells)
            + " |"
        )

    return (
        "\n".join(
            [line(headers), line(["---"] * len(headers))] + [line(row) for row in rows]
        )
        + "\n"
    )


def build_report(out: Path) -> str:
    verdict_path = out / "verdicts.json"
    verdicts = (
        json.loads(verdict_path.read_text(encoding="utf-8"))
        if verdict_path.exists()
        else []
    )
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in verdicts:
        grouped[row["id"], row["arm"]].append(row)
    medians = {}
    for path in sorted(out.glob("*/*_median.json")):
        medians[path.parent.name, path.stem.removesuffix("_median")] = json.loads(
            path.read_text(encoding="utf-8")
        )
    keys = sorted(set(medians) | set(grouped))

    def suite(key: tuple[str, str]) -> str:
        rows = grouped.get(key, [])
        return rows[0]["suite"] if rows else CATALOG.get(key[0], {}).get("suite", "")

    def component(key: tuple[str, str]) -> str:
        rows = grouped.get(key, [])
        return (
            rows[0]["component"]
            if rows
            else CATALOG.get(key[0], {}).get("component", "")
        )

    hidden = [key for key in keys if suite(key) == "hidden"]
    parts = [
        "# Experiment summary\n",
        "Medians across stored runs. Thresholds are our own targets, not the hidden ones. "
        "**Bold scenario/arm names flag completeness below 99%.** Missing values are `-`.\n",
        "## Hidden-workload verdicts\n",
    ]
    verdict_rows = []
    counts: dict[str, Counter] = defaultdict(Counter)
    for key in hidden:
        if key not in grouped:
            continue
        rows = grouped.get(key, [])
        state = overall(rows)
        counts[component(key)][state] += 1
        failures = [
            f"{r['check']}: {fmt(r.get('value'))} vs {fmt(r.get('threshold'))}"
            for r in rows
            if r["verdict"] == "FAIL"
        ]
        failures += [r["check"] for r in rows if r["verdict"] == "ERROR"]
        verdict_rows.append(
            [
                label(*key, medians.get(key, {})),
                component(key),
                state,
                "; ".join(failures) or "-",
            ]
        )
    parts.append(
        table(
            [
                "Test / arm",
                "§13.1 component",
                "Verdict",
                "Failing checks (value vs threshold)",
            ],
            verdict_rows,
        )
    )
    parts.append(
        table(
            ["Component", "PASS", "FAIL", "SKIP", "ERROR"],
            [
                [name] + [str(counter[s]) for s in ("PASS", "FAIL", "SKIP", "ERROR")]
                for name, counter in sorted(counts.items())
            ],
        )
    )

    def metrics_table(selected: list[tuple[str, str]], columns: list[tuple]) -> str:
        return table(
            ["Scenario / arm", "Correctness gate"] + [c[1] for c in columns],
            [
                [
                    label(*key, medians.get(key, {})),
                    "**<99%**" if gate(medians.get(key, {})) else "-",
                ]
                + [
                    fmt(nested(medians.get(key, {}), path), places)
                    for path, _, places in columns
                ]
                for key in selected
            ],
        )

    parts += [
        "## Public tests T1–T5 medians\n",
        metrics_table([key for key in keys if suite(key) == "public"], COLUMNS),
    ]
    control = medians.get(("T5_none", "final"), {})

    def ratio(row: dict, metric: str) -> float | None:
        base, value = control.get(metric), row.get(metric)
        return value / base if numeric(base) and base > 0 and numeric(value) else None

    parts += [
        "### T5 tails versus same-runner control\n",
        table(
            ["Scenario / arm", "P95 × control", "P99 × control"],
            [
                [label(*key, medians.get(key, {}))]
                + [
                    (
                        fmt(ratio(medians.get(key, {}), metric), 1) + "x"
                        if ratio(medians.get(key, {}), metric) is not None
                        else "-"
                    )
                    for metric in ("p95_ms", "p99_ms")
                ]
                for key in keys
                if key[0].startswith("T5_")
            ],
        ),
    ]
    parts += [
        "## Before/after\n",
        "Δ is arm minus final (percentage points for % columns).\n",
    ]
    for test_id in sorted({key[0] for key in keys if suite(key) == "before_after"}):
        baseline = medians.get((test_id, "final"), {})
        rows = []
        for key in sorted(
            (key for key in keys if key[0] == test_id),
            key=lambda key: (key[1] != "final", key[1]),
        ):
            row = medians.get(key, {})
            cells = [label(*key, row), "**<99%**" if gate(row) else "-"]
            for metric, _, places in BA_COLUMNS:
                value, base = nested(row, metric), nested(baseline, metric)
                delta = value - base if numeric(value) and numeric(base) else None
                cells += [fmt(value, places), fmt(delta, places, signed=True)]
            rows.append(cells)
        parts += [
            f"### {test_id}\n",
            table(
                ["Scenario / arm", "Correctness gate"]
                + [
                    head
                    for _, heading, _ in BA_COLUMNS
                    for head in (heading, "Δ " + heading)
                ],
                rows,
            ),
        ]
    parts += ["## Hidden-test metrics\n", metrics_table(hidden, COLUMNS + EXTRA)]

    # Keep a single machine-readable row per scenario/arm for later comparisons.
    columns = (
        COLUMNS + EXTRA + [c for c in BA_COLUMNS if c[0] not in {c[0] for c in COLUMNS}]
    )
    with (out / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
        fields = ["id", "suite", "arm", "component", "verdict", "correctness_gate"]
        fields += [c[0] for c in columns] + ["p95_x_control", "p99_x_control"]
        fields += ["delta_" + c[0] for c in BA_COLUMNS]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for key in keys:
            row = medians.get(key, {})
            result = dict(
                id=key[0],
                suite=suite(key),
                arm=key[1],
                component=component(key),
                verdict=overall(grouped.get(key, [])),
                correctness_gate="FAIL" if gate(row) else "-",
            )
            result.update(
                {path: fmt(nested(row, path), places) for path, _, places in columns}
            )
            for metric in ("p95_ms", "p99_ms"):
                result[metric.replace("_ms", "_x_control")] = (
                    fmt(ratio(row, metric), 1) if key[0].startswith("T5_") else "-"
                )
            baseline = medians.get((key[0], "final"), {})
            for metric, _, places in BA_COLUMNS:
                value, base = nested(row, metric), nested(baseline, metric)
                result["delta_" + metric] = fmt(
                    (
                        value - base
                        if suite(key) == "before_after"
                        and numeric(value)
                        and numeric(base)
                        else None
                    ),
                    places,
                    signed=True,
                )
            writer.writerow(result)
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("output/experiments"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    markdown = build_report(args.out)
    (args.out / "SUMMARY.md").write_text(markdown, encoding="utf-8")
    print(markdown)


if __name__ == "__main__":
    main()
