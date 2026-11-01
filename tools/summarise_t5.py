#!/usr/bin/env python3
"""Collapse the T5 impairment matrix into one Markdown table.

``run_demo.py`` already prints a median block per invocation, but T5 produces
one invocation per network profile. This gathers them into a single table that
can go straight into the report, with each impaired arm expressed as a delta
against the unimpaired baseline measured on the same machine.

Reads ``output/t5_<arm>.json`` (single run) or ``output/t5_<arm>_runNN.json``
(``--runs N``) and writes Markdown to stdout.

    python tools/summarise_t5.py [--output-dir output] [--prefix t5_]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path

RUN_SUFFIX = re.compile(r"_run\d+$")

# (json key, column heading, decimal places). Order is the column order.
COLUMNS = [
    ("p50_ms", "P50 ms", 1),
    ("p95_ms", "P95 ms", 1),
    ("p99_ms", "P99 ms", 1),
    ("fresh_pct", "Fresh %", 2),
    ("fresh_final_pct", "Fresh final %", 2),
    ("final_completeness_pct", "Completeness %", 2),
    ("normal_update_coverage_pct", "Coverage %", 2),
    ("duplicates", "Dup", 0),
    ("stale", "Stale", 0),
]


def load_arms(output_dir: Path, prefix: str) -> dict[str, list[dict]]:
    """Group every result file under ``output_dir`` by its arm name."""
    arms: dict[str, list[dict]] = {}
    for path in sorted(output_dir.glob(f"{prefix}*.json")):
        stem = RUN_SUFFIX.sub("", path.stem)
        arm = stem[len(prefix) :] if stem.startswith(prefix) else stem
        try:
            arms.setdefault(arm, []).append(json.loads(path.read_text()))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"<!-- skipped {path.name}: {exc} -->")
    return arms


def median_of(summaries: list[dict], key: str) -> float | None:
    values = [s[key] for s in summaries if isinstance(s.get(key), (int, float))]
    return statistics.median(values) if values else None


def fmt(value: float | None, places: int) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="output", type=Path)
    parser.add_argument("--prefix", default="t5_")
    args = parser.parse_args()

    arms = load_arms(args.output_dir, args.prefix)
    if not arms:
        print(f"No `{args.prefix}*.json` results found in `{args.output_dir}/`.")
        return

    # Baseline first, then the impaired arms alphabetically.
    order = sorted(arms, key=lambda name: (name != "baseline", name))
    runs = max(len(v) for v in arms.values())

    print("## T5 network impairment")
    print()
    print(f"Median of {runs} independent run(s) per arm, all on the same machine.")
    print()
    print("| Arm | " + " | ".join(head for _, head, _ in COLUMNS) + " |")
    print("|" + "---|" * (len(COLUMNS) + 1))

    for arm in order:
        cells = [fmt(median_of(arms[arm], key), places) for key, _, places in COLUMNS]
        print(f"| `{arm}` | " + " | ".join(cells) + " |")

    baseline = arms.get("baseline")
    if baseline:
        print()
        print("### Tail latency versus the unimpaired baseline")
        print()
        print("| Arm | P95 x baseline | P99 x baseline |")
        print("|---|---|---|")
        for arm in order:
            if arm == "baseline":
                continue
            row = []
            for key in ("p95_ms", "p99_ms"):
                base = median_of(baseline, key)
                value = median_of(arms[arm], key)
                row.append(
                    f"{value / base:.1f}x" if base and value and base > 0 else "-"
                )
            print(f"| `{arm}` | " + " | ".join(row) + " |")

    print()
    print(
        "WebSocket runs over TCP, so IP-level loss and reordering surface as "
        "retransmission and head-of-line delay rather than as missing "
        "application messages. Completeness staying at 100% while the tail "
        "grows is the expected signature, not a contradiction."
    )


if __name__ == "__main__":
    main()
