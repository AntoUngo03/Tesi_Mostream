#!/usr/bin/env python3
"""Summarize WCQ scalability and relative performance from run_suite.py."""

import csv
import math
import statistics
import sys
from collections import Counter
from pathlib import Path


QUEUE_COLUMNS = {
    "CAS": "cas_mean_Mmsg_s",
    "FAA": "faa_mean_Mmsg_s",
    "HYBRID1": "hybrid1_mean_Mmsg_s",
    "HYBRID2": "hybrid2_mean_Mmsg_s",
    "HYBRID4": "hybrid4_mean_Mmsg_s",
    "HYBRID8": "hybrid8_mean_Mmsg_s",
    "RIGTORP": "rigtorp_mean_Mmsg_s",
    "PADDEDFAA": "padded_faa_mean_Mmsg_s",
    "BLPRQ": "blprq_mean_Mmsg_s",
    "WCQ": "wcq_mean_Mmsg_s",
}


def geometric_mean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def main() -> int:
    path = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path(__file__).with_name("results_wcq.csv")
    )
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise RuntimeError(f"No result rows in {path}")

    print("Balanced scalability at capacity 1024")
    print("P/C | WCQ Mmsg/s | rank/10 | WCQ/Padded | WCQ/Rigtorp")
    for row in rows:
        if row["producers"] != row["consumers"] or row["capacity"] != "1024":
            continue
        values = {
            name: float(row[column]) for name, column in QUEUE_COLUMNS.items()
        }
        ranking = sorted(values, key=values.get, reverse=True)
        print(
            f'{row["producers"]:>3} | {values["WCQ"]:>10.3f} | '
            f'{ranking.index("WCQ") + 1:>7}/10 | '
            f'{float(row["wcq_vs_padded_faa_mean"]):>10.3f}x | '
            f'{float(row["wcq_vs_rigtorp_mean"]):>11.3f}x'
        )

    print("\nGeometric mean over every topology/capacity")
    wcq = [float(row[QUEUE_COLUMNS["WCQ"]]) for row in rows]
    for name, column in QUEUE_COLUMNS.items():
        if name == "WCQ":
            continue
        ratios = [
            wcq_value / float(row[column])
            for wcq_value, row in zip(wcq, rows)
        ]
        print(
            f"WCQ/{name:<9} {geometric_mean(ratios):.3f}x; "
            f"wins {sum(ratio > 1.0 for ratio in ratios)}/{len(ratios)}"
        )

    rank_counts: Counter[int] = Counter()
    for row in rows:
        values = {
            name: float(row[column]) for name, column in QUEUE_COLUMNS.items()
        }
        ranking = sorted(values, key=values.get, reverse=True)
        rank_counts[ranking.index("WCQ") + 1] += 1
    print("\nWCQ rank counts:", dict(sorted(rank_counts.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
