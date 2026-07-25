#!/usr/bin/env python3
"""Summarize the bounded LPRQ experiment without third-party packages."""

import csv
import math
import statistics
import sys
from pathlib import Path


DEFAULT_RESULTS = Path(__file__).with_name("results_bounded_lprq.csv")
QUEUES = {
    "CAS": "cas_mean_Mmsg_s",
    "FAA": "faa_mean_Mmsg_s",
    "Rigtorp": "rigtorp_mean_Mmsg_s",
    "Padded-FAA": "padded_faa_mean_Mmsg_s",
    "Hybrid-1": "hybrid1_mean_Mmsg_s",
    "Hybrid-2": "hybrid2_mean_Mmsg_s",
    "Hybrid-4": "hybrid4_mean_Mmsg_s",
    "Hybrid-8": "hybrid8_mean_Mmsg_s",
    "BLPRQ": "blprq_mean_Mmsg_s",
}
CI95 = {
    "CAS": "cas_ci95_Mmsg_s",
    "FAA": "faa_ci95_Mmsg_s",
    "Rigtorp": "rigtorp_ci95_Mmsg_s",
    "Padded-FAA": "padded_faa_ci95_Mmsg_s",
    "Hybrid-1": "hybrid1_ci95_Mmsg_s",
    "Hybrid-2": "hybrid2_ci95_Mmsg_s",
    "Hybrid-4": "hybrid4_ci95_Mmsg_s",
    "Hybrid-8": "hybrid8_ci95_Mmsg_s",
    "BLPRQ": "blprq_ci95_Mmsg_s",
}


def geometric_mean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def values(row: dict[str, str]) -> dict[str, float]:
    return {name: float(row[column]) for name, column in QUEUES.items()}


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_RESULTS
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise RuntimeError(f"No benchmark rows in {path}")

    wins = {name: 0 for name in QUEUES}
    best_over_blprq: list[float] = []
    for row in rows:
        measured = values(row)
        winner = max(measured, key=measured.get)
        wins[winner] += 1
        best_over_blprq.append(measured[winner] / measured["BLPRQ"])

    print("# Bounded LPRQ experiment summary\n")
    print(f"Source: `{path}` ({len(rows)} configurations)\n")
    print("## Balanced workloads, capacity 1024\n")
    print(
        "| Threads | CAS | FAA | Rigtorp | Padded-FAA | "
        "Best Hybrid | BLPRQ | Winner / BLPRQ |"
    )
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        if row["producers"] != row["consumers"] or row["capacity"] != "1024":
            continue
        measured = values(row)
        best_hybrid = max(
            ("Hybrid-1", "Hybrid-2", "Hybrid-4", "Hybrid-8"),
            key=measured.get,
        )
        winner = max(measured, key=measured.get)
        ratio = measured[winner] / measured["BLPRQ"]
        print(
            f"| {row['producers']}P/{row['consumers']}C "
            f"| {measured['CAS']:.3f}±{float(row[CI95['CAS']]):.3f} "
            f"| {measured['FAA']:.3f}±{float(row[CI95['FAA']]):.3f} "
            f"| {measured['Rigtorp']:.3f}±{float(row[CI95['Rigtorp']]):.3f} "
            f"| {measured['Padded-FAA']:.3f}±{float(row[CI95['Padded-FAA']]):.3f} "
            f"| {measured[best_hybrid]:.3f}±{float(row[CI95[best_hybrid]]):.3f} "
            f"| {measured['BLPRQ']:.3f}±{float(row[CI95['BLPRQ']]):.3f} "
            f"| {winner}, {ratio:.2f}x |"
        )

    print("\n## Whole matrix\n")
    print("Wins by mean throughput:")
    for name, count in sorted(wins.items(), key=lambda pair: (-pair[1], pair[0])):
        print(f"- {name}: {count}/{len(rows)}")

    print("\nBest implementation / BLPRQ throughput ratio:")
    print(f"- minimum: {min(best_over_blprq):.2f}x")
    print(f"- median: {statistics.median(best_over_blprq):.2f}x")
    print(f"- geometric mean: {geometric_mean(best_over_blprq):.2f}x")
    print(f"- maximum: {max(best_over_blprq):.2f}x")

    print("\nPer implementation / BLPRQ geometric-mean ratio:")
    for name, column in QUEUES.items():
        if name == "BLPRQ":
            continue
        ratios = [float(row[column]) / float(row[QUEUES["BLPRQ"]]) for row in rows]
        print(f"- {name}: {geometric_mean(ratios):.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
