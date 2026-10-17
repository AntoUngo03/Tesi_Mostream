#!/usr/bin/env python3
"""Disegna le mediane del benchmark con matplotlib (dipendenza facoltativa)."""
import argparse
import csv
from pathlib import Path
import math
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, help="Cartella contenente summary.csv")
    args = parser.parse_args()
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with (args.results / "summary.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    rows = [row for row in rows if row["queue"] in ("rigtorp", "vyukov")]
    if not rows:
        parser.error("Nessun risultato Rigtorp o Vyukov nel CSV")
    targets = sorted({float(row["target_total_us"]) for row in rows})
    colors = {"rigtorp": "#2563eb", "vyukov": "#d97706"}
    columns = min(2, len(targets))
    height = math.ceil(len(targets) / columns)
    figure, axes = plt.subplots(height, columns, figsize=(7 * columns, 4.5 * height), squeeze=False)
    for axis, target in zip(axes.flat, targets):
        selected = [row for row in rows if float(row["target_total_us"]) == target]
        for queue in colors:
            group = sorted((row for row in selected if row["queue"] == queue), key=lambda row: int(row["stages"]))
            if not group:
                continue
            n = [int(row["stages"]) for row in group]
            median = [float(row["median_ns_per_input"]) for row in group]
            axis.plot(n, median, marker="o", color=colors[queue], label=queue)
            axis.fill_between(n, [float(row["min_ns_per_input"]) for row in group],
                              [float(row["max_ns_per_input"]) for row in group], color=colors[queue], alpha=0.12)
        n = sorted({int(row["stages"]) for row in selected})
        if target > 0:
            baseline = statistics.median(float(row["median_ns_per_input"]) for row in selected if int(row["stages"]) == 1)
            axis.plot(n, [baseline / value for value in n], "--", color="#64748b", label="Ideale: baseline N=1 / N")
        axis.set_title(f"T totale obiettivo = {target:g} µs/input")
        axis.set_xlabel("Numero N di stadi sequenziali")
        axis.set_ylabel("Tempo medio per input (ns); minore è meglio")
        axis.set_xscale("log", base=2)
        axis.set_xticks(n, [str(value) for value in n])
        axis.set_ylim(bottom=0)
        axis.grid(alpha=0.2)
        axis.legend(fontsize=8)
    for axis in list(axes.flat)[len(targets):]:
        axis.set_visible(False)
    figure.suptitle("Grana della pipeline: mediane e intervallo min–max delle ripetizioni")
    figure.tight_layout(rect=(0, 0, 1, 0.95))
    for extension in ("png", "pdf"):
        figure.savefig(args.results / f"pipeline_grain.{extension}", dpi=180, bbox_inches="tight")
    print(f"Grafici salvati in {args.results}")


if __name__ == "__main__":
    main()
