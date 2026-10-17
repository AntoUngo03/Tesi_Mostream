#!/usr/bin/env python3
"""Grafici throughput e rapporto rispetto all'ideale N/T del benchmark 2."""
import argparse
import csv
import json
import math
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    contract = json.loads((args.results / 'metadata.json').read_text())['comparison_contract']
    capacity_note = (f"L={contract['physical_ring_cells_both']} celle fisiche; "
                     f"posti utili Tsigas={contract['tsigas_usable_capacity']}, Vyukov={contract['vyukov_usable_capacity']}. "
                     "Prove finite senza giro completo.")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with (args.results / "summary.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    rows = [row for row in rows if row["queue"] in ("tsigas", "vyukov")]
    if not rows:
        parser.error("Nessun risultato TsigasZhang/Vyukov")
    targets = sorted({float(row["target_work_us"]) for row in rows})
    colors = {"tsigas": "#2563eb", "vyukov": "#d97706"}
    columns = min(2, len(targets))
    height = math.ceil(len(targets) / columns)
    figure, axes = plt.subplots(height, columns, figsize=(7 * columns, 4.5 * height), squeeze=False)
    for axis, target in zip(axes.flat, targets):
        selected = [row for row in rows if float(row["target_work_us"]) == target]
        ns = sorted({int(row["consumers"]) for row in selected})
        for queue, color in colors.items():
            group = sorted((row for row in selected if row["queue"] == queue), key=lambda row: int(row["consumers"]))
            if not group:
                continue
            n = [int(row["consumers"]) for row in group]
            axis.plot(n, [float(row["throughput_per_second"]) for row in group], marker="o", color=color, label=queue)
            axis.fill_between(n, [float(row["min_throughput_per_second"]) for row in group],
                              [float(row["max_throughput_per_second"]) for row in group], color=color, alpha=0.14)
        if target > 0:
            axis.plot(ns, [n * 1e6 / target for n in ns], "--", color="#64748b", label="Ideale N/T (sorgente non limitante)")
        else:
            axis.text(0.03, 0.05, "T=0: N/T non definito", transform=axis.transAxes, fontsize=9)
        axis.set_title(f"T obiettivo per input = {target:g} µs")
        axis.set_xlabel("N consumatori (+ una sorgente sequenziale)")
        axis.set_ylabel("Throughput (input/s), scala log; maggiore è meglio")
        axis.set_xscale("log", base=2)
        axis.set_yscale("log")
        axis.set_xticks(ns, [str(n) for n in ns])
        axis.grid(alpha=0.2, which="both")
        axis.legend(fontsize=8)
    for axis in list(axes.flat)[len(targets):]:
        axis.set_visible(False)
    figure.suptitle("Sorgente -> coda condivisa -> N consumatori\nMediane e intervallo min–max delle ripetizioni")
    figure.text(.5, .008, capacity_note, ha="center", fontsize=8)
    figure.tight_layout(rect=(0, .04, 1, 0.94))
    for extension in ("png", "pdf"):
        figure.savefig(args.results / f"farm_throughput.{extension}", dpi=180, bbox_inches="tight")
    plt.close(figure)

    positive = [t for t in targets if t > 0]
    if positive:
        figure, axis = plt.subplots(figsize=(9, 5))
        markers = ["o", "s", "^", "D", "v", "P", "X"]
        for number, target in enumerate(positive):
            for queue, color in colors.items():
                group = sorted((row for row in rows if row["queue"] == queue and float(row["target_work_us"]) == target),
                               key=lambda row: int(row["consumers"]))
                if group:
                    axis.plot([int(row["consumers"]) for row in group],
                              [100 * float(row["efficiency_vs_target"]) for row in group],
                              marker=markers[number % len(markers)], color=color,
                              linestyle="-" if number % 2 == 0 else "--", label=f"{queue}, T={target:g} µs")
        axis.axhline(100, color="#64748b", linestyle=":", label="Throughput ideale N/T")
        axis.set_xscale("log", base=2)
        ns = sorted({int(row["consumers"]) for row in rows})
        axis.set_xticks(ns, [str(n) for n in ns])
        axis.set_xlabel("N consumatori (+ una sorgente sequenziale)")
        axis.set_ylabel("Throughput misurato / (N/T), percentuale")
        axis.set_title("Distanza dal throughput ideale; T è un obiettivo calibrato")
        axis.set_ylim(bottom=0)
        axis.grid(alpha=0.2)
        axis.legend(fontsize=8)
        figure.text(.5, .008, capacity_note, ha="center", fontsize=7)
        figure.tight_layout(rect=(0, .04, 1, 1))
        for extension in ("png", "pdf"):
            figure.savefig(args.results / f"farm_efficiency.{extension}", dpi=180, bbox_inches="tight")
    print(f"Grafici salvati in {args.results}")


if __name__ == "__main__":
    main()
