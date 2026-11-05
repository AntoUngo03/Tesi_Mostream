#!/usr/bin/env python3
"""Disegna il throughput del benchmark (input completati al secondo) con matplotlib.

Legge raw.csv della cartella dei risultati: per ogni ripetizione il throughput è
numero di input / tempo totale della prova (colonna throughput_per_second).
Le linee sono le mediane sulle ripetizioni, le fasce minimo e massimo osservati.
Per T > 0 la linea tratteggiata è il throughput ideale: N volte la mediana con N = 1.
Produce pipeline_grain_throughput.png/.pdf e throughput_summary.csv nella stessa cartella.
"""
import argparse
import csv
from pathlib import Path
import math
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", type=Path, help="Cartella contenente raw.csv")
    args = parser.parse_args()
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with (args.results / "raw.csv").open(newline="") as stream:
        raw = list(csv.DictReader(stream))
    if not raw:
        parser.error("raw.csv vuoto")

    # throughput di ogni ripetizione, raggruppato per (coda, T, N)
    groups = {}
    for row in raw:
        key = (row["queue"], float(row["target_total_us"]), int(row["stages"]))
        groups.setdefault(key, []).append(float(row["throughput_per_second"]))
    stats = {key: (statistics.median(v), min(v), max(v), len(v)) for key, v in groups.items()}

    # la coda di riferimento (vyukov) in arancione, quella confrontata in blu, come in plot.py
    queues = sorted({key[0] for key in stats}, key=lambda q: (q == "vyukov", q))
    palette = ["#2563eb", "#16a34a", "#9333ea"]
    colors = {q: ("#d97706" if q == "vyukov" else palette[i % len(palette)]) for i, q in enumerate(queues)}
    targets = sorted({key[1] for key in stats})

    with (args.results / "throughput_summary.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["queue", "target_total_us", "stages", "repeats", "median_input_per_s",
                         "min_input_per_s", "max_input_per_s", "speedup_vs_n1"])
        for (queue, target, stages), (median, low, high, count) in sorted(stats.items()):
            base = stats.get((queue, target, 1))
            writer.writerow([queue, target, stages, count, f"{median:.1f}", f"{low:.1f}", f"{high:.1f}",
                             f"{median / base[0]:.4f}" if base else ""])

    columns = min(2, len(targets))
    height = math.ceil(len(targets) / columns)
    figure, axes = plt.subplots(height, columns, figsize=(7 * columns, 4.5 * height), squeeze=False)
    for axis, target in zip(axes.flat, targets):
        n = sorted({key[2] for key in stats if key[1] == target})
        for queue in queues:
            points = [(s, stats[(queue, target, s)]) for s in n if (queue, target, s) in stats]
            if not points:
                continue
            x = [s for s, _ in points]
            axis.plot(x, [v[0] / 1e6 for _, v in points], marker="o", color=colors[queue], label=queue)
            axis.fill_between(x, [v[1] / 1e6 for _, v in points], [v[2] / 1e6 for _, v in points],
                              color=colors[queue], alpha=0.12)
        if target > 0:
            # ideale: con N stadi che lavorano in parallelo su input diversi, N volte il throughput di N = 1
            base = statistics.median(stats[(q, target, 1)][0] for q in queues if (q, target, 1) in stats)
            axis.plot(n, [base * s / 1e6 for s in n], "--", color="#64748b", label="Ideale: N × throughput N=1")
            axis.set_ylim(bottom=0)
        else:
            # con T = 0 il caso N = 1 non usa code ed è di ordini di grandezza più veloce: scala logaritmica
            axis.set_yscale("log")
        axis.set_title(f"T totale obiettivo = {target:g} µs/input")
        axis.set_xlabel("Numero N di stadi sequenziali")
        axis.set_ylabel("Throughput (milioni di input/s); maggiore è meglio"
                        + (" — scala log" if target == 0 else ""))
        axis.set_xscale("log", base=2)
        axis.set_xticks(n, [str(value) for value in n])
        axis.grid(alpha=0.2)
        axis.legend(fontsize=8)
    for axis in list(axes.flat)[len(targets):]:
        axis.set_visible(False)
    figure.suptitle("Grana della pipeline: throughput, mediane e intervallo min–max delle ripetizioni")
    figure.tight_layout(rect=(0, 0, 1, 0.95))
    for extension in ("png", "pdf"):
        figure.savefig(args.results / f"pipeline_grain_throughput.{extension}", dpi=180, bbox_inches="tight")
    print(f"Grafici salvati in {args.results}")


if __name__ == "__main__":
    main()
