#!/usr/bin/env python3
"""Grafico della campagna farm_multisource: throughput in funzione del numero S di sorgenti.

Una riga di pannelli per ogni T, una colonna per ogni N (worker Compute).
Linee: mediane sulle ripetizioni; fasce: minimo e massimo osservati.
Produce multisource_throughput.png/.pdf/.svg nella cartella dei risultati.
"""
import argparse
import csv
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path, help='Cartella contenente raw.csv')
    args = parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    with (args.results / 'raw.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    groups = {}
    for row in rows:
        key = (row['backend'], int(row['sources']), int(row['workers']), float(row['target_us']))
        groups.setdefault(key, []).append(float(row['throughput']) / 1e6)
    sources = sorted({k[1] for k in groups})
    workers = sorted({k[2] for k in groups})
    targets = sorted({k[3] for k in groups})
    colors = {'vyukov': '#d97706', 'rigtorp': '#2563eb'}

    figure, axes = plt.subplots(len(targets), len(workers), figsize=(3.6 * len(workers), 3.0 * len(targets)),
                                squeeze=False, sharex=True)
    for i, target in enumerate(targets):
        # stessa scala verticale per tutti i pannelli di uno stesso T
        top = max(max(v) for k, v in groups.items() if k[3] == target) * 1.08
        for j, n in enumerate(workers):
            axis = axes[i][j]
            for backend, color in colors.items():
                values = [groups[(backend, s, n, target)] for s in sources]
                axis.plot(sources, [statistics.median(v) for v in values], marker='o', color=color, label=backend)
                axis.fill_between(sources, [min(v) for v in values], [max(v) for v in values], color=color, alpha=0.12)
            axis.set_xscale('log', base=2)
            axis.set_xticks(sources, [str(s) for s in sources])
            axis.set_ylim(0, top)
            axis.grid(alpha=0.2)
            axis.set_title(f'T = {target:g} µs, N = {n} worker', fontsize=10)
            if j == 0:
                axis.set_ylabel('Throughput (M input/s)')
            if i == len(targets) - 1:
                axis.set_xlabel('S: sorgenti')
    axes[0][0].legend(fontsize=8)
    figure.suptitle('Farm con S sorgenti, N worker e un sink (runtime standard): mediane e intervallo min–max')
    figure.tight_layout(rect=(0, 0, 1, 0.96))
    for extension in ('png', 'pdf', 'svg'):
        figure.savefig(args.results / f'multisource_throughput.{extension}', dpi=160, bbox_inches='tight')
    print(f'Grafici salvati in {args.results}')


if __name__ == '__main__':
    main()
