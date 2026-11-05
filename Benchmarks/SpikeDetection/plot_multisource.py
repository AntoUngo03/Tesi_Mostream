#!/usr/bin/env python3
"""Grafico della campagna Spike Detection multi-sorgente (run_campaign_multisource.sh).

Un pannello per ogni configurazione MovingAverage + SpikeDetector; sull'asse x il numero S di
sorgenti, sull'asse y il throughput in milioni di tuple/s. Linee: mediane sulle ripetizioni;
fasce: minimo e massimo. Produce <csv senza estensione>.png/.pdf/.svg accanto al CSV.

Uso: python3 plot_multisource.py risultati/campagna_multisource_AAAAMMGG.csv
"""
import argparse
import csv
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('csv', type=Path)
    args = parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    groups = {}
    with args.csv.open(newline='') as stream:
        for row in csv.DictReader(stream):
            if row['throughput']:
                key = (row['queue'], int(row['ma']), int(row['sd']), int(row['sources']))
                groups.setdefault(key, []).append(float(row['throughput']) / 1e6)
    configs = sorted({(k[1], k[2]) for k in groups})
    sources = sorted({k[3] for k in groups})
    colors = {'vyukov': '#d97706', 'rigtorp': '#2563eb'}
    top = max(max(v) for v in groups.values()) * 1.08

    figure, axes = plt.subplots(1, len(configs), figsize=(3.8 * len(configs), 3.6), squeeze=False, sharey=True)
    for axis, (ma, sd) in zip(axes.flat, configs):
        for queue, color in colors.items():
            values = [groups[(queue, ma, sd, s)] for s in sources]
            axis.plot(sources, [statistics.median(v) for v in values], marker='o', color=color, label=queue.capitalize())
            axis.fill_between(sources, [min(v) for v in values], [max(v) for v in values], color=color, alpha=.13)
        axis.set_title(f'MovingAverage {ma} + SpikeDetector {sd}', fontsize=10)
        axis.set_xscale('log', base=2)
        axis.set_xticks(sources, [str(s) for s in sources])
        axis.set_xlabel('S: sorgenti')
        axis.set_ylim(0, top)
        axis.grid(axis='y', alpha=.25)
        axis.spines[['top', 'right']].set_visible(False)
    axes[0][0].set_ylabel('Milioni di tuple/s')
    axes[0][0].legend(frameon=False, fontsize=8)
    figure.suptitle('Spike Detection multi-sorgente, runtime standard: mediane e intervallo min–max', fontsize=12)
    figure.tight_layout(rect=(0, 0, 1, .93))
    for extension in ('png', 'pdf', 'svg'):
        figure.savefig(args.csv.with_suffix('.' + extension), dpi=160, facecolor='white')
    print(f'Grafici salvati accanto a {args.csv}')


if __name__ == '__main__':
    main()
