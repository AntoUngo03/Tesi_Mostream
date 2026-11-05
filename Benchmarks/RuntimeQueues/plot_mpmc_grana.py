"""Grafico della farm MPMC al variare della grana T: un pannello per ogni T, throughput in funzione di N.

Legge summary.csv prodotto da run_mpmc.py con una sola capacità e una sola modalità di pinning.
Linee: mediane; fasce: minimo e massimo delle ripetizioni. La linea tratteggiata è il
throughput ideale del solo calcolo, 2N / T input al secondo: ogni input costa T µs in totale,
metà nello stadio A e metà nello stadio B, e ciascuno dei due stadi ha N repliche, quindi
2N thread lavorano in parallelo per T/2 su ogni input. Per T = 0 non è definita.
Produce mpmc_grana.png/.pdf/.svg nella cartella dei risultati.
"""
import argparse
import csv
import math
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    rows = list(csv.DictReader((args.results / 'summary.csv').open()))
    targets = sorted({float(r['target_us']) for r in rows})
    colors = {'vyukov': '#d97706', 'rigtorp': '#2563eb'}
    columns = 4
    lines = math.ceil(len(targets) / columns)
    fig, axes = plt.subplots(lines, columns, figsize=(4.2 * columns, 3.6 * lines), squeeze=False)
    for ax, target in zip(axes.flat, targets):
        group = sorted((r for r in rows if float(r['target_us']) == target), key=lambda r: int(r['workers']))
        n = [int(r['workers']) for r in group]
        top = 0.0
        for b, color in colors.items():
            median = [float(r[b + '_median']) / 1e6 for r in group]
            ax.plot(n, median, marker='o', color=color, label=b.capitalize())
            ax.fill_between(n, [float(r[b + '_min']) / 1e6 for r in group],
                            [float(r[b + '_max']) / 1e6 for r in group], color=color, alpha=.13)
            top = max(top, max(float(r[b + '_max']) / 1e6 for r in group))
        if target > 0:
            ideal = [2 * v / target for v in n]  # 2N / T(µs) = milioni di input/s
            ax.plot(n, ideal, '--', color='#64748b', label='Ideale 2N / T')
            top = max(top, min(max(ideal), top * 1.6))
        ax.set_ylim(0, top * 1.08)
        ax.set_title(f'T = {target:g} µs per input')
        ax.set_xscale('log', base=2)
        ax.set_xticks(n, [str(v) for v in n])
        ax.set_xlabel('N repliche per stadio (2N+2 task)')
        ax.set_ylabel('Milioni di input/s')
        ax.grid(axis='y', alpha=.25)
        ax.spines[['top', 'right']].set_visible(False)
    axes.flat[0].legend(frameon=False, fontsize=8)
    for ax in list(axes.flat)[len(targets):]:
        ax.set_visible(False)
    fig.suptitle('MoStream: Source → N Compute A → N Compute B → Sink, al variare della grana T', fontsize=14)
    fig.text(.5, .005, 'Mediane del throughput · fasce min–max · ideale 2N/T troncato quando esce dalla scala',
             ha='center', fontsize=9)
    fig.tight_layout(rect=(0, .02, 1, .96))
    for ext in ('png', 'pdf', 'svg'):
        fig.savefig(args.results / f'mpmc_grana.{ext}', dpi=160, facecolor='white')
    print(f'Grafici salvati in {args.results}')


if __name__ == '__main__':
    main()
