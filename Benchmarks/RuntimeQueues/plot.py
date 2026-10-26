"""Grafico del throughput misurato nella vera Pipeline MoStream."""
import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    with (args.results / 'summary.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    targets = sorted({float(row['target_us']) for row in rows})
    fig, axes = plt.subplots(1, len(targets), figsize=(15, 5.5), sharey=True, squeeze=False)
    colors = {'vyukov': '#d97706', 'rigtorp': '#2563eb'}
    for ax, target in zip(axes[0], targets):
        group = sorted((r for r in rows if float(r['target_us']) == target), key=lambda r: int(r['workers']))
        workers = [int(r['workers']) for r in group]
        for backend in ('vyukov', 'rigtorp'):
            median = [float(r[f'{backend}_median']) / 1e6 for r in group]
            lower = [float(r[f'{backend}_min']) / 1e6 for r in group]
            upper = [float(r[f'{backend}_max']) / 1e6 for r in group]
            ax.fill_between(workers, lower, upper, color=colors[backend], alpha=0.14)
            ax.plot(workers, median, marker='o', linewidth=2.2, markersize=6,
                    color=colors[backend], label=backend.capitalize())
        ax.set_title(f'T = {target:g} µs per input', fontsize=13, pad=12)
        ax.set_xscale('log', base=2)
        ax.set_xticks(workers, [str(n) for n in workers])
        ax.set_xlabel('N: worker di calcolo (task totali = N + 2)', fontsize=10)
        ax.set_ylim(bottom=0)
        ax.grid(axis='y', color='#d1d5db', alpha=0.7)
        ax.spines[['top', 'right']].set_visible(False)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda value, pos: f'{value:.1f}'.replace('.', ',')))
    axes[0, 0].set_ylabel('Throughput (milioni di input/s) · maggiore è meglio', fontsize=11)
    axes[0, -1].legend(frameon=False, loc='upper left')
    fig.suptitle('Rigtorp e Vyukov nei Communicator di MoStream', fontsize=17, fontweight='bold', y=0.98)
    fig.text(0.5, 0.9, 'Source sequenziale → Compute parallelo → Sink sequenziale | Runtime standard', ha='center', fontsize=11)
    fig.text(0.5, 0.045, 'Linee: mediane di 5 ripetizioni · Fasce: minimo–massimo osservato, non intervalli di confidenza', ha='center', fontsize=10)
    fig.text(0.5, 0.005, '20.000 input · Capacità 1024 · Pinning disabilitato · Mojo 1.1 · 26 ottobre 2026 · Prova preliminare', ha='center', fontsize=9, color='#4b5563')
    fig.subplots_adjust(top=0.79, bottom=0.19, left=0.075, right=0.985, wspace=0.12)
    for extension in ('png', 'pdf', 'svg'):
        path = args.results / f'runtime_farm_throughput.{extension}'
        fig.savefig(path, dpi=200, bbox_inches='tight', facecolor='white')
        print(path)
    plt.close(fig)


if __name__ == '__main__':
    main()
