#!/usr/bin/env python3
"""Confronto fra tre campagne con gli stessi parametri:

  harness   - code chiamate direttamente (BenchMark Vyukov vs RigTorp, 50 ripetizioni)
  try_pop   - MoStream, Pipeline originale (Communicator.pop = ciclo di try_pop)
  blocking  - MoStream, BlockingPipeline (pop bloccante della coda, EOS espliciti)

Uso: python3 confronta_pop.py HARNESS_DIR TRYPOP_DIR BLOCKING_DIR [--output DIR]

Produce, per FarmThroughput e PipelineGrain:
  - rapporto Rigtorp/Vyukov dei throughput mediani nelle tre campagne;
  - effetto del pop bloccante su ciascuna coda: throughput blocking / try_pop;
  - tabella CONFRONTO_POP.md, confronto_pop.csv e grafici PNG/PDF.
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
import statistics

QUEUES = ('vyukov', 'rigtorp')
SUITES = ('harness', 'try_pop', 'blocking')
KEYS = {'FarmThroughput': {'harness': ('queue', 'consumers', 'target_work_us', 'throughput_per_second'),
                           'mostream': ('backend', 'consumers', 'target_work_us', 'throughput')},
        'PipelineGrain': {'harness': ('queue', 'stages', 'target_total_us', 'throughput_per_second'),
                          'mostream': ('backend', 'stages', 'total_work_us', 'throughput')}}


def medians(path, keys):
    q, n, t, v = keys
    groups = defaultdict(list)
    with open(path, newline='') as stream:
        for r in csv.DictReader(stream):
            groups[(r[q], int(r[n]), float(r[t]))].append(float(r[v]))
    return {k: statistics.median(x) for k, x in groups.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('harness', type=Path)
    parser.add_argument('trypop', type=Path)
    parser.add_argument('blocking', type=Path)
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.blocking / 'confronto_pop'
    output.mkdir(parents=True, exist_ok=True)
    dirs = {'harness': args.harness, 'try_pop': args.trypop, 'blocking': args.blocking}
    rows = []
    for bench, keys in KEYS.items():
        data = {s: medians(dirs[s] / bench / 'raw.csv', keys['harness' if s == 'harness' else 'mostream'])
                for s in SUITES}
        common = set.intersection(*({(k[1], k[2]) for k in d} for d in data.values()))
        for n, t in sorted(common):
            row = dict(benchmark=bench, N=n, T_us=t)
            for s in SUITES:
                for q in QUEUES:
                    row[f'{s}_{q}'] = data[s][(q, n, t)]
                row[f'{s}_RV'] = data[s][('rigtorp', n, t)] / data[s][('vyukov', n, t)]
            for q in QUEUES:
                row[f'blocking_over_trypop_{q}'] = row[f'blocking_{q}'] / row[f'try_pop_{q}']
            rows.append(row)
    with (output / 'confronto_pop.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = ['# Pop bloccante vs try_pop in MoStream, con riferimento harness', '',
             'Throughput mediani su 50 ripetizioni, stessi parametri nelle tre campagne. '
             '**R/V** > 1 favorisce Rigtorp. **B/T** = throughput con pop bloccante / con try_pop, per la stessa coda. '
             'In PipelineGrain l\'harness con N=1 non usa code.', '']
    for bench in KEYS:
        lines += [f'## {bench}', '',
                  '| N | T µs | R/V harness | R/V try_pop | R/V blocking | Vyukov blocking | Rigtorp blocking | B/T Vyukov | B/T Rigtorp |',
                  '| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
        for r in rows:
            if r['benchmark'] == bench:
                lines.append(f"| {r['N']} | {r['T_us']:g} | {r['harness_RV']:.3f} | {r['try_pop_RV']:.3f} | "
                             f"{r['blocking_RV']:.3f} | {r['blocking_vyukov']:,.0f} | {r['blocking_rigtorp']:,.0f} | "
                             f"{r['blocking_over_trypop_vyukov']:.3f} | {r['blocking_over_trypop_rigtorp']:.3f} |")
        lines.append('')
    text = '\n'.join(lines)
    (output / 'CONFRONTO_POP.md').write_text(text)
    print(text)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print('matplotlib non disponibile: grafici non generati')
        return
    styles = {'harness': ('#0f766e', '-'), 'try_pop': ('#7c3aed', '--'), 'blocking': ('#dc2626', '-.')}
    for bench in KEYS:
        sel = [r for r in rows if r['benchmark'] == bench]
        ts = sorted({r['T_us'] for r in sel})
        label = 'N consumatori' if bench == 'FarmThroughput' else 'N stadi'
        # 1) R/V nelle tre campagne
        fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False, sharey=True)
        for ax, t in zip(axes[0], ts):
            line = sorted((r for r in sel if r['T_us'] == t), key=lambda r: r['N'])
            ns = [r['N'] for r in line]
            for s in SUITES:
                color, dash = styles[s]
                ax.plot(ns, [r[f'{s}_RV'] for r in line], dash, color=color, marker='o', markersize=4,
                        label={'harness': 'harness', 'try_pop': 'MoStream try_pop',
                               'blocking': 'MoStream pop bloccante'}[s])
            ax.axhline(1, color='#64748b', linestyle=':')
            ax.set_xscale('log', base=2)
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_title(f'T = {t:g} µs')
            ax.set_xlabel(label)
            ax.grid(alpha=0.3)
        axes[0][0].set_ylabel('Rigtorp / Vyukov (>1 favorisce Rigtorp)')
        axes[0][-1].legend(fontsize=7)
        fig.suptitle(f'{bench}: rapporto Rigtorp/Vyukov — harness, MoStream try_pop, MoStream pop bloccante')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'{bench}_RV_tre_campagne.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)
        # 2) throughput delle due code, try_pop vs bloccante (con harness come riferimento)
        fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False)
        colors = {'vyukov': '#1d4ed8', 'rigtorp': '#b45309'}
        for ax, t in zip(axes[0], ts):
            line = sorted((r for r in sel if r['T_us'] == t), key=lambda r: r['N'])
            ns = [r['N'] for r in line]
            for q in QUEUES:
                for s, dash, marker in (('harness', '-', 'o'), ('try_pop', '--', 's'), ('blocking', '-.', '^')):
                    if bench == 'PipelineGrain' and s == 'harness':
                        xs = [r['N'] for r in line if r['N'] > 1]
                        ys = [r[f'{s}_{q}'] for r in line if r['N'] > 1]
                    else:
                        xs, ys = ns, [r[f'{s}_{q}'] for r in line]
                    ax.plot(xs, ys, dash, color=colors[q], marker=marker, markersize=4, alpha=0.9,
                            label=f'{q} {s}')
            ax.set_xscale('log', base=2)
            ax.set_yscale('log')
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_title(f'T = {t:g} µs')
            ax.set_xlabel(label)
            ax.grid(alpha=0.3)
        axes[0][0].set_ylabel('throughput (input/s)')
        axes[0][-1].legend(fontsize=6)
        fig.suptitle(f'{bench}: throughput — harness (continua), try_pop (tratteggio), pop bloccante (tratto-punto)'
                     + (' — harness N=1 omesso (senza code)' if bench == 'PipelineGrain' else ''))
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'{bench}_throughput_tre_campagne.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)
    print('Grafici in', output)


if __name__ == '__main__':
    main()
