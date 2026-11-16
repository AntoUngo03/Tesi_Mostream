#!/usr/bin/env python3
"""Confronto fra la campagna harness (code chiamate direttamente) e la stessa
campagna eseguita dentro MoStream (Pipeline + Communicator).

Uso:
  python3 confronta_harness.py HARNESS_DIR MOSTREAM_DIR [--output DIR]

HARNESS_DIR contiene FarmThroughput/ e PipelineGrain/ della campagna
"BenchMark Vyukov vs RigTorp/Campagna_50_ripetizioni"; MOSTREAM_DIR quelli
prodotti da MoStreamSynthetic/*/run.py. Tutte le statistiche sono mediane
delle ripetizioni. Scalabilità:
  - farm: speedup(N) = throughput(N) / throughput(N=1) della stessa suite e
    coda; efficienza = speedup / N.
  - pipeline: speedup(N) = throughput(N) / throughput seriale senza code,
    con lo stesso riferimento per le due suite (N=1 dell'harness).
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
import statistics

QUEUES = ('vyukov', 'rigtorp')


def read(path):
    with open(path, newline='') as stream:
        return list(csv.DictReader(stream))


def medians(rows, queue_key, n_key, t_key, value_key):
    groups = defaultdict(list)
    for r in rows:
        groups[(r[queue_key], int(r[n_key]), float(r[t_key]))].append(float(r[value_key]))
    return {k: statistics.median(v) for k, v in groups.items()}, {k: len(v) for k, v in groups.items()}


def load(harness, mostream):
    data = {}
    data['farm_h'], data['farm_h_n'] = medians(read(harness / 'FarmThroughput/raw.csv'),
                                               'queue', 'consumers', 'target_work_us', 'throughput_per_second')
    data['farm_m'], data['farm_m_n'] = medians(read(mostream / 'FarmThroughput/raw.csv'),
                                               'backend', 'consumers', 'target_work_us', 'throughput')
    data['grain_h'], data['grain_h_n'] = medians(read(harness / 'PipelineGrain/raw.csv'),
                                                 'queue', 'stages', 'target_total_us', 'throughput_per_second')
    data['grain_m'], data['grain_m_n'] = medians(read(mostream / 'PipelineGrain/raw.csv'),
                                                 'backend', 'stages', 'total_work_us', 'throughput')
    return data


def build_rows(data):
    rows = []
    ns_farm = sorted({k[1] for k in data['farm_m']} & {k[1] for k in data['farm_h']})
    ts_farm = sorted({k[2] for k in data['farm_m']} & {k[2] for k in data['farm_h']})
    for q in QUEUES:
        for t in ts_farm:
            h1, m1 = data['farm_h'][(q, 1, t)], data['farm_m'][(q, 1, t)]
            for n in ns_farm:
                h, m = data['farm_h'][(q, n, t)], data['farm_m'][(q, n, t)]
                rows.append(dict(benchmark='FarmThroughput', queue=q, N=n, T_us=t,
                                 harness=h, mostream=m, mostream_over_harness=m / h,
                                 harness_speedup=h / h1, mostream_speedup=m / m1,
                                 harness_efficiency=h / h1 / n, mostream_efficiency=m / m1 / n,
                                 ideal=n * 1e6 / t if t else '',
                                 repeats_harness=data['farm_h_n'][(q, n, t)],
                                 repeats_mostream=data['farm_m_n'][(q, n, t)]))
    ns_grain = sorted({k[1] for k in data['grain_m']} & {k[1] for k in data['grain_h']})
    ts_grain = sorted({k[2] for k in data['grain_m']} & {k[2] for k in data['grain_h']})
    for q in QUEUES:
        for t in ts_grain:
            serial = data['grain_h'][(q, 1, t)]  # N=1 harness: nessuna coda
            for n in ns_grain:
                h, m = data['grain_h'][(q, n, t)], data['grain_m'][(q, n, t)]
                rows.append(dict(benchmark='PipelineGrain', queue=q, N=n, T_us=t,
                                 harness=h, mostream=m, mostream_over_harness=m / h,
                                 harness_speedup=h / serial, mostream_speedup=m / serial,
                                 harness_efficiency=h / serial / n, mostream_efficiency=m / serial / n,
                                 ideal=n * 1e6 / t if t else '',
                                 repeats_harness=data['grain_h_n'][(q, n, t)],
                                 repeats_mostream=data['grain_m_n'][(q, n, t)]))
    return rows


def fmt(x, digits=0):
    if x == '':
        return '—'
    return f'{x:,.{digits}f}'.replace(',', ' ') if digits == 0 else f'{x:.{digits}f}'


def markdown(rows):
    out = ['# Harness vs MoStream: FarmThroughput e PipelineGrain', '',
           'Throughput mediano in input/s. **M/H** = MoStream / harness (< 1: MoStream più lento). '
           'Speedup ed efficienza come descritto in `confronta_harness.py`. '
           'Nota: in PipelineGrain l\'harness con N=1 non usa code (baseline seriale), '
           'mentre MoStream con N=1 ha comunque sorgente, stadio e sink (2 code).', '']
    for bench in ('FarmThroughput', 'PipelineGrain'):
        out += [f'## {bench}', '',
                '| Coda | N | T µs | Harness | MoStream | M/H | Speedup H | Speedup M | Eff. H | Eff. M |',
                '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
        for r in rows:
            if r['benchmark'] != bench:
                continue
            out.append(f"| {r['queue']} | {r['N']} | {r['T_us']:g} | {fmt(r['harness'])} | {fmt(r['mostream'])} | "
                       f"{r['mostream_over_harness']:.3f} | {r['harness_speedup']:.2f} | {r['mostream_speedup']:.2f} | "
                       f"{r['harness_efficiency']:.2f} | {r['mostream_efficiency']:.2f} |")
        out.append('')
    return '\n'.join(out)


def plots(rows, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    styles = {('harness', 'vyukov'): ('#1d4ed8', '-', 'o'), ('harness', 'rigtorp'): ('#b45309', '-', 's'),
              ('mostream', 'vyukov'): ('#1d4ed8', '--', 'o'), ('mostream', 'rigtorp'): ('#b45309', '--', 's')}
    for bench in ('FarmThroughput', 'PipelineGrain'):
        selected = [r for r in rows if r['benchmark'] == bench]
        ts = sorted({r['T_us'] for r in selected})
        for metric, ylabel, log, stem in (('', 'throughput (input/s)', True, 'throughput'),
                                          ('_speedup', 'speedup', False, 'speedup'),
                                          ('_efficiency', 'efficienza = speedup / N', False, 'efficienza')):
            fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False)
            for ax, t in zip(axes[0], ts):
                for suite in ('harness', 'mostream'):
                    for q in QUEUES:
                        line = sorted((r for r in selected if r['T_us'] == t and r['queue'] == q), key=lambda r: r['N'])
                        color, dash, marker = styles[(suite, q)]
                        ax.plot([r['N'] for r in line], [r[suite + metric] for r in line], dash, color=color,
                                marker=marker, markersize=4, label=f'{suite} {q}')
                ns = sorted({r['N'] for r in selected})
                if metric == '' and t:
                    ax.plot(ns, [n * 1e6 / t for n in ns], ':', color='#64748b', label='ideale N/T')
                if metric == '_speedup':
                    ax.plot(ns, ns, ':', color='#64748b', label='lineare')
                if metric == '_efficiency':
                    ax.axhline(1.0, linestyle=':', color='#64748b', label='ideale')
                    ax.set_ylim(0, 1.1)
                ax.set_xscale('log', base=2)
                ax.set_xticks(ns, [str(n) for n in ns])
                if log:
                    ax.set_yscale('log')
                ax.set_title(f'T = {t:g} µs')
                ax.set_xlabel('N consumatori' if bench == 'FarmThroughput' else 'N stadi')
                ax.grid(alpha=0.3)
            axes[0][0].set_ylabel(ylabel)
            axes[0][-1].legend(fontsize=7)
            reference = ('rispetto a N=1 della stessa suite' if bench == 'FarmThroughput'
                         else 'rispetto al seriale senza code (harness N=1)')
            fig.suptitle(f'{bench}: harness (linea continua) vs MoStream (tratteggio)'
                         + (f' — {stem} {reference}' if metric else ''))
            fig.tight_layout()
            for ext in ('png', 'pdf'):
                fig.savefig(output / f'{bench}_{stem}.{ext}', dpi=160, bbox_inches='tight')
            plt.close(fig)
        # Rapporto Rigtorp/Vyukov nelle due suite: >1 favorisce Rigtorp.
        fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False, sharey=True)
        for ax, t in zip(axes[0], ts):
            ns = sorted({r['N'] for r in selected})
            for suite, color, dash in (('harness', '#0f766e', '-'), ('mostream', '#7c3aed', '--')):
                value = {(r['queue'], r['N']): r[suite] for r in selected if r['T_us'] == t}
                ax.plot(ns, [value[('rigtorp', n)] / value[('vyukov', n)] for n in ns], dash, color=color,
                        marker='o', markersize=4, label=suite)
            ax.axhline(1.0, linestyle=':', color='#64748b')
            ax.set_xscale('log', base=2)
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_title(f'T = {t:g} µs')
            ax.set_xlabel('N consumatori' if bench == 'FarmThroughput' else 'N stadi')
            ax.grid(alpha=0.3)
        axes[0][0].set_ylabel('Rigtorp / Vyukov (>1 favorisce Rigtorp)')
        axes[0][-1].legend(fontsize=8)
        fig.suptitle(f'{bench}: rapporto dei throughput mediani Rigtorp/Vyukov, harness vs MoStream')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'{bench}_rigtorp_su_vyukov.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('harness', type=Path)
    parser.add_argument('mostream', type=Path)
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.mostream / 'confronto_harness'
    output.mkdir(parents=True, exist_ok=True)
    rows = build_rows(load(args.harness, args.mostream))
    with (output / 'confronto.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    text = markdown(rows)
    (output / 'CONFRONTO.md').write_text(text)
    print(text)
    try:
        plots(rows, output)
        print('\nGrafici in', output)
    except ImportError:
        print('\nmatplotlib non disponibile: grafici non generati')


if __name__ == '__main__':
    main()
