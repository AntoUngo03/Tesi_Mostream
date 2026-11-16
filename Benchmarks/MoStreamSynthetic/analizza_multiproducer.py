#!/usr/bin/env python3
"""Analisi della campagna MultiProducer (Fase 0: contesa fra produttori).

Uso: python3 analizza_multiproducer.py CAMPAGNA_MULTIPRODUCER CAMPAGNA_FARM [--output DIR]

CAMPAGNA_MULTIPRODUCER contiene MultiProducer/raw.csv; CAMPAGNA_FARM contiene
FarmThroughput/raw.csv misurata con lo stesso Communicator (riferimento 1:N).
Per ogni scenario e configurazione: rapporto Rigtorp/Vyukov dei throughput
mediani con IC bootstrap 95% appaiato per ripetizione, coppie vinte, test dei
segni ed esito, con lo stesso metodo di analizza_rigtorp.py.
"""
import argparse
import csv
from pathlib import Path
import random
import statistics

from analizza_rigtorp import analyze, RESAMPLES, THRESHOLD, SEED

SCENARIOS = {'n1': 'N:1 (P produttori, 1 consumatore)', 'nm': 'N:M (P produttori, P consumatori)'}


def read(path):
    with open(path, newline='') as stream:
        return list(csv.DictReader(stream))


def imbalance(raw, n_key, t_key):
    """Sbilanciamento del carico fra consumatori, dai conteggi registrati a ogni esecuzione.

    Per esecuzione: max/media (1 = bilanciato, C = tutto a un consumatore), min/media
    e coefficiente di variazione dei conteggi. Per configurazione e coda: mediana sulle
    ripetizioni e caso peggiore. Solo configurazioni con più di un consumatore.
    """
    groups = {}
    for r in raw:
        counts = [int(x) for x in str(r['worker_counts']).strip('[] ').replace(' ', '').split(',') if x != '']
        if len(counts) < 2:
            continue
        mean = sum(counts) / len(counts)
        groups.setdefault((int(r[n_key]), float(r[t_key]), r['backend']), []).append(
            (max(counts) / mean, min(counts) / mean, statistics.pstdev(counts) / mean))
    out = []
    for (n, t, q), values in sorted(groups.items()):
        out.append(dict(N=n, T_us=t, backend=q, consumers=n, repeats=len(values),
                        max_over_mean_median=statistics.median(v[0] for v in values),
                        max_over_mean_worst=max(v[0] for v in values),
                        min_over_mean_median=statistics.median(v[1] for v in values),
                        min_over_mean_worst=min(v[1] for v in values),
                        cv_median=statistics.median(v[2] for v in values)))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('multiproducer', type=Path)
    parser.add_argument('farm', type=Path)
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.multiproducer / 'analisi'
    output.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    raw = read(args.multiproducer / 'MultiProducer' / 'raw.csv')
    results = {s: analyze([r for r in raw if r['scenario'] == s], 'producers', 'T_us', rng) for s in SCENARIOS}
    results = {s: v for s, v in results.items() if v}
    results['farm'] = analyze(read(args.farm / 'FarmThroughput' / 'raw.csv'), 'consumers', 'target_work_us', rng)

    fields = ['scenario', 'N', 'T_us', 'repeats', 'vyukov_median', 'rigtorp_median', 'ratio', 'ci95_low',
              'ci95_high', 'rigtorp_wins', 'sign_test_p', 'vyukov_cv', 'rigtorp_cv', 'verdict']
    with (output / 'valutazione.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for s, rs in results.items():
            for r in rs:
                writer.writerow(dict(r, scenario=s))

    lines = ['# Fase 0: contesa fra produttori (Communicator con EOS espliciti)', '',
             f'Throughput mediano su 50 ripetizioni, input/s. **R/V** > 1 favorisce Rigtorp. IC 95% bootstrap appaiato '
             f'({RESAMPLES:,} ricampionamenti), soglia pratica {THRESHOLD:.0%}, intervalli individuali.', '',
             'Riferimento 1:N: FarmThroughput con lo stesso Communicator (N = consumatori).', '']
    titles = dict(SCENARIOS, farm='1:N (1 produttore, N consumatori), FarmThroughput')
    for s, rs in results.items():
        counts = {}
        for r in rs:
            counts[r['verdict']] = counts.get(r['verdict'], 0) + 1
        lines += [f'## {titles[s]}', '', 'Esiti: ' + ', '.join(f'{k}: {v}' for k, v in sorted(counts.items())), '',
                  '| N | T µs | Vyukov | Rigtorp | R/V | IC 95% | vinte R | CV V | CV R | Esito |',
                  '| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |']
        for r in rs:
            lines.append(f"| {r['N']} | {r['T_us']:g} | {r['vyukov_median']:,.0f} | {r['rigtorp_median']:,.0f} | "
                         f"{r['ratio']:.3f} | {r['ci95_low']:.3f}–{r['ci95_high']:.3f} | {r['rigtorp_wins']}/{r['repeats']} | "
                         f"{r['vyukov_cv']:.1%} | {r['rigtorp_cv']:.1%} | {r['verdict']} |")
        lines.append('')
    # Sbilanciamento del carico: N:M (C = P consumatori) e farm 1:N (N consumatori).
    farm_raw = read(args.farm / 'FarmThroughput' / 'raw.csv')
    balance = {'nm': imbalance([r for r in raw if r['scenario'] == 'nm'], 'consumers', 'T_us'),
               'farm': imbalance(farm_raw, 'consumers', 'target_work_us')}
    balance = {s: v for s, v in balance.items() if v}
    bfields = ['scenario', 'N', 'T_us', 'backend', 'consumers', 'repeats', 'max_over_mean_median',
               'max_over_mean_worst', 'min_over_mean_median', 'min_over_mean_worst', 'cv_median']
    with (output / 'sbilanciamento.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=bfields)
        writer.writeheader()
        for s, rs in balance.items():
            for r in rs:
                writer.writerow(dict(r, scenario=s))
    lines += ['# Sbilanciamento del carico fra consumatori', '',
              'Per esecuzione: **max/media** = input del consumatore più carico / media (1 = bilanciato, '
              'C = tutto a un consumatore); **min/media** = consumatore meno carico; **CV** = coefficiente di '
              'variazione dei conteggi. Mediana sulle ripetizioni e caso peggiore.', '']
    for s, rs in balance.items():
        lines += [f'## {titles[s]}', '',
                  '| N | T µs | coda | max/media (mediana) | max/media (peggiore) | min/media (mediana) | '
                  'min/media (peggiore) | CV (mediana) |',
                  '| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |']
        for r in rs:
            lines.append(f"| {r['N']} | {r['T_us']:g} | {r['backend']} | {r['max_over_mean_median']:.3f} | "
                         f"{r['max_over_mean_worst']:.3f} | {r['min_over_mean_median']:.3f} | "
                         f"{r['min_over_mean_worst']:.3f} | {r['cv_median']:.1%} |")
        lines.append('')
    text = '\n'.join(lines)
    (output / 'ANALISI.md').write_text(text)
    print(text)

    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print('matplotlib non disponibile: grafici non generati')
        return
    colors = {'Rigtorp': '#b45309', 'Vyukov': '#1d4ed8', 'differenza < 5%': '#64748b', 'non distinguibile': '#94a3b8'}
    style = {'farm': ('#0f766e', '-', '1:N contesa fra consumatori (FarmThroughput)'),
             'n1': ('#dc2626', '--', 'N:1 contesa fra produttori'),
             'nm': ('#7c3aed', '-.', 'N:M contesa su entrambi i lati')}
    ts = sorted({r['T_us'] for r in results['farm']})
    # 1) confronto dei rapporti R/V nelle tre topologie, con IC
    fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4.2), squeeze=False, sharey=True)
    for ax, t in zip(axes[0], ts):
        ax.axhspan(1 - THRESHOLD, 1 + THRESHOLD, color='#e2e8f0')
        ax.axhline(1, color='#64748b', linestyle=':')
        for s in ('farm', 'n1', 'nm'):
            if s not in results:
                continue
            sel = sorted((r for r in results[s] if r['T_us'] == t), key=lambda r: r['N'])
            color, dash, label = style[s]
            xs = [r['N'] for r in sel]
            ax.plot(xs, [r['ratio'] for r in sel], dash, color=color, marker='o', markersize=4, label=label)
            ax.errorbar(xs, [r['ratio'] for r in sel],
                        yerr=[[r['ratio'] - r['ci95_low'] for r in sel], [r['ci95_high'] - r['ratio'] for r in sel]],
                        fmt='none', ecolor=color, alpha=0.6, capsize=2)
        ns = sorted({r['N'] for s in results for r in results[s]})
        ax.set_xscale('log', base=2)
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.set_title(f'T = {t:g} µs')
        ax.set_xlabel('N (repliche dal lato conteso)')
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel('Rigtorp / Vyukov (IC 95%)')
    axes[0][-1].legend(fontsize=7)
    fig.suptitle('Rapporto Rigtorp/Vyukov per lato della contesa — Communicator con EOS espliciti')
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(output / f'RV_per_lato_contesa.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)
    # 2) per ogni scenario MultiProducer: R/V con IC colorato per esito, e throughput assoluto
    for s in [x for x in ('n1', 'nm') if x in results]:
        fig, axes = plt.subplots(2, len(ts), figsize=(4.2 * len(ts), 7.4), squeeze=False)
        for j, t in enumerate(ts):
            sel = sorted((r for r in results[s] if r['T_us'] == t), key=lambda r: r['N'])
            ns = [r['N'] for r in sel]
            ax = axes[0][j]
            ax.axhspan(1 - THRESHOLD, 1 + THRESHOLD, color='#e2e8f0')
            ax.axhline(1, color='#64748b', linestyle=':')
            ax.plot(ns, [r['ratio'] for r in sel], '-', color='#7c3aed', alpha=0.5)
            for r in sel:
                ax.errorbar(r['N'], r['ratio'], yerr=[[r['ratio'] - r['ci95_low']], [r['ci95_high'] - r['ratio']]],
                            fmt='o', color=colors[r['verdict']], capsize=3)
            ax.set_xscale('log', base=2)
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_title(f'T = {t:g} µs')
            ax.grid(alpha=0.3)
            ax = axes[1][j]
            for q, c in (('vyukov', '#1d4ed8'), ('rigtorp', '#b45309')):
                med = [statistics.median(r['samples'][q]) for r in sel]
                lo = [min(r['samples'][q]) for r in sel]
                hi = [max(r['samples'][q]) for r in sel]
                ax.plot(ns, med, '-o', color=c, markersize=4, label=q.capitalize())
                ax.fill_between(ns, lo, hi, color=c, alpha=0.12)
            ax.set_xscale('log', base=2)
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_xlabel('P produttori')
            ax.grid(alpha=0.3)
        axes[0][0].set_ylabel('Rigtorp / Vyukov (IC 95%)')
        axes[1][0].set_ylabel('throughput (input/s), mediana e min–max')
        handles = [plt.Line2D([], [], marker='o', linestyle='', color=c, label=k) for k, c in colors.items()]
        axes[0][-1].legend(handles=handles, fontsize=7)
        axes[1][-1].legend(fontsize=8)
        fig.suptitle(f'MultiProducer {SCENARIOS[s]}: rapporto Rigtorp/Vyukov (sopra) e throughput (sotto)')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'MultiProducer_{s}.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)
    # 3) sbilanciamento: max/media per coda, farm 1:N e N:M
    fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4.2), squeeze=False, sharey=True)
    for ax, t in zip(axes[0], ts):
        ax.axhline(1, color='#64748b', linestyle=':')
        for s, dash in (('farm', '-'), ('nm', '--')):
            for q, c in (('vyukov', '#1d4ed8'), ('rigtorp', '#b45309')):
                sel = sorted((r for r in balance.get(s, []) if r['T_us'] == t and r['backend'] == q),
                             key=lambda r: r['N'])
                if not sel:
                    continue
                xs = [r['N'] for r in sel]
                ax.plot(xs, [r['max_over_mean_median'] for r in sel], dash, color=c, marker='o', markersize=4,
                        label=f"{q.capitalize()} {'1:N' if s == 'farm' else 'N:M'}")
                ax.fill_between(xs, [r['max_over_mean_median'] for r in sel],
                                [r['max_over_mean_worst'] for r in sel], color=c, alpha=0.08)
        ns = sorted({r['N'] for s in balance for r in balance[s]})
        ax.set_xscale('log', base=2)
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.set_title(f'T = {t:g} µs')
        ax.set_xlabel('consumatori')
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel('max / media (1 = bilanciato)')
    axes[0][-1].legend(fontsize=7)
    fig.suptitle('Sbilanciamento del carico fra consumatori: mediana (linea) e caso peggiore (fascia)')
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(output / f'sbilanciamento.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)
    print('Grafici in', output)


if __name__ == '__main__':
    main()
