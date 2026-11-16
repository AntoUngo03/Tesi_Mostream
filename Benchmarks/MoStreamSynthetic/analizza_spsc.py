#!/usr/bin/env python3
"""Analisi della campagna PipelineGrain con tre code: Vyukov, Rigtorp e SPSC FastFlow.

Uso: python3 analizza_spsc.py CAMPAGNA_SPSC [CAMPAGNA_RIFERIMENTO] [--output DIR]

CAMPAGNA_SPSC contiene PipelineGrain/raw.csv e metadata.json con i backend
vyukov, rigtorp e spsc. CAMPAGNA_RIFERIMENTO (facoltativa) è una campagna
PipelineGrain precedente con lo stesso Communicator: serve a controllare che
Vyukov e Rigtorp abbiano gli stessi valori, cioè che la macchina fosse nelle
stesse condizioni.

Per ogni configurazione (N, T): mediane e CV; rapporti SPSC/Vyukov, SPSC/Rigtorp
e Rigtorp/Vyukov con IC bootstrap 95% appaiato per ripetizione (le tre code sono
misurate nella stessa ripetizione), coppie vinte, test dei segni, esito con la
stessa regola di analizza_rigtorp.py; speedup rispetto al seriale senza code.
"""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
import random
import statistics

from analizza_rigtorp import sign_test, verdict, RESAMPLES, THRESHOLD, SEED

QUEUES = ('vyukov', 'rigtorp', 'spsc')
PAIRS = (('spsc', 'vyukov'), ('spsc', 'rigtorp'), ('rigtorp', 'vyukov'))
NAMES = {'vyukov': 'Vyukov', 'rigtorp': 'Rigtorp', 'spsc': 'SPSC'}


def read(path):
    with open(path, newline='') as stream:
        return list(csv.DictReader(stream))


def grouped(rows):
    """{(N, T): {repeat: {coda: throughput}}}"""
    g = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        g[(int(r['stages']), float(r['total_work_us']))][int(r['repeat'])][r['backend']] = float(r['throughput'])
    return g


def paired_ratio(by_repeat, a, b, rng):
    reps = [p for p in by_repeat.values() if a in p and b in p]
    xa, xb = [p[a] for p in reps], [p[b] for p in reps]
    ratio = statistics.median(xa) / statistics.median(xb)
    n = len(reps)
    boot = []
    for _ in range(RESAMPLES):
        idx = [rng.randrange(n) for _ in range(n)]
        boot.append(statistics.median(xa[i] for i in idx) / statistics.median(xb[i] for i in idx))
    boot.sort()
    low, high = boot[int(0.025 * RESAMPLES)], boot[int(0.975 * RESAMPLES) - 1]
    wins = sum(x > y for x, y in zip(xa, xb))
    return dict(ratio=ratio, low=low, high=high, wins=wins, n=n, p=sign_test(wins, n),
                verdict=verdict(ratio, low, high))


def label(v, a, b):
    """Esito leggibile per il confronto a/b."""
    return {'Rigtorp': NAMES[a], 'Vyukov': NAMES[b]}.get(v, v)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('campaign', type=Path)
    parser.add_argument('reference', type=Path, nargs='?')
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.campaign / 'analisi'
    output.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    meta = json.loads((args.campaign / 'PipelineGrain' / 'metadata.json').read_text())
    serial = {float(t): 1e9 / min(v) if min(v) > 0 else None for t, v in meta['compute_samples_ns_per_input'].items()}
    data = grouped(read(args.campaign / 'PipelineGrain' / 'raw.csv'))

    results = []
    for (n, t), by_repeat in sorted(data.items()):
        entry = dict(N=n, T_us=t)
        for q in QUEUES:
            vals = [p[q] for p in by_repeat.values() if q in p]
            entry[f'{q}_median'] = statistics.median(vals)
            entry[f'{q}_cv'] = statistics.stdev(vals) / statistics.mean(vals)
            entry[f'{q}_speedup'] = entry[f'{q}_median'] / serial[t] if serial.get(t) else None
            entry[f'{q}_samples'] = vals
        for a, b in PAIRS:
            r = paired_ratio(by_repeat, a, b, rng)
            for k, v in r.items():
                entry[f'{a}_{b}_{k}'] = v
        results.append(entry)

    fields = ['N', 'T_us'] + [f'{q}_{k}' for q in QUEUES for k in ('median', 'cv', 'speedup')] + \
             [f'{a}_{b}_{k}' for a, b in PAIRS for k in ('ratio', 'low', 'high', 'wins', 'n', 'p', 'verdict')]
    with (output / 'valutazione.csv').open('w', newline='') as stream:
        w = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(results)

    lines = ['# PipelineGrain con tre code: Vyukov, Rigtorp e SPSC FastFlow', '',
             f'Communicator con EOS espliciti, runtime standard. Throughput mediano su 50 ripetizioni (milioni di input/s). '
             f'Rapporti con IC 95% bootstrap appaiato per ripetizione ({RESAMPLES:,} ricampionamenti), soglia pratica '
             f'{THRESHOLD:.0%}, intervalli individuali. Calibrazione: {meta["ns_per_iteration"]:.3f} ns/iterazione.', '']
    for a, b in PAIRS:
        counts = defaultdict(int)
        for e in results:
            counts[label(e[f'{a}_{b}_verdict'], a, b)] += 1
        lines += [f'## {NAMES[a]} / {NAMES[b]}', '', 'Esiti: ' + ', '.join(f'{k}: {v}' for k, v in sorted(counts.items())), '',
                  f'| N | T µs | {NAMES[a]} | {NAMES[b]} | rapporto | IC 95% | vinte {NAMES[a]} | esito |',
                  '| ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |']
        for e in results:
            lines.append(f"| {e['N']} | {e['T_us']:g} | {e[f'{a}_median'] / 1e6:.3f} | {e[f'{b}_median'] / 1e6:.3f} | "
                         f"{e[f'{a}_{b}_ratio']:.3f} | {e[f'{a}_{b}_low']:.3f}–{e[f'{a}_{b}_high']:.3f} | "
                         f"{e[f'{a}_{b}_wins']}/{e[f'{a}_{b}_n']} | {label(e[f'{a}_{b}_verdict'], a, b)} |")
        lines.append('')
    lines += ['## Speedup rispetto al seriale senza code', '',
              '| N | T µs | Vyukov | Rigtorp | SPSC |', '| ---: | ---: | ---: | ---: | ---: |']
    for e in results:
        if e['T_us'] > 0:
            lines.append(f"| {e['N']} | {e['T_us']:g} | {e['vyukov_speedup']:.2f} | {e['rigtorp_speedup']:.2f} | "
                         f"{e['spsc_speedup']:.2f} |")
    lines.append('')
    lines += ['## Variabilità (CV)', '', '| N | T µs | Vyukov | Rigtorp | SPSC |', '| ---: | ---: | ---: | ---: | ---: |']
    for e in results:
        lines.append(f"| {e['N']} | {e['T_us']:g} | {e['vyukov_cv']:.1%} | {e['rigtorp_cv']:.1%} | {e['spsc_cv']:.1%} |")
    lines.append('')

    if args.reference:
        ref = grouped(read(args.reference / 'PipelineGrain' / 'raw.csv'))
        lines += ['## Riproducibilità: Vyukov e Rigtorp rispetto alla campagna di riferimento', '',
                  f'Riferimento: `{args.reference.name}`. Rapporto fra le mediane (questa campagna / riferimento): '
                  'valori vicini a 1 indicano condizioni della macchina comparabili.', '',
                  '| N | T µs | Vyukov | Rigtorp |', '| ---: | ---: | ---: | ---: |']
        drift = []
        for e in results:
            key = (e['N'], e['T_us'])
            if key not in ref:
                continue
            row = []
            for q in ('vyukov', 'rigtorp'):
                old = statistics.median(p[q] for p in ref[key].values() if q in p)
                row.append(e[f'{q}_median'] / old)
                drift.append(e[f'{q}_median'] / old)
            lines.append(f"| {e['N']} | {e['T_us']:g} | {row[0]:.3f} | {row[1]:.3f} |")
        if drift:
            lines += ['', f'Mediana dei rapporti: {statistics.median(drift):.3f}; minimo {min(drift):.3f}; massimo {max(drift):.3f}.', '']
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
    colors = {'vyukov': '#1d4ed8', 'rigtorp': '#b45309', 'spsc': '#15803d'}
    vcolors = {'SPSC': '#15803d', 'Vyukov': '#1d4ed8', 'Rigtorp': '#b45309', 'differenza < 5%': '#64748b',
               'non distinguibile': '#94a3b8'}
    ts = sorted({e['T_us'] for e in results})
    # 1) throughput delle tre code
    fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False)
    for ax, t in zip(axes[0], ts):
        sel = sorted((e for e in results if e['T_us'] == t), key=lambda e: e['N'])
        ns = [e['N'] for e in sel]
        for q in QUEUES:
            ax.plot(ns, [e[f'{q}_median'] for e in sel], '-o', color=colors[q], markersize=4, label=NAMES[q])
            ax.fill_between(ns, [min(e[f'{q}_samples']) for e in sel], [max(e[f'{q}_samples']) for e in sel],
                            color=colors[q], alpha=0.10)
        if serial.get(t):
            ax.axhline(serial[t], color='#64748b', linestyle=':', label='seriale senza code')
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.set_title(f'T = {t:g} µs')
        ax.set_xlabel('N stadi')
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel('throughput (input/s)')
    axes[0][-1].legend(fontsize=7)
    fig.suptitle('PipelineGrain: throughput delle tre code (mediana e fascia min–max)')
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(output / f'throughput_tre_code.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)
    # 2) rapporti SPSC/Vyukov e SPSC/Rigtorp con IC
    fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False, sharey=True)
    for ax, t in zip(axes[0], ts):
        sel = sorted((e for e in results if e['T_us'] == t), key=lambda e: e['N'])
        ns = [e['N'] for e in sel]
        ax.axhspan(1 - THRESHOLD, 1 + THRESHOLD, color='#e2e8f0')
        ax.axhline(1, color='#64748b', linestyle=':')
        for (a, b), dash in ((('spsc', 'vyukov'), '-'), (('spsc', 'rigtorp'), '--')):
            ax.plot(ns, [e[f'{a}_{b}_ratio'] for e in sel], dash, color=colors[b], alpha=0.6,
                    label=f'{NAMES[a]} / {NAMES[b]}')
            for e in sel:
                v = label(e[f'{a}_{b}_verdict'], a, b)
                ax.errorbar(e['N'], e[f'{a}_{b}_ratio'],
                            yerr=[[e[f'{a}_{b}_ratio'] - e[f'{a}_{b}_low']], [e[f'{a}_{b}_high'] - e[f'{a}_{b}_ratio']]],
                            fmt='o', color=vcolors.get(v, '#94a3b8'), capsize=3, markersize=4)
        ax.set_xscale('log', base=2)
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.set_title(f'T = {t:g} µs')
        ax.set_xlabel('N stadi')
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel('rapporto dei throughput (IC 95%)')
    axes[0][-1].legend(fontsize=7)
    fig.suptitle('PipelineGrain: SPSC rispetto a Vyukov (continua) e a Rigtorp (tratteggio); punto verde = vince SPSC')
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(output / f'rapporti_spsc.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)
    # 3) speedup rispetto al seriale
    tpos = [t for t in ts if t > 0]
    fig, axes = plt.subplots(1, len(tpos), figsize=(4.2 * len(tpos), 4), squeeze=False)
    for ax, t in zip(axes[0], tpos):
        sel = sorted((e for e in results if e['T_us'] == t), key=lambda e: e['N'])
        ns = [e['N'] for e in sel]
        for q in QUEUES:
            ax.plot(ns, [e[f'{q}_speedup'] for e in sel], '-o', color=colors[q], markersize=4, label=NAMES[q])
        ax.plot(ns, ns, ':', color='#64748b', label='lineare')
        ax.axhline(1, color='#94a3b8', linestyle='--', linewidth=0.8)
        ax.set_xscale('log', base=2)
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.set_title(f'T = {t:g} µs')
        ax.set_xlabel('N stadi')
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel('speedup rispetto al seriale senza code')
    axes[0][-1].legend(fontsize=7)
    fig.suptitle('PipelineGrain: speedup delle tre code rispetto all\'esecuzione seriale')
    fig.tight_layout()
    for ext in ('png', 'pdf'):
        fig.savefig(output / f'speedup_tre_code.{ext}', dpi=160, bbox_inches='tight')
    plt.close(fig)
    print('Grafici in', output)


if __name__ == '__main__':
    main()
