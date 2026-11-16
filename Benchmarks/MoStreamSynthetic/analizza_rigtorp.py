#!/usr/bin/env python3
"""Valutazione della coda Rigtorp rispetto a Vyukov dentro MoStream.

Uso:  python3 analizza_rigtorp.py CAMPAGNA_DIR [--output DIR]

CAMPAGNA_DIR contiene FarmThroughput/raw.csv e PipelineGrain/raw.csv prodotti
da MoStreamSynthetic/*/run.py. Per ogni configurazione (N, T):

- rapporto R/V dei throughput mediani (>1 favorisce Rigtorp);
- intervallo percentile bootstrap al 95% del rapporto, 20.000 ricampionamenti
  APPAIATI per ripetizione: il runner esegue i due backend uno dopo l'altro,
  in ordine casuale, nella stessa ripetizione;
- coppie vinte da Rigtorp e test dei segni esatto bilaterale;
- coefficiente di variazione di ciascun backend.

Esito: "Rigtorp" o "Vyukov" se l'intervallo esclude 1 e il rapporto supera la
soglia pratica del 5%; "differenza < 5%" se l'intervallo esclude 1 ma la
differenza è piccola; "non distinguibile" se l'intervallo contiene 1.
Gli intervalli sono individuali, senza correzione per confronti multipli.
"""
import argparse
import csv
from collections import defaultdict
from math import comb
from pathlib import Path
import random
import statistics

RESAMPLES = 20000
THRESHOLD = 0.05
SEED = 20261105
BENCHES = {'FarmThroughput': ('consumers', 'target_work_us', 'N consumatori'),
           'PipelineGrain': ('stages', 'total_work_us', 'N stadi')}


def sign_test(wins, n):
    """p-value esatto bilaterale del test dei segni (H0: P(vince Rigtorp)=0.5)."""
    k = min(wins, n - wins)
    p = 2 * sum(comb(n, i) for i in range(k + 1)) / 2 ** n
    return min(1.0, p)


def verdict(ratio, low, high):
    if low <= 1 <= high:
        return 'non distinguibile'
    if ratio >= 1 + THRESHOLD:
        return 'Rigtorp'
    if ratio <= 1 - THRESHOLD:
        return 'Vyukov'
    return 'differenza < 5%'


def analyze(raw, n_key, t_key, randomizer):
    pairs = defaultdict(dict)
    for r in raw:
        pairs[(int(r[n_key]), float(r[t_key]))].setdefault(int(r['repeat']), {})[r['backend']] = float(r['throughput'])
    results = []
    for (n, t), by_repeat in sorted(pairs.items()):
        complete = [p for p in by_repeat.values() if 'rigtorp' in p and 'vyukov' in p]
        rig = [p['rigtorp'] for p in complete]
        vyu = [p['vyukov'] for p in complete]
        ratio = statistics.median(rig) / statistics.median(vyu)
        size = len(complete)
        boot = []
        for _ in range(RESAMPLES):
            idx = [randomizer.randrange(size) for _ in range(size)]
            boot.append(statistics.median(rig[i] for i in idx) / statistics.median(vyu[i] for i in idx))
        boot.sort()
        low, high = boot[int(0.025 * RESAMPLES)], boot[int(0.975 * RESAMPLES) - 1]
        wins = sum(a > b for a, b in zip(rig, vyu))
        results.append(dict(N=n, T_us=t, repeats=size,
                            vyukov_median=statistics.median(vyu), rigtorp_median=statistics.median(rig),
                            ratio=ratio, ci95_low=low, ci95_high=high,
                            rigtorp_wins=wins, sign_test_p=sign_test(wins, size),
                            vyukov_cv=statistics.stdev(vyu) / statistics.mean(vyu),
                            rigtorp_cv=statistics.stdev(rig) / statistics.mean(rig),
                            verdict=verdict(ratio, low, high),
                            samples={'vyukov': vyu, 'rigtorp': rig}))
    return results


def markdown(all_results):
    out = ['# Rigtorp vs Vyukov dentro MoStream', '',
           f'Throughput mediano in input/s su 50 ripetizioni. **R/V** > 1 favorisce Rigtorp. '
           f'IC 95%: bootstrap percentile appaiato per ripetizione, {RESAMPLES:,} ricampionamenti. '
           f'Soglia pratica {THRESHOLD:.0%}. Intervalli individuali, senza correzione per confronti multipli.', '']
    for bench, results in all_results.items():
        counts = defaultdict(int)
        for r in results:
            counts[r['verdict']] += 1
        out += [f'## {bench}', '',
                'Esiti: ' + ', '.join(f'{k}: {v}' for k, v in sorted(counts.items())), '',
                '| N | T µs | Vyukov | Rigtorp | R/V | IC 95% | vinte R | p segni | CV V | CV R | Esito |',
                '| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |']
        for r in results:
            out.append(f"| {r['N']} | {r['T_us']:g} | {r['vyukov_median']:,.0f} | {r['rigtorp_median']:,.0f} | "
                       f"{r['ratio']:.3f} | {r['ci95_low']:.3f}–{r['ci95_high']:.3f} | {r['rigtorp_wins']}/{r['repeats']} | "
                       f"{r['sign_test_p']:.2g} | {r['vyukov_cv']:.1%} | {r['rigtorp_cv']:.1%} | {r['verdict']} |")
        out.append('')
    return '\n'.join(out)


def plots(all_results, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    colors = {'Rigtorp': '#b45309', 'Vyukov': '#1d4ed8', 'differenza < 5%': '#64748b', 'non distinguibile': '#94a3b8'}
    for bench, results in all_results.items():
        label = BENCHES[bench][2]
        ts = sorted({r['T_us'] for r in results})
        # 1) rapporto con IC per ogni T
        fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False, sharey=True)
        for ax, t in zip(axes[0], ts):
            sel = sorted((r for r in results if r['T_us'] == t), key=lambda r: r['N'])
            ns = [r['N'] for r in sel]
            ax.axhspan(1 - THRESHOLD, 1 + THRESHOLD, color='#e2e8f0', label='±5%')
            ax.axhline(1, color='#64748b', linestyle=':')
            for r in sel:
                ax.errorbar(r['N'], r['ratio'], yerr=[[r['ratio'] - r['ci95_low']], [r['ci95_high'] - r['ratio']]],
                            fmt='o', color=colors[r['verdict']], capsize=3)
            ax.plot(ns, [r['ratio'] for r in sel], '-', color='#7c3aed', alpha=0.5)
            ax.set_xscale('log', base=2)
            ax.set_xticks(ns, [str(n) for n in ns])
            ax.set_title(f'T = {t:g} µs')
            ax.set_xlabel(label)
            ax.grid(alpha=0.3)
        axes[0][0].set_ylabel('Rigtorp / Vyukov (IC 95%)')
        handles = [plt.Line2D([], [], marker='o', linestyle='', color=c, label=k) for k, c in colors.items()]
        axes[0][-1].legend(handles=handles, fontsize=7, loc='best')
        fig.suptitle(f'{bench} in MoStream: rapporto dei throughput mediani Rigtorp/Vyukov con IC bootstrap 95%')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'{bench}_rapporto_IC.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)
        # 2) distribuzione dei 50 campioni per backend (box plot affiancati)
        fig, axes = plt.subplots(1, len(ts), figsize=(4.2 * len(ts), 4), squeeze=False)
        for ax, t in zip(axes[0], ts):
            sel = sorted((r for r in results if r['T_us'] == t), key=lambda r: r['N'])
            positions, data, cols = [], [], []
            for i, r in enumerate(sel):
                for j, backend in enumerate(('vyukov', 'rigtorp')):
                    positions.append(3 * i + j)
                    data.append(r['samples'][backend])
                    cols.append('#1d4ed8' if backend == 'vyukov' else '#b45309')
            box = ax.boxplot(data, positions=positions, widths=0.8, patch_artist=True, showfliers=True,
                             flierprops=dict(markersize=2))
            for patch, c in zip(box['boxes'], cols):
                patch.set_facecolor(c)
                patch.set_alpha(0.55)
            ax.set_xticks([3 * i + 0.5 for i in range(len(sel))], [str(r['N']) for r in sel])
            ax.set_title(f'T = {t:g} µs')
            ax.set_xlabel(label)
            ax.grid(alpha=0.3, axis='y')
        axes[0][0].set_ylabel('throughput (input/s)')
        axes[0][-1].legend(handles=[plt.Rectangle((0, 0), 1, 1, color='#1d4ed8', alpha=0.55, label='Vyukov'),
                                    plt.Rectangle((0, 0), 1, 1, color='#b45309', alpha=0.55, label='Rigtorp')],
                           fontsize=8)
        fig.suptitle(f'{bench} in MoStream: distribuzione delle 50 ripetizioni per backend')
        fig.tight_layout()
        for ext in ('png', 'pdf'):
            fig.savefig(output / f'{bench}_distribuzioni.{ext}', dpi=160, bbox_inches='tight')
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('campagna', type=Path)
    parser.add_argument('--output', type=Path, default=None)
    args = parser.parse_args()
    output = args.output or args.campagna / 'valutazione_rigtorp'
    output.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(SEED)
    all_results = {}
    for bench, (n_key, t_key, _) in BENCHES.items():
        with open(args.campagna / bench / 'raw.csv', newline='') as stream:
            all_results[bench] = analyze(list(csv.DictReader(stream)), n_key, t_key, randomizer)
    with (output / 'valutazione.csv').open('w', newline='') as stream:
        fields = ['benchmark', 'N', 'T_us', 'repeats', 'vyukov_median', 'rigtorp_median', 'ratio', 'ci95_low',
                  'ci95_high', 'rigtorp_wins', 'sign_test_p', 'vyukov_cv', 'rigtorp_cv', 'verdict']
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for bench, results in all_results.items():
            for r in results:
                writer.writerow(dict(r, benchmark=bench))
    text = markdown(all_results)
    (output / 'VALUTAZIONE.md').write_text(text)
    print(text)
    try:
        plots(all_results, output)
        print('Grafici in', output)
    except ImportError:
        print('matplotlib non disponibile: grafici non generati')


if __name__ == '__main__':
    main()
