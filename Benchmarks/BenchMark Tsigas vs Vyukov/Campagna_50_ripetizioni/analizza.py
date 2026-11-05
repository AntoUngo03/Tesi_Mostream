#!/usr/bin/env python3
"""Analisi esplorativa del rapporto fra throughput mediani, con bootstrap al 95%."""
import argparse
import csv
import json
from pathlib import Path
import random
import statistics


def quantile(values, probability):
    position = probability * (len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def interval(tsigas, vyukov, paired, samples, seed):
    # Rapporto throughput T/V = mediana tempo V / mediana tempo R, con M uguale.
    rng = random.Random(seed)
    ratios = []
    indices = list(range(len(tsigas)))
    for _ in range(samples):
        ri = rng.choices(indices, k=len(indices))
        vi = ri if paired else rng.choices(indices, k=len(indices))
        ratios.append(statistics.median(vyukov[i] for i in vi) /
                      statistics.median(tsigas[i] for i in ri))
    ratios.sort()
    return quantile(ratios, .025), quantile(ratios, .975)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    parser.add_argument('--benchmark', choices=['PipelineGrain', 'FarmThroughput'], required=True)
    parser.add_argument('--bootstrap-samples', type=int, default=20000)
    args = parser.parse_args()
    if args.bootstrap_samples < 1000:
        parser.error('Usare almeno 1.000 ricampionamenti')
    with (args.results / 'raw.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    size_key, time_key = (('stages', 'target_total_us') if args.benchmark == 'PipelineGrain'
                          else ('consumers', 'target_work_us'))
    groups = {}
    for row in rows:
        groups.setdefault((int(row[size_key]), float(row[time_key])), {}).setdefault(row['queue'], []).append(row)
    output = []
    for number, ((size, target), queues) in enumerate(sorted(groups.items())):
        r = sorted(queues['tsigas'], key=lambda row: int(row['repeat']))
        v = sorted(queues['vyukov'], key=lambda row: int(row['repeat']))
        if len(r) != 50 or len(v) != 50:
            raise ValueError(f'N={size}, T={target}: servono 50 misure complete per coda')
        assert {int(row['repeat']) for row in r} == set(range(50))
        assert {int(row['repeat']) for row in v} == set(range(50))
        assert {int(row['messages']) for row in r + v} == {50000}
        rt = [float(row['elapsed_ns']) for row in r]
        vt = [float(row['elapsed_ns']) for row in v]
        ratio = statistics.median(vt) / statistics.median(rt)
        paired = args.benchmark == 'FarmThroughput'
        baseline = args.benchmark == 'PipelineGrain' and size == 1
        low, high = (None, None) if baseline else interval(rt, vt, paired, args.bootstrap_samples, 42 + number)
        if baseline:
            verdict = 'baseline senza code: nessun confronto delle code'
        elif low > 1.05:
            verdict = 'vantaggio TsigasZhang oltre il 5% supportato dall’intervallo individuale'
        elif high < 1 / 1.05:
            verdict = 'vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale'
        elif low > 1:
            verdict = 'indicazione a favore di TsigasZhang; vantaggio minimo del 5% non dimostrato'
        elif high < 1:
            verdict = 'indicazione a favore di Vyukov; vantaggio minimo del 5% non dimostrato'
        else:
            verdict = 'confronto inconclusivo: intervallo comprende la parità'
        output.append({'N': size, 'T_us': target, 'repeats_per_queue': 50,
                       'throughput_tsigas': 50000 * 1e9 / statistics.median(rt),
                       'throughput_vyukov': 50000 * 1e9 / statistics.median(vt),
                       'throughput_ratio_T_over_V': ratio,
                       'advantage_T_percent': (ratio - 1) * 100,
                       'ci95_ratio_low': low, 'ci95_ratio_high': high,
                       'bootstrap_paired': paired, 'interpretation': verdict})
    with (args.results / 'confronto_50.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    notes = [f'# {args.benchmark}: confronto con 50 ripetizioni', '',
             'Rapporto > 1 favorisce TsigasZhang; rapporto < 1 favorisce Vyukov.',
             'Intervalli percentile bootstrap al 95%, per singola configurazione; non corretti per confronti multipli.',
             'Analisi esplorativa: non dimostra stabilità fra sessioni, giorni o macchine.',
             'Il 5% è una soglia pratica scelta per questa analisi, non una garanzia prestazionale.', '',
             '| N | T (µs) | Rapporto throughput T/V | Intervallo 95% | Interpretazione |',
             '|---:|---:|---:|---|---|']
    for row in output:
        bounds = 'non applicabile' if row['ci95_ratio_low'] is None else f"{row['ci95_ratio_low']:.3f}–{row['ci95_ratio_high']:.3f}"
        notes.append(f"| {row['N']} | {row['T_us']:g} | {row['throughput_ratio_T_over_V']:.3f} | {bounds} | {row['interpretation']} |")
    (args.results / 'confronto_50.md').write_text('\n'.join(notes) + '\n')
    (args.results / 'analisi.json').write_text(json.dumps({'bootstrap_samples': args.bootstrap_samples,
        'confidence_per_configuration': .95, 'multiple_comparisons_adjusted': False,
        'practical_advantage_threshold': .05, 'paired': args.benchmark == 'FarmThroughput'}, indent=2))
    print(f'Analisi salvata in {args.results}')


if __name__ == '__main__':
    main()
