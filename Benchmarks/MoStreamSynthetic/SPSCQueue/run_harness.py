#!/usr/bin/env python3
"""Microbenchmark 1:1 sulla coda nuda: Vyukov, Rigtorp e SPSC FastFlow.

Compila spsc_harness.mojo, poi esegue blocchi di misure alternando le code in
ordine casuale (seed fisso). Ogni processo fa un warm-up e `--per-process`
misure; i blocchi si ripetono finché ogni coda ha `--samples` misure per
capacità. Riporta il costo per elemento (ns) e il rapporto con Vyukov, con IC
bootstrap al 95% (ricampionamento indipendente delle due serie).
"""
import argparse
import csv
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
KINDS = ('vyukov', 'rigtorp', 'spsc')


def bootstrap_ratio(a, b, rng, n=20000):
    """IC 95% di mediana(a) / mediana(b) con ricampionamento indipendente."""
    values = []
    for _ in range(n):
        ra = [a[rng.randrange(len(a))] for _ in a]
        rb = [b[rng.randrange(len(b))] for _ in b]
        values.append(statistics.median(ra) / statistics.median(rb))
    values.sort()
    return values[int(0.025 * n)], values[int(0.975 * n) - 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mojo', default=os.environ.get('MOJO', 'mojo'))
    parser.add_argument('--messages', type=int, default=10_000_000)
    parser.add_argument('--capacities', default='64,1024')
    parser.add_argument('--samples', type=int, default=50)
    parser.add_argument('--per-process', type=int, default=5)
    parser.add_argument('--cpus', default='1,2', help='core del produttore e del consumatore')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    cpu_p, cpu_c = [int(x) for x in args.cpus.split(',')]
    capacities = [int(x) for x in args.capacities.split(',')]
    env = dict(os.environ, MOSTREAM_HOME=str(ROOT))
    rng = random.Random(args.seed)
    rows = []
    with tempfile.TemporaryDirectory(prefix='spsc-harness-') as d:
        binary = Path(d) / 'spsc_harness'
        subprocess.run([args.mojo, 'build', '-I', str(ROOT), str(HERE / 'spsc_harness.mojo'), '-o', str(binary)],
                       cwd=ROOT, check=True)
        blocks = (args.samples + args.per_process - 1) // args.per_process
        for block in range(blocks):
            jobs = [(k, c) for k in KINDS for c in capacities]
            rng.shuffle(jobs)
            for kind, cap in jobs:
                out = subprocess.check_output([str(binary), kind, str(args.messages), str(cap), str(cpu_p),
                                               str(cpu_c), str(args.per_process)], cwd=ROOT, env=env,
                                              text=True, timeout=600)
                for line in out.splitlines():
                    if line.startswith('RESULT '):
                        _, k, ns = line.split()
                        rows.append(dict(kind=k, capacity=cap, block=block, ns_per_item=float(ns)))
            print(f'blocco {block + 1}/{blocks}', flush=True)
    with (args.output / 'raw.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    summary = []
    for cap in capacities:
        base = [r['ns_per_item'] for r in rows if r['kind'] == 'vyukov' and r['capacity'] == cap]
        for kind in KINDS:
            vals = [r['ns_per_item'] for r in rows if r['kind'] == kind and r['capacity'] == cap]
            low, high = bootstrap_ratio(vals, base, rng) if kind != 'vyukov' else (1.0, 1.0)
            med = statistics.median(vals)
            summary.append(dict(capacity=cap, kind=kind, samples=len(vals), median_ns=med, min_ns=min(vals),
                                max_ns=max(vals), cv=statistics.stdev(vals) / statistics.mean(vals),
                                throughput_mops=1e3 / med,
                                ns_ratio_vs_vyukov=med / statistics.median(base),
                                ci95_low=low, ci95_high=high))
    with (args.output / 'summary.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    meta = {'utc': datetime.now(timezone.utc).isoformat(), 'host': platform.node(),
            'mojo': subprocess.check_output([args.mojo, '--version'], text=True).strip(),
            'parameters': {k: str(v) for k, v in vars(args).items()},
            'boost': Path('/sys/devices/system/cpu/cpufreq/boost').read_text().strip()
            if Path('/sys/devices/system/cpu/cpufreq/boost').exists() else None}
    (args.output / 'metadata.json').write_text(json.dumps(meta, indent=2))
    print(f"\n{'cap':>5} {'coda':>8} {'mediana ns':>11} {'min':>8} {'max':>8} {'CV':>6} {'Mop/s':>7} {'ns/Vyukov':>10} IC95%")
    for s in summary:
        print(f"{s['capacity']:>5} {s['kind']:>8} {s['median_ns']:>11.2f} {s['min_ns']:>8.2f} {s['max_ns']:>8.2f} "
              f"{s['cv']:>6.1%} {s['throughput_mops']:>7.1f} {s['ns_ratio_vs_vyukov']:>10.3f} "
              f"{s['ci95_low']:.3f}–{s['ci95_high']:.3f}")


if __name__ == '__main__':
    main()
