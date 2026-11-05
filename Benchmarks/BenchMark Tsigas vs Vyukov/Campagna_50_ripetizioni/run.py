#!/usr/bin/env python3
"""Campagna Tsigas–Vyukov: tre benchmark a 50 ripetizioni, oppure smoke test."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
BASE = HERE.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='Cartella nuova; non sovrascrive risultati')
    parser.add_argument('--smoke', action='store_true', help='Casi piccoli, una ripetizione; nessuna inferenza statistica')
    args = parser.parse_args()
    repeats = 1 if args.smoke else 50
    messages = 128 if args.smoke else 50000
    ns = '1,2,4' if args.smoke else '1,2,4,8,16'
    targets = '0,2' if args.smoke else '0,1,2,5'
    output = (args.output or HERE / 'risultati' / datetime.now().strftime('%Y%m%d-%H%M%S')).resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = os.environ | {'PYTHONDONTWRITEBYTECODE': '1', 'MPLCONFIGDIR': str(output / 'matplotlib-cache')}
    config = {'comparison': 'Tsigas-Zhang vs Vyukov', 'repeats': repeats, 'messages': messages,
              'capacity': 'auto; strict total-success bound < physical ring length',
              'capacity_comparison': 'same physical cells; Tsigas L-2 useful, Vyukov L useful',
              'synthetic_kernel': '63-bit xorshift, identical for both backends',
              'queues': ['tsigas', 'vyukov'], 'benchmarks': ['PipelineGrain', 'FarmThroughput', 'ImageTiles'],
              'smoke': args.smoke, 'historical_results_modified': False,
              'expected_raw_rows': {'PipelineGrain': 12 if args.smoke else 2000,
                                    'FarmThroughput': 12 if args.smoke else 2000,
                                    'ImageTiles': 13 if args.smoke else 1550}}
    (output / 'campagna.json').write_text(json.dumps(config, indent=2))
    try:
        for name, n_flag, t_flag in (('PipelineGrain', '--stages', '--total-us'),
                                     ('FarmThroughput', '--consumers', '--work-us')):
            dest = output / name
            command = [sys.executable, '-u', str(BASE / name / 'run.py'), n_flag, ns,
                       t_flag, targets, '--messages', str(messages), '--capacity', '0',
                       '--repeats', str(repeats), '--queues', 'tsigas,vyukov', '--output', str(dest)]
            print(f'Avvio {name}: {dest}', flush=True)
            subprocess.run(command, check=True, env=environment)
            subprocess.run([sys.executable, str(BASE / name / 'plot.py'), str(dest)], check=True, env=environment)
            if not args.smoke:
                subprocess.run([sys.executable, str(HERE / 'analizza.py'), str(dest), '--benchmark', name], check=True, env=environment)
        dest = output / 'ImageTiles'
        command = [sys.executable, '-u', str(BASE / 'ImageTiles/run.py'),
                   '--sizes', '19x13' if args.smoke else '512x512',
                   '--images', '3' if args.smoke else '32',
                   '--block-rows', '1,4,13' if args.smoke else '1,16,128',
                   '--consumers', '1,4' if args.smoke else '1,2,4,8,16',
                   '--capacity', '0', '--repeats', str(repeats), '--output', str(dest)]
        subprocess.run(command, check=True, env=environment)
        subprocess.run([sys.executable, str(BASE / 'ImageTiles/plot.py'), str(dest)], check=True, env=environment)
        # Require the expected number of real observations before declaring completion.
        import csv
        for name, expected in config['expected_raw_rows'].items():
            with (output / name / 'raw.csv').open(newline='') as stream:
                count = sum(1 for _ in csv.DictReader(stream))
            if count != expected:
                raise RuntimeError(f'{name}: {count} righe invece di {expected}')
        label = 'Smoke test' if args.smoke else 'Campagna con 50 ripetizioni'
        (output / 'COMPLETATO.txt').write_text(label + ': tre benchmark, controlli e grafici completati.\n')
        print(f'COMPLETATO: {output}', flush=True)
    except BaseException as error:
        (output / 'INTERROTTO.txt').write_text(f'{type(error).__name__}: {error}\nDati parziali conservati.\n')
        raise


if __name__ == '__main__':
    main()
