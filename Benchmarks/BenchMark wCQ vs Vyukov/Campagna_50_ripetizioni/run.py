#!/usr/bin/env python3
"""Tre benchmark wCQ/Vyukov, in sequenza, con 50 ripetizioni per configurazione."""
import argparse
import csv
from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
SPEC = importlib.util.spec_from_file_location('wcq_bench_helpers', BASE / 'PipelineGrain/run.py')
HELPERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPERS)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--smoke', action='store_true', help='Una ripetizione, casi piccoli: solo validazione')
    parser.add_argument('--nodes', default='auto', help='auto: griglia 1,2,4,8,16 limitata ai core fisici per N+2 task; oppure lista esplicita')
    parser.add_argument('--mojo', default=os.environ.get('MOJO', 'mojo'))
    args = parser.parse_args()
    compiler = HELPERS.configure_mojo(args.mojo)
    physical = HELPERS.physical_cpus()
    canonical = [1, 2, 4] if args.smoke else [1, 2, 4, 8, 16]
    nodes = [n for n in canonical if n + 2 <= len(physical)] if args.nodes == 'auto' else HELPERS.parse_numbers(args.nodes, int)
    if not nodes or nodes[0] != 1 or nodes[-1] + 2 > len(physical):
        parser.error('Servono N=1 e almeno N+2 core fisici distinti per ImageTiles')
    output = (args.output or HERE / 'risultati' / datetime.now(ZoneInfo('Europe/Rome')).strftime('%Y%m%d-%H%M%S')).resolve()
    output.mkdir(parents=True, exist_ok=False)
    repeats = 1 if args.smoke else 50
    messages = 128 if args.smoke else 50000
    targets = '0,2' if args.smoke else '0,1,2,5'
    node_arg = ','.join(map(str, nodes))
    environment = os.environ | {'MOJO': compiler, 'PYTHONDONTWRITEBYTECODE': '1',
                                'MPLCONFIGDIR': str(output / 'matplotlib-cache'),
                                'MODULAR_CACHE_DIR': str(output / 'mojo-cache')}
    configuration = {
        'comparison': 'native Mojo wCQ vs Vyukov', 'smoke': args.smoke,
        'repeats': repeats, 'messages': messages, 'capacity': 1024,
        'canonical_nodes': canonical, 'measured_nodes': nodes,
        'not_measured_nodes': [n for n in canonical if n not in nodes],
        'physical_cpus': physical, 'mojo': compiler,
        'implementation_notes': HELPERS.implementation_notes(),
        'expected_raw_rows': {'PipelineGrain': 2 * len(nodes) * len(targets.split(',')) * repeats,
                              'FarmThroughput': 2 * len(nodes) * len(targets.split(',')) * repeats,
                              'ImageTiles': (1 + 2 * 3 * len(nodes)) * repeats},
    }
    (output / 'campagna.json').write_text(json.dumps(configuration, indent=2))
    print('N misurati:', nodes, '; core fisici:', physical, '; esclusi:', configuration['not_measured_nodes'], flush=True)
    try:
        for name, nflag, tflag in (('PipelineGrain', '--stages', '--total-us'), ('FarmThroughput', '--consumers', '--work-us')):
            destination = output / name
            command = [sys.executable, '-u', str(BASE / name / 'run.py'), nflag, node_arg,
                       tflag, targets, '--messages', str(messages), '--capacity', '1024',
                       '--repeats', str(repeats), '--queues', 'wcq,vyukov', '--output', str(destination), '--mojo', compiler]
            print('Avvio', name, flush=True)
            subprocess.run(command, env=environment, check=True)
            subprocess.run([sys.executable, str(BASE / name / 'plot.py'), str(destination)], env=environment, check=True)
            if not args.smoke:
                subprocess.run([sys.executable, str(HERE / 'analizza.py'), str(destination), '--benchmark', name], env=environment, check=True)
        destination = output / 'ImageTiles'
        subprocess.run([sys.executable, '-u', str(BASE / 'ImageTiles/run.py'), '--sizes', '19x13' if args.smoke else '512x512',
                        '--images', '3' if args.smoke else '32', '--block-rows', '1,4,13' if args.smoke else '1,16,128',
                        '--consumers', node_arg, '--capacity', '1024', '--repeats', str(repeats),
                        '--output', str(destination), '--mojo', compiler], env=environment, check=True)
        subprocess.run([sys.executable, str(BASE / 'ImageTiles/plot.py'), str(destination)], env=environment, check=True)
        for name, expected in configuration['expected_raw_rows'].items():
            with (output / name / 'raw.csv').open(newline='') as stream:
                actual = sum(1 for _ in csv.DictReader(stream))
            if actual != expected:
                raise RuntimeError(f'{name}: {actual} misure, attese {expected}')
        verification = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in output.rglob('*') if p.suffix in ('.csv', '.png', '.pdf', '.md')}
        (output / 'VERIFICA.json').write_text(json.dumps({'raw_rows': configuration['expected_raw_rows'], 'artifacts_sha256': verification}, indent=2))
        (output / 'COMPLETATO.txt').write_text(('Smoke test' if args.smoke else 'Campagna a 50 ripetizioni') + ': tre benchmark, correttezza e grafici completati.\n')
        print('COMPLETATO:', output, flush=True)
    except BaseException as error:
        (output / 'INTERROTTO.txt').write_text(f'{type(error).__name__}: {error}\nDati parziali conservati.\n')
        raise


if __name__ == '__main__':
    main()
