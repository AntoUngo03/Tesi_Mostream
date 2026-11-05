#!/usr/bin/env python3
"""Nuova campagna da 50 ripetizioni, senza modificare risultati o script storici."""
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
    parser.add_argument('--output', type=Path, help='Cartella nuova per tutti i risultati')
    args = parser.parse_args()
    output = (args.output or HERE / 'risultati' / datetime.now().strftime('%Y%m%d-%H%M%S')).resolve()
    output.mkdir(parents=True, exist_ok=False)
    # Tutte le nuove uscite restano in output; niente cache nei benchmark originari.
    environment = os.environ | {'PYTHONDONTWRITEBYTECODE': '1', 'MPLCONFIGDIR': str(output / 'matplotlib-cache')}
    configuration = {'repeats': 50, 'messages': 50000, 'capacity': 1024,
                     'N': [1, 2, 4, 8, 16], 'T_us': [0, 1, 2, 5],
                     'queues': ['rigtorp', 'vyukov'], 'measures_per_benchmark': 2000,
                     'historical_results_modified': False}
    (output / 'campagna.json').write_text(json.dumps(configuration, indent=2))
    try:
        for name, n_flag, t_flag in (('PipelineGrain', '--stages', '--total-us'),
                                     ('FarmThroughput', '--consumers', '--work-us')):
            destination = output / name
            command = [sys.executable, '-u', str(BASE / name / 'run.py'),
                       n_flag, '1,2,4,8,16', t_flag, '0,1,2,5',
                       '--messages', '50000', '--capacity', '1024', '--repeats', '50',
                       '--queues', 'rigtorp,vyukov', '--output', str(destination)]
            print(f'Avvio {name}: 2.000 nuove misure, output {destination}', flush=True)
            subprocess.run(command, check=True, env=environment)
            subprocess.run([sys.executable, str(BASE / name / 'plot.py'), str(destination)],
                           check=True, env=environment)
            subprocess.run([sys.executable, str(HERE / 'analizza.py'), str(destination),
                            '--benchmark', name], check=True, env=environment)
        (output / 'COMPLETATO.txt').write_text('Entrambi i benchmark: 50 ripetizioni, grafici e analisi completati.\n')
        print(f'CAMPAGNA COMPLETATA: {output}', flush=True)
    except BaseException as error:
        (output / 'INTERROTTO.txt').write_text(f'{type(error).__name__}: {error}\nI dati già prodotti sono conservati.\n')
        raise


if __name__ == '__main__':
    main()
