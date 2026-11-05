#!/usr/bin/env python3
"""Effetto delle letture fuori ordine sugli spike (soluzione 1: lock per sensore).

Esegue Spike Detection multi-sorgente per un numero esatto di passate del dataset, salva gli spike
e li confronta con quelli del riferimento sequenziale (stesse passate) con compare_dump.py.
Con MovingAverage a una replica il risultato deve essere identico (controllo); con più repliche
le letture di uno stesso sensore possono essere elaborate fuori ordine e cambiare alcuni spike.

Per ogni esecuzione scrive: letture fuori ordine (late), spike del riferimento e di MoStream,
spike mancanti e in più rispetto al riferimento, indice di Jaccard.

Uso: python3 check_order.py <root> <dataset.bin> <riferimento_dump> <passate> <output.csv> [ripetizioni]
  <root> contiene bin/sdm_vyukov e bin/sdm_rigtorp (spike_detection_multisource.mojo)
  <riferimento_dump> si ottiene con: python3 reference.py <dataset.bin> <passate> <riferimento_dump>
"""
import csv
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIGS = [(1, 1, 4), (1, 4, 4), (1, 8, 8), (1, 16, 16), (8, 1, 4), (8, 4, 4), (8, 8, 8), (8, 16, 16)]  # (S, MA, SD)


def parse(line):
    return dict(kv.split('=', 1) for kv in line.split()[1:] if '=' in kv)


def main():
    if len(sys.argv) not in (6, 7):
        sys.exit(__doc__)
    root, data, reference, passes, out = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], Path(sys.argv[5])
    repeats = int(sys.argv[6]) if len(sys.argv) > 6 else 3
    if out.exists():
        sys.exit(f'{out} already exists')
    with out.open('w', newline='') as stream, tempfile.TemporaryDirectory() as temp:
        writer = csv.writer(stream)
        writer.writerow(['queue', 'sources', 'ma', 'sd', 'rep', 'tuples', 'late', 'lock_waits',
                         'reference', 'mostream', 'missing', 'extra', 'jaccard', 'status'])
        dump = str(Path(temp) / 'dump.txt')
        for rep in range(repeats):
            for s, ma, sd in CONFIGS:
                # alterna l'ordine delle code fra le ripetizioni
                for queue in (('vyukov', 'rigtorp') if rep % 2 == 0 else ('rigtorp', 'vyukov')):
                    command = [str(root / 'bin' / f'sdm_{queue}'), data, '0', passes, str(s), str(ma), str(sd),
                               'std', str(s + ma + sd + 1), dump]
                    output = subprocess.run(command, cwd=root, env={'MOSTREAM_HOME': str(root), 'PATH': '/usr/bin:/bin'},
                                            capture_output=True, text=True, timeout=600).stdout
                    result = parse(next(line for line in output.splitlines() if line.startswith('RESULT')))
                    if result['queue'] != queue or not (result['tuples'] == result['averaged'] == result['detected']):
                        sys.exit(f'invalid run: {result}')
                    compare = subprocess.run([sys.executable, str(HERE / 'compare_dump.py'), reference, dump],
                                             capture_output=True, text=True, check=True).stdout.split()
                    c = dict(kv.split('=') for kv in compare[1:])
                    writer.writerow([queue, s, ma, sd, rep, result['tuples'], result['late'], result['lock_waits'],
                                     c['reference'], c['mostream'], c['missing'], c['extra'], c['jaccard'], compare[0]])
                    stream.flush()
                    print(rep, queue, f'S={s}', f'{ma}+{sd}', 'late=' + result['late'], compare[0],
                          'missing=' + c['missing'], 'extra=' + c['extra'], flush=True)


if __name__ == '__main__':
    main()
