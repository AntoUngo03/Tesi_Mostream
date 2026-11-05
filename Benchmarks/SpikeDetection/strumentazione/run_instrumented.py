#!/usr/bin/env python3
"""SOLO PER MISURE: esegue Spike Detection multi-sorgente con le code strumentate e raccoglie i contatori.

Per ogni esecuzione legge le righe QSTATS (una per collegamento della pipeline) e la riga RESULT.
Ogni collegamento è riconosciuto dalla dimensione del messaggio e dal numero di consumatori:
  sorgenti -> MovingAverage       SensorReading, 24 byte
  MovingAverage -> SpikeDetector  AverageReading, 32 byte, consumatori = repliche di SpikeDetector
  SpikeDetector -> Sink           SpikeEvent, 32 byte, 1 consumatore
Scrive una riga per collegamento con i contatori e i messaggi passati (tuple o spike).

Uso: python3 run_instrumented.py <root copia strumentata> <dataset.bin> <output.csv> [durata_s] [ripetizioni]
"""
import csv
import subprocess
import sys
from pathlib import Path

CONFIGS = [(1, 1, 4), (8, 1, 4), (1, 4, 4), (8, 4, 4), (1, 8, 8), (8, 8, 8), (1, 16, 16)]  # (S, MA, SD)
COUNTERS = ['push_cas_fail', 'push_stale', 'push_full', 'pop_cas_fail', 'pop_stale', 'pop_empty']


def parse(line):
    return dict(kv.split('=', 1) for kv in line.split()[1:] if '=' in kv)


def main():
    if len(sys.argv) not in (4, 5, 6):
        sys.exit(__doc__)
    root, data, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    duration = sys.argv[4] if len(sys.argv) > 4 else '10'
    repeats = int(sys.argv[5]) if len(sys.argv) > 5 else 3
    if out.exists():
        sys.exit(f'{out} already exists')
    with out.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['queue', 'sources', 'ma', 'sd', 'rep', 'edge', 'producers', 'consumers', 'messages',
                         *COUNTERS, 'throughput'])
        for rep in range(repeats):
            for s, ma, sd in CONFIGS:
                # alterna l'ordine delle code fra le ripetizioni
                for queue in (('vyukov', 'rigtorp') if rep % 2 == 0 else ('rigtorp', 'vyukov')):
                    command = [str(root / 'bin' / f'sdmi_{queue}'), data, duration, '0', str(s), str(ma), str(sd),
                               'std', str(s + ma + sd + 1)]
                    output = subprocess.run(command, cwd=root, env={'MOSTREAM_HOME': str(root), 'PATH': '/usr/bin:/bin'},
                                            capture_output=True, text=True, timeout=int(duration) * 6 + 30).stdout
                    result = parse(next(line for line in output.splitlines() if line.startswith('RESULT')))
                    if result['queue'] != queue or not (result['tuples'] == result['averaged'] == result['detected']):
                        sys.exit(f'invalid run: {result}')
                    for line in output.splitlines():
                        if not line.startswith('QSTATS'):
                            continue
                        q = parse(line)
                        if q['msg_bytes'] == '24':
                            edge, messages = 'source_ma', result['tuples']
                        elif q['consumers'] == '1' and q['producers'] == str(sd):
                            edge, messages = 'sd_sink', result['spikes']
                        else:
                            edge, messages = 'ma_sd', result['tuples']
                        writer.writerow([queue, s, ma, sd, rep, edge, q['producers'], q['consumers'], messages,
                                         *(q[c] for c in COUNTERS), result['throughput']])
                    stream.flush()
                    print(rep, queue, s, ma, sd, round(float(result['throughput'])), flush=True)


if __name__ == '__main__':
    main()
