#!/usr/bin/env python3
# Implementazione sequenziale di riferimento di Spike Detection, usata per verificare
# l'applicazione MoStream. Legge lo stesso file binario e lo riproduce lo stesso
# numero di volte; con MovingAverage a una replica l'output di MoStream deve
# coincidere esattamente (stesso numero di spike e stesso hash indipendente dall'ordine).
#
# MovingAverage e SpikeDetector seguono MovingAverageBolt e SpikeDetectionBolt
# di dspbench-storm (finestra 1000, soglia 0,03), compreso l'ordine delle
# operazioni in virgola mobile sulla somma.
#
# Uso: python3 reference.py sensors.bin [passes] [dump_file]

import struct
import sys
from collections import deque

WINDOW = 1000
THRESHOLD = 0.03
MASK = (1 << 64) - 1  # gli interi Python non hanno limite: la maschera emula l'aritmetica a 64 bit


def spike_hash(device, seq):
    # finalizzatore splitmix64 di (device << 32 | seq), sommato modulo 2^64 dal sink
    z = ((device << 32) | seq) & MASK
    z = (z + 0x9E3779B97F4A7C15) & MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    return z ^ (z >> 31)


def load(path):
    with open(path, "rb") as f:
        data = f.read()
    n, max_device = struct.unpack_from("<qq", data, 0)
    return [struct.unpack_from("<qd", data, 16 + 16 * i) for i in range(n)], max_device


def main():
    if len(sys.argv) not in (2, 3, 4):
        sys.exit("usage: reference.py <sensors.bin> [passes] [dump_file]")
    records, _ = load(sys.argv[1])
    passes = int(sys.argv[2]) if len(sys.argv) >= 3 else 1
    # stato per sensore: finestra degli ultimi WINDOW valori, somma, prossimo numero di sequenza
    windows, sums, seqs = {}, {}, {}
    spikes, total_hash, tuples = 0, 0, 0
    dump = []
    for _ in range(passes):
        for device, value in records:
            # stessa numerazione di SensorSource: posizione della lettura fra le letture del suo
            # sensore, crescente da una passata all'altra
            seq = seqs.get(device, 0)
            seqs[device] = seq + 1
            tuples += 1
            # MovingAverage: si toglie il valore più vecchio, poi si aggiunge il nuovo, come in
            # MovingAverageBolt e sd_stages.mojo, così la somma in virgola mobile è la stessa
            window = windows.setdefault(device, deque())
            total = sums.get(device, 0.0)
            if len(window) > WINDOW - 1:
                total -= window.popleft()
            window.append(value)
            total += value
            sums[device] = total
            avg = total / len(window)
            # SpikeDetector: stesso test di SpikeDetectionBolt
            if abs(value - avg) > THRESHOLD * avg:
                spikes += 1
                total_hash = (total_hash + spike_hash(device, seq)) & MASK
                dump.append((device, seq))
    print(f"RESULT tuples={tuples} spikes={spikes} hash={total_hash}")
    if len(sys.argv) == 4:
        with open(sys.argv[3], "w") as f:
            # ordinato solo per leggibilità: compare_dump.py confronta insiemi
            f.writelines(f"{d} {s}\n" for d, s in sorted(dump))


if __name__ == "__main__":
    main()
