#!/usr/bin/env python3
# Converte il dataset di Spike Detection di DSPBench (formato Intel Berkeley Research Lab)
# nel file binario letto dall'applicazione MoStream.
#
# Le regole di parsing seguono SensorParser di dspbench-storm:
#   - una riga è divisa sugli spazi e deve avere esattamente 8 campi
#     (data ora epoca moteid temperatura umidità luce tensione);
#   - data e ora devono formare "yyyy-MM-dd HH:mm:ss", eventualmente seguito da una
#     frazione da 1 a 6 cifre, altrimenti la riga è scartata (appendFractionOfSecond(3, 6)
#     di Joda-Time stampa almeno 3 cifre ma in lettura ne accetta da 1 a 6);
#   - la chiave è il moteid, il valore è il campo scelto (la temperatura per
#     default, come sd.parser.value_field=temp);
#   - le righe il cui valore non è un numero valido sono scartate.
#
# Formato di uscita (little endian):
#   int64 n, int64 max_device, poi n record (int64 device, float64 value)
#
# Uso: python3 prepare_dataset.py sensors.dat sensors.bin [temp|humid|light|volt]

import re
import struct
import sys

# indice di ogni valore selezionabile negli 8 campi di una riga
FIELDS = {"temp": 4, "humid": 5, "light": 6, "volt": 7}
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$")


def main():
    if len(sys.argv) not in (3, 4):
        sys.exit("usage: prepare_dataset.py <sensors.dat> <output.bin> [temp|humid|light|volt]")
    src, dst = sys.argv[1], sys.argv[2]
    value_field = FIELDS[sys.argv[3] if len(sys.argv) == 4 else "temp"]
    records = []  # (device, value) delle righe valide, nell'ordine del file
    skipped = {"fields": 0, "date": 0, "key": 0, "value": 0}  # righe scartate, per motivo
    with open(src) as f:
        for line in f:
            fields = line.split()
            if len(fields) != 8:
                skipped["fields"] += 1
                continue
            # data e ora sono due campi separati della riga
            if not DATE_RE.match(fields[0] + " " + fields[1]):
                skipped["date"] += 1
                continue
            try:
                device = int(fields[3])
            except ValueError:
                skipped["key"] += 1
                continue
            try:
                value = float(fields[value_field])
            except ValueError:
                skipped["value"] += 1
                continue
            records.append((device, value))
    if not records or min(d for d, _ in records) < 0:
        sys.exit("error: no valid records or negative device id")
    # l'applicazione indicizza le tabelle per sensore con l'id, quindi le serve l'id più alto
    max_device = max(d for d, _ in records)
    with open(dst, "wb") as out:
        # "<" = little endian, q = int64, d = float64: 16 byte di intestazione, 16 byte per record
        out.write(struct.pack("<qq", len(records), max_device))
        for device, value in records:
            out.write(struct.pack("<qd", device, value))
    print(f"records={len(records)} devices={len({d for d, _ in records})} max_device={max_device} skipped={skipped}")


if __name__ == "__main__":
    main()
