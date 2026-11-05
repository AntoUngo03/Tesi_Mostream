#!/usr/bin/env python3
# Confronta gli spike scritti dal sink di MoStream (argomento dump_file) con
# quelli scritti da reference.py. Ogni riga di entrambi i file è "device seq".
#
# Uso: python3 compare_dump.py reference_dump mostream_dump

import sys


# insieme di coppie (device, seq): l'ordine delle righe non conta
def load(path):
    with open(path) as f:
        return {tuple(map(int, line.split())) for line in f if line.strip()}


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: compare_dump.py <reference_dump> <mostream_dump>")
    ref, got = load(sys.argv[1]), load(sys.argv[2])
    common = len(ref & got)
    missing, extra = len(ref - got), len(got - ref)
    union = len(ref | got)
    # 1.0 = stesso insieme; valori più bassi = frazione di spike comuni alle due esecuzioni
    jaccard = common / union if union else 1.0
    status = "IDENTICAL" if missing == 0 and extra == 0 else "DIFFERENT"
    print(f"{status} reference={len(ref)} mostream={len(got)} common={common} "
          f"missing={missing} extra={extra} jaccard={jaccard:.6f}")


if __name__ == "__main__":
    main()
