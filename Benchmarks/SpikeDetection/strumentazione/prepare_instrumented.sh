#!/bin/bash
# SOLO PER MISURE: crea una copia di questa versione di MoStream con le code strumentate
# (contatori degli eventi interni a push e try_pop, stampati alla distruzione di ogni Communicator).
# La libreria del repository non viene modificata: i file in strumentazione/MoStream sostituiscono
# quelli della copia. I throughput di questa copia non vanno confrontati con le campagne normali.
#
# Uso, dalla radice del repository: Benchmarks/SpikeDetection/strumentazione/prepare_instrumented.sh <target_dir>

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:?usage: prepare_instrumented.sh <target_dir>}"
if [ -e "$TARGET" ]; then echo "$TARGET already exists" >&2; exit 1; fi
mkdir -p "$TARGET/Benchmarks"
cp -r "$ROOT/MoStream" "$TARGET/MoStream"
cp -r "$ROOT/Benchmarks/SpikeDetection" "$TARGET/Benchmarks/SpikeDetection"
cp "$HERE"/MoStream/*.mojo "$TARGET/MoStream/"
make -s -C "$TARGET/MoStream/lib"
echo "Ready: $TARGET (export MOSTREAM_HOME=$TARGET before running)"
