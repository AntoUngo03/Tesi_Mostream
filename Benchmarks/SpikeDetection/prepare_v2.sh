#!/bin/bash
# Crea una copia del branch cooperative-runtime-v2 di MoStream con la scelta della coda fra stadi
# di questo repository (Vyukov per default, Rigtorp con -D MOSTREAM_RIGTORP=1).
# L'unica modifica al branch è il tipo di coda usato da communicator.mojo.
#
# Uso, dalla radice del repository: Benchmarks/SpikeDetection/prepare_v2.sh <target_dir>

set -euo pipefail
COMMIT=423b32b   # "[FIX] Fixed to compile with mojo 1.1.0"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET="${1:?usage: prepare_v2.sh <target_dir>}"
if [ -e "$TARGET" ]; then echo "$TARGET already exists" >&2; exit 1; fi

git clone -q --branch cooperative-runtime-v2 https://github.com/ParaGroup/MoStream.git "$TARGET"
git -C "$TARGET" checkout -q "$COMMIT"
# communication_queue.mojo sceglie Vyukov o Rigtorp in compilazione e richiede la coda Rigtorp
cp "$ROOT/MoStream/communication_queue.mojo" "$ROOT/MoStream/MPMC_queue_rigtorp.mojo" "$TARGET/MoStream/"

# Sostituisce MPMCQueue (Vyukov) con CommunicationQueue nel communicator. Ogni riga deve comparire
# esattamente una volta: se il branch cambia, lo script si ferma invece di produrre una copia sbagliata.
python3 - "$TARGET/MoStream/communicator.mojo" <<'EOF'
import sys
path = sys.argv[1]
text = open(path).read()
replacements = [
    ("from MoStream.MPMC_queue import MPMCQueue", "from MoStream.communication_queue import CommunicationQueue"),
    ("Pointer[MPMCQueue[MessageWrapper[Self.T]], MutUntrackedOrigin]", "Pointer[CommunicationQueue[MessageWrapper[Self.T]], MutUntrackedOrigin]"),
    ("unsafe_alloc[MPMCQueue[MessageWrapper[Self.T]]](1)", "unsafe_alloc[CommunicationQueue[MessageWrapper[Self.T]]](1)"),
    ("unsafe_write(MPMCQueue[MessageWrapper[Self.T]](size=queue_size))", "unsafe_write(CommunicationQueue[MessageWrapper[Self.T]](size=queue_size))"),
]
for old, new in replacements:
    if text.count(old) != 1:
        sys.exit("communicator.mojo: unexpected content, cannot replace: " + old)
    text = text.replace(old, new)
open(path, "w").write(text)
EOF

# libpinning.so, usata da setPinning
make -s -C "$TARGET/MoStream/lib"
# mostra i file cambiati, per controllare che sia stato modificato solo communicator.mojo
git -C "$TARGET" diff --stat
echo "Ready: $TARGET (commit $COMMIT, export MOSTREAM_HOME=$TARGET before running)"
