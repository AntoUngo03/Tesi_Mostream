#!/bin/bash
# Campagna Spike Detection con la sorgente replicata (partizionata per sensore): Vyukov contro
# Rigtorp, runtime standard di questa versione di MoStream, al variare del numero S di sorgenti.
# I binari devono già esistere:
#   $MAIN/bin/sdm_{vyukov,rigtorp}   compilati da spike_detection_multisource.mojo
#
# Uso: run_campaign_multisource.sh <main_root> <dataset.bin> <output.csv> [duration_s] [repeats]

set -u
MAIN="$1"; DATA="$2"; OUT="$3"; DUR="${4:-10}"; REPS="${5:-10}"
# numero di sorgenti
SOURCES=(1 2 4 8)
# repliche di MovingAverage e SpikeDetector
CONFIGS=("1 4" "4 4" "8 8" "16 16")
# un'esecuzione che non termina entro questo tempo viene interrotta e registrata come fallita
TIMEOUT=$((DUR * 6 + 30))

if [ -e "$OUT" ]; then echo "$OUT already exists" >&2; exit 1; fi
echo "sources,ma,sd,workers,rep,queue,part_min,part_max,tuples,averaged,detected,spikes,late,lock_waits,elapsed_ms,throughput" > "$OUT"

# esegue una prova e aggiunge una riga al CSV
run_one() {
    local s=$1 ma=$2 sd=$3 rep=$4 q=$5
    # worker = actor (sorgenti + repliche + sink); usato solo dal runtime cooperativo, qui solo registrato
    local workers=$((s + ma + sd + 1)) log rc
    log=$(cd "$MAIN" && MOSTREAM_HOME="$MAIN" timeout "$TIMEOUT" "$MAIN/bin/sdm_$q" "$DATA" "$DUR" 0 "$s" "$ma" "$sd" std "$workers" 2>&1); rc=$?
    local line; line=$(echo "$log" | grep '^RESULT')
    if [ -z "$line" ]; then
        echo "WARN rc=$rc S=$s $ma+$sd rep$rep $q: $(echo "$log" | tail -2 | tr '\n' ' ')" >&2
        echo "$s,$ma,$sd,$workers,$rep,$q,,,,,,,,,," >> "$OUT"
        return
    fi
    # divide la riga RESULT in coppie chiave=valore
    declare -A v
    for kv in $line; do [[ "$kv" == *=* ]] && v[${kv%%=*}]=${kv#*=}; done
    # la coda stampata dal binario deve essere quella attesa
    [ "${v[queue]}" = "$q" ] || echo "WARN queue mismatch S=$s $ma+$sd rep$rep: ${v[queue]} != $q" >&2
    if [ "${v[tuples]}" != "${v[averaged]}" ] || [ "${v[tuples]}" != "${v[detected]}" ]; then
        echo "WARN lost tuples S=$s $ma+$sd rep$rep $q: ${v[tuples]} ${v[averaged]} ${v[detected]}" >&2
    fi
    echo "$s,$ma,$sd,$workers,$rep,$q,${v[part_min]},${v[part_max]},${v[tuples]},${v[averaged]},${v[detected]},${v[spikes]},${v[late]},${v[lock_waits]},${v[elapsed_ms]},${v[throughput]}" | tee -a "$OUT"
}

# riscaldamento, scartato
(cd "$MAIN" && MOSTREAM_HOME="$MAIN" timeout "$TIMEOUT" "$MAIN/bin/sdm_vyukov" "$DATA" "$DUR" 0 4 4 4 std 13 > /dev/null 2>&1)

for cfg in "${CONFIGS[@]}"; do
    set -- $cfg; ma=$1; sd=$2
    for s in "${SOURCES[@]}"; do
        for rep in $(seq 1 "$REPS"); do
            # alterna quale coda parte per prima, così una deriva nel tempo non favorisce nessuna delle due
            if (( rep % 2 )); then order="vyukov rigtorp"; else order="rigtorp vyukov"; fi
            for q in $order; do run_one "$s" "$ma" "$sd" "$rep" "$q"; done
        done
    done
done
echo DONE
