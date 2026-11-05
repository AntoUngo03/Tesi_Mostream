#!/bin/bash
# Campagna Spike Detection: Vyukov contro Rigtorp, questa versione di MoStream (std, coop) e
# branch cooperative-runtime-v2 (std, coop-ws con rounds 1 e 8).
# I binari devono già esistere:
#   $MAIN/bin/sd_{vyukov,rigtorp}    compilati da spike_detection.mojo in questo repository
#   $V2/bin/sd_v2_{vyukov,rigtorp}   compilati da spike_detection_v2.mojo nella copia creata da prepare_v2.sh
# coop-mpmc del v2 è escluso: può fermarsi con "cooperative ready queue is full!" (vedi README).
#
# Uso: run_campaign.sh <main_root> <v2_root> <dataset.bin> <output.csv> [duration_s] [repeats]

set -u
MAIN="$1"; V2="$2"; DATA="$3"; OUT="$4"; DUR="${5:-10}"; REPS="${6:-10}"
# repliche di MovingAverage e SpikeDetector
CONFIGS=("1 4" "4 4" "8 8" "16 16")
# libreria, runtime, max_consume_rounds (usato solo dal v2)
RUNTIMES=("main std 1" "main coop 1" "v2 std 1" "v2 coop-ws 1" "v2 coop-ws 8")
# un'esecuzione che non termina entro questo tempo viene interrotta e registrata come fallita
TIMEOUT=$((DUR * 6 + 30))

if [ -e "$OUT" ]; then echo "$OUT already exists" >&2; exit 1; fi
echo "lib,runtime,rounds,ma,sd,workers,rep,queue,tuples,averaged,detected,spikes,late,lock_waits,elapsed_ms,throughput" > "$OUT"

# esegue una prova e aggiunge una riga al CSV
run_one() {
    local lib=$1 rt=$2 rounds=$3 ma=$4 sd=$5 rep=$6 q=$7
    # worker cooperativi = actor (sorgente + repliche + sink); ignorato dal runtime standard
    local workers=$((ma + sd + 2)) root bin log rc
    if [ "$lib" = main ]; then
        root="$MAIN"; bin="$MAIN/bin/sd_$q"
        cmd=("$bin" "$DATA" "$DUR" 0 "$ma" "$sd" "$rt" "$workers")
    else
        root="$V2"; bin="$V2/bin/sd_v2_$q"
        cmd=("$bin" "$DATA" "$DUR" 0 "$ma" "$sd" "$rt" "$workers" "$rounds")
    fi
    # ogni libreria gira dalla propria radice, con il proprio MOSTREAM_HOME (libpinning.so)
    log=$(cd "$root" && MOSTREAM_HOME="$root" timeout "$TIMEOUT" "${cmd[@]}" 2>&1); rc=$?
    local line; line=$(echo "$log" | grep '^RESULT')
    if [ -z "$line" ]; then
        echo "WARN rc=$rc $lib $rt r$rounds $ma+$sd rep$rep $q: $(echo "$log" | tail -2 | tr '\n' ' ')" >&2
        echo "$lib,$rt,$rounds,$ma,$sd,$workers,$rep,$q,,,,,,,," >> "$OUT"
        return
    fi
    # divide la riga RESULT in coppie chiave=valore
    declare -A v
    for kv in $line; do [[ "$kv" == *=* ]] && v[${kv%%=*}]=${kv#*=}; done
    # la coda stampata dal binario deve essere quella attesa
    [ "${v[queue]}" = "$q" ] || echo "WARN queue mismatch $lib $rt $ma+$sd rep$rep: ${v[queue]} != $q" >&2
    if [ "${v[tuples]}" != "${v[averaged]}" ] || [ "${v[tuples]}" != "${v[detected]}" ]; then
        echo "WARN lost tuples $lib $rt $ma+$sd rep$rep $q: ${v[tuples]} ${v[averaged]} ${v[detected]}" >&2
    fi
    echo "$lib,$rt,$rounds,$ma,$sd,$workers,$rep,$q,${v[tuples]},${v[averaged]},${v[detected]},${v[spikes]},${v[late]},${v[lock_waits]},${v[elapsed_ms]},${v[throughput]}" | tee -a "$OUT"
}

# riscaldamento, scartato: la prima esecuzione dopo una pausa è più lenta (cache, frequenza della CPU)
(cd "$MAIN" && MOSTREAM_HOME="$MAIN" timeout "$TIMEOUT" "$MAIN/bin/sd_vyukov" "$DATA" "$DUR" 0 4 4 std 10 > /dev/null 2>&1)

for cfg in "${CONFIGS[@]}"; do
    set -- $cfg; ma=$1; sd=$2
    for rtspec in "${RUNTIMES[@]}"; do
        set -- $rtspec; lib=$1; rt=$2; rounds=$3
        for rep in $(seq 1 "$REPS"); do
            # alterna quale coda parte per prima, così una deriva nel tempo non favorisce nessuna delle due
            if (( rep % 2 )); then order="vyukov rigtorp"; else order="rigtorp vyukov"; fi
            for q in $order; do run_one "$lib" "$rt" "$rounds" "$ma" "$sd" "$rep" "$q"; done
        done
    done
done
echo DONE
