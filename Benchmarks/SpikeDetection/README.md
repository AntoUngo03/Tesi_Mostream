# Spike Detection in MoStream

Porting dell'applicazione Spike Detection di
[DSPBench](https://github.com/GMAP/DSPBench) (versione Storm:
`dspbench-storm/src/main/java/org/dspbench/applications/spikedetection/`).

```
SensorSource → MovingAverage → SpikeDetector → SpikeSink
```

| Stadio | Parallelismo | Cosa fa |
|---|---|---|
| SensorSource | 1 | riproduce le letture caricate in memoria, per una durata fissa o per un numero esatto di passate |
| MovingAverage | N | media mobile delle ultime 1000 letture di ogni sensore |
| SpikeDetector | M | emette uno spike se `|valore − media| > 0,03 × media` (senza stato) |
| SpikeSink | 1 | conta gli spike e ne calcola un hash indipendente dall'ordine |

Finestra (1000), soglia (0,03) e campo usato (temperatura) sono i valori
predefiniti di DSPBench.

## Stato per chiave: un lock per sensore

In DSPBench MovingAverage riceve i dati con `fieldsGrouping` per sensore: tutte
le letture di un sensore vanno alla stessa replica. MoStream collega gli stadi con
una sola coda condivisa, quindi una lettura può arrivare a qualsiasi replica.
Qui lo stato di tutti i sensori sta in una tabella condivisa fra le repliche e
ogni sensore ha il proprio spinlock: lo stato è aggiornato in mutua esclusione.

A differenza dell'originale, con più repliche di MovingAverage due letture dello
stesso sensore possono essere elaborate in ordine inverso. Il programma le conta
(`late`: letture arrivate dopo una lettura successiva dello stesso sensore) e
conta i tentativi falliti di acquisire un lock (`lock_waits`). Con MovingAverage
a una replica l'ordine è garantito e l'output coincide con il riferimento.

## File

| File | Contenuto |
|---|---|
| `prepare_dataset.py` | converte `sensors.dat` nel file binario letto dall'applicazione, con le regole di `SensorParser` |
| `reference.py` | implementazione sequenziale di riferimento in Python |
| `compare_dump.py` | confronta l'insieme degli spike di MoStream con quello del riferimento |
| `sd_stages.mojo` | messaggi, stato condiviso e stadi |
| `spike_detection.mojo` | programma per questa versione di MoStream (runtime standard e cooperativo) |
| `spike_detection_v2.mojo` | programma per il branch `cooperative-runtime-v2` |
| `prepare_v2.sh` | crea una copia del branch v2 con la scelta della coda Vyukov/Rigtorp |

## Dataset

Il dataset è `dspbench-spark/data/spikedetection/sensors.dat` del repository
DSPBench: 80 000 righe del dataset Intel Berkeley Research Lab. Il parsing avviene
una volta, fuori dal tempo misurato:

```sh
python3 Benchmarks/SpikeDetection/prepare_dataset.py sensors.dat Benchmarks/SpikeDetection/data/sensors.bin
# records=77578 devices=52 max_device=54 skipped={'fields': 2422, 'date': 0, 'key': 0, 'value': 0}
```

Le righe con un numero di campi diverso da 8 sono scartate, come in DSPBench.
La frazione di secondo può avere da 1 a 6 cifre: `appendFractionOfSecond(3, 6)`
di Joda-Time stampa almeno 3 cifre ma ne accetta da 1 a 6 in lettura.

## Compilazione ed esecuzione

Dalla radice del repository, con Mojo 1.1 e `libpinning.so` compilata
(`make -C MoStream/lib`):

```sh
export MOSTREAM_HOME="$PWD"
mojo build -O3 -I . -I Benchmarks/SpikeDetection Benchmarks/SpikeDetection/spike_detection.mojo -o sd_vyukov
mojo build -O3 -I . -I Benchmarks/SpikeDetection -D MOSTREAM_RIGTORP=1 Benchmarks/SpikeDetection/spike_detection.mojo -o sd_rigtorp

# ./sd_<coda> <dataset.bin> <durata_s> <passate> <MovingAverage> <SpikeDetector> <std|coop> <n_workers> [dump]
./sd_rigtorp Benchmarks/SpikeDetection/data/sensors.bin 10 0 4 4 std 10      # misura: 10 secondi
./sd_rigtorp Benchmarks/SpikeDetection/data/sensors.bin 0 3 1 4 coop 6 d.txt # verifica: 3 passate esatte
```

Con `passate > 0` la durata è ignorata. `n_workers` è usato solo dal runtime
cooperativo. L'ultima riga stampata ha la forma
`RESULT runtime=… queue=… ma=… sd=… workers=… tuples=… averaged=… detected=… spikes=… hash=… late=… lock_waits=… elapsed_ms=… throughput=…`;
il throughput è in tuple emesse dalla sorgente al secondo.

### Branch cooperative-runtime-v2

```sh
Benchmarks/SpikeDetection/prepare_v2.sh ~/mostream-v2
cd ~/mostream-v2 && export MOSTREAM_HOME="$PWD"
B=<repository>/Benchmarks/SpikeDetection
mojo build -O3 -I . -I $B -D MOSTREAM_RIGTORP=1 $B/spike_detection_v2.mojo -o sd_v2_rigtorp

# ./sd_v2_<coda> <dataset.bin> <durata_s> <passate> <MA> <SD> <std|coop-mpmc|coop-ws> <n_workers> <rounds> [dump]
./sd_v2_rigtorp $B/data/sensors.bin 10 0 4 4 coop-ws 10 8
```

`coop-mpmc` usa code dei pronti MPMC per worker, `coop-ws` code per worker con
work stealing; `rounds` è il parametro `max_consume_rounds` di `run_cooperative`.

## Verifica

```sh
python3 Benchmarks/SpikeDetection/reference.py Benchmarks/SpikeDetection/data/sensors.bin 3 ref_3.txt
# RESULT tuples=232734 spikes=200774 hash=6159549295451861730
./sd_vyukov Benchmarks/SpikeDetection/data/sensors.bin 0 3 1 4 std 6 d.txt
python3 Benchmarks/SpikeDetection/compare_dump.py ref_3.txt d.txt
```

Con 1 e 3 passate e MovingAverage a una replica, tutte le 30 esecuzioni provate
(26 configurazioni: Vyukov/Rigtorp × runtime standard, cooperativo, v2 `coop-mpmc`
e `coop-ws`, SpikeDetector a 1 e 4 repliche, `rounds` 1 e 8) danno conteggi e hash
identici al riferimento. Con MovingAverage a 4 e 16 repliche si osservano letture fuori ordine,
ma nelle prove eseguite l'insieme degli spike è rimasto identico. Le medie emesse
possono comunque differire da quelle del riferimento.

## Problema noto nel v2 con `coop-mpmc`

Con `coop-mpmc` il v2 a volte termina con
`Error: cooperative ready queue is full!` (3 esecuzioni su 20 con Vyukov,
configurazione 1+4, 7 worker). `try_push` della coda Vyukov fa un solo tentativo
e restituisce l'elemento anche quando la CAS fallisce per la contesa di un altro
produttore, non solo quando la coda è piena. `push_local` del v2 tratta questo
fallimento come errore fatale. In una copia in cui `push_local` ripete `try_push`,
20 esecuzioni su 20 sono terminate correttamente. `coop-ws` non ha mostrato il problema.
