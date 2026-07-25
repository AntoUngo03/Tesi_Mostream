# MPMC vs Padded-FAA nelle pipeline MoStream

Questa cartella confronta le code dati inter-stage dei tre test richiesti:

- `Tests/test_pipe_1.mojo` (standard, `1P/1C`, stringa singola);
- `Tests/test_pipe_2.mojo` (standard, `1P/1C`, due emissioni);
- `Tests/test_pipe_3_coop.mojo` (cooperativo, topologia `2/2/3/3`).

I test originali restano smoke test funzionali. I primi due stampano ogni
elemento e tutti e tre elaborano soltanto circa 1000 elementi, quindi il loro
wall time non isola il costo della coda. Le tre varianti qui presenti conservano
grafo e lavoro degli stage, eliminano le stampe per elemento, accettano un
numero di elementi configurabile e verificano conteggio e checksum dopo ogni
run. Il limite configurabile produce esattamente `N` elementi per sorgente;
nei due test originali la condizione `count > 1000` ne produce invece 1001.

La coda di `Communicator` si seleziona in compilazione:

```text
default                         -> MPMC CAS originale
-DMOSTREAM_PADDED_FAA=1         -> Padded-FAA
```

La capacità è sempre 1024 e il pinning resta disabilitato, come nei test
originali. Nel caso cooperativo cambiano soltanto le tre code dati tra gli
stage; le code `ready/wait` dello scheduler restano MPMC-CAS. Questo isola la
variabile sperimentale, ma significa anche che il risultato non rappresenta
uno scheduler interamente riscritto con Padded-FAA.

Entrambe le build passano dallo stesso adapter `PipelineQueue`: contiene un
solo puntatore e il ramo di selezione è risolto a compile time. Non viene quindi
aggiunto un branch runtime soltanto a una delle due implementazioni.

## Esecuzione

Dalla root del repository:

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_benchmarks.py \
  --elements 250000 --warmups 3 --repetitions 30 \
  --workers 1,2,4,8
```

Per una prova esplorativa più rapida si possono usare 10 ripetizioni. Il runner
compila sei binari `-O3`, rimescola i casi in blocchi con seed registrato,
mantiene adiacente ogni coppia MPMC/Padded-FAA e assegna a ogni backend 15 prime
e 15 seconde esecuzioni. Il runner rifiuta output con backend/test/worker,
conteggio, checksum o throughput inattesi e produce:

- `results_raw.csv`: ogni singolo campione;
- `results_summary.csv`: media, mediana, deviazione standard, CoV, IC95% e
  speedup Padded-FAA/MPMC;
- `run_manifest.json`: parametri, seed, hardware, toolchain, stato Git e hash
  SHA-256 di sorgenti, binari e risultati.

Lo speedup è calcolato sui tempi accoppiati come `tempo_MPMC / tempo_Padded`:
un valore maggiore di 1 indica Padded-FAA più veloce. La stima principale è la
media geometrica dei rapporti, con IC95% calcolato sui log-rapporti.

`Mtransfer/s` conta milioni di elementi che attraversano un edge al secondo,
EOS esclusi. Un transfer comporta una enqueue e una dequeue, quindi non è una
singola operazione di coda. Il timer comprende `pipeline.run()` o
`pipeline.run_cooperative()`: include setup interno di runtime/code e le poche
stampe di stato, ma esclude costruzione della `Pipeline`, caricamento iniziale
della libreria di pinning e distruzione successiva.

## Interpretazione

`pipe_1` e `pipe_2` sono SPSC: non generano la contesa MPMC nella quale FAA
tende a essere più utile. Inoltre conservano il costo di costruzione e
trasferimento delle `String`, quindi misurano correttamente l'applicazione ma
non una micro-misura pura della coda.

Nel runtime cooperativo gli actor chiamano `try_push` e `try_pop`. Queste API
devono controllare pieno/vuoto senza attendere e nella Padded-FAA reclamano il
ticket tramite CAS; il test misura soprattutto layout/padding e comportamento
non bloccante, non il percorso FAA bloccante usato dal runtime standard.

Conteggio e checksum sono controlli di sanità per perdita/duplicazione, non una
prova formale di exact-once. I risultati della campagna del 22 luglio 2026 e le
limitazioni osservate sono riassunti in `RESULTS.md`.
