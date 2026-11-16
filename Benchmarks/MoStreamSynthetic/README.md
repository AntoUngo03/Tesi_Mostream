# FarmThroughput e PipelineGrain dentro MoStream

I due benchmark sintetici delle suite *Vyukov vs RigTorp* e *wCQ vs Vyukov*
usavano un harness dedicato, con task `_asyncrt` che chiamano direttamente le
code. Qui gli stessi carichi sono scritti con la **vera API MoStream**
(`Pipeline`, `seq`, `parallel`, `StageTrait`). La comunicazione passa quindi
per i `Communicator` del runtime, con il loro protocollo di EOS e chiusura.

La coda fra gli stadi viene scelta in compilazione. Il default è Vyukov; con
`-D MOSTREAM_RIGTORP=1` si usa Rigtorp. wCQ non è integrata nel runtime e quindi
non compare in questo confronto.

| Benchmark | Topologia MoStream | Task | Code |
| --- | --- | --- | --- |
| [FarmThroughput](FarmThroughput/farm.mojo) | `seq(FarmSource) -> parallel(FarmWorker, N)` | N+1 | 1 (1 produttore, N consumatori) |
| [PipelineGrain](PipelineGrain/pipeline.mojo) | `seq(GrainSource) -> seq(GrainStage) x N -> seq(GrainSink)` | N+2 | N+1 (1:1) |

## Esecuzione

Servono Linux, **Mojo 1.1**, Python 3 (solo libreria standard) e `taskset`.
Dalla radice del repository:

```sh
export MOJO=/opt/mojo/bin/mojo        # oppure --mojo /percorso/mojo
python3 Benchmarks/MoStreamSynthetic/FarmThroughput/run.py
python3 Benchmarks/MoStreamSynthetic/PipelineGrain/run.py
```

Default:

- **FarmThroughput**: N=1,2,4,8,16 consumatori; T=0,1,2,5 µs per input.
- **PipelineGrain**: N=1,2,4,8,16 stadi; T totale=0,1,2,5,10 µs per input.
- Per entrambi: M=100.000 input, capacità 1024, 5 ripetizioni, pinning attivo,
  runtime standard.

Opzioni comuni: `--messages`, `--repeats`, `--capacity` (potenza di due),
`--pinning on|off`, `--runtime std|coop`, `--seed`, `--output` (deve essere una
cartella nuova; il default è `<Benchmark>/results/<data-ora>-<runtime>-pin<on|off>`).

Prova breve:

```sh
python3 Benchmarks/MoStreamSynthetic/FarmThroughput/run.py \
  --consumers 1,4 --work-us 0,2 --messages 20000 --repeats 2
python3 Benchmarks/MoStreamSynthetic/PipelineGrain/run.py \
  --stages 1,4 --total-us 0,2 --messages 20000 --repeats 2
```

I binari si possono anche usare direttamente:

```sh
mojo build -I . [-D MOSTREAM_RIGTORP=1] Benchmarks/MoStreamSynthetic/FarmThroughput/farm.mojo -o farm
./farm M N I capacita on|off std|coop      # RESULT elapsed_ns source_ns conteggi_per_replica
mojo build -I . [-D MOSTREAM_RIGTORP=1] Benchmarks/MoStreamSynthetic/PipelineGrain/pipeline.mojo -o grain
./grain N M I capacita on|off std|coop     # RESULT elapsed_ns
```

Altre modalità: `info` (worker del runtime), `calibrate` (ns per iterazione),
`compute M I` (baseline senza code). Con il pinning attivo `MOSTREAM_HOME`
deve puntare alla radice del repository (lo imposta il runner) e
`MOSTREAM_PINNING` contiene i core; il runner sceglie **una CPU logica per
core fisico**.

## Cosa resta uguale agli originali

- Stesso kernel xorshift a 64 bit e stessa calibrazione. Il kernel viene
  calibrato cinque volte sul core del primo consumatore/stadio; si usa la
  mediana e da questa si ricavano le iterazioni per T. Tutti i backend usano le
  stesse iterazioni. Niente `sleep`, nessuna lettura dell'orologio per input.
- Farm: T è il lavoro di un **intero** input e non si divide per N. I
  consumatori prendono il prossimo input disponibile, senza quote fisse. Ogni
  replica scrive gli ID in un log privato e mantiene un digest locale. Dopo la
  misura si verifica che **ogni ID sia elaborato esattamente una volta** e si
  ricalcola il digest di ogni replica.
- Pipeline: il lavoro totale per input è fisso. Lo stadio k esegue `I//N`
  iterazioni, più una se `k < I%N`. Il digest calcolato al sink dipende
  dall'ordine e viene confrontato con il calcolo seriale.
- Il pinning usa core fisici distinti e un warm-up completo precede la misura.
  L'ordine delle configurazioni è mescolato e quello dei backend è casuale per
  ogni ripetizione, con un seed fisso.

## Cosa cambia, e perché

- **Il timer misura `Pipeline.run()`.** Comprende creazione di communicator e
  code, avvio dei thread, pinning, EOS e teardown, incluse le stampe del
  runtime. Sulla VM questo costo fisso vale circa 3 ms per prova: con M piccolo
  abbassa il throughput, quindi conviene usare M ≥ 100.000. Negli originali il
  cronometro partiva da una barriera con i task già pronti.
- **PipelineGrain ha sorgente e sink separati** (N+2 task, N+1 code). Questi due
  stadi non eseguono il kernel. Il caso N=1 quindi comunica: la baseline senza
  code dell'originale è la modalità `compute`, riportata in
  `serial_throughput`.
- **N di PipelineGrain è fissato in compilazione.** Gli stadi di una `Pipeline`
  sono una tupla, quindi ogni N ha il proprio ramo. Gli N supportati sono
  1,2,4,8,12,16,24; per aggiungerne uno basta aggiungere un ramo in
  `grain_pipeline`.
- **Farm, durata della sorgente** (`source_elapsed_ns`): va dal primo
  `next_element` al ritorno di `None` e include l'attesa di spazio nella coda.
- Con `--runtime coop` si usa `run_cooperative(task)` con un worker per task.
  È un confronto possibile, ma misura soprattutto lo scheduler cooperativo.

Non confrontare i valori assoluti con le suite harness: cambiano timer e
topologia. Il confronto valido è fra **Vyukov e Rigtorp nello stesso
esperimento**.

## File prodotti

- `raw.csv`: una riga per misura e backend. Contiene throughput, iterazioni, T
  calibrato, baseline e, per la farm, `source_elapsed_ns` e `worker_counts`.
- `summary.csv`: per ogni (N, T) riporta mediane, minimo e massimo,
  `rigtorp_over_vyukov` (>1 favorisce Rigtorp) e `rigtorp_wins` (coppie per
  ripetizione vinte da Rigtorp). La farm aggiunge throughput ideale N/T ed
  efficienza; la pipeline aggiunge `serial_throughput` e lo speedup.
- `metadata.json`: compilatore, CPU, core fisici e pinnati, parametri,
  calibrazioni, baseline e hash SHA-256 dei sorgenti MoStream e del benchmark.

Cinque ripetizioni sono un confronto esplorativo. Le fasce min–max non sono
intervalli di confidenza.
