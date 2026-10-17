# Benchmark 2: sorgente sequenziale e stadio replicato

Questo benchmark misura una topologia diversa dal benchmark `PipelineGrain`:

```text
                          +--> consumatore 0: calcolo T
sorgente --> coda condivisa+--> consumatore 1: calcolo T
                          +--> ...
                          +--> consumatore N-1: calcolo T
```

Una sola sorgente sequenziale genera ID da 1 a M. Ogni ID è estratto da **uno
solo degli N consumatori**, che esegue l'intero calcolo T. La coda è condivisa,
non una coda distinta per ogni worker. Si confrontano solo **Rigtorp e Vyukov**,
riusando le implementazioni del primo benchmark senza modificarle.

## Esecuzione

Richiede Linux, Mojo 1.1.0, Python 3 e GCC. Dalla radice del repository remoto:

```sh
cd /home/ungolo/MoStream
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/run.py"
```

Default: N=1,2,4,8,16 consumatori; T=0,1,2,5 µs **per input**; M=100.000 input;
capacità 1024; tre ripetizioni per configurazione. N=1 viene sempre incluso.
Ogni coppia di prove Rigtorp/Vyukov viene eseguita vicina nel tempo, con ordine
casuale e seed riproducibile. Anche l'ordine delle configurazioni viene mescolato.

Prova breve, anche con capacità minima e messaggi non divisibili per N:

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/run.py" \
  --consumers 1,2,4 --work-us 0,2 --messages 2003 --capacity 2 --repeats 1
```

Scansione usata per la validazione:

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/run.py" \
  --consumers 1,2,4,8,16 --work-us 0,1,2,5 \
  --messages 50000 --repeats 3 --output "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/results/validation"
```

L'output deve essere una cartella nuova. Per scegliere i core usare
`--cpus 0,1,2,3,4` (qui bastano per N fino a 4); `--cpus none` disabilita il pinning.

## Significato di N e T

- **N conta soltanto i consumatori**: ci sono N+1 task, contando la sorgente.
- Il numero di input M resta fisso quando N cambia.
- T è il lavoro di un intero input: **non viene diviso per N**.
- Il kernel xorshift viene calibrato cinque volte; per ogni T tutti i worker e
  tutte le code usano lo stesso numero di iterazioni. Non si usa sleep né si
  legge l'orologio per ogni input.
- T è un obiettivo calibrato, non una durata esatta garantita. Una baseline
  separata senza code misura il kernel e il suo digest, per interpretare lo
  scostamento fra T richiesto e costo osservato.

Se sorgente e comunicazione non limitano il sistema, il throughput ideale è:

```text
throughput ideale = N / T_secondi = N * 1.000.000 / T_microsecondi
```

Esempio: T=2 µs e N=8 danno 4 milioni di input/s ideali. Questo non è il throughput
che ci aspettiamo obbligatoriamente di raggiungere: il calcolo trascura coda,
contesa, generazione, sincronizzazione e gestione degli output.
Per T=0 la formula non è definita; il benchmark registra soltanto il throughput
osservato, senza inventare un throughput ideale infinito o un'efficienza.

## Misura e controlli

I task sono fissati a N+1 core fisici distinti per default, rispettando l'affinità
concessa. Il runner rifiuta N se mancano core o worker del runtime. Il pinning,
le allocazioni, la preparazione delle pagine di memoria, il warm-up e la verifica
dei risultati sono fuori dal cronometro. Una barriera avvia insieme i task.
La misura termina quando tutti i task hanno finito; include avvio del flusso,
svuotamento e messaggi di terminazione (N zeri, distinti dagli ID degli input).

I consumatori non ricevono quote prefissate: ciascuno prende il prossimo input
quando è disponibile. Un log privato di ID per worker consente di verificare
**esattamente una elaborazione per ogni ID** dopo la misura. Durante il tempo
misurato questo aggiunge una scrittura sequenziale per input e un digest locale,
uguali per entrambi i backend; non c'è un atomico di verifica per messaggio.
L'oracolo ricalcola anche i risultati del kernel per ciascun worker.
La memoria del log richiede circa `8 * N * M` byte ed è liberata dopo la prova.

Prima delle misure, ogni backend viene verificato anche con capacità 2 e al
massimo tre input, per controllare la terminazione quando alcuni worker non
ricevono dati.

Il conteggio per worker viene salvato: differenze fra worker sono possibili e
non implicano da sole perdita di messaggi. L'assegnazione è dinamica.

## Output e interpretazione

Ogni esecuzione crea `raw.csv`, `summary.csv` e `metadata.json` nella cartella
indicata da `--output`, oppure sotto `"Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/results/"`.

- `throughput_per_second`: input completati al secondo; maggiore è meglio.
- `ideal_target_throughput`: N/T per T>0.
- `efficiency_vs_target`: rapporto fra throughput misurato e N/T.
- `ideal_measured_compute_throughput`: N diviso per il costo della baseline
  di solo calcolo; anche questo è un riferimento approssimato.
- `speedup_vs_one_consumer`: guadagno rispetto alla farm con un consumatore,
  che ha comunque sorgente e coda; non è una baseline senza comunicazione.
- `scaling_efficiency`: speedup/N.
- `source_elapsed_ns`: tempo per inserire tutti gli input, incluso il tempo
  trascorso aspettando spazio. **Non misura il limite intrinseco della sorgente**
  e non dimostra da solo che la sorgente sia il collo di bottiglia.
- `worker_counts`: numero esatto di input elaborati da ogni consumatore.

Un plateau o un peggioramento aumentando N segnala un limite alla scalabilità.
Può dipendere dalla sorgente, dalla coda condivisa o da interferenze del sistema;
il solo throughput non basta a separare queste cause. T=0 offre un riferimento
empirico del percorso sorgente/coda/consumatori, non una misura pura della coda.

Sono prove su un harness dedicato con `_asyncrt` di Mojo 1.1. Non modificano il
runtime principale di MoStream né misurano l'intero Communicator con il suo
protocollo di chiusura. Le operazioni di coda fanno attesa attiva. Tre ripetizioni
servono a un confronto preliminare, non a dimostrare una superiorità stabile.
I metadati includono versione Mojo, CPU, parametri, calibrazioni, stato Git e hash
dei sorgenti; i risultati generati e le cache sono esclusi da Git.

## Grafici

Matplotlib è facoltativo ed è già disponibile sulla macchina remota:

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/plot.py" "Benchmarks/BenchMark Vyukov vs RigTorp/FarmThroughput/results/validation"
```

Produce `farm_throughput.png/pdf` (throughput e ideale N/T) e
`farm_efficiency.png/pdf` (rapporto rispetto all'ideale per T>0).
Le fasce dei grafici indicano il minimo e il massimo delle ripetizioni,
non intervalli di confidenza. Il throughput usa una scala logaritmica per
mostrare nello stesso pannello throughput osservato e ideale.
