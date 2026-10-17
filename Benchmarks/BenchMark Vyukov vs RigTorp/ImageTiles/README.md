# ImageTiles: immagini, blocchi di righe e confronto con il seriale

Terzo benchmark, separato da ImagePipeline e dagli esperimenti sintetici.
Confronta **seriale senza code**, **Rigtorp** e **Vyukov** sulla stessa conversione
RGB in scala di grigi. I file e i risultati precedenti restano invariati.

```text
                         +--> consumer 0 --+
produttore --> coda lavori+--> ...         +--> coda completamenti --> raccoglitore
                         +--> consumer N-1+
```

N indica solo i consumer: il percorso parallelo usa N+2 task e core distinti.
Il seriale usa un solo thread senza allocare code né creare task applicativi.

## Avvio sul server

```sh
cd "/home/ungolo/MoStream/Benchmarks/BenchMark Vyukov vs RigTorp"
python3 ImageTiles/run.py
```

Richiede Linux, Mojo 1.1.0, Python 3 e GCC; matplotlib serve solo per i grafici.
Il runner compila automaticamente i due backend e il kernel. Default:

- risoluzione 512×512, 32 immagini per misura;
- blocchi da 1, 8, 32, 128, 512 righe (strisce orizzontali, non tile quadrati);
- N=1,2,4,8,16 consumer;
- 50 ripetizioni per configurazione e capacità 1024 per ciascuna coda.

La baseline seriale è eseguita 50 volte per risoluzione e condivisa dai confronti
su blocchi e N. I job sono mescolati; Rigtorp e Vyukov sono eseguiti in coppie
vicine nel tempo con ordine casuale. Non avviare altri benchmark contemporaneamente.

Prova breve:

```sh
python3 ImageTiles/run.py --sizes 256x256 --images 8 \
  --block-rows 1,16,64 --consumers 1,4 --repeats 3
```

Campagna da 50 ripetizioni con tre dimensioni di blocco:

```sh
python3 ImageTiles/run.py --sizes 512x512 --images 32 \
  --block-rows 1,16,128 --consumers 1,2,4,8,16 --repeats 50
```

Produce 1.550 misure: 1.500 parallele più 50 seriali. La cartella di output è
nuova e ha nome data-ora. `--output /percorso/nuovo` sceglie una destinazione;
un percorso esistente viene rifiutato. `--cpus none` disabilita il pinning.
Per cambiare risoluzioni: `--sizes 256x256,512x512,1920x1080`.
Il numero di immagini deve essere uguale fra i backend del confronto.

## Immagini e trasformazione

Sono immagini RGB generate in memoria, con gradienti deterministici differenti
per identificativo di immagine. Non è un dataset fotografico e non c'è I/O su file.
Si riusano il layout planare e la formula di ImagePipeline:

```text
gray = (77*R + 150*G + 29*B) >> 8
output RGB = (gray, gray, gray)
```

Il kernel misurato elabora otto pixel alla volta con SIMD e una coda scalare
per le dimensioni non multiple di otto. Lo stesso kernel è usato nel seriale,
che elabora l'intera immagine, e nei consumer, che elaborano una striscia.
L'oracolo di correttezza è una implementazione scalare indipendente.
La scala di grigi non richiede pixel vicini e non introduce bordi/halo fra blocchi.

Gli input, l'output e il riferimento vengono allocati e toccati prima del timer.
Richiedono circa `9 * larghezza * altezza * immagini` byte, oltre a piccoli buffer
di controllo e al dataset del warm-up. `--max-memory-mib` limita la stima dei tre
buffer misurati (default 2048 MiB). Le immagini restano vive fino al termine dei task.

## Comunicazione e ricomposizione

Le code trasportano UInt64 che identificano immagine e blocco. I descrittori sono
interpretati tramite i parametri comuni: **non si copiano i pixel nelle code**.
I consumer scrivono regioni disgiunte del buffer finale e inviano un completamento.
Il raccoglitore verifica gli identificativi e segna l'immagine completa solo dopo
l'arrivo di tutti i blocchi. È una ricomposizione logica, senza copia finale.
Gli arrivi possono essere fuori ordine. La concorrenza è 1 produttore/N consumer
nella coda lavori e N produttori/1 consumer nella coda completamenti.

Si usano push/pop bloccanti con attesa attiva in un harness Mojo dedicato,
non il Communicator né i runtime standard/cooperativo completi di MoStream.
Il raccoglitore può diventare un collo di bottiglia; i risultati includono il
suo lavoro e non isolano il costo puro della coda.

## Metriche e correttezza

- **Throughput (immagini/s):** immagini interamente elaborate divise per tempo
  totale. Include avvio, svuotamento, messaggi di terminazione e raccolta.
- **Latenza parallela per immagine:** da prima dell'enqueue del primo blocco fino
  alla ricezione dell'ultimo completamento da parte del raccoglitore. Include
  backpressure e attesa nelle due code. Diverse immagini possono essere in volo.
- **Latenza seriale:** inizio/fine del kernel dell'intera immagine. Non comprende
  una coda d'ingresso o una simulazione di arrivi esterni.
Il timestamp iniziale non è comune a tutte le immagini: il produttore avvia la
latenza di un'immagine quando inizia a inviarne il primo blocco. L'attesa dietro
alle immagini precedenti prima di quell'istante non è inclusa. Cambiare la
suddivisione può cambiare il ritmo di ammissione e il numero di immagini in volo.
Queste sono latenze dall'emissione, non tempi di risposta rispetto a un flusso
esterno con arrivi fissati. Il seriale misura il proprio tempo di servizio per
immagine; non simula la stessa coda di arrivi. Leggere sempre latenza e throughput
insieme e non attribuire al solo algoritmo di coda le differenze osservate.

- **Media, mediana e p95:** calcolati sulle immagini di ogni singola prova. Il p95
  usa interpolazione lineare. Il riepilogo prende la mediana dei valori per prova,
  senza trattare immagini della stessa prova come repliche sperimentali indipendenti.
- **Speedup:** tempo mediano seriale / tempo mediano parallelo, a parità di risoluzione
  e numero di immagini. Un valore inferiore a uno indica un rallentamento.
- **Correttezza:** confronto byte per byte con il riferimento scalare, checksum
  ordine-sensibile e verifica esatta di copertura dei blocchi, senza duplicazioni.

Confronto esatto e checksum sono **fuori dalla misura**. I controlli dei blocchi
nel raccoglitore e i timestamp una volta per immagine sono invece inclusi e uguali
per i due backend. Prima delle misure vengono eseguiti un warm-up e casi mirati:
19×13 con blocchi di 4 righe e code di capacità 2; 1×1 con consumer eventualmente
più numerosi dei blocchi. Si verificano così residui SIMD, ultimo blocco parziale,
correttezza dei canali e terminazione.

## File e grafici

Ogni esecuzione crea sotto `ImageTiles/results/`:

- `raw.csv`: tutte le prove, throughput, latenza media/mediana/p95 e checksum;
- `latencies.csv`: un campione per immagine, con configurazione e ripetizione;
- `summary.csv`: tempo totale mediano, throughput ricavato da quel tempo, min/max throughput, speedup e riepiloghi di latenza;
- `metadata.json`: ambiente, parametri, definizione di latenza e hash sorgenti; non serve calibrare un T sintetico;
- `COMPLETATO.txt`: presente solo dopo il completamento di tutte le misure.

Per i grafici, sostituire NOME_CARTELLA con la cartella effettivamente prodotta:

```sh
python3 ImageTiles/plot.py "ImageTiles/results/NOME_CARTELLA"
```

Per ogni risoluzione vengono creati `throughput_WxH.png/pdf`, `speedup_WxH.png/pdf`
e `latency_WxH.png/pdf`. Blu = Rigtorp; arancione = Vyukov; linee grigie = seriale.
Nei grafici del throughput i punti sono ricavati dal tempo totale mediano;
le fasce indicano minimo e massimo, non intervalli di confidenza. I pannelli
possono usare scale verticali diverse: leggere i valori, non solo le altezze. Nei grafici di latenza le linee continue indicano la mediana e quelle
tratteggiate il p95. Maggiore throughput non garantisce minore latenza.

I risultati dipendono anche da banda di memoria, cache, costo del kernel, numero
finito di immagini e collettore. Blocchi piccoli aumentano le comunicazioni;
blocchi grandi riducono il loro numero ma possono distribuire peggio il lavoro.
Cinquanta ripetizioni nella stessa sessione non dimostrano superiorità universale.
I dati generati sono esclusi da Git; il contenuto di ImagePipeline non viene modificato.
