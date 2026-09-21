# Michael–Scott bounded per il runtime cooperativo

Implementazione: [`MoStream/Micheal_Scott.mojo`](../../MoStream/Micheal_Scott.mojo).
Il nome del file originale è conservato; il tipo pubblico è
`MichaelScottQueue[T: Copyable & Deinitable]`.

La coda conserva lista collegata, nodo dummy, CAS su head/tail e helping della
Michael–Scott, usando un pool preallocato di `capacity + 1` nodi. È una variante
bounded con gestione esplicita della vita del payload, non una trascrizione
letterale dello [pseudocodice originale](https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html).
Non alloca nodi durante push/pop; il tipo del payload può comunque allocare.

## Correzioni di concorrenza

Nella prima versione il consumer aggiornava `head` prima di estrarre il valore,
ma un consumer successivo poteva già restituire quel nodo al pool e cancellarne
il payload. Ora ogni nodo pubblicato ha due obblighi, rappresentati da
`remaining = 2`:

1. il consumer vincitore del CAS su `head` deve completare `data.take()`;
2. un consumer successivo deve rimuovere quel nodo dal ruolo di dummy.

Ogni evento decrementa `remaining` con `ACQUIRE_RELEASE`. Solo l'ultimo evento
restituisce il nodo alla free-list. Il dummy iniziale parte da 1 perché non ha
payload. Il consumer che perde il CAS non accede mai al payload. Il successore
può rimuovere il nodo dalla lista mentre il proprietario del payload è sospeso,
ma non può distruggerne il valore o consentirne il riuso.

I lettori speculativi accedono solo a campi atomici di nodi che restano allocati
fino alla distruzione della coda:

- `free_next` è atomico, perché un perdente del CAS sulla free-list può ancora
  leggerlo mentre un altro thread modifica il nodo;
- i tag di `head`, `tail` e `free_head` avanzano a ogni aggiornamento;
- anche la generazione di `node.next` avanza durante il riuso, senza essere
  azzerata: un producer sospeso tra la verifica di `tail` e il CAS sul link non
  deve poter inserire attraverso una vecchia incarnazione dello stesso nodo;
- il CAS vincente su `head` usa `ACQUIRE_RELEASE` per propagare la pubblicazione
  del nodo ai consumer successivi;
- `count` è signed e la stima è limitata a `[0, capacity]`. Una dequeue può
  precedere l'incremento del producer dopo la pubblicazione: un temporaneo
  valore negativo non deve diventare una falsa stima di coda piena.

## Contratto e limiti

- `try_push` restituisce il payload se non trova un nodo disponibile;
  `try_pop` restituisce `None` quando osserva la coda vuota.
- I metodi `try_*` possono riprovare sotto contesa: non sono wait-free e non
  garantiscono un tempo massimo per attivazione cooperativa.
- Operazioni sospese possono trattenere nodi. Di conseguenza un `try_push` può
  fallire anche se la lista contiene meno di `capacity` elementi. La capacità
  nominale è interamente disponibile in assenza di operazioni in corso.
- Non si rivendica una garanzia generale di progresso lock-free per questa
  variante bounded: un'operazione sospesa può trattenere capacità del pool.
- La protezione ABA assume che nessuna operazione rimanga sospesa per un intero
  giro di `2**32` modifiche della stessa parola tagged. I test non dimostrano
  correttezza oltre tale assunzione, né costituiscono una prova formale.
- Move e distruzione richiedono che nessun thread stia usando la coda.
- Le capacità ammesse sono da 2 a `2**32 - 2`; non devono essere potenze di due.
  I benchmark comparativi usano potenze di due per compatibilità con le altre
  implementazioni.

## Integrazione

```bash
mojo build --Werror -O3 -I . -DMOSTREAM_MICHAEL_SCOTT=1 \
  Benchmarks/PipelineQueueBenchmark/pipe_mpmc_cooperative_benchmark.mojo \
  -o /tmp/pipe_coop_ms
MOSTREAM_HOME="$PWD" /tmp/pipe_coop_ms 50000 4 4 1024 0
```

Il flag modifica solo le code dati di `Communicator`, tramite `PipelineQueue`.
Le code ready/wait dello scheduler restano MPMC, uguali per tutti i backend.
La selezione di più backend contemporaneamente è un errore di compilazione.
I benchmark pipeline stampano `backend=MICHAELSCOTT` quando selezionato.

## Verifica

```bash
python3 Tests/run_tests.py

# Stress con consumer sospesi dopo il CAS vincente su head.
mojo build --Werror -O3 -I . -DMOSTREAM_MS_TEST_PAUSE=1 \
  Tests/Test_Queue/test_michael_scott_concurrent.mojo -o /tmp/test_ms_paused
timeout 60s /tmp/test_ms_paused
```

`test_michael_scott_queue.mojo` controlla FIFO, pieno/vuoto, recupero della
capacità, payload String, move di una coda popolata e dimensioni non valide.
Due regressioni deterministiche riproducono il consumer sospeso dopo il CAS e
il producer con un vecchio snapshot di `Tail.next` dopo il riuso dello stesso
nodo. La prima separa esplicitamente claim e completamento della dequeue per
riprodurre l'interleaving senza dipendere dalle scelte del sistema operativo.

`test_michael_scott_concurrent.mojo` verifica ogni identificativo esattamente
una volta e l'integrità della stringa associata: 4P/4C con capacità 2, 3, 16,
1024 nei percorsi bloccante e try; inoltre 1P/8C e 8P/1C con capacità 4. Ogni
producer invia 2000 elementi, per 82.000 messaggi complessivi per suite, EOS
esclusi. La variante `MOSTREAM_MS_TEST_PAUSE` introduce un ritardo solo nella
build di test; è disabilitata nei benchmark.

## Confronto cooperativo

Il [runner dedicato](../../Benchmarks/PipelineQueueBenchmark/run_michael_scott.py)
confronta MPMC, Padded-FAA, SCQ e Michael–Scott nello stesso grafo:

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_michael_scott.py \
  --output /tmp/mostream-ms-comparison
```

Configurazione predefinita: 4 source e 4 sink, 50.000 messaggi per source,
capacità 1024, 1/2/4/8 worker, lavoro per messaggio 0/160 iterazioni xorshift,
2 warm-up e 12 ripetizioni per combinazione. Il runner verifica conteggio,
checksum calcolato indipendentemente in Python e coerenza del throughput.

I casi sono rimescolati in blocchi; la posizione di ciascun backend è bilanciata
ruotando l'ordine a ogni blocco. Non vengono eliminati outlier. I risultati
comprendono tempi grezzi, mediana/media/CoV, rapporti accoppiati rispetto a MPMC,
IC95% nominali sui log-rapporti, log delle esecuzioni e manifest con hardware,
compilatore, parametri, stato Git e hash di sorgenti, binari e risultati.
L'output deve essere una directory vuota per preservare campagne precedenti.

Il timer comprende inizializzazione dello scheduler e `run_cooperative`, con
pinning disabilitato. Misura quindi la pipeline completa, non il costo isolato
della coda. Il controllo count/checksum della pipeline non sostituisce il test
per-identificativo della coda. Le differenze sono esplorative e gli intervalli
non sono corretti per confronti multipli.

Risultati della campagna: [MICHAEL_SCOTT_RESULTS.md](../../Benchmarks/PipelineQueueBenchmark/MICHAEL_SCOTT_RESULTS.md).

Nel cooperativo MPMC e Padded-FAA usano CAS nei percorsi `try_*`; il ticket FAA
bloccante non viene esercitato. Michael–Scott aggiunge operazioni atomiche per
il pool e per i due obblighi di vita del nodo, oltre ai CAS della lista e al
contatore indicativo. SCQ usa invece due code di indici e payload separati.
Queste differenze spiegano quali costi cercare nelle misure, ma non permettono
da sole di prevedere un vincitore: layout, contesa e costo dello scheduler
concorrono al tempo osservato.
