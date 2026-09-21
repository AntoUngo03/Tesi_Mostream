# Padico NBLFQ per MoStream

Implementazione: [`MoStream/NBLFQ_queue.mojo`](../../MoStream/NBLFQ_queue.mojo).
Tipo pubblico: `NBLFQQueue[T: Copyable & Deinitable]`.

È il port della macro **`PUK_LFQUEUE_NBLFQ_TYPE`** del file fornito dall'utente,
conservato integralmente in [Puk-nblfq.original.h](Puk-nblfq.original.h).
La [documentazione upstream di Puk](https://pm2.gitlabpages.inria.fr/pm2/Puk/doc/Puk-nblfq_8h_source.html)
pubblica lo stesso algoritmo. Il sorgente originale attribuisce il lavoro a
INRIA/University of Rennes 1, Alexandre Denis e Christian Perez e riporta
GPL-2.0-or-later; il port conserva attribuzione e licenza nel proprio header.

L'allegato contiene anche `PUK_LFQUEUE_NBLFQ2_TYPE`, che su sistemi a 64 bit
usa coppie a 128 bit. **NBLFQ2 non è il backend implementato o misurato qui.**
Non vengono emulate operazioni atomiche a 128 bit tramite due accessi separati.

## Algoritmo e adattamento

NBLFQ pubblica o rimuove il valore direttamente con un CAS sulla cella packed.
`head` e `tail` sono suggerimenti sulla posizione dalla quale iniziare la
scansione, non ticket riservati. Per riconoscere il punto di inserimento o
rimozione, la scansione confronta generazione, posizione e stato vuoto/pieno
di celle adiacenti. Un thread può completare il CAS e fermarsi prima di
aggiornare il suggerimento: gli altri recuperano la posizione dalla scansione.

Il port conserva scansioni, calcolo delle generazioni, CAS sull'intera cella
e aggiornamenti opportunistici degli indici. Le differenze esplicite sono:

- La word atomica a 64 bit contiene `indice + 1` nei 32 bit bassi e la
  generazione nei 32 bit alti. Zero è il valore vuoto. Non comprime indirizzi
  reali e non dipende dal layout di `Puk-tagged-ptr.h`.
- Celle e suggerimenti usano `Atomic`; non vengono tradotti gli accessi
  concorrenti C in accessi ordinari o `volatile` Mojo. I suggerimenti sono
  relaxed; le letture delle celle acquire; i CAS vincenti sequential, in linea
  con l'ordinamento forte dei CAS `__sync` del sorgente.
- I suggerimenti sono separati da 64 byte; gli array di celle sono allocati
  con allineamento 64. La rimappatura degli indici rimane disabilitata.
- Una coppia di generazioni ambigua causa una nuova scansione invece di
  terminare il processo. Questo non elimina l'assunzione sul wrap dei tag.
- Il profiling e le funzioni specializzate single-reader/single-writer
  dell'interfaccia C non fanno parte del port. Non viene riprodotto il backoff
  `puk_lfbackoff`, la cui implementazione non era nell'allegato.

Per supportare `String`, `MessageWrapper` e gli altri tipi generici di MoStream,
la coda usa **due ring NBLFQ di indici** e un array preallocato di payload:

1. Il producer estrae un indice da `free_indices`, scrive il payload e pubblica
   l'indice in `allocated_indices`.
2. Il consumer estrae un indice da `allocated_indices`, prende il payload e
   solo allora restituisce l'indice a `free_indices`.

Ogni payload ha un solo proprietario durante scrittura e lettura. Il CAS di
pubblicazione sincronizza la scrittura con l'acquisizione del consumer; il
ritorno dell'indice libero sincronizza il riuso con il producer successivo.
Un consumer sospeso dopo avere estratto l'indice ne conserva l'ownership.
Un producer sospeso prima della pubblicazione trattiene uno slot, ma non crea
un ticket non pubblicato davanti agli altri messaggi.

Questa indirection, analoga a quella del backend SCQ del repository, **fa parte
del costo misurato**. Il benchmark non rappresenta il costo della sola macro C
che scambia puntatori già gestiti dal chiamante.

## Contratto e limiti

- API: `push`, `try_push`, `pop`, `try_pop`, `estimated_len`.
- `try_push` restituisce il payload quando non trova un indice libero;
  `try_pop` restituisce `None` quando non trova un indice pubblicato.
- Le scansioni e i retry non hanno un limite fisso di passi. Il nome `try`
  non significa wait-free o durata massima garantita per il worker.
- Operazioni sospese possono trattenere slot privati. La capacità nominale
  torna disponibile quando queste completano; la variante generica bounded
  non eredita automaticamente tutte le garanzie di progresso del ring isolato.
- Il confronto modulare assume snapshot distanti meno di un quarto del range
  delle generazioni: con 32 bit, meno di `2**30` generazioni. Il retry su una
  coppia ambigua non protegge da snapshot sospesi per un intero giro dei tag.
- La capacità ammessa è `[2, 2**32 - 2]`, anche non potenza di due.
- Move e distruzione richiedono assenza di operazioni concorrenti.
- `estimated_len` è solo una stima signed, limitata a `[0, capacity]`; non
  determina pieno/vuoto. La pubblicazione può precedere l'incremento del count.

## Uso

```bash
mojo build --Werror -O3 -I . -DMOSTREAM_NBLFQ=1 \
  Benchmarks/PipelineQueueBenchmark/pipe_mpmc_cooperative_benchmark.mojo \
  -o /tmp/pipe_coop_nblfq
MOSTREAM_HOME="$PWD" /tmp/pipe_coop_nblfq 50000 4 4 1024 0
```

Il backend modifica solo `PipelineQueue` nei comunicatori. Le code ready/wait
dello scheduler restano MPMC e i flag per scegliere backend diversi sono
mutuamente esclusivi. L'output dei benchmark riporta `backend=NBLFQ`.

## Test

```bash
python3 Tests/run_tests.py

mojo build --Werror -O3 -I . \
  -DMOSTREAM_NBLFQ_TEST_PAUSE=1 -DMOSTREAM_NBLFQ_TEST_PAUSE_HINT=1 \
  Tests/Test_Queue/test_nblfq_concurrent.mojo -o /tmp/test_nblfq_paused
timeout 60s /tmp/test_nblfq_paused
```

- `test_nblfq_queue.mojo`: FIFO, pieno/vuoto, payload String, capacità non
  potenza di due, ownership su push fallita, move con dati presenti, capacità
  invalide. Forza inoltre hint arretrati, 1000 cicli con tag a 3 bit, un CAS
  con vecchia generazione e sospensioni deterministiche dei proprietari dei
  payload. Il test con tag piccoli è sequenziale e non copre thread sospesi
  per un intero giro dei tag.
- `test_nblfq_concurrent.mojo`: 82.000 messaggi per suite con identificativi
  univoci e stringhe, verifica esattamente una consegna per ID e integrità
  del payload. Esercita 4P/4C, capacità 2/3/16/1024, API try e bloccanti;
  inoltre 1P/8C e 8P/1C, verificando l'ordine per producer nel caso 8P/1C.
- `MOSTREAM_NBLFQ_TEST_PAUSE` sospende prima di pubblicare il payload e dopo
  averne acquisito l'indice. `MOSTREAM_NBLFQ_TEST_PAUSE_HINT` sospende dopo
  il CAS di inserimento/rimozione e prima dell'aggiornamento dell'hint.
  Entrambi sono disabilitati nelle build normali e nei benchmark.

I controlli di stress e le regressioni non costituiscono una prova formale.

## Confronto cooperativo

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_nblfq.py \
  --elements 50000 --degree 4 --workers 1,2,4,8 --work 0,160 \
  --capacity 1024 --warmups 2 --repetitions 10 \
  --output /tmp/mostream-nblfq-comparison
```

Il runner riusa validazione e statistica della campagna Michael–Scott e
ricompila **tutti e cinque** i backend: MPMC, Padded-FAA, SCQ, Michael–Scott,
NBLFQ. Le 10 ripetizioni bilanciano le cinque posizioni di esecuzione (due
presenze per posizione). I casi sono randomizzati in blocchi e tutti i run
verificano count/checksum, con checksum calcolato indipendentemente in Python.
I risultati precedenti non vengono sovrascritti né mescolati con i nuovi.

Il timer comprende `run_cooperative` e setup dello scheduler. Non si effettuano
altri test o compilazioni durante le misure della campagna. Sono salvati
campioni, statistiche, log e manifest con toolchain, hardware e hash.
L'output deve essere una directory vuota. La campagna e i suoi limiti sono
riportati in [NBLFQ_RESULTS.md](../../Benchmarks/PipelineQueueBenchmark/NBLFQ_RESULTS.md).
