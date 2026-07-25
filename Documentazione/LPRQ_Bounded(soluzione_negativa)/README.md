# Esperimento Bounded LPRQ-inspired

## Documenti

- `relazione.pdf`: relazione tecnica completa in formato PDF;
- `relazione.ms`: sorgente testuale modificabile del PDF.

## Risultato in breve

È stata implementata una coda MPMC con memoria e capacità sempre limitate,
ispirata alle celle indicizzate della Portable Ring Queue (PRQ). Il prototipo è
corretto nei test eseguiti, ma **non è la LPRQ pubblicata** e non ne conserva la
proprietà lock-free.

Nelle 18 configurazioni misurate `BoundedLPRQInspired` non ha ottenuto vittorie.
La migliore coda di ogni configurazione è risultata fra 2,50 e 5,75 volte più
veloce, con rapporto medio geometrico 3,63x. Padded-FAA ha vinto 14 casi, FAA 2
e Rigtorp 2.

Questo è un risultato negativo utile per la tesi: **non dimostra che LPRQ sia
lenta**. Mostra che forzare il suo segmento PRQ in un unico ring bounded e
riusabile elimina proprio il meccanismo con cui LPRQ gestisce i ticket saltati.

## LPRQ originale e requisito bounded

La LPRQ di Raed Romanov e Nikita Koval è una lista dinamica di segmenti PRQ.
Quando un producer supera il ring, il segmento viene chiuso definitivamente e
ne viene collegato uno nuovo. Il bit `UNSAFE` può restare permanente perché il
segmento chiuso verrà drenato e poi abbandonato. L'algoritmo completo è quindi
unbounded rispetto al numero di segmenti allocabili.

Riutilizzare direttamente un singolo PRQ non è sicuro. Un'operazione sospesa di
una generazione precedente potrebbe riprendere dopo il reset e pubblicare nella
generazione nuova (problema ABA); inoltre un `UNSAFE` permanente finirebbe per
rendere inutilizzabile il ring. Una variante più fedele richiederebbe un pool
fisso di segmenti, generation tag e un protocollo sicuro di reclamation.

## Implementazione realizzata

Il file `MoStream/Bounded_LPRQ.mojo` implementa quindi una riduzione prudente a
turno esatto, chiamata `BoundedLPRQInspired`:

- un solo array preallocato, senza allocazioni dopo il costruttore;
- capacità potenza di due, almeno 2;
- ticket logici monotoni per `head` e `tail`, su cache line separate;
- una cella fisica per `ticket & (capacity - 1)`;
- assert compile-time che stride delle celle e wrapper dei contatori siano
  multipli esatti di una cache line da 64 byte per il tipo `T` scelto;
- indice/epoca atomico per impedire il riuso nella generazione sbagliata;
- stati `EMPTY`, `RESERVED`, `WRITING`, `FULL` e `READING`;
- token derivato dal ticket nello stesso word atomico dello stato;
- pubblicazione `release` del payload e lettura `acquire`;
- gate CAS che non consente a `tail - head` di superare la capacità.

`push` attende quando il ring è pieno; `try_push` restituisce al chiamante il
valore non inserito. `pop` attende quando è vuoto; `try_pop` può restituire
`None`. Le due operazioni `try_*` sono intenzionalmente weak: un CAS perso o un
producer ritardato sul ticket precedente può causare un fallimento temporaneo.
Quindi `None` non è una prova linearizzabile che la coda astratta sia vuota. Le
API bloccanti e il percorso cooperativo usato nei test riprovano il fallimento.
Dopo che un ticket è stato prenotato l'operazione deve completarlo: un producer
sospeso può fermare i consumer sul ticket FIFO successivo.

Di conseguenza questa `try_pop` non è un rimpiazzo diretto per un componente
che interpreta immediatamente `None` come end-of-stream: in quel caso serve un
segnale di chiusura separato e occorre continuare il polling, oppure un risultato
tri-state (`Item`, `Empty`, `Pending/Contended`).

Gli stati `WRITING` e `READING` sono necessari perché in Mojo un payload
generico `Optional[T]` non è contenuto nella stessa parola atomica del token.
Evitano che due thread accedano contemporaneamente al payload non atomico.

La coda è bounded in entrambi i significati richiesti:

1. memoria fisica fissa: esistono esattamente `capacity` celle;
2. occupazione logica limitata: non vengono prenotati più di `capacity` ticket
   non consumati.

Non è però lock-free: l'attesa del turno esatto introduce head-of-line
blocking. Inoltre move e distruzione sono sicuri soltanto a coda quiescente e
il token nello stato si ripete dopo 2^61 ticket (l'indice completo offre
un'ulteriore guardia, ma il wrap non è formalmente supportato). Per questo il
nome contiene `Inspired` e i risultati non devono essere presentati come
benchmark della LPRQ originale.

## Difetto Mojo individuato durante lo sviluppo

Una prima versione usava un helper `publish_claimed(mut self, ...)`. Con Mojo
1.0.0b1 il relativo lowering effettuava copy-in/copy-out dell'intera struct e
poteva riscrivere `head` e `tail` con snapshot obsoleti. Il sintomo era la
prenotazione apparente dello stesso ticket da parte di due thread.

La correzione definitiva rende l'helper `@staticmethod` e gli passa soltanto
puntatore alle celle, capacità, mask, ticket e valore. L'IR LLVM è stato
controllato: nel percorso concorrente non rimangono load/store aggregati della
coda.

## Verifica di correttezza

Sono stati eseguiti:

- test FIFO, pieno/vuoto e 10.000 generazioni sequenziali;
- test 4P/4C con API bloccante, 100.000 messaggi per producer;
- test 4P/4C con `try_push`/`try_pop`, 100.000 messaggi per producer;
- entrambi i test concorrenti con capacità 2, 4, 8 e 16;
- 20 esecuzioni concorrenti aggiuntive sui binari compilati;
- validazione di count, checksum, coda vuota e una sola occorrenza per ciascun
  ID mediante 400.000 contatori atomici per-ID;
- controllo del codice LLVM generato per escludere il copy-in/copy-out.

Tutti questi controlli sono passati. Non costituiscono una prova formale di
linearizzabilità; per una tesi è consigliabile aggiungere model checking o uno
stress harness con storie concorrenti e verifica offline.

## Metodo del benchmark

- data: 22 luglio 2026;
- Mojo 1.0.0b1, Linux x86-64;
- Intel Xeon Gold 5512U, 28 core / 56 thread hardware, singolo nodo NUMA;
- 100.000 messaggi per producer;
- 10 ripetizioni per configurazione;
- topologie 1P/1C, 2P/2C, 4P/4C, 8P/8C, 1P/8C e 8P/1C;
- capacità 16, 1024 e 65536;
- ordine delle implementazioni ruotato fra le ripetizioni;
- count e checksum verificati in ogni esecuzione;
- media, deviazione standard campionaria e intervallo di confidenza 95%.

La macchina non è stata isolata e i thread non sono stati pinnati. I numeri
sono quindi un esperimento comparativo locale, non risultati definitivi da
pubblicare senza una replica su host controllato.

## Risultati bilanciati, capacità 1024

Throughput medio in milioni di messaggi al secondo (Mmsg/s); `±` indica il
semi-intervallo di confidenza al 95%.

| Thread | CAS | FAA | Rigtorp | Padded-FAA | Migliore Hybrid | BLPRQ | Vincitore / BLPRQ |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1P/1C | 5,023 ± 0,439 | 6,690 ± 0,210 | 4,410 ± 0,194 | 4,993 ± 0,095 | 6,367 ± 0,076 | 2,219 ± 0,031 | FAA, 3,01x |
| 2P/2C | 4,528 ± 0,548 | 7,237 ± 0,393 | 7,880 ± 0,038 | 9,085 ± 0,015 | 5,746 ± 0,191 | 3,315 ± 0,009 | Padded-FAA, 2,74x |
| 4P/4C | 3,841 ± 0,184 | 8,692 ± 0,073 | 8,645 ± 0,110 | 10,842 ± 0,350 | 7,132 ± 0,156 | 3,012 ± 0,066 | Padded-FAA, 3,60x |
| 8P/8C | 2,530 ± 0,102 | 9,416 ± 0,293 | 8,659 ± 0,097 | 10,588 ± 0,597 | 6,678 ± 0,482 | 2,159 ± 0,034 | Padded-FAA, 4,90x |

Sull'intera matrice:

- Padded-FAA: 14 vittorie su 18;
- FAA: 2 vittorie su 18;
- Rigtorp: 2 vittorie su 18;
- BLPRQ, CAS e tutte le Hybrid: 0 vittorie.

Il rapporto medio geometrico di ciascuna implementazione rispetto a BLPRQ è:
CAS 1,53x, FAA 2,69x, Rigtorp 2,91x, Padded-FAA 3,46x, Hybrid-1 2,43x,
Hybrid-2 2,17x, Hybrid-4 1,80x e Hybrid-8 1,65x.

## Interpretazione

I costi principali della riduzione bounded sono:

1. LPRQ usa FAA e può tollerare buchi chiudendo il segmento; il prototipo deve
   impedire la sovraprenotazione del ring con CAS.
2. Ogni trasferimento attraversa più metadati e transizioni atomiche rispetto
   a FAA/Padded-FAA: ticket, stato, epoca, `WRITING`, `FULL`, `READING` e reset.
3. Il payload generico separato impedisce la rappresentazione compatta
   token-puntatore usata dall'implementazione C++ di riferimento.
4. Il turno FIFO esatto elimina gli skip di PRQ e introduce head-of-line
   blocking.
5. Con più thread, cache coherence e CAS retry dominano il lavoro utile.

Per MoStream, Padded-FAA resta la scelta generale più forte in questa matrice;
Rigtorp è molto competitiva nei carichi sbilanciati e FAA nel caso semplice
1P/1C. La coda bounded-LPRQ è utile come prototipo e risultato negativo, non
come sostituzione prestazionale delle code già implementate.

## Riproduzione

```bash
MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo run -I . Tests/test_bounded_lprq.mojo

MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo run -I . Tests/test_bounded_lprq_concurrent.mojo

MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo run -I . Tests/test_bounded_lprq_try_concurrent.mojo

python3 Benchmarks/QueueBenchmark/run_suite.py \
  100000 10 Benchmarks/QueueBenchmark/results_bounded_lprq.csv

python3 Benchmarks/QueueBenchmark/analyze_lprq_results.py \
  Benchmarks/QueueBenchmark/results_bounded_lprq.csv
```

## Artefatti

- implementazione: `MoStream/Bounded_LPRQ.mojo`;
- test: `Tests/test_bounded_lprq*.mojo`;
- benchmark: `Benchmarks/QueueBenchmark/queue_benchmark.mojo`;
- orchestrazione/statistica: `Benchmarks/QueueBenchmark/run_suite.py`;
- dati grezzi aggregati: `Benchmarks/QueueBenchmark/results_bounded_lprq.csv`;
- riepilogo: `Benchmarks/QueueBenchmark/analyze_lprq_results.py`.

## Fonti

- R. Romanov e N. Koval, *The State-of-the-Art LCRQ Concurrent Queue
  Algorithm Does NOT Require CAS2*, PPoPP 2023, pp. 14–26,
  DOI 10.1145/3572848.3577485:
  <https://nikitakoval.org/publications/ppopp23-lprq.pdf>
- Artifact ufficiale degli autori, comprendente `LPRQueue.hpp` e `PRQueue.hpp`:
  <https://zenodo.org/records/7337237>
