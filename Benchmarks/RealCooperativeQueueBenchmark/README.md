# FAA nella pipeline cooperativa reale

Questa campagna usa `Pipeline.run_cooperative` e non lo scheduler round-robin
del primo esperimento. Il carico attraversa tre stage: source, transform, sink.
La topologia predefinita e' 4/4/4 actor, con uno oppure quattro worker.

## Esito della campagna salvata

Con quattro worker e batch 1, la FAA non mostra un vantaggio convincente:
gli intervalli dei rapporti con Vyukov includono 1 oppure indicano una lieve
regressione. Il vantaggio vicino a 2x del precedente microbenchmark non si
trasferisce automaticamente alla pipeline.

Con quattro worker e batch 8, il throughput di esecuzione e' invece:

| Work nel transform | Vyukov Mmsg/s | PaddedFAA Mmsg/s | FAA Mmsg/s | FAA / Vyukov [IC95%] |
|---:|---:|---:|---:|---|
| 0 | 0.715 | 0.692 | 1.094 | 1.53 [1.49, 1.57] |
| 256 | 0.581 | 0.590 | 0.858 | 1.48 [1.45, 1.51] |
| 2048 | 0.368 | 0.364 | 0.394 | 1.07 [1.04, 1.10] |

Il vantaggio resta anche contro PaddedFAA-broadcast, quindi non e' spiegato
soltanto dal cambiamento dei risvegli: rispettivamente 1.50x, 1.40x e 1.06x.
Con un solo worker la FAA e' piu' lenta in tutte le configurazioni misurate.

Il guadagno sulla durata complessiva e' piu' piccolo per queste pipeline brevi:
nel caso work=0, quattro worker e batch 8, `run_cooperative` passa da 215.23 ms
(Vyukov) a 196.22 ms (FAA), circa il 9% di tempo in meno. La sola esecuzione
passa da 55.96 a 36.59 ms; il resto e' soprattutto inizializzazione/distruzione
del runtime. La latenza p99 campionata scende da circa 1506 a 973 us nello
stesso caso. Non sono garanzie per altre topologie o carichi applicativi.

## Confronto

| Variante | Coda dati | Parcheggio e risvegli |
|---|---|---|
| Vyukov | MPMC originale, CAS nei try | wait queue originali |
| PaddedFAA | PaddedFAA originale, CAS nei try | wait queue originali |
| PaddedFAA-broadcast | PaddedFAA originale, CAS nei try | scansione degli actor bloccati sul collegamento |
| FAA-cooperative | ticket FAA conservati nell'actor | stessa scansione del controllo broadcast |

La ready queue rimane la MPMC originale in tutte le varianti. Gli actor possono
migrare fra worker. Un actor bloccato non viene riprovato continuamente: entra
nello stato BLOCKED_INPUT/OUTPUT e attende una notifica, salvo il ricontrollo
immediato che evita di perdere un evento durante il parcheggio.

Con FAA non basta risvegliare un waiter arbitrario: ogni ticket appartiene a un
actor preciso. La prima integrazione risveglia tutti gli actor bloccati nella
direzione interessata. La scansione usa descrittori immutabili e stati atomici,
senza leggere ticket o payload che un altro worker potrebbe modificare.
Fence sequenzialmente consistenti separano pubblicazione/scansione e
registrazione/ricontrollo. Il costo di questa politica e' incluso nei tempi.

Il confronto con PaddedFAA-broadcast isola meglio le prenotazioni FAA a parita'
di risveglio. Quello con le due baseline confronta l'intera integrazione nel
runtime: non va attribuita alla sola istruzione FAA qualsiasi differenza.

## Modifiche al runtime

- `MOSTREAM_COOPERATIVE_FAA=1` seleziona il nuovo backend.
- L'actor conserva `PushOperation` e `PopOperation` tra attivazioni e parcheggi.
- Il communicator traduce la chiusura definitiva della FAA in EOS solo per
  letture terminali; i ticket validi ancora pendenti vengono completati.
- `run_cooperative(workers, batch_size=1)` permette batch identici per ogni coda.
  Le notifiche tengono conto del lavoro completato nei passi precedenti del batch.
- I comunicatori del runtime cooperativo vengono distrutti dopo il join di tutti
  i worker. Questo evita che una notifica di fine actor acceda al communicator
  gia' distrutto dall'ultimo consumer. Vale anche per le baseline.
- Pipeline espone durata dell'esecuzione cooperativa, attivazioni e ingressi nei
  percorsi di parcheggio. I contatori sono locali ai worker e aggregati al join.

La politica predefinita resta Vyukov con batch 1. Il runtime standard mantiene
la propria gestione dei comunicatori. La nuova FAA resta sperimentale: nessun
abbandono/cancellazione di ticket e nessuna gestione dell'overflow UInt64.
Gli stage cooperativi supportati restano SOURCE, TRANSFORM e SINK.

## Misure

La matrice predefinita include capacita' 1024, batch 1/8, work 0/256/2048.
Work controlla una catena di operazioni UInt64 dipendenti nel transform. Il
checksum verificato nel sink rende osservabile il risultato del calcolo.
I worker sono fissati a core fisici distinti, selezionati entro l'affinita'
consentita; gli ID effettivi sono nel manifest.

Il timer `runtime_ms` include `Scheduler.start`: dispatch/join, ready queue,
calcolo degli stage, prenotazioni, parcheggio, ricontrolli, risvegli e chiusura.
`total_ms` comprende l'intera chiamata `run_cooperative`, incluse allocazione e
distruzione delle grandi code interne originali. La costruzione di Pipeline,
che inizializza anche Python e il pinning, precede entrambi i timer. Non si
eliminano le wait queue inutilizzate nelle varianti broadcast, per mantenere
comune il costo di inizializzazione del runtime.

Si misura un messaggio completato all'uscita della pipeline, non ogni singolo
trasferimento intermedio. La latenza campionata parte dalla creazione nella
source e termina nel sink: include attese, scheduling e calcolo degli stage.
Il campionamento riguarda un ID ogni 256; il p99 riportato e' la media dei p99
delle singole esecuzioni, non il percentile di tutti i messaggi.

Il runner esegue prima 60 casi di verifica separati: exact-once per ID, input
vuoti, piu' waiter che slot e transform che elimina tutti o parte dei messaggi.
Poi esegue un warm-up per caso/variante e sette blocchi misurati, randomizzando
l'ordine dei casi e delle varianti. Ogni esecuzione verifica conteggio, checksum,
campioni di latenza e assenza di payload/ticket pendenti negli actor terminati.

I timeout/fallimenti interrompono la campagna; non diventano campioni scartati.
Le misure sono scritte progressivamente nel CSV. Il manifest completo e la
relazione vengono prodotti solo dopo tutte le esecuzioni riuscite.

## Riproduzione

Dalla radice del repository, con Mojo 1.0 e Python/matplotlib:

```bash
python3 Benchmarks/RealCooperativeQueueBenchmark/run.py
```

Per una configurazione diversa, usare una cartella di output distinta:

```bash
python3 Benchmarks/RealCooperativeQueueBenchmark/run.py \
  --topology 8:2:2 --workers 2,4 --capacities 64,1024 \
  --batches 1,8 --work 0,256 --repetitions 7 \
  --output Benchmarks/RealCooperativeQueueBenchmark/results_asymmetric
```

Per usare il backend in una propria pipeline:

```bash
MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache mojo build --Werror -O3 -I . \
  -DMOSTREAM_COOPERATIVE_FAA=1 Tests/test_pipe_3_coop.mojo \
  -o /tmp/pipeline-faa
MOSTREAM_HOME="$PWD" /tmp/pipeline-faa 4
```

## Artefatti

- [Relazione tecnica in PDF](../../Documentazione/CooperativeFAA/relazione_faa_cooperativa.pdf)
- [Risultati e intervalli](results/RESULTS.md)
- [Grafico](results/comparison.png)
- [Misure grezze](results/results.csv)
- [Metriche complete](results/summary.csv)
- [Hardware, pinning, comandi e hash](results/manifest.json)
- [Carico e verifiche](../../Tests/test_cooperative_pipeline.mojo)

Il [precedente microbenchmark](../CooperativeQueueBenchmark/README.md) rimane
una campagna storica separata: i suoi numeri non includono questi costi del runtime.
