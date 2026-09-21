# Padico NBLFQ nel runtime cooperativo: risultati

Campagna del **21 settembre 2026**, Mojo `1.0.0 (ed45d567)`, Intel Xeon Gold
5512U, build `--Werror -O3`, pinning disabilitato. Quattro source e quattro sink
condividono una coda dati di capacità 1024, con 50.000 elementi per source:
200.000 trasferimenti per esecuzione. Si variano i worker (1/2/4/8) e il lavoro
per messaggio (0/160 iterazioni xorshift).

Il backend misurato è **NBLFQ a 64 bit con due ring di indici e payload
preallocati**, port della macro `PUK_LFQUEUE_NBLFQ_TYPE` fornita dall'utente.
Non è NBLFQ2 a 128 bit né la macro C che scambia puntatori gestiti esternamente.
La [documentazione del port](../../Documentazione/Padico_NBLFQ/README.md)
spiega algoritmo, provenienza, differenze e assunzioni.

## Campagna

Tutti i **400 campioni misurati** e gli **80 warm-up** hanno superato controllo
di conteggio, checksum indipendente in Python e coerenza del throughput.
Sono stati ricompilati tutti e cinque i backend nella stessa campagna.
I risultati della precedente campagna Michael–Scott non sono stati riutilizzati.

Ogni combinazione backend/worker/lavoro ha 2 warm-up e 10 ripetizioni misurate.
I casi sono randomizzati in blocchi; ogni backend occupa ciascuna delle cinque
posizioni esattamente due volte. Nessun outlier è stato rimosso. Durante le
misure non sono stati avviati altri test o compilazioni della sessione.

## Tempi osservati

Mediane in millisecondi; valori inferiori sono migliori.

| Worker | Lavoro | MPMC | Padded-FAA | SCQ | Michael–Scott | NBLFQ |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 220.51 | 222.29 | 240.76 | 234.19 | 323.92 |
| 2 | 0 | 454.44 | 437.78 | 494.93 | 486.79 | 657.43 |
| 4 | 0 | 429.72 | 437.22 | 460.54 | 440.50 | 545.15 |
| 8 | 0 | 421.73 | 420.33 | 415.87 | 414.05 | 442.85 |
| 1 | 160 | 305.26 | 309.57 | 325.35 | 315.42 | 410.25 |
| 2 | 160 | 489.76 | 478.53 | 589.29 | 524.29 | 656.71 |
| 4 | 160 | 452.65 | 451.47 | 539.32 | 505.57 | 602.76 |
| 8 | 160 | 433.49 | 431.38 | 477.36 | 447.65 | 484.65 |

Lo speedup è la media geometrica dei rapporti accoppiati
`tempo_MPMC / tempo_NBLFQ`. Valori inferiori a 1 indicano NBLFQ più lenta.
Gli IC95% sono calcolati sui log-rapporti, con t di Student e 9 gradi di
libertà. Sono nominali e non corretti per confronti multipli. L'aumento di
tempo è `1 / speedup - 1`, non `1 - speedup`.

| Worker | Lavoro | Speedup NBLFQ / MPMC | IC95% | Tempo aggiuntivo NBLFQ |
|---:|---:|---:|---:|---:|
| 1 | 0 | 0.677 | [0.660; 0.694] | +47.7% |
| 2 | 0 | 0.694 | [0.664; 0.726] | +44.0% |
| 4 | 0 | 0.789 | [0.760; 0.819] | +26.7% |
| 8 | 0 | 0.958 | [0.927; 0.989] | +4.4% |
| 1 | 160 | 0.744 | [0.724; 0.765] | +34.4% |
| 2 | 160 | 0.755 | [0.726; 0.784] | +32.5% |
| 4 | 160 | 0.758 | [0.748; 0.768] | +31.9% |
| 8 | 160 | 0.896 | [0.887; 0.906] | +11.6% |

## Interpretazione

**Questa versione NBLFQ non offre un vantaggio sul workload misurato.**
Tutti gli otto intervalli nominali rispetto a MPMC sono sotto 1: il tempo
aggiuntivo va da circa il 4,4% al 47,7%. NBLFQ ha inoltre la mediana più alta
tra i cinque backend in tutte le otto configurazioni; questa osservazione
sulle mediane non è un test di significatività per ogni coppia di backend.

La penalità rispetto a MPMC si riduce con 8 worker: +4,4% senza lavoro sintetico
e +11,6% con lavoro 160. Il caso 8 worker/lavoro 0 ha un intervallo vicino alla
parità e va letto come evidenza esplorativa. MPMC e Padded-FAA restano
compatibili con la parità in tutte le configurazioni secondo gli IC95%
nominali del runner.

Le scansioni delle celle, i CAS con ordinamento forte e la gestione di due
ring sono costi plausibili del port. Non è stato fatto profiling che attribuisca
la differenza a uno di questi fattori. La versione originale usa puntatori;
questo adattamento deve acquisire e restituire anche lo storage dei payload.
Il risultato non giustifica una conclusione generale contro l'algoritmo C.

Il timer include inizializzazione dello scheduler e `run_cooperative()`.
Le code ready/wait restano MPMC per tutti i backend. Non è un microbenchmark
della sola coda; pinning disabilitato, macchina e carico fissati limitano la
generalizzazione. Il CoV è incluso nel CSV; i test non provano formalmente
correttezza concorrente o progresso con thread sospesi per un giro dei tag.

## Verifiche di correttezza

- **21 test del repository passati**, con compilazione `--Werror`.
- Test NBLFQ diretti `-O3`: FIFO, capacità 2/3/16, String, proprietà del
  payload su fallimento, move con elementi presenti, dimensioni invalide,
  1000 cicli con tag ridotti a 3 bit e hint forzatamente arretrati.
- Regressioni deterministiche: CAS con vecchia generazione, consumer sospeso
  dopo acquisizione dell'indice, producer sospeso prima della pubblicazione.
- Stress: **82.000 payload per suite**, identificativi univoci e stringhe,
  capacità 2/3/16/1024, API bloccanti e try, 4P/4C, 1P/8C, 8P/1C. Controllo
  di consegna esattamente una volta e dell'ordine per producer con un consumer.
- Passano anche le build con pause sul payload e con entrambe le pause
  payload/hint: ritardi dopo il CAS e prima degli aggiornamenti di head/tail.
- Pipeline NBLFQ con String (`pipe_1`, `pipe_2`), grafo cooperativo `2/2/3/3`
  con 1/2/4/8 worker, pipeline standard 4P/4C a capacità 2: count/checksum validi.
- Smoke cooperativo 8P/8C, 8 worker, capacità 2: 16.000 messaggi verificati.
- La compilazione rifiuta la selezione simultanea NBLFQ e SCQ.

## Riproduzione

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_nblfq.py \
  --elements 50000 --degree 4 --workers 1,2,4,8 --work 0,160 \
  --capacity 1024 --warmups 2 --repetitions 10 --seed 20260921 \
  --output /tmp/mostream-nblfq-comparison
```

L'output deve essere una directory vuota. Il runner conserva tutti i campioni,
verifica che i sorgenti non cambino durante il run e segnala esplicitamente
un esito fallito se un controllo o un'esecuzione non riesce.

- [Campioni grezzi](nblfq_results/raw.csv)
- [Statistiche per tutti i backend](nblfq_results/summary.csv)
- [Manifest, comandi, hardware e hash](nblfq_results/manifest.json)
- [Log delle 480 esecuzioni](nblfq_results/runs.log)
- [Sorgente originale allegato](../../Documentazione/Padico_NBLFQ/Puk-nblfq.original.h)
