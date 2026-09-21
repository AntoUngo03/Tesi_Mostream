# Michael–Scott nel runtime cooperativo: risultati

Campagna del **21 settembre 2026**, Mojo `1.0.0 (ed45d567)`, Intel Xeon Gold
5512U, build `--Werror -O3`, pinning disabilitato. Quattro source e quattro sink
condividono una sola coda dati di capacità 1024; ogni source produce 50.000
messaggi, per 200.000 trasferimenti per run. Si confrontano 1/2/4/8 worker e
0/160 iterazioni xorshift per messaggio. Le code dello scheduler restano MPMC.

Tutti i **384 campioni misurati** e i 64 warm-up hanno superato conteggio,
checksum indipendente e verifica del throughput. Per ciascuna configurazione
ci sono 12 campioni per backend, preceduti da 2 warm-up. L'ordine dei casi è
randomizzato e ogni backend occupa ciascuna delle quattro posizioni esattamente
3 volte nei blocchi misurati. Nessun campione è stato escluso.

## Tempi osservati

Mediane in millisecondi; valori inferiori sono migliori. Lo speedup è invece
la media geometrica dei rapporti **accoppiati** `tempo_MPMC / tempo_MS`, con
IC95% nominale sui log-rapporti: valori inferiori a 1 indicano MS più lenta.
Non coincide necessariamente con il rapporto delle mediane.

| Worker | Lavoro | MPMC ms | Padded-FAA ms | SCQ ms | Michael–Scott ms | Speedup MS / MPMC [IC95%] |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 220.79 | 222.04 | 240.51 | 235.35 | 0.938 [0.929; 0.946] |
| 2 | 0 | 456.64 | 452.86 | 485.06 | 478.32 | 0.954 [0.927; 0.981] |
| 4 | 0 | 430.16 | 430.22 | 455.87 | 438.77 | 0.978 [0.959; 0.997] |
| 8 | 0 | 416.66 | 421.41 | 413.92 | 415.36 | 1.011 [0.981; 1.041] |
| 1 | 160 | 305.61 | 305.51 | 325.25 | 315.46 | 0.966 [0.958; 0.975] |
| 2 | 160 | 484.79 | 477.89 | 557.03 | 529.17 | 0.918 [0.905; 0.932] |
| 4 | 160 | 451.13 | 444.30 | 541.79 | 505.03 | 0.903 [0.893; 0.914] |
| 8 | 160 | 431.46 | 431.79 | 476.93 | 442.96 | 0.971 [0.962; 0.980] |

## Interpretazione

- **Questa variante Michael–Scott non migliora MPMC nel workload misurato.**
  Sette intervalli su otto sono sotto 1: il tempo MS è circa il 2,3–10,7% più
  alto secondo i rapporti geometrici. Il caso 4 worker/lavoro 0 è marginale;
  gli intervalli sono nominali e non corretti per confronti multipli.
- Con 8 worker e lavoro 0, lo speedup MS è 1,011 ma l'intervallo include 1:
  non emerge un vantaggio convincente.
- Padded-FAA e MPMC sono compatibili con la parità in tutte le otto
  configurazioni secondo gli IC95% nominali del runner.
- Michael–Scott ha mediane inferiori a SCQ in sette configurazioni su otto.
  Questo confronto descrittivo non è un test di significatività MS/SCQ.
- Un solo worker è il più veloce per tutti i backend in questi casi. Aumentare
  i worker aggiunge concorrenza e costi dello scheduler; il risultato non
  dimostra che una particolare coda sia la sola causa della scalabilità.

Il pool e il completamento dei due obblighi per nodo aggiungono operazioni
atomiche alla lista Michael–Scott. È una spiegazione plausibile del maggior
costo, non un'attribuzione dimostrata da profiling. La versione qui misurata
include le correzioni di ownership e riuso: non si confronta una variante
non sicura, né si misura l'algoritmo originale con allocazione dinamica.

Il timer include setup dello scheduler e `run_cooperative()`, con pinning
disabilitato. I risultati riguardano questo grafo, queste dimensioni e questa
macchina; non sono misure isolate delle operazioni di coda. Il CoV per backend
è riportato nel CSV. Non sono stati eseguiti benchmark concorrenti ad altri
test o compilazioni avviati durante la sessione.

## Correttezza e integrazione

- Suite del repository: **19 test passati**, compilati con `--Werror`.
- Test diretti MS `-O3`: FIFO, capacità, String, move di una coda popolata,
  dimensioni invalide e interleaving deterministici per consumer sospeso e
  producer con snapshot di un nodo successivamente riusato.
- Stress della coda: 82.000 payload con ID univoci e stringhe per suite,
  capacità 2/3/16/1024, percorsi try e bloccanti, 4P/4C, 1P/8C e 8P/1C.
  Passa anche la build con pause forzate dopo il CAS su head.
- Pipeline cooperativa MPMC: smoke con 8P/8C, 8 worker, capacità 2;
  tutti i campioni della campagna verificano count/checksum.
- Integrazione MS: `pipe_1` e `pipe_2` con String, grafo cooperativo
  `2/2/3/3` con 1/2/4/8 worker e pipeline standard 4P/4C con capacità 2:
  tutti i controlli count/checksum passati. Verificato anche il rifiuto in
  compilazione di flag backend incompatibili.

Il dettaglio delle correzioni, il contratto sul riuso e l'assunzione di non
wrap dei tag a 32 bit sono nella
[documentazione della coda](../../Documentazione/Michael_Scott/README.md).
I test non costituiscono una prova formale di correttezza concorrente.

## Riproduzione e dati

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_michael_scott.py \
  --elements 50000 --degree 4 --workers 1,2,4,8 --work 0,160 \
  --capacity 1024 --warmups 2 --repetitions 12 --seed 20260921 \
  --output /tmp/mostream-ms-comparison
```

- [Campioni grezzi](michael_scott_results/raw.csv)
- [Statistiche e intervalli](michael_scott_results/summary.csv)
- [Manifest: toolchain, hardware, comandi e hash](michael_scott_results/manifest.json)
- [Log completo delle esecuzioni](michael_scott_results/runs.log)

Il runner rifiuta directory di output non vuote e registra un esito fallito
se un run non supera i controlli o se i sorgenti cambiano durante la campagna.
