# Documentazione HybridMPMCQueue

Questa cartella contiene la relazione tecnica separata dedicata alla coda
bounded `HybridMPMCQueue` di MoStream.

- `relazione.pdf`: documento finale.
- `relazione.ms`: sorgente modificabile.

La relazione usa come dati sperimentali
`Benchmarks/QueueBenchmark/results_hybrid_thresholds.csv` e descrive le soglie
CAS-to-FAA 1, 2, 4 e 8.

Per rigenerare il PDF dalla radice del repository, senza dipendenze Python
esterne:

```bash
python3 Documentazione/Studio_FAA_vs_MPMC/genera_pdf.py \
  Documentazione/Hybrid_Queue/relazione.ms \
  Documentazione/Hybrid_Queue/relazione.pdf
```

Test di correttezza richiamati nella relazione:

```bash
mkdir -p /tmp/mostream-mojo-cache

MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo build Tests/test_hybrid_queue.mojo -I . \
  -o /tmp/test_hybrid_queue
/tmp/test_hybrid_queue

MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo build Tests/test_hybrid_queue_concurrent.mojo -I . \
  -o /tmp/test_hybrid_queue_concurrent
/tmp/test_hybrid_queue_concurrent
```

I contatori diagnostici dei fallback sono disattivati nella build corrente per
non perturbare il throughput. Un valore stampato pari a zero non indica quindi
che il ramo FAA non sia stato percorso.
