# Documentazione Rigtorp MPMC Queue

Questa cartella contiene la relazione tecnica dedicata al port Mojo di
`RigtorpMPMCQueue` usato negli esperimenti di MoStream.

- `relazione.pdf`: documento finale in formato PDF.
- `relazione.ms`: sorgente testuale modificabile.

Rigenerazione dalla radice del repository:

```bash
python3 Documentazione/Studio_FAA_vs_MPMC/genera_pdf.py \
  Documentazione/Rigtorp_Queue/relazione.ms \
  Documentazione/Rigtorp_Queue/relazione.pdf
```

Il documento si basa sui seguenti artefatti riproducibili:

- `MoStream/Rigtorp_queue.mojo`;
- `Tests/test_rigtorp_queue.mojo`;
- `Benchmarks/QueueBenchmark/queue_benchmark.mojo`;
- `Benchmarks/QueueBenchmark/run_suite.py`;
- `Benchmarks/QueueBenchmark/results_with_rigtorp.csv`;
- `Benchmarks/QueueBenchmark/results_padded_faa.csv`.

