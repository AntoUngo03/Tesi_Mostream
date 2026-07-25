# Documentazione wCQ

Questa cartella contiene la relazione tecnica dedicata all'implementazione
bounded di wCQ e alla sua valutazione in MoStream.

- `relazione.pdf`: documento finale per la tesi.
- `relazione.ms`: sorgente testuale modificabile.

Rigenerazione dalla radice del repository:

```bash
python3 Documentazione/Studio_FAA_vs_MPMC/genera_pdf.py \
  Documentazione/WCQ/relazione.ms \
  Documentazione/WCQ/relazione.pdf \
  "wCQ bounded - MoStream"
```

I dati numerici provengono esclusivamente da:

- `Benchmarks/QueueBenchmark/results_wcq.csv`;
- 100.000 messaggi per producer;
- 10 ripetizioni per configurazione;
- capacità 16, 1024 e 65536;
- topologie 1P/1C, 2P/2C, 4P/4C, 8P/8C, 1P/8C e 8P/1C.
