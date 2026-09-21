# PaddedFAA sleep(0.0): studio empirico

Questa cartella contiene la relazione PDF e lo script che la genera.

- `genera_pdf.py`: produce il PDF a partire dai CSV aggregati del benchmark.
- `relazione_sleep0.pdf`: risultato finale in PDF, con grafici e spiegazione del confronto.

Il benchmark è il confronto diretto tra:

- `PaddedFAAQueue`: spin fino a 1024 e poi `sleep(0.0)`
- `PaddedFAASpinQueue`: spin continuo senza yield

L'obiettivo è misurare il costo reale del `sleep(0.0)` senza cambiare la semantica della coda, il carico e l'ordine delle esecuzioni.

Per rigenerare il PDF dalla radice del repository:

```bash
python3 Documentazione/PaddedFAA_sleep0/genera_pdf.py
```

Il report usa i dati di benchmark generati in:

- `/tmp/paddedfaa_spin_sweep.csv`
- oppure `Benchmarks/PipelineQueueBenchmark/paddedfaa_spin_results.csv`
