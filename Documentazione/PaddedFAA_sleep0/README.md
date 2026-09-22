# PaddedFAA sleep(0.0): studio empirico

Questa cartella contiene la relazione PDF e lo script che la genera. La campagna
del punto 4 comprende 2P/2C, 4P/2C, 8P/2C e 8P/8C, ciascuna OFF/ON,
con 20 ripetizioni accoppiate, 50.000 messaggi per produttore e capacita' 1024.

- `genera_pdf.py`: produce il PDF a partire dai CSV aggregati del benchmark.
- `relazione_sleep0.pdf`: risultato finale in PDF, con grafici e spiegazione del confronto.

Il benchmark è il confronto diretto tra:

- `PaddedFAAQueue`: spin fino a 1024 e poi `sleep(0.0)`
- `PaddedFAASpinQueue`: spin continuo senza yield

Si confrontano le due policy complete, incluso il contatore di attesa. Ogni
coppia usa lo stesso seed di avvio dei worker. L'ordine delle due code alterna
H/S e S/H; le otto configurazioni sono mescolate in ogni blocco. Due coppie di
warm-up per configurazione sono escluse. Si verificano conteggio e checksum
effettivi, senza rimozione degli outlier.

ON significa `taskset` sulle CPU 0-15 (16 core fisici distinti su questa macchina);
OFF eredita le 56 CPU logiche disponibili. Non e' pinning individuale dei worker:
non dimostra il modello con un core dedicato per worker. Una prova preliminare
8P/2C sulle sole CPU 0-7 ha superato il timeout di 120 secondi durante il warm-up
(seed 869193496018642825). Il timeout non e' incluso nelle statistiche; il motivo
del mancato completamento non e' stato diagnosticato. La campagna completa usa
16 CPU ammesse per non avere meno CPU che task applicativi.

Per ripetere la campagna sulla stessa topologia CPU:

```bash
python3 Benchmarks/PipelineQueueBenchmark/run_paddedfaa_spin.py \
  --point4 --messages 50000 --capacity 1024 --repetitions 20 \
  --cpus 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 \
  --output Benchmarks/PipelineQueueBenchmark/paddedfaa_point4.csv
```

Per rigenerare il PDF dalla radice del repository:

```bash
python3 Documentazione/PaddedFAA_sleep0/genera_pdf.py
```

Il report usa esclusivamente dati persistenti in `Benchmarks/PipelineQueueBenchmark/`:

- `paddedfaa_point4.csv`: 160 coppie grezze;
- `paddedfaa_point4.manifest.json`: parametri, hardware, compilatore e hash;
- `paddedfaa_point4.summary.csv`: tempi medi e rapporto geometrico con IC95%.

L'IC95% usa Student sui logaritmi dei rapporti accoppiati `T_spin/T_ibrida`,
con lo stesso helper della campagna NBLFQ. Sotto 1 vince spin; sopra 1 vince
l'ibrida; un intervallo che include 1 e' inconclusivo, non prova equivalenza.
Sono intervalli individuali, senza correzione per confronti multipli.

I vecchi CSV esplorativi e quelli in `/tmp` non vengono uniti ai nuovi dati.
Il generatore rifiuta duplicati, campioni invalidi, matrici incomplete e CSV
il cui hash non coincide con il manifest della campagna completata.
