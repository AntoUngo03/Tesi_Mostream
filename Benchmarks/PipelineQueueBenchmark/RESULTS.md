# Risultati MPMC vs Padded-FAA nelle pipeline

Campagna eseguita il 22 luglio 2026 con Mojo `1.0.0b1`, Intel Xeon Gold 5512U,
Linux 5.15, build `-O3`, pinning disabilitato, capacità 1024, 250.000 elementi
per replica source, 3 warm-up e 30 coppie intercalate per configurazione. Non è
stato rimosso alcun campione.

Lo speedup è `tempo_MPMC / tempo_Padded`; l'intervallo è l'IC95% nominale della
media geometrica dei rapporti accoppiati.

| Test | MPMC media | Padded media | Speedup Padded | Esito |
|---|---:|---:|---:|---|
| pipe_1 | 260,34 ms | 243,37 ms | **1,066x [1,024; 1,109]** | Padded +6,6% |
| pipe_2 | 397,82 ms | 383,82 ms | 1,038x [0,995; 1,082] | inconclusivo |
| pipe_3_coop, 1 worker | 813,73 ms | 806,18 ms | 1,009x [0,978; 1,041] | pareggio compatibile |
| pipe_3_coop, 2 worker | 2951,23 ms | 2898,95 ms | 1,019x [0,994; 1,045] | inconclusivo |
| pipe_3_coop, 4 worker | 2778,98 ms | 2709,83 ms | 1,032x [0,978; 1,088] | instabile/inconclusivo |
| pipe_3_coop, 8 worker | 2313,75 ms | 2413,76 ms | 0,940x [0,858; 1,030] | instabile/inconclusivo |

## Conclusione

In questa campagna Padded-FAA migliora in modo statisticamente chiaro soltanto
`pipe_1`, di circa il 6,6%. Gli altri cinque intervalli includono la parità: i
dati non dimostrano che Padded-FAA sia migliore in tutte le pipeline.

Il risultato va interpretato in base al percorso realmente esercitato:

- `pipe_1` e `pipe_2` sono SPSC e spendono parte del tempo nella costruzione e
  nel trasferimento di `String`; il vantaggio non può essere attribuito a FAA
  sotto contesa MPMC;
- nel cooperativo gli actor usano `try_push/try_pop`, che reclamano il ticket
  con CAS anche nella Padded-FAA;
- le nove grandi code `ready/wait` dello scheduler cooperativo restano MPMC-CAS
  in entrambe le build e il loro setup è incluso nel tempo;
- 4 e 8 worker hanno CoV superiore al 10% in almeno un backend; 8 worker è
  chiaramente bimodale. Questi casi richiedono pinning/controllo del runtime
  prima di essere usati come evidenza definitiva;
- `pipe_2` mostra deriva: il vantaggio geometrico passa da circa 1,076x nella
  prima metà a circa 1,001x nella seconda.

Gli smoke test sui file originali sono corretti con entrambe le code:
`test_pipe_1` e `test_pipe_2` producono output identico byte per byte; in
`test_pipe_3_coop` la somma dei tre sink è 1.001.000 in entrambi i casi.

I 360 campioni, le statistiche complete e il manifest riproducibile sono in
`results_raw.csv`, `results_summary.csv` e `run_manifest.json`.
