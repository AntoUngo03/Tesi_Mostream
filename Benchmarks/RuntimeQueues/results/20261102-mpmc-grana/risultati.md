# Farm MPMC al variare della grana — 2 novembre 2026

Stesso benchmark della campagna del 26 ottobre (`farm_mpmc.mojo`, `run_mpmc.py`, non modificati):
Source → N Compute A → N Compute B → Sink, 2N+2 task, la coda centrale con N produttori e N
consumatori. Qui si estende la grana: T = 0, 1, 2, 5, 10, 20, 50 µs totali per input (metà in A,
metà in B), N = 1, 2, 4, 8, 12, capacità 1024, pinning off, 50.000 input, 10 ripetizioni.
700 esecuzioni, tutte con verifica esatta degli output.

```sh
python3 Benchmarks/RuntimeQueues/run_mpmc.py --messages 50000 --workers 1,2,4,8,12 \
  --work-us 0,1,2,5,10,20,50 --capacities 1024 --pinning off --repeats 10 \
  --output Benchmarks/RuntimeQueues/results/20261102-mpmc-grana
python3 Benchmarks/RuntimeQueues/plot_mpmc_grana.py Benchmarks/RuntimeQueues/results/20261102-mpmc-grana
```

Mediane del throughput (milioni di input/s), Vyukov / Rigtorp; tra parentesi la differenza di
Rigtorp e le ripetizioni vinte da Rigtorp su 10.

| T (µs) | N = 1 | N = 2 | N = 4 | N = 8 | N = 12 |
|---:|---|---|---|---|---|
| 0 | 0,89 / 0,83 (−7,2%, 3) | 1,17 / 1,24 (+5,8%, 10) | 1,24 / 1,39 (+11,9%, 10) | 0,87 / 1,17 (+34,7%, 10) | 0,71 / 1,08 (+50,9%, 10) |
| 1 | 0,65 / 0,65 (0,0%, 5) | 0,99 / 1,05 (+5,9%, 10) | 1,34 / 1,38 (+2,4%, 8) | 0,90 / 1,21 (+34,3%, 10) | 0,72 / 1,09 (+51,6%, 10) |
| 2 | 0,49 / 0,51 (+3,3%, 8) | 0,85 / 0,84 (−1,8%, 2) | 1,29 / 1,33 (+3,3%, 9) | 0,93 / 1,28 (+37,7%, 10) | 0,73 / 1,09 (+50,3%, 10) |
| 5 | 0,30 / 0,31 (+1,3%, 8) | 0,54 / 0,54 (−0,5%, 2) | 0,91 / 0,92 (+1,1%, 9) | 1,14 / 1,42 (+24,8%, 10) | 0,79 / 1,16 (+46,8%, 10) |
| 10 | 0,19 / 0,19 (+1,6%, 9) | 0,34 / 0,35 (+0,8%, 8) | 0,60 / 0,59 (−1,1%, 3) | 1,05 / 1,06 (+0,8%, 6) | 1,00 / 1,39 (+38,9%, 10) |
| 20 | 0,11 / 0,11 (+0,6%, 7) | 0,20 / 0,20 (+0,8%, 8) | 0,37 / 0,37 (−1,1%, 1) | 0,64 / 0,64 (−0,8%, 2) | 0,89 / 0,89 (0,0%, 6) |
| 50 | 0,05 / 0,05 (+0,2%, 8) | 0,09 / 0,09 (+0,1%, 5) | 0,17 / 0,17 (−0,2%, 3) | 0,32 / 0,32 (−0,2%, 6) | 0,45 / 0,45 (+0,6%, 6) |

Osservazioni:

- **Le due code diventano indistinguibili quando la grana è abbastanza grossa rispetto a N.**
  Con N = 8 Rigtorp è avanti fino a T = 5 µs (+24,8%) e alla pari da T = 10 µs; con N = 12 è
  avanti fino a T = 10 µs (+38,9%) e alla pari da T = 20 µs. Con N ≤ 4 le differenze sono
  piccole a ogni T (al massimo +11,9%, con N = 4 e T = 0).
- La differenza compare solo quando la farm deve sostenere più di circa 1 milione di input/s:
  in quel regime Vyukov cala oltre N = 4, mentre Rigtorp resta intorno a 1,1–1,4 M input/s.
  Sotto questa soglia il limite è il calcolo e la coda non conta.
- **Scalabilità:** con T ≤ 2 µs il throughput non cresce oltre N = 4, perché si raggiunge il
  tetto di circa 1,3–1,4 M input/s di sorgente, sink e comunicazione. Con grana più grossa la farm
  scala anche oltre: con T = 50 µs passa da 0,046 a 0,45 M input/s da N = 1 a N = 12 (9,8 volte),
  con T = 20 µs di 8,4 volte.
- Nessuna configurazione mostra un vantaggio rilevante di Vyukov: il caso peggiore per Rigtorp è
  −7,2% con N = 1 e T = 0, non significativo (3 ripetizioni su 10).
- La retta ideale del grafico è 2N/T (2N thread, ciascuno T/2 per input). Con T = 20 e 50 µs e
  pochi worker il misurato supera l'ideale fino a circa il 15%: la calibrazione del kernel, fatta
  su un solo thread prima della prova, sovrastima leggermente il costo effettivo di T.

Fonti: raw.csv, summary.csv, metadata.json, campaign.log; grafico mpmc_grana.png/.pdf/.svg.
