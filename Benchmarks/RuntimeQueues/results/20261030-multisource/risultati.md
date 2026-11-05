# Farm con S sorgenti — 30 ottobre 2026

Mojo 1.1.0, runtime standard, 100.000 input, capacità 1024, 10 ripetizioni per
configurazione, pinning disabilitato. 960 esecuzioni; ogni output è stato
verificato. S = sorgenti, N = worker Compute, T = costo obiettivo per input.
Valori: mediane del throughput in milioni di input/s; "vinte" = ripetizioni in
cui Rigtorp supera Vyukov; p = test del segno bilaterale sulle 10 coppie.

| T (µs) | N | S=1 V / R | S=2 V / R | S=4 V / R | S=8 V / R | Rigtorp con S=8 |
|---:|---:|---|---|---|---|---:|
| 0 | 1 | 0,86 / 0,83 | 0,86 / 0,81 | 0,71 / 0,79 | 0,50 / 0,80 | +61,7% (10/10) |
| 0 | 2 | 1,20 / 1,25 | 1,11 / 1,20 | 0,95 / 1,18 | 0,69 / 1,18 | +71,6% (10/10) |
| 0 | 4 | 1,50 / 1,43 | 1,36 / 1,43 | 1,11 / 1,40 | 0,86 / 1,44 | +66,7% (10/10) |
| 0 | 8 | 1,25 / 1,21 | 1,15 / 1,21 | 1,02 / 1,18 | 0,88 / 1,20 | +35,9% (10/10) |
| 2 | 1 | 0,38 / 0,38 | 0,37 / 0,38 | 0,37 / 0,38 | 0,36 / 0,38 | +5,5% (8/10) |
| 2 | 2 | 0,71 / 0,71 | 0,70 / 0,70 | 0,67 / 0,70 | 0,55 / 0,69 | +26,8% (10/10) |
| 2 | 4 | 1,13 / 1,15 | 1,09 / 1,13 | 0,96 / 1,14 | 0,77 / 1,13 | +46,2% (10/10) |
| 2 | 8 | 1,48 / 1,39 | 1,31 / 1,38 | 1,11 / 1,43 | 0,91 / 1,44 | +57,3% (10/10) |
| 5 | 1 | 0,20 / 0,21 | 0,20 / 0,21 | 0,20 / 0,21 | 0,20 / 0,21 | +4,1% (9/10) |
| 5 | 2 | 0,39 / 0,39 | 0,39 / 0,39 | 0,38 / 0,39 | 0,37 / 0,39 | +4,5% (10/10) |
| 5 | 4 | 0,71 / 0,72 | 0,71 / 0,72 | 0,69 / 0,71 | 0,60 / 0,71 | +17,6% (10/10) |
| 5 | 8 | 1,19 / 1,20 | 1,16 / 1,19 | 1,06 / 1,18 | 0,86 / 1,18 | +38,2% (10/10) |

Con S = 1 (topologia di farm.mojo) le differenze vanno da −6,1% a +3,5%: le
uniche significative sono N=2, T=0 (+3,5%, 9/10 per Rigtorp), N=4, T=0 (−4,4%,
1/10) e N=8, T=2 (−6,1%, 1/10).

Aumentando le sorgenti il throughput di Rigtorp resta quasi costante, mentre
quello di Vyukov cala: per esempio con N = 4 e T = 0 passa da 1,50 a 0,86 M
input/s da S = 1 a S = 8, contro 1,43 e 1,44 di Rigtorp. Con S = 4 e S = 8
Rigtorp vince 10 ripetizioni su 10 in 20 configurazioni su 24. Il calo di Vyukov
è più forte quando la coda è più sollecitata (T piccolo o N grande); con T = 5 e
N = 1, dove il limite è il calcolo di un solo worker, le code sono quasi alla pari.

Fonti: raw.csv, summary.csv, metadata.json e campaign.log in questa cartella;
grafico multisource_throughput.png/.pdf/.svg.
