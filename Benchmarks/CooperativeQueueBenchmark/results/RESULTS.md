# Confronto cooperativo: risultati

25,000 messaggi/produttore; 7 ripetizioni per caso e variante.
Scheduler sperimentale round-robin con actor assegnati stabilmente ai worker; nessun parcheggio, work stealing o calcolo applicativo. Non e' una misura della pipeline MoStream completa.

Speedup = tempo Vyukov / tempo variante: sopra 1 vince la variante. IC95% bootstrap percentile dei rapporti accoppiati in log, 4000 ricampionamenti, senza correzione per confronti multipli. Campagna esplorativa; nessuna esclusione di outlier.

p99: media dei p99 delle singole esecuzioni, campionamento ogni 256 messaggi; dal primo tentativo di push al completamento del pop, attese incluse. Non e' il p99 di tutti i messaggi.

| P/C | Worker | Capacita | Batch | Coda | Mmsg/s | Speedup vs Vyukov [IC95%] | vs PaddedFAA | p99 medio (us) |
|---|---:|---:|---:|---|---:|---|---:|---:|
| 4/4 | 1 | 64 | 1 | Vyukov | 6.538 | 1.00 [1.00, 1.00] | 0.95 | 0.52 |
| 4/4 | 1 | 64 | 1 | PaddedFAA | 6.924 | 1.06 [0.99, 1.13] | 1.00 | 0.49 |
| 4/4 | 1 | 64 | 1 | CAS-bounded | 6.184 | 0.94 [0.87, 1.01] | 0.89 | 0.52 |
| 4/4 | 1 | 64 | 1 | FAA-cooperative | 5.954 | 0.91 [0.87, 0.96] | 0.86 | 0.58 |
| 4/4 | 1 | 64 | 8 | Vyukov | 6.641 | 1.00 [1.00, 1.00] | 0.95 | 3.17 |
| 4/4 | 1 | 64 | 8 | PaddedFAA | 6.974 | 1.05 [1.01, 1.10] | 1.00 | 3.01 |
| 4/4 | 1 | 64 | 8 | CAS-bounded | 6.287 | 0.95 [0.87, 1.01] | 0.90 | 3.36 |
| 4/4 | 1 | 64 | 8 | FAA-cooperative | 6.162 | 0.93 [0.87, 0.98] | 0.88 | 3.25 |
| 4/4 | 1 | 1024 | 1 | Vyukov | 6.628 | 1.00 [1.00, 1.00] | 0.97 | 0.52 |
| 4/4 | 1 | 1024 | 1 | PaddedFAA | 6.833 | 1.03 [0.99, 1.07] | 1.00 | 0.51 |
| 4/4 | 1 | 1024 | 1 | CAS-bounded | 6.320 | 0.95 [0.92, 0.99] | 0.92 | 0.53 |
| 4/4 | 1 | 1024 | 1 | FAA-cooperative | 6.062 | 0.91 [0.88, 0.94] | 0.89 | 0.55 |
| 4/4 | 1 | 1024 | 8 | Vyukov | 6.717 | 1.00 [1.00, 1.00] | 0.98 | 3.10 |
| 4/4 | 1 | 1024 | 8 | PaddedFAA | 6.872 | 1.02 [0.99, 1.07] | 1.00 | 3.36 |
| 4/4 | 1 | 1024 | 8 | CAS-bounded | 6.259 | 0.93 [0.88, 0.97] | 0.91 | 3.59 |
| 4/4 | 1 | 1024 | 8 | FAA-cooperative | 5.960 | 0.88 [0.78, 0.95] | 0.86 | 3.29 |
| 4/4 | 2 | 64 | 1 | Vyukov | 1.285 | 1.00 [1.00, 1.00] | 0.73 | 10.58 |
| 4/4 | 2 | 64 | 1 | PaddedFAA | 1.760 | 1.37 [1.31, 1.43] | 1.00 | 5.54 |
| 4/4 | 2 | 64 | 1 | CAS-bounded | 1.620 | 1.26 [1.21, 1.32] | 0.92 | 46.63 |
| 4/4 | 2 | 64 | 1 | FAA-cooperative | 2.250 | 1.75 [1.70, 1.82] | 1.28 | 4.08 |
| 4/4 | 2 | 64 | 8 | Vyukov | 1.853 | 1.00 [1.00, 1.00] | 0.89 | 15.62 |
| 4/4 | 2 | 64 | 8 | PaddedFAA | 2.096 | 1.13 [1.08, 1.20] | 1.00 | 15.72 |
| 4/4 | 2 | 64 | 8 | CAS-bounded | 2.527 | 1.36 [1.28, 1.45] | 1.21 | 35.47 |
| 4/4 | 2 | 64 | 8 | FAA-cooperative | 2.773 | 1.50 [1.48, 1.51] | 1.33 | 10.67 |
| 4/4 | 2 | 1024 | 1 | Vyukov | 1.276 | 1.00 [1.00, 1.00] | 0.72 | 9.27 |
| 4/4 | 2 | 1024 | 1 | PaddedFAA | 1.773 | 1.39 [1.33, 1.45] | 1.00 | 5.39 |
| 4/4 | 2 | 1024 | 1 | CAS-bounded | 1.667 | 1.31 [1.24, 1.37] | 0.94 | 626.10 |
| 4/4 | 2 | 1024 | 1 | FAA-cooperative | 2.265 | 1.78 [1.70, 1.86] | 1.28 | 8.37 |
| 4/4 | 2 | 1024 | 8 | Vyukov | 1.908 | 1.00 [1.00, 1.00] | 0.90 | 17.03 |
| 4/4 | 2 | 1024 | 8 | PaddedFAA | 2.110 | 1.11 [1.08, 1.12] | 1.00 | 14.09 |
| 4/4 | 2 | 1024 | 8 | CAS-bounded | 2.563 | 1.34 [1.30, 1.39] | 1.21 | 424.47 |
| 4/4 | 2 | 1024 | 8 | FAA-cooperative | 2.747 | 1.44 [1.38, 1.48] | 1.30 | 10.65 |
| 4/4 | 4 | 64 | 1 | Vyukov | 1.492 | 1.00 [1.00, 1.00] | 0.91 | 7.62 |
| 4/4 | 4 | 64 | 1 | PaddedFAA | 1.644 | 1.10 [1.06, 1.14] | 1.00 | 6.82 |
| 4/4 | 4 | 64 | 1 | CAS-bounded | 1.476 | 0.99 [0.96, 1.02] | 0.90 | 53.07 |
| 4/4 | 4 | 64 | 1 | FAA-cooperative | 2.917 | 1.95 [1.88, 2.02] | 1.78 | 8.50 |
| 4/4 | 4 | 64 | 8 | Vyukov | 1.526 | 1.00 [1.00, 1.00] | 0.88 | 23.03 |
| 4/4 | 4 | 64 | 8 | PaddedFAA | 1.739 | 1.14 [1.11, 1.17] | 1.00 | 24.67 |
| 4/4 | 4 | 64 | 8 | CAS-bounded | 1.531 | 1.00 [0.97, 1.03] | 0.88 | 57.65 |
| 4/4 | 4 | 64 | 8 | FAA-cooperative | 2.996 | 1.96 [1.89, 2.03] | 1.72 | 9.86 |
| 4/4 | 4 | 1024 | 1 | Vyukov | 1.505 | 1.00 [1.00, 1.00] | 0.89 | 7.37 |
| 4/4 | 4 | 1024 | 1 | PaddedFAA | 1.699 | 1.13 [1.09, 1.16] | 1.00 | 6.35 |
| 4/4 | 4 | 1024 | 1 | CAS-bounded | 1.485 | 0.99 [0.96, 1.01] | 0.87 | 721.37 |
| 4/4 | 4 | 1024 | 1 | FAA-cooperative | 3.008 | 2.00 [1.97, 2.03] | 1.77 | 11.98 |
| 4/4 | 4 | 1024 | 8 | Vyukov | 1.560 | 1.00 [1.00, 1.00] | 0.90 | 24.69 |
| 4/4 | 4 | 1024 | 8 | PaddedFAA | 1.742 | 1.12 [1.08, 1.16] | 1.00 | 22.54 |
| 4/4 | 4 | 1024 | 8 | CAS-bounded | 1.558 | 1.00 [0.95, 1.05] | 0.89 | 698.61 |
| 4/4 | 4 | 1024 | 8 | FAA-cooperative | 3.112 | 1.99 [1.92, 2.07] | 1.79 | 10.58 |
| 8/2 | 1 | 64 | 1 | Vyukov | 4.818 | 1.00 [1.00, 1.00] | 0.95 | 21.48 |
| 8/2 | 1 | 64 | 1 | PaddedFAA | 5.059 | 1.05 [1.01, 1.08] | 1.00 | 20.61 |
| 8/2 | 1 | 64 | 1 | CAS-bounded | 5.518 | 1.15 [1.11, 1.18] | 1.09 | 15.68 |
| 8/2 | 1 | 64 | 1 | FAA-cooperative | 4.674 | 0.97 [0.93, 1.01] | 0.92 | 17.83 |
| 8/2 | 1 | 64 | 8 | Vyukov | 6.411 | 1.00 [1.00, 1.00] | 0.93 | 12.19 |
| 8/2 | 1 | 64 | 8 | PaddedFAA | 6.918 | 1.08 [1.02, 1.13] | 1.00 | 11.14 |
| 8/2 | 1 | 64 | 8 | CAS-bounded | 6.466 | 1.01 [0.97, 1.05] | 0.94 | 10.43 |
| 8/2 | 1 | 64 | 8 | FAA-cooperative | 6.120 | 0.95 [0.92, 0.99] | 0.89 | 13.06 |
| 8/2 | 1 | 1024 | 1 | Vyukov | 4.860 | 1.00 [1.00, 1.00] | 0.94 | 333.13 |
| 8/2 | 1 | 1024 | 1 | PaddedFAA | 5.196 | 1.07 [1.05, 1.09] | 1.00 | 286.55 |
| 8/2 | 1 | 1024 | 1 | CAS-bounded | 5.559 | 1.14 [1.11, 1.18] | 1.07 | 244.34 |
| 8/2 | 1 | 1024 | 1 | FAA-cooperative | 4.724 | 0.97 [0.96, 0.99] | 0.91 | 235.84 |
| 8/2 | 1 | 1024 | 8 | Vyukov | 6.642 | 1.00 [1.00, 1.00] | 0.96 | 190.92 |
| 8/2 | 1 | 1024 | 8 | PaddedFAA | 6.918 | 1.04 [1.01, 1.07] | 1.00 | 174.74 |
| 8/2 | 1 | 1024 | 8 | CAS-bounded | 6.401 | 0.96 [0.93, 0.99] | 0.92 | 181.96 |
| 8/2 | 1 | 1024 | 8 | FAA-cooperative | 5.986 | 0.90 [0.86, 0.94] | 0.86 | 216.55 |
| 8/2 | 2 | 64 | 1 | Vyukov | 1.057 | 1.00 [1.00, 1.00] | 0.93 | 109.89 |
| 8/2 | 2 | 64 | 1 | PaddedFAA | 1.137 | 1.08 [1.04, 1.11] | 1.00 | 100.84 |
| 8/2 | 2 | 64 | 1 | CAS-bounded | 0.883 | 0.84 [0.82, 0.85] | 0.78 | 164.40 |
| 8/2 | 2 | 64 | 1 | FAA-cooperative | 1.278 | 1.21 [1.19, 1.23] | 1.12 | 61.56 |
| 8/2 | 2 | 64 | 8 | Vyukov | 1.656 | 1.00 [1.00, 1.00] | 0.86 | 70.23 |
| 8/2 | 2 | 64 | 8 | PaddedFAA | 1.926 | 1.16 [1.14, 1.19] | 1.00 | 62.36 |
| 8/2 | 2 | 64 | 8 | CAS-bounded | 1.908 | 1.15 [1.11, 1.19] | 0.99 | 71.54 |
| 8/2 | 2 | 64 | 8 | FAA-cooperative | 2.323 | 1.40 [1.38, 1.43] | 1.21 | 34.76 |
| 8/2 | 2 | 1024 | 1 | Vyukov | 1.035 | 1.00 [1.00, 1.00] | 0.90 | 1165.27 |
| 8/2 | 2 | 1024 | 1 | PaddedFAA | 1.152 | 1.11 [1.10, 1.13] | 1.00 | 1061.52 |
| 8/2 | 2 | 1024 | 1 | CAS-bounded | 0.887 | 0.86 [0.83, 0.88] | 0.77 | 1526.20 |
| 8/2 | 2 | 1024 | 1 | FAA-cooperative | 1.309 | 1.26 [1.23, 1.30] | 1.14 | 840.65 |
| 8/2 | 2 | 1024 | 8 | Vyukov | 1.673 | 1.00 [1.00, 1.00] | 0.86 | 696.10 |
| 8/2 | 2 | 1024 | 8 | PaddedFAA | 1.951 | 1.17 [1.12, 1.21] | 1.00 | 624.09 |
| 8/2 | 2 | 1024 | 8 | CAS-bounded | 1.942 | 1.16 [1.12, 1.21] | 0.99 | 626.29 |
| 8/2 | 2 | 1024 | 8 | FAA-cooperative | 2.295 | 1.37 [1.31, 1.42] | 1.18 | 510.54 |
| 8/2 | 4 | 64 | 1 | Vyukov | 1.055 | 1.00 [1.00, 1.00] | 0.90 | 103.40 |
| 8/2 | 4 | 64 | 1 | PaddedFAA | 1.171 | 1.11 [1.08, 1.14] | 1.00 | 97.22 |
| 8/2 | 4 | 64 | 1 | CAS-bounded | 0.927 | 0.88 [0.86, 0.90] | 0.79 | 121.45 |
| 8/2 | 4 | 64 | 1 | FAA-cooperative | 1.557 | 1.48 [1.42, 1.54] | 1.33 | 50.38 |
| 8/2 | 4 | 64 | 8 | Vyukov | 1.813 | 1.00 [1.00, 1.00] | 0.91 | 76.90 |
| 8/2 | 4 | 64 | 8 | PaddedFAA | 1.992 | 1.10 [1.06, 1.13] | 1.00 | 72.43 |
| 8/2 | 4 | 64 | 8 | CAS-bounded | 1.626 | 0.90 [0.85, 0.94] | 0.82 | 77.56 |
| 8/2 | 4 | 64 | 8 | FAA-cooperative | 2.555 | 1.41 [1.37, 1.45] | 1.28 | 30.93 |
| 8/2 | 4 | 1024 | 1 | Vyukov | 1.062 | 1.00 [1.00, 1.00] | 0.91 | 1093.47 |
| 8/2 | 4 | 1024 | 1 | PaddedFAA | 1.164 | 1.10 [1.06, 1.13] | 1.00 | 1032.35 |
| 8/2 | 4 | 1024 | 1 | CAS-bounded | 0.967 | 0.91 [0.88, 0.94] | 0.83 | 1239.16 |
| 8/2 | 4 | 1024 | 1 | FAA-cooperative | 1.592 | 1.50 [1.45, 1.55] | 1.37 | 704.14 |
| 8/2 | 4 | 1024 | 8 | Vyukov | 1.874 | 1.00 [1.00, 1.00] | 0.94 | 666.76 |
| 8/2 | 4 | 1024 | 8 | PaddedFAA | 1.997 | 1.07 [1.04, 1.09] | 1.00 | 612.95 |
| 8/2 | 4 | 1024 | 8 | CAS-bounded | 1.698 | 0.91 [0.86, 0.95] | 0.85 | 743.45 |
| 8/2 | 4 | 1024 | 8 | FAA-cooperative | 2.536 | 1.35 [1.29, 1.40] | 1.27 | 444.09 |

## Interpretazione

Le baseline espongono solo Optional: i loro fallimenti non distinguono collisioni e indisponibilita. Il valore retries=0 delle baseline significa non osservabile, non assenza di CAS falliti. CAS-bounded distingue RETRY da WAIT; entrambi cedono l'attivazione in questo scheduler, quindi qui non si misura il risparmio di parcheggi nel runtime di produzione.

FAA-cooperative elimina i CAS di prenotazione, ma mantiene contesa sui contatori FAA, attese sugli slot e dipendenze dagli actor proprietari dei ticket. Il batch limita i messaggi per attivazione, non prenota un blocco con una sola FAA.

Raw: results.csv. Metriche complete: summary.csv. Ambiente e hash: manifest.json. Le verifiche exact-once sono separate dai tempi riportati.
