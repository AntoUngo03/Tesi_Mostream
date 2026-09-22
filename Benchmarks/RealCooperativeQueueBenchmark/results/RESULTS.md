# Pipeline cooperativa reale: risultati

Topologia source/transform/sink: 4:4:4. 10,000 messaggi per source; 7 ripetizioni. Worker fissati a core fisici distinti.

Sono misure di Pipeline.run_cooperative, con ready queue MPMC, migrazione degli actor, parcheggio, risvegli, MessageWrapper, stage di calcolo e chiusura EOS. Work e' il numero di iterazioni dipendenti di mixing UInt64 per messaggio nel transform.

Runtime misura scheduler.start: dispatch/join, elaborazione, notifiche e metriche locali. Totale misura run_cooperative e comprende anche costruzione/distruzione delle code del runtime e dei comunicatori. L'inizializzazione di Pipeline (incluso Python) precede entrambi. Il runtime conserva le grandi wait queue originali anche nei backend broadcast.

PaddedFAA-broadcast e' il controllo: stessa coda PaddedFAA normale, ma stessa politica di risveglio della FAA. Permette di distinguere i cambiamenti della coda da quelli dei risvegli.

Speedup = tempo baseline / tempo variante. IC95% bootstrap percentile su rapporti accoppiati in log (4000 ricampionamenti); intervalli individuali, senza correzione per confronti multipli. Nessun outlier rimosso. p99 e' la media dei p99 campionati di ogni esecuzione (un messaggio ogni 256), dalla source al sink.

| Worker | Capacita | Batch | Work | Coda | Mmsg/s | Runtime ms | Totale ms | vs Vyukov [IC95%] | vs PaddedFAA | p99 us |
|---:|---:|---:|---:|---|---:|---:|---:|---|---:|---:|
| 1 | 1024 | 1 | 0 | Vyukov | 0.667 | 59.94 | 219.17 | 1.00 [1.00, 1.00] | 1.00 | 8.90 |
| 1 | 1024 | 1 | 0 | PaddedFAA | 0.665 | 60.12 | 218.33 | 1.00 [0.99, 1.01] | 1.00 | 7.59 |
| 1 | 1024 | 1 | 0 | PaddedFAA-broadcast | 0.672 | 59.50 | 219.44 | 1.01 [0.99, 1.02] | 1.01 | 7.18 |
| 1 | 1024 | 1 | 0 | FAA-cooperative | 0.624 | 64.11 | 223.25 | 0.93 [0.92, 0.95] | 0.94 | 7.53 |
| 1 | 1024 | 1 | 256 | Vyukov | 0.443 | 90.35 | 248.99 | 1.00 [1.00, 1.00] | 0.99 | 9.71 |
| 1 | 1024 | 1 | 256 | PaddedFAA | 0.447 | 89.54 | 248.01 | 1.01 [1.00, 1.02] | 1.00 | 10.96 |
| 1 | 1024 | 1 | 256 | PaddedFAA-broadcast | 0.449 | 89.17 | 249.00 | 1.01 [1.01, 1.02] | 1.00 | 9.50 |
| 1 | 1024 | 1 | 256 | FAA-cooperative | 0.429 | 93.20 | 251.06 | 0.97 [0.96, 0.98] | 0.96 | 10.79 |
| 1 | 1024 | 1 | 2048 | Vyukov | 0.152 | 262.59 | 422.00 | 1.00 [1.00, 1.00] | 1.00 | 34.60 |
| 1 | 1024 | 1 | 2048 | PaddedFAA | 0.152 | 262.34 | 420.89 | 1.00 [1.00, 1.01] | 1.00 | 34.62 |
| 1 | 1024 | 1 | 2048 | PaddedFAA-broadcast | 0.153 | 261.05 | 421.62 | 1.01 [1.00, 1.01] | 1.00 | 37.17 |
| 1 | 1024 | 1 | 2048 | FAA-cooperative | 0.150 | 265.91 | 426.77 | 0.99 [0.98, 0.99] | 0.99 | 35.19 |
| 1 | 1024 | 8 | 0 | Vyukov | 0.967 | 41.38 | 199.84 | 1.00 [1.00, 1.00] | 0.99 | 39.54 |
| 1 | 1024 | 8 | 0 | PaddedFAA | 0.976 | 41.00 | 201.07 | 1.01 [1.00, 1.02] | 1.00 | 38.09 |
| 1 | 1024 | 8 | 0 | PaddedFAA-broadcast | 0.985 | 40.63 | 198.84 | 1.02 [1.01, 1.03] | 1.01 | 35.25 |
| 1 | 1024 | 8 | 0 | FAA-cooperative | 0.854 | 46.87 | 206.15 | 0.88 [0.87, 0.89] | 0.87 | 41.41 |
| 1 | 1024 | 8 | 256 | Vyukov | 0.529 | 75.59 | 236.43 | 1.00 [1.00, 1.00] | 0.99 | 80.38 |
| 1 | 1024 | 8 | 256 | PaddedFAA | 0.534 | 74.96 | 233.53 | 1.01 [1.00, 1.01] | 1.00 | 58.16 |
| 1 | 1024 | 8 | 256 | PaddedFAA-broadcast | 0.534 | 74.86 | 235.06 | 1.01 [1.00, 1.02] | 1.00 | 67.28 |
| 1 | 1024 | 8 | 256 | FAA-cooperative | 0.498 | 80.24 | 240.60 | 0.94 [0.94, 0.95] | 0.93 | 79.11 |
| 1 | 1024 | 8 | 2048 | Vyukov | 0.160 | 249.61 | 408.66 | 1.00 [1.00, 1.00] | 1.00 | 261.08 |
| 1 | 1024 | 8 | 2048 | PaddedFAA | 0.160 | 249.59 | 408.54 | 1.00 [1.00, 1.00] | 1.00 | 261.68 |
| 1 | 1024 | 8 | 2048 | PaddedFAA-broadcast | 0.160 | 249.51 | 408.04 | 1.00 [1.00, 1.00] | 1.00 | 259.31 |
| 1 | 1024 | 8 | 2048 | FAA-cooperative | 0.158 | 253.40 | 410.68 | 0.99 [0.98, 0.99] | 0.98 | 263.66 |
| 4 | 1024 | 1 | 0 | Vyukov | 0.288 | 138.99 | 298.35 | 1.00 [1.00, 1.00] | 1.01 | 3857.89 |
| 4 | 1024 | 1 | 0 | PaddedFAA | 0.286 | 140.25 | 298.94 | 0.99 [0.96, 1.02] | 1.00 | 3893.17 |
| 4 | 1024 | 1 | 0 | PaddedFAA-broadcast | 0.292 | 137.51 | 296.74 | 1.01 [0.98, 1.05] | 1.02 | 3770.92 |
| 4 | 1024 | 1 | 0 | FAA-cooperative | 0.285 | 141.13 | 298.56 | 0.99 [0.92, 1.05] | 1.00 | 3952.98 |
| 4 | 1024 | 1 | 256 | Vyukov | 0.266 | 150.66 | 309.72 | 1.00 [1.00, 1.00] | 1.01 | 4257.17 |
| 4 | 1024 | 1 | 256 | PaddedFAA | 0.263 | 152.18 | 311.19 | 0.99 [0.96, 1.03] | 1.00 | 4301.22 |
| 4 | 1024 | 1 | 256 | PaddedFAA-broadcast | 0.263 | 152.10 | 311.58 | 0.99 [0.96, 1.03] | 1.00 | 4254.58 |
| 4 | 1024 | 1 | 256 | FAA-cooperative | 0.267 | 150.05 | 309.47 | 1.00 [0.96, 1.03] | 1.01 | 4236.31 |
| 4 | 1024 | 1 | 2048 | Vyukov | 0.207 | 193.43 | 353.35 | 1.00 [1.00, 1.00] | 1.02 | 5919.09 |
| 4 | 1024 | 1 | 2048 | PaddedFAA | 0.204 | 196.37 | 356.06 | 0.99 [0.96, 1.01] | 1.00 | 5937.54 |
| 4 | 1024 | 1 | 2048 | PaddedFAA-broadcast | 0.208 | 192.12 | 351.12 | 1.01 [0.98, 1.04] | 1.02 | 5869.06 |
| 4 | 1024 | 1 | 2048 | FAA-cooperative | 0.203 | 197.46 | 356.88 | 0.98 [0.96, 1.00] | 0.99 | 5869.54 |
| 4 | 1024 | 8 | 0 | Vyukov | 0.715 | 55.96 | 215.23 | 1.00 [1.00, 1.00] | 1.03 | 1505.56 |
| 4 | 1024 | 8 | 0 | PaddedFAA | 0.692 | 57.82 | 216.47 | 0.97 [0.95, 0.99] | 1.00 | 1554.82 |
| 4 | 1024 | 8 | 0 | PaddedFAA-broadcast | 0.729 | 54.92 | 214.23 | 1.02 [0.99, 1.05] | 1.05 | 1489.61 |
| 4 | 1024 | 8 | 0 | FAA-cooperative | 1.094 | 36.59 | 196.22 | 1.53 [1.49, 1.57] | 1.58 | 972.64 |
| 4 | 1024 | 8 | 256 | Vyukov | 0.581 | 68.92 | 228.24 | 1.00 [1.00, 1.00] | 0.99 | 1862.62 |
| 4 | 1024 | 8 | 256 | PaddedFAA | 0.590 | 67.92 | 228.68 | 1.02 [0.99, 1.04] | 1.00 | 1843.86 |
| 4 | 1024 | 8 | 256 | PaddedFAA-broadcast | 0.611 | 65.48 | 223.73 | 1.05 [1.03, 1.08] | 1.04 | 1764.69 |
| 4 | 1024 | 8 | 256 | FAA-cooperative | 0.858 | 46.63 | 205.93 | 1.48 [1.45, 1.51] | 1.46 | 1291.48 |
| 4 | 1024 | 8 | 2048 | Vyukov | 0.368 | 108.69 | 269.06 | 1.00 [1.00, 1.00] | 1.01 | 3162.13 |
| 4 | 1024 | 8 | 2048 | PaddedFAA | 0.364 | 109.90 | 269.83 | 0.99 [0.96, 1.02] | 1.00 | 3279.26 |
| 4 | 1024 | 8 | 2048 | PaddedFAA-broadcast | 0.370 | 108.04 | 267.75 | 1.01 [0.99, 1.02] | 1.02 | 3169.89 |
| 4 | 1024 | 8 | 2048 | FAA-cooperative | 0.394 | 101.57 | 260.22 | 1.07 [1.04, 1.10] | 1.08 | 2985.83 |

## Prenotazioni FAA a parita' di risveglio

Rapporti contro PaddedFAA-broadcast; sopra 1 vince FAA-cooperative.

| Worker | Capacita | Batch | Work | Speedup [IC95%] | Parcheggi input/msg | Parcheggi output/msg |
|---:|---:|---:|---:|---|---:|---:|
| 1 | 1024 | 1 | 0 | 0.93 [0.92, 0.94] | 0.000 | 0.000 |
| 1 | 1024 | 1 | 256 | 0.96 [0.95, 0.96] | 0.000 | 0.000 |
| 1 | 1024 | 1 | 2048 | 0.98 [0.98, 0.99] | 0.000 | 0.000 |
| 1 | 1024 | 8 | 0 | 0.87 [0.86, 0.87] | 0.000 | 0.000 |
| 1 | 1024 | 8 | 256 | 0.93 [0.93, 0.94] | 0.000 | 0.000 |
| 1 | 1024 | 8 | 2048 | 0.98 [0.98, 0.99] | 0.000 | 0.000 |
| 4 | 1024 | 1 | 0 | 0.97 [0.92, 1.03] | 0.095 | 0.578 |
| 4 | 1024 | 1 | 256 | 1.01 [0.96, 1.06] | 0.194 | 0.813 |
| 4 | 1024 | 1 | 2048 | 0.97 [0.95, 1.00] | 0.526 | 1.139 |
| 4 | 1024 | 8 | 0 | 1.50 [1.46, 1.55] | 0.044 | 0.048 |
| 4 | 1024 | 8 | 256 | 1.40 [1.37, 1.44] | 0.071 | 0.071 |
| 4 | 1024 | 8 | 2048 | 1.06 [1.05, 1.08] | 0.098 | 0.094 |

I contatori parcheggi indicano ingressi nel percorso BLOCKED, inclusi ricontrolli che rendono immediatamente pronto l'actor. Non sono context switch del sistema operativo. Il batch limita le chiamate process() per attivazione; non raggruppa le prenotazioni atomiche.

Validazione: conteggio, checksum del calcolo, latenza campionata e assenza di operazioni pendenti in tutte le misure. Prove exact-once separate (contatore per ID), con code da due slot, input vuoto e transform che elimina messaggi. Dettagli nel manifest.
