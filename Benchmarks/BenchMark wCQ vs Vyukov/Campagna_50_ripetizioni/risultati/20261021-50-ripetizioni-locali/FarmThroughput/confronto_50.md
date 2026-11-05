# FarmThroughput: confronto con 50 ripetizioni

Rapporto > 1 favorisce wCQ; rapporto < 1 favorisce Vyukov.
Intervalli percentile bootstrap al 95%, per singola configurazione; non corretti per confronti multipli.
Analisi esplorativa: non dimostra stabilità fra sessioni, giorni o macchine.
Il 5% è una soglia pratica scelta per questa analisi, non una garanzia prestazionale.

| N | T (µs) | Rapporto throughput W/V | Intervallo 95% | Interpretazione |
|---:|---:|---:|---|---|
| 1 | 0 | 0.441 | 0.413–0.471 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 1 | 1 | 0.810 | 0.798–0.824 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 1 | 2 | 0.886 | 0.873–0.892 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 1 | 5 | 0.946 | 0.941–0.953 | indicazione a favore di Vyukov; vantaggio minimo del 5% non dimostrato |
| 2 | 0 | 0.313 | 0.296–0.328 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 1 | 0.802 | 0.787–0.820 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 2 | 0.879 | 0.865–0.888 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 5 | 0.944 | 0.923–0.951 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 0 | 0.123 | 0.118–0.127 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 1 | 0.795 | 0.780–0.813 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 2 | 0.881 | 0.872–0.891 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 5 | 0.951 | 0.942–0.961 | indicazione a favore di Vyukov; vantaggio minimo del 5% non dimostrato |
