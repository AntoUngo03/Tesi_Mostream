# PipelineGrain: confronto con 50 ripetizioni

Rapporto > 1 favorisce wCQ; rapporto < 1 favorisce Vyukov.
Intervalli percentile bootstrap al 95%, per singola configurazione; non corretti per confronti multipli.
Analisi esplorativa: non dimostra stabilità fra sessioni, giorni o macchine.
Il 5% è una soglia pratica scelta per questa analisi, non una garanzia prestazionale.

| N | T (µs) | Rapporto throughput W/V | Intervallo 95% | Interpretazione |
|---:|---:|---:|---|---|
| 1 | 0 | 0.835 | non applicabile | baseline senza code: nessun confronto delle code |
| 1 | 1 | 1.013 | non applicabile | baseline senza code: nessun confronto delle code |
| 1 | 2 | 1.012 | non applicabile | baseline senza code: nessun confronto delle code |
| 1 | 5 | 1.001 | non applicabile | baseline senza code: nessun confronto delle code |
| 2 | 0 | 0.409 | 0.379–0.444 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 1 | 0.788 | 0.766–0.811 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 2 | 0.891 | 0.868–0.910 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 2 | 5 | 0.932 | 0.923–0.943 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 0 | 0.313 | 0.303–0.334 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 1 | 0.446 | 0.432–0.461 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 2 | 0.596 | 0.585–0.618 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
| 4 | 5 | 0.837 | 0.825–0.849 | vantaggio Vyukov oltre il 5% supportato dall’intervallo individuale |
