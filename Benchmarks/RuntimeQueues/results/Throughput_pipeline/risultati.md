# Primo confronto nella libreria — 26 ottobre 2026

Mojo 1.1.0, runtime standard, 20.000 input, capacità 1024, cinque ripetizioni per configurazione. Pinning disabilitato. 120 esecuzioni misurate; ogni output è stato verificato. Il tempo è quello di Pipeline.run, comprendente setup, task, EOS e teardown; il throughput è il numero di input diviso per questo tempo.

Il lavoro per input resta intero sul worker che lo riceve. N è il numero di repliche della trasformazione, cui si aggiungono source e sink. T è il costo obiettivo calibrato del kernel, in microsecondi.

| Worker | T (µs) | Vyukov (input/s) | Rigtorp (input/s) | Differenza Rigtorp |
|---:|---:|---:|---:|---:|
| 1 | 0 | 847,308 | 802,934 | -5.2% |
| 1 | 2 | 279,790 | 282,385 | +0.9% |
| 1 | 5 | 153,980 | 155,870 | +1.2% |
| 2 | 0 | 1,174,353 | 1,221,802 | +4.0% |
| 2 | 2 | 556,291 | 573,928 | +3.2% |
| 2 | 5 | 293,199 | 309,411 | +5.5% |
| 4 | 0 | 1,492,693 | 1,377,045 | -7.7% |
| 4 | 2 | 997,542 | 987,605 | -1.0% |
| 4 | 5 | 519,411 | 529,768 | +2.0% |
| 8 | 0 | 1,213,924 | 1,172,910 | -3.4% |
| 8 | 2 | 1,431,941 | 1,396,659 | -2.5% |
| 8 | 5 | 989,424 | 956,576 | -3.3% |

Valori: mediane del throughput. Minimo e massimo delle singole misure sono in summary.csv; tutti i campioni sono in raw.csv.

In questa prova le differenze delle mediane vanno da −7,7% a +5,5% per Rigtorp. La direzione cambia con la configurazione; gli intervalli min–max si sovrappongono in tutti i dodici casi. Cinque campioni non sostengono una conclusione statistica di equivalenza o superiorità. Il sink e i costi del runtime possono attenuare o cambiare il vantaggio osservato con un harness dedicato.

La conclusione supportata è che Rigtorp funziona nei communicator reali e che il vantaggio sintetico non si trasferisce automaticamente a questa topologia. Per una conclusione prestazionale più solida servono misure più lunghe, più ripetizioni e il workload applicativo di interesse.

Fonti: raw.csv, summary.csv e metadata.json in questa cartella. Metodo completo: ../../README.md. I risultati non misurano la latenza individuale e non coprono le prestazioni del runtime cooperativo.
