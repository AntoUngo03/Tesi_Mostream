# Tre benchmark a 50 ripetizioni

```sh
python3 -u Campagna_50_ripetizioni/run.py --mojo /percorso/mojo
```

Esegue PipelineGrain, FarmThroughput e ImageTiles in sequenza, con compilazione
automatica, pinning su core fisici, warm-up, controlli di correttezza, CSV,
grafici e analisi dei due benchmark sintetici. Vedere il
[README principale](../README.md) per parametri e metodologia.

La griglia predefinita è selezionata automaticamente fra 1,2,4,8,16 senza
oversubscription: servono N+2 core fisici per usare lo stesso N nei tre test.
Sulla macchina locale la griglia è 1,2,4, per 3.350 misure. Per eseguire la
griglia completa su una macchina adeguata usare `--nodes 1,2,4,8,16`.

`--smoke` esegue una sola ripetizione con carichi piccoli e non produce
inferenze statistiche. `--output` sceglie una cartella nuova; non sovrascrive
risultati. `COMPLETATO.txt` appare solo alla fine; in caso di errore rimane
`INTERROTTO.txt`. La presenza degli script non dimostra l'esecuzione delle prove.
