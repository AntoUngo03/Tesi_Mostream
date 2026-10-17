# Campagna Tsigas–Zhang vs Vyukov a 50 ripetizioni

Dalla cartella comune:

```sh
python3 -u Campagna_50_ripetizioni/run.py
```

Avvia in sequenza PipelineGrain, FarmThroughput e ImageTiles, genera grafici
PNG/PDF e analisi bootstrap per i due sintetici. Totale: 5.550 misure.
Con `--smoke` esegue solo casi piccoli a una ripetizione.

Tutti i risultati vengono salvati in una cartella nuova sotto `risultati/`.
`--output` consente una destinazione esplicita che non deve già esistere.
`COMPLETATO.txt` attesta la fine dei tre esperimenti; `INTERROTTO.txt` segnala
risultati parziali. Nessun dato Rigtorp viene copiato come se fosse Tsigas.

Leggere il [README comune](../README.md) prima di confrontare i valori:
capacità automatica grande, posti utili L−2/L, vincolo anti-ABA e kernel
sintetico a 63 bit differiscono dalla vecchia campagna.

L'analisi del rapporto Tsigas/Vyukov è esplorativa e non corretta per confronti
multipli; ImageTiles conserva statistiche descrittive, senza bootstrap.

Il PDF locale facoltativo dei due sintetici può essere generato, dopo la copia
locale dei risultati, con il Python di sistema:

```sh
/usr/bin/python3 Campagna_50_ripetizioni/genera_pdf_locale.py "Campagna_50_ripetizioni/risultati/NOME_CARTELLA"
```

Usa soltanto il renderer della relazione Rigtorp già presente in locale e
rifiuta di sovrascrivere un PDF esistente. Non esegue connessioni SSH.
