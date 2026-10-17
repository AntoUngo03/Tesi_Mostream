# Nuovi risultati con 50 ripetizioni

Questa campagna non modifica gli script originari, i CSV, i grafici o il PDF
con tre ripetizioni. Esegue gli stessi benchmark con gli stessi 50.000 input,
capacità 1024, N=1,2,4,8,16 e T=0,1,2,5 µs, ma con 50 ripetizioni per coda.

Sulla macchina con Mojo 1.1, dalla cartella comune:

```sh
python3 Campagna_50_ripetizioni/run.py
```

I due benchmark vengono eseguiti in sequenza, per un totale di 4.000 misure.
Tutti i nuovi file vengono salvati in una cartella nuova:

```text
Campagna_50_ripetizioni/risultati/AAAAMMGG-HHMMSS/
├── PipelineGrain/   # 2.000 misure, nuovi CSV, grafici e confronto statistico
├── FarmThroughput/  # 2.000 misure, nuovi CSV, grafici e confronto statistico
└── COMPLETATO.txt   # appare soltanto al termine di entrambe le campagne
```

`--output /percorso/nuovo` consente di scegliere una destinazione diversa; se
esiste già, l'avvio viene rifiutato. In caso di errore compare `INTERROTTO.txt`.
La sola presenza degli script non indica che la campagna sia stata eseguita.

`confronto_50.csv` e `confronto_50.md` riportano il rapporto fra i throughput
mediani Rigtorp/Vyukov e intervalli percentile bootstrap al 95%. Per la farm
vengono ricampionate insieme le coppie con lo stesso identificatore di
ripetizione, eseguite vicine nel tempo. Per PipelineGrain si usa ricampionamento
indipendente perché il runner mescola globalmente le singole prove.
La baseline PipelineGrain con N=1 non usa code e non attribuisce un vincitore.

Un vantaggio pratico del 5% viene distinto dalla semplice differenza di segno.
La soglia è una scelta di analisi. Gli intervalli sono individuali, senza
correzione per i molti confronti: l'analisi è esplorativa. Cinquanta ripetizioni
nella stessa sessione non dimostrano superiorità universale o stabilità fra
sessioni. I conteggi di correttezza dei benchmark restano attivi.

Dipendenze: quelle dei benchmark originali e matplotlib per i grafici.
L'analisi bootstrap usa soltanto la libreria standard Python.

## Nuovo PDF, soltanto in locale

Dopo avere copiato i nuovi risultati nella cartella locale, è possibile creare
un supplemento PDF separato, senza rigenerare la relazione da tre ripetizioni:

```sh
/usr/bin/python3 Campagna_50_ripetizioni/genera_pdf_locale.py \
  "Campagna_50_ripetizioni/risultati/AAAAMMGG-HHMMSS"
```

Il generatore richiede il renderer locale conservato in
`Relazione/genera_relazione.py`, Cairo e Pango del Python di sistema. Crea
`Confronto_50_ripetizioni.pdf` solo nella nuova cartella e rifiuta di sovrascriverlo
se esiste già. Non usa SSH e non rigenera il PDF precedente.
