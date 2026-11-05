# BenchMark Tsigas vs Vyukov

Confronto fra il porting Mojo Tsigas–Zhang e Vyukov senza spinlock. Questa suite
è distinta da `BenchMark Vyukov vs RigTorp`: non ne modifica risultati o script.

| Cartella | Esperimento |
|---|---|
| `PipelineGrain` | N stadi sequenziali, lavoro totale T distribuito come T/N |
| `FarmThroughput` | una sorgente e N consumer, lavoro T per input |
| `ImageTiles` | grayscale di immagini a strisce, throughput, latenza, seriale e correttezza |
| `Campagna_50_ripetizioni` | avvio sequenziale di tutti e tre, grafici e analisi |

## Prima di interpretare i risultati

Il porting Tsigas è sperimentale e conserva l'ipotesi anti-ABA del paper:
non basta superare test per garantire correttezza sotto sospensioni arbitrarie.
La versione provata rafforza la scansione di dequeue: oltre a head ricontrolla
lo snapshot di tail, per evitare di saltare pubblicazioni concorrenti dopo
letture di vuoto obsolete. Non è la trascrizione letterale iniziale.
Leggere anche [la descrizione del porting](../../docs/Tsigas_Zhang_Mojo.md).

Per questa suite si sceglie una prova finita con **code nuove e nessun giro
completo**. Ogni runner calcola un limite conservativo dei successi per coda:

- pipeline: `2 × messaggi`;
- farm: `2 × (messaggi + consumer)`, comprese le sentinelle;
- immagini: `2 × (blocchi totali + consumer)` per ciascuna delle due code.

Con `--capacity 0` viene scelta una potenza di due **strettamente maggiore**
del limite. Un valore esplicito insufficiente è rifiutato, anche dal binario
Mojo. Warm-up e misura usano code distinte. I fallimenti dei tentativi non
riempiono o svuotano celle; il limite conta le modifiche riuscite.

Per 50.000 messaggi, il default di pipeline e farm è quindi **131.072 celle
fisiche**, non 1.024. Con 32 immagini 512×512 e blocchi 1/16/128 righe è
**65.536 celle**. Entrambe le implementazioni ricevono la stessa lunghezza
fisica: Tsigas ha `L−2` posti utili e Vyukov `L`. La differenza è registrata
nei metadati e nei dati grezzi. Non si dichiara uguale capacità utile.

Questa scelta studia un regime a lotti con anello grande: **non misura il
riuso continuo di una piccola coda o la backpressure a coda piena**. I risultati
non si possono confrontare direttamente con la vecchia campagna Rigtorp da
1.024 posti, né dimostrano correttezza universale di Tsigas.

Il kernel sintetico xorshift applica una maschera a 63 bit a ogni iterazione,
identica per entrambe le code: nessun risultato può coincidere con i due UInt64
non rappresentabili nel porting. La maschera è interna all'iterazione, così
la divisione in N stadi conserva esattamente il risultato seriale. È una modifica
esplicita del carico rispetto agli esperimenti storici. ImageTiles mantiene
invece lo stesso kernel grayscale e gli identificatori di blocco.

Il confronto riguarda due implementazioni concrete: Tsigas conserva gli accessi
atomici sequenzialmente consistenti e il payload nella cella atomica; Vyukov usa
prenotazioni relaxed, pubblicazione acquire/release e un campo dati separato.
Anche dimensione delle celle e footprint di memoria differiscono. Un vantaggio
misurato non isola quindi il solo algoritmo o il solo numero di CAS.

## Avvio sulla macchina con Mojo 1.1

Servono Mojo 1.1.0, Python 3, GCC e matplotlib. Dalla radice del repository:

```sh
cd "Benchmarks/BenchMark Tsigas vs Vyukov"
```

Validazione breve (una ripetizione, nessuna conclusione statistica):

```sh
python3 Campagna_50_ripetizioni/run.py --smoke
```

Campagna completa di **tutti e tre** gli esperimenti:

```sh
python3 -u Campagna_50_ripetizioni/run.py
```

Produce 2.000 misure PipelineGrain, 2.000 FarmThroughput e 1.550 ImageTiles:
**5.550 esecuzioni**, tutte con i controlli di correttezza attivi. Ogni avvio
crea una cartella nuova sotto `Campagna_50_ripetizioni/risultati/`. Non eseguire
altre prove prestazionali contemporaneamente.

Il file `COMPLETATO.txt` compare solo dopo verifica dei conteggi, salvataggio
dei CSV e generazione dei grafici dei tre benchmark. In caso di errore compare
`INTERROTTO.txt` e i dati parziali restano disponibili. Lo smoke test è etichettato
come tale e non viene presentato come una campagna a 50 ripetizioni.

## Avviare un solo benchmark

```sh
python3 PipelineGrain/run.py --stages 1,2,4,8,16 --total-us 0,1,2,5 --messages 50000 --repeats 50
python3 FarmThroughput/run.py --consumers 1,2,4,8,16 --work-us 0,1,2,5 --messages 50000 --repeats 50
python3 ImageTiles/run.py --sizes 512x512 --images 32 --block-rows 1,16,128 --consumers 1,2,4,8,16 --repeats 50
```

I runner individuali hanno tre ripetizioni come default per prove esplorative.
`--output` sceglie una cartella nuova. I grafici si generano con:

```sh
python3 PipelineGrain/plot.py "PipelineGrain/results/NOME_CARTELLA"
python3 FarmThroughput/plot.py "FarmThroughput/results/NOME_CARTELLA"
python3 ImageTiles/plot.py "ImageTiles/results/NOME_CARTELLA"
```

## Dati e analisi

`raw.csv`, `summary.csv` e `metadata.json` conservano misure, riepiloghi,
versione Mojo, pinning, capacità, vincolo anti-ABA e hash dei sorgenti.
ImageTiles conserva anche le latenze individuali e verifica ogni byte contro
un riferimento scalare, oltre a checksum e copertura dei blocchi.

Per pipeline e farm, `confronto_50.csv`/`.md` riportano il rapporto dei throughput
**Tsigas/Vyukov** e intervalli percentile bootstrap al 95%, con 20.000 campioni:
indipendenti nella pipeline, appaiati per repeat nella farm. Sono intervalli
individuali non corretti per confronti multipli: analisi esplorativa, non prova
di stabilità fra sessioni. N=1 della pipeline non confronta code.

ImageTiles riporta mediane e p95 per immagine, throughput e speedup sul seriale;
non calcola intervalli di confidenza del rapporto. Le fasce min–max dei grafici
non sono intervalli di confidenza. Latenza e throughput hanno significati diversi:
la latenza esclude l'attesa precedente all'invio del primo blocco di un'immagine.

L'eventuale generatore PDF locale non avvia benchmark né sincronizza documenti.

## Risultati già ottenuti — 16 ottobre 2026

Campagna completa salvata in
`Campagna_50_ripetizioni/risultati/20261016-campagna-50/`:
2.000 prove PipelineGrain, 2.000 FarmThroughput e 1.550 ImageTiles, con 50
ripetizioni per configurazione. Tutti i controlli di correttezza sono passati.
Sono disponibili sei grafici, ciascuno in PNG e PDF, e 49.600 latenze individuali.
`VERIFICA.json` registra i controlli conclusivi e gli hash degli artefatti.

Ogni cartella individuale offre anche il collegamento `results/50_ripetizioni`
agli stessi dati, senza duplicarli. Lo smoke test a una ripetizione resta
separato in `risultati/smoke-validation-04` e non va usato per conclusioni
statistiche. Il PDF `Confronto_50_ripetizioni.pdf`, se presente nella campagna,
è il rapporto locale sui due benchmark sintetici.
