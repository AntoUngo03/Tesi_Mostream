# Benchmark wCQ vs Vyukov

Confronto del porting **nativo Mojo wCQ** con il protocollo Vyukov già usato
nel confronto Rigtorp. La suite riutilizza i tre carichi e l'harness Mojo 1.1
di quell'esperimento, senza modificarne script o risultati.

| Esperimento | Topologia e misura |
| --- | --- |
| [PipelineGrain](PipelineGrain/README.md) | N stadi, lavoro totale T diviso fra gli stadi; throughput e costo per input |
| [FarmThroughput](FarmThroughput/README.md) | Una sorgente, una coda condivisa, N consumer; lavoro T per input e throughput |
| [ImageTiles](ImageTiles/README.md) | Sorgente, N consumer e raccoglitore; due code, grayscale SIMD, seriale, throughput e latenza per immagine |

## Campagna completa

Servono Linux x86-64 con CMPXCHG16B, **Mojo 1.1**, GCC, Python 3 e matplotlib.
Dalla cartella di questa suite:

```sh
python3 -u Campagna_50_ripetizioni/run.py --mojo /percorso/al/mojo
```

I tre esperimenti vengono eseguiti **uno alla volta** con 50 ripetizioni, capacità
utile 1024, 50.000 messaggi nei sintetici, T=0,1,2,5 µs. Per le immagini si
usano 32 frame 512×512 e strisce da 1,16,128 righe, più il seriale senza code.
Warm-up e verifiche sono eseguiti anche durante la campagna, fuori dai tempi
misurati salvo i controlli del raccoglitore già presenti nel benchmark originale.

`--nodes auto` sceglie dalla griglia 1,2,4,8,16 soltanto i valori compatibili con
N+2 **core fisici distinti**, rispettando l'affinità disponibile. La griglia
scelta e i valori esclusi sono registrati e stampati. Su una macchina con
almeno 18 core fisici si può richiedere esplicitamente la griglia originale:

```sh
python3 -u Campagna_50_ripetizioni/run.py --nodes 1,2,4,8,16
```

Su questa macchina ci sono 6 core fisici: la griglia locale è **N=1,2,4**,
con 1.200 misure PipelineGrain, 1.200 FarmThroughput e 950 ImageTiles,
**3.350 misure in totale**. N=8,16 non vengono misurati in oversubscription.
Con la griglia a cinque valori le misure sarebbero 5.550.

Validazione breve, separata dai risultati a 50 ripetizioni:

```sh
python3 Campagna_50_ripetizioni/run.py --smoke --mojo /percorso/al/mojo
```

Non avviare benchmark contemporaneamente. Un risultato breve o incompleto non
deve essere presentato come campagna completa.

## Backend e confronto equo

La capacità indicata è quella **utile** e coincide per entrambe le code. wCQ
usa un array dati e due ring di indici, ciascuno con 2×capacità celle fisiche;
Vyukov usa capacità celle con sequenza e dato. Non si impone uguale memoria.

wCQ conserva il porting sperimentale in `MoStream/MPMC_queue_wcq.mojo`, con
fast path 16/64 tentativi e `help_delay=16`. L'algoritmo è Mojo; allocazione
allineata e primitive atomiche sono C, con ordinamento sequenziale e CAS doppia
hardware. Le chiamate alle primitive, la verifica del tid e i contatori
diagnostici già interni al porting fanno parte del costo misurato.

Vyukov conserva il corpo del protocollo del confronto Rigtorp: CAS relaxed sui
contatori, acquire/release sulle sequenze e dato `UInt64` separato, senza
spinlock o backoff aggiunto. Il parametro tid è ignorato da questo backend.
Non gli vengono aggiunte chiamate atomiche C per pareggiare artificialmente i
costi. I due binari sono compilati con `-O3`, senza sanitizer.

Gli ID di wCQ sono esclusivi per ogni coda: nella pipeline produttore 0 e
consumatore 1; nella farm sorgente 0 e consumer i+1; nelle immagini lavoratori
i+1 e sorgente/raccoglitore 0 sulle rispettive code distinte. Una coda della
farm o delle immagini richiede capacità almeno N+1; i preflight usano la
minima potenza di due compatibile, almeno 4.

Il kernel sintetico xorshift è quello originale a **64 bit**, senza la maschera
aggiunta per l'esperimento Tsigas. Il kernel immagini resta lo stesso per il
seriale e per entrambe le code. Sorgenti, calibrazione, pinning e definizioni
delle misure sono conservati nei metadati. Non si uniscono tempi storici di
altre macchine o compilatori a quelli nuovi.

Questi risultati confrontano le **implementazioni concrete**, inclusi memoria
e ordinamenti: non isolano il solo algoritmo, non misurano la latenza di una
singola chiamata alla coda e non certificano wait-freedom. `push`/`pop` attendono
attivamente; sono necessarie risorse sufficienti per tutti i task.

## File prodotti

Ogni campagna crea una cartella nuova sotto `Campagna_50_ripetizioni/risultati`:

- `campagna.json`: griglia effettiva, parametri, esclusioni e note sui backend;
- per ciascun esperimento, `raw.csv`, `summary.csv`, `metadata.json` e grafici PNG/PDF;
- per PipelineGrain e FarmThroughput, `confronto_50.csv`/`.md`, con rapporto wCQ/Vyukov e bootstrap al 95%;
- per ImageTiles, `latencies.csv`, con i campioni di ogni immagine;
- `VERIFICA.json`: conteggi e hash degli artefatti;
- `COMPLETATO.txt`: scritto soltanto dopo tutte le verifiche e tutti i grafici;
- `INTERROTTO.txt`: indica un fallimento; i dati parziali rimangono disponibili.

Il rapporto di throughput maggiore di 1 favorisce wCQ. PipelineGrain con N=1
non usa code e non assegna un vincitore. Il bootstrap è indipendente nella
pipeline e appaiato per ripetizione nella farm, coerentemente con l'ordine di
esecuzione. Gli intervalli sono individuali, senza correzione per confronti
multipli; il 5% è una soglia pratica di analisi. ImageTiles riporta confronti
descrittivi di throughput e latenza, senza intervalli di confidenza del rapporto.
Le fasce min–max dei grafici non sono intervalli di confidenza. Cinquanta prove
nella stessa sessione non dimostrano stabilità fra sessioni.

Per generare il riepilogo leggibile di una campagna completata:

```sh
python3 genera_confronto.py Campagna_50_ripetizioni/risultati/NOME_CARTELLA
```

Il compilatore ufficiale Mojo 1.1 e matplotlib usati localmente sono stati
preparati in cartelle temporanee; l'installazione di sistema non è stata cambiata.
Il runner accetta `--mojo` e la variabile `MOJO`, per usare il compilatore del
proprio ambiente. I risultati restano nella cartella del progetto.

## Risultati locali del 21 ottobre 2026

Campagna completata in
`Campagna_50_ripetizioni/risultati/20261021-50-ripetizioni-locali/`:
3.350 misure, 50 ripetizioni per configurazione, N=1,2,4 e tutti i controlli
di correttezza superati. CPU Intel Core i7-10750H, 6 core fisici, Mojo 1.1.0.

- [Rapporto PDF, 10 pagine](Campagna_50_ripetizioni/risultati/20261021-50-ripetizioni-locali/Confronto_wCQ_Vyukov.pdf)
- [Tabelle, metodologia e grafici](Campagna_50_ripetizioni/risultati/20261021-50-ripetizioni-locali/CONFRONTO.md)
- [Confronto numerico CSV](Campagna_50_ripetizioni/risultati/20261021-50-ripetizioni-locali/confronto.csv)
- [Verifica dei risultati](Campagna_50_ripetizioni/risultati/20261021-50-ripetizioni-locali/VERIFICA.json)

In questa sessione Vyukov ha throughput maggiore in tutte le configurazioni
sintetiche che usano code. Il divario è maggiore a T=0 e si riduce aumentando
il lavoro per input. Nelle immagini, Vyukov è più veloce con strisce da 1 o 16
righe; con 128 righe i throughput sono vicini e wCQ ha un vantaggio descrittivo
di circa 0,7–3,5%, senza intervalli di confidenza di quel rapporto. Non si tratta
di una superiorità universale né di un confronto con l'implementazione C del paper.

I collegamenti `PipelineGrain/results/50_ripetizioni`,
`FarmThroughput/results/50_ripetizioni` e `ImageTiles/results/50_ripetizioni`
portano ai medesimi dati, senza duplicarli. Lo smoke test è separato in
`risultati/20261021-smoke-01`. `esecuzione.log`, `toolchain.json` e una copia
dei sorgenti misurati sotto `sorgenti/` completano la provenienza della campagna.
