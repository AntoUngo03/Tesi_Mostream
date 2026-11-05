# Benchmark Vyukov vs Rigtorp

Questa cartella raccoglie benchmark che confrontano Rigtorp e Vyukov:

- [PipelineGrain](PipelineGrain/README.md): N stadi sequenziali, lavoro totale T diviso fra gli stadi (T/N per stadio).
- [FarmThroughput](FarmThroughput/README.md): una sorgente sequenziale e N consumatori; ogni input è elaborato da un solo consumatore con lavoro T.
- [ImageTiles](ImageTiles/README.md): immagini divise in blocchi di righe, confronto con il seriale senza code, throughput e latenza per immagine.


## 1. Avviare PipelineGrain

Dalla cartella comune indicata sopra:

```sh
python3 PipelineGrain/run.py
```

Configurazione predefinita: N=1,2,4,8,16 stadi; T=0,1,2,5,10 microsecondi totali
per input; 100.000 input; capacità 1024; cinque ripetizioni per configurazione.

Per ripetere i parametri usati nel confronto a tre ripetizioni:

```sh
python3 PipelineGrain/run.py \
  --stages 1,2,4,8,16 \
  --total-us 0,1,2,5 \
  --messages 50000 \
  --repeats 3
```

Per una prova breve:

```sh
python3 PipelineGrain/run.py \
  --stages 1,2,4 --total-us 0,2 --messages 20000 --repeats 1
```

## 2. Avviare FarmThroughput

Dalla stessa cartella comune:

```sh
python3 FarmThroughput/run.py
```

Configurazione predefinita: N=1,2,4,8,16 consumatori, più la sorgente;
T=0,1,2,5 microsecondi di lavoro per input; 100.000 input; capacità 1024;
tre ripetizioni per configurazione.

Per ripetere i parametri usati nel confronto a tre ripetizioni:

```sh
python3 FarmThroughput/run.py \
  --consumers 1,2,4,8,16 \
  --work-us 0,1,2,5 \
  --messages 50000 \
  --repeats 3
```

Per una prova breve:

```sh
python3 FarmThroughput/run.py \
  --consumers 1,2,4 --work-us 0,2 --messages 20000 --repeats 1
```

Eseguire i due benchmark uno alla volta: attendere la fine del primo prima di
avviare il secondo, così non si contendono le CPU e non alterano reciprocamente
le misure. Non serve compilare manualmente: i runner compilano le code, calibrano
il lavoro, eseguono warm-up e controlli di correttezza, poi raccolgono le misure.

## 3. Trovare i risultati e generare i grafici

Alla fine ogni runner stampa `Risultati:` seguito dal percorso della nuova
cartella. Senza `--output`, la cartella ha un nome con data e ora e si trova in:

```text
PipelineGrain/results/AAAAMMGG-HHMMSS/
FarmThroughput/results/AAAAMMGG-HHMMSS/
```

Ogni cartella contiene:

- `raw.csv`: tutte le singole misure.
- `summary.csv`: mediane, throughput e indicatori di scalabilità.
- `metadata.json`: parametri, ambiente e calibrazione.

I grafici si generano con un comando separato. Nei comandi seguenti, sostituire
`AAAAMMGG-HHMMSS` con il nome effettivo della cartella appena creata:

```sh
python3 PipelineGrain/plot.py "PipelineGrain/results/AAAAMMGG-HHMMSS"
python3 FarmThroughput/plot.py "FarmThroughput/results/AAAAMMGG-HHMMSS"
```

PipelineGrain produce `pipeline_grain.png` e `pipeline_grain.pdf`.
FarmThroughput produce `farm_throughput.png/pdf` e `farm_efficiency.png/pdf`.
I file vengono salvati nella stessa cartella dei CSV.

Per rigenerare i grafici dei risultati già presenti:

```sh
python3 PipelineGrain/plot.py "PipelineGrain/results/rigtorp-vyukov"
python3 FarmThroughput/plot.py "FarmThroughput/results/validation"
```

## Opzioni utili

```sh
python3 PipelineGrain/run.py --help
python3 FarmThroughput/run.py --help
```

`--capacity 1024` imposta la capacità della coda, comune alle due implementazioni;
deve essere una potenza di due almeno pari a 2. `--repeats` cambia le ripetizioni.
`--output "PipelineGrain/results/mia-prova"`, oppure l'equivalente per
FarmThroughput, sceglie il percorso dei risultati: deve essere una cartella nuova.

Il pinning automatico usa core fisici distinti. PipelineGrain richiede almeno N
worker e core disponibili; FarmThroughput almeno N+1, contando la sorgente.
`--cpus none` disabilita il pinning, ma rende le prove meno controllate.

I risultati e le cache sono esclusi da Git. I metadati dei risultati storici
conservano i percorsi originali, precedenti al raggruppamento dei benchmark.

## Benchmark immagini: ImageTiles

Dalla cartella comune:

```sh
python3 ImageTiles/run.py
```

Default: grayscale 512×512, 32 immagini, N=1,2,4,8,16 consumer, blocchi da
1,8,32,128,512 righe, 50 ripetizioni. Include sempre il seriale senza code e
verifica esattamente l'output. Per una prova breve:

```sh
python3 ImageTiles/run.py --sizes 256x256 --images 8 \
  --block-rows 1,16,64 --consumers 1,4 --repeats 3
```

Per generare i grafici dei nuovi risultati:

```sh
python3 ImageTiles/plot.py "ImageTiles/results/NOME_CARTELLA"
```

Dettagli su misure, ricomposizione, limiti e baseline in [ImageTiles/README.md](ImageTiles/README.md).
