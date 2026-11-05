# Farm con un collegamento MPMC

`farm_mpmc.mojo` aggiunge un benchmark separato, senza modificare la farm originale:

```text
Source sequenziale -> N Compute A -> N Compute B -> Sink sequenziale
```

La coda centrale ha N produttori e N consumatori. Tutti i communicator usano il
backend scelto: il confronto misura l'intera pipeline, non isola il costo della
sola coda centrale. I messaggi contengono ID Int e valore UInt64. Il lavoro
totale I per input resta fisso: A esegue I//2 iterazioni, B le rimanenti. T è
il costo obiettivo calibrato di tutte le I iterazioni, non il costo di ciascun
stadio. Anche per N=1 sono presenti entrambi gli stadi (baseline senza contesa
fra repliche). Il numero totale di task è 2N+2.

```sh
python3 Benchmarks/RuntimeQueues/run_mpmc.py \
  --messages 50000 --workers 1,2,4,8 --work-us 0,0.5,2 \
  --capacities 64,1024 --pinning off,on --repeats 7 \
  --output Benchmarks/RuntimeQueues/results/NUOVO_NOME
python3 Benchmarks/RuntimeQueues/plot_mpmc.py \
  Benchmarks/RuntimeQueues/results/NUOVO_NOME
```

Il runner seleziona una CPU logica per core fisico distinto, rispettando
l'affinità disponibile; ordina per socket e core e registra la lista in
metadata.json. Servono almeno 18 core fisici e 18 worker runtime per N=8 con
pinning. Source, repliche A, repliche B e sink ricevono CPU diverse tramite
MOSTREAM_PINNING e l'API setPinning della libreria. Il helper di affinità della
libreria viene compilato nella copia di lavoro. Un avviso di pinning fa fallire
la prova: non vengono accettati silenziosamente campioni senza affinità.

Ogni campione esegue un processo nuovo; warm-up iniziale per backend e modalità
di pinning. Calibrazione condivisa, stesso numero di iterazioni per backend,
configurazioni mescolate e ordine casuale dei backend all'interno di ogni
coppia. I tempi includono l'intero Pipeline.run, quindi anche setup, pinning,
stampe, chiusura e teardown. Non includono costruzione della Pipeline,
compilazione, avvio del processo o oracolo finale. Il sink registra ogni ID
e valore; fuori dal tempo misurato si verificano esattamente unicità e risultato.

raw.csv conserva tutti i campioni; summary.csv riporta mediane, min–max,
rapporto delle mediane e numero di coppie vinte da Rigtorp. metadata.json
registra ambiente, parametri e hash dei sorgenti. I grafici mostrano tutte le
configurazioni, incluse quelle favorevoli a Vyukov. Le fasce non sono intervalli
di confidenza. Lo sweep è esplorativo: il caso migliore va verificato con
ripetizioni indipendenti prima di sostenerne un vantaggio ripetibile. Il pinning
può aiutare o peggiorare il throughput; non determina da solo quale coda vince.

## Campagna eseguita

Il 26 ottobre 2026 sono state completate 672 esecuzioni esplorative e 120
esecuzioni di conferma, tutte con verifica esatta degli output. Risultati,
metadati e grafici sono in
[results/20261026-mpmc/risultati.md](results/20261026-mpmc/risultati.md).
La conferma di N=8, T=0 e capacità 64 osserva +35,4% senza pinning e +30,2%
con pinning per Rigtorp; entrambe le modalità hanno 30 coppie su 30 favorevoli
a Rigtorp. Si tratta di un risultato per questa macchina e questo workload.
