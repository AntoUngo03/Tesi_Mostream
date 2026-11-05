# Confronto nel runtime MoStream

Questo benchmark usa la vera API `Pipeline` e i suoi `Communicator`, non un
harness che chiama direttamente le code:

```text
seq(Source) -> parallel(Compute, N) -> seq(Sink)
```

Ogni input contiene un ID e un valore UInt64. Un singolo worker esegue tutto il
kernel xorshift del messaggio; il sink registra ogni risultato. Dopo la misura
si verifica esattamente che ogni ID compaia una sola volta e che il suo valore
sia uguale al risultato sequenziale. Allocazione dei buffer e verifica sono
fuori dalla misura.

Si cronometra `Pipeline.run()`: creazione dei communicator, allocazione delle
code, lancio e attesa dei task, comunicazione dei messaggi, EOS e distruzione,
incluse le stampe del runtime. Costruzione della Pipeline, compilazione e avvio
del processo sono fuori dal tempo misurato. Il throughput è M * 1e9 / elapsed_ns.
Il runtime usato qui è quello standard; la suite Tests verifica anche quello
cooperativo. Il pinning è disabilitato per entrambi i backend.

```sh
python3 Benchmarks/RuntimeQueues/run.py \
  --messages 20000 --workers 1,2,4,8 --work-us 0,2,5 --repeats 5 \
  --output Benchmarks/RuntimeQueues/results/NUOVO_NOME
```

La cartella deve essere nuova. Il runner compila lo stesso sorgente due volte,
con e senza `-D MOSTREAM_RIGTORP=1`, calibra il kernel cinque volte e usa la
mediana per determinare le iterazioni di ogni T. Entrambi i backend eseguono
lo stesso numero di iterazioni. Dopo un warm-up di entrambi, si mescolano le
configurazioni e si alterna casualmente l'ordine dei backend, con seed fisso.
Ogni coppia usa gli stessi input, lavoro e capacità (default 1024).

`raw.csv` contiene le singole misure; `summary.csv` riporta mediane di
throughput, minimo/massimo e rapporto Rigtorp/Vyukov (>1 favorisce Rigtorp).
`metadata.json` registra toolchain, CPU, affinità, calibrazione, parametri e
hash dei sorgenti. Le mediane hanno cinque campioni nella prova preliminare:
le differenze piccole non dimostrano un vantaggio stabile o significativo.

Il sink aggiunge un ulteriore punto di sincronizzazione e può diventare il
collo di bottiglia, soprattutto con T=0. Questo esperimento mantiene il carico
sintetico per isolare il passaggio all'API reale; non è ancora un'applicazione
come l'elaborazione immagini. Il numero di task è N+2, mentre il precedente
FarmThroughput usava una sorgente e N consumatori senza sink. Non confrontare
direttamente i valori assoluti delle due suite. Il confronto valido qui è fra
i due backend nello stesso esperimento.

## Grafico

Con matplotlib disponibile:

```sh
python3 Benchmarks/RuntimeQueues/plot.py \
  Benchmarks/RuntimeQueues/results/Throughput_pipeline
```

Genera `runtime_farm_throughput.png`, `.pdf` e `.svg`: un pannello per ogni T,
mediane del throughput e fasce minimo–massimo delle ripetizioni. Le fasce non
sono intervalli di confidenza. N indica i worker Compute; source e sink
aggiungono due task. Il grafico usa la stessa scala verticale nei tre pannelli.

## Variante con un collegamento MPMC e pinning

Il nuovo esperimento `farm_mpmc.mojo` usa due stadi di calcolo paralleli e
confronta anche il pinning attivo/disattivo. Mantiene fisso il lavoro totale
per input. Sorgenti, comandi e limiti sono descritti in
[README_MPMC.md](README_MPMC.md).
