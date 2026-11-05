# Farm con sorgente replicata

`farm_multisource.mojo` è una variante di `farm.mojo`, che resta invariato:

```text
parallel(Source, S) -> parallel(Compute, N) -> seq(Sink)
```

La coda fra sorgenti e worker ha S produttori e N consumatori (MPMC); quella fra
worker e sink ha N produttori e un consumatore. Resta una sola farm. Gli M input
sono divisi fra le S sorgenti senza sincronizzazione durante la prova: alla prima
chiamata ogni replica prende un indice r da un contatore atomico e produce gli ID
r, r+S, r+2S, ... Con S = 1 la topologia coincide con quella di `farm.mojo`.

Kernel, sink, verifica esatta di ogni risultato (fuori dal tempo misurato),
pinning disabilitato e capacità 1024 sono gli stessi di `farm.mojo`. Il tempo è
quello di `Pipeline.run()`; il throughput è M * 1e9 / elapsed_ns. I task sono S + N + 1.

```sh
python3 Benchmarks/RuntimeQueues/run_multisource.py \
  --messages 100000 --sources 1,2,4,8 --workers 1,2,4,8 --work-us 0,2,5 --repeats 10 \
  --output Benchmarks/RuntimeQueues/results/NUOVO_NOME
python3 Benchmarks/RuntimeQueues/plot_multisource.py Benchmarks/RuntimeQueues/results/NUOVO_NOME
```

`summary.csv` riporta per ogni (S, N, T) le mediane, minimo e massimo e
`rigtorp_wins`: in quante ripetizioni Rigtorp ha superato Vyukov nella stessa
ripetizione. Il grafico ha una riga per T e una colonna per N, con S sull'asse x.
