# PipelineGrain: Tsigas–Zhang vs Vyukov

N stadi sequenziali e T/N di lavoro per stadio.

Dalla cartella comune:

```sh
python3 PipelineGrain/run.py --messages 50000 --repeats 50
```

Per il grafico, usare `PipelineGrain/plot.py` sulla cartella stampata dal runner.
Il default individuale è tre ripetizioni. Non avviare insieme altri benchmark.

Leggere il [README comune](../README.md) per capacità fisica/utile, limite
anti-ABA, kernel a 63 bit e interpretazione dei risultati. `--capacity 0`
seleziona automaticamente un anello che impedisce giri completi nella prova.
I risultati della suite Rigtorp non vengono riutilizzati o rinominati.
