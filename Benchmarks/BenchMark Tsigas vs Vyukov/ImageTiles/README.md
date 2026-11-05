# ImageTiles: Tsigas–Zhang vs Vyukov

Grayscale a blocchi di righe, seriale senza code, throughput e latenze.

Dalla cartella comune:

```sh
python3 ImageTiles/run.py --sizes 512x512 --images 32 --block-rows 1,16,128 --repeats 50
```

Per il grafico, usare `ImageTiles/plot.py` sulla cartella stampata dal runner.
Il default individuale è tre ripetizioni. Non avviare insieme altri benchmark.

Leggere il [README comune](../README.md) per capacità fisica/utile, limite
anti-ABA, kernel a 63 bit e interpretazione dei risultati. `--capacity 0`
seleziona automaticamente un anello che impedisce giri completi nella prova.
I risultati della suite Rigtorp non vengono riutilizzati o rinominati.
