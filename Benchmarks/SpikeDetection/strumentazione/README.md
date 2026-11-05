# Code strumentate (solo per misure)

Copia di `MPMC_queue.mojo` (Vyukov), `MPMC_queue_rigtorp.mojo`, `communication_queue.mojo` e
`communicator.mojo` con contatori degli eventi interni alle code fra stadi. La libreria del
repository non è modificata: `prepare_instrumented.sh` crea una copia separata e sostituisce questi
file. I throughput della copia strumentata non vanno confrontati con le campagne normali.

| Contatore | Vyukov | Rigtorp |
|---|---|---|
| `push_cas_fail` | CAS di prenotazione della posizione fallita: un altro produttore l'ha presa | sempre 0: il posto si prende con `fetch_add` |
| `push_stale` | posizione letta già superata da un altro produttore | sempre 0 |
| `push_full` | giri di attesa perché la cella non è ancora libera (coda piena) | giri di attesa sullo slot del proprio ticket (coda piena) |
| `pop_cas_fail` | CAS di prenotazione della lettura fallita | uguale |
| `pop_stale` | posizione già consumata da un altro consumatore | ticket già avanzato da un altro consumatore |
| `pop_empty` | tentativi a coda vuota, contati nel Communicator | uguale |

`push_full` conta giri di un ciclo di attesa diverso nelle due code: si confronta fra configurazioni
della stessa coda, non fra le due code. Ogni chiamata conta in variabili locali e somma ai contatori
condivisi (uno per linea di cache) una sola volta alla fine; ogni Communicator stampa una riga
`QSTATS` quando viene distrutto.

```sh
Benchmarks/SpikeDetection/strumentazione/prepare_instrumented.sh ~/mostream-instr
cd ~/mostream-instr && export MOSTREAM_HOME=$PWD && B=Benchmarks/SpikeDetection
mojo build -O3 -I . -I $B $B/spike_detection_multisource.mojo -o bin/sdmi_vyukov
mojo build -O3 -I . -I $B -D MOSTREAM_RIGTORP=1 $B/spike_detection_multisource.mojo -o bin/sdmi_rigtorp
python3 Benchmarks/SpikeDetection/strumentazione/run_instrumented.py $PWD $B/data/sensors.bin qstats.csv 10 3
```

Risultati del 3 novembre 2026: `../risultati/strumentazione_20261103.csv`.
