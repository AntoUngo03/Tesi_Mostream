# Rigtorp nei communicator MoStream

Il backend Rigtorp è selezionabile per le code fra gli stadi della libreria,
attraverso `CommunicationQueue`. Il backend predefinito resta Vyukov.
La selezione avviene in compilazione: non aggiunge un dispatch a runtime a ogni
messaggio e non alloca una seconda coda inutilizzata.

Dalla radice del progetto, con la toolchain Mojo 1.1 usata per i test Rigtorp:

```sh
# Stesso programma, backend Vyukov
mojo build -I . Tests/test_pipe_1.mojo -o /tmp/pipeline-vyukov
# Stesso programma, backend Rigtorp
mojo build -I . -D MOSTREAM_RIGTORP=1 Tests/test_pipe_1.mojo -o /tmp/pipeline-rigtorp
/tmp/pipeline-vyukov
/tmp/pipeline-rigtorp
```

La sola presenza della definizione MOSTREAM_RIGTORP seleziona Rigtorp: anche
MOSTREAM_RIGTORP=0 lo seleziona. Omettere la definizione per usare Vyukov.
Il runtime stampa il backend selezionato all'avvio.

## Ambito

La selezione interessa `Communicator`, quindi `Pipeline.run()` e
`Pipeline.run_cooperative()`, inclusi i payload `MessageWrapper[T]`, la chiusura
con `producer_finished`, il rilevamento EOS e la distruzione del communicator.
Le ready/wait queue dello scheduler cooperativo restano Vyukov: il confronto
riguarda esclusivamente la comunicazione fra gli stadi.

`Communicator.push` usa il push bloccante del backend. `Communicator.pop`
continua a usare try_pop e il protocollo di chiusura: il pop bloccante Rigtorp
non consente di osservare la chiusura di uno stream senza ulteriori elementi.
Il runtime cooperativo usa try_push/try_pop, senza prenotare ticket bloccanti
che richiedano il progresso di un altro actor sul medesimo worker.

Per un confronto omogeneo usare capacità uguali, potenze di due e almeno 2,
come richiesto da Vyukov. Non cambiano i parametri dell'API Pipeline.

## Test di integrazione

```sh
python3 Tests/run_runtime_queue_tests.py --mojo /percorso/mojo-1.1
```

Il runner compila entrambi i backend e prova 144 configurazioni, con una farm
e con due trasformazioni parallele consecutive (collegamento MPMC): runtime
standard/cooperativo, 1/2/4 trasformazioni replicate, 0/1/2003 input e capacità
2/16. La pipeline pubblica trasforma Int in String; il sink verifica che ogni
ID sia arrivato esattamente una volta e che non ci siano valori estranei.
Lo stream vuoto verifica la chiusura senza messaggi. Il timeout rileva anche
mancata terminazione. Servono almeno 10 worker Mojo per le prove standard;
le prove cooperative usano due scheduler worker.

Il 26 ottobre 2026 tutte le **144 configurazioni sono passate** sulla macchina
ungolo@131.114.2.95 con Mojo 1.1.0 (8189361e). Il log e gli hash dei sorgenti
sono in `Tests/risultati_runtime_queue/20261026-mojo1.1.log` e `.json`.

Per compilare la libreria con questa toolchain sono stati aggiornati anche gli
import TaskGroup (`std.runtime._asyncrt`) e parallelism_level (`std.runtime`),
i tipi degli atomici (`Atomic[UInt64]`/`Atomic[Int64]`) e le chiamate store.
L'algoritmo Vyukov, gli ordini di memoria e il protocollo EOS restano gli stessi.
L'integrazione ora richiede Mojo 1.1; la compatibilità con toolchain precedenti
non è stata verificata. `_asyncrt` è ancora un'API privata del runtime Mojo.

## Confronto con i benchmark sintetici

Prima superare i test di integrazione. Poi compilare lo stesso workload
applicativo MoStream due volte cambiando soltanto la definizione del backend.
Per esempio, gli esempi in Benchmarks/ImagePipeline usano la vera libreria.
Mantenere uguali input, capacità, parallelismo, pinning e runtime. Alternare
l'ordine delle due esecuzioni e ripetere le misure; verificare gli output.

Misurare il tempo della pipeline e calcolare throughput = input completati /
tempo in secondi. Allocazioni del workload, letture/scritture di file e verifica
dei risultati devono avere un ambito dichiarato e uguale fra backend.
I tempi di compilazione e avvio del processo non vanno confusi con il tempo
del runtime. Le mediane delle ripetizioni e la dispersione aiutano a distinguere
un vantaggio ripetibile da variabilità della macchina.

I vecchi numeri PipelineGrain/FarmThroughput usano un harness dedicato e non
sono misure della libreria completa. Il primo confronto attraverso Pipeline.run è ora in
`Benchmarks/RuntimeQueues/results/Throughput_pipeline/`, con metodo e limiti
in `Benchmarks/RuntimeQueues/README.md`. Non emerge un vantaggio uniforme
di Rigtorp e questa prova preliminare non stabilisce una classifica generale.
