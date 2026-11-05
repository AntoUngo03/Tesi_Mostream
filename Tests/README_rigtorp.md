# Test di correttezza Rigtorp

La suite richiede la toolchain Mojo 1.1 del progetto. Il runner concatena
la coda e il test in sorgenti temporanei e compila ogni suite una sola volta.
Non utilizza il runtime di MoStream: i task sono gestiti da TaskGroup di Mojo.

```bash
python3 Tests/run_rigtorp_tests.py --mojo /percorso/mojo
```

La configurazione predefinita verifica N=1..50, con N produttori e N consumatori,
10.000 interi totali per prova, capacità 1, 3, 8 e 64, entrambe le API
push/pop e try_push/try_pop, e tre ripetizioni: 1.200 prove concorrenti.
Le immagini e i benchmark di prestazioni non sono coinvolti.

Il produttore j inserisce gli interi i nell'intervallo [0, messages) per cui
i % N == j. I consumatori condividono contatori atomici e verificano che ogni
intero sia ricevuto esattamente una volta. Un valore fuori intervallo è registrato
come errore senza usarlo come indice. Si controlla anche che la coda finale sia
vuota. Le quote di estrazione sommano esattamente a messages, anche quando non è
divisibile per N; non impongono quali ID debba ricevere ciascun consumatore.
Le capacità piccole esercitano ripetutamente il riutilizzo delle celle.

Entrambe le modalità impiegano attesa attiva: riprovare try_* non costituisce
esecuzione cooperativa. Per impostazione predefinita il runner richiede almeno 2*N worker Mojo e rifiuta una
matrice troppo grande senza saltare casi silenziosamente. Per rispettare questa precauzione, con N=50 servono almeno
100 worker; il numero di worker non coincide necessariamente con quello dei core.
Una prova con meno task, utile anche per una prima verifica:

```bash
python3 Tests/run_rigtorp_tests.py --mojo /percorso/mojo --max-n 2 --repeats 1
```

Ogni configurazione viene eseguita in un processo separato con timeout di 60
secondi, modificabile con --timeout. Un blocco, compreso quello causato da una
perdita di elementi, fallisce la suite. Un timeout non distingue automaticamente
un deadlock da un'esecuzione molto lenta. Il primo errore interrompe il runner;
la riga precedente identifica ripetizione, N, capacità e modalità.
Questi sono test di correttezza sotto contesa, non una dimostrazione formale
né una misura di scalabilità.

## Esecuzione remota

Il runner può trasferire i soli sorgenti necessari in una directory temporanea
remota, compilare ed eseguire la suite, e rimuovere la directory al termine:

```bash
python3 Tests/run_rigtorp_tests.py --ssh ungolo@131.114.2.95 --max-n 28
```

La macchina `xilinx` espone Mojo 1.1.0 (8189361e) e 56 worker: con l'attuale
controllo conservativo dell’harness ammette al massimo N=28, ovvero 672 prove con
le altre opzioni predefinite. Per provare N=29..50 con 56 worker si usa
--allow-oversubscription, come descritto sotto.

## Più task dei worker

Il limite 2*N <= worker è una precauzione dell'harness, non una soglia certa di
blocco. È possibile provare esplicitamente configurazioni oltre tale limite:

```bash
python3 Tests/run_rigtorp_tests.py --ssh ungolo@131.114.2.95 --min-n 29 --max-n 50 --allow-oversubscription --timeout 15
```

L'opzione non aumenta il numero di worker e non introduce cooperazione nei loop
try_*. Mantiene i controlli di correttezza e il timeout; la suite si interrompe
al primo errore o timeout. Un successo non garantisce il progresso con qualsiasi
ordine di schedulazione. Senza l'opzione resta attivo il controllo conservativo.

## Campagna con 50 ripetizioni

Il 22 ottobre 2026 su xilinx con Mojo 1.1.0 e 56 worker sono passate tutte
le 20.000 prove: N=1..50, capacità 1/3/8/64, due modalità e 50 ripetizioni.
Ogni prova elabora 10.000 interi. Nessun errore o timeout (15 secondi per prova).
Sono passati anche i test sequenziali. Log completo, comando e hash dei sorgenti
sono in `risultati_rigtorp/20261022-n1-50-ripetizioni50.log` e `.json`.
