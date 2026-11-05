# wCQ: porting nativo in Mojo

Il codice dell'algoritmo è in [MPMC_queue_wcq.mojo](../MoStream/MPMC_queue_wcq.mojo):
inserimento, estrazione, gestione dei cicli, descrittori, aiuto e passaggio tra
percorso veloce e lento sono scritti in Mojo. È una versione sperimentale
autonoma che trasporta `UInt64`; il runtime di MoStream continua a usare la
propria coda attuale.

## Cosa resta in C

[wcq_atomics.c](../MoStream/lib/wcq_atomics.c) contiene soltanto allocazione
allineata, rilascio della memoria e primitive atomiche a 64/128 bit. Non contiene
una coda né richiama `wfring_enqueue` o `wfring_dequeue` dell'implementazione C.
Il compilatore verificato in questa macchina non offre `Atomic[UInt128]`.

Il supporto attuale richiede **Linux x86-64 little endian con CMPXCHG16B**.
Il costruttore verifica il supporto della CPU. La CAS doppia usa direttamente
l'istruzione `lock cmpxchg16b`, senza ricorrere a un eventuale mutex di
`libatomic`. Celle e coppie dei contatori sono allineate a 16 byte, mentre
l'allocazione e gli inizi dei descrittori sono allineati a 64 byte.

Le operazioni atomiche sono sequenzialmente consistenti, come nel modello
espositivo del paper. Questo semplifica la verifica degli ordinamenti, ma ha
un costo: le prestazioni devono ancora essere misurate e non si possono
dedurre dai grafici dell'articolo. Anche le chiamate alle primitive C hanno un
costo. Le letture delle coppie impiegano CMPXCHG16B, quindi richiedono memoria
scrivibile anche se il chiamante vuole soltanto leggere.

## Utilizzo e contratto

```mojo
var queue = WCQMPMCQueue(capacity=1024, num_threads=2)

# tid 0 appartiene al produttore; tid 1 al consumatore.
var rejected = queue.try_push(UInt64(42), 0)
# None significa inserito; un Optional presente restituisce il dato rifiutato.

var item = queue.try_pop(1)
if item:
    print(item.take())
```

La capacità deve essere una potenza di due almeno pari a 4; il numero dei
partecipanti deve essere compreso tra 1 e `min(capacità, 65535)`. Il valore predefinito
di `num_threads` è 1, quindi per usare più partecipanti occorre dichiararli.
Ogni partecipante concorrente deve usare **un `tid` esclusivo** in
`0 .. num_threads-1`. Il `tid` è un indice logico scelto dal chiamante, non il
numero di thread del sistema operativo. Due operazioni contemporanee non
possono condividere lo stesso descrittore. Dopo il termine di un partecipante
il suo indice può essere riutilizzato da un altro, senza sovrapposizione.

Tutti i valori `UInt64`, compresi zero e `2**64-1`, sono dati validi: i marcatori
di vuoto appartengono alle code degli indici e non al dato applicativo.
`try_push` può fallire quando non trova un indice libero; durante operazioni
concorrenti alcuni indici possono essere in transito tra i ring. Non espone un
conteggio istantaneo degli elementi. `try_pop` restituisce un `Optional`.

`push` e `pop` ripetono attivamente i tentativi finché trovano spazio o dati:
**queste due funzioni di attesa non sono wait-free**. Una coda piena senza
consumatori o vuota senza produttori può farle attendere indefinitamente.
In un runtime cooperativo questo va considerato prima di usarle.

La coda possiede le proprie allocazioni: si sposta con `queue^`, non si copia.
Spostamento e distruzione richiedono che tutti gli accessi concorrenti siano
terminati. Nessuna operazione della coda alloca nuovi nodi o descrittori.

## Dal dato ai due ring

Per capacità `n`, la struttura possiede un array di `n` dati e due `_WCQRing`:

```text
try_push(x, tid):
    indice = free_indices.dequeue(tid)
    se assente: restituisci x
    data[indice] = x
    allocated_indices.enqueue(indice, tid)

try_pop(tid):
    indice = allocated_indices.dequeue(tid)
    se assente: restituisci vuoto
    x = data[indice]
    free_indices.enqueue(indice, tid)
    restituisci x
```

All'inizio `free_indices` contiene tutti gli indici e `allocated_indices` è
vuoto. Ogni indice si trova in un solo ring oppure è temporaneamente posseduto
da un'operazione. Questo limita il numero degli indici che possono essere
inseriti in ciascun ring e permette di usare l'interfaccia interna senza un
controllo del pieno aggiuntivo.

Ogni ring usa `2*n` celle fisiche pur trasportando al massimo `n` indici. Questa
riserva di spazio fa parte del disegno SCQ/wCQ: non offre `2*n` posti all'utente.

## Ticket, celle e cicli

Head e Tail assegnano ticket mediante fetch-and-add. Un ticket prenota un
tentativo; l'inserimento vero richiede ancora una modifica atomica della cella.
La posizione fisica è ricavata dal ticket con una permutazione che separa
celle adiacenti su linee di cache diverse, seguendo il riferimento C.

Una cella è una coppia di 128 bit:

| Parola | Contenuto |
| --- | --- |
| `Note` | Ciclo per cui una decisione di scartare la cella è già stata presa |
| `Value` | Ciclo, bit `IsSafe`, bit `Enq`, indice o marcatore di vuoto |

Le funzioni `_cycle` e `_slot` ricostruiscono il ciclo e la posizione. I due bit
meno significativi dei **contatori locali dei descrittori** rappresentano
`FIN=1` e `INC=2`; perciò un incremento logico dei contatori globali vale **4**.
Questi bit dei contatori sono distinti dai flag contenuti nel valore delle celle.
I confronti usano differenze con segno, come `__wfring_cmp` nel riferimento.
Richiedono distanze inferiori a `2**63` nelle unità del contatore. I ticket
finiti non offrono protezione contro un lettore sospeso attraverso un intero
riavvolgimento del contatore.

## Percorso veloce

`enqueue` e `dequeue` eseguono il procedimento SCQ per un numero limitato di
tentativi: rispettivamente 16 e 64 per impostazione predefinita.
Le celle vengono controllate e aggiornate atomicamente. Se un consumatore
arriva troppo presto, può invalidare un tentativo: il produttore deve scegliere
un altro ticket, anziché attendere che quel consumatore si ritiri.

`Threshold`, inizializzato a `3*n-1` dopo un inserimento, limita le ricerche
infruttuose dei consumatori e permette di riconoscere il vuoto. Non è la
dimensione della coda. `_catchup` avvicina Tail a Head quando serve e ha un
limite esplicito di iterazioni.

## Percorso lento e aiuto

Dopo i tentativi veloci il thread pubblica nel proprio descrittore il tipo di
richiesta, l'indice da inserire quando necessario, il ticket iniziale e una
coppia di numeri di sequenza. Gli altri partecipanti visitano periodicamente i
descrittori con `_help`. `help_delay` regola la frequenza delle visite.

`_slow_inc` coordina gli aiutanti: la stessa richiesta deve prenotare un solo
ticket per iterazione, anche quando diversi thread la eseguono insieme.

1. Il contatore locale riceve il nuovo ticket e il flag `INC`.
2. Una CAS doppia aggiorna il contatore globale e pubblica l'identità di chi
   possiede il descrittore della seconda fase.
3. Chiunque trovi questa seconda fase può contribuire a togliere `INC` e a
   rimuovere il riferimento dal contatore globale.
4. `FIN` segnala che la richiesta è completata e arresta ulteriori prenotazioni.

Il descrittore della seconda fase appartiene al **thread che esegue l'aiuto**,
come in figura 7 del paper; il contatore locale aggiornato appartiene invece
al thread aiutato. Queste due identità non devono essere confuse.

Il riferimento pubblicato nel contatore globale codifica l'identità
dell'aiutante nei 16 bit bassi e la generazione della seconda fase nei 48 bit
alti. La lettura controlla che la generazione del riferimento corrisponda a
quella del descrittore. Questo evita che un aiutante sospeso su un vecchio
riferimento completi per errore il descrittore già riutilizzato per un'altra
prenotazione. Come per i ticket, si assume che un lettore sospeso non attraversi
un intero riavvolgimento dei 48 bit della generazione.

Un inserimento lento pubblica prima una cella con `Enq=0`. Poi finalizza la
richiesta e la cella. Se un consumatore arriva nel mezzo, `_finalize` aiuta a
segnare `FIN` prima di consumare. `Note` rende persistente una decisione di
saltare una cella, evitando che un aiutante ritardatario la usi diversamente.

## Scelte rispetto al riferimento

La base è il [sorgente C indicato dall'utente](https://github.com/rusnikola/lfqueue/blob/708c0052872950dcb15b487fa7a5dd77ce2a2746/wfring_cas2.h),
confrontato con le figure 3--7 del [paper](https://arxiv.org/abs/2201.02179).
Non è una traduzione letterale di ogni riga:

- L'elenco circolare dei partecipanti usa indici preallocati anziché puntatori.
- Il riferimento alla seconda fase contiene anche la generazione, per validare
  il riuso dei descrittori rispetto a una lettura ritardata del contatore globale.
- Tutti i campi condivisi dei descrittori hanno accessi atomici.
- Le primitive usano ordinamento sequenziale e CAS forti.
- L'incremento lento è 4, coerente con l'incremento veloce e con i flag locali.
- L'aiuto della seconda fase usa il descrittore dell'aiutante, seguendo il paper.
- Il percorso lento distingue una cella invalidata prima dell'inserimento da
  una cella effettivamente pubblicata e finalizza la richiesta già riconosciuta.
- Gli aiutanti ricontrollano la generazione dopo `_slow_inc`. Un tentativo di
  segnalare vuoto che perde la CAS su `FIN` deve seguire il ticket più recente:
  non può considerare conclusa una prenotazione avanzata da un altro aiutante.
- `patience_enq=0` e `patience_deq=0` permettono di collaudare il percorso lento.
  In questa modalità il contatore locale parte da un marcatore che include la
  generazione della richiesta e usa il bit alto. Il marcatore deve essere
  diverso anche dal primo ticket prenotato: altrimenti due aiutanti potrebbero
  confondere l'inizializzazione con una prenotazione già conclusa e saltare una
  cella. `_slow_inc` assegna anche il primo ticket. Questa modalità di collaudo
  richiede contatori reali inferiori a `2**63` e generazioni inferiori a `2**61`.

Queste scelte devono essere considerate in una revisione della correttezza.
La dimostrazione del paper non viene automaticamente trasferita a questo
codice. Lo stato del porting è **sperimentale**, non una certificazione formale
di linearizzabilità o wait-freedom dell'implementazione.

## Compilazione e test

Il runner compila il supporto atomico e antepone il modulo Mojo ai test in una
cartella temporanea, come gli altri runner sperimentali del progetto. Non
richiede l'importazione del runtime MoStream né ne modifica la configurazione.

```sh
python3 Tests/run_wcq_tests.py --mojo /percorso/al/compilatore/mojo
```

Il compilatore usato per la verifica locale è
`Mojo 1.0.0b3.dev2026072006 (581d26cd)`. Il `mojo` predefinito della macchina è
0.26.2 e ha API incompatibili; va indicato il compilatore appropriato. La
suite è stata successivamente compilata ed eseguita anche con Mojo 1.1.0
(`8189361e`). `UnsafePointer` produce avvisi di deprecazione su questa versione;
il confronto prestazionale usa Mojo 1.1 come il precedente confronto Rigtorp.

Per compilare soltanto il supporto atomico:

```sh
make -C MoStream/lib wcq
```

Un programma che importa questo modulo deve collegare quell'oggetto con
`-Xlinker MoStream/lib/wcq_atomics.o`. Le importazioni del pacchetto MoStream
richiedono inoltre un compilatore compatibile con il resto del progetto.

L'[esempio autonomo](../Tests/example_wcq.mojo) importa direttamente il modulo
senza importare il runtime delle pipeline. Il runner esegue anche questo
esempio, verificando l'importazione reale del modulo oltre ai test del
protocollo interno. Con il compilatore locale l'importazione dalla directory
del progetto, che contiene spazi nel percorso, fallisce: il runner copia il
modulo in una directory temporanea e usa quella come percorso di importazione.

I test controllano FIFO sequenziale, pieno/vuoto, riuso su molti giri, estremi
`UInt64`, spostamento e argomenti invalidi. Pubblicano anche richieste di un
thread sospeso e ne verificano il completamento da parte di un altro.
Il banco concorrente avvia veri pthread che eseguono funzioni **Mojo**:
4 produttori/4 consumatori per capacità 8, 32 e 1024, e 4 produttori/1 consumatore
per verificare l'ordine di ogni produttore. Controlla ogni valore singolarmente
per rilevare perdite e duplicati. Prova parametri normali, percorso lento
forzato e passaggio rapido al lento; conta anche le richieste aiutate.

Sono presenti prove deterministiche anche per la seconda fase lasciata a metà,
per il riuso del descrittore mentre resta un riferimento vecchio, per un
inserimento sospeso con `Enq=0` completato dal consumatore, per una cella
invalidata prima della pubblicazione e per un rilevamento del vuoto che gareggia
con l'avanzamento del ticket locale.

Per il controllo degli accessi alla memoria, il runner supporta
`--sanitize address`, strumentando anche i file C. Nell'ambiente sandbox locale
LeakSanitizer non può ispezionare i processi: la prova AddressSanitizer è stata
eseguita con `ASAN_OPTIONS=detect_leaks=0`. Questo controllo non verifica le
perdite di memoria e non strumenta gli accessi interni all'assembly della CAS
doppia.

I test hanno timeout; un blocco è un fallimento, non un risultato da ignorare.
Le prove concorrenti non esauriscono tutti gli interleaving e non misurano le
prestazioni. La licenza del codice derivato è conservata in
[WCQ_LICENSE.txt](WCQ_LICENSE.txt).

Esito della verifica locale del 21 ottobre 2026: suite ottimizzata e suite con
AddressSanitizer superate, con 12 scenari concorrenti e 72.000 valori verificati
per ciascuna esecuzione, oltre ai controlli sequenziali e alle sospensioni
deterministiche. Anche l'esempio con importazione del modulo è stato compilato
ed eseguito correttamente nella verifica ottimizzata.

La verifica Mojo 1.1 ha richiesto di mantenere vivi i ring dei test finché
terminano le letture atomiche tramite indirizzi grezzi: questi indirizzi `UInt64`
non mantengono un'origine di proprietà e la distruzione anticipata all'ultimo
uso poteva liberare il ring prima di una lettura finale nel test. Le operazioni
pubbliche della coda mantengono il prestito del proprietario durante la chiamata.
