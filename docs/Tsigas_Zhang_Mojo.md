# Porting Tsigas–Zhang in Mojo

Il modulo `MoStream/MPMC_queue_tsigas_zhang.mojo` implementa una coda limitata
MPMC sperimentale per Mojo 1.1.0, con commenti in italiano. È separato dal
Communicator e dai backend dei benchmark esistenti.

Fonte primaria: Philippas Tsigas e Yi Zhang, *A Simple, Fast and Scalable
Non-Blocking Concurrent FIFO Queue for Shared Memory Multiprocessor Systems*,
SPAA 2001, DOI 10.1145/378580.378611:
[versione pubblicata, figure 5–7](https://www.cse.chalmers.se/~tsigas/papers/spaa01.pdf).
Si segue questa versione, con `vnull`, non la variante `latest-spaa01.pdf`, che
codifica diversamente il colore. Gli appunti locali
`Tsigas_Zhang_pseudocodice_commentato.txt` descrivono lo stesso protocollo.

## Rappresentazione e scelte del porting

- `head` è la guida di lettura e `tail` quella di scrittura: i nomi seguono il paper.
- Le guide possono restare arretrate; le operazioni cercano la posizione effettiva
  e aiutano gli altri thread ad avanzare le guide.
- Una CAS sulla cella pubblica o rimuove il dato. Nel percorso normale si tenta
  l'aggiornamento della guida ogni due posizioni (`m=2`).
- `0` e `1` sono marcatori interni di vuoto. Il colore cambia al passaggio per zero,
  coordinato da `vnull`. Non sono puntatori da dereferenziare.
- Per evitare allocazioni per elemento e problemi di reclamation dei puntatori,
  questa prima versione contiene **scalari UInt64**, non oggetti generici.
  Il valore `x` viene codificato come `x+2`: zero e uno applicativi sono validi.
  Sono accettati i valori da `0` a `18446744073709551613` inclusi (`2**64-3`).
  I due valori superiori producono `Error` prima di modificare la coda.
- Tutti gli accessi condivisi usano atomiche con `Ordering.SEQUENTIAL`, incluse
  le CAS e `vnull`. È una scelta conservativa, senza ottimizzazioni degli ordini.
- Nessun lock globale, nessuna allocazione durante push/pop. Il padding distanzia
  i contatori, ma non impone allineamento a una cache line.

Il costruttore riceve la **lunghezza fisica** dell'anello, potenza di due almeno 4.
La capacità utile è **L−2**, per le celle necessarie al protocollo. Con
`TsigasZhangMPMCQueue(1024)` si possono contenere 1022 valori. Non confrontare
questa configurazione con code da 1024 valori attribuendo loro capacità uguali.

## API

| Operazione | Risultato |
|---|---|
| `TsigasZhangMPMCQueue(ring_size)` | costruisce la coda, può sollevare Error |
| `try_push(item)` | None al successo; Optional con il valore rifiutato se piena; Error per valore non codificabile |
| `try_pop()` | Optional con il valore, oppure None se vuota |
| `push(item)` | ripete try_push con attesa attiva; può sollevare Error |
| `pop()` | ripete try_pop con attesa attiva |
| `size()` | scansione O(L), conteggio esatto solo senza concorrenza |
| `empty()` | osservazione basata sulla scansione, non autorizza un pop successivo |

Spostamento e distruzione richiedono la fine di ogni accesso concorrente.
Non usare le operazioni bloccanti dentro un attore cooperativo senza studiare
l'interazione con lo scheduler.

## Limite ABA del protocollo originale

Il Teorema 1 del paper assume che un'operazione non rimanga sospesa mentre
ne vengono completate più di `l`, legato alla lunghezza dell'anello. I due
colori non forniscono protezione illimitata da ABA: dopo abbastanza giri
possono ripetersi sia gli indici sia i marcatori. Il porting conserva questo
limite e **non è una coda MPMC con correttezza garantita sotto pause arbitrarie**.
Le atomiche sequenzialmente consistenti non eliminano questo problema.

Di conseguenza il passaggio dei test non autorizza una garanzia generale di
linearizzabilità o progresso. Non si dichiara questo backend pronto per il
runtime o equivalente a Rigtorp/Vyukov quanto a contratto di correttezza.

## Test

Dalla radice `MoStream-main`, con Mojo 1.1.0:

```sh
python3 Tests/run_tsigas_zhang_tests.py
```

Dalla macchina locale con accesso SSH alla toolchain aggiornata:

```sh
python3 Tests/run_tsigas_zhang_tests.py --ssh ungolo@131.114.2.95
```

Il runner antepone il modulo ai test, compila due eseguibili e li avvia in
cartelle temporanee. Non importa il package MoStream né modifica il repository
remoto. La versione locale Mojo 0.26.2 non è la toolchain di riferimento.

Il test sequenziale verifica anelli da 4, 8, 16 e 64 celle, 200 cicli completi
ciascuno, FIFO, pieno/vuoto, payload ripetuti, zero/uno, massimo valore,
spostamento di una coda usata, distruzione non vuota e parametri rifiutati.

Sei scenari concorrenti usano quattro produttori e quattro consumatori. Un
registro atomico controlla ogni ID esattamente una volta; il valore è verificato
prima di usarlo come indice. Si provano push/pop e try_*, anelli da 64 e 16384
celle, avvio normale e vicino allo zero per attraversare il confine fisico.
Ogni scenario gira in un processo distinto con almeno otto worker; la barriera
parte solo dopo che tutti i task sono pronti. Il runner applica un timeout.

Il totale dei successi durante ciascuna fase concorrente è inferiore alla
lunghezza dell'anello (16+16 oppure 4000+4000); l'avvicinamento al confine è
sequenziale. Questo esercita contesa e attraversamento dello zero, senza
presentare stress con innumerevoli giri come prova contro l'ABA. Il totale
trasferito dalle sei prove è 16.032 valori. La verifica MPMC di perdite e
ripetizioni non dimostra da sola l'ordine globale di una storia concorrente.

Nessun risultato prestazionale è stato raccolto: questi sono test funzionali,
non un nuovo confronto di throughput con Rigtorp o Vyukov.

Validazione del 16 ottobre 2026 sulla macchina condivisa con Mojo 1.1.0:
test sequenziali e sei scenari concorrenti superati. Gli hash dei sorgenti
verificati sono registrati in `Tsigas_Zhang_validazione.json`.

## Rafforzamento della scansione, 16 ottobre 2026

La validazione PipelineGrain con pinning ha rilevato che la trascrizione iniziale
poteva saltare un valore pubblicato fra una lettura di vuoto e la lettura del
confine di scrittura. `try_pop` ora acquisisce uno snapshot di `tail` prima di
leggere la cella e invalida la scansione se cambia `head` oppure `tail`.
La correzione aggiunge letture di coerenza, senza mutex o nuove CAS per elemento.
È un rafforzamento esplicito del porting, non una copia letterale delle figure.
Il limite ABA sotto sospensioni arbitrarie rimane.

Il runner compila a `-O3`; oltre ai sei scenari 4P/4C esegue una regressione
SPSC che verifica l'ordine esatto di 2.000 valori (settimo processo isolato).
Il totale dei valori concorrenti passa quindi a 18.032.
