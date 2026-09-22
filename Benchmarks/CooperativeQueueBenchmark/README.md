# PaddedFAA cooperativa

Il codice nuovo e' in [Cooperative_FAA_queue.mojo](../../MoStream/Cooperative_FAA_queue.mojo).
Questa prima campagna usa un runtime isolato. L'integrazione successiva in
MoStream e le nuove misure sono documentate nel
[confronto della pipeline reale](../RealCooperativeQueueBenchmark/README.md).
I risultati qui conservati restano quelli della campagna precedente, con gli
hash dei sorgenti usati allora nel manifest.

## Varianti

| Nome | Prenotazione | Attesa | Slot |
|---|---|---|---|
| Vyukov | CAS originale, `try_push/try_pop` | fallimento restituito all'actor; `try_pop` puo' riprovare internamente | compatti |
| PaddedFAA | CAS originale nei `try_*` | come sopra | padded |
| CAS-bounded | al massimo un CAS per poll | `RETRY` distingue contesa da `WAIT` | padded |
| FAA-cooperative | una FAA per operazione | ticket conservato nell'actor, poll senza loop | padded |

La variante CAS-bounded usa `CooperativeFAAQueue[T, False]`; la FAA usa
`CooperativeFAAQueue[T]`. Riutilizzano lo storage della PaddedFAA originale:
stessa maschera, padding e protocollo acquire/release sugli slot. I metodi
bloccanti dell'oggetto interno non devono essere mescolati con questa API.

## Operazioni persistenti

Ogni producer possiede una `PushOperation[T]`, ogni consumer una `PopOperation[T]`.
Quando il producer ha un nuovo messaggio, lo sposta in `operation.item` e chiama
`poll_push`. In caso di `WAIT` o `RETRY`, l'actor conserva l'operazione e cede il
worker. In caso di `SUCCESS`, il payload e' stato trasferito alla coda.

Il consumer chiama `poll_pop`: con `SUCCESS` prende `operation.item.take()`,
con `WAIT/RETRY` cede il worker, con `CLOSED` termina. Dopo un successo lo stesso
contenitore puo' essere riusato per l'operazione successiva.

`WAIT` significa che lo slot atteso non e' disponibile; non promette una
fotografia esatta di coda piena/vuota. `RETRY` nella variante CAS significa
prenotazione contesa o posizione osservata diventata obsoleta. La FAA non ha
CAS di prenotazione e non restituisce `RETRY`.

Un ticket FAA viene assegnato solo al primo poll, e mantenuto in tutti i poll
successivi. Non e' possibile annullare, copiare o abbandonare un'operazione
pendente. Non si puo' usarla su un'altra coda o contemporaneamente da piu' worker.
La coda non e' lock-free: un proprietario sospeso puo' impedire il riuso del suo slot.
Il wrap-around fisico e' testato; l'overflow dei contatori UInt64 non e' supportato.

## Chiusura

Il costruttore riceve il numero dei producer. Ciascuno chiama `producer_finished`
esattamente una volta, dopo il completamento dell'ultimo push, senza inviare altri
messaggi. L'ultimo decremento chiude definitivamente la coda.

Un consumer che ha gia' prenotato oltre l'ultimo ticket pubblicato riceve `CLOSED`.
Non si tratta di cancellare un buco in una coda ancora aperta: dopo la chiusura
non possono arrivare producer che usino quella posizione. I ticket precedenti
devono comunque essere consumati dai rispettivi proprietari. La memoria si
libera solo dopo il completamento di tutti gli actor e dei relativi task.

## Scheduler dell'esperimento

`compare.mojo` assegna gli actor stabilmente ai worker. Ogni worker li visita
round-robin e prova al massimo `batch` messaggi per attivazione, fermandosi al
primo `WAIT/RETRY`. Gli actor sospesi vengono sempre rivisitati: nessun ticket
pronto puo' restare senza un futuro tentativo per un risveglio perso.

E' un benchmark cooperativo isolato, non `Pipeline.run_cooperative`.
Non include ready queue, wait queue, parcheggio, work stealing, notifiche o
calcolo di stage. `WAIT` e `RETRY` interrompono entrambi l'attivazione: qui si
misura il budget limitato del CAS, ma non il risparmio di parcheggi ottenibile
distinguendoli nel runtime completo. Il batch e' applicato a tutte le varianti;
non e' una prenotazione di piu' elementi con una sola atomica.

L'integrazione successiva nel runtime usa stati persistenti nell'actor, EOS
basato sul completamento delle prenotazioni e risvegli broadcast dei waiter.
Questi costi sono misurati nella campagna della pipeline reale, non qui.

## Riproduzione

Dalla radice del repository, con Mojo 1.0 e Python con matplotlib:

```bash
MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache mojo build --Werror -I . \
  Tests/Test_Queue/test_cooperative_faa_queue.mojo -o /tmp/test-cooperative-faa
/tmp/test-cooperative-faa
python3 Benchmarks/CooperativeQueueBenchmark/run.py
```

Il runner compila con `--Werror -O3`, esegue 40 casi di validazione separati
(exact-once con un contatore per ID, inclusi input vuoti), poi un warm-up per
caso/variante e sette blocchi misurati. Ogni blocco randomizza l'ordine dei casi
e delle quattro varianti. Ogni processo ha un timeout: fallimenti e timeout
interrompono la campagna, non vengono eliminati dalle statistiche.

Il payload e' `MessageWrapper[Value]`, dove Value contiene ID e timestamp
campionato. Per il layout misurato, payload=32 byte, slot compatto=48 byte,
slot padded=64 byte. Il runner controlla che lo stride padded sia multiplo di 64.
Non e' una verifica del layout per qualunque T: eventuali payload con diverso
allineamento richiedono una verifica separata.

La regione temporizzata include dispatch/join dei worker, polling, contatori
locali, campionamento della latenza e chiusura. Le allocazioni e la validazione
finale sono escluse. Le code non selezionate vengono costruite fuori dal timer
e mai accedute nella regione misurata. Nelle prove temporizzate si verificano
conteggio e checksum, non si esegue l'atomica exact-once per messaggio.

Non e' impostata affinita' individuale dei worker. L'affinita' ereditata,
hardware, compilatore, parametri e hash dei sorgenti sono nel manifest.
Le repliche degli actor possono superare i worker; questi ultimi devono poter
essere eseguiti dal thread pool. I risultati non dimostrano il comportamento
con un pool che non riesce ad avviare tutti i task worker.

## Risultati

Nella prima campagna, con 4 producer, 4 consumer, 4 worker, capacita' 1024 e
batch 8, il throughput medio e' 1.560 Mmsg/s per Vyukov, 1.742 per PaddedFAA,
1.558 per CAS-bounded e 3.112 per FAA-cooperative. Il rapporto accoppiato della
FAA e' 1.99 rispetto a Vyukov (IC95% 1.92-2.07) e 1.79 rispetto a PaddedFAA.

Con un solo worker la FAA e' invece piu' lenta delle due baseline in media
in tutti i casi provati: non c'e' contesa tra worker da compensare con la
prenotazione FAA. Nei casi 8 producer / 2 consumer, con 2 o 4 worker, il
vantaggio medio rispetto a Vyukov e' circa 1.21-1.50. Queste osservazioni
sono specifiche della campagna salvata, non una garanzia generale.

Il maggiore throughput non assicura una minore latenza in ogni configurazione:
per esempio, con 4/4 actor, 4 worker, capacita' 1024 e batch 1, la FAA ha un
p99 campionato medio di 11.98 us contro 7.37 us di Vyukov. La tabella completa
riporta entrambe le metriche, comprese le regressioni della variante CAS-bounded.

- [Tabella e interpretazione](results/RESULTS.md)
- [Grafico con intervalli](results/comparison.png)
- [Tutte le misure](results/results.csv)
- [Metriche aggregate](results/summary.csv)
- [Ambiente e hash](results/manifest.json)

Le misure distinguono messaggi trasferiti al secondo da singole operazioni:
un messaggio conta un push e un pop. La latenza campionata include il tempo
dal primo tentativo di push al completamento del pop, quindi anche le attese
dell'actor. I risultati riguardano questa macchina e questo carico sintetico.
