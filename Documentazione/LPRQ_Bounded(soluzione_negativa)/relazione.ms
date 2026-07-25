.TL
BoundedLPRQInspired: riduzione bounded exact-turn della PRQ in Mojo
.AU
Documentazione tecnica per attività di tesi
.AI
Progetto MoStream - 22 luglio 2026
.AB
Questa relazione documenta BoundedLPRQInspired, un prototipo MPMC bounded e
riusabile ispirato al protocollo di cella della Portable Ring Queue usata dalla
LPRQ di Raed Romanov e Nikita Koval. La LPRQ pubblicata collega dinamicamente
segmenti PRQ ed è unbounded rispetto al numero di segmenti allocabili. Il
prototipo MoStream usa invece un solo ring preallocato, gate CAS e turno FIFO
esatto; non implementa skip, UNSAFE e chiusura dei segmenti e non eredita la
proprietà lock-free della LPRQ. I test eseguiti non hanno rilevato perdita,
duplicazione o errori di riuso, ma il throughput è inferiore a tutte le
alternative migliori: zero vittorie su 18 configurazioni e distacco geometrico
medio di 3,63 volte dal vincitore. Il risultato riguarda questa riduzione
bounded, non le prestazioni della LPRQ originale.
.AE

.NH 1
Obiettivo dello studio
.PP
MoStream usa code bounded per comunicare fra stage e per strutture del runtime.
L'obiettivo dell'esperimento è verificare se alcune idee della LPRQ possano
essere trasferite a una coda con memoria fisica e occupazione logica limitate,
con payload Mojo generico Optional[T].
.PP
Il risultato implementato si chiama deliberatamente BoundedLPRQInspired. Non è
corretto chiamarlo LPRQ bounded senza qualificazione, perché la trasformazione
rimuove meccanismi essenziali dell'algoritmo pubblicato e modifica le garanzie
di progresso.

.NH 1
LPRQ originale e segmenti PRQ
.PP
La LPRQ di Raed Romanov e Nikita Koval è una lista dinamica di segmenti Portable
Ring Queue. Quando un producer supera il ring, il segmento viene chiuso
definitivamente, ne viene collegato uno nuovo e il precedente viene drenato e
poi abbandonato. Il numero di segmenti allocabili non è fissato alla costruzione;
in questo senso l'algoritmo completo è unbounded.
.PP
Il protocollo PRQ può marcare una cella UNSAFE e tollerare ticket saltati perché
il segmento non viene riciclato indefinitamente. Il bit può restare permanente:
la chiusura sposta il lavoro su un segmento nuovo.
.PP
Riutilizzare direttamente lo stesso segmento come ring bounded è pericoloso.
Un'operazione sospesa di una generazione precedente potrebbe riprendere dopo un
reset e pubblicare nella nuova generazione, producendo un problema ABA. Se
UNSAFE restasse permanente, il ring riusato perderebbe progressivamente celle
fino al livelock.
.PP
Una trasposizione più fedele richiederebbe almeno un pool fisso di segmenti,
generation tag per segmento e un protocollo sicuro di reclamation. Non è
l'oggetto del prototipo corrente.

.NH 1
Scelta exact-turn
.PP
BoundedLPRQInspired elimina skip, UNSAFE e close. Usa ticket monotoni ma ammette
un'operazione soltanto sulla cella e sull'epoca esatte associate al ticket FIFO
corrente. Il riuso diventa esplicito e bounded, al costo di head-of-line
blocking.
.PP
La coda è bounded in due sensi:
.IP 1. 4
memoria fisica fissa: un solo array di capacity celle, allocato nel costruttore;
.IP 2. 4
occupazione logica limitata: il gate impedisce più di capacity ticket producer
non ancora consumati.
.PP
La capacità deve essere una potenza di due e almeno pari a due. L'indice fisico
è ticket & (capacity-1), mentre il ticket completo identifica la generazione.

.NH 1
Struttura dati
.PP
I campi principali sono:
.DS L
cells       array allineato di BoundedLPRQCell[T]
capacity    numero fisso di celle
mask        capacity - 1
head        prossimo ticket consumatore
tail        prossimo ticket producer
.DE
.PP
I nomi seguono il codice corrente: tail avanza al push e head al pop. Entrambi
sono atomici UInt64 racchiusi in strutture padded da 64 byte, così producer e
consumer non aggiornano la stessa cache line.
.PP
Ogni cella contiene:
.DS L
state       token del ticket e fase, in un UInt64 atomico
index       epoca/ticket logico della cella, in un UInt64 atomico
data        payload generico Optional[T], non atomico
padding     isolamento al multiplo successivo di 64 byte
.DE
.PP
L'array è allocato con alignment=64. Il padding riduce il false sharing fra
celle adiacenti, ma aumenta il footprint e può esercitare pressione su cache e
TLB con capacità elevate. Il costruttore contiene assert compile-time che lo
stride di BoundedLPRQCell[T] sia multiplo di 64 byte e che il wrapper dei
contatori occupi esattamente 64 byte; un tipo T con layout incompatibile viene
quindi rifiutato in compilazione invece di invalidare silenziosamente il
padding.

.NH 1
Indice ed epoca della cella
.PP
All'inizializzazione, la cella fisica i contiene index=i e stato EMPTY. Per il
ticket producer t, la cella attesa è cells[t & mask] e deve avere index=t.
.PP
Dopo la riserva, il producer cambia index da t a t+capacity. Il consumer del
ticket t richiede proprio index=t+capacity. Dopo il consumo, index non viene
azzerato: t+capacity è già il ticket producer della generazione successiva
sulla stessa cella.
.DS L
vuota per producer t:       index = t
piena per consumer t:       index = t + capacity
vuota per producer t+C:     index = t + capacity
.DE
.PP
Lo stato distingue pieno e vuoto quando il valore di index coincide al confine
fra due generazioni.

.NH 1
Token e fasi
.PP
Tre bit bassi di state rappresentano la fase. I bit superiori contengono un
token derivato dal ticket:
.DS L
ticket_state(t, phase) = ((t + 1) << 3) | phase
phase(state)           = state & 7
.DE
.PP
Le fasi sono:
.TS
tab(|),box;
l r l.
Fase|Valore|Significato
EMPTY|0|nessun proprietario, cella riusabile
RESERVED|1|producer ha rivendicato la cella
WRITING|2|accesso esclusivo al payload in scrittura
FULL|3|payload pubblicato
READING|4|accesso esclusivo al payload in lettura
.TE
.PP
Il token rende lo stato specifico del ticket e riduce l'ABA fra generazioni.
WRITING e READING sono necessari perché Optional[T] non è nello stesso word
atomico del token: solo il proprietario della fase può accedere al payload non
atomico.
.PP
La codifica usa 61 bit effettivi per il ticket. Dopo 2^61 ticket il token di
stato si ripete per wrap-around. Il prototipo considera il limite impraticabile
nell'esperimento, ma il wrap non è formalmente supportato o verificato.

.NH 1
Gate bounded dei ticket
.PP
Il producer carica tail=t e head=h. Rifiuta il tentativo se h>t oppure se
t-h>=capacity. Il secondo controllo impedisce di riservare oltre la capacità
logica. Se la cella ha index=t e stato EMPTY, un CAS tail:t->t+1 assegna il
ticket.
.PP
Il consumer carica head=h e tail=t. Se h>=t non esiste un ticket producer
riservato. Anche quando h<t, il consumer procede soltanto se la cella di h
espone index=h+capacity e l'esatto stato FULL del ticket h. Un CAS head:h->h+1
assegna il consumo.
.PP
Questo gate è una differenza sostanziale dalla LPRQ: PRQ può tollerare buchi e
chiudere il segmento; il prototipo bounded deve impedire sovraprenotazione e
attendere il ticket FIFO esatto.

.NH 1
Protocollo push
.PP
try_push esegue un singolo tentativo prudente:
.IP 1. 4
legge tail e head e verifica il limite capacity;
.IP 2. 4
verifica index e EMPTY sull'epoca esatta;
.IP 3. 4
riserva tail con CAS;
.IP 4. 4
invoca publish_claimed sul ticket ormai irrevocabile.
.PP
publish_claimed transita EMPTY->RESERVED, cambia index da t a t+capacity,
transita RESERVED->WRITING, scrive data e pubblica lo stato FULL con store
RELEASE. Il metodo è statico e riceve soltanto puntatore, capacity, mask,
ticket e payload.
.DS L
EMPTY -> RESERVED -> WRITING -> FULL
index t             -> t + capacity
payload              scritto prima della RELEASE FULL
.DE
.PP
push è un ciclo sopra try_push. Se il ring è pieno o un tentativo perde il CAS,
conserva il payload e riprova; dopo 1024 spin chiama sleep(0.0).

.NH 1
Protocollo pop
.PP
try_pop verifica il ticket head esatto, index=t+capacity e lo stato FULL
contenente il token di t. Solo dopo questi controlli avanza head con CAS,
cambia FULL->READING, estrae il payload e pubblica EMPTY con store RELEASE.
.DS L
FULL -> READING -> EMPTY
payload estratto
index resta t + capacity per il producer della generazione seguente
.DE
.PP
pop ripete try_pop e applica lo stesso spin-then-yield. Se il producer del
ticket head è ritardato dopo la prenotazione di tail, i consumer non possono
saltarlo anche se altri producer hanno completato ticket successivi.

.NH 1
Semantica weak delle operazioni try
.PP
try_push e try_pop sono single-attempt e possono fallire temporaneamente. Un
try_push può restituire il payload anche se la capacità diventerebbe disponibile
subito dopo o se perde il CAS contro un altro producer.
.PP
try_pop può restituire None perché la coda è realmente vuota, perché il ticket
producer precedente è stato riservato ma non ancora pubblicato, oppure perché
il CAS su head è stato perso. None non prova quindi in modo linearizzabile che
la coda fosse vuota in un preciso istante. L'API va considerata weak try e il
chiamante deve essere disposto a ritentare, come fanno i test e il percorso
cooperativo di MoStream.
.PP
Questa try_pop non è quindi un rimpiazzo diretto per un componente che interpreta
subito None come end-of-stream. Tale integrazione richiede un segnale di chiusura
separato e polling ripetuto, oppure un risultato tri-state Item, Empty e
Pending/Contended.
.PP
Una volta vinto il CAS di prenotazione, invece, il ticket non può essere
abbandonato: publish_claimed deve completare.

.NH 1
Memory ordering
.PP
Il prototipo privilegia prudenza e usa Ordering.SEQUENTIAL per i contatori,
l'indice e la maggior parte dei CAS. Le osservazioni di state usano ACQUIRE;
la pubblicazione FULL e il ritorno a EMPTY usano RELEASE.
.PP
La store RELEASE di FULL rende il payload visibile al consumer che osserva lo
stato con ACQUIRE. La fase READING impedisce accessi concorrenti durante
l'estrazione. Un lavoro futuro può tentare ordinamenti più deboli, ma soltanto
dopo una revisione formale del protocollo e test su architetture weak-memory.

.NH 1
Correzione specifica del lowering Mojo
.PP
Una prima versione usava publish_claimed(mut self,...). Con Mojo 1.0.0b1 il
lowering applicava copy-in/copy-out dell'intera struct e poteva riscrivere head
e tail con snapshot obsoleti. Il sintomo osservato era la prenotazione apparente
dello stesso ticket da parte di due thread.
.PP
La correzione rende publish_claimed @staticmethod e passa solo i dati necessari.
L'IR LLVM del percorso concorrente è stato ispezionato per escludere load/store
aggregati della coda. Questa è una correzione del port Mojo, non una proprietà
dell'algoritmo LPRQ pubblicato.

.NH 1
API e ciclo di vita
.PP
Esempio:
.DS L
from MoStream.Bounded_LPRQ import BoundedLPRQInspired

var queue = BoundedLPRQInspired[Int](1024)
queue.push(42)
var value = queue.pop()
.DE
.PP
Il payload deve essere Copyable. Sono disponibili push, pop, try_push, try_pop,
estimated_len e metodi debug per ticket e celle piene. estimated_len carica
head e tail, calcola tail-head e limita il risultato all'intervallo fra zero e
capacity; resta uno snapshot approssimativo sotto concorrenza.
.PP
Move e distruzione sono supportati soltanto in quiescenza, senza operazioni
concorrenti attive e senza thread che conservino ticket o puntatori alle celle.
Spostare o distruggere la coda mentre è in uso causerebbe invalidazione dei
puntatori e non rientra nel contratto verificato.

.NH 1
Garanzie di progresso
.PP
BoundedLPRQInspired non è lock-free e non eredita la prova di progresso della
LPRQ. Un producer può vincere il CAS su tail e venire sospeso prima di FULL. Il
consumer head non può saltare quel ticket; il ring può quindi subire
head-of-line blocking e, una volta esauriti gli slot successivi, il progresso
globale dipende dal thread sospeso.
.PP
Lo yield periodico riduce lo spin aggressivo ma non risolve la dipendenza. La
FIFO esatta è precisamente il compromesso usato per rendere sicuro il riuso di
un singolo ring senza il protocollo di chiusura PRQ.

.NH 1
Verifica di correttezza
.PP
Il test sequenziale verifica riempimento, rifiuto al nono elemento con capacità
otto, FIFO, coda vuota, 10000 generazioni e riempimento dopo wrap-around.
.PP
Due test 4P-4C inviano 100000 messaggi per producer con capacità 2, 4, 8 e 16:
uno usa push/pop bloccanti, l'altro ritenta le weak try_push/try_pop. Entrambi
verificano count, checksum N(N-1)/2, coda vuota dopo i sentinel e una sola
occorrenza di ciascun ID mediante 400000 contatori atomici per-ID. Quest'ultimo
oracolo rileva anche combinazioni di duplicati e perdite che conservano somma e
conteggio complessivi.
.PP
La campagna documentata comprende inoltre 20 esecuzioni aggiuntive dei binari e
il controllo dell'IR LLVM. Il 22 luglio 2026 i tre test principali sono stati
rieseguiti e hanno prodotto:
.DS L
PASS: sequential boundedness and FIFO correctness
PASS: blocking API capacity 2, 4, 8, 16
PASS: concurrent exact-once correctness
PASS: try API capacity 2, 4, 8, 16
PASS: concurrent try exact-once correctness
.DE
.PP
I test sono evidenza sperimentale, non una prova formale di linearizzabilità o
di correttezza su tutti gli interleaving e memory model.

.NH 1
Metodo sperimentale
.PP
Il benchmark è stato eseguito il 22 luglio 2026 su Intel Xeon Gold 5512U, 28
core e 56 thread hardware, singolo nodo NUMA, Linux x86-64 e Mojo 1.0.0b1.
.PP
Configurazione:
.IP \(bu 3
100000 messaggi per producer e 10 ripetizioni;
.IP \(bu 3
topologie 1P-1C, 2P-2C, 4P-4C, 8P-8C, 1P-8C e 8P-1C;
.IP \(bu 3
capacità 16, 1024 e 65536;
.IP \(bu 3
ordine delle implementazioni ruotato fra le ripetizioni;
.IP \(bu 3
count e checksum verificati in ogni esecuzione;
.IP \(bu 3
media, deviazione standard campionaria e intervallo di confidenza 95 percento.
.PP
Le baseline sono CAS-MPMC, FAA, Rigtorp, Padded-FAA e Hybrid con soglie 1, 2,
4 e 8. La macchina non è stata isolata e i thread non sono stati pinnati. I
risultati sono quindi comparativi locali e richiedono replica controllata prima
di una pubblicazione definitiva.

.NH 1
Risultati bilanciati a capacità 1024
.PP
La tabella riporta throughput medio in milioni di messaggi al secondo dal CSV
rigenerato results_bounded_lprq.csv. Best Hybrid è il massimo delle quattro
soglie; l'ultima colonna confronta il vincitore con BLPRQ.
.TS
tab(|),box;
l r r r r r r l.
Thread|CAS|FAA|Rigtorp|Padded|Best H|BLPRQ|Winner/BLPRQ
1P-1C|5.023|6.690|4.410|4.993|6.367|2.219|FAA 3.01x
2P-2C|4.528|7.237|7.880|9.085|5.746|3.315|Padded 2.74x
4P-4C|3.841|8.692|8.645|10.842|7.132|3.012|Padded 3.60x
8P-8C|2.530|9.416|8.659|10.588|6.678|2.159|Padded 4.90x
.TE
.PP
Gli intervalli 95 percento di BLPRQ sono rispettivamente +/-0.031, +/-0.009,
+/-0.066 e +/-0.034 Mmsg/s. Gli intervalli completi di ogni baseline sono
conservati nel CSV.

.NH 1
Risultati sull'intera matrice
.PP
Vittorie per massimo throughput medio:
.TS
tab(|),box;
l r.
Implementazione|Vittorie su 18
Padded-FAA|14
FAA|2
Rigtorp|2
BLPRQ|0
CAS|0
Hybrid-1, 2, 4, 8|0
.TE
.PP
La migliore implementazione di ciascuna configurazione è fra 2.50 e 5.75
volte più veloce di BLPRQ. La mediana del rapporto è 3.57x e la media geometrica
3.63x.
.PP
Media geometrica dei rapporti rispetto a BLPRQ:
.TS
tab(|),box;
l r l r.
Queue|Rapporto|Queue|Rapporto
CAS|1.53x|FAA|2.69x
Rigtorp|2.91x|Padded-FAA|3.46x
Hybrid-1|2.43x|Hybrid-2|2.17x
Hybrid-4|1.80x|Hybrid-8|1.65x
.TE
.PP
Il distacco minimo, 2.50x, si verifica in 2P-2C capacità 16; il massimo, 5.75x,
in 1P-1C capacità 65536. Quest'ultimo caso presenta elevata variabilità per
Padded-FAA e va replicato su macchina isolata.

.NH 1
Interpretazione del risultato negativo
.PP
BoundedLPRQInspired non ottiene vittorie. I costi principali sono:
.IP 1. 4
il gate CAS è necessario per impedire la sovraprenotazione che PRQ gestisce
tramite skip e chiusura del segmento;
.IP 2. 4
ogni trasferimento attraversa ticket, index, token e più transizioni atomiche;
.IP 3. 4
Optional[T] separato richiede WRITING e READING, mentre una rappresentazione
compatta token-puntatore può ridurre passaggi;
.IP 4. 4
la FIFO exact-turn elimina gli skip e introduce head-of-line blocking;
.IP 5. 4
SEQ_CST, CAS retry e cache coherence crescono con la contesa;
.IP 6. 4
padding e due word atomici per cella aumentano memoria e traffico.
.PP
Il peggioramento relativo cresce nelle topologie 8P-8C e SPMC, coerentemente
con un percorso ricco di CAS e metadati. BLPRQ resta un prototipo utile per
studiare perché la chiusura dei segmenti sia strutturale nella LPRQ.

.NH 1
Cosa non dimostrano i numeri
.PP
Questi dati non dimostrano che la LPRQ di Romanov e Koval sia lenta. Non è stata
implementata né benchmarkata la lista dinamica di segmenti con il protocollo
PRQ completo. È stata misurata una riduzione bounded conservativa che sacrifica
proprio skip, close e lock-freedom.
.PP
Le classifiche sono basate sulle medie di una singola campagna. Intervalli
sovrapposti, rumore, frequenza CPU, allocazione dei thread e assenza di pinning
limitano inferenze causali. Il throughput non descrive latenza p99, fairness,
energia o risposta alla sospensione dei thread.

.NH 1
Limiti di correttezza e API
.IP \(bu 3
Le try sono weak: un fallimento temporaneo non certifica pieno o vuoto.
.IP \(bu 3
Move e distruzione richiedono quiescenza completa.
.IP \(bu 3
Il token si ripete dopo 2^61 ticket; wrap-around non formalmente supportato.
.IP \(bu 3
Non esiste una prova formale di linearizzabilità del port Mojo.
.IP \(bu 3
Non sono stati esplorati sistematicamente tutti gli interleaving.
.IP \(bu 3
Il lowering corretto è stato verificato per Mojo 1.0.0b1, non per versioni future.
.IP \(bu 3
Un thread sospeso dopo la prenotazione può arrestare il ticket FIFO successivo.
.IP \(bu 3
Payload e capacità differenti possono modificare cache, layout e prestazioni.

.NH 1
Applicabilità a MoStream
.PP
Nella matrice corrente Padded-FAA resta la scelta generale più forte, FAA vince
due casi semplici e Rigtorp due workload SPMC. BoundedLPRQInspired non è una
sostituzione prestazionale consigliata per Communicator o scheduler.
.PP
Il prototipo è invece utile come risultato di tesi: mostra empiricamente che
fissare un singolo ring e attendere il turno esatto annulla i vantaggi del
meccanismo PRQ. Per avvicinarsi all'algoritmo originale senza memoria illimitata
serve studiare un pool bounded di segmenti con generazioni e reclamation, non
aggiungere altre ottimizzazioni locali al solo ring exact-turn.

.NH 1
Lavoro futuro
.IP 1. 4
Formalizzare linearizzabilità e progresso con model checking.
.IP 2. 4
Creare uno stress harness con storie concorrenti e verifica offline.
.IP 3. 4
Sospendere thread dopo tail CAS e durante WRITING o READING.
.IP 4. 4
Misurare p50, p95, p99, fairness, CPU ed energia su host pinnato.
.IP 5. 4
Provare ordinamenti meno forti solo dopo analisi memory-model.
.IP 6. 4
Progettare un pool fisso di segmenti PRQ con generation tag.
.IP 7. 4
Definire reclamation sicura per segmenti riutilizzabili.
.IP 8. 4
Confrontare payload atomici compatti e Optional[T] generico.
.IP 9. 4
Ripetere su ARM e sistemi NUMA per esporre weak ordering e locality.

.NH 1
Conclusione
.PP
BoundedLPRQInspired raggiunge memoria e occupazione limitate e supera i test
eseguiti grazie a index per epoca, token per ticket, fasi esclusive e gate CAS.
Questa prudenza trasforma però il protocollo in una coda exact-turn non
lock-free, con più metadati e operazioni atomiche delle baseline.
.PP
Il risultato sperimentale è netto: zero vittorie su 18, Padded-FAA prima in 14
casi e un vincitore mediamente 3.63x più veloce. La conclusione scientificamente
corretta non è che LPRQ sia inefficiente, ma che la conversione in un unico ring
bounded e riusabile rimuove il meccanismo con cui LPRQ tollera i ticket saltati.

.NH 1
Artefatti e fonti
.IP \(bu 3
MoStream/Bounded_LPRQ.mojo, implementazione exact-turn.
.IP \(bu 3
Tests/test_bounded_lprq.mojo, test sequenziale e generazionale.
.IP \(bu 3
Tests/test_bounded_lprq_concurrent.mojo, test bloccante 4P-4C.
.IP \(bu 3
Tests/test_bounded_lprq_try_concurrent.mojo, test weak try 4P-4C.
.IP \(bu 3
Benchmarks/QueueBenchmark/results_bounded_lprq.csv, dati aggregati.
.IP \(bu 3
Benchmarks/QueueBenchmark/analyze_lprq_results.py, riepilogo riproducibile.
.IP \(bu 3
Raed Romanov e Nikita Koval, The State-of-the-Art LCRQ Concurrent Queue
Algorithm Does NOT Require CAS2, PPoPP 2023, DOI 10.1145/3572848.3577485.
.IP \(bu 3
Artifact ufficiale: https://zenodo.org/records/7337237.
