# COME LEGGERE QUESTA CODA
# MPMC significa: piu produttori inseriscono e piu consumatori estraggono.
# FIFO significa: gli elementi devono uscire nell'ordine di inserimento.
# Il buffer e un cerchio: dopo la cella L-1 si torna alla cella 0.
# Ogni cella contiene una sola parola atomica:
#   0 oppure 1 -> cella vuota; il numero e il "colore" della fase;
#   valore >= 2 -> cella occupata; il dato applicativo e valore - 2.
# head e tail sono guide per la ricerca, non contatori degli elementi:
# possono restare indietro perche non vengono aggiornati a ogni operazione.
# La modifica decisiva avviene sulla cella tramite CAS (compare_exchange):
# "scrivi il nuovo valore SOLO SE trovi ancora quello che avevo letto".
# Se la CAS fallisce, un altro task potrebbe aver modificato la cella: si riprova.
#
# Porting didattico del protocollo Tsigas–Zhang (SPAA 2001), figure 5–7.
# Fonte: https://www.cse.chalmers.se/~tsigas/papers/spaa01.pdf
# Implementazione sperimentale: vale l'ipotesi anti-ABA del Teorema 1 del paper.
# Due colori NON proteggono da sospensioni arbitrarie durante molti giri.
# ABA: un task si ferma, gli altri riusano il ring e al suo risveglio indici
# e contenuti possono sembrare quelli iniziali. Non significa che nulla sia
# cambiato. Il paper impone un limite alle operazioni completate durante la
# pausa; questo porting conserva tale ipotesi, non la rende superflua.
# Correzione di porting: la scansione di dequeue valida anche tail, per non
# saltare una cella pubblicata dopo una lettura di vuoto ormai obsoleta.
# Tutto lo stato condiviso è atomico SEQUENTIAL: nessun accesso ordinario in gara.
# Non è una coda generica: trasporta UInt64 in [0, 2**64-3], codificati come x+2.
# 0 e 1 sono riservati internamente ai due colori; nessun puntatore da liberare.
# L celle fisiche (potenza di due >= 4) offrono L-2 posti utilizzabili.
# push/pop attendono attivamente; try_* possono riprovare in caso di contesa.
# Spostamento, size/empty esatti e distruzione richiedono accessi quiescenti.

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.sys.info import size_of

# Il padding separa in memoria le guide head e tail, per ridurre la condivisione
# delle linee di cache fra chi legge e chi scrive. Non garantisce che il primo
# byte della struttura sia allineato all'inizio di una linea di cache.
struct TsigasZhangPaddedAtomic:
    comptime PAD_BYTES = 64 - size_of[Atomic[UInt64]]()
    var value: Atomic[UInt64]
    var padding: Array[UInt8, Self.PAD_BYTES]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[UInt64](initial)
        self.padding = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

# La coda possiede il buffer allocato: Movable consente di trasferirne la
# proprieta senza copiarla. Due copie proprietarie libererebbero lo stesso buffer.
struct TsigasZhangMPMCQueue(Movable):
    # Limite incluso: la somma con due non deve traboccare nei marcatori.
    comptime MAX_VALUE = UInt64(18446744073709551613)
    # capacity è la capacità utile; ring_size include le due celle di separazione.
    var capacity: Int
    var ring_size: Int
    # Con L potenza di due, (indice + 1) & mask equivale al ritorno circolare modulo L.
    var mask: UInt64
    # Celle condivise: sia il dato sia il marcatore di vuoto sono atomici.
    var nodes: Pointer[Atomic[UInt64], MutUntrackedOrigin]
    # head: il consumatore inizia la ricerca dalla cella successiva.
    # tail: il produttore inizia la ricerca da questa cella.
    # Le guide possono essere arretrate; le scansioni e l'aiuto le fanno avanzare.
    var head: TsigasZhangPaddedAtomic
    var tail: TsigasZhangPaddedAtomic

    # Colore di riferimento per segnare le celle svuotate (0 oppure 1).
    # Quando si consuma la cella zero si cambia fase, invertendo il colore.
    # Non e un dato dell'utente e non conta il numero degli elementi.
    var vnull: Atomic[UInt64]

    def __init__(out self, ring_size: Int = 1024) raises:
        # Si accetta la lunghezza fisica esatta; nessun arrotondamento nascosto.
        if ring_size < 4 or (ring_size & (ring_size - 1)) != 0:
            raise Error("TsigasZhang: ring_size deve essere una potenza di due >= 4")
        self.ring_size = ring_size
        self.capacity = ring_size - 2
        self.mask = UInt64(ring_size - 1)
        self.nodes = unsafe_alloc[Atomic[UInt64]](ring_size)
        # Stato vuoto iniziale: prima scrittura in 1, prima ricerca di lettura in 1.
        self.head = TsigasZhangPaddedAtomic(0)
        self.tail = TsigasZhangPaddedAtomic(1)
        self.vnull = Atomic[UInt64](1)
        for i in range(ring_size):
            # UInt64(i == 0) vale 1 nella cella zero, 0 nelle altre.
            # Sono tutte vuote: la differenza di colore prepara il primo giro.
            self.nodes.unsafe_offset(i).unsafe_write(Atomic[UInt64](UInt64(i == 0)))

    # Costruttore di spostamento: trasferisce il buffer e conserva lo stato.
    # Va chiamato solo quando nessun altro task sta usando questa coda.
    def __init__(out self, *, deinit move: Self):
        self.capacity = move.capacity
        self.ring_size = move.ring_size
        self.mask = move.mask
        self.nodes = move.nodes
        self.head = TsigasZhangPaddedAtomic(move.head.value.load())
        self.tail = TsigasZhangPaddedAtomic(move.tail.value.load())
        self.vnull = Atomic[UInt64](move.vnull.load())

    # Prima di distruggere la coda occorre aver terminato tutti gli accessi:
    # gli altri task non devono piu leggere ne scrivere il buffer liberato.
    def __deinit__(deinit self):
        for i in range(self.ring_size):
            self.nodes.unsafe_offset(i).unsafe_deinit_pointee()
        self.nodes.unsafe_free()

    # INSERIMENTO: cerca una cella libera e prova a occuparla con una CAS.
    # Risultato: None = inserito; Optional(item) = piena, il dato resta al chiamante.
    # "try" evita l'attesa di spazio quando la coda risulta piena; non significa
    # un solo tentativo: sotto contesa questa funzione puo riprovare internamente.
    def try_push(mut self, item: UInt64) raises -> Optional[UInt64]:
        if item > Self.MAX_VALUE:
            raise Error("TsigasZhang: valore fuori dall'intervallo 0..2**64-3")
        # Aggiungendo 2 teniamo distinti i dati dai marcatori 0 e 1.
        # Dato e stato della cella cambiano insieme nella stessa CAS atomica.
        var encoded = item + 2
        while True:
            # Nomi del paper: te = tail letto all'inizio del tentativo;
            # ate = posizione che stiamo esaminando; tt = contenuto letto.
            var te = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
            var ate = te
            var tt = self.nodes.unsafe_offset(ate)[].load[ordering=Ordering.SEQUENTIAL]()
            var next_index = (ate + 1) & self.mask
            # 1. Saltiamo le celle occupate (tt > 1): tail puo essere arretrato.
            # Ci fermiamo se cambia la guida o se raggiungiamo il confine head.
            # Ogni lettura descrive solo quell'istante: altri task lavorano ancora.
            while tt > 1:
                if te != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                    break
                if next_index == self.head.value.load[ordering=Ordering.SEQUENTIAL]():
                    break
                tt = self.nodes.unsafe_offset(next_index)[].load[ordering=Ordering.SEQUENTIAL]()
                ate = next_index
                next_index = (ate + 1) & self.mask
            if te != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # 2. Il prossimo indice coincide con head: potremmo essere pieni,
            # oppure head potrebbe essere rimasto indietro rispetto alle estrazioni.
            # Esaminiamo la cella dopo head: se occupata restituiamo il dato;
            # se vuota proviamo ad avanzare head per conto dei consumatori.
            if next_index == self.head.value.load[ordering=Ordering.SEQUENTIAL]():
                ate = (next_index + 1) & self.mask
                tt = self.nodes.unsafe_offset(ate)[].load[ordering=Ordering.SEQUENTIAL]()
                if tt > 1:
                    return Optional(item)
                # Attraversando zero aggiorniamo anche il colore di riferimento.
                if ate == 0:
                    self.vnull.store[ordering=Ordering.SEQUENTIAL](tt)
                var expected_head = next_index
                _ = self.head.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_head, ate)
                continue
            if te != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # 3. Proviamo a sostituire il marcatore letto con il dato codificato.
            # Solo chi riesce nella CAS ha inserito: due produttori non possono
            # vincere lo stesso confronto sullo stesso stato della cella.
            # Una CAS fallita modifica expected; al prossimo giro rileggiamo tutto.
            var expected = tt
            if self.nodes.unsafe_offset(ate)[].compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected, encoded):
                # Il dato e gia pubblicato. Ora proviamo ad aggiornare la guida
                # ogni due posizioni (m=2), riducendo gli aggiornamenti di tail.
                # Se questa seconda CAS fallisce, l'inserimento resta riuscito:
                # un altro task ha cambiato tail e non dobbiamo inserire di nuovo.
                if (next_index & 1) == 0:
                    var expected_tail = te
                    _ = self.tail.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_tail, next_index)
                return None

    # ESTRAZIONE: cerca un dato e prova a sostituirlo con un marcatore di vuoto.
    # Risultato: Optional(dato) = estratto; None = osservata coda vuota.
    # Anche zero e uno applicativi sono validi, perche dentro erano codificati.
    # Anche qui la contesa puo causare piu tentativi dentro la stessa chiamata.
    def try_pop(mut self) -> Optional[UInt64]:
        while True:
            var th = self.head.value.load[ordering=Ordering.SEQUENTIAL]()
            # 1. Salviamo head (th) e tail PRIMA di leggere la prima cella.
            # Correzione del porting: controllare solo head non bastava. Una cella
            # vista vuota poteva ricevere un dato mentre la scansione proseguiva.
            # Se una guida cambia scartiamo la scansione e ricominciamo, invece
            # di continuare a fidarci delle precedenti osservazioni di vuoto.
            # Questi controlli non eliminano il limite ABA descritto in apertura.
            var tail_snapshot = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
            var index = (th + 1) & self.mask
            var tt = self.nodes.unsafe_offset(index)[].load[ordering=Ordering.SEQUENTIAL]()
            # 2. Saltiamo i marcatori di vuoto: altri consumatori possono aver
            # gia estratto dei dati senza aggiornare head a ogni estrazione.
            # Se arriviamo a tail_snapshot trovando vuoto e con guide invariate,
            # restituiamo None; se cambia una guida ripartiamo dal ciclo esterno.
            while tt <= 1:
                if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]() or tail_snapshot != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                    break
                if index == tail_snapshot:
                    return None
                index = (index + 1) & self.mask
                tt = self.nodes.unsafe_offset(index)[].load[ordering=Ordering.SEQUENTIAL]()
            if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]() or tail_snapshot != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # 3. Abbiamo trovato una cella occupata proprio sulla guida tail:
            # il produttore ha pubblicato il dato ma tail e ancora arretrato.
            # Aiutiamo ad avanzarlo e ricominciamo con nuovi riferimenti.
            if index == tail_snapshot:
                var expected_tail = index
                _ = self.tail.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_tail, (index + 1) & self.mask)
                continue
            # 4. Scegliamo il marcatore da lasciare al posto del dato.
            # Entrambi i colori significano "vuoto", ma distinguono le fasi
            # del riuso del ring entro le ipotesi del protocollo.
            var empty_color: UInt64
            if index != 0:
                if index < th:
                    # index < th indica che la ricerca e passata dalla fine a 0.
                    # Usiamo il colore letto dalla cella zero, che segna il passaggio.
                    empty_color = self.nodes.unsafe_offset(0)[].load[ordering=Ordering.SEQUENTIAL]()
                else:
                    # Senza attraversamento usiamo il colore di riferimento.
                    empty_color = self.vnull.load[ordering=Ordering.SEQUENTIAL]()
            else:
                # Consumando zero si cambia fase: 0 diventa 1 e viceversa.
                empty_color = self.vnull.load[ordering=Ordering.SEQUENTIAL]() ^ UInt64(1)
            if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # 5. La CAS rimuove il dato solo se la cella contiene ancora tt.
            # Se perdiamo la gara, non restituiamo il dato: rileggiamo e riproviamo.
            var expected = tt
            if self.nodes.unsafe_offset(index)[].compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected, empty_color):
                if index == 0:
                    self.vnull.store[ordering=Ordering.SEQUENTIAL](empty_color)
                # Il dato e gia nostro. Aggiorniamo head sugli indici pari (m=2).
                # La CAS sulla guida e un aiuto: anche se fallisce, l'estrazione
                # dalla cella e riuscita e il dato va restituito una sola volta.
                if (index & 1) == 0:
                    var expected_head = th
                    _ = self.head.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_head, index)
                # Decodifica: torniamo al valore originale dell'utente.
                return Optional(tt - 2)

    # Attende spazio ripetendo try_push: Optional presente significa riprovare.
    # E attesa attiva (busy-wait): il task non si sospende e continua a usare CPU.
    # In uno scheduler cooperativo occorre valutare il rischio di impedire
    # l'esecuzione proprio ai task che dovrebbero liberare spazio.
    def push(mut self, item: UInt64) raises:
        while self.try_push(item):
            pass

    # Attende un dato ripetendo try_pop; stesse cautele sull'attesa attiva.
    def pop(mut self) -> UInt64:
        var result = self.try_pop()
        while not result:
            result = self.try_pop()
        return result.take()

    # Visita tutte le L celle: costo O(L), anche se la coda e quasi vuota.
    # Il conteggio e esatto solo senza inserimenti/estrazioni concorrenti:
    # le singole letture atomiche non formano una fotografia unica del buffer.
    def size(self) -> Int:
        var count = 0
        for i in range(self.ring_size):
            if self.nodes.unsafe_offset(i)[].load[ordering=Ordering.SEQUENTIAL]() > 1:
                count += 1
        return count

    # Anche empty eredita costo e limiti di size. Durante la concorrenza non
    # autorizza un pop successivo: lo stato puo cambiare subito dopo il controllo.
    # Per provare a estrarre, usare direttamente try_pop e verificarne il risultato.
    def empty(self) -> Bool:
        return self.size() == 0
