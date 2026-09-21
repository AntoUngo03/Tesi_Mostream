# ===------------------------------------------------------------------------=== #
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU Lesser General Public License version 3 as
#  published by the Free Software Foundation.
#  
#  This program is distributed in the hope that it will be useful, but WITHOUT
#  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
#  License for more details.
#  
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# ===------------------------------------------------------------------------=== #

# Atomic rappresenta un valore modificabile in sicurezza da piu thread;
# Ordering specifica le garanzie di ordinamento delle operazioni atomiche.
# fence e importata per il backoff sperimentale commentato piu sotto.
from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering, fence
# sleep al momento non e usato: e rimasto da precedenti strategie di attesa.
from std.time import sleep
# size_of permette di calcolare quanti byte occupa un tipo.
from std.sys.info import size_of
# Optional rappresenta un valore presente oppure None.
from std.collections import Optional
# Funzione di utilita del progetto per visualizzare gli errori in rosso.
from MoStream.utils import print_red_color

# Contenitore di un'atomica che occupa una cache line intera. Il padding evita
# il false sharing: producer e consumer non si invalidano reciprocamente la
# cache quando modificano enqueue_pos e dequeue_pos, che sono dati distinti.
struct PaddedAtomicU64:
    # Dimensione convenzionale di una cache line sulle CPU di destinazione.
    comptime CACHE_LINE_SIZE_BYTES = 64
    # Numero di byte da aggiungere dopo l'atomica per arrivare a 64 byte.
    comptime PAD_BYTES = Self.CACHE_LINE_SIZE_BYTES - size_of[Atomic[DType.uint64]]()
    # Il contatore UInt64 realmente usato dall'algoritmo.
    var atomicVal: Atomic[DType.uint64]
    # Byte senza significato logico: servono soltanto a occupare spazio.
    var pad: Array[UInt8, Self.PAD_BYTES]

    # Costruisce il contatore atomico partendo da initial.
    def __init__(out self, initial: UInt64):
        # Inizializza il valore atomico.
        self.atomicVal = Atomic[DType.uint64](initial)
        # Il padding non verra mai letto, quindi puo restare non inizializzato.
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

# Una posizione fisica del buffer circolare. sequence dice a quale giro logico
# appartiene la cella e se essa e libera o contiene un elemento pubblicato.
struct Cell[T: Copyable & Deinitable](Movable):
    # Stato atomico usato per sincronizzare producer e consumer sulla cella.
    var sequence: Atomic[DType.uint64]
    # Payload della cella; None significa che non contiene un valore posseduto.
    var data: Optional[Self.T]

    # Costruisce una cella vuota con il numero di sequenza indicato.
    def __init__(out self, seq: UInt64):
        # La sequenza iniziale i rende la cella i disponibile al ticket i.
        self.sequence = Atomic[DType.uint64](seq)
        # All'inizio non esiste alcun payload.
        self.data = Optional[Self.T](None)

    # Costruisce una cella prendendo le risorse da move.
    def __init__(out self, *, deinit move: Self):
        # Un'atomica non viene copiata direttamente: se ne legge il valore.
        var val = move.sequence.load()
        # Ricrea l'atomica di destinazione con la sequenza letta.
        self.sequence = Atomic[DType.uint64](val)
        # ^ trasferisce, anziche copiare, l'Optional e il suo payload.
        self.data = move.data^

# MPMC queue implementation based the algorithm by Dmitry Vyukov
struct MPMCQueue[T: Copyable & Deinitable](Movable):
    # Alias del tipo puntatore usato per accedere alle celle allocate a mano.
    comptime CellPointer = Pointer[Cell[Self.T], MutUntrackedOrigin]
    # Numero iniziale di iterazioni vuote dopo una collisione tra thread.
    comptime BACKOFF_MIN = 128
    # Limite dichiarato per il backoff (si veda la nota nel metodo push).
    comptime BACKOFF_MAX = 1024
    # Primo elemento del blocco di memoria che contiene le celle.
    var buffer: Self.CellPointer
    # Capacita massima della coda; deve essere una potenza di due.
    var size: UInt64
    # Vale size - 1 e converte un ticket in un indice con ticket & mask.
    var mask: UInt64
    # Prossimo ticket che un producer tentera di riservare.
    var enqueue_pos: PaddedAtomicU64
    # Prossimo ticket che un consumer tentera di riservare.
    var dequeue_pos: PaddedAtomicU64

    # Costruisce una coda vuota; puo sollevare un Error per size non valida.
    def __init__(out self, size: Int = 1024) raises:
        # x & (x - 1) == 0 riconosce le potenze di due. Si richiede anche
        # almeno 2, perche il buffer circolare non ammette capacita 0 o 1.
        if not ((size >= 2) and (size & (size - 1)) == 0):
            # Comunica all'utente perche la costruzione non puo proseguire.
            print_red_color("{MoStream} Error: MPMC queues need size to be a power of 2 and at least 2!")
            # Interrompe il costruttore.
            raise Error("error in MPMC_Queue()")
        # Memorizza la capacita nel tipo usato dai contatori atomici.
        self.size = UInt64(size)
        # Con size potenza di due, pw & mask equivale a pw % size.
        self.mask = UInt64(size - 1)
        # Alloca size celle non ancora costruite.
        self.buffer = unsafe_alloc[Cell[Self.T]](Int(self.size))
        # Il primo producer parte dal ticket logico zero.
        self.enqueue_pos = PaddedAtomicU64(0)
        # Anche il primo consumer parte dal ticket logico zero.
        self.dequeue_pos = PaddedAtomicU64(0)
        # Costruisce esplicitamente ogni cella nel blocco appena allocato.
        for i in range(self.size):
            # La cella i riceve sequence=i: e libera per il primo giro.
            (self.buffer.unsafe_offset(i)).unsafe_write(Cell[Self.T](UInt64(i)))

    # Sposta la coda senza copiare il buffer.
    def __init__(out self, *, deinit move: Self):
        # Trasferisce il puntatore al medesimo blocco di celle.
        self.buffer = move.buffer
        # Conserva la capacita.
        self.size = move.size
        # Conserva la maschera per l'indicizzazione circolare.
        self.mask = move.mask
        # ATTENZIONE: il codice corrente azzera i contatori invece di trasferirli.
        # Spostare una coda gia usata puo quindi perdere la posizione corrente.
        self.enqueue_pos = PaddedAtomicU64(0)
        # La stessa osservazione vale per il contatore dei consumer.
        self.dequeue_pos = PaddedAtomicU64(0)

    # Distrugge la coda. Nessun altro thread deve usarla a questo punto.
    def __deinit__(deinit self):
        # Visita tutte le celle costruite dal costruttore.
        for i in range(self.size):
            # Esegue il distruttore della cella e dell'eventuale payload.
            (self.buffer.unsafe_offset(i)).unsafe_deinit_pointee()
        # Restituisce all'allocatore il blocco di memoria del buffer.
        self.buffer.unsafe_free()

    # Inserimento bloccante tramite busy waiting: ritorna soltanto dopo avere
    # riservato una cella, scritto item e pubblicato la nuova sequenza.
    def push(mut self, var item: Self.T):
        # Posizione logica di scrittura (producer/write ticket).
        var pw: UInt64
        # Sequenza osservata nella cella candidata.
        var seq: UInt64
        # Durata corrente del backoff in caso di CAS fallito.
        var bk: UInt64 = Self.BACKOFF_MIN
        # Continua finche l'inserimento non riesce.
        while True:
            # RELAXED basta per ottenere un ticket candidato: la pubblicazione
            # del payload e sincronizzata separatamente tramite sequence.
            pw = self.enqueue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
            # Traduce il ticket crescente nell'indice del buffer circolare.
            var cell_ptr = self.buffer.unsafe_offset(Int(pw & self.mask))
            # ACQUIRE si sincronizza con il RELEASE dell'ultimo consumer che
            # ha liberato questa cella.
            seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
            # sequence == pw significa: cella libera per questo ticket/giro.
            if pw == seq:
                # Il CAS assegna il ticket a un solo producer. Se riesce,
                # enqueue_pos avanza e gli altri producer useranno ticket nuovi.
                if self.enqueue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pw, pw + 1):
                    # Trasferisce il payload nella cella appena riservata.
                    cell_ptr[].data = Optional(item^)
                    # Pubblica il dato. RELEASE garantisce che un consumer che
                    # vede pw+1 con ACQUIRE veda anche la scrittura precedente.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pw + 1)
                    # L'elemento e ora visibile ai consumer.
                    return  # successfully pushed
                # Un altro producer ha vinto il CAS: attende prima di riprovare.
                for _ in range(bk):
                    # Una fence era stata ipotizzata, ma al momento non e usata.
                    # fence[ordering=Ordering.SEQUENTIAL]()
                    # Corpo vuoto: il compilatore potrebbe eliminare il ritardo.
                    pass
                # Raddoppia il numero di iterazioni per la prossima collisione.
                bk <<= 1
                # ATTENZIONE: AND non implementa un vero limite massimo.
                # Per esempio, 2048 & 1024 produce 0; sarebbe piu naturale
                # usare min(bk, BACKOFF_MAX). Qui si conserva la logica originale.
                bk &= Self.BACKOFF_MAX

    # try_push method for producers, returns None if the item has been successfully pushed, or the item itself if the queue
    #   is currently full (i.e. no slot is currently available for pushing)
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        # Legge una sola volta il prossimo ticket producer.
        var pw = self.enqueue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
        # Trova la cella fisica associata al ticket.
        var cell_ptr = self.buffer.unsafe_offset(Int(pw & self.mask))
        # Legge lo stato pubblicato della cella.
        var seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
        # Se le sequenze non coincidono, la cella non e libera per pw.
        if pw != seq:
            # Restituisce il payload al chiamante, che ne mantiene il possesso.
            return Optional(item^) # queue is currently full
        # Prova una sola volta a riservare pw. Un fallimento puo indicare
        # contesa con un altro producer, non necessariamente una coda piena.
        if not self.enqueue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pw, pw + 1):
            # Il chiamante potra decidere se e quando riprovare.
            return Optional(item^) # queue is currently full
        # Il ticket e nostro: trasferisce il payload nella cella.
        cell_ptr[].data = Optional(item^)
        # Pubblica il dato ai consumer con semantica RELEASE.
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pw + 1)
        # None comunica che item e stato consumato e inserito correttamente.
        return None # successfully pushed

    # Estrazione bloccante tramite busy waiting: attende finche trova un dato.
    def pop(mut self) -> Self.T:
        # Continua a interrogare la coda quando questa appare vuota.
        while (True):
            # Esegue un tentativo non bloccante.
            item = self.try_pop()
            # Un Optional valorizzato indica che l'estrazione e riuscita.
            if item:
                # Estrae e trasferisce il payload dall'Optional al chiamante.
                return item.take()

    # try_pop method for consumers, returns an Optional containing the item if popped successfully,
    #   or None if the queue is empty
    def try_pop(mut self) -> Optional[Self.T]:
        # Posizione logica di lettura (consumer/read ticket).
        var pr: UInt64
        # Sequenza osservata nella cella candidata.
        var seq: UInt64
        # Durata corrente del backoff dopo una collisione tra consumer.
        var bk: UInt64 = Self.BACKOFF_MIN
        # Ripete soltanto quando incontra contesa; se rileva il vuoto ritorna.
        while True:
            # Legge il prossimo ticket consumer senza imporre altro ordine.
            pr = self.dequeue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
            # Traduce il ticket nell'indice del buffer circolare.
            var cell_ptr = self.buffer.unsafe_offset(Int(pr & self.mask))
            # ACQUIRE si sincronizza con il RELEASE usato dal producer per
            # pubblicare il payload in questa cella.
            seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
            # Per il ticket pr, un elemento pronto ha sequence == pr + 1.
            var expected_seq = pr + 1
            # Verifica che il producer abbia completato la pubblicazione.
            if seq == expected_seq:
                # element is ready to be consumed, try to claim it by incrementing pr
                # Il CAS assegna l'elemento a un solo consumer e, in caso di
                # successo, fa avanzare il prossimo ticket di lettura.
                if self.dequeue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pr, pr + 1):
                    # Rimuove il payload dalla cella e ne prende il possesso.
                    var item = cell_ptr[].data.take()
                    # Libera la cella per il giro seguente. Poiche mask+1=size,
                    # la nuova sequenza e pr+size. RELEASE rende conclusa la lettura.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pr + self.mask + 1)
                    # Trasferisce al chiamante l'elemento estratto.
                    return Optional(item^)
                # CAS failed, another consumer might have claimed this item, retry
                # Riduce la pressione sull'atomica durante la contesa.
                for _ in range(bk):
                    # La fence sperimentale e intenzionalmente disabilitata.
                    # fence[ordering=Ordering.SEQUENTIAL]()
                    # Corpo vuoto; potrebbe essere ottimizzato via.
                    pass
                # Raddoppia il backoff dopo ogni CAS perso.
                bk <<= 1
                # Come in push, questo AND non e un corretto clamp al massimo.
                bk &= Self.BACKOFF_MAX
            # Una sequenza piu vecchia di quella attesa indica che, per il
            # ticket corrente, il producer non ha pubblicato alcun elemento.
            elif seq < expected_seq:
                # empty slot, the producer has not yet written the item, return None
                # Optional vuoto comunica che la coda appare vuota ora.
                return Optional[Self.T](None)

    # Restituisce solo una stima: le due atomiche sono lette separatamente e
    # possono cambiare tra una lettura e l'altra.
    def estimated_len(self) -> Int:
        # Fotografa approssimativamente il numero di ticket producer assegnati.
        var enq = self.enqueue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
        # Fotografa approssimativamente il numero di ticket consumer assegnati.
        var deq = self.dequeue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
        # Se non risultano inserimenti davanti alle estrazioni, stima zero.
        if enq <= deq:
            # La lunghezza di una coda non puo essere negativa.
            return 0
        # Ticket prodotti meno ticket consumati.
        var diff = enq - deq
        # Una lettura concorrente incoerente non deve superare la capacita.
        if diff > self.size:
            # Limita la stima al massimo numero possibile di elementi.
            return Int(self.size)
        # Converte la differenza valida nel tipo di ritorno Int.
        return Int(diff)
