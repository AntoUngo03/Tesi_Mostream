# Implementazione bounded della SCQ descritta da Nikolaev (DISC 2019,
# Algoritmo 6). MPMCQueue viene presa come riferimento soltanto per l'API
# pubblica richiesta da MoStream; il protocollo concorrente e autonomo.
# (Spiegazione generale: questa intestazione descrive a grandi linee lo
# scopo del file e come la coda separa indici e payload per evitare di mettere
# T direttamente nel ring atomico.)

# La coda generica non inserisce T direttamente nel ring atomico. Usa invece:
# - `free_indices`: indici delle celle `data` disponibili per i producer
# - `data`: array separato che contiene i payload di tipo `T`
# - `allocated_indices`: indici dei payload pronti per i consumer
# (Flusso: producer prende un indice da `free_indices`, scrive in `data[index]`,
#  poi pubblica l'indice in `allocated_indices`; il consumer fa l'opposto.)

# Nota: Mojo 1.0 non espone un operatore atomic OR per interi; il dequeue
# emula l'OR con un CAS che imposta i bit Index a `bottom` mantenendo Cycle
# e IsSafe. In caso di contesa il CAS viene ritentato sullo stesso ticket.

from std.atomic import Atomic, Ordering
# importa `Atomic` e gli ordinamenti di memoria per operazioni atomiche
from std.collections import Optional
# importa `Optional` per la gestione dei payload opzionali
from std.sys import get_defined_bool
# funzione per leggere flag di compilazione definiti esternamente
from std.sys.info import size_of
# funzione che ritorna la dimensione in byte di un tipo
from std.sys.terminate import exit
# funzione per terminare il processo con codice d'errore
from std.time import sleep
# funzione sleep per sospendere il thread (usata solo nei test)
from MoStream.utils import print_red_color
# utilità locale per stampare messaggi di errore in rosso


# Hook compilati soltanto nei test di robustezza. Permettono di sospendere un
# thread nei punti in cui un algoritmo FAA scorretto tenderebbe a bloccarsi.
comptime SCQ_TEST_PAUSE_AFTER_TICKET = get_defined_bool[
    "SCQ_TEST_PAUSE_AFTER_TICKET", False
]()  # flag di test: pausa breve dopo il fetch-add del ticket
comptime SCQ_TEST_PAUSE_BEFORE_PUBLISH = get_defined_bool[
    "SCQ_TEST_PAUSE_BEFORE_PUBLISH", False
]()  # flag di test: pausa prima del publish (CAS) della entry


struct SCQPaddedAtomicU64:
    # Head e Tail vengono modificati da gruppi di thread diversi. Portare ogni
    # contatore a 64 byte evita che condividano accidentalmente una cache line.
    comptime PAD = 64 - size_of[Atomic[DType.uint64]]()
    var value: Atomic[DType.uint64]
    var padding: InlineArray[UInt8, Self.PAD]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)

# `SCQPaddedAtomicU64` è un wrapper che allinea un contatore atomico a 64 byte
# per ridurre il false sharing tra `head` e `tail` quando sono aggiornati da
# thread differenti; contiene `value` (Atomic<uint64>) e padding per riempire
# la cache line.


struct SCQIndexRing(Movable):
    """Algoritmo 6 specializzato per indici compresi in [0, n)."""

    comptime EntryPointer = UnsafePointer[
        Atomic[DType.uint64], MutExternalOrigin
    ]
    comptime EARLY_SPINS = 256
    var entries: Self.EntryPointer
    var capacity: UInt64       # n: massimo numero di indici presenti
    var ring_size: UInt64      # 2n: numero di entry fisiche SCQ
    var ring_mask: UInt64      # 2n-1: ticket -> posizione circolare
    var index_bits: Int        # bit necessari per rappresentare [0, 2n)
    var index_mask: UInt64     # tutti i bit Index a 1 rappresentano bottom
    var safe_mask: UInt64      # bit IsSafe nella word packed
    var head: SCQPaddedAtomicU64
    var tail: SCQPaddedAtomicU64
    var threshold: Atomic[DType.int64]

# `SCQIndexRing` implementa l'SCQ (Single-Consumer Queue) specializzato per
# memorizzare indici interi in [0, n). I campi definiscono il ring fisico di
# dimensione 2n, le maschere per estrarre i campi packed (Index, IsSafe, Cycle)
# e i contatori atomici `head`/`tail` con padding.

    def __init__(out self, capacity: Int, full: Bool):
        # n deve essere una potenza di due; questa condizione viene verificata
        # dal costruttore pubblico SCQQueue. Di conseguenza anche 2n lo e.
        self.capacity = UInt64(capacity)
        self.ring_size = UInt64(2 * capacity)
        self.ring_mask = self.ring_size - 1
        # Il paper riserva l'ultimo valore rappresentabile, 2n-1, come bottom:
        # significa che l'entry non contiene un indice valido.
        self.index_mask = self.ring_mask
        self.index_bits = 0
        var width = self.ring_size
        while width > 1:
            self.index_bits += 1
            width >>= 1
        # Layout della word atomica, dai bit meno significativi:
        # [ Index (index_bits) | IsSafe (1 bit) | Cycle (bit restanti) ].
        self.safe_mask = UInt64(1) << UInt64(self.index_bits)
        self.entries = alloc[Atomic[DType.uint64]](2 * capacity, alignment=64)
        for i in range(2 * capacity):
            var index = self.index_mask
            var initial_cycle: UInt64 = 0
            if full and i < capacity:
                index = UInt64(i)
                # Una SCQ piena equivale ad avere inserito gli indici 0..n-1
                # partendo dallo stato vuoto Head=Tail=2n: queste entry sono
                # quindi nel ciclo 1, mentre le restanti sono ancora al ciclo 0.
                initial_cycle = 1
            self.entries[i] = Atomic[DType.uint64](self.safe_mask | index)
            if initial_cycle != 0:
                self.entries[i] = Atomic[DType.uint64](
                    (initial_cycle << UInt64(self.index_bits + 1))
                    | self.safe_mask | index
                )
        if full:
            # free_indices nasce piena: Head punta al primo indice valido e
            # Tail si trova n ticket piu avanti.
            self.head = SCQPaddedAtomicU64(self.ring_size)
            self.tail = SCQPaddedAtomicU64(
                self.ring_size + self.capacity
            )
            self.threshold = Atomic[DType.int64](Int64(3 * capacity - 1))
        else:
            # allocated_indices nasce vuota come nell'Algoritmo 6.
            self.head = SCQPaddedAtomicU64(self.ring_size)
            self.tail = SCQPaddedAtomicU64(self.ring_size)
            self.threshold = Atomic[DType.int64](-1)

# Costruttore: inizializza le entry del ring con `IsSafe` settato e
# `Index` a bottom (index_mask). Se `full` è True, le prime `capacity`
# entry contengono indici validi e hanno `Cycle=1` per rappresentare che
# sono già stati inseriti una volta.

    def __init__(out self, *, deinit take: Self):
        self.entries = take.entries
        self.capacity = take.capacity
        self.ring_size = take.ring_size
        self.ring_mask = take.ring_mask
        self.index_bits = take.index_bits
        self.index_mask = take.index_mask
        self.safe_mask = take.safe_mask
        self.head = SCQPaddedAtomicU64(
            take.head.value.load[ordering=Ordering.RELAXED]()
        )
        self.tail = SCQPaddedAtomicU64(
            take.tail.value.load[ordering=Ordering.RELAXED]()
        )
        self.threshold = Atomic[DType.int64](
            take.threshold.load[ordering=Ordering.RELAXED]()
        )

# Move-constructor: trasferisce le risorse dal valore `take` senza copiare
# l'array; carica i contatori con ordine `RELAXED` perché sono stati
# trasferiti esplicitamente.

    def __del__(deinit self):
        for i in range(Int(self.ring_size)):
            (self.entries + i).destroy_pointee()
        self.entries.free()

# Distruttore: dealloca le entry atomiche e libera la memoria dell'array.

    @always_inline
    def entry_cycle(self, entry: UInt64) -> UInt64:
        # Elimina dalla word packed Index e IsSafe.
        return entry >> UInt64(self.index_bits + 1)

# `entry_cycle`: estrae il campo Cycle spostando a destra la word packed.

    @always_inline
    def ticket_cycle(self, ticket: UInt64) -> UInt64:
        # Ogni gruppo di 2n ticket costituisce un giro del ring.
        return ticket // self.ring_size

# `ticket_cycle`: calcola quale giro (cycle) corrisponde al ticket.

    @always_inline
    def index(self, entry: UInt64) -> UInt64:
        return entry & self.index_mask

# `index`: estrae il campo Index dalla word packed usando `index_mask`.

    @always_inline
    def is_safe(self, entry: UInt64) -> Bool:
        return (entry & self.safe_mask) != 0

# `is_safe`: controlla se il bit IsSafe è impostato nella word packed.

    @always_inline
    def make_entry(self, cycle: UInt64, safe: Bool, index: UInt64) -> UInt64:
        # Costruisce atomicamente rappresentabile {Cycle, IsSafe, Index}.
        var result = cycle << UInt64(self.index_bits + 1)
        if safe:
            result |= self.safe_mask
        return result | index

# `make_entry`: costruisce la word packed a partire da `cycle`, `safe` e
# `index` posizionando i bit nei campi corretti.

    @always_inline
    def entry_at(self, ticket: UInt64) -> Self.EntryPointer:
        # Identity is correct; Cache_Remap is only a locality optimization.
        return self.entries + Int(ticket & self.ring_mask)

# `entry_at`: mappa un `ticket` alla posizione fisica nel ring usando
# `ring_mask` (equivalente a modulo ring_size, ma efficiente se power-of-two).

    def catchup(mut self, tail: UInt64, head: UInt64):
        # Un dequeue puo avanzare Head oltre Tail mentre invalida slot vuoti.
        # catchup porta Tail almeno fino a Head, evitando che gli enqueue futuri
        # debbano attraversare nuovamente tutta la distanza ormai obsoleta.
        var current_tail = tail
        var current_head = head
        while not self.tail.value.compare_exchange[
            success_ordering=Ordering.RELAXED,
            failure_ordering=Ordering.RELAXED,
        ](current_tail, current_head):
            current_head = self.head.value.load[ordering=Ordering.RELAXED]()
            current_tail = self.tail.value.load[ordering=Ordering.RELAXED]()
            if current_tail >= current_head:
                return

# `catchup`: se un dequeuer ha avanzato `head` oltre `tail`, aggiorna
# `tail` per evitare che enqueue futuri debbano riprocessare ticket obsoleti.
# Usa `compare_exchange` in ciclo finché non riesce o scopre che `tail` è
# già >= `head`.

    def enqueue(mut self, index: UInt64):
        # L'enqueue interno non controlla "full": nell'architettura a due code
        # viene chiamato solo dopo aver estratto lo stesso indice dall'altra SCQ.
        while True:
            # FAA assegna a ogni producer un ticket logico distinto senza il
            # CAS conteso sui contatori tipico di altre bounded queue.
            var ticket = self.tail.value.fetch_add[
                ordering=Ordering.RELAXED
            ](1)
            var slot = self.entry_at(ticket)
            var cycle = self.ticket_cycle(ticket)
            comptime if SCQ_TEST_PAUSE_AFTER_TICKET:
                sleep(0.00001)

            # Un CAS fallito sull'entry rilegge e ritenta LO STESSO ticket
            # (Algoritmo 6, linea 19). Solo un'entry non idonea fa abbandonare
            # il ticket e tornare al FAA esterno.
            while True:
                var entry = slot[].load[ordering=Ordering.ACQUIRE]()
                if not (
                    # L'entry puo essere riciclata soltanto se appartiene a un
                    # ciclo precedente ed e gia marcata bottom.
                    self.entry_cycle(entry) < cycle
                    and self.index(entry) == self.index_mask
                    and (
                        # IsSafe=0 segnala che un dequeuer e arrivato prima.
                        # In quel caso si puo pubblicare solo se Head non ha
                        # ancora oltrepassato questo ticket.
                        self.is_safe(entry)
                        or self.head.value.load[ordering=Ordering.ACQUIRE]()
                        <= ticket
                    )
                ):
                    break
                var desired = self.make_entry(cycle, True, index)
                comptime if SCQ_TEST_PAUSE_BEFORE_PUBLISH:
                    sleep(0.00001)
                if not slot[].compare_exchange[
                    # RELEASE pubblica anche tutte le scritture avvenute prima;
                    # per allocated_indices include data[index] = item.
                    success_ordering=Ordering.RELEASE,
                    failure_ordering=Ordering.ACQUIRE,
                ](entry, desired):
                    continue
                var maximum = Int64(3 * self.capacity - 1)
                # Un enqueue riuscito ristabilisce il budget massimo con cui i
                # dequeuer possono attraversare slot mancati senza livelock.
                if self.threshold.load[ordering=Ordering.RELAXED]() != maximum:
                    Atomic[DType.int64].store[ordering=Ordering.RELEASE](
                        UnsafePointer(to=self.threshold.value), maximum
                    )
                return

# `enqueue`: pubblica un `index` nel ring usando un ticket assegnato via
# fetch_add su `tail`. Controlla che lo slot sia riciclabile (vecchio ciclo
# e index==bottom) e poi usa compare_exchange per impostare la word
# (Cycle, IsSafe=1, Index=index). Se pubblica riesce aggiorna `threshold`.

    def try_dequeue(mut self) -> Optional[UInt64]:
        # Threshold negativo e il fast path che certifica una coda vuota senza
        # incrementare ulteriormente Head.
        if self.threshold.load[ordering=Ordering.ACQUIRE]() < 0:
            return None
        while True:
            var ticket = self.head.value.fetch_add[
                ordering=Ordering.RELAXED
            ](1)
            var slot = self.entry_at(ticket)
            var cycle = self.ticket_cycle(ticket)
            comptime if SCQ_TEST_PAUSE_AFTER_TICKET:
                sleep(0.00001)

            # All retries caused by a changing entry stay on this Head ticket.
            # Anche qui un conflitto sull'entry non deve consumare un nuovo
            # ticket: Head e gia stato incrementato irrevocabilmente dal FAA.
            while True:
                var entry = slot[].load[ordering=Ordering.ACQUIRE]()
                var spins = 0
                while (
                    # Piccolo spin suggerito dal paper: concede tempo al
                    # producer che ha gia il ticket ma non ha ancora pubblicato.
                    self.entry_cycle(entry) < cycle
                    and spins < Self.EARLY_SPINS
                ):
                    entry = slot[].load[ordering=Ordering.ACQUIRE]()
                    spins += 1

                if self.entry_cycle(entry) == cycle:
                    # Il ciclo coincide: l'indice appartiene esattamente a
                    # questo ticket di dequeue.
                    if self.index(entry) == self.index_mask:
                        break
                    var found = self.index(entry)
                    # Atomic-OR emulation: OR changes only Index bits. A CAS
                    # failure (normally an IsSafe change) reloads and retries
                    # the exact same ticket, preserving the winning state.
                    var consumed = entry | self.index_mask
                    if not slot[].compare_exchange[
                        success_ordering=Ordering.ACQUIRE_RELEASE,
                        failure_ordering=Ordering.ACQUIRE,
                    ](entry, consumed):
                        continue
                    return Optional(found)

                if self.entry_cycle(entry) < cycle:
                    # Il dequeuer e arrivato prima del producer corrispondente.
                    # Se trova un vecchio valore occupato, azzera IsSafe; se lo
                    # slot e bottom, lo porta al ciclo corrente preservando safe.
                    var replacement = self.make_entry(
                        self.entry_cycle(entry), False, self.index(entry)
                    )
                    if self.index(entry) == self.index_mask:
                        replacement = self.make_entry(
                            cycle, self.is_safe(entry), self.index_mask
                        )
                    if not slot[].compare_exchange[
                        success_ordering=Ordering.ACQUIRE_RELEASE,
                        failure_ordering=Ordering.ACQUIRE,
                    ](entry, replacement):
                        continue
                break

            var observed_tail = self.tail.value.load[
                ordering=Ordering.ACQUIRE
            ]()
            if observed_tail <= ticket + 1:
                # Non risultano enqueue davanti a questo ticket: riallinea Tail,
                # penalizza Threshold e restituisce vuoto.
                self.catchup(observed_tail, ticket + 1)
                _ = self.threshold.fetch_sub[
                    ordering=Ordering.ACQUIRE_RELEASE
                ](1)
                return None
            if self.threshold.fetch_sub[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1) <= 0:
                return None

# `try_dequeue`: cerca di prelevare un `index` pubblicato. Usa `threshold`
# per un rapido path vuoto. Ottiene un ticket tramite fetch_add su `head`,
# poi osserva lo slot: se `entry` è nel ciclo atteso e contiene un index
# valido lo marca consumato con un CAS (emulando un OR su Index), altrimenti
# se l'entry appartiene a un ciclo precedente la sistema per consentire
# progressione. Se non ci sono enqueue davanti, riallinea `tail` e decrementa
# `threshold` restituendo `None`.


struct SCQQueue[T: Copyable](Movable):
    """Coda bounded MPMC generica costruita con due SCQ di indici."""

    comptime DataPointer = UnsafePointer[
        Optional[Self.T], MutExternalOrigin
    ]
    comptime SPINS_BEFORE_YIELD = 1024
    var data: Self.DataPointer
    var capacity: UInt64
    var free_indices: SCQIndexRing
    var allocated_indices: SCQIndexRing
    # count serve soltanto a estimated_len e non partecipa alla correttezza del
    # protocollo, alla rilevazione full/empty o alla gestione di Threshold.
    var count: Atomic[DType.int64]

# `SCQQueue` combina due `SCQIndexRing`: `free_indices` (indici liberi) e
# `allocated_indices` (indici pubblicati). `data` è un array di `Optional[T]`
# che contiene i payload. `count` è solo indicativo per `estimated_len`.

    def __init__(out self, size: Int = 1024):
        if not (size >= 2 and (size & (size - 1)) == 0):
            print_red_color(
                "{MoStream} Error: SCQ size must be a power of two and at least 2!"
            )
            exit(1)
        self.capacity = UInt64(size)
        self.data = alloc[Optional[Self.T]](size, alignment=64)
        for i in range(size):
            (self.data + i).init_pointee_move(Optional[Self.T](None))
        self.free_indices = SCQIndexRing(size, full=True)
        self.allocated_indices = SCQIndexRing(size, full=False)
        self.count = Atomic[DType.int64](0)

# Costruttore pubblico: valida che `size` sia potenza di due >= 2, allocando
# l'array `data` e inizializzando le due SCQ: `free_indices` parte piena,
# `allocated_indices` parte vuota. `count` inizialmente a 0.

    def __init__(out self, *, deinit take: Self):
        self.data = take.data
        self.capacity = take.capacity
        self.free_indices = take.free_indices^
        self.allocated_indices = take.allocated_indices^
        self.count = Atomic[DType.int64](
            take.count.load[ordering=Ordering.RELAXED]()
        )

# Move-constructor per `SCQQueue`: trasferisce le risorse senza copiare l'array
# `data` e ricarica `count` con ordine `RELAXED`.

    def __del__(deinit self):
        for i in range(Int(self.capacity)):
            (self.data + i).destroy_pointee()
        self.data.free()

# Distruttore: distrugge i valori in `data` e libera la memoria.

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        # 1. Acquisisce la proprieta esclusiva di una cella payload.
        var available = self.free_indices.try_dequeue()
        if not available:
            return Optional(item^)
        var index = available.value()
        # 2. Scrive il payload non atomico: nessun altro thread possiede index.
        (self.data + Int(index))[] = Optional(item^)
        comptime if SCQ_TEST_PAUSE_BEFORE_PUBLISH:
            sleep(0.00001)
        # 3. Pubblica index. Il RELEASE dell'entry in enqueue sincronizza questa
        # scrittura con l'ACQUIRE eseguito dal consumer di allocated_indices.
        self.allocated_indices.enqueue(index)
        _ = self.count.fetch_add[ordering=Ordering.RELAXED](1)
        return None

# `try_push`: tentativo non-bloccante di push.
# 1) prende un `index` libero dalla SCQ `free_indices` (se vuota restituisce
#    l'item indietro come `Optional(item)`).
# 2) scrive il payload in `data[index]` in modo non-atomico (nessun altro
#    possiede l'indice in quel momento).
# 3) pubblica l'indice in `allocated_indices.enqueue`, che fa RELEASE per
#    sincronizzare la scrittura del payload con i consumer; aggiorna `count`.

    def push(mut self, var item: Self.T):
        # API bloccante compatibile con MPMCQueue: se free_indices appare vuota,
        # conserva la ownership del payload e riprova con yield periodici.
        var pending = Optional(item^)
        var spins = 0
        while True:
            var result = self.try_push(pending.take())
            if not result:
                return
            pending = result^
            spins += 1
            if spins >= Self.SPINS_BEFORE_YIELD:
                sleep(0.0)
                spins = 0

# `push`: versione bloccante che riprova `try_push` finché non ha successo.
# Tiene l'ownership del payload in `pending` e dopo molti spin cede la CPU
# con `sleep(0.0)` per evitare busy-wait prolungato.

    def try_pop(mut self) -> Optional[Self.T]:
        # 1. Acquisisce un indice pubblicato da un producer.
        var allocated = self.allocated_indices.try_dequeue()
        if not allocated:
            return None
        var index = allocated.value()
        # 2. Prende il payload; l'indice non puo essere riusato finche non viene
        # reinserito in free_indices.
        var item = (self.data + Int(index))[].take()  # preleva il payload
        # 3. Restituisce la cella ai producer reinserendo l'indice in free_indices
        #    e decrementando il contatore indicativo `count`.
        self.free_indices.enqueue(index)
        _ = self.count.fetch_sub[ordering=Ordering.RELAXED](1)
        return item^

    def pop(mut self) -> Self.T:
        var spins = 0
        while True:
            var item = self.try_pop()
            if item:
                return item.take()
            spins += 1
            if spins >= Self.SPINS_BEFORE_YIELD:
                sleep(0.0)
                spins = 0

# `pop`: versione bloccante di pop che chiama `try_pop` in loop con yield
# periodici per evitare busy-wait prolungato.

    def estimated_len(self) -> Int:
        var value = self.count.load[ordering=Ordering.RELAXED]()
        if value <= 0:
            return 0
        if value >= Int64(self.capacity):
            return Int(self.capacity)
        return Int(value)

# `estimated_len`: ritorna una stima della lunghezza usando `count` (non
# perfettamente precisa perché `count` non è parte del protocollo di
# correttezza, ma utile per osservabilità).
