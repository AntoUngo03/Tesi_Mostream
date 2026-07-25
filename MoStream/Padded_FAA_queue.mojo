# Experimental padded FAA bounded MPMC queue.
#
# Combines MoStream's FAA ticket allocation and power-of-two indexing with
# Rigtorp-style cache-line-isolated slots and aligned storage.

from std.atomic import Atomic, Ordering  # importa Atomic e Ordering per operazioni atomiche
from std.collections import Optional  # importa Optional per gestire valori opzionali
from std.sys.info import size_of  # importa size_of per calcolare taglie in byte
from std.sys.terminate import exit  # importa exit per terminare il processo in caso di errore
from std.time import sleep  # importa sleep per cedere la CPU dopo troppi spin
from MoStream.utils import print_red_color  # importa print_red_color per messaggi di errore colorati


struct PaddedFAAAtomicU64:
    comptime CACHE_LINE = 64  # dimensione della cache line in byte
    comptime PAD = Self.CACHE_LINE - size_of[Atomic[DType.uint64]]()  # padding per allineare alla cache line
    var value: Atomic[DType.uint64]  # valore atomico a 64 bit
    var padding: InlineArray[UInt8, Self.PAD]  # spazio di padding per evitare false condivisioni

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)  # inizializza il valore atomico con il valore iniziale
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)  # inizializza il padding senza valori


struct PaddedFAASlot[T: Copyable](Movable):
    comptime CACHE_LINE = 64  # dimensione della cache line in byte
    comptime USED = (
        size_of[Atomic[DType.uint64]]() + size_of[Optional[Self.T]]()
    )  # byte usati da sequence e dall'Optional del dato
    comptime PAD = (Self.CACHE_LINE - (Self.USED % Self.CACHE_LINE)) % Self.CACHE_LINE  # padding per allineare il record alla cache line

    var sequence: Atomic[DType.uint64]  # sequenza atomica per lo stato del slot
    var data: Optional[Self.T]  # dato opzionale immagazzinato nello slot
    var padding: InlineArray[UInt8, Self.PAD]  # padding per separare gli slot in memoria

    def __init__(out self, sequence: UInt64):
        self.sequence = Atomic[DType.uint64](sequence)  # inizializza la sequenza dello slot
        self.data = Optional[Self.T](None)  # imposta il dato su nessun valore
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)  # inizializza il padding dello slot

    def __init__(out self, *, deinit take: Self):
        var sequence = take.sequence.load[ordering=Ordering.RELAXED]()  # legge la sequenza dallo slot originale
        self.sequence = Atomic[DType.uint64](sequence)  # ricrea l'atomico con lo stesso valore di sequenza
        self.data = take.data^  # prende il dato dall'oggetto sorgente
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)  # mantiene il padding non inizializzato


struct PaddedFAAQueue[T: Copyable](Movable):
    comptime SlotPointer = UnsafePointer[
        PaddedFAASlot[Self.T], MutExternalOrigin
    ]  # puntatore non sicuro a slot allineati
    comptime SPINS_BEFORE_YIELD = 1024  # numero di spin prima di cedere la CPU

    var slots: Self.SlotPointer  # puntatore all'array di slot
    var size: UInt64  # dimensione della coda (potenza di due)
    var mask: UInt64  # maschera per l'indicizzazione modulo size
    var enqueue_pos: PaddedFAAAtomicU64  # posizione di enqueue atomica
    var dequeue_pos: PaddedFAAAtomicU64  # posizione di dequeue atomica


    def __init__(out self, size: Int = 1024):
        if not ((size >= 2) and ((size & (size - 1)) == 0)):
            print_red_color(
                "{MoStream} Error: padded FAA queue size must be "
                "a power of two and at least 2!"
            )  # stampa un errore se la dimensione non è potenza di due o è minore di 2
            exit(1)  # termina il processo con codice 1
        self.size = UInt64(size)  # salva la dimensione della coda come UInt64
        self.mask = UInt64(size - 1)  # calcola la maschera per l'indice circolare
        self.slots = alloc[PaddedFAASlot[Self.T]](size, alignment=64)  # alloca memoria per gli slot con allineamento a 64 byte
        self.enqueue_pos = PaddedFAAAtomicU64(0)  # inizializza il contatore enqueue a 0
        self.dequeue_pos = PaddedFAAAtomicU64(0)  # inizializza il contatore dequeue a 0
        for i in range(size):
            (self.slots + i).init_pointee_move(
                PaddedFAASlot[Self.T](UInt64(i))
            )  # inizializza ogni slot con il valore di sequenza appropriato


    def __init__(out self, *, deinit take: Self):
        var enqueue = take.enqueue_pos.value.load[ordering=Ordering.RELAXED]()  # legge la posizione enqueue esistente
        var dequeue = take.dequeue_pos.value.load[ordering=Ordering.RELAXED]()  # legge la posizione dequeue esistente
        self.slots = take.slots  # riusa i puntatori agli slot dell'oggetto di input
        self.size = take.size  # copia la dimensione esistente
        self.mask = take.mask  # copia la maschera esistente
        self.enqueue_pos = PaddedFAAAtomicU64(enqueue)  # ripristina il contatore enqueue
        self.dequeue_pos = PaddedFAAAtomicU64(dequeue)  # ripristina il contatore dequeue


    def __del__(deinit self):
        for i in range(Int(self.size)):
            (self.slots + i).destroy_pointee()  # distrugge ogni slot singolarmente
        self.slots.free()  # libera la memoria allocata per gli slot


    @always_inline
    def wait_or_yield(self, spins: Int) -> Int:
        var next = spins + 1  # incrementa il contatore di spin
        if next >= Self.SPINS_BEFORE_YIELD:
            sleep(0.0)  # cede la CPU se ha spinto troppo a lungo
            return 0  # resetta il contatore di spin
        return next  # continua a spinare


    def push(mut self, var item: Self.T):
        var ticket = self.enqueue_pos.value.fetch_add[
            ordering=Ordering.RELAXED
        ](1)  # ottiene un ticket univoco per l'operazione di enqueue
        var slot = self.slots + Int(ticket & self.mask)  # calcola l'indice dello slot corrispondente
        var spins = 0  # inizializza il contatore di spin
        while slot[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != ticket:
            spins = self.wait_or_yield(spins)  # attende finché lo slot non è pronto per l'enqueue
        slot[].data = Optional(item^)  # scrive il dato nello slot
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value), ticket + 1
        )  # aggiorna la sequenza per segnalare che lo slot è pieno


    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var ticket = self.enqueue_pos.value.load[ordering=Ordering.RELAXED]()  # legge la posizione corrente di enqueue senza avanzarla
        var slot = self.slots + Int(ticket & self.mask)  # trova lo slot target per questo ticket
        if slot[].sequence.load[ordering=Ordering.ACQUIRE]() != ticket:
            return Optional(item^)  # se lo slot non è pronto, restituisce l'item all'esterno
        var expected = ticket  # imposta il valore atteso per il confronto atomico
        if not self.enqueue_pos.value.compare_exchange[
            success_ordering=Ordering.RELAXED,
            failure_ordering=Ordering.RELAXED,
        ](expected, ticket + 1):
            # se la posizione enqueue è cambiata, la collisione è temporanea
            return Optional(item^)  # restituisce l'item all'esterno
        slot[].data = Optional(item^)  # scrive il dato nello slot
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value), ticket + 1
        )  # segna lo slot come pieno
        return Optional[Self.T](None)  # indica successo senza restituire l'item


    def pop(mut self) -> Self.T:
        var ticket = self.dequeue_pos.value.fetch_add[
            ordering=Ordering.RELAXED
        ](1)  # ottiene un ticket univoco per l'operazione di dequeue
        var slot = self.slots + Int(ticket & self.mask)  # calcola lo slot corrispondente al ticket
        var expected_sequence = ticket + 1  # sequenza attesa per leggere un elemento valido
        var spins = 0  # inizializza il contatore di spin
        while slot[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != expected_sequence:
            spins = self.wait_or_yield(spins)  # aspetta finché lo slot non contiene un elemento valido
        var item = slot[].data.take()  # prende il dato dallo slot
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value), ticket + self.size
        )  # aggiorna la sequenza per indicare che lo slot è libero
        return item^  # restituisce l'elemento letto


    def try_pop(mut self) -> Optional[Self.T]:
        while True:
            var ticket = self.dequeue_pos.value.load[
                ordering=Ordering.RELAXED
            ]()  # legge la posizione corrente di dequeue senza avanzarla
            var slot = self.slots + Int(ticket & self.mask)  # calcola lo slot target
            if slot[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]() != ticket + 1:
                # se non c'è ancora un elemento valido nello slot
                if self.dequeue_pos.value.load[
                    ordering=Ordering.RELAXED
                ]() != ticket:
                    continue  # se qualcun altro ha avanzato, riprova
                return Optional[Self.T](None)  # indica coda vuota al momento
            var expected = ticket  # imposta il valore atteso per il confronto atomico
            if not self.dequeue_pos.value.compare_exchange[
                success_ordering=Ordering.RELAXED,
                failure_ordering=Ordering.RELAXED,
            ](expected, ticket + 1):
                continue  # se la posizione è cambiata, riprova
            var item = slot[].data.take()  # prende il dato dallo slot
            Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
                UnsafePointer(to=slot[].sequence.value), ticket + self.size
            )  # segna lo slot come disponibile nuovamente
            return Optional(item^)  # restituisce l'elemento preso


    def estimated_len(self) -> Int:
        var enqueue = self.enqueue_pos.value.load[ordering=Ordering.RELAXED]()  # legge il contatore enqueue
        var dequeue = self.dequeue_pos.value.load[ordering=Ordering.RELAXED]()  # legge il contatore dequeue
        if enqueue <= dequeue:
            return 0  # se non ci sono nuovi elementi, la coda è vuota
        var difference = enqueue - dequeue  # calcola quanti elementi sono stati inseriti ma non rimossi
        if difference > self.size:
            return Int(self.size)  # non può superare la dimensione massima della coda
        return Int(difference)  # restituisce la stima della lunghezza
