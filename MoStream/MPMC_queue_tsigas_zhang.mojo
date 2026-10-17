# Porting didattico del protocollo Tsigas–Zhang (SPAA 2001), figure 5–7.
# Fonte: https://www.cse.chalmers.se/~tsigas/papers/spaa01.pdf
# Implementazione sperimentale: vale l'ipotesi anti-ABA del Teorema 1 del paper.
# Due colori NON proteggono da sospensioni arbitrarie durante molti giri.
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

# Distanza gli indici senza promettere l'allineamento hardware a una cache line.
struct TsigasZhangPaddedAtomic:
    comptime PAD_BYTES = 64 - size_of[Atomic[UInt64]]()
    var value: Atomic[UInt64]
    var padding: Array[UInt8, Self.PAD_BYTES]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[UInt64](initial)
        self.padding = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

# Movable trasferisce la proprietà del buffer, senza permetterne copie accidentali.
struct TsigasZhangMPMCQueue(Movable):
    # Limite incluso: la somma con due non deve traboccare nei marcatori.
    comptime MAX_VALUE = UInt64(18446744073709551613)
    # capacity è la capacità utile; ring_size include le due celle di separazione.
    var capacity: Int
    var ring_size: Int
    var mask: UInt64
    var nodes: Pointer[Atomic[UInt64], MutUntrackedOrigin]
    # Qui head guida le letture e tail le scritture, come nel paper.
    var head: TsigasZhangPaddedAtomic
    var tail: TsigasZhangPaddedAtomic

    # Colore condiviso del giro corrente, come nella versione pubblicata SPAA.
    var vnull: Atomic[UInt64]

    def __init__(out self, ring_size: Int = 1024) raises:
        # Si accetta la lunghezza fisica esatta; nessun arrotondamento nascosto.
        if ring_size < 4 or (ring_size & (ring_size - 1)) != 0:
            raise Error("TsigasZhang: ring_size deve essere una potenza di due >= 4")
        self.ring_size = ring_size
        self.capacity = ring_size - 2
        self.mask = UInt64(ring_size - 1)
        self.nodes = unsafe_alloc[Atomic[UInt64]](ring_size)
        self.head = TsigasZhangPaddedAtomic(0)
        self.tail = TsigasZhangPaddedAtomic(1)
        self.vnull = Atomic[UInt64](1)
        for i in range(ring_size):
            # La cella zero parte col colore opposto a tutte le altre.
            self.nodes.unsafe_offset(i).unsafe_write(Atomic[UInt64](UInt64(i == 0)))

    # Il buffer non viene ricostruito: conserviamo celle, indici e colore.
    def __init__(out self, *, deinit move: Self):
        self.capacity = move.capacity
        self.ring_size = move.ring_size
        self.mask = move.mask
        self.nodes = move.nodes
        self.head = TsigasZhangPaddedAtomic(move.head.value.load())
        self.tail = TsigasZhangPaddedAtomic(move.tail.value.load())
        self.vnull = Atomic[UInt64](move.vnull.load())

    def __deinit__(deinit self):
        for i in range(self.ring_size):
            self.nodes.unsafe_offset(i).unsafe_deinit_pointee()
        self.nodes.unsafe_free()

    # None indica successo; un Optional presente restituisce il valore rifiutato.
    def try_push(mut self, item: UInt64) raises -> Optional[UInt64]:
        if item > Self.MAX_VALUE:
            raise Error("TsigasZhang: valore fuori dall'intervallo 0..2**64-3")
        # Il payload vive nella stessa parola atomica modificata dalla CAS.
        var encoded = item + 2
        while True:
            var te = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
            var ate = te
            var tt = self.nodes.unsafe_offset(ate)[].load[ordering=Ordering.SEQUENTIAL]()
            var next_index = (ate + 1) & self.mask
            # tail può essere arretrato: cerchiamo la prima cella vuota.
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
            # Prima di dichiarare pieno, aiutiamo eventualmente il lettore.
            if next_index == self.head.value.load[ordering=Ordering.SEQUENTIAL]():
                ate = (next_index + 1) & self.mask
                tt = self.nodes.unsafe_offset(ate)[].load[ordering=Ordering.SEQUENTIAL]()
                if tt > 1:
                    return Optional(item)
                if ate == 0:
                    self.vnull.store[ordering=Ordering.SEQUENTIAL](tt)
                var expected_head = next_index
                _ = self.head.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_head, ate)
                continue
            if te != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # Una CAS fallita modifica expected: non riutilizziamo quel valore.
            var expected = tt
            if self.nodes.unsafe_offset(ate)[].compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected, encoded):
                # La pubblicazione avviene sulla cella; tail è soltanto una guida.
                if (next_index & 1) == 0:
                    var expected_tail = te
                    _ = self.tail.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_tail, next_index)
                return None

    # Un Optional presente contiene il valore, compresi zero e uno.
    def try_pop(mut self) -> Optional[UInt64]:
        while True:
            var th = self.head.value.load[ordering=Ordering.SEQUENTIAL]()
            # Legge il confine PRIMA della cella: un avanzamento invalida la scansione.
            var tail_snapshot = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
            var index = (th + 1) & self.mask
            var tt = self.nodes.unsafe_offset(index)[].load[ordering=Ordering.SEQUENTIAL]()
            # head può essere arretrato rispetto alle celle già svuotate.
            while tt <= 1:
                if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]() or tail_snapshot != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                    break
                if index == tail_snapshot:
                    return None
                index = (index + 1) & self.mask
                tt = self.nodes.unsafe_offset(index)[].load[ordering=Ordering.SEQUENTIAL]()
            if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]() or tail_snapshot != self.tail.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            # Il dato esiste ma la guida di scrittura non lo registra ancora.
            if index == tail_snapshot:
                var expected_tail = index
                _ = self.tail.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_tail, (index + 1) & self.mask)
                continue
            var empty_color: UInt64
            if index != 0:
                if index < th:
                    # La scansione ha attraversato zero: recupera quel colore.
                    empty_color = self.nodes.unsafe_offset(0)[].load[ordering=Ordering.SEQUENTIAL]()
                else:
                    empty_color = self.vnull.load[ordering=Ordering.SEQUENTIAL]()
            else:
                # Consumando zero si cambia fase: 0 diventa 1 e viceversa.
                empty_color = self.vnull.load[ordering=Ordering.SEQUENTIAL]() ^ UInt64(1)
            if th != self.head.value.load[ordering=Ordering.SEQUENTIAL]():
                continue
            var expected = tt
            if self.nodes.unsafe_offset(index)[].compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected, empty_color):
                if index == 0:
                    self.vnull.store[ordering=Ordering.SEQUENTIAL](empty_color)
                # m=2: il normale aggiornamento della guida avviene sugli indici pari.
                if (index & 1) == 0:
                    var expected_head = th
                    _ = self.head.value.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](expected_head, index)
                return Optional(tt - 2)

    # Wrapper bloccante: riprova senza sospendere il task o prendere un mutex.
    def push(mut self, item: UInt64) raises:
        while self.try_push(item):
            pass

    def pop(mut self) -> UInt64:
        var result = self.try_pop()
        while not result:
            result = self.try_pop()
        return result.take()

    # Scansione O(L), esatta soltanto senza operazioni concorrenti.
    def size(self) -> Int:
        var count = 0
        for i in range(self.ring_size):
            if self.nodes.unsafe_offset(i)[].load[ordering=Ordering.SEQUENTIAL]() > 1:
                count += 1
        return count

    # Non usare questa osservazione per autorizzare una successiva estrazione.
    def empty(self) -> Bool:
        return self.size() == 0
