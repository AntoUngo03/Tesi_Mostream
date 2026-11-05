# SOLO PER MISURE: contatori degli eventi interni alle code fra stadi (copia strumentata).
# Ogni contatore sta su una propria linea di cache. Le code contano in variabili locali durante
# una chiamata e sommano qui una sola volta, alla fine, e solo se il conteggio è diverso da zero.
from std.atomic import Atomic, Ordering
from std.sys.info import size_of

# contatore atomico distanziato dagli altri
struct PaddedCounter:
    comptime PAD_BYTES = 64 - size_of[Atomic[Int64]]()
    var value: Atomic[Int64]
    var pad: Array[UInt8, Self.PAD_BYTES]

    def __init__(out self):
        self.value = Atomic[Int64](0)
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

    # somma n se diverso da zero
    @always_inline
    def add(mut self, n: Int):
        if n != 0:
            _ = self.value.fetch_add[ordering=Ordering.RELAXED](Int64(n))

    def get(self) -> Int:
        return Int(self.value.load[ordering=Ordering.RELAXED]())

# eventi contati per una coda
struct QueueStats:
    var push_cas_fail: PaddedCounter # push: CAS di prenotazione fallita (solo Vyukov)
    var push_stale: PaddedCounter # push: posizione letta già superata da un altro produttore (solo Vyukov)
    var push_full: PaddedCounter # push: giri di attesa perché la cella non è ancora libera
    var pop_cas_fail: PaddedCounter # try_pop: CAS di prenotazione fallita
    var pop_stale: PaddedCounter # try_pop: posizione già consumata da un altro consumatore, si riprova
    var pop_empty: PaddedCounter # try_pop a coda vuota (contati dal Communicator in pop)

    def __init__(out self):
        self.push_cas_fail = PaddedCounter()
        self.push_stale = PaddedCounter()
        self.push_full = PaddedCounter()
        self.pop_cas_fail = PaddedCounter()
        self.pop_stale = PaddedCounter()
        self.pop_empty = PaddedCounter()

    def report(self) -> String:
        return ("push_cas_fail=" + String(self.push_cas_fail.get()) + " push_stale=" + String(self.push_stale.get())
                + " push_full=" + String(self.push_full.get()) + " pop_cas_fail=" + String(self.pop_cas_fail.get())
                + " pop_stale=" + String(self.pop_stale.get()) + " pop_empty=" + String(self.pop_empty.get()))
