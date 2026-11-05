# Variante di farm.mojo con la sorgente replicata, attraverso la vera API MoStream:
# Source replicato su S repliche -> Compute replicato su N worker -> Sink sequenziale.
# La coda fra sorgenti e worker ha quindi S produttori e N consumatori (MPMC);
# quella fra worker e sink ha N produttori e un consumatore. Resta una sola farm.
# Ogni input viene elaborato interamente da UNA replica di Compute.
# La coda fra gli stadi viene scelta in compilazione dal Communicator:
# default Vyukov; con -D MOSTREAM_RIGTORP=1 si usa Rigtorp.
# Con S = 1 la topologia coincide con quella di farm.mojo.
#
# Modalità del programma, richiamate dal runner Python run_multisource.py:
#   binary info
#   binary calibrate
#   binary M S N iterazioni capacita
# M = numero totale di input, diviso fra le S sorgenti; N = repliche Compute.
# I task totali sono S + N + 1.
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

# Kernel sintetico xorshift, identico a farm.mojo: ogni operazione dipende dal
# risultato precedente; iterations determina la quantità di calcolo.
@no_inline
def work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# Messaggio trasportato dalle code: ID per verificare la consegna e valore da elaborare.
@fieldwise_init
struct Item(Copyable):
    var id: Int
    var value: UInt64

# Sorgente replicata: le S repliche producono insieme gli M messaggi con ID da 0 a M-1.
# parallel() crea S copie dello stadio, tutte con gli stessi campi: alla prima chiamata
# ogni copia prende un indice di replica r dal contatore condiviso (una sola fetch_add
# per replica, quindi nessuna contesa durante la prova) e produce gli ID r, r+S, r+2S, ...
# Così ogni ID è prodotto da una sola replica e le repliche non si sincronizzano fra loro.
struct Source(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Source"
    var next_replica: Pointer[Atomic[Int64], MutUntrackedOrigin]
    var sources: Int
    var messages: Int
    var index: Int # prossimo ID da produrre; -1 finché la replica non ha preso il suo indice

    def __init__(out self, next_replica: Pointer[Atomic[Int64], MutUntrackedOrigin], sources: Int, messages: Int):
        self.next_replica = next_replica
        self.sources = sources
        self.messages = messages
        self.index = -1

    # Il runtime richiama questo metodo finché riceve None, che termina la replica.
    def next_element(mut self) -> Optional[Item]:
        if self.index < 0:
            self.index = Int(self.next_replica[].fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(1)))
        if self.index >= self.messages:
            return None
        var index = self.index
        self.index += self.sources
        # Il seme è ID+1 per evitare lo zero, punto fisso del kernel xorshift.
        return Item(index, UInt64(index + 1))

# Trasformazione replicata, identica a farm.mojo.
@fieldwise_init
struct Compute(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Compute"
    var iterations: Int

    def compute(mut self, var input: Item) -> Optional[Item]:
        input.value = work(input.value, self.iterations)
        return input^

# Un solo sink raccoglie i risultati, identico a farm.mojo.
# seen[id] conta le ricezioni; values[id] conserva il risultato del calcolo.
@fieldwise_init
struct Sink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Sink"
    var seen: Pointer[Int, MutUntrackedOrigin]
    var values: Pointer[UInt64, MutUntrackedOrigin]
    var messages: Int

    def consume_element(mut self, var input: Item):
        # La cella aggiuntiva seen[M] conta gli ID non validi.
        if input.id < 0 or input.id >= self.messages:
            self.seen.unsafe_offset(self.messages)[] += 1
        else:
            self.seen.unsafe_offset(input.id)[] += 1
            self.values.unsafe_offset(input.id)[] = input.value


def main() raises:
    var args = argv()
    # Comunica quanti worker offre il runtime Mojo: devono bastare per S+N+1 task.
    if args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    # Calibrazione del kernel, identica a farm.mojo.
    if args[1] == "calibrate":
        var value = UInt64(12345)
        var begin = perf_counter_ns()
        value = work(value, 2000000)
        print("CALIBRATION", Float64(perf_counter_ns() - begin) / 2000000.0, value)
        return
    var messages = Int(args[1])
    var sources = Int(args[2])
    var workers = Int(args[3])
    var iterations = Int(args[4])
    var capacity = Int(args[5])
    # Buffer di verifica e contatore delle repliche, allocati prima del cronometro.
    var seen = unsafe_alloc[Int](messages + 1)
    var values = unsafe_alloc[UInt64](messages)
    for i in range(messages + 1):
        seen.unsafe_offset(i)[] = 0
    var next_replica = unsafe_alloc[Atomic[Int64]](1)
    next_replica.unsafe_write(Atomic[Int64](0))
    # parallel crea S repliche della sorgente e N della trasformazione: S+N+1 task.
    var pipeline = Pipeline((parallel(Source(next_replica, sources, messages), sources),
                             parallel(Compute(iterations), workers),
                             seq(Sink(seen, values, messages))))
    # Stesse impostazioni di farm.mojo: nessun pinning, stessa capacità per i due backend.
    pipeline.setPinning(False)
    pipeline.setQueueSize(capacity)
    # Si misura il tempo di parete di Pipeline.run(), come in farm.mojo.
    var begin = perf_counter_ns()
    pipeline.run()
    var elapsed = Int(perf_counter_ns() - begin)
    # Verifica esatta dopo aver fermato il cronometro: tutte le S repliche hanno
    # preso un indice, ogni ID compare una volta e il valore coincide con quello sequenziale.
    assert_equal(Int(next_replica[].load()), sources)
    for i in range(messages):
        assert_equal(seen.unsafe_offset(i)[], 1)
        assert_equal(values.unsafe_offset(i)[], work(UInt64(i + 1), iterations))
    assert_equal(seen.unsafe_offset(messages)[], 0)
    seen.unsafe_free()
    values.unsafe_free()
    next_replica.unsafe_deinit_pointee()
    next_replica.unsafe_free()
    # Il runner legge RESULT e calcola throughput = M * 1e9 / elapsed.
    print("RESULT", elapsed)
