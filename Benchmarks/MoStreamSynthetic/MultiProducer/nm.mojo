# MultiProducer: una sola coda con P produttori e C consumatori, dentro MoStream.
#
#     parallel(NMSource, P) -> parallel(NMSink, C)
#
# È il benchmark che mancava: FarmThroughput misura 1:N (contesa solo fra i
# consumatori), PipelineGrain 1:1 (nessuna contesa). Qui ci sono più produttori
# sulla stessa coda, quindi contesa sul push:
#   - scenario N:1  (C = 1): il lavoro T lo fanno i produttori;
#   - scenario N:M  (C = P): il lavoro T è diviso fra produttore e consumatore.
# L'unica coda è il Communicator MoStream fra i due stadi: Vyukov di default,
# Rigtorp con -D MOSTREAM_RIGTORP=1. I task sono P + C.
#
# Ogni produttore, alla prima chiamata, prende un indice esclusivo r con un solo
# fetch_add ed emette gli ID r+1, r+1+P, r+1+2P, ... fino a M: gli ID 1..M sono
# divisi fra i produttori senza sovrapposizioni. Il produttore calcola
# value = work(id, Ip); il consumatore calcola work(value, Ic), scrive l'ID nel
# proprio log privato e aggiorna un digest locale. Dopo la misura si verifica
# che ogni ID arrivi esattamente una volta e si ricalcola il digest di ogni
# consumatore sullo stesso ordine di ricezione.
#
# Modalità, richiamate da run.py:
#   nm info
#   nm calibrate
#   nm compute M I
#   nm M P C Ip Ic capacita on|off std|coop
# Il processo esegue un warm-up completo (non stampato) e poi una misura.
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, parallel

comptime FNV: UInt64 = 1099511628211

# Stesso kernel xorshift a 64 bit di FarmThroughput e PipelineGrain.
@no_inline
def nm_work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# Messaggio: ID per la verifica e valore già elaborato dal produttore.
@fieldwise_init
struct Item(Copyable):
    var id: Int
    var value: UInt64

# Produttore replicato: emette la propria parte degli ID 1..M.
struct NMSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Item
    comptime OutType = Item
    comptime name = "NMSource"
    var messages: Int
    var producers: Int
    var iterations: Int
    var next_rid: Pointer[Atomic[Int64], MutUntrackedOrigin]
    var next_id: Int # prossimo ID da emettere; 0 = indice di replica non ancora preso

    def __init__(out self, messages: Int, producers: Int, iterations: Int,
                 next_rid: Pointer[Atomic[Int64], MutUntrackedOrigin]):
        self.messages = messages
        self.producers = producers
        self.iterations = iterations
        self.next_rid = next_rid
        self.next_id = 0

    def next_element(mut self) -> Optional[Item]:
        if self.next_id == 0:
            # Un solo fetch_add per replica, non per messaggio.
            self.next_id = Int(self.next_rid[].fetch_add[ordering=Ordering.ACQUIRE_RELEASE](1)) + 1
        if self.next_id > self.messages:
            return None
        var id = self.next_id
        self.next_id += self.producers
        return Item(id, nm_work(UInt64(id), self.iterations))

# Consumatore replicato: completa il lavoro, registra l'ID nel log privato.
struct NMSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Item
    comptime OutType = Item
    comptime name = "NMSink"
    var iterations: Int
    var messages: Int
    var next_rid: Pointer[Atomic[Int64], MutUntrackedOrigin]
    var logs: Pointer[Int, MutUntrackedOrigin]
    var counts: Pointer[Int, MutUntrackedOrigin]
    var digests: Pointer[UInt64, MutUntrackedOrigin]
    var rid: Int
    var base: Int
    var count: Int
    var digest: UInt64

    def __init__(out self, iterations: Int, messages: Int,
                 next_rid: Pointer[Atomic[Int64], MutUntrackedOrigin],
                 logs: Pointer[Int, MutUntrackedOrigin],
                 counts: Pointer[Int, MutUntrackedOrigin],
                 digests: Pointer[UInt64, MutUntrackedOrigin]):
        self.iterations = iterations
        self.messages = messages
        self.next_rid = next_rid
        self.logs = logs
        self.counts = counts
        self.digests = digests
        self.rid = -1
        self.base = 0
        self.count = 0
        self.digest = 0

    def consume_element(mut self, var input: Item):
        if self.rid < 0:
            self.rid = Int(self.next_rid[].fetch_add[ordering=Ordering.ACQUIRE_RELEASE](1))
            self.base = self.rid * self.messages
        var result = nm_work(input.value, self.iterations)
        # Più di M input a una replica implica duplicati: il contatore li rende visibili.
        if self.count < self.messages:
            self.logs.unsafe_offset(self.base + self.count)[] = input.id
        self.count += 1
        self.digest = (self.digest * FNV) ^ result

    def received_eos(mut self):
        if self.rid >= 0:
            self.counts.unsafe_offset(self.rid)[] = self.count
            self.digests.unsafe_offset(self.rid)[] = self.digest

# Una prova completa: allocazioni fuori dal timer, verifica esatta dopo.
def nm_run(messages: Int, producers: Int, consumers: Int, ip: Int, ic: Int, capacity: Int,
           pinned: Bool, coop: Bool, report: Bool) raises:
    var src_rid = unsafe_alloc[Atomic[Int64]](1)
    src_rid.unsafe_write(Atomic[Int64](0))
    var dst_rid = unsafe_alloc[Atomic[Int64]](1)
    dst_rid.unsafe_write(Atomic[Int64](0))
    var logs = unsafe_alloc[Int](consumers * messages)
    for i in range(consumers * messages):
        logs.unsafe_offset(i)[] = 0
    var counts = unsafe_alloc[Int](consumers)
    var digests = unsafe_alloc[UInt64](consumers)
    for r in range(consumers):
        counts.unsafe_offset(r)[] = 0
        digests.unsafe_offset(r)[] = 0
    var pipeline = Pipeline((parallel(NMSource(messages, producers, ip, src_rid), producers),
                             parallel(NMSink(ic, messages, dst_rid, logs, counts, digests), consumers)))
    pipeline.setPinning(pinned)
    pipeline.setQueueSize(capacity)
    var begin = perf_counter_ns()
    if coop:
        pipeline.run_cooperative(producers + consumers)
    else:
        pipeline.run()
    var elapsed = Int(perf_counter_ns() - begin)
    _ = pipeline
    # Verifica: ogni ID esattamente una volta, digest di ogni consumatore ricalcolato.
    var active = Int(dst_rid[].load[ordering=Ordering.ACQUIRE]())
    var seen = unsafe_alloc[Int](messages + 1)
    for i in range(messages + 1):
        seen.unsafe_offset(i)[] = 0
    var total = 0
    for r in range(active):
        var n = counts.unsafe_offset(r)[]
        assert_equal(n <= messages, True)
        total += n
        var expected = UInt64(0)
        for k in range(n):
            var id = logs.unsafe_offset(r * messages + k)[]
            assert_equal(id >= 1 and id <= messages, True)
            seen.unsafe_offset(id)[] += 1
            expected = (expected * FNV) ^ nm_work(nm_work(UInt64(id), ip), ic)
        assert_equal(digests.unsafe_offset(r)[], expected)
    assert_equal(total, messages)
    for i in range(1, messages + 1):
        assert_equal(seen.unsafe_offset(i)[], 1)
    if report:
        var line = String("RESULT ") + String(elapsed) + " "
        for r in range(consumers):
            if r > 0:
                line += ","
            line += String(counts.unsafe_offset(r)[])
        print(line)
    seen.unsafe_free()
    digests.unsafe_free()
    counts.unsafe_free()
    logs.unsafe_free()
    dst_rid.unsafe_deinit_pointee()
    dst_rid.unsafe_free()
    src_rid.unsafe_deinit_pointee()
    src_rid.unsafe_free()

# Calibrazione del kernel, identica agli altri benchmark: 20 milioni di iterazioni.
def nm_calibrate():
    var value = UInt64(1)
    var begin = perf_counter_ns()
    for _ in range(20000):
        value = nm_work(value, 1000)
    print("CALIBRATION", Float64(perf_counter_ns() - begin) / 20000000.0, value)

# Baseline senza comunicazione: costo del kernel intero più il digest, per input.
def nm_compute_only(messages: Int, iterations: Int):
    var checksum = UInt64(0)
    var begin = perf_counter_ns()
    for i in range(messages):
        checksum = (checksum * FNV) ^ nm_work(UInt64(i + 1), iterations)
    print("COMPUTE", Int(perf_counter_ns() - begin), checksum)

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    if len(args) == 2 and args[1] == "calibrate":
        nm_calibrate()
        return
    if len(args) == 4 and args[1] == "compute":
        nm_compute_only(Int(args[2]), Int(args[3]))
        return
    if len(args) != 9:
        raise Error("Uso: nm M P C Ip Ic capacita on|off std|coop")
    var messages = Int(args[1])
    var producers = Int(args[2])
    var consumers = Int(args[3])
    var ip = Int(args[4])
    var ic = Int(args[5])
    var capacity = Int(args[6])
    var pinned = args[7] == "on"
    var coop = args[8] == "coop"
    if messages < 1 or producers < 1 or consumers < 1 or ip < 0 or ic < 0:
        raise Error("Parametri non validi")
    if producers + consumers > parallelism_level():
        raise Error("P + C maggiore dei worker del runtime")
    if (args[7] != "on" and args[7] != "off") or (args[8] != "std" and args[8] != "coop"):
        raise Error("pinning on|off, runtime std|coop")
    # Warm-up completo, non riportato.
    nm_run(min(messages, 10000), producers, consumers, ip, ic, capacity, pinned, coop, False)
    nm_run(messages, producers, consumers, ip, ic, capacity, pinned, coop, True)
