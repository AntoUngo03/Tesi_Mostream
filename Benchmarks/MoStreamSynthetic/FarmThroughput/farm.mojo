# FarmThroughput dentro la vera libreria MoStream.
#
# Topologia, identica al FarmThroughput dell'harness (una sorgente, una coda
# condivisa, N consumatori, nessun sink separato):
#
#     seq(FarmSource) -> parallel(FarmWorker, N)
#
# FarmWorker è uno stadio SINK replicato: ogni ID viene estratto da UNA sola
# replica, che esegue l'intero lavoro T. I task sono N+1. L'unica coda è il
# Communicator MoStream fra i due stadi (1 produttore, N consumatori): Vyukov di
# default, Rigtorp con -D MOSTREAM_RIGTORP=1.
#
# Modalità, richiamate da run.py:
#   farm info
#   farm calibrate
#   farm compute M I
#   farm M N I capacita on|off std|coop
# M = input, N = consumatori, I = iterazioni del kernel per input.
# Il processo esegue un warm-up completo (non stampato) e poi una misura.
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

comptime FNV: UInt64 = 1099511628211

# Stesso kernel xorshift a 64 bit del FarmThroughput originale.
@no_inline
def farm_work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# La sorgente genera gli ID 1..M (lo zero è punto fisso di xorshift).
# timing[0] = primo next_element, timing[1] = ritorno di None: il secondo
# istante segue l'ultimo push completato, quindi include l'attesa di spazio.
struct FarmSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "FarmSource"
    var emitted: Int
    var messages: Int
    var timing: Pointer[Int, MutUntrackedOrigin]

    def __init__(out self, messages: Int, timing: Pointer[Int, MutUntrackedOrigin]):
        self.emitted = 0
        self.messages = messages
        self.timing = timing

    def next_element(mut self) -> Optional[Int]:
        if self.emitted == 0:
            self.timing.unsafe_offset(0)[] = Int(perf_counter_ns())
        if self.emitted == self.messages:
            self.timing.unsafe_offset(1)[] = Int(perf_counter_ns())
            return None
        self.emitted += 1
        return self.emitted

# Consumatore replicato. Al primo input la replica ottiene un indice esclusivo
# con un solo fetch_add (non per messaggio). Poi scrive gli ID ricevuti nel
# proprio log privato (righe di M celle: nessuna condivisione fra repliche) e
# aggiorna un digest locale. Contatore e digest sono pubblicati all'EOS.
# Il lavoro per input è quindi: kernel + una scrittura sequenziale + un digest,
# come nell'harness originale.
struct FarmWorker(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Int
    comptime OutType = Int
    comptime name = "FarmWorker"
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

    def consume_element(mut self, var input: Int):
        if self.rid < 0:
            self.rid = Int(self.next_rid[].fetch_add[ordering=Ordering.ACQUIRE_RELEASE](1))
            self.base = self.rid * self.messages
        var result = farm_work(UInt64(input), self.iterations)
        # Più di M input a una replica implica duplicati: non si scrive fuori dal log,
        # il contatore li rende comunque visibili alla verifica.
        if self.count < self.messages:
            self.logs.unsafe_offset(self.base + self.count)[] = input
        self.count += 1
        self.digest = (self.digest * FNV) ^ result

    def received_eos(mut self):
        if self.rid >= 0:
            self.counts.unsafe_offset(self.rid)[] = self.count
            self.digests.unsafe_offset(self.rid)[] = self.digest

# Una prova completa: allocazioni e preparazione fuori dal timer, verifica esatta dopo.
# Restituisce (elapsed_ns, source_ns) e stampa i conteggi per replica.
def farm_run(messages: Int, workers: Int, iterations: Int, capacity: Int,
             pinned: Bool, coop: Bool, report: Bool) raises:
    var timing = unsafe_alloc[Int](2)
    timing.unsafe_offset(0)[] = 0
    timing.unsafe_offset(1)[] = 0
    var next_rid = unsafe_alloc[Atomic[Int64]](1)
    next_rid.unsafe_write(Atomic[Int64](0))
    var logs = unsafe_alloc[Int](workers * messages)
    # Tocca tutte le pagine del log prima della misura.
    for i in range(workers * messages):
        logs.unsafe_offset(i)[] = 0
    var counts = unsafe_alloc[Int](workers)
    var digests = unsafe_alloc[UInt64](workers)
    for r in range(workers):
        counts.unsafe_offset(r)[] = 0
        digests.unsafe_offset(r)[] = 0
    var pipeline = Pipeline((seq(FarmSource(messages, timing)),
                             parallel(FarmWorker(iterations, messages, next_rid, logs, counts, digests), workers)))
    pipeline.setPinning(pinned)
    pipeline.setQueueSize(capacity)
    var begin = perf_counter_ns()
    if coop:
        pipeline.run_cooperative(workers + 1)
    else:
        pipeline.run()
    var elapsed = Int(perf_counter_ns() - begin)
    _ = pipeline
    # Verifica fuori dal tempo: ogni ID esattamente una volta e digest di ogni
    # replica ricalcolato sullo stesso ordine di ricezione.
    var active = Int(next_rid[].load[ordering=Ordering.ACQUIRE]())
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
            expected = (expected * FNV) ^ farm_work(UInt64(id), iterations)
        assert_equal(digests.unsafe_offset(r)[], expected)
    assert_equal(total, messages)
    for i in range(1, messages + 1):
        assert_equal(seen.unsafe_offset(i)[], 1)
    if report:
        var line = String("RESULT ") + String(elapsed) + " " + String(timing.unsafe_offset(1)[] - timing.unsafe_offset(0)[]) + " "
        for r in range(workers):
            if r > 0:
                line += ","
            line += String(counts.unsafe_offset(r)[])
        print(line)
    seen.unsafe_free()
    digests.unsafe_free()
    counts.unsafe_free()
    logs.unsafe_free()
    next_rid.unsafe_deinit_pointee()
    next_rid.unsafe_free()
    timing.unsafe_free()

# Calibrazione lunga del kernel, identica all'originale: 20 milioni di iterazioni.
def farm_calibrate():
    var value = UInt64(1)
    var begin = perf_counter_ns()
    for _ in range(20000):
        value = farm_work(value, 1000)
    print("CALIBRATION", Float64(perf_counter_ns() - begin) / 20000000.0, value)

# Baseline senza comunicazione: costo effettivo del kernel e del digest per input.
def farm_compute_only(messages: Int, iterations: Int):
    var checksum = UInt64(0)
    var begin = perf_counter_ns()
    for i in range(messages):
        checksum = (checksum * FNV) ^ farm_work(UInt64(i + 1), iterations)
    print("COMPUTE", Int(perf_counter_ns() - begin), checksum)

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    if len(args) == 2 and args[1] == "calibrate":
        farm_calibrate()
        return
    if len(args) == 4 and args[1] == "compute":
        farm_compute_only(Int(args[2]), Int(args[3]))
        return
    if len(args) != 7:
        raise Error("Uso: farm M N I capacita on|off std|coop")
    var messages = Int(args[1])
    var workers = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    var pinned = args[5] == "on"
    var coop = args[6] == "coop"
    if messages < 1 or workers < 1 or iterations < 0 or workers + 1 > parallelism_level():
        raise Error("Parametri non validi o N+1 maggiore dei worker del runtime")
    if (args[5] != "on" and args[5] != "off") or (args[6] != "std" and args[6] != "coop"):
        raise Error("pinning on|off, runtime std|coop")
    # Warm-up completo, non riportato.
    farm_run(min(messages, 10000), workers, iterations, capacity, pinned, coop, False)
    farm_run(messages, workers, iterations, capacity, pinned, coop, True)
