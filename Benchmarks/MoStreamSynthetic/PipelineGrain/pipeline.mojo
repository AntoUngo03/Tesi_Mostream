# PipelineGrain dentro la vera libreria MoStream.
#
# Topologia:  seq(GrainSource) -> seq(GrainStage) x N -> seq(GrainSink)
#
# Il lavoro totale per input I (iterazioni del kernel, ricavate da T) è fisso;
# lo stadio k esegue I//N iterazioni, più una se k < I%N, come nell'harness
# originale. Aumentando N il lavoro si frammenta e crescono i passaggi fra
# stadi: N+1 Communicator MoStream, ciascuno con 1 produttore e 1 consumatore.
# Vyukov di default, Rigtorp con -D MOSTREAM_RIGTORP=1.
#
# Differenza dall'originale: sorgente e sink sono stadi a parte (N+2 task).
# Non eseguono il kernel: la sorgente genera i valori 1..M, il sink calcola un
# digest sensibile all'ordine. Con N=1 c'è quindi comunicazione (2 code):
# la baseline senza code è la modalità "compute".
#
# Gli stadi di una Pipeline MoStream sono una tupla fissata in compilazione:
# per questo ogni N supportato ha il proprio ramo in grain_run.
#
# Modalità, richiamate da run.py:
#   grain info
#   grain calibrate
#   grain compute M I
#   grain N M I capacita on|off std|coop
# Il processo esegue un warm-up completo (non stampato) e poi una misura.
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel
from MoStream.node import NodeTrait, SeqNode

comptime FNV: UInt64 = 1099511628211
comptime SUPPORTED = "1,2,4,8,12,16,24"

# Stesso kernel xorshift a 64 bit del PipelineGrain originale.
@no_inline
def grain_work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# Quota di iterazioni dello stadio k su N: i resti valgono una iterazione.
def share(total: Int, stages: Int, k: Int) -> Int:
    var part = total // stages
    if k < total % stages:
        part += 1
    return part

# La sorgente emette i valori 1..M, gli stessi semi dell'originale.
struct GrainSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = UInt64
    comptime OutType = UInt64
    comptime name = "GrainSource"
    var emitted: Int
    var messages: Int

    def __init__(out self, messages: Int):
        self.emitted = 0
        self.messages = messages

    def next_element(mut self) -> Optional[UInt64]:
        if self.emitted == self.messages:
            return None
        self.emitted += 1
        return UInt64(self.emitted)

# Uno stadio sequenziale: applica la propria quota del kernel e inoltra.
@fieldwise_init
struct GrainStage(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = UInt64
    comptime OutType = UInt64
    comptime name = "GrainStage"
    var iterations: Int

    def compute(mut self, var input: UInt64) -> Optional[UInt64]:
        return grain_work(input, self.iterations)

# Il sink calcola il digest sensibile all'ordine e lo pubblica all'EOS
# insieme al numero di valori ricevuti: result_buf[0] = digest, result_buf[1] = conteggio.
struct GrainSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = UInt64
    comptime OutType = UInt64
    comptime name = "GrainSink"
    var result_buf: Pointer[UInt64, MutUntrackedOrigin]
    var digest: UInt64
    var count: UInt64

    def __init__(out self, result_buf: Pointer[UInt64, MutUntrackedOrigin]):
        self.result_buf = result_buf
        self.digest = 0
        self.count = 0

    def consume_element(mut self, var input: UInt64):
        self.digest = (self.digest * FNV) ^ input
        self.count += 1

    def received_eos(mut self):
        self.result_buf.unsafe_offset(0)[] = self.digest
        self.result_buf.unsafe_offset(1)[] = self.count

@always_inline
def g(total: Int, stages: Int, k: Int) -> SeqNode[GrainStage]:
    return seq(GrainStage(share(total, stages, k)))

# Configurazione e misura comuni a tutti gli N: si cronometra Pipeline.run().
def timed_run[*Ts: NodeTrait](mut pipeline: Pipeline[*Ts], capacity: Int, pinned: Bool,
                              coop: Bool, tasks: Int) raises -> Int:
    pipeline.setPinning(pinned)
    pipeline.setQueueSize(capacity)
    var begin = perf_counter_ns()
    if coop:
        pipeline.run_cooperative(tasks)
    else:
        pipeline.run()
    return Int(perf_counter_ns() - begin)

def grain_pipeline(stages: Int, messages: Int, I: Int, capacity: Int, pinned: Bool, coop: Bool,
                   result_buf: Pointer[UInt64, MutUntrackedOrigin]) raises -> Int:
    if stages == 1:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 1, 0),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 3)
    elif stages == 2:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 2, 0), g(I, 2, 1),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 4)
    elif stages == 4:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 4, 0), g(I, 4, 1), g(I, 4, 2), g(I, 4, 3),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 6)
    elif stages == 8:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 8, 0), g(I, 8, 1), g(I, 8, 2), g(I, 8, 3),
                          g(I, 8, 4), g(I, 8, 5), g(I, 8, 6), g(I, 8, 7),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 10)
    elif stages == 12:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 12, 0), g(I, 12, 1), g(I, 12, 2), g(I, 12, 3),
                          g(I, 12, 4), g(I, 12, 5), g(I, 12, 6), g(I, 12, 7),
                          g(I, 12, 8), g(I, 12, 9), g(I, 12, 10), g(I, 12, 11),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 14)
    elif stages == 16:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 16, 0), g(I, 16, 1), g(I, 16, 2), g(I, 16, 3),
                          g(I, 16, 4), g(I, 16, 5), g(I, 16, 6), g(I, 16, 7),
                          g(I, 16, 8), g(I, 16, 9), g(I, 16, 10), g(I, 16, 11),
                          g(I, 16, 12), g(I, 16, 13), g(I, 16, 14), g(I, 16, 15),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 18)
    elif stages == 24:
        var p = Pipeline((seq(GrainSource(messages)),
                          g(I, 24, 0), g(I, 24, 1), g(I, 24, 2), g(I, 24, 3),
                          g(I, 24, 4), g(I, 24, 5), g(I, 24, 6), g(I, 24, 7),
                          g(I, 24, 8), g(I, 24, 9), g(I, 24, 10), g(I, 24, 11),
                          g(I, 24, 12), g(I, 24, 13), g(I, 24, 14), g(I, 24, 15),
                          g(I, 24, 16), g(I, 24, 17), g(I, 24, 18), g(I, 24, 19),
                          g(I, 24, 20), g(I, 24, 21), g(I, 24, 22), g(I, 24, 23),
                          seq(GrainSink(result_buf))))
        return timed_run(p, capacity, pinned, coop, 26)

    raise Error("N non supportato; valori ammessi: " + SUPPORTED)

# Una prova: misura e verifica esatta del digest contro il calcolo seriale.
def grain_run(stages: Int, messages: Int, iterations: Int, capacity: Int,
              pinned: Bool, coop: Bool, report: Bool) raises:
    var result_buf = unsafe_alloc[UInt64](2)
    result_buf.unsafe_offset(0)[] = 0
    result_buf.unsafe_offset(1)[] = 0
    var elapsed = grain_pipeline(stages, messages, iterations, capacity, pinned, coop, result_buf)
    # L'oracolo applica a ogni seme le stesse quote nello stesso ordine.
    var expected = UInt64(0)
    for i in range(messages):
        var value = UInt64(i + 1)
        for k in range(stages):
            value = grain_work(value, share(iterations, stages, k))
        expected = (expected * FNV) ^ value
    assert_equal(result_buf.unsafe_offset(1)[], UInt64(messages))
    assert_equal(result_buf.unsafe_offset(0)[], expected)
    result_buf.unsafe_free()
    if report:
        print("RESULT", elapsed)

# Calibrazione lunga del kernel, identica all'originale: 20 milioni di iterazioni.
def grain_calibrate():
    var value = UInt64(1)
    var begin = perf_counter_ns()
    for _ in range(20000):
        value = grain_work(value, 1000)
    print("CALIBRATION", Float64(perf_counter_ns() - begin) / 20000000.0, value)

# Baseline senza code (equivale a N=1 dell'originale): kernel intero + digest.
def grain_compute_only(messages: Int, iterations: Int):
    var digest = UInt64(0)
    var begin = perf_counter_ns()
    for i in range(messages):
        digest = (digest * FNV) ^ grain_work(UInt64(i + 1), iterations)
    print("COMPUTE", Int(perf_counter_ns() - begin), digest)

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print("WORKERS", parallelism_level(), "SUPPORTED", SUPPORTED)
        return
    if len(args) == 2 and args[1] == "calibrate":
        grain_calibrate()
        return
    if len(args) == 4 and args[1] == "compute":
        grain_compute_only(Int(args[2]), Int(args[3]))
        return
    if len(args) != 7:
        raise Error("Uso: grain N M I capacita on|off std|coop")
    var stages = Int(args[1])
    var messages = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    var pinned = args[5] == "on"
    var coop = args[6] == "coop"
    if stages < 1 or messages < 1 or iterations < 0 or stages + 2 > parallelism_level():
        raise Error("Parametri non validi o N+2 maggiore dei worker del runtime")
    if (args[5] != "on" and args[5] != "off") or (args[6] != "std" and args[6] != "coop"):
        raise Error("pinning on|off, runtime std|coop")
    # Warm-up completo, non riportato.
    grain_run(stages, min(messages, 10000), iterations, capacity, pinned, coop, False)
    grain_run(stages, messages, iterations, capacity, pinned, coop, True)
