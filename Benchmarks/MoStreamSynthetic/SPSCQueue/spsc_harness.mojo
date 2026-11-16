# Microbenchmark 1:1 sulla coda nuda (senza runtime MoStream).
#
# Un produttore e un consumatore, ciascuno fissato a un core, si passano M valori
# UInt64 attraverso una coda limitata. Si misura il tempo dalla partenza comune
# (barriera) all'ultimo pop, e si riporta il costo per elemento in ns.
# Confronta tre code: Vyukov (MPMCQueue), Rigtorp (RigtorpMPMCQueue) e la SPSC
# di FastFlow (FastFlowSPSCQueue), tutte con push e pop bloccanti.
# Il consumatore verifica che i valori arrivino tutti e in ordine (FIFO).
#
# Uso: spsc_harness KIND M capacita cpu_produttore cpu_consumatore ripetizioni
#      KIND = vyukov | rigtorp | spsc ; cpu = -1 per non fissare il thread.
# Stampa: RESULT KIND ns_per_elemento (una riga per ripetizione, dopo un warm-up).
from std.atomic import Atomic, Ordering
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime._asyncrt import TaskGroup
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream.MPMC_queue import MPMCQueue
from MoStream.MPMC_queue_rigtorp import RigtorpMPMCQueue
from MoStream.SPSC_queue_fastflow import FastFlowSPSCQueue
from MoStream.pipeline import pin_thread_to_cpu

comptime CP = Pointer[Atomic[UInt64], MutUntrackedOrigin]

# Barriera: ogni thread si dichiara pronto e aspetta il via.
def wait_start(ready: CP, start: CP):
    _ = ready[].fetch_add[ordering=Ordering.RELEASE](1)
    while start[].load[ordering=Ordering.ACQUIRE]() == 0:
        pass

comptime VyukovPtr = Pointer[MPMCQueue[UInt64], MutUntrackedOrigin]

async def vyukov_producer(q: VyukovPtr, m: Int, cpu: Int, ready: CP, start: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    for i in range(m):
        q[].push(UInt64(i))

async def vyukov_consumer(q: VyukovPtr, m: Int, cpu: Int, ready: CP, start: CP, done_ns: CP, bad: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    var errors = UInt64(0)
    for i in range(m):
        if q[].pop() != UInt64(i):
            errors += 1
    done_ns[].store(UInt64(perf_counter_ns()))
    bad[].store(errors)

def vyukov_run(m: Int, capacity: Int, cpu_p: Int, cpu_c: Int) raises -> Float64:
    var q = unsafe_alloc[MPMCQueue[UInt64]](1)
    q.unsafe_write(MPMCQueue[UInt64](capacity))
    var ready = unsafe_alloc[Atomic[UInt64]](1); ready.unsafe_write(Atomic[UInt64](0))
    var start = unsafe_alloc[Atomic[UInt64]](1); start.unsafe_write(Atomic[UInt64](0))
    var done_ns = unsafe_alloc[Atomic[UInt64]](1); done_ns.unsafe_write(Atomic[UInt64](0))
    var bad = unsafe_alloc[Atomic[UInt64]](1); bad.unsafe_write(Atomic[UInt64](0))
    var tg = TaskGroup()
    tg.create_task(vyukov_producer(q, m, cpu_p, ready, start))
    tg.create_task(vyukov_consumer(q, m, cpu_c, ready, start, done_ns, bad))
    while ready[].load[ordering=Ordering.ACQUIRE]() != 2:
        pass
    var begin = UInt64(perf_counter_ns())
    start[].store[ordering=Ordering.RELEASE](1)
    tg.wait()
    var elapsed = done_ns[].load() - begin
    assert_equal(bad[].load(), UInt64(0))
    q.unsafe_deinit_pointee(); q.unsafe_free()
    for p in [ready, start, done_ns, bad]:
        p.unsafe_deinit_pointee(); p.unsafe_free()
    return Float64(elapsed) / Float64(m)

comptime RigtorpPtr = Pointer[RigtorpMPMCQueue[UInt64], MutUntrackedOrigin]

async def rigtorp_producer(q: RigtorpPtr, m: Int, cpu: Int, ready: CP, start: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    for i in range(m):
        q[].push(UInt64(i))

async def rigtorp_consumer(q: RigtorpPtr, m: Int, cpu: Int, ready: CP, start: CP, done_ns: CP, bad: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    var errors = UInt64(0)
    for i in range(m):
        if q[].pop() != UInt64(i):
            errors += 1
    done_ns[].store(UInt64(perf_counter_ns()))
    bad[].store(errors)

def rigtorp_run(m: Int, capacity: Int, cpu_p: Int, cpu_c: Int) raises -> Float64:
    var q = unsafe_alloc[RigtorpMPMCQueue[UInt64]](1)
    q.unsafe_write(RigtorpMPMCQueue[UInt64](capacity))
    var ready = unsafe_alloc[Atomic[UInt64]](1); ready.unsafe_write(Atomic[UInt64](0))
    var start = unsafe_alloc[Atomic[UInt64]](1); start.unsafe_write(Atomic[UInt64](0))
    var done_ns = unsafe_alloc[Atomic[UInt64]](1); done_ns.unsafe_write(Atomic[UInt64](0))
    var bad = unsafe_alloc[Atomic[UInt64]](1); bad.unsafe_write(Atomic[UInt64](0))
    var tg = TaskGroup()
    tg.create_task(rigtorp_producer(q, m, cpu_p, ready, start))
    tg.create_task(rigtorp_consumer(q, m, cpu_c, ready, start, done_ns, bad))
    while ready[].load[ordering=Ordering.ACQUIRE]() != 2:
        pass
    var begin = UInt64(perf_counter_ns())
    start[].store[ordering=Ordering.RELEASE](1)
    tg.wait()
    var elapsed = done_ns[].load() - begin
    assert_equal(bad[].load(), UInt64(0))
    q.unsafe_deinit_pointee(); q.unsafe_free()
    for p in [ready, start, done_ns, bad]:
        p.unsafe_deinit_pointee(); p.unsafe_free()
    return Float64(elapsed) / Float64(m)

comptime SpscPtr = Pointer[FastFlowSPSCQueue[UInt64], MutUntrackedOrigin]

async def spsc_producer(q: SpscPtr, m: Int, cpu: Int, ready: CP, start: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    for i in range(m):
        q[].push(UInt64(i))

async def spsc_consumer(q: SpscPtr, m: Int, cpu: Int, ready: CP, start: CP, done_ns: CP, bad: CP):
    try:
        _ = pin_thread_to_cpu(cpu)
    except:
        pass
    wait_start(ready, start)
    var errors = UInt64(0)
    for i in range(m):
        if q[].pop() != UInt64(i):
            errors += 1
    done_ns[].store(UInt64(perf_counter_ns()))
    bad[].store(errors)

def spsc_run(m: Int, capacity: Int, cpu_p: Int, cpu_c: Int) raises -> Float64:
    var q = unsafe_alloc[FastFlowSPSCQueue[UInt64]](1)
    q.unsafe_write(FastFlowSPSCQueue[UInt64](capacity))
    var ready = unsafe_alloc[Atomic[UInt64]](1); ready.unsafe_write(Atomic[UInt64](0))
    var start = unsafe_alloc[Atomic[UInt64]](1); start.unsafe_write(Atomic[UInt64](0))
    var done_ns = unsafe_alloc[Atomic[UInt64]](1); done_ns.unsafe_write(Atomic[UInt64](0))
    var bad = unsafe_alloc[Atomic[UInt64]](1); bad.unsafe_write(Atomic[UInt64](0))
    var tg = TaskGroup()
    tg.create_task(spsc_producer(q, m, cpu_p, ready, start))
    tg.create_task(spsc_consumer(q, m, cpu_c, ready, start, done_ns, bad))
    while ready[].load[ordering=Ordering.ACQUIRE]() != 2:
        pass
    var begin = UInt64(perf_counter_ns())
    start[].store[ordering=Ordering.RELEASE](1)
    tg.wait()
    var elapsed = done_ns[].load() - begin
    assert_equal(bad[].load(), UInt64(0))
    q.unsafe_deinit_pointee(); q.unsafe_free()
    for p in [ready, start, done_ns, bad]:
        p.unsafe_deinit_pointee(); p.unsafe_free()
    return Float64(elapsed) / Float64(m)

def run_kind(kind: String, m: Int, capacity: Int, cpu_p: Int, cpu_c: Int) raises -> Float64:
    if kind == "vyukov":
        return vyukov_run(m, capacity, cpu_p, cpu_c)
    if kind == "rigtorp":
        return rigtorp_run(m, capacity, cpu_p, cpu_c)
    if kind == "spsc":
        return spsc_run(m, capacity, cpu_p, cpu_c)
    raise Error("KIND: vyukov | rigtorp | spsc")

def main() raises:
    var a = argv()
    if len(a) != 7:
        raise Error("Uso: spsc_harness KIND M capacita cpu_produttore cpu_consumatore ripetizioni")
    var kind = String(a[1])
    var m = Int(a[2]); var capacity = Int(a[3])
    var cpu_p = Int(a[4]); var cpu_c = Int(a[5]); var reps = Int(a[6])
    _ = run_kind(kind, min(m, 1000000), capacity, cpu_p, cpu_c)  # warm-up
    for _ in range(reps):
        print("RESULT", kind, run_kind(kind, m, capacity, cpu_p, cpu_c))
