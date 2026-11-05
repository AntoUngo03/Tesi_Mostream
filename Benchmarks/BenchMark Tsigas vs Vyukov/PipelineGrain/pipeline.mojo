# Harness sperimentale per Mojo 1.1; il runner antepone il backend scelto.
from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.time import perf_counter_ns
from std.ffi import OwnedDLHandle, c_int
from std.sys import argv
from std.testing import assert_equal

# Un kernel dipendente dal valore ricevuto: il risultato viene verificato al sink.
# Non usa sleep o letture dell'orologio per ogni messaggio.
@no_inline
def grain_work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
        # Stesso kernel a 63 bit per entrambi: mai payload riservati a Tsigas.
        value &= UInt64(9223372036854775807)
    return value

# Ogni task è uno stadio sequenziale e lavora su un messaggio alla volta.
async def grain_stage(
    index: Int, stages: Int, messages: Int, total_iterations: Int,
    queues: Pointer[GrainQueue, MutUntrackedOrigin],
    ready: Pointer[Atomic[UInt64], MutUntrackedOrigin],
    start: Pointer[Atomic[UInt64], MutUntrackedOrigin],
    digest: Pointer[UInt64, MutUntrackedOrigin],
    pin_errors: Pointer[Atomic[UInt64], MutUntrackedOrigin],
    cpu: Int, pin_library: String,
):
    # Il pinning precede la barriera e la misura temporale.
    if cpu >= 0:
        try:
            var library = OwnedDLHandle(pin_library)
            var result = library.call["pin_thread_to_cpu", c_int](c_int(cpu))
            if result != 0:
                _ = pin_errors[].fetch_add(1)
        except:
            _ = pin_errors[].fetch_add(1)
    _ = ready[].fetch_add[ordering=Ordering.RELEASE](1)
    while (start[].load[ordering=Ordering.ACQUIRE]() == 0):
        pass
    # Distribuisce esattamente lo stesso lavoro totale: i resti valgono una iterazione.
    var iterations = total_iterations // stages
    if index < total_iterations % stages:
        iterations += 1
    var checksum = UInt64(0)
    for i in range(messages):
        var value: UInt64
        if index == 0:
            value = UInt64(i + 1)
        else:
            value = queues.unsafe_offset(index - 1)[].pop()
        value = grain_work(value, iterations)
        if index + 1 < stages:
            try:
                queues.unsafe_offset(index)[].push(value)
            except:
                abort("Payload non rappresentabile dal backend Tsigas")
        else:
            # Digest sensibile all'ordine: rileva anche riordinamenti della sequenza.
            checksum = (checksum * UInt64(1099511628211)) ^ value
    if index + 1 == stages:
        digest[] = checksum

# Una esecuzione crea N-1 code; N include primo produttore e ultimo consumatore.
def grain_run(stages: Int, messages: Int, iterations: Int, capacity: Int,
              cpus: List[Int], pin_library: String) raises -> Int:
    var queues = unsafe_alloc[GrainQueue](max(stages - 1, 1))
    for i in range(stages - 1):
        queues.unsafe_offset(i).unsafe_write(GrainQueue(capacity))
    var ready = unsafe_alloc[Atomic[UInt64]](1)
    ready.unsafe_write(Atomic[UInt64](0))
    var start = unsafe_alloc[Atomic[UInt64]](1)
    start.unsafe_write(Atomic[UInt64](0))
    var digest = unsafe_alloc[UInt64](1)
    digest.unsafe_write(UInt64(0))
    var pin_errors = unsafe_alloc[Atomic[UInt64]](1)
    pin_errors.unsafe_write(Atomic[UInt64](0))
    var tasks = TaskGroup()
    for index in range(stages):
        var cpu = -1
        if len(cpus) > 0:
            cpu = cpus[index]
        tasks.create_task(grain_stage(index, stages, messages, iterations, queues,
                          ready, start, digest, pin_errors, cpu, pin_library))
    while (ready[].load[ordering=Ordering.ACQUIRE]() != UInt64(stages)):
        pass
    var begin = perf_counter_ns()
    start[].store[ordering=Ordering.RELEASE](1)
    tasks.wait()
    var elapsed = Int(perf_counter_ns() - begin)
    # L'oracolo è fuori dalla misura: N non deve cambiare né lavoro né risultato.
    var expected = UInt64(0)
    for i in range(messages):
        expected = (expected * UInt64(1099511628211)) ^ grain_work(UInt64(i + 1), iterations)
    var actual = digest[]
    var errors = pin_errors[].load()
    for i in range(stages - 1):
        queues.unsafe_offset(i).unsafe_deinit_pointee()
    queues.unsafe_free()
    ready.unsafe_deinit_pointee()
    ready.unsafe_free()
    start.unsafe_deinit_pointee()
    start.unsafe_free()
    digest.unsafe_free()
    pin_errors.unsafe_deinit_pointee()
    pin_errors.unsafe_free()
    assert_equal(actual, expected)
    assert_equal(errors, UInt64(0))
    return elapsed

# Misura un kernel sequenziale sufficientemente lungo per stimare ns/iterazione.
def grain_calibrate() -> Float64:
    var value = UInt64(1)
    var begin = perf_counter_ns()
    for _ in range(20000):
        value = grain_work(value, 1000)
    var elapsed = perf_counter_ns() - begin
    # Il valore stampato rende osservabile il calcolo e impedisce di eliminarlo.
    print("CALIBRATION", Float64(elapsed) / 20000000.0, value)
    return Float64(elapsed) / 20000000.0

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    if len(args) == 2 and args[1] == "calibrate":
        _ = grain_calibrate()
        print("WORKERS", parallelism_level())
        return
    if len(args) != 8:
        raise Error("Uso: binary N messages iterations capacity repeats cpus library")
    var stages = Int(args[1])
    var messages = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    var repeats = Int(args[5])
    if stages < 1 or stages > parallelism_level() or messages < 1 or iterations < 0 or capacity < 1 or repeats < 1:
        raise Error("Parametri non validi o N maggiore del numero di worker")
    if capacity <= 2 * messages:
        raise Error("Anello insufficiente per il vincolo anti-ABA della prova")
    var cpus = List[Int]()
    if args[6] != "none":
        var parts = args[6].split(",")
        for part in parts:
            cpus.append(Int(part))
        if len(cpus) < stages:
            raise Error("Servono almeno N CPU per il pinning")
    # Warm-up completo, non incluso nelle righe RESULT.
    _ = grain_run(stages, min(messages, 10000), iterations, capacity, cpus, args[7])
    for repeat in range(repeats):
        var elapsed = grain_run(stages, messages, iterations, capacity, cpus, args[7])
        print("RESULT", repeat, elapsed)
