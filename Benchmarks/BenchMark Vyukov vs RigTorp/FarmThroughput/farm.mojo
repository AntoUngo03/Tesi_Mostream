# Benchmark 2: una sorgente sequenziale e N repliche dello stadio consumatore.
# Il runner antepone le due implementazioni di coda già usate nel benchmark 1.
from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.time import perf_counter_ns
from std.ffi import OwnedDLHandle, c_int
from std.sys import argv
from std.testing import assert_equal

# Ogni input richiede questo intero lavoro T, indipendentemente da N.
@no_inline
def farm_work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# Stato della barriera: nessun aggiornamento atomico aggiuntivo per singolo input.
struct FarmSignals:
    var ready: Atomic[UInt64]
    var start: Atomic[UInt64]
    var pin_errors: Atomic[UInt64]

    def __init__(out self):
        self.ready = Atomic[UInt64](0)
        self.start = Atomic[UInt64](0)
        self.pin_errors = Atomic[UInt64](0)

# Ogni consumatore scrive il proprio risultato soltanto alla fine.
@fieldwise_init
struct FarmWorkerResult(Copyable):
    var count: Int
    var digest: UInt64

# Risultato della prova restituito al main dopo tutti i controlli.
@fieldwise_init
struct FarmMeasurement(Movable):
    var elapsed_ns: Int
    var source_elapsed_ns: Int
    var counts: List[Int]

# Fissa il task al suo core prima della barriera e dell'avvio del cronometro.
def farm_wait(signals: Pointer[FarmSignals, MutUntrackedOrigin], cpu: Int, library_path: String):
    if cpu >= 0:
        try:
            var library = OwnedDLHandle(library_path)
            if library.call["pin_thread_to_cpu", c_int](c_int(cpu)) != 0:
                _ = signals[].pin_errors.fetch_add(1)
        except:
            _ = signals[].pin_errors.fetch_add(1)
    _ = signals[].ready.fetch_add[ordering=Ordering.RELEASE](1)
    while (signals[].start.load[ordering=Ordering.ACQUIRE]() == 0):
        pass

# Il produttore non esegue il kernel: genera ID e li inserisce nella coda condivisa.
async def farm_source(queue: Pointer[GrainQueue, MutUntrackedOrigin], messages: Int,
                      consumers: Int, signals: Pointer[FarmSignals, MutUntrackedOrigin],
                      source_elapsed: Pointer[Int, MutUntrackedOrigin], cpu: Int, library_path: String):
    farm_wait(signals, cpu, library_path)
    var begin = perf_counter_ns()
    for i in range(messages):
        queue[].push(UInt64(i + 1))
    # Include l'attesa per coda piena: NON è il limite intrinseco della sola generazione.
    source_elapsed[] = Int(perf_counter_ns() - begin)
    # Zero è riservato alla terminazione: un messaggio per ciascun consumatore.
    for _ in range(consumers):
        queue[].push(UInt64(0))

# Ogni input viene preso da un solo consumatore, che esegue tutte le iterazioni T.
async def farm_consumer(index: Int, queue: Pointer[GrainQueue, MutUntrackedOrigin],
                        messages: Int, iterations: Int,
                        signals: Pointer[FarmSignals, MutUntrackedOrigin],
                        results: Pointer[FarmWorkerResult, MutUntrackedOrigin],
                        records: Pointer[UInt64, MutUntrackedOrigin], cpu: Int, library_path: String):
    farm_wait(signals, cpu, library_path)
    var count = 0
    var digest = UInt64(0)
    while True:
        var value = queue[].pop()
        if value == 0:
            break
        # Un log privato per worker permette la verifica esatta fuori dalla misura.
        # Niente CAS di verifica per ogni messaggio: soltanto una scrittura dell'ID.
        if count < messages:
            records.unsafe_offset(index * messages + count)[] = value
        digest += farm_work(value, iterations)
        count += 1
    results.unsafe_offset(index)[] = FarmWorkerResult(count, digest)

# Crea una sola coda per tutti i worker; la sorgente è un task aggiuntivo.
def farm_run(consumers: Int, messages: Int, iterations: Int, capacity: Int,
             cpus: List[Int], library_path: String) raises -> FarmMeasurement:
    var queue = unsafe_alloc[GrainQueue](1)
    queue.unsafe_write(GrainQueue(capacity))
    var signals = unsafe_alloc[FarmSignals](1)
    signals.unsafe_write(FarmSignals())
    var results = unsafe_alloc[FarmWorkerResult](consumers)
    for i in range(consumers):
        results.unsafe_offset(i).unsafe_write(FarmWorkerResult(0, 0))
    # Ogni worker può ricevere fino a messages input, senza quote prefissate.
    var records = unsafe_alloc[UInt64](consumers * messages)
    # Tocca le pagine prima del cronometro, evitando i primi page fault nel log misurato.
    for i in range(consumers * messages):
        records.unsafe_offset(i).unsafe_write(UInt64(0))
    var source_elapsed = unsafe_alloc[Int](1)
    source_elapsed.unsafe_write(0)
    var source_cpu = -1
    if len(cpus) > 0:
        source_cpu = cpus[0]
    var tasks = TaskGroup()
    tasks.create_task(farm_source(queue, messages, consumers, signals, source_elapsed, source_cpu, library_path))
    for i in range(consumers):
        var cpu = -1
        if len(cpus) > 0:
            cpu = cpus[i + 1]
        tasks.create_task(farm_consumer(i, queue, messages, iterations, signals, results, records, cpu, library_path))
    while (signals[].ready.load[ordering=Ordering.ACQUIRE]() != UInt64(consumers + 1)):
        pass
    var begin = perf_counter_ns()
    signals[].start.store[ordering=Ordering.RELEASE](1)
    tasks.wait()
    var elapsed = Int(perf_counter_ns() - begin)
    # Fine misura. Controlla ogni ID, senza affidarsi soltanto a un checksum aggregato.
    var seen = unsafe_alloc[UInt8](messages)
    for i in range(messages):
        seen.unsafe_offset(i).unsafe_write(UInt8(0))
    var errors = Int(signals[].pin_errors.load())
    var counts = List[Int]()
    var total = 0
    for worker in range(consumers):
        var count = results.unsafe_offset(worker)[].count
        counts.append(count)
        total += count
        if count > messages:
            errors += 1
        var expected = UInt64(0)
        for j in range(min(count, messages)):
            var value = records.unsafe_offset(worker * messages + j)[]
            if value < 1 or value > UInt64(messages):
                errors += 1
            else:
                var position = seen.unsafe_offset(Int(value) - 1)
                if position[] != 0:
                    errors += 1
                position[] = 1
                expected += farm_work(value, iterations)
        if expected != results.unsafe_offset(worker)[].digest:
            errors += 1
    for i in range(messages):
        if seen.unsafe_offset(i)[] != 1:
            errors += 1
    var source_ns = source_elapsed[]
    seen.unsafe_free()
    records.unsafe_free()
    results.unsafe_free()
    source_elapsed.unsafe_free()
    signals.unsafe_deinit_pointee()
    signals.unsafe_free()
    queue.unsafe_deinit_pointee()
    queue.unsafe_free()
    assert_equal(total, messages)
    assert_equal(errors, 0)
    return FarmMeasurement(elapsed, source_ns, counts^)

# Calibrazione lunga del kernel per tradurre il T richiesto in iterazioni.
def farm_calibrate():
    var value = UInt64(1)
    var begin = perf_counter_ns()
    for _ in range(20000):
        value = farm_work(value, 1000)
    print("CALIBRATION", Float64(perf_counter_ns() - begin) / 20000000.0, value)

# Baseline senza comunicazione: stima il costo effettivo del kernel e del digest.
def farm_compute_only(messages: Int, iterations: Int):
    var checksum = UInt64(0)
    var begin = perf_counter_ns()
    for i in range(messages):
        checksum += farm_work(UInt64(i + 1), iterations)
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
    if len(args) != 8:
        raise Error("Uso: binary consumers messages iterations capacity repeats cpus library")
    var consumers = Int(args[1])
    var messages = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    var repeats = Int(args[5])
    if consumers < 1 or consumers + 1 > parallelism_level() or messages < 1 or iterations < 0 or capacity < 1 or repeats < 1:
        raise Error("Parametri non validi: servono almeno N+1 worker del runtime")
    var cpus = List[Int]()
    if args[6] != "none":
        var parts = args[6].split(",")
        for part in parts:
            cpus.append(Int(part))
        if len(cpus) < consumers + 1:
            raise Error("Servono almeno N+1 CPU per sorgente e consumatori")
    _ = farm_run(consumers, min(messages, 10000), iterations, capacity, cpus, args[7])
    for repeat in range(repeats):
        var measured = farm_run(consumers, messages, iterations, capacity, cpus, args[7])
        print("RESULT", repeat, measured.elapsed_ns, measured.source_elapsed_ns)
        for worker in range(consumers):
            print("COUNT", worker, measured.counts[worker])
