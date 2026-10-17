# I task sono soltanto un mezzo per esercitare la coda, non parte del porting.
from std.runtime._asyncrt import TaskGroup
from std.testing import assert_equal, assert_true
from std.sys import argv
from std.runtime import parallelism_level

comptime TZQueuePointer = Pointer[TsigasZhangMPMCQueue, MutUntrackedOrigin]
comptime TZCounterPointer = Pointer[Atomic[UInt64], MutUntrackedOrigin]

# Tutti i task partono dalla stessa barriera dopo l'inizializzazione.
async def tz_producer(queue: TZQueuePointer, seen_ready: TZCounterPointer, start: TZCounterPointer, invalid: TZCounterPointer, first: Int, count: Int, use_try: Bool):
    _ = seen_ready[].fetch_add(1)
    while start[].load() == 0:
        pass
    try:
        for i in range(count):
            var value = UInt64(first + i)
            if use_try:
                while queue[].try_push(value):
                    pass
            else:
                queue[].push(value)
    except:
        # Un errore non atteso impedisce il successo: contatore e timeout del runner.
        _ = invalid[].fetch_add(1)


async def tz_consumer(queue: TZQueuePointer, ready: TZCounterPointer, start: TZCounterPointer, seen: TZCounterPointer, invalid: TZCounterPointer, count: Int, total: Int, use_try: Bool):
    _ = ready[].fetch_add(1)
    while start[].load() == 0:
        pass
    for _ in range(count):
        var value: UInt64
        if use_try:
            var item = queue[].try_pop()
            while not item:
                item = queue[].try_pop()
            value = item.take()
        else:
            value = queue[].pop()
        # Controlla l'intervallo PRIMA di usare il valore come indice di memoria.
        if value >= UInt64(total):
            _ = invalid[].fetch_add(1)
        else:
            _ = seen.unsafe_offset(Int(value))[].fetch_add(1)

# Il totale dei successi resta inferiore a L: niente giri completi concorrenti.
# L'attraversamento di zero è preparato prima dell'avvio, senza concorrenza.
def check_tz_concurrent(length: Int, count: Int, use_try: Bool, cross_zero: Bool) raises:
    var queue = unsafe_alloc[TsigasZhangMPMCQueue](1)
    queue.unsafe_write(TsigasZhangMPMCQueue(length))
    if cross_zero:
        for _ in range(length - 8):
            queue[].push(777)
            assert_equal(queue[].pop(), UInt64(777))
    # Gli atomici condivisi hanno origine non tracciata e vivono fino ai join.
    var control = unsafe_alloc[Atomic[UInt64]](3)
    for i in range(3):
        control.unsafe_offset(i).unsafe_write(Atomic[UInt64](0))
    var ready_ptr = control
    var start_ptr = control.unsafe_offset(1)
    var invalid_ptr = control.unsafe_offset(2)
    var seen = unsafe_alloc[Atomic[UInt64]](4 * count)
    for i in range(4 * count):
        seen.unsafe_offset(i).unsafe_write(Atomic[UInt64](0))
    var tasks = TaskGroup()
    tasks.create_task(tz_producer(queue, ready_ptr, start_ptr, invalid_ptr, 0, count, use_try))
    tasks.create_task(tz_producer(queue, ready_ptr, start_ptr, invalid_ptr, count, count, use_try))
    tasks.create_task(tz_producer(queue, ready_ptr, start_ptr, invalid_ptr, 2 * count, count, use_try))
    tasks.create_task(tz_producer(queue, ready_ptr, start_ptr, invalid_ptr, 3 * count, count, use_try))
    tasks.create_task(tz_consumer(queue, ready_ptr, start_ptr, seen, invalid_ptr, count, 4 * count, use_try))
    tasks.create_task(tz_consumer(queue, ready_ptr, start_ptr, seen, invalid_ptr, count, 4 * count, use_try))
    tasks.create_task(tz_consumer(queue, ready_ptr, start_ptr, seen, invalid_ptr, count, 4 * count, use_try))
    tasks.create_task(tz_consumer(queue, ready_ptr, start_ptr, seen, invalid_ptr, count, 4 * count, use_try))
    while ready_ptr[].load() < 8:
        pass
    start_ptr[].store(1)
    tasks.wait()
    assert_equal(invalid_ptr[].load(), UInt64(0))
    for i in range(4 * count):
        assert_equal(seen.unsafe_offset(i)[].load(), UInt64(1))
        seen.unsafe_offset(i).unsafe_deinit_pointee()
    seen.unsafe_free()
    for i in range(3):
        control.unsafe_offset(i).unsafe_deinit_pointee()
    control.unsafe_free()
    assert_true(queue[].empty())
    queue.unsafe_deinit_pointee()
    queue.unsafe_free()
    print("Tsigas-Zhang: 4P/4C, valori", 4 * count, "ring", length, "try", use_try, "passaggio zero", cross_zero)


# Regressione SPSC: la scansione non deve saltare una pubblicazione concorrente.
async def tz_fifo_consumer(queue: TZQueuePointer, ready: TZCounterPointer, start: TZCounterPointer, invalid: TZCounterPointer, count: Int):
    _ = ready[].fetch_add(1)
    while start[].load() == 0:
        pass
    for i in range(count):
        var value = queue[].pop()
        if value != UInt64(i):
            _ = invalid[].fetch_add(1)


def check_tz_fifo() raises:
    var queue = unsafe_alloc[TsigasZhangMPMCQueue](1)
    queue.unsafe_write(TsigasZhangMPMCQueue(8192))
    var signals = unsafe_alloc[Atomic[UInt64]](3)
    for i in range(3):
        signals.unsafe_offset(i).unsafe_write(Atomic[UInt64](0))
    var tasks = TaskGroup()
    tasks.create_task(tz_producer(queue, signals, signals.unsafe_offset(1), signals.unsafe_offset(2), 0, 2000, False))
    tasks.create_task(tz_fifo_consumer(queue, signals, signals.unsafe_offset(1), signals.unsafe_offset(2), 2000))
    while signals[].load() != 2:
        pass
    signals.unsafe_offset(1)[].store(1)
    tasks.wait()
    assert_equal(signals.unsafe_offset(2)[].load(), UInt64(0))
    assert_true(queue[].empty())
    queue.unsafe_deinit_pointee()
    queue.unsafe_free()
    for i in range(3):
        signals.unsafe_offset(i).unsafe_deinit_pointee()
    signals.unsafe_free()
    print("Tsigas-Zhang: SPSC, ordine esatto di 2000 valori verificato")


# Un processo per scenario evita di riutilizzare il pool privato dopo busy-wait.
def main() raises:
    var args = argv()
    if len(args) != 2:
        raise Error("Usare il runner Python: ogni scenario richiede un processo separato")
    if parallelism_level() < 8:
        raise Error("Il test con barriera richiede almeno otto worker runtime")
    var scenario = Int(args[1])
    if scenario == 0:
        check_tz_concurrent(64, 4, False, False)
    elif scenario == 1:
        check_tz_concurrent(64, 4, True, True)
    elif scenario == 2:
        check_tz_concurrent(16384, 1000, False, False)
    elif scenario == 3:
        check_tz_concurrent(16384, 1000, True, False)
    elif scenario == 4:
        check_tz_concurrent(16384, 1000, False, True)
    elif scenario == 5:
        check_tz_concurrent(16384, 1000, True, True)
    elif scenario == 6:
        check_tz_fifo()
    else:
        raise Error("Scenario non valido: scegliere 0..6")
