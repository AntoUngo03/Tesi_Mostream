# Il runner unisce il test alla coda, senza usare il runtime MoStream.
# Entrambe le modalità usano attesa attiva, anche quando riprovano le try_*.
from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.sys import argv
from std.testing import assert_equal

comptime QueuePointer = Pointer[RigtorpMPMCQueue[Int], MutUntrackedOrigin]
comptime CounterPointer = Pointer[Atomic[UInt64], MutUntrackedOrigin]

async def producer(queue: QueuePointer, index: Int, workers: Int,
                   messages: Int, nonblocking: Bool):
    # Il produttore j genera precisamente i valori i con i % N == j.
    for value in range(index, messages, workers):
        if nonblocking:
            var pending = Optional(value)
            while pending:
                pending = queue[].try_push(pending.take())
        else:
            queue[].push(value)

async def consumer(queue: QueuePointer, seen: CounterPointer,
                   errors: CounterPointer, messages: Int, count: Int,
                   nonblocking: Bool):
    for _ in range(count):
        var value: Int
        if nonblocking:
            var item = queue[].try_pop()
            while not item:
                item = queue[].try_pop()
            value = item.take()
        else:
            value = queue[].pop()
        # Un payload corrotto non deve diventare un accesso fuori dal buffer.
        if value < 0 or value >= messages:
            _ = errors[].fetch_add(1)
        else:
            _ = seen.unsafe_offset(value)[].fetch_add(1)


def check_concurrent(workers: Int, messages: Int, capacity: Int,
                     nonblocking: Bool, allow_oversubscription: Bool) raises:
    # I task non cedono il worker: devono poter partire tutti prima delle attese.
    if 2 * workers > parallelism_level() and not allow_oversubscription:
        raise Error("Servono almeno 2*N worker Mojo per questo test con attesa attiva")
    var queue = unsafe_alloc[RigtorpMPMCQueue[Int]](1)
    queue.unsafe_write(RigtorpMPMCQueue[Int](capacity))
    var seen = unsafe_alloc[Atomic[UInt64]](messages)
    for i in range(messages):
        seen.unsafe_offset(i).unsafe_write(Atomic[UInt64](0))
    var errors = unsafe_alloc[Atomic[UInt64]](1)
    errors.unsafe_write(Atomic[UInt64](0))
    var tasks = TaskGroup()
    for j in range(workers):
        tasks.create_task(producer(queue, j, workers, messages, nonblocking))
        # Quote di estrazione, senza imporre quali ID riceve ciascun consumatore.
        var count = messages // workers
        if j < messages % workers:
            count += 1
        tasks.create_task(consumer(queue, seen, errors, messages, count, nonblocking))
    tasks.wait()
    var missing = 0
    var duplicates = UInt64(0)
    for i in range(messages):
        var count = seen.unsafe_offset(i)[].load()
        if count == 0:
            missing += 1
        elif count > 1:
            duplicates += count - 1
        seen.unsafe_offset(i).unsafe_deinit_pointee()
    var invalid = errors[].load()
    var remaining = queue[].size()
    seen.unsafe_free()
    errors.unsafe_deinit_pointee()
    errors.unsafe_free()
    queue.unsafe_deinit_pointee()
    queue.unsafe_free()
    assert_equal(invalid, UInt64(0))
    assert_equal(missing, 0)
    assert_equal(duplicates, UInt64(0))
    assert_equal(remaining, 0)
    print("PASS N", workers, "input", messages, "capacità", capacity, "try_*", nonblocking)


def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print(parallelism_level())
        return
    if len(args) != 5 and len(args) != 6:
        raise Error("Uso: test N messages capacity blocking|try")
    var workers = Int(args[1])
    var messages = Int(args[2])
    var capacity = Int(args[3])
    if workers < 1 or messages < 1 or capacity < 1:
        raise Error("N, messages e capacity devono essere positivi")
    if args[4] != "blocking" and args[4] != "try":
        raise Error("Modalità ammessa: blocking oppure try")
    var allow_oversubscription = False
    if len(args) == 6:
        if args[5] != "allow-oversubscription":
            raise Error("Opzione non riconosciuta")
        allow_oversubscription = True
    check_concurrent(workers, messages, capacity, args[4] == "try", allow_oversubscription)
