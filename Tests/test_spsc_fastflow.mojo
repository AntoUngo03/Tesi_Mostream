# Test della coda SPSC FastFlow (MoStream/SPSC_queue_fastflow.mojo).
#
#   test seq                              test sequenziali (un solo thread)
#   test conc M capacita blocking|try     un produttore e un consumatore concorrenti
#
# Nel test concorrente il consumatore verifica che i valori 0..M-1 arrivino
# tutti ed esattamente in ordine: una coda SPSC deve essere FIFO, quindi il
# controllo è più severo del solo "ogni valore una volta".
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.sys import argv
from std.testing import assert_equal, assert_true
from MoStream.SPSC_queue_fastflow import FastFlowSPSCQueue

# Test sequenziali: vuota, piena, FIFO, giri del buffer, capacità 1, tipo non banale.
def check_sequential() raises:
    for capacity in [1, 2, 3, 7, 64]:
        var q = FastFlowSPSCQueue[Int](capacity)
        assert_equal(q.capacity(), capacity)
        # Coda nuova: vuota.
        assert_true(not q.try_pop())
        # Si riempie esattamente alla capacità: tutte le celle sono utilizzabili.
        for i in range(capacity):
            assert_true(not q.try_push(i))
        var rejected = q.try_push(999)
        assert_true(rejected.__bool__())
        assert_equal(rejected.take(), 999)
        # Svuotamento in ordine FIFO.
        for i in range(capacity):
            var item = q.try_pop()
            assert_true(item.__bool__())
            assert_equal(item.take(), i)
        assert_true(not q.try_pop())
        # Molti giri del buffer circolare, alternando inserimenti ed estrazioni.
        var next_in = 0
        var next_out = 0
        for round in range(50):
            var burst = 1 + (round * 7) % capacity
            for _ in range(burst):
                q.push(next_in)
                next_in += 1
            for _ in range(burst):
                assert_equal(q.pop(), next_out)
                next_out += 1
        assert_true(not q.try_pop())
    # Tipo non banale: String (verifica che i dati vengano spostati correttamente).
    var s = FastFlowSPSCQueue[String](4)
    for round in range(10):
        for k in range(4):
            s.push(String("msg-") + String(round) + "-" + String(k))
        for k in range(4):
            assert_equal(s.pop(), String("msg-") + String(round) + "-" + String(k))
    # Distruzione con elementi ancora in coda: deve liberare senza errori.
    var leftover = FastFlowSPSCQueue[String](8)
    for k in range(5):
        leftover.push(String("resto-") + String(k))
    _ = leftover^
    print("PASS sequenziale")

comptime QueuePointer = Pointer[FastFlowSPSCQueue[Int], MutUntrackedOrigin]
comptime CounterPointer = Pointer[Atomic[UInt64], MutUntrackedOrigin]

async def producer(queue: QueuePointer, messages: Int, nonblocking: Bool):
    for value in range(messages):
        if nonblocking:
            var pending = Optional(value)
            while pending:
                pending = queue[].try_push(pending.take())
        else:
            queue[].push(value)

# Il consumatore conta gli elementi fuori ordine e quelli ricevuti.
async def consumer(queue: QueuePointer, errors: CounterPointer, received: CounterPointer,
                   messages: Int, nonblocking: Bool):
    for expected in range(messages):
        var value: Int
        if nonblocking:
            var item = queue[].try_pop()
            while not item:
                item = queue[].try_pop()
            value = item.take()
        else:
            value = queue[].pop()
        if value != expected:
            _ = errors[].fetch_add(1)
        _ = received[].fetch_add(1)

def check_concurrent(messages: Int, capacity: Int, nonblocking: Bool) raises:
    if parallelism_level() < 2:
        raise Error("Servono almeno 2 worker Mojo per il test concorrente")
    var queue = unsafe_alloc[FastFlowSPSCQueue[Int]](1)
    queue.unsafe_write(FastFlowSPSCQueue[Int](capacity))
    var errors = unsafe_alloc[Atomic[UInt64]](1)
    errors.unsafe_write(Atomic[UInt64](0))
    var received = unsafe_alloc[Atomic[UInt64]](1)
    received.unsafe_write(Atomic[UInt64](0))
    var tasks = TaskGroup()
    tasks.create_task(producer(queue, messages, nonblocking))
    tasks.create_task(consumer(queue, errors, received, messages, nonblocking))
    tasks.wait()
    var bad = errors[].load()
    var got = received[].load()
    var empty_at_end = not queue[].try_pop()
    errors.unsafe_deinit_pointee()
    errors.unsafe_free()
    received.unsafe_deinit_pointee()
    received.unsafe_free()
    queue.unsafe_deinit_pointee()
    queue.unsafe_free()
    assert_equal(bad, UInt64(0))
    assert_equal(got, UInt64(messages))
    assert_true(empty_at_end)
    print("PASS concorrente input", messages, "capacità", capacity, "try_*", nonblocking)

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "seq":
        check_sequential()
        return
    if len(args) == 5 and args[1] == "conc":
        var messages = Int(args[2])
        var capacity = Int(args[3])
        if messages < 1 or capacity < 1 or (args[4] != "blocking" and args[4] != "try"):
            raise Error("Uso: test conc M capacita blocking|try")
        check_concurrent(messages, capacity, args[4] == "try")
        return
    raise Error("Uso: test seq | test conc M capacita blocking|try")
