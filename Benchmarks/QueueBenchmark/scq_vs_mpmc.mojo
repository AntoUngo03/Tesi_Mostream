from std.memory.alloc import unsafe_alloc
from std.runtime.asyncrt import TaskGroup
from std.atomic import Atomic, Ordering
from std.sys import argv
from std.time import perf_counter_ns

from MoStream.MPMC_queue import MPMCQueue
from MoStream.SCQ_queue import SCQQueue

# Microbenchmark della sola coda: non crea Pipeline, Communicator, actor o
# scheduler. Serve a distinguere la scalabilita intrinseca della queue dal costo
# dei runtime MoStream. Ogni backend riceve esattamente lo stesso workload.


def expected_checksum(messages: Int, producers: Int) -> UInt64:
    var total = UInt64(messages * producers)
    return (total * (total - 1)) // 2


def print_result(
    name: String,
    elapsed_ns: Int,
    messages: Int,
    producers: Int,
    consumers: Int,
    capacity: Int,
    actual_count: UInt64,
    checksum: UInt64,
):
    var total = UInt64(messages * producers)
    var valid = (
        actual_count == total
        and checksum == expected_checksum(messages, producers)
    )
    print(
        "QUEUE_RESULT backend=", name,
        " producers=", producers,
        " consumers=", consumers,
        " capacity=", capacity,
        " messages=", total,
        " time_ms=", Float64(elapsed_ns) / 1_000_000.0,
        " throughput_Mmsg_s=", Float64(total) / Float64(elapsed_ns) * 1000.0,
        " valid=", valid,
    )


def run_mpmc(
    messages: Int, producers: Int, consumers: Int, capacity: Int
) raises:
    var queue = MPMCQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(thread_id: Int):
        # parallelize assegna un ID distinto a ogni producer/consumer. I range
        # prodotti sono disgiunti, rendendo noto checksum e numero di messaggi.
        if thread_id < producers:
            var base = thread_id * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                # Solo l'ultimo producer inserisce un sentinel per consumer,
                # dopo che tutti i messaggi applicativi sono stati pubblicati.
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer = thread_id - producers
            var count: UInt64 = 0
            var checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer] = count
                    checksums[unsafe_offset=consumer] = checksum
                    return
                count += 1
                checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var count: UInt64 = 0
    var checksum: UInt64 = 0
    for i in range(consumers):
        count += counts[unsafe_offset=i]
        checksum += checksums[unsafe_offset=i]
    print_result(
        "MPMC", elapsed, messages, producers, consumers, capacity,
        count, checksum,
    )
    counts.unsafe_free()
    checksums.unsafe_free()


def run_scq(messages: Int, producers: Int, consumers: Int, capacity: Int):
    var queue = SCQQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(thread_id: Int):
        if thread_id < producers:
            var base = thread_id * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer = thread_id - producers
            var count: UInt64 = 0
            var checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer] = count
                    checksums[unsafe_offset=consumer] = checksum
                    return
                count += 1
                checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var count: UInt64 = 0
    var checksum: UInt64 = 0
    for i in range(consumers):
        count += counts[unsafe_offset=i]
        checksum += checksums[unsafe_offset=i]
    print_result(
        "SCQ", elapsed, messages, producers, consumers, capacity,
        count, checksum,
    )
    counts.unsafe_free()
    checksums.unsafe_free()


def main() raises:
    var args = argv()
    if len(args) != 5:
        print("Usage: scq_vs_mpmc <messages/producer> <P> <C> <capacity>")
        raise Error("invalid arguments")
    var messages = Int(args[1])
    var producers = Int(args[2])
    var consumers = Int(args[3])
    var capacity = Int(args[4])
    if messages < 1 or producers < 1 or consumers < 1:
        raise Error("arguments must be positive")
    run_mpmc(messages, producers, consumers, capacity)
    run_scq(messages, producers, consumers, capacity)
