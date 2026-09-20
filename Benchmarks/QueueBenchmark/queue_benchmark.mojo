# Compare MoStream's bounded CAS, FAA, Hybrid, Rigtorp, Padded-FAA,
# LPRQ-inspired, and wCQ-adapter MPMC queues.
#
# Each producer inserts a disjoint range of non-negative integers.  The last
# producer to finish inserts one -1 sentinel per consumer.  Consumers count
# and sum all values, allowing the benchmark to detect lost or duplicated
# messages after every run.

from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering
from std.runtime.asyncrt import TaskGroup
from std.sys import argv
from std.time import perf_counter_ns

from MoStream.MPMC_queue import MPMCQueue as CASQueue
from MoStream.FAA_Queue import MPMCQueue as FAAQueue
from MoStream.Hybrid_queue import HybridMPMCQueue
from MoStream.Rigtorp_queue import RigtorpMPMCQueue
from MoStream.Padded_FAA_queue import PaddedFAAQueue
from MoStream.Bounded_LPRQ import BoundedLPRQInspired
from MoStream.WCQ_queue import WCQQueue


def expected_checksum(total: UInt64) -> UInt64:
    return (total * (total - 1)) // 2


def print_result(
    name: String,
    elapsed_ns: Int,
    expected_count: UInt64,
    actual_count: UInt64,
    actual_checksum: UInt64,
):
    var seconds = Float64(elapsed_ns) / 1_000_000_000.0
    var throughput = Float64(expected_count) / seconds / 1_000_000.0
    var valid = (
        actual_count == expected_count
        and actual_checksum == expected_checksum(expected_count)
    )
    print(
        name,
        "time_ms=",
        Float64(elapsed_ns) / 1_000_000.0,
        "throughput_Mmsg_s=",
        throughput,
        "valid=",
        valid,
    )
    if not valid:
        print(
            "  expected count/checksum:",
            expected_count,
            expected_checksum(expected_count),
            "actual:",
            actual_count,
            actual_checksum,
        )


def run_cas(
    messages: Int, producers: Int, consumers: Int, capacity: Int
) raises:
    var queue = CASQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.

    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result(
        "CAS ",
        elapsed,
        total,
        actual_count,
        actual_checksum,
    )
    counts.unsafe_free()
    checksums.unsafe_free()


def run_faa(messages: Int, producers: Int, consumers: Int, capacity: Int):
    var queue = FAAQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.

    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result(
        "FAA ",
        elapsed,
        total,
        actual_count,
        actual_checksum,
    )
    counts.unsafe_free()
    checksums.unsafe_free()


def run_hybrid[
    threshold: Int
](messages: Int, producers: Int, consumers: Int, capacity: Int):
    var queue = HybridMPMCQueue[Int, threshold](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.

    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result(
        "HYBRID" + String(threshold) + " ",
        elapsed,
        total,
        actual_count,
        actual_checksum,
    )
    counts.unsafe_free()
    checksums.unsafe_free()


def run_padded_faa(
    messages: Int, producers: Int, consumers: Int, capacity: Int
):
    var queue = PaddedFAAQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result("PADDEDFAA ", elapsed, total, actual_count, actual_checksum)
    counts.unsafe_free()
    checksums.unsafe_free()


def run_rigtorp(messages: Int, producers: Int, consumers: Int, capacity: Int):
    var queue = RigtorpMPMCQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result("RIGTORP ", elapsed, total, actual_count, actual_checksum)
    counts.unsafe_free()
    checksums.unsafe_free()


def run_bounded_lprq(
    messages: Int, producers: Int, consumers: Int, capacity: Int
):
    var queue = BoundedLPRQInspired[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    if actual_count != total or actual_checksum != expected_checksum(total):
        print(
            "  BLPRQ post-run head/tail/full:",
            queue.debug_head_ticket(),
            queue.debug_tail_ticket(),
            queue.debug_full_cells(),
        )
        queue.debug_dump_cells()
    print_result("BLPRQ ", elapsed, total, actual_count, actual_checksum)
    counts.unsafe_free()
    checksums.unsafe_free()


def run_wcq(messages: Int, producers: Int, consumers: Int, capacity: Int):
    # wCQ keeps one private descriptor per concurrent caller. Worker indices
    # are dense and unique for the lifetime of this parallel region, so they
    # can be used directly as thread IDs.
    var queue = WCQQueue(capacity, producers + consumers)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(index, UInt64(base + i))
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(index, UInt64.MAX)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop(index)
                if value == UInt64.MAX:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.
    var elapsed = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]
    print_result("WCQ ", elapsed, total, actual_count, actual_checksum)
    counts.unsafe_free()
    checksums.unsafe_free()


def main() raises:
    var args = argv()
    var messages = 250_000
    var producers = 4
    var consumers = 4
    var capacity = 1024
    var repetitions = 5

    if len(args) == 6:
        messages = Int(args[1])
        producers = Int(args[2])
        consumers = Int(args[3])
        capacity = Int(args[4])
        repetitions = Int(args[5])
    elif len(args) != 1:
        print(
            "Usage: queue_benchmark "
            "[messages_per_producer producers consumers capacity repetitions]"
        )
        return

    if messages <= 0 or producers <= 0 or consumers <= 0 or repetitions <= 0:
        print("All parameters must be positive")
        return
    if capacity < 8 or (capacity & (capacity - 1)) != 0:
        print("Capacity must be a power of two and at least 8 for wCQ")
        return
    if producers + consumers > capacity:
        print("Capacity must be at least producers + consumers for wCQ")
        return

    print("MoStream bounded MPMC queue benchmark")
    print(
        "messages/producer=",
        messages,
        "producers=",
        producers,
        "consumers=",
        consumers,
        "capacity=",
        capacity,
        "repetitions=",
        repetitions,
    )
    print("Each repetition rotates execution order to reduce order bias.")

    for repetition in range(repetitions):
        print("\nrepetition", repetition + 1)
        if repetition % 3 == 0:
            run_cas(messages, producers, consumers, capacity)
            run_faa(messages, producers, consumers, capacity)
            run_hybrid[1](messages, producers, consumers, capacity)
            run_hybrid[2](messages, producers, consumers, capacity)
            run_hybrid[4](messages, producers, consumers, capacity)
            run_hybrid[8](messages, producers, consumers, capacity)
            run_rigtorp(messages, producers, consumers, capacity)
            run_padded_faa(messages, producers, consumers, capacity)
            run_bounded_lprq(messages, producers, consumers, capacity)
            run_wcq(messages, producers, consumers, capacity)
        elif repetition % 3 == 1:
            run_wcq(messages, producers, consumers, capacity)
            run_bounded_lprq(messages, producers, consumers, capacity)
            run_padded_faa(messages, producers, consumers, capacity)
            run_rigtorp(messages, producers, consumers, capacity)
            run_hybrid[8](messages, producers, consumers, capacity)
            run_hybrid[4](messages, producers, consumers, capacity)
            run_hybrid[2](messages, producers, consumers, capacity)
            run_hybrid[1](messages, producers, consumers, capacity)
            run_faa(messages, producers, consumers, capacity)
            run_cas(messages, producers, consumers, capacity)
        else:
            run_hybrid[2](messages, producers, consumers, capacity)
            run_cas(messages, producers, consumers, capacity)
            run_hybrid[8](messages, producers, consumers, capacity)
            run_faa(messages, producers, consumers, capacity)
            run_wcq(messages, producers, consumers, capacity)
            run_hybrid[1](messages, producers, consumers, capacity)
            run_hybrid[4](messages, producers, consumers, capacity)
            run_rigtorp(messages, producers, consumers, capacity)
            run_padded_faa(messages, producers, consumers, capacity)
            run_bounded_lprq(messages, producers, consumers, capacity)
