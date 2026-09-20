from std.memory.alloc import unsafe_alloc
from std.runtime.asyncrt import TaskGroup
from std.atomic import Atomic, Ordering
from MoStream.Hybrid_queue import HybridMPMCQueue


comptime PRODUCERS = 4
comptime CONSUMERS = 4
comptime MESSAGES_PER_PRODUCER = 250_000


def main():
    var queue = HybridMPMCQueue[Int](1024)
    var finished_producers = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](CONSUMERS)
    var checksums = unsafe_alloc[UInt64](CONSUMERS)
    for i in range(CONSUMERS):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0

    @parameter
    async def worker(index: Int):
        if index < PRODUCERS:
            var base = index * MESSAGES_PER_PRODUCER
            for i in range(MESSAGES_PER_PRODUCER):
                queue.push(base + i)
            var previous = finished_producers.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(PRODUCERS):
                for _ in range(CONSUMERS):
                    queue.push(-1)
        else:
            var consumer_id = index - PRODUCERS
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var item = queue.pop()
                if item == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(item)

    var tasks = TaskGroup()
    for worker_id in range(PRODUCERS + CONSUMERS):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Legacy task captures must not outlive the queue allocation.

    var count: UInt64 = 0
    var checksum: UInt64 = 0
    for i in range(CONSUMERS):
        count += counts[unsafe_offset=i]
        checksum += checksums[unsafe_offset=i]
    counts.unsafe_free()
    checksums.unsafe_free()

    var expected_count = UInt64(PRODUCERS * MESSAGES_PER_PRODUCER)
    var expected_checksum = (expected_count * (expected_count - 1)) // 2
    if count != expected_count or checksum != expected_checksum:
        print(
            "FAIL: expected count/checksum", expected_count, expected_checksum,
            "actual", count, checksum,
        )
        return

    print("PASS: HybridMPMCQueue concurrent correctness")
    print("enqueue FAA fallbacks:", queue.enqueue_fallback_count())
    print("dequeue FAA fallbacks:", queue.dequeue_fallback_count())
