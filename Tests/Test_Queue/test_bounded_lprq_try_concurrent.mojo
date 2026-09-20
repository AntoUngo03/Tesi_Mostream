from std.memory.alloc import unsafe_alloc
from std.runtime.asyncrt import TaskGroup
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.terminate import exit
from MoStream.Bounded_LPRQ import BoundedLPRQInspired


comptime PRODUCERS = 4
comptime CONSUMERS = 4
comptime MESSAGES_PER_PRODUCER = 100_000
comptime TOTAL_MESSAGES = PRODUCERS * MESSAGES_PER_PRODUCER


def run_case(capacity: Int) -> Bool:
    # This is the API pattern used by MoStream's cooperative scheduling path:
    # failed try_push values are retried and empty try_pop calls are polled.
    var queue = BoundedLPRQInspired[Int](capacity)
    var finished_producers = Atomic[DType.uint64](0)
    var invalid_values = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](CONSUMERS)
    var checksums = unsafe_alloc[UInt64](CONSUMERS)
    var seen = unsafe_alloc[UInt64](TOTAL_MESSAGES)
    for i in range(CONSUMERS):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0
    for i in range(TOTAL_MESSAGES):
        seen[unsafe_offset=i] = 0

    @parameter
    async def worker(index: Int):
        if index < PRODUCERS:
            var base = index * MESSAGES_PER_PRODUCER
            for i in range(MESSAGES_PER_PRODUCER):
                var pending = Optional(base + i)
                while pending:
                    var result = queue.try_push(pending.take())
                    if not result:
                        break
                    pending = result^

            var previous = finished_producers.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(PRODUCERS):
                for _ in range(CONSUMERS):
                    var sentinel = Optional(-1)
                    while sentinel:
                        var result = queue.try_push(sentinel.take())
                        if not result:
                            break
                        sentinel = result^
        else:
            var consumer_id = index - PRODUCERS
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var item = queue.try_pop()
                if not item:
                    continue
                var value = item.take()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                if value < 0 or value >= TOTAL_MESSAGES:
                    _ = invalid_values.fetch_add[ordering=Ordering.RELAXED](1)
                else:
                    _ = Atomic[DType.uint64].fetch_add[
                        ordering=Ordering.RELAXED
                    ](seen.unsafe_offset(value), 1)
                local_count += 1
                local_checksum += UInt64(value)

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
    var first_bad_id = -1
    for i in range(TOTAL_MESSAGES):
        if Atomic[DType.uint64].load[ordering=Ordering.RELAXED](seen.unsafe_offset(i)) != 1:
            first_bad_id = i
            break
    var invalid_count = invalid_values.load[ordering=Ordering.RELAXED]()
    counts.unsafe_free()
    checksums.unsafe_free()
    seen.unsafe_free()

    var expected_count = UInt64(PRODUCERS * MESSAGES_PER_PRODUCER)
    var expected_checksum = (expected_count * (expected_count - 1)) // 2
    if count != expected_count or checksum != expected_checksum:
        print(
            "FAIL: capacity/expected count/checksum",
            capacity,
            expected_count,
            expected_checksum,
            "actual",
            count,
            checksum,
        )
        return False

    if invalid_count != 0 or first_bad_id >= 0:
        print(
            "FAIL: exact-once validation, capacity/invalid/first bad id",
            capacity,
            invalid_count,
            first_bad_id,
        )
        return False

    if queue.estimated_len() != 0:
        print(
            "FAIL: queue is not empty after all sentinels, capacity", capacity
        )
        return False

    print("PASS: try API capacity", capacity)
    return True


def main():
    if not run_case(2):
        exit(1)
    if not run_case(4):
        exit(1)
    if not run_case(8):
        exit(1)
    if not run_case(16):
        exit(1)

    print("PASS: BoundedLPRQInspired concurrent try exact-once correctness")
