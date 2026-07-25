from std.algorithm import parallelize
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.terminate import exit
from MoStream.WCQ_queue import WCQQueue


comptime PRODUCERS = 4
comptime CONSUMERS = 4
comptime MESSAGES_PER_PRODUCER = 100_000
comptime TOTAL_MESSAGES = PRODUCERS * MESSAGES_PER_PRODUCER


def main():
    var queue = WCQQueue(16, PRODUCERS + CONSUMERS)
    var finished_producers = Atomic[DType.uint64](0)
    var invalid_values = Atomic[DType.uint64](0)
    var seen = alloc[UInt64](TOTAL_MESSAGES)
    var counts = alloc[UInt64](CONSUMERS)
    for i in range(TOTAL_MESSAGES):
        seen[i] = 0
    for i in range(CONSUMERS):
        counts[i] = 0

    @parameter
    def worker(thread_id: Int):
        if thread_id < PRODUCERS:
            var base = thread_id * MESSAGES_PER_PRODUCER
            for i in range(MESSAGES_PER_PRODUCER):
                var pending = Optional(UInt64(base + i))
                while pending:
                    var result = queue.try_push(
                        thread_id, pending.take()
                    )
                    if not result:
                        break
                    pending = result^

            var previous = finished_producers.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(PRODUCERS):
                for _ in range(CONSUMERS):
                    var sentinel = Optional(UInt64.MAX)
                    while sentinel:
                        var result = queue.try_push(
                            thread_id, sentinel.take()
                        )
                        if not result:
                            break
                        sentinel = result^
        else:
            var consumer_id = thread_id - PRODUCERS
            var local_count: UInt64 = 0
            while True:
                var item = queue.try_pop(thread_id)
                if not item:
                    continue
                var value = item.take()
                if value == UInt64.MAX:
                    counts[consumer_id] = local_count
                    return
                if value >= UInt64(TOTAL_MESSAGES):
                    _ = invalid_values.fetch_add[
                        ordering=Ordering.RELAXED
                    ](1)
                else:
                    _ = Atomic[DType.uint64].fetch_add[
                        ordering=Ordering.RELAXED
                    ](seen + Int(value), 1)
                local_count += 1

    parallelize[worker](PRODUCERS + CONSUMERS)

    var actual_count: UInt64 = 0
    for i in range(CONSUMERS):
        actual_count += counts[i]
    var first_bad = -1
    for i in range(TOTAL_MESSAGES):
        if Atomic[DType.uint64].load[
            ordering=Ordering.RELAXED
        ](seen + i) != 1:
            first_bad = i
            break
    var invalid = invalid_values.load[ordering=Ordering.RELAXED]()
    seen.free()
    counts.free()

    if (
        actual_count != UInt64(TOTAL_MESSAGES)
        or invalid != 0
        or first_bad >= 0
    ):
        print(
            "FAIL: WCQ try API count/expected/invalid/first_bad",
            actual_count,
            TOTAL_MESSAGES,
            invalid,
            first_bad,
        )
        exit(1)
    print("PASS: WCQ concurrent try exact-once correctness")
