from std.algorithm import parallelize
from std.atomic import Atomic, Ordering
from std.sys.terminate import exit
from MoStream.WCQ_queue import WCQQueue


def run_case[
    producers: Int, consumers: Int, messages_per_producer: Int
](capacity: Int) -> Bool:
    comptime total_messages = producers * messages_per_producer
    var queue = WCQQueue(capacity, producers + consumers)
    var finished_producers = Atomic[DType.uint64](0)
    var invalid_values = Atomic[DType.uint64](0)
    var seen = alloc[UInt64](total_messages)
    var counts = alloc[UInt64](consumers)
    for i in range(total_messages):
        seen[i] = 0
    for i in range(consumers):
        counts[i] = 0

    @parameter
    def worker(thread_id: Int):
        if thread_id < producers:
            var base = thread_id * messages_per_producer
            for i in range(messages_per_producer):
                queue.push(thread_id, UInt64(base + i))

            var previous = finished_producers.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(thread_id, UInt64.MAX)
        else:
            var consumer_id = thread_id - producers
            var local_count: UInt64 = 0
            while True:
                var value = queue.pop(thread_id)
                if value == UInt64.MAX:
                    counts[consumer_id] = local_count
                    return
                if value >= UInt64(total_messages):
                    _ = invalid_values.fetch_add[
                        ordering=Ordering.RELAXED
                    ](1)
                else:
                    _ = Atomic[DType.uint64].fetch_add[
                        ordering=Ordering.RELAXED
                    ](seen + Int(value), 1)
                local_count += 1

    parallelize[worker](producers + consumers)

    var actual_count: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[i]
    var first_bad = -1
    for i in range(total_messages):
        if Atomic[DType.uint64].load[
            ordering=Ordering.RELAXED
        ](seen + i) != 1:
            first_bad = i
            break
    var invalid = invalid_values.load[ordering=Ordering.RELAXED]()

    # With all workers quiescent, prove that the complete index pool was
    # returned and remains reusable after the concurrent run.
    var boundary_ok = True
    for i in range(capacity):
        if queue.try_push(0, UInt64(i)):
            boundary_ok = False
            break
    var overflow = queue.try_push(0, UInt64.MAX - 1)
    if not overflow:
        boundary_ok = False
    for i in range(capacity):
        var item = queue.try_pop(0)
        if not item or item.take() != UInt64(i):
            boundary_ok = False
            break
    if queue.try_pop(0):
        boundary_ok = False

    seen.free()
    counts.free()
    var valid = (
        actual_count == UInt64(total_messages)
        and invalid == 0
        and first_bad < 0
        and boundary_ok
    )
    print(
        "case",
        producers,
        "P-",
        consumers,
        "C capacity",
        capacity,
        "valid=",
        valid,
    )
    if not valid:
        print(
            "  count/expected invalid first_bad boundary_ok:",
            actual_count,
            total_messages,
            invalid,
            first_bad,
            boundary_ok,
        )
    return valid


def main():
    if not run_case[1, 1, 50_000](8):
        exit(1)
    if not run_case[2, 2, 50_000](8):
        exit(1)
    if not run_case[4, 4, 50_000](8):
        exit(1)
    if not run_case[8, 8, 50_000](16):
        exit(1)
    print("PASS: WCQ blocking concurrent exact-once correctness")
