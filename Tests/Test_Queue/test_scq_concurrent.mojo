from std.algorithm import parallelize
from std.atomic import Atomic, Ordering
from std.sys.terminate import exit
from MoStream.SCQ_queue import SCQQueue


comptime PRODUCERS = 4
comptime CONSUMERS = 4
comptime MESSAGES = 5_000
comptime TOTAL = PRODUCERS * MESSAGES


def run_case(capacity: Int) -> Bool:
    var queue = SCQQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var invalid = Atomic[DType.uint64](0)
    var seen = alloc[Atomic[DType.uint64]](TOTAL)
    var counts = alloc[UInt64](CONSUMERS)
    for i in range(TOTAL):
        seen[i] = Atomic[DType.uint64](0)
    for i in range(CONSUMERS):
        counts[i] = 0

    @parameter
    def worker(thread_id: Int):
        if thread_id < PRODUCERS:
            var base = thread_id * MESSAGES
            for i in range(MESSAGES):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(PRODUCERS):
                for _ in range(CONSUMERS):
                    queue.push(-1)
        else:
            var consumer = thread_id - PRODUCERS
            var local_count: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[consumer] = local_count
                    return
                if value < 0 or value >= TOTAL:
                    _ = invalid.fetch_add[ordering=Ordering.RELAXED](1)
                else:
                    _ = seen[value].fetch_add[ordering=Ordering.RELAXED](1)
                local_count += 1

    parallelize[worker](PRODUCERS + CONSUMERS)

    var actual: UInt64 = 0
    for i in range(CONSUMERS):
        actual += counts[i]
    var first_bad = -1
    for i in range(TOTAL):
        if seen[i].load[ordering=Ordering.RELAXED]() != 1:
            first_bad = i
            break
    var invalid_count = invalid.load[ordering=Ordering.RELAXED]()
    for i in range(TOTAL):
        (seen + i).destroy_pointee()
    seen.free()
    counts.free()

    if (
        actual != UInt64(TOTAL)
        or invalid_count != 0
        or first_bad >= 0
        or queue.estimated_len() != 0
    ):
        print(
            "FAIL: SCQ capacity/count/invalid/first_bad/remaining",
            capacity,
            actual,
            invalid_count,
            first_bad,
            queue.estimated_len(),
        )
        return False
    print("PASS: SCQ concurrent exact-once capacity", capacity)
    return True


def main():
    if not run_case(2):
        exit(1)
    if not run_case(4):
        exit(1)
    if not run_case(16):
        exit(1)
    if not run_case(1024):
        exit(1)
    print("PASS: SCQ concurrent suite")
