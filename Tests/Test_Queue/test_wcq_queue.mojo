from std.sys.terminate import exit
from MoStream.WCQ_queue import WCQQueue


def fail(message: String, cycle: Int = -1, index: Int = -1):
    print("FAIL:", message, "cycle=", cycle, "index=", index)
    exit(1)


def main():
    comptime CAPACITY = 8
    var queue = WCQQueue(CAPACITY, 1)

    if queue.try_pop(0):
        fail("empty queue returned an item")

    # Repeatedly hit both exact boundaries and verify FIFO across many wraps.
    for cycle in range(100_000):
        for i in range(CAPACITY):
            if queue.try_push(0, UInt64(cycle * CAPACITY + i)):
                fail("queue became full before capacity", cycle, i)

        var rejected = queue.try_push(0, UInt64.MAX)
        if not rejected or rejected.take() != UInt64.MAX:
            fail("queue accepted more than its bounded capacity", cycle)

        for i in range(CAPACITY):
            var item = queue.try_pop(0)
            if not item:
                fail("queue became empty before capacity items", cycle, i)
            var expected = UInt64(cycle * CAPACITY + i)
            var actual = item.take()
            if actual != expected:
                print(
                    "FAIL: FIFO mismatch cycle/index/expected/actual",
                    cycle,
                    i,
                    expected,
                    actual,
                )
                exit(1)

        if queue.try_pop(0):
            fail("queue retained an item after complete drain", cycle)

    print("PASS: WCQ sequential bounded/FIFO/reuse correctness")
