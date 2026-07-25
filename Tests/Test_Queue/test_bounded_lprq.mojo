from MoStream.Bounded_LPRQ import BoundedLPRQInspired
from std.sys.terminate import exit


def fail(message: String):
    print("FAIL:", message)
    exit(1)


def main():
    var queue = BoundedLPRQInspired[Int](8)

    for i in range(8):
        if queue.try_push(i):
            fail("unexpected rejection before capacity")
            return

    var extra = 99
    if not queue.try_push(extra):
        fail("full bounded queue accepted a ninth item")
        return

    if queue.estimated_len() > 8:
        fail("estimated length exceeded fixed capacity")
        return

    for expected in range(8):
        var item = queue.try_pop()
        if not item or item.value() != expected:
            fail("FIFO mismatch while draining full queue")
            return

    if queue.try_pop():
        fail("empty queue returned an item")
        return

    # Exercise many generations of the same eight physical cells.
    for cycle in range(10_000):
        var value = cycle + 1_000
        queue.push(value)
        if queue.pop() != value:
            fail("cell generation was not reusable")
            return

    # Refill after wrap-around to verify that bounded-full detection recovers.
    for i in range(8):
        queue.push(20_000 + i)
    for i in range(8):
        if queue.pop() != 20_000 + i:
            fail("FIFO mismatch after wrap-around")
            return

    print(
        "PASS: BoundedLPRQInspired sequential boundedness and FIFO correctness"
    )
