from MoStream.SCQ_queue import SCQQueue
from std.sys.terminate import exit


def fail(message: String):
    print("FAIL:", message)
    exit(1)


def main():
    var queue = SCQQueue[Int](8)
    for i in range(8):
        if queue.try_push(i):
            fail("unexpected rejection before capacity")
            return
    var extra = 99
    if not queue.try_push(extra):
        fail("full queue accepted an extra item")
        return
    for expected in range(8):
        var item = queue.try_pop()
        if not item or item.value() != expected:
            fail("FIFO mismatch")
            return
    if queue.try_pop():
        fail("empty queue returned an item")
        return
    for cycle in range(10_000):
        queue.push(cycle)
        if queue.pop() != cycle:
            fail("slot was not reusable after wrap-around")
            return
    print("PASS: SCQ sequential boundedness and FIFO correctness")
