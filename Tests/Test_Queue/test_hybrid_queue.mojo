from MoStream.Hybrid_queue import HybridMPMCQueue


def main():
    var queue = HybridMPMCQueue[Int](8)

    for i in range(8):
        var rejected = queue.try_push(i)
        if rejected:
            print("FAIL: unexpected try_push rejection")
            return

    var extra = 99
    if not queue.try_push(extra):
        print("FAIL: full queue accepted an extra item")
        return

    for expected in range(8):
        var item = queue.try_pop()
        if not item or item.value() != expected:
            print("FAIL: FIFO mismatch")
            return

    if queue.try_pop():
        print("FAIL: empty queue returned an item")
        return

    # Exercise blocking methods and ring-buffer wrap-around.
    for i in range(32):
        queue.push(i)
        if queue.pop() != i:
            print("FAIL: blocking push/pop mismatch")
            return

    print("PASS: HybridMPMCQueue sequential correctness")
