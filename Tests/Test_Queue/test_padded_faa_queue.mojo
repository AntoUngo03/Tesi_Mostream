from MoStream.Padded_FAA_queue import PaddedFAAQueue


def main():
    var queue = PaddedFAAQueue[Int](8)
    for i in range(8):
        if queue.try_push(i):
            print("FAIL: unexpected rejection")
            return
    var extra = 99
    if not queue.try_push(extra):
        print("FAIL: full queue accepted item")
        return
    for expected in range(8):
        var item = queue.try_pop()
        if not item or item.value() != expected:
            print("FAIL: FIFO mismatch")
            return
    if queue.try_pop():
        print("FAIL: empty queue returned item")
        return
    for i in range(32):
        queue.push(i)
        if queue.pop() != i:
            print("FAIL: wrap-around mismatch")
            return
    print("PASS: PaddedFAAQueue sequential correctness")
