from std.collections import Optional
from MoStream.Cooperative_FAA_queue import (
    CooperativeFAAQueue, PushOperation, PopOperation, PollStatus,
)


def check(condition: Bool) raises:
    if not condition:
        raise Error("cooperative FAA invariant failed")


def test_pending_and_close() raises:
    var queue = CooperativeFAAQueue[Int](2, 1)
    var push = PushOperation[Int]()
    var first = PopOperation[Int]()
    var second = PopOperation[Int]()
    var beyond_end = PopOperation[Int]()
    # More pending consumers than slots: each must retain its own ticket.
    check(queue.poll_pop(first) == PollStatus.WAIT)
    check(queue.poll_pop(second) == PollStatus.WAIT)
    check(queue.poll_pop(beyond_end) == PollStatus.WAIT)
    for _ in range(10):
        check(queue.poll_pop(first) == PollStatus.WAIT)
        check(first.ticket == 0)
    push.item = Optional(41)
    check(queue.poll_push(push) == PollStatus.SUCCESS)
    push.item = Optional(42)
    check(queue.poll_push(push) == PollStatus.SUCCESS)
    queue.producer_finished()
    # A terminal waiter may finish before the earlier readers drain the ring.
    check(queue.poll_pop(beyond_end) == PollStatus.CLOSED)
    check(not beyond_end.reserved)
    check(queue.poll_pop(second) == PollStatus.SUCCESS)
    check(second.item.take() == 42)
    check(queue.poll_pop(first) == PollStatus.SUCCESS)
    check(first.item.take() == 41)
    check(queue.poll_pop(first) == PollStatus.CLOSED)


def test_full_and_wrap[use_faa: Bool]() raises:
    var queue = CooperativeFAAQueue[Int, use_faa](2, 1)
    var push = PushOperation[Int]()
    var pop = PopOperation[Int]()
    for cycle in range(100):
        for i in range(2):
            push.item = Optional(cycle * 3 + i)
            check(queue.poll_push(push) == PollStatus.SUCCESS)
        push.item = Optional(cycle * 3 + 2)
        check(queue.poll_push(push) == PollStatus.WAIT)
        var ticket = push.ticket
        for _ in range(10):
            check(queue.poll_push(push) == PollStatus.WAIT)
            check(push.ticket == ticket)
            check(push.item.value() == cycle * 3 + 2)
        check(queue.poll_pop(pop) == PollStatus.SUCCESS)
        check(pop.item.take() == cycle * 3)
        check(queue.poll_push(push) == PollStatus.SUCCESS)
        for i in range(1, 3):
            check(queue.poll_pop(pop) == PollStatus.SUCCESS)
            check(pop.item.take() == cycle * 3 + i)
    queue.producer_finished()
    check(queue.poll_pop(pop) == PollStatus.CLOSED)


def test_empty_close() raises:
    var queue = CooperativeFAAQueue[Int](2, 2)
    var pop = PopOperation[Int]()
    check(queue.poll_pop(pop) == PollStatus.WAIT)
    queue.producer_finished()
    check(queue.poll_pop(pop) == PollStatus.WAIT)
    queue.producer_finished()
    check(queue.poll_pop(pop) == PollStatus.CLOSED)


def main() raises:
    test_pending_and_close()
    test_full_and_wrap[True]()
    test_full_and_wrap[False]()
    test_empty_close()
    print("PASS: cooperative FAA suspension, ownership, wrap, and close")
