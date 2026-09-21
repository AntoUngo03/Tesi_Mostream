from std.atomic import Ordering
from std.collections import List
from std.sys.terminate import exit
from MoStream.NBLFQ_queue import NBLFQQueue, NBLFQIndexRing


def require(condition: Bool, message: String):
    if not condition:
        print("FAIL:", message)
        exit(1)


def test_payloads() raises:
    for capacity in [2, 3, 16]:
        var queue = NBLFQQueue[String](capacity)
        for cycle in range(100):
            require(not queue.try_pop(), "empty")
            for i in range(capacity):
                require(not queue.try_push("payload " + String(cycle * capacity + i)), "fill")
            var rejected = queue.try_push("ownership retained")
            require(Bool(rejected), "bounded full")
            require(rejected.take() == "ownership retained", "rejected payload intact")
            require(queue.estimated_len() == capacity, "full estimate")
            for i in range(capacity):
                require(queue.pop() == "payload " + String(cycle * capacity + i), "FIFO")
            require(queue.estimated_len() == 0, "empty estimate")
        queue.push("survives move with nonempty index rings")
        var moved = queue^
        require(moved.pop() == "survives move with nonempty index rings", "move")
        moved.push("destroy while still queued")
    var rejected = 0
    for capacity in [0, 1, 0xFFFFFFFF]:
        try:
            var invalid = NBLFQQueue[Int](capacity)
            _ = invalid
        except:
            rejected += 1
    require(rejected == 3, "invalid capacity")


def test_ring_wrap_and_hints() raises:
    # Small tags exercise many complete wraps without stalled snapshots.
    # Arbitrary valid hints emulate late stores by other completed operations.
    var ring = NBLFQIndexRing[3](3)
    for cycle in range(1000):
        for i in range(3):
            ring.head.value.store[ordering=Ordering.RELAXED](UInt64((cycle + i) % 3))
            require(ring.try_enqueue(UInt32(i)), "recover stale insertion hint")
        require(not ring.try_enqueue(0), "full raw ring")
        for i in range(3):
            ring.tail.value.store[ordering=Ordering.RELAXED](UInt64((cycle + i) % 3))
            var result = ring.try_dequeue()
            require(Bool(result), "recover stale removal hint")
            require(result.take() == UInt32(i), "FIFO across tag wraps")
        require(not ring.try_dequeue(), "empty raw ring")

    var ordinary = NBLFQIndexRing[](2)
    var stale = ordinary.load(0)
    require(ordinary.try_enqueue(0), "publish before stale CAS")
    require(ordinary.try_dequeue().value() == 0, "consume before stale CAS")
    require(not ordinary.entries[].compare_exchange[
        success_ordering=Ordering.SEQUENTIAL,
        failure_ordering=Ordering.RELAXED,
    ](stale, ordinary.pack(2, 0)), "old empty generation cannot publish")


def test_inflight_payload() raises:
    var queue = NBLFQQueue[String](2)
    queue.push("owned by suspended consumer")
    queue.push("second")
    # Suspend consumer A after winning the occupied-index dequeue. Other
    # operations can reuse B's storage, but cannot get A's private index.
    var held = queue.allocated_indices.try_dequeue().value()
    require(queue.pop() == "second", "other consumer progresses")
    queue.push("replacement")
    require(Bool(queue.try_push("still unavailable")), "private slot retained")
    var payload = queue.data.unsafe_offset(Int(held))[].take()
    require(payload == "owned by suspended consumer", "in-flight payload intact")
    queue.free_indices.enqueue(held)
    _ = queue.count.fetch_sub[ordering=Ordering.RELAXED](1)
    require(queue.pop() == "replacement", "unrelated payload intact")
    require(queue.estimated_len() == 0, "all completions accounted for")
    # Suspend a producer before publication: it must not reserve a FIFO hole.
    var private_slot = queue.free_indices.try_dequeue().value()
    queue.push("later producer can publish")
    require(queue.pop() == "later producer can publish", "no unpublished FIFO hole")
    queue.free_indices.enqueue(private_slot)


def main() raises:
    test_payloads()
    test_ring_wrap_and_hints()
    test_inflight_payload()
    print("PASS: NBLFQ FIFO, capacity, ownership, move, wraps, stale hints and paused owners")
