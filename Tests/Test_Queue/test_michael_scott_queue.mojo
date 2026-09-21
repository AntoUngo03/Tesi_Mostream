from std.collections import List
from std.atomic import Ordering
from std.sys.terminate import exit
from MoStream.Micheal_Scott import MichaelScottQueue, NULL_INDEX, _pack, _index, _tag


def require(condition: Bool, message: String):
    if not condition:
        print("FAIL:", message)
        exit(1)


def test_sequential() raises:
    # Non-power-of-two capacities are supported too.
    for capacity in [2, 3, 16]:
        var queue = MichaelScottQueue[String](capacity)
        require(not queue.try_pop(), "initially empty")
        for cycle in range(100):
            for i in range(capacity):
                require(not queue.try_push(String(cycle * capacity + i)), "fill")
            require(queue.estimated_len() == capacity, "full length")
            var rejected = queue.try_push("retained on failure")
            require(Bool(rejected), "bounded capacity")
            require(rejected.take() == "retained on failure", "failed push owns payload")
            for i in range(capacity):
                require(queue.pop() == String(cycle * capacity + i), "FIFO after reuse")
            require(not queue.try_pop(), "empty after drain")
            require(queue.estimated_len() == 0, "empty length")
        queue.push("survives move")
        var moved = queue^
        require(moved.pop() == "survives move", "move populated queue")
        moved.push("destroy pending payload")

    var rejected_sizes = 0
    for size in [0, 1, Int(NULL_INDEX)]:
        try:
            var invalid = MichaelScottQueue[Int](size)
            _ = invalid
        except:
            rejected_sizes += 1
    require(rejected_sizes == 3, "invalid sizes rejected before allocation")


def test_suspended_consumer() raises:
    var queue = MichaelScottQueue[String](2)
    queue.push("first payload with separately owned storage")
    queue.push("second payload with separately owned storage")
    # Deterministically suspend consumer A just after its successful Head CAS.
    # Consumer B then removes A's payload node from the linked list.
    var old = queue.head.load[ordering=Ordering.ACQUIRE]()
    var old_index = _index(old)
    var next = queue.nodes.unsafe_offset(Int(old_index))[].next.load[ordering=Ordering.ACQUIRE]()
    var payload_index = _index(next)
    require(queue.head.compare_exchange[
        success_ordering=Ordering.ACQUIRE_RELEASE,
        failure_ordering=Ordering.RELAXED,
    ](old, _pack(payload_index, _tag(old) + 1)), "claim first payload")
    require(queue.pop() == "second payload with separately owned storage", "second consumer")
    require(not queue.try_pop(), "logically empty during suspended consumer")
    # Retired nodes are still owned by A, so a failed push is permitted here.
    require(Bool(queue.try_push("cannot recycle A yet")), "retain suspended consumer nodes")
    var first = queue._finish_pop(old_index, payload_index)
    require(first.take() == "first payload with separately owned storage", "suspended payload intact")
    queue.push("reused one")
    queue.push("reused two")
    require(queue.pop() == "reused one", "capacity recovered 1")
    require(queue.pop() == "reused two", "capacity recovered 2")
    require(queue.estimated_len() == 0, "count after suspended consumer")


def test_stale_producer_link() raises:
    var queue = MichaelScottQueue[Int](2)
    queue.push(1)
    # Producer A has validated Tail and is about to CAS Tail.next.
    var tail = queue.tail.load[ordering=Ordering.ACQUIRE]()
    var node = queue.nodes.unsafe_offset(Int(_index(tail)))
    var stale = node[].next.load[ordering=Ordering.ACQUIRE]()
    require(_index(stale) == NULL_INDEX, "snapshot of last node")
    # Other operations retire and reuse precisely that node as the new tail.
    queue.push(2)
    require(queue.pop() == 1, "retire initial dummy")
    require(queue.pop() == 2, "retire producer snapshot node")
    queue.push(3)
    require(_index(queue.tail.load()) == _index(tail), "same physical node reused")
    require(not node[].next.compare_exchange[
        success_ordering=Ordering.RELEASE,
        failure_ordering=Ordering.RELAXED,
    ](stale, _pack(UInt32(0), _tag(stale) + 1)), "reject stale link CAS after reuse")
    require(queue.pop() == 3, "queue intact after stale producer")


def main() raises:
    test_sequential()
    test_suspended_consumer()
    test_stale_producer_link()
    print("PASS: MichaelScott FIFO, ownership, move, delayed consumer and stale producer")
