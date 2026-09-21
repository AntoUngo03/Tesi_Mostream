# Bounded Michael-Scott-style queue with a type-preserving node pool.
# Nodes remain allocated until queue destruction. Only atomic links are read
# speculatively; only the winner of Head CAS accesses the payload.
#
# Each published node has two lifetime obligations: its payload must be taken,
# and it must be retired as the dummy head. The last completion recycles it.
# This lets a later consumer retire a node while its payload owner is paused.
#
# Tagged indices assume no operation spans 2**32 changes of the same tagged
# word. try_* may retry under contention; they are not wait-free. A stalled
# operation may retain pool capacity. Move/destruction require quiescence.

from std.memory.alloc import unsafe_alloc
from std.sys import get_defined_bool
from std.time import sleep
from std.atomic import Atomic, Ordering
from std.collections import Optional
from MoStream.utils import print_red_color

comptime MS_TEST_PAUSE_AFTER_HEAD_CAS = get_defined_bool[
    "MOSTREAM_MS_TEST_PAUSE", False
]()

comptime NULL_INDEX = UInt32(0xFFFFFFFF)

# Pack {version,index} into one UInt64:
# high 32 bits = version/tag, low 32 bits = node index.
def _pack(index: UInt32, tag: UInt32) -> UInt64:
    return (UInt64(tag) << 32) | UInt64(index)

def _index(x: UInt64) -> UInt32:
    return UInt32(x & UInt64(0xFFFFFFFF))

def _tag(x: UInt64) -> UInt32:
    return UInt32(x >> 32)


struct MSNode[T: Copyable & Deinitable](Movable):
    # Queue link. The tag changes whenever the link is modified.
    var next: Atomic[DType.uint64]

    # Free-list link; only meaningful while the node belongs to the pool.
    var free_next: Atomic[DType.uint32]

    # Two completions per published node; initial dummy needs only retirement.
    var remaining: Atomic[DType.uint64]

    # Payload may remain in a retired dummy until its consumer finishes.
    var data: Optional[Self.T]

    def __init__(out self):
        self.next = Atomic[DType.uint64](_pack(NULL_INDEX, 0))
        self.free_next = Atomic[DType.uint32](NULL_INDEX)
        self.remaining = Atomic[DType.uint64](0)
        self.data = Optional[Self.T](None)

    def __init__(out self, *, deinit move: Self):
        self.next = Atomic[DType.uint64](move.next.load[ordering=Ordering.RELAXED]())
        self.free_next = Atomic[DType.uint32](move.free_next.load[ordering=Ordering.RELAXED]())
        self.remaining = Atomic[DType.uint64](move.remaining.load[ordering=Ordering.RELAXED]())
        self.data = move.data^


struct MichaelScottQueue[T: Copyable & Deinitable](Movable):
    comptime NodePointer = Pointer[MSNode[Self.T], MutUntrackedOrigin]

    # Pool has capacity + 1 nodes because one node is always the dummy head.
    var nodes: Self.NodePointer
    var capacity: UInt32
    var pool_size: UInt32

    # Tagged indices reject stale snapshots until the 32-bit tag wraps.
    var head: Atomic[DType.uint64]
    var tail: Atomic[DType.uint64]

    # Treiber-style free-list head, also tagged against ABA.
    var free_head: Atomic[DType.uint64]

    # Publication can race ahead of the producer's increment. Signed storage
    # prevents a transient negative estimate from looking like a full queue.
    var count: Atomic[DType.int64]

    def __init__(out self, size: Int = 1024) raises:
        if size < 2 or size >= Int(NULL_INDEX):
            print_red_color("{MoStream} Error: MichaelScottQueue size must be in [2, 2**32 - 2]!")
            raise Error("error in MichaelScottQueue()")

        self.capacity = UInt32(size)
        self.pool_size = UInt32(size + 1)
        self.nodes = unsafe_alloc[MSNode[Self.T]](size + 1)

        for i in range(size + 1):
            self.nodes.unsafe_offset(i).unsafe_write(MSNode[Self.T]())

        # Node 0 is the initial dummy node.
        # Nodes 1..capacity start in the free list.
        for i in range(1, size + 1):
            var next_free = NULL_INDEX if i == size else UInt32(i + 1)
            self.nodes.unsafe_offset(i)[].free_next.store[
                ordering=Ordering.RELAXED
            ](next_free)

        self.nodes[].remaining.store[ordering=Ordering.RELAXED](1)
        self.head = Atomic[DType.uint64](_pack(UInt32(0), UInt32(0)))
        self.tail = Atomic[DType.uint64](_pack(UInt32(0), UInt32(0)))
        self.free_head = Atomic[DType.uint64](_pack(UInt32(1), UInt32(0)))
        self.count = Atomic[DType.int64](0)

    def __init__(out self, *, deinit move: Self):
        self.nodes = move.nodes
        self.capacity = move.capacity
        self.pool_size = move.pool_size

        self.head = Atomic[DType.uint64](move.head.load[ordering=Ordering.RELAXED]())
        self.tail = Atomic[DType.uint64](move.tail.load[ordering=Ordering.RELAXED]())
        self.free_head = Atomic[DType.uint64](move.free_head.load[ordering=Ordering.RELAXED]())
        self.count = Atomic[DType.int64](move.count.load[ordering=Ordering.RELAXED]())

    def __deinit__(deinit self):
        for i in range(Int(self.pool_size)):
            self.nodes.unsafe_offset(i).unsafe_deinit_pointee()
        self.nodes.unsafe_free()

    # ------------------------------------------------------------------
    # Pool management
    # ------------------------------------------------------------------

    def _try_alloc_node(mut self) -> Optional[UInt32]:
        while True:
            var old = self.free_head.load[ordering=Ordering.ACQUIRE]()
            var idx = _index(old)

            if idx == NULL_INDEX:
                return Optional[UInt32](None)

            var nxt = self.nodes.unsafe_offset(Int(idx))[].free_next.load[
                ordering=Ordering.RELAXED
            ]()
            var desired = _pack(nxt, _tag(old) + 1)

            if self.free_head.compare_exchange[
                failure_ordering=Ordering.RELAXED,
                success_ordering=Ordering.ACQUIRE
            ](old, desired):
                var node = self.nodes.unsafe_offset(Int(idx))
                # Never reset a link's generation: a producer can still hold
                # an old Tail.next snapshot even after Tail has advanced.
                var previous = node[].next.load[ordering=Ordering.RELAXED]()
                node[].next.store[ordering=Ordering.RELAXED](
                    _pack(NULL_INDEX, _tag(previous) + 1)
                )
                node[].remaining.store[ordering=Ordering.RELAXED](2)
                return Optional(idx)

    def _release_node(mut self, idx: UInt32):
        # Both lifetime obligations have completed; payload is already empty.
        # Stale observers may still read atomic links, but cannot own this node.

        while True:
            var old = self.free_head.load[ordering=Ordering.RELAXED]()
            self.nodes.unsafe_offset(Int(idx))[].free_next.store[
                ordering=Ordering.RELAXED
            ](_index(old))
            var desired = _pack(idx, _tag(old) + 1)

            if self.free_head.compare_exchange[
                failure_ordering=Ordering.RELAXED,
                success_ordering=Ordering.RELEASE
            ](old, desired):
                return

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        # Failure preserves item; in-flight operations may retain pool nodes
        # even when the linked queue contains fewer than capacity elements.
        var maybe_idx = self._try_alloc_node()
        if not maybe_idx:
            return Optional(item^)

        var node_idx = maybe_idx.take()
        var node_ptr = self.nodes.unsafe_offset(Int(node_idx))
        node_ptr[].data = Optional(item^)

        while True:
            var tail_old = self.tail.load[ordering=Ordering.ACQUIRE]()
            var tail_idx = _index(tail_old)
            var tail_ptr = self.nodes.unsafe_offset(Int(tail_idx))
            var next_old = tail_ptr[].next.load[ordering=Ordering.ACQUIRE]()

            # Revalidate Tail after reading Tail.next.
            if tail_old != self.tail.load[ordering=Ordering.ACQUIRE]():
                continue

            if _index(next_old) == NULL_INDEX:
                # Linearization point of enqueue: link node after current tail.
                var desired_next = _pack(node_idx, _tag(next_old) + 1)

                if tail_ptr[].next.compare_exchange[
                    failure_ordering=Ordering.RELAXED,
                    success_ordering=Ordering.RELEASE
                ](next_old, desired_next):

                    # Tail may lag. This CAS is only a helping optimization.
                    var desired_tail = _pack(node_idx, _tag(tail_old) + 1)
                    _ = self.tail.compare_exchange[
                        failure_ordering=Ordering.RELAXED,
                        success_ordering=Ordering.RELEASE
                    ](tail_old, desired_tail)

                    _ = self.count.fetch_add[ordering=Ordering.RELAXED](1)
                    return None
            else:
                # Tail is behind: help another producer advance it.
                var desired_tail = _pack(
                    _index(next_old),
                    _tag(tail_old) + 1
                )
                _ = self.tail.compare_exchange[
                    failure_ordering=Ordering.RELAXED,
                    success_ordering=Ordering.RELEASE
                ](tail_old, desired_tail)

    def push(mut self, var item: Self.T):
        # Blocking API compatible with MPMCQueue.
        var pending = Optional(item^)
        while pending:
            pending = self.try_push(pending.take())

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    def try_pop(mut self) -> Optional[Self.T]:
        while True:
            var head_old = self.head.load[ordering=Ordering.ACQUIRE]()
            var tail_old = self.tail.load[ordering=Ordering.ACQUIRE]()

            var head_idx = _index(head_old)
            var head_ptr = self.nodes.unsafe_offset(Int(head_idx))
            var next_old = head_ptr[].next.load[ordering=Ordering.ACQUIRE]()

            # Revalidate Head after reading Head.next.
            if head_old != self.head.load[ordering=Ordering.ACQUIRE]():
                continue

            var next_idx = _index(next_old)

            if head_idx == _index(tail_old):
                if next_idx == NULL_INDEX:
                    # Head == Tail and dummy has no successor => empty.
                    return Optional[Self.T](None)

                # Tail is behind: help it.
                var desired_tail = _pack(
                    next_idx,
                    _tag(tail_old) + 1
                )
                _ = self.tail.compare_exchange[
                    failure_ordering=Ordering.RELAXED,
                    success_ordering=Ordering.RELEASE
                ](tail_old, desired_tail)
                continue

            # Winning Head CAS assigns the payload to this consumer. The
            # node cannot be recycled until _finish_pop drops its obligation.
            var desired_head = _pack(
                next_idx,
                _tag(head_old) + 1
            )

            if self.head.compare_exchange[
                failure_ordering=Ordering.RELAXED,
                success_ordering=Ordering.ACQUIRE_RELEASE
            ](head_old, desired_head):

                comptime if MS_TEST_PAUSE_AFTER_HEAD_CAS:
                    sleep(0.00001)
                return self._finish_pop(head_idx, next_idx)

    def _complete_node_use(mut self, idx: UInt32):
        # ACQ_REL joins payload completion with dummy retirement, whichever
        # arrives last, before publishing the node back to the free-list.
        var previous = self.nodes.unsafe_offset(Int(idx))[].remaining.fetch_sub[
            ordering=Ordering.ACQUIRE_RELEASE
        ](1)
        if previous == 1:
            self._release_node(idx)

    def _finish_pop(
        mut self, old_dummy: UInt32, payload_node: UInt32
    ) -> Optional[Self.T]:
        # Called exactly once by the winner of Head CAS, never by a loser.
        var result = self.nodes.unsafe_offset(Int(payload_node))[].data.take()
        _ = self.count.fetch_sub[ordering=Ordering.RELAXED](1)
        self._complete_node_use(payload_node)
        self._complete_node_use(old_dummy)
        return Optional(result^)

    def pop(mut self) -> Self.T:
        while True:
            var item = self.try_pop()
            if item:
                return item.take()

    def estimated_len(self) -> Int:
        var n = self.count.load[ordering=Ordering.RELAXED]()
        if n <= 0:
            return 0
        if n > Int64(self.capacity):
            return Int(self.capacity)
        return Int(n)
