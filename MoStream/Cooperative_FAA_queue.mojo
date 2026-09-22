"""Experimental actor-owned operations over the existing padded sequence ring.

Every pending FAA operation must be polled to completion by its owner. Operations
must not be copied, abandoned, used with another queue, or polled concurrently.
The scheduler must fairly revisit ALL pending actors, including after close.
Close is permanent; cancellation and counter wrap-around are not supported.
"""

from std.atomic import Atomic, Ordering
from std.collections import Optional
from MoStream.Padded_FAA_queue import PaddedFAAQueue


struct PollStatus:
    comptime SUCCESS: Int = 0
    comptime RETRY: Int = 1
    comptime WAIT: Int = 2
    comptime CLOSED: Int = 3


struct PushOperation[T: Copyable & Deinitable](Movable):
    var ticket: UInt64
    var reserved: Bool
    var item: Optional[Self.T]

    def __init__(out self):
        self.ticket = 0
        self.reserved = False
        self.item = None


struct PopOperation[T: Copyable & Deinitable](Movable):
    var ticket: UInt64
    var reserved: Bool
    var item: Optional[Self.T]

    def __init__(out self):
        self.ticket = 0
        self.reserved = False
        self.item = None


struct CooperativeFAAQueue[T: Copyable & Deinitable, use_faa: Bool = True](Movable):
    """One bounded poll per activation; no spin, sleep, or internal retry loop.

With use_faa=False, a lost CAS returns RETRY and no ticket is retained.
With use_faa=True, WAIT retains the ticket until success or terminal read.
Exactly producer_count calls to producer_finished are required, each AFTER
that producer's last push completes. No subsequent pushes are allowed.
"""

    var ring: PaddedFAAQueue[Self.T]
    var remaining: Atomic[DType.uint64]

    def __init__(out self, size: Int, producer_count: Int) raises:
        if producer_count < 1:
            raise Error("producer_count must be positive")
        self.ring = PaddedFAAQueue[Self.T](size)
        self.remaining = Atomic[DType.uint64](UInt64(producer_count))

    def __init__(out self, *, deinit move: Self):
        self.ring = move.ring^
        self.remaining = Atomic[DType.uint64](
            move.remaining.load[ordering=Ordering.RELAXED]()
        )

    def producer_finished(mut self):
        _ = self.remaining.fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)

    def is_closed(self) -> Bool:
        return self.remaining.load[ordering=Ordering.ACQUIRE]() == 0

    def poll_push(mut self, mut operation: PushOperation[Self.T]) -> Int:
        debug_assert(Bool(operation.item), "push requires a payload")
        comptime if Self.use_faa:
            if not operation.reserved:
                operation.ticket = self.ring.enqueue_pos.value.fetch_add[
                    ordering=Ordering.RELAXED
                ](1)
                operation.reserved = True
            var slot = self.ring.slots.unsafe_offset(
                Int(operation.ticket & self.ring.mask)
            )
            if slot[].sequence.load[ordering=Ordering.ACQUIRE]() != operation.ticket:
                return PollStatus.WAIT
        else:
            operation.ticket = self.ring.enqueue_pos.value.load[
                ordering=Ordering.RELAXED
            ]()
            var slot = self.ring.slots.unsafe_offset(
                Int(operation.ticket & self.ring.mask)
            )
            if slot[].sequence.load[ordering=Ordering.ACQUIRE]() != operation.ticket:
                if self.ring.enqueue_pos.value.load[ordering=Ordering.RELAXED]() != operation.ticket:
                    return PollStatus.RETRY
                return PollStatus.WAIT
            var expected = operation.ticket
            if not self.ring.enqueue_pos.value.compare_exchange[
                success_ordering=Ordering.RELAXED,
                failure_ordering=Ordering.RELAXED,
            ](expected, operation.ticket + 1):
                return PollStatus.RETRY

        var slot = self.ring.slots.unsafe_offset(Int(operation.ticket & self.ring.mask))
        slot[].data = Optional(operation.item.take())
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=slot[].sequence.value), operation.ticket + 1
        )
        operation.reserved = False
        return PollStatus.SUCCESS

    def poll_pop(mut self, mut operation: PopOperation[Self.T]) -> Int:
        debug_assert(not operation.item, "consume the previous pop result first")
        comptime if Self.use_faa:
            if not operation.reserved:
                # Once closed, avoid issuing further terminal tickets.
                if self.is_closed():
                    if self.ring.dequeue_pos.value.load[ordering=Ordering.RELAXED]() >= self.ring.enqueue_pos.value.load[ordering=Ordering.RELAXED]():
                        return PollStatus.CLOSED
                operation.ticket = self.ring.dequeue_pos.value.fetch_add[
                    ordering=Ordering.RELAXED
                ](1)
                operation.reserved = True
        else:
            operation.ticket = self.ring.dequeue_pos.value.load[
                ordering=Ordering.RELAXED
            ]()

        var slot = self.ring.slots.unsafe_offset(Int(operation.ticket & self.ring.mask))
        if slot[].sequence.load[ordering=Ordering.ACQUIRE]() != operation.ticket + 1:
            # Acquire of remaining==0 sees ALL producer publications. Tickets
            # beyond the final enqueue position are terminal, never cancelled
            # while the queue is open and never reused by a future producer.
            if self.is_closed():
                if operation.ticket >= self.ring.enqueue_pos.value.load[ordering=Ordering.RELAXED]():
                    operation.reserved = False
                    return PollStatus.CLOSED
            comptime if not Self.use_faa:
                if self.ring.dequeue_pos.value.load[ordering=Ordering.RELAXED]() != operation.ticket:
                    return PollStatus.RETRY
            return PollStatus.WAIT

        comptime if not Self.use_faa:
            var expected = operation.ticket
            if not self.ring.dequeue_pos.value.compare_exchange[
                success_ordering=Ordering.RELAXED,
                failure_ordering=Ordering.RELAXED,
            ](expected, operation.ticket + 1):
                return PollStatus.RETRY
        operation.item = Optional(slot[].data.take())
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=slot[].sequence.value), operation.ticket + self.ring.size
        )
        operation.reserved = False
        return PollStatus.SUCCESS
