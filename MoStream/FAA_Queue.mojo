# ===------------------------------------------------------------------------=== #
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU Lesser General Public License version 3 as
#  published by the Free Software Foundation.
#
#  Experimental bounded MPMC queue:
#  - based on Dmitry Vyukov's sequence-number ring buffer;
#  - blocking push/pop use Fetch-And-Add ticket allocation;
#  - try_push/try_pop retain CAS because reservations must be cancellable.
# ===------------------------------------------------------------------------=== #

from std.atomic import Atomic, Ordering
from std.time import sleep
from std.sys.info import size_of
from std.collections import Optional
from std.sys.terminate import exit
from MoStream.utils import print_red_color


# Atomic counter padded to one cache line.
#
# enqueue_pos and dequeue_pos are modified by different groups of threads.
# Keeping them on separate cache lines reduces false sharing.
struct PaddedAtomicU64:
    comptime CACHE_LINE_SIZE_BYTES = 64
    comptime ATOMIC_SIZE_BYTES = size_of[Atomic[DType.uint64]]()
    comptime PAD_BYTES = Self.CACHE_LINE_SIZE_BYTES - Self.ATOMIC_SIZE_BYTES

    var atomicVal: Atomic[DType.uint64]
    var pad: InlineArray[UInt8, Self.PAD_BYTES]

    def __init__(out self, initial: UInt64):
        self.atomicVal = Atomic[DType.uint64](initial)
        self.pad = InlineArray[UInt8, Self.PAD_BYTES](uninitialized=True)


# One ring-buffer slot.
#
# sequence indicates the generation/state of the cell:
#
# Producer ticket p:
#     sequence == p       -> cell available to producer
#     sequence  = p + 1   -> item published
#
# Consumer ticket c:
#     sequence == c + 1   -> item available to consumer
#     sequence  = c+size  -> cell released for next ring cycle
struct Cell[T: Copyable](Movable):
    var sequence: Atomic[DType.uint64]
    var data: Optional[Self.T]

    def __init__(out self, seq: UInt64):
        self.sequence = Atomic[DType.uint64](seq)
        self.data = Optional[Self.T](None)

    def __init__(out self, *, deinit take: Self):
        var seq = take.sequence.load[ordering=Ordering.RELAXED]()
        self.sequence = Atomic[DType.uint64](seq)
        self.data = take.data^


# Experimental bounded MPMC queue.
#
# Important:
# This is not the complete wait-free queue from Yang and
# Mellor-Crummey. It applies the paper's main performance idea:
# using FAA to distribute distinct positions without CAS retries.
#
# A thread that obtains a ticket in push/pop must wait for that
# ticket's slot. Therefore these blocking methods are not wait-free.
struct MPMCQueue[T: Copyable](Movable):
    comptime CellPointer = UnsafePointer[
        Cell[Self.T],
        MutExternalOrigin
    ]

    # Number of unsuccessful slot observations before yielding.
    #
    # This is not a memory-ordering mechanism. It only prevents one
    # delayed producer/consumer from causing an unlimited tight spin.
    comptime SPINS_BEFORE_YIELD = 1024

    var buffer: Self.CellPointer
    var size: UInt64
    var mask: UInt64

    # Producer and consumer counters occupy different padded objects.
    var enqueue_pos: PaddedAtomicU64
    var dequeue_pos: PaddedAtomicU64


    # -------------------------------------------------------------------------
    # Construction
    # -------------------------------------------------------------------------

    def __init__(out self, size: Int = 1024):
        if not ((size >= 2) and ((size & (size - 1)) == 0)):
            print_red_color(
                "{MoStream} Error: MPMC queue size must be "
                "a power of two and at least 2!"
            )
            exit(1)

        self.size = UInt64(size)
        self.mask = UInt64(size - 1)

        self.buffer = alloc[Cell[Self.T]](size)

        self.enqueue_pos = PaddedAtomicU64(0)
        self.dequeue_pos = PaddedAtomicU64(0)

        for i in range(size):
            (self.buffer + i).init_pointee_move(
                Cell[Self.T](UInt64(i))
            )


    # Move the queue while preserving the logical positions.
    #
    # The original implementation reset both positions to zero. That
    # would make the counters inconsistent with the sequence values
    # already stored in the cells whenever a non-empty or previously
    # used queue is moved.
    def __init__(out self, *, deinit take: Self):
        var enqueue = take.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()

        var dequeue = take.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()

        self.buffer = take.buffer
        self.size = take.size
        self.mask = take.mask

        self.enqueue_pos = PaddedAtomicU64(enqueue)
        self.dequeue_pos = PaddedAtomicU64(dequeue)


    def __del__(deinit self):
        for i in range(Int(self.size)):
            (self.buffer + i).destroy_pointee()

        self.buffer.free()


    # -------------------------------------------------------------------------
    # Internal waiting utility
    # -------------------------------------------------------------------------

    # Perform an occasional scheduler yield during a long wait.
    #
    # The original:
    #
    #     for _ in range(backoff):
    #         pass
    #
    # may be optimized away because it has no observable effect.
    #
    # sleep(0.0) should be used sparingly because invoking the scheduler
    # on every failed observation would severely reduce performance.
    @always_inline
    def wait_or_yield(self, spins: Int) -> Int:
        var new_spins = spins + 1

        if new_spins >= Self.SPINS_BEFORE_YIELD:
            sleep(0.0)
            new_spins = 0

        return new_spins


    # -------------------------------------------------------------------------
    # Blocking producer operation: FAA fast ticket allocation
    # -------------------------------------------------------------------------

    # Reserve one unique ticket using fetch_add and wait until its slot
    # becomes available.
    #
    # Compared with load + compare_exchange, producers no longer waste
    # work retrying the enqueue_pos CAS under high producer contention.
    def push(mut self, var item: Self.T):
        var producer_ticket = self.enqueue_pos.atomicVal.fetch_add[
            ordering=Ordering.RELAXED
        ](1)

        var cell_ptr = self.buffer + Int(producer_ticket & self.mask)
        var spins = 0

        while True:
            var sequence = cell_ptr[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]()

            # This generation of the cell is available to this producer.
            if sequence == producer_ticket:
                # Write data before publishing the READY sequence.
                cell_ptr[].data = Optional(item^)

                Atomic[DType.uint64].store[
                    ordering=Ordering.RELEASE
                ](
                    UnsafePointer(to=cell_ptr[].sequence.value),
                    producer_ticket + 1
                )

                return

            # The ticket cannot be abandoned: wait for this exact cell.
            spins = self.wait_or_yield(spins)


    # -------------------------------------------------------------------------
    # Non-blocking producer operation: CAS reservation
    # -------------------------------------------------------------------------

    # Try once to reserve the current producer position.
    #
    # A CAS failure means another producer won the same current ticket.
    # It does not necessarily mean the entire queue is full. However,
    # returning the item is valid try-operation behaviour: the operation
    # did not complete immediately.
    #
    # FAA is intentionally not used here. If the cell were unavailable
    # after fetch_add, the reserved ticket could not be rolled back safely.
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var producer_ticket = self.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()

        var cell_ptr = self.buffer + Int(producer_ticket & self.mask)

        var sequence = cell_ptr[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]()

        # The slot for the current producer position is not available.
        if sequence != producer_ticket:
            return Optional(item^)

        var expected = producer_ticket

        if not self.enqueue_pos.atomicVal.compare_exchange[
            failure_ordering=Ordering.RELAXED,
            success_ordering=Ordering.RELAXED
        ](
            expected,
            producer_ticket + 1
        ):
            # Another producer claimed this position.
            return Optional(item^)

        cell_ptr[].data = Optional(item^)

        Atomic[DType.uint64].store[
            ordering=Ordering.RELEASE
        ](
            UnsafePointer(to=cell_ptr[].sequence.value),
            producer_ticket + 1
        )

        return Optional[Self.T](None)


    # -------------------------------------------------------------------------
    # Blocking consumer operation: FAA fast ticket allocation
    # -------------------------------------------------------------------------

    # Reserve one unique consumer ticket using fetch_add and wait until
    # the corresponding producer publishes the item.
    #
    # This operation should be used when the caller is willing to block
    # until an element is available.
    def pop(mut self) -> Self.T:
        var consumer_ticket = self.dequeue_pos.atomicVal.fetch_add[
            ordering=Ordering.RELAXED
        ](1)

        var cell_ptr = self.buffer + Int(consumer_ticket & self.mask)
        var expected_sequence = consumer_ticket + 1
        var spins = 0

        while True:
            var sequence = cell_ptr[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]()

            if sequence == expected_sequence:
                var item = cell_ptr[].data.take()

                # Make the cell available to the producer belonging to
                # the next traversal of the circular buffer.
                Atomic[DType.uint64].store[
                    ordering=Ordering.RELEASE
                ](
                    UnsafePointer(to=cell_ptr[].sequence.value),
                    consumer_ticket + self.size
                )

                return item^

            # Wait for the producer that owns this specific ticket.
            spins = self.wait_or_yield(spins)


    # -------------------------------------------------------------------------
    # Non-blocking consumer operation: CAS reservation
    # -------------------------------------------------------------------------

    # Try once to remove the current item.
    #
    # Returns None when:
    # - the queue currently has no published item at dequeue_pos; or
    # - another consumer wins the position concurrently.
    #
    # As for try_push, FAA cannot be used because an unsuccessful
    # reservation must not leave a permanent hole in dequeue_pos.
    def try_pop(mut self) -> Optional[Self.T]:
        var consumer_ticket = self.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()

        var cell_ptr = self.buffer + Int(consumer_ticket & self.mask)
        var expected_sequence = consumer_ticket + 1

        var sequence = cell_ptr[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]()

        if sequence != expected_sequence:
            return Optional[Self.T](None)

        var expected = consumer_ticket

        if not self.dequeue_pos.atomicVal.compare_exchange[
            failure_ordering=Ordering.RELAXED,
            success_ordering=Ordering.RELAXED
        ](
            expected,
            consumer_ticket + 1
        ):
            # Another consumer claimed this position.
            return Optional[Self.T](None)

        var item = cell_ptr[].data.take()

        Atomic[DType.uint64].store[
            ordering=Ordering.RELEASE
        ](
            UnsafePointer(to=cell_ptr[].sequence.value),
            consumer_ticket + self.size
        )

        return Optional(item^)


    # -------------------------------------------------------------------------
    # Optional diagnostic methods
    # -------------------------------------------------------------------------

    # These values are snapshots only. They must not be used as an exact
    # concurrent size because producers may reserve tickets before publishing
    # their elements.
    def producer_position(self) -> UInt64:
        return self.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()


    def consumer_position(self) -> UInt64:
        return self.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()


    # Snapshot estimate kept for API compatibility with the CAS queue.
    # Reserved but not yet published/consumed tickets make this approximate.
    def estimated_len(self) -> Int:
        var enqueue = self.producer_position()
        var dequeue = self.consumer_position()
        if enqueue <= dequeue:
            return 0
        var difference = enqueue - dequeue
        if difference > self.size:
            return Int(self.size)
        return Int(difference)
