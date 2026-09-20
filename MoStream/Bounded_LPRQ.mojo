# ===------------------------------------------------------------------------=== #
# Bounded, reusable queue inspired by the Portable Ring Queue (PRQ) cell
# protocol used by LPRQ.
#
# The published LPRQ is unbounded: a PRQ segment is permanently closed after
# contention makes producers lap its ring, then a new segment is linked.  In
# particular, PRQ's UNSAFE bit is permanent because the segment will never be
# reused after closing.
#
# This implementation deliberately keeps one fixed ring.  It retains:
#   * monotonically increasing logical tickets;
#   * a per-cell epoch/index;
#   * a ticket-unique token that replaces double-width CAS;
#   * separate WRITING and READING phases for generic Optional[T] payloads.
#
# It does NOT retain PRQ's skip/UNSAFE/close protocol.  Reusing an UNSAFE cell
# without segment generations is incorrect; keeping it permanent eventually
# causes livelock.  Instead, head and tail are CAS-gated and an operation waits
# for the exact turn of its cell.  The result is bounded and reusable, but it
# has head-of-line blocking and does not inherit LPRQ's lock-freedom proof.
# This is therefore named BoundedLPRQInspired rather than LPRQ.
#
# The try_* operations are weak single-attempt operations: under concurrency
# they may report temporary failure even when a later ticket has already been
# published.  The blocking operations retry those failures.  Moving or
# destroying the queue is supported only after all concurrent operations have
# quiesced.
# ===------------------------------------------------------------------------=== #

from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.info import size_of
from std.sys.terminate import exit
from std.time import sleep
from MoStream.utils import print_red_color


struct BoundedLPRQPaddedAtomicU64:
    comptime CACHE_LINE = 64
    comptime PAD = Self.CACHE_LINE - size_of[Atomic[DType.uint64]]()

    var value: Atomic[DType.uint64]
    var padding: Array[UInt8, Self.PAD]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)


# One atomic state word contains both a ticket-derived token and its phase.
# `data` is non-atomic and may be accessed only by the owner of WRITING or
# READING.  `index` is the logical producer ticket for which the cell is free,
# or ticket + capacity while that ticket's item is published.
struct BoundedLPRQCell[T: Copyable & Deinitable](Movable):
    comptime CACHE_LINE = 64
    comptime USED = (
        2 * size_of[Atomic[DType.uint64]]() + size_of[Optional[Self.T]]()
    )
    comptime PAD = (
        Self.CACHE_LINE - (Self.USED % Self.CACHE_LINE)
    ) % Self.CACHE_LINE

    var state: Atomic[DType.uint64]
    var index: Atomic[DType.uint64]
    var data: Optional[Self.T]
    var padding: Array[UInt8, Self.PAD]

    def __init__(out self, initial_index: UInt64):
        self.state = Atomic[DType.uint64](0)
        self.index = Atomic[DType.uint64](initial_index)
        self.data = Optional[Self.T](None)
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)

    def __init__(out self, *, deinit move: Self):
        var state = move.state.load[ordering=Ordering.RELAXED]()
        var index = move.index.load[ordering=Ordering.RELAXED]()
        self.state = Atomic[DType.uint64](state)
        self.index = Atomic[DType.uint64](index)
        self.data = move.data^
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)


struct BoundedLPRQInspired[T: Copyable & Deinitable](Movable):
    comptime CellPointer = Pointer[
        BoundedLPRQCell[Self.T], MutUntrackedOrigin
    ]

    comptime EMPTY: UInt64 = 0
    comptime RESERVED_PHASE: UInt64 = 1
    comptime WRITING_PHASE: UInt64 = 2
    comptime FULL_PHASE: UInt64 = 3
    comptime READING_PHASE: UInt64 = 4
    comptime PHASE_MASK: UInt64 = 7
    comptime CACHE_LINE = 64
    comptime SPINS_BEFORE_YIELD = 1024

    var cells: Self.CellPointer
    var capacity: UInt64
    var mask: UInt64
    var head: BoundedLPRQPaddedAtomicU64
    var tail: BoundedLPRQPaddedAtomicU64

    def __init__(out self, capacity: Int = 1024):
        comptime assert (
            size_of[BoundedLPRQCell[Self.T]]() % Self.CACHE_LINE == 0
        ), "BoundedLPRQ cell size must be a cache-line multiple"
        comptime assert (
            size_of[BoundedLPRQPaddedAtomicU64]() == Self.CACHE_LINE
        ), "BoundedLPRQ counter wrapper must occupy one cache line"

        if not (capacity >= 2 and (capacity & (capacity - 1)) == 0):
            print_red_color(
                "{MoStream} Error: BoundedLPRQ capacity must be "
                "a power of two and at least 2!"
            )
            exit(1)

        self.capacity = UInt64(capacity)
        self.mask = UInt64(capacity - 1)
        self.cells = unsafe_alloc[BoundedLPRQCell[Self.T]](capacity, alignment=64)
        self.head = BoundedLPRQPaddedAtomicU64(0)
        self.tail = BoundedLPRQPaddedAtomicU64(0)

        for i in range(capacity):
            (self.cells.unsafe_offset(i)).unsafe_write(
                BoundedLPRQCell[Self.T](UInt64(i))
            )

    def __init__(out self, *, deinit move: Self):
        var head = move.head.value.load[ordering=Ordering.RELAXED]()
        var tail = move.tail.value.load[ordering=Ordering.RELAXED]()
        self.cells = move.cells
        self.capacity = move.capacity
        self.mask = move.mask
        self.head = BoundedLPRQPaddedAtomicU64(head)
        self.tail = BoundedLPRQPaddedAtomicU64(tail)

    def __deinit__(deinit self):
        for i in range(Int(self.capacity)):
            (self.cells.unsafe_offset(i)).unsafe_deinit_pointee()
        self.cells.unsafe_free()

    @always_inline
    @staticmethod
    def phase(state: UInt64) -> UInt64:
        return state & Self.PHASE_MASK

    @always_inline
    @staticmethod
    def ticket_state(ticket: UInt64, phase: UInt64) -> UInt64:
        # Three bits are reserved for the phase, so state tokens must not wrap
        # 2^61.  This is far beyond the practical lifetime of this prototype.
        return ((ticket + 1) << 3) | phase

    @always_inline
    @staticmethod
    def wait_or_yield(spins: Int) -> Int:
        var next = spins + 1
        if next >= Self.SPINS_BEFORE_YIELD:
            sleep(0.0)
            return 0
        return next

    # Complete a producer ticket after tail has been reserved.  The pre-check
    # in try_push makes the cell immediately available in normal execution.
    # Waiting here is a defensive measure: a claimed ticket cannot be safely
    # abandoned or rolled back.
    @staticmethod
    def publish_claimed(
        cells: Self.CellPointer,
        capacity: UInt64,
        mask: UInt64,
        ticket: UInt64,
        var item: Self.T,
    ):
        var cell = cells.unsafe_offset(Int(ticket & mask))
        var reserved = Self.ticket_state(ticket, Self.RESERVED_PHASE)
        var spins = 0

        while True:
            var index = cell[].index.load[ordering=Ordering.SEQUENTIAL]()
            var state = cell[].state.load[ordering=Ordering.ACQUIRE]()
            if index == ticket and state == Self.EMPTY:
                var expected_state = Self.EMPTY
                if cell[].state.compare_exchange[
                    success_ordering=Ordering.SEQUENTIAL,
                    failure_ordering=Ordering.SEQUENTIAL,
                ](expected_state, reserved):
                    break
            spins = Self.wait_or_yield(spins)

        var expected_index = ticket
        while not cell[].index.compare_exchange[
            success_ordering=Ordering.SEQUENTIAL,
            failure_ordering=Ordering.SEQUENTIAL,
        ](expected_index, ticket + capacity):
            expected_index = ticket

        var writing = Self.ticket_state(ticket, Self.WRITING_PHASE)
        var expected_reserved = reserved
        while not cell[].state.compare_exchange[
            success_ordering=Ordering.SEQUENTIAL,
            failure_ordering=Ordering.SEQUENTIAL,
        ](expected_reserved, writing):
            expected_reserved = reserved

        cell[].data = Optional(item^)
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=cell[].state.value),
            Self.ticket_state(ticket, Self.FULL_PHASE),
        )

    # Weak single-attempt enqueue.  A failed try_push never consumes a tail
    # ticket, but it may return the item after losing a CAS even if capacity is
    # still available.  Once the tail CAS wins, the cell was already observed
    # EMPTY at the exact epoch, so publication is immediate apart from the
    # atomic protocol itself.
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var ticket = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
        var current_head = self.head.value.load[ordering=Ordering.SEQUENTIAL]()

        if current_head > ticket:
            return Optional(item^)
        if ticket - current_head >= self.capacity:
            return Optional(item^)

        var cell = self.cells.unsafe_offset(Int(ticket & self.mask))
        if (
            cell[].index.load[ordering=Ordering.SEQUENTIAL]() != ticket
            or cell[].state.load[ordering=Ordering.ACQUIRE]() != Self.EMPTY
        ):
            return Optional(item^)

        var expected_tail = ticket
        if not self.tail.value.compare_exchange[
            success_ordering=Ordering.SEQUENTIAL,
            failure_ordering=Ordering.SEQUENTIAL,
        ](expected_tail, ticket + 1):
            return Optional(item^)

        Self.publish_claimed(
            self.cells, self.capacity, self.mask, ticket, item^
        )
        return Optional[Self.T](None)

    def push(mut self, var item: Self.T):
        var pending = Optional(item^)
        var spins = 0
        while True:
            var result = self.try_push(pending.take())
            if not result:
                return
            pending = result^
            spins = Self.wait_or_yield(spins)

    # Weak single-attempt dequeue.  It advances head only after the exact FULL
    # state is visible.  A delayed producer or a lost head CAS can therefore
    # produce a temporary None even if a later ticket is already FULL; None is
    # not a strict linearizable proof that the abstract queue is empty.
    def try_pop(mut self) -> Optional[Self.T]:
        var ticket = self.head.value.load[ordering=Ordering.SEQUENTIAL]()
        var current_tail = self.tail.value.load[ordering=Ordering.SEQUENTIAL]()
        if ticket >= current_tail:
            return Optional[Self.T](None)

        var cell = self.cells.unsafe_offset(Int(ticket & self.mask))
        var expected_full = Self.ticket_state(ticket, Self.FULL_PHASE)
        if (
            cell[].index.load[ordering=Ordering.SEQUENTIAL]()
            != ticket + self.capacity
            or cell[].state.load[ordering=Ordering.ACQUIRE]() != expected_full
        ):
            return Optional[Self.T](None)

        var expected_head = ticket
        if not self.head.value.compare_exchange[
            success_ordering=Ordering.SEQUENTIAL,
            failure_ordering=Ordering.SEQUENTIAL,
        ](expected_head, ticket + 1):
            return Optional[Self.T](None)

        var reading = Self.ticket_state(ticket, Self.READING_PHASE)
        var claim_expected = expected_full
        while not cell[].state.compare_exchange[
            success_ordering=Ordering.SEQUENTIAL,
            failure_ordering=Ordering.SEQUENTIAL,
        ](claim_expected, reading):
            claim_expected = expected_full

        if not cell[].data:
            print_red_color(
                "{MoStream} Error: BoundedLPRQ FULL cell has no payload!"
            )
            exit(2)

        var item = cell[].data.take()
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=cell[].state.value), Self.EMPTY
        )
        return Optional(item^)

    def pop(mut self) -> Self.T:
        var spins = 0
        while True:
            var item = self.try_pop()
            if item:
                return item.take()
            spins = Self.wait_or_yield(spins)

    def estimated_len(self) -> Int:
        var head = self.head.value.load[ordering=Ordering.RELAXED]()
        var tail = self.tail.value.load[ordering=Ordering.RELAXED]()
        if tail <= head:
            return 0
        var difference = tail - head
        if difference > self.capacity:
            return Int(self.capacity)
        return Int(difference)

    # Post-run diagnostics used by the correctness-checking benchmark.
    def debug_head_ticket(self) -> UInt64:
        return self.head.value.load[ordering=Ordering.RELAXED]()

    def debug_tail_ticket(self) -> UInt64:
        return self.tail.value.load[ordering=Ordering.RELAXED]()

    def debug_full_cells(self) -> Int:
        var full = 0
        for i in range(Int(self.capacity)):
            var state = (self.cells.unsafe_offset(i))[].state.load[
                ordering=Ordering.RELAXED
            ]()
            if Self.phase(state) == Self.FULL_PHASE:
                full += 1
        return full

    def debug_dump_cells(self):
        for i in range(Int(self.capacity)):
            var cell = self.cells.unsafe_offset(i)
            var state = cell[].state.load[ordering=Ordering.RELAXED]()
            print(
                "    cell/index/state/phase/state_ticket:",
                i,
                cell[].index.load[ordering=Ordering.RELAXED](),
                state,
                Self.phase(state),
                (state >> 3) - 1 if state != Self.EMPTY else -1,
            )
