# ===------------------------------------------------------------------------=== #
# Hybrid bounded MPMC queue for MoStream.
#
# Blocking push/pop use CAS as the low-contention fast path. After a bounded
# number of actual CAS collisions, the operation falls back to FAA ticket
# allocation. A full/empty slot is not considered contention and therefore
# does not trigger FAA by itself.
#
# Non-blocking try_push/try_pop always use CAS: an FAA ticket cannot safely be
# cancelled when an immediate operation cannot complete.
# ===------------------------------------------------------------------------=== #

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.info import size_of
from std.sys.terminate import exit
from std.time import sleep
from MoStream.utils import print_red_color


struct PaddedAtomicU64:
    comptime CACHE_LINE_SIZE_BYTES = 64
    comptime ATOMIC_SIZE_BYTES = size_of[Atomic[DType.uint64]]()
    comptime PAD_BYTES = Self.CACHE_LINE_SIZE_BYTES - Self.ATOMIC_SIZE_BYTES

    var atomicVal: Atomic[DType.uint64]
    var pad: InlineArray[UInt8, Self.PAD_BYTES]

    def __init__(out self, initial: UInt64):
        self.atomicVal = Atomic[DType.uint64](initial)
        self.pad = InlineArray[UInt8, Self.PAD_BYTES](uninitialized=True)


struct Cell[T: Copyable](Movable):
    var sequence: Atomic[DType.uint64]
    var data: Optional[Self.T]

    def __init__(out self, sequence: UInt64):
        self.sequence = Atomic[DType.uint64](sequence)
        self.data = Optional[Self.T](None)

    def __init__(out self, *, deinit take: Self):
        var sequence = take.sequence.load[ordering=Ordering.RELAXED]()
        self.sequence = Atomic[DType.uint64](sequence)
        self.data = take.data^


struct HybridMPMCQueue[T: Copyable, cas_failures_before_faa: Int = 4](Movable):
    comptime CellPointer = UnsafePointer[Cell[Self.T], MutExternalOrigin]

    # FAA is used only after this many CAS collisions in one blocking call.
    comptime CAS_FAILURES_BEFORE_FAA = Self.cas_failures_before_faa
    comptime SPINS_BEFORE_YIELD = 1024
    # Enable only for diagnostic runs: the extra shared RMW perturbs throughput.
    comptime ENABLE_FALLBACK_DIAGNOSTICS = False

    var buffer: Self.CellPointer
    var size: UInt64
    var mask: UInt64
    var enqueue_pos: PaddedAtomicU64
    var dequeue_pos: PaddedAtomicU64

    # Diagnostics are deliberately not updated on every operation. Only the
    # uncommon fallback path touches these shared counters.
    var enqueue_faa_fallbacks: PaddedAtomicU64
    var dequeue_faa_fallbacks: PaddedAtomicU64


    def __init__(out self, size: Int = 1024):
        comptime assert Self.CAS_FAILURES_BEFORE_FAA >= 1
        if not ((size >= 2) and ((size & (size - 1)) == 0)):
            print_red_color(
                "{MoStream} Error: hybrid MPMC queue size must be "
                "a power of two and at least 2!"
            )
            exit(1)

        self.size = UInt64(size)
        self.mask = UInt64(size - 1)
        self.buffer = alloc[Cell[Self.T]](size)
        self.enqueue_pos = PaddedAtomicU64(0)
        self.dequeue_pos = PaddedAtomicU64(0)
        self.enqueue_faa_fallbacks = PaddedAtomicU64(0)
        self.dequeue_faa_fallbacks = PaddedAtomicU64(0)

        for i in range(size):
            (self.buffer + i).init_pointee_move(Cell[Self.T](UInt64(i)))


    def __init__(out self, *, deinit take: Self):
        var enqueue = take.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var dequeue = take.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var enqueue_fallbacks = take.enqueue_faa_fallbacks.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var dequeue_fallbacks = take.dequeue_faa_fallbacks.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()

        self.buffer = take.buffer
        self.size = take.size
        self.mask = take.mask
        self.enqueue_pos = PaddedAtomicU64(enqueue)
        self.dequeue_pos = PaddedAtomicU64(dequeue)
        self.enqueue_faa_fallbacks = PaddedAtomicU64(enqueue_fallbacks)
        self.dequeue_faa_fallbacks = PaddedAtomicU64(dequeue_fallbacks)


    def __del__(deinit self):
        for i in range(Int(self.size)):
            (self.buffer + i).destroy_pointee()
        self.buffer.free()


    @always_inline
    def wait_or_yield(self, spins: Int) -> Int:
        var new_spins = spins + 1
        if new_spins >= Self.SPINS_BEFORE_YIELD:
            sleep(0.0)
            return 0
        return new_spins


    @always_inline
    def publish(mut self, ticket: UInt64, var item: Self.T):
        var cell_ptr = self.buffer + Int(ticket & self.mask)
        cell_ptr[].data = Optional(item^)
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=cell_ptr[].sequence.value),
            ticket + 1,
        )


    @always_inline
    def release_cell(mut self, ticket: UInt64) -> Self.T:
        var cell_ptr = self.buffer + Int(ticket & self.mask)
        var item = cell_ptr[].data.take()
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=cell_ptr[].sequence.value),
            ticket + self.size,
        )
        return item^


    # CAS fast path; FAA fallback only after genuine producer collisions.
    def push(mut self, var item: Self.T):
        var cas_failures = 0
        var spins = 0

        while True:
            var ticket = self.enqueue_pos.atomicVal.load[
                ordering=Ordering.RELAXED
            ]()
            var cell_ptr = self.buffer + Int(ticket & self.mask)
            var sequence = cell_ptr[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]()

            if sequence != ticket:
                # Queue full, or the current position is reserved but has not
                # been published. This is availability, not a CAS collision.
                spins = self.wait_or_yield(spins)
                continue

            var expected = ticket
            if self.enqueue_pos.atomicVal.compare_exchange[
                failure_ordering=Ordering.RELAXED,
                success_ordering=Ordering.RELAXED,
            ](expected, ticket + 1):
                self.publish(ticket, item^)
                return

            cas_failures += 1
            if cas_failures < Self.CAS_FAILURES_BEFORE_FAA:
                continue

            comptime if Self.ENABLE_FALLBACK_DIAGNOSTICS:
                _ = self.enqueue_faa_fallbacks.atomicVal.fetch_add[
                    ordering=Ordering.RELAXED
                ](1)
            var faa_ticket = self.enqueue_pos.atomicVal.fetch_add[
                ordering=Ordering.RELAXED
            ](1)
            var faa_cell = self.buffer + Int(faa_ticket & self.mask)
            spins = 0
            while faa_cell[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]() != faa_ticket:
                spins = self.wait_or_yield(spins)
            self.publish(faa_ticket, item^)
            return


    # The operation either reserves immediately with CAS or leaves no ticket.
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var ticket = self.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var cell_ptr = self.buffer + Int(ticket & self.mask)
        if cell_ptr[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != ticket:
            return Optional(item^)

        var expected = ticket
        if not self.enqueue_pos.atomicVal.compare_exchange[
            failure_ordering=Ordering.RELAXED,
            success_ordering=Ordering.RELAXED,
        ](expected, ticket + 1):
            return Optional(item^)

        self.publish(ticket, item^)
        return Optional[Self.T](None)


    # CAS fast path; FAA fallback only after genuine consumer collisions.
    def pop(mut self) -> Self.T:
        var cas_failures = 0
        var spins = 0

        while True:
            var ticket = self.dequeue_pos.atomicVal.load[
                ordering=Ordering.RELAXED
            ]()
            var cell_ptr = self.buffer + Int(ticket & self.mask)
            var expected_sequence = ticket + 1
            var sequence = cell_ptr[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]()

            if sequence != expected_sequence:
                # Empty/unpublished is not treated as consumer contention.
                spins = self.wait_or_yield(spins)
                continue

            var expected = ticket
            if self.dequeue_pos.atomicVal.compare_exchange[
                failure_ordering=Ordering.RELAXED,
                success_ordering=Ordering.RELAXED,
            ](expected, ticket + 1):
                return self.release_cell(ticket)

            cas_failures += 1
            if cas_failures < Self.CAS_FAILURES_BEFORE_FAA:
                continue

            comptime if Self.ENABLE_FALLBACK_DIAGNOSTICS:
                _ = self.dequeue_faa_fallbacks.atomicVal.fetch_add[
                    ordering=Ordering.RELAXED
                ](1)
            var faa_ticket = self.dequeue_pos.atomicVal.fetch_add[
                ordering=Ordering.RELAXED
            ](1)
            var faa_cell = self.buffer + Int(faa_ticket & self.mask)
            var faa_expected_sequence = faa_ticket + 1
            spins = 0
            while faa_cell[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]() != faa_expected_sequence:
                spins = self.wait_or_yield(spins)
            return self.release_cell(faa_ticket)


    def try_pop(mut self) -> Optional[Self.T]:
        var ticket = self.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var cell_ptr = self.buffer + Int(ticket & self.mask)
        if cell_ptr[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != ticket + 1:
            return Optional[Self.T](None)

        var expected = ticket
        if not self.dequeue_pos.atomicVal.compare_exchange[
            failure_ordering=Ordering.RELAXED,
            success_ordering=Ordering.RELAXED,
        ](expected, ticket + 1):
            return Optional[Self.T](None)

        return Optional(self.release_cell(ticket))


    def estimated_len(self) -> Int:
        var enqueue = self.enqueue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        var dequeue = self.dequeue_pos.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
        if enqueue <= dequeue:
            return 0
        var difference = enqueue - dequeue
        if difference > self.size:
            return Int(self.size)
        return Int(difference)


    def enqueue_fallback_count(self) -> UInt64:
        return self.enqueue_faa_fallbacks.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()


    def dequeue_fallback_count(self) -> UInt64:
        return self.dequeue_faa_fallbacks.atomicVal.load[
            ordering=Ordering.RELAXED
        ]()
