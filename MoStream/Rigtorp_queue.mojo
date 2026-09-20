# Mojo port of Erik Rigtorp's bounded MPMCQueue turn-based algorithm.
# Original: https://github.com/rigtorp/MPMCQueue (MIT license).

from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.info import size_of
from std.sys.terminate import exit
from MoStream.utils import print_red_color


struct PaddedAtomicU64:
    comptime CACHE_LINE = 64
    comptime PAD = Self.CACHE_LINE - size_of[Atomic[DType.uint64]]()
    var value: Atomic[DType.uint64]
    var padding: Array[UInt8, Self.PAD]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)


# Rigtorp isolates adjacent slot turn counters on different cache lines.
struct RigtorpSlot[T: Copyable & Deinitable](Movable):
    comptime CACHE_LINE = 64
    comptime USED = (
        size_of[Atomic[DType.uint64]]() + size_of[Optional[Self.T]]()
    )
    comptime PAD = (Self.CACHE_LINE - (Self.USED % Self.CACHE_LINE)) % Self.CACHE_LINE

    var turn: Atomic[DType.uint64]
    var data: Optional[Self.T]
    var padding: Array[UInt8, Self.PAD]

    def __init__(out self):
        self.turn = Atomic[DType.uint64](0)
        self.data = Optional[Self.T](None)
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)

    def __init__(out self, *, deinit move: Self):
        var turn = move.turn.load[ordering=Ordering.RELAXED]()
        self.turn = Atomic[DType.uint64](turn)
        self.data = move.data^
        self.padding = Array[UInt8, Self.PAD](uninitialized=True)


struct RigtorpMPMCQueue[T: Copyable & Deinitable](Movable):
    comptime SlotPointer = Pointer[
        RigtorpSlot[Self.T], MutUntrackedOrigin
    ]

    var raw_buffer: Self.SlotPointer
    var slots: Self.SlotPointer
    var capacity: UInt64
    var head: PaddedAtomicU64
    var tail: PaddedAtomicU64


    def __init__(out self, capacity: Int = 1024):
        if capacity < 1:
            print_red_color(
                "{MoStream} Error: Rigtorp MPMC capacity must be at least 1!"
            )
            exit(1)

        self.capacity = UInt64(capacity)
        self.raw_buffer = unsafe_alloc[RigtorpSlot[Self.T]](
            capacity, alignment=64
        )
        self.slots = self.raw_buffer
        self.head = PaddedAtomicU64(0)
        self.tail = PaddedAtomicU64(0)
        for i in range(capacity):
            (self.slots.unsafe_offset(i)).unsafe_write(RigtorpSlot[Self.T]())


    def __init__(out self, *, deinit move: Self):
        var head = move.head.value.load[ordering=Ordering.RELAXED]()
        var tail = move.tail.value.load[ordering=Ordering.RELAXED]()
        self.raw_buffer = move.raw_buffer
        self.slots = move.slots
        self.capacity = move.capacity
        self.head = PaddedAtomicU64(head)
        self.tail = PaddedAtomicU64(tail)


    def __deinit__(deinit self):
        for i in range(Int(self.capacity)):
            (self.slots.unsafe_offset(i)).unsafe_deinit_pointee()
        self.raw_buffer.unsafe_free()


    @always_inline
    def index(self, ticket: UInt64) -> Int:
        return Int(ticket % self.capacity)


    @always_inline
    def cycle(self, ticket: UInt64) -> UInt64:
        return ticket // self.capacity


    def push(mut self, var item: Self.T):
        # The original C++ implementation uses the default seq_cst fetch_add.
        var ticket = self.head.value.fetch_add[
            ordering=Ordering.SEQUENTIAL
        ](1)
        var slot = self.slots.unsafe_offset(self.index(ticket))
        var expected_turn = self.cycle(ticket) * 2
        while slot[].turn.load[
            ordering=Ordering.ACQUIRE
        ]() != expected_turn:
            pass
        slot[].data = Optional(item^)
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=slot[].turn.value), expected_turn + 1
        )


    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var ticket = self.head.value.load[ordering=Ordering.ACQUIRE]()
        while True:
            var slot = self.slots.unsafe_offset(self.index(ticket))
            var expected_turn = self.cycle(ticket) * 2
            if slot[].turn.load[
                ordering=Ordering.ACQUIRE
            ]() == expected_turn:
                var expected = ticket
                if self.head.value.compare_exchange[
                    success_ordering=Ordering.SEQUENTIAL,
                    failure_ordering=Ordering.ACQUIRE,
                ](expected, ticket + 1):
                    slot[].data = Optional(item^)
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
                        Pointer(to=slot[].turn.value), expected_turn + 1
                    )
                    return Optional[Self.T](None)
                ticket = expected
            else:
                var previous = ticket
                ticket = self.head.value.load[ordering=Ordering.ACQUIRE]()
                if ticket == previous:
                    return Optional(item^)


    def pop(mut self) -> Self.T:
        var ticket = self.tail.value.fetch_add[
            ordering=Ordering.SEQUENTIAL
        ](1)
        var slot = self.slots.unsafe_offset(self.index(ticket))
        var expected_turn = self.cycle(ticket) * 2 + 1
        while slot[].turn.load[
            ordering=Ordering.ACQUIRE
        ]() != expected_turn:
            pass
        var item = slot[].data.take()
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            Pointer(to=slot[].turn.value), expected_turn + 1
        )
        return item^


    def try_pop(mut self) -> Optional[Self.T]:
        var ticket = self.tail.value.load[ordering=Ordering.ACQUIRE]()
        while True:
            var slot = self.slots.unsafe_offset(self.index(ticket))
            var expected_turn = self.cycle(ticket) * 2 + 1
            if slot[].turn.load[
                ordering=Ordering.ACQUIRE
            ]() == expected_turn:
                var expected = ticket
                if self.tail.value.compare_exchange[
                    success_ordering=Ordering.SEQUENTIAL,
                    failure_ordering=Ordering.ACQUIRE,
                ](expected, ticket + 1):
                    var item = slot[].data.take()
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
                        Pointer(to=slot[].turn.value), expected_turn + 1
                    )
                    return Optional(item^)
                ticket = expected
            else:
                var previous = ticket
                ticket = self.tail.value.load[ordering=Ordering.ACQUIRE]()
                if ticket == previous:
                    return Optional[Self.T](None)


    def estimated_len(self) -> Int:
        var head = self.head.value.load[ordering=Ordering.RELAXED]()
        var tail = self.tail.value.load[ordering=Ordering.RELAXED]()
        return Int(head) - Int(tail)
