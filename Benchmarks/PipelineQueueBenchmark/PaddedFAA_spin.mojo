# ===------------------------------------------------------------------------=== #
# Experimental padded FAA bounded MPMC queue - PURE SPIN variant.
#
# This version is intentionally identical in queue semantics to PaddedFAAQueue,
# except for the waiting policy used by blocking push()/pop():
#
#   - no sleep(0.0)
#   - no yield/backoff threshold
#   - pure busy-waiting on the per-slot sequence number
#
# try_push() and try_pop() are intentionally unchanged in behavior so that the
# experiment isolates the effect of sleep/yield in the blocking runtime.
# ===------------------------------------------------------------------------=== #

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys.info import size_of
from std.sys.terminate import exit
from MoStream.utils import print_red_color
from std.runtime.asyncrt import TaskGroup
from std.sys import argv
from std.time import perf_counter_ns
from MoStream.Padded_FAA_queue import PaddedFAAQueue
from std.memory.alloc import unsafe_alloc


struct XorShift64:
    var state: UInt64

    def __init__(out self, seed: UInt64):
        self.state = seed if seed != 0 else UInt64(0x9E3779B97F4A7C15)

    @always_inline
    def next_u64(mut self) -> UInt64:
        var x = self.state
        x = x ^ (x << 13)
        x = x ^ (x >> 7)
        x = x ^ (x << 17)
        self.state = x
        return x


struct PaddedFAASpinAtomicU64:
    comptime CACHE_LINE = 64
    comptime PAD = Self.CACHE_LINE - size_of[Atomic[DType.uint64]]()

    var value: Atomic[DType.uint64]
    var padding: InlineArray[UInt8, Self.PAD]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)


struct PaddedFAASpinSlot[T: Copyable & Deinitable](Movable):
    comptime CACHE_LINE = 64

    comptime USED = (
        size_of[Atomic[DType.uint64]]() + size_of[Optional[Self.T]]()
    )

    comptime PAD = (
        Self.CACHE_LINE - (Self.USED % Self.CACHE_LINE)
    ) % Self.CACHE_LINE

    var sequence: Atomic[DType.uint64]
    var data: Optional[Self.T]
    var padding: InlineArray[UInt8, Self.PAD]

    def __init__(out self, sequence: UInt64):
        self.sequence = Atomic[DType.uint64](sequence)
        self.data = Optional[Self.T](None)
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)

    def __init__(out self, *, deinit move: Self):
        var sequence = move.sequence.load[ordering=Ordering.RELAXED]()
        self.sequence = Atomic[DType.uint64](sequence)
        self.data = move.data^
        self.padding = InlineArray[UInt8, Self.PAD](uninitialized=True)


struct PaddedFAASpinQueue[T: Copyable & Deinitable](Movable):
    comptime SlotPointer = UnsafePointer[
        PaddedFAASpinSlot[Self.T], MutExternalOrigin
    ]
    var slots: Self.SlotPointer
    var size: UInt64
    var mask: UInt64
    var enqueue_pos: PaddedFAASpinAtomicU64
    var dequeue_pos: PaddedFAASpinAtomicU64

    def __init__(out self, size: Int = 1024):
        if not ((size >= 2) and ((size & (size - 1)) == 0)):
            print_red_color(
                "{MoStream} Error: padded FAA spin queue size must be "
                "a power of two and at least 2!"
            )
            exit(1)

        self.size = UInt64(size)
        self.mask = UInt64(size - 1)

        self.slots = alloc[PaddedFAASpinSlot[Self.T]](
            size, alignment=64
        )

        self.enqueue_pos = PaddedFAASpinAtomicU64(0)
        self.dequeue_pos = PaddedFAASpinAtomicU64(0)

        for i in range(size):
            (self.slots + i).init_pointee_move(
                PaddedFAASpinSlot[Self.T](UInt64(i))
            )

    def __init__(out self, *, deinit take: Self):
        var enqueue = take.enqueue_pos.value.load[
            ordering=Ordering.RELAXED
        ]()
        var dequeue = take.dequeue_pos.value.load[
            ordering=Ordering.RELAXED
        ]()

        self.slots = take.slots
        self.size = take.size
        self.mask = take.mask
        self.enqueue_pos = PaddedFAASpinAtomicU64(enqueue)
        self.dequeue_pos = PaddedFAASpinAtomicU64(dequeue)

    def __del__(deinit self):
        for i in range(Int(self.size)):
            (self.slots + i).destroy_pointee()

        self.slots.free()

    @always_inline
    def wait_or_yield(self, spins: Int) -> Int:
        var next = spins + 1
        if next >= 1024:
            return 0
        return next

    def push(mut self, var item: Self.T):
        var ticket = self.enqueue_pos.value.fetch_add[
            ordering=Ordering.RELAXED
        ](1)

        var slot = self.slots + Int(ticket & self.mask)

        while slot[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != ticket:
            pass

        slot[].data = Optional(item^)

        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value),
            ticket + 1,
        )

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var ticket = self.enqueue_pos.value.load[
            ordering=Ordering.RELAXED
        ]()

        var slot = self.slots + Int(ticket & self.mask)

        if slot[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != ticket:
            return Optional(item^)

        var expected = ticket

        if not self.enqueue_pos.value.compare_exchange[
            success_ordering=Ordering.RELAXED,
            failure_ordering=Ordering.RELAXED,
        ](expected, ticket + 1):
            return Optional(item^)

        slot[].data = Optional(item^)

        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value),
            ticket + 1,
        )

        return Optional[Self.T](None)

    def pop(mut self) -> Self.T:
        var ticket = self.dequeue_pos.value.fetch_add[
            ordering=Ordering.RELAXED
        ](1)

        var slot = self.slots + Int(ticket & self.mask)
        var expected_sequence = ticket + 1

        while slot[].sequence.load[
            ordering=Ordering.ACQUIRE
        ]() != expected_sequence:
            pass

        var item = slot[].data.take()

        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
            UnsafePointer(to=slot[].sequence.value),
            ticket + self.size,
        )

        return item^

    def try_pop(mut self) -> Optional[Self.T]:
        while True:
            var ticket = self.dequeue_pos.value.load[
                ordering=Ordering.RELAXED
            ]()

            var slot = self.slots + Int(ticket & self.mask)

            if slot[].sequence.load[
                ordering=Ordering.ACQUIRE
            ]() != ticket + 1:
                if self.dequeue_pos.value.load[
                    ordering=Ordering.RELAXED
                ]() != ticket:
                    continue

                return Optional[Self.T](None)

            var expected = ticket

            if not self.dequeue_pos.value.compare_exchange[
                success_ordering=Ordering.RELAXED,
                failure_ordering=Ordering.RELAXED,
            ](expected, ticket + 1):
                continue

            var item = slot[].data.take()

            Atomic[DType.uint64].store[ordering=Ordering.RELEASE](
                UnsafePointer(to=slot[].sequence.value),
                ticket + self.size,
            )

            return Optional(item^)

    def estimated_len(self) -> Int:
        var enqueue = self.enqueue_pos.value.load[
            ordering=Ordering.RELAXED
        ]()
        var dequeue = self.dequeue_pos.value.load[
            ordering=Ordering.RELAXED
        ]()

        if enqueue <= dequeue:
            return 0

        var difference = enqueue - dequeue

        if difference > self.size:
            return Int(self.size)

        return Int(difference)


def shuffle_list(mut values: List[Int], seed: UInt64):
    var rng = XorShift64(seed)
    for i in range(len(values) - 1, 0, -1):
        var j = Int(rng.next_u64() % UInt64(i + 1))
        var tmp = values[i]
        values[i] = values[j]
        values[j] = tmp


def print_queue_result(
    label: String,
    elapsed_ns: Int,
    expected_count: UInt64,
    actual_count: UInt64,
    actual_checksum: UInt64,
):
    var seconds = Float64(elapsed_ns) / 1_000_000_000.0
    var throughput = Float64(expected_count) / seconds / 1_000_000.0
    var valid = actual_count == expected_count and actual_checksum == expected_count * (expected_count - 1) // 2
    print(
        label,
        "time_ms=",
        Float64(elapsed_ns) / 1_000_000.0,
        "throughput_Mmsg_s=",
        throughput,
        "valid=",
        valid,
    )


def run_padded_faa_benchmark(
    messages: Int,
    producers: Int,
    consumers: Int,
    capacity: Int,
    seed: UInt64,
) raises -> Tuple[Float64, UInt64, UInt64]:
    var queue = PaddedFAAQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0

    var order = List[Int]()
    for i in range(producers + consumers):
        order.append(i)
    shuffle_list(order, seed)

    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in order:
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue

    var elapsed_ns = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]

    counts.unsafe_free()
    checksums.unsafe_free()
    return (Float64(elapsed_ns) / 1_000_000.0, total, actual_count)


def run_padded_faa_spin_benchmark(
    messages: Int,
    producers: Int,
    consumers: Int,
    capacity: Int,
    seed: UInt64,
) raises -> Tuple[Float64, UInt64, UInt64]:
    var queue = PaddedFAASpinQueue[Int](capacity)
    var finished = Atomic[DType.uint64](0)
    var counts = unsafe_alloc[UInt64](consumers)
    var checksums = unsafe_alloc[UInt64](consumers)
    for i in range(consumers):
        counts[unsafe_offset=i] = 0
        checksums[unsafe_offset=i] = 0

    var order = List[Int]()
    for i in range(producers + consumers):
        order.append(i)
    shuffle_list(order, seed ^ UInt64(0xA5A5A5A5A5A5A5A5))

    var start = perf_counter_ns()

    @parameter
    async def worker(index: Int):
        if index < producers:
            var base = index * messages
            for i in range(messages):
                queue.push(base + i)
            var previous = finished.fetch_add[
                ordering=Ordering.ACQUIRE_RELEASE
            ](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    queue.push(-1)
        else:
            var consumer_id = index - producers
            var local_count: UInt64 = 0
            var local_checksum: UInt64 = 0
            while True:
                var value = queue.pop()
                if value == -1:
                    counts[unsafe_offset=consumer_id] = local_count
                    checksums[unsafe_offset=consumer_id] = local_checksum
                    return
                local_count += 1
                local_checksum += UInt64(value)

    var tasks = TaskGroup()
    for worker_id in order:
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue

    var elapsed_ns = perf_counter_ns() - start
    var total = UInt64(messages * producers)
    var actual_count: UInt64 = 0
    var actual_checksum: UInt64 = 0
    for i in range(consumers):
        actual_count += counts[unsafe_offset=i]
        actual_checksum += checksums[unsafe_offset=i]

    counts.unsafe_free()
    checksums.unsafe_free()
    return (Float64(elapsed_ns) / 1_000_000.0, total, actual_count)


def main() raises:
    var args = argv()
    var messages = 50_000
    var producers = 4
    var consumers = 4
    var capacity = 1024
    var seed = UInt64(0xC0FFEE123456789)

    if len(args) >= 2:
        messages = Int(args[1])
    if len(args) >= 3:
        producers = Int(args[2])
    if len(args) >= 4:
        consumers = Int(args[3])
    if len(args) >= 5:
        capacity = Int(args[4])
    if len(args) >= 6:
        seed = UInt64(Int(args[5]))

    if messages <= 0 or producers <= 0 or consumers <= 0 or capacity <= 0:
        print("Usage: PaddedFAA_spin <messages> <producers> <consumers> <capacity> [seed]")
        raise Error("invalid benchmark parameters")

    var baseline = run_padded_faa_benchmark(
        messages, producers, consumers, capacity, seed
    )
    var spin = run_padded_faa_spin_benchmark(
        messages, producers, consumers, capacity, seed
    )

    var baseline_ms = baseline[0]
    var spin_ms = spin[0]
    var expected = UInt64(messages * producers)

    print_queue_result(
        "PaddedFAAQueue",
        Int(baseline_ms * 1_000_000.0),
        expected,
        baseline[2],
        expected * (expected - 1) // 2,
    )
    print_queue_result(
        "PaddedFAASpinQueue",
        Int(spin_ms * 1_000_000.0),
        expected,
        spin[2],
        expected * (expected - 1) // 2,
    )

    var ratio = 1.0
    if baseline_ms > 0.0:
        ratio = spin_ms / baseline_ms
    var delta_pct = 0.0
    if baseline_ms > 0.0:
        delta_pct = ((spin_ms - baseline_ms) / baseline_ms) * 100.0

    print(
        "sleep0_cost_ratio=",
        ratio,
        " spin_vs_hybrid_pct=",
        delta_pct,
    )