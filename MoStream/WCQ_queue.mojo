# ===------------------------------------------------------------------------=== #
# Native bounded wCQ adapter for 64-bit values.
#
# The queue core is Nikolaev and Ravindran's wCQ (SPAA 2022), vendored under
# MoStream/lib/wcq_vendor at upstream commit
# 708c0052872950dcb15b487fa7a5dd77ce2a2746.  The C adapter uses the pointer
# indirection scheme from the paper: one wCQ ring holds published indices and
# a second wCQ ring holds free indices.  A fixed uint64 payload array stores
# the actual values, so every uint64 value (including UInt64.MAX) is valid.
#
# Empty/full admission counters are important for a blocking bounded adapter.
# They prevent repeated probes of an empty ring from racing Head indefinitely
# ahead of Tail.  A try operation performs one CAS on its admission counter;
# CAS contention is reported as a transient would-block result.  push/pop are
# convenience spin operations and therefore are not themselves wait-free.
#
# Requirements:
#   * x86-64 with CMPXCHG16B;
#   * power-of-two capacity >= 8;
#   * one dense thread_id in [0, max_threads) per concurrent caller;
#   * no concurrent operation while moving or destroying the queue.
#
# Build clients with MoStream/lib/wcq_native.o (the benchmark runner and the
# library Makefile do this automatically).
# ===------------------------------------------------------------------------=== #

from std.collections import Optional
from std.ffi import c_int, c_ulong, external_call
from std.sys.terminate import exit
from MoStream.utils import print_red_color


struct WCQQueue(Movable):
    """Fixed-capacity MPMC wCQ adapter for UInt64 values."""

    comptime Handle = Pointer[UInt8, MutUntrackedOrigin]
    comptime SUCCESS = 1
    comptime WOULD_BLOCK = 0

    var handle: Self.Handle
    var queue_capacity: Int
    var max_threads: Int

    def __init__(
        out self, capacity: Int = 1024, max_threads: Int = 16
    ):
        if not (
            capacity >= 8
            and (capacity & (capacity - 1)) == 0
            and max_threads >= 1
            and max_threads <= capacity
        ):
            print_red_color(
                "{MoStream} Error: wCQ capacity must be a power of two "
                ">= 8 and 1 <= max_threads <= capacity!"
            )
            exit(1)

        self.handle = Self.Handle.unsafe_dangling()
        self.queue_capacity = capacity
        self.max_threads = max_threads
        var status = external_call["mostream_wcq_u64_create", c_int](
            c_ulong(capacity),
            c_ulong(max_threads),
            Pointer(to=self.handle),
        )
        if status != 0:
            print_red_color(
                "{MoStream} Error: native wCQ initialization failed "
                "(errno=" + String(status) + ")!"
            )
            exit(1)

    def __init__(out self, *, deinit move: Self):
        self.handle = move.handle
        self.queue_capacity = move.queue_capacity
        self.max_threads = move.max_threads

    def __deinit__(deinit self):
        external_call["mostream_wcq_u64_destroy", NoneType](self.handle)

    @always_inline
    def try_push(
        mut self, thread_id: Int, item: UInt64
    ) -> Optional[UInt64]:
        var status = external_call[
            "mostream_wcq_u64_try_enqueue", c_int
        ](self.handle, c_ulong(thread_id), c_ulong(item))
        if status == Self.SUCCESS:
            return Optional[UInt64](None)
        if status == Self.WOULD_BLOCK:
            return Optional(item)
        Self.fail_operation("try_push", thread_id)
        return Optional(item)

    @always_inline
    def push(mut self, thread_id: Int, item: UInt64):
        var status = external_call["mostream_wcq_u64_enqueue", c_int](
            self.handle, c_ulong(thread_id), c_ulong(item)
        )
        if status != Self.SUCCESS:
            Self.fail_operation("push", thread_id)

    @always_inline
    def try_pop(mut self, thread_id: Int) -> Optional[UInt64]:
        var value: UInt64 = 0
        var status = external_call[
            "mostream_wcq_u64_try_dequeue", c_int
        ](self.handle, c_ulong(thread_id), Pointer(to=value))
        if status == Self.SUCCESS:
            return Optional(value)
        if status == Self.WOULD_BLOCK:
            return Optional[UInt64](None)
        Self.fail_operation("try_pop", thread_id)
        return Optional[UInt64](None)

    @always_inline
    def pop(mut self, thread_id: Int) -> UInt64:
        var value: UInt64 = 0
        var status = external_call["mostream_wcq_u64_dequeue", c_int](
            self.handle, c_ulong(thread_id), Pointer(to=value)
        )
        if status != Self.SUCCESS:
            Self.fail_operation("pop", thread_id)
        return value

    @staticmethod
    def fail_operation(operation: String, thread_id: Int):
        print_red_color(
            "{MoStream} Error: native wCQ " + operation
            + " failed for thread_id=" + String(thread_id) + "!"
        )
        exit(2)
