# Compile-time-selectable queue used by pipeline communicators.
#
# The default keeps MoStream's original CAS-based MPMC queue.  Build an
# executable with `-DMOSTREAM_PADDED_FAA=1`, `-DMOSTREAM_SCQ=1`, or
# `-DMOSTREAM_MICHAEL_SCOTT=1` / `-DMOSTREAM_NBLFQ=1` to select a data queue
# between pipeline stages.

from std.memory.alloc import unsafe_alloc
from std.collections import Optional
from std.sys import get_defined_bool
from MoStream.MPMC_queue import MPMCQueue
from MoStream.Padded_FAA_queue import PaddedFAAQueue
from MoStream.SCQ_queue import SCQQueue
from MoStream.Micheal_Scott import MichaelScottQueue
from MoStream.NBLFQ_queue import NBLFQQueue


comptime USE_PADDED_FAA = get_defined_bool[
    "MOSTREAM_PADDED_FAA", False
]()
comptime USE_NBLFQ = get_defined_bool["MOSTREAM_NBLFQ", False]()
comptime USE_MICHAEL_SCOTT = get_defined_bool["MOSTREAM_MICHAEL_SCOTT", False]()
comptime USE_SCQ = get_defined_bool["MOSTREAM_SCQ", False]()


struct PipelineQueue[T: Copyable & Deinitable](Movable):
    """Owning, zero-runtime-branch adapter for a pipeline data queue."""

    # All implementations are heap allocated and represented by one opaque
    # pointer.  This keeps the adapter (and therefore Communicator) the same
    # size across benchmark builds.
    var storage: Pointer[UInt8, MutUntrackedOrigin]

    def __init__(out self, size: Int = 1024) raises:
        comptime assert (
            Int(USE_PADDED_FAA) + Int(USE_SCQ) + Int(USE_MICHAEL_SCOTT)
            + Int(USE_NBLFQ)
        ) <= 1, (
            "Select at most one MoStream pipeline queue backend"
        )
        comptime if USE_NBLFQ:
            var queue = unsafe_alloc[NBLFQQueue[Self.T]](1)
            queue.unsafe_write(NBLFQQueue[Self.T](size=size))
            self.storage = rebind[
                Pointer[UInt8, MutUntrackedOrigin]
            ](queue)
        elif USE_MICHAEL_SCOTT:
            var queue = unsafe_alloc[MichaelScottQueue[Self.T]](1)
            queue.unsafe_write(MichaelScottQueue[Self.T](size=size))
            self.storage = rebind[
                Pointer[UInt8, MutUntrackedOrigin]
            ](queue)
        elif USE_SCQ:
            var queue = unsafe_alloc[SCQQueue[Self.T]](1)
            queue.unsafe_write(SCQQueue[Self.T](size=size))
            self.storage = rebind[
                Pointer[UInt8, MutUntrackedOrigin]
            ](queue)
        elif USE_PADDED_FAA:
            var queue = unsafe_alloc[PaddedFAAQueue[Self.T]](1)
            queue.unsafe_write(PaddedFAAQueue[Self.T](size=size))
            self.storage = rebind[
                Pointer[UInt8, MutUntrackedOrigin]
            ](queue)
        else:
            var queue = unsafe_alloc[MPMCQueue[Self.T]](1)
            queue.unsafe_write(MPMCQueue[Self.T](size=size))
            self.storage = rebind[
                Pointer[UInt8, MutUntrackedOrigin]
            ](queue)

    def __init__(out self, *, deinit move: Self):
        self.storage = move.storage

    def __deinit__(deinit self):
        comptime if USE_NBLFQ:
            var queue = rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)
            queue.unsafe_deinit_pointee()
            queue.unsafe_free()
        elif USE_MICHAEL_SCOTT:
            var queue = rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)
            queue.unsafe_deinit_pointee()
            queue.unsafe_free()
        elif USE_SCQ:
            var queue = rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)
            queue.unsafe_deinit_pointee()
            queue.unsafe_free()
        elif USE_PADDED_FAA:
            var queue = rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)
            queue.unsafe_deinit_pointee()
            queue.unsafe_free()
        else:
            var queue = rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)
            queue.unsafe_deinit_pointee()
            queue.unsafe_free()

    @always_inline
    def push(mut self, var item: Self.T):
        comptime if USE_NBLFQ:
            rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].push(item^)
        elif USE_MICHAEL_SCOTT:
            rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].push(item^)
        elif USE_SCQ:
            rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].push(item^)
        elif USE_PADDED_FAA:
            rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].push(item^)
        else:
            rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].push(item^)

    @always_inline
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        comptime if USE_NBLFQ:
            return rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_push(item^)
        elif USE_MICHAEL_SCOTT:
            return rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_push(item^)
        elif USE_SCQ:
            return rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_push(item^)
        elif USE_PADDED_FAA:
            return rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_push(item^)
        else:
            return rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_push(item^)

    @always_inline
    def pop(mut self) -> Self.T:
        comptime if USE_NBLFQ:
            return rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].pop()
        elif USE_MICHAEL_SCOTT:
            return rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].pop()
        elif USE_SCQ:
            return rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].pop()
        elif USE_PADDED_FAA:
            return rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].pop()
        else:
            return rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].pop()

    @always_inline
    def try_pop(mut self) -> Optional[Self.T]:
        comptime if USE_NBLFQ:
            return rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_pop()
        elif USE_MICHAEL_SCOTT:
            return rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_pop()
        elif USE_SCQ:
            return rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_pop()
        elif USE_PADDED_FAA:
            return rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_pop()
        else:
            return rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].try_pop()

    @always_inline
    def estimated_len(self) -> Int:
        comptime if USE_NBLFQ:
            return rebind[
                Pointer[NBLFQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].estimated_len()
        elif USE_MICHAEL_SCOTT:
            return rebind[
                Pointer[MichaelScottQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].estimated_len()
        elif USE_SCQ:
            return rebind[
                Pointer[SCQQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].estimated_len()
        elif USE_PADDED_FAA:
            return rebind[
                Pointer[PaddedFAAQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].estimated_len()
        else:
            return rebind[
                Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
            ](self.storage)[].estimated_len()
