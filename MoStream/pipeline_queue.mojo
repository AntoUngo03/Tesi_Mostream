# Compile-time-selectable queue used by pipeline communicators.
#
# The default keeps MoStream's original CAS-based MPMC queue.  Build an
# executable with `-DMOSTREAM_PADDED_FAA=1` or `-DMOSTREAM_SCQ=1` to select an
# experimental bounded data queue between pipeline stages.

from std.memory.alloc import unsafe_alloc
from std.collections import Optional
from std.sys import get_defined_bool
from MoStream.MPMC_queue import MPMCQueue
from MoStream.Padded_FAA_queue import PaddedFAAQueue
from MoStream.SCQ_queue import SCQQueue


comptime USE_PADDED_FAA = get_defined_bool[
    "MOSTREAM_PADDED_FAA", False
]()
comptime USE_SCQ = get_defined_bool["MOSTREAM_SCQ", False]()


struct PipelineQueue[T: Copyable & Deinitable](Movable):
    """Owning, zero-runtime-branch adapter for a pipeline data queue."""

    # Both implementations are heap allocated and represented by one opaque
    # pointer.  This keeps the adapter (and therefore Communicator) the same
    # size in both benchmark builds.
    var storage: Pointer[UInt8, MutUntrackedOrigin]

    def __init__(out self, size: Int = 1024) raises:
        comptime assert not (USE_PADDED_FAA and USE_SCQ), (
            "Select at most one MoStream pipeline queue backend"
        )
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
        comptime if USE_SCQ:
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
