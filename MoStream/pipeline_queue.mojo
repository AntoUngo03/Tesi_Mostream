# Compile-time-selectable queue used by pipeline communicators.
#
# The default keeps MoStream's original CAS-based MPMC queue.  Build an
# executable with `-DMOSTREAM_PADDED_FAA=1` to use PaddedFAAQueue for the
# bounded data queues between pipeline stages.

from std.collections import Optional
from std.sys import get_defined_bool
from MoStream.MPMC_queue import MPMCQueue
from MoStream.Padded_FAA_queue import PaddedFAAQueue


comptime USE_PADDED_FAA = get_defined_bool[
    "MOSTREAM_PADDED_FAA", False
]()


struct PipelineQueue[T: Copyable](Movable):
    """Owning, zero-runtime-branch adapter for a pipeline data queue."""

    # Both implementations are heap allocated and represented by one opaque
    # pointer.  This keeps the adapter (and therefore Communicator) the same
    # size in both benchmark builds.
    var storage: UnsafePointer[UInt8, MutExternalOrigin]

    def __init__(out self, size: Int = 1024) raises:
        comptime if USE_PADDED_FAA:
            var queue = alloc[PaddedFAAQueue[Self.T]](1)
            queue.init_pointee_move(PaddedFAAQueue[Self.T](size=size))
            self.storage = rebind[
                UnsafePointer[UInt8, MutExternalOrigin]
            ](queue)
        else:
            var queue = alloc[MPMCQueue[Self.T]](1)
            queue.init_pointee_move(MPMCQueue[Self.T](size=size))
            self.storage = rebind[
                UnsafePointer[UInt8, MutExternalOrigin]
            ](queue)

    def __init__(out self, *, deinit take: Self):
        self.storage = take.storage

    def __del__(deinit self):
        comptime if USE_PADDED_FAA:
            var queue = rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)
            queue.destroy_pointee()
            queue.free()
        else:
            var queue = rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)
            queue.destroy_pointee()
            queue.free()

    @always_inline
    def push(mut self, var item: Self.T):
        comptime if USE_PADDED_FAA:
            rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].push(item^)
        else:
            rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].push(item^)

    @always_inline
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        comptime if USE_PADDED_FAA:
            return rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].try_push(item^)
        else:
            return rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].try_push(item^)

    @always_inline
    def pop(mut self) -> Self.T:
        comptime if USE_PADDED_FAA:
            return rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].pop()
        else:
            return rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].pop()

    @always_inline
    def try_pop(mut self) -> Optional[Self.T]:
        comptime if USE_PADDED_FAA:
            return rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].try_pop()
        else:
            return rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].try_pop()

    @always_inline
    def estimated_len(self) -> Int:
        comptime if USE_PADDED_FAA:
            return rebind[
                UnsafePointer[PaddedFAAQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].estimated_len()
        else:
            return rebind[
                UnsafePointer[MPMCQueue[Self.T], MutExternalOrigin]
            ](self.storage)[].estimated_len()
