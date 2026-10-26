# Queue selection applies only to inter-stage communication, not scheduler queues.
# Define MOSTREAM_RIGTORP at compile time to select Rigtorp; otherwise use Vyukov.
from std.sys import is_defined
from std.collections import Optional
from MoStream.MPMC_queue import MPMCQueue
from MoStream.MPMC_queue_rigtorp import RigtorpMPMCQueue

comptime USE_RIGTORP = is_defined["MOSTREAM_RIGTORP"]()
comptime COMMUNICATION_QUEUE_NAME = "rigtorp" if USE_RIGTORP else "vyukov"

from std.memory import Pointer
from std.memory.alloc import unsafe_alloc

struct CommunicationQueue[T: Copyable & Deinitable](Movable):
    var rigtorp: Pointer[RigtorpMPMCQueue[Self.T], MutUntrackedOrigin]
    var vyukov: Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]

    def __init__(out self, size: Int) raises:
        self.rigtorp = Pointer[RigtorpMPMCQueue[Self.T], MutUntrackedOrigin].unsafe_dangling()
        self.vyukov = Pointer[MPMCQueue[Self.T], MutUntrackedOrigin].unsafe_dangling()
        comptime if USE_RIGTORP:
            var queue = RigtorpMPMCQueue[Self.T](size)
            self.rigtorp = unsafe_alloc[RigtorpMPMCQueue[Self.T]](1)
            self.rigtorp.unsafe_write(queue^)
        else:
            var queue = MPMCQueue[Self.T](size)
            self.vyukov = unsafe_alloc[MPMCQueue[Self.T]](1)
            self.vyukov.unsafe_write(queue^)

    def __init__(out self, *, deinit move: Self):
        self.rigtorp = move.rigtorp
        self.vyukov = move.vyukov

    def __deinit__(deinit self):
        comptime if USE_RIGTORP:
            self.rigtorp.unsafe_deinit_pointee()
            self.rigtorp.unsafe_free()
        else:
            self.vyukov.unsafe_deinit_pointee()
            self.vyukov.unsafe_free()

    def push(mut self, var item: Self.T):
        comptime if USE_RIGTORP:
            self.rigtorp[].push(item^)
        else:
            self.vyukov[].push(item^)

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        comptime if USE_RIGTORP:
            return self.rigtorp[].try_push(item^)
        else:
            return self.vyukov[].try_push(item^)

    def try_pop(mut self) -> Optional[Self.T]:
        comptime if USE_RIGTORP:
            return self.rigtorp[].try_pop()
        else:
            return self.vyukov[].try_pop()
