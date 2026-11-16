# Queue selection applies only to inter-stage communication, not scheduler queues.
# Define MOSTREAM_RIGTORP at compile time to select Rigtorp; otherwise use Vyukov.
# Define MOSTREAM_SPSC to select the FastFlow SPSC queue (experimental): it is
# correct only on channels with exactly one producer and one consumer, and the
# Communicator rejects any other channel when this backend is selected.
from std.sys import is_defined
from std.collections import Optional
from MoStream.MPMC_queue import MPMCQueue
from MoStream.MPMC_queue_rigtorp import RigtorpMPMCQueue
from MoStream.SPSC_queue_fastflow import FastFlowSPSCQueue

comptime USE_RIGTORP = is_defined["MOSTREAM_RIGTORP"]()
comptime USE_SPSC = is_defined["MOSTREAM_SPSC"]()
comptime COMMUNICATION_QUEUE_NAME = "spsc" if USE_SPSC else ("rigtorp" if USE_RIGTORP else "vyukov")

from std.memory import Pointer
from std.memory.alloc import unsafe_alloc

struct CommunicationQueue[T: Copyable & Deinitable](Movable):
    var rigtorp: Pointer[RigtorpMPMCQueue[Self.T], MutUntrackedOrigin]
    var vyukov: Pointer[MPMCQueue[Self.T], MutUntrackedOrigin]
    var spsc: Pointer[FastFlowSPSCQueue[Self.T], MutUntrackedOrigin]

    def __init__(out self, size: Int) raises:
        comptime assert not (USE_RIGTORP and USE_SPSC), "{MoStream} Assert: define only one of MOSTREAM_RIGTORP and MOSTREAM_SPSC!"
        self.rigtorp = Pointer[RigtorpMPMCQueue[Self.T], MutUntrackedOrigin].unsafe_dangling()
        self.vyukov = Pointer[MPMCQueue[Self.T], MutUntrackedOrigin].unsafe_dangling()
        self.spsc = Pointer[FastFlowSPSCQueue[Self.T], MutUntrackedOrigin].unsafe_dangling()
        comptime if USE_SPSC:
            var queue = FastFlowSPSCQueue[Self.T](size)
            self.spsc = unsafe_alloc[FastFlowSPSCQueue[Self.T]](1)
            self.spsc.unsafe_write(queue^)
        elif USE_RIGTORP:
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
        self.spsc = move.spsc

    def __deinit__(deinit self):
        comptime if USE_SPSC:
            self.spsc.unsafe_deinit_pointee()
            self.spsc.unsafe_free()
        elif USE_RIGTORP:
            self.rigtorp.unsafe_deinit_pointee()
            self.rigtorp.unsafe_free()
        else:
            self.vyukov.unsafe_deinit_pointee()
            self.vyukov.unsafe_free()

    def push(mut self, var item: Self.T):
        comptime if USE_SPSC:
            self.spsc[].push(item^)
        elif USE_RIGTORP:
            self.rigtorp[].push(item^)
        else:
            self.vyukov[].push(item^)

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        comptime if USE_SPSC:
            return self.spsc[].try_push(item^)
        elif USE_RIGTORP:
            return self.rigtorp[].try_push(item^)
        else:
            return self.vyukov[].try_push(item^)

    # pop bloccante del backend: Rigtorp prende un ticket con fetch_add e attende
    # il turno della propria cella; Vyukov ripete il proprio try_pop; la SPSC
    # attende che la propria cella sia piena.
    # Usato dal Communicator con EOS espliciti (runtime standard).
    def pop(mut self) -> Self.T:
        comptime if USE_SPSC:
            return self.spsc[].pop()
        elif USE_RIGTORP:
            return self.rigtorp[].pop()
        else:
            return self.vyukov[].pop()

    def try_pop(mut self) -> Optional[Self.T]:
        comptime if USE_SPSC:
            return self.spsc[].try_pop()
        elif USE_RIGTORP:
            return self.rigtorp[].try_pop()
        else:
            return self.vyukov[].try_pop()
