# ===------------------------------------------------------------------------=== #
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU Lesser General Public License version 3 as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but WITHOUT
#  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
#  License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# ===------------------------------------------------------------------------=== #

# Variante SPERIMENTALE del runtime standard con pop bloccante ed EOS espliciti.
#
# Il Communicator originale non inserisce l'EOS nella coda: ogni consumatore lo
# ricava da solo quando try_pop trova la coda vuota e il flag `closed` è alzato.
# Per questo Communicator.pop() è un ciclo di try_pop, e con Rigtorp si usa la
# prenotazione con CAS invece del ticket con fetch_add del pop bloccante.
#
# Qui il protocollo di chiusura cambia:
#   - quando l'ULTIMO produttore di un communicator termina, inserisce nella
#     coda esattamente `consNum` messaggi EOS, uno per consumatore;
#   - pop() chiama il pop bloccante della coda: Rigtorp prende un ticket con un
#     solo fetch_add e attende il turno della propria cella; Vyukov usa il suo
#     pop (ciclo sul proprio try_pop, algoritmo invariato);
#   - ogni consumatore si ferma al primo EOS ricevuto.
#
# Correttezza: ogni produttore completa i suoi push prima di producer_finished
# (fetch_sub ACQUIRE_RELEASE), quindi gli EOS occupano posizioni successive a
# tutti i dati. Le posizioni sono prenotate in ordine e ciascuna da un solo
# consumatore; poiché ogni consumatore si ferma dopo il proprio EOS, vengono
# consumate esattamente D + C posizioni (D dati, C consumatori): nessun dato
# perso, nessun ticket senza elemento, nessun consumatore bloccato.
#
# Uso: come Pipeline, ma con BlockingPipeline e solo run() (runtime standard).
#   from MoStream.pipeline_blocking import BlockingPipeline
# La coda si sceglie come sempre in compilazione (-D MOSTREAM_RIGTORP=1).
# Non supporta il runtime cooperativo (gli attori non possono bloccare un worker)
# né gli stadi TRANSFORM_MANY (l'Emitter è legato al Communicator originale).
# Pipeline, Communicator e i due runtime originali non sono modificati.

from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.os import getenv
from std.python import Python
from std.sys.terminate import exit
from MoStream.communicator import MessageTrait, MessageWrapper
from MoStream.communication_queue import COMMUNICATION_QUEUE_NAME, USE_RIGTORP
from MoStream.MPMC_queue import MPMCQueue
from MoStream.MPMC_queue_rigtorp import RigtorpMPMCQueue
from MoStream.node import NodeTrait
from MoStream.stage import StageKind, StageTrait
from MoStream.pipeline import CoresList, pin_thread_to_cpu
from MoStream.utils import print_cyan_color, print_red_color

# Coda fra due stadi con push e pop BLOCCANTI del backend scelto in compilazione.
struct BlockingQueue[T: Copyable & Deinitable](Movable):
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

    def pop(mut self) -> Self.T:
        comptime if USE_RIGTORP:
            return self.rigtorp[].pop()
        else:
            return self.vyukov[].pop()

# Communicator con EOS espliciti: nessun flag `closed`, nessun try_pop.
struct BlockingCommunicator[T: MessageTrait](Movable):
    var queue: Pointer[BlockingQueue[MessageWrapper[Self.T]], MutUntrackedOrigin]
    var consNum: Int
    var remainingProducers: Pointer[Atomic[Int64], MutUntrackedOrigin]
    var destroyCount: Pointer[Atomic[Int64], MutUntrackedOrigin]

    def __init__(out self, pN: Int, cN: Int, queue_size: Int) raises:
        self.queue = unsafe_alloc[BlockingQueue[MessageWrapper[Self.T]]](1)
        self.queue.unsafe_write(BlockingQueue[MessageWrapper[Self.T]](size=queue_size))
        self.consNum = cN
        self.remainingProducers = unsafe_alloc[Atomic[Int64]](1)
        self.remainingProducers[] = Atomic[Int64](Int64(pN))
        self.destroyCount = unsafe_alloc[Atomic[Int64]](1)
        self.destroyCount[] = Atomic[Int64](Int64(cN))

    def __init__(out self, *, deinit move: Self):
        self.queue = move.queue
        self.consNum = move.consNum
        self.remainingProducers = move.remainingProducers
        self.destroyCount = move.destroyCount

    def __deinit__(deinit self):
        self.queue.unsafe_deinit_pointee()
        self.queue.unsafe_free()
        self.remainingProducers.unsafe_deinit_pointee()
        self.remainingProducers.unsafe_free()
        self.destroyCount.unsafe_deinit_pointee()
        self.destroyCount.unsafe_free()

    def push(mut self, var msg: MessageWrapper[Self.T]):
        self.queue[].push(msg^)

    def pop(mut self) -> MessageWrapper[Self.T]:
        return self.queue[].pop()

    # L'ultimo produttore inserisce un EOS per ogni consumatore.
    def producer_finished(mut self):
        var old_count = self.remainingProducers[].fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)
        if old_count == Int64(1):
            for _ in range(self.consNum):
                self.queue[].push(MessageWrapper[Self.T](eos=True))

    # Vero per l'ultimo consumatore: a quel punto tutti hanno ricevuto il proprio EOS.
    def check_isDestroyable(mut self) -> Bool:
        var old_count = self.destroyCount[].fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)
        return old_count == Int64(1)

# Esecutori: stessi cicli del runtime standard, con il BlockingCommunicator.
def blocking_execute_source[Stage: StageTrait, In: MessageTrait, Out: MessageTrait]
        (mut s: Stage,
         inComm: Pointer[mut=True, BlockingCommunicator[In], MutUntrackedOrigin],
         outComm: Pointer[mut=True, BlockingCommunicator[Out], MutUntrackedOrigin]) raises:
    var end_of_stream = False
    while (not end_of_stream):
        var output = s.next_element()
        if output == None:
            end_of_stream = True
            outComm[].producer_finished()
            s.received_eos()
        else:
            outComm[].push(MessageWrapper[Out](data = rebind[Optional[Out]](output).take(), eos = False))

def blocking_execute_sink[Stage: StageTrait, In: MessageTrait, Out: MessageTrait]
        (mut s: Stage,
         inComm: Pointer[mut=True, BlockingCommunicator[In], MutUntrackedOrigin],
         outComm: Pointer[mut=True, BlockingCommunicator[Out], MutUntrackedOrigin]) raises:
    var end_of_stream = False
    while (not end_of_stream):
        var input = inComm[].pop()
        if input.eos:
            end_of_stream = True
            s.received_eos()
        else:
            s.consume_element(rebind[MessageWrapper[Stage.InType]](input).data.take())
    if (inComm[].check_isDestroyable()):
        inComm.unsafe_deinit_pointee()
        inComm.unsafe_free()

def blocking_execute_transform[Stage: StageTrait, In: MessageTrait, Out: MessageTrait]
        (mut s: Stage,
         inComm: Pointer[mut=True, BlockingCommunicator[In], MutUntrackedOrigin],
         outComm: Pointer[mut=True, BlockingCommunicator[Out], MutUntrackedOrigin]) raises:
    var end_of_stream = False
    while (not end_of_stream):
        var input = inComm[].pop()
        if input.eos:
            end_of_stream = True
            outComm[].producer_finished()
            s.received_eos()
        else:
            var output = s.compute(rebind[MessageWrapper[Stage.InType]](input).data.take())
            if (output != None):
                outComm[].push(MessageWrapper[Out](data = rebind[Optional[Out]](output).take(), eos = False))
    if (inComm[].check_isDestroyable()):
        inComm.unsafe_deinit_pointee()
        inComm.unsafe_free()

async def blocking_executor_task[NodeT: NodeTrait,
                 In: MessageTrait,
                 Out: MessageTrait, //,
                 idx: Int,
                 len: Int]
                 (mut node: NodeT,
                 inComm: Pointer[mut=True, BlockingCommunicator[In], MutUntrackedOrigin],
                 outComm: Pointer[mut=True, BlockingCommunicator[Out], MutUntrackedOrigin],
                 core_id: Int):
    try:
        var s = node.make_stage()
        _ = pin_thread_to_cpu(core_id)
        comptime if NodeT.StageT.kind == StageKind.SOURCE:
            blocking_execute_source[NodeT.StageT, In, Out](s, inComm, outComm)
        elif NodeT.StageT.kind == StageKind.SINK:
            blocking_execute_sink[NodeT.StageT, In, Out](s, inComm, outComm)
        elif NodeT.StageT.kind == StageKind.TRANSFORM:
            blocking_execute_transform[NodeT.StageT, In, Out](s, inComm, outComm)
        else:
            print_red_color("{MoStream} Error: stage " + String(NodeT.StageT.name) + " is not supported by BlockingPipeline (only SOURCE, TRANSFORM, SINK)!")
            raise Error("error in blocking_executor_task()")
    except e:
        print("Raised: " + String(e))
        exit(1)

# Pipeline con il protocollo bloccante; stessa interfaccia di Pipeline per run().
struct BlockingPipeline[*Ts: NodeTrait]:
    comptime N = len(Self.Ts)
    var nodes: Tuple[*Self.Ts]
    var queue_size: Int
    var coreslist: CoresList
    var alreadyRun: Bool

    def __init__(out self, var nodes: Tuple[*Self.Ts]) raises:
        comptime assert Self.N > 1, "{MoStream} Assert: pipeline must have at least 2 stages!"
        comptime for i in range(Self.N):
            comptime assert Self.Ts[i].StageT.kind != StageKind.SOURCE or i == 0, "{MoStream} Assert: source stage must be the first stage of the pipeline!"
            comptime assert Self.Ts[i].StageT.kind != StageKind.SINK or i == Self.N - 1, "{MoStream} Assert: sink stage must be the last stage of the pipeline!"
            comptime assert Self.Ts[i].StageT.kind != StageKind.TRANSFORM_MANY, "{MoStream} Assert: TRANSFORM_MANY is not supported by BlockingPipeline!"
        self.nodes = nodes^
        self.queue_size = 1024
        self.coreslist = CoresList()
        var mapping_str = getenv("MOSTREAM_PINNING", "")
        var mp = Python.import_module("multiprocessing")
        self.coreslist.init_cores_list(mapping_str, Int(py=mp.cpu_count()))
        self.alreadyRun = False

    def _run_from[idx: Int,
                 length: Int,
                 M: MessageTrait]
                 (mut self,
                 mut tg: TaskGroup,
                 in_comm: Pointer[mut=True, BlockingCommunicator[M], MutUntrackedOrigin]) raises:
        var np = self.nodes[idx].parallelism()
        var out_comm = Pointer[BlockingCommunicator[Self.Ts[idx].StageT.OutType], MutUntrackedOrigin].unsafe_dangling()
        comptime if idx < Self.N-1:
            var nc = self.nodes[idx+1].parallelism()
            out_comm = unsafe_alloc[BlockingCommunicator[Self.Ts[idx].StageT.OutType]](1)
            out_comm.unsafe_write(BlockingCommunicator[Self.Ts[idx].StageT.OutType](pN=np, cN=nc, queue_size=self.queue_size))
        for _ in range(0, np):
            var core_id = self.coreslist.get_next_core_id()
            var task = blocking_executor_task[idx, length](self.nodes[idx], in_comm, out_comm, core_id)
            tg.create_task(task^)
        comptime if idx + 1 < Self.N:
            self._run_from[idx + 1, length, Self.Ts[idx].StageT.OutType](tg, out_comm)

    def run(mut self) raises:
        if (self.alreadyRun):
            print_red_color("{MoStream} Error: run() can be called only once for each pipeline instance!")
            raise Error("error in run()")
        self.alreadyRun = True
        if (self.getNumNodes() > parallelism_level()):
            print_red_color("{MoStream} Error: the number of nodes in the pipeline is greater than the number threads available in the thread pool!")
            raise Error("error in run()")
        var pinning = "disabled"
        if self.coreslist.enabled:
            pinning = "enabled"
        print_cyan_color("{MoStream} Starting pipeline execution with " + String(Self.N) + " stages and total parallelism of " + String(self.getNumNodes()) + " nodes")
        print_cyan_color("{MoStream} Standard MoStream runtime (blocking pop, explicit EOS) is used with " + String(self.getNumNodes()) + " threads")
        print_cyan_color("{MoStream} CPU pinning is " + pinning)
        print_cyan_color("{MoStream} Inter-stage queue: " + String(COMMUNICATION_QUEUE_NAME) + " (blocking pop)")
        print_cyan_color("{MoStream} Pipeline starts...")
        var tg = TaskGroup()
        var first_comm = Pointer[BlockingCommunicator[Self.Ts[0].StageT.InType], MutUntrackedOrigin].unsafe_dangling()
        self._run_from[0, Self.N](tg, first_comm)
        tg.wait()
        print_cyan_color("{MoStream} ...terminated successfully!")

    def setPinning(mut self, enabled: Bool):
        self.coreslist.setPinning(enabled)

    def setQueueSize(mut self, queue_size: Int):
        self.queue_size = queue_size

    def getNumNodes(self) -> Int:
        var total_nodes = 0
        comptime for i in range(0, Self.N):
            total_nodes += self.nodes[i].parallelism()
        return total_nodes
