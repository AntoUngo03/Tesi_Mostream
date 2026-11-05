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

# Sorgente replicata per Spike Detection, partizionata per sensore.
# Ogni sensore è assegnato a una sola delle S sorgenti, che ne produce tutte le letture
# nell'ordine del dataset. Così:
#   - le letture di uno stesso sensore escono da un solo produttore, in ordine; la coda è FIFO
#     per i messaggi di uno stesso produttore, quindi con MovingAverage a una replica l'ordine
#     per sensore è garantito e il risultato deve coincidere con il riferimento sequenziale;
#   - il contatore di sequenza di un sensore è usato da una sola sorgente: niente sincronizzazione.
# Gli altri stadi (MovingAverage, SpikeDetector, SpikeSink) sono quelli di sd_stages.mojo.

from MoStream import StageKind, StageTrait
from sd_stages import SensorReading, SharedPtr
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.time import perf_counter_ns

# Partizione del dataset fra le sorgenti, calcolata in main prima della misura.
# order contiene le posizioni delle letture del dataset raggruppate per sorgente: le letture
# della sorgente r sono order[start[r]] .. order[start[r + 1] - 1], nell'ordine del dataset.
struct SourcePartition(Movable):
    var sources: Int
    var order: Pointer[Int, MutUntrackedOrigin] # n posizioni nel dataset
    var start: Pointer[Int, MutUntrackedOrigin] # sources + 1 elementi
    var owner: Pointer[Int, MutUntrackedOrigin] # sorgente a cui è assegnato ogni sensore
    var next_replica: Atomic[Int64] # indice di replica da assegnare alla prossima sorgente

    # costruttore: assegna i sensori alle sorgenti bilanciando il numero di letture.
    # I sensori sono presi dal più grande al più piccolo e ognuno va alla sorgente che ha
    # meno letture in quel momento (a parità, quella con indice minore): deterministico.
    def __init__(out self, shared: SharedPtr, sources: Int) raises:
        if sources < 1:
            raise Error("sources must be >= 1")
        var n = shared[].n
        var num_devices = shared[].max_device + 1
        self.sources = sources
        self.order = unsafe_alloc[Int](n)
        self.start = unsafe_alloc[Int](sources + 1)
        self.owner = unsafe_alloc[Int](num_devices)
        self.next_replica = Atomic[Int64](0)
        # letture per sensore
        var counts = List[Int](length=num_devices, fill=0)
        for i in range(n):
            counts[shared[].devices.unsafe_offset(i)[]] += 1
        # sensori in ordine di letture decrescenti (a parità, id crescente)
        var by_size = List[Int]()
        for d in range(num_devices):
            if counts[d] > 0:
                by_size.append(d)
        for i in range(1, len(by_size)):
            var d = by_size[i]
            var j = i
            while j > 0 and (counts[by_size[j - 1]] < counts[d] or (counts[by_size[j - 1]] == counts[d] and by_size[j - 1] > d)):
                by_size[j] = by_size[j - 1]
                j -= 1
            by_size[j] = d
        var load = List[Int](length=sources, fill=0)
        for d in range(num_devices):
            self.owner.unsafe_offset(d)[] = -1
        for d in by_size:
            var best = 0
            for r in range(1, sources):
                if load[r] < load[best]:
                    best = r
            self.owner.unsafe_offset(d)[] = best
            load[best] += counts[d]
        # posizioni raggruppate per sorgente, mantenendo l'ordine del dataset in ogni gruppo
        self.start.unsafe_offset(0)[] = 0
        for r in range(sources):
            self.start.unsafe_offset(r + 1)[] = self.start.unsafe_offset(r)[] + load[r]
        var fill = List[Int](length=sources, fill=0)
        for i in range(n):
            var r = self.owner.unsafe_offset(shared[].devices.unsafe_offset(i)[])[]
            self.order.unsafe_offset(self.start.unsafe_offset(r)[] + fill[r])[] = i
            fill[r] += 1

    # costruttore di spostamento
    def __init__(out self, *, deinit move: Self):
        self.sources = move.sources
        self.order = move.order
        self.start = move.start
        self.owner = move.owner
        self.next_replica = Atomic[Int64](move.next_replica.load())

    # distruttore
    def __deinit__(deinit self):
        self.order.unsafe_free()
        self.start.unsafe_free()
        self.owner.unsafe_free()

    # numero di letture assegnate alla sorgente r
    def size(self, r: Int) -> Int:
        return self.start.unsafe_offset(r + 1)[] - self.start.unsafe_offset(r)[]

comptime PartitionPtr = Pointer[SourcePartition, MutUntrackedOrigin]

# PartitionedSensorSource: una delle S repliche della sorgente.
# parallel() crea S copie identiche: alla prima chiamata ogni copia prende il suo indice r dal
# contatore della partizione (una sola fetch_add per replica) e da lì riproduce solo le letture
# dei suoi sensori, a ciclo continuo per duration_sec secondi, o per passes passate se passes > 0.
# Con passes > 0 ogni sorgente fa passes passate sulla sua parte: in totale si producono le stesse
# letture, con gli stessi numeri di sequenza, della sorgente unica di sd_stages.mojo.
struct PartitionedSensorSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = SensorReading
    comptime OutType = SensorReading
    comptime name = "PartitionedSensorSource"
    var shared: SharedPtr
    var partition: PartitionPtr
    var duration_ns: Int
    var passes: Int
    var first: Int # prima posizione in order della parte di questa sorgente; -1 = non ancora assegnata
    var last: Int # posizione successiva all'ultima
    var pos: Int # prossima posizione in order da emettere
    var passes_done: Int
    var count: Int # letture emesse da questa replica
    var start_ns: Int

    # costruttore
    def __init__(out self, shared: SharedPtr, partition: PartitionPtr, duration_sec: Int, passes: Int):
        self.shared = shared
        self.partition = partition
        self.duration_ns = duration_sec * 1_000_000_000
        self.passes = passes
        self.first = -1
        self.last = -1
        self.pos = -1
        self.passes_done = 0
        self.count = 0
        self.start_ns = 0

    # implementazione di next_element
    def next_element(mut self) -> Optional[SensorReading]:
        if self.first < 0:
            # prima chiamata: prende l'indice di replica e fa partire il cronometro
            var r = Int(self.partition[].next_replica.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(1)))
            self.first = self.partition[].start.unsafe_offset(r)[]
            self.last = self.partition[].start.unsafe_offset(r + 1)[]
            self.pos = self.first
            self.start_ns = Int(perf_counter_ns())
        var finished = self.first == self.last # nessun sensore assegnato a questa sorgente
        if self.pos == self.last:
            self.pos = self.first
            self.passes_done += 1
        if self.passes > 0:
            finished = finished or self.passes_done == self.passes
        elif (self.count & 1023) == 0:
            # come in SensorSource: l'orologio si legge ogni 1024 tuple
            finished = finished or Int(perf_counter_ns()) - self.start_ns >= self.duration_ns
        if finished:
            _ = self.shared[].emitted.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.count))
            return None
        var i = self.partition[].order.unsafe_offset(self.pos)[]
        var device = self.shared[].devices.unsafe_offset(i)[]
        # il sensore appartiene solo a questa sorgente: next_seq non ha bisogno di sincronizzazione
        var seq_ptr = self.shared[].next_seq.unsafe_offset(device)
        var seq = seq_ptr[]
        seq_ptr[] = seq + 1
        var value = self.shared[].values.unsafe_offset(i)[]
        self.pos += 1
        self.count += 1
        return SensorReading(device, seq, value)
