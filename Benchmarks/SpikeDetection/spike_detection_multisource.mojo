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

# Spike Detection (DSPBench) in MoStream con la sorgente replicata, partizionata per sensore:
#   PartitionedSensorSource x S -> MovingAverage x MA -> SpikeDetector x SD -> SpikeSink
#   La coda fra sorgenti e MovingAverage ha S produttori e MA consumatori.
#   Con S = 1 la pipeline equivale a quella di spike_detection.mojo.
#   Il dataset e la partizione sono preparati prima del tempo misurato; ogni sorgente riproduce la
#   sua parte per <duration_s> secondi, oppure esattamente <passes> volte se passes > 0 (verifica).

from MoStream import Pipeline, seq, parallel
from MoStream.communication_queue import COMMUNICATION_QUEUE_NAME
from sd_stages import SpikeShared, MovingAverage, SpikeDetector, SpikeSink
from sd_multisource_stages import SourcePartition, PartitionedSensorSource
from std.atomic import Ordering
from std.time import perf_counter_ns
from std.sys import argv
from std.memory.alloc import unsafe_alloc

# Main
def main():
    var args = argv()
    if len(args) != 9 and len(args) != 10:
        print("Usage: ./spike_detection_multisource <dataset.bin> <duration_s> <passes> <sources> <MovingAverage> <SpikeDetector> <std|coop> <n_workers> [dump_file]")
        return
    try:
        var path = String(args[1])
        var duration = Int(args[2])
        var passes = Int(args[3])
        var sources = Int(args[4])
        var ma_degree = Int(args[5])
        var sd_degree = Int(args[6])
        var runtime = String(args[7])
        var n_workers = Int(args[8])
        var dump_path = String(args[9]) if len(args) == 10 else String()
        if runtime != "std" and runtime != "coop":
            raise Error("unknown runtime: " + runtime)
        var shared = unsafe_alloc[SpikeShared](1)
        shared.unsafe_write(SpikeShared(path))
        # assegnazione dei sensori alle sorgenti, fuori dal tempo misurato
        var partition = unsafe_alloc[SourcePartition](1)
        partition.unsafe_write(SourcePartition(shared, sources))
        var smin = partition[].size(0)
        var smax = smin
        for r in range(1, sources):
            smin = min(smin, partition[].size(r))
            smax = max(smax, partition[].size(r))
        var source = PartitionedSensorSource(shared, partition, duration, passes)
        var ma = MovingAverage(shared)
        var sd = SpikeDetector(shared)
        var sink = SpikeSink(shared, dump_path)
        # Source x S -> MovingAverage x ma_degree -> SpikeDetector x sd_degree -> Sink
        var pipeline = Pipeline((parallel(source, sources), parallel(ma, ma_degree), parallel(sd, sd_degree), seq(sink)))
        pipeline.setPinning(True) # come in spike_detection.mojo
        var t0 = perf_counter_ns()
        if runtime == "std":
            pipeline.run()
        else:
            pipeline.run_cooperative(n_workers)
        var ms = Float64(Int(perf_counter_ns() - t0)) / 1_000_000.0
        _ = pipeline
        # tutte le S repliche devono aver preso un indice: altrimenti una parte dei sensori non è stata prodotta
        if Int(partition[].next_replica.load()) != sources:
            raise Error("not all the source replicas started")
        # stessa riga RESULT di spike_detection.mojo, con in più sources e le letture min/max per sorgente
        var emitted = Int(shared[].emitted.load[ordering=Ordering.ACQUIRE]())
        print("RESULT runtime=" + runtime + " queue=" + COMMUNICATION_QUEUE_NAME
              + " sources=" + String(sources) + " part_min=" + String(smin) + " part_max=" + String(smax)
              + " ma=" + String(ma_degree) + " sd=" + String(sd_degree) + " workers=" + String(n_workers)
              + " tuples=" + String(emitted)
              + " averaged=" + String(shared[].averaged.load[ordering=Ordering.ACQUIRE]())
              + " detected=" + String(shared[].detected.load[ordering=Ordering.ACQUIRE]())
              + " spikes=" + String(shared[].spikes.load[ordering=Ordering.ACQUIRE]())
              + " hash=" + String(shared[].spike_hash_sum.load[ordering=Ordering.ACQUIRE]())
              + " late=" + String(shared[].late.load[ordering=Ordering.ACQUIRE]())
              + " lock_waits=" + String(shared[].lock_waits.load[ordering=Ordering.ACQUIRE]())
              + " elapsed_ms=" + String(ms)
              + " throughput=" + String(Float64(emitted) / (ms / 1000.0)))
        partition.unsafe_deinit_pointee()
        partition.unsafe_free()
        shared.unsafe_deinit_pointee()
        shared.unsafe_free()
    except e:
        print("Execution failed:", e)
