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

# Spike Detection (DSPBench) in MoStream, branch cooperative-runtime-v2 (vedi prepare_v2.sh):
#   SensorSource -> MovingAverage -> SpikeDetector -> SpikeSink
#   Sorgente e sink sono sequenziali, MovingAverage e SpikeDetector possono essere paralleli.
#   Il dataset è caricato prima del tempo misurato; la sorgente lo riproduce per <duration_s>
#   secondi, oppure esattamente <passes> volte se passes > 0 (modalità di verifica).
#   Runtime: std, coop-mpmc (code dei pronti MPMC per worker), coop-ws (code dei pronti per worker con
#   work stealing); <rounds> è il parametro max_consume_rounds di run_cooperative.

from MoStream import Pipeline, seq, parallel
from MoStream.communication_queue import COMMUNICATION_QUEUE_NAME
from MoStream.ready_queue import ReadyQueueKind
from sd_stages import SpikeShared, SensorSource, MovingAverage, SpikeDetector, SpikeSink
from std.atomic import Ordering
from std.time import perf_counter_ns
from std.sys import argv
from std.memory.alloc import unsafe_alloc

# Main
def main():
    var args = argv()
    if len(args) != 9 and len(args) != 10:
        print("Usage: ./spike_detection_v2 <dataset.bin> <duration_s> <passes> <MovingAverage> <SpikeDetector> <std|coop-mpmc|coop-ws> <n_workers> <rounds> [dump_file]")
        return
    try:
        var path = String(args[1])
        var duration = Int(args[2])
        var passes = Int(args[3])
        var ma_degree = Int(args[4])
        var sd_degree = Int(args[5])
        var runtime = String(args[6])
        var n_workers = Int(args[7])
        var rounds = Int(args[8])
        var dump_path = String(args[9]) if len(args) == 10 else String()
        if runtime != "std" and runtime != "coop-mpmc" and runtime != "coop-ws":
            raise Error("unknown runtime: " + runtime)
        # i dati condivisi sono allocati una volta sola sullo heap; gli stadi tengono solo un puntatore,
        # perché parallel() copia lo stadio per ogni replica
        var shared = unsafe_alloc[SpikeShared](1)
        shared.unsafe_write(SpikeShared(path))
        var source = SensorSource(shared, duration, passes)
        var ma = MovingAverage(shared)
        var sd = SpikeDetector(shared)
        var sink = SpikeSink(shared, dump_path)
        # Source -> MovingAverage x ma_degree -> SpikeDetector x sd_degree -> Sink
        var pipeline = Pipeline((seq(source), parallel(ma, ma_degree), parallel(sd, sd_degree), seq(sink)))
        pipeline.setPinning(True) # fissa i thread ai core, come negli altri benchmark
        # il tempo misurato comprende l'avvio e la chiusura dei thread, non il caricamento del dataset
        var t0 = perf_counter_ns()
        if runtime == "std":
            pipeline.run()
        elif runtime == "coop-mpmc":
            # code dei pronti MPMC per worker (può fermarsi con "cooperative ready queue is full!", vedi README)
            pipeline.run_cooperative(n_workers, ReadyQueueKind.MPMC, rounds)
        else:
            # code dei pronti per worker con work stealing; rounds = max_consume_rounds, il numero massimo
            # di giri di consumo che un actor fa ogni volta che viene schedulato
            pipeline.run_cooperative(n_workers, ReadyQueueKind.WORK_STEALING, rounds)
        var ms = Float64(Int(perf_counter_ns() - t0)) / 1_000_000.0
        _ = pipeline # tiene viva la pipeline (e gli stadi) fino alla fine dell'esecuzione
        # l'esecuzione è finita: ogni replica ha pubblicato i suoi contatori all'EOS.
        # La riga RESULT è quella letta da run_campaign.sh; la coda si sceglie in compilazione
        # (-D MOSTREAM_RIGTORP=1), quindi viene stampata per controllare quale binario è stato eseguito.
        # tuples = averaged = detected significa che nessuna lettura è andata persa.
        var emitted = Int(shared[].emitted.load[ordering=Ordering.ACQUIRE]())
        print("RESULT runtime=" + runtime + " queue=" + COMMUNICATION_QUEUE_NAME
              + " ma=" + String(ma_degree) + " sd=" + String(sd_degree) + " workers=" + String(n_workers) + " rounds=" + String(rounds)
              + " tuples=" + String(emitted)
              + " averaged=" + String(shared[].averaged.load[ordering=Ordering.ACQUIRE]())
              + " detected=" + String(shared[].detected.load[ordering=Ordering.ACQUIRE]())
              + " spikes=" + String(shared[].spikes.load[ordering=Ordering.ACQUIRE]())
              + " hash=" + String(shared[].spike_hash_sum.load[ordering=Ordering.ACQUIRE]())
              + " late=" + String(shared[].late.load[ordering=Ordering.ACQUIRE]())
              + " lock_waits=" + String(shared[].lock_waits.load[ordering=Ordering.ACQUIRE]())
              + " elapsed_ms=" + String(ms)
              + " throughput=" + String(Float64(emitted) / (ms / 1000.0)))
        shared.unsafe_deinit_pointee()
        shared.unsafe_free()
    except e:
        print("Execution failed:", e)
