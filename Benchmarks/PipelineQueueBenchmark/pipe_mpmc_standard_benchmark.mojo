from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys import argv
from std.time import perf_counter_ns

from MoStream import Pipeline, StageKind, StageTrait, parallel
from MoStream.pipeline_queue import USE_PADDED_FAA, USE_SCQ, USE_MICHAEL_SCOTT, USE_NBLFQ

# Pipeline intenzionalmente minimale per isolare il costo del comunicatore nel
# runtime standard. `degree` crea lo stesso numero di producer e consumer:
#
#   parallel(LightSource, degree) -> queue -> parallel(LightSink, degree)
#
# Ogni replica della source produce la sequenza 1..elements. Il checksum atteso
# viene quindi calcolato una volta e moltiplicato per degree.


struct LightSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "LightSource"
    var next: Int
    var limit: Int

    def __init__(out self, limit: Int):
        self.next = 1
        self.limit = limit

    def next_element(mut self) -> Optional[Int]:
        if self.next > self.limit:
            return None
        var value = self.next
        self.next += 1
        return value


struct LightSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Int
    comptime OutType = Int
    comptime name = "LightSink"
    var local_count: UInt64
    var local_checksum: UInt64
    var work_iterations: Int
    var total_count: Pointer[Atomic[DType.uint64], MutUntrackedOrigin]
    var total_checksum: Pointer[Atomic[DType.uint64], MutUntrackedOrigin]

    def __init__(
        out self,
        total_count: Pointer[Atomic[DType.uint64], MutUntrackedOrigin],
        total_checksum: Pointer[Atomic[DType.uint64], MutUntrackedOrigin],
        work_iterations: Int,
    ):
        self.local_count = 0
        self.local_checksum = 0
        self.work_iterations = work_iterations
        self.total_count = total_count
        self.total_checksum = total_checksum

    def consume_element(mut self, var input: Int):
        self.local_count += 1
        self.local_checksum += apply_work(UInt64(input), self.work_iterations)

    def received_eos(mut self):
        _ = self.total_count[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_count)
        _ = self.total_checksum[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_checksum)


@always_inline
def apply_work(input: UInt64, iterations: Int) -> UInt64:
    """Lavoro intero dependency-chained con risultato osservabile."""
    # Ogni iterazione dipende dalla precedente: il compilatore non puo
    # parallelizzare le operazioni dello stesso messaggio. Il risultato viene
    # accumulato dal sink e confrontato a fine esecuzione.
    var value = input
    for _ in range(iterations):
        value = value ^ (value << UInt64(13))
        value = value ^ (value >> UInt64(7))
        value = value ^ (value << UInt64(17))
    return value


def expected_checksum(elements: Int, degree: Int, iterations: Int) -> UInt64:
    var checksum: UInt64 = 0
    for value in range(1, elements + 1):
        checksum += apply_work(UInt64(value), iterations)
    return checksum * UInt64(degree)


def run_once(
    elements: Int, degree: Int, capacity: Int, work_iterations: Int
) raises:
    var total_count = unsafe_alloc[Atomic[DType.uint64]](1)
    var total_checksum = unsafe_alloc[Atomic[DType.uint64]](1)
    total_count[] = Atomic[DType.uint64](0)
    total_checksum[] = Atomic[DType.uint64](0)

    var source = LightSource(elements)
    var sink = LightSink(total_count, total_checksum, work_iterations)
    var pipeline = Pipeline((parallel(source, degree), parallel(sink, degree)))
    pipeline.setQueueSize(capacity)
    pipeline.setPinning(enabled=False)

    # Costruzione di stage e Pipeline esclusa; run() include invece creazione
    # dei communicator, avvio/join dei task e trasferimento di tutti i messaggi.
    var start = perf_counter_ns()
    pipeline.run()
    var elapsed = perf_counter_ns() - start

    var actual_count = total_count[].load[ordering=Ordering.ACQUIRE]()
    var actual_checksum = total_checksum[].load[ordering=Ordering.ACQUIRE]()
    var expected_count = UInt64(elements * degree)
    var expected_sum = expected_checksum(elements, degree, work_iterations)
    var valid = (
        actual_count == expected_count
        and actual_checksum == expected_sum
    )
    # Il backend e risolto a compile time: nessun branch viene eseguito nel
    # percorso caldo della coda.
    var backend = String("MPMC")
    comptime if USE_NBLFQ:
        backend = String("NBLFQ")
    elif USE_MICHAEL_SCOTT:
        backend = String("MICHAELSCOTT")
    elif USE_SCQ:
        backend = String("SCQ")
    elif USE_PADDED_FAA:
        backend = String("PADDEDFAA")
    print(
        "PIPE_MPMC_RESULT backend=", backend,
        " producers=", degree,
        " consumers=", degree,
        " capacity=", capacity,
        " work_iterations=", work_iterations,
        " messages=", expected_count,
        " time_ms=", Float64(elapsed) / 1_000_000.0,
        " throughput_Mtransfer_s=",
        Float64(expected_count) / Float64(elapsed) * 1000.0,
        " count=", actual_count,
        " checksum=", actual_checksum,
        " valid=", valid,
    )

    total_count.unsafe_deinit_pointee()
    total_count.unsafe_free()
    total_checksum.unsafe_deinit_pointee()
    total_checksum.unsafe_free()
    if not valid:
        raise Error("standard MPMC pipeline validation failed")


def main() raises:
    var args = argv()
    if len(args) != 4 and len(args) != 5:
        print(
            "Usage: pipe_mpmc_standard_benchmark "
            "<elements/source> <degree> <capacity> [work_iterations]"
        )
        raise Error("invalid arguments")
    var elements = Int(args[1])
    var degree = Int(args[2])
    var capacity = Int(args[3])
    var work_iterations = 0
    if len(args) == 5:
        work_iterations = Int(args[4])
    elif len(args) != 4:
        raise Error("invalid arguments")
    if elements < 1 or degree < 1 or capacity < 2 or work_iterations < 0:
        raise Error("arguments out of range")
    run_once(elements, degree, capacity, work_iterations)
