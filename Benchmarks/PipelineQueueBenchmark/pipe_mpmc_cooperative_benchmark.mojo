from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys import argv
from std.time import perf_counter_ns

from MoStream import Pipeline, StageKind, StageTrait, parallel
from MoStream.pipeline_queue import USE_PADDED_FAA, USE_SCQ

# Equivalente cooperativo del benchmark standard. `degree` controlla il numero
# di source actor e sink actor; `workers` controlla separatamente quanti worker
# dello scheduler li eseguono. Le ready/wait queue dello scheduler non cambiano
# backend: soltanto il Communicator tra source e sink usa MPMC oppure SCQ.


@always_inline
def apply_work(input: UInt64, iterations: Int) -> UInt64:
    # Kernel sintetico identico alla versione standard, così i due runtime sono
    # confrontabili a parità di calcolo applicativo per messaggio.
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


struct CooperativeSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "CooperativeSource"
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


struct CooperativeSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Int
    comptime OutType = Int
    comptime name = "CooperativeSink"
    var local_count: UInt64
    var local_checksum: UInt64
    var work_iterations: Int
    var total_count: UnsafePointer[Atomic[DType.uint64], MutExternalOrigin]
    var total_checksum: UnsafePointer[Atomic[DType.uint64], MutExternalOrigin]

    def __init__(
        out self,
        total_count: UnsafePointer[Atomic[DType.uint64], MutExternalOrigin],
        total_checksum: UnsafePointer[Atomic[DType.uint64], MutExternalOrigin],
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


def run_once(
    elements: Int,
    degree: Int,
    workers: Int,
    capacity: Int,
    work_iterations: Int,
) raises:
    var total_count = alloc[Atomic[DType.uint64]](1)
    var total_checksum = alloc[Atomic[DType.uint64]](1)
    total_count[] = Atomic[DType.uint64](0)
    total_checksum[] = Atomic[DType.uint64](0)

    var source = CooperativeSource(elements)
    var sink = CooperativeSink(
        total_count, total_checksum, work_iterations
    )
    var pipeline = Pipeline((parallel(source, degree), parallel(sink, degree)))
    pipeline.setQueueSize(capacity)
    pipeline.setPinning(enabled=False)

    # La regione temporizzata comprende run_cooperative(): inizializzazione
    # dello scheduler, esecuzione delle attivazioni e join dei worker.
    var start = perf_counter_ns()
    pipeline.run_cooperative(workers)
    var elapsed = perf_counter_ns() - start

    var actual_count = total_count[].load[ordering=Ordering.ACQUIRE]()
    var actual_checksum = total_checksum[].load[ordering=Ordering.ACQUIRE]()
    var expected_count = UInt64(elements * degree)
    var expected_sum = expected_checksum(elements, degree, work_iterations)
    var valid = (
        actual_count == expected_count and actual_checksum == expected_sum
    )
    var backend = String("MPMC")
    comptime if USE_SCQ:
        backend = String("SCQ")
    elif USE_PADDED_FAA:
        backend = String("PADDEDFAA")
    print(
        "PIPE_COOP_MPMC_RESULT backend=", backend,
        " producers=", degree,
        " consumers=", degree,
        " workers=", workers,
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

    total_count.destroy_pointee()
    total_count.free()
    total_checksum.destroy_pointee()
    total_checksum.free()
    if not valid:
        raise Error("cooperative MPMC pipeline validation failed")


def main() raises:
    var args = argv()
    if len(args) != 5 and len(args) != 6:
        print(
            "Usage: pipe_mpmc_cooperative_benchmark "
            "<elements/source> <degree> <workers> <capacity> "
            "[work_iterations]"
        )
        raise Error("invalid arguments")
    var elements = Int(args[1])
    var degree = Int(args[2])
    var workers = Int(args[3])
    var capacity = Int(args[4])
    var work_iterations = 0
    if len(args) == 6:
        work_iterations = Int(args[5])
    if (
        elements < 1 or degree < 1 or workers < 1
        or capacity < 2 or work_iterations < 0
    ):
        raise Error("arguments out of range")
    run_once(elements, degree, workers, capacity, work_iterations)
