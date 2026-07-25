# Timed, silent equivalent of Tests/test_pipe_3_coop.mojo.

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys import argv
from std.time import perf_counter_ns

from MoStream import Pipeline, StageKind, StageTrait, parallel
from MoStream.pipeline_queue import USE_PADDED_FAA


struct FirstStage(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "FirstStage"
    var count: Int
    var limit: Int

    def __init__(out self, limit: Int):
        self.count = 0
        self.limit = limit

    def next_element(mut self) -> Optional[Int]:
        if self.count >= self.limit:
            return None
        self.count += 1
        return self.count


struct ForwardStage(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Int
    comptime OutType = Int
    comptime name = "ForwardStage"

    def __init__(out self):
        pass

    def compute(mut self, var input: Int) -> Int:
        return input


struct SinkStage(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Int
    comptime OutType = Int
    comptime name = "SinkStage"
    var local_count: UInt64
    var local_checksum: UInt64
    var observed_count: UnsafePointer[
        Atomic[DType.uint64], MutExternalOrigin
    ]
    var observed_checksum: UnsafePointer[
        Atomic[DType.uint64], MutExternalOrigin
    ]

    def __init__(
        out self,
        observed_count: UnsafePointer[
            Atomic[DType.uint64], MutExternalOrigin
        ],
        observed_checksum: UnsafePointer[
            Atomic[DType.uint64], MutExternalOrigin
        ],
    ):
        self.local_count = 0
        self.local_checksum = 0
        self.observed_count = observed_count
        self.observed_checksum = observed_checksum

    def consume_element(mut self, var input: Int):
        self.local_count += 1
        self.local_checksum += UInt64(input)

    def received_eos(mut self):
        # Only one shared atomic update per sink replica, outside the hot path.
        _ = self.observed_count[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_count)
        _ = self.observed_checksum[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_checksum)


def run_once(elements: Int, workers: Int) raises:
    var observed_count = alloc[Atomic[DType.uint64]](1)
    var observed_checksum = alloc[Atomic[DType.uint64]](1)
    observed_count[] = Atomic[DType.uint64](0)
    observed_checksum[] = Atomic[DType.uint64](0)

    var first = FirstStage(elements)
    var second = ForwardStage()
    var third = ForwardStage()
    var fourth = SinkStage(observed_count, observed_checksum)
    var pipeline = Pipeline((
        parallel(first, 2),
        parallel(second, 2),
        parallel(third, 3),
        parallel(fourth, 3),
    ))
    pipeline.setQueueSize(1024)
    pipeline.setPinning(enabled=False)

    var start = perf_counter_ns()
    pipeline.run_cooperative(workers)
    var elapsed_ns = perf_counter_ns() - start

    var actual_count = observed_count[].load[ordering=Ordering.ACQUIRE]()
    var actual_checksum = observed_checksum[].load[
        ordering=Ordering.ACQUIRE
    ]()
    var expected_count = UInt64(elements) * 2
    var expected_sum = UInt64(elements) * UInt64(elements + 1)
    var valid = (
        actual_count == expected_count and actual_checksum == expected_sum
    )
    var backend = String("MPMC")
    comptime if USE_PADDED_FAA:
        backend = String("PADDEDFAA")
    var elapsed_ms = Float64(elapsed_ns) / 1_000_000.0
    # Two source replicas each emit N messages across three edges.
    var transfers = expected_count * 3
    var throughput = (
        Float64(transfers) / Float64(elapsed_ns) * 1000.0
    )
    print(
        "PIPE_RESULT test=pipe_3_coop backend=", backend,
        " workers=", workers,
        " time_ms=", elapsed_ms,
        " throughput_Mtransfer_s=", throughput,
        " count=", actual_count,
        " checksum=", actual_checksum,
        " valid=", valid,
    )

    observed_count.destroy_pointee()
    observed_count.free()
    observed_checksum.destroy_pointee()
    observed_checksum.free()
    if not valid:
        raise Error("pipe_3_coop benchmark correctness failure")


def main() raises:
    var args = argv()
    if len(args) != 3:
        print("Usage: pipe_3_coop_benchmark <elements_per_source> <workers>")
        raise Error("invalid arguments")
    var elements = Int(args[1])
    var workers = Int(args[2])
    if elements < 1 or workers < 1:
        raise Error("elements and workers must be positive")
    run_once(elements, workers)
