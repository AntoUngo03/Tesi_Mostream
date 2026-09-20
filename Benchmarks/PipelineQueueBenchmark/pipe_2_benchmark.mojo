# Timed, silent equivalent of Tests/test_pipe_2.mojo.

from std.memory.alloc import unsafe_alloc
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.sys import argv
from std.time import perf_counter_ns

from MoStream import Emitter, Pipeline, StageKind, StageTrait, seq
from MoStream.pipeline_queue import USE_PADDED_FAA, USE_SCQ


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


struct SecondStage(StageTrait):
    comptime kind = StageKind.TRANSFORM_MANY
    comptime InType = Int
    comptime OutType = String
    comptime name = "SecondStage"

    def __init__(out self):
        pass

    def compute_many(mut self, var input: Int, mut emitter: Emitter[String]):
        var value = input + 1
        emitter.emit(String("Value " + String(value)))
        emitter.emit(String("Value " + String(value * 2)))


struct ThirdStage(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = String
    comptime OutType = String
    comptime name = "ThirdStage"
    var local_count: UInt64
    var local_checksum: UInt64
    var observed_count: Pointer[
        Atomic[DType.uint64], MutUntrackedOrigin
    ]
    var observed_checksum: Pointer[
        Atomic[DType.uint64], MutUntrackedOrigin
    ]

    def __init__(
        out self,
        observed_count: Pointer[
            Atomic[DType.uint64], MutUntrackedOrigin
        ],
        observed_checksum: Pointer[
            Atomic[DType.uint64], MutUntrackedOrigin
        ],
    ):
        self.local_count = 0
        self.local_checksum = 0
        self.observed_count = observed_count
        self.observed_checksum = observed_checksum

    def consume_element(mut self, var input: String):
        self.local_count += 1
        self.local_checksum += UInt64(input.byte_length())

    def received_eos(mut self):
        _ = self.observed_count[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_count)
        _ = self.observed_checksum[].fetch_add[
            ordering=Ordering.ACQUIRE_RELEASE
        ](self.local_checksum)


def decimal_digits(value: Int) -> UInt64:
    var remaining = value
    var digits: UInt64 = 1
    while remaining >= 10:
        remaining //= 10
        digits += 1
    return digits


def expected_checksum(elements: Int) -> UInt64:
    var checksum: UInt64 = 0
    for input in range(1, elements + 1):
        var value = input + 1
        checksum += 12 + decimal_digits(value) + decimal_digits(value * 2)
    return checksum


def run_once(elements: Int) raises:
    var observed_count = unsafe_alloc[Atomic[DType.uint64]](1)
    var observed_checksum = unsafe_alloc[Atomic[DType.uint64]](1)
    observed_count[] = Atomic[DType.uint64](0)
    observed_checksum[] = Atomic[DType.uint64](0)

    var first = FirstStage(elements)
    var second = SecondStage()
    var third = ThirdStage(observed_count, observed_checksum)
    var pipeline = Pipeline((seq(first), seq(second), seq(third)))
    pipeline.setQueueSize(1024)
    pipeline.setPinning(enabled=False)

    var start = perf_counter_ns()
    pipeline.run()
    var elapsed_ns = perf_counter_ns() - start

    var actual_count = observed_count[].load[ordering=Ordering.ACQUIRE]()
    var actual_checksum = observed_checksum[].load[
        ordering=Ordering.ACQUIRE
    ]()
    var expected_count = UInt64(elements) * 2
    var expected_sum = expected_checksum(elements)
    var valid = (
        actual_count == expected_count and actual_checksum == expected_sum
    )
    var backend = String("MPMC")
    comptime if USE_SCQ:
        backend = String("SCQ")
    elif USE_PADDED_FAA:
        backend = String("PADDEDFAA")
    var elapsed_ms = Float64(elapsed_ns) / 1_000_000.0
    # N messages cross the first edge and 2N cross the second.
    var transfers = UInt64(elements) * 3
    var throughput = (
        Float64(transfers) / Float64(elapsed_ns) * 1000.0
    )
    print(
        "PIPE_RESULT test=pipe_2 backend=", backend,
        " time_ms=", elapsed_ms,
        " throughput_Mtransfer_s=", throughput,
        " count=", actual_count,
        " checksum=", actual_checksum,
        " valid=", valid,
    )

    observed_count.unsafe_deinit_pointee()
    observed_count.unsafe_free()
    observed_checksum.unsafe_deinit_pointee()
    observed_checksum.unsafe_free()
    if not valid:
        raise Error("pipe_2 benchmark correctness failure")


def main() raises:
    var args = argv()
    if len(args) != 2:
        print("Usage: pipe_2_benchmark <elements>")
        raise Error("invalid arguments")
    var elements = Int(args[1])
    if elements < 1:
        raise Error("elements must be positive")
    run_once(elements)
