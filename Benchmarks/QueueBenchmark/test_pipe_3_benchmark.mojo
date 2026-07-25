# Timed, longer-running version of Tests/test_pipe_3.mojo.

from std.collections import Optional
from std.time import perf_counter_ns
from MoStream import StageKind, StageTrait, parallel, Pipeline

comptime ELEMENTS_PER_SOURCE = 1_000_000


struct FirstStage(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "FirstStage"
    var count: Int

    def __init__(out self):
        self.count = 0

    def next_element(mut self) -> Optional[Int]:
        if self.count >= ELEMENTS_PER_SOURCE:
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
    var sum: Int

    def __init__(out self):
        self.sum = 0

    def consume_element(mut self, var input: Int):
        self.sum += input

    def received_eos(mut self):
        # Keep the accumulated result observable to the optimizer.
        print("partial_sum=", self.sum)


def main():
    try:
        var first = FirstStage()
        var second = ForwardStage()
        var third = ForwardStage()
        var fourth = SinkStage()
        var pipeline = Pipeline((
            parallel(first, 2),
            parallel(second, 2),
            parallel(third, 3),
            parallel(fourth, 3),
        ))
        pipeline.setPinning(enabled=False)
        var start = perf_counter_ns()
        pipeline.run()
        var elapsed_ms = Float64(perf_counter_ns() - start) / 1_000_000.0
        var total_messages = 2 * ELEMENTS_PER_SOURCE
        var throughput = Float64(total_messages) / elapsed_ms / 1000.0
        print("PIPE3_RESULT time_ms=", elapsed_ms, "throughput_Mmsg_s=", throughput)
    except error:
        print("Execution failed:", error)
