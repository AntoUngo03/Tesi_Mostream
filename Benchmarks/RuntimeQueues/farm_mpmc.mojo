# Farm MPMC nella vera libreria MoStream:
# Source -> N Compute A -> N Compute B -> Sink (2*N+2 task).
# La coda centrale collega N produttori a N consumatori.
# Ogni input esegue I//2 iterazioni in A e I-I//2 in B: totale I fisso.
# CLI: binary M N I capacita on|off; oppure info / calibrate.
# Il runner seleziona core fisici distinti e imposta MOSTREAM_PINNING.
# Il timer comprende Pipeline.run; l'oracolo esatto è fuori dalla misura.
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

@no_inline
def work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

@fieldwise_init
struct Item(Copyable):
    var id: Int
    var value: UInt64

struct Source(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Source"
    var index: Int
    var messages: Int

    def __init__(out self, messages: Int):
        self.index = 0
        self.messages = messages

    def next_element(mut self) -> Optional[Item]:
        if self.index == self.messages:
            return None
        var index = self.index
        self.index += 1
        return Item(index, UInt64(index + 1))

@fieldwise_init
struct Compute(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Compute"
    var iterations: Int

    def compute(mut self, var input: Item) -> Optional[Item]:
        input.value = work(input.value, self.iterations)
        return input^

@fieldwise_init
struct Sink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Sink"
    var seen: Pointer[Int, MutUntrackedOrigin]
    var values: Pointer[UInt64, MutUntrackedOrigin]
    var messages: Int

    def consume_element(mut self, var input: Item):
        if input.id < 0 or input.id >= self.messages:
            self.seen.unsafe_offset(self.messages)[] += 1
        else:
            self.seen.unsafe_offset(input.id)[] += 1
            self.values.unsafe_offset(input.id)[] = input.value


def main() raises:
    var args = argv()
    if args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    if args[1] == "calibrate":
        var value = UInt64(12345)
        var begin = perf_counter_ns()
        value = work(value, 2000000)
        print("CALIBRATION", Float64(perf_counter_ns() - begin) / 2000000.0, value)
        return
    var messages = Int(args[1])
    var workers = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    var pinned = args[5] == "on"
    var seen = unsafe_alloc[Int](messages + 1)
    var values = unsafe_alloc[UInt64](messages)
    for i in range(messages + 1):
        seen.unsafe_offset(i)[] = 0
    var pipeline = Pipeline((seq(Source(messages)), parallel(Compute(iterations // 2), workers),
                             parallel(Compute(iterations - iterations // 2), workers),
                             seq(Sink(seen, values, messages))))
    pipeline.setPinning(pinned)
    pipeline.setQueueSize(capacity)
    var begin = perf_counter_ns()
    pipeline.run()
    var elapsed = Int(perf_counter_ns() - begin)
    for i in range(messages):
        assert_equal(seen.unsafe_offset(i)[], 1)
        assert_equal(values.unsafe_offset(i)[], work(UInt64(i + 1), iterations))
    assert_equal(seen.unsafe_offset(messages)[], 0)
    seen.unsafe_free()
    values.unsafe_free()
    print("RESULT", elapsed)
