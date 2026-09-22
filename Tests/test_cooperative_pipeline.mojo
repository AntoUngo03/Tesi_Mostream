"""Exact-delivery regression and benchmark of Pipeline.run_cooperative."""

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory.alloc import unsafe_alloc
from std.sys import argv
from std.time import perf_counter_ns
from MoStream import Pipeline, StageKind, StageTrait, parallel
from MoStream.node import NodeTrait

comptime SAMPLE_EVERY = 256


struct Record(Copyable):
    var id: Int
    var value: UInt64
    var started: Int

    def __init__(out self, id: Int, started: Int):
        self.id = id
        self.value = UInt64(id + 1)
        self.started = started


@always_inline
def compute_work(value: UInt64, rounds: Int) -> UInt64:
    var result = value
    for _ in range(rounds):
        result = (result ^ (result >> 27)) * UInt64(0x2545F4914F6CDD1D)
    return result


struct Source(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Record
    comptime OutType = Record
    var ids: Pointer[Atomic[DType.uint64], MutUntrackedOrigin]
    var replica: Int
    var messages: Int
    var next: Int

    def __init__(out self, ids: Pointer[Atomic[DType.uint64], MutUntrackedOrigin], messages: Int):
        self.ids = ids
        self.replica = -1
        self.messages = messages
        self.next = 0

    def next_element(mut self) -> Optional[Record]:
        if self.replica < 0:
            self.replica = Int(self.ids[].fetch_add[ordering=Ordering.RELAXED](1))
        if self.next == self.messages:
            return None
        var id = self.replica * self.messages + self.next
        self.next += 1
        var started = 0
        if id % SAMPLE_EVERY == 0:
            started = perf_counter_ns()
        return Optional(Record(id, started))


struct Transform(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Record
    comptime OutType = Record
    var rounds: Int
    var drop_every: Int

    def __init__(out self, rounds: Int, drop_every: Int):
        self.rounds = rounds
        self.drop_every = drop_every

    def compute(mut self, var input: Record) -> Optional[Record]:
        if self.drop_every > 0 and input.id % self.drop_every == 0:
            return None
        input.value = compute_work(input.value, self.rounds)
        return Optional(input^)


struct Sink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Record
    comptime OutType = Record
    var seen: Pointer[Atomic[DType.uint64], MutUntrackedOrigin]
    var samples: Pointer[Int, MutUntrackedOrigin]
    var verify: Bool
    var total: Int
    var count: UInt64
    var checksum: UInt64
    var invalid: Bool

    def __init__(out self, seen: Pointer[Atomic[DType.uint64], MutUntrackedOrigin],
                 samples: Pointer[Int, MutUntrackedOrigin], verify: Bool, total: Int):
        self.seen = seen
        self.samples = samples
        self.verify = verify
        self.total = total
        self.count = 0
        self.checksum = 0
        self.invalid = False

    def consume_element(mut self, var input: Record):
        if input.id < 0 or input.id >= self.total:
            self.invalid = True
            return
        self.count += 1
        self.checksum += input.value
        if self.verify:
            _ = self.seen[unsafe_offset=input.id].fetch_add[ordering=Ordering.RELAXED](1)
        if input.id % SAMPLE_EVERY == 0:
            self.samples[unsafe_offset=input.id // SAMPLE_EVERY] = perf_counter_ns() - input.started


def actors_finished[Node: NodeTrait](mut node: Node) raises -> Bool:
    var valid = True
    for i in range(node.parallelism()):
        var actor = node.actor_ref(i)
        valid = valid and actor[].done
        valid = valid and not actor[].input_operation.reserved and not actor[].output_operation.reserved
        valid = valid and not actor[].input_operation.item and not actor[].output_operation.item
    return valid


def run_case(messages: Int, sources: Int, transforms: Int, sinks: Int,
             workers: Int, capacity: Int, batch: Int, work: Int,
             verify: Bool, drop_every: Int, pinning: Bool) raises:
    var total = messages * sources
    var ids = unsafe_alloc[Atomic[DType.uint64]](1)
    ids.unsafe_write(Atomic[DType.uint64](0))
    var seen = unsafe_alloc[Atomic[DType.uint64]](max(total, 1) if verify else 1)
    if verify:
        for i in range(total):
            seen.unsafe_offset(i).unsafe_write(Atomic[DType.uint64](0))
    var sample_count = (total + SAMPLE_EVERY - 1) // SAMPLE_EVERY
    var samples = unsafe_alloc[Int](max(sample_count, 1))
    for i in range(sample_count):
        samples[unsafe_offset=i] = 0
    var pipeline = Pipeline((
        parallel(Source(ids, messages), sources),
        parallel(Transform(work, drop_every), transforms),
        parallel(Sink(seen, samples, verify, total), sinks),
    ))
    pipeline.setQueueSize(capacity)
    pipeline.setPinning(pinning)
    var start = perf_counter_ns()
    pipeline.run_cooperative(workers, batch)
    var elapsed = perf_counter_ns() - start
    var count: UInt64 = 0
    var checksum: UInt64 = 0
    var valid = True
    for i in range(sinks):
        var actor = pipeline.nodes[2].actor_ref(i)
        count += actor[].stage.count
        checksum += actor[].stage.checksum
        valid = valid and not actor[].stage.invalid
    # A completed actor must not leave either a payload or a ticket behind.
    valid = valid and actors_finished(pipeline.nodes[0])
    valid = valid and actors_finished(pipeline.nodes[1])
    valid = valid and actors_finished(pipeline.nodes[2])
    var expected_count: UInt64 = 0
    var expected_checksum: UInt64 = 0
    for id in range(total):
        var included = drop_every == 0 or id % drop_every != 0
        if included:
            expected_count += 1
            expected_checksum += compute_work(UInt64(id + 1), work)
        if verify:
            valid = valid and seen[unsafe_offset=id].load[ordering=Ordering.RELAXED]() == UInt64(included)
            seen.unsafe_offset(id).unsafe_deinit_pointee()
    valid = valid and count == expected_count and checksum == expected_checksum
    print("PIPELINE_RESULT", Float64(pipeline.cooperative_execution_ns) / 1_000_000.0,
          Float64(elapsed) / 1_000_000.0,
          Float64(count) * 1000.0 / Float64(pipeline.cooperative_execution_ns),
          count, pipeline.cooperative_activations, pipeline.cooperative_input_parks,
          pipeline.cooperative_output_parks, valid)
    for i in range(sample_count):
        if drop_every == 0 or (i * SAMPLE_EVERY) % drop_every != 0:
            if samples[unsafe_offset=i] <= 0:
                valid = False
            print("PIPELINE_LATENCY_NS", samples[unsafe_offset=i])
    samples.unsafe_free()
    seen.unsafe_free()
    ids.unsafe_deinit_pointee()
    ids.unsafe_free()
    if not valid:
        raise Error("pipeline exact delivery, checksum, ownership, or latency validation failed")
    print("PASS: cooperative pipeline")


def main() raises:
    var args = argv()
    if len(args) == 1:
        for workers in range(1, 5, 3):
            for batch in range(1, 9, 7):
                run_case(100, 4, 4, 4, workers, 2, batch, 8, True, 0, False)
                run_case(100, 2, 4, 6, workers, 2, batch, 8, True, 1, False)
                run_case(0, 2, 4, 6, workers, 2, batch, 0, True, 0, False)
        return
    if len(args) != 12:
        raise Error("usage: benchmark messages sources transforms sinks workers capacity batch work verify drop_every pinning")
    var messages = Int(args[1])
    var sources = Int(args[2])
    var transforms = Int(args[3])
    var sinks = Int(args[4])
    var workers = Int(args[5])
    var capacity = Int(args[6])
    var batch = Int(args[7])
    var work = Int(args[8])
    var verify = Int(args[9]) != 0
    var drop_every = Int(args[10])
    var pinning = Int(args[11]) != 0
    if messages < 0 or sources < 1 or transforms < 1 or sinks < 1 or workers < 1 or workers > sources + transforms + sinks or batch < 1 or work < 0 or drop_every < 0:
        raise Error("invalid pipeline workload")
    if capacity < 2 or (capacity & (capacity - 1)) != 0:
        raise Error("capacity must be a power of two >= 2")
    run_case(messages, sources, transforms, sinks, workers, capacity, batch, work, verify, drop_every, pinning)
