# Integration test: the public Pipeline API, replicated transforms, String
# payloads, a tiny communicator and EOS. No direct queue or TaskGroup calls.
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.sys import argv
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

struct QueueSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "QueueSource"
    var next: Int
    var messages: Int

    def __init__(out self, messages: Int):
        self.next = 0
        self.messages = messages

    def next_element(mut self) -> Optional[Int]:
        if self.next == self.messages:
            return None
        var result = self.next
        self.next += 1
        return result

@fieldwise_init
struct QueueTransform(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Int
    comptime OutType = String
    comptime name = "QueueTransform"

    def compute(mut self, var input: Int) -> Optional[String]:
        return String(input)

@fieldwise_init
struct QueueStringTransform(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = String
    comptime OutType = String
    comptime name = "QueueStringTransform"

    def compute(mut self, var input: String) -> Optional[String]:
        return input^

struct QueueSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = String
    comptime OutType = String
    comptime name = "QueueSink"
    var seen: Pointer[Int, MutUntrackedOrigin]
    var messages: Int

    def __init__(out self, seen: Pointer[Int, MutUntrackedOrigin], messages: Int):
        self.seen = seen
        self.messages = messages

    def consume_element(mut self, var input: String):
        # Invalid IDs are recorded without indexing outside the allocation.
        try:
            var index = Int(input)
            if index < 0 or index >= self.messages:
                self.seen.unsafe_offset(self.messages)[] += 1
            else:
                self.seen.unsafe_offset(index)[] += 1
        except:
            self.seen.unsafe_offset(self.messages)[] += 1


def main() raises:
    var args = argv()
    var messages = Int(args[1])
    var workers = Int(args[2])
    var capacity = Int(args[3])
    var cooperative = args[4] == "cooperative"
    var seen = unsafe_alloc[Int](messages + 1)
    for i in range(messages + 1):
        seen.unsafe_offset(i)[] = 0
    if args[5] == "mpmc":
        # The middle communicator has multiple producers AND consumers.
        var pipeline = Pipeline((seq(QueueSource(messages)),
                                 parallel(QueueTransform(), workers),
                                 parallel(QueueStringTransform(), workers),
                                 seq(QueueSink(seen, messages))))
        pipeline.setPinning(False)
        pipeline.setQueueSize(capacity)
        if cooperative:
            pipeline.run_cooperative(2)
        else:
            pipeline.run()
    else:
        var pipeline = Pipeline((seq(QueueSource(messages)),
                                 parallel(QueueTransform(), workers),
                                 seq(QueueSink(seen, messages))))
        pipeline.setPinning(False)
        pipeline.setQueueSize(capacity)
        if cooperative:
            pipeline.run_cooperative(2)
        else:
            pipeline.run()
    for i in range(messages):
        assert_equal(seen.unsafe_offset(i)[], 1)
    assert_equal(seen.unsafe_offset(messages)[], 0)
    seen.unsafe_free()
    print("PASS runtime queue", messages, workers, capacity, args[4], args[5])
