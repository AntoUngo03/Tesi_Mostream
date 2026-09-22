"""Fair round-robin actors on a fixed set of workers, with resumable polls.

This deliberately excludes MoStream's ready/wait queues and parking protocol.
All variants use the same scheduler, payload, batching, and instrumentation.
"""

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory.alloc import unsafe_alloc
from std.runtime.asyncrt import TaskGroup
from std.sys import argv
from std.sys.info import size_of
from std.time import perf_counter_ns
from MoStream.communicator import MessageWrapper
from MoStream.MPMC_queue import MPMCQueue, Cell
from MoStream.Padded_FAA_queue import PaddedFAAQueue, PaddedFAASlot
from MoStream.Cooperative_FAA_queue import (
    CooperativeFAAQueue, PushOperation, PopOperation, PollStatus,
)


struct Value(Copyable):
    var id: Int
    var started_ns: Int

    def __init__(out self, id: Int, started_ns: Int):
        self.id = id
        self.started_ns = started_ns


comptime Payload = MessageWrapper[Value]
comptime SAMPLE_EVERY = 256


struct ActorState(Movable):
    var push: PushOperation[Payload]
    var pop: PopOperation[Payload]
    var next: Int
    var done: Bool
    var count: UInt64
    var checksum: UInt64
    var retries: UInt64
    var waits: UInt64
    var activations: UInt64

    def __init__(out self):
        self.push = PushOperation[Payload]()
        self.pop = PopOperation[Payload]()
        self.next = 0
        self.done = False
        self.count = 0
        self.checksum = 0
        self.retries = 0
        self.waits = 0
        self.activations = 0


def run[kind: Int](messages: Int, producers: Int, consumers: Int,
                  capacity: Int, workers: Int, batch: Int, verify: Bool) raises:
    # Only the selected queue is touched in the timed region. The other two
    # allocations keep compile-time dispatch simple and are outside the timer.
    var cas = MPMCQueue[Payload](capacity)
    var padded = PaddedFAAQueue[Payload](capacity)
    var cooperative = CooperativeFAAQueue[Payload, kind == 3](capacity, producers)
    var remaining = Atomic[DType.uint64](UInt64(producers))
    var total = messages * producers
    var actor_count = producers + consumers
    var actors = unsafe_alloc[ActorState](actor_count, alignment=64)
    for i in range(actor_count):
        actors.unsafe_offset(i).unsafe_write(ActorState())
    var sample_count = (total + SAMPLE_EVERY - 1) // SAMPLE_EVERY
    var samples = unsafe_alloc[Int](max(sample_count, 1))
    for i in range(sample_count):
        samples[unsafe_offset=i] = 0
    var seen = unsafe_alloc[Atomic[DType.uint64]](max(total, 1) if verify else 1)
    if verify:
        for i in range(total):
            seen.unsafe_offset(i).unsafe_write(Atomic[DType.uint64](0))
    var invalid = Atomic[DType.uint64](0)
    var start = perf_counter_ns()

    @parameter
    async def worker(worker_id: Int):
        var unfinished = (actor_count - 1 - worker_id) // workers + 1
        while unfinished > 0:
            for actor_id in range(worker_id, actor_count, workers):
                var actor = actors.unsafe_offset(actor_id)
                if actor[].done:
                    continue
                actor[].activations += 1
                for _ in range(batch):
                    var status = PollStatus.WAIT
                    if actor_id < producers:
                        if actor[].next == messages:
                            comptime if kind >= 2:
                                cooperative.producer_finished()
                            else:
                                _ = remaining.fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)
                            actor[].done = True
                            unfinished -= 1
                            break
                        if not actor[].push.item:
                            var id = actor_id * messages + actor[].next
                            var timestamp = 0
                            if id % SAMPLE_EVERY == 0:
                                timestamp = perf_counter_ns()
                            actor[].push.item = Optional(Payload(Value(id, timestamp), False))
                        comptime if kind == 0:
                            actor[].push.item = cas.try_push(actor[].push.item.take())
                            if not actor[].push.item:
                                status = PollStatus.SUCCESS
                        elif kind == 1:
                            actor[].push.item = padded.try_push(actor[].push.item.take())
                            if not actor[].push.item:
                                status = PollStatus.SUCCESS
                        else:
                            status = cooperative.poll_push(actor[].push)
                        if status == PollStatus.SUCCESS:
                            actor[].next += 1
                    else:
                        comptime if kind < 2:
                            comptime if kind == 0:
                                actor[].pop.item = cas.try_pop()
                            else:
                                actor[].pop.item = padded.try_pop()
                            if actor[].pop.item:
                                status = PollStatus.SUCCESS
                            elif remaining.load[ordering=Ordering.ACQUIRE]() == 0:
                                # Synchronize with close, then recheck before EOS.
                                comptime if kind == 0:
                                    actor[].pop.item = cas.try_pop()
                                else:
                                    actor[].pop.item = padded.try_pop()
                                status = PollStatus.SUCCESS if actor[].pop.item else PollStatus.CLOSED
                        else:
                            status = cooperative.poll_pop(actor[].pop)
                        if status == PollStatus.CLOSED:
                            actor[].done = True
                            unfinished -= 1
                            break
                        if status == PollStatus.SUCCESS:
                            var envelope = actor[].pop.item.take()
                            var value = envelope.data.take()
                            if value.id < 0 or value.id >= total or envelope.eos:
                                _ = invalid.fetch_add[ordering=Ordering.RELAXED](1)
                            else:
                                actor[].count += 1
                                actor[].checksum += UInt64(value.id)
                                if verify:
                                    _ = seen[unsafe_offset=value.id].fetch_add[ordering=Ordering.RELAXED](1)
                                if value.id % SAMPLE_EVERY == 0:
                                    samples[unsafe_offset=value.id // SAMPLE_EVERY] = perf_counter_ns() - value.started_ns
                    if status == PollStatus.RETRY:
                        actor[].retries += 1
                        break
                    if status == PollStatus.WAIT:
                        actor[].waits += 1
                        break

    var tasks = TaskGroup()
    for i in range(workers):
        tasks.create_task(worker(i))
    tasks.wait()
    _ = cas
    _ = padded
    _ = cooperative
    var elapsed = perf_counter_ns() - start
    var count: UInt64 = 0
    var checksum: UInt64 = 0
    var retries: UInt64 = 0
    var waits: UInt64 = 0
    var activations: UInt64 = 0
    var pending = False
    for i in range(actor_count):
        var actor = actors.unsafe_offset(i)
        count += actor[].count
        checksum += actor[].checksum
        retries += actor[].retries
        waits += actor[].waits
        activations += actor[].activations
        pending = pending or actor[].push.reserved or actor[].pop.reserved
        pending = pending or Bool(actor[].push.item) or Bool(actor[].pop.item)
    var valid = count == UInt64(total) and checksum == UInt64(total) * UInt64(max(total - 1, 0)) // 2
    valid = valid and not pending and invalid.load[ordering=Ordering.RELAXED]() == 0
    if verify:
        for i in range(total):
            valid = valid and seen[unsafe_offset=i].load[ordering=Ordering.RELAXED]() == 1
            seen.unsafe_offset(i).unsafe_deinit_pointee()
    print("RESULT", kind, Float64(elapsed) / 1_000_000.0,
          Float64(total) * 1000.0 / Float64(elapsed), count, retries, waits, activations, valid)
    for i in range(sample_count):
        print("LATENCY_NS", samples[unsafe_offset=i])
    print("LAYOUT", size_of[Payload](), size_of[Cell[Payload]](), size_of[PaddedFAASlot[Payload]]())
    for i in range(actor_count):
        actors.unsafe_offset(i).unsafe_deinit_pointee()
    actors.unsafe_free()
    samples.unsafe_free()
    seen.unsafe_free()
    if not valid:
        raise Error("count, checksum, ownership, or exact delivery validation failed")


def main() raises:
    var args = argv()
    if len(args) != 9:
        raise Error("usage: compare kind messages producers consumers capacity workers batch verify")
    var kind = Int(args[1])
    var messages = Int(args[2])
    var producers = Int(args[3])
    var consumers = Int(args[4])
    var capacity = Int(args[5])
    var workers = Int(args[6])
    var batch = Int(args[7])
    var verify = Int(args[8]) != 0
    if messages < 0 or producers < 1 or consumers < 1 or workers < 1 or workers > producers + consumers or batch < 1:
        raise Error("invalid workload parameters")
    if capacity < 2 or (capacity & (capacity - 1)) != 0:
        raise Error("capacity must be a power of two, at least two")
    if kind == 0:
        run[0](messages, producers, consumers, capacity, workers, batch, verify)
    elif kind == 1:
        run[1](messages, producers, consumers, capacity, workers, batch, verify)
    elif kind == 2:
        run[2](messages, producers, consumers, capacity, workers, batch, verify)
    elif kind == 3:
        run[3](messages, producers, consumers, capacity, workers, batch, verify)
    else:
        raise Error("kind must be 0=Vyukov, 1=PaddedFAA, 2=bounded CAS, 3=cooperative FAA")
