from std.memory.alloc import unsafe_alloc
from std.runtime.asyncrt import TaskGroup
from std.atomic import Atomic, Ordering
from std.collections import Optional, List
from std.sys.terminate import exit
from MoStream.Micheal_Scott import MichaelScottQueue


struct Payload(Copyable):
    var id: Int
    var text: String

    def __init__(out self, id: Int):
        self.id = id
        self.text = "owned payload storage for message " + String(id)


def run_case(capacity: Int, producers: Int, consumers: Int, use_try: Bool) raises:
    var messages = 2000
    var total = producers * messages
    var queue = MichaelScottQueue[Payload](capacity)
    var finished = Atomic[DType.uint64](0)
    var invalid = Atomic[DType.uint64](0)
    var seen = unsafe_alloc[Atomic[DType.uint64]](total)
    for i in range(total):
        seen[unsafe_offset=i] = Atomic[DType.uint64](0)

    @parameter
    def send(id: Int):
        var payload = Payload(id)
        if use_try:
            var pending = Optional(payload^)
            while pending:
                pending = queue.try_push(pending.take())
        else:
            queue.push(payload^)

    @parameter
    async def worker(thread_id: Int):
        if thread_id < producers:
            for i in range(messages):
                send(thread_id * messages + i)
            var previous = finished.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](1)
            if previous + 1 == UInt64(producers):
                for _ in range(consumers):
                    send(-1)
        else:
            while True:
                var item: Optional[Payload]
                if use_try:
                    item = queue.try_pop()
                    if not item:
                        continue
                else:
                    item = Optional(queue.pop())
                var payload = item.take()
                if payload.id == -1:
                    return
                if payload.id < 0 or payload.id >= total:
                    _ = invalid.fetch_add[ordering=Ordering.RELAXED](1)
                else:
                    _ = seen[unsafe_offset=payload.id].fetch_add[ordering=Ordering.RELAXED](1)
                    if payload.text != "owned payload storage for message " + String(payload.id):
                        _ = invalid.fetch_add[ordering=Ordering.RELAXED](1)

    var tasks = TaskGroup()
    for worker_id in range(producers + consumers):
        tasks.create_task(worker(worker_id))
    tasks.wait()
    _ = queue  # Keep allocation alive through legacy task captures.
    var valid = invalid.load[ordering=Ordering.RELAXED]() == 0
    for i in range(total):
        valid = valid and seen[unsafe_offset=i].load[ordering=Ordering.RELAXED]() == 1
        seen.unsafe_offset(i).unsafe_deinit_pointee()
    seen.unsafe_free()
    valid = valid and queue.estimated_len() == 0 and not queue.try_pop()
    if not valid:
        print("FAIL: MichaelScott exact-once", capacity, producers, consumers, use_try)
        exit(1)
    print("PASS: MichaelScott exact-once capacity/P/C/try", capacity, producers, consumers, use_try)


def main() raises:
    for capacity in [2, 3, 16, 1024]:
        run_case(capacity, 4, 4, True)
        run_case(capacity, 4, 4, False)
    run_case(4, 1, 8, True)
    run_case(4, 8, 1, True)
    print("PASS: MichaelScott concurrent suite with owned string payloads")
