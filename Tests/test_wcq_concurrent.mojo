from std.sys import argv
from std.testing import assert_equal, assert_true
from std.collections import List

comptime _WCQPointer = UnsafePointer[WCQMPMCQueue, MutAnyOrigin]

def wcq_producer(q: _WCQPointer, control: UInt64, tid: Int, count: Int):
    try:
        for i in range(count):
            q[].push(UInt64(tid * count + i), tid)
            _ = _wcq_add(control + UInt64(24 + tid * 8), 1)
    except:
        _ = _wcq_add(control + 16, 1)

def wcq_consumer(q: _WCQPointer, control: UInt64, seen: UInt64, tid: Int, count: Int, total: Int, producers: Int, check_order: Bool):
    # Four producers, each with an increasing disjoint range.
    var last = List[Int]()
    for _ in range(4):
        last.append(-1)
    try:
        for _ in range(count):
            var value = q[].pop(tid)
            _ = _wcq_add(control + UInt64(24 + tid * 8), 1)
            if value >= UInt64(total):
                _ = _wcq_add(control + 16, 1)
            else:
                _ = _wcq_add(seen + value * 8, 1)
                if check_order:
                    var producer = Int(value) // (total // producers)
                    var offset = Int(value) % (total // producers)
                    if offset <= last[producer]:
                        _ = _wcq_add(control + 16, 1)
                    last[producer] = offset
    except:
        _ = _wcq_add(control + 16, 1)

@export
def mostream_wcq_test_worker(q: _WCQPointer, control: UInt64, seen: UInt64, tid_value: UInt64, count_value: UInt64, consumers_value: UInt64) abi("C"):
    # pthreads only launch this Mojo function; all queue logic remains Mojo.
    _ = _wcq_add(control, 1)
    while _wcq_load(control + 8) == 0:
        pass
    if _wcq_load(control + 8) == 2:
        return
    var tid = Int(tid_value)
    var count = Int(count_value)
    var consumers = Int(consumers_value)
    if tid < 4:
        wcq_producer(q, control, tid, count)
    else:
        wcq_consumer(q, control, seen, tid, 4 * count // consumers, 4 * count, 4, consumers == 1)

def dump_ring(ring: _WCQRing):
    print("head/tail/threshold", _wcq_load(ring.base + 8), _wcq_load(ring.base + 72), Int64(_wcq_load(ring.base + 128)))
    for tid in range(ring.threads):
        print("tid seq1 seq2 eidx tail head:", tid, _wcq_load(ring._record(tid, _WCQRing.SEQ1)), _wcq_load(ring._record(tid, _WCQRing.SEQ2)), _wcq_load(ring._record(tid, _WCQRing.EIDX)), _wcq_load(ring._record(tid, _WCQRing.TAIL)), _wcq_load(ring._record(tid, _WCQRing.HEAD)))
    if ring.half <= 8:
        for i in range(Int(ring.n)):
            var pair = _wcq_load2(ring._slot(UInt64(i * 4)))
            print("slot", i, "entry/note", pair.entry, pair.note)

@export
def mostream_wcq_test_dump(q: _WCQPointer) abi("C"):
    print("FREE RING")
    dump_ring(q[].free_indices)
    print("ALLOCATED RING")
    dump_ring(q[].allocated_indices)

def concurrent_case(capacity: Int, mode: Int, consumers: Int) raises:
    comptime producers = 4
    comptime count = 1500
    comptime total = producers * count
    var workers = producers + consumers
    var q = WCQMPMCQueue(capacity, workers, patience_enq=0 if mode == 1 else 16 if mode == 0 else 1, patience_deq=0 if mode == 1 else 64 if mode == 0 else 1, help_delay=1)
    var ptr = UnsafePointer(to=q).unsafe_origin_cast[MutAnyOrigin]()
    var control = external_call["mostream_wcq_alloc", UInt64](UInt64(24 + 8 * workers))
    var seen = external_call["mostream_wcq_alloc", UInt64](UInt64(total * 8))
    assert_true(control != 0 and seen != 0)
    for i in range(3 + workers):
        _wcq_store(control + UInt64(i * 8), 0)
    for i in range(total):
        _wcq_store(seen + UInt64(i * 8), 0)
    var result = external_call["mostream_wcq_test_run", Int32](ptr, control, seen, UInt64(count), UInt64(consumers))
    assert_equal(result, Int32(0))
    assert_equal(_wcq_load(control), UInt64(workers))
    assert_equal(_wcq_load(control + 16), UInt64(0))
    for i in range(total):
        assert_equal(_wcq_load(seen + UInt64(i * 8)), UInt64(1))
    assert_true(not q.try_pop(0))
    var slow_enq: UInt64 = 0
    var slow_deq: UInt64 = 0
    var helped: UInt64 = 0
    for tid in range(workers):
        var counters = q.slow_path_counts(tid)
        slow_enq += counters[0]
        slow_deq += counters[1]
        helped += counters[2]
    if mode == 1:
        assert_true(slow_enq > 0 and slow_deq > 0)
    print("wCQ: 4P/", consumers, "C capacity", capacity, "mode", mode, "items", total, "slow enq/deq/help", slow_enq, slow_deq, helped, "PASS")
    external_call["mostream_wcq_free", NoneType](seen)
    external_call["mostream_wcq_free", NoneType](control)

def main() raises:
    var mode = Int(argv()[1])
    concurrent_case(8, mode, 4)
    concurrent_case(32, mode, 4)
    concurrent_case(1024, mode, 4)
    concurrent_case(8, mode, 1)
