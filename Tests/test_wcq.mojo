from std.testing import assert_equal, assert_true

def check_wcq(capacity: Int, slow: Bool) raises:
    var q = WCQMPMCQueue(capacity, 2, patience_enq=0 if slow else 16, patience_deq=0 if slow else 64, help_delay=1)
    assert_true(not q.try_pop(0))
    for cycle in range(100):
        for i in range(capacity):
            assert_true(not q.try_push(UInt64(cycle * capacity + i), 0))
        var rejected = q.try_push(UInt64(999999), 0)
        assert_true(Bool(rejected))
        assert_equal(rejected.take(), UInt64(999999))
        for i in range(capacity):
            var item = q.try_pop(1)
            assert_true(Bool(item))
            assert_equal(item.take(), UInt64(cycle * capacity + i))
        assert_true(not q.try_pop(1))
    q.push(UInt64(0), 0)
    q.push(UInt64(18446744073709551615), 0)
    var moved = q^
    assert_equal(moved.pop(1), UInt64(0))
    assert_equal(moved.pop(1), UInt64(18446744073709551615))
    if slow:
        var producer = moved.slow_path_counts(0)
        var consumer = moved.slow_path_counts(1)
        assert_true(producer[0] > 0 and producer[1] > 0)
        assert_true(consumer[0] > 0 and consumer[1] > 0)

def check_help() raises:
    # Publish a request for tid 0 and leave its owner suspended. tid 1 alone
    # must finish it; resuming tid 0 must not publish the index twice.
    var ring = _WCQRing(4, 2, False, 0, 0, 1)
    var previous = _WCQRing.REQUEST_SEED | UInt64(4)
    var encoded = UInt64(2) ^ (ring.n - 1)
    _wcq_store(ring._record(0, _WCQRing.TAIL), previous)
    _wcq_store(ring._record(0, _WCQRing.INIT_TAIL), previous)
    _wcq_store(ring._record(0, _WCQRing.EIDX), encoded)
    _wcq_store(ring._record(0, _WCQRing.SEQ2), 1)
    ring._help(1)
    assert_equal(_wcq_load(ring._record(1, _WCQRing.HELPED)), UInt64(1))
    ring._do_enq_slow(previous, encoded, 0, 0, 1)
    _wcq_store(ring._record(0, _WCQRing.SEQ1), 2)
    _wcq_store(ring._record(0, _WCQRing.EIDX), 0)
    var item = ring.dequeue(0)
    assert_true(Bool(item))
    assert_equal(item.take(), UInt64(2))
    assert_true(not ring.dequeue(0))

    # Suspend a dequeue owner after publishing its request, then help it.
    ring.enqueue(1, 1)
    var seq = _wcq_load(ring._record(0, _WCQRing.SEQ1))
    previous = _WCQRing.REQUEST_SEED | (seq << 2)
    _wcq_store(ring._record(0, _WCQRing.HEAD), previous)
    _wcq_store(ring._record(0, _WCQRing.INIT_HEAD), previous)
    _wcq_store(ring._record(0, _WCQRing.EIDX), 1)
    _wcq_store(ring._record(0, _WCQRing.SEQ2), seq)
    _wcq_store(ring._record(1, _WCQRing.NEXT), 0)
    _wcq_store(ring._record(1, _WCQRing.CHECK), 1)
    ring._help(1)
    var h = _wcq_load(ring._record(0, _WCQRing.HEAD))
    assert_true((h & _WCQRing.FIN) != 0)
    ring._do_deq_slow(previous, 0, 0, seq)
    _wcq_store(ring._record(0, _WCQRing.SEQ1), seq + 1)
    _wcq_store(ring._record(0, _WCQRing.EIDX), 0)
    var ticket = h & ~UInt64(3)
    var entry = _wcq_load(ring._slot(ticket) + 8)
    assert_equal(entry & (ring.n - 1), UInt64(1))
    ring._consume(ticket, entry)
    assert_true(not ring.dequeue(0))

def check_invalid() raises:
    var caught = 0
    for cap in range(4):
        try:
            var q = WCQMPMCQueue(cap)
        except:
            caught += 1
    assert_equal(caught, 4)
    try:
        var q = WCQMPMCQueue(6)
    except:
        caught += 1
    try:
        var q = WCQMPMCQueue(4, 5)
    except:
        caught += 1
    try:
        var q = WCQMPMCQueue(4, 0)
    except:
        caught += 1
    var q = WCQMPMCQueue(4, 2)
    try:
        _ = q.try_push(1, -1)
    except:
        caught += 1
    try:
        _ = q.try_pop(2)
    except:
        caught += 1
    assert_equal(caught, 9)

def check_phase2_and_partial_publication() raises:
    # Raw UInt64 addresses do not carry an ownership origin. Keep all ring
    # owners alive until AFTER the final external atomic reads (Mojo 1.1 uses
    # eager destruction at last use, including extraction of a raw address).
    var retired = List[_WCQRing]()
    var ring = _WCQRing(4, 2, False, 0, 0, 1)
    var local = ring._record(0, _WCQRing.TAIL)
    # The winning helper is suspended immediately after the global CAS2.
    _wcq_store(local, _WCQRing.INC)
    _wcq_store(ring._record(1, _WCQRing.PLOCAL), local)
    _wcq_store(ring._record(1, _WCQRing.PCNT), 0)
    _wcq_store(ring._record(1, _WCQRing.PSEQ2), 1)
    assert_true(_wcq_cas2(ring.base + 64, _WCQPair(0, 0), _WCQPair(4, UInt64(65538))))
    var global_value = ring._load_global(ring.base + 64, local)
    assert_true(Bool(global_value))
    assert_equal(global_value.take(), UInt64(4))
    assert_equal(_wcq_load(local), UInt64(0))
    assert_equal(_wcq_load2(ring.base + 64).note, UInt64(0))

    # A reader still sees an old global token, but its owner has reused phase2.
    # It must not clear INC on the NEW request before that request's global CAS.
    _wcq_store(local, UInt64(4) | _WCQRing.INC)
    _wcq_store(ring._record(1, _WCQRing.PSEQ1), 2)
    _wcq_store(ring._record(1, _WCQRing.PSEQ2), 2)
    _wcq_store(ring._record(1, _WCQRing.PCNT), 4)
    assert_true(_wcq_cas2(ring.base + 64, _WCQPair(4, 0), _WCQPair(4, UInt64(65538))))
    _ = ring._load_global(ring.base + 64, local)
    assert_equal(_wcq_load(local), UInt64(6))
    assert_equal(_wcq_load2(ring.base + 64).note, UInt64(0))

    # Suspend an enqueuer after publishing Enq=0. The consumer must set FIN
    # before marking the cell consumed, so the owner cannot enqueue again.
    var partial = _WCQRing(4, 2, False, 0, 0, 1)
    var ticket = _wcq_add(partial.base + 72, 4)
    var encoded = UInt64(3) ^ (partial.n - 1)
    var pair = _wcq_load2(partial._slot(ticket))
    _wcq_store(partial._record(0, _WCQRing.TAIL), ticket)
    _wcq_store(partial._record(0, _WCQRing.INIT_TAIL), ticket)
    _wcq_store(partial._record(0, _WCQRing.EIDX), encoded)
    _wcq_store(partial._record(0, _WCQRing.SEQ2), 1)
    assert_true(_wcq_cas2(partial._slot(ticket), pair, _WCQPair(partial._cycle(ticket) ^ encoded ^ partial.n, pair.note)))
    _wcq_store(partial.base + 128, 3 * partial.half - 1)
    # Avoid proactive help: exercise the consumer's _finalize specifically.
    _wcq_store(partial._record(1, _WCQRing.CHECK), 100)
    var item = partial.dequeue(1)
    assert_true(Bool(item))
    assert_equal(item.take(), UInt64(3))
    assert_true((_wcq_load(partial._record(0, _WCQRing.TAIL)) & _WCQRing.FIN) != 0)
    partial._do_enq_slow(ticket, encoded, 0, 0, 1)
    _wcq_store(partial._record(0, _WCQRing.SEQ1), 2)
    _wcq_store(partial._record(0, _WCQRing.EIDX), 0)
    assert_true(not partial.dequeue(1))

    # A cell invalidated by an early consumer is not a published element.
    var invalidated = _WCQRing(4, 2, False, 0, 0, 1)
    ticket = _wcq_add(invalidated.base + 72, 4)
    _wcq_store(invalidated._slot(ticket) + 8, invalidated._cycle(ticket) ^ UInt64(1))
    assert_true(not invalidated._try_enq_slow(ticket, encoded, 0))

    # Empty detection races with a helper that already advanced localHead.
    # Failing to mark the old ticket FIN must not complete the newer ticket.
    var empty_race = _WCQRing(4, 2, False, 0, 0, 1)
    _wcq_store(empty_race._record(0, _WCQRing.HEAD), 8)
    assert_true(not empty_race._try_deq_slow(4, 0))
    assert_equal(_wcq_load(empty_race._record(0, _WCQRing.HEAD)), UInt64(8))
    assert_true(empty_race._try_deq_slow(8, 0))
    assert_equal(_wcq_load(empty_race._record(0, _WCQRing.HEAD)), UInt64(9))

    # The initial value must differ from the FIRST reserved ticket too.
    # Otherwise a second helper could increment again before processing it.
    var first = _WCQRing(4, 2, False, 0, 0, 1)
    var initial = _WCQRing.REQUEST_SEED | UInt64(4)
    local = first._record(0, _WCQRing.HEAD)
    _wcq_store(local, initial)
    var first_helper = first._slow_inc(first.base, local, initial, False, 1)
    var second_helper = first._slow_inc(first.base, local, initial, False, 0)
    assert_true(first_helper[0] and second_helper[0])
    assert_equal(first_helper[1], UInt64(0))
    assert_equal(second_helper[1], UInt64(0))
    assert_equal(_wcq_load(first.base + 8), UInt64(4))
    retired.append(ring^)
    retired.append(partial^)
    retired.append(invalidated^)
    retired.append(empty_race^)
    retired.append(first^)

def main() raises:
    check_invalid()
    for power in range(2, 11):
        var capacity = 1 << power
        check_wcq(capacity, False)
        check_wcq(capacity, True)
    check_help()
    check_phase2_and_partial_publication()
    print("wCQ: FIFO, full/empty, wraps, UInt64, move, invalid arguments, forced slow paths and suspended owners: PASS")
