# Native Mojo adaptation of wCQ (Nikolaev & Ravindran, SPAA 2022).
# Reference: paper Figures 3--7 and wfring_cas2.h, revision
# 708c0052872950dcb15b487fa7a5dd77ce2a2746, Copyright (c) 2021 Ruslan Nikolaev.
# This adaptation uses the MIT license; see docs/WCQ_LICENSE.txt.
#
# The queue algorithm, descriptors, helping and two-ring indirection are Mojo.
# wcq_atomics.c supplies ONLY aligned allocation and CPU atomic primitives.
# Experimental port: tests do not constitute a proof of wait-freedom.
# Each concurrent participant must own a distinct tid in [0, num_threads).
# Move/destruction require quiescence. Calls do not allocate memory.
# UInt64 payloads use their full range. No payload value denotes empty.

from std.ffi import external_call
from std.memory import UnsafePointer
from std.collections import Optional


@always_inline
def _wcq_load(address: UInt64) -> UInt64:
    return external_call["mostream_wcq_load", UInt64](address)

@always_inline
def _wcq_store(address: UInt64, value: UInt64):
    external_call["mostream_wcq_store", NoneType](address, value)

@always_inline
def _wcq_add(address: UInt64, value: UInt64) -> UInt64:
    return external_call["mostream_wcq_add", UInt64](address, value)

@always_inline
def _wcq_or(address: UInt64, value: UInt64):
    _ = external_call["mostream_wcq_or", UInt64](address, value)

@always_inline
def _wcq_cas(address: UInt64, expected: UInt64, desired: UInt64) -> Bool:
    return external_call["mostream_wcq_cas", Int32](address, expected, desired) != 0

@fieldwise_init
struct _WCQPair(Copyable, Movable):
    var entry: UInt64
    var note: UInt64

@always_inline
def _wcq_load2(address: UInt64) -> _WCQPair:
    var entry: UInt64 = 0
    var note: UInt64 = 0
    external_call["mostream_wcq_load2", NoneType](address, UnsafePointer(to=entry), UnsafePointer(to=note))
    return _WCQPair(entry, note)

@always_inline
def _wcq_cas2(address: UInt64, old: _WCQPair, new: _WCQPair) -> Bool:
    return external_call["mostream_wcq_cas2", Int32](address, old.entry, old.note, new.entry, new.note) != 0

@always_inline
def _wcq_before(x: UInt64, y: UInt64) -> Bool:
    # Modular comparison, as in __wfring_cmp. Assumes distances < 2**63.
    return Int64(x - y) < 0


struct _WCQRing(Movable):
    # Header: head pair at 0, tail pair at 64, threshold at 128.
    # Cells: [Note, Value] (16 bytes), starting at 192.
    # Descriptors: 128 bytes per participant after the cells.
    comptime FIN = UInt64(1)
    comptime INC = UInt64(2)
    comptime MAX = UInt64(18446744073709551615)
    comptime REQUEST_SEED = UInt64(9223372036854775808)
    # Descriptor field numbers (each field is one UInt64):
    comptime CHECK = 0
    comptime NEXT = 1
    comptime SEQ1 = 2
    comptime TAIL = 3
    comptime INIT_TAIL = 4
    comptime HEAD = 5
    comptime INIT_HEAD = 6
    comptime EIDX = 7
    comptime SEQ2 = 8
    comptime PSEQ1 = 9
    comptime PLOCAL = 10
    comptime PCNT = 11
    comptime PSEQ2 = 12
    comptime ENQ_SLOW = 13
    comptime DEQ_SLOW = 14
    comptime HELPED = 15

    var base: UInt64
    var records: UInt64
    # half is n in the paper; n below is the physical ring length, 2*n.
    var half: UInt64
    var n: UInt64
    var order: Int
    var threads: Int
    var patience_enq: Int
    var patience_deq: Int
    var help_delay: Int

    def __init__(out self, capacity: Int, threads: Int, full: Bool, patience_enq: Int, patience_deq: Int, help_delay: Int) raises:
        self.half = UInt64(capacity)
        self.n = self.half * 2
        self.order = 0
        var size = capacity
        while size > 1:
            self.order += 1
            size >>= 1
        self.threads = threads
        self.patience_enq = patience_enq
        self.patience_deq = patience_deq
        self.help_delay = help_delay
        self.base = external_call["mostream_wcq_alloc", UInt64](UInt64(192) + 16 * self.n + UInt64(128 * threads))
        if self.base == 0:
            raise Error("wCQ: allocation failed or CPU lacks CMPXCHG16B")
        self.records = self.base + 192 + 16 * self.n
        _wcq_store(self.base, 0)
        _wcq_store(self.base + 8, 0)
        _wcq_store(self.base + 64, 0)
        _wcq_store(self.base + 72, UInt64(capacity * 4) if full else UInt64(0))
        _wcq_store(self.base + 128, 3 * self.half - 1 if full else Self.MAX)
        for i in range(Int(self.n)):
            var address = self._slot(UInt64(i * 4))
            _wcq_store(address, Self.MAX - self.n)
            _wcq_store(address + 8, Self.MAX)
            if full and i < capacity:
                # Sequential indices, independent of the physical cache remap.
                _wcq_store(address, Self.MAX)
                _wcq_store(address + 8, 3 * self.n + UInt64(i))
        for tid in range(threads):
            for field in range(16):
                _wcq_store(self._record(tid, field), 0)
            _wcq_store(self._record(tid, Self.CHECK), UInt64(help_delay))
            _wcq_store(self._record(tid, Self.NEXT), UInt64((tid + 1) % threads))
            _wcq_store(self._record(tid, Self.SEQ1), 1)
            _wcq_store(self._record(tid, Self.PSEQ1), 1)
            _wcq_store(self._record(tid, Self.TAIL), Self.MAX)
            _wcq_store(self._record(tid, Self.HEAD), Self.MAX)

    def __init__(out self, *, deinit move: Self):
        self.base = move.base
        self.records = move.records
        self.half = move.half
        self.n = move.n
        self.order = move.order
        self.threads = move.threads
        self.patience_enq = move.patience_enq
        self.patience_deq = move.patience_deq
        self.help_delay = move.help_delay

    def __deinit__(deinit self):
        external_call["mostream_wcq_free", NoneType](self.base)

    @always_inline
    def _record(self, tid: Int, field: Int) -> UInt64:
        return self.records + UInt64(tid * 128 + field * 8)

    @always_inline
    def _slot(self, ticket: UInt64) -> UInt64:
        # Same cache permutation as upstream WFRING_MIN=2 (64-byte cache line).
        var i = ticket >> 2
        var j = ((i & (self.n - 1)) >> UInt64(self.order - 1)) | ((i << 2) & (self.n - 1))
        return self.base + 192 + j * 16

    @always_inline
    def _cycle(self, ticket: UInt64) -> UInt64:
        return ticket | (4 * self.n - 1)

    def _catchup(self, tail: UInt64, head: UInt64):
        # Explicit bound: catchup is an optimization, not a completion condition.
        var old_tail = tail
        var new_head = head
        for _ in range(self.threads + 1):
            if not _wcq_before(old_tail, new_head):
                return
            if _wcq_cas(self.base + 72, old_tail, new_head):
                return
            old_tail = _wcq_load(self.base + 72)
            new_head = _wcq_load(self.base + 8)

    def _finalize(self, ticket: UInt64):
        for tid in range(self.threads):
            var address = self._record(tid, Self.TAIL)
            var local = _wcq_load(address)
            if (local & ~UInt64(3)) == ticket:
                _ = _wcq_cas(address, ticket, ticket | Self.FIN)
                return

    def _consume(self, ticket: UInt64, entry: UInt64):
        if (entry & self.n) == 0:
            self._finalize(ticket)
        _wcq_or(self._slot(ticket) + 8, 2 * self.n - 1)

    def _load_global(self, global_address: UInt64, local_address: UInt64) -> Optional[UInt64]:
        # Figure 7, load_global_help_phase2. Encode BOTH owner and generation:
        # a delayed reader of an old global pair must not complete a descriptor
        # that its owner has already recycled for a different increment.
        while True:
            if (_wcq_load(local_address) & Self.FIN) != 0:
                return None
            var gp = _wcq_load2(global_address)
            if gp.note == 0:
                return Optional(gp.entry)
            var owner = Int(gp.note & UInt64(65535)) - 1
            var generation = gp.note >> 16
            var seq = _wcq_load(self._record(owner, Self.PSEQ2))
            var local = _wcq_load(self._record(owner, Self.PLOCAL))
            var count = _wcq_load(self._record(owner, Self.PCNT))
            if seq == generation and _wcq_load(self._record(owner, Self.PSEQ1)) == seq:
                _ = _wcq_cas(local, count | Self.INC, count)
            if _wcq_cas2(global_address, gp, _WCQPair(gp.entry, 0)):
                return Optional(gp.entry)

    def _slow_inc(self, global_address: UInt64, local: UInt64, previous: UInt64, decrement: Bool, helper: Int) -> Tuple[Bool, UInt64]:
        # Phase-2 descriptor belongs to the executing helper (paper Figure 7).
        var prev = previous
        var count: UInt64 = 0
        while True:
            var loaded = self._load_global(global_address, local)
            var won = False
            if loaded:
                count = loaded.take()
                won = _wcq_cas(local, prev, count | Self.INC)
            if not won:
                prev = _wcq_load(local)
                if (prev & Self.FIN) != 0:
                    return (False, prev)
                if (prev & Self.INC) == 0:
                    return (True, prev)
                count = prev & ~UInt64(3)
            else:
                prev = count | Self.INC
            var seq = (_wcq_load(self._record(helper, Self.PSEQ1)) + 1) & UInt64(281474976710655)
            _wcq_store(self._record(helper, Self.PSEQ1), seq)
            _wcq_store(self._record(helper, Self.PLOCAL), local)
            _wcq_store(self._record(helper, Self.PCNT), count)
            _wcq_store(self._record(helper, Self.PSEQ2), seq)
            # A logical increment is FOUR: low two bits encode FIN and INC.
            var token = (seq << 16) | UInt64(helper + 1)
            if _wcq_cas2(global_address, _WCQPair(count, 0), _WCQPair(count + 4, token)):
                break
        if decrement:
            _ = _wcq_add(self.base + 128, Self.MAX)
        _ = _wcq_cas(local, count | Self.INC, count)
        var token = (_wcq_load(self._record(helper, Self.PSEQ1)) << 16) | UInt64(helper + 1)
        _ = _wcq_cas2(global_address, _WCQPair(count + 4, token), _WCQPair(count + 4, 0))
        return (True, count)

    def _try_enq_slow(self, ticket: UInt64, encoded: UInt64, target: Int) -> Bool:
        var address = self._slot(ticket)
        var cycle = self._cycle(ticket)
        while True:
            var pair = _wcq_load2(address)
            var entry = pair.entry
            var ecycle = self._cycle(entry)
            if _wcq_before(ecycle, cycle) and _wcq_before(pair.note, cycle):
                var empty = (entry & (self.n - 1)) >= self.n - 2
                var safe = (entry & (2 * self.n)) != 0 or not _wcq_before(ticket, _wcq_load(self.base + 8))
                if not empty or not safe:
                    if not _wcq_cas2(address, pair, _WCQPair(entry, cycle)):
                        continue
                    return False
                var partial = cycle ^ encoded ^ self.n
                if not _wcq_cas2(address, pair, _WCQPair(partial, pair.note)):
                    continue
                if _wcq_cas(self._record(target, Self.TAIL), ticket, ticket | Self.FIN):
                    _ = _wcq_cas2(address, _WCQPair(partial, pair.note), _WCQPair(partial | self.n, pair.note))
                _wcq_store(self.base + 128, 3 * self.half - 1)
                return True
            # A dequeuer may already have invalidated this ticket without data.
            # Only an actual publication (or its consumed marker) is success.
            if ecycle == cycle and (entry & (self.n - 1)) != self.n - 2:
                _ = _wcq_cas(self._record(target, Self.TAIL), ticket, ticket | Self.FIN)
                _wcq_store(self.base + 128, 3 * self.half - 1)
                return True
            return False

    def _do_enq_slow(self, previous: UInt64, encoded: UInt64, target: Int, helper: Int, sequence: UInt64):
        var ticket = previous
        while _wcq_load(self._record(target, Self.SEQ1)) == sequence:
            var step = self._slow_inc(self.base + 64, self._record(target, Self.TAIL), ticket, False, helper)
            ticket = step[1]
            if not step[0]:
                return
            if _wcq_load(self._record(target, Self.SEQ1)) != sequence:
                return
            if self._try_enq_slow(ticket, encoded, target):
                return

    def _try_deq_slow(self, ticket: UInt64, target: Int) -> Bool:
        var address = self._slot(ticket)
        var cycle = self._cycle(ticket)
        while True:
            var pair = _wcq_load2(address)
            var entry = pair.entry
            var ecycle = self._cycle(entry)
            var index = entry & (self.n - 1)
            if ecycle == cycle and index != self.n - 2:
                _ = _wcq_cas(self._record(target, Self.HEAD), ticket, ticket | Self.FIN)
                return True
            var new_entry = cycle ^ ((~entry) & (2 * self.n)) ^ UInt64(1)
            if index < self.n - 2:
                if _wcq_before(ecycle, cycle) and _wcq_before(pair.note, cycle):
                    if not _wcq_cas2(address, pair, _WCQPair(entry, cycle)):
                        continue
                    pair.note = cycle
                new_entry = entry & ~(2 * self.n)
            if _wcq_before(ecycle, cycle):
                if not _wcq_cas2(address, pair, _WCQPair(new_entry, pair.note)):
                    continue
            break
        var tail = _wcq_load(self.base + 72)
        if not _wcq_before(ticket + 4, tail):
            self._catchup(tail, ticket + 4)
        if Int64(_wcq_load(self.base + 128)) < 0:
            if _wcq_cas(self._record(target, Self.HEAD), ticket, ticket | Self.FIN):
                return True
            # Another helper can have advanced HEAD after observing a positive
            # threshold. A failed FIN CAS is not completion of that new ticket.
            return (_wcq_load(self._record(target, Self.HEAD)) & Self.FIN) != 0
        return False

    def _do_deq_slow(self, previous: UInt64, target: Int, helper: Int, sequence: UInt64):
        var ticket = previous
        while _wcq_load(self._record(target, Self.SEQ1)) == sequence:
            var step = self._slow_inc(self.base, self._record(target, Self.HEAD), ticket, True, helper)
            ticket = step[1]
            if not step[0]:
                return
            if _wcq_load(self._record(target, Self.SEQ1)) != sequence:
                return
            if self._try_deq_slow(ticket, target):
                return

    def _help(self, helper: Int):
        var check = self._record(helper, Self.CHECK)
        if _wcq_add(check, Self.MAX) != 1:
            return
        _wcq_store(check, UInt64(self.help_delay))
        var next_address = self._record(helper, Self.NEXT)
        var target = Int(_wcq_load(next_address))
        _wcq_store(next_address, UInt64((target + 1) % self.threads))
        if target == helper:
            return
        var seq = _wcq_load(self._record(target, Self.SEQ2))
        var eidx = _wcq_load(self._record(target, Self.EIDX))
        var ticket = _wcq_load(self._record(target, Self.INIT_HEAD if eidx == 1 else Self.INIT_TAIL))
        if eidx == 0 or _wcq_load(self._record(target, Self.SEQ1)) != seq:
            return
        _ = _wcq_add(self._record(helper, Self.HELPED), 1)
        if eidx == 1:
            self._do_deq_slow(ticket, target, helper, seq)
        else:
            self._do_enq_slow(ticket, eidx, target, helper, seq)

    def enqueue(self, index: UInt64, tid: Int):
        self._help(tid)
        var encoded = index ^ (self.n - 1)
        var ticket: UInt64 = 0
        for _ in range(self.patience_enq):
            ticket = _wcq_add(self.base + 72, 4)
            var address = self._slot(ticket) + 8
            var cycle = self._cycle(ticket)
            while True:
                var entry = _wcq_load(address)
                var ecycle = self._cycle(entry)
                var empty = (entry & (self.n - 1)) >= self.n - 2
                var safe = (entry & (2 * self.n)) != 0 or not _wcq_before(ticket, _wcq_load(self.base + 8))
                if not (_wcq_before(ecycle, cycle) and empty and safe):
                    break
                if _wcq_cas(address, entry, cycle ^ encoded):
                    _wcq_store(self.base + 128, 3 * self.half - 1)
                    return
        _ = _wcq_add(self._record(tid, Self.ENQ_SLOW), 1)
        var seq = _wcq_load(self._record(tid, Self.SEQ1))
        if self.patience_enq == 0:
            # No failed fast ticket exists. Use a generation-specific sentinel
            # outside real counter values, not the first ticket to be reserved.
            # Two helpers must distinguish initial state from the first result.
            ticket = Self.REQUEST_SEED | (seq << 2)
        _wcq_store(self._record(tid, Self.TAIL), ticket)
        _wcq_store(self._record(tid, Self.INIT_TAIL), ticket)
        _wcq_store(self._record(tid, Self.EIDX), encoded)
        _wcq_store(self._record(tid, Self.SEQ2), seq)
        self._do_enq_slow(ticket, encoded, tid, tid, seq)
        _wcq_store(self._record(tid, Self.SEQ1), seq + 1)
        _wcq_store(self._record(tid, Self.EIDX), 0)

    def dequeue(self, tid: Int) -> Optional[UInt64]:
        if Int64(_wcq_load(self.base + 128)) < 0:
            return None
        self._help(tid)
        var ticket: UInt64 = 0
        for _ in range(self.patience_deq):
            ticket = _wcq_add(self.base + 8, 4)
            var address = self._slot(ticket) + 8
            var cycle = self._cycle(ticket)
            while True:
                var entry = _wcq_load(address)
                var ecycle = self._cycle(entry)
                if ecycle == cycle:
                    self._consume(ticket, entry)
                    return Optional(entry & (self.n - 1))
                var new_entry = cycle ^ ((~entry) & (2 * self.n)) ^ UInt64(1)
                if (entry & (self.n - 1)) < self.n - 2:
                    new_entry = entry & ~(2 * self.n)
                    if entry == new_entry:
                        break
                if not _wcq_before(ecycle, cycle):
                    break
                if _wcq_cas(address, entry, new_entry):
                    break
            var tail = _wcq_load(self.base + 72)
            if not _wcq_before(ticket + 4, tail):
                self._catchup(tail, ticket + 4)
                _ = _wcq_add(self.base + 128, Self.MAX)
                return None
            if Int64(_wcq_add(self.base + 128, Self.MAX)) <= 0:
                return None
        _ = _wcq_add(self._record(tid, Self.DEQ_SLOW), 1)
        var seq = _wcq_load(self._record(tid, Self.SEQ1))
        if self.patience_deq == 0:
            ticket = Self.REQUEST_SEED | (seq << 2)
        _wcq_store(self._record(tid, Self.HEAD), ticket)
        _wcq_store(self._record(tid, Self.INIT_HEAD), ticket)
        _wcq_store(self._record(tid, Self.EIDX), 1)
        _wcq_store(self._record(tid, Self.SEQ2), seq)
        self._do_deq_slow(ticket, tid, tid, seq)
        _wcq_store(self._record(tid, Self.SEQ1), seq + 1)
        _wcq_store(self._record(tid, Self.EIDX), 0)
        ticket = _wcq_load(self._record(tid, Self.HEAD)) & ~UInt64(3)
        var entry = _wcq_load(self._slot(ticket) + 8)
        if self._cycle(entry) == self._cycle(ticket) and (entry & (self.n - 1)) < self.n - 2:
            self._consume(ticket, entry)
            return Optional(entry & (self.n - 1))
        return None


struct WCQMPMCQueue(Movable):
    """Bounded UInt64 MPMC queue; one exclusive tid per concurrent participant."""
    var capacity: Int
    var num_threads: Int
    var data: UInt64
    var free_indices: _WCQRing
    var allocated_indices: _WCQRing

    def __init__(out self, capacity: Int = 1024, num_threads: Int = 1, *, patience_enq: Int = 16, patience_deq: Int = 64, help_delay: Int = 16) raises:
        if capacity < 4 or (capacity & (capacity - 1)) != 0:
            raise Error("wCQ: capacity must be a power of two >= 4")
        if num_threads < 1 or num_threads > capacity or num_threads > 65535:
            raise Error("wCQ: require 1 <= num_threads <= min(capacity, 65535)")
        if patience_enq < 0 or patience_deq < 0 or help_delay < 1:
            raise Error("wCQ: patience >= 0 and help_delay >= 1 required")
        # Conservative limits also avoid overflowing the allocation arithmetic.
        if UInt64(capacity) > (UInt64(1) << 40):
            raise Error("wCQ: capacity exceeds the supported bound")
        self.capacity = capacity
        self.num_threads = num_threads
        self.free_indices = _WCQRing(capacity, num_threads, True, patience_enq, patience_deq, help_delay)
        self.allocated_indices = _WCQRing(capacity, num_threads, False, patience_enq, patience_deq, help_delay)
        self.data = external_call["mostream_wcq_alloc", UInt64](UInt64(capacity * 8))
        if self.data == 0:
            raise Error("wCQ: data allocation failed")

    def __init__(out self, *, deinit move: Self):
        self.capacity = move.capacity
        self.num_threads = move.num_threads
        self.data = move.data
        self.free_indices = move.free_indices^
        self.allocated_indices = move.allocated_indices^

    def __deinit__(deinit self):
        external_call["mostream_wcq_free", NoneType](self.data)

    def _check_tid(self, tid: Int) raises:
        if tid < 0 or tid >= self.num_threads:
            raise Error("wCQ: tid out of range")

    def try_push(self, item: UInt64, tid: Int) raises -> Optional[UInt64]:
        self._check_tid(tid)
        var slot = self.free_indices.dequeue(tid)
        if not slot:
            return Optional(item)
        var index = slot.take()
        _wcq_store(self.data + index * 8, item)
        self.allocated_indices.enqueue(index, tid)
        return None

    def try_pop(self, tid: Int) raises -> Optional[UInt64]:
        self._check_tid(tid)
        var slot = self.allocated_indices.dequeue(tid)
        if not slot:
            return None
        var index = slot.take()
        var value = _wcq_load(self.data + index * 8)
        self.free_indices.enqueue(index, tid)
        return Optional(value)

    def push(self, item: UInt64, tid: Int) raises:
        # Busy waiting for space is NOT a wait-free operation.
        while self.try_push(item, tid):
            pass

    def pop(self, tid: Int) raises -> UInt64:
        var item = self.try_pop(tid)
        while not item:
            item = self.try_pop(tid)
        return item.take()

    def slow_path_counts(self, tid: Int) raises -> Tuple[UInt64, UInt64, UInt64]:
        """Diagnostic counters; take a stable reading after joining workers."""
        self._check_tid(tid)
        var enq: UInt64 = 0
        var deq: UInt64 = 0
        var helped: UInt64 = 0
        enq += _wcq_load(self.free_indices._record(tid, _WCQRing.ENQ_SLOW))
        enq += _wcq_load(self.allocated_indices._record(tid, _WCQRing.ENQ_SLOW))
        deq += _wcq_load(self.free_indices._record(tid, _WCQRing.DEQ_SLOW))
        deq += _wcq_load(self.allocated_indices._record(tid, _WCQRing.DEQ_SLOW))
        helped += _wcq_load(self.free_indices._record(tid, _WCQRing.HELPED))
        helped += _wcq_load(self.allocated_indices._record(tid, _WCQRing.HELPED))
        return (enq, deq, helped)
