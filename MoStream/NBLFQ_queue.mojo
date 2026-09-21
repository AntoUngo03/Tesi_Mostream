# Padico NBLFQ, adapted to Mojo and preallocated payload indices.
# Copyright (c) 2002-2026 INRIA and the University of Rennes 1
# Alexandre DENIS <Alexandre.Denis@inria.fr>
# Christian PEREZ <Christian.Perez@inria.fr>
# SPDX-License-Identifier: GPL-2.0-or-later
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2, or (at your option) any later.
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See Documentazione/Padico_NBLFQ/Puk-nblfq.original.h for the supplied source
# and its complete copyright notice. This ports NBLFQ, not NBLFQ2/CAS128.

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory.alloc import unsafe_alloc
from std.sys import get_defined_bool
from std.sys.info import size_of
from std.time import sleep


comptime NBLFQ_TEST_PAUSE = get_defined_bool["MOSTREAM_NBLFQ_TEST_PAUSE", False]()
comptime NBLFQ_TEST_PAUSE_HINT = get_defined_bool[
    "MOSTREAM_NBLFQ_TEST_PAUSE_HINT", False
]()


struct NBLFQHint:
    var value: Atomic[DType.uint64]
    var padding: Array[UInt8, 64 - size_of[Atomic[DType.uint64]]()]

    def __init__(out self, initial: UInt64):
        self.value = Atomic[DType.uint64](initial)
        self.padding = Array[UInt8, 64 - size_of[Atomic[DType.uint64]]()](uninitialized=True)


struct NBLFQIndexRing[tag_bits: Int = 32](Movable):
    """Padico's tagged-word NBLFQ; head/tail are recoverable scan hints."""

    comptime EntryPointer = Pointer[Atomic[DType.uint64], MutUntrackedOrigin]
    comptime TAG_MASK = (UInt64(1) << UInt64(Self.tag_bits)) - 1
    comptime QUARTER = (UInt64(1) << UInt64(Self.tag_bits)) // 4
    var entries: Self.EntryPointer
    var capacity: Int
    var head: NBLFQHint  # insertion hint, matching the names in the C source
    var tail: NBLFQHint  # removal hint

    def __init__(out self, capacity: Int, full: Bool = False) raises:
        comptime assert 3 <= Self.tag_bits <= 32
        if capacity < 2 or capacity > 0xFFFFFFFE:
            raise Error("NBLFQ capacity must be in [2, 2**32 - 2]")
        self.capacity = capacity
        self.entries = unsafe_alloc[Atomic[DType.uint64]](capacity, alignment=64)
        self.head = NBLFQHint(0)
        self.tail = NBLFQHint(0)
        for i in range(capacity):
            # A full initial ring contains indices 0..capacity-1 in FIFO order.
            var value = UInt64(i + 1) if full else UInt64(0)
            self.entries.unsafe_offset(i).unsafe_write(Atomic[DType.uint64](value))

    def __init__(out self, *, deinit move: Self):
        self.entries = move.entries
        self.capacity = move.capacity
        self.head = NBLFQHint(move.head.value.load[ordering=Ordering.RELAXED]())
        self.tail = NBLFQHint(move.tail.value.load[ordering=Ordering.RELAXED]())

    def __deinit__(deinit self):
        for i in range(self.capacity):
            self.entries.unsafe_offset(i).unsafe_deinit_pointee()
        self.entries.unsafe_free()

    @always_inline
    def advance(self, index: Int) -> Int:
        return 0 if index + 1 == self.capacity else index + 1

    @always_inline
    def previous(self, index: Int) -> Int:
        return self.capacity - 1 if index == 0 else index - 1

    @always_inline
    def load(self, index: Int) -> UInt64:
        return self.entries.unsafe_offset(index)[].load[ordering=Ordering.ACQUIRE]()

    @always_inline
    def sequence(self, word: UInt64) -> UInt64:
        return (word >> 32) & Self.TAG_MASK

    @always_inline
    def empty(self, word: UInt64) -> Bool:
        return (word & 0xFFFFFFFF) == 0

    @always_inline
    def pack(self, value: UInt64, sequence: UInt64) -> UInt64:
        return ((sequence & Self.TAG_MASK) << 32) | value

    @always_inline
    def compare(self, i1: Int, u1: UInt64, i2: Int, u2: UInt64) -> Int:
        # 1: first precedes second, 0: wrap boundary, -1: stale/ambiguous pair.
        # As in Padico, modular ordering requires a distance < one quarter of
        # the tag range. Unlike aborting, an ambiguous snapshot is retried.
        var s1 = self.sequence(u1)
        var s2 = self.sequence(u2)
        if s1 == s2:
            return Int(i1 < i2)
        if ((s2 - s1) & Self.TAG_MASK) < Self.QUARTER:
            return 1
        if ((s1 - s2) & Self.TAG_MASK) < Self.QUARTER:
            return 0
        return -1

    def try_enqueue(mut self, index: UInt32) -> Bool:
        # Caller supplies an index in [0, capacity). Zero in a cell means empty.
        var value = UInt64(index) + 1
        while True:
            var head = Int(self.head.value.load[ordering=Ordering.RELAXED]())
            var prev = self.previous(head)
            var p = self.load(prev)
            var u = self.load(head)
            var restart = False
            while True:
                if not self.empty(p) and self.empty(u):
                    break
                var order = self.compare(prev, p, head, u)
                if order < 0:
                    restart = True
                    break
                if order == 0:
                    if self.empty(p) and self.empty(u):
                        break
                    if not self.empty(p) and not self.empty(u):
                        self.head.value.store[ordering=Ordering.RELAXED](UInt64(head))
                        return False
                prev = head
                head = self.advance(head)
                p = u
                u = self.load(head)
            if restart:
                continue
            var sequence = self.sequence(p)
            if self.empty(p):
                sequence = (sequence - 1) & Self.TAG_MASK
            if head == 0:
                sequence = (sequence + 1) & Self.TAG_MASK
            # Do not reserve a ticket: publish the index and generation in
            # one CAS. A delayed hint update can be recovered by scanning.
            var expected = self.pack(0, sequence)
            if self.entries.unsafe_offset(head)[].compare_exchange[
                success_ordering=Ordering.SEQUENTIAL,
                failure_ordering=Ordering.RELAXED,
            ](expected, self.pack(value, sequence)):
                comptime if NBLFQ_TEST_PAUSE_HINT:
                    sleep(0.00001)
                self.head.value.store[ordering=Ordering.RELAXED](UInt64(self.advance(head)))
                return True

    def enqueue(mut self, index: UInt32):
        while not self.try_enqueue(index):
            pass

    def try_dequeue(mut self) -> Optional[UInt32]:
        while True:
            var tail = Int(self.tail.value.load[ordering=Ordering.RELAXED]())
            var prev = self.previous(tail)
            var p = self.load(prev)
            var u = self.load(tail)
            var order = self.compare(prev, p, tail, u)
            while order == 1:
                prev = tail
                tail = self.advance(tail)
                p = u
                u = self.load(tail)
                order = self.compare(prev, p, tail, u)
            if order < 0:
                continue
            if self.empty(u):
                self.tail.value.store[ordering=Ordering.RELAXED](UInt64(tail))
                return None
            var sequence = (self.sequence(u) + 1) & Self.TAG_MASK
            var expected = u
            if self.entries.unsafe_offset(tail)[].compare_exchange[
                success_ordering=Ordering.SEQUENTIAL,
                failure_ordering=Ordering.RELAXED,
            ](expected, self.pack(0, sequence)):
                comptime if NBLFQ_TEST_PAUSE_HINT:
                    sleep(0.00001)
                self.tail.value.store[ordering=Ordering.RELAXED](UInt64(self.advance(tail)))
                return UInt32((u & 0xFFFFFFFF) - 1)


struct NBLFQQueue[T: Copyable & Deinitable](Movable):
    """Two NBLFQ index rings plus exclusive, preallocated generic payloads."""

    var data: Pointer[Optional[Self.T], MutUntrackedOrigin]
    var capacity: Int
    var free_indices: NBLFQIndexRing[]
    var allocated_indices: NBLFQIndexRing[]
    var count: Atomic[DType.int64]

    def __init__(out self, size: Int = 1024) raises:
        if size < 2 or size > 0xFFFFFFFE:
            raise Error("NBLFQ capacity must be in [2, 2**32 - 2]")
        self.capacity = size
        self.free_indices = NBLFQIndexRing[](size, full=True)
        self.allocated_indices = NBLFQIndexRing[](size)
        self.data = unsafe_alloc[Optional[Self.T]](size, alignment=64)
        for i in range(size):
            self.data.unsafe_offset(i).unsafe_write(Optional[Self.T](None))
        self.count = Atomic[DType.int64](0)

    def __init__(out self, *, deinit move: Self):
        self.data = move.data
        self.capacity = move.capacity
        self.free_indices = move.free_indices^
        self.allocated_indices = move.allocated_indices^
        self.count = Atomic[DType.int64](move.count.load[ordering=Ordering.RELAXED]())

    def __deinit__(deinit self):
        # No concurrent operations may overlap move or destruction.
        for i in range(self.capacity):
            self.data.unsafe_offset(i).unsafe_deinit_pointee()
        self.data.unsafe_free()

    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var available = self.free_indices.try_dequeue()
        if not available:
            return Optional(item^)
        var index = available.take()
        self.data.unsafe_offset(Int(index))[] = Optional(item^)
        comptime if NBLFQ_TEST_PAUSE:
            sleep(0.00001)
        # Owning an unpublished index guarantees space in the occupied ring.
        self.allocated_indices.enqueue(index)
        _ = self.count.fetch_add[ordering=Ordering.RELAXED](1)
        return None

    def try_pop(mut self) -> Optional[Self.T]:
        var occupied = self.allocated_indices.try_dequeue()
        if not occupied:
            return None
        var index = occupied.take()
        comptime if NBLFQ_TEST_PAUSE:
            sleep(0.00001)
        var item = self.data.unsafe_offset(Int(index))[].take()
        # The index stays private until payload extraction has completed.
        self.free_indices.enqueue(index)
        _ = self.count.fetch_sub[ordering=Ordering.RELAXED](1)
        return Optional(item^)

    def push(mut self, var item: Self.T):
        var pending = Optional(item^)
        while pending:
            pending = self.try_push(pending.take())

    def pop(mut self) -> Self.T:
        while True:
            var item = self.try_pop()
            if item:
                return item.take()

    def estimated_len(self) -> Int:
        var count = self.count.load[ordering=Ordering.RELAXED]()
        return min(self.capacity, max(0, Int(count)))
