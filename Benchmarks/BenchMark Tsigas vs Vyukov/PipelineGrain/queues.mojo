# ===------------------------------------------------------------------------=== #
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU Lesser General Public License version 3 as
#  published by the Free Software Foundation.
#  
#  This program is distributed in the hope that it will be useful, but WITHOUT
#  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
#  License for more details.
#  
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# ===------------------------------------------------------------------------=== #


# Code alternative per il benchmark; ciascun collegamento usa una sola istanza.
# Il runner antepone il modulo Tsigas–Zhang; il padding qui è condiviso dai backend.
# Vyukov riprende il protocollo della MPMC_queue.mojo del progetto (LGPL v3).

# La stessa distanza fra contatori è usata nel porting Tsigas–Zhang.
struct BenchmarkPaddedAtomic:
    comptime PAD_BYTES = 64 - size_of[Atomic[UInt64]]()
    var atomicVal: Atomic[UInt64]
    var pad: Array[UInt8, Self.PAD_BYTES]

    def __init__(out self, initial: UInt64):
        self.atomicVal = Atomic[UInt64](initial)
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

# Gli errori del backend Tsigas vengono terminati nei task del solo harness.
from std.os import abort

# Una sequenza per cella distingue disponibilità e generazione nel protocollo Vyukov.
struct GrainVyukovSlot(Movable):
    var sequence: Atomic[UInt64]
    var data: UInt64

    def __init__(out self, sequence: UInt64):
        self.sequence = Atomic[UInt64](sequence)
        self.data = 0

    def __init__(out self, *, deinit move: Self):
        self.sequence = Atomic[UInt64](move.sequence.load())
        self.data = move.data

# MPMC limitata: prenotazione CAS e sequenze delle celle, senza il backoff della vecchia coda.
struct GrainVyukovQueue:
    var slots: Pointer[GrainVyukovSlot, MutUntrackedOrigin]
    var capacity: UInt64
    var head: BenchmarkPaddedAtomic
    var tail: BenchmarkPaddedAtomic

    def __init__(out self, capacity: Int) raises:
        if capacity < 2 or (capacity & (capacity - 1)) != 0:
            raise Error("Vyukov richiede capacità potenza di due >= 2")
        self.capacity = UInt64(capacity)
        self.slots = unsafe_alloc[GrainVyukovSlot](capacity)
        self.head = BenchmarkPaddedAtomic(0)
        self.tail = BenchmarkPaddedAtomic(0)
        for i in range(capacity):
            self.slots.unsafe_offset(i).unsafe_write(GrainVyukovSlot(UInt64(i)))

    def __deinit__(deinit self):
        for i in range(self.capacity):
            self.slots.unsafe_offset(i).unsafe_deinit_pointee()
        self.slots.unsafe_free()

    def push(mut self, value: UInt64):
        while True:
            var ticket = self.head.atomicVal.load[ordering=Ordering.RELAXED]()
            var slot = self.slots.unsafe_offset(ticket & (self.capacity - 1))
            if slot[].sequence.load[ordering=Ordering.ACQUIRE]() == ticket:
                if self.head.atomicVal.compare_exchange[success_ordering=Ordering.RELAXED, failure_ordering=Ordering.RELAXED](ticket, ticket + 1):
                    slot[].data = value
                    slot[].sequence.store[ordering=Ordering.RELEASE](ticket + 1)
                    return

    def pop(mut self) -> UInt64:
        while True:
            var ticket = self.tail.atomicVal.load[ordering=Ordering.RELAXED]()
            var slot = self.slots.unsafe_offset(ticket & (self.capacity - 1))
            if slot[].sequence.load[ordering=Ordering.ACQUIRE]() == ticket + 1:
                if self.tail.atomicVal.compare_exchange[success_ordering=Ordering.RELAXED, failure_ordering=Ordering.RELAXED](ticket, ticket + 1):
                    var value = slot[].data
                    slot[].sequence.store[ordering=Ordering.RELEASE](ticket + self.capacity)
                    return value
