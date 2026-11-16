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

# Coda SPSC limitata (un solo produttore, un solo consumatore): porting in Mojo
# di SWSR_Ptr_Buffer della libreria FastFlow (ff/buffer.hpp, autore Massimo
# Torquati; FastFlow è distribuita con doppia licenza LGPLv3 / MIT).
# Riferimento: M. Torquati, "Single-Producer/Single-Consumer Queue on Shared
# Cache Multi-Core Systems", TR-10-20, Dipartimento di Informatica, Università
# di Pisa, 2010.
#
# Idea dell'algoritmo originale:
#   - il produttore usa solo il proprio indice di scrittura (pwrite), il
#     consumatore solo il proprio indice di lettura (pread): NESSUN indice è
#     condiviso fra i due thread;
#   - l'unica memoria condivisa è la cella: una cella vuota vale NULL. Il
#     produttore scrive solo in una cella vuota, il consumatore legge solo una
#     cella piena e poi la rimette a NULL;
#   - nessuna operazione atomica read-modify-write (niente CAS, niente fetch_add).
#
# Differenza di questo porting: FastFlow trasporta puntatori e usa NULL come
# marcatore di cella vuota. Qui la coda trasporta valori di tipo T (in MoStream
# un MessageWrapper), che non hanno un valore nullo; ogni cella ha quindi un
# flag atomico pieno/vuoto accanto al dato. Il flag svolge esattamente il ruolo
# di NULL: il produttore scrive il dato e poi alza il flag (RELEASE); il
# consumatore legge il flag (ACQUIRE), prende il dato e lo riabbassa (RELEASE).
# Su x86 (modello TSO) queste operazioni non richiedono barriere esplicite, come
# nell'originale, dove WMB() è vuota su x86.
#
# Disposizione in memoria: ogni lato ha un proprio blocco da 128 byte con il suo
# indice e una copia privata di capacità e puntatore al buffer. I due blocchi
# sono allocati in memoria separata dalla struttura della coda, uno dopo l'altro:
# con passo di 128 byte i campi usati da un lato non condividono mai una linea
# di cache con quelli dell'altro, anche se l'allocazione non è allineata a 64.
#
# PERCHÉ I BLOCCHI NON SONO CAMPI DI self: gli indici sono variabili normali (non
# atomiche) modificate da due thread diversi. In Mojo un argomento `mut self` è un
# riferimento esclusivo: durante la chiamata il compilatore può assumere che
# nessun altro modifichi i campi di self. Una prima versione con i due blocchi
# dentro la struttura superava i test sequenziali ma nei test concorrenti con
# try_push/try_pop desincronizzava gli indici e si bloccava. Con i blocchi
# raggiunti tramite puntatore, self contiene solo puntatori che non cambiano mai,
# e ogni indice è modificato da un solo thread.
#
# ATTENZIONE: la coda è corretta solo se usata da UN produttore e UN consumatore.
# Più produttori o più consumatori corrompono la coda senza alcun errore.

from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.sys.info import size_of

# Cella del buffer: flag pieno/vuoto (0 = vuota, 1 = piena) e dato.
struct FFSlot[T: Copyable & Deinitable](Movable):
    # Ruolo del NULL di FastFlow: 0 = cella vuota, 1 = cella piena.
    var full: Atomic[UInt64]
    # Il dato vero e proprio; None quando la cella è vuota.
    var data: Optional[Self.T]

    # Costruttore: cella vuota.
    def __init__(out self):
        self.full = Atomic[UInt64](0)
        self.data = None

    # Costruttore di spostamento (usato solo prima che la coda sia condivisa).
    def __init__(out self, *, deinit move: Self):
        self.full = Atomic[UInt64](move.full.load())
        self.data = move.data^

# Blocco privato di un lato (produttore o consumatore), da 128 byte.
struct FFSide[T: Copyable & Deinitable](Movable):
    comptime Buffer = Pointer[FFSlot[Self.T], MutUntrackedOrigin]
    comptime PAD_BYTES = 128 - 2 * size_of[UInt64]() - size_of[Self.Buffer]()
    # Indice della prossima cella da scrivere (produttore) o da leggere (consumatore).
    var index: UInt64
    # Copia privata della capacità, per non leggere un campo condiviso.
    var capacity: UInt64
    # Copia privata del puntatore al buffer.
    var buf: Self.Buffer
    # Riempimento fino a 128 byte, per tenere i due lati su linee di cache diverse.
    var pad: Array[UInt8, Self.PAD_BYTES]

    def __init__(out self, capacity: UInt64, buf: Self.Buffer):
        self.index = 0
        self.capacity = capacity
        self.buf = buf
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

    def __init__(out self, *, deinit move: Self):
        self.index = move.index
        self.capacity = move.capacity
        self.buf = move.buf
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

    # Avanza l'indice in modo circolare, come in FastFlow (senza modulo).
    @always_inline
    def advance(mut self):
        self.index = 0 if self.index + 1 >= self.capacity else self.index + 1

    # Cella corrente di questo lato.
    @always_inline
    def slot(self) -> Self.Buffer:
        return self.buf.unsafe_offset(Int(self.index))

# Coda SPSC limitata, interfaccia uguale a MPMCQueue e RigtorpMPMCQueue.
struct FastFlowSPSCQueue[T: Copyable & Deinitable](Movable):
    comptime Buffer = Pointer[FFSlot[Self.T], MutUntrackedOrigin]
    comptime SidePointer = Pointer[FFSide[Self.T], MutUntrackedOrigin]
    # Due blocchi consecutivi in memoria separata: [0] produttore, [1] consumatore.
    # Il puntatore non cambia mai dopo il costruttore.
    var sides: Self.SidePointer

    # Costruttore: capacity celle, tutte utilizzabili (nessuna cella sprecata).
    def __init__(out self, capacity: Int = 1024) raises:
        if capacity < 1:
            raise Error("FastFlowSPSCQueue: capacity deve essere almeno 1")
        var buf = unsafe_alloc[FFSlot[Self.T]](capacity)
        for i in range(capacity):
            buf.unsafe_offset(i).unsafe_write(FFSlot[Self.T]())
        self.sides = unsafe_alloc[FFSide[Self.T]](2)
        self.sides.unsafe_offset(0).unsafe_write(FFSide[Self.T](UInt64(capacity), buf))
        self.sides.unsafe_offset(1).unsafe_write(FFSide[Self.T](UInt64(capacity), buf))

    # Costruttore di spostamento: trasferisce il puntatore ai blocchi, senza copiarli.
    def __init__(out self, *, deinit move: Self):
        self.sides = move.sides

    # Distruttore: distrugge le celle (e gli eventuali dati rimasti), libera buffer e blocchi.
    def __deinit__(deinit self):
        var buf = self.sides[].buf
        for i in range(Int(self.sides[].capacity)):
            buf.unsafe_offset(i).unsafe_deinit_pointee()
        buf.unsafe_free()
        self.sides.unsafe_offset(0).unsafe_deinit_pointee()
        self.sides.unsafe_offset(1).unsafe_deinit_pointee()
        self.sides.unsafe_free()

    # Blocco del produttore (lo modifica solo il produttore).
    @always_inline
    def producer(self) -> Self.SidePointer:
        return self.sides

    # Blocco del consumatore (lo modifica solo il consumatore).
    @always_inline
    def consumer(self) -> Self.SidePointer:
        return self.sides.unsafe_offset(1)

    # push bloccante (lato produttore): aspetta che la cella corrente sia vuota.
    def push(mut self, var item: Self.T):
        var side = self.producer()
        var cell = side[].slot()
        # ACQUIRE: vede la rimozione del dato fatta dal consumatore prima di riusare la cella.
        while cell[].full.load[ordering=Ordering.ACQUIRE]() != 0:
            pass
        cell[].data = Optional(item^)
        # RELEASE: il dato è visibile al consumatore prima del flag.
        cell[].full.store[ordering=Ordering.RELEASE](1)
        side[].advance()

    # try_push (lato produttore): None se inserito, altrimenti restituisce l'elemento (coda piena).
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        var side = self.producer()
        var cell = side[].slot()
        if cell[].full.load[ordering=Ordering.ACQUIRE]() != 0:
            return Optional(item^)
        cell[].data = Optional(item^)
        cell[].full.store[ordering=Ordering.RELEASE](1)
        side[].advance()
        return None

    # pop bloccante (lato consumatore): aspetta che la cella corrente sia piena.
    def pop(mut self) -> Self.T:
        var side = self.consumer()
        var cell = side[].slot()
        # ACQUIRE: vede il dato scritto dal produttore prima del flag.
        while cell[].full.load[ordering=Ordering.ACQUIRE]() == 0:
            pass
        var item = cell[].data.take()
        # RELEASE: la cella torna vuota solo dopo che il dato è stato preso.
        cell[].full.store[ordering=Ordering.RELEASE](0)
        side[].advance()
        return item^

    # try_pop (lato consumatore): l'elemento se c'è, altrimenti None (coda vuota).
    def try_pop(mut self) -> Optional[Self.T]:
        var side = self.consumer()
        var cell = side[].slot()
        if cell[].full.load[ordering=Ordering.ACQUIRE]() == 0:
            return None
        var item = cell[].data.take()
        cell[].full.store[ordering=Ordering.RELEASE](0)
        side[].advance()
        return Optional(item^)

    # Capacità (numero di celle).   
    def capacity(self) -> Int:
        return Int(self.sides[].capacity)
