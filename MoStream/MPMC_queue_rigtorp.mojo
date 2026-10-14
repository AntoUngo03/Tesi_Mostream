# Copyright (c) 2020 Erik Rigtorp <erik@rigtorp.se>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Porting del protocollo Rigtorp, per la toolchain Mojo >= 1.0 del progetto.
# Ticket t: indice t % capacity; generazione g = t // capacity.
# turn = 2*g: libero; turn = 2*g+1: pubblicato; turn = 2*g+2: libero al giro successivo.
# Optional sostituisce storage grezzo, placement new e distruzione esplicita di T.
# L'API segue MoStream: try_push restituisce l'elemento se fallisce, try_pop un Optional.
# Non replica allocator C++, emplace variadico o allineamento hardware di Rigtorp.
# Spostamento e distruzione richiedono che tutti gli accessi concorrenti siano terminati.
# Attesa attiva: questa coda non garantisce lock-freedom o fairness.
# I ticket UInt64 non devono esaurire il loro intervallo durante la vita della coda.

# Importa gli atomici e gli ordini che regolano la visibilità fra thread.
from std.atomic import Atomic, Ordering
# Importa un contenitore che gestisce la vita di un valore eventualmente presente.
from std.collections import Optional
# Importa il puntatore con origine non tracciata, come nella coda esistente.
from std.memory import Pointer
# Importa l'allocazione di celle ancora da inizializzare.
from std.memory.alloc import unsafe_alloc
# Riusa il contatore con padding; padding non significa allineamento a 64 byte.
from .MPMC_queue import PaddedAtomicU64

# Una cella contiene lo stato atomico e un valore gestito dal protocollo.
struct RigtorpSlot[T: Copyable & Deinitable](Movable):
    # Il turno sincronizza accessi al dato, che non è atomico.
    var turn: Atomic[DType.uint64]
    # L'assenza di valore rappresenta una cella libera.
    var data: Optional[Self.T]

    # Costruisce una cella nella generazione iniziale.
    def __init__(out self):
        # Zero significa disponibile al primo produttore assegnato alla cella.
        self.turn = Atomic[DType.uint64](0)
        # Nessun T viene costruito finché non arriva un inserimento.
        self.data = None

    # Rende spostabile la cella; non usarlo durante accessi concorrenti.
    def __init__(out self, *, deinit move: Self):
        # Ricrea l'atomico preservandone lo stato.
        self.turn = Atomic[DType.uint64](move.turn.load())
        # Trasferisce anche la proprietà dell'eventuale valore.
        self.data = move.data^

# Coda limitata con più produttori e consumatori; non copiabile.
struct RigtorpMPMCQueue[T: Copyable & Deinitable](Movable):
    # Definisce il tipo del puntatore proprietario al buffer.
    comptime SlotPointer = Pointer[RigtorpSlot[Self.T], MutUntrackedOrigin]
    # Memorizza la capacità, che può essere anche uno o non potenza di due.
    var capacity: UInt64
    # Conserva l'indirizzo del buffer, liberato dal distruttore.
    var slots: Self.SlotPointer
    # Il prossimo ticket da assegnare a un produttore.
    var head: PaddedAtomicU64
    # Il prossimo ticket da assegnare a un consumatore.
    var tail: PaddedAtomicU64

    # Costruisce la coda; soltanto questa operazione può sollevare Error.
    def __init__(out self, capacity: Int = 1024) raises:
        # Impedisce divisioni per zero e capacità negative.
        if capacity < 1:
            # Segnala il parametro non valido prima di allocare memoria.
            raise Error("RigtorpMPMCQueue: capacity deve essere almeno 1")
        # Converte la capacità validata al tipo dei ticket.
        self.capacity = UInt64(capacity)
        # Alloca esattamente le celle usate; non promette l'allineamento del C++.
        self.slots = unsafe_alloc[RigtorpSlot[Self.T]](capacity)
        # Inizia ad assegnare ticket di scrittura da zero.
        self.head = PaddedAtomicU64(0)
        # Inizia ad assegnare ticket di lettura da zero.
        self.tail = PaddedAtomicU64(0)
        # Visita tutte le celle allocate.
        for i in range(capacity):
            # Costruisce una cella vuota nella memoria prima non inizializzata.
            self.slots.unsafe_offset(i).unsafe_write(RigtorpSlot[Self.T]())

    # Trasferisce la coda senza copiare le celle; richiede accessi quiescenti.
    def __init__(out self, *, deinit move: Self):
        # Mantiene la capacità originale.
        self.capacity = move.capacity
        # Trasferisce la proprietà del buffer dalla sorgente consumata.
        self.slots = move.slots
        # Preserva il ticket di scrittura: azzerarlo corromperebbe una coda usata.
        self.head = PaddedAtomicU64(move.head.atomicVal.load())
        # Preserva anche il ticket di lettura e quindi la generazione corrente.
        self.tail = PaddedAtomicU64(move.tail.atomicVal.load())

    # Distrugge la coda soltanto dopo la fine di tutti gli accessi.
    def __deinit__(deinit self):
        # Include le celle con elementi ancora presenti.
        for i in range(self.capacity):
            # Distrugge la cella e il T eventualmente contenuto nell'Optional.
            self.slots.unsafe_offset(i).unsafe_deinit_pointee()
        # Rilascia il buffer una sola volta.
        self.slots.unsafe_free()

    # Inserisce prenotando subito un ticket; può attendere a coda piena.
    def push(mut self, var item: Self.T):
        # Fetch-add assegna un ticket esclusivo, come emplace nel C++.
        var ticket = self.head.atomicVal.fetch_add[ordering=Ordering.SEQUENTIAL](1)
        # Il modulo individua la cella anche con capacità non potenza di due.
        var slot = self.slots.unsafe_offset(ticket % self.capacity)
        # Il quoziente identifica il giro; lo stato pari atteso indica cella libera.
        var expected = (ticket // self.capacity) * 2
        # Acquire osserva la liberazione della cella da parte del consumatore.
        while slot[].turn.load[ordering=Ordering.ACQUIRE]() != expected:
            # Attesa attiva: la lettura atomica viene ripetuta, senza sospendere il thread.
            pass
        # Il ticket e il turno danno accesso esclusivo al contenuto.
        slot[].data = Optional(item^)
        # Release pubblica il dato prima dello stato dispari visibile ai consumatori.
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=slot[].turn.value), expected + 1)

    # Estrae prenotando subito un ticket; può attendere a coda vuota.
    def pop(mut self) -> Self.T:
        # Fetch-add riserva una lettura esclusiva, come pop nel C++.
        var ticket = self.tail.atomicVal.fetch_add[ordering=Ordering.SEQUENTIAL](1)
        # Seleziona la cella associata al ticket.
        var slot = self.slots.unsafe_offset(ticket % self.capacity)
        # Il turno dispari identifica il dato della generazione richiesta.
        var expected = (ticket // self.capacity) * 2 + 1
        # Acquire rende visibile il valore scritto prima del Release del produttore.
        while slot[].turn.load[ordering=Ordering.ACQUIRE]() != expected:
            # Continua finché il produttore non pubblica quel ticket.
            pass
        # Trasferisce il dato e lascia l'Optional della cella vuoto.
        var item = slot[].data.take()
        # Release permette il riuso solo dopo che l'estrazione è completa.
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=slot[].turn.value), expected + 1)
        # Restituisce il valore trasferendone la proprietà.
        return item^

    # Tenta inserimento senza prenotare un ticket indisponibile; riprova sotto contesa.
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        # Legge un candidato; non acquisisce ancora la proprietà della cella.
        var ticket = self.head.atomicVal.load[ordering=Ordering.ACQUIRE]()
        # Ripete se un altro thread modifica il candidato.
        while True:
            # Trova la cella del ticket corrente.
            var slot = self.slots.unsafe_offset(ticket % self.capacity)
            # Calcola il turno pari libero di questa generazione.
            var expected = (ticket // self.capacity) * 2 + 0
            # Verifica la disponibilità e sincronizza gli accessi al contenuto.
            if slot[].turn.load[ordering=Ordering.ACQUIRE]() == expected:
                # CAS forte prenota il ticket; al fallimento aggiorna ticket al valore osservato.
                if self.head.atomicVal.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](ticket, ticket + 1):
                    # Solo il vincitore sposta il valore nella cella.
                    slot[].data = Optional(item^)
                    # Pubblica il dato con il turno dispari.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=slot[].turn.value), expected + 1)
                    # Nessun valore restituito significa inserimento riuscito.
                    return None
            # Se il turno non è disponibile, controlla se esiste un nuovo candidato.
            else:
                # Conserva la posizione già esaminata.
                var previous = ticket
                # Rilegge il contatore che altri thread possono aver avanzato.
                ticket = self.head.atomicVal.load[ordering=Ordering.ACQUIRE]()
                # Un contatore stabile significa che il prossimo ticket resta indisponibile.
                if ticket == previous:
                    # Il fallimento non è una fotografia atomica di tutta la coda.
                    return Optional(item^)

    # Tenta estrazione senza prenotare un ticket indisponibile; riprova sotto contesa.
    def try_pop(mut self) -> Optional[Self.T]:
        # Legge un candidato; non acquisisce ancora la proprietà della cella.
        var ticket = self.tail.atomicVal.load[ordering=Ordering.ACQUIRE]()
        # Ripete se un altro thread modifica il candidato.
        while True:
            # Trova la cella del ticket corrente.
            var slot = self.slots.unsafe_offset(ticket % self.capacity)
            # Calcola il turno dispari pubblicato di questa generazione.
            var expected = (ticket // self.capacity) * 2 + 1
            # Verifica la disponibilità e sincronizza gli accessi al contenuto.
            if slot[].turn.load[ordering=Ordering.ACQUIRE]() == expected:
                # CAS forte prenota il ticket; al fallimento aggiorna ticket al valore osservato.
                if self.tail.atomicVal.compare_exchange[success_ordering=Ordering.SEQUENTIAL, failure_ordering=Ordering.SEQUENTIAL](ticket, ticket + 1):
                    # Solo il vincitore estrae il dato, svuotando la cella.
                    var item = slot[].data.take()
                    # Pubblica lo stato libero della generazione successiva.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=slot[].turn.value), expected + 1)
                    # Restituisce un Optional presente anche quando il dato vale zero.
                    return Optional(item^)
            # Se il turno non è disponibile, controlla se esiste un nuovo candidato.
            else:
                # Conserva la posizione già esaminata.
                var previous = ticket
                # Rilegge il contatore che altri thread possono aver avanzato.
                ticket = self.tail.atomicVal.load[ordering=Ordering.ACQUIRE]()
                # Un contatore stabile significa che il prossimo ticket resta indisponibile.
                if ticket == previous:
                    # Il fallimento non è una fotografia atomica di tutta la coda.
                    return None

    # Stima la differenza fra prenotazioni, non il numero dei valori già pubblicati.
    def size(self) -> Int:
        # Legge separatamente le scritture; Relaxed basta per una stima.
        var head = self.head.atomicVal.load[ordering=Ordering.RELAXED]()
        # Legge le letture: pop può prenotare prima che arrivi un dato.
        var tail = self.tail.atomicVal.load[ordering=Ordering.RELAXED]()
        # Converte prima della sottrazione per consentire stime negative.
        return Int(head) - Int(tail)

    # Questa stima non deve essere usata per autorizzare push o pop.
    def empty(self) -> Bool:
        # Include il caso di consumatori che hanno prenotato letture in attesa.
        return self.size() <= 0
