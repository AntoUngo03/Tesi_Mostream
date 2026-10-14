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

# Importa operazioni atomiche e ordini di memoria; fence e importato ma usato solo in righe commentate.
from std.atomic import Atomic, Ordering, fence
# Importa sleep, attualmente inutilizzato: i cicli di attesa non sospendono il thread.
from std.time import sleep
# Importa size_of, che calcola la dimensione in byte di un tipo.
from std.sys.info import size_of
# Importa Optional[T]: contiene un valore T oppure nessun valore (None).
from std.collections import Optional
# Importa l'allocazione manuale di memoria non inizializzata; richiede distruzione e liberazione esplicite.
from std.memory.alloc import unsafe_alloc
# Importa Pointer, il puntatore tipizzato usato per accedere alle celle allocate.
from std.memory import Pointer
# Importa la funzione del progetto che stampa messaggi di errore in rosso.
from MoStream.utils import print_red_color

# Struct to add padding to an atomic variable to avoid false sharing between producer and consumer
# Raggruppa un contatore atomico e byte aggiuntivi per distanziare contatori usati da thread diversi.
struct PaddedAtomicU64:
    # Costante risolta a compilazione: assume cache line da 64 byte, non le rileva dalla CPU.
    comptime CACHE_LINE_SIZE_BYTES = 64
    # Calcola quanti byte aggiungere alla dimensione dell'atomico; Self indica il tipo corrente.
    comptime PAD_BYTES = Self.CACHE_LINE_SIZE_BYTES - size_of[Atomic[DType.uint64]]()
    # Contatore intero senza segno a 64 bit, accessibile con operazioni atomiche.
    var atomicVal: Atomic[DType.uint64]
    # Array di padding: distanzia i contatori, ma non impone da solo allineamento a 64 byte.
    var pad: Array[UInt8, Self.PAD_BYTES]

    # constructor
    # Costruttore: out self indica che l'oggetto viene inizializzato; initial e il valore di partenza.
    def __init__(out self, initial: UInt64):
        # Costruisce l'atomico con il valore iniziale ricevuto.
        self.atomicVal = Atomic[DType.uint64](initial)
        # Riserva il padding senza inizializzarne i byte: il programma non ne legge il contenuto.
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

# Cell struct used in the MPMC queue, containing a sequence number and the actual data (one slot of the queue)
# Cella generica: T deve essere copiabile e distruggibile; Movable consente il trasferimento della cella.
struct Cell[T: Copyable & Deinitable](Movable):
    # Numero di sequenza atomico: identifica la generazione della cella e se il dato e pubblicato.
    var sequence: Atomic[DType.uint64]
    # Contenuto non atomico: l'accesso e protetto dal protocollo di prenotazione e dalla sequenza.
    var data: Optional[Self.T]

    # constructor
    # Costruisce una cella con il numero di sequenza ricevuto.
    def __init__(out self, seq: UInt64):
        # Inizializza l'atomico che descrive lo stato della cella.
        self.sequence = Atomic[DType.uint64](seq)
        # Inizialmente la cella non contiene alcun elemento T.
        self.data = Optional[Self.T](None)

    # move constructor
    # Costruttore di spostamento: * rende move keyword-only; deinit consuma l'oggetto sorgente.
    def __init__(out self, *, deinit move: Self):
        # Legge il valore dell'atomico sorgente con l'ordine predefinito della libreria.
        var val = move.sequence.load()
        # Ricostruisce l'atomico destinazione con lo stesso numero di sequenza.
        self.sequence = Atomic[DType.uint64](val)
        # L'operatore postfix ^ trasferisce il contenuto Optional dalla sorgente alla destinazione.
        self.data = move.data^

# MPMC queue implementation based the algorithm by Dmitry Vyukov
#   (https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue)
# Coda generica a capacita fissa, per piu produttori e piu consumatori (MPMC), spostabile.
struct MPMCQueue[T: Copyable & Deinitable](Movable):
    # Alias di tipo a compilazione: puntatore a celle mutabili con origine non tracciata dal compilatore.
    comptime CellPointer = Pointer[Cell[Self.T], MutUntrackedOrigin]
    # Numero iniziale di iterazioni del tentativo di attesa dopo una CAS fallita.
    comptime BACKOFF_MIN = 128
    # Valore denominato massimo del backoff; attenzione: sotto viene usato come maschera, non come limite.
    comptime BACKOFF_MAX = 1024
    # Puntatore al primo elemento del buffer allocato manualmente.
    var buffer: Self.CellPointer
    # Capacita del buffer, ossia numero totale di celle.
    var size: UInt64
    # Maschera size - 1: permette di ricavare l'indice circolare con un AND bit a bit.
    var mask: UInt64
    # Posizione logica della prossima prenotazione di scrittura, separata con padding.
    var enqueue_pos: PaddedAtomicU64
    # Posizione logica della prossima prenotazione di lettura, separata con padding.
    var dequeue_pos: PaddedAtomicU64

    # constructor
    # Costruttore con capacita predefinita 1024; raises dichiara che puo sollevare un errore.
    def __init__(out self, size: Int = 1024) raises:
        # Accetta solo size >= 2 e potenze di due: per queste size & (size - 1) vale zero.
        if not ((size >= 2) and (size & (size - 1)) == 0):
            # Segnala una capacita non valida tramite la funzione di stampa colorata.
            print_red_color("{MoStream} Error: MPMC queues need size to be a power of 2 and at least 2!")
            # Interrompe la costruzione sollevando un errore.
            raise Error("error in MPMC_Queue()")
        # Converte la capacita validata da Int a UInt64 e la memorizza.
        self.size = UInt64(size)
        # Prepara la maschera: per size = 4 vale 3, cioe 0b11.
        self.mask = UInt64(size - 1)
        # Alloca spazio per size celle; la memoria non contiene ancora celle inizializzate.
        self.buffer = unsafe_alloc[Cell[Self.T]](Int(self.size))
        # Inizializza a zero il contatore delle prenotazioni dei produttori.
        self.enqueue_pos = PaddedAtomicU64(0)
        # Inizializza a zero il contatore delle prenotazioni dei consumatori.
        self.dequeue_pos = PaddedAtomicU64(0)
        # Visita tutti gli indici del buffer da zero a size - 1.
        for i in range(self.size):
            # Calcola il puntatore alla cella i e la inizializza con sequence = i e dato assente.
            self.buffer.unsafe_offset(i).unsafe_write(Cell[Self.T](UInt64(i)))

    # move constructor
    # Sposta la coda consumando la sorgente; deve avvenire senza accessi concorrenti.
    def __init__(out self, *, deinit move: Self):
        # Trasferisce l'indirizzo del buffer senza copiare le celle; deinit move consuma la sorgente.
        self.buffer = move.buffer
        # Conserva la capacita del buffer trasferito.
        self.size = move.size
        # Conserva la maschera per gli indici circolari.
        self.mask = move.mask
        # ATTENZIONE: azzera la posizione di scrittura invece di conservarla; una coda gia usata puo diventare incoerente.
        self.enqueue_pos = PaddedAtomicU64(0)
        # ATTENZIONE: azzera anche la posizione di lettura, ma le sequenze delle celle restano quelle originali.
        self.dequeue_pos = PaddedAtomicU64(0)

    # destructor
    # Distruttore: consuma self e rilascia le risorse; nessun thread deve usare ancora la coda.
    def __deinit__(deinit self):
        # Visita ogni cella allocata, anche quelle che contengono un dato residuo.
        for i in range(self.size):
            # Distrugge la cella e il suo eventuale elemento T senza liberare ancora il buffer.
            self.buffer.unsafe_offset(i).unsafe_deinit_pointee()
        # Libera l'allocazione del buffer dopo aver distrutto tutte le celle.
        self.buffer.unsafe_free()

    # push method for producers, continuously retries until the item has been successfully pushed into the queue
    # Inserisce item attendendo fino al successo; mut self consente modifiche, var item e trasferibile.
    def push(mut self, var item: Self.T):
        # Dichiara la posizione logica di scrittura, assegnata a ogni iterazione prima dell'uso.
        var pw: UInt64
        # Dichiara la variabile che conterra la sequenza letta dalla cella.
        var seq: UInt64
        # Inizializza il backoff al numero minimo di iterazioni.
        var bk: UInt64 = Self.BACKOFF_MIN
        # Ripete senza limite: a coda piena puo attendere indefinitamente consumando CPU.
        while True:
            # Legge la prossima posizione; RELAXED garantisce atomicita senza pubblicare il contenuto delle celle.
            pw = self.enqueue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
            # Converte la posizione logica in indice fisico: pw & mask equivale a pw modulo size.
            var cell_ptr = self.buffer.unsafe_offset(pw & self.mask)
            # [] dereferenzia il puntatore; ACQUIRE sincronizza con il RELEASE che ha liberato la cella.
            seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
            # La cella e riutilizzabile per questa posizione solo se sequence coincide con pw.
            if pw == seq:
                # CAS: se enqueue_pos vale ancora pw, lo porta a pw + 1 e prenota la posizione per questo produttore.
                if self.enqueue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pw, pw + 1):
                    # Dopo la prenotazione esclusiva trasferisce item nell'Optional della cella; ^ indica un move.
                    cell_ptr[].data = Optional(item^)
                    # Pubblica il dato: store atomico RELEASE di sequence = pw + 1, osservabile dai consumatori.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pw + 1)
                    # Termina dopo aver pubblicato l'elemento.
                    return  # successfully pushed
                # Dopo una CAS fallita tenta un'attesa con bk iterazioni; _ indica un indice non utilizzato.
                for _ in range(bk):
                    #fence[ordering=Ordering.SEQUENTIAL]() # I am not sure of this, I suppose however that this for loop is compiled out
                    # Corpo vuoto, senza effetti osservabili: il compilatore potrebbe eliminare questo ciclo.
                    pass
                # Raddoppia bk spostandone i bit di una posizione verso sinistra.
                bk <<= 1
                # ATTENZIONE: AND bit a bit con 1024, non min(bk, 1024); gia 256 & 1024 produce zero.
                bk &= Self.BACKOFF_MAX

    # try_push method for producers, returns None if the item has been successfully pushed, or the item itself if the queue
    #   is currently full (i.e. no slot is currently available for pushing)
    # Prova un solo inserimento: None significa successo, Optional(item) significa elemento non inserito.
    def try_push(mut self, var item: Self.T) -> Optional[Self.T]:
        # Legge la posizione di scrittura corrente senza imporre ordinamento sul dato.
        var pw = self.enqueue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
        # Individua la cella associata alla posizione circolare.
        var cell_ptr = self.buffer.unsafe_offset(pw & self.mask)
        # Legge con ACQUIRE la sequenza per verificare se la cella e disponibile.
        var seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
        # Se la sequenza non coincide, questo tentativo non puo usare la cella.
        if pw != seq:
            # Restituisce l'elemento al chiamante; il fallimento non prova da solo che tutta la coda sia piena.
            return Optional(item^) # queue is currently full
        # Prova a prenotare la posizione; fallisce se un altro produttore ha aggiornato il contatore.
        if not self.enqueue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pw, pw + 1):
            # Restituisce item anche in caso di contesa: la coda potrebbe avere altre celle disponibili.
            return Optional(item^) # queue is currently full
        # Trasferisce il dato nella cella prenotata dal produttore.
        cell_ptr[].data = Optional(item^)
        # Pubblica sequence = pw + 1 con RELEASE, rendendo il dato leggibile tramite ACQUIRE.
        Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pw + 1)
        # Nessun elemento restituito: item e stato inserito correttamente.
        return None # successfully pushed

    # pop method for consumers, returns the popped item
    # Estrae un valore T attendendo finche try_pop restituisce un elemento.
    def pop(mut self) -> Self.T:
        # Attesa attiva senza timeout o sospensione del thread.
        while (True):
            # Tenta l'estrazione e riceve un Optional[T].
            var item = self.try_pop()
            # Il test dell'Optional e vero quando contiene un elemento, anche se quell'elemento vale zero.
            if item:
                # Estrae e restituisce T; take svuota l'Optional locale trasferendone il contenuto.
                return item.take()

    # try_pop method for consumers, returns an Optional containing the item if popped successfully,
    #   or None if the queue is empty
    # Tenta l'estrazione; restituisce un Optional[T] e puo riprovare piu volte in caso di contesa.
    def try_pop(mut self) -> Optional[Self.T]:
        # Dichiara la posizione logica della lettura, assegnata prima di ogni uso.
        var pr: UInt64
        # Dichiara la sequenza che verra letta dalla cella.
        var seq: UInt64
        # Inizializza il backoff per gli eventuali fallimenti della CAS dei consumatori.
        var bk: UInt64 = Self.BACKOFF_MIN
        # Ripete se perde la prenotazione; try_pop non garantisce un tempo di esecuzione limitato.
        while True:
            # Legge atomicamente la posizione corrente di estrazione con RELAXED.
            pr = self.dequeue_pos.atomicVal.load[ordering=Ordering.RELAXED]()
            # Trova la cella usando l'indice circolare pr & mask.
            var cell_ptr = self.buffer.unsafe_offset(pr & self.mask)
            # ACQUIRE rende visibili le scritture del produttore pubblicate con RELEASE sulla sequenza.
            seq = cell_ptr[].sequence.load[ordering=Ordering.ACQUIRE]()
            # Per consumare la posizione pr, la sequenza pubblicata deve essere pr + 1.
            var expected_seq = pr + 1
            # Verifica che il dato della posizione attesa sia stato pubblicato.
            if seq == expected_seq:
                # element is ready to be consumed, try to claim it by incrementing pr
                # Prenota in esclusiva la lettura avanzando dequeue_pos solo se vale ancora pr.
                if self.dequeue_pos.atomicVal.compare_exchange[failure_ordering=Ordering.RELAXED, success_ordering=Ordering.RELAXED](pr, pr + 1):
                    # Estrae il dato dall'Optional della cella, lasciandolo vuoto.
                    var item = cell_ptr[].data.take()
                    # Libera la cella con RELEASE: pr + mask + 1 = pr + size, la prossima generazione di scrittura.
                    Atomic[DType.uint64].store[ordering=Ordering.RELEASE](Pointer(to=cell_ptr[].sequence.value), pr + self.mask + 1)
                    # Trasferisce il dato in un Optional presente e lo restituisce.
                    return Optional(item^)
                # CAS failed, another consumer might have claimed this item, retry
                # Se un altro consumatore ha vinto la CAS, tenta un breve backoff prima di riprovare.
                for _ in range(bk):
                    #fence[ordering=Ordering.SEQUENTIAL]() # I am not sure of this, I suppose however that this for loop is compiled out
                    # Ciclo vuoto: l'attesa effettiva non e garantita con le ottimizzazioni.
                    pass
                # Raddoppia il conteggio del backoff dopo aver perso la prenotazione.
                bk <<= 1
                # Stesso problema di push: la maschera 1024 puo azzerare bk, non lo limita a un massimo.
                bk &= Self.BACKOFF_MAX
            # Una sequenza inferiore all'attesa indica che il dato in testa non e ancora pubblicato.
            elif seq < expected_seq:
                # empty slot, the producer has not yet written the item, return None
                # Restituisce assenza di elemento; puo esserci una scrittura prenotata ma ancora incompleta.
                return Optional[Self.T](None)
