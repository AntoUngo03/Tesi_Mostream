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

# Wrapper della coda scelta in compilazione (Vyukov di default, Rigtorp con -D MOSTREAM_RIGTORP=1,
# SPSC FastFlow con -D MOSTREAM_SPSC=1, valida solo su canali 1:1).
from MoStream.communication_queue import CommunicationQueue, USE_SPSC
# Optional rappresenta un valore che può esserci oppure no (usato da try_push/try_pop).
from std.collections import Optional
# size_of restituisce la dimensione in byte di un tipo (importato, non usato in questo file).
from std.sys.info import size_of
# Atomic per i contatori condivisi fra thread; Ordering per gli ordinamenti di memoria.
from std.atomic import Atomic, Ordering
# unsafe_alloc alloca memoria grezza sullo heap, da liberare a mano con unsafe_free.
from std.memory.alloc import unsafe_alloc
# Pointer è il puntatore usato per riferirsi alle strutture allocate sullo heap.
from std.memory import Pointer
# Stampa un messaggio di errore in rosso.
from MoStream.utils import print_red_color

# Requisiti di un messaggio trasportabile: deve poter essere copiato e distrutto.
comptime MessageTrait = Copyable & Deinitable

# Involucro di ogni messaggio in coda: contiene il dato oppure segnala la fine dello stream (EOS).
struct MessageWrapper[T: MessageTrait](Copyable):
    # Il dato vero e proprio, dentro un Optional: è vuoto nei messaggi di EOS.
    var data: Optional[Self.T]
    # True se il messaggio è un EOS (end of stream), False se trasporta un dato.
    var eos: Bool

    # Costruttore I: messaggio con un dato.
    def __init__(out self, var data: Self.T, eos: Bool):
        # Sposta (^) il dato dentro l'Optional, senza copiarlo.
        self.data = Optional(data^)
        # Normalmente False: è un messaggio di dati.
        self.eos = eos

    # Costruttore II: messaggio senza dato, usato per l'EOS.
    def __init__(out self, eos: Bool):
        # Nessun dato: l'Optional resta vuoto.
        self.data = None
        # Normalmente True: è un messaggio di fine stream.
        self.eos = eos

# Communicator: il canale fra due stadi consecutivi della pipeline, costruito
# sopra la coda MPMC limitata scelta in compilazione.
#
# Due protocolli di fine stream, scelti in compilazione dal parametro `explicit_eos`:
#
#   explicit_eos = False (default, runtime cooperativo)
#     L'EOS non viene mai inserito nella coda. L'ultimo produttore alza il flag
#     `closed`; un consumatore che trova la coda vuota controlla il flag, ricontrolla
#     la coda e, se è ancora vuota, costruisce l'EOS da solo. I consumatori usano
#     solo try_pop(), che non blocca mai un worker dello scheduler; pop() non è
#     disponibile in questa modalità.
#
#   explicit_eos = True (runtime standard)
#     L'ultimo produttore alza `closed` e poi inserisce nella coda esattamente
#     `consNum` messaggi EOS, uno per consumatore. pop() è il pop bloccante della
#     coda (per Rigtorp: un solo fetch_add per prendere il ticket, poi attesa sulla
#     propria cella) e ogni consumatore si ferma al primo EOS che riceve.
#     Correttezza: ogni produttore completa i propri push prima di chiamare
#     producer_finished() (fetch_sub ACQUIRE_RELEASE), quindi gli EOS occupano
#     posizioni successive a tutti i dati. Le posizioni vengono prese in ordine,
#     ciascuna da un solo consumatore, e ogni consumatore si ferma dopo il proprio
#     EOS: si consumano esattamente D + consNum posizioni (D = messaggi di dati).
#     Nessun dato perso, nessun ticket senza elemento, nessun consumatore in
#     attesa per sempre.
#     In questa modalità i consumatori non devono usare try_pop().
struct Communicator[T: MessageTrait, explicit_eos: Bool = False](Movable):
    # Puntatore alla coda condivisa fra produttori e consumatori, allocata sullo heap.
    var queue: Pointer[CommunicationQueue[MessageWrapper[Self.T]], MutUntrackedOrigin]
    # Numero di produttori: le repliche dello stadio che scrive in questa coda.
    var prodNum: Int
    # Numero di consumatori: le repliche dello stadio che legge da questa coda.
    var consNum: Int
    # Consumatori che non hanno ancora finito; chi lo porta a zero distrugge il communicator.
    var destroyCount: Pointer[Atomic[Int64], MutUntrackedOrigin]
    # Produttori che stanno ancora inviando dati; l'ultimo chiude il canale.
    var remainingProducers: Pointer[Atomic[Int64], MutUntrackedOrigin]
    # Flag di chiusura: 1 quando tutti i produttori hanno finito, 0 altrimenti.
    var closed: Pointer[Atomic[Int64], MutUntrackedOrigin]

    # Costruttore: pN produttori, cN consumatori, coda da queue_size posizioni.
    def __init__(out self, pN: Int, cN: Int, queue_size: Int) raises:
        # Con la coda SPSC (scelta in compilazione) sono ammessi solo canali con
        # un produttore e un consumatore: con più thread su un lato la coda si
        # corromperebbe senza alcun errore, quindi la configurazione va rifiutata.
        comptime if USE_SPSC:
            if pN != 1 or cN != 1:
                print_red_color("{MoStream} Error: the SPSC queue (MOSTREAM_SPSC) supports only channels with one producer and one consumer, got " + String(pN) + ":" + String(cN) + "!")
                raise Error("error in Communicator()")
        # Alloca sullo heap lo spazio per una coda.
        self.queue = unsafe_alloc[CommunicationQueue[MessageWrapper[Self.T]]](1)
        # Costruisce la coda nello spazio appena allocato.
        self.queue.unsafe_write(CommunicationQueue[MessageWrapper[Self.T]](size=queue_size))
        # Memorizza il numero di produttori.
        self.prodNum = pN
        # Memorizza il numero di consumatori (serve anche per sapere quanti EOS inviare).
        self.consNum = cN
        # Alloca il contatore di distruzione.
        self.destroyCount = unsafe_alloc[Atomic[Int64]](1)
        # Parte da cN: ogni consumatore lo decrementa una volta quando ha finito.
        self.destroyCount[] = Atomic[Int64](Int64(cN))
        # Alloca il contatore dei produttori attivi.
        self.remainingProducers = unsafe_alloc[Atomic[Int64]](1)
        # Parte da pN: ogni produttore lo decrementa una volta quando ha finito.
        self.remainingProducers[] = Atomic[Int64](Int64(pN))
        # Alloca il flag di chiusura.
        self.closed = unsafe_alloc[Atomic[Int64]](1)
        # Di norma il canale nasce aperto (0).
        var initially_closed = Int64(0)
        # Senza produttori non arriverà mai nessun dato:
        if pN == 0:
            # il canale nasce già chiuso.
            initially_closed = Int64(1)
        # Scrive il valore iniziale del flag.
        self.closed[] = Atomic[Int64](initially_closed)
        # Solo nella modalità con EOS espliciti (scelta in compilazione):
        comptime if Self.explicit_eos:
            # senza produttori nessuno inserirebbe mai gli EOS, quindi li inserisce
            # subito il costruttore. Devono starci tutti, perché nessun consumatore
            # sta ancora girando e un push su coda piena resterebbe bloccato.
            if pN == 0:
                # Se gli EOS non ci stanno nella coda, la configurazione è impossibile:
                if cN > queue_size:
                    # segnala l'errore
                    print_red_color("{MoStream} Error: a communicator without producers needs queue size >= number of consumers!")
                    # e interrompe la costruzione.
                    raise Error("error in Communicator()")
                # Inserisce un EOS per ogni consumatore.
                for _ in range(cN):
                    self.queue[].push(MessageWrapper[Self.T](eos=True))

    # Costruttore di spostamento: trasferisce i puntatori senza copiare le strutture.
    def __init__(out self, *, deinit move: Self):
        # Prende il puntatore alla coda.
        self.queue = move.queue
        # Copia il numero di produttori.
        self.prodNum = move.prodNum
        # Copia il numero di consumatori.
        self.consNum = move.consNum
        # Prende il puntatore al contatore di distruzione.
        self.destroyCount = move.destroyCount
        # Prende il puntatore al contatore dei produttori.
        self.remainingProducers = move.remainingProducers
        # Prende il puntatore al flag di chiusura.
        self.closed = move.closed

    # Distruttore: libera tutta la memoria allocata dal costruttore.
    def __deinit__(deinit self):
        # Distrugge la coda (e i messaggi rimasti al suo interno)
        self.queue.unsafe_deinit_pointee()
        # e libera la sua memoria.
        self.queue.unsafe_free()
        # Distrugge il contatore di distruzione
        self.destroyCount.unsafe_deinit_pointee()
        # e libera la sua memoria.
        self.destroyCount.unsafe_free()
        # Distrugge il contatore dei produttori
        self.remainingProducers.unsafe_deinit_pointee()
        # e libera la sua memoria.
        self.remainingProducers.unsafe_free()
        # Distrugge il flag di chiusura
        self.closed.unsafe_deinit_pointee()
        # e libera la sua memoria.
        self.closed.unsafe_free()

    # Dice se il canale è chiuso, cioè se tutti i produttori hanno finito.
    # Lo usano try_pop() e lo scheduler del runtime cooperativo.
    def is_closed(mut self) -> Bool:
        # ACQUIRE: chi vede il flag a 1 vede anche tutti i push fatti prima della chiusura.
        return self.closed[].load[ordering=Ordering.ACQUIRE]() == Int64(1)

    # Chiamata da ogni produttore quando ha finito di inviare dati.
    def producer_finished(mut self):
        # Decrementa atomicamente i produttori attivi e legge il valore precedente.
        # ACQUIRE_RELEASE: i push di questo produttore sono visibili a chi decrementa
        # dopo, e l'ultimo produttore vede i push di tutti gli altri.
        var old_count = self.remainingProducers[].fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)
        # Se valeva 1, questo era l'ultimo produttore:
        if old_count == Int64(1):
            # alza il flag di chiusura PRIMA di inserire gli EOS. Appena l'ultimo
            # consumatore riceve il proprio EOS può distruggere il communicator,
            # quindi l'ultimo push di EOS deve essere l'ultimo accesso del produttore.
            # RELEASE: chi legge il flag a 1 vede tutto ciò che è stato fatto prima.
            self.closed[].store[ordering=Ordering.RELEASE](Int64(1))
            # Solo nella modalità con EOS espliciti (scelta in compilazione):
            comptime if Self.explicit_eos:
                # inserisce un EOS per ogni consumatore;
                for _ in range(self.consNum):
                    # push bloccante: se la coda è piena aspetta che i consumatori la svuotino.
                    self.queue[].push(MessageWrapper[Self.T](eos=True))

    # Chiamata da ogni consumatore dopo aver ricevuto l'EOS: vero solo per l'ultimo,
    # che a quel punto può distruggere il communicator in sicurezza.
    def check_isDestroyable(mut self) -> Bool:
        # Decrementa atomicamente i consumatori rimasti e legge il valore precedente.
        var old_count = self.destroyCount[].fetch_sub[ordering=Ordering.ACQUIRE_RELEASE](1)
        # Se valeva 1, questo è l'ultimo consumatore.
        return old_count == Int64(1)

    # push: inserisce un messaggio, aspettando se la coda è piena (push bloccante della coda).
    def push(mut self, var msg: MessageWrapper[Self.T]):
        # Sposta (^) il messaggio nella coda; per Rigtorp il push usa un fetch_add.
        _ = self.queue[].push(msg^)

    # try_push: prova a inserire senza aspettare. Restituisce None se il messaggio
    # è stato inserito, oppure il messaggio stesso se la coda è piena.
    # Usato dal runtime cooperativo.
    def try_push(mut self, var msg: MessageWrapper[Self.T]) -> Optional[MessageWrapper[Self.T]]:
        # Inoltra il tentativo alla coda.
        return self.queue[].try_push(msg^)

    # pop: estrae un messaggio, aspettando finché ce n'è uno (pop bloccante della
    # coda; per Rigtorp: ticket con fetch_add). L'EOS arriva come un messaggio.
    # Esiste solo con explicit_eos = True (runtime standard): senza EOS in coda un
    # pop bloccante non terminerebbe mai a fine stream. Il runtime cooperativo usa
    # soltanto try_pop().
    def pop(mut self) -> MessageWrapper[Self.T]:
        # Errore di compilazione se chiamato su un Communicator senza EOS espliciti.
        comptime assert Self.explicit_eos, "{MoStream} Assert: Communicator.pop() requires explicit_eos=True (use try_pop() otherwise)!"
        # Pop bloccante della coda.
        return self.queue[].pop()

    # try_pop: prova a estrarre senza aspettare. Restituisce None se in questo
    # momento non c'è nulla, oppure un messaggio (anche un EOS se il canale è chiuso).
    # Usato dal runtime cooperativo (explicit_eos = False).
    def try_pop(mut self) -> Optional[MessageWrapper[Self.T]]:
        # Primo tentativo di estrazione dalla coda.
        var maybe_msg = self.queue[].try_pop()
        # Se ha trovato un messaggio
        if maybe_msg:
            # lo restituisce.
            return maybe_msg^
        # La coda sembrava vuota: se i produttori non hanno ancora finito
        if not self.is_closed():
            # arriveranno altri messaggi, per ora non c'è nulla.
            return None
        # Il canale è chiuso: ricontrolla la coda, perché un messaggio inserito
        # appena prima della chiusura potrebbe essere arrivato dopo il primo tentativo.
        maybe_msg = self.queue[].try_pop()
        # Se ora c'è un messaggio
        if maybe_msg:
            # lo restituisce.
            return maybe_msg^
        # Chiuso e ancora vuoto dopo il secondo controllo: lo stream è finito,
        # e il consumatore costruisce da solo il proprio EOS.
        return Optional(MessageWrapper[Self.T](eos=True))
