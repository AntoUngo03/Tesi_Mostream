# Il runner unisce questo test al modulo per isolare la coda dal runtime MoStream.
# Importa la creazione di task dal runtime privato di Mojo 1.1, usato solo dal test.
from std.runtime._asyncrt import create_task
# Importa il controllo di uguaglianza che interrompe il test in caso di differenza.
from std.testing import assert_equal

# Quattro task condividono soltanto un puntatore; il protocollo protegge gli slot.
# Definisce a compilazione un puntatore mutabile alla coda con origine non tracciata.
comptime QueuePointer = Pointer[RigtorpMPMCQueue[Int], MutUntrackedOrigin]

# Definisce il task produttore: first sceglie il primo valore, count quanti inviarne.
async def producer(queue: QueuePointer, first: Int, count: Int, nonblocking: Bool):
    # Produce count interi consecutivi.
    for i in range(count):
        # Seleziona le operazioni try_*; il ciclo esterno del test riprova fino al successo.
        if nonblocking:
            # Conserva il valore da inserire in un Optional finché non viene accettato.
            var pending = Optional(first + i)
            # Riprova finché esiste un valore ancora da inserire.
            while pending:
                # Dereferenzia la coda, tenta l’inserimento e recupera il valore in caso di fallimento.
                pending = queue[].try_push(pending.take())
        # Seleziona invece l’operazione bloccante push o pop.
        else:
            # Inserisce il valore, attendendo attivamente se non c’è spazio.
            queue[].push(first + i)

# Definisce il task consumatore: seen conta atomicamente le occorrenze di ciascun valore.
async def consumer(queue: QueuePointer, seen: Pointer[Atomic[UInt64], MutUntrackedOrigin], count: Int, nonblocking: Bool):
    for _ in range(count):
        # Dichiara il valore da assegnare in uno dei due rami prima di usarlo.
        var value: Int
        # Seleziona le operazioni try_*; il ciclo esterno del test riprova fino al successo.
        if nonblocking:
            # Tenta una prima estrazione senza prenotare un ticket indisponibile.
            var item = queue[].try_pop()
            # Riprova finché il tentativo non restituisce un elemento.
            while not item:
                # Ripete l’estrazione dopo un tentativo senza risultato.
                item = queue[].try_pop()
            # Trasferisce il valore dall’Optional presente alla variabile locale.
            value = item.take()
        # Seleziona invece l’operazione bloccante push o pop.
        else:
            # Estrae un valore, attendendo attivamente se il dato non è ancora disponibile.
            value = queue[].pop()
        # Incrementa atomicamente il contatore del valore ricevuto e ignora il conteggio precedente.
        _ = seen.unsafe_offset(value)[].fetch_add(1)

# Ogni valore deve arrivare esattamente una volta, anche sotto contesa.
# Definisce una prova concorrente per una capacità e una modalità di accesso.
def check_concurrent(capacity: Int, nonblocking: Bool) raises:
    # Fissa a compilazione il numero di valori prodotti da ciascun produttore.
    comptime COUNT = 10000
    # Alloca memoria per una coda condivisa senza costruirla ancora.
    var queue = unsafe_alloc[RigtorpMPMCQueue[Int]](1)
    # Costruisce la coda nella memoria allocata con la capacità scelta.
    queue.unsafe_write(RigtorpMPMCQueue[Int](capacity))
    # Alloca un contatore per ciascuno dei 20.000 valori distinti attesi.
    var seen = unsafe_alloc[Atomic[UInt64]](2 * COUNT)
    # Visita tutti i contatori delle occorrenze.
    for i in range(2 * COUNT):
        # Costruisce ogni contatore atomico con valore iniziale zero.
        seen.unsafe_offset(i).unsafe_write(Atomic[UInt64](0))
    # Avvia il primo produttore con i valori da 0 a 9.999.
    var p0 = create_task(producer(queue, 0, COUNT, nonblocking))
    # Avvia il secondo produttore con i valori da 10.000 a 19.999.
    var p1 = create_task(producer(queue, COUNT, COUNT, nonblocking))
    # Avvia un consumatore che estrarrà 10.000 valori.
    var c0 = create_task(consumer(queue, seen, COUNT, nonblocking))
    # Avvia un secondo consumatore che estrarrà gli altri 10.000 valori.
    var c1 = create_task(consumer(queue, seen, COUNT, nonblocking))
    # Attende la conclusione del primo produttore; tutti e quattro i task sono già stati avviati.
    p0.wait()
    # Attende la conclusione del secondo produttore.
    p1.wait()
    # Attende la conclusione del primo consumatore.
    c0.wait()
    # Attende la conclusione del secondo consumatore prima di controllare o liberare la memoria.
    c1.wait()
    # Visita tutti i contatori delle occorrenze.
    for i in range(2 * COUNT):
        # Richiede una sola occorrenza per valore: zero indica perdita, più di uno duplicazione.
        assert_equal(seen.unsafe_offset(i)[].load(), UInt64(1))
        # Distrugge il contatore verificato senza liberare ancora l’intero array.
        seen.unsafe_offset(i).unsafe_deinit_pointee()
    # Libera l’array dopo la fine dei task e la distruzione di tutti i contatori.
    seen.unsafe_free()
    # Verifica che al termine delle produzioni e delle estrazioni la coda sia vuota.
    assert_equal(queue[].size(), 0)
    # Distrugge la coda, che rilascia a sua volta il buffer interno degli slot.
    queue.unsafe_deinit_pointee()
    # Libera la memoria che conteneva l’oggetto coda.
    queue.unsafe_free()
    # Riporta il successo, la capacità e se sono state usate le operazioni try_*.
    print("Rigtorp: 20000 valori senza perdite o duplicazioni; capacità", capacity, "try_*", nonblocking)

# Il runtime privato è usato soltanto dal test, non dall'implementazione della coda.
# Definisce il punto di ingresso; raises consente di propagare errori di allocazione o dei controlli.
def main() raises:
    # Verifica capacità 1 con operazioni bloccanti push/pop.
    check_concurrent(1, False)
    # Verifica capacità 3 con operazioni bloccanti push/pop.
    check_concurrent(3, False)
    # Verifica capacità 8 con operazioni bloccanti push/pop.
    check_concurrent(8, False)
    # Verifica capacità 1 con operazioni try_* ripetute fino al successo.
    check_concurrent(1, True)
    # Verifica capacità 3 con operazioni try_* ripetute fino al successo.
    check_concurrent(3, True)
    # Verifica capacità 8 con operazioni try_* ripetute fino al successo.
    check_concurrent(8, True)
