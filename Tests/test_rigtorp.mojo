# Eseguire con python3 Tests/run_rigtorp_tests.py: il runner antepone il modulo.
# Importa le verifiche di uguaglianza e delle condizioni; al fallimento sollevano un errore.
from std.testing import assert_equal, assert_true

# Verifica capacità arbitrarie, FIFO, pieno/vuoto e più generazioni degli slot.
def check_capacity(capacity: Int) raises:
    # Costruisce una coda di interi con la capacità da verificare.
    var queue = RigtorpMPMCQueue[Int](capacity)
    # Controlla che la coda appena costruita sia vuota.
    assert_true(queue.empty())
    # Controlla che da una coda vuota non si possa estrarre un valore.
    assert_true(not queue.try_pop())
    # Ripete cento riempimenti e svuotamenti, verificando il riuso degli slot su più generazioni.
    for cycle in range(100):
        # Visita tutte le posizioni della capacità in ordine crescente.
        for i in range(capacity):
            # Inserisce un intero univoco; None, verificato con not, significa inserimento riuscito.
            assert_true(not queue.try_push(cycle * capacity + i))
        # Senza accessi concorrenti, dopo il riempimento la dimensione deve coincidere con la capacità.
        assert_equal(queue.size(), capacity)
        # Tenta un inserimento quando tutte le celle sono occupate.
        var rejected = queue.try_push(-1)
        # Verifica che il fallimento abbia restituito un Optional contenente il valore rifiutato.
        assert_true(Bool(rejected))
        # Estrae il valore rifiutato e controlla che sia ancora -1.
        assert_equal(rejected.take(), -1)
        # Visita tutte le posizioni della capacità in ordine crescente.
        for i in range(capacity):
            # Tenta di estrarre il prossimo valore inserito.
            var item = queue.try_pop()
            # Verifica la presenza di un elemento: anche un intero uguale a zero è un risultato valido.
            assert_true(Bool(item))
            # Estrae il dato e controlla l’ordine FIFO confrontandolo con quello inserito.
            assert_equal(item.take(), cycle * capacity + i)
        # Controlla che da una coda vuota non si possa estrarre un valore.
        assert_true(not queue.try_pop())
    # Verifica l’inserimento bloccante quando la coda ha spazio.
    queue.push(42)
    # Verifica che l’estrazione bloccante restituisca il valore inserito.
    assert_equal(queue.pop(), 42)
    # Lascia un valore nella coda già usata prima di spostarla.
    queue.push(101)
    # Trasferisce la proprietà della coda: ^ consuma queue senza copiare il buffer.
    var moved = queue^
    # Controlla che lo spostamento abbia conservato dato, ticket e generazione.
    assert_equal(moved.pop(), 101)
    # Verifica che la coda spostata possa essere riutilizzata.
    moved.push(102)
    # Controlla che anche il nuovo valore venga estratto correttamente.
    assert_equal(moved.pop(), 102)

# Esegue le verifiche anche con String, che possiede memoria da trasferire/liberare.
def main() raises:
    # Verifica tutte le capacità da 1 a 8, incluse quelle non potenze di due.
    for capacity in range(1, 9):
        # Esegue la suite sequenziale per la capacità corrente.
        check_capacity(capacity)
    # Costruisce una coda di stringhe per verificare valori che gestiscono memoria.
    var words = RigtorpMPMCQueue[String](3)
    # Trasferisce una stringa nella coda.
    words.push(String("ciao"))
    # Controlla che l’estrazione conservi il contenuto della stringa.
    assert_equal(words.pop(), "ciao")
    # Lascia una stringa nella coda: il distruttore dovrà rilasciarla alla fine della funzione.
    words.push(String("residuo da distruggere"))
    # Prepara un indicatore per verificare il rifiuto della capacità zero.
    var invalid_rejected = False
    # Inizia il blocco in cui è atteso un errore di costruzione.
    try:
        # Tenta di costruire una coda con capacità non valida.
        var invalid = RigtorpMPMCQueue[Int](0)
    # Intercetta l’errore sollevato dal costruttore.
    except:
        # Registra che la costruzione non valida è stata rifiutata.
        invalid_rejected = True
    # Fa fallire il test se il costruttore ha accettato capacità zero.
    assert_true(invalid_rejected)
    # Stampa il successo soltanto dopo che tutte le verifiche sono passate.
    print("Rigtorp: test sequenziali superati")
