# Benchmark attraverso la vera API MoStream:
# Source sequenziale -> Compute replicato su N worker -> Sink sequenziale.
# Ogni input viene elaborato interamente da UNA replica di Compute.
# Le repliche lavorano su input diversi: non dividono il lavoro del singolo input.
# La coda fra gli stadi viene scelta in compilazione dal Communicator:
# default Vyukov; con -D MOSTREAM_RIGTORP=1 si usa Rigtorp.
#
# Modalità del programma, richiamate dal runner Python run.py:
#   binary info
#   binary calibrate
#   binary M N iterazioni capacita
# M = numero di input; N = repliche Compute. I task totali sono N + 2.
# Il runner valida i parametri, ripete le prove e calcola il throughput.
from std.collections import Optional
from std.memory import Pointer
from std.memory.alloc import unsafe_alloc
from std.runtime import parallelism_level
from std.sys import argv
from std.time import perf_counter_ns
from std.testing import assert_equal
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

# Kernel sintetico xorshift: ogni operazione dipende dal risultato precedente.
# @no_inline impedisce l'inlining della funzione nei punti di chiamata.
# var value è una copia locale modificabile; UInt64 lavora su 64 bit.
# iterations determina la quantità di calcolo, senza sleep o letture del clock.
# Con iterations=0 viene restituito il valore iniziale.
@no_inline
def work(var value: UInt64, iterations: Int) -> UInt64:
    for _ in range(iterations):
        value ^= value << 13
        value ^= value >> 7
        value ^= value << 17
    return value

# Messaggio trasportato dalle code: ID per verificare la consegna e valore da elaborare.
# Copyable permette l'uso del tipo nei messaggi di MoStream.
# @fieldwise_init genera il costruttore Item(id, value).
@fieldwise_init
struct Item(Copyable):
    var id: Int
    var value: UInt64

# La sorgente produce M messaggi con ID da 0 a M-1.
# Le dichiarazioni comptime descrivono lo stadio al compilatore MoStream.
struct Source(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Source"
    var index: Int
    var messages: Int

    def __init__(out self, messages: Int):
        self.index = 0
        self.messages = messages

    # Il runtime richiama questo metodo finché riceve None, che termina la sorgente.
    # Optional[Item] rappresenta un messaggio presente oppure l'assenza di messaggio.
    def next_element(mut self) -> Optional[Item]:
        if self.index == self.messages:
            return None
        var index = self.index
        self.index += 1
        # Il seme è ID+1 per evitare lo zero, punto fisso del kernel xorshift.
        return Item(index, UInt64(index + 1))

# Trasformazione replicata: ciascuna replica estrae un input dalla coda condivisa,
# applica tutte le iterations e invia il risultato verso il sink.
# Anche qui @fieldwise_init genera il costruttore, Compute(iterations).
@fieldwise_init
struct Compute(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Compute"
    var iterations: Int

    def compute(mut self, var input: Item) -> Optional[Item]:
        input.value = work(input.value, self.iterations)
        # ^ trasferisce la proprietà del messaggio al risultato restituito.
        # L'ID rimane invariato; solo value viene trasformato.
        return input^

# Un solo sink raccoglie i risultati, anche se arrivano fuori ordine.
# seen[id] conta le ricezioni; values[id] conserva il risultato del calcolo.
# I puntatori si riferiscono ai buffer allocati in main, validi fino a fine prova.
# La registrazione dei risultati rientra nel tempo misurato; il confronto finale no.
@fieldwise_init
struct Sink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Item
    comptime OutType = Item
    comptime name = "Sink"
    var seen: Pointer[Int, MutUntrackedOrigin]
    var values: Pointer[UInt64, MutUntrackedOrigin]
    var messages: Int

    def consume_element(mut self, var input: Item):
        # La cella aggiuntiva seen[M] conta gli ID non validi senza accedere
        # fuori dai buffer. unsafe_offset(i)[] accede alla cella i del puntatore.
        if input.id < 0 or input.id >= self.messages:
            self.seen.unsafe_offset(self.messages)[] += 1
        else:
            self.seen.unsafe_offset(input.id)[] += 1
            self.values.unsafe_offset(input.id)[] = input.value


def main() raises:
    # argv()[0] è il nome del programma; gli argomenti utili iniziano da 1.
    var args = argv()
    # Comunica quanti worker offre il runtime Mojo: devono bastare per N+2 task.
    if args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    # Misura il costo medio di un'iterazione su una chiamata con due milioni
    # di iterazioni. Il runner ripete questa calibrazione e usa la mediana
    # per convertire il tempo obiettivo T in un numero di iterazioni.
    # Questa calibrazione è separata dalla misura della pipeline.
    if args[1] == "calibrate":
        var value = UInt64(12345)
        var begin = perf_counter_ns()
        value = work(value, 2000000)
        # Stampare anche value rende osservabile il risultato del kernel.
        # Il primo numero dopo CALIBRATION è espresso in nanosecondi/iterazione.
        print("CALIBRATION", Float64(perf_counter_ns() - begin) / 2000000.0, value)
        return
    # Modalità benchmark: gli argomenti numerici sono preparati dal runner.
    var messages = Int(args[1])
    var workers = Int(args[2])
    var iterations = Int(args[3])
    var capacity = Int(args[4])
    # Buffer di verifica, allocati prima di avviare il cronometro.
    # seen ha M+1 celle: M contatori per gli ID validi e uno per quelli errati.
    # values non richiede inizializzazione: si legge una cella solo dopo aver
    # verificato che il suo messaggio sia stato ricevuto esattamente una volta.
    var seen = unsafe_alloc[Int](messages + 1)
    var values = unsafe_alloc[UInt64](messages)
    for i in range(messages + 1):
        seen.unsafe_offset(i)[] = 0
    # seq crea una sola istanza; parallel crea N repliche della trasformazione.
    # La pipeline contiene tre stadi logici e N+2 nodi/task complessivi.
    # MoStream gestisce i Communicator, la chiusura e la propagazione di EOS.
    var pipeline = Pipeline((seq(Source(messages)), parallel(Compute(iterations), workers),
                             seq(Sink(seen, values, messages))))
    # Nessun vincolo esplicito task-CPU; stessa impostazione per i due backend.
    pipeline.setPinning(False)
    # Capacità di ciascuna coda fra gli stadi, uguale per Vyukov e Rigtorp.
    pipeline.setQueueSize(capacity)
    # Si misura il tempo di parete di Pipeline.run(): creazione dei communicator
    # e delle code, lancio dei task, elaborazione, EOS, attesa finale e teardown.
    # Sono incluse anche le stampe del runtime. La costruzione di Pipeline sopra,
    # l'avvio del processo e la verifica sotto sono fuori da questo intervallo.
    # run() ritorna quando tutti i task della pipeline sono terminati.
    var begin = perf_counter_ns()
    pipeline.run()
    var elapsed = Int(perf_counter_ns() - begin)
    # Verifica esatta DOPO aver fermato il cronometro:
    # ogni ID deve comparire una volta e il valore deve coincidere con quello
    # calcolato sequenzialmente applicando lo stesso kernel allo stesso seme.
    # Questo controllo rileva input mancanti, duplicati e risultati errati.
    for i in range(messages):
        assert_equal(seen.unsafe_offset(i)[], 1)
        assert_equal(values.unsafe_offset(i)[], work(UInt64(i + 1), iterations))
    # Nessun messaggio con ID esterno all'intervallo previsto.
    assert_equal(seen.unsafe_offset(messages)[], 0)
    # I task sono terminati: i buffer non sono più usati e si possono liberare.
    seen.unsafe_free()
    values.unsafe_free()
    # Il runner legge RESULT e calcola throughput = M * 1e9 / elapsed.
    # elapsed è in nanosecondi e misura l'intera prova, non la latenza individuale.
    print("RESULT", elapsed)
