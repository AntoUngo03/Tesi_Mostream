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

# Stadi dell'applicazione Spike Detection (da DSPBench):
#   - SensorSource: riproduce le letture dei sensori caricate in memoria, per una durata o un numero di passate fissati
#   - MovingAverage: media mobile degli ultimi WINDOW valori di ogni sensore
#   - SpikeDetector: emette uno spike quando |valore - media| > THRESHOLD * media
#   - SpikeSink: conta gli spike e calcola un hash indipendente dall'ordine, per la verifica
#   MovingAverage tiene lo stato di ogni sensore in una tabella condivisa da tutte le sue repliche,
#   e lo stato di ogni sensore è protetto da un proprio spinlock. Con MovingAverage a una replica
#   l'output coincide con il riferimento sequenziale; con più repliche le letture dello stesso
#   sensore possono essere elaborate fuori ordine, e le letture in ritardo vengono contate.
#
#   Perché una tabella condivisa: in DSPBench MovingAverage riceve i dati con fieldsGrouping, quindi
#   tutte le letture di un sensore vanno alla stessa replica e il suo stato è privato. In MoStream
#   tutte le repliche di uno stadio leggono da un'unica coda condivisa, quindi qualsiasi replica può
#   ricevere qualsiasi sensore: lo stato va condiviso e protetto (prima soluzione proposta per lo stato per chiave).
#
#   Contatori: ogni replica conta in campi locali (nessuna contesa durante l'esecuzione) e li somma
#   ai contatori atomici di SpikeShared una sola volta, quando riceve la fine dello stream (EOS).
#   main li legge dopo il ritorno di run() / run_cooperative(), quando tutte le repliche hanno finito.

from MoStream import StageKind, StageTrait
from std.atomic import Atomic, Ordering
from std.collections import Optional
from std.memory import Pointer, bitcast
from std.memory.alloc import unsafe_alloc
from std.sys.info import size_of
from std.time import perf_counter_ns

comptime WINDOW: Int = 1000 # valori tenuti per ogni sensore (moving_average.window di DSPBench)
comptime THRESHOLD: Float64 = 0.03 # distanza relativa dalla media (spike_detector.threshold di DSPBench)

# Messaggi
# SensorSource -> MovingAverage: una lettura di un sensore
struct SensorReading(Copyable & Deinitable):
    var device: Int
    var seq: Int # posizione della lettura nella sequenza del suo sensore
    var value: Float64

    # costruttore
    def __init__(out self, device: Int, seq: Int, value: Float64):
        self.device = device
        self.seq = seq
        self.value = value

# MovingAverage -> SpikeDetector: la lettura insieme alla media mobile del suo sensore
struct AverageReading(Copyable & Deinitable):
    var device: Int
    var seq: Int
    var value: Float64
    var average: Float64

    # costruttore
    def __init__(out self, device: Int, seq: Int, value: Float64, average: Float64):
        self.device = device
        self.seq = seq
        self.value = value
        self.average = average

# SpikeDetector -> SpikeSink: una lettura riconosciuta come spike
struct SpikeEvent(Copyable & Deinitable):
    var device: Int
    var seq: Int
    var value: Float64
    var average: Float64

    # costruttore
    def __init__(out self, device: Int, seq: Int, value: Float64, average: Float64):
        self.device = device
        self.seq = seq
        self.value = value
        self.average = average

# Hash di uno spike indipendente dall'ordine: finalizzatore splitmix64 di (device << 32 | seq)
# (device, seq) identifica una lettura in ogni esecuzione, qualunque sia il parallelismo. Il sink somma
# gli hash modulo 2^64: la somma non dipende dall'ordine di arrivo degli spike, quindi due esecuzioni
# hanno trovato lo stesso insieme di spike se hanno lo stesso conteggio e la stessa somma (reference.py
# calcola lo stesso valore). Il finalizzatore mescola i bit, quindi insiemi diversi danno somme diverse
# con probabilità altissima.
def spike_hash(device: Int, seq: Int) -> UInt64:
    var z = (UInt64(device) << 32) | UInt64(seq)
    z = z + UInt64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> 30)) * UInt64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> 27)) * UInt64(0x94D049BB133111EB)
    return z ^ (z >> 31)

# Stato di un sensore, allungato a una linea di cache per evitare il false sharing fra sensori
# La finestra è un buffer circolare di WINDOW valori: i size valori a partire da head, in ordine
# di arrivo. sum è la somma dei valori nella finestra, aggiornata in modo incrementale come in
# DSPBench (si toglie il valore più vecchio, si aggiunge il nuovo), così i risultati in virgola mobile coincidono.
# Lo stato sta in una tabella indicizzata dall'id del sensore; senza il riempimento gli stati di sensori
# vicini condividerebbero una linea di cache, e repliche che lavorano su sensori diversi si
# rallenterebbero a vicenda.
struct DeviceState(Movable):
    # byte che mancano per arrivare a 64: 4 campi da un Int (size, head, max_seq, puntatore window), il lock e sum
    comptime PAD_BYTES = 64 - 4 * size_of[Int]() - size_of[Atomic[Int64]]() - size_of[Float64]()
    var lock: Atomic[Int64] # 0 = libero, 1 = preso
    var size: Int # numero di valori nella finestra
    var head: Int # posizione del valore più vecchio nella finestra circolare
    var sum: Float64
    var max_seq: Int # numero di sequenza più alto elaborato, per contare le letture in ritardo
    var window: Pointer[Float64, MutUntrackedOrigin] # WINDOW valori, allocati da SpikeShared
    var pad: Array[UInt8, Self.PAD_BYTES]

    # costruttore
    def __init__(out self, window: Pointer[Float64, MutUntrackedOrigin]):
        self.lock = Atomic[Int64](0)
        self.size = 0
        self.head = 0
        self.sum = 0.0
        self.max_seq = -1 # nessuna lettura ancora elaborata
        self.window = window
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

    # costruttore di spostamento
    def __init__(out self, *, deinit move: Self):
        self.lock = Atomic[Int64](move.lock.load())
        self.size = move.size
        self.head = move.head
        self.sum = move.sum
        self.max_seq = move.max_seq
        self.window = move.window
        self.pad = Array[UInt8, Self.PAD_BYTES](uninitialized=True)

    # acquisizione dello spinlock, restituisce il numero di tentativi falliti
    # La CAS porta il lock da 0 a 1 solo se è libero. ACQUIRE in caso di successo rende visibili a questo
    # thread le scritture fatte dal proprietario precedente prima del suo rilascio. Se fallisce, la CAS
    # scrive in expected il valore attuale, quindi expected va rimesso a 0 prima di riprovare.
    @always_inline
    def acquire(mut self) -> Int:
        var waits = 0
        var expected = Int64(0)
        while not self.lock.compare_exchange[success_ordering=Ordering.ACQUIRE, failure_ordering=Ordering.RELAXED](expected, Int64(1)):
            expected = Int64(0)
            waits += 1
        return waits

    # rilascio dello spinlock
    # RELEASE rende visibili le modifiche allo stato al prossimo thread che acquisisce il lock
    @always_inline
    def release(mut self):
        self.lock.store[ordering=Ordering.RELEASE](Int64(0))

# Dati condivisi da tutti gli stadi e le repliche: dataset, stati dei sensori e contatori finali
# Sono allocati una sola volta in main e ogni stadio riceve un puntatore (SharedPtr), così tutte le
# copie di uno stadio fatte da parallel() lavorano sullo stesso oggetto.
struct SpikeShared(Movable):
    var n: Int # numero di letture nel dataset
    var max_device: Int # id di sensore più alto: le tabelle sotto hanno max_device + 1 elementi
    var devices: Pointer[Int, MutUntrackedOrigin] # id del sensore di ogni lettura
    var values: Pointer[Float64, MutUntrackedOrigin] # valore (temperatura) di ogni lettura
    var next_seq: Pointer[Int, MutUntrackedOrigin] # contatori di sequenza per sensore, usati solo dalla sorgente
    var states: Pointer[DeviceState, MutUntrackedOrigin]
    var windows: Pointer[Float64, MutUntrackedOrigin] # (max_device + 1) * WINDOW valori
    var emitted: Atomic[Int64] # letture prodotte dalla sorgente
    var averaged: Atomic[Int64] # letture elaborate da MovingAverage
    var late: Atomic[Int64] # letture elaborate dopo una lettura successiva dello stesso sensore
    var lock_waits: Atomic[Int64] # tentativi falliti di acquisire il lock di un sensore
    var detected: Atomic[Int64] # letture elaborate da SpikeDetector
    var spikes: Atomic[Int64] # spike ricevuti dal sink
    var spike_hash_sum: Atomic[UInt64] # somma di spike_hash degli spike ricevuti, modulo 2^64

    # costruttore: carica il file binario scritto da prepare_dataset.py
    # Formato (little endian): int64 n, int64 max_device, poi n record (int64 device, float64 value)
    def __init__(out self, path: String) raises:
        var f = open(path, "r")
        var bytes = f.read_bytes()
        f.close()
        if len(bytes) < 16:
            raise Error("invalid dataset file: " + path)
        self.n = Int(read_u64(bytes, 0))
        self.max_device = Int(read_u64(bytes, 8))
        if len(bytes) != 16 + 16 * self.n:
            raise Error("invalid dataset size: " + path)
        self.devices = unsafe_alloc[Int](self.n)
        self.values = unsafe_alloc[Float64](self.n)
        for i in range(self.n):
            # il record i inizia al byte 16 + 16 * i: device nei primi 8 byte, value nei successivi 8
            self.devices.unsafe_offset(i)[] = Int(read_u64(bytes, 16 + 16 * i))
            # il valore è salvato come i 64 bit di un float64: vanno reinterpretati, non convertiti
            self.values.unsafe_offset(i)[] = bitcast[DType.float64](read_u64(bytes, 24 + 16 * i))
        var num_devices = self.max_device + 1
        self.next_seq = unsafe_alloc[Int](num_devices)
        self.windows = unsafe_alloc[Float64](num_devices * WINDOW)
        self.states = unsafe_alloc[DeviceState](num_devices)
        for d in range(num_devices):
            self.next_seq.unsafe_offset(d)[] = 0
            # ogni sensore riceve la propria porzione di WINDOW valori dell'array windows
            self.states.unsafe_offset(d).unsafe_write(DeviceState(self.windows.unsafe_offset(d * WINDOW)))
        self.emitted = Atomic[Int64](0)
        self.averaged = Atomic[Int64](0)
        self.late = Atomic[Int64](0)
        self.lock_waits = Atomic[Int64](0)
        self.detected = Atomic[Int64](0)
        self.spikes = Atomic[Int64](0)
        self.spike_hash_sum = Atomic[UInt64](0)

    # costruttore di spostamento
    def __init__(out self, *, deinit move: Self):
        self.n = move.n
        self.max_device = move.max_device
        self.devices = move.devices
        self.values = move.values
        self.next_seq = move.next_seq
        self.states = move.states
        self.windows = move.windows
        self.emitted = Atomic[Int64](move.emitted.load())
        self.averaged = Atomic[Int64](move.averaged.load())
        self.late = Atomic[Int64](move.late.load())
        self.lock_waits = Atomic[Int64](move.lock_waits.load())
        self.detected = Atomic[Int64](move.detected.load())
        self.spikes = Atomic[Int64](move.spikes.load())
        self.spike_hash_sum = Atomic[UInt64](move.spike_hash_sum.load())

    # distruttore
    def __deinit__(deinit self):
        for d in range(self.max_device + 1):
            self.states.unsafe_offset(d).unsafe_deinit_pointee()
        self.states.unsafe_free()
        self.windows.unsafe_free()
        self.next_seq.unsafe_free()
        self.values.unsafe_free()
        self.devices.unsafe_free()

# legge un UInt64 little endian da una lista di byte
def read_u64(bytes: List[UInt8], offset: Int) -> UInt64:
    var u = UInt64(0)
    for i in range(8):
        u |= UInt64(bytes[offset + i]) << UInt64(8 * i) # il byte i contiene i bit 8*i .. 8*i+7
    return u

# puntatore ai dati condivisi, memorizzato in ogni stadio (gli stadi vengono copiati, i dati no)
comptime SharedPtr = Pointer[SpikeShared, MutUntrackedOrigin]

# SensorSource: riproduce il dataset per duration_sec secondi, o per un numero fissato di passate se passes > 0
# Le letture sono già in memoria, quindi la sorgente legge solo array: il throughput misurato è
# quello della pipeline, non del parsing. Arrivata alla fine del dataset riparte dalla prima
# lettura; i numeri di sequenza continuano a crescere da una passata all'altra.
struct SensorSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = SensorReading
    comptime OutType = SensorReading
    comptime name = "SensorSource"
    var shared: SharedPtr
    var duration_ns: Int
    var passes: Int
    var pos: Int # prossima lettura del dataset da emettere
    var passes_done: Int
    var count: Int # letture emesse finora
    var start_ns: Int
    var started: Bool

    # costruttore
    def __init__(out self, shared: SharedPtr, duration_sec: Int, passes: Int):
        self.shared = shared
        self.duration_ns = duration_sec * 1_000_000_000
        self.passes = passes
        self.pos = 0
        self.passes_done = 0
        self.count = 0
        self.start_ns = 0
        self.started = False

    # implementazione di next_element
    # Restituire None termina lo stream: MoStream manda poi l'EOS agli stadi successivi.
    def next_element(mut self) -> Optional[SensorReading]:
        # il cronometro parte alla prima chiamata, a pipeline avviata, non nel costruttore
        if not self.started:
            self.start_ns = Int(perf_counter_ns())
            self.started = True
        if self.pos == self.shared[].n:
            self.pos = 0
            self.passes_done += 1
        var finished = False
        if self.passes > 0:
            finished = self.passes_done == self.passes
        elif (self.count & 1023) == 0:
            # leggere l'orologio a ogni tupla costerebbe più della tupla stessa:
            # si controlla ogni 1024 tuple (count & 1023 == 0 quando count è multiplo di 1024)
            finished = Int(perf_counter_ns()) - self.start_ns >= self.duration_ns
        if finished:
            _ = self.shared[].emitted.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.count))
            return None
        var device = self.shared[].devices.unsafe_offset(self.pos)[]
        # la sorgente è sequenziale, quindi next_seq non ha bisogno di sincronizzazione
        var seq_ptr = self.shared[].next_seq.unsafe_offset(device)
        var seq = seq_ptr[]
        seq_ptr[] = seq + 1
        var value = self.shared[].values.unsafe_offset(self.pos)[]
        self.pos += 1
        self.count += 1
        return SensorReading(device, seq, value)

# MovingAverage: media mobile degli ultimi WINDOW valori di ogni sensore, sotto il lock del sensore
# Repliche che lavorano su sensori diversi non si aspettano mai; due repliche che ricevono
# nello stesso momento letture dello stesso sensore vengono serializzate dal suo lock.
struct MovingAverage(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = SensorReading
    comptime OutType = AverageReading
    comptime name = "MovingAverage"
    var shared: SharedPtr
    var count: Int
    var late: Int
    var lock_waits: Int

    # costruttore
    def __init__(out self, shared: SharedPtr):
        self.shared = shared
        self.count = 0
        self.late = 0
        self.lock_waits = 0

    # implementazione di compute: stesse operazioni, nello stesso ordine, di MovingAverageBolt di DSPBench
    def compute(mut self, var input: SensorReading) -> Optional[AverageReading]:
        var state = self.shared[].states.unsafe_offset(input.device)
        self.lock_waits += state[].acquire()
        # Una lettura è in ritardo se è già stata elaborata una lettura dello stesso sensore con seq
        # più alto: un'altra replica l'ha sorpassata. Viene comunque aggiunta alla finestra (il risultato
        # può differire da quello sequenziale), solo contata.
        if input.seq < state[].max_seq:
            self.late += 1
        else:
            state[].max_seq = input.seq
        if state[].size > WINDOW - 1:
            # finestra piena: il nuovo valore prende il posto del più vecchio, e il più vecchio
            # diventa quello nella posizione successiva
            var oldest = state[].window.unsafe_offset(state[].head)
            state[].sum -= oldest[]
            oldest[] = input.value
            state[].head = (state[].head + 1) % WINDOW
        else:
            # finestra non ancora piena: si aggiunge dopo l'ultimo valore
            state[].window.unsafe_offset((state[].head + state[].size) % WINDOW)[] = input.value
            state[].size += 1
        state[].sum += input.value
        var average = state[].sum / Float64(state[].size)
        state[].release()
        # il messaggio è creato dopo il rilascio: il lock è tenuto solo mentre si usa lo stato
        self.count += 1
        return AverageReading(input.device, input.seq, input.value, average)

    # alla ricezione dell'EOS pubblica i contatori locali
    def received_eos(mut self):
        _ = self.shared[].averaged.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.count))
        _ = self.shared[].late.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.late))
        _ = self.shared[].lock_waits.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.lock_waits))

# SpikeDetector: senza stato, emette solo le letture lontane dalla loro media mobile
# Restituire None scarta la lettura: al sink arrivano solo gli spike.
struct SpikeDetector(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = AverageReading
    comptime OutType = SpikeEvent
    comptime name = "SpikeDetector"
    var shared: SharedPtr
    var count: Int

    # costruttore
    def __init__(out self, shared: SharedPtr):
        self.shared = shared
        self.count = 0

    # implementazione di compute
    def compute(mut self, var input: AverageReading) -> Optional[SpikeEvent]:
        self.count += 1
        # stesso test di SpikeDetectionBolt di DSPBench
        if abs(input.value - input.average) > THRESHOLD * input.average:
            return SpikeEvent(input.device, input.seq, input.value, input.average)
        return None

    # alla ricezione dell'EOS pubblica il contatore locale
    def received_eos(mut self):
        _ = self.shared[].detected.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.count))

# SpikeSink: conta gli spike e somma i loro hash; se richiesto scrive le coppie (device, seq) su file
# Il dump è tenuto in memoria e scritto solo alla fine, così non rallenta l'esecuzione; serve
# per le esecuzioni di verifica (compare_dump.py), non per le misure.
struct SpikeSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = SpikeEvent
    comptime OutType = SpikeEvent
    comptime name = "SpikeSink"
    var shared: SharedPtr
    var count: Int
    var hash_sum: UInt64 # si azzera ciclicamente modulo 2^64, come in reference.py
    var dump_path: String # vuoto: nessun dump
    var dump: List[Int] # device e seq di ogni spike, uno dopo l'altro

    # costruttore
    def __init__(out self, shared: SharedPtr, dump_path: String):
        self.shared = shared
        self.count = 0
        self.hash_sum = 0
        self.dump_path = dump_path
        self.dump = List[Int]()

    # implementazione di consume_element
    def consume_element(mut self, var input: SpikeEvent):
        self.count += 1
        self.hash_sum += spike_hash(input.device, input.seq)
        if self.dump_path:
            self.dump.append(input.device)
            self.dump.append(input.seq)

    # alla ricezione dell'EOS pubblica i risultati e scrive il dump, se richiesto
    def received_eos(mut self):
        _ = self.shared[].spikes.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](Int64(self.count))
        _ = self.shared[].spike_hash_sum.fetch_add[ordering=Ordering.ACQUIRE_RELEASE](self.hash_sum)
        if self.dump_path:
            try:
                # una riga "device seq" per spike, il formato letto da compare_dump.py
                var text = String()
                for i in range(0, len(self.dump), 2):
                    text += String(self.dump[i]) + " " + String(self.dump[i + 1]) + "\n"
                var f = open(self.dump_path, "w")
                f.write(text)
                f.close()
            except e:
                print("SpikeSink: cannot write", self.dump_path, e)
