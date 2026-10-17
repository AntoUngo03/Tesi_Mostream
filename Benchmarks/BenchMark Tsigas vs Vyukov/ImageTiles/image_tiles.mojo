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


# Benchmark immagini: descrittori UInt64 di blocchi di righe, senza copie dei pixel.
# Layout RGB planare e formula grayscale derivati da Benchmarks/ImagePipeline (LGPL v3).
# Il runner antepone TsigasZhang e il backend Vyukov, senza modificare ImagePipeline.
from std.runtime._asyncrt import TaskGroup
from std.runtime import parallelism_level
from std.time import perf_counter_ns
from std.ffi import OwnedDLHandle, c_int
from std.sys import argv
from std.testing import assert_equal

# Vista non proprietaria: i buffer restano vivi fino alla fine di tutti i task.
@fieldwise_init
struct TileView(Copyable):
    var input: Pointer[UInt8, MutUntrackedOrigin]
    var output: Pointer[UInt8, MutUntrackedOrigin]
    var starts: Pointer[Int, MutUntrackedOrigin]
    var ends: Pointer[Int, MutUntrackedOrigin]
    var completed: Pointer[Int, MutUntrackedOrigin]
    var seen: Pointer[UInt8, MutUntrackedOrigin]
    var width: Int
    var height: Int
    var frames: Int
    var rows: Int
    var blocks: Int

# Buffer di input diversi per immagine e oracolo scalare preparati fuori misura.
struct TileImages:
    var view: TileView
    var reference: Pointer[UInt8, MutUntrackedOrigin]

    def __init__(out self, width: Int, height: Int, frames: Int, rows: Int):
        var pixels = width * height
        var length = pixels * 3 * frames
        var blocks = (height + rows - 1) // rows
        var src = unsafe_alloc[UInt8](length)
        var dst = unsafe_alloc[UInt8](length)
        self.reference = unsafe_alloc[UInt8](length)
        var starts = unsafe_alloc[Int](frames)
        var ends = unsafe_alloc[Int](frames)
        var completed = unsafe_alloc[Int](frames)
        var seen = unsafe_alloc[UInt8](frames * blocks)
        self.view = TileView(src, dst, starts, ends, completed, seen, width, height, frames, rows, blocks)
        for i in range(frames):
            starts.unsafe_offset(i).unsafe_write(0)
            ends.unsafe_offset(i).unsafe_write(0)
            completed.unsafe_offset(i).unsafe_write(0)
            for y in range(height):
                for x in range(width):
                    var position = i * pixels * 3 + y * width + x
                    var r = ((x * 255) // max(width - 1, 1) + i * 17) & 255
                    var g = ((y * 255) // max(height - 1, 1) + i * 31) & 255
                    var b = (((x + y) * 127) // max(width + height - 2, 1) + i * 47) & 255
                    src.unsafe_offset(position).unsafe_write(UInt8(r))
                    src.unsafe_offset(position + pixels).unsafe_write(UInt8(g))
                    src.unsafe_offset(position + 2 * pixels).unsafe_write(UInt8(b))
                    # Oracolo scalare indipendente dal percorso SIMD misurato.
                    var gray = UInt8((r * 77 + g * 150 + b * 29) >> 8)
                    for channel in range(3):
                        self.reference.unsafe_offset(position + channel * pixels).unsafe_write(gray)
                        dst.unsafe_offset(position + channel * pixels).unsafe_write(UInt8(0))
        for i in range(frames * blocks):
            seen.unsafe_offset(i).unsafe_write(UInt8(0))

    def __deinit__(deinit self):
        self.view.input.unsafe_free()
        self.view.output.unsafe_free()
        self.view.starts.unsafe_free()
        self.view.ends.unsafe_free()
        self.view.completed.unsafe_free()
        self.view.seen.unsafe_free()
        self.reference.unsafe_free()

# Lo stesso kernel SIMD è usato nella baseline seriale e nei consumer.
@no_inline
def tile_grayscale(view: TileView, frame: Int, first: Int, last: Int):
    var pixels = view.width * view.height
    var base = frame * pixels * 3
    var src = view.input.unsafe_offset(base)
    var dst = view.output.unsafe_offset(base)
    var i = first
    comptime CHUNK = 8
    while i + CHUNK <= last:
        var r = src.unsafe_offset(i).unsafe_load[width=CHUNK]().cast[DType.uint16]()
        var g = src.unsafe_offset(i + pixels).unsafe_load[width=CHUNK]().cast[DType.uint16]()
        var b = src.unsafe_offset(i + 2 * pixels).unsafe_load[width=CHUNK]().cast[DType.uint16]()
        var gray = ((r * 77 + g * 150 + b * 29) >> 8).cast[DType.uint8]()
        dst.unsafe_offset(i).unsafe_store(gray)
        dst.unsafe_offset(i + pixels).unsafe_store(gray)
        dst.unsafe_offset(i + 2 * pixels).unsafe_store(gray)
        i += CHUNK
    while i < last:
        var r = Int(src.unsafe_offset(i)[])
        var g = Int(src.unsafe_offset(i + pixels)[])
        var b = Int(src.unsafe_offset(i + 2 * pixels)[])
        var gray = UInt8((r * 77 + g * 150 + b * 29) >> 8)
        dst.unsafe_offset(i)[] = gray
        dst.unsafe_offset(i + pixels)[] = gray
        dst.unsafe_offset(i + 2 * pixels)[] = gray
        i += 1

struct TileSignals:
    var ready: Atomic[UInt64]
    var start: Atomic[UInt64]
    var errors: Atomic[UInt64]

    def __init__(out self):
        self.ready = Atomic[UInt64](0)
        self.start = Atomic[UInt64](0)
        self.errors = Atomic[UInt64](0)

def tile_pin(cpu: Int, library_path: String) -> Int:
    if cpu < 0:
        return 0
    try:
        var library = OwnedDLHandle(library_path)
        return Int(library.call["pin_thread_to_cpu", c_int](c_int(cpu)))
    except:
        return 1

def tile_wait(signals: Pointer[TileSignals, MutUntrackedOrigin], cpu: Int, library: String):
    if tile_pin(cpu, library) != 0:
        _ = signals[].errors.fetch_add(1)
    _ = signals[].ready.fetch_add[ordering=Ordering.RELEASE](1)
    while (signals[].start.load[ordering=Ordering.ACQUIRE]() == 0):
        pass

# L'inizio di latenza precede il primo enqueue dell'immagine, incluse eventuali attese.
async def tile_source(view: TileView, queue: Pointer[GrainQueue, MutUntrackedOrigin], workers: Int,
                      signals: Pointer[TileSignals, MutUntrackedOrigin], cpu: Int, library: String):
    tile_wait(signals, cpu, library)
    for frame in range(view.frames):
        view.starts.unsafe_offset(frame)[] = perf_counter_ns()
        for block in range(view.blocks):
            try:
                queue[].push(UInt64(frame * view.blocks + block + 1))
            except:
                abort("Payload non rappresentabile dal backend Tsigas")
    for _ in range(workers):
        try:
            queue[].push(UInt64(0))
        except:
            abort("Payload non rappresentabile dal backend Tsigas")

# Ogni blocco scrive una regione disgiunta dell'output già allocato.
async def tile_consumer(view: TileView, incoming: Pointer[GrainQueue, MutUntrackedOrigin],
                        outgoing: Pointer[GrainQueue, MutUntrackedOrigin],
                        counts: Pointer[Int, MutUntrackedOrigin], index: Int,
                        signals: Pointer[TileSignals, MutUntrackedOrigin], cpu: Int, library: String):
    tile_wait(signals, cpu, library)
    var count = 0
    while True:
        var message = incoming[].pop()
        if message == 0:
            try:
                outgoing[].push(UInt64(0))
            except:
                abort("Payload non rappresentabile dal backend Tsigas")
            break
        if message > UInt64(view.frames * view.blocks):
            _ = signals[].errors.fetch_add(1)
            continue
        var task = Int(message) - 1
        var frame = task // view.blocks
        var block = task % view.blocks
        var first = block * view.rows * view.width
        var last = min((block + 1) * view.rows, view.height) * view.width
        tile_grayscale(view, frame, first, last)
        # Release della coda risultati pubblica anche le scritture dei pixel.
        try:
            outgoing[].push(message)
        except:
            abort("Payload non rappresentabile dal backend Tsigas")
        count += 1
    counts.unsafe_offset(index)[] = count

# Ricomposizione logica: i pixel sono già al loro posto; qui si certifica il completamento.
async def tile_collector(view: TileView, queue: Pointer[GrainQueue, MutUntrackedOrigin], workers: Int,
                         signals: Pointer[TileSignals, MutUntrackedOrigin], cpu: Int, library: String):
    tile_wait(signals, cpu, library)
    var finished = 0
    while finished < workers:
        var message = queue[].pop()
        if message == 0:
            finished += 1
            continue
        if message > UInt64(view.frames * view.blocks):
            _ = signals[].errors.fetch_add(1)
            continue
        var task = Int(message) - 1
        if view.seen.unsafe_offset(task)[] != 0:
            _ = signals[].errors.fetch_add(1)
            continue
        view.seen.unsafe_offset(task)[] = 1
        var frame = task // view.blocks
        view.completed.unsafe_offset(frame)[] += 1
        if view.completed.unsafe_offset(frame)[] == view.blocks:
            view.ends.unsafe_offset(frame)[] = perf_counter_ns()

@fieldwise_init
struct TileMeasurement(Movable):
    var elapsed: Int
    var checksum: UInt64
    var reference_checksum: UInt64
    var latencies: List[Int]
    var worker_counts: List[Int]

# Timing separato dal confronto esatto di tutti i byte e dal checksum.
def tile_measure(mut images: TileImages, serial: Bool, workers: Int, capacity: Int,
                 cpus: List[Int], library: String) raises -> TileMeasurement:
    var view = images.view.copy()
    var elapsed: Int
    var errors = 0
    var worker_counts = List[Int]()
    if serial:
        var cpu = -1
        if len(cpus) > 1:
            cpu = cpus[1]
        assert_equal(tile_pin(cpu, library), 0)
        var begin = perf_counter_ns()
        for frame in range(view.frames):
            view.starts.unsafe_offset(frame)[] = perf_counter_ns()
            tile_grayscale(view, frame, 0, view.width * view.height)
            view.ends.unsafe_offset(frame)[] = perf_counter_ns()
        elapsed = Int(perf_counter_ns() - begin)
    else:
        var incoming = unsafe_alloc[GrainQueue](1)
        incoming.unsafe_write(GrainQueue(capacity))
        var outgoing = unsafe_alloc[GrainQueue](1)
        outgoing.unsafe_write(GrainQueue(capacity))
        var signals = unsafe_alloc[TileSignals](1)
        signals.unsafe_write(TileSignals())
        var counts = unsafe_alloc[Int](workers)
        for i in range(workers):
            counts.unsafe_offset(i).unsafe_write(0)
        var tasks = TaskGroup()
        var source_cpu = -1
        var collector_cpu = -1
        if len(cpus) > 0:
            source_cpu = cpus[0]
            collector_cpu = cpus[workers + 1]
        tasks.create_task(tile_source(view, incoming, workers, signals, source_cpu, library))
        for i in range(workers):
            var cpu = -1
            if len(cpus) > 0:
                cpu = cpus[i + 1]
            tasks.create_task(tile_consumer(view, incoming, outgoing, counts, i, signals, cpu, library))
        tasks.create_task(tile_collector(view, outgoing, workers, signals, collector_cpu, library))
        while (signals[].ready.load[ordering=Ordering.ACQUIRE]() != UInt64(workers + 2)):
            pass
        var begin = perf_counter_ns()
        signals[].start.store[ordering=Ordering.RELEASE](1)
        tasks.wait()
        elapsed = Int(perf_counter_ns() - begin)
        errors += Int(signals[].errors.load())
        var total_blocks = 0
        for i in range(workers):
            worker_counts.append(counts.unsafe_offset(i)[])
            total_blocks += counts.unsafe_offset(i)[]
        if total_blocks != view.frames * view.blocks:
            errors += 1
        for task in range(view.frames * view.blocks):
            if view.seen.unsafe_offset(task)[] != 1:
                errors += 1
        for frame in range(view.frames):
            if view.completed.unsafe_offset(frame)[] != view.blocks:
                errors += 1
        counts.unsafe_free()
        signals.unsafe_deinit_pointee()
        signals.unsafe_free()
        incoming.unsafe_deinit_pointee()
        incoming.unsafe_free()
        outgoing.unsafe_deinit_pointee()
        outgoing.unsafe_free()
    var checksum = UInt64(14695981039346656037)
    var reference_checksum = UInt64(14695981039346656037)
    for i in range(view.width * view.height * view.frames * 3):
        var actual = view.output.unsafe_offset(i)[]
        var expected = images.reference.unsafe_offset(i)[]
        if actual != expected:
            errors += 1
        checksum = (checksum ^ UInt64(actual)) * UInt64(1099511628211)
        reference_checksum = (reference_checksum ^ UInt64(expected)) * UInt64(1099511628211)
    var latencies = List[Int]()
    for frame in range(view.frames):
        var latency = view.ends.unsafe_offset(frame)[] - view.starts.unsafe_offset(frame)[]
        if view.ends.unsafe_offset(frame)[] == 0 or latency < 0:
            errors += 1
        latencies.append(latency)
    assert_equal(errors, 0)
    assert_equal(checksum, reference_checksum)
    return TileMeasurement(elapsed, checksum, reference_checksum, latencies^, worker_counts^)

def main() raises:
    var args = argv()
    if len(args) == 2 and args[1] == "info":
        print("WORKERS", parallelism_level())
        return
    if len(args) != 10:
        raise Error("Uso: binary serial|parallel width height frames block_rows consumers capacity cpus library")
    if args[1] != "serial" and args[1] != "parallel":
        raise Error("Modalità non valida")
    var serial = args[1] == "serial"
    var width = Int(args[2])
    var height = Int(args[3])
    var frames = Int(args[4])
    var rows = Int(args[5])
    var workers = Int(args[6])
    var capacity = Int(args[7])
    if width < 1 or height < 1 or frames < 1 or rows < 1 or capacity < 2:
        raise Error("Dimensioni o capacità non valide")
    if not serial and (workers < 1 or workers + 2 > parallelism_level()):
        raise Error("Servono N+2 worker: produttore, N consumer e raccoglitore")
    var cpus = List[Int]()
    if args[8] != "none":
        var parts = args[8].split(",")
        for part in parts:
            cpus.append(Int(part))
        if len(cpus) < max(2, workers + 2):
            raise Error("CPU insufficienti per il pinning")
    # Ogni coda è nuova per warm-up e misura; include le sentinelle per worker.
    if args[1] != "serial" and capacity <= 2 * (frames * ((height + min(rows, height) - 1) // min(rows, height)) + workers):
        raise Error("Anello insufficiente per il vincolo anti-ABA della prova")
    var warm = TileImages(width, height, min(frames, 4), rows)
    _ = tile_measure(warm, serial, workers, capacity, cpus, args[9])
    var images = TileImages(width, height, frames, rows)
    var result = tile_measure(images, serial, workers, capacity, cpus, args[9])
    print("RESULT", result.elapsed, result.checksum, result.reference_checksum)
    for frame in range(frames):
        print("LATENCY", frame, result.latencies[frame])
    for i in range(len(result.worker_counts)):
        print("COUNT", i, result.worker_counts[i])
