#!/usr/bin/env python3
"""Benchmark immagini: seriale senza code contro Rigtorp e Vyukov, blocchi di righe."""
import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import tempfile
import time

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
ROOT = BASE.parents[1]
SHARED = BASE / 'PipelineGrain'
SPEC = importlib.util.spec_from_file_location('image_tiles_helpers', SHARED / 'run.py')
HELPERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPERS)
execute = HELPERS.execute


def quantile(values, p):
    ordered = sorted(values)
    position = p * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summarize(rows, output):
    groups = {}
    for row in rows:
        groups.setdefault((row['backend'], row['width'], row['height'], row['block_rows'], row['consumers']), []).append(row)
    baselines = {(width, height): statistics.median(r['elapsed_ns'] for r in group)
                 for (backend, width, height, _, _), group in groups.items() if backend == 'serial'}
    summary = []
    for (backend, width, height, block_rows, consumers), group in sorted(groups.items()):
        elapsed = statistics.median(r['elapsed_ns'] for r in group)
        frames = group[0]['images']
        summary.append({
            'backend': backend, 'width': width, 'height': height, 'images': frames,
            'block_rows': block_rows, 'blocks_per_image': group[0]['blocks_per_image'],
            'consumers': consumers, 'total_tasks': 1 if backend == 'serial' else consumers + 2,
            'repeats': len(group), 'median_elapsed_ns': elapsed,
            'images_per_second': frames * 1e9 / elapsed,
            'min_images_per_second': min(r['images_per_second'] for r in group),
            'max_images_per_second': max(r['images_per_second'] for r in group),
            'speedup_vs_serial': baselines[(width, height)] / elapsed,
            'median_run_mean_latency_ms': statistics.median(r['latency_mean_ns'] for r in group) / 1e6,
            'median_run_median_latency_ms': statistics.median(r['latency_median_ns'] for r in group) / 1e6,
            'median_run_p95_latency_ms': statistics.median(r['latency_p95_ns'] for r in group) / 1e6,
            'min_run_p95_latency_ms': min(r['latency_p95_ns'] for r in group) / 1e6,
            'max_run_p95_latency_ms': max(r['latency_p95_ns'] for r in group) / 1e6,
            'all_outputs_equal': all(r['output_equal'] for r in group),
            'checksum': group[0]['checksum'],
        })
    with output.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sizes', default='512x512', help='Risoluzioni separate da virgole, es. 256x256,512x512')
    parser.add_argument('--images', type=int, default=32)
    parser.add_argument('--block-rows', default='1,8,32,128,512')
    parser.add_argument('--consumers', default='1,2,4,8,16')
    parser.add_argument('--repeats', type=int, default=50)
    parser.add_argument('--capacity', type=int, default=1024)
    parser.add_argument('--cpus', default='auto', help='auto, none oppure una lista di N+2 CPU distinte')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--max-memory-mib', type=int, default=2048, help='Limite stimato per i tre buffer misurati, senza warm-up')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        sizes = sorted(set(tuple(int(v) for v in value.lower().split('x')) for value in args.sizes.split(',')))
        consumers = HELPERS.parse_numbers(args.consumers, int)
        blocks = HELPERS.parse_numbers(args.block_rows, int)
    except ValueError:
        parser.error('Dimensioni e parametri devono essere interi')
    if any(len(size) != 2 or min(size) < 1 for size in sizes) or not consumers or not blocks:
        parser.error('Specificare risoluzioni WxH positive, consumer e blocchi')
    if min(consumers + blocks + [args.images, args.repeats, args.timeout, args.max_memory_mib]) < 1:
        parser.error('I parametri numerici devono essere positivi')
    if args.capacity < 2 or args.capacity & (args.capacity - 1):
        parser.error('La capacità deve essere potenza di due >= 2')
    for width, height in sizes:
        if width * height * args.images * 9 > args.max_memory_mib * 1024**2:
            parser.error('Input, output e riferimento superano --max-memory-mib; ridurre --images o la risoluzione')
    cpus = HELPERS.physical_cpus() if args.cpus == 'auto' else (
        [] if args.cpus == 'none' else [int(v) for v in args.cpus.split(',')])
    if cpus and (len(cpus) < max(consumers) + 2 or len(set(cpus)) != len(cpus)
                 or not set(cpus) <= os.sched_getaffinity(0)):
        parser.error('Servono N+2 CPU distinte: sorgente, N consumer, raccoglitore')
    output = (args.output or HERE / 'results' / time.strftime('%Y%m%d-%H%M%S')).resolve()
    output.mkdir(parents=True, exist_ok=False)
    source_paths = [ROOT / 'MoStream/MPMC_queue_rigtorp.mojo', SHARED / 'queues.mojo',
                    SHARED / 'run.py', HERE / 'image_tiles.mojo', HERE / 'run.py',
                    ROOT / 'Benchmarks/ImagePipeline/image_stages.mojo',
                    ROOT / 'Benchmarks/ImagePipeline/ppm_image.mojo', ROOT / 'MoStream/lib/libpinning.c']
    metadata = {
        'benchmark': 'ImageTiles', 'topology': 'source -> shared work queue -> N consumers -> shared completion queue -> collector',
        'transformation': '(77*R + 150*G + 29*B) >> 8; output RGB planar with equal channels',
        'payload': 'UInt64 descriptor ID; no pixel copy through queues',
        'input': 'deterministic RGB gradients, varied by image ID; no file I/O',
        'arguments': vars(args) | {'output': str(output)}, 'cpus': cpus,
        'allowed_cpus': sorted(os.sched_getaffinity(0)),
        'mojo_version': execute(['mojo', '--version']).strip(), 'platform': platform.platform(),
        'cpuinfo': Path('/proc/cpuinfo').read_text(),
        'latency_definition': 'before first enqueue of image through collector receipt of its final block; serial: full-image kernel start/end',
        'source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths},
    }
    try:
        metadata['git_commit'] = execute(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).strip()
        metadata['git_status'] = execute(['git', '-C', str(ROOT), 'status', '--short'])
    except RuntimeError:
        metadata['git_commit'] = None
    rows = []
    with tempfile.TemporaryDirectory(prefix='mostream-image-tiles-') as directory:
        work = Path(directory)
        library = work / 'libpinning.so'
        if cpus:
            execute(['gcc', '-O3', '-shared', '-fPIC', '-D_GNU_SOURCE',
                     str(ROOT / 'MoStream/lib/libpinning.c'), '-pthread', '-o', str(library)])
        common = source_paths[0].read_text() + '\n' + source_paths[1].read_text()
        harness = (HERE / 'image_tiles.mojo').read_text()
        binaries = {}
        for backend in ('rigtorp', 'vyukov'):
            source = work / f'{backend}.mojo'
            source.write_text(common + '\ncomptime GrainQueue = ' + HELPERS.BACKENDS[backend] + '\n' + harness)
            binary = work / backend
            execute(['mojo', 'build', '-O3', str(source), '-o', str(binary)], args.timeout)
            binaries[backend] = binary
        available = int(execute([str(binaries['rigtorp']), 'info']).split()[-1])
        if max(consumers) + 2 > available:
            raise RuntimeError(f'Servono N+2 worker del runtime: disponibili {available}')
        metadata['runtime_workers'] = available
        cpu_argument = ','.join(map(str, cpus)) if cpus else 'none'

        def measure(backend, width, height, images, block_rows, workers, capacity):
            binary = binaries['rigtorp' if backend == 'serial' else backend]
            command = [str(binary), 'serial' if backend == 'serial' else 'parallel',
                       str(width), str(height), str(images), str(block_rows), str(workers),
                       str(capacity), cpu_argument, str(library)]
            lines = execute(command, args.timeout).splitlines()
            result = next(line.split() for line in lines if line.startswith('RESULT '))
            elapsed, checksum, reference = int(result[1]), result[2], result[3]
            latency_entries = [tuple(map(int, line.split()[1:])) for line in lines if line.startswith('LATENCY ')]
            if sorted(i for i, _ in latency_entries) != list(range(images)):
                raise RuntimeError('Latenze mancanti o duplicate')
            counts = [int(line.split()[2]) for line in lines if line.startswith('COUNT ')]
            effective_rows = min(block_rows, height)
            blocks_per_image = (height + effective_rows - 1) // effective_rows
            if backend != 'serial' and (len(counts) != workers or sum(counts) != images * blocks_per_image):
                raise RuntimeError('Conteggi dei blocchi non coerenti')
            if checksum != reference or elapsed <= 0:
                raise RuntimeError('Output non coerente con l’oracolo seriale')
            return elapsed, checksum, sorted(latency_entries), counts

        # Casi mirati: code di capacità 2, dimensioni non SIMD e più consumer che blocchi.
        for backend in ('serial', 'rigtorp', 'vyukov'):
            n = 0 if backend == 'serial' else max(consumers)
            for width, height, count, block_rows in ((19, 13, 3, 4), (1, 1, 1, 8)):
                measure(backend, width, height, count, block_rows, n, 2)
        metadata['preflight'] = 'passed: 19x13 rows4 frames3; 1x1 rows8 frames1; capacity2; scalar oracle and exact coverage'
        (output / 'metadata.json').write_text(json.dumps(metadata, indent=2))
        # Baseline seriale una volta per risoluzione e repeat; coppie di code vicine nel tempo.
        jobs = []
        for repeat in range(args.repeats):
            for width, height in sizes:
                jobs.append(('serial', width, height, height, 0, repeat))
                for block_rows in sorted({min(row, height) for row in blocks}):
                    for n in consumers:
                        jobs.append(('pair', width, height, block_rows, n, repeat))
        rng = random.Random(args.seed)
        rng.shuffle(jobs)
        total = sum(1 if job[0] == 'serial' else 2 for job in jobs)
        checksums = {}
        position = 0
        with (output / 'raw.csv').open('w', newline='') as raw_stream, (output / 'latencies.csv').open('w', newline='') as latency_stream:
            writer = None
            latency_writer = csv.writer(latency_stream)
            latency_writer.writerow(['backend', 'width', 'height', 'block_rows', 'consumers', 'repeat', 'image_id', 'latency_ns'])
            for kind, width, height, block_rows, workers, repeat in jobs:
                order = ['serial'] if kind == 'serial' else ['rigtorp', 'vyukov']
                rng.shuffle(order)
                for backend in order:
                    elapsed, checksum, latencies, counts = measure(backend, width, height, args.images, block_rows, workers, args.capacity)
                    key = (width, height, args.images)
                    if key in checksums and checksums[key] != checksum:
                        raise RuntimeError('Checksum differente fra baseline, code o configurazioni')
                    checksums[key] = checksum
                    values = [value for _, value in latencies]
                    row = {
                        'backend': backend, 'width': width, 'height': height, 'images': args.images,
                        'block_rows': 0 if backend == 'serial' else block_rows,
                        'blocks_per_image': 0 if backend == 'serial' else (height + block_rows - 1) // block_rows,
                        'consumers': workers, 'total_tasks': 1 if backend == 'serial' else workers + 2,
                        'capacity': 0 if backend == 'serial' else args.capacity, 'repeat': repeat,
                        'elapsed_ns': elapsed, 'images_per_second': args.images * 1e9 / elapsed,
                        'latency_mean_ns': statistics.mean(values), 'latency_median_ns': statistics.median(values),
                        'latency_p95_ns': quantile(values, .95), 'checksum': checksum,
                        'reference_checksum': checksum, 'output_equal': True, 'worker_blocks': json.dumps(counts),
                    }
                    if writer is None:
                        writer = csv.DictWriter(raw_stream, fieldnames=list(row))
                        writer.writeheader()
                    writer.writerow(row)
                    raw_stream.flush()
                    for image, latency in latencies:
                        latency_writer.writerow([backend, width, height, row['block_rows'], workers, repeat, image, latency])
                    latency_stream.flush()
                    rows.append(row)
                    position += 1
                    print(f'[{position}/{total}] {backend} {width}x{height} rows={row["block_rows"]} N={workers}: {row["images_per_second"]:.1f} img/s, p95={row["latency_p95_ns"]/1e6:.3f} ms, output uguale', flush=True)
    summarize(rows, output / 'summary.csv')
    (output / 'COMPLETATO.txt').write_text('Tutti gli output verificati byte per byte contro il riferimento scalare.\n')
    print(f'Risultati: {output}')


if __name__ == '__main__':
    main()
