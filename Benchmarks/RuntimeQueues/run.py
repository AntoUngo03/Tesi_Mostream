"""Compare communication backends through the public MoStream Pipeline API."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
from datetime import datetime, timezone


def execute(command, root, timeout=180):
    return subprocess.check_output(command, cwd=root, text=True, timeout=timeout)


def tagged(output, tag):
    return next(line.split()[1:] for line in output.splitlines() if line.startswith(tag + ' '))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mojo', default='mojo')
    parser.add_argument('--messages', type=int, default=20000)
    parser.add_argument('--workers', default='1,2,4,8')
    parser.add_argument('--work-us', default='0,2,5')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--capacity', type=int, default=1024)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    here = Path(__file__).resolve().parent
    workers = [int(x) for x in args.workers.split(',')]
    targets = [float(x) for x in args.work_us.split(',')]
    if args.messages < 1 or args.repeats < 1 or min(workers) < 1 or min(targets) < 0:
        parser.error('Invalid messages/repeats/workers/work')
    if args.capacity < 2 or args.capacity & (args.capacity - 1):
        parser.error('Capacity must be a power of two >= 2')
    args.output.mkdir(parents=True, exist_ok=False)
    sources = [*sorted((root / 'MoStream').rglob('*.mojo')), here / 'farm.mojo', Path(__file__)]
    metadata = {'utc': datetime.now(timezone.utc).isoformat(), 'host': platform.node(),
                'mojo': execute([args.mojo, '--version'], root).strip(),
                'cpu': Path('/proc/cpuinfo').read_text(), 'affinity': sorted(os.sched_getaffinity(0)),
                'parameters': {**vars(args), 'output': str(args.output)},
                'sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
                'scope': 'Pipeline.run wall time; exact validation outside timer; pinning disabled',
                'seed': 20261026}
    rows = []
    with tempfile.TemporaryDirectory(prefix='mostream-runtime-bench-') as directory:
        binaries = {}
        for backend in ('vyukov', 'rigtorp'):
            binary = Path(directory) / backend
            command = [args.mojo, 'build', '-I', str(root)]
            if backend == 'rigtorp':
                command += ['-D', 'MOSTREAM_RIGTORP=1']
            subprocess.run([*command, str(here / 'farm.mojo'), '-o', str(binary)], cwd=root, check=True, timeout=180)
            binaries[backend] = str(binary)
        available = int(tagged(execute([binaries['vyukov'], 'info'], root), 'WORKERS')[0])
        if max(workers) + 2 > available:
            parser.error(f'Requires {max(workers)+2} Mojo workers; available: {available}')
        samples = [float(tagged(execute([binaries['vyukov'], 'calibrate'], root), 'CALIBRATION')[0]) for _ in range(5)]
        ns_iteration = statistics.median(samples)
        iterations = {t: 0 if t == 0 else max(1, round(t * 1000 / ns_iteration)) for t in targets}
        metadata.update(calibration_ns=samples, ns_per_iteration=ns_iteration, runtime_workers=available)
        (args.output / 'metadata.json').write_text(json.dumps(metadata, indent=2))
        # Warm up both binaries before collecting samples.
        for backend in binaries:
            execute([binaries[backend], '1000', str(max(workers)), '0', str(args.capacity)], root)
        randomizer = random.Random(metadata['seed'])
        jobs = [(n, t, repeat) for n in workers for t in targets for repeat in range(args.repeats)]
        randomizer.shuffle(jobs)
        with (args.output / 'raw.csv').open('w') as stream:
            writer = None
            for n, t, repeat in jobs:
                backends = list(binaries)
                randomizer.shuffle(backends)
                for backend in backends:
                    elapsed = int(tagged(execute([binaries[backend], str(args.messages), str(n),
                                                 str(iterations[t]), str(args.capacity)], root), 'RESULT')[0])
                    row = dict(backend=backend, workers=n, target_us=t, iterations=iterations[t],
                               messages=args.messages, repeat=repeat, elapsed_ns=elapsed,
                               throughput=args.messages * 1e9 / elapsed)
                    if writer is None:
                        writer = csv.DictWriter(stream, fieldnames=row)
                        writer.writeheader()
                    writer.writerow(row)
                    stream.flush()
                    rows.append(row)
                    print(backend, n, t, repeat, round(row['throughput']), flush=True)
    summary = []
    for n in workers:
        for t in targets:
            group = {b: [r['throughput'] for r in rows if r['backend'] == b and r['workers'] == n and r['target_us'] == t]
                     for b in binaries}
            medians = {b: statistics.median(v) for b, v in group.items()}
            summary.append(dict(workers=n, target_us=t, vyukov_median=medians['vyukov'],
                                rigtorp_median=medians['rigtorp'],
                                rigtorp_over_vyukov=medians['rigtorp']/medians['vyukov'],
                                vyukov_min=min(group['vyukov']), vyukov_max=max(group['vyukov']),
                                rigtorp_min=min(group['rigtorp']), rigtorp_max=max(group['rigtorp'])))
    with (args.output / 'summary.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=summary[0])
        writer.writeheader()
        writer.writerows(summary)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
