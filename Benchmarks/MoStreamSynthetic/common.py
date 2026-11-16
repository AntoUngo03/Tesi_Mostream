"""Parti comuni ai runner FarmThroughput e PipelineGrain dentro MoStream.

Ogni runner compila lo stesso sorgente due volte (Vyukov di default, Rigtorp con
-D MOSTREAM_RIGTORP=1), calibra il kernel, mescola le configurazioni con un seed
fisso e alterna in ordine casuale i due backend per ogni ripetizione.
Ogni campione è un processo nuovo: warm-up interno, poi una misura RESULT.
"""
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

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
BACKENDS = ('vyukov', 'rigtorp')


def numbers(text, kind):
    return [kind(x) for x in text.split(',') if x.strip()]


def common_arguments(parser, messages, repeats):
    parser.add_argument('--mojo', default=os.environ.get('MOJO', 'mojo'),
                        help='compilatore Mojo 1.1 (default: $MOJO oppure mojo)')
    parser.add_argument('--messages', type=int, default=messages)
    parser.add_argument('--repeats', type=int, default=repeats)
    parser.add_argument('--capacity', type=int, default=1024)
    parser.add_argument('--pinning', choices=('on', 'off'), default='on')
    parser.add_argument('--runtime', choices=('std', 'coop'), default='std')
    parser.add_argument('--seed', type=int, default=20261105)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--output', type=Path, default=None,
                        help='cartella nuova; default results/<data-ora>')


def check_common(parser, args):
    if args.messages < 1 or args.repeats < 1:
        parser.error('messages e repeats devono essere >= 1')
    if args.capacity < 2 or args.capacity & (args.capacity - 1):
        parser.error('La capacità deve essere una potenza di due >= 2')


def physical_cpus():
    """Una CPU logica per core fisico, fra quelle dell'affinità corrente."""
    chosen, seen = [], set()
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        try:
            key = (topology.joinpath('physical_package_id').read_text().strip(),
                   topology.joinpath('core_id').read_text().strip())
        except OSError:
            key = ('?', str(cpu))
        if key not in seen:
            seen.add(key)
            chosen.append(cpu)
    return chosen


def cpu_model():
    for line in Path('/proc/cpuinfo').read_text().splitlines():
        if line.startswith('model name'):
            return line.split(':', 1)[1].strip()
    return platform.processor()


def environment(args):
    env = dict(os.environ, MOSTREAM_HOME=str(ROOT))
    if args.pinning == 'on':
        env['MOSTREAM_PINNING'] = ','.join(map(str, args.cpus))
    return env


def execute(command, env, timeout):
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True, timeout=timeout,
                                   stderr=subprocess.STDOUT)


def tagged(output, tag):
    return next(line.split()[1:] for line in output.splitlines() if line.startswith(tag + ' '))


# Define di compilazione che selezionano la coda fra gli stadi (vedi MoStream/communication_queue.mojo).
DEFINES = {'vyukov': [], 'rigtorp': ['-D', 'MOSTREAM_RIGTORP=1'], 'spsc': ['-D', 'MOSTREAM_SPSC=1']}


def build(args, source, directory, backends=BACKENDS):
    """Compila il sorgente una volta per backend. L'ordine di `backends` è quello che
    run_jobs mescola: con il default (vyukov, rigtorp) le campagne restano riproducibili."""
    binaries = {}
    for backend in backends:
        binary = Path(directory) / backend
        command = [args.mojo, 'build', '-I', str(ROOT), *DEFINES[backend]]
        print('compilazione', backend, flush=True)
        subprocess.run([*command, str(source), '-o', str(binary)], cwd=ROOT, check=True, timeout=args.timeout)
        binaries[backend] = str(binary)
    return binaries


def prepare(parser, args, max_tasks):
    """Controlla risorse e core; imposta args.cpus e args.output."""
    physical = physical_cpus()
    args.physical = physical
    if args.pinning == 'on':
        if max_tasks > len(physical):
            parser.error(f'Servono {max_tasks} core fisici distinti per il pinning; '
                         f'disponibili {len(physical)}. Ridurre la griglia o usare --pinning off')
        args.cpus = physical[:max_tasks]
    else:
        args.cpus = []
    if args.output is None:
        stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        args.output = HERE / args.benchmark / 'results' / f'{stamp}-{args.runtime}-pin{args.pinning}'
    args.output.mkdir(parents=True, exist_ok=False)


def calibrate(binary, env, timeout, targets, messages, cpus):
    """Mediana di cinque calibrazioni; T (µs) -> iterazioni; baseline senza code.

    Con il pinning attivo si calibra sul core del primo consumatore/stadio
    (cpus[1]), distinto da quello della sorgente, come nell'harness originale.
    """
    prefix = ['taskset', '-c', str(cpus[1])] if len(cpus) > 1 else []
    samples = [float(tagged(execute([*prefix, binary, 'calibrate'], env, timeout), 'CALIBRATION')[0])
               for _ in range(5)]
    ns_iteration = statistics.median(samples)
    iterations = {t: 0 if t == 0 else max(1, round(t * 1000 / ns_iteration)) for t in targets}
    compute = {t: [int(tagged(execute([*prefix, binary, 'compute', str(messages), str(iterations[t])], env, timeout),
                              'COMPUTE')[0]) / messages for _ in range(3)] for t in targets}
    return samples, ns_iteration, iterations, compute


def metadata(args, sources, extra):
    return {
        'utc': datetime.now(timezone.utc).isoformat(), 'host': platform.node(),
        'mojo': subprocess.check_output([args.mojo, '--version'], text=True).strip(),
        'cpu_model': cpu_model(), 'physical_cpus': args.physical, 'pinned_cpus': args.cpus,
        'affinity': sorted(os.sched_getaffinity(0)),
        'parameters': {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()
                       if k not in ('physical', 'cpus')},
        'sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        **extra,
    }


def sources_for(benchmark_source, runner):
    return [*sorted((ROOT / 'MoStream').glob('*.mojo')), benchmark_source, Path(runner).resolve(),
            Path(__file__).resolve()]


def run_jobs(args, binaries, env, configurations, command_for, row_for, label):
    """Esegue configurazioni x ripetizioni in ordine casuale; backend alternati a caso."""
    randomizer = random.Random(args.seed)
    jobs = [(config, repeat) for config in configurations for repeat in range(args.repeats)]
    randomizer.shuffle(jobs)
    rows = []
    with (args.output / 'raw.csv').open('w', newline='') as stream:
        writer = None
        for index, (config, repeat) in enumerate(jobs, 1):
            order = list(binaries)
            randomizer.shuffle(order)
            for backend in order:
                output = execute([binaries[backend], *command_for(config)], env, args.timeout)
                expected_queue = f'Inter-stage queue: {backend}'
                if expected_queue not in output:
                    raise RuntimeError(f'Il binario {backend} non usa la coda attesa:\n{output}')
                row = row_for(backend, config, repeat, tagged(output, 'RESULT'))
                if writer is None:
                    writer = csv.DictWriter(stream, fieldnames=list(row))
                    writer.writeheader()
                writer.writerow(row)
                stream.flush()
                rows.append(row)
                print(f'[{index}/{len(jobs)}] {backend:7s} {label(config)} rep={repeat} '
                      f'{row["throughput"]:,.0f} input/s', flush=True)
    return rows


def summarize(args, rows, keys, extra_for=None):
    """Mediane, min-max, rapporto Rigtorp/Vyukov (>1 favorisce Rigtorp) e coppie vinte."""
    summary = []
    configurations = sorted({tuple(row[k] for k in keys) for row in rows})
    for config in configurations:
        selected = [r for r in rows if tuple(r[k] for k in keys) == config]
        by = {b: {r['repeat']: r['throughput'] for r in selected if r['backend'] == b} for b in BACKENDS}
        medians = {b: statistics.median(by[b].values()) for b in BACKENDS}
        wins = sum(by['rigtorp'][rep] > by['vyukov'][rep] for rep in by['vyukov'] if rep in by['rigtorp'])
        entry = dict(zip(keys, config))
        entry.update(repeats=len(by['vyukov']),
                     vyukov_median=medians['vyukov'], rigtorp_median=medians['rigtorp'],
                     rigtorp_over_vyukov=medians['rigtorp'] / medians['vyukov'],
                     rigtorp_wins=wins,
                     vyukov_min=min(by['vyukov'].values()), vyukov_max=max(by['vyukov'].values()),
                     rigtorp_min=min(by['rigtorp'].values()), rigtorp_max=max(by['rigtorp'].values()))
        if extra_for:
            entry.update(extra_for(entry, medians))
        summary.append(entry)
    with (args.output / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    return summary


def print_summary(summary, keys):
    header = ' '.join(f'{k:>9s}' for k in keys)
    print(f'\n{header} {"Vyukov":>14s} {"Rigtorp":>14s} {"R/V":>6s} {"vinte":>6s}')
    for s in summary:
        cells = ' '.join(f'{s[k]:>9}' for k in keys)
        print(f'{cells} {s["vyukov_median"]:>14,.0f} {s["rigtorp_median"]:>14,.0f} '
              f'{s["rigtorp_over_vyukov"]:>6.3f} {s["rigtorp_wins"]:>3d}/{s["repeats"]}')


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def temporary_directory():
    return tempfile.TemporaryDirectory(prefix='mostream-synthetic-')


def parser(description):
    return argparse.ArgumentParser(description=description)
