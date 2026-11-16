#!/usr/bin/env python3
"""PipelineGrain dentro MoStream: Source -> N stadi seq -> Sink.

Code confrontate: Vyukov e Rigtorp di default; con --backends vyukov,rigtorp,spsc
anche la SPSC FastFlow (-D MOSTREAM_SPSC=1), corretta qui perché ogni canale è 1:1.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

SOURCE = Path(__file__).resolve().parent / 'pipeline.mojo'
SUPPORTED = {1, 2, 4, 8, 12, 16, 24}


def main():
    parser = common.parser(__doc__)
    parser.add_argument('--stages', default='1,2,4,8,16')
    parser.add_argument('--total-us', default='0,1,2,5,10')
    parser.add_argument('--backends', default='vyukov,rigtorp',
                        help='code da confrontare, fra vyukov, rigtorp, spsc (vyukov e rigtorp obbligatorie)')
    parser.add_argument('--expected-ns', type=float, default=0,
                        help='ns/iterazione attesi; se > 0 la campagna non parte se la calibrazione si scosta')
    parser.add_argument('--tolerance', type=float, default=0.05)
    common.common_arguments(parser, messages=100000, repeats=5)
    args = parser.parse_args()
    args.benchmark = 'PipelineGrain'
    common.check_common(parser, args)
    stages = sorted(set([1] + common.numbers(args.stages, int)))
    targets = common.numbers(args.total_us, float)
    if not set(stages) <= SUPPORTED:
        parser.error(f'N supportati: {sorted(SUPPORTED)} (la tupla degli stadi è fissata in compilazione)')
    if not targets or min(targets) < 0:
        parser.error('total-us >= 0')
    backends = tuple(common.numbers(args.backends, str))
    if not {'vyukov', 'rigtorp'} <= set(backends) or not set(backends) <= set(common.DEFINES):
        parser.error('--backends: vyukov e rigtorp obbligatorie, ammesse vyukov, rigtorp, spsc')
    common.prepare(parser, args, max(stages) + 2)
    with common.temporary_directory() as directory:
        binaries = common.build(args, SOURCE, directory, backends)
        env = common.environment(args)
        workers = int(common.tagged(common.execute([binaries['vyukov'], 'info'], env, args.timeout), 'WORKERS')[0])
        if max(stages) + 2 > workers:
            parser.error(f'Servono N+2={max(stages)+2} worker del runtime Mojo; disponibili {workers}')
        samples, ns_iteration, iterations, compute = common.calibrate(
            binaries["vyukov"], env, args.timeout, targets, args.messages, args.cpus)
        if args.expected_ns > 0 and abs(ns_iteration / args.expected_ns - 1) > args.tolerance:
            sys.exit(f'Calibrazione anomala: {ns_iteration:.3f} ns/iterazione, attesi {args.expected_ns:.3f} '
                     f'(±{args.tolerance:.0%}). Probabile cambio di frequenza della CPU: campagna non avviata. '
                     f'Campioni: {samples}')
        info = common.metadata(args, common.sources_for(SOURCE, __file__), {
            'topology': 'seq(GrainSource) -> seq(GrainStage) x N -> seq(GrainSink); N+2 task, N+1 code 1:1',
            'timer': 'Pipeline.run() / run_cooperative(N+2), verifica esatta del digest fuori dal timer',
            'runtime_workers': workers, 'calibrations_ns_per_iteration': samples,
            'ns_per_iteration': ns_iteration, 'iterations_by_t': {str(k): v for k, v in iterations.items()},
            'compute_samples_ns_per_input': {str(k): v for k, v in compute.items()},
            'baseline_without_queues': 'compute_samples_ns_per_input: kernel intero + digest, nessuna Pipeline'})
        common.write_json(args.output / 'metadata.json', info)
        configurations = [(n, t) for n in stages for t in targets]

        def command_for(config):
            n, t = config
            return [str(n), str(args.messages), str(iterations[t]), str(args.capacity), args.pinning, args.runtime]

        def row_for(backend, config, repeat, result):
            n, t = config
            elapsed = int(result[0])
            return dict(backend=backend, stages=n, total_tasks=n + 2, queues=n + 1, total_work_us=t,
                        total_iterations=iterations[t], iterations_per_stage=iterations[t] / n,
                        calibrated_total_us=iterations[t] * ns_iteration / 1000,
                        baseline_ns_per_input=min(compute[t]),
                        messages=args.messages, capacity=args.capacity, runtime=args.runtime,
                        pinning=args.pinning, repeat=repeat, elapsed_ns=elapsed,
                        ns_per_input=elapsed / args.messages,
                        throughput=args.messages * 1e9 / elapsed)

        rows = common.run_jobs(args, binaries, env, configurations, command_for, row_for,
                               lambda c: f'N={c[0]:<3d} T={c[1]}us')

    def extra(entry, medians):
        baseline = min(compute[entry['total_work_us']])
        serial = 1e9 / baseline if baseline else ''
        return dict(serial_throughput=serial,
                    vyukov_speedup_vs_serial=medians['vyukov'] / serial if serial else '',
                    rigtorp_speedup_vs_serial=medians['rigtorp'] / serial if serial else '')

    summary = common.summarize(args, rows, ['stages', 'total_work_us'], extra)
    common.print_summary(summary, ['stages', 'total_work_us'])
    print('\nRisultati in', args.output)


if __name__ == '__main__':
    main()
