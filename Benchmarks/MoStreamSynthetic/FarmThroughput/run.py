#!/usr/bin/env python3
"""FarmThroughput dentro MoStream: seq(Source) -> parallel(FarmWorker, N), Vyukov vs Rigtorp."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

SOURCE = Path(__file__).resolve().parent / 'farm.mojo'


def main():
    parser = common.parser(__doc__)
    parser.add_argument('--consumers', default='1,2,4,8,16')
    parser.add_argument('--work-us', default='0,1,2,5')
    common.common_arguments(parser, messages=100000, repeats=5)
    args = parser.parse_args()
    args.benchmark = 'FarmThroughput'
    common.check_common(parser, args)
    consumers = sorted(set([1] + common.numbers(args.consumers, int)))
    targets = common.numbers(args.work_us, float)
    if min(consumers) < 1 or not targets or min(targets) < 0:
        parser.error('consumers >= 1 e work-us >= 0')
    if args.capacity < max(consumers) + 1:
        parser.error('La capacità deve essere almeno N+1')
    common.prepare(parser, args, max(consumers) + 1)
    with common.temporary_directory() as directory:
        binaries = common.build(args, SOURCE, directory)
        env = common.environment(args)
        workers = int(common.tagged(common.execute([binaries['vyukov'], 'info'], env, args.timeout), 'WORKERS')[0])
        if max(consumers) + 1 > workers:
            parser.error(f'Servono N+1={max(consumers)+1} worker del runtime Mojo; disponibili {workers}')
        samples, ns_iteration, iterations, compute = common.calibrate(
            binaries["vyukov"], env, args.timeout, targets, args.messages, args.cpus)
        info = common.metadata(args, common.sources_for(SOURCE, __file__), {
            'topology': 'seq(FarmSource) -> parallel(FarmWorker, N); N+1 task, una coda 1:N',
            'timer': 'Pipeline.run() / run_cooperative(N+1), verifica esatta fuori dal timer',
            'runtime_workers': workers, 'calibrations_ns_per_iteration': samples,
            'ns_per_iteration': ns_iteration, 'iterations_by_t': {str(k): v for k, v in iterations.items()},
            'compute_samples_ns_per_input': {str(k): v for k, v in compute.items()}})
        common.write_json(args.output / 'metadata.json', info)
        # Controllo di terminazione: meno input dei consumatori, capacità minima.
        small = max(4, 1 << max(consumers).bit_length())
        for backend in common.BACKENDS:
            common.execute([binaries[backend], '3', str(max(consumers)), '0', str(small), args.pinning,
                            args.runtime], env, args.timeout)
        configurations = [(n, t) for n in consumers for t in targets]

        def command_for(config):
            n, t = config
            return [str(args.messages), str(n), str(iterations[t]), str(args.capacity), args.pinning, args.runtime]

        def row_for(backend, config, repeat, result):
            n, t = config
            elapsed, source_ns, counts = int(result[0]), int(result[1]), result[2]
            return dict(backend=backend, consumers=n, total_tasks=n + 1, target_work_us=t,
                        iterations_per_input=iterations[t],
                        calibrated_work_us=iterations[t] * ns_iteration / 1000,
                        measured_compute_ns_per_input=min(compute[t]),
                        messages=args.messages, capacity=args.capacity, runtime=args.runtime,
                        pinning=args.pinning, repeat=repeat, elapsed_ns=elapsed,
                        throughput=args.messages * 1e9 / elapsed,
                        ideal_target_throughput=n * 1e6 / t if t else '',
                        source_elapsed_ns=source_ns, worker_counts=counts)

        rows = common.run_jobs(args, binaries, env, configurations, command_for, row_for,
                               lambda c: f'N={c[0]:<3d} T={c[1]}us')

    def extra(entry, medians):
        t, n = entry['target_work_us'], entry['consumers']
        if not t:
            return dict(ideal_target_throughput='', vyukov_efficiency='', rigtorp_efficiency='')
        ideal = n * 1e6 / t
        return dict(ideal_target_throughput=ideal, vyukov_efficiency=medians['vyukov'] / ideal,
                    rigtorp_efficiency=medians['rigtorp'] / ideal)

    summary = common.summarize(args, rows, ['consumers', 'target_work_us'], extra)
    common.print_summary(summary, ['consumers', 'target_work_us'])
    print('\nRisultati in', args.output)


if __name__ == '__main__':
    main()
