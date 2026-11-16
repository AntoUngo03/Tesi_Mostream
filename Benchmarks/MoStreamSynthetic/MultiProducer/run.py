#!/usr/bin/env python3
"""MultiProducer dentro MoStream: parallel(Source, P) -> parallel(Sink, C), Vyukov vs Rigtorp.

Scenari:
  n1  P produttori, 1 consumatore; il lavoro T lo fanno i produttori (contesa sul push)
  nm  P produttori, P consumatori; T diviso a metà fra produttore e consumatore
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

SOURCE = Path(__file__).resolve().parent / 'nm.mojo'


def main():
    parser = common.parser(__doc__)
    parser.add_argument('--scenarios', default='n1,nm')
    parser.add_argument('--producers-n1', default='1,2,4,8,16')
    parser.add_argument('--producers-nm', default='2,4,8')
    parser.add_argument('--work-us', default='0,1,2,5')
    parser.add_argument('--expected-ns', type=float, default=2.874,
                        help='ns/iterazione attesi (frequenza nominale); 0 disattiva il controllo')
    parser.add_argument('--tolerance', type=float, default=0.05)
    common.common_arguments(parser, messages=50000, repeats=50)
    args = parser.parse_args()
    args.benchmark = 'MultiProducer'
    common.check_common(parser, args)
    scenarios = [s for s in args.scenarios.split(',') if s]
    if not scenarios or any(s not in ('n1', 'nm') for s in scenarios):
        parser.error('scenari ammessi: n1, nm')
    targets = common.numbers(args.work_us, float)
    configurations = []
    if 'n1' in scenarios:
        configurations += [('n1', p, 1, t) for p in common.numbers(args.producers_n1, int) for t in targets]
    if 'nm' in scenarios:
        configurations += [('nm', p, p, t) for p in common.numbers(args.producers_nm, int) for t in targets]
    max_tasks = max(p + c for _, p, c, _ in configurations)
    if args.capacity < max(c for _, _, c, _ in configurations):
        parser.error('La capacità deve essere almeno pari al numero di consumatori')
    common.prepare(parser, args, max_tasks)
    with common.temporary_directory() as directory:
        binaries = common.build(args, SOURCE, directory)
        env = common.environment(args)
        workers = int(common.tagged(common.execute([binaries['vyukov'], 'info'], env, args.timeout), 'WORKERS')[0])
        if max_tasks > workers:
            parser.error(f'Servono {max_tasks} worker del runtime Mojo; disponibili {workers}')
        samples, ns_iteration, iterations, compute = common.calibrate(
            binaries['vyukov'], env, args.timeout, targets, args.messages, args.cpus)
        # Controllo della frequenza: una calibrazione fatta in turbo assegnerebbe
        # a ogni input più lavoro del previsto (vedi campagna del 6 novembre).
        if args.expected_ns > 0 and abs(ns_iteration / args.expected_ns - 1) > args.tolerance:
            sys.exit(f'Calibrazione anomala: {ns_iteration:.3f} ns/iterazione, attesi {args.expected_ns:.3f} '
                     f'(±{args.tolerance:.0%}). Probabile cambio di frequenza della CPU: campagna non avviata. '
                     f'Campioni: {samples}')
        info = common.metadata(args, common.sources_for(SOURCE, __file__), {
            'topology': 'parallel(NMSource, P) -> parallel(NMSink, C); P+C task, una coda P:C',
            'timer': 'Pipeline.run() / run_cooperative(P+C), verifica esatta fuori dal timer',
            'work_split': 'n1: Ip = I, Ic = 0; nm: Ip = I//2, Ic = I - I//2',
            'runtime_workers': workers, 'calibrations_ns_per_iteration': samples,
            'ns_per_iteration': ns_iteration, 'iterations_by_t': {str(k): v for k, v in iterations.items()},
            'compute_samples_ns_per_input': {str(k): v for k, v in compute.items()}})
        common.write_json(args.output / 'metadata.json', info)

        def split(scenario, t):
            total = iterations[t]
            return (total, 0) if scenario == 'n1' else (total // 2, total - total // 2)

        # Controllo di terminazione con pochissimi messaggi e capacità minima.
        for backend in common.BACKENDS:
            for scenario, p, c, _ in [cfg for cfg in configurations if cfg[3] == targets[0]]:
                common.execute([binaries[backend], '3', str(p), str(c), '0', '0',
                                str(max(2, 1 << max(c - 1, 1).bit_length())), args.pinning, args.runtime],
                               env, args.timeout)

        def command_for(config):
            scenario, p, c, t = config
            ip, ic = split(scenario, t)
            return [str(args.messages), str(p), str(c), str(ip), str(ic), str(args.capacity),
                    args.pinning, args.runtime]

        def row_for(backend, config, repeat, result):
            scenario, p, c, t = config
            ip, ic = split(scenario, t)
            elapsed = int(result[0])
            return dict(backend=backend, scenario=scenario, producers=p, consumers=c, total_tasks=p + c,
                        T_us=t, iter_producer=ip, iter_consumer=ic,
                        calibrated_work_us=iterations[t] * ns_iteration / 1000,
                        messages=args.messages, capacity=args.capacity, runtime=args.runtime,
                        pinning=args.pinning, repeat=repeat, elapsed_ns=elapsed,
                        throughput=args.messages * 1e9 / elapsed, worker_counts=result[1])

        rows = common.run_jobs(args, binaries, env, configurations, command_for, row_for,
                               lambda c: f'{c[0]} P={c[1]:<2d} C={c[2]:<2d} T={c[3]}us')

    summary = common.summarize(args, rows, ['scenario', 'producers', 'consumers', 'T_us'])
    common.print_summary(summary, ['scenario', 'producers', 'consumers', 'T_us'])
    print('\nRisultati in', args.output)


if __name__ == '__main__':
    main()
