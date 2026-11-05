"""Verifica Rigtorp isolatamente da MoStream; richiede la toolchain Mojo 1.1."""
import argparse
import json
import shlex
import sys
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ssh', help='Host SSH con Mojo 1.1; usa una directory temporanea remota')
    parser.add_argument('--mojo', default='mojo', help='Compilatore Mojo del progetto')
    parser.add_argument('--min-n', type=int, default=1)
    parser.add_argument('--allow-oversubscription', action='store_true', help='Prova anche con più task dei worker, mantenendo il timeout')
    parser.add_argument('--max-n', type=int, default=50)
    parser.add_argument('--messages', type=int, default=10000)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--capacities', type=int, nargs='+', default=[1, 3, 8, 64])
    parser.add_argument('--timeout', type=float, default=60, help='Secondi per configurazione')
    args = parser.parse_args()
    if min(args.min_n, args.max_n, args.messages, args.repeats, *args.capacities) < 1 or args.timeout <= 0:
        parser.error('Tutti i parametri numerici devono essere positivi')
    if args.min_n > args.max_n:
        parser.error('--min-n deve essere <= --max-n')
    if args.ssh:
        root = Path(__file__).resolve().parents[1]
        files = ['Tests/run_rigtorp_tests.py', 'Tests/test_rigtorp.mojo',
                 'Tests/test_rigtorp_concurrent.mojo', 'MoStream/MPMC_queue_rigtorp.mojo']
        payload = {name: (root / name).read_text() for name in files}
        forwarded = ['--mojo', args.mojo, '--max-n', str(args.max_n),
                     '--messages', str(args.messages), '--repeats', str(args.repeats),
                     '--timeout', str(args.timeout), '--capacities', *map(str, args.capacities)]
        forwarded += ['--min-n', str(args.min_n)]
        if args.allow_oversubscription:
            forwarded.append('--allow-oversubscription')
        remote = """import json, pathlib, subprocess, sys, tempfile
files, arguments = json.load(sys.stdin)
with tempfile.TemporaryDirectory(prefix='mostream-rigtorp-remote-') as directory:
    root = pathlib.Path(directory)
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    result = subprocess.run([sys.executable, '-u', str(root / 'Tests/run_rigtorp_tests.py'), *arguments])
    sys.exit(result.returncode)
"""
        subprocess.run(['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes',
                        '-o', 'ConnectTimeout=15', args.ssh,
                        'python3 -u -c ' + shlex.quote(remote)],
                       input=json.dumps([payload, forwarded]), text=True, check=True)
        return
    compiler = shutil.which(args.mojo)
    if compiler is None:
        parser.error(f'Compilatore non trovato: {args.mojo}')
    root = Path(__file__).resolve().parents[1]
    module = (root / 'MoStream/MPMC_queue_rigtorp.mojo').read_text()
    with tempfile.TemporaryDirectory(prefix='mostream-rigtorp-') as directory:
        binaries = {}
        for name in ('test_rigtorp', 'test_rigtorp_concurrent'):
            source = Path(directory) / f'{name}.mojo'
            binary = Path(directory) / name
            source.write_text(module + '\n' + (root / f'Tests/{name}.mojo').read_text())
            subprocess.run([compiler, 'build', str(source), '-o', str(binary)],
                           check=True, timeout=120)
            binaries[name] = str(binary)
        subprocess.run([binaries['test_rigtorp']], check=True, timeout=args.timeout)
        concurrent = binaries['test_rigtorp_concurrent']
        workers = int(subprocess.check_output([concurrent, 'info'], text=True, timeout=args.timeout).strip())
        print(f"Runtime workers: {workers}; task per prova: 2*N", flush=True)
        if 2 * args.max_n > workers and not args.allow_oversubscription:
            parser.error(f'N={args.max_n} richiede {2 * args.max_n} worker Mojo; '
                         f'disponibili {workers}. Impostare --max-n <= {workers // 2} '
                         'oppure configurare il runtime con più worker. Nessuna configurazione omessa silenziosamente.')
        passed = 0
        for repeat in range(args.repeats):
            for n in range(args.min_n, args.max_n + 1):
                for capacity in args.capacities:
                    for mode in ('blocking', 'try'):
                        label = f'repeat={repeat + 1} N={n} capacity={capacity} mode={mode}'
                        print(label, flush=True)
                        try:
                            subprocess.run([concurrent, str(n), str(args.messages), str(capacity), mode,
                                            *(["allow-oversubscription"] if args.allow_oversubscription else [])],
                                           check=True, timeout=args.timeout)
                        except subprocess.TimeoutExpired as error:
                            raise RuntimeError(f'Test bloccato o troppo lento: {label}') from error
                        passed += 1
        print(f'PASS: {passed} prove concorrenti, oltre alla suite sequenziale.')


if __name__ == '__main__':
    main()
