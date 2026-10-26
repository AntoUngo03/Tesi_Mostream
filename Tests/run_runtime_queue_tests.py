"""Build and test both inter-stage backends through the real MoStream runtimes."""
import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mojo', default='mojo', help='Mojo 1.1 compiler')
    parser.add_argument('--timeout', type=float, default=60)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix='mostream-runtime-') as directory:
        for backend in ('vyukov', 'rigtorp'):
            binary = Path(directory) / backend
            command = [args.mojo, 'build', '-I', str(root)]
            if backend == 'rigtorp':
                command += ['-D', 'MOSTREAM_RIGTORP=1']
            command += [str(root / 'Tests/test_runtime_queue.mojo'), '-o', str(binary)]
            subprocess.run(command, check=True, timeout=180)
            for topology in ('farm', 'mpmc'):
                for runtime in ('standard', 'cooperative'):
                    for workers in (1, 2, 4):
                        for messages in (0, 1, 2003):
                            for capacity in (2, 16):
                                print(backend, topology, runtime, workers, messages, capacity, flush=True)
                                subprocess.run([str(binary), str(messages), str(workers),
                                                str(capacity), runtime, topology], cwd=root,
                                               check=True, timeout=args.timeout)
    print('PASS: 144 integration configurations across both backends')


if __name__ == '__main__':
    main()
