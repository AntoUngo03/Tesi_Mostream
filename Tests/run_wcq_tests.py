"""Build native Mojo wCQ with the x86-64 atomic shim, then run bounded tests."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mojo', default='mojo', help='Mojo compiler executable')
    parser.add_argument('--sanitize', choices=('address',))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    compiler = shutil.which(args.mojo)
    if not compiler:
        parser.error('Mojo compiler not found')
    compiler = str(Path(compiler).resolve())
    env = os.environ.copy()
    sdk = Path(compiler).parent.parent
    if (sdk / 'lib/mojo').is_dir():
        env['MODULAR_MOJO_MAX_PACKAGE_ROOT'] = str(sdk)
        env['MODULAR_MOJO_MAX_IMPORT_PATH'] = str(sdk / 'lib/mojo')
    with tempfile.TemporaryDirectory(prefix='mostream-wcq-') as temp:
        directory = Path(temp)
        env['MODULAR_CACHE_DIR'] = str(directory / 'cache')
        obj = directory / 'wcq_atomics.o'
        instrumentation = ['-fsanitize=address', '-g'] if args.sanitize else []
        subprocess.run(['cc', '-std=c11', '-D_POSIX_C_SOURCE=200112L', '-O3',
                        '-Wall', '-Wextra', '-Werror', '-fPIC', '-mcx16', '-c',
                        str(root / 'MoStream/lib/wcq_atomics.c'), '-o', str(obj), *instrumentation],
                       check=True, timeout=60)
        module = (root / 'MoStream/MPMC_queue_wcq.mojo').read_text()
        for name in ('test_wcq', 'test_wcq_concurrent'):
            test = root / f'Tests/{name}.mojo'
            source = directory / f'{name}.mojo'
            source.write_text(module + '\n' + test.read_text())
            binary = directory / name
            command = [compiler, 'build', '-O3', str(source), '-Xlinker', str(obj), '-o', str(binary)]
            if name == 'test_wcq_concurrent':
                scheduler = directory / 'wcq_threads.o'
                subprocess.run(['cc', '-std=c11', '-O2', '-Wall', '-Wextra',
                                '-Werror', '-c', str(root / 'Tests/wcq_threads.c'),
                                '-o', str(scheduler), '-pthread', *instrumentation], check=True, timeout=60)
                command += ['-Xlinker', str(scheduler), '-Xlinker', '-lpthread']
            if args.sanitize:
                command += ['--sanitize', args.sanitize]
            subprocess.run(command, env=env, check=True, timeout=180)
            cases = [()] if name == 'test_wcq' else [('0',), ('1',), ('2',)]
            for case in cases:
                subprocess.run([str(binary), *case], env=env, check=True, timeout=90)
        # Import the module normally as well; the tests above concatenate it
        # because deterministic suspension tests inspect the internal protocol.
        example = directory / 'example_wcq'
        (directory / 'MPMC_queue_wcq.mojo').write_text(module)
        command = [compiler, 'build', '-O3', '-I', str(directory),
                   str(root / 'Tests/example_wcq.mojo'), '-Xlinker', str(obj),
                   '-o', str(example)]
        if args.sanitize:
            command += ['--sanitize', args.sanitize]
        subprocess.run(command, env=env, check=True, timeout=180)
        subprocess.run([str(example)], env=env, check=True, timeout=30)


if __name__ == '__main__':
    main()
