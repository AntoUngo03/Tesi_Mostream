"""Compila i test isolati con Mojo 1.1; --ssh esegue in una cartella remota temporanea."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

# Il processo remoto riceve soltanto i sorgenti via stdin; non modifica il repo.
REMOTE = '''import json, pathlib, subprocess, sys, tempfile
sources = json.load(sys.stdin)
with tempfile.TemporaryDirectory(prefix="mostream-tsigas-zhang-") as directory:
    for name, source in sources.items():
        path = pathlib.Path(directory) / (name + ".mojo")
        path.write_text(source)
        binary = pathlib.Path(directory) / name
        subprocess.run(["mojo", "build", "-O3", str(path), "-o", str(binary)], check=True, timeout=120)
        cases = [[str(case)] for case in range(7)] if name.endswith("_concurrent") else [[]]
        for args in cases:
            subprocess.run([str(binary), *args], check=True, timeout=30)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ssh', help='Esempio: ungolo@131.114.2.95; richiede Mojo 1.1 remoto')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    module = (root / 'MoStream/MPMC_queue_tsigas_zhang.mojo').read_text()
    names = ('test_tsigas_zhang', 'test_tsigas_zhang_concurrent')
    sources = {name: module + '\n' + (root / f'Tests/{name}.mojo').read_text() for name in names}
    if args.ssh:
        # Quoting POSIX per il solo programma remoto; i sorgenti passano da stdin.
        import shlex
        subprocess.run(['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', '-o',
                        'ConnectTimeout=15', args.ssh, 'python3 -c ' + shlex.quote(REMOTE)],
                       input=json.dumps(sources), text=True, check=True, timeout=450)
    else:
        with tempfile.TemporaryDirectory(prefix='mostream-tsigas-zhang-') as directory:
            for name, source in sources.items():
                path = Path(directory) / f'{name}.mojo'
                path.write_text(source)
                binary = Path(directory) / name
                subprocess.run(['mojo', 'build', '-O3', str(path), '-o', str(binary)], check=True, timeout=120)
                cases = [[str(case)] for case in range(7)] if name.endswith('_concurrent') else [[]]
                for case in cases:
                    subprocess.run([str(binary), *case], check=True, timeout=30)


if __name__ == '__main__':
    main()
