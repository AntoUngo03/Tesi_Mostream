#!/usr/bin/env python3
"""Build with warnings as errors and run MoStream's tests on Mojo 1.0."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmarks", action="store_true",
                        help="also compile every benchmark entry point")
    parser.add_argument("--timeout", type=int, default=120,
                        help="timeout in seconds for each test")
    args = parser.parse_args()
    subprocess.run(["mojo", "--version"], check=True)
    subprocess.run(["make", "-C", "MoStream/lib"], cwd=ROOT, check=True)
    sources = sorted((ROOT / "Tests").rglob("*.mojo"))
    if args.benchmarks:
        sources += sorted((ROOT / "Benchmarks").rglob("*.mojo"))

    with tempfile.TemporaryDirectory(prefix="mostream-tests-") as tmp:
        env = dict(os.environ, MOSTREAM_HOME=str(ROOT))
        env.setdefault("MODULAR_CACHE_DIR", str(Path(tmp) / "cache"))
        for source in sources:
            if "def main(" not in source.read_text():
                continue
            binary = Path(tmp) / source.stem
            command = ["mojo", "build", "--Werror", "-I", str(ROOT),
                       "-I", str(ROOT / "Benchmarks/ImagePipeline"),
                       str(source), "-o", str(binary)]
            if "wcq" in source.stem or source.stem == "queue_benchmark":
                command += ["-Xlinker", str(ROOT / "MoStream/lib/wcq_native.o")]
            subprocess.run(command, cwd=ROOT, env=env, check=True)
            if source.is_relative_to(ROOT / "Tests"):
                command = [str(binary)]
                if source.stem == "test_pipe_3_coop":
                    command += ["4"]
                result = subprocess.run(command, cwd=ROOT, env=env, check=True,
                                        capture_output=True, text=True,
                                        timeout=args.timeout)
                # The original pipeline examples catch errors and exit with 0.
                marker = ("terminated successfully!" if source.stem.startswith("test_pipe_")
                          else "PASS:")
                if marker not in result.stdout or "FAIL" in result.stdout:
                    raise RuntimeError(result.stdout + result.stderr)
                print(f"PASS {source.relative_to(ROOT)}", flush=True)
            else:
                print(f"BUILD {source.relative_to(ROOT)}", flush=True)


if __name__ == "__main__":
    main()
