#!/usr/bin/env python3
"""Compare four data queues in the same cooperative pipeline, with validated results."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import statistics
import subprocess
import tempfile

from run_benchmarks import t_critical_95

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = HERE / "pipe_mpmc_cooperative_benchmark.mojo"
BACKENDS = {
    "MPMC": [],
    "PADDEDFAA": ["-DMOSTREAM_PADDED_FAA=1"],
    "SCQ": ["-DMOSTREAM_SCQ=1"],
    "MICHAELSCOTT": ["-DMOSTREAM_MICHAEL_SCOTT=1"],
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


@lru_cache(None)
def expected_checksum(elements, degree, work):
    mask = (1 << 64) - 1
    result = 0
    for value in range(1, elements + 1):
        for _ in range(work):
            value ^= (value << 13) & mask
            value ^= value >> 7
            value ^= (value << 17) & mask
        result = (result + value) & mask
    return (result * degree) & mask


def parse(stdout, backend, args, workers, work):
    lines = [line for line in stdout.splitlines() if line.startswith("PIPE_COOP_MPMC_RESULT ")]
    if len(lines) != 1:
        raise RuntimeError("Missing/duplicate result line:\n" + stdout)
    fields = dict(re.findall(r"(\w+)=\s*(\S+)", lines[0]))
    expected = dict(backend=backend, producers=str(args.degree), consumers=str(args.degree),
                    workers=str(workers), capacity=str(args.capacity), work_iterations=str(work),
                    messages=str(args.elements * args.degree), count=str(args.elements * args.degree),
                    checksum=str(expected_checksum(args.elements, args.degree, work)), valid="True")
    if any(fields.get(key) != value for key, value in expected.items()):
        raise RuntimeError(f"Invalid result: {fields}; expected {expected}")
    elapsed = float(fields["time_ms"])
    throughput = float(fields["throughput_Mtransfer_s"])
    if not math.isfinite(elapsed) or elapsed <= 0 or not math.isclose(
        throughput, args.elements * args.degree / elapsed / 1000, rel_tol=1e-8
    ):
        raise RuntimeError(f"Invalid timing: {fields}")
    return dict(time_ms=elapsed, throughput_Mtransfer_s=throughput,
                count=int(fields["count"]), checksum=int(fields["checksum"]))


def summarize(rows, cases, backends=BACKENDS):
    result = []
    for workers, work in cases:
        selected = [r for r in rows if r["workers"] == workers and r["work_iterations"] == work]
        baseline = {r["block"]: r["time_ms"] for r in selected if r["backend"] == "MPMC"}
        for backend in backends:
            samples = [r for r in selected if r["backend"] == backend]
            times = [r["time_ms"] for r in samples]
            logs = [math.log(baseline[r["block"]] / r["time_ms"]) for r in samples]
            center = statistics.mean(logs)
            half = t_critical_95(len(logs) - 1) * statistics.stdev(logs) / math.sqrt(len(logs))
            result.append(dict(workers=workers, work_iterations=work, backend=backend,
                               samples=len(times), mean_ms=statistics.mean(times),
                               median_ms=statistics.median(times),
                               cov=statistics.stdev(times) / statistics.mean(times),
                               speedup_vs_mpmc=math.exp(center),
                               speedup_ci95_low=math.exp(center - half),
                               speedup_ci95_high=math.exp(center + half)))
    return result


def main(*, backends=None, repetitions=12, output=None, target="MICHAELSCOTT", extra_sources=()):
    # Other comparisons reuse the validation and balanced-block protocol.
    backends = BACKENDS if backends is None else backends
    output = HERE / "michael_scott_results" if output is None else output
    parser = argparse.ArgumentParser(
        description="Compare " + ", ".join(backends) + " in the same cooperative pipeline."
    )
    parser.add_argument("--elements", type=int, default=50000)
    parser.add_argument("--degree", type=int, default=4)
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--work", default="0,160")
    parser.add_argument("--capacity", type=int, default=1024)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repetitions", type=int, default=repetitions)
    parser.add_argument("--seed", type=int, default=20260921)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--output", type=Path, default=output)
    args = parser.parse_args()
    workers = [int(x) for x in args.workers.split(",")]
    work_values = [int(x) for x in args.work.split(",")]
    if (min(args.elements, args.degree, args.timeout, *workers) < 1 or min(work_values) < 0
            or args.capacity < 2 or args.capacity & (args.capacity - 1)
            or args.warmups < 0 or args.repetitions < len(backends) or args.repetitions % len(backends)):
        parser.error(f"Positive dimensions, power-of-two capacity and repetitions divisible by {len(backends)} required")
    if len(set(workers)) != len(workers) or len(set(work_values)) != len(work_values):
        parser.error("Duplicate cases are not allowed")
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error("Output directory must be empty; choose a new --output to preserve earlier runs")
    cases = [(w, work) for w in workers for work in work_values]
    rng = random.Random(args.seed)
    orders = {}
    for case in cases:
        order = list(backends)
        rng.shuffle(order)
        orders[case] = order
    sources = sorted((ROOT / "MoStream").rglob("*.mojo")) + [SOURCE, Path(__file__).resolve(), HERE / "run_benchmarks.py", ROOT / "MoStream/lib/libpinning.c"]
    sources += list(extra_sources)
    source_hashes = {str(p.relative_to(ROOT)): sha(p) for p in sources}
    manifest = dict(started_utc=datetime.now(timezone.utc).isoformat(),
                    args={**vars(args), "output": str(args.output)}, backends=backends,
                    compiler=capture(["mojo", "--version"]), system=platform.platform(),
                    cpu=capture(["lscpu"]), affinity=sorted(os.sched_getaffinity(0)),
                    git_head=capture(["git", "rev-parse", "HEAD"]),
                    git_status=capture(["git", "status", "--short"]),
                    source_sha256=source_hashes, commands={}, binary_sha256={},
                    policy="Randomized case blocks; fixed random backend order rotated each block; equal order positions; no outlier exclusion",
                    timer="run_cooperative including scheduler setup; pinning disabled; scheduler queues always MPMC",
                    status="running")
    manifest_path = args.output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    rows = []
    try:
        subprocess.run(["make", "-C", "MoStream/lib", "libpinning.so"], cwd=ROOT, check=True)
        manifest["pinning_library_sha256"] = sha(ROOT / "MoStream/lib/libpinning.so")
        with tempfile.TemporaryDirectory(prefix="mostream-ms-bench-") as temp:
            env = dict(os.environ, MOSTREAM_HOME=str(ROOT), MODULAR_CACHE_DIR=str(Path(temp) / "cache"))
            binaries = {}
            for backend, flags in backends.items():
                binary = Path(temp) / backend
                command = ["mojo", "build", "--Werror", "-O3", "-I", str(ROOT), *flags, str(SOURCE), "-o", str(binary)]
                subprocess.run(command, cwd=ROOT, env=env, check=True)
                binaries[backend] = binary
                manifest["commands"][backend] = command
                manifest["binary_sha256"][backend] = sha(binary)
                print("Built", backend, flush=True)
            # Compute independent checksums before timed runs.
            for work in work_values:
                expected_checksum(args.elements, args.degree, work)
            columns = ["block", "position", "workers", "work_iterations", "backend", "time_ms", "throughput_Mtransfer_s", "count", "checksum"]
            with (args.output / "raw.csv").open("w") as raw, (args.output / "runs.log").open("w") as log:
                writer = csv.DictWriter(raw, fieldnames=columns)
                writer.writeheader()
                for block in range(-args.warmups, args.repetitions):
                    shuffled = list(cases)
                    rng.shuffle(shuffled)
                    for workers_count, work in shuffled:
                        base = orders[(workers_count, work)]
                        rotation = block % len(base)
                        order = base[rotation:] + base[:rotation]
                        for position, backend in enumerate(order):
                            command = [str(binaries[backend]), str(args.elements), str(args.degree), str(workers_count), str(args.capacity), str(work)]
                            proc = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=args.timeout)
                            log.write(f"block={block} position={position} command={command!r}\n{proc.stdout}{proc.stderr}\n")
                            log.flush()
                            proc.check_returncode()
                            sample = parse(proc.stdout, backend, args, workers_count, work)
                            if block >= 0:
                                row = dict(block=block, position=position, workers=workers_count, work_iterations=work, backend=backend, **sample)
                                writer.writerow(row)
                                raw.flush()
                                rows.append(row)
                    print(f"Completed {'warmup' if block < 0 else 'measured'} block {block}; {len(rows)} samples", flush=True)
        if source_hashes != {str(p.relative_to(ROOT)): sha(p) for p in sources}:
            raise RuntimeError("Sources changed during benchmark")
        summary = summarize(rows, cases, backends)
        with (args.output / "summary.csv").open("w") as file:
            writer = csv.DictWriter(file, fieldnames=list(summary[0]))
            writer.writeheader()
            writer.writerows(summary)
        manifest["status"] = "complete"
        manifest["measured_samples"] = len(rows)
        manifest["result_sha256"] = {p.name: sha(p) for p in args.output.iterdir() if p.name != "manifest.json"}
        for row in summary:
            if row["backend"] == target:
                print(f"{target} workers={row['workers']} work={row['work_iterations']}: {row['median_ms']:.2f} ms; speedup={row['speedup_vs_mpmc']:.3f} [{row['speedup_ci95_low']:.3f}, {row['speedup_ci95_high']:.3f}]")
    except BaseException as error:
        manifest["status"] = "failed"
        manifest["error"] = repr(error)
        raise
    finally:
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
