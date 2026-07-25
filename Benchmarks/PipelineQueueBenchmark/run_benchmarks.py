#!/usr/bin/env python3
"""Compare MPMC-CAS and Padded-FAA in MoStream pipeline workloads."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import statistics
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "Benchmarks" / "PipelineQueueBenchmark"
BINARY_DIR = Path("/tmp/mostream-pipeline-queue-benchmark")
CACHE_DIR = Path("/tmp/mostream-pipeline-queue-cache")
RESULT_RE = re.compile(
    r"^PIPE_RESULT\s+test=(?P<test>\S+)\s+"
    r"backend=\s*(?P<backend>\S+)\s+"
    r"(?:workers=\s*(?P<workers>\d+)\s+)?"
    r"time_ms=\s*(?P<time>[0-9.eE+-]+)\s+"
    r"throughput_Mtransfer_s=\s*(?P<throughput>[0-9.eE+-]+)\s+"
    r"count=\s*(?P<count>\d+)\s+"
    r"checksum=\s*(?P<checksum>\d+)\s+"
    r"valid=\s*(?P<valid>True|False)\s*$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class Case:
    test: str
    workers: int


@dataclass(frozen=True)
class Sample:
    test: str
    backend: str
    workers: int
    time_ms: float
    throughput: float
    count: int
    checksum: int


def t_critical_95(df: int) -> float:
    table = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
        11: 2.201,
        12: 2.179,
        13: 2.160,
        14: 2.145,
        15: 2.131,
        16: 2.120,
        17: 2.110,
        18: 2.101,
        19: 2.093,
        20: 2.086,
        21: 2.080,
        22: 2.074,
        23: 2.069,
        24: 2.064,
        25: 2.060,
        26: 2.056,
        27: 2.052,
        28: 2.048,
        29: 2.045,
        30: 2.042,
    }
    return table.get(df, 1.96)


def stats(values: list[float]) -> dict[str, float]:
    mean = statistics.fmean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    ci95 = t_critical_95(len(values) - 1) * stddev / math.sqrt(len(values))
    return {
        "mean": mean,
        "median": statistics.median(values),
        "stddev": stddev,
        "ci95": ci95,
        "cov_percent": 100.0 * stddev / mean if mean else 0.0,
    }


def geometric_speedup_stats(ratios: list[float]) -> dict[str, float]:
    log_summary = stats([math.log(value) for value in ratios])
    lower = math.exp(log_summary["mean"] - log_summary["ci95"])
    upper = math.exp(log_summary["mean"] + log_summary["ci95"])
    return {
        "geomean": math.exp(log_summary["mean"]),
        "ci95_lower": lower,
        "ci95_upper": upper,
        "median": statistics.median(ratios),
    }


@lru_cache(maxsize=None)
def expected_metrics(test: str, elements: int) -> tuple[int, int, int]:
    """Return expected output count, checksum, and edge transfers."""
    if test == "pipe_1":
        checksum = sum(7 + len(str(value + 1)) for value in range(1, elements + 1))
        return elements, checksum, 2 * elements
    if test == "pipe_2":
        checksum = sum(
            12 + len(str(value + 1)) + len(str((value + 1) * 2))
            for value in range(1, elements + 1)
        )
        return 2 * elements, checksum, 3 * elements
    if test == "pipe_3_coop":
        return 2 * elements, elements * (elements + 1), 6 * elements
    raise ValueError(f"Unknown test: {test}")


def command_text(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return (completed.stdout or completed.stderr).strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def machine_manifest(
    args: argparse.Namespace,
    binaries: dict[tuple[str, str], Path],
    cases: list[Case],
) -> dict[str, object]:
    cpu_model = "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(errors="replace").splitlines():
            if line.lower().startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    git_status = command_text(["git", "status", "--porcelain"])
    source_paths = [
        ROOT / "MoStream" / "communicator.mojo",
        ROOT / "MoStream" / "pipeline_queue.mojo",
        ROOT / "MoStream" / "MPMC_queue.mojo",
        ROOT / "MoStream" / "Padded_FAA_queue.mojo",
        SOURCE_DIR / "pipe_1_benchmark.mojo",
        SOURCE_DIR / "pipe_2_benchmark.mojo",
        SOURCE_DIR / "pipe_3_coop_benchmark.mojo",
        SOURCE_DIR / "run_benchmarks.py",
    ]
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "command_parameters": {
            "elements_per_source": args.elements,
            "repetitions": args.repetitions,
            "warmups": args.warmups,
            "workers": args.workers,
            "queue_capacity": 1024,
            "timeout_seconds": args.timeout,
            "seed": args.seed,
            "pinning": False,
            "optimization": "O3",
        },
        "cases": [
            {"test": case.test, "workers": case.workers} for case in cases
        ],
        "scope": {
            "selected_queues": "inter-stage Communicator data queues",
            "cooperative_scheduler_queues": "MPMC-CAS in both builds",
            "timed_region": "pipeline.run()/pipeline.run_cooperative()",
        },
        "toolchain": {
            "mojo": command_text(["mojo", "--version"]),
            "python": platform.python_version(),
        },
        "host": {
            "platform": platform.platform(),
            "cpu_model": cpu_model,
            "logical_cpus": os.cpu_count(),
            "affinity": sorted(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else None,
        },
        "git": {
            "commit": command_text(["git", "rev-parse", "HEAD"]),
            "dirty": bool(git_status),
            "status_porcelain": git_status.splitlines(),
        },
        "binaries": {
            f"{test}:{backend}": {
                "path": str(path),
                "sha256": sha256(path),
            }
            for (test, backend), path in sorted(binaries.items())
        },
        "relevant_sources": {
            str(path.relative_to(ROOT)): sha256(path) for path in source_paths
        },
    }


def build() -> dict[tuple[str, str], Path]:
    BINARY_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "MODULAR_CACHE_DIR": str(CACHE_DIR)}
    subprocess.run(
        ["make", "-C", "MoStream/lib", "libpinning.so"],
        cwd=ROOT,
        env=env,
        check=True,
    )
    binaries: dict[tuple[str, str], Path] = {}
    sources = {
        "pipe_1": SOURCE_DIR / "pipe_1_benchmark.mojo",
        "pipe_2": SOURCE_DIR / "pipe_2_benchmark.mojo",
        "pipe_3_coop": SOURCE_DIR / "pipe_3_coop_benchmark.mojo",
    }
    for test, source in sources.items():
        for backend in ("MPMC", "PADDEDFAA"):
            output = BINARY_DIR / f"{test}_{backend.lower()}"
            command = [
                "mojo",
                "build",
                "-O3",
                "-I",
                ".",
            ]
            if backend == "PADDEDFAA":
                command.append("-DMOSTREAM_PADDED_FAA=1")
            command.extend([str(source), "-o", str(output)])
            print(f"Building {test} [{backend}] ...", flush=True)
            subprocess.run(command, cwd=ROOT, env=env, check=True)
            binaries[(test, backend)] = output
    return binaries


def existing_binaries() -> dict[tuple[str, str], Path]:
    binaries = {}
    for test in ("pipe_1", "pipe_2", "pipe_3_coop"):
        for backend in ("MPMC", "PADDEDFAA"):
            path = BINARY_DIR / f"{test}_{backend.lower()}"
            if not path.is_file():
                raise FileNotFoundError(f"Missing benchmark binary: {path}")
            binaries[(test, backend)] = path
    return binaries


def run_one(
    binary: Path,
    case: Case,
    expected_backend: str,
    elements: int,
    timeout_seconds: int,
) -> Sample:
    command = [str(binary), str(elements)]
    if case.test == "pipe_3_coop":
        command.append(str(case.workers))
    env = {**os.environ, "MOSTREAM_HOME": str(ROOT)}
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=False,
    )
    matches = list(RESULT_RE.finditer(completed.stdout))
    if (
        completed.returncode != 0
        or len(matches) != 1
        or matches[0].group("valid") != "True"
    ):
        raise RuntimeError(
            f"Benchmark failed: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    match = matches[0]
    workers_text = match.group("workers")
    sample = Sample(
        test=match.group("test"),
        backend=match.group("backend"),
        workers=int(workers_text) if workers_text else 0,
        time_ms=float(match.group("time")),
        throughput=float(match.group("throughput")),
        count=int(match.group("count")),
        checksum=int(match.group("checksum")),
    )
    expected_count, expected_checksum, expected_transfers = expected_metrics(
        case.test, elements
    )
    expected_throughput = expected_transfers / sample.time_ms / 1000.0
    if (
        sample.test != case.test
        or sample.backend != expected_backend
        or sample.workers != case.workers
        or sample.count != expected_count
        or sample.checksum != expected_checksum
        or not math.isclose(
            sample.throughput,
            expected_throughput,
            rel_tol=1e-10,
            abs_tol=1e-12,
        )
    ):
        raise RuntimeError(
            f"Unexpected benchmark result from {' '.join(command)}: {sample}"
        )
    return sample


def write_raw(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def parse_workers(value: str) -> list[int]:
    workers = [int(part) for part in value.split(",") if part]
    if (
        not workers
        or any(item < 1 for item in workers)
        or len(set(workers)) != len(workers)
    ):
        raise argparse.ArgumentTypeError(
            "workers must be distinct positive integers"
        )
    return workers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--elements", type=int, default=250_000)
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--workers", type=parse_workers, default=[1, 2, 4, 8])
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--seed", type=int, default=20260722)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=SOURCE_DIR)
    args = parser.parse_args()
    if (
        args.elements < 1
        or args.repetitions < 2
        or args.repetitions % 2
        or args.warmups < 0
        or args.timeout < 1
    ):
        parser.error(
            "elements>0, an even repetitions>=2, warmups>=0, and timeout>0 "
            "are required"
        )
    if os.cpu_count() is not None and max(args.workers) > os.cpu_count():
        parser.error("a worker count exceeds the available logical CPUs")

    binaries = existing_binaries() if args.skip_build else build()
    cases = [Case("pipe_1", 0), Case("pipe_2", 0)] + [
        Case("pipe_3_coop", workers) for workers in args.workers
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output_dir / "run_manifest.json"
    manifest = machine_manifest(args, binaries, cases)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    raw_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    rng = random.Random(args.seed)
    start_with_padded = {case: bool(rng.getrandbits(1)) for case in cases}

    print(
        f"Warm-up: {args.warmups} interleaved blocks (seed={args.seed}) ...",
        flush=True,
    )
    for warmup in range(args.warmups):
        case_order = list(cases)
        rng.shuffle(case_order)
        for case in case_order:
            padded_first = start_with_padded[case] ^ bool(warmup % 2)
            order = (
                ("PADDEDFAA", "MPMC")
                if padded_first
                else ("MPMC", "PADDEDFAA")
            )
            for backend in order:
                run_one(
                    binaries[(case.test, backend)],
                    case,
                    backend,
                    args.elements,
                    args.timeout,
                )

    samples: dict[Case, dict[str, list[Sample]]] = {
        case: {"MPMC": [], "PADDEDFAA": []} for case in cases
    }
    global_execution_index = 0
    print(
        f"Measured run: {args.repetitions} paired interleaved blocks ...",
        flush=True,
    )
    for repetition in range(args.repetitions):
        case_order = list(cases)
        rng.shuffle(case_order)
        print(f"  block {repetition + 1}/{args.repetitions}", flush=True)
        for case_order_index, case in enumerate(case_order, start=1):
            padded_first = start_with_padded[case] ^ bool(repetition % 2)
            order = (
                ("PADDEDFAA", "MPMC")
                if padded_first
                else ("MPMC", "PADDEDFAA")
            )
            for order_index, backend in enumerate(order, start=1):
                global_execution_index += 1
                sample = run_one(
                    binaries[(case.test, backend)],
                    case,
                    backend,
                    args.elements,
                    args.timeout,
                )
                samples[case][backend].append(sample)
                raw_rows.append(
                    {
                        "global_execution_index": global_execution_index,
                        "test": sample.test,
                        "workers": sample.workers,
                        "elements_per_source": args.elements,
                        "repetition": repetition + 1,
                        "case_order_in_block": case_order_index,
                        "execution_order_in_pair": order_index,
                        "backend": sample.backend,
                        "time_ms": sample.time_ms,
                        "throughput_Mtransfer_s": sample.throughput,
                        "count": sample.count,
                        "checksum": sample.checksum,
                        "valid": True,
                    }
                )

    for case in cases:
        label = case.test if not case.workers else f"{case.test}/{case.workers}w"
        case_samples = samples[case]
        cas_times = [sample.time_ms for sample in case_samples["MPMC"]]
        padded_times = [sample.time_ms for sample in case_samples["PADDEDFAA"]]
        cas_throughput = [sample.throughput for sample in case_samples["MPMC"]]
        padded_throughput = [
            sample.throughput for sample in case_samples["PADDEDFAA"]
        ]
        speedups = [
            cas / padded for cas, padded in zip(cas_times, padded_times)
        ]
        cas_time_stats = stats(cas_times)
        padded_time_stats = stats(padded_times)
        cas_tp_stats = stats(cas_throughput)
        padded_tp_stats = stats(padded_throughput)
        speedup_stats = stats(speedups)
        speedup_geo = geometric_speedup_stats(speedups)
        unstable = (
            cas_time_stats["cov_percent"] > 10.0
            or padded_time_stats["cov_percent"] > 10.0
        )
        summary_rows.append(
            {
                "test": case.test,
                "workers": case.workers,
                "elements_per_source": args.elements,
                "repetitions": args.repetitions,
                "queue_capacity": 1024,
                "mpmc_mean_ms": cas_time_stats["mean"],
                "mpmc_median_ms": cas_time_stats["median"],
                "mpmc_stddev_ms": cas_time_stats["stddev"],
                "mpmc_ci95_ms": cas_time_stats["ci95"],
                "mpmc_cov_percent": cas_time_stats["cov_percent"],
                "padded_faa_mean_ms": padded_time_stats["mean"],
                "padded_faa_median_ms": padded_time_stats["median"],
                "padded_faa_stddev_ms": padded_time_stats["stddev"],
                "padded_faa_ci95_ms": padded_time_stats["ci95"],
                "padded_faa_cov_percent": padded_time_stats["cov_percent"],
                "mpmc_mean_Mtransfer_s": cas_tp_stats["mean"],
                "padded_faa_mean_Mtransfer_s": padded_tp_stats["mean"],
                "padded_faa_speedup_mean": speedup_stats["mean"],
                "padded_faa_speedup_ci95": speedup_stats["ci95"],
                "padded_faa_speedup_geomean": speedup_geo["geomean"],
                "padded_faa_speedup_ci95_lower": speedup_geo["ci95_lower"],
                "padded_faa_speedup_ci95_upper": speedup_geo["ci95_upper"],
                "padded_faa_speedup_median": speedup_geo["median"],
                "padded_faa_delta_percent": 100.0
                * (speedup_geo["geomean"] - 1.0),
                "padded_faa_wins": sum(value > 1.0 for value in speedups),
                "unstable_cov_over_10_percent": unstable,
            }
        )
        print(
            f"{label}: MPMC {cas_time_stats['mean']:.3f} ± "
            f"{cas_time_stats['ci95']:.3f} ms; "
            f"Padded-FAA {padded_time_stats['mean']:.3f} ± "
            f"{padded_time_stats['ci95']:.3f} ms; "
            f"geometric speedup {speedup_geo['geomean']:.3f}x "
            f"[{speedup_geo['ci95_lower']:.3f}, "
            f"{speedup_geo['ci95_upper']:.3f}]",
            flush=True,
        )

    raw_path = args.output_dir / "results_raw.csv"
    summary_path = args.output_dir / "results_summary.csv"
    write_raw(raw_path, raw_rows)
    write_summary(summary_path, summary_rows)
    manifest["results"] = {
        "raw_csv": str(raw_path),
        "raw_csv_sha256": sha256(raw_path),
        "summary_csv": str(summary_path),
        "summary_csv_sha256": sha256(summary_path),
        "samples": len(raw_rows),
    }
    manifest["completion_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\nRaw samples: {raw_path}")
    print(f"Summary: {summary_path}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
