#!/usr/bin/env python3
"""Run a multi-configuration sweep of PaddedFAA vs pure-spin comparisons."""

from __future__ import annotations

import argparse
import csv
import itertools
import os
import shutil
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "Benchmarks" / "PipelineQueueBenchmark"
BINARY_PATH = Path("/tmp/paddedfaa_spin_compare")
DEFAULT_OUTPUT = SOURCE_DIR / "paddedfaa_spin_results.csv"


def parse_csv_ints(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError(f"invalid comma-separated integer list: {raw!r}")
    return [int(v) for v in values]


def compile_benchmark() -> None:
    cmd = [
        "mojo",
        "build",
        "-O3",
        "-I.",
        str(SOURCE_DIR / "PaddedFAA_spin.mojo"),
        "-o",
        str(BINARY_PATH),
    ]
    subprocess.run(cmd, cwd=ROOT, check=True)


def build_binary_command(args: argparse.Namespace, messages: int, producers: int, consumers: int, capacity: int, seed: int) -> list[str]:
    cmd = [
        str(BINARY_PATH),
        str(messages),
        str(producers),
        str(consumers),
        str(capacity),
        str(seed),
    ]
    if args.pinning:
        if not shutil.which("taskset"):
            raise RuntimeError("taskset is required for --pinning on Linux")
        cpus = parse_csv_ints(args.cpus)
        return ["taskset", "-c", ",".join(str(cpu) for cpu in cpus)] + cmd
    return cmd


def parse_run(output: str) -> dict[str, float | bool]:
    metrics: dict[str, float | bool] = {}
    for line in output.splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        if parts[0] == "PaddedFAAQueue":
            metrics["hybrid_time_ms"] = float(parts[2])
            metrics["hybrid_throughput_mmsg_s"] = float(parts[4])
            metrics["hybrid_valid"] = parts[6].lower() == "true"
        elif parts[0] == "PaddedFAASpinQueue":
            metrics["spin_time_ms"] = float(parts[2])
            metrics["spin_throughput_mmsg_s"] = float(parts[4])
            metrics["spin_valid"] = parts[6].lower() == "true"
        elif parts[0].startswith("sleep0_cost_ratio="):
            metrics["sleep0_cost_ratio"] = float(parts[1])
            if "spin_vs_hybrid_pct=" in line:
                tail = line.split("spin_vs_hybrid_pct=", 1)[1].strip()
                metrics["spin_vs_hybrid_pct"] = float(tail)
    return metrics


def summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {"mean": 0.0, "median": 0.0, "stdev": 0.0}
    mean = statistics.fmean(values)
    stdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "mean": mean,
        "median": statistics.median(values),
        "stdev": stdev,
    }


def run_case(
    messages: int,
    producers: int,
    consumers: int,
    capacity: int,
    repetitions: int,
    seed: int,
    pinning: bool,
    cpus: str,
) -> list[dict[str, float | bool | int | str]]:
    results: list[dict[str, float | bool | int | str]] = []
    for rep in range(repetitions):
        run_seed = seed + rep
        cmd = build_binary_command(
            argparse.Namespace(pinning=pinning, cpus=cpus),
            messages,
            producers,
            consumers,
            capacity,
            run_seed,
        )
        completed = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"benchmark failed: messages={messages} producers={producers} "
                f"consumers={consumers} capacity={capacity} rep={rep}\n"
                f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
            )
        parsed = parse_run(completed.stdout)
        if not {"hybrid_time_ms", "spin_time_ms"}.issubset(parsed):
            raise RuntimeError(
                f"unexpected output: messages={messages} producers={producers} "
                f"consumers={consumers} capacity={capacity} rep={rep}\n{completed.stdout}"
            )
        item = {
            "messages": messages,
            "producers": producers,
            "consumers": consumers,
            "capacity": capacity,
            "rep": rep,
            "pinning": "on" if pinning else "off",
            "hybrid_time_ms": float(parsed["hybrid_time_ms"]),
            "spin_time_ms": float(parsed["spin_time_ms"]),
            "hybrid_valid": bool(parsed["hybrid_valid"]),
            "spin_valid": bool(parsed["spin_valid"]),
            "ratio_spin_hybrid": float(parsed.get("sleep0_cost_ratio", 1.0)),
            "pct_spin_vs_hybrid": float(parsed.get("spin_vs_hybrid_pct", 0.0)),
        }
        results.append(item)
    return results


def config_grid(args: argparse.Namespace) -> list[tuple[int, int, int, int]]:
    messages = parse_csv_ints(args.messages)
    producers = parse_csv_ints(args.producers)
    consumers = parse_csv_ints(args.consumers)
    capacities = parse_csv_ints(args.capacity)
    return [(m, p, c, cap) for m, p, c, cap in itertools.product(messages, producers, consumers, capacities)]


def write_csv(rows: list[dict[str, float | bool | int | str]], path: Path) -> None:
    fieldnames = [
        "messages",
        "producers",
        "consumers",
        "capacity",
        "rep",
        "pinning",
        "hybrid_time_ms",
        "spin_time_ms",
        "hybrid_valid",
        "spin_valid",
        "ratio_spin_hybrid",
        "pct_spin_vs_hybrid",
    ]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_summary(rows: list[dict[str, float | bool | int | str]], args: argparse.Namespace) -> None:
    by_config: dict[tuple[int, int, int, int, str], list[dict[str, float | bool | int | str]]] = {}
    for row in rows:
        key = (
            int(row["messages"]),
            int(row["producers"]),
            int(row["consumers"]),
            int(row["capacity"]),
            str(row["pinning"]),
        )
        by_config.setdefault(key, []).append(row)

    print(f"\nSweep: PaddedFAAQueue vs PaddedFAASpinQueue")
    print(f"repetitions={args.repetitions} seed={args.seed} pinning={args.pinning}")
    for key in sorted(by_config):
        config_rows = by_config[key]
        hybrid = [float(r["hybrid_time_ms"]) for r in config_rows]
        spin = [float(r["spin_time_ms"]) for r in config_rows]
        ratio = [float(r["ratio_spin_hybrid"]) for r in config_rows]
        pct = [float(r["pct_spin_vs_hybrid"]) for r in config_rows]
        h = summary(hybrid)
        s = summary(spin)
        rr = summary(ratio)
        pp = summary(pct)
        print(f"\nconfig messages={key[0]} producers={key[1]} consumers={key[2]} capacity={key[3]} pinning={key[4]}")
        print(f"  hybrid mean_ms={h['mean']:.3f} median_ms={h['median']:.3f} stdev_ms={h['stdev']:.3f}")
        print(f"  spin   mean_ms={s['mean']:.3f} median_ms={s['median']:.3f} stdev_ms={s['stdev']:.3f}")
        print(f"  ratio  mean_spin/hybrid={rr['mean']:.4f} median={rr['median']:.4f} stdev={rr['stdev']:.4f}")
        print(f"  delta  mean_pct={pp['mean']:.3f} median_pct={pp['median']:.3f} stdev_pct={pp['stdev']:.3f}")
    print(f"\nCSV saved to: {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=str, default="20000,50000,100000")
    parser.add_argument("--producers", type=str, default="2,4,8")
    parser.add_argument("--consumers", type=str, default="2,8")
    parser.add_argument("--capacity", type=str, default="1024")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=0xC0FFEE123456789)
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--no-compile", action="store_true")
    parser.add_argument("--pinning", action="store_true", help="Run each benchmark under taskset CPU pinning")
    parser.add_argument("--cpus", type=str, default="0,1,2,3,4,5,6,7", help="Comma-separated CPU list for taskset when --pinning is enabled")
    args = parser.parse_args()

    configs = config_grid(args)
    if not configs:
        raise SystemExit("No benchmark configuration selected")

    if not args.no_compile:
        compile_benchmark()
    if not BINARY_PATH.exists():
        raise SystemExit(f"Binary not found: {BINARY_PATH}")

    rows: list[dict[str, float | bool | int | str]] = []
    for messages, producers, consumers, capacity in configs:
        rows.extend(
            run_case(
                messages=messages,
                producers=producers,
                consumers=consumers,
                capacity=capacity,
                repetitions=args.repetitions,
                seed=args.seed,
                pinning=args.pinning,
                cpus=args.cpus,
            )
        )

    output_path = Path(args.output)
    write_csv(rows, output_path)
    print_summary(rows, args)


if __name__ == "__main__":
    main()
