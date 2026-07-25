#!/usr/bin/env python3
"""Run the queue benchmark matrix and export thesis-friendly statistics."""

import csv
import math
import re
import statistics
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = Path("/tmp/queue_benchmark")
CACHE = Path("/tmp/mostream-mojo-cache")
RESULT_PATTERN = re.compile(
    r"^(CAS|FAA|PADDEDFAA|RIGTORP|BLPRQ|WCQ|HYBRID1|HYBRID2|HYBRID4|HYBRID8)\s+time_ms=\s*([0-9.eE+-]+)\s+"
    r"throughput_Mmsg_s=\s*([0-9.eE+-]+)\s+valid=\s*(True|False)$",
    re.MULTILINE,
)
RUN_TIMEOUT_SECONDS = 300


def build() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["make", "-C", "MoStream/lib", "wcq_native.o"],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "mojo",
            "build",
            "Benchmarks/QueueBenchmark/queue_benchmark.mojo",
            "-I",
            ".",
            "-o",
            str(BINARY),
            "-Xlinker",
            str(ROOT / "MoStream/lib/wcq_native.o"),
        ],
        cwd=ROOT,
        env={**__import__("os").environ, "MODULAR_CACHE_DIR": str(CACHE)},
        check=True,
    )


def t_critical_95(df: int) -> float:
    # Two-sided Student-t critical values (alpha=0.05). The normal limit is
    # sufficiently accurate beyond the tabulated range for this benchmark.
    table = {
        1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
        6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
        11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
        16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
        21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
        26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
    }
    return table.get(df, 1.96)


def summarize(values: list[float]) -> tuple[float, float, float]:
    mean = statistics.fmean(values)
    stddev = statistics.stdev(values) if len(values) > 1 else 0.0
    ci95 = t_critical_95(len(values) - 1) * stddev / math.sqrt(len(values))
    return mean, stddev, ci95


def main() -> int:
    messages = int(sys.argv[1]) if len(sys.argv) > 1 else 250_000
    repetitions = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    output_path = Path(sys.argv[3]) if len(sys.argv) > 3 else ROOT / "Benchmarks/QueueBenchmark/results.csv"

    # Balanced scalability plus deliberately unbalanced workloads.
    topologies = [(1, 1), (2, 2), (4, 4), (8, 8), (1, 8), (8, 1)]
    capacities = [16, 1024, 65536]

    build()
    rows = []
    for producers, consumers in topologies:
        for capacity in capacities:
            command = [
                str(BINARY),
                str(messages),
                str(producers),
                str(consumers),
                str(capacity),
                str(repetitions),
            ]
            print(f"Running {producers}P-{consumers}C capacity={capacity} ...", flush=True)
            try:
                completed = subprocess.run(
                    command,
                    text=True,
                    capture_output=True,
                    check=True,
                    timeout=RUN_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired as error:
                if error.stdout:
                    print(error.stdout, file=sys.stderr)
                raise RuntimeError(
                    f"Benchmark timed out after {RUN_TIMEOUT_SECONDS}s for "
                    f"{producers}P-{consumers}C, capacity={capacity}"
                ) from error
            matches = RESULT_PATTERN.findall(completed.stdout)
            queue_names = [
                "CAS", "FAA", "PADDEDFAA", "RIGTORP", "BLPRQ", "WCQ",
                "HYBRID1", "HYBRID2", "HYBRID4", "HYBRID8",
            ]
            if len(matches) != repetitions * len(queue_names):
                print(completed.stdout, file=sys.stderr)
                raise RuntimeError("Unexpected benchmark output")

            samples: dict[str, list[float]] = {name: [] for name in queue_names}
            for queue, _time_ms, throughput, valid in matches:
                if valid != "True":
                    raise RuntimeError(
                        f"Correctness failure for {queue}, {producers}P-{consumers}C, capacity={capacity}"
                    )
                samples[queue].append(float(throughput))

            cas_mean, cas_stddev, cas_ci95 = summarize(samples["CAS"])
            faa_mean, faa_stddev, faa_ci95 = summarize(samples["FAA"])
            rigtorp_mean, rigtorp_stddev, rigtorp_ci95 = summarize(samples["RIGTORP"])
            padded_mean, padded_stddev, padded_ci95 = summarize(samples["PADDEDFAA"])
            blprq_mean, blprq_stddev, blprq_ci95 = summarize(samples["BLPRQ"])
            wcq_mean, wcq_stddev, wcq_ci95 = summarize(samples["WCQ"])
            faa_speedups = [faa / cas for faa, cas in zip(samples["FAA"], samples["CAS"])]
            faa_speedup_mean, faa_speedup_stddev, faa_speedup_ci95 = summarize(faa_speedups)
            wcq_vs_padded = [
                wcq / padded
                for wcq, padded in zip(samples["WCQ"], samples["PADDEDFAA"])
            ]
            wcq_vs_rigtorp = [
                wcq / rigtorp
                for wcq, rigtorp in zip(samples["WCQ"], samples["RIGTORP"])
            ]
            wcq_vs_padded_mean, _, wcq_vs_padded_ci95 = summarize(wcq_vs_padded)
            wcq_vs_rigtorp_mean, _, wcq_vs_rigtorp_ci95 = summarize(wcq_vs_rigtorp)
            row = {
                    "producers": producers,
                    "consumers": consumers,
                    "capacity": capacity,
                    "messages_per_producer": messages,
                    "repetitions": repetitions,
                    "cas_mean_Mmsg_s": cas_mean,
                    "cas_stddev_Mmsg_s": cas_stddev,
                    "cas_ci95_Mmsg_s": cas_ci95,
                    "faa_mean_Mmsg_s": faa_mean,
                    "faa_stddev_Mmsg_s": faa_stddev,
                    "faa_ci95_Mmsg_s": faa_ci95,
                    "rigtorp_mean_Mmsg_s": rigtorp_mean,
                    "rigtorp_stddev_Mmsg_s": rigtorp_stddev,
                    "rigtorp_ci95_Mmsg_s": rigtorp_ci95,
                    "padded_faa_mean_Mmsg_s": padded_mean,
                    "padded_faa_stddev_Mmsg_s": padded_stddev,
                    "padded_faa_ci95_Mmsg_s": padded_ci95,
                    "blprq_mean_Mmsg_s": blprq_mean,
                    "blprq_stddev_Mmsg_s": blprq_stddev,
                    "blprq_ci95_Mmsg_s": blprq_ci95,
                    "wcq_mean_Mmsg_s": wcq_mean,
                    "wcq_stddev_Mmsg_s": wcq_stddev,
                    "wcq_ci95_Mmsg_s": wcq_ci95,
                    "wcq_vs_padded_faa_mean": wcq_vs_padded_mean,
                    "wcq_vs_padded_faa_ci95": wcq_vs_padded_ci95,
                    "wcq_vs_rigtorp_mean": wcq_vs_rigtorp_mean,
                    "wcq_vs_rigtorp_ci95": wcq_vs_rigtorp_ci95,
                    "faa_vs_cas_mean": faa_speedup_mean,
                    "faa_vs_cas_ci95": faa_speedup_ci95,
            }
            hybrid_summaries = {}
            for threshold in (1, 2, 4, 8):
                name = f"HYBRID{threshold}"
                mean, stddev, ci95 = summarize(samples[name])
                vs_cas = [hybrid / cas for hybrid, cas in zip(samples[name], samples["CAS"])]
                vs_faa = [hybrid / faa for hybrid, faa in zip(samples[name], samples["FAA"])]
                vs_cas_mean, _, vs_cas_ci95 = summarize(vs_cas)
                vs_faa_mean, _, vs_faa_ci95 = summarize(vs_faa)
                hybrid_summaries[threshold] = (mean, ci95, vs_cas_mean, vs_cas_ci95, vs_faa_mean, vs_faa_ci95)
                row.update({
                    f"hybrid{threshold}_mean_Mmsg_s": mean,
                    f"hybrid{threshold}_stddev_Mmsg_s": stddev,
                    f"hybrid{threshold}_ci95_Mmsg_s": ci95,
                    f"hybrid{threshold}_vs_cas_mean": vs_cas_mean,
                    f"hybrid{threshold}_vs_cas_ci95": vs_cas_ci95,
                    f"hybrid{threshold}_vs_faa_mean": vs_faa_mean,
                    f"hybrid{threshold}_vs_faa_ci95": vs_faa_ci95,
                })
            rows.append(row)
            print(
                f"  CAS {cas_mean:.3f}±{cas_ci95:.3f}, "
                f"FAA {faa_mean:.3f}±{faa_ci95:.3f}, "
                f"RIGTORP {rigtorp_mean:.3f}±{rigtorp_ci95:.3f}, "
                f"PADDED-FAA {padded_mean:.3f}±{padded_ci95:.3f}, "
                f"BLPRQ {blprq_mean:.3f}±{blprq_ci95:.3f}, "
                f"WCQ {wcq_mean:.3f}±{wcq_ci95:.3f} Mmsg/s"
            )
            print(
                f"  WCQ/PADDED-FAA {wcq_vs_padded_mean:.3f}"
                f"±{wcq_vs_padded_ci95:.3f}x, "
                f"WCQ/RIGTORP {wcq_vs_rigtorp_mean:.3f}"
                f"±{wcq_vs_rigtorp_ci95:.3f}x"
            )
            print("  " + ", ".join(
                f"H{threshold} {hybrid_summaries[threshold][0]:.3f}±{hybrid_summaries[threshold][1]:.3f}"
                for threshold in (1, 2, 4, 8)
            ) + " Mmsg/s")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Results written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
