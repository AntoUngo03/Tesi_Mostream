#!/usr/bin/env python3
"""Confronta code diverse con lavoro totale fisso e N stadi sequenziali."""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
import time

BACKENDS = {
    "tsigas": "TsigasZhangMPMCQueue",
    "vyukov": "GrainVyukovQueue",
}


def execute(command, timeout=120):
    # Argomenti separati, senza shell; errori e timeout interrompono la misura.
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Comando fallito: {command}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def physical_cpus():
    # Una CPU logica per core fisico, rispettando l'affinità concessa al processo.
    selected, seen = [], set()
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            key = ((topology / "physical_package_id").read_text().strip(),
                   (topology / "core_id").read_text().strip())
        except OSError:
            key = ("unknown", str(cpu))
        if key not in seen:
            selected.append(cpu)
            seen.add(key)
    return selected


def parse_numbers(text, convert):
    return sorted(set(convert(value) for value in text.split(",")))


def summarize(rows, output):
    groups = {}
    for row in rows:
        key = (row["queue"], row["stages"], row["target_total_us"])
        groups.setdefault(key, []).append(row)
    baselines = {(q, t): statistics.median(r["elapsed_ns"] for r in group)
                 for (q, n, t), group in groups.items() if n == 1}
    summary = []
    for (queue, stages, target), group in sorted(groups.items()):
        elapsed = statistics.median(row["elapsed_ns"] for row in group)
        messages = group[0]["messages"]
        baseline = baselines[(queue, target)]
        summary.append({
            "queue": queue, "stages": stages, "target_total_us": target,
            "iterations_total": group[0]["iterations_total"],
            "messages": messages, "repeats": len(group),
            "median_ns_per_input": elapsed / messages,
            "min_ns_per_input": min(r["elapsed_ns"] for r in group) / messages,
            "max_ns_per_input": max(r["elapsed_ns"] for r in group) / messages,
            "throughput_per_second": messages * 1e9 / elapsed,
            "speedup_vs_n1": baseline / elapsed,
            "parallel_efficiency": baseline / elapsed / stages,
            "ideal_ns_per_input_from_n1": baseline / messages / stages,
        })
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)


def resolve_capacity(requested, successful_operations):
    """No full lap in a concurrent run: strict bound per queue instance."""
    if requested < 0:
        raise ValueError("capacity deve essere zero (auto) o una potenza di due")
    capacity = requested or max(4, 1 << successful_operations.bit_length())
    if capacity < 4 or capacity & (capacity - 1):
        raise ValueError("L'anello deve essere potenza di due >= 4")
    if capacity <= successful_operations:
        raise ValueError(f"Vincolo anti-ABA: servono più di {successful_operations} celle fisiche; usare --capacity 0 per auto")
    return capacity


def capacity_metadata(capacity, bound):
    return {"physical_ring_cells_both": capacity, "tsigas_usable_capacity": capacity - 2,
            "vyukov_usable_capacity": capacity, "successful_operations_upper_bound_per_queue": bound,
            "tsigas_dequeue_validation": "head and tail snapshot checked; prevents stale-empty scan skipping a concurrent publish",
            "aba_policy": "no complete lap in each fresh queue; strict total-success bound < ring size",
            "capacity_comparison": "equal physical ring length; different usable capacity",
            "synthetic_kernel": "xorshift with 63-bit mask after each iteration, identical for both backends",
            "limitations": "large-ring batch regime; no steady-state reuse/backpressure performance claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stages", default="1,2,4,8,16")
    parser.add_argument("--total-us", default="0,1,2,5")
    parser.add_argument("--queues", default=",".join(BACKENDS))
    parser.add_argument("--messages", type=int, default=50000)
    parser.add_argument("--capacity", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cpus", default="auto", help="auto: core fisici; none: nessun pinning; oppure CPU separate da virgole")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    stages = sorted(set([1] + parse_numbers(args.stages, int)))
    targets = parse_numbers(args.total_us, float)
    queues = list(dict.fromkeys(args.queues.split(",")))
    if not targets or not queues or any(q not in BACKENDS for q in queues):
        parser.error("Specificare T e code valide: " + ",".join(BACKENDS))
    if min(stages) < 1 or any(not math.isfinite(t) or t < 0 for t in targets):
        parser.error("N deve essere positivo; T deve essere finito e non negativo")
    if min(args.messages, args.repeats, args.timeout) < 1:
        parser.error("messages, repeats e timeout devono essere positivi")
    bound = 2 * args.messages
    try:
        args.capacity = resolve_capacity(args.capacity, bound)
    except ValueError as error:
        parser.error(str(error))
    allowed = set(os.sched_getaffinity(0))
    cpus = physical_cpus() if args.cpus == "auto" else (
        [] if args.cpus == "none" else [int(cpu) for cpu in args.cpus.split(",")])
    if cpus and (len(cpus) < max(stages) or len(cpus) != len(set(cpus)) or not set(cpus) <= allowed):
        parser.error("Servono almeno N CPU distinte e consentite per il pinning")
    here = Path(__file__).resolve().parent
    root = here.parents[2]
    output = args.output or here / "results" / time.strftime("%Y%m%d-%H%M%S")
    output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "arguments": vars(args) | {"output": str(output)},
        "mojo_version": execute(["mojo", "--version"]).strip(),
        "platform": platform.platform(), "cpus": cpus,
        "allowed_cpus": sorted(allowed), "seed": args.seed,
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "queue_sources": "Tsigas-Zhang UInt64 direct; Vyukov nel file queues.mojo",
        "comparison_contract": capacity_metadata(args.capacity, bound),
    }
    metadata["arguments"]["output"] = str(output)
    metadata["source_sha256"] = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root / "MoStream/MPMC_queue_tsigas_zhang.mojo", here / "queues.mojo",
                     here / "pipeline.mojo", here / "run.py")
    }
    try:
        metadata["git_commit"] = execute(["git", "-C", str(root), "rev-parse", "HEAD"]).strip()
        metadata["git_status"] = execute(["git", "-C", str(root), "status", "--short"])
    except RuntimeError:
        metadata["git_commit"] = None
    rows = []
    with tempfile.TemporaryDirectory(prefix="mostream-grain-") as temporary:
        work = Path(temporary)
        pin_library = work / "libpinning.so"
        if cpus:
            execute(["gcc", "-O3", "-shared", "-fPIC", "-D_GNU_SOURCE",
                     str(root / "MoStream/lib/libpinning.c"), "-pthread", "-o", str(pin_library)])
        # Tutti i backend condividono lo stesso harness e messaggi UInt64.
        common = (root / "MoStream/MPMC_queue_tsigas_zhang.mojo").read_text()
        alternatives = (here / "queues.mojo").read_text()
        harness = (here / "pipeline.mojo").read_text()
        binaries = {}
        for queue in queues:
            source = work / f"{queue}.mojo"
            source.write_text(common + "\n" + alternatives + "\ncomptime GrainQueue = " + BACKENDS[queue] + "\n" + harness)
            binary = work / queue
            execute(["mojo", "build", "-O3", str(source), "-o", str(binary)], args.timeout)
            binaries[queue] = binary
        first_binary = str(binaries[queues[0]])
        worker_output = execute([first_binary, "info"])
        workers = int(worker_output.split()[-1])
        if max(stages) > workers:
            raise RuntimeError(f"N={max(stages)} supera i {workers} worker del runtime")
        calibration_command = [first_binary, "calibrate"]
        if cpus:
            calibration_command = ["taskset", "-c", str(cpus[0])] + calibration_command
        calibrations = []
        for _ in range(5):
            text = execute(calibration_command)
            calibrations.append(float(next(line for line in text.splitlines() if line.startswith("CALIBRATION ")).split()[1]))
        ns_per_iteration = statistics.median(calibrations)
        metadata.update({"runtime_workers": workers, "ns_per_iteration": ns_per_iteration,
                         "calibrations_ns_per_iteration": calibrations})
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2))
        jobs = [(q, n, t, repeat) for repeat in range(args.repeats)
                for t in targets for n in stages for q in queues]
        random.Random(args.seed).shuffle(jobs)
        with (output / "raw.csv").open("w", newline="") as stream:
            writer = None
            for position, (queue, n, target, repeat) in enumerate(jobs, 1):
                iterations = 0 if target == 0 else max(1, round(target * 1000 / ns_per_iteration))
                command = [str(binaries[queue]), str(n), str(args.messages), str(iterations),
                           str(args.capacity), "1", ",".join(map(str, cpus)) if cpus else "none", str(pin_library)]
                text = execute(command, args.timeout)
                elapsed = int(next(line for line in text.splitlines() if line.startswith("RESULT ")).split()[2])
                row = {"queue": queue, "stages": n, "target_total_us": target,
                       "target_stage_us": target / n, "iterations_total": iterations,
                       "stage_iterations_min": iterations // n,
                       "stage_iterations_max": iterations // n + int(iterations % n != 0),
                       "calibrated_total_us": iterations * ns_per_iteration / 1000,
                       "messages": args.messages, "capacity": args.capacity,
                       "repeat": repeat, "tsigas_usable_capacity": args.capacity - 2,
                       "vyukov_usable_capacity": args.capacity, "aba_success_bound": bound, "elapsed_ns": elapsed,
                       "ns_per_input": elapsed / args.messages,
                       "throughput_per_second": args.messages * 1e9 / elapsed}
                if writer is None:
                    writer = csv.DictWriter(stream, fieldnames=list(row))
                    writer.writeheader()
                writer.writerow(row)
                stream.flush()
                rows.append(row)
                print(f"[{position}/{len(jobs)}] {queue} N={n} T={target:g} us: {row['ns_per_input']:.1f} ns/input", flush=True)
    summarize(rows, output / "summary.csv")
    print(f"Risultati: {output}")


if __name__ == "__main__":
    main()
