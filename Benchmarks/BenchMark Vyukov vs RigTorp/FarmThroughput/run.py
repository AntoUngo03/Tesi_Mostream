#!/usr/bin/env python3
"""Benchmark 2: sorgente sequenziale, una coda condivisa e N consumatori."""
import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import tempfile
import time

# Riusa compilazione, selezione dei core e backend del benchmark 1, senza modificarlo.
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SHARED = HERE.parent / "PipelineGrain"
SPEC = importlib.util.spec_from_file_location("pipeline_grain_helpers", SHARED / "run.py")
HELPERS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPERS)
execute = HELPERS.execute
BACKENDS = HELPERS.BACKENDS


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows, path):
    groups = {}
    for row in rows:
        groups.setdefault((row["queue"], row["consumers"], row["target_work_us"]), []).append(row)
    baselines = {(q, t): statistics.median(row["elapsed_ns"] for row in group)
                 for (q, n, t), group in groups.items() if n == 1}
    summary = []
    for (queue, consumers, target), group in sorted(groups.items()):
        messages = group[0]["messages"]
        elapsed = statistics.median(row["elapsed_ns"] for row in group)
        throughput = messages * 1e9 / elapsed
        ideal = consumers * 1e6 / target if target else None
        measured_compute = group[0]["measured_compute_ns_per_input"]
        summary.append({
            "queue": queue, "consumers": consumers, "total_tasks": consumers + 1,
            "target_work_us": target, "messages": messages, "repeats": len(group),
            "median_ns_per_input": elapsed / messages,
            "throughput_per_second": throughput,
            "min_throughput_per_second": min(row["throughput_per_second"] for row in group),
            "max_throughput_per_second": max(row["throughput_per_second"] for row in group),
            "ideal_target_throughput": ideal,
            "efficiency_vs_target": throughput / ideal if ideal else None,
            "measured_compute_ns_per_input": measured_compute,
            "ideal_measured_compute_throughput": consumers * 1e9 / measured_compute if target else None,
            "speedup_vs_one_consumer": baselines[(queue, target)] / elapsed,
            "scaling_efficiency": baselines[(queue, target)] / elapsed / consumers,
            "median_source_elapsed_ns": statistics.median(row["source_elapsed_ns"] for row in group),
            "min_worker_count_observed": min(min(row["worker_counts"]) for row in group),
            "max_worker_count_observed": max(max(row["worker_counts"]) for row in group),
        })
    write_csv(path, summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumers", default="1,2,4,8,16")
    parser.add_argument("--work-us", default="0,1,2,5")
    parser.add_argument("--queues", default="rigtorp,vyukov")
    parser.add_argument("--messages", type=int, default=100000)
    parser.add_argument("--capacity", type=int, default=1024)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cpus", default="auto", help="auto, none oppure una lista di N+1 CPU distinte")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    consumers = sorted(set([1] + HELPERS.parse_numbers(args.consumers, int)))
    targets = HELPERS.parse_numbers(args.work_us, float)
    queues = list(dict.fromkeys(args.queues.split(",")))
    if not targets or any(not math.isfinite(t) or t < 0 for t in targets) or min(consumers) < 1:
        parser.error("N positivo e T finito non negativo sono richiesti")
    if not queues or any(queue not in BACKENDS for queue in queues):
        parser.error("Code ammesse: rigtorp,vyukov")
    if min(args.messages, args.repeats, args.timeout) < 1:
        parser.error("messages, repeats e timeout devono essere positivi")
    if args.capacity < 2 or args.capacity & (args.capacity - 1):
        parser.error("Capacità comune: potenza di due >= 2")
    cpus = HELPERS.physical_cpus() if args.cpus == "auto" else (
        [] if args.cpus == "none" else [int(cpu) for cpu in args.cpus.split(",")])
    if cpus and (len(cpus) < max(consumers) + 1 or len(cpus) != len(set(cpus))
                 or not set(cpus) <= os.sched_getaffinity(0)):
        parser.error("Servono N+1 CPU distinte consentite, inclusa quella della sorgente")
    output = args.output or HERE / "results" / time.strftime("%Y%m%d-%H%M%S")
    output.mkdir(parents=True, exist_ok=False)
    paths = [ROOT / "MoStream/MPMC_queue_rigtorp.mojo", SHARED / "queues.mojo",
             SHARED / "run.py", HERE / "farm.mojo", HERE / "run.py", ROOT / "MoStream/lib/libpinning.c"]
    metadata = {
        "topology": "one sequential source -> one shared bounded queue -> N consumers",
        "arguments": vars(args) | {"output": str(output)},
        "mojo_version": execute(["mojo", "--version"]).strip(),
        "platform": platform.platform(), "cpus": cpus,
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
    }
    try:
        metadata["git_commit"] = execute(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()
        metadata["git_status"] = execute(["git", "-C", str(ROOT), "status", "--short"])
    except RuntimeError:
        metadata["git_commit"] = None
    rows = []
    with tempfile.TemporaryDirectory(prefix="mostream-farm-") as directory:
        work = Path(directory)
        library = work / "libpinning.so"
        if cpus:
            execute(["gcc", "-O3", "-shared", "-fPIC", "-D_GNU_SOURCE",
                     str(ROOT / "MoStream/lib/libpinning.c"), "-pthread", "-o", str(library)])
        common = paths[0].read_text() + "\n" + paths[1].read_text()
        harness = (HERE / "farm.mojo").read_text()
        binaries = {}
        for queue in queues:
            source = work / f"{queue}.mojo"
            source.write_text(common + "\ncomptime GrainQueue = " + BACKENDS[queue] + "\n" + harness)
            binary = work / queue
            execute(["mojo", "build", "-O3", str(source), "-o", str(binary)], args.timeout)
            binaries[queue] = binary
        reference = str(binaries[queues[0]])
        workers = int(execute([reference, "info"]).split()[-1])
        if max(consumers) + 1 > workers:
            raise RuntimeError(f"Servono N+1 worker: disponibili {workers}")
        # Controllo di terminazione con pochi input, anche meno dei consumatori.
        # Capienza 2 forza il riuso degli slot e l'invio progressivo delle sentinelle.
        for queue in queues:
            execute([str(binaries[queue]), str(max(consumers)), str(min(args.messages, 3)),
                     "0", "2", "1", ",".join(map(str, cpus)) if cpus else "none", str(library)], args.timeout)
        metadata["preflight"] = {"consumers": max(consumers), "messages": min(args.messages, 3),
                                  "capacity": 2, "iterations": 0, "passed": True}
        # Calibra sul core del primo consumatore, distinto da quello della sorgente.
        prefix = ["taskset", "-c", str(cpus[1])] if cpus else []
        calibrations = [float(execute(prefix + [reference, "calibrate"]).split()[1]) for _ in range(5)]
        ns_per_iteration = statistics.median(calibrations)
        iterations_by_t = {t: 0 if t == 0 else max(1, round(t * 1000 / ns_per_iteration)) for t in targets}
        compute_samples = {}
        for t in targets:
            compute_samples[t] = [int(execute(prefix + [reference, "compute", str(args.messages),
                                                       str(iterations_by_t[t])]).split()[1]) / args.messages for _ in range(3)]
        metadata.update({"runtime_workers": workers, "ns_per_iteration": ns_per_iteration,
                         "calibrations_ns_per_iteration": calibrations,
                         "compute_samples_ns_per_input": compute_samples})
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2))
        # Coppie vicine nel tempo, con ordine delle due code casuale e riproducibile.
        rng = random.Random(args.seed)
        jobs = [(n, t, repeat) for repeat in range(args.repeats) for t in targets for n in consumers]
        rng.shuffle(jobs)
        position = 0
        with (output / "raw.csv").open("w", newline="") as stream:
            writer = None
            for n, t, repeat in jobs:
                order = list(queues)
                rng.shuffle(order)
                for queue in order:
                    command = [str(binaries[queue]), str(n), str(args.messages), str(iterations_by_t[t]),
                               str(args.capacity), "1", ",".join(map(str, cpus)) if cpus else "none", str(library)]
                    lines = execute(command, args.timeout).splitlines()
                    result = next(line.split() for line in lines if line.startswith("RESULT "))
                    elapsed, source_ns = int(result[2]), int(result[3])
                    counts = [int(line.split()[2]) for line in lines if line.startswith("COUNT ")]
                    if len(counts) != n or sum(counts) != args.messages:
                        raise RuntimeError("Conteggi dei worker non coerenti")
                    throughput = args.messages * 1e9 / elapsed
                    ideal = n * 1e6 / t if t else None
                    row = {
                        "queue": queue, "consumers": n, "total_tasks": n + 1,
                        "target_work_us": t, "iterations_per_input": iterations_by_t[t],
                        "calibrated_work_us": iterations_by_t[t] * ns_per_iteration / 1000,
                        "measured_compute_ns_per_input": statistics.median(compute_samples[t]),
                        "messages": args.messages, "capacity": args.capacity, "repeat": repeat,
                        "elapsed_ns": elapsed, "ns_per_input": elapsed / args.messages,
                        "throughput_per_second": throughput,
                        "ideal_target_throughput": ideal,
                        "efficiency_vs_target": throughput / ideal if ideal else None,
                        "source_elapsed_ns": source_ns, "worker_counts": counts,
                    }
                    if writer is None:
                        writer = csv.DictWriter(stream, fieldnames=list(row))
                        writer.writeheader()
                    stored = row | {"worker_counts": json.dumps(counts)}
                    writer.writerow(stored)
                    stream.flush()
                    rows.append(row)
                    position += 1
                    print(f"[{position}/{len(jobs) * len(queues)}] {queue} N={n} T={t:g} us: {throughput:,.0f} input/s", flush=True)
    summarize(rows, output / "summary.csv")
    print(f"Risultati: {output}")


if __name__ == "__main__":
    main()
