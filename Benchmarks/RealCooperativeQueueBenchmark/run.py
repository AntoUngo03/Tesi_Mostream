#!/usr/bin/env python3
"""Measure the actual Pipeline.run_cooperative, including parking and wakeups."""

import argparse
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from Benchmarks.CooperativeQueueBenchmark.run import paired_interval, percentile, write_csv

VARIANTS = {
    "Vyukov": [],
    "PaddedFAA": ["-DMOSTREAM_PADDED_FAA=1"],
    "PaddedFAA-broadcast": ["-DMOSTREAM_PADDED_FAA=1", "-DMOSTREAM_COOPERATIVE_BROADCAST=1"],
    "FAA-cooperative": ["-DMOSTREAM_COOPERATIVE_FAA=1"],
}
KEYS = ("workers", "capacity", "batch", "work")


def ints(value):
    return [int(x) for x in value.split(",")]


def physical_cpus():
    allowed = os.sched_getaffinity(0)
    lines = subprocess.check_output(["lscpu", "-p=CPU,CORE,SOCKET"], text=True).splitlines()
    selected, cores = [], set()
    for line in lines:
        if line.startswith("#"):
            continue
        cpu, core, socket = map(int, line.split(","))
        if cpu in allowed and (socket, core) not in cores:
            selected.append(cpu)
            cores.add((socket, core))
    return selected


def execute(binary, env, messages, topology, case, verify=False, drop=0, timeout=60):
    workers, capacity, batch, work = case
    command = [str(binary), str(messages), *map(str, topology), str(workers),
               str(capacity), str(batch), str(work), str(int(verify)), str(drop), "1"]
    completed = subprocess.run(command, cwd=ROOT, env=env, check=True,
                               capture_output=True, text=True, timeout=timeout)
    if "Warning:" in completed.stdout or "PASS:" not in completed.stdout:
        raise RuntimeError(completed.stdout + completed.stderr)
    lines = completed.stdout.splitlines()
    result = next(line.split() for line in lines if line.startswith("PIPELINE_RESULT "))
    if result[-1] != "True":
        raise RuntimeError(completed.stdout)
    total = messages * topology[0]
    expected_count = total - ((total + drop - 1) // drop if drop else 0)
    if int(result[4]) != expected_count:
        raise RuntimeError("unexpected sink message count")
    samples = [int(line.split()[1]) for line in lines if line.startswith("PIPELINE_LATENCY_NS ")]
    expected_samples = sum(1 for i in range(0, total, 256) if not drop or i % drop)
    if len(samples) != expected_samples or any(v <= 0 for v in samples):
        raise RuntimeError("missing latency sample")
    return {"runtime_ms": float(result[1]), "total_ms": float(result[2]),
            "mmsg_s": float(result[3]), "count": int(result[4]),
            "activations": int(result[5]), "input_parks": int(result[6]),
            "output_parks": int(result[7]),
            "p99_us": percentile(samples, .99) / 1000 if samples else 0,
            "latency_samples": len(samples)}


def summarize(rows, cases, out, args):
    summary = []
    for case in cases:
        selected = [r for r in rows if tuple(r[k] for k in KEYS) == case]
        reference = {name: {r["rep"]: r for r in selected if r["queue"] == name}
                     for name in VARIANTS}
        for name in VARIANTS:
            group = list(reference[name].values())
            speedup, low, high = paired_interval([
                reference["Vyukov"][r["rep"]]["runtime_ms"] / r["runtime_ms"] for r in group])
            padded_speedup, padded_low, padded_high = paired_interval([
                reference["PaddedFAA"][r["rep"]]["runtime_ms"] / r["runtime_ms"] for r in group])
            control_speedup, control_low, control_high = paired_interval([
                reference["PaddedFAA-broadcast"][r["rep"]]["runtime_ms"] / r["runtime_ms"] for r in group])
            total_speedup, _, _ = paired_interval([
                reference["Vyukov"][r["rep"]]["total_ms"] / r["total_ms"] for r in group])
            summary.append(dict(zip(KEYS, case), queue=name,
                runtime_ms=statistics.fmean(r["runtime_ms"] for r in group),
                total_ms=statistics.fmean(r["total_ms"] for r in group),
                mmsg_s=statistics.fmean(r["mmsg_s"] for r in group),
                speedup_vs_vyukov=speedup, low95=low, high95=high,
                speedup_vs_padded=padded_speedup, padded_low95=padded_low, padded_high95=padded_high,
                speedup_vs_control=control_speedup, control_low95=control_low, control_high95=control_high,
                total_speedup_vs_vyukov=total_speedup,
                mean_run_p99_us=statistics.fmean(r["p99_us"] for r in group),
                activations_per_message=statistics.fmean(r["activations"] / r["count"] for r in group),
                input_parks_per_message=statistics.fmean(r["input_parks"] / r["count"] for r in group),
                output_parks_per_message=statistics.fmean(r["output_parks"] / r["count"] for r in group)))
    write_csv(out / "summary.csv", summary)
    text = ["# Pipeline cooperativa reale: risultati", "",
            f"Topologia source/transform/sink: {args.topology}. {args.messages:,} messaggi per source; "
            f"{args.repetitions} ripetizioni. Worker fissati a core fisici distinti.", "",
            "Sono misure di Pipeline.run_cooperative, con ready queue MPMC, migrazione degli actor, "
            "parcheggio, risvegli, MessageWrapper, stage di calcolo e chiusura EOS. "
            "Work e' il numero di iterazioni dipendenti di mixing UInt64 per messaggio nel transform.", "",
            "Runtime misura scheduler.start: dispatch/join, elaborazione, notifiche e metriche locali. "
            "Totale misura run_cooperative e comprende anche costruzione/distruzione delle code "
            "del runtime e dei comunicatori. L'inizializzazione di Pipeline (incluso Python) precede entrambi. "
            "Il runtime conserva le grandi wait queue originali anche nei backend broadcast.", "",
            "PaddedFAA-broadcast e' il controllo: stessa coda PaddedFAA normale, ma stessa politica "
            "di risveglio della FAA. Permette di distinguere i cambiamenti della coda da quelli dei risvegli.", "",
            "Speedup = tempo baseline / tempo variante. IC95% bootstrap percentile su rapporti "
            "accoppiati in log (4000 ricampionamenti); intervalli individuali, senza correzione "
            "per confronti multipli. Nessun outlier rimosso. p99 e' la media dei p99 campionati "
            "di ogni esecuzione (un messaggio ogni 256), dalla source al sink.", "",
            "| Worker | Capacita | Batch | Work | Coda | Mmsg/s | Runtime ms | Totale ms | vs Vyukov [IC95%] | vs PaddedFAA | p99 us |",
            "|---:|---:|---:|---:|---|---:|---:|---:|---|---:|---:|"]
    for r in summary:
        text.append(f"| {r['workers']} | {r['capacity']} | {r['batch']} | {r['work']} | {r['queue']} "
                    f"| {r['mmsg_s']:.3f} | {r['runtime_ms']:.2f} | {r['total_ms']:.2f} "
                    f"| {r['speedup_vs_vyukov']:.2f} [{r['low95']:.2f}, {r['high95']:.2f}] "
                    f"| {r['speedup_vs_padded']:.2f} | {r['mean_run_p99_us']:.2f} |")
    text += ["", "## Prenotazioni FAA a parita' di risveglio", "",
             "Rapporti contro PaddedFAA-broadcast; sopra 1 vince FAA-cooperative.", "",
             "| Worker | Capacita | Batch | Work | Speedup [IC95%] | Parcheggi input/msg | Parcheggi output/msg |",
             "|---:|---:|---:|---:|---|---:|---:|"]
    for r in summary:
        if r["queue"] == "FAA-cooperative":
            text.append(f"| {r['workers']} | {r['capacity']} | {r['batch']} | {r['work']} "
                        f"| {r['speedup_vs_control']:.2f} [{r['control_low95']:.2f}, {r['control_high95']:.2f}] "
                        f"| {r['input_parks_per_message']:.3f} | {r['output_parks_per_message']:.3f} |")
    text += ["", "I contatori parcheggi indicano ingressi nel percorso BLOCKED, inclusi ricontrolli "
             "che rendono immediatamente pronto l'actor. Non sono context switch del sistema operativo. "
             "Il batch limita le chiamate process() per attivazione; non raggruppa le prenotazioni atomiche.", "",
             "Validazione: conteggio, checksum del calcolo, latenza campionata e assenza di operazioni "
             "pendenti in tutte le misure. Prove exact-once separate (contatore per ID), con code "
             "da due slot, input vuoto e transform che elimina messaggi. Dettagli nel manifest."]
    (out / "RESULTS.md").write_text("\n".join(text) + "\n")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mostream-cooperative-matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    panels = sorted({(c[0], c[1]) for c in cases})
    fig, axes = plt.subplots(len(panels), 1, figsize=(10, 4 * len(panels)), squeeze=False)
    for axis, (workers, capacity) in zip(axes[:, 0], panels):
        configs = sorted({(r["batch"], r["work"]) for r in summary
                          if (r["workers"], r["capacity"]) == (workers, capacity)})
        for offset, name, color in zip((-.22, 0, .22), list(VARIANTS)[1:],
                                       ("#247BA0", "#707070", "#C23B54")):
            group = [next(r for r in summary if (r["workers"], r["capacity"], r["batch"], r["work"], r["queue"])
                          == (workers, capacity, b, w, name)) for b, w in configs]
            centers = [r["speedup_vs_vyukov"] for r in group]
            axis.errorbar([i + offset for i in range(len(configs))], centers,
                          yerr=[[v - r["low95"] for v, r in zip(centers, group)],
                                [r["high95"] - v for v, r in zip(centers, group)]],
                          fmt="o", capsize=3, label=name, color=color)
        axis.axhline(1, color="black", linewidth=1, linestyle="--")
        axis.set_xticks(range(len(configs)), [f"B{b} / work {w}" for b, w in configs])
        axis.set_title(f"Pipeline {args.topology}, {workers} worker, capacita {capacity}")
        axis.set_ylabel("Speedup runtime vs Vyukov")
        axis.grid(axis="y", alpha=.2)
        axis.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "comparison.png", dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=10000)
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--workers", type=ints, default=[1, 4])
    parser.add_argument("--capacities", type=ints, default=[1024])
    parser.add_argument("--batches", type=ints, default=[1, 8])
    parser.add_argument("--work", type=ints, default=[0, 256, 2048])
    parser.add_argument("--topology", default="4:4:4")
    parser.add_argument("--cpus", type=ints)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--output", type=Path, default=HERE / "results")
    args = parser.parse_args()
    topology = tuple(map(int, args.topology.split(":")))
    if len(topology) != 3 or min(topology) < 1 or args.messages < 1 or args.repetitions < 2:
        parser.error("positive 3-stage topology/messages and at least 2 repetitions required")
    cases = list(itertools.product(args.workers, args.capacities, args.batches, args.work))
    if any(w < 1 or w > sum(topology) or cap < 2 or cap & (cap - 1) or b < 1 or work < 0
           for w, cap, b, work in cases):
        parser.error("invalid worker count, power-of-two capacity, batch or work")
    cpus = args.cpus or physical_cpus()
    if len(cpus) < max(max(args.workers), 4) or not set(cpus) <= os.sched_getaffinity(0):
        parser.error("not enough available CPUs (at least four needed for regression cases)")
    env = dict(os.environ, MODULAR_CACHE_DIR="/tmp/mostream-mojo-cache",
               MOSTREAM_HOME=str(ROOT), MOSTREAM_PINNING=",".join(map(str, cpus)))
    subprocess.run(["make", "-C", "MoStream/lib"], cwd=ROOT, check=True, capture_output=True)
    args.output.mkdir(parents=True, exist_ok=True)
    rows, validation = [], []
    rng = random.Random(args.seed)
    builds = {}
    with tempfile.TemporaryDirectory(prefix="mostream-real-cooperative-") as temporary:
        binaries = {}
        for name, flags in VARIANTS.items():
            binary = Path(temporary) / name
            command = ["mojo", "build", "--Werror", "-O3", "-I", str(ROOT), *flags,
                       str(ROOT / "Tests/test_cooperative_pipeline.mojo"), "-o", str(binary)]
            subprocess.run(command, cwd=ROOT, env=env, check=True)
            builds[name] = command
            binaries[name] = binary
            result = subprocess.run([str(binary)], cwd=ROOT, env=env, check=True,
                                    capture_output=True, text=True, timeout=args.timeout)
            if result.stdout.count("PASS: cooperative pipeline") != 12:
                raise RuntimeError(result.stdout + result.stderr)
            for drop in (0, 1, 3):
                execute(binary, env, 1000, (8, 2, 8), (4, 2, 8, 8), True, drop, args.timeout)
            validation.append({"queue": name, "regression_cases": 12, "stress_cases": 3})
            print(f"PASS {name}: 15 pipeline validation cases", flush=True)
        for case in cases:
            for name in VARIANTS:
                execute(binaries[name], env, args.messages, topology, case, timeout=args.timeout)
        print("Warm-ups complete", flush=True)
        # Persist each completed sample; a failure leaves explicit partial data
        # and failure.json, never an apparently completed summary.
        with (args.output / "results.csv").open("w", newline="") as output:
            writer = None
            try:
                for rep in range(args.repetitions):
                    order = cases.copy()
                    rng.shuffle(order)
                    for case in order:
                        names = list(VARIANTS)
                        rng.shuffle(names)
                        for name in names:
                            metrics = execute(binaries[name], env, args.messages, topology, case, timeout=args.timeout)
                            row = dict(zip(KEYS, case), messages=args.messages, topology=args.topology,
                                       rep=rep, queue=name, **metrics)
                            if writer is None:
                                writer = csv.DictWriter(output, fieldnames=list(row))
                                writer.writeheader()
                            writer.writerow(row)
                            output.flush()
                            rows.append(row)
                    print(f"Completed block {rep + 1}/{args.repetitions}", flush=True)
            except Exception as error:
                (args.output / "failure.json").write_text(json.dumps({
                    "rep": rep, "case": case, "queue": name, "error": str(error),
                    "completed_samples": len(rows)}, indent=2) + "\n")
                raise
    sources = sorted((ROOT / "MoStream").glob("*.mojo")) + [Path(__file__),
               ROOT / "Tests/test_cooperative_pipeline.mojo",
               ROOT / "Benchmarks/CooperativeQueueBenchmark/run.py", ROOT / "MoStream/lib/libpinning.c"]
    manifest = {"created_utc": datetime.now(timezone.utc).isoformat(),
                "platform": platform.platform(), "mojo": subprocess.check_output(["mojo", "--version"], text=True).strip(),
                "lscpu": subprocess.check_output(["lscpu"], text=True),
                "affinity": sorted(os.sched_getaffinity(0)), "worker_cpu_order": cpus,
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "builds": builds, "validation": validation, "warmups_per_case_variant": 1,
                "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
                "results_sha256": hashlib.sha256((args.output / "results.csv").read_bytes()).hexdigest()}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    summarize(rows, cases, args.output, args)
    print(args.output / "RESULTS.md")


if __name__ == "__main__":
    main()
