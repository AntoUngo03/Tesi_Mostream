#!/usr/bin/env python3
"""Build, verify, and compare four queues under the same cooperative scheduler."""

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
NAMES = ("Vyukov", "PaddedFAA", "CAS-bounded", "FAA-cooperative")
KEYS = ("producers", "consumers", "workers", "capacity", "batch")


def integers(value):
    result = [int(part) for part in value.split(",")]
    if not result or any(v < 1 for v in result):
        raise argparse.ArgumentTypeError("expected positive comma-separated integers")
    return result


def percentile(values, quantile):
    values = sorted(values)
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def paired_interval(ratios):
    logs = [math.log(value) for value in ratios]
    rng = random.Random(90421)
    boot = [math.exp(statistics.fmean(rng.choices(logs, k=len(logs))))
            for _ in range(4000)]
    return math.exp(statistics.fmean(logs)), percentile(boot, .025), percentile(boot, .975)


def execute(binary, env, kind, messages, case, verify, timeout):
    p, c, w, capacity, batch = case
    command = [str(binary), str(kind), str(messages), str(p), str(c),
               str(capacity), str(w), str(batch), str(int(verify))]
    completed = subprocess.run(command, cwd=ROOT, env=env, check=True,
                               capture_output=True, text=True, timeout=timeout)
    lines = completed.stdout.splitlines()
    result = next(line.split() for line in lines if line.startswith("RESULT "))
    if result[-1] != "True":
        raise RuntimeError(completed.stdout)
    samples = [int(line.split()[1]) for line in lines if line.startswith("LATENCY_NS ")]
    if len(samples) != (messages * p + 255) // 256 or any(v <= 0 for v in samples):
        raise RuntimeError("Missing or invalid latency samples")
    layout = next(line.split()[1:] for line in lines if line.startswith("LAYOUT "))
    if int(layout[2]) % 64:
        raise RuntimeError("Padded slot stride is not a multiple of 64 bytes")
    return {"ms": float(result[2]), "mmsg_s": float(result[3]),
            "count": int(result[4]), "retries": int(result[5]),
            "waits": int(result[6]), "activations": int(result[7]),
            "p99_us": percentile(samples, .99) / 1000 if samples else 0,
            "latency_samples": len(samples), "layout": layout}


def write_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def report(rows, cases, out, args):
    summary = []
    for case in cases:
        selected = [r for r in rows if tuple(r[key] for key in KEYS) == case]
        baselines = {r["rep"]: r["ms"] for r in selected if r["kind"] == 0}
        padded = {r["rep"]: r["ms"] for r in selected if r["kind"] == 1}
        for kind, name in enumerate(NAMES):
            group = [r for r in selected if r["kind"] == kind]
            speedup, low, high = paired_interval([baselines[r["rep"]] / r["ms"] for r in group])
            vs_padded, _, _ = paired_interval([padded[r["rep"]] / r["ms"] for r in group])
            summary.append(dict(zip(KEYS, case), queue=name,
                                mmsg_s=statistics.fmean(r["mmsg_s"] for r in group),
                                speedup_vs_vyukov=speedup, low95=low, high95=high,
                                speedup_vs_padded=vs_padded,
                                mean_run_p99_us=statistics.fmean(r["p99_us"] for r in group),
                                retries_per_message=statistics.fmean(r["retries"] / r["count"] for r in group),
                                waits_per_message=statistics.fmean(r["waits"] / r["count"] for r in group),
                                activations_per_message=statistics.fmean(r["activations"] / r["count"] for r in group)))
    write_csv(out / "summary.csv", summary)
    text = ["# Confronto cooperativo: risultati", "",
            f"{args.messages:,} messaggi/produttore; {args.repetitions} ripetizioni per caso e variante.",
            "Scheduler sperimentale round-robin con actor assegnati stabilmente ai worker; "
            "nessun parcheggio, work stealing o calcolo applicativo. Non e' una misura della pipeline MoStream completa.", "",
            "Speedup = tempo Vyukov / tempo variante: sopra 1 vince la variante. "
            "IC95% bootstrap percentile dei rapporti accoppiati in log, 4000 ricampionamenti, "
            "senza correzione per confronti multipli. Campagna esplorativa; nessuna esclusione di outlier.", "",
            "p99: media dei p99 delle singole esecuzioni, campionamento ogni 256 messaggi; "
            "dal primo tentativo di push al completamento del pop, attese incluse. "
            "Non e' il p99 di tutti i messaggi.", "",
            "| P/C | Worker | Capacita | Batch | Coda | Mmsg/s | Speedup vs Vyukov [IC95%] | vs PaddedFAA | p99 medio (us) |",
            "|---|---:|---:|---:|---|---:|---|---:|---:|"]
    for r in summary:
        text.append(f"| {r['producers']}/{r['consumers']} | {r['workers']} | {r['capacity']} | {r['batch']} "
                    f"| {r['queue']} | {r['mmsg_s']:.3f} "
                    f"| {r['speedup_vs_vyukov']:.2f} [{r['low95']:.2f}, {r['high95']:.2f}] "
                    f"| {r['speedup_vs_padded']:.2f} | {r['mean_run_p99_us']:.2f} |")
    text += ["", "## Interpretazione", "",
             "Le baseline espongono solo Optional: i loro fallimenti non distinguono collisioni e indisponibilita. "
             "Il valore retries=0 delle baseline significa non osservabile, non assenza di CAS falliti. "
             "CAS-bounded distingue RETRY da WAIT; entrambi cedono l'attivazione in questo scheduler, "
             "quindi qui non si misura il risparmio di parcheggi nel runtime di produzione.", "",
             "FAA-cooperative elimina i CAS di prenotazione, ma mantiene contesa sui contatori FAA, "
             "attese sugli slot e dipendenze dagli actor proprietari dei ticket. "
             "Il batch limita i messaggi per attivazione, non prenota un blocco con una sola FAA.", "",
             "Raw: results.csv. Metriche complete: summary.csv. Ambiente e hash: manifest.json. "
             "Le verifiche exact-once sono separate dai tempi riportati."]
    (out / "RESULTS.md").write_text("\n".join(text) + "\n")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mostream-cooperative-matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    panels = sorted({(c[0], c[1], c[3]) for c in cases})
    figure, axes = plt.subplots(len(panels), 1, figsize=(10, 3.5 * len(panels)), squeeze=False)
    for axis, (p, c, capacity) in zip(axes[:, 0], panels):
        configs = sorted({(r["workers"], r["batch"]) for r in summary
                          if (r["producers"], r["consumers"], r["capacity"]) == (p, c, capacity)})
        for offset, name, color in zip((-.22, 0, .22), NAMES[1:], ("#247BA0", "#707070", "#C23B54")):
            selected = [next(r for r in summary if (r["producers"], r["consumers"], r["capacity"],
                        r["workers"], r["batch"], r["queue"]) == (p, c, capacity, w, b, name)) for w, b in configs]
            centers = [r["speedup_vs_vyukov"] for r in selected]
            axis.errorbar([i + offset for i in range(len(configs))], centers,
                          yerr=[[v - r["low95"] for v, r in zip(centers, selected)],
                                [r["high95"] - v for v, r in zip(centers, selected)]],
                          fmt="o", capsize=3, color=color, label=name)
        axis.axhline(1, color="black", linewidth=1, linestyle="--")
        axis.set_xticks(range(len(configs)), [f"W{w} / B{b}" for w, b in configs])
        axis.set_title(f"{p} producer / {c} consumer, capacita {capacity}")
        axis.set_ylabel("Speedup vs Vyukov")
        axis.grid(axis="y", alpha=.2)
        axis.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(out / "comparison.png", dpi=150)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=25000)
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--workers", type=integers, default=[1, 2, 4])
    parser.add_argument("--capacities", type=integers, default=[64, 1024])
    parser.add_argument("--batches", type=integers, default=[1, 8])
    parser.add_argument("--topologies", default="4:4,8:2")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260921)
    parser.add_argument("--output", type=Path, default=HERE / "results")
    args = parser.parse_args()
    if args.messages < 1 or args.repetitions < 2:
        parser.error("positive messages and at least two repetitions required")
    topologies = [tuple(map(int, part.split(":"))) for part in args.topologies.split(",")]
    if any(len(t) != 2 or min(t) < 1 for t in topologies):
        parser.error("topologies must be positive producer:consumer pairs")
    cases = [(p, c, w, cap, batch) for (p, c), w, cap, batch in
             itertools.product(topologies, args.workers, args.capacities, args.batches)]
    if any(w > p + c or cap < 2 or cap & (cap - 1) for p, c, w, cap, _ in cases):
        parser.error("workers must not exceed actors; capacities must be powers of two >= 2")
    args.output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, MODULAR_CACHE_DIR="/tmp/mostream-mojo-cache")
    rng = random.Random(args.seed)
    rows = []
    with tempfile.TemporaryDirectory(prefix="mostream-cooperative-") as temporary:
        binary = Path(temporary) / "compare"
        build = ["mojo", "build", "--Werror", "-O3", "-I", str(ROOT),
                 str(HERE / "compare.mojo"), "-o", str(binary)]
        subprocess.run(build, cwd=ROOT, env=env, check=True)
        # Include a single worker, more waiters than slots, wrap-around, zero
        # messages, imbalanced actors, and one actor per worker.
        checks = [(4, 4, 1, 2, 1), (4, 4, 4, 2, 8),
                  (8, 2, 4, 8, 1), (2, 8, 4, 2, 8), (4, 4, 8, 2, 1)]
        for case in checks:
            for kind in range(4):
                execute(binary, env, kind, 2000, case, True, args.timeout)
                execute(binary, env, kind, 0, case, True, args.timeout)
        print("PASS: 40 exact-once and empty-close cases", flush=True)
        for case in cases:
            for kind in range(4):
                execute(binary, env, kind, args.messages, case, False, args.timeout)
        for rep in range(args.repetitions):
            order = cases.copy()
            rng.shuffle(order)
            for case in order:
                kinds = list(range(4))
                rng.shuffle(kinds)
                for kind in kinds:
                    metrics = execute(binary, env, kind, args.messages, case, False, args.timeout)
                    layout = metrics.pop("layout")
                    rows.append(dict(zip(KEYS, case), messages=args.messages,
                                     rep=rep, kind=kind, queue=NAMES[kind], **metrics))
            print(f"Completed block {rep + 1}/{args.repetitions}", flush=True)
        write_csv(args.output / "results.csv", rows)
        sources = [HERE / "compare.mojo", Path(__file__),
                   ROOT / "Tests/Test_Queue/test_cooperative_faa_queue.mojo"]
        sources += sorted((ROOT / "MoStream").glob("*.mojo"))
        manifest = {"created_utc": datetime.now(timezone.utc).isoformat(),
                    "platform": platform.platform(), "seed": args.seed,
                    "arguments": {key: str(value) if isinstance(value, Path) else value
                                  for key, value in vars(args).items()},
                    "mojo": subprocess.check_output(["mojo", "--version"], text=True).strip(),
                    "lscpu": subprocess.check_output(["lscpu"], text=True),
                    "affinity": sorted(os.sched_getaffinity(0)), "worker_pinning": False,
                    "build_command": build, "exact_checks": 40,
                    "layout_bytes_payload_compact_padded": list(map(int, layout)),
                    "warmups_per_case_variant": 1,
                    "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in sources},
                    "results_sha256": hashlib.sha256((args.output / "results.csv").read_bytes()).hexdigest()}
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    report(rows, cases, args.output, args)
    print(args.output / "RESULTS.md")


if __name__ == "__main__":
    main()
