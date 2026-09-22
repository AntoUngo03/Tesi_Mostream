#!/usr/bin/env python3
"""Generate the point-4 PDF from validated paired measurements."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import textwrap

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "Benchmarks/PipelineQueueBenchmark"
sys.path.insert(0, str(BENCH))
from run_benchmarks import geometric_speedup_stats

OUTPUT = Path(__file__).with_name("relazione_sleep0.pdf")


def load_rows(path):
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    seen = set()
    for row in rows:
        for key in ("messages", "producers", "consumers", "capacity", "rep"):
            row[key] = int(row[key])
        key = tuple(row[k] for k in ("messages", "producers", "consumers", "capacity", "pinning", "rep"))
        if key in seen:
            raise ValueError(f"Duplicate pair: {key}")
        seen.add(key)
        for name in ("hybrid", "spin"):
            if row[f"{name}_valid"].lower() != "true":
                raise ValueError(f"Invalid observation: {key}")
            value = float(row[f"{name}_time_ms"])
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"Invalid time: {key}")
            row[f"{name}_time_ms"] = value
    return rows


def summarize(rows):
    groups = {}
    for row in rows:
        key = tuple(row[k] for k in ("messages", "capacity", "producers", "consumers", "pinning"))
        groups.setdefault(key, []).append(row)
    result = []
    for (messages, capacity, producers, consumers, pinning), items in sorted(groups.items()):
        if len(items) < 10:
            raise ValueError("At least 10 paired repetitions are required")
        ratios = [r["spin_time_ms"] / r["hybrid_time_ms"] for r in items]
        stats = geometric_speedup_stats(ratios)
        low, high = stats["ci95_lower"], stats["ci95_upper"]
        result.append(dict(messages=messages, capacity=capacity, producers=producers,
                           consumers=consumers, pinning=pinning, n=len(items),
                           hybrid_ms=statistics.mean(r["hybrid_time_ms"] for r in items),
                           spin_ms=statistics.mean(r["spin_time_ms"] for r in items),
                           ratio=stats["geomean"], low=low, high=high,
                           verdict="Spin" if high < 1 else "Ibrida" if low > 1 else "Inconclusivo"))
    for messages, capacity in {(r["messages"], r["capacity"]) for r in result}:
        actual = {(r["producers"], r["consumers"], r["pinning"]) for r in result
                  if r["messages"] == messages and r["capacity"] == capacity}
        expected = {(p, c, pin) for p, c in [(2, 2), (4, 2), (8, 2), (8, 8)] for pin in ("off", "on")}
        if actual != expected:
            raise ValueError(f"Incomplete point-4 matrix: {actual}")
    if not result:
        raise ValueError("Empty campaign")
    return result


def text_page(pdf, title, paragraphs):
    fig = plt.figure(figsize=(11.7, 8.3))
    fig.text(.07, .92, title, fontsize=19, weight="bold")
    y = .84
    for paragraph in paragraphs:
        lines = textwrap.wrap(paragraph, width=112)
        fig.text(.07, y, "\n".join(lines), fontsize=11, va="top", linespacing=1.5)
        y -= len(lines) * .029 + .03
    pdf.savefig(fig)
    plt.close(fig)


def build_report():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=BENCH / "paddedfaa_point4.csv")
    args = parser.parse_args()
    rows = load_rows(args.csv)
    summary = summarize(rows)
    manifest = json.loads(args.csv.with_suffix(".manifest.json").read_text())
    if manifest["status"] != "complete" or hashlib.sha256(args.csv.read_bytes()).hexdigest() != manifest["csv_sha256"]:
        raise ValueError("Incomplete campaign or mismatched CSV hash")
    with args.csv.with_suffix(".summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)
    with PdfPages(OUTPUT) as pdf:
        text_page(pdf, "PaddedFAA: spin oppure sleep(0.0)?", [
            "Punto 4: PaddedFAAQueue (spin fino a 1024 iterazioni, poi sleep(0.0)) contro "
            "PaddedFAASpinQueue (attesa attiva continua). Misuriamo la policy completa, incluso "
            "il contatore di spin, non la latenza isolata della chiamata sleep.",
            f"{len(rows)} coppie misurate; due coppie di warm-up escluse per configurazione. "
            "Stesso carico e seed di avvio dei worker; ordine H/S alternato fra ripetizioni; "
            "configurazioni mescolate in ogni blocco. Conteggio e checksum verificati. Nessun outlier rimosso.",
            "Il carico e' espresso in messaggi PER produttore: interi senza lavoro applicativo. "
            "Il tempo comprende avvio e attesa dei TaskGroup. Aumentando P aumenta anche il lavoro "
            "totale: il confronto fra policy va fatto entro la stessa configurazione.",
            f"OFF: affinita' ereditata ({len(manifest['affinity_off'])} CPU logiche). ON: taskset sulle CPU "
            f"{manifest['affinity_on']}. E' affinita' del processo, non un core dedicato per worker. "
            "I worker possono migrare all'interno dell'insieme ammesso.",
            "R = exp(media(log(T_spin / T_ibrida))) sulle coppie. IC95% = exp(media(log R_i) +/- "
            "t_(n-1,0.975) * s(log R_i)/sqrt(n)), come nella campagna NBLFQ. R < 1 favorisce spin; "
            "R > 1 favorisce l'ibrida. Se l'intervallo attraversa 1, il risultato e' inconclusivo: "
            "non prova equivalenza. IC individuali senza correzione per confronti multipli.",
            f"Esecuzione UTC: {manifest['started_utc']}. {manifest['compiler']}. "
            "Hash di sorgenti e binario, topologia CPU e parametri sono conservati nel manifest accanto al CSV.",
        ])
        for messages, capacity in sorted({(r['messages'], r['capacity']) for r in summary}):
            selected = [r for r in summary if (r['messages'], r['capacity']) == (messages, capacity)]
            labels = [f"{r['producers']}P/{r['consumers']}C  {r['pinning'].upper()}" for r in selected]
            fig, ax = plt.subplots(figsize=(11.7, 8.3))
            for i, r in enumerate(selected):
                ax.errorbar(r['ratio'], i, xerr=[[r['ratio']-r['low']], [r['high']-r['ratio']]],
                            fmt='o', capsize=5, color={'Spin': '#16855b', 'Ibrida': '#b33f42', 'Inconclusivo': '#666666'}[r['verdict']])
            ax.axvline(1, color='black', linestyle='--')
            ax.set_yticks(range(len(labels)), labels)
            ax.invert_yaxis()
            ax.set_xlabel("Rapporto geometrico T_spin / T_ibrida, IC95% accoppiato")
            ax.set_title(f"{messages:,} messaggi/produttore; capacita' {capacity}\nSinistra di 1: spin piu' veloce; destra: ibrida piu' veloce")
            ax.grid(axis='x', alpha=.25)
            fig.tight_layout(pad=3)
            pdf.savefig(fig)
            plt.close(fig)
            fig, ax = plt.subplots(figsize=(11.7, 8.3))
            ax.axis('off')
            data = [[label, r['n'], f"{r['hybrid_ms']:.3f}", f"{r['spin_ms']:.3f}",
                     f"{r['ratio']:.3f}", f"[{r['low']:.3f}, {r['high']:.3f}]", r['verdict']]
                    for label, r in zip(labels, selected)]
            table = ax.table(cellText=data, colLabels=['Config', 'n', 'Ibrida ms', 'Spin ms', 'R S/H', 'IC95%', 'Esito'],
                             colWidths=[.17,.05,.13,.13,.10,.22,.20], loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 2.2)
            ax.set_title(f"Tempi medi e rapporto accoppiato: {messages:,} messaggi/P, capacita' {capacity}", pad=20)
            fig.text(.08,.16,"Le medie dei tempi sono aritmetiche; R e' la media geometrica dei rapporti per coppia.",fontsize=10)
            pdf.savefig(fig)
            plt.close(fig)
        counts = {v: sum(r['verdict'] == v for r in summary) for v in ['Spin', 'Ibrida', 'Inconclusivo']}
        text_page(pdf, "Conclusioni e limiti", [
            f"IC95% individuali: {counts['Spin']} configurazioni favoriscono spin, "
            f"{counts['Ibrida']} favoriscono l'ibrida, {counts['Inconclusivo']} sono inconclusive. "
            "La tabella identifica i singoli regimi: la media da sola non basta per dichiarare una vincitrice.",
            "Il risultato vale per questo hardware, runtime, capacita' e carico. Non autorizza "
            "una sostituzione universale della PaddedFAAQueue: consumo CPU, energia, latenze "
            "e pipeline con lavoro applicativo non sono misurati qui.",
            "Prova preliminare con taskset 0-7: 8P/2C non ha completato il warm-up entro 120 secondi "
            "(seed 869193496018642825). La causa non e' stata diagnosticata. Il timeout non entra "
            "nelle statistiche; la campagna completa usa 16 CPU ammesse, almeno quante i task applicativi.",
            "Il punto 4 e' coperto per la matrice OFF/ON con taskset descritta nel metodo. "
            "Se il requisito del professore e' un core dedicato a ciascun worker, resta necessaria "
            "una campagna con affinita' individuale e numero sufficiente di core fisici.",
            "Questa relazione sostituisce l'analisi esplorativa. I vecchi dati non sono mescolati "
            "ai nuovi: avevano poche ripetizioni, ordine H/S fisso e seed di avvio differenti. "
            "Ora il checksum e' quello realmente osservato e il rapporto e' ricalcolato dai tempi. "
            "I file temporanei /tmp non alimentano il report.",
        ])
    print(f"Creato {OUTPUT}")
    for row in summary:
        print(row)


if __name__ == "__main__":
    build_report()
