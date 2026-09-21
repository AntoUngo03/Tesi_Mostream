#!/usr/bin/env python3
"""Generate a PDF report comparing PaddedFAAQueue and PaddedFAASpinQueue."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSVS = [
    Path("/tmp/paddedfaa_spin_sweep.csv"),
    ROOT / "Benchmarks" / "PipelineQueueBenchmark" / "paddedfaa_spin_results.csv",
]
OUTPUT = Path(__file__).with_name("relazione_sleep0.pdf")


def load_rows(csv_paths: list[Path]) -> list[dict[str, float | str | int | bool]]:
    rows: list[dict[str, float | str | int | bool]] = []
    for csv_path in csv_paths:
        if not csv_path.exists():
            continue
        with csv_path.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append({
                    "messages": int(row["messages"]),
                    "producers": int(row["producers"]),
                    "consumers": int(row["consumers"]),
                    "capacity": int(row["capacity"]),
                    "rep": int(row["rep"]),
                    "hybrid_time_ms": float(row["hybrid_time_ms"]),
                    "spin_time_ms": float(row["spin_time_ms"]),
                    "ratio_spin_hybrid": float(row["ratio_spin_hybrid"]),
                    "pct_spin_vs_hybrid": float(row["pct_spin_vs_hybrid"]),
                })
    if not rows:
        raise FileNotFoundError(
            "No benchmark CSV data found in: " + ", ".join(str(p) for p in csv_paths)
        )
    return rows


def summarize(rows: list[dict[str, float | str | int | bool]]) -> list[dict[str, float | str | int]]:
    groups: dict[tuple[int, int, int, int], list[dict[str, float | str | int | bool]]] = defaultdict(list)
    for row in rows:
        key = (int(row["messages"]), int(row["producers"]), int(row["consumers"]), int(row["capacity"]))
        groups[key].append(row)

    summary: list[dict[str, float | str | int]] = []
    for (messages, producers, consumers, capacity), items in sorted(groups.items()):
        hybrid = [float(item["hybrid_time_ms"]) for item in items]
        spin = [float(item["spin_time_ms"]) for item in items]
        ratio = [float(item["ratio_spin_hybrid"]) for item in items]
        delta = [float(item["pct_spin_vs_hybrid"]) for item in items]
        summary.append({
            "label": f"{messages:,}/{producers}P/{consumers}C",
            "messages": messages,
            "producers": producers,
            "consumers": consumers,
            "capacity": capacity,
            "hybrid_mean": sum(hybrid) / len(hybrid),
            "spin_mean": sum(spin) / len(spin),
            "ratio_mean": sum(ratio) / len(ratio),
            "delta_mean": sum(delta) / len(delta),
        })
    return summary


def make_chart_page(summary_rows: list[dict[str, float | str | int]]) -> None:
    labels = [row["label"] for row in summary_rows]
    hybrid = [float(row["hybrid_mean"]) for row in summary_rows]
    spin = [float(row["spin_mean"]) for row in summary_rows]
    deltas = [float(row["delta_mean"]) for row in summary_rows]
    ratios = [float(row["ratio_mean"]) for row in summary_rows]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.8))
    x = range(len(labels))
    width = 0.35
    axes[0].bar([i - width / 2 for i in x], hybrid, width=width, label="PaddedFAAQueue")
    axes[0].bar([i + width / 2 for i in x], spin, width=width, label="PaddedFAASpinQueue")
    axes[0].set_xticks(list(x))
    axes[0].set_xticklabels(labels, rotation=20, ha="right")
    axes[0].set_ylabel("Tempo medio (ms)")
    axes[0].set_title("Confronto tempi medi")
    axes[0].legend()
    axes[0].grid(axis="y", linestyle="--", alpha=0.35)

    axes[1].bar(labels, deltas, color=["tab:green" if v >= 0 else "tab:red" for v in deltas], edgecolor="black")
    axes[1].set_ylabel("Δ% (spin vs hybrid)")
    axes[1].set_title("Variazione percentuale del pure-spin")
    axes[1].axhline(0, color="black", linewidth=1)
    axes[1].grid(axis="y", linestyle="--", alpha=0.35)
    plt.setp(axes[1].get_xticklabels(), rotation=20, ha="right")

    fig.suptitle("PaddedFAAQueue vs PaddedFAASpinQueue: costo di sleep(0.0)", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def make_ratio_page(summary_rows: list[dict[str, float | str | int]]) -> None:
    labels = [row["label"] for row in summary_rows]
    ratios = [float(row["ratio_mean"]) for row in summary_rows]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.plot(labels, ratios, marker="o", color="tab:blue", linewidth=2)
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1)
    ax.set_ylabel("Ratio spin / hybrid")
    ax.set_title("Rapporto tra tempo medio pure-spin e hybrid")
    ax.set_ylim(bottom=0.8, top=max(1.2, max(ratios) * 1.15))
    ax.grid(True, linestyle="--", alpha=0.35)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    fig.tight_layout()
    return fig


def make_table_page(summary_rows: list[dict[str, float | str | int]]) -> None:
    columns = [
        "Config",
        "Hybrid ms",
        "Spin ms",
        "Ratio S/H",
        "Δ%",
    ]
    data = [
        [
            row["label"],
            f"{float(row['hybrid_mean']):.2f}",
            f"{float(row['spin_mean']):.2f}",
            f"{float(row['ratio_mean']):.3f}",
            f"{float(row['delta_mean']):.2f}",
        ]
        for row in summary_rows
    ]

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.axis("off")
    table = ax.table(
        cellText=data,
        colLabels=columns,
        loc="center",
        cellLoc="center",
        colColours=["#d9edf7"] * len(columns),
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.0, 1.7)
    ax.set_title("Sintesi delle medie per configurazione", fontsize=14, fontweight="bold", pad=18)
    fig.tight_layout()
    return fig


def add_narrative_page(pdf: PdfPages) -> None:
    fig = plt.figure(figsize=(11, 8))
    fig.patch.set_facecolor("white")
    fig.text(0.05, 0.92, "Interpretazione dei risultati", fontsize=18, fontweight="bold")
    body = [
        "Il confronto è stato costruito per isolare il costo di sleep(0.0) senza cambiare il resto del runtime.",
        "Le due code condividono lo stesso layout di slot, la stessa capacità, gli stessi ticket FAA e lo stesso ordine di scheduling dei worker. L'unica differenza è la policy di attesa: la variante ibrida fa spin fino a 1024 iterazioni e poi cede la CPU, mentre la pure-spin resta in busy wait continuo.",
        "In pratica, il benchmark misura quanto il yield esplicito stia costando in throughput e latenza sotto contenimento.",
        "I dati mostrano un comportamento non uniforme: per 20k e 100k messaggi con più producer, il pure-spin resta più veloce, mentre per 50k il rapporto si avvicina a 1 e in alcuni casi la variante ibrida può pareggiare o superare il pure-spin.",
        "Questo indica che sleep(0.0) non è necessariamente ""gratis"" ma il suo beneficio dipende dalle condizioni di contesa e dal regime del sistema: in carico medio-alto il pure-spin paga meno overhead di schedulazione; in carico più basso il beneficio del yield può diventare marginale o addirittura controproducente.",
        "La conclusione più prudente è che sleep(0.0) è una policy di backoff utile per contenere il busy-wait, ma il suo costo va misurato empiricamente e non assunto a priori.",
    ]
    y = 0.80
    for paragraph in body:
        fig.text(0.07, y, paragraph, fontsize=11.5, ha="left", va="top", wrap=True)
        y -= 0.12
        if y < 0.08:
            break
    fig.text(0.07, 0.08, "Nota: i grafici sono ottenuti dai dati raccolti con la stessa pipeline, stesso workload e stesso seed di scheduling per ogni esecuzione.", fontsize=10, color="dimgray")
    pdf.savefig(fig)
    plt.close(fig)


def build_report() -> None:
    rows = load_rows(DEFAULT_CSVS)
    summary_rows = summarize(rows)
    with PdfPages(str(OUTPUT)) as pdf:
        fig = make_chart_page(summary_rows)
        pdf.savefig(fig)
        plt.close(fig)

        fig = make_ratio_page(summary_rows)
        pdf.savefig(fig)
        plt.close(fig)

        fig = make_table_page(summary_rows)
        pdf.savefig(fig)
        plt.close(fig)

        add_narrative_page(pdf)

    print(f"Creato {OUTPUT}")


if __name__ == "__main__":
    build_report()
