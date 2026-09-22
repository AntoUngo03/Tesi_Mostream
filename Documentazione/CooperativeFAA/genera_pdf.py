#!/usr/bin/env python3
"""Create the Italian technical report from the completed real-pipeline sweep."""

import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import statistics
import sys

os.environ.setdefault("MPLCONFIGDIR", "/tmp/mostream-cooperative-matplotlib")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DATA = ROOT / "Benchmarks/RealCooperativeQueueBenchmark/results"
PREVIEW = Path("/tmp/mostream-faa-report-preview")
sys.path.insert(0, str(ROOT))
from Benchmarks.CooperativeQueueBenchmark.run import paired_interval

NAMES = ("Vyukov", "PaddedFAA", "PaddedFAA-broadcast", "FAA-cooperative")
SHORT = {"Vyukov": "Vyukov", "PaddedFAA": "PaddedFAA",
         "PaddedFAA-broadcast": "Padded + broadcast", "FAA-cooperative": "FAA cooperativa"}
COLORS = {"Vyukov": "#454E57", "PaddedFAA": "#217C9D",
          "PaddedFAA-broadcast": "#7A8893", "FAA-cooperative": "#B32E4C"}
INK = "#20282F"
MUTED = "#56636C"
WIDTH, HEIGHT = 595.276, 841.89
MARGIN = 43
CONTENT = WIDTH - 2 * MARGIN
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9,
                     "pdf.fonttype": 42, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.labelcolor": INK,
                     "text.color": INK})


def load_data():
    manifest = json.loads((DATA / "manifest.json").read_text())
    raw = DATA / "results.csv"
    if hashlib.sha256(raw.read_bytes()).hexdigest() != manifest["results_sha256"]:
        raise ValueError("Raw CSV does not match the completed campaign")
    for path, digest in manifest["source_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Source changed since the measured campaign: {path}")
    with raw.open(newline="") as file:
        rows = list(csv.DictReader(file))
    args = manifest["arguments"]
    expected = set(itertools.product(args["workers"], args["capacities"], args["batches"],
                                     args["work"], NAMES, range(args["repetitions"])))
    actual = [(int(r["workers"]), int(r["capacity"]), int(r["batch"]), int(r["work"]),
               r["queue"], int(r["rep"])) for r in rows]
    if set(actual) != expected or len(actual) != len(expected):
        raise ValueError("Incomplete or duplicate measurement matrix")
    if args["topology"] != "4:4:4" or args["capacities"] != [1024]:
        raise ValueError("This report layout describes the 4:4:4 / capacity 1024 campaign")
    with (DATA / "summary.csv").open(newline="") as file:
        summary = list(csv.DictReader(file))
    indexed = {}
    for item in summary:
        key = (int(item["workers"]), int(item["batch"]), int(item["work"]), item["queue"])
        if key in indexed:
            raise ValueError("Duplicate summary row")
        indexed[key] = {k: float(v) if k != "queue" else v for k, v in item.items()}
    for w, cap, b, work in itertools.product(args["workers"], args["capacities"], args["batches"], args["work"]):
        grouped = {name: {int(r["rep"]): r for r in rows if
                   (int(r["workers"]), int(r["capacity"]), int(r["batch"]), int(r["work"]), r["queue"])
                   == (w, cap, b, work, name)} for name in NAMES}
        for name in NAMES:
            item = indexed[w, b, work, name]
            for metric in ("runtime_ms", "total_ms", "mmsg_s"):
                mean = statistics.fmean(float(r[metric]) for r in grouped[name].values())
                if not math.isclose(mean, item[metric], rel_tol=1e-12):
                    raise ValueError("Summary does not match raw measurements")
            for reference, keys in (
                ("Vyukov", ("speedup_vs_vyukov", "low95", "high95")),
                ("PaddedFAA", ("speedup_vs_padded", "padded_low95", "padded_high95")),
                ("PaddedFAA-broadcast", ("speedup_vs_control", "control_low95", "control_high95")),
            ):
                ratios = [float(grouped[reference][rep]["runtime_ms"]) / float(r["runtime_ms"])
                          for rep, r in grouped[name].items()]
                for key, value in zip(keys, paired_interval(ratios)):
                    if not math.isclose(value, item[key], rel_tol=1e-12):
                        raise ValueError("Confidence interval does not match the raw CSV")
    return manifest, indexed


class Page:
    def __init__(self, number, title, subtitle=""):
        self.number = number
        self.fig = plt.figure(figsize=(WIDTH / 72, HEIGHT / 72), dpi=110)
        self.fig.canvas.draw()
        self.renderer = self.fig.canvas.get_renderer()
        self.y = 757
        self.markdown = [f"## {number}. {title}", ""]
        self.at(MARGIN, 803, "MOSTREAM  /  RELAZIONE TECNICA", 8, color=MUTED, weight="bold")
        self.at(MARGIN, self.y, title, 23, weight="bold")
        self.y -= 38
        if subtitle:
            self.paragraph(subtitle, size=9, color=MUTED)
        self.fig.add_artist(Line2D([MARGIN / WIDTH, (WIDTH - MARGIN) / WIDTH],
                                  [45 / HEIGHT, 45 / HEIGHT], color="#D1D8DD", linewidth=.6))
        self.at(MARGIN, 31, "FAA cooperativa | campagna locale 21 settembre 2026", 7.5, color=MUTED)
        self.at(WIDTH - MARGIN, 31, f"{number} / 8", 7.5, color=MUTED, ha="right")

    def at(self, x, y, text, size=10, color=INK, weight="normal", ha="left", family="DejaVu Sans"):
        return self.fig.text(x / WIDTH, y / HEIGHT, text, fontsize=size, color=color,
                             weight=weight, ha=ha, va="top", family=family)

    def wrap(self, text, width, size, weight="normal", family="DejaVu Sans"):
        font = FontProperties(family=family, size=size, weight=weight)
        limit = width * self.fig.dpi / 72
        lines = []
        for paragraph in text.split("\n"):
            current = ""
            for word in paragraph.split():
                candidate = f"{current} {word}".strip()
                measured = self.renderer.get_text_width_height_descent(candidate, font, False)[0]
                if measured > limit and current:
                    lines.append(current)
                    current = word
                else:
                    current = candidate
            lines.append(current)
        return lines

    def paragraph(self, text, size=10.2, color=INK, weight="normal", gap=10):
        lines = self.wrap(text, CONTENT, size, weight)
        for line in lines:
            self.at(MARGIN, self.y, line, size, color, weight)
            self.y -= size * 1.45
        self.y -= gap
        self.markdown.extend([text, ""])
        self.check()

    def heading(self, text):
        self.y -= 6
        self.paragraph(text, size=12, weight="bold", gap=6)

    def table(self, headers, rows, widths, size=8.7):
        widths = [value * CONTENT / sum(widths) for value in widths]
        self.markdown += ["| " + " | ".join(headers) + " |",
                          "| " + " | ".join("---" for _ in headers) + " |"]
        self.markdown += ["| " + " | ".join(map(str, row)) + " |" for row in rows]
        self.markdown.append("")
        for index, row in enumerate([headers, *rows]):
            weight = "bold" if index == 0 else "normal"
            wrapped = [self.wrap(str(value), width - 12, size, weight)
                       for value, width in zip(row, widths)]
            height = max(map(len, wrapped)) * size * 1.35 + 12
            shade = "#E8EEF1" if index == 0 else ("#F5F7F8" if index % 2 else "white")
            self.fig.add_artist(Rectangle((MARGIN / WIDTH, (self.y - height) / HEIGHT),
                                          CONTENT / WIDTH, height / HEIGHT, facecolor=shade,
                                          edgecolor="none", zorder=0))
            x = MARGIN
            for lines, width in zip(wrapped, widths):
                for n, line in enumerate(lines):
                    self.at(x + 6, self.y - 6 - n * size * 1.35, line, size, weight=weight)
                x += width
            self.y -= height
        self.y -= 14
        self.check()

    def chart(self, height, bottom=34):
        top = self.y - 8
        axis = self.fig.add_axes([(MARGIN + 34) / WIDTH, (top - height + bottom) / HEIGHT,
                                 (CONTENT - 46) / WIDTH, (height - bottom - 12) / HEIGHT])
        self.y = top - height - 8
        self.check()
        return axis

    def check(self):
        if self.y < 61:
            raise ValueError(f"Content exceeds page {self.number}: y={self.y}")

    def save(self, pdf):
        self.fig.canvas.draw()
        renderer = self.fig.canvas.get_renderer()
        for text in self.fig.texts:
            box = text.get_window_extent(renderer)
            if box.x0 < 0 or box.x1 > self.fig.bbox.width + 1 or box.y0 < 0 or box.y1 > self.fig.bbox.height + 1:
                raise ValueError(f"Text outside page {self.number}: {text.get_text()}")
        pdf.savefig(self.fig)
        self.fig.savefig(PREVIEW / f"page-{self.number:02}.png", dpi=120)
        plt.close(self.fig)
        return "\n".join(self.markdown)


def main():
    manifest, stats = load_data()
    PREVIEW.mkdir(parents=True, exist_ok=True)
    def row(name, workers=4, batch=8, work=0):
        return stats[workers, batch, work, name]
    def ci(item, name="vyukov"):
        keys = {"vyukov": ("speedup_vs_vyukov", "low95", "high95"),
                "padded": ("speedup_vs_padded", "padded_low95", "padded_high95"),
                "control": ("speedup_vs_control", "control_low95", "control_high95")}[name]
        return f"{item[keys[0]]:.2f} [{item[keys[1]]:.2f}, {item[keys[2]]:.2f}]"
    faa, cas, padded = row("FAA-cooperative"), row("Vyukov"), row("PaddedFAA")
    gain = (faa["speedup_vs_vyukov"] - 1) * 100
    gain_padded = (faa["speedup_vs_padded"] - 1) * 100
    total_reduction = (1 - faa["total_ms"] / cas["total_ms"]) * 100
    document = ["# FAA cooperativa: confronto e relazione tecnica", ""]
    output = HERE / "relazione_faa_cooperativa.pdf"
    with PdfPages(output, metadata={"Title": "FAA cooperativa: confronto nella pipeline MoStream",
                                   "Author": "MoStream", "Subject": "Misure, metodologia e valutazione condizionata delle prestazioni"}) as pdf:
        p = Page(1, "FAA cooperativa", "Confronto nella pipeline MoStream: risultati e valutazione tecnica")
        p.heading("Conclusione principale")
        p.paragraph("La nuova FAA migliora il throughput in un regime preciso: quattro worker, "
                    "batch 8 e stage con poco o moderato calcolo. Non risulta una sostituzione "
                    "universalmente migliore di Vyukov o della PaddedFAA normale.")
        p.table(["Caso favorevole", "Guadagno misurato"], [
            ["Throughput contro Vyukov", f"+{gain:.0f}%  |  rapporto {ci(faa)}"],
            ["Throughput contro PaddedFAA", f"+{gain_padded:.0f}%  |  rapporto {ci(faa, 'padded')}"],
            ["Tempo totale della pipeline", f"-{total_reduction:.1f}% contro Vyukov, incluso avvio e distruzione"],
        ], [1, 1.35])
        p.paragraph("Configurazione: 4 source, 4 transform, 4 sink; 4 worker su core fisici "
                    "distinti; capacit\u00e0 1024; batch 8; work=0; 40.000 messaggi totali.", size=9, color=MUTED)
        ax = p.chart(210)
        values = [row(name)["mmsg_s"] for name in NAMES]
        bars = ax.barh(range(4), values, color=[COLORS[name] for name in NAMES], height=.58)
        ax.set_yticks(range(4), ["Vyukov", "PaddedFAA", "Padded + broadcast", "FAA cooperativa"], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlim(0, max(values) * 1.18)
        ax.set_xlabel("Throughput di esecuzione (milioni di messaggi/s)", fontsize=8)
        ax.bar_label(bars, fmt="%.3f", padding=5, fontsize=9)
        ax.grid(axis="x", alpha=.15)
        # Give long row labels room without shrinking the body text.
        position = ax.get_position()
        ax.set_position([position.x0 + .13, position.y0, position.width - .13, position.height])
        p.paragraph("Con batch 1 e quattro worker non emerge un vantaggio convincente. "
                    "Con un solo worker la FAA perde in tutte le configurazioni misurate. "
                    "Aumentando il calcolo per messaggio, il vantaggio con batch 8 scende a circa il 7%.")
        p.paragraph("Base empirica: 336 misure temporizzate, sette ripetizioni per configurazione "
                    "e variante, pi\u00f9 60 casi di verifica della pipeline. I risultati riguardano "
                    "questa implementazione, macchina e carico sintetico.", size=9, color=MUTED)
        document.append(p.save(pdf))

        p = Page(2, "Le quattro varianti", "Il confronto riguarda sia la coda sia la sua integrazione nel runtime")
        p.table(["Variante", "Prenotazione cooperativa", "Risveglio"], [
            ["Vyukov", "CAS tramite try_push / try_pop", "Wait queue originali"],
            ["PaddedFAA", "CAS nei try; slot padded", "Wait queue originali"],
            ["PaddedFAA-broadcast", "Come PaddedFAA normale", "Scansione degli actor bloccati"],
            ["FAA cooperativa", "FAA una volta, ticket mantenuto", "Stessa scansione broadcast"],
        ], [1.1, 1.4, 1.3])
        p.heading("Che cosa introduce la FAA cooperativa")
        p.paragraph("Ogni actor conserva una PushOperation o PopOperation. Il primo tentativo "
                    "prenota un ticket con fetch_add. Se lo slot non \u00e8 pronto, l'actor si "
                    "parcheggia mantenendo ticket e payload. Alla riattivazione riprova lo "
                    "stesso ticket, senza spin interno e senza prenotarne un altro.")
        p.paragraph("Il protocollo sequence conserva acquire/release sullo slot. La prenotazione "
                    "non pu\u00f2 essere abbandonata: un proprietario fermo pu\u00f2 impedire il riuso "
                    "della sua posizione. Questa variante non fornisce una garanzia lock-free "
                    "e non supporta cancellazione o overflow dei contatori UInt64.")
        p.heading("Perch\u00e9 cambia il risveglio")
        p.paragraph("Con ticket persistenti, risvegliare un waiter arbitrario pu\u00f2 lasciare "
                    "parcheggiato il proprietario dello slot pronto. La prima integrazione "
                    "risveglia quindi tutti gli actor bloccati nella direzione interessata. "
                    "Registrazione dello stato BLOCKED, fence e ricontrollo evitano di perdere "
                    "un evento durante il parcheggio.")
        p.paragraph("PaddedFAA-broadcast usa la stessa politica di notifica della FAA, ma "
                    "mantiene i CAS della coda originale. \u00c8 il controllo necessario per "
                    "non attribuire alla sola FAA un effetto dovuto ai risvegli.")
        p.heading("Chiusura e durata di vita")
        p.paragraph("Ogni producer segnala la fine dopo l'ultimo push completato. Le letture "
                    "prenotate oltre l'ultimo messaggio diventano terminali solo dopo la "
                    "chiusura definitiva. I comunicatori restano vivi fino al join di tutti "
                    "i worker, comprese le notifiche finali. La ready queue resta MPMC CAS "
                    "in tutte le varianti; il backend predefinito non viene sostituito.")
        document.append(p.save(pdf))

        p = Page(3, "Metodo sperimentale", "Campagna locale del 21 settembre 2026; comandi e hash conservati nel manifest")
        p.table(["Parametro", "Valore"], [
            ["CPU", "Intel Xeon Gold 5512U; 28 core / 56 thread; un socket"],
            ["Compilazione", manifest["mojo"] + "; --Werror -O3"],
            ["Pipeline", "4 source -> 4 transform -> 4 sink; 10.000 messaggi/source"],
            ["Worker e pinning", "1 o 4 worker; CPU 0 oppure CPU 0,1,2,3, core fisici distinti"],
            ["Capacit\u00e0 e batch", "1024 slot; batch 1 oppure 8"],
            ["Calcolo nel transform", "work=0, 256, 2048 iterazioni dipendenti di mixing UInt64"],
            ["Campionamento", "12 configurazioni x 4 varianti x 7 ripetizioni = 336 misure"],
        ], [1, 2.6], size=9)
        p.heading("Due tempi distinti")
        p.paragraph("Runtime: durata di Scheduler.start, inclusi dispatch/join, ready queue, "
                    "calcolo, prenotazioni, parcheggio, ricontrolli e risvegli. Il throughput "
                    "usa questo intervallo e conta i messaggi arrivati al sink.")
        p.paragraph("Totale: intera chiamata run_cooperative, inclusa costruzione e distruzione "
                    "di comunicatori e grandi code interne. La costruzione dell'oggetto "
                    "Pipeline, Python e preparazione dei dati precedono entrambi i timer. "
                    "Le wait queue originali sono allocate anche nelle varianti broadcast.")
        p.heading("Protocollo e controlli")
        p.paragraph("Un warm-up per caso/variante, escluso dalle statistiche. Sette blocchi "
                    "con ordine randomizzato dei casi e delle varianti. Timeout e fallimenti "
                    "interrompono la campagna; nessun outlier viene rimosso. Il checksum "
                    "del calcolo viene verificato fuori dal tempo misurato.")
        p.paragraph("Prima delle misure: 60 casi di verifica, inclusi input vuoto, due slot, "
                    "pi\u00f9 waiter che slot e transform che scarta tutti o parte dei messaggi. "
                    "La verifica exact-once per ID \u00e8 separata dalla temporizzazione. In "
                    "ogni misura si controllano conteggio, checksum, campioni di latenza "
                    "e assenza di ticket o payload pendenti alla fine.")
        document.append(p.save(pdf))

        p = Page(4, "Throughput con 4 worker", "Capacit\u00e0 1024; milioni di messaggi completati al secondo")
        table_rows = []
        for batch, work in itertools.product((1, 8), (0, 256, 2048)):
            table_rows.append([str(batch), str(work), *[f"{row(n, batch=batch, work=work)['mmsg_s']:.3f}" for n in NAMES]])
        p.table(["Batch", "Work", "Vyukov", "PaddedFAA", "Padded + broadcast", "FAA coop."],
                table_rows, [.6, .8, 1, 1, 1.2, 1], size=8.5)
        ax = p.chart(248, bottom=45)
        configs = list(itertools.product((1, 8), (0, 256, 2048)))
        for offset, ref, keys, color in (
            (-.17, "vs Vyukov", ("speedup_vs_vyukov", "low95", "high95"), COLORS["Vyukov"]),
            (0, "vs PaddedFAA", ("speedup_vs_padded", "padded_low95", "padded_high95"), COLORS["PaddedFAA"]),
            (.17, "vs Padded + broadcast", ("speedup_vs_control", "control_low95", "control_high95"), COLORS["FAA-cooperative"]),
        ):
            values = [row("FAA-cooperative", batch=b, work=w) for b, w in configs]
            centers = [v[keys[0]] for v in values]
            ax.errorbar([i + offset for i in range(6)], centers,
                        yerr=[[v[keys[0]] - v[keys[1]] for v in values],
                              [v[keys[2]] - v[keys[0]] for v in values]],
                        fmt="o", color=color, label=ref, capsize=3, markersize=4)
        ax.axhline(1, color="#777777", linestyle="--", linewidth=.8)
        ax.set_xticks(range(6), [f"B{b}\nW{w}" for b, w in configs], fontsize=8)
        ax.set_ylabel("Rapporto a favore della FAA", fontsize=9)
        ax.legend(fontsize=7.5, loc="upper left")
        ax.grid(axis="y", alpha=.15)
        p.paragraph("Nel grafico B indica il batch e W il work del transform, non il numero "
                    "di worker (sempre quattro). Sopra 1 vince la FAA; le barre indicano IC95%.", size=9, color=MUTED)
        p.paragraph("Con batch 1 le differenze sono piccole: non c'\u00e8 evidenza di una "
                    "superiorit\u00e0 generale della FAA. Con batch 8, work=0 e work=256, "
                    "il vantaggio resta anche rispetto al controllo con identici risvegli. "
                    "Quando work=2048, il guadagno si riduce sensibilmente.")
        p.paragraph("Gli intervalli sono bootstrap percentile su rapporti temporali accoppiati "
                    "in log, con 4.000 ricampionamenti. Sono intervalli individuali, senza "
                    "correzione per confronti multipli; sette ripetizioni rendono questa "
                    "una campagna esplorativa, non una prova universale.", size=9, color=MUTED)
        document.append(p.save(pdf))

        p = Page(5, "Batch e parallelismo", "Un confronto favorevole fra code non implica scalabilit\u00e0 lineare del runtime")
        p.table(["Worker", "Batch", "Work", "Vyukov Mmsg/s", "FAA Mmsg/s", "FAA / Vyukov"], [
            [str(w), str(b), str(work), f"{row('Vyukov',w,b,work)['mmsg_s']:.3f}",
             f"{row('FAA-cooperative',w,b,work)['mmsg_s']:.3f}",
             f"{row('FAA-cooperative',w,b,work)['speedup_vs_vyukov']:.2f}"]
            for w in (1, 4) for b in (1, 8) for work in (0, 2048)
        ], [.6, .6, .8, 1.1, 1, 1], size=8.7)
        p.heading("Un solo worker")
        p.paragraph("Con un solo worker non esiste competizione simultanea fra worker "
                    "sui contatori della coda. La FAA aggiunge gestione dello stato persistente "
                    "senza eliminare CAS falliti fra worker. Nei sei casi misurati risulta "
                    "pi\u00f9 lenta di Vyukov e PaddedFAA, con penalit\u00e0 pi\u00f9 visibile nello stage leggero.")
        p.heading("Il batch conta pi\u00f9 del semplice cambio di coda")
        p.paragraph("Con quattro worker e work=0, portare il batch da 1 a 8 aumenta il "
                    "throughput anche per Vyukov (da 0.288 a 0.715 Mmsg/s). Con FAA si passa "
                    "da 0.285 a 1.094 Mmsg/s. Il batch ammortizza attivazioni e notifiche; "
                    "non raggruppa pi\u00f9 prenotazioni in una sola FAA.")
        p.heading("Pi\u00f9 worker non sono sempre meglio")
        p.paragraph("Con batch 1 e stage leggero, un worker supera quattro worker in entrambe "
                    "le code. Aumentare il parallelismo introduce costi condivisi del runtime. "
                    "La ready queue, gli stati degli actor e le notifiche sono candidati da "
                    "profilare: i tempi da soli non consentono di attribuire il costo a "
                    "una singola struttura o istruzione.")
        p.paragraph("Quando il calcolo dello stage cresce, una frazione maggiore del lavoro "
                    "non dipende dalla coda: il margine osservato fra le code si restringe. "
                    "Per scegliere la configurazione conviene confrontare insieme numero "
                    "di worker, batch, throughput e latenza.")
        document.append(p.save(pdf))

        p = Page(6, "Tempo totale e latenza", "Quattro worker, batch 8: distinguere esecuzione e costo della chiamata completa")
        ax = p.chart(220, bottom=45)
        runtime = [row(name)["runtime_ms"] for name in NAMES]
        other = [row(name)["total_ms"] - row(name)["runtime_ms"] for name in NAMES]
        ax.bar(range(4), runtime, label="Esecuzione scheduler", color="#217C9D", width=.62)
        ax.bar(range(4), other, bottom=runtime, label="Resto di run_cooperative", color="#CCD5DC", width=.62)
        ax.set_xticks(range(4), ["Vyukov", "PaddedFAA", "Padded +\nbroadcast", "FAA coop."], fontsize=8)
        ax.set_ylabel("Tempo medio (ms)")
        ax.legend(fontsize=8, loc="upper right")
        ax.set_ylim(0, max(row(name)["total_ms"] for name in NAMES) * 1.30)
        ax.grid(axis="y", alpha=.15)
        p.table(["Work", "Totale Vyukov ms", "Totale FAA ms", "p99 Vyukov us", "p99 FAA us"], [
            [str(work), f"{row('Vyukov',work=work)['total_ms']:.2f}",
             f"{row('FAA-cooperative',work=work)['total_ms']:.2f}",
             f"{row('Vyukov',work=work)['mean_run_p99_us']:.0f}",
             f"{row('FAA-cooperative',work=work)['mean_run_p99_us']:.0f}"] for work in (0, 256, 2048)
        ], [.6, 1.15, 1.15, 1.1, 1.1])
        p.paragraph(f"Con work=0, la sola esecuzione scende da {cas['runtime_ms']:.2f} a "
                    f"{faa['runtime_ms']:.2f} ms. La chiamata completa passa da {cas['total_ms']:.2f} "
                    f"a {faa['total_ms']:.2f} ms: riduzione del {total_reduction:.1f}%. Il +{gain:.0f}% "
                    "di throughput non significa +53% di prestazione complessiva per una pipeline breve.")
        p.heading("Come leggere il p99")
        p.paragraph("La latenza parte dalla creazione del messaggio nella source e termina "
                    "nel sink, includendo accodamento, scheduling e calcolo. Si campiona "
                    "un ID ogni 256: 157 campioni per esecuzione da 40.000 messaggi. Il valore "
                    "riportato \u00e8 la media dei p99 delle sette esecuzioni, non il p99 dell'intero flusso.")
        p.paragraph("Con cos\u00ec pochi campioni per esecuzione, la coda estrema \u00e8 descritta da "
                    "pochissime osservazioni. Questi p99 sono indicativi; non dimostrano "
                    "un vincolo di latenza o un obiettivo di servizio. Il batch va valutato "
                    "anche per la latenza, non soltanto per il throughput.", size=9, color=MUTED)
        document.append(p.save(pdf))

        p = Page(7, "Valutazione tecnica", "Relazione sulle condizioni in cui la nuova variante offre un vantaggio")
        p.heading("Risultato sostenuto dalle misure")
        p.paragraph("La FAA cooperativa \u00e8 una candidata preferibile quando la pipeline usa "
                    "pi\u00f9 worker, il batch ammortizza il costo dello scheduler e il lavoro "
                    "per messaggio \u00e8 limitato. Nella configurazione a quattro worker e batch 8 "
                    "il vantaggio sul throughput \u00e8 ripetuto in tutti i tre carichi, ma passa "
                    "da circa il 53% al 7% rispetto a Vyukov.")
        p.heading("Interpretazione del vantaggio")
        p.paragraph("Il mantenimento del ticket elimina i tentativi CAS ripetuti per "
                    "assegnare una posizione e distribuisce l'attesa sugli slot prenotati. "
                    "Il controllo PaddedFAA-broadcast mantiene il medesimo protocollo di "
                    "risveglio: con work=0 e batch 8 la FAA conserva un rapporto di circa "
                    "1.50 rispetto a quel controllo. Il vantaggio non \u00e8 quindi spiegato "
                    "soltanto dal passaggio dalle wait queue al broadcast.")
        p.paragraph("Resta un'interpretazione algoritmica, non una misura diretta del numero "
                    "di CAS falliti o dei trasferimenti di cache line. Non sono stati raccolti "
                    "contatori hardware, consumo energetico o un profilo dettagliato. "
                    "FAA conserva contesa sui contatori globali e dipendenze dai proprietari "
                    "dei ticket; il broadcast aggiunge scansioni e possibili risvegli superflui.")
        p.heading("Quando non sceglierla")
        p.paragraph("Non emerge un motivo prestazionale per preferirla con un solo worker "
                    "o con batch 1 in questa pipeline. Non va scelta come sostituzione "
                    "trasparente se il sistema richiede cancellazione di operazioni prenotate "
                    "o garanzie lock-free. Il backend predefinito resta Vyukov.")
        p.heading("Portata della conclusione")
        p.paragraph("La campagna riguarda un socket, una capacit\u00e0, una topologia e un "
                    "transform sintetico. Non stabilisce una classifica contro Rigtorp, "
                    "SCQ, NBLFQ, wCQ o tutte le altre code presenti nel repository: qui non "
                    "sono state misurate nello stesso runtime e con lo stesso protocollo.")
        p.paragraph("Il circa 2x osservato nel precedente scheduler round-robin non si "
                    "trasferisce automaticamente al runtime reale. I due esperimenti hanno "
                    "topologia e costi diversi. La raccomandazione \u00e8 sperimentare la FAA con "
                    "batch configurabile sul carico applicativo effettivo, mantenendo la "
                    "baseline e verificando throughput, latenza e durata totale.")
        document.append(p.save(pdf))

        p = Page(8, "Dati e riproducibilit\u00e0", "Rapporti di tempo accoppiati: sopra 1 vince FAA-cooperative")
        p.table(["Worker", "Batch", "Work", "vs Vyukov [IC95%]", "vs PaddedFAA [IC95%]"], [
            [str(w), str(b), str(work), ci(row("FAA-cooperative", w, b, work)),
             ci(row("FAA-cooperative", w, b, work), "padded")]
            for w, b, work in itertools.product((1, 4), (1, 8), (0, 256, 2048))
        ], [.65, .6, .8, 2, 2], size=8.2)
        p.heading("Fonti interne")
        p.paragraph("Benchmarks/RealCooperativeQueueBenchmark/results/ contiene results.csv "
                    "(misure grezze), summary.csv (aggregati), manifest.json (ambiente, "
                    "comandi, parametri e hash) e RESULTS.md (tabella completa). Il carico "
                    "e le verifiche sono in Tests/test_cooperative_pipeline.mojo.", size=9)
        p.paragraph("Questo PDF verifica l'hash del CSV e dei sorgenti, la completezza "
                    "della matrice e ricalcola medie e intervalli dai dati grezzi prima "
                    "di generare le pagine. Non unisce i dati del vecchio microbenchmark.", size=9)
        p.heading("Comandi dalla radice del repository")
        p.paragraph("python3 Benchmarks/RealCooperativeQueueBenchmark/run.py\n"
                    "python3 Documentazione/CooperativeFAA/genera_pdf.py", size=8.7)
        p.paragraph("Per nuove campagne usare --output con una directory distinta. "
                    "Sono richiesti Mojo 1.0, Python e matplotlib. Per riprodurre esattamente "
                    "questa relazione occorrono i sorgenti identificati dal manifest.", size=9)
        p.paragraph("SHA256 di results.csv:\n" + manifest["results_sha256"][:32] + "\n" + manifest["results_sha256"][32:],
                    size=8, color=MUTED)
        document.append(p.save(pdf))
    (HERE / "relazione_faa_cooperativa.md").write_text("\n\n".join(document) + "\n")
    raw_pdf = output.read_bytes()
    if not raw_pdf.startswith(b"%PDF-") or b"/Count 8" not in raw_pdf:
        raise ValueError("Invalid PDF page tree")
    print(f"PASS: validated 336 samples and confidence intervals; 8 PDF pages: {output}")
    print(f"Previews: {PREVIEW}")


if __name__ == "__main__":
    main()
