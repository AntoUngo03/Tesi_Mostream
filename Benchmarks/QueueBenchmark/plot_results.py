#!/usr/bin/env python3
"""Generate dependency-free SVG plots for the bounded queue benchmark.

The CSVs produced by run_suite.py contain aggregate means and 95% confidence
interval half-widths, but not the raw samples.  The plots therefore show
mean +/- CI95; they deliberately do not attempt boxplots or distributional
claims.
"""

from __future__ import annotations

import argparse
import csv
import html
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


HERE = Path(__file__).resolve().parent
DEFAULT_INPUT = HERE / "results_wcq.csv"
DEFAULT_OUTPUT_DIR = HERE / "plots"

BALANCED = ((1, 1), (2, 2), (4, 4), (8, 8))
ASYMMETRIC = ((1, 8), (8, 1))
CAPACITIES = (16, 1024, 65536)


@dataclass(frozen=True)
class Series:
    key: str
    label: str
    color: str

    @property
    def mean_column(self) -> str:
        return f"{self.key}_mean_Mmsg_s"

    @property
    def ci_column(self) -> str:
        return f"{self.key}_ci95_Mmsg_s"


MAIN_SERIES = (
    Series("cas", "Vyukov-CAS", "#4C78A8"),
    Series("faa", "FAA-ticket compact", "#F58518"),
    Series("padded_faa", "Padded-FAA 64 B", "#54A24B"),
    Series("rigtorp", "Rigtorp", "#E45756"),
)

ASYMMETRIC_SERIES = MAIN_SERIES + (
    Series("hybrid1", "Hybrid k=1", "#7A5195"),
    Series("hybrid2", "Hybrid k=2", "#BC5090"),
    Series("hybrid4", "Hybrid k=4", "#EF5675"),
    Series("hybrid8", "Hybrid k=8", "#FFA600"),
)

HYBRID_TOPOLOGY_COLORS = {
    (1, 1): "#4C78A8",
    (2, 2): "#54A24B",
    (4, 4): "#F58518",
    (8, 8): "#E45756",
}


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


class SVG:
    def __init__(self, width: int, height: int, title: str, description: str):
        self.width = width
        self.height = height
        self.parts = [
            (
                f'<svg xmlns="http://www.w3.org/2000/svg" '
                f'width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
                f'role="img" aria-labelledby="title desc">'
            ),
            f"<title id=\"title\">{esc(title)}</title>",
            f"<desc id=\"desc\">{esc(description)}</desc>",
            (
                "<style>"
                "text{font-family:Inter,Arial,sans-serif;fill:#222}"
                ".title{font-size:28px;font-weight:700}"
                ".subtitle{font-size:15px;fill:#555}"
                ".panel-title{font-size:18px;font-weight:700}"
                ".tick{font-size:13px;fill:#444}"
                ".axis-label{font-size:15px;font-weight:600}"
                ".legend{font-size:14px}"
                ".value{font-size:12px;font-weight:600}"
                "</style>"
            ),
            f'<rect width="{width}" height="{height}" fill="#fff"/>',
        ]

    def add(self, element: str) -> None:
        self.parts.append(element)

    def line(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        *,
        stroke: str = "#222",
        width: float = 1,
        dash: str | None = None,
        opacity: float = 1,
    ) -> None:
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(
            f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" '
            f'y2="{y2:.2f}" stroke="{stroke}" stroke-width="{width}" '
            f'opacity="{opacity}"{dash_attr}/>'
        )

    def rect(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        *,
        fill: str,
        stroke: str = "none",
        stroke_width: float = 0,
        opacity: float = 1,
        radius: float = 0,
    ) -> None:
        self.add(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" '
            f'height="{height:.2f}" rx="{radius}" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="{stroke_width}" opacity="{opacity}"/>'
        )

    def circle(
        self,
        x: float,
        y: float,
        radius: float,
        *,
        fill: str,
        stroke: str = "#fff",
        stroke_width: float = 1.5,
    ) -> None:
        self.add(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius}" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="{stroke_width}"/>'
        )

    def text(
        self,
        x: float,
        y: float,
        value: object,
        *,
        css_class: str = "tick",
        anchor: str = "start",
        rotate: float | None = None,
        fill: str | None = None,
    ) -> None:
        transform = (
            f' transform="rotate({rotate} {x:.2f} {y:.2f})"'
            if rotate is not None
            else ""
        )
        fill_attr = f' fill="{fill}"' if fill else ""
        self.add(
            f'<text x="{x:.2f}" y="{y:.2f}" class="{css_class}" '
            f'text-anchor="{anchor}"{transform}{fill_attr}>{esc(value)}</text>'
        )

    def finish(self) -> str:
        return "\n".join((*self.parts, "</svg>", ""))


def load_rows(path: Path) -> tuple[list[dict[str, str]], int, int]:
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise ValueError(f"{path} contains no data rows")

    keys = {
        (int(row["producers"]), int(row["consumers"]), int(row["capacity"]))
        for row in rows
    }
    if len(keys) != len(rows):
        raise ValueError(f"{path} contains duplicate topology/capacity rows")

    required_keys = {
        (p, c, capacity)
        for p, c in (*BALANCED, *ASYMMETRIC)
        for capacity in CAPACITIES
    }
    missing = required_keys - keys
    if missing:
        raise ValueError(f"{path} is missing configurations: {sorted(missing)}")

    messages = {int(row["messages_per_producer"]) for row in rows}
    repetitions = {int(row["repetitions"]) for row in rows}
    if len(messages) != 1 or len(repetitions) != 1:
        raise ValueError("all plotted rows must use the same messages/repetitions")
    return rows, messages.pop(), repetitions.pop()


def index_rows(rows: Iterable[dict[str, str]]) -> dict[tuple[int, int, int], dict[str, str]]:
    return {
        (int(row["producers"]), int(row["consumers"]), int(row["capacity"])): row
        for row in rows
    }


def require_columns(rows: list[dict[str, str]], columns: Iterable[str]) -> None:
    available = set(rows[0])
    missing = set(columns) - available
    if missing:
        raise ValueError(f"input CSV lacks columns required for these plots: {sorted(missing)}")


def nice_ceiling(value: float) -> float:
    if value <= 0:
        return 1.0
    exponent = 10 ** math.floor(math.log10(value))
    scaled = value / exponent
    for candidate in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if scaled <= candidate:
            return candidate * exponent
    return 10 * exponent


def tick_values(maximum: float, count: int = 5) -> list[float]:
    step = maximum / count
    return [i * step for i in range(count + 1)]


def format_tick(value: float) -> str:
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.1f}"


def format_int_it(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def draw_title(svg: SVG, title: str, subtitle: str) -> None:
    svg.text(60, 48, title, css_class="title")
    svg.text(60, 76, subtitle, css_class="subtitle")


def draw_legend(
    svg: SVG,
    entries: Iterable[tuple[str, str]],
    *,
    x: float,
    y: float,
    max_width: float,
) -> float:
    cursor_x = x
    cursor_y = y
    row_height = 27
    for label, color in entries:
        estimated_width = 34 + max(80, len(label) * 8)
        if cursor_x + estimated_width > x + max_width:
            cursor_x = x
            cursor_y += row_height
        svg.rect(cursor_x, cursor_y - 13, 20, 12, fill=color, radius=2)
        svg.text(cursor_x + 27, cursor_y - 2, label, css_class="legend")
        cursor_x += estimated_width
    return cursor_y


def draw_y_axis(
    svg: SVG,
    *,
    left: float,
    top: float,
    plot_width: float,
    plot_height: float,
    y_max: float,
    label: str,
) -> None:
    for value in tick_values(y_max):
        y = top + plot_height - (value / y_max) * plot_height
        svg.line(left, y, left + plot_width, y, stroke="#D9D9D9", width=1)
        svg.text(left - 12, y + 5, format_tick(value), anchor="end")
    svg.line(left, top, left, top + plot_height, stroke="#333", width=1.2)
    svg.line(
        left,
        top + plot_height,
        left + plot_width,
        top + plot_height,
        stroke="#333",
        width=1.2,
    )
    svg.text(
        left - 64,
        top + plot_height / 2,
        label,
        css_class="axis-label",
        anchor="middle",
        rotate=-90,
    )


def draw_error_bar(
    svg: SVG,
    x: float,
    mean: float,
    ci: float,
    *,
    top: float,
    plot_height: float,
    y_max: float,
) -> None:
    upper = min(y_max, mean + ci)
    lower = max(0.0, mean - ci)
    y_upper = top + plot_height - upper / y_max * plot_height
    y_lower = top + plot_height - lower / y_max * plot_height
    svg.line(x, y_upper, x, y_lower, stroke="#222", width=1.2)
    svg.line(x - 4, y_upper, x + 4, y_upper, stroke="#222", width=1.2)
    svg.line(x - 4, y_lower, x + 4, y_lower, stroke="#222", width=1.2)


def grouped_bar_figure(
    rows: list[dict[str, str]],
    *,
    source_name: str,
    messages: int,
    repetitions: int,
    topologies: tuple[tuple[int, int], ...],
    capacities: tuple[int, ...],
    series: tuple[Series, ...],
    title: str,
    filename: Path,
    horizontal_panels: bool,
) -> None:
    columns = [column for item in series for column in (item.mean_column, item.ci_column)]
    require_columns(rows, columns)
    by_key = index_rows(rows)
    maximum = max(
        float(by_key[p, c, capacity][item.mean_column])
        + float(by_key[p, c, capacity][item.ci_column])
        for p, c in topologies
        for capacity in capacities
        for item in series
    )
    y_max = nice_ceiling(maximum * 1.05)

    if horizontal_panels:
        width, height = 1600, 760
        panel_columns, panel_rows = len(topologies), 1
    else:
        width, height = 1500, 1160
        panel_columns, panel_rows = 1, len(capacities)

    subtitle = (
        f"{format_int_it(messages)} messaggi/produttore, "
        f"{repetitions} ripetizioni, "
        f"media ± IC95%; sorgente: {source_name}"
    )
    svg = SVG(
        width,
        height,
        title,
        f"{title}. {subtitle}. Throughput in millions of completed message transfers per second.",
    )
    draw_title(svg, title, subtitle)
    legend_bottom = draw_legend(
        svg,
        ((item.label, item.color) for item in series),
        x=60,
        y=111,
        max_width=width - 120,
    )

    if horizontal_panels:
        outer_left, outer_right = 92, 40
        gap = 75
        panel_width = (width - outer_left - outer_right - gap) / panel_columns
        panel_top = legend_bottom + 38
        plot_height = height - panel_top - 95
        for panel_index, (p, c) in enumerate(topologies):
            panel_left = outer_left + panel_index * (panel_width + gap)
            svg.text(
                panel_left + panel_width / 2,
                panel_top - 13,
                f"{p}P / {c}C",
                css_class="panel-title",
                anchor="middle",
            )
            draw_y_axis(
                svg,
                left=panel_left,
                top=panel_top,
                plot_width=panel_width,
                plot_height=plot_height,
                y_max=y_max,
                label="Mmsg/s",
            )
            group_width = panel_width / len(capacities)
            usable = group_width * 0.82
            bar_width = usable / len(series)
            for group_index, capacity in enumerate(capacities):
                center = panel_left + (group_index + 0.5) * group_width
                start = center - usable / 2
                for series_index, item in enumerate(series):
                    row = by_key[p, c, capacity]
                    mean = float(row[item.mean_column])
                    ci = float(row[item.ci_column])
                    x = start + series_index * bar_width
                    bar_height = mean / y_max * plot_height
                    y = panel_top + plot_height - bar_height
                    svg.rect(
                        x + 1,
                        y,
                        max(1, bar_width - 2),
                        bar_height,
                        fill=item.color,
                        opacity=0.92,
                    )
                    draw_error_bar(
                        svg,
                        x + bar_width / 2,
                        mean,
                        ci,
                        top=panel_top,
                        plot_height=plot_height,
                        y_max=y_max,
                    )
                svg.text(
                    center,
                    panel_top + plot_height + 24,
                    f"cap {capacity}",
                    anchor="middle",
                )
    else:
        panel_left = 105
        panel_width = width - panel_left - 45
        top_start = legend_bottom + 30
        bottom_margin = 35
        gap = 47
        panel_height = (
            height - top_start - bottom_margin - gap * (panel_rows - 1)
        ) / panel_rows
        plot_height = panel_height - 47
        group_width = panel_width / len(topologies)
        usable = group_width * 0.76
        bar_width = usable / len(series)
        for panel_index, capacity in enumerate(capacities):
            panel_top = top_start + panel_index * (panel_height + gap)
            svg.text(
                panel_left,
                panel_top - 11,
                f"Capacità {capacity}",
                css_class="panel-title",
            )
            draw_y_axis(
                svg,
                left=panel_left,
                top=panel_top,
                plot_width=panel_width,
                plot_height=plot_height,
                y_max=y_max,
                label="Mmsg/s",
            )
            for group_index, (p, c) in enumerate(topologies):
                center = panel_left + (group_index + 0.5) * group_width
                start = center - usable / 2
                for series_index, item in enumerate(series):
                    row = by_key[p, c, capacity]
                    mean = float(row[item.mean_column])
                    ci = float(row[item.ci_column])
                    x = start + series_index * bar_width
                    bar_height = mean / y_max * plot_height
                    y = panel_top + plot_height - bar_height
                    svg.rect(
                        x + 1,
                        y,
                        max(1, bar_width - 2),
                        bar_height,
                        fill=item.color,
                        opacity=0.92,
                    )
                    draw_error_bar(
                        svg,
                        x + bar_width / 2,
                        mean,
                        ci,
                        top=panel_top,
                        plot_height=plot_height,
                        y_max=y_max,
                    )
                svg.text(
                    center,
                    panel_top + plot_height + 23,
                    f"{p}P / {c}C",
                    anchor="middle",
                )

    filename.write_text(svg.finish())


def hybrid_sensitivity_figure(
    rows: list[dict[str, str]],
    *,
    source_name: str,
    messages: int,
    repetitions: int,
    filename: Path,
) -> None:
    x_series = (
        ("FAA", "faa_vs_cas_mean", "faa_vs_cas_ci95"),
        ("H-1", "hybrid1_vs_cas_mean", "hybrid1_vs_cas_ci95"),
        ("H-2", "hybrid2_vs_cas_mean", "hybrid2_vs_cas_ci95"),
        ("H-4", "hybrid4_vs_cas_mean", "hybrid4_vs_cas_ci95"),
        ("H-8", "hybrid8_vs_cas_mean", "hybrid8_vs_cas_ci95"),
    )
    require_columns(
        rows,
        [column for _, mean, ci in x_series for column in (mean, ci)],
    )
    by_key = index_rows(rows)
    maximum = max(
        float(by_key[p, c, capacity][mean_col])
        + float(by_key[p, c, capacity][ci_col])
        for p, c in BALANCED
        for capacity in CAPACITIES
        for _, mean_col, ci_col in x_series
    )
    y_max = nice_ceiling(maximum * 1.05)
    y_min = 0.75

    width, height = 1500, 1160
    title = "Sensibilità delle varianti Hybrid"
    subtitle = (
        f"Speedup accoppiato rispetto a Vyukov-CAS; "
        f"{format_int_it(messages)} "
        f"messaggi/produttore, {repetitions} ripetizioni, media ± IC95%; "
        f"sorgente: {source_name}"
    )
    svg = SVG(
        width,
        height,
        title,
        f"{title}. FAA is the pure ticket queue; H-k falls back to FAA after k CAS collisions.",
    )
    draw_title(svg, title, subtitle)
    legend_bottom = draw_legend(
        svg,
        (
            (f"{p}P / {c}C", HYBRID_TOPOLOGY_COLORS[p, c])
            for p, c in BALANCED
        ),
        x=60,
        y=111,
        max_width=width - 120,
    )
    svg.text(
        60,
        legend_bottom + 24,
        "H-k: una CAS iniziale; fallback FAA dopo la k-esima collisione CAS.",
        css_class="subtitle",
    )

    left = 112
    plot_width = width - left - 47
    top_start = legend_bottom + 57
    gap = 48
    bottom_margin = 34
    panel_height = (height - top_start - bottom_margin - 2 * gap) / 3
    plot_height = panel_height - 45
    x_step = plot_width / len(x_series)

    def y_position(value: float, top: float) -> float:
        return top + plot_height - (value - y_min) / (y_max - y_min) * plot_height

    for panel_index, capacity in enumerate(CAPACITIES):
        top = top_start + panel_index * (panel_height + gap)
        svg.text(left, top - 11, f"Capacità {capacity}", css_class="panel-title")
        for tick in tick_values(y_max - y_min, 5):
            value = y_min + tick
            y = y_position(value, top)
            svg.line(left, y, left + plot_width, y, stroke="#D9D9D9")
            svg.text(left - 12, y + 5, f"{value:.1f}×", anchor="end")
        parity_y = y_position(1.0, top)
        svg.line(
            left,
            parity_y,
            left + plot_width,
            parity_y,
            stroke="#222",
            width=1.5,
            dash="7 5",
        )
        svg.text(
            left + plot_width - 3,
            parity_y - 6,
            "parità",
            anchor="end",
            css_class="subtitle",
        )
        svg.line(left, top, left, top + plot_height, stroke="#333", width=1.2)
        svg.line(
            left,
            top + plot_height,
            left + plot_width,
            top + plot_height,
            stroke="#333",
            width=1.2,
        )
        svg.text(
            left - 72,
            top + plot_height / 2,
            "speedup / CAS",
            css_class="axis-label",
            anchor="middle",
            rotate=-90,
        )
        for x_index, (label, _, _) in enumerate(x_series):
            x = left + (x_index + 0.5) * x_step
            svg.text(x, top + plot_height + 23, label, anchor="middle")

        for p, c in BALANCED:
            color = HYBRID_TOPOLOGY_COLORS[p, c]
            points: list[tuple[float, float, float]] = []
            row = by_key[p, c, capacity]
            for x_index, (_, mean_col, ci_col) in enumerate(x_series):
                x = left + (x_index + 0.5) * x_step
                mean = float(row[mean_col])
                ci = float(row[ci_col])
                points.append((x, mean, ci))
            for first, second in zip(points, points[1:]):
                svg.line(
                    first[0],
                    y_position(first[1], top),
                    second[0],
                    y_position(second[1], top),
                    stroke=color,
                    width=2.4,
                    opacity=0.9,
                )
            for x, mean, ci in points:
                y_upper = y_position(min(y_max, mean + ci), top)
                y_lower = y_position(max(y_min, mean - ci), top)
                svg.line(x, y_upper, x, y_lower, stroke=color, width=1.5)
                svg.line(x - 4, y_upper, x + 4, y_upper, stroke=color, width=1.5)
                svg.line(x - 4, y_lower, x + 4, y_lower, stroke=color, width=1.5)
                svg.circle(x, y_position(mean, top), 5, fill=color)

    filename.write_text(svg.finish())


def write_readme(
    output_dir: Path,
    *,
    source_path: Path,
    messages: int,
    repetitions: int,
) -> None:
    text = f"""# Grafici del queue benchmark

Queste figure sono generate da `{source_path.name}` con
`plot_results.py`, senza dipendenze Python esterne.

Dataset: {format_int_it(messages)} messaggi per produttore, {repetitions} ripetizioni per
configurazione. Le barre e i punti mostrano la media; le barre di errore sono
gli IC95% già presenti nel CSV.

- `balanced_main_queues.svg`: throughput delle quattro code principali nelle
  topologie bilanciate.
- `hybrid_sensitivity.svg`: speedup accoppiato di FAA e Hybrid rispetto a
  Vyukov-CAS. `H-k` usa FAA dopo la k-esima collisione CAS della singola
  operazione bloccante.
- `asymmetric_queues.svg`: throughput delle code principali e delle quattro
  soglie Hybrid nelle topologie 1P/8C e 8P/1C.

I CSV conservano solo statistiche aggregate. Le figure possono quindi mostrare
media e IC95%, ma non boxplot, distribuzioni o multimodalità. Per queste analisi
il runner deve essere esteso per salvare i campioni raw.
"""
    (output_dir / "README.md").write_text(text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate SVG figures from a QueueBenchmark aggregate CSV."
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"aggregate CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"SVG output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, messages, repetitions = load_rows(args.input)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    grouped_bar_figure(
        rows,
        source_name=args.input.name,
        messages=messages,
        repetitions=repetitions,
        topologies=BALANCED,
        capacities=CAPACITIES,
        series=MAIN_SERIES,
        title="Throughput delle code principali — topologie bilanciate",
        filename=args.output_dir / "balanced_main_queues.svg",
        horizontal_panels=False,
    )
    hybrid_sensitivity_figure(
        rows,
        source_name=args.input.name,
        messages=messages,
        repetitions=repetitions,
        filename=args.output_dir / "hybrid_sensitivity.svg",
    )
    grouped_bar_figure(
        rows,
        source_name=args.input.name,
        messages=messages,
        repetitions=repetitions,
        topologies=ASYMMETRIC,
        capacities=CAPACITIES,
        series=ASYMMETRIC_SERIES,
        title="Throughput — topologie asimmetriche",
        filename=args.output_dir / "asymmetric_queues.svg",
        horizontal_panels=True,
    )
    write_readme(
        args.output_dir,
        source_path=args.input,
        messages=messages,
        repetitions=repetitions,
    )
    print(f"SVG plots written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
