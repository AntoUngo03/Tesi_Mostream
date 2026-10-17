#!/usr/bin/env python3
"""Grafici throughput, speedup sul seriale e latenza end-to-end per immagine."""
import argparse
import csv
import math
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    with (args.results / 'summary.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    sizes = sorted({(int(r['width']), int(r['height'])) for r in rows})
    colors = {'rigtorp': '#2563eb', 'vyukov': '#d97706'}
    for width, height in sizes:
        selected = [r for r in rows if int(r['width']) == width and int(r['height']) == height]
        serial = next(r for r in selected if r['backend'] == 'serial')
        block_rows = sorted({int(r['block_rows']) for r in selected if r['backend'] != 'serial'})
        for metric in ('throughput', 'speedup', 'latency'):
            columns = min(2, len(block_rows))
            figure, axes = plt.subplots(math.ceil(len(block_rows) / columns), columns,
                                       figsize=(7 * columns, 4.5 * math.ceil(len(block_rows) / columns)), squeeze=False)
            for axis, block in zip(axes.flat, block_rows):
                group = [r for r in selected if int(r['block_rows']) == block]
                ns = sorted({int(r['consumers']) for r in group})
                for backend, color in colors.items():
                    data = sorted((r for r in group if r['backend'] == backend), key=lambda r: int(r['consumers']))
                    n = [int(r['consumers']) for r in data]
                    if metric == 'throughput':
                        axis.plot(n, [float(r['images_per_second']) for r in data], marker='o', color=color, label=backend)
                        axis.fill_between(n, [float(r['min_images_per_second']) for r in data],
                                          [float(r['max_images_per_second']) for r in data], alpha=.12, color=color)
                    elif metric == 'speedup':
                        axis.plot(n, [float(r['speedup_vs_serial']) for r in data], marker='o', color=color, label=backend)
                    else:
                        axis.plot(n, [float(r['median_run_median_latency_ms']) for r in data], marker='o', color=color, label=f'{backend}: mediana')
                        axis.plot(n, [float(r['median_run_p95_latency_ms']) for r in data], marker='s', linestyle='--', color=color, label=f'{backend}: p95')
                if metric == 'throughput':
                    axis.axhline(float(serial['images_per_second']), color='#64748b', linestyle='--', label='Seriale senza code')
                    axis.set_ylabel('Immagini/s; maggiore è meglio')
                elif metric == 'speedup':
                    axis.axhline(1, color='#64748b', linestyle='--', label='Parità con seriale')
                    axis.set_ylabel('Speedup sul seriale; >1 significa più veloce')
                else:
                    axis.axhline(float(serial['median_run_median_latency_ms']), color='#64748b', linestyle=':', label='Seriale: mediana')
                    axis.axhline(float(serial['median_run_p95_latency_ms']), color='#64748b', linestyle='--', label='Seriale: p95')
                    axis.set_ylabel('Latenza per immagine (ms); minore è meglio')
                axis.set_title(f'{block} righe/blocco, {math.ceil(height / block)} blocchi/immagine')
                axis.set_xscale('log', base=2)
                axis.set_xticks(ns, [str(n) for n in ns])
                axis.set_xlabel('N consumer (+ produttore e raccoglitore)')
                axis.set_ylim(bottom=0)
                axis.grid(alpha=.2)
                axis.legend(fontsize=7)
            for axis in list(axes.flat)[len(block_rows):]:
                axis.set_visible(False)
            subtitle = ('Throughput dal tempo mediano; fasce min–max, non intervalli di confidenza' if metric == 'throughput'
                        else 'Mediana dei riepiloghi per prova; I/O, allocazione e verifica fuori misura')
            figure.suptitle(f'ImageTiles — grayscale {width}×{height} — {metric}\n{subtitle}')
            figure.tight_layout(rect=(0, 0, 1, .94))
            for extension in ('png', 'pdf'):
                figure.savefig(args.results / f'{metric}_{width}x{height}.{extension}', dpi=180, bbox_inches='tight')
            plt.close(figure)
    print(f'Grafici salvati in {args.results}')


if __name__ == '__main__':
    main()
