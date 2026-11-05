#!/usr/bin/env python3
"""Riepilogo verificato dei tre benchmark, con PDF facoltativo e grafici esistenti."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import textwrap

BASE = Path(__file__).resolve().parent
ROOT = BASE.parents[1]


def read_csv(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def audit(directory, config):
    if config['smoke'] or config['repeats'] != 50:
        raise ValueError('Questo rapporto richiede una campagna completa a 50 ripetizioni')
    if not (directory / 'COMPLETATO.txt').exists():
        raise ValueError('La campagna non è completata')
    if (directory / 'INTERROTTO.txt').exists():
        raise ValueError('La cartella contiene una campagna interrotta')
    metadata = {}
    for name, expected in config['expected_raw_rows'].items():
        rows = read_csv(directory / name / 'raw.csv')
        if len(rows) != expected:
            raise ValueError(f'{name}: numero di misure errato')
        groups = {}
        for row in rows:
            if name == 'ImageTiles':
                key = tuple(row[k] for k in ('backend', 'width', 'height', 'block_rows', 'consumers'))
                if row['output_equal'] != 'True' or row['checksum'] != row['reference_checksum']:
                    raise ValueError('Output immagini non verificato')
            else:
                key = (row['queue'], row['stages' if name == 'PipelineGrain' else 'consumers'], row['target_total_us' if name == 'PipelineGrain' else 'target_work_us'])
                if int(row['messages']) != 50000:
                    raise ValueError('Numero di messaggi differente dalla campagna richiesta')
            groups.setdefault(key, []).append(int(row['repeat']))
        if any(len(repeats) != 50 or set(repeats) != set(range(50)) for repeats in groups.values()):
            raise ValueError(f'{name}: ripetizioni mancanti o duplicate')
        meta = json.loads((directory / name / 'metadata.json').read_text())
        for relative, expected_hash in meta['source_sha256'].items():
            actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            if actual != expected_hash:
                raise ValueError(f'Sorgente diverso da quello misurato: {relative}')
        metadata[name] = meta
    return metadata


def comparisons(directory):
    result = []
    for name, nkey, tkey in (('PipelineGrain', 'stages', 'target_total_us'), ('FarmThroughput', 'consumers', 'target_work_us')):
        summaries = read_csv(directory / name / 'summary.csv')
        groups = {}
        for row in summaries:
            groups.setdefault((int(row[nkey]), float(row[tkey])), {})[row['queue']] = row
        intervals = {(int(r['N']), float(r['T_us'])): r for r in read_csv(directory / name / 'confronto_50.csv')}
        for (n, target), backends in sorted(groups.items()):
            w, v = backends['wcq'], backends['vyukov']
            baseline = name == 'PipelineGrain' and n == 1
            stat = intervals[(n, target)]
            result.append({'benchmark': name, 'N': n, 'T_us': target, 'block_rows': '', 'resolution': '',
                           'throughput_wcq': float(w['throughput_per_second']),
                           'throughput_vyukov': float(v['throughput_per_second']),
                           'ratio_W_over_V': '' if baseline else float(w['throughput_per_second']) / float(v['throughput_per_second']),
                           'ci95_low': stat['ci95_ratio_low'], 'ci95_high': stat['ci95_ratio_high'],
                           'p95_ms_wcq': '', 'p95_ms_vyukov': '', 'baseline_without_queues': baseline})
    groups = {}
    for row in read_csv(directory / 'ImageTiles/summary.csv'):
        if row['backend'] == 'serial':
            continue
        groups.setdefault((int(row['width']), int(row['height']), int(row['block_rows']), int(row['consumers'])), {})[row['backend']] = row
    for (width, height, block, n), backends in sorted(groups.items()):
        w, v = backends['wcq'], backends['vyukov']
        result.append({'benchmark': 'ImageTiles', 'N': n, 'T_us': '', 'block_rows': block,
                       'resolution': f'{width}x{height}',
                       'throughput_wcq': float(w['images_per_second']), 'throughput_vyukov': float(v['images_per_second']),
                       'ratio_W_over_V': float(w['images_per_second']) / float(v['images_per_second']),
                       'ci95_low': '', 'ci95_high': '',
                       'p95_ms_wcq': float(w['median_run_p95_latency_ms']),
                       'p95_ms_vyukov': float(v['median_run_p95_latency_ms']), 'baseline_without_queues': False})
    return result


def markdown(config, metadata, rows, figures):
    cpuinfo = metadata['PipelineGrain']['cpuinfo']
    model = next((line.split(':', 1)[1].strip() for line in cpuinfo.splitlines() if line.startswith('model name')), 'vedere metadata.json')
    lines = ['# Confronto wCQ–Vyukov: tre benchmark', '',
             f"Campagna completata: **{sum(config['expected_raw_rows'].values()):,} misure**, 50 ripetizioni per configurazione.",
             f"CPU: {model}. Core fisici usati: {config['physical_cpus']}.",
             f"Compilatore: {metadata['PipelineGrain']['mojo_version']}; ottimizzazione -O3, nessun sanitizer.",
             f"N misurati: {config['measured_nodes']}. N della griglia canonica non misurati: {config['not_measured_nodes']}.", '',
             'Capacità utile 1024 per entrambe le code. Sintetici: 50.000 input, T=0,1,2,5 µs. '
             'Immagini: 32 frame RGB 512×512, strisce 1/16/128 righe e baseline seriale.', '',
             '## Come leggere il confronto', '',
             '**Rapporto W/V > 1 favorisce wCQ; < 1 favorisce Vyukov.** Il rapporto usa throughput ricavati '
             'dai tempi mediani. N=1 della pipeline non usa code e non assegna un vincitore.', '',
             'Si confrontano implementazioni concrete: wCQ usa due ring di indici e primitive atomiche C '
             'sequenzialmente consistenti; Vyukov usa atomiche Mojo relaxed/acquire/release e payload separato. '
             'Sono inclusi costo delle chiamate C, rappresentazione dei dati e diagnostica già interna al porting. '
             'I risultati non isolano il costo teorico del solo algoritmo e non certificano wait-freedom.', '',
             'I carichi e la topologia sono quelli del confronto Rigtorp, ma tutti i tempi sono nuovi: '
             'i risultati storici non vengono uniti a questi campioni.', '',
             '## PipelineGrain e FarmThroughput', '',
             'Throughput in input/s. Intervalli percentile bootstrap al 95% individuali, 20.000 ricampionamenti: '
             'indipendenti nella pipeline e appaiati per repeat nella farm. Non corretti per confronti multipli; '
             'analisi esplorativa di una sola sessione. Il dettaglio della soglia pratica del 5% è nei singoli `confronto_50.md`.', '',
             '| Benchmark | N | T µs | wCQ input/s | Vyukov input/s | W/V | IC 95% |',
             '| --- | ---: | ---: | ---: | ---: | ---: | --- |']
    for row in rows:
        if row['benchmark'] == 'ImageTiles':
            continue
        ratio = 'baseline senza code' if row['baseline_without_queues'] else f"{row['ratio_W_over_V']:.3f}"
        interval = '—' if not row['ci95_low'] else f"{float(row['ci95_low']):.3f}–{float(row['ci95_high']):.3f}"
        lines.append(f"| {row['benchmark']} | {row['N']} | {row['T_us']:g} | {row['throughput_wcq']:,.0f} | {row['throughput_vyukov']:,.0f} | {ratio} | {interval} |")
    lines += ['', '## ImageTiles', '',
              'Throughput in immagini/s; p95 in millisecondi, riepilogato come mediana dei p95 delle singole prove. '
              'Questi rapporti sono descrittivi: non sono corredati da intervalli di confidenza.', '',
              '| Righe/blocco | N | wCQ img/s | Vyukov img/s | W/V | p95 wCQ ms | p95 Vyukov ms |',
              '| ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for row in rows:
        if row['benchmark'] == 'ImageTiles':
            lines.append(f"| {row['block_rows']} | {row['N']} | {row['throughput_wcq']:,.1f} | {row['throughput_vyukov']:,.1f} | {row['ratio_W_over_V']:.3f} | {row['p95_ms_wcq']:.3f} | {row['p95_ms_vyukov']:.3f} |")
    serial = next(r for r in read_csv(metadata['_directory'] / 'ImageTiles/summary.csv') if r['backend'] == 'serial')
    lines += ['', f"Baseline seriale senza code: **{float(serial['images_per_second']):,.1f} immagini/s**.", '',
              'La latenza parallela va da prima del primo enqueue di una immagine al suo ultimo completamento. '
              'Non include l’attesa prima di quella emissione. Il seriale misura il tempo di servizio del kernel '
              'per immagine: non simula la stessa coda di arrivi esterni. Leggere latenza e throughput insieme.', '',
              '## Correttezza e verifiche', '',
              'Tutte le ripetizioni sono presenti una sola volta; i conteggi e gli hash dei sorgenti sono stati '
              'ricontrollati dal generatore del rapporto. La farm verifica ogni ID e il relativo calcolo, '
              'ImageTiles verifica copertura dei blocchi e ogni byte rispetto a un oracolo scalare. '
              'La pipeline verifica un digest sensibile all’ordine contro il calcolo seriale; '
              'quest’ultimo resta un controllo probabilistico.', '',
              'Il pinning e le allocazioni precedono il timer. I tempi includono riempimento, svuotamento, '
              'attesa e completamento dei task; non sono la latenza di una singola operazione di coda. '
              'I risultati non dimostrano superiorità universale o stabilità fra giorni e macchine.', '', '## Grafici', '']
    for figure in figures:
        lines += [f'![{figure.stem}]({figure.as_posix()})', '']
    return '\n'.join(lines)


def make_pdf(path, config, metadata, rows, figures):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    with path.open('xb') as stream, PdfPages(stream) as pdf:
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.text(.07, .91, 'wCQ vs Vyukov — tre benchmark', fontsize=22, weight='bold')
        notes = [f"50 ripetizioni; N={config['measured_nodes']}; capacità utile 1024; {sum(config['expected_raw_rows'].values()):,} misure.",
                 f"{metadata['PipelineGrain']['mojo_version']}; -O3; core fisici {config['physical_cpus']}.",
                 'PipelineGrain: N stadi, T totale 0/1/2/5 µs, 50.000 input; N=1 senza code.',
                 'FarmThroughput: sorgente e N consumatori, T per input 0/1/2/5 µs, 50.000 input.',
                 'ImageTiles: 32 immagini 512×512, strisce 1/16/128 righe e seriale senza code.',
                 'Rapporto W/V > 1 favorisce wCQ; < 1 favorisce Vyukov.',
                 'wCQ: porting sperimentale Mojo, due ring di indici, primitive atomiche C seq-cst.',
                 'Vyukov: stesso protocollo del confronto Rigtorp, atomiche native relaxed/acquire/release.',
                 'Il confronto include rappresentazione, footprint e costo delle chiamate alle primitive.',
                 'I risultati Rigtorp storici non vengono usati come campioni di questa campagna.',
                 f"N canonici non misurati per risorse fisiche insufficienti: {config['not_measured_nodes']}.",
                 'Le fasce nei grafici sono min–max, non intervalli di confidenza.',
                 'Bootstrap sintetici: 20.000 campioni, IC individuali 95%, nessuna correzione multipla.',
                 'Un’unica sessione non dimostra superiorità universale. Vedere CONFRONTO.md per tabelle e limiti.']
        fig.text(.07, .83, '\n\n'.join(textwrap.fill(line, 112) for line in notes), fontsize=10, va='top')
        pdf.savefig(fig)
        plt.close(fig)
        for benchmark in ('PipelineGrain', 'FarmThroughput', 'ImageTiles'):
            selected = [row for row in rows if row['benchmark'] == benchmark]
            body = []
            for row in selected:
                ratio = 'baseline' if row['baseline_without_queues'] else f"{row['ratio_W_over_V']:.3f}"
                bounds = '—' if not row['ci95_low'] else f"{float(row['ci95_low']):.3f}–{float(row['ci95_high']):.3f}"
                if benchmark == 'ImageTiles':
                    body.append([str(row['block_rows']), str(row['N']), f"{row['throughput_wcq']:,.1f}",
                                 f"{row['throughput_vyukov']:,.1f}", ratio,
                                 f"{row['p95_ms_wcq']:.3f}", f"{row['p95_ms_vyukov']:.3f}"])
                else:
                    body.append([str(row['N']), f"{row['T_us']:g}", f"{row['throughput_wcq']:,.0f}",
                                 f"{row['throughput_vyukov']:,.0f}", ratio, bounds])
            labels = (['Righe', 'N', 'wCQ img/s', 'Vyukov img/s', 'W/V', 'p95 W ms', 'p95 V ms']
                      if benchmark == 'ImageTiles' else ['N', 'T µs', 'wCQ input/s', 'Vyukov input/s', 'W/V', 'IC 95% W/V'])
            fig, ax = plt.subplots(figsize=(11.69, 8.27))
            ax.axis('off')
            fig.suptitle(benchmark + ' — mediane, 50 ripetizioni', fontsize=18, y=.94)
            table = ax.table(cellText=body, colLabels=labels, cellLoc='center', loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 2)
            for (i, _), cell in table.get_celld().items():
                if i == 0:
                    cell.set_facecolor('#e2e8f0')
                    cell.set_text_props(weight='bold')
            note = ('p95: mediana dei p95 per prova; rapporto di throughput descrittivo, senza IC.'
                    if benchmark == 'ImageTiles' else 'IC individuali non corretti per confronti multipli; N=1 pipeline: nessuna coda.')
            fig.text(.08, .08, note, fontsize=10)
            pdf.savefig(fig)
            plt.close(fig)
        for relative in figures:
            fig, ax = plt.subplots(figsize=(11.69, 8.27))
            ax.imshow(plt.imread(path.parent / relative))
            ax.axis('off')
            fig.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    parser.add_argument('--pdf', action='store_true')
    args = parser.parse_args()
    directory = args.results.resolve()
    targets = ['CONFRONTO.md', 'confronto.csv'] + (['Confronto_wCQ_Vyukov.pdf'] if args.pdf else [])
    if any((directory / name).exists() for name in targets):
        parser.error('Il rapporto esiste già: nessun file sovrascritto')
    config = json.loads((directory / 'campagna.json').read_text())
    metadata = audit(directory, config)
    metadata['_directory'] = directory
    rows = comparisons(directory)
    figures = [p.relative_to(directory) for name in ('PipelineGrain', 'FarmThroughput', 'ImageTiles') for p in sorted((directory / name).glob('*.png'))]
    if len(figures) != 6:
        raise ValueError(f'Attesi sei grafici, trovati {len(figures)}')
    with (directory / 'confronto.csv').open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (directory / 'CONFRONTO.md').write_text(markdown(config, metadata, rows, figures))
    if args.pdf:
        make_pdf(directory / 'Confronto_wCQ_Vyukov.pdf', config, metadata, rows, figures)
    verification = json.loads((directory / 'VERIFICA.json').read_text())
    verification['report_generator_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    verification['report_artifacts_sha256'] = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in targets}
    verification['source_hashes_match'] = True
    verification['repeat_groups_complete'] = True
    (directory / 'VERIFICA.json').write_text(json.dumps(verification, indent=2))
    print('Confronto verificato:', directory / 'CONFRONTO.md')


if __name__ == '__main__':
    main()
