#!/usr/bin/python3
"""Crea un PDF separato per i nuovi risultati; non rigenera la relazione precedente."""
import argparse
import ast
import csv
import hashlib
import html
import json
from pathlib import Path


def load_csv(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path, help='Cartella con PipelineGrain e FarmThroughput completati')
    args = parser.parse_args()
    root = args.results.resolve()
    if not (root / 'COMPLETATO.txt').is_file():
        parser.error('La campagna deve essere completata prima di generare il PDF')
    # Riusa soltanto il renderer del documento locale, senza eseguirne il corpo.
    engine_path = Path(__file__).resolve().parent.parent / 'Relazione/genera_relazione.py'
    source = engine_path.read_text()
    namespace = {}
    exec(compile(source.split('HERE = Path(__file__).resolve().parent')[0], str(engine_path), 'exec'), namespace)
    node = next(node for node in ast.parse(source).body if isinstance(node, ast.ClassDef) and node.name == 'Document')
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(engine_path), 'exec'), namespace)
    output = root / 'Confronto_50_ripetizioni.pdf'
    if output.exists():
        parser.error('Il nuovo PDF esiste già: non viene sovrascritto')
    doc = namespace['Document'](output)
    def number(value, decimals=2):
        return f'{float(value):,.{decimals}f}'.replace(',', '_').replace('.', ',').replace('_', '.')
    datasets = {}
    for benchmark in ('PipelineGrain', 'FarmThroughput'):
        folder = root / benchmark
        raw = load_csv(folder / 'raw.csv')
        summary = load_csv(folder / 'summary.csv')
        comparison = load_csv(folder / 'confronto_50.csv')
        assert len(raw) == 2000 and len(summary) == 40 and len(comparison) == 20
        assert all(int(row['repeats']) == 50 for row in summary)
        datasets[benchmark] = (summary, comparison)
    doc.title('Nuova campagna: 50 ripetizioni', 'Supplemento sperimentale al confronto Rigtorp–Vyukov')
    doc.p('Questo documento contiene nuovi risultati, separati dalla relazione con tre ripetizioni. Sono rimasti invariati payload UInt64, 50.000 input per misura, capacità 1024, N=1,2,4,8,16 e T=0,1,2,5 µs. I dati storici e il PDF precedente non sono stati sovrascritti.')
    doc.table(['Campagna','Configurazioni','Ripetizioni','Misure'],[['PipelineGrain',40,50,2000],['FarmThroughput',40,50,2000]],size=9)
    doc.p('PipelineGrain usa N stadi con lavoro totale T suddiviso fra gli stadi; FarmThroughput usa una sorgente e N consumatori che eseguono l’intero T per input. I benchmark sono stati eseguiti uno dopo l’altro e conservano i controlli di correttezza previsti dagli harness.')
    doc.h('Significato dell’analisi statistica')
    doc.p('Il rapporto di throughput R/V è la mediana dei tempi Vyukov divisa per quella Rigtorp: un valore maggiore di uno favorisce Rigtorp. Gli intervalli sono percentile bootstrap al 95%, con 20.000 ricampionamenti. La farm ricampiona le coppie di prove con lo stesso repeat; la pipeline usa campioni indipendenti. Il caso N=1 della pipeline non usa code e non attribuisce un vincitore.')
    doc.p('Sono intervalli individuali, non corretti per i molti confronti. L’analisi rimane esplorativa e non dimostra stabilità fra giorni o macchine. La soglia pratica scelta è un vantaggio del 5%; il semplice superamento della parità non implica che questo margine sia raggiunto con sufficiente evidenza.')
    labels = {'baseline': 'Nessuna coda', 'inconclusivo': 'Inconclusivo'}
    for benchmark in ('PipelineGrain', 'FarmThroughput'):
        summary, comparison = datasets[benchmark]
        nk = 'stages' if benchmark == 'PipelineGrain' else 'consumers'
        tk = 'target_total_us' if benchmark == 'PipelineGrain' else 'target_work_us'
        for target in (0, 1, 2, 5):
            doc.title(f'{benchmark}: T={target} µs')
            selected = [row for row in comparison if float(row['T_us']) == target]
            metric_rows, effect_rows = [], []
            for row in sorted(selected, key=lambda row: int(row['N'])):
                size = int(row['N'])
                r = next(r for r in summary if r['queue'] == 'rigtorp' and int(r[nk]) == size and float(r[tk]) == target)
                v = next(r for r in summary if r['queue'] == 'vyukov' and int(r[nk]) == size and float(r[tk]) == target)
                if benchmark == 'PipelineGrain':
                    metric_rows.append([size, number(r['median_ns_per_input'], 1), number(v['median_ns_per_input'], 1)])
                else:
                    metric_rows.append([size, number(float(r['throughput_per_second']) / 1e6, 3), number(float(v['throughput_per_second']) / 1e6, 3)])
                low, high = row['ci95_ratio_low'], row['ci95_ratio_high']
                bounds = '—' if not low else number(low, 3) + '–' + number(high, 3)
                verdict = row['interpretation']
                if 'baseline' in verdict:
                    label = 'Nessuna coda'
                elif 'oltre il 5%' in verdict:
                    label = ('Rigtorp' if 'Rigtorp' in verdict else 'Vyukov') + ': margine >5%'
                elif 'inconclusivo' in verdict:
                    label = 'Inconclusivo'
                else:
                    label = ('Rigtorp' if 'Rigtorp' in verdict else 'Vyukov') + ': segno positivo'
                effect_rows.append([size, number(row['throughput_ratio_R_over_V'], 3), bounds, label])
            unit = 'ns/input' if benchmark == 'PipelineGrain' else 'milioni input/s'
            doc.p(f'Mediane su 50 ripetizioni per coda. Unità: <b>{unit}</b>. ' + ('Minore è meglio.' if benchmark == 'PipelineGrain' else 'Maggiore è meglio.'))
            doc.table(['N', 'Rigtorp', 'Vyukov'], metric_rows, size=9)
            doc.h('Vantaggio e incertezza')
            doc.table(['N','Rapporto R/V','Intervallo 95%','Indicazione individuale'],effect_rows,widths=[30,90,125,doc.area-245],size=8.5)
            doc.p('“Margine >5%” richiede che l’intero intervallo superi la soglia pratica nella direzione indicata. “Segno positivo” indica una differenza dall’uguaglianza, ma non un vantaggio minimo del 5% sostenuto dall’intervallo. “Inconclusivo” non dimostra equivalenza: indica che questi dati non separano chiaramente le code.',size=9.5)
            if benchmark == 'FarmThroughput':
                doc.p('L’ideale è N/T solo per T>0 e assume che sorgente e comunicazione non limitino il sistema. Un plateau non identifica da solo quale componente sia il collo di bottiglia.',size=9.5)
        if benchmark == 'PipelineGrain':
            doc.figure_page('PipelineGrain — nuove 50 ripetizioni',root/benchmark/'pipeline_grain.png',
                            'Tempo medio per input; minore è meglio.',
                            'Blu: Rigtorp; arancione: Vyukov. I punti sono mediane; le fasce min–max non sono gli intervalli bootstrap riportati nelle tabelle. Il riferimento ideale usa la baseline N=1 divisa per N.')
        else:
            doc.figure_page('FarmThroughput — nuove 50 ripetizioni',root/benchmark/'farm_throughput.png',
                            'Throughput con una sorgente e N consumatori; maggiore è meglio.',
                            'Assi logaritmici; tratteggio: N/T. Le fasce rappresentano minimo e massimo delle 50 misure, non intervalli di confidenza. Per T=0 l’ideale non è definito.')
            doc.figure_page('FarmThroughput — efficienza con 50 ripetizioni',root/benchmark/'farm_efficiency.png',
                            'Rapporto fra throughput misurato e ideale N/T.',
                            'La percentuale non è l’utilizzo della CPU. La grana e la topologia rimangono determinanti per il confronto. Per protocolli e dettagli degli harness si rimanda alla relazione precedente e ai sorgenti conservati.')
    doc.title('Conclusioni e limiti della nuova campagna')
    for name, (_, comparisons) in datasets.items():
        meaningful = [row for row in comparisons if 'baseline' not in row['interpretation']]
        r = sum('vantaggio Rigtorp oltre' in row['interpretation'] for row in meaningful)
        v = sum('vantaggio Vyukov oltre' in row['interpretation'] for row in meaningful)
        uncertain = sum('inconclusivo' in row['interpretation'] for row in meaningful)
        doc.p(f'<b>{name}:</b> su {len(meaningful)} confronti pertinenti alle code, {r} hanno un intervallo individuale che sostiene un margine Rigtorp oltre il 5%, {v} un margine Vyukov oltre il 5%; {uncertain} comprendono la parità. Gli altri mostrano una direzione senza sostenere il margine pratico del 5%. Questi conteggi non sono una correzione per confronti multipli né una classifica universale.')
    doc.p('La nuova campagna riduce l’incertezza campionaria rispetto a tre prove, ma non controlla tutti i fattori: macchina condivisa, frequenza CPU, dipendenze temporali e differenze di implementazione possono influenzare i risultati. L’harness non misura il runtime completo MoStream, la latenza individuale o i contatori hardware.')
    doc.p('Una conclusione definitiva richiederebbe repliche in sessioni indipendenti e, per affermazioni su molte configurazioni, un’analisi confermativa con controllo dei confronti multipli. La soglia del 5% serve a distinguere rilevanza pratica e semplice direzione statistica.')
    doc.h('Provenienza e conservazione')
    doc.p('Questo supplemento legge esclusivamente i nuovi CSV e grafici della campagna da 50 ripetizioni. Il PDF precedente è mantenuto invariato. Nessun nuovo benchmark viene eseguito durante la generazione del documento. Il supplemento è generato soltanto in locale.')
    doc.finish()
    manifest = {'pdf':output.name,'pages':doc.page,'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
                'source_data':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
                               for name in datasets for p in (root/name).glob('*.csv')}}
    (root/'fonti_pdf_50.json').write_text(json.dumps(manifest,indent=2))
    print(f'Creato nuovo PDF locale: {output} ({doc.page} pagine)')


if __name__ == '__main__':
    main()
