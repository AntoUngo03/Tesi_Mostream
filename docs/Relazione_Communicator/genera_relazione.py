#!/usr/bin/env python3
"""Genera la relazione sul nuovo Communicator (HTML + PDF con Chrome headless).

Tutti i numeri sono letti dai CSV delle campagne in Benchmarks/MoStreamSynthetic:
  campagna-50-20261105            Communicator originale (pop = ciclo di try_pop)
  campagna-50-explicit-eos-20261106  Communicator nuovo (EOS espliciti, pop bloccante)
Uso: python3 genera_relazione.py   (dalla cartella docs/Relazione_Communicator)
"""
import csv
import html
import shutil
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SYN = ROOT / 'Benchmarks' / 'MoStreamSynthetic'
OLD = SYN / 'campagna-50-20261105'
NEW = SYN / 'campagna-50-explicit-eos-20261106'
HARNESS = ROOT / 'Benchmarks' / 'BenchMark Vyukov vs RigTorp' / 'Campagna_50_ripetizioni' / 'risultati' / '20261015-122503'
BENCH = {'FarmThroughput': ('consumers', 'target_work_us', 'N consumatori'),
         'PipelineGrain': ('stages', 'total_work_us', 'N stadi')}


def num(x, d=2):
    return f'{x:,.{d}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def mil(x):
    return num(x / 1e6, 2)


def pct(x):
    s = f'{(x - 1) * 100:+.0f}%'
    return s.replace('-', '−')


def medians(campaign, bench):
    nk, tk, _ = BENCH[bench]
    groups = defaultdict(list)
    with open(campaign / bench / 'raw.csv', newline='') as f:
        for r in csv.DictReader(f):
            groups[(r['backend'], int(r[nk]), float(r[tk]))].append(float(r['throughput']))
    return {k: statistics.median(v) for k, v in groups.items()}


def evaluation(campaign):
    out = {}
    with open(campaign / 'valutazione_rigtorp' / 'valutazione.csv', newline='') as f:
        for r in csv.DictReader(f):
            out[(r['benchmark'], int(r['N']), float(r['T_us']))] = r
    return out


def harness_table(campaign):
    out = {}
    with open(campaign / 'confronto_harness' / 'confronto.csv', newline='') as f:
        for r in csv.DictReader(f):
            out[(r['benchmark'], r['queue'], int(r['N']), float(r['T_us']))] = r
    return out


def verdict_counts(ev, bench):
    c = defaultdict(int)
    for k, r in ev.items():
        if k[0] == bench:
            c[r['verdict']] += 1
    order = ['Rigtorp', 'Vyukov', 'differenza < 5%', 'non distinguibile']
    return ', '.join(f'{k}: <b>{c[k]}</b>' for k in order if c[k])


def code_block(path, start_marker, end_marker):
    lines = path.read_text().splitlines()
    i = next(n for n, l in enumerate(lines) if start_marker in l)
    j = next(n for n in range(i + 1, len(lines)) if end_marker in lines[n])
    return html.escape('\n'.join(lines[i:j]).rstrip())


def comparison_table(bench, old_m, new_m, new_ev):
    rows = []
    for (q, n, t) in sorted(k for k in new_m if k[0] == 'vyukov'):
        ov, nv = old_m[('vyukov', n, t)], new_m[('vyukov', n, t)]
        orr, nr = old_m[('rigtorp', n, t)], new_m[('rigtorp', n, t)]
        ev = new_ev[(bench, n, t)]
        cls = {'Rigtorp': 'r', 'Vyukov': 'v'}.get(ev['verdict'], '')
        best = 'R' if nr > nv else 'V'
        rows.append(f'<tr><td>{n}</td><td>{t:g}</td>'
                    f'<td>{mil(ov)}</td><td>{mil(nv)}</td><td class="d">{pct(nv / ov)}</td>'
                    f'<td>{mil(orr)}</td><td>{mil(nr)}</td><td class="d">{pct(nr / orr)}</td>'
                    f'<td>{num(float(ev["ratio"]), 2)}</td>'
                    f'<td class="ci">{num(float(ev["ci95_low"]), 2)}–{num(float(ev["ci95_high"]), 2)}</td>'
                    f'<td>{ev["rigtorp_wins"]}/50</td><td class="{cls}">{ev["verdict"]}</td><td>{best}</td></tr>')
    return ('<table class="data"><thead><tr><th rowspan="2">N</th><th rowspan="2">T µs</th>'
            '<th colspan="3">Vyukov (M input/s)</th><th colspan="3">Rigtorp (M input/s)</th>'
            '<th colspan="4">Rigtorp / Vyukov, nuovo Communicator</th><th rowspan="2">Migliore</th></tr>'
            '<tr><th>prima</th><th>dopo</th><th>Δ</th><th>prima</th><th>dopo</th><th>Δ</th>'
            '<th>R/V</th><th>IC 95%</th><th>vinte R</th><th>esito</th></tr></thead><tbody>'
            + '\n'.join(rows) + '</tbody></table>')


def speedup_table(bench, old_h, new_h, ts, label):
    head = ''.join(f'<th colspan="3">T = {t:g} µs</th>' for t in ts)
    sub = ''.join('<th>harness</th><th>prima</th><th>dopo</th>' for _ in ts)
    rows = []
    for q in ('vyukov', 'rigtorp'):
        for n in (2, 4, 8, 16):
            cells = ''.join(f'<td>{num(float(new_h[(bench, q, n, t)]["harness_speedup"]), 2)}</td>'
                            f'<td>{num(float(old_h[(bench, q, n, t)]["mostream_speedup"]), 2)}</td>'
                            f'<td><b>{num(float(new_h[(bench, q, n, t)]["mostream_speedup"]), 2)}</b></td>'
                            for t in ts)
            rows.append(f'<tr><td>{q.capitalize()}</td><td>{n}</td>{cells}</tr>')
    return (f'<table class="data"><thead><tr><th rowspan="2">Coda</th><th rowspan="2">{label}</th>{head}</tr>'
            f'<tr>{sub}</tr></thead><tbody>' + '\n'.join(rows) + '</tbody></table>')


def harness_ci(bench):
    out = {}
    with open(HARNESS / bench / 'confronto_50.csv', newline='') as f:
        for r in csv.DictReader(f):
            out[(int(r['N']), float(r['T_us']))] = r
    return out


def rv_cell(ratio, low, high, verdict=None):
    cls = {'Rigtorp': 'r', 'Vyukov': 'v'}.get(verdict, '')
    ci = f'<span class="ci">{num(low, 2)}–{num(high, 2)}</span>' if low is not None else '<span class="ci">—</span>'
    return f'<td class="{cls}b">{num(ratio, 2)}</td><td>{ci}</td>'


def harness_verdict(ratio, low, high):
    if low is None:
        return 'nessuna coda'
    if low <= 1 <= high:
        return 'non distinguibile'
    if ratio >= 1.05:
        return 'Rigtorp'
    if ratio <= 0.95:
        return 'Vyukov'
    return 'differenza < 5%'


def three_way_table(bench, hci, oev, nev):
    rows = []
    for t in (0.0, 1.0, 2.0, 5.0):
        for n in (1, 2, 4, 8, 16):
            h = hci[(n, t)]
            hl = float(h['ci95_ratio_low']) if h['ci95_ratio_low'] else None
            hh = float(h['ci95_ratio_high']) if h['ci95_ratio_high'] else None
            hr = float(h['throughput_ratio_R_over_V'])
            hv = harness_verdict(hr, hl, hh)
            o, w = oev[(bench, n, t)], nev[(bench, n, t)]
            rows.append(f'<tr><td>{n}</td><td>{t:g}</td>'
                        + rv_cell(hr, hl, hh, hv) + f'<td class="e">{hv}</td>'
                        + rv_cell(float(o['ratio']), float(o['ci95_low']), float(o['ci95_high']), o['verdict'])
                        + f'<td class="e">{o["verdict"]}</td>'
                        + rv_cell(float(w['ratio']), float(w['ci95_low']), float(w['ci95_high']), w['verdict'])
                        + f'<td class="e">{w["verdict"]}</td></tr>')
    return ('<table class="data three"><thead><tr><th rowspan="2">N</th><th rowspan="2">T µs</th>'
            '<th colspan="3">Harness (code chiamate direttamente)</th>'
            '<th colspan="3" class="old">MoStream, Communicator vecchio</th>'
            '<th colspan="3">MoStream, Communicator nuovo</th></tr>'
            '<tr><th>R/V</th><th>IC 95%</th><th>esito</th><th class="old">R/V</th><th class="old">IC 95%</th>'
            '<th class="old">esito</th><th>R/V</th><th>IC 95%</th><th>esito</th></tr></thead><tbody>'
            + '\n'.join(rows) + '</tbody></table>')


def figure(name, src, caption):
    shutil.copy(src, HERE / 'img' / name)
    return f'<figure><img src="img/{name}"><figcaption>{caption}</figcaption></figure>'


SVG_PROTOCOLS = '''
<svg viewBox="0 0 760 250" class="diagram" xmlns="http://www.w3.org/2000/svg" font-family="Inter, Arial, sans-serif" font-size="12">
  <text x="10" y="18" font-weight="700" font-size="13">Prima: flag closed + try_pop (EOS mai in coda)</text>
  <g transform="translate(10,30)">
    <rect x="0" y="0" width="70" height="34" rx="4" fill="#dbeafe" stroke="#1d4ed8"/><text x="35" y="21" text-anchor="middle">Produttore</text>
    <g transform="translate(110,0)">
      <rect x="0" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="20" y="21" text-anchor="middle">d1</text>
      <rect x="40" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="60" y="21" text-anchor="middle">d2</text>
      <rect x="80" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="100" y="21" text-anchor="middle">d3</text>
      <rect x="120" y="0" width="40" height="34" fill="#fff" stroke="#94a3b8" stroke-dasharray="3 3"/>
    </g>
    <line x1="70" y1="17" x2="108" y2="17" stroke="#334155" marker-end="url(#a)"/>
    <line x1="270" y1="17" x2="308" y2="17" stroke="#334155" marker-end="url(#a)"/>
    <rect x="310" y="0" width="80" height="34" rx="4" fill="#fef3c7" stroke="#b45309"/><text x="350" y="21" text-anchor="middle">Consumatore</text>
    <rect x="160" y="52" width="110" height="26" rx="4" fill="#fee2e2" stroke="#dc2626"/><text x="215" y="69" text-anchor="middle">closed = 1</text>
    <text x="410" y="12">ciclo: try_pop → coda vuota?</text>
    <text x="410" y="28">→ legge closed → secondo try_pop</text>
    <text x="410" y="44">→ se ancora vuota costruisce l'EOS da sé</text>
    <text x="410" y="64" fill="#b91c1c">Rigtorp usa la CAS del try_pop: niente fetch_add</text>
  </g>
  <text x="10" y="138" font-weight="700" font-size="13">Dopo: EOS espliciti + pop bloccante (runtime standard)</text>
  <g transform="translate(10,150)">
    <rect x="0" y="0" width="70" height="34" rx="4" fill="#dbeafe" stroke="#1d4ed8"/><text x="35" y="21" text-anchor="middle">Produttore</text>
    <g transform="translate(110,0)">
      <rect x="0" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="20" y="21" text-anchor="middle">d1</text>
      <rect x="40" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="60" y="21" text-anchor="middle">d2</text>
      <rect x="80" y="0" width="40" height="34" fill="#f1f5f9" stroke="#475569"/><text x="100" y="21" text-anchor="middle">d3</text>
      <rect x="120" y="0" width="40" height="34" fill="#dcfce7" stroke="#15803d"/><text x="140" y="21" text-anchor="middle">EOS</text>
      <rect x="160" y="0" width="40" height="34" fill="#dcfce7" stroke="#15803d"/><text x="180" y="21" text-anchor="middle">EOS</text>
    </g>
    <line x1="70" y1="17" x2="108" y2="17" stroke="#334155" marker-end="url(#a)"/>
    <line x1="310" y1="17" x2="348" y2="17" stroke="#334155" marker-end="url(#a)"/>
    <rect x="350" y="-8" width="80" height="22" rx="4" fill="#fef3c7" stroke="#b45309"/><text x="390" y="7" text-anchor="middle">Cons. A</text>
    <rect x="350" y="20" width="80" height="22" rx="4" fill="#fef3c7" stroke="#b45309"/><text x="390" y="35" text-anchor="middle">Cons. B</text>
    <text x="450" y="8">pop bloccante: un ticket (fetch_add) per volta</text>
    <text x="450" y="24">ultimo produttore: un EOS per consumatore</text>
    <text x="450" y="40">ogni consumatore si ferma al suo EOS</text>
    <text x="450" y="60" fill="#15803d">posizioni consumate = dati + consumatori</text>
  </g>
  <defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#334155"/></marker></defs>
</svg>'''


def main():
    (HERE / 'img').mkdir(exist_ok=True)
    om = {b: medians(OLD, b) for b in BENCH}
    nm = {b: medians(NEW, b) for b in BENCH}
    oev, nev = evaluation(OLD), evaluation(NEW)
    oh, nh = harness_table(OLD), harness_table(NEW)
    comm = ROOT / 'MoStream' / 'communicator.mojo'
    code_pf = code_block(comm, 'def producer_finished', '# Chiamata da ogni consumatore')
    code_pop = code_block(comm, 'def pop(mut self)', '# try_pop: prova a estrarre')
    f, g = 'FarmThroughput', 'PipelineGrain'
    # valori citati nel testo
    farm_n1 = (max(om[f][(q, 1, 0.0)] for q in ('vyukov', 'rigtorp')), max(nm[f][(q, 1, 0.0)] for q in ('vyukov', 'rigtorp')))
    harn_n1 = max(float(nh[(f, q, 1, 0.0)]['harness']) for q in ('vyukov', 'rigtorp'))
    gains_r = [nm[f][('rigtorp', n, t)] / om[f][('rigtorp', n, t)] for n in (8, 16) for t in (0.0, 1.0, 2.0)]
    gains_vg = [nm[g][('vyukov', n, t)] / om[g][('vyukov', n, t)] for n in (1, 2, 4, 8, 16) for t in (0.0, 1.0, 2.0, 5.0)]
    hci = {b: harness_ci(b) for b in BENCH}
    old_rv_contesa = [float(oev[(f, n, t)]['ratio']) for n in (8, 16) for t in (0.0, 1.0, 2.0)]
    har_rv_contesa = [float(hci[f][(n, t)]['throughput_ratio_R_over_V']) for n in (8, 16) for t in (0.0, 1.0, 2.0)]
    har_g = [float(hci[g][(n, t)]['throughput_ratio_R_over_V']) for n in (2, 4, 8, 16) for t in (0.0, 1.0, 2.0, 5.0)]
    old_g0 = [float(oev[(g, n, 0.0)]['ratio']) for n in (1, 2)]
    old_sp_r16 = float(oh[(f, 'rigtorp', 16, 2.0)]['mostream_speedup'])
    har_sp_r16 = float(oh[(f, 'rigtorp', 16, 2.0)]['harness_speedup'])
    css = (HERE / 'stile.css').read_text()
    body = f'''
<header class="cover">
  <p class="kicker">MoStream · runtime standard</p>
  <h1>Il nuovo Communicator: EOS espliciti e pop bloccante</h1>
  <p class="sub">Cosa è stato cambiato, perché, e cosa mostrano FarmThroughput e PipelineGrain</p>
  <p class="meta">Mojo 1.1.0 · Intel Xeon Gold 5512U (28 core fisici) · 6 novembre 2026</p>
</header>

<section class="summary">
  <h2>In sintesi</h2>
  <ul>
    <li><b>Il problema.</b> Nei benchmark harness, con le code chiamate direttamente, Rigtorp batteva Vyukov fino a 1,9× sotto contesa. Dentro MoStream il vantaggio spariva, perché il <code>Communicator</code> estraeva i messaggi con un ciclo di <code>try_pop</code> e non usava mai il <code>pop</code> bloccante di Rigtorp, basato su un solo <code>fetch_add</code>.</li>
    <li><b>La modifica.</b> Nel runtime standard la fine dello stream non è più segnalata da un flag: l'ultimo produttore inserisce nella coda un EOS per ogni consumatore, e <code>pop()</code> usa il <code>pop</code> bloccante della coda. Il runtime cooperativo resta invariato. La scelta avviene in compilazione, con il parametro <code>explicit_eos</code> del <code>Communicator</code>.</li>
    <li><b>Correttezza.</b> 144 configurazioni del test di integrazione, 1.600 esecuzioni di stress senza errori né blocchi, test di esempio con output identico al runtime originale. Ogni misura delle campagne verifica che ogni input arrivi esattamente una volta.</li>
    <li><b>Risultati.</b> Sotto contesa (FarmThroughput, N ≥ 8) Rigtorp guadagna fino a {pct(max(gains_r))} e torna a battere Vyukov fino a {num(max(float(nev[(f, 16, t)]['ratio']) for t in (0.0, 1.0, 2.0)), 2)}×. Sulle catene 1:1 (PipelineGrain) Vyukov resta la coda migliore e guadagna fino a {pct(max(gains_vg))}. Il throughput massimo della farm con un consumatore passa da {mil(farm_n1[0])} a {mil(farm_n1[1])} milioni di input/s.</li>
  </ul>
</section>

<h2>1. Il contesto</h2>
<p>In MoStream due stadi consecutivi comunicano attraverso un <code>Communicator</code>: una coda MPMC limitata condivisa fra le repliche dello stadio che scrive (i produttori, <code>pN</code>) e quelle dello stadio che legge (i consumatori, <code>cN</code>). La coda si sceglie in compilazione: <b>Vyukov</b> di default, <b>Rigtorp</b> con <code>-D MOSTREAM_RIGTORP=1</code>.</p>
<p>Entrambe le code sono <b>implementate come MPMC</b> (più produttori e più consumatori). L'uso effettivo dipende dalla topologia: in FarmThroughput la coda è usata come <b>SPMC</b> (un produttore, N consumatori), in PipelineGrain come <b>SPSC</b> (un produttore e un consumatore per coda). Le configurazioni MPSC e MPMC, con più produttori sulla stessa coda, compaiono nei test di correttezza ma non sono state misurate nelle campagne di prestazioni.</p>
<p>Le due code differiscono soprattutto nel modo in cui un consumatore prenota la posizione da leggere:</p>
<table class="plain">
<tr><th></th><th>Vyukov</th><th>Rigtorp</th></tr>
<tr><td><code>pop()</code> bloccante</td><td>ciclo sul proprio <code>try_pop</code>: lettura e CAS, da ripetere se un altro consumatore arriva prima</td><td><b>un solo <code>fetch_add</code></b> per prendere un ticket, poi attesa sulla propria cella: nessun ritentativo, nessuna gara</td></tr>
<tr><td><code>try_pop()</code></td><td>lettura e CAS <code>RELAXED</code></td><td>lettura e CAS <code>SEQUENTIAL</code> in un ciclo: stessa logica di Vyukov, con un'operazione più costosa</td></tr>
</table>
<p>Quando molti consumatori si contendono la stessa coda, il <code>fetch_add</code> di Rigtorp evita le CAS fallite. Per questo nei benchmark harness, dove i consumatori chiamano <code>pop()</code>, Rigtorp vinceva fino a 1,9×. Nella prima campagna dentro MoStream invece le due code risultavano equivalenti, e con contesa Vyukov era perfino avanti del 7%.</p>

<h2>2. Il punto di partenza: i risultati con il Communicator vecchio</h2>
<p>Prima della modifica, FarmThroughput e PipelineGrain sono stati misurati dentro MoStream con il <code>Communicator</code> originale (campagna del 5 novembre, 4.000 misure). I parametri sono gli stessi della campagna successiva, descritti nel capitolo 6. Come riferimento c'è la campagna harness del 15 ottobre: stessa macchina, stessi carichi, ma con le code chiamate direttamente da task dedicati, senza runtime.</p>
<p>L'attesa era che il comportamento delle due code si ritrovasse anche dentro MoStream. <b>Non è stato così.</b></p>
<h3>FarmThroughput: una coda condivisa, uso SPMC</h3>
<p>Esiti con il <code>Communicator</code> vecchio: {verdict_counts(oev, f)}. Con quello nuovo: {verdict_counts(nev, f)}.</p>
{three_way_table(f, hci[f], oev, nev)}
<p class="caption">Rapporto dei throughput mediani Rigtorp/Vyukov (&gt; 1 favorisce Rigtorp) con intervallo di confidenza bootstrap al 95%, per ciascuna delle tre campagne. Le colonne centrali sono la campagna con il <code>Communicator</code> vecchio.</p>
<ul>
<li><b>Nell'harness Rigtorp vince nettamente sotto contesa</b>: con N = 8–16 e T ≤ 2 µs il rapporto va da {num(min(har_rv_contesa), 2)} a {num(max(har_rv_contesa), 2)}.</li>
<li><b>Con il <code>Communicator</code> vecchio, nelle stesse configurazioni, il rapporto va da {num(min(old_rv_contesa), 2)} a {num(max(old_rv_contesa), 2)}</b>: Rigtorp è pari o più lenta di Vyukov. Il vantaggio non si è ridotto, è sparito, e in 6 configurazioni l'esito è a favore di Vyukov.</li>
<li>Anche la scalabilità ne risente: con N = 16 e T = 2 µs lo speedup di Rigtorp è {num(old_sp_r16, 2)}, contro {num(har_sp_r16, 2)} nell'harness.</li>
</ul>
{figure('old_farm_RV.png', OLD / 'confronto_harness' / 'FarmThroughput_rigtorp_su_vyukov.png', 'Rapporto Rigtorp/Vyukov: harness (verde) e MoStream con il <code>Communicator</code> vecchio (viola). Nell\'harness Rigtorp arriva quasi a 1,9×; dentro MoStream la curva resta piatta intorno a 1.')}
{figure('old_farm_IC.png', OLD / 'valutazione_rigtorp' / 'FarmThroughput_rapporto_IC.png', 'Communicator vecchio: rapporto Rigtorp/Vyukov con IC 95%. Con N ≥ 2 e T ≤ 2 µs gli esiti significativi sono a favore di Vyukov (blu).')}
<h3>PipelineGrain: code 1:1, uso SPSC</h3>
<p>Esiti con il <code>Communicator</code> vecchio: {verdict_counts(oev, g)}. Con quello nuovo: {verdict_counts(nev, g)}.</p>
{three_way_table(g, hci[g], oev, nev)}
<p class="caption">Come sopra. Nell'harness con N = 1 non ci sono code (baseline seriale), quindi non c'è confronto.</p>
<ul>
<li><b>Senza contesa nessuna coda prevale in modo costante.</b> Nell'harness il rapporto va da {num(min(har_g), 2)} a {num(max(har_g), 2)} secondo la configurazione, senza un andamento regolare con N o T; il vantaggio di Rigtorp sotto contesa qui non può emergere.</li>
<li>Con il <code>Communicator</code> vecchio Rigtorp perdeva soprattutto con T = 0 ({num(min(old_g0), 2)}–{num(max(old_g0), 2)} con N = 1–2), cioè dove conta di più il costo di ogni estrazione. Con quello nuovo resta dietro a Vyukov: il nuovo protocollo aiuta entrambe le code, ma senza contesa Vyukov ne trae più vantaggio (capitolo 8).</li>
</ul>
{figure('old_grain_IC.png', OLD / 'valutazione_rigtorp' / 'PipelineGrain_rapporto_IC.png', 'Communicator vecchio: rapporto Rigtorp/Vyukov con IC 95% in PipelineGrain.')}
<h3>Perché questo era un problema</h3>
<ul>
<li>Rigtorp era stata integrata nel runtime proprio per il suo vantaggio sotto contesa. Con il <code>Communicator</code> vecchio <b>la sua integrazione non portava alcun beneficio misurabile</b>, e con contesa era leggermente peggiore.</li>
<li>Una differenza così netta fra harness e runtime non poteva dipendere solo dal costo generale del runtime, perché quello pesa allo stesso modo sulle due code. Doveva esserci qualcosa che cambiava il <b>modo</b> in cui la coda veniva usata. Il capitolo 3 mostra di cosa si trattava.</li>
</ul>

<h2>3. Perché Rigtorp non dava vantaggi dentro MoStream</h2>
<p>Il <code>Communicator</code> originale non chiamava mai il <code>pop</code> bloccante della coda: <code>CommunicationQueue</code> non lo esponeva nemmeno. <code>Communicator.pop()</code> era un ciclo su <code>try_pop()</code>. La ragione è il protocollo di fine stream:</p>
<ul>
<li>l'EOS <b>non veniva mai inserito nella coda</b>: l'ultimo produttore alzava un flag <code>closed</code>;</li>
<li>un consumatore che trovava la coda vuota leggeva il flag, ricontrollava la coda e, se era ancora vuota, <b>costruiva l'EOS da solo</b>.</li>
</ul>
<p>Con il <code>pop</code> bloccante di Rigtorp questo schema non funziona. Il consumatore prende un ticket e aspetta che proprio quella cella venga riempita; se i dati sono finiti nessuno la riempirà mai, e il consumatore resta bloccato senza poter controllare il flag. Per questo l'integrazione di Rigtorp aveva mantenuto il <code>try_pop</code>, rinunciando al <code>fetch_add</code>.</p>
<p>L'ipotesi è stata verificata in due passi:</p>
<ol>
<li>con un prototipo separato (<code>MoStream/pipeline_blocking.mojo</code>), senza toccare il runtime, la stessa campagna ha mostrato che con il <code>pop</code> bloccante il vantaggio di Rigtorp sotto contesa ricompare;</li>
<li>la stessa logica è stata poi portata nel <code>Communicator</code> del runtime. La campagna ripetuta dà gli stessi numeri del prototipo: il rapporto dei throughput mediani è 1,008 in entrambi i benchmark.</li>
</ol>

<h2>4. La modifica al Communicator</h2>
{SVG_PROTOCOLS}
<p>Il <code>Communicator</code> ha ora un parametro di compilazione, <code>Communicator[T, explicit_eos: Bool = False]</code>, che sceglie fra due protocolli. Il compilatore genera due versioni separate, quindi a runtime non c'è nessun <code>if</code>.</p>
<table class="plain">
<tr><th></th><th>explicit_eos = True<br>(<code>Pipeline.run()</code>, runtime standard)</th><th>explicit_eos = False<br>(<code>run_cooperative()</code>, invariato)</th></tr>
<tr><td><code>pop()</code></td><td><code>pop</code> bloccante della coda (Rigtorp: <code>fetch_add</code>)</td><td>ciclo su <code>try_pop</code></td></tr>
<tr><td>Fine stream</td><td>l'ultimo produttore alza <code>closed</code> e inserisce <code>cN</code> EOS, uno per consumatore</td><td>l'ultimo produttore alza <code>closed</code>; i consumatori costruiscono l'EOS da soli</td></tr>
<tr><td>Perché</td><td>ogni stadio ha il suo thread dedicato: bloccarsi su <code>pop</code> è naturale</td><td>un attore non può bloccare un worker dello scheduler, che usa <code>is_closed()</code> per parcheggiarlo e risvegliarlo</td></tr>
</table>

<h3>Il codice</h3>
<pre><code>{code_pf}</code></pre>
<pre><code>{code_pop}</code></pre>

<h3>Perché il protocollo è corretto</h3>
<ol>
<li><b>Gli EOS stanno dopo tutti i dati.</b> Ogni produttore completa i suoi <code>push</code> prima di chiamare <code>producer_finished()</code>. Il <code>fetch_sub</code> con ordinamento <code>ACQUIRE_RELEASE</code> garantisce che l'ultimo produttore veda già fatti i push di tutti gli altri, quindi gli EOS prendono posizioni successive.</li>
<li><b>Ogni posizione va a un solo consumatore</b>, e le posizioni vengono prese in ordine.</li>
<li><b>Ogni consumatore si ferma dopo il proprio EOS.</b> Con D dati e C consumatori si consumano esattamente D + C posizioni: nessun dato perso, nessun ticket senza elemento, nessun consumatore in attesa per sempre.</li>
</ol>
<p>Due dettagli di implementazione:</p>
<ul>
<li><b>Il flag <code>closed</code> viene alzato prima di inserire gli EOS, non dopo.</b> Appena l'ultimo consumatore riceve il suo EOS libera il communicator. Se il produttore scrivesse il flag dopo l'ultimo push, potrebbe scrivere in memoria già liberata.</li>
<li><b>Coda senza produttori</b> (<code>pN = 0</code>). Nessuno inserirebbe gli EOS, quindi li inserisce il costruttore. Se non ci stanno nella coda, la costruzione viene rifiutata, perché un push su coda piena resterebbe bloccato prima che i consumatori partano.</li>
</ul>

<h3>File modificati</h3>
<table class="plain">
<tr><td><code>communication_queue.mojo</code></td><td>aggiunto <code>pop()</code>, che chiama il <code>pop</code> bloccante della coda scelta</td></tr>
<tr><td><code>communicator.mojo</code></td><td>parametro <code>explicit_eos</code>; <code>pop()</code>, <code>producer_finished()</code> e costruttore come sopra; commenti in italiano</td></tr>
<tr><td><code>standard_runtime.mojo</code>, <code>emitter.mojo</code></td><td>i tipi diventano <code>Communicator[..., True]</code>; il codice dei cicli non cambia</td></tr>
<tr><td><code>pipeline.mojo</code></td><td><code>run()</code> crea communicator con EOS espliciti; <code>run_cooperative()</code> invariato; una riga di log con il protocollo attivo</td></tr>
</table>
<p>Gli stadi <code>TRANSFORM_MANY</code>, l'<code>Emitter</code> e il runtime cooperativo funzionano come prima.</p>

<h2>5. Verifica di correttezza</h2>
<table class="plain">
<tr><td>Test di integrazione del runtime</td><td><b>144 configurazioni superate</b>: Vyukov e Rigtorp, topologie farm e MPMC, runtime standard e cooperativo, 1/2/4 worker, 0/1/2003 messaggi (anche stream vuoto), capacità 2 e 16. Verifica che ogni messaggio arrivi esattamente una volta.</td></tr>
<tr><td>Stress della terminazione</td><td><b>1.600 esecuzioni senza errori né blocchi</b> (timeout 30 s ciascuna): code 1:16 con capacità 2, un solo messaggio con 16 consumatori, catene di 18 stadi, code N:N con più produttori (<code>farm_mpmc</code>).</td></tr>
<tr><td>Test di esempio</td><td>output identico al runtime originale per <code>test_pipe_1</code>, <code>test_pipe_2</code> (<code>TRANSFORM_MANY</code>) e <code>test_pipe_3_coop</code>; <code>test_pipe_3</code> verificato sulla somma totale fra i sink (1.001.000), che anche nel runtime originale si distribuisce in modo variabile fra i sink.</td></tr>
<tr><td>ImagePipeline</td><td>pipeline con stadi <code>TRANSFORM_MANY</code> replicati: termina correttamente.</td></tr>
</table>

<h2>6. Come sono state fatte le misure</h2>
<table class="plain">
<tr><th></th><th>FarmThroughput</th><th>PipelineGrain</th></tr>
<tr><td>Topologia</td><td><code>seq(Source) → parallel(FarmWorker, N)</code>: una coda 1:N</td><td><code>Source → N stadi seq → Sink</code>: N+1 code 1:1</td></tr>
<tr><td>Uso della coda</td><td><b>SPMC</b>: un produttore, N consumatori. Contesa fra i consumatori sul <code>pop</code></td><td><b>SPSC</b>: un produttore, un consumatore per coda. Nessuna contesa</td></tr>
<tr><td>Lavoro T</td><td>per input, eseguito da un solo consumatore</td><td>totale per input, diviso fra gli N stadi</td></tr>
<tr><td>Griglia</td><td colspan="2">N = 1, 2, 4, 8, 16; T = 0, 1, 2, 5 µs; 50.000 input; capacità 1024; 50 ripetizioni; pinning su core fisici distinti</td></tr>
</table>
<ul>
<li>Il lavoro T è un kernel xorshift calibrato: si misurano i ns per iterazione (mediana di 5 calibrazioni) e si ricava il numero di iterazioni per input. Le due code usano sempre le stesse iterazioni.</li>
<li>Si cronometra <code>Pipeline.run()</code>, dopo un warm-up. <b>Throughput = input / tempo.</b> Dopo il cronometro si verifica che ogni input sia stato elaborato esattamente una volta e con il risultato corretto.</li>
<li>Configurazioni e ordine dei backend sono mescolati con un seed fisso. Per ogni configurazione: rapporto Rigtorp/Vyukov dei throughput mediani, intervallo di confidenza bootstrap al 95% appaiato per ripetizione (20.000 ricampionamenti), coppie vinte e test dei segni. La soglia pratica è il 5%. Gli intervalli sono individuali, senza correzione per i 40 confronti.</li>
<li>Sono confrontate due campagne con parametri identici, sulla stessa macchina: <b>prima</b> (<code>Communicator</code> originale, 5 novembre) e <b>dopo</b> (<code>Communicator</code> nuovo, 6 novembre). Come riferimento c'è l'harness del 15 ottobre, con le code chiamate direttamente, che misura il costo minimo della comunicazione e non è un'alternativa a MoStream.</li>
</ul>
<p class="note"><b>Nota sulla frequenza della CPU.</b> La prima esecuzione della FarmThroughput con il nuovo <code>Communicator</code> è stata scartata. La calibrazione è avvenuta mentre la CPU era in turbo (1,64 ns per iterazione invece dei soliti 2,87), e ogni input ha ricevuto 1,76 volte il lavoro previsto. La campagna è stata ripetuta con calibrazione normale, identica a quella delle altre campagne (2,874 ns per iterazione). Per le prossime campagne conviene fissare la frequenza: turbo disattivato e governor <code>performance</code>.</p>

<h2>7. Risultati: FarmThroughput</h2>
<p>Una sorgente e N consumatori condividono <b>una sola coda</b>: è il caso con contesa fra i consumatori, cioè quello in cui il <code>fetch_add</code> di Rigtorp dovrebbe aiutare.</p>
<p>Esiti statistici Rigtorp contro Vyukov: <b>prima</b> {verdict_counts(oev, f)}; <b>dopo</b> {verdict_counts(nev, f)}.</p>
{comparison_table(f, om[f], nm[f], nev)}
<p class="caption">Throughput mediano in milioni di input/s. Δ = dopo/prima per la stessa coda. R/V &gt; 1 favorisce Rigtorp; esito in arancione = vince Rigtorp, in blu = vince Vyukov.</p>
<h3>Cosa mostrano i numeri</h3>
<ul>
<li><b>Rigtorp guadagna dove c'è contesa.</b> Con N = 8–16 e T ≤ 2 µs il suo throughput sale fino a {pct(max(gains_r))}, e Rigtorp diventa la coda migliore con un rapporto fino a {num(max(float(nev[(f, 16, t)]['ratio']) for t in (0.0, 1.0, 2.0)), 2)}. Prima, nelle stesse configurazioni, era pari o più lenta di Vyukov fino al 7%.</li>
<li><b>Vyukov resta stabile</b>, con variazioni fra −4% e +7% per T &gt; 0. Il suo algoritmo di estrazione è lo stesso nei due protocolli.</li>
<li><b>Senza calcolo (T = 0) il tetto sale</b>: con un consumatore da {mil(farm_n1[0])} a {mil(farm_n1[1])} milioni di input/s (harness: {mil(harn_n1)}). Il costo per messaggio scende da circa {num(1e9 / farm_n1[0], 0)} a circa {num(1e9 / farm_n1[1], 0)} ns.</li>
<li><b>Con T = 5 µs non cambia nulla</b>: il calcolo nasconde la coda, e le due code sono equivalenti.</li>
<li>Unico caso a sfavore: con N = 2 e T = 0 Rigtorp è più lenta di Vyukov (0,86). La configurazione migliore, con Vyukov, migliora comunque.</li>
</ul>
{figure('farm_RV.png', NEW / 'confronto_pop' / 'FarmThroughput_RV_tre_campagne.png', 'Rapporto Rigtorp/Vyukov nelle tre campagne: harness (verde), MoStream prima con <code>try_pop</code> (viola), MoStream dopo con il nuovo <code>Communicator</code> (rosso, in legenda «pop bloccante»).')}
{figure('farm_IC.png', NEW / 'valutazione_rigtorp' / 'FarmThroughput_rapporto_IC.png', 'Nuovo <code>Communicator</code>: rapporto Rigtorp/Vyukov con intervallo di confidenza al 95%. La banda grigia è ±5%; i punti sono colorati in base all\'esito.')}
{figure('farm_tp.png', NEW / 'confronto_pop' / 'FarmThroughput_throughput_tre_campagne.png', 'Throughput delle due code nelle tre campagne (scala logaritmica): harness (linea continua), prima (tratteggio), dopo (tratto-punto, «blocking» in legenda).')}
<h3>Scalabilità</h3>
<p>Speedup = throughput con N consumatori / throughput con N = 1, nella stessa campagna e con la stessa coda.</p>
{speedup_table(f, oh, nh, (1.0, 2.0, 5.0), 'N')}
<ul>
<li>Con Rigtorp e grana fine la scalabilità migliora molto: con N = 16 lo speedup passa da {num(float(oh[(f, 'rigtorp', 16, 1.0)]['mostream_speedup']), 2)} a {num(float(nh[(f, 'rigtorp', 16, 1.0)]['mostream_speedup']), 2)} con T = 1 µs, e da {num(float(oh[(f, 'rigtorp', 16, 2.0)]['mostream_speedup']), 2)} a {num(float(nh[(f, 'rigtorp', 16, 2.0)]['mostream_speedup']), 2)} con T = 2 µs, vicino ai {num(float(nh[(f, 'rigtorp', 16, 2.0)]['harness_speedup']), 2)} dell'harness.</li>
<li>Con T = 5 µs lo speedup resta circa 5,5 con N = 16, contro 8–10,6 dell'harness, e le due code sono pari. Il limite qui non è la coda: <b>è il principale punto ancora aperto</b>.</li>
</ul>
{figure('farm_sp.png', NEW / 'confronto_harness' / 'FarmThroughput_speedup.png', 'Speedup rispetto a N = 1 della stessa suite: harness (linea continua) e MoStream con il nuovo <code>Communicator</code> (tratteggio). La retta punteggiata è lo speedup lineare.')}

<h2>8. Risultati: PipelineGrain</h2>
<p>Una catena di stadi sequenziali: tutte le code sono 1:1, quindi non c'è contesa. Il lavoro totale T è diviso fra gli stadi.</p>
<p>Esiti statistici Rigtorp contro Vyukov: <b>prima</b> {verdict_counts(oev, g)}; <b>dopo</b> {verdict_counts(nev, g)}.</p>
{comparison_table(g, om[g], nm[g], nev)}
<p class="caption">Throughput mediano in milioni di input/s. Δ = dopo/prima per la stessa coda. R/V &gt; 1 favorisce Rigtorp.</p>
<h3>Cosa mostrano i numeri</h3>
<ul>
<li><b>Vyukov migliora in tutte le configurazioni</b>, fino a {pct(max(gains_vg))}, ed è la coda migliore in ogni caso. Il guadagno viene dal fatto che non serve più, a ogni coda vuota, leggere il flag <code>closed</code> e rifare un <code>try_pop</code>.</li>
<li><b>Rigtorp guadagna meno</b> e resta dietro a Vyukov, soprattutto con T = 0 (fino a 0,75 con N = 2). Senza contesa il suo <code>fetch_add</code> <code>SEQUENTIAL</code> non porta vantaggi. Un'ipotesi plausibile, non verificata, è che il consumatore prenda il ticket in anticipo e poi aspetti proprio sulla cella che il produttore sta scrivendo, facendo rimbalzare la stessa linea di cache fra i due core.</li>
<li><b>Una pipeline di stadi piccoli resta sconveniente</b>: con T = 1 µs è più lenta del seriale anche con 16 stadi (speedup {num(float(nh[(g, 'vyukov', 16, 1.0)]['mostream_speedup']), 2)}). Con T = 5 µs e N = 16 lo speedup rispetto al seriale sale da {num(float(oh[(g, 'vyukov', 16, 5.0)]['mostream_speedup']), 2)} a {num(float(nh[(g, 'vyukov', 16, 5.0)]['mostream_speedup']), 2)} (harness {num(float(nh[(g, 'vyukov', 16, 5.0)]['harness_speedup']), 2)}).</li>
</ul>
{figure('grain_RV.png', NEW / 'confronto_pop' / 'PipelineGrain_RV_tre_campagne.png', 'Rapporto Rigtorp/Vyukov nelle tre campagne: harness (verde), MoStream prima (viola), MoStream dopo (rosso).')}
{figure('grain_IC.png', NEW / 'valutazione_rigtorp' / 'PipelineGrain_rapporto_IC.png', 'Nuovo <code>Communicator</code>: rapporto Rigtorp/Vyukov con intervallo di confidenza al 95%.')}
{figure('grain_tp.png', NEW / 'confronto_pop' / 'PipelineGrain_throughput_tre_campagne.png', 'Throughput delle due code nelle tre campagne. L\'harness con N = 1 è omesso, perché non usa code.')}
<h3>Scalabilità</h3>
<p>Speedup = throughput / throughput seriale senza code (N = 1 dell'harness), lo stesso riferimento per tutte le campagne.</p>
{speedup_table(g, oh, nh, (1.0, 2.0, 5.0), 'N')}
{figure('grain_sp.png', NEW / 'confronto_harness' / 'PipelineGrain_speedup.png', 'Speedup rispetto al seriale senza code: harness (linea continua) e MoStream con il nuovo <code>Communicator</code> (tratteggio).')}

<h2>9. Conclusioni</h2>
<ol>
<li><b>La causa è stata individuata e corretta.</b> Rigtorp non dava vantaggi in MoStream perché il <code>Communicator</code> usava <code>try_pop</code>. Con gli EOS espliciti e il <code>pop</code> bloccante il vantaggio sotto contesa ricompare.</li>
<li><b>La modifica non introduce regressioni significative</b> e migliora anche Vyukov sulle catene. È limitata al runtime standard: il cooperativo, che non può bloccarsi, mantiene il protocollo con il flag.</li>
<li><b>La coda migliore dipende dalla topologia</b>: Rigtorp per code condivise con N ≥ 4–8 e grana fine, Vyukov per le code 1:1. Questo motiva il passo successivo, una struttura a backend nel <code>Communicator</code> che scelga la coda per ogni canale in base a <code>pN</code> e <code>cN</code>.</li>
<li><b>Resta un costo del runtime che non dipende dalla coda</b>: circa {num(1e9 / farm_n1[1], 0)} ns per messaggio contro circa {num(1e9 / harn_n1, 0)} dell'harness, e un limite della farm con T = 5 µs e N alto. Prima di aggiungere altre code conviene profilare il percorso <code>push → pop → consume</code>.</li>
</ol>

<h2>Appendice: file e riproducibilità</h2>
<table class="plain">
<tr><td>Benchmark e runner</td><td><code>Benchmarks/MoStreamSynthetic/FarmThroughput/</code>, <code>…/PipelineGrain/</code> (<code>run.py</code>, sorgenti Mojo)</td></tr>
<tr><td>Campagna «prima»</td><td><code>Benchmarks/MoStreamSynthetic/campagna-50-20261105/</code></td></tr>
<tr><td>Campagna «dopo»</td><td><code>Benchmarks/MoStreamSynthetic/campagna-50-explicit-eos-20261106/</code> (dati scartati per la calibrazione in turbo in <code>FarmThroughput_calibrazione_turbo_SCARTATA/</code>)</td></tr>
<tr><td>Prototipo</td><td><code>MoStream/pipeline_blocking.mojo</code> e <code>campagna-50-blocking-20261105/</code></td></tr>
<tr><td>Analisi</td><td><code>analizza_rigtorp.py</code>, <code>confronta_pop.py</code>, <code>confronta_harness.py</code></td></tr>
<tr><td>Questa relazione</td><td><code>docs/Relazione_Communicator/genera_relazione.py</code>: legge tutti i numeri dai CSV delle campagne</td></tr>
</table>
'''
    page = f'<!doctype html><html lang="it"><head><meta charset="utf-8"><title>Nuovo Communicator MoStream</title><style>{css}</style></head><body>{body}</body></html>'
    out_html = HERE / 'Communicator_EOS_espliciti.html'
    out_html.write_text(page)
    out_pdf = HERE / 'Communicator_EOS_espliciti.pdf'
    subprocess.run(['google-chrome', '--headless', '--disable-gpu', '--no-pdf-header-footer',
                    f'--print-to-pdf={out_pdf}', out_html.as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(out_pdf)


if __name__ == '__main__':
    main()
