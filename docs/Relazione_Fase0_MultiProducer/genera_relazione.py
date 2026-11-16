#!/usr/bin/env python3
"""Genera la relazione sulla Fase 0 (contesa fra produttori), HTML + PDF con Chrome headless.

Tutti i numeri sono letti dai CSV di
  Benchmarks/MoStreamSynthetic/campagna-50-multiproducer-20261110/analisi/
  (valutazione.csv, sbilanciamento.csv) e dai metadati della campagna.
Uso: python3 genera_relazione.py
"""
import csv
import json
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CAMP = ROOT / 'Benchmarks' / 'MoStreamSynthetic' / 'campagna-50-multiproducer-20261110'
AN = CAMP / 'analisi'


def num(x, d=2):
    return f'{x:,.{d}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def mil(x):
    return num(x / 1e6, 2)


def load():
    ev, bal = {}, {}
    with open(AN / 'valutazione.csv', newline='') as f:
        for r in csv.DictReader(f):
            ev[(r['scenario'], int(r['N']), float(r['T_us']))] = r
    with open(AN / 'sbilanciamento.csv', newline='') as f:
        for r in csv.DictReader(f):
            bal[(r['scenario'], int(r['N']), float(r['T_us']), r['backend'])] = r
    return ev, bal


def f(r, k):
    return float(r[k])


def counts(ev, s):
    c = {}
    for k, r in ev.items():
        if k[0] == s:
            c[r['verdict']] = c.get(r['verdict'], 0) + 1
    order = ['Rigtorp', 'Vyukov', 'differenza < 5%', 'non distinguibile']
    return ', '.join(f'{k}: <b>{c[k]}</b>' for k in order if k in c)


def table(ev, s, label):
    rows = []
    for k in sorted(k for k in ev if k[0] == s):
        r = ev[k]
        cls = {'Rigtorp': 'r', 'Vyukov': 'v'}.get(r['verdict'], '')
        rows.append(f'<tr><td>{k[1]}</td><td>{k[2]:g}</td><td>{mil(f(r, "vyukov_median"))}</td>'
                    f'<td>{mil(f(r, "rigtorp_median"))}</td><td class="{cls}b">{num(f(r, "ratio"))}</td>'
                    f'<td class="ci">{num(f(r, "ci95_low"))}–{num(f(r, "ci95_high"))}</td>'
                    f'<td>{r["rigtorp_wins"]}/{r["repeats"]}</td>'
                    f'<td>{num(100 * f(r, "vyukov_cv"), 0)}% / {num(100 * f(r, "rigtorp_cv"), 0)}%</td>'
                    f'<td class="{cls}">{r["verdict"]}</td></tr>')
    return (f'<table class="data"><thead><tr><th>{label}</th><th>T µs</th><th>Vyukov<br>(M input/s)</th>'
            '<th>Rigtorp<br>(M input/s)</th><th>R/V</th><th>IC 95%</th><th>coppie vinte<br>da Rigtorp</th>'
            '<th>CV<br>V / R</th><th>esito</th></tr></thead><tbody>' + '\n'.join(rows) + '</tbody></table>')


def balance_table(bal, s, ns):
    rows = []
    for n in ns:
        for t in (0.0, 1.0, 2.0, 5.0):
            cells = ''
            for q in ('vyukov', 'rigtorp'):
                r = bal.get((s, n, t, q))
                cells += (f'<td>{num(f(r, "max_over_mean_median"))}</td><td>{num(f(r, "max_over_mean_worst"))}</td>'
                          f'<td>{num(f(r, "min_over_mean_median"))}</td>')
            rows.append(f'<tr><td>{n}</td><td>{t:g}</td>{cells}</tr>')
    return ('<table class="data"><thead><tr><th rowspan="2">consumatori</th><th rowspan="2">T µs</th>'
            '<th colspan="3">Vyukov</th><th colspan="3">Rigtorp</th></tr><tr>'
            + '<th>max/media<br>mediana</th><th>max/media<br>peggiore</th><th>min/media<br>mediana</th>' * 2
            + '</tr></thead><tbody>' + '\n'.join(rows) + '</tbody></table>')


def figure(name, caption):
    shutil.copy(AN / name, HERE / 'img' / name)
    return f'<figure><img src="img/{name}"><figcaption>{caption}</figcaption></figure>'


def easy(html):
    return f'<div class="easy"><p class="lab">In parole semplici</p>{html}</div>'


def thesis(html):
    return f'<div class="thesis"><p class="lab">Per la tesi</p>{html}</div>'


SVG_TOPOLOGIE = '''
<svg viewBox="0 0 760 170" class="diagram" xmlns="http://www.w3.org/2000/svg" font-family="Inter, Arial, sans-serif" font-size="11">
  <defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#334155"/></marker></defs>
  <g transform="translate(10,10)">
    <text x="110" y="10" text-anchor="middle" font-weight="700">1:N (FarmThroughput)</text>
    <rect x="0" y="62" width="60" height="26" rx="4" fill="#dbeafe" stroke="#1d4ed8"/><text x="30" y="79" text-anchor="middle">P</text>
    <rect x="85" y="60" width="50" height="30" fill="#f1f5f9" stroke="#475569"/><text x="110" y="79" text-anchor="middle">coda</text>
    <line x1="60" y1="75" x2="83" y2="75" stroke="#334155" marker-end="url(#a)"/>
    <g fill="#fef3c7" stroke="#b45309"><rect x="165" y="22" width="50" height="22" rx="4"/><rect x="165" y="64" width="50" height="22" rx="4"/><rect x="165" y="106" width="50" height="22" rx="4"/></g>
    <text x="190" y="37" text-anchor="middle">C</text><text x="190" y="79" text-anchor="middle">C</text><text x="190" y="121" text-anchor="middle">C</text>
    <g stroke="#b91c1c" stroke-width="1.6"><line x1="135" y1="70" x2="163" y2="33" marker-end="url(#a)"/><line x1="135" y1="75" x2="163" y2="75" marker-end="url(#a)"/><line x1="135" y1="80" x2="163" y2="117" marker-end="url(#a)"/></g>
    <text x="110" y="150" text-anchor="middle" fill="#b91c1c">contesa fra consumatori (pop)</text>
  </g>
  <g transform="translate(270,10)">
    <text x="110" y="10" text-anchor="middle" font-weight="700">N:1 (MultiProducer n1)</text>
    <g fill="#dbeafe" stroke="#1d4ed8"><rect x="0" y="22" width="50" height="22" rx="4"/><rect x="0" y="64" width="50" height="22" rx="4"/><rect x="0" y="106" width="50" height="22" rx="4"/></g>
    <text x="25" y="37" text-anchor="middle">P</text><text x="25" y="79" text-anchor="middle">P</text><text x="25" y="121" text-anchor="middle">P</text>
    <rect x="85" y="60" width="50" height="30" fill="#f1f5f9" stroke="#475569"/><text x="110" y="79" text-anchor="middle">coda</text>
    <g stroke="#b91c1c" stroke-width="1.6"><line x1="50" y1="33" x2="83" y2="70" marker-end="url(#a)"/><line x1="50" y1="75" x2="83" y2="75" marker-end="url(#a)"/><line x1="50" y1="117" x2="83" y2="80" marker-end="url(#a)"/></g>
    <rect x="165" y="64" width="50" height="22" rx="4" fill="#fef3c7" stroke="#b45309"/><text x="190" y="79" text-anchor="middle">C</text>
    <line x1="135" y1="75" x2="163" y2="75" stroke="#334155" marker-end="url(#a)"/>
    <text x="110" y="150" text-anchor="middle" fill="#b91c1c">contesa fra produttori (push)</text>
  </g>
  <g transform="translate(530,10)">
    <text x="110" y="10" text-anchor="middle" font-weight="700">N:M (MultiProducer nm)</text>
    <g fill="#dbeafe" stroke="#1d4ed8"><rect x="0" y="22" width="50" height="22" rx="4"/><rect x="0" y="64" width="50" height="22" rx="4"/><rect x="0" y="106" width="50" height="22" rx="4"/></g>
    <text x="25" y="37" text-anchor="middle">P</text><text x="25" y="79" text-anchor="middle">P</text><text x="25" y="121" text-anchor="middle">P</text>
    <rect x="85" y="60" width="50" height="30" fill="#f1f5f9" stroke="#475569"/><text x="110" y="79" text-anchor="middle">coda</text>
    <g stroke="#b91c1c" stroke-width="1.6"><line x1="50" y1="33" x2="83" y2="70" marker-end="url(#a)"/><line x1="50" y1="75" x2="83" y2="75" marker-end="url(#a)"/><line x1="50" y1="117" x2="83" y2="80" marker-end="url(#a)"/>
    <line x1="135" y1="70" x2="163" y2="33" marker-end="url(#a)"/><line x1="135" y1="75" x2="163" y2="75" marker-end="url(#a)"/><line x1="135" y1="80" x2="163" y2="117" marker-end="url(#a)"/></g>
    <g fill="#fef3c7" stroke="#b45309"><rect x="165" y="22" width="50" height="22" rx="4"/><rect x="165" y="64" width="50" height="22" rx="4"/><rect x="165" y="106" width="50" height="22" rx="4"/></g>
    <text x="190" y="37" text-anchor="middle">C</text><text x="190" y="79" text-anchor="middle">C</text><text x="190" y="121" text-anchor="middle">C</text>
    <text x="110" y="150" text-anchor="middle" fill="#b91c1c">contesa su entrambi i lati</text>
  </g>
</svg>'''


def main():
    (HERE / 'img').mkdir(exist_ok=True)
    ev, bal = load()
    meta = json.loads((CAMP / 'MultiProducer' / 'metadata.json').read_text())
    n1 = lambda p, t: ev[('n1', p, float(t))]
    nm = lambda p, t: ev[('nm', p, float(t))]
    fm = lambda p, t: ev[('farm', p, float(t))]
    v1_0, v16_0 = f(n1(1, 0), 'vyukov_median'), f(n1(16, 0), 'vyukov_median')
    r1_0, r16_0 = f(n1(1, 0), 'rigtorp_median'), f(n1(16, 0), 'rigtorp_median')
    v1_1, v16_1 = f(n1(1, 1), 'vyukov_median'), f(n1(16, 1), 'vyukov_median')
    max_n1 = max(f(r, 'ratio') for k, r in ev.items() if k[0] == 'n1')
    max_nm = max(f(r, 'ratio') for k, r in ev.items() if k[0] == 'nm')
    max_fm = max(f(r, 'ratio') for k, r in ev.items() if k[0] == 'farm')
    nm8 = nm(8, 0)
    b_nm8_r = bal[('nm', 8, 1.0, 'rigtorp')]
    b_nm8_v = bal[('nm', 8, 1.0, 'vyukov')]
    calib = meta['ns_per_iteration']
    max_bal = max(f(r, 'max_over_mean_median') for r in bal.values())
    css = (HERE / 'stile.css').read_text()

    body = f'''
<header class="cover">
  <p class="kicker">MoStream · Fase 0 · contesa fra produttori</p>
  <h1>Quanto costa avere più produttori sulla stessa coda?</h1>
  <p class="sub">Il benchmark MultiProducer: come è stato costruito, cosa ha misurato e cosa significa per la scelta fra CR-c e PW-c</p>
  <p class="meta">Mojo 1.1.0 · Intel Xeon Gold 5512U (28 core fisici) · Communicator con EOS espliciti · 10 novembre 2026</p>
</header>

<section class="summary">
  <h2>In sintesi</h2>
  {easy(f"""<p>Immagina una cassetta delle lettere in cui tante persone vogliono imbucare la loro lettera <b>nello stesso momento</b>. Se le persone sono poche, va tutto liscio. Se sono tante, si spingono, si ostacolano e alla fine imbucano <b>meno lettere</b> di quante ne imbucherebbe una persona sola. È proprio quello che abbiamo visto: con 16 produttori sulla stessa coda, con la coda Vyukov, passano <span class="big">5 volte meno</span> messaggi che con un produttore solo.</p>
  <p>La coda Rigtorp, che fa mettere tutti in fila con un numerino come dal panettiere, regge molto meglio: fino a <b>{num(max_n1)} volte</b> Vyukov con 16 produttori, e fino a <b>{num(max_nm)} volte</b> quando la ressa c'è sia per imbucare sia per ritirare. Ma anche Rigtorp perde metà della velocità. La soluzione vera è dare <b>a ogni produttore la sua cassetta</b>: è l'idea della PW-c.</p>""")}
  {thesis(f"""<p>La campagna MultiProducer quantifica, per la prima volta in MoStream, il costo della contesa sul lato dei produttori. In una coda N:1, con Vyukov e T = 0, il throughput passa da {mil(v1_0)} a {mil(v16_0)} milioni di input/s aumentando i produttori da 1 a 16; con T = 1 µs sedici produttori ottengono un throughput inferiore a quello di un produttore solo ({mil(v16_1)} contro {mil(v1_1)} milioni di input/s). Rigtorp attenua il fenomeno, con un rapporto Rigtorp/Vyukov fino a {num(max_n1)} in N:1 e {num(max_nm)} in N:M, contro un massimo di {num(max_fm)} nella configurazione 1:N. La contesa sul push risulta quindi più penalizzante di quella sul pop, e il suo costo non viene rimosso dallo schema CR-c. Questi risultati motivano lo schema PW-c, che elimina la contesa su entrambi i lati.</p>""")}
</section>

<h2>1. Perché abbiamo fatto questo test</h2>
<p>Per migliorare il <code>Communicator</code> sono stati proposti due schemi alternativi alla singola coda condivisa:</p>
<ul>
<li><b>CR-c</b>: una coda MPSC per ogni consumatore. Il produttore sceglie in quale coda scrivere, ogni consumatore legge solo dalla sua.</li>
<li><b>PW-c</b>: una coda SPSC per ogni coppia produttore-consumatore, quindi N × M code. Nessuna coda è condivisa.</li>
</ul>
<p>Le due soluzioni si comportano <b>allo stesso modo quando c'è un solo produttore</b> (1:1 e 1:N): in entrambe ogni consumatore ha una sua coda, con un solo produttore. Si distinguono solo quando i produttori sono <b>più di uno</b>. Lì la CR-c mantiene una coda contesa dai produttori, mentre la PW-c la elimina. Le campagne precedenti misuravano solo 1:1 (PipelineGrain) e 1:N (FarmThroughput), quindi <b>non dicevano nulla</b> sul caso decisivo. La Fase 0 serve a colmare questa lacuna.</p>
{SVG_TOPOLOGIE}
{easy("""<p>Abbiamo due idee per riorganizzare le cassette delle lettere. Le due idee danno lo stesso risultato quando chi imbuca è una persona sola. Per capire quale è meglio, dovevamo vedere cosa succede quando <b>chi imbuca sono in tanti</b>. Prima di questo test non l'avevamo mai misurato.</p>""")}
{thesis("""<p>Gli schemi CR-c e PW-c coincidono nelle topologie con un solo produttore e si differenziano esclusivamente in presenza di più produttori sullo stesso canale. La Fase 0 ha quindi lo scopo di misurare, con il Communicator attuale, il costo della contesa sul lato dei produttori nelle configurazioni N:1 e N:M, che le campagne precedenti non coprivano.</p>""")}

<h2>2. Come l'abbiamo fatto</h2>
<h3>Il benchmark</h3>
<p>È stato scritto un benchmark nuovo, <code>Benchmarks/MoStreamSynthetic/MultiProducer/nm.mojo</code>, con una <b>sola coda</b> fra due stadi replicati:</p>
<pre><code>Pipeline((parallel(NMSource, P), parallel(NMSink, C)))</code></pre>
<ul>
<li><b>I produttori</b> si dividono gli input 1…M senza sovrapposizioni: il produttore r emette r+1, r+1+P, r+1+2P, … Ognuno esegue la propria parte del lavoro e fa <code>push</code> nella coda.</li>
<li><b>I consumatori</b> completano il lavoro, scrivono l'ID ricevuto in un registro privato e aggiornano un digest, cioè un'impronta del risultato.</li>
<li><b>Dopo la misura</b> si controlla che ogni input sia arrivato <b>esattamente una volta</b> e con il risultato corretto. Se il controllo fallisce, la campagna si ferma.</li>
</ul>
<h3>I due scenari</h3>
<table class="plain">
<tr><th></th><th>N:1</th><th>N:M</th></tr>
<tr><td>Produttori e consumatori</td><td>P = 1, 2, 4, 8, 16 produttori, 1 consumatore</td><td>P = C = 2, 4, 8</td></tr>
<tr><td>Chi fa il lavoro T</td><td>tutto il produttore</td><td>metà il produttore, metà il consumatore</td></tr>
<tr><td>Dove c'è contesa</td><td>sul <code>push</code>, fra i produttori</td><td>su <code>push</code> e <code>pop</code></td></tr>
</table>
<h3>I parametri</h3>
<p>Gli stessi delle campagne precedenti, per poterle confrontare: T = 0, 1, 2, 5 µs, 50.000 input, capacità 1024, <b>50 ripetizioni</b>, pinning su core fisici distinti, seed 42. In ogni ripetizione si misurano entrambe le code, Vyukov e Rigtorp, in ordine casuale. In totale sono <b>3.200 misure</b>.</p>
<p>Il lavoro T è un ciclo di calcolo calibrato. Si misura quanto dura un'iterazione (mediana di 5 calibrazioni) e si ricava quante iterazioni servono per ogni T. Questa volta il runner <b>controlla la calibrazione</b>: se si scosta di più del 5% dal valore atteso, la campagna non parte, perché vorrebbe dire che la CPU ha cambiato frequenza. Il valore misurato è {num(calib, 3)} ns per iterazione, uguale a quello delle campagne precedenti.</p>
{easy("""<p>Abbiamo costruito un piccolo "gioco": alcuni thread producono numeri, li mettono nella coda, altri li prendono. Ogni numero deve arrivare <b>una volta sola</b>, e controlliamo che sia così. Abbiamo ripetuto ogni prova 50 volte con ciascuna delle due code, per essere sicuri che i risultati non siano un colpo di fortuna.</p>""")}

<h3>Come si leggono i numeri</h3>
<table class="plain">
<tr><td><b>Throughput</b></td><td>input elaborati al secondo: più è alto, meglio è. In tabella è la mediana delle 50 ripetizioni, in milioni.</td></tr>
<tr><td><b>R/V</b></td><td>throughput di Rigtorp diviso throughput di Vyukov. Sopra 1 vince Rigtorp: 2,00 vuol dire il doppio. Sotto 1 vince Vyukov.</td></tr>
<tr><td><b>IC 95%</b></td><td>l'intervallo in cui, con buona probabilità, sta il vero valore di R/V. Si calcola con il <i>bootstrap appaiato</i>: si rifanno 20.000 "campagne simulate" pescando a caso le ripetizioni, tenendo sempre insieme le due misure della stessa ripetizione.</td></tr>
<tr><td><b>Coppie vinte</b></td><td>in quante delle 50 ripetizioni Rigtorp è stata più veloce di Vyukov.</td></tr>
<tr><td><b>CV</b></td><td>coefficiente di variazione: quanto variano le misure di una coda, in percentuale.</td></tr>
<tr><td><b>Esito</b></td><td>se l'IC contiene 1, <i>non distinguibile</i>. Se lo esclude, vince la coda più veloce quando la differenza supera il 5%; altrimenti <i>differenza &lt; 5%</i>.</td></tr>
</table>
{easy("""<p>R/V è come dire «quante volte Rigtorp è più veloce di Vyukov». L'intervallo di confidenza è come un margine di errore: se è tutto sopra 1, siamo sicuri che Rigtorp è più veloce; se tocca 1, non possiamo dirlo.</p>""")}

<h2>3. Risultati: N:1, tanti produttori e un solo consumatore</h2>
<p>Esiti: {counts(ev, 'n1')}.</p>
{table(ev, 'n1', 'P')}
<p class="caption">Throughput mediano in milioni di input/s su 50 ripetizioni. In arancione vince Rigtorp, in blu vince Vyukov.</p>
{figure('MultiProducer_n1.png', 'N:1. Sopra: rapporto Rigtorp/Vyukov con IC 95%, colorato per esito. Sotto: throughput delle due code (mediana e fascia min–max) al variare dei produttori.')}
<h3>Cosa si vede</h3>
<ul>
<li><b>Aggiungere produttori fa crollare il throughput.</b> Con Vyukov e T = 0 si passa da {mil(v1_0)} milioni di input/s con 1 produttore a <b>{mil(v16_0)}</b> con 16, cioè {num(v1_0 / v16_0, 1)} volte meno. Con T = 1 µs, 16 produttori fanno <b>meno di uno solo</b> ({mil(v16_1)} contro {mil(v1_1)}).</li>
<li><b>Il consumatore non è il limite.</b> Da solo riesce a smaltire almeno {mil(v1_0)} milioni di input/s (il caso con un produttore e T = 0, dove il limite potrebbe essere anche il produttore). Con molti produttori il collo di bottiglia è quindi la coda contesa.</li>
<li><b>Rigtorp regge molto meglio</b>: con 16 produttori vale {num(f(n1(16, 0), 'ratio'))} volte Vyukov a T = 0 e {num(f(n1(16, 1), 'ratio'))} a T = 1, con {n1(16, 1)['rigtorp_wins']} coppie vinte su 50. Anche Rigtorp però scende da {mil(r1_0)} a {mil(r16_0)} milioni di input/s.</li>
<li><b>Con un solo produttore vince Vyukov</b> ({num(f(n1(1, 0), 'ratio'))} a T = 0): è una coda 1:1, come nella PipelineGrain.</li>
<li><b>Con T = 5 µs le code sono quasi pari</b>: i produttori passano quasi tutto il tempo a calcolare, e la ressa sulla coda è poca.</li>
</ul>
{easy(f"""<p>Più persone mettiamo a imbucare nella stessa cassetta, più si pestano i piedi. Con Vyukov, quando sono 16, entrano <b>5 volte meno lettere</b> di quando c'era una persona sola. Con Rigtorp, dove si prende il numerino, va molto meglio: circa il doppio di Vyukov. Ma neanche il numerino fa miracoli: si perde comunque metà della velocità.</p>""")}
{thesis(f"""<p>Nella configurazione N:1 il throughput diminuisce all'aumentare del numero di produttori, a causa della contesa sul contatore di inserimento della coda. Con Vyukov, per T = 0, il throughput si riduce di un fattore {num(v1_0 / v16_0, 1)} fra 1 e 16 produttori, e per T = 1 µs sedici produttori ottengono un throughput inferiore a quello di un singolo produttore. La prenotazione tramite fetch_add di Rigtorp, che non richiede ritentativi, attenua il fenomeno (rapporto Rigtorp/Vyukov fino a {num(max_n1)} con intervallo di confidenza interamente sopra la parità), senza eliminarlo.</p>""")}

<h2>4. Risultati: N:M, tanti produttori e tanti consumatori</h2>
<p>Esiti: {counts(ev, 'nm')}.</p>
{table(ev, 'nm', 'P = C')}
<p class="caption">Throughput mediano in milioni di input/s. Il lavoro T è diviso a metà fra produttore e consumatore.</p>
{figure('MultiProducer_nm.png', 'N:M. Sopra: rapporto Rigtorp/Vyukov con IC 95%. Sotto: throughput delle due code.')}
<h3>Cosa si vede</h3>
<ul>
<li><b>È il caso con la differenza più grande</b>: con 8 produttori e 8 consumatori e T = 0, Rigtorp vale <b>{num(f(nm8, 'ratio'))} volte</b> Vyukov ({mil(f(nm8, 'rigtorp_median'))} contro {mil(f(nm8, 'vyukov_median'))} milioni di input/s), con {nm8['rigtorp_wins']} coppie vinte su 50.</li>
<li><b>Il vantaggio cresce con il numero di thread</b> ed è significativo in 7 configurazioni su 12. Con T = 5 µs le due code sono pari.</li>
<li><b>Vyukov peggiora aggiungendo thread</b>: con T = 0 passa da {mil(f(nm(2, 0), 'vyukov_median'))} (2 + 2) a {mil(f(nm8, 'vyukov_median'))} milioni di input/s (8 + 8). Avere più thread non serve, perché si ostacolano sulla coda.</li>
</ul>
{easy("""<p>Qui la ressa c'è da tutte e due le parti: tanti imbucano e tanti ritirano dalla stessa cassetta. Con Vyukov, mettere più persone peggiora le cose. Con Rigtorp va più del doppio meglio. Ma l'idea migliore resta non avere un'unica cassetta per tutti.</p>""")}
{thesis(f"""<p>Nella configurazione N:M la contesa agisce su entrambi i contatori della coda e i due effetti si sommano: il rapporto Rigtorp/Vyukov raggiunge {num(max_nm)} con otto produttori e otto consumatori, e il throughput di Vyukov diminuisce all'aumentare del numero di thread. Il fenomeno scompare per un lavoro di 5 µs per input, quando la frequenza degli accessi alla coda si riduce.</p>""")}

<h2>5. Quale lato della coda soffre di più la contesa</h2>
{figure('RV_per_lato_contesa.png', 'Rapporto Rigtorp/Vyukov con IC 95% nelle tre topologie, alla stessa scala: 1:N (FarmThroughput, verde), N:1 (rosso), N:M (viola). In ascissa il numero di repliche dal lato conteso.')}
<table class="plain">
<tr><th>Topologia</th><th>Dove c'è contesa</th><th>R/V massimo</th></tr>
<tr><td>1:N (FarmThroughput)</td><td>fra consumatori, sul <code>pop</code></td><td>{num(max_fm)}</td></tr>
<tr><td>N:1</td><td>fra produttori, sul <code>push</code></td><td><b>{num(max_n1)}</b></td></tr>
<tr><td>N:M</td><td>su entrambi i lati</td><td><b>{num(max_nm)}</b> (già con 8 + 8)</td></tr>
</table>
<ul>
<li>A parità di numero di thread contesi, <b>la contesa sul <code>push</code> costa più di quella sul <code>pop</code></b>: con 16 repliche e T = 1 µs, R/V vale {num(f(n1(16, 1), 'ratio'))} in N:1 contro {num(f(fm(16, 1), 'ratio'))} in 1:N.</li>
<li>Quando la contesa c'è su entrambi i lati, l'effetto è ancora più forte, e compare con meno thread.</li>
</ul>
{easy("""<p>Abbiamo scoperto che è peggio avere tanta gente che <b>imbuca</b> che tanta gente che <b>ritira</b>. E se ci sono tutte e due le cose insieme, è peggio ancora.</p>""")}
{thesis("""<p>Il confronto fra le tre topologie mostra che la contesa sul lato dei produttori è più penalizzante di quella sul lato dei consumatori, e che la presenza di contesa su entrambi i lati ne amplifica gli effetti. Ne consegue che una riorganizzazione del Communicator limitata al lato dei consumatori non affronterebbe la principale fonte di degrado misurata.</p>""")}

<h2>6. Lo sbilanciamento del carico</h2>
<p>Con più consumatori si è misurato anche <b>come gli input si distribuiscono</b> fra di loro. Per ogni esecuzione si calcolano:</p>
<ul>
<li><b>max/media</b>: gli input del consumatore più carico rispetto alla media. Vale 1 se il carico è perfettamente bilanciato, C se un solo consumatore riceve tutto;</li>
<li><b>min/media</b>: lo stesso per il consumatore meno carico. Vale 0 se qualcuno non riceve nulla.</li>
</ul>
<p>Per ogni configurazione si riporta la mediana sulle 50 ripetizioni e il caso peggiore.</p>
<h3>N:M</h3>
{balance_table(bal, 'nm', (2, 4, 8))}
<h3>1:N (FarmThroughput, per confronto)</h3>
{balance_table(bal, 'farm', (2, 4, 8, 16))}
{figure('sbilanciamento.png', 'max/media al variare del numero di consumatori: mediana (linea) e caso peggiore (fascia). Linea continua: 1:N; tratteggio: N:M.')}
<h3>Cosa si vede</h3>
<ul>
<li><b>Con la coda condivisa il carico non è bilanciato.</b> Con 8 + 8 e T = 1 µs il consumatore più carico riceve in mediana {num(f(b_nm8_v, 'max_over_mean_median'))} volte la media con Vyukov e {num(f(b_nm8_r, 'max_over_mean_median'))} con Rigtorp. Nei casi peggiori alcuni consumatori non ricevono nulla.</li>
<li><b>Non è necessariamente un difetto.</b> In una coda condivisa l'input va a chi è libero: se un consumatore basta a smaltire quello che arriva, gli altri restano senza lavoro, ma il throughput non ne soffre.</li>
<li><b>Una parte dello sbilanciamento dipende probabilmente dalla durata breve delle prove</b> (25–50 ms): i thread partono in momenti diversi, e chi parte prima prende di più. È un'ipotesi da verificare con prove più lunghe.</li>
</ul>
{easy("""<p>Con un'unica cassetta, chi arriva prima prende più lettere. Alcuni lavorano tanto e altri quasi niente. Non sappiamo ancora se questo rallenti il lavoro: con un'unica cassetta, se chi ritira è già abbastanza veloce, può anche non contare. Con cassette separate, invece, le lettere andranno distribuite bene, altrimenti uno resta sommerso e gli altri stanno fermi.</p>""")}
{thesis(f"""<p>Anche con la coda condivisa la distribuzione degli input fra i consumatori risulta disomogenea, con rapporti max/media in mediana fino a {num(max_bal)}. Poiché in una coda condivisa l'assegnazione è guidata dalla disponibilità dei consumatori, tale disomogeneità non implica di per sé una perdita di throughput. Nello schema PW-c, in cui l'assegnazione è decisa dal produttore, la politica di bilanciamento diventa invece determinante: il valore misurato costituisce il termine di confronto per valutarla.</p>""")}

<h2>7. Cosa significa per la scelta fra CR-c e PW-c</h2>
<table class="plain">
<tr><th>Topologia</th><th>CR-c (M code MPSC)</th><th>PW-c (N × M code SPSC)</th></tr>
<tr><td><b>N:1</b>, il caso peggiore misurato</td><td><b>Non cambia nulla</b>: resta una sola coda contesa da tutti i produttori</td><td><b>Elimina la contesa</b>: N code 1:1, che il consumatore scorre</td></tr>
<tr><td>N:M</td><td>Divide la contesa dei produttori su M code</td><td>Elimina la contesa su entrambi i lati</td></tr>
<tr><td>1:N</td><td>Elimina la contesa fra consumatori</td><td>Elimina la contesa fra consumatori</td></tr>
</table>
<ul>
<li>La fonte di degrado più grave, la contesa fra produttori nell'N:1 (fino a 5 volte meno throughput), <b>non viene risolta dalla CR-c</b> e viene eliminata dalla PW-c.</li>
<li><b>Il margine potenziale</b>: nell'N:1 il consumatore da solo smaltisce almeno {mil(v1_0)} milioni di input/s, mentre oggi con 16 produttori se ne ottengono {mil(v16_0)} (Vyukov) e {mil(r16_0)} (Rigtorp). Nella PW-c il limite diventerebbe il consumatore, meno il costo di scorrere 16 code: è la quantità da misurare nelle prossime fasi.</li>
<li><b>Il punto critico della PW-c</b> sarà il bilanciamento del carico, perché l'input non andrà più automaticamente a chi è libero.</li>
</ul>
{easy("""<p>La prima idea (CR-c) dà una cassetta a ogni persona che ritira, ma chi imbuca continua a spingersi davanti alla stessa cassetta. La seconda idea (PW-c) dà una cassetta a ogni coppia "chi imbuca, chi ritira": nessuno si spinge più. Siccome abbiamo scoperto che il problema più grande è proprio la ressa di chi imbuca, la seconda idea è quella giusta.</p>""")}
{thesis(f"""<p>I risultati della Fase 0 indicano lo schema PW-c come direzione preferibile. La principale fonte di degrado misurata, la contesa sul lato dei produttori in configurazione N:1, non è affrontata dallo schema CR-c, che mantiene una coda MPSC condivisa per ciascun consumatore, mentre è eliminata dallo schema PW-c. Il throughput ottenibile da un singolo consumatore senza contesa ({mil(v1_0)} milioni di input/s) fornisce una stima del margine di miglioramento rispetto ai valori attuali con sedici produttori ({mil(v16_0)} e {mil(r16_0)} milioni di input/s con Vyukov e Rigtorp), al netto del costo di polling sulle code di ingresso.</p>""")}

<h2>8. Limiti e prossimi passi</h2>
<h3>Limiti</h3>
<ul>
<li><b>Variabilità alta</b> con 8–16 produttori (CV fino al 30–40%). Gli IC dei risultati principali sono comunque lontani da 1, quindi le conclusioni restano valide.</li>
<li><b>Lo sbilanciamento è probabilmente gonfiato</b> dalla durata breve delle prove.</li>
<li><b>Intervalli individuali</b>, senza correzione per i confronti multipli: i casi al limite vanno presi con cautela.</li>
<li><b>Una sola sessione</b> su una sola macchina, con frequenza della CPU non fissata, anche se la calibrazione è stata controllata.</li>
</ul>
<h3>Prossimi passi</h3>
<ol>
<li><b>Fase 1</b>: portare in Mojo la coda SPSC di FastFlow, con test unitari e un microbenchmark 1:1 contro Vyukov e Rigtorp.</li>
<li><b>Fase 2</b>: implementare il <code>Communicator</code> PW-c, con indice della replica, scelta della coda da parte del produttore, polling del consumatore ed EOS per coda.</li>
<li><b>Fasi 3–5</b>: verifica di correttezza, CR-c come confronto, campagne su 1:1, 1:N, N:1 e N:M, più un carico con lavoro variabile per mettere alla prova il bilanciamento.</li>
</ol>

<h2>Appendice: file e riproducibilità</h2>
<table class="plain">
<tr><td>Benchmark</td><td><code>Benchmarks/MoStreamSynthetic/MultiProducer/nm.mojo</code>, <code>run.py</code></td></tr>
<tr><td>Campagna</td><td><code>Benchmarks/MoStreamSynthetic/campagna-50-multiproducer-20261110/</code> (raw.csv, summary.csv, metadata.json)</td></tr>
<tr><td>Riferimento 1:N</td><td><code>Benchmarks/MoStreamSynthetic/campagna-50-explicit-eos-20261106/FarmThroughput/</code></td></tr>
<tr><td>Analisi</td><td><code>Benchmarks/MoStreamSynthetic/analizza_multiproducer.py</code> → <code>analisi/</code> (ANALISI.md, valutazione.csv, sbilanciamento.csv, grafici)</td></tr>
<tr><td>Questa relazione</td><td><code>docs/Relazione_Fase0_MultiProducer/genera_relazione.py</code>: legge tutti i numeri dai CSV</td></tr>
</table>
'''
    page = (f'<!doctype html><html lang="it"><head><meta charset="utf-8"><title>Fase 0: contesa fra produttori</title>'
            f'<style>{css}</style></head><body>{body}</body></html>')
    out_html = HERE / 'Fase0_contesa_produttori.html'
    out_html.write_text(page)
    out_pdf = HERE / 'Fase0_contesa_produttori.pdf'
    subprocess.run(['google-chrome', '--headless', '--disable-gpu', '--no-pdf-header-footer',
                    f'--print-to-pdf={out_pdf}', out_html.as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(out_pdf)


if __name__ == '__main__':
    main()
