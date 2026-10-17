# Benchmark della grana di una pipeline

Il benchmark confronta soltanto le code Rigtorp e Vyukov in una pipeline con **N stadi sequenziali**.
Per ogni input, il lavoro totale T è fisso; ogni stadio ne esegue circa T/N.
Aumentando N, il lavoro si frammenta mentre crescono i passaggi tra stadi.
L'obiettivo è osservare quando il costo della comunicazione limita il throughput.

## Esecuzione

Richiede Linux, Mojo 1.1.0, Python 3 e GCC; non richiede pacchetti Python esterni.
Dalla radice del progetto sulla macchina remota:

```sh
cd /home/ungolo/MoStream
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/run.py"
```

Configurazione predefinita: N = 1, 2, 4, 8, 16; T = 0, 1, 2, 5, 10 microsecondi;
100.000 input; capacità 1024; cinque ripetizioni per configurazione.
N = 1 viene sempre aggiunto come baseline senza comunicazione.

Prova breve:

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/run.py" \
  --stages 1,2,4 --total-us 0,2 --messages 20000 --repeats 2
```

Esempio di una scansione più ampia:

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/run.py" \
  --stages 1,2,4,8,16,24 --total-us 0,0.5,1,2,5 \
  --messages 200000 --repeats 5 --capacity 1024
```

## Cosa viene confrontato

| Nome | Implementazione |
|---|---|
| `rigtorp` | Porting MPMC con ticket fetch-add, turni pari/dispari e Optional per gli slot |
| `vyukov` | MPMC con prenotazione CAS e sequenza per cella, adattata dalla coda del progetto a messaggi UInt64 e Mojo 1.1 |

Entrambe le implementazioni supportano più produttori e consumatori (MPMC).
In questo benchmark ogni collegamento ha un produttore e un consumatore perché
gli stadi sono sequenziali. Le operazioni attendono attivamente.
Gli algoritmi differiscono anche nella rappresentazione degli slot: il risultato
confronta queste implementazioni, non isola soltanto il costo teorico degli atomici.

## Topologia e lavoro

N conta **tutti gli stadi che fanno calcolo**, incluso il primo che genera gli
input e l'ultimo che li raccoglie. Non ci sono source/sink aggiuntivi:

```text
N=1: genera -> calcola T -> raccoglie (un solo task, nessuna coda)
N=4: [calcola T/4] -> coda -> [calcola T/4] -> coda -> [calcola T/4] -> coda -> [calcola T/4]
```

Il runner calibra cinque volte un kernel xorshift dipendente dal valore ricevuto,
usa la mediana dei nanosecondi per iterazione e converte T in un numero di
iterazioni. Tutte le code e tutti gli N usano lo **stesso numero totale di
iterazioni per quel T**. Ogni stadio ne esegue `iterations // N`; i primi
`iterations % N` stadi eseguono una iterazione in più. Il totale resta esatto.

T è un tempo **obiettivo calibrato**, non un tempo garantito: frequenza CPU,
contesa e overhead di chiamata possono cambiare il tempo effettivo. I CSV
registrano target, stima calibrata e minimo/massimo delle iterazioni per stadio.
Per grane inferiori a una iterazione alcuni stadi fanno zero iterazioni.
Il caso T=0 misura comunicazione, scheduling e overhead minimo del kernel.
Non usa sleep o letture dell'orologio a ogni input. Il kernel non viene eliminato
perché il sink controlla il risultato contro un'esecuzione sequenziale di riferimento.

## Misurazione e correttezza

- Il numero di worker del runtime deve essere almeno N; altrimenti l'esecuzione viene rifiutata.
- Per default ogni stadio è fissato a una CPU logica di un core fisico diverso, rispettando l'affinità disponibile.
- Il pinning avviene prima della misura; un errore di pinning fa fallire la prova.
- Una barriera aspetta tutti gli stadi; il cronometro parte appena prima del rilascio.
- Allocazioni, compilazione, warm-up e controllo del risultato sono fuori dal tempo misurato.
- Il tempo include riempimento e svuotamento della pipeline e attesa finale dei task.
- Ogni prova verifica un digest sensibile all'ordine di tutti gli output contro l'oracolo sequenziale; resta un controllo probabilistico, non una prova formale.
- Le configurazioni sono mescolate con seed riproducibile per distribuire gli effetti di riscaldamento e carico esterno.

`--cpus 0,2,4,6` imposta manualmente la lista; `--cpus none` disabilita il pinning.
Il benchmark usa il runtime privato `_asyncrt` di Mojo 1.1 come harness dedicato:
non modifica né migra il runtime principale MoStream. Tutti i backend condividono
lo stesso percorso di comunicazione; non è una misura dell'intero Communicator
MoStream con messaggi EOS e chiusura. Il numero di input è noto a ciascuno stadio.

## Risultati

Ogni esecuzione crea una nuova cartella in `"Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/results/"`:

- `raw.csv`: tutte le ripetizioni, tempo totale, ns/input e input/secondo.
- `summary.csv`: mediana, minimo/massimo, throughput, speedup e efficienza rispetto a N=1.
- `metadata.json`: versione Mojo, CPU, affinità, parametri, calibrazione e seed.

`ns_per_input` è tempo totale / numero di input: **non è la latenza individuale**.
Lo speedup confronta il tempo N=1 e quello con N stadi per lo stesso T e backend.
L'efficienza è speedup/N. Il tempo ideale per input è il valore N=1 diviso N;
la distanza da quel valore include comunicazione, scheduling, fill/drain e
interferenze hardware: non è una misura pura del solo costo della coda.
Per T=0 la baseline è molto piccola; la metrica più utile è il throughput assoluto.

Su una macchina condivisa, ripetere le prove in condizioni di carico simili e
usare abbastanza input perché avvio e svuotamento pesino poco. I risultati di
una prova breve servono alla verifica del benchmark, non a classificare le code.

Grafico esportabile (matplotlib è facoltativo e già disponibile sul server):

```sh
python3 "Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/plot.py" "Benchmarks/BenchMark Vyukov vs RigTorp/PipelineGrain/results/NOME_CARTELLA"
```

Produce `pipeline_grain.png` e `pipeline_grain.pdf`. Le linee sono le mediane;
le fasce indicano minimo e massimo osservati, non intervalli di confidenza.
La curva ideale usa la mediana delle baseline N=1 dei diversi backend.
