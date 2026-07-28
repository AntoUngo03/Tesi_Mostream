# Grafici del queue benchmark

Queste figure sono generate da `results_wcq.csv` con
`plot_results.py`, senza dipendenze Python esterne.

Dataset: 100.000 messaggi per produttore, 10 ripetizioni per
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
