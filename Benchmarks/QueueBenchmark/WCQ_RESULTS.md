# Esperimento wCQ bounded

## Esito

Il bridge wCQ è risultato corretto nei test eseguiti, ma non è la coda più
veloce per il workload MPMC di MoStream. Nella matrice prestazionale completa
non supera mai FAA, Rigtorp o Padded-FAA. Riduce però il divario aumentando la
contesa e, da 4P/4C in poi, supera nettamente la coda CAS originale.

Questa conclusione riguarda l'adattatore implementato nel progetto, non prova
che l'algoritmo wCQ puro sia intrinsecamente più lento. L'adattatore esegue due
ring wCQ, un'indirezione sul payload, un CAS di admission e una chiamata FFI per
operazione. Questi costi non sono presenti nel ring diretto valutato dagli
autori.

## Configurazione

- Data: 2026-07-22.
- CPU: Intel Xeon Gold 5512U, 28 core / 56 thread, un socket.
- Sistema: Linux 5.15 x86-64.
- Compilatori: Mojo 1.0.0b1 e GCC 11.4.0, ottimizzazione `-O3`.
- Workload: 100.000 messaggi per producer, 10 ripetizioni.
- Topologie: 1P/1C, 2P/2C, 4P/4C, 8P/8C, 1P/8C e 8P/1C.
- Capacità logiche: 16, 1024 e 65536.
- Metriche: media del throughput, deviazione standard e IC 95% Student-t.
- Correttezza in ogni campione: count e checksum dei messaggi consumati.
- File dati: `results_wcq.csv`.

Il benchmark non applica pinning esplicito dei worker e la macchina non è stata
isolata a livello di sistema operativo. I risultati molto brevi, soprattutto
1P/1C con capacità 65536, mostrano intervalli di confidenza ampi e non vanno
usati da soli.

## Scalabilità bilanciata, capacità 1024

| P/C | WCQ (Mmsg/s) | IC 95% | CAS | FAA | Rigtorp | Padded-FAA | WCQ/Padded | WCQ/Rigtorp |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,181 | ±0,059 | 4,973 | 6,726 | 3,413 | 4,856 | 0,450x | 0,643x |
| 2 | 2,852 | ±0,530 | 3,609 | 5,677 | 4,851 | 5,556 | 0,539x | 0,638x |
| 4 | 5,377 | ±0,107 | 3,298 | 7,895 | 8,340 | 9,420 | 0,572x | 0,645x |
| 8 | 6,598 | ±0,101 | 2,665 | 9,709 | 8,693 | 11,130 | 0,593x | 0,759x |

Il rapporto riportato nelle ultime due colonne è la media dei rapporti paired
delle dieci ripetizioni, non il semplice rapporto tra le due medie.

Con 8P/8C, wCQ è 2,48 volte più veloce di CAS, ma Padded-FAA è circa
1,69 volte più veloce di wCQ e Rigtorp circa 1,32 volte. Gli IC 95% dei
rapporti wCQ/Padded e wCQ/Rigtorp restano interamente sotto 1.

## Risultato aggregato

Sulle 18 combinazioni topologia/capacità:

- Padded-FAA ha il throughput migliore in 12 casi ed è prima per media
  geometrica (7,953 Mmsg/s).
- Rigtorp vince 2 casi; FAA 3; Hybrid-1 un caso.
- wCQ non vince alcun caso. La sua media geometrica è 3,941 Mmsg/s.
- Il rapporto geometrico wCQ/Padded-FAA è 0,495x; wCQ/Rigtorp 0,575x;
  wCQ/FAA 0,611x.
- wCQ supera CAS in 11 casi su 18 e il bounded-LPRQ sperimentale in 16 su 18.

## Correttezza e limiti formali

La suite nativa verifica full/empty, riuso dopo wrap, `UINT64_MAX`, capacità
esatta, bounded fill concorrente, exact-once, ordine per-producer osservato dai
consumer e matrici bilanciate/sbilanciate a capacità 8, 16, 1024 e 65536.
Esecuzioni ripetute e build ASan/UBSan/TSan sono passate senza errori osservati.

Il nucleo vendorizzato è il wCQ ufficiale al commit
`708c0052872950dcb15b487fa7a5dd77ce2a2746`. La composizione MoStream aggiunge
contatori di admission: un `try_*` può restituire `WOULD_BLOCK` transitorio se
perde il CAS, mentre `push` e `pop` fanno spin/retry. Non si deve quindi
attribuire automaticamente all'intero adattatore la prova di wait-freedom del
ring upstream; manca una prova formale separata della composizione.

## Conclusione applicativa

Per il percorso dati corrente, Padded-FAA resta la scelta predefinita orientata
al throughput. Rigtorp rimane una buona alternativa robusta. wCQ diventa
interessante se la priorità è studiare il progresso wait-free del nucleo, se si
possono trasportare direttamente indici unici evitando l'adattatore, oppure se
si progetta una admission FAA/sharded che non reintroduca un hot spot CAS.
