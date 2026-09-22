# Relazione sulla FAA cooperativa

- [PDF: confronto e relazione tecnica, 8 pagine](relazione_faa_cooperativa.pdf)
- [Testo modificabile](relazione_faa_cooperativa.md)
- [Misure originali e protocollo](../../Benchmarks/RealCooperativeQueueBenchmark/README.md)

Il confronto comprende Vyukov, PaddedFAA normale, PaddedFAA con risvegli
broadcast e FAA cooperativa nella pipeline reale. Riporta throughput,
intervalli di confidenza, latenza campionata, tempo totale, funzionamento
dell'integrazione e condizioni nelle quali la variante conviene.

La conclusione e' condizionata: quattro worker e batch 8 favoriscono la FAA,
soprattutto con poco calcolo nello stage. Con un solo worker o batch 1 non
emerge una superiorita' generale. Non viene estesa la classifica ad altre
code del repository non misurate in questa campagna.

Per rigenerare PDF e testo dalla radice del repository:

```bash
python3 Documentazione/CooperativeFAA/genera_pdf.py
```

Il generatore richiede matplotlib. Verifica hash di dati e sorgenti,
completezza della matrice e corrispondenza degli aggregati con il CSV grezzo;
ricalcola gli intervalli prima di produrre il documento. Le anteprime delle
pagine sono generate in `/tmp/mostream-faa-report-preview`.
