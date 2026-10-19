# Descrive lo scopo del file; questa stringa è la documentazione del modulo.
"""Compila il porting isolatamente dal runtime MoStream, con Mojo 1.1."""
# Importa Path per costruire e leggere percorsi senza concatenarli manualmente.
from pathlib import Path
# Importa subprocess per avviare il compilatore e attendere la fine dei test.
import subprocess
# Importa tempfile per creare una directory temporanea con pulizia automatica.
import tempfile


# Definisce la funzione che prepara ed esegue entrambe le suite di test.
def main():
    # __file__ è questo script; resolve rende il percorso assoluto e parents[1] trova la radice del progetto.
    root = Path(__file__).resolve().parents[1]
    module = (root / "MoStream/MPMC_queue_rigtorp.mojo").read_text()

    with tempfile.TemporaryDirectory(prefix="mostream-rigtorp-") as directory:
        # Visita prima i test sequenziali e poi quelli con produttori e consumatori concorrenti.
        for name in ("test_rigtorp", "test_rigtorp_concurrent"):
            # La f-string inserisce il nome del test nel percorso del sorgente temporaneo.
            source = Path(directory) / f"{name}.mojo"
            # Scrive modulo, nuova riga e test nello stesso file, evitando gli import del runtime MoStream.
            source.write_text(module + "\n" + (root / f"Tests/{name}.mojo").read_text())
            # Avvia mojo senza shell; check=True segnala un fallimento e timeout=60 limita ogni esecuzione a 60 secondi.
            subprocess.run(["mojo", str(source)], check=True, timeout=60)


# Esegue i test soltanto quando lo script viene avviato direttamente, non quando è importato.
if __name__ == "__main__":
    # Chiama la funzione principale; eventuali errori interrompono lo script e vengono mostrati nel terminale.
    main()
