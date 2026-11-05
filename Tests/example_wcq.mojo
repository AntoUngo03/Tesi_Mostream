# Standalone example: -I MoStream avoids importing the pipeline runtime.
from MPMC_queue_wcq import WCQMPMCQueue

def main() raises:
    var queue = WCQMPMCQueue(capacity=8, num_threads=2)
    # tid 0 e tid 1 identificano due partecipanti logici distinti.
    # Qui le chiamate sono sequenziali per mostrare soltanto l'interfaccia.
    var rejected = queue.try_push(UInt64(42), 0)
    if rejected:
        print("Nessun posto libero; dato restituito:", rejected.take())
    var item = queue.try_pop(1)
    if item:
        print("Estratto:", item.take())
    var empty = queue.try_pop(1)
    print("Coda vuota:", not Bool(empty))
