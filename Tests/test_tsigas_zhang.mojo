# Il runner antepone il modulo: non importa il runtime completo MoStream.
from std.testing import assert_equal, assert_true

# Copre pieno/vuoto, FIFO, colori e molti attraversamenti della cella zero.
def check_ring(length: Int) raises:
    var queue = TsigasZhangMPMCQueue(length)
    assert_equal(queue.capacity, length - 2)
    assert_true(queue.empty())
    assert_true(not queue.try_pop())
    for cycle in range(200):
        for i in range(queue.capacity):
            assert_true(not queue.try_push(UInt64(cycle * queue.capacity + i)))
        assert_equal(queue.size(), queue.capacity)
        var rejected = queue.try_push(999999)
        assert_true(Bool(rejected))
        assert_equal(rejected.take(), UInt64(999999))
        for i in range(queue.capacity):
            var item = queue.try_pop()
            assert_true(Bool(item))
            assert_equal(item.take(), UInt64(cycle * queue.capacity + i))
        assert_true(not queue.try_pop())
    # Ripetizioni dello stesso payload: non lo confondiamo con il colore vuoto.
    for _ in range(100):
        queue.push(0)
        queue.push(0)
        assert_equal(queue.pop(), UInt64(0))
        assert_equal(queue.pop(), UInt64(0))
    queue.push(1)
    queue.push(TsigasZhangMPMCQueue.MAX_VALUE)
    var moved = queue^
    assert_equal(moved.pop(), UInt64(1))
    assert_equal(moved.pop(), TsigasZhangMPMCQueue.MAX_VALUE)
    moved.push(42)
    assert_equal(moved.pop(), UInt64(42))
    # La distruzione gestisce anche una coda non vuota di scalari.
    moved.push(123)


def main() raises:
    check_ring(4)
    check_ring(8)
    check_ring(16)
    check_ring(64)
    for invalid in range(-1, 8):
        if invalid == 4 or invalid == 6:
            continue
        var rejected = False
        try:
            var queue = TsigasZhangMPMCQueue(invalid)
        except:
            rejected = True
        assert_true(rejected)
    var queue = TsigasZhangMPMCQueue(4)
    # I due valori non codificabili devono produrre errore, senza inserire dati.
    for offset in range(2):
        var rejected = False
        try:
            _ = queue.try_push(TsigasZhangMPMCQueue.MAX_VALUE + UInt64(offset + 1))
        except:
            rejected = True
        assert_true(rejected)
        assert_true(queue.empty())
    print("Tsigas-Zhang: FIFO, pieno/vuoto, wrap, move e limiti superati")
