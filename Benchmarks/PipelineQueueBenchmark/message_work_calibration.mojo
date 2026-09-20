from std.sys import argv
from std.time import perf_counter_ns


@always_inline
def apply_work(input: UInt64, iterations: Int) -> UInt64:
    var value = input
    for _ in range(iterations):
        value = value ^ (value << UInt64(13))
        value = value ^ (value >> UInt64(7))
        value = value ^ (value << UInt64(17))
    return value


def main() raises:
    var args = argv()
    if len(args) != 3:
        raise Error("Usage: message_work_calibration <messages> <iterations>")
    var messages = Int(args[1])
    var iterations = Int(args[2])
    var checksum: UInt64 = 0
    var start = perf_counter_ns()
    for value in range(1, messages + 1):
        checksum += apply_work(UInt64(value), iterations)
    var elapsed = perf_counter_ns() - start
    print(
        "WORK_RESULT iterations=", iterations,
        " ns_per_message=", Float64(elapsed) / Float64(messages),
        " checksum=", checksum,
    )
