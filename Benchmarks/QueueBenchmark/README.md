# Queue benchmark

This benchmark compares the CAS, FAA, Hybrid, Rigtorp, Padded-FAA,
bounded-LPRQ-inspired, and wCQ queues under the same concurrent workload.
Every run also checks the number and checksum of consumed messages.

`BoundedLPRQInspired` is deliberately named as an adaptation. The published
LPRQ is an unbounded list of PRQ segments; this implementation uses a fixed,
reusable ring and therefore does not inherit the original lock-freedom proof.
Its `try_push` and `try_pop` methods are weak single-attempt operations and can
fail spuriously under contention; the blocking benchmark paths retry them.

Build and run from the repository root:

```bash
mkdir -p /tmp/mostream-mojo-cache
make -C MoStream/lib wcq_native.o
MODULAR_CACHE_DIR=/tmp/mostream-mojo-cache \
  mojo build Benchmarks/QueueBenchmark/queue_benchmark.mojo \
  -I . -o /tmp/queue_benchmark \
  -Xlinker "$PWD/MoStream/lib/wcq_native.o"
/tmp/queue_benchmark
```

Optional arguments are:

```text
queue_benchmark messages_per_producer producers consumers capacity repetitions
```

For example:

```bash
/tmp/queue_benchmark 1000000 8 8 1024 10
```

Run multiple configurations rather than drawing conclusions from a single
result. Useful configurations include 1P/1C, 2P/2C, 4P/4C, and 8P/8C with
capacities 16, 1024, and 65536. Use a release build and an otherwise idle
machine for thesis measurements.

To run the complete balanced and unbalanced matrix and produce a CSV containing
means, sample standard deviations, and 95% confidence intervals:

```bash
python3 Benchmarks/QueueBenchmark/run_suite.py
```

Optional arguments select messages per producer, repetitions, and output path:

```bash
python3 Benchmarks/QueueBenchmark/run_suite.py \
  250000 30 Benchmarks/QueueBenchmark/results.csv
```

The runner builds and links the native wCQ bridge automatically. The wCQ
adapter is limited to x86-64 processors with `CMPXCHG16B`; it stores `UInt64`
values and requires a distinct dense thread ID for every concurrent caller.

The reproducible wCQ experiment from 2026-07-22 is in
`results_wcq.csv`; its interpretation and limitations are documented in
`WCQ_RESULTS.md`. Summarize any compatible result file with:

```bash
python3 Benchmarks/QueueBenchmark/analyze_wcq_results.py \
  Benchmarks/QueueBenchmark/results_wcq.csv
```

Summarize an LPRQ result CSV without external Python dependencies:

```bash
python3 Benchmarks/QueueBenchmark/analyze_lprq_results.py \
  Benchmarks/QueueBenchmark/results_bounded_lprq.csv
```

The per-consumer count and checksum are accumulated locally during the timed
region. This avoids adding a shared atomic operation to every queue operation.
