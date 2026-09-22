[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)
![MoStream Logo](https://raw.githubusercontent.com/ParaGroup/MoStream/main/Logo/mostream_small.png)

# MoStream

MoStream is a Mojo library for building stream-processing pipelines with
sequential and parallel stages. A pipeline is composed from user-defined stages
connected by inter-stage communicators, then executed by Mojo async tasks.

The library is intended for workloads that naturally fit a flow such as:

```text
source -> stage -> stage -> ... -> sink
```

Each stage declares its input and output message types, and can be run as a
sequential node (thread), or as a collection of parallel nodes (threads each
running a replica of the stage).

## Features

- Source, transform, one-to-many transform, and sink stages.
- Sequential nodes with `seq(stage)`.
- Replicated parallel nodes with `parallel(stage, degree)`.
- Optional CPU pinning through a small C helper library.
- Configurable communicators (e.g., customizable queue size).

## Requirements

- Mojo 1.0.0 (the tested toolchain for this version).
- A C compiler such as `gcc` for the CPU-affinity helper.
- Linux-style pthread CPU affinity support for thread pinning.

The current runtime expects the helper library at:

```text
$MOSTREAM_HOME/MoStream/lib/libpinning.so
```

## Setup

Build the C helper and point `MOSTREAM_HOME` at the repository root:

```sh
make -C MoStream/lib
export MOSTREAM_HOME="$PWD"
```

Then run one of the included examples:

```sh
mojo -O3 -I. Tests/test_pipe_1.mojo
```

If `MOSTREAM_HOME` is not set, MoStream falls back to the current directory.

### Mojo 1.0 migration and validation

The library, tests, and benchmarks use Mojo 1.0's `Pointer`, `Array`,
`Deinitable`, `__deinit__`, and `deinit move` APIs. Manually managed allocations
use `MutUntrackedOrigin` and explicit `unsafe_*` operations; their owners must
remain alive until all pointer accesses and asynchronous tasks have finished.
Timer values use `Int`, matching `perf_counter_ns()`.

The CPU queue tests and benchmarks use `TaskGroup` from Mojo's runtime, so they
do not require MAX's relocated `parallelize` API. See the
[Mojo 1.0 release notes](https://mojolang.org/releases/v1.0.0/) for language changes.

Build with warnings treated as errors and run all tests, including image
copy/move and filter checks:

```sh
python3 Tests/run_tests.py
```

To also compile every benchmark entry point:

```sh
python3 Tests/run_tests.py --benchmarks
```

The runner builds the C helpers, links the native wCQ object where required,
and supplies the worker count to the cooperative pipeline test. Benchmark
compilation does not execute the timed performance workloads.

## Quick Example

```mojo
from std.collections import Optional
from MoStream import Pipeline, StageKind, StageTrait, seq, parallel

struct NumberSource(StageTrait):
    comptime kind = StageKind.SOURCE
    comptime InType = Int
    comptime OutType = Int
    comptime name = "NumberSource"
    var next: Int

    def __init__(out self):
        self.next = 1

    def next_element(mut self) -> Optional[Int]:
        if self.next > 100:
            return None
        var value = self.next
        self.next = self.next + 1
        return value

@fieldwise_init
struct AddOne(StageTrait):
    comptime kind = StageKind.TRANSFORM
    comptime InType = Int
    comptime OutType = Int
    comptime name = "AddOne"

    def compute(mut self, var input: Int) -> Optional[Int]:
        return input + 1

@fieldwise_init
struct PrintSink(StageTrait):
    comptime kind = StageKind.SINK
    comptime InType = Int
    comptime OutType = Int
    comptime name = "PrintSink"

    def consume_element(mut self, var input: Int):
        print(input)

def main() raises:
    var source = NumberSource()
    var add_one = AddOne()
    var sink = PrintSink()

    var pipeline = Pipeline((
        seq(source),
        parallel(add_one, 4),
        seq(sink),
    ))

    pipeline.setPinning(False)
    pipeline.run()
```

## Defining Stages

Every stage implements `StageTrait` and sets these compile-time fields:

```mojo
comptime kind: Int
comptime InType: MessageTrait
comptime OutType: MessageTrait
comptime name: String
```

Supported stage kinds:

- `StageKind.SOURCE`: produces stream elements with `next_element()`.
- `StageKind.TRANSFORM`: consumes one input and returns zero or one output with
  `compute()`.
- `StageKind.TRANSFORM_MANY`: consumes one input and emits zero or more outputs
  with `compute_many(input, emitter)`.
- `StageKind.SINK`: consumes stream elements with `consume_element()`.

Stages can optionally implement:

```mojo
def received_eos(mut self):
    ...
```

MoStream calls this hook when a stage observes the end of the stream.

## One-to-Many Transforms

`TRANSFORM_MANY` stages use `Emitter` to produce any number of output elements
for a single input:

```mojo
from MoStream import Emitter

@fieldwise_init
struct Duplicate(StageTrait):
    comptime kind = StageKind.TRANSFORM_MANY
    comptime InType = Int
    comptime OutType = Int
    comptime name = "Duplicate"

    def compute_many(mut self, var input: Int, mut emitter: Emitter[Int]):
        emitter.emit(input)
        emitter.emit(input)
```

## Parallelism

Use `seq(stage)` for a single stage instance and `parallel(stage, degree)` for
replicated execution:

```mojo
var pipeline = Pipeline((
    seq(source),
    parallel(filter, 4),
    parallel(mapper, 8),
    seq(sink),
))
```

The source stage must be the first pipeline stage, and the sink stage must be
the last. The total number of node replicas must fit within Mojo's available
async runtime parallelism.

The source stage must be the first pipeline stage, and the sink stage must be
the last. The total number of node replicas must fit within Mojo's available
async runtime parallelism.

## Runtime Backends

MoStream provides two execution runtimes: the standard runtime and the
cooperative runtime.

### Standard Runtime

The standard runtime is selected with:

```mojo
pipeline.run()
```

In this runtime, each pipeline node replica is executed by one long-lived Mojo
async task. For example:

```mojo
var pipeline = Pipeline((
    seq(source),
    parallel(stage_a, 2),
    parallel(stage_b, 4),
    seq(sink),
))

pipeline.run()
```

creates one task for the source, two tasks for `stage_a`, four tasks for
`stage_b`, and one task for the sink.

Each task repeatedly executes the logic of its stage and communicates with the
next/previous stage through bounded communicators. This runtime is simple
and direct, but each node replica occupies one task for the whole lifetime of
the pipeline. Therefore, the total number of node replicas should not exceed the
parallelism available in Mojo's async runtime.

Use the standard runtime when the pipeline parallelism degree naturally matches
the number of available runtime threads.

### Cooperative Runtime

The cooperative runtime is selected with:

```mojo
pipeline.run_cooperative(n_workers)
```

where `n_workers` is the number of scheduler workers used to execute the
pipeline actors.

An optional `batch_size` limits the number of stage processing steps per
activation, stopping early when an actor blocks or finishes:

```mojo
pipeline.run_cooperative(4, batch_size=8)
```

The default batch size is one. Completed runs expose
`cooperative_execution_ns`, `cooperative_activations`,
`cooperative_input_parks`, and `cooperative_output_parks` on the pipeline.

In this runtime, each node replica is represented as an actor. Actors do not own
a runtime thread permanently. Instead, a smaller number of scheduler workers
repeatedly pick ready actors, run one non-blocking activation, and then either
reschedule the actor or park it if it cannot make progress.

For example:

```mojo
var pipeline = Pipeline((
    seq(source),
    parallel(stage_a, 8),
    parallel(stage_b, 8),
    seq(sink),
))

pipeline.run_cooperative(4)
```

creates many logical actors, but only four scheduler workers execute them. This
allows MoStream to experiment with pipeline configurations where the number of
logical stage replicas is larger than the number of runtime worker threads.

The cooperative runtime is especially useful when some stages frequently block
on input or output. In that case, blocked actors do not need to occupy a worker
thread while waiting. Please, note that TRANSFORM_MANY stages are currently
not supported with the cooperative runtime.

## Runtime Configuration

### Queue Size

MoStream uses bounded MPMC queues between stages. The default backend is the
original CAS queue; a build can select the experimental Padded-FAA backend for
the inter-stage communicators with `-DMOSTREAM_PADDED_FAA=1`. The cooperative
scheduler's internal ready/wait queues remain CAS-based. The default queue size
is `1024`. Queue sizes must be powers of two and at least `2`.

`-DMOSTREAM_COOPERATIVE_FAA=1` selects an experimental backend whose cooperative
actors retain FAA tickets while parked. It uses broadcast wakeups for blocked
actors on the affected connection; the ready queue remains CAS-based. Select
only one data queue backend per build. See the
[real cooperative pipeline comparison](Benchmarks/RealCooperativeQueueBenchmark/README.md)
for measurements, correctness checks, and limitations.

```mojo
pipeline.setQueueSize(2048)
```

### CPU Pinning

CPU pinning is disabled by default.

```mojo
pipeline.setPinning(True)
```

By default, MoStream assigns CPU IDs from `0` to the number of detected CPUs.
You can provide a custom CPU order with `MOSTREAM_PINNING`:

```sh
export MOSTREAM_PINNING="0,2,4,6"
```

## Benchmarks

The image benchmark demonstrates a deeper stream-processing pipeline:

```text
TimedImageSource -> Grayscale -> GaussianBlur -> Sharpen -> ImageSink
```

Run it with the parallelism degree for each transform stage:

```sh
mojo -I. Benchmarks/ImagePipeline/test_image_1.mojo 1 2 4 2 1
```

## License

Source file headers state that MoStream is distributed under the GNU Lesser
General Public License version 3.

## Requests for Modifications
If you are using MoStream for your purposes and you are interested in specific modifications of the API (or of the runtime system), please send an email to the maintainer.

## Contributors
The main developer and maintainer of MoStream is [Gabriele Mencagli](mailto:gabriele.mencagli@unipi.it) (Department of Computer Science, University of Pisa, Italy).
