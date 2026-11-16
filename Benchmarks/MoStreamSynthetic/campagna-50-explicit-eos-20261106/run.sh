#!/bin/bash
set -e
cd ~/mostream-synthetic
export MOJO=/opt/mojo/bin/mojo
C=Benchmarks/MoStreamSynthetic/campagna-50-explicit-eos-20261106
COMMON="--messages 50000 --capacity 1024 --repeats 50 --seed 42 --pinning on --runtime std"
python3 -u Benchmarks/MoStreamSynthetic/FarmThroughput/run.py --consumers 1,2,4,8,16 --work-us 0,1,2,5 $COMMON --output $C/FarmThroughput
python3 -u Benchmarks/MoStreamSynthetic/PipelineGrain/run.py --stages 1,2,4,8,16 --total-us 0,1,2,5 $COMMON --output $C/PipelineGrain
date > $C/COMPLETATO.txt
