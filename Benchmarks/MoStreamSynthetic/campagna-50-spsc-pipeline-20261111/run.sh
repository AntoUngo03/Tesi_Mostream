#!/bin/bash
set -e
cd ~/mostream-synthetic
export MOJO=/opt/mojo/bin/mojo
C=Benchmarks/MoStreamSynthetic/campagna-50-spsc-pipeline-20261111
python3 -u Benchmarks/MoStreamSynthetic/PipelineGrain/run.py --backends vyukov,rigtorp,spsc --stages 1,2,4,8,16 --total-us 0,1,2,5 --messages 50000 --capacity 1024 --repeats 50 --seed 42 --pinning on --runtime std --expected-ns 2.874 --output $C/PipelineGrain
date > $C/COMPLETATO.txt
