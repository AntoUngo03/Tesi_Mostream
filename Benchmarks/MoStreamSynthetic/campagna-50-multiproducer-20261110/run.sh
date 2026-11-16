#!/bin/bash
set -e
cd ~/mostream-synthetic
export MOJO=/opt/mojo/bin/mojo
C=Benchmarks/MoStreamSynthetic/campagna-50-multiproducer-20261110
python3 -u Benchmarks/MoStreamSynthetic/MultiProducer/run.py --scenarios n1,nm --producers-n1 1,2,4,8,16 --producers-nm 2,4,8 --work-us 0,1,2,5 --messages 50000 --capacity 1024 --repeats 50 --seed 42 --pinning on --runtime std --output $C/MultiProducer
date > $C/COMPLETATO.txt
