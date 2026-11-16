#!/usr/bin/env python3
"""PipelineGrain con BlockingPipeline (pop bloccante, EOS espliciti), Vyukov vs Rigtorp.

Riusa run.py senza modificarlo: cambia soltanto il sorgente compilato in
pipeline_blocking.mojo. Stessi argomenti di run.py; il runtime cooperativo non è supportato.
"""
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run  # noqa: E402

run.SOURCE = HERE / 'pipeline_blocking.mojo'

if __name__ == '__main__':
    if '--runtime' in sys.argv and sys.argv[sys.argv.index('--runtime') + 1] != 'std':
        sys.exit('BlockingPipeline supporta solo --runtime std')
    run.main()
