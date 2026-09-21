#!/usr/bin/env python3
"""Compare Padico NBLFQ with MPMC, Padded-FAA, SCQ and Michael-Scott."""
from pathlib import Path
import run_michael_scott as comparison

if __name__ == "__main__":
    comparison.main(
        backends={**comparison.BACKENDS, "NBLFQ": ["-DMOSTREAM_NBLFQ=1"]},
        repetitions=10,
        output=Path(__file__).resolve().parent / "nblfq_results",
        target="NBLFQ",
        extra_sources=[
            Path(__file__).resolve(),
            comparison.ROOT / "Documentazione/Padico_NBLFQ/Puk-nblfq.original.h",
        ],
    )
