#!/usr/bin/env bash
set -euo pipefail

# Sources to sweep over.
SOURCES=(memory parquet oasis)

# Thread counts to sweep over.
THREADS=(1 2 4 8 16)

for s in "${SOURCES[@]}"; do
    for t in "${THREADS[@]}"; do
        # Build the core list "0,1,...,t-1" for numactl -C.
        cores=$(seq -s, 0 $((t - 1)))

        echo "=== source=${s} threads=${t} cores=${cores} ==="
        numactl -N 0 -m 0 -C "${cores}" ./build/throughput -s "${s}" -r 1 -t "${t}" -S 4
    done
done
