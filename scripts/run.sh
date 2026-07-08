#!/usr/bin/env bash
set -euo pipefail

# Sources to sweep over.
SOURCES=(parquet oasis)

# Thread counts to sweep over.
THREADS=($(seq 1 64))

for s in "${SOURCES[@]}"; do
    for t in "${THREADS[@]}"; do
        # Build the core range "64-(64+t-1)" for numactl -C.
        cores="64-$((64 + t - 1))"

        echo "=== source=${s} threads=${t} cores=${cores} ==="
        numactl -m 1 -C "${cores}" ./build/throughput -s "${s}" -r 1 -t "${t}" -S 4 -d rdma:// -i <ip-address>
    done
done
