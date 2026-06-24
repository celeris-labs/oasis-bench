#!/bin/bash
set -e

mkdir -p data/clickbench
wget -c https://datasets.clickhouse.com/hits_compatible/hits.parquet -O data/clickbench/hits.parquet
