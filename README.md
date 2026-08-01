# SIGMOD'27 Benchmark for "Oasis: Hiding the Cost of Querying Parquet Files in the Datapath"
This is the benchmarking code used to get the results presented in the paper "Oasis: Hiding the Cost 
of Querying Parquet Files in the Datapath". After cloning the repository, you have to initialize the 
DuckDB submodule with:

``` bash
git submodule update --init
```

## Building the benchmark
To build the benchmark:
``` bash
mkdir build && cd build
cmake ..
make -j
cd ..
```

This will produce four executables:
- `verify` for verification against the Parquet extension of DuckDB.
- `throughput` for throughput benchmarks with multiple clients submitting queries to the database as fast as possible.
- `queries` for per-query benchmarks with operator-level breakdown of runtime.
- `columns` for per-column scan benchmarks that run every column of every table as a single-column query (e.g. `SELECT l_orderkey FROM lineitem`).

## Getting the data
To generate TPC-H scale factor 30 as Parquet files in the directory `./data/tpch-30`, execute:
``` bash
./scripts/generate.py -s 30
```

To get more options, execute with `-h`. The ClickBench Parquet file can be downloaded with `./scripts/download_clickbench.sh`.
To put data into tmpfs on the HACC cluster (the benchmark were run on hacc-box-01 and -02 of the HACC cluster):
``` bash
sudo tmpfs-create -s 32 -n 1
cp -r ./data/tpch-30 /mnt/ramdisk
```

## Executing experiments
To run experiments, you have to build the DuckDB extension from the Oasis repo https://github.com/celeris-labs/oasis and put it into `./extension`.
Then, you have to allocate 1GB huge pages, program the FPGA, and can execute the benchmark:
``` bash
sudo hdev set hugepages -g 32 -n 1
../coyote/util/program_hacc_local.sh /scratch/<nethz-user>/bitstreams/oasis/<build-dir>/cyt_top.bit ../coyote/driver/build/coyote_driver.ko
./scripts/run.sh
```

*Note: You should disable verbose logging in the Coyote driver because we otherwise fill the disk with interrupt log messages.*
