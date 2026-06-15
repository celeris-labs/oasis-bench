# ???'27 Benchmark for "Oasis: Hiding the Cost of Querying Parquet Files in the Datapath"
This is the benchmarking code used to get the results presented in the paper "Oasis: Hiding the Cost of Querying Parquet Files in the Datapath". When cloning the repo, you have to initialize the DuckDB submodule with:

``` bash
git submodule update --init
```

## Building the benchmark & extension
To build the benchmark:
``` bash
mkdir build && cd build
cmake ..
make -j 64
cd ..
```

To build the DuckDB ViewRewriter extension:
```bash
cd extension
make
```

## Getting the data
To generate scale factor 30 as Parquet files in the directory `data/tpch-30` with default TPC-H ordering execute:
``` bash
./generate -s 30
```

To get more options, execute with `-h`. The ClickBench Parquet file can be downloaded with `./download_clickbench.sh`.

To put data into tmpfs on the HACC cluster (the benchmark were run on hacc-box-01 of the HACC cluster):
``` bash
sudo tmpfs-create -s 32 -n 0
cp -r data/tpch-30 /mnt/ramdisk
```

## Executing experiments & generating plots
To run experiments and plot the results (this requires some manual moving the measurements to the correct folders for now):
``` bash
./run_all.sh
python3 plot.py
```
