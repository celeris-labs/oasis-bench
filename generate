#!/usr/bin/env python3

import duckdb
from pathlib import Path
import argparse

COPY_OPTIONS = "FORMAT PARQUET, COMPRESSION SNAPPY"


def generate_data(output_dir: str, scale_factor: float):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")

    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    print(f"Generating TPC-H SF{scale_factor} data...")
    con.execute(f"CALL dbgen(sf={scale_factor})")

    tables = [
        row[0] for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
    ]

    for table in tables:
        filename = output_path / f"{table}.parquet"
        print(f"Writing {table} to {filename}...")
        con.execute(f"COPY {table} TO '{filename}' ({COPY_OPTIONS})")

        row_count = con.execute(f"SELECT COUNT(*) FROM '{filename}'").fetchone()[0]
        print(f"  -> {row_count:,} rows")

    con.close()
    print(f"\nDone! Files written to {output_path.absolute()}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate TPC-H data and export to Parquet files using DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-s", "--scale-factor",
        type=float,
        default=1,
        dest="scale_factor",
        metavar="SF",
        help="Scale factor (default: 1)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        dest="output_dir",
        metavar="DIR",
        help="Output directory (default: data/tpch-<sf>)",
    )

    args = parser.parse_args()

    sf = int(args.scale_factor) if args.scale_factor == int(args.scale_factor) else args.scale_factor
    output_dir = args.output_dir or f"data/tpch-{str(sf).replace('.', '')}"
    generate_data(
        scale_factor=args.scale_factor,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
