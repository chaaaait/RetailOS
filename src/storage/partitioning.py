import duckdb
import pandas as pd
import os
import time
import glob

DB_PATH = "data/warehouse/retail.duckdb"
PARTITION_BASE_PATH = "data/warehouse/partitioned/fact_sales"


def partition_fact_sales():
    print("\nPartitioning fact_sales...")

    con = duckdb.connect(DB_PATH)

    # Join to get region and date
    df = con.execute(
        """
        SELECT 
            fs.*,
            dd.date AS full_date,
            ds.region
        FROM fact_sales fs
        JOIN dim_date dd ON fs.date_key = dd.date_key
        JOIN dim_store ds ON fs.store_key = ds.store_key
        """
    ).fetchdf()

    if df.empty:
        print("fact_sales is empty; skipping partition write.")
        con.close()
        return

    # Create partition columns
    df["date_partition"] = pd.to_datetime(df["full_date"]).dt.strftime("%Y-%m-%d")

    # Ensure base partition directory exists
    os.makedirs(PARTITION_BASE_PATH, exist_ok=True)

    # Save partitioned Parquet dataset
    df.to_parquet(
        PARTITION_BASE_PATH,
        partition_cols=["date_partition", "region"],
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    print(f"fact_sales partitioned successfully under '{PARTITION_BASE_PATH}'.")
    con.close()


def benchmark_query():
    print("\nRunning benchmark...")

    con = duckdb.connect(DB_PATH)

    # Without partitioning (DuckDB table)
    start = time.time()
    con.execute(
        """
        SELECT SUM(revenue)
        FROM fact_sales
        WHERE date_key = 20240325
        """
    ).fetchone()
    normal_time = time.time() - start

    print(f"Query time WITHOUT partitioning: {normal_time:.6f} seconds")

    # With partitioned parquet
    # Safety check: folder must exist and contain parquet files
    if not os.path.isdir(PARTITION_BASE_PATH):
        print(
            f"Partition folder '{PARTITION_BASE_PATH}' does not exist. "
            "Skip partitioned benchmark (run partition_fact_sales() first)."
        )
        con.close()
        return

    parquet_pattern = os.path.join(PARTITION_BASE_PATH, "**", "*.parquet")
    parquet_files = glob.glob(parquet_pattern, recursive=True)

    if not parquet_files:
        print(
            f"No parquet files found under '{PARTITION_BASE_PATH}'. "
            "Skip partitioned benchmark (partitioning may have produced no data)."
        )
        con.close()
        return

    start = time.time()
    con.execute(
        """
        SELECT SUM(revenue)
        FROM read_parquet('data/warehouse/partitioned/fact_sales/**/*.parquet')
        WHERE date_partition = '2024-03-25'
        """
    ).fetchone()
    partitioned_time = time.time() - start

    print(f"Query time WITH partitioning: {partitioned_time:.6f} seconds")

    con.close()


if __name__ == "__main__":
    partition_fact_sales()
    benchmark_query()
