import duckdb
from pathlib import Path

DB_PATH = "data/warehouse/retail.duckdb"


def main() -> None:
    """
    Load initial snapshot of customers into dim_customer as SCD Type 2 version 1.

    - Reads the latest customer snapshot from Parquet files in data/raw.
    - Inserts only customers that do not already exist in dim_customer.
    - Sets version = 1, valid_from = CURRENT_TIMESTAMP, valid_to = NULL, is_current = TRUE.
    - Safe to re-run: uses NOT EXISTS filter.
    """
    con = duckdb.connect(DB_PATH)

    # Create a staging snapshot from ingested parquet files
    # Adjust the path pattern if your files differ.
    customers_parquet = Path("data/raw")
    if not customers_parquet.exists():
        raise FileNotFoundError("data/raw directory not found; generate/ingest customers first.")

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE staging_customers AS
        SELECT *
        FROM read_parquet('data/raw/customers_*.parquet')
        """
    )

    # Insert only brand-new customers (no history yet)
    con.execute(
        """
        INSERT INTO dim_customer (
            customer_key,
            customer_id,
            name,
            email,
            phone,
            city,
            valid_from,
            valid_to,
            is_current,
            version
        )
        SELECT
            NULL AS customer_key,
            s.customer_id,
            s.name,
            s.email,
            s.phone,
            s.city,
            CURRENT_TIMESTAMP AS valid_from,
            NULL AS valid_to,
            TRUE AS is_current,
            1 AS version
        FROM (
            SELECT
                customer_id,
                MIN(name)  AS name,
                MIN(email) AS email,
                MIN(phone) AS phone,
                MIN(city)  AS city
            FROM staging_customers
            GROUP BY customer_id
        ) s
        LEFT JOIN dim_customer d
            ON d.customer_id = s.customer_id
        WHERE d.customer_id IS NULL
        """
    )

    # Optional: simple summary
    inserted_count = con.execute(
        """
        SELECT COUNT(*) FROM dim_customer WHERE version = 1
        """
    ).fetchone()[0]

    print(f"Initial load complete. dim_customer now has {inserted_count} version=1 rows.")


if __name__ == "__main__":
    main()

