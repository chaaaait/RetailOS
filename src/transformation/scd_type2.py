import duckdb

DB_PATH = "data/warehouse/retail.duckdb"


def main() -> None:
    """
    Apply SCD Type 2 changes for dim_customer in DuckDB.

    Assumes:
    - dim_customer already contains an initial snapshot (version = 1, is_current = TRUE).
    - Latest snapshot of customers is available in Parquet under data/raw/customers_*.parquet.

    Behavior:
    - Type 2 on city changes only.
    - Type 1 (email/phone/name) should be handled in a separate step if desired.
    - Idempotent: re-running with same snapshot will not create duplicates or multiple current rows.
    """
    con = duckdb.connect(DB_PATH)

    print("\n=== Running SCD Type 2 for dim_customer ===\n")

    # ------------------------------------------------------------------
    # Safety check: ensure there are no duplicate (customer_id, version)
    # ------------------------------------------------------------------
    dup_count = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT customer_id, version, COUNT(*) AS cnt
            FROM dim_customer
            GROUP BY customer_id, version
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if dup_count > 0:
        raise RuntimeError(
            "dim_customer contains duplicate (customer_id, version) rows. "
            "Clean up existing data before running SCD Type 2."
        )

    # ------------------------------------------------------------------
    # Step A: Load latest snapshot into staging_customers
    # ------------------------------------------------------------------
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE staging_customers AS
        SELECT *
        FROM read_parquet('data/raw/customers_*.parquet')
        """
    )

    # ------------------------------------------------------------------
    # Step A: Identify customers whose CITY changed (strict SCD2 trigger)
    # ------------------------------------------------------------------
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE changed_customers AS
        SELECT
            s.customer_id,
            s.name,
            s.email,
            s.phone,
            s.city,
            d.version AS current_version
        FROM staging_customers s
        JOIN dim_customer d
            ON d.customer_id = s.customer_id
        WHERE d.is_current = TRUE
          AND COALESCE(s.city, '') <> COALESCE(d.city, '')
        """
    )

    changed_count = con.execute(
        "SELECT COUNT(*) FROM changed_customers"
    ).fetchone()[0]

    print(f"Identified {changed_count} customers with city changes for SCD Type 2.")

    if changed_count == 0:
        print("No SCD Type 2 changes detected. Exiting.\n")
        return

    # ------------------------------------------------------------------
    # Step B: Expire old rows (set is_current = FALSE, valid_to = now)
    # ------------------------------------------------------------------
    con.execute(
        """
        UPDATE dim_customer
        SET is_current = FALSE,
            valid_to   = CURRENT_TIMESTAMP
        WHERE is_current = TRUE
          AND customer_id IN (SELECT customer_id FROM changed_customers)
        """
    )

    # ------------------------------------------------------------------
    # Step C: Insert new versions using JUST-EXPIRED rows
    # ------------------------------------------------------------------
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
            d.version + 1 AS version
        FROM changed_customers s
        JOIN dim_customer d
            ON d.customer_id = s.customer_id
           AND d.is_current = FALSE
           AND d.valid_to = (
               SELECT MAX(valid_to)
               FROM dim_customer d2
               WHERE d2.customer_id = d.customer_id
           )
        LEFT JOIN dim_customer existing
            ON existing.customer_id = d.customer_id
           AND existing.version     = d.version + 1
        WHERE existing.customer_id IS NULL
        """
    )

    print("SCD Type 2 changes applied.\n")

    # ------------------------------------------------------------------
    # Verification Queries
    # ------------------------------------------------------------------
    # Query A: ensure at most one current row per customer
    multiple_current = con.execute(
        """
        SELECT customer_id
        FROM dim_customer
        WHERE is_current = TRUE
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    # Query B: ensure each customer has at least one row
    missing_any = con.execute(
        """
        SELECT customer_id
        FROM dim_customer
        GROUP BY customer_id
        HAVING COUNT(*) < 1
        """
    ).fetchall()

    print("Verification - customers with >1 current row (expected: 0 rows):")
    print(multiple_current)

    print("\nVerification - customers with <1 total row (expected: 0 rows):")
    print(missing_any)

    print("\nSCD Type 2 process complete.\n")


if __name__ == "__main__":
    main()

