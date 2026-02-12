import duckdb
from pathlib import Path

DB_PATH = "data/warehouse/retail.duckdb"
SCHEMA_SQL_PATH = Path("src/transformation/star_schema.sql")


def main() -> None:
    """
    Schema-only setup for the RetailOS warehouse.

    - Creates tables with IF NOT EXISTS.
    - Creates/maintains indexes and constraints (e.g. SCD2 keys for dim_customer).
    - Does NOT insert, delete, or drop any data.
    - Safe and idempotent to run multiple times.
    """
    con = duckdb.connect(DB_PATH)

    if not SCHEMA_SQL_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_SQL_PATH}")

    with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    con.execute(ddl_sql)
    print("Warehouse schema ensured (tables and indexes created if missing).")


if __name__ == "__main__":
    main()

