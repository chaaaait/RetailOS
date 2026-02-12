import duckdb
from pathlib import Path

DB_PATH = "data/warehouse/retail.duckdb"


def get_region_from_city(city: str) -> str:
    """
    Map city to region based on the specified mapping.
    
    Mapping:
    - Mumbai, Pune → West
    - Delhi, Jaipur → North
    - Bangalore, Chennai, Hyderabad → South
    - Kolkata → East
    - Ahmedabad, Surat → West
    """
    city_upper = city.upper().strip()
    
    if city_upper in ['MUMBAI', 'PUNE', 'AHMEDABAD', 'SURAT']:
        return 'West'
    elif city_upper in ['DELHI', 'JAIPUR']:
        return 'North'
    elif city_upper in ['BANGALORE', 'CHENNAI', 'HYDERABAD']:
        return 'South'
    elif city_upper == 'KOLKATA':
        return 'East'
    else:
        # Default fallback for unknown cities
        return 'Unknown'


def main() -> None:
    """
    Load stores into dim_store with region derived from city.
    
    - Drops and recreates dim_store table to ensure schema matches.
    - Reads from stores parquet files in data/raw.
    - Derives region from city using the specified mapping.
    - Safe to re-run: drops and recreates table each time.
    """
    con = duckdb.connect(DB_PATH)

    print("\n=== Loading dim_store with region ===\n")

    # Drop and recreate table to ensure schema matches
    print("Dropping existing dim_store table...")
    con.execute("DROP TABLE IF EXISTS dim_store")
    
    print("Recreating dim_store table with region column...")
    con.execute("""
        CREATE TABLE dim_store (
            store_key BIGINT,
            store_id VARCHAR,
            store_name VARCHAR,
            city VARCHAR,
            region VARCHAR
        )
    """)

    # Load staging stores from parquet
    stores_parquet = Path("data/raw")
    if not stores_parquet.exists():
        raise FileNotFoundError("data/raw directory not found; generate/ingest stores first.")

    print("Loading staging stores from parquet...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE staging_stores AS
        SELECT *
        FROM read_parquet('data/raw/stores_*.parquet')
    """)

    # Insert stores with region derived from city
    # Using CASE statement in SQL for region mapping
    print("Inserting stores with region mapping...")
    con.execute("""
        INSERT INTO dim_store (
            store_key,
            store_id,
            store_name,
            city,
            region
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY store_id) AS store_key,
            store_id,
            store_name,
            city,
            CASE
                WHEN UPPER(TRIM(city)) IN ('MUMBAI', 'PUNE', 'AHMEDABAD', 'SURAT') THEN 'West'
                WHEN UPPER(TRIM(city)) IN ('DELHI', 'JAIPUR') THEN 'North'
                WHEN UPPER(TRIM(city)) IN ('BANGALORE', 'CHENNAI', 'HYDERABAD') THEN 'South'
                WHEN UPPER(TRIM(city)) = 'KOLKATA' THEN 'East'
                ELSE 'Unknown'
            END AS region
        FROM staging_stores
    """)

    # Verify regions
    region_counts = con.execute("""
        SELECT region, COUNT(*) as store_count
        FROM dim_store
        GROUP BY region
        ORDER BY region
    """).fetchall()

    print("\nStore counts by region:")
    for region, count in region_counts:
        print(f"  {region}: {count} stores")

    total_stores = con.execute("SELECT COUNT(*) FROM dim_store").fetchone()[0]
    print(f"\nTotal stores loaded: {total_stores}")

    # Verify distinct regions
    distinct_regions = con.execute("SELECT DISTINCT region FROM dim_store ORDER BY region").fetchall()
    print(f"\nDistinct regions: {[r[0] for r in distinct_regions]}")

    print("\n=== dim_store load complete ===\n")


if __name__ == "__main__":
    main()
