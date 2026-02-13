
import duckdb
import pandas as pd
from datetime import datetime
import os

DB_PATH = "data/warehouse/retail.duckdb"
CSV_PATH = "data/raw/inventory.csv"

def populate_inventory():
    print(f"Connecting to {DB_PATH}...")
    con = duckdb.connect(DB_PATH)

    print(f"Reading {CSV_PATH}...")
    # The header says (date, store_id, product_id, stock_level)
    # But data is (store_id, product_id, date, stock_level)
    # We load it and rename columns correctly
    df = pd.read_csv(CSV_PATH)
    
    # Rename columns to match actual data in the file
    df.columns = ['actual_store_id', 'actual_product_id', 'actual_date', 'actual_stock_level']
    
    print("Mapping date to date_key...")
    df['date_key'] = pd.to_datetime(df['actual_date']).dt.strftime('%Y%m%d').astype(int)

    print("Mapping product_id to product_key...")
    # Get mapping from dim_product
    product_map = con.execute("SELECT product_id, product_key FROM dim_product").fetchdf()
    product_dict = dict(zip(product_map['product_id'], product_map['product_key']))
    df['product_key'] = df['actual_product_id'].map(product_dict)

    print("Mapping store_id to store_key...")
    # Get mapping from dim_store
    store_map = con.execute("SELECT store_id, store_key FROM dim_store").fetchdf()
    store_dict = dict(zip(store_map['store_id'], store_map['store_key']))
    df['store_key'] = df['actual_store_id'].map(store_dict)
    
    # Define stock_level correctly
    df['stock_level'] = df['actual_stock_level']

    # Some data might not map if IDs were changed, fill with random keys if necessary or just drop
    # For this test, we expect them to match.
    initial_rows = len(df)
    df = df.dropna(subset=['product_key', 'store_key'])
    if len(df) < initial_rows:
        print(f"Dropped {initial_rows - len(df)} rows due to missing keys.")

    print("Preparing fact_inventory table data...")
    # Cols: inventory_id, date_key, product_key, store_key, stock_level, reorder_point
    # We don't have reorder_point in CSV, so we generate it
    df['reorder_point'] = 20  # Constant for demo
    
    # Register as a temporary table for DuckDB to ingest
    con.register('temp_inventory', df)

    print("Cleaning existing fact_inventory...")
    con.execute("DELETE FROM fact_inventory")

    print("Inserting data into fact_inventory...")
    con.execute("""
        INSERT INTO fact_inventory (date_key, product_key, store_key, stock_level, reorder_point)
        SELECT date_key, product_key, store_key, stock_level, reorder_point
        FROM temp_inventory
    """)

    rows = con.execute("SELECT COUNT(*) FROM fact_inventory").fetchone()[0]
    print(f"Successfully loaded {rows} rows into fact_inventory.")

    con.close()

if __name__ == "__main__":
    populate_inventory()
