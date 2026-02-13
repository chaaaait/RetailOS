import duckdb
try:
    con = duckdb.connect('data/warehouse/retail.duckdb')
    print("Connected!")
    print(con.execute('SHOW TABLES').fetchdf())
    print("Query success!")
except Exception as e:
    print(f"Error: {e}")
