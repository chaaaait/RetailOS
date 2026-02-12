import duckdb

con = duckdb.connect("data/warehouse/retail.duckdb")

print("\nTables:")
print(con.execute("SHOW TABLES").fetchall())

print("\nFact Sales Row Count:")
print(con.execute("SELECT COUNT(*) FROM fact_sales").fetchall())

print("\nTop 5 Products by Revenue:")
print(con.execute("""
SELECT dp.product_name, SUM(fs.revenue) as total_revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY total_revenue DESC
LIMIT 5
""").fetchdf())
