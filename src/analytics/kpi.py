import duckdb

def get_daily_revenue():
    conn = duckdb.connect("data/warehouse/retail.duckdb")
    result = conn.execute("""
        SELECT d.date, SUM(f.revenue) as total_revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.date
        ORDER BY d.date
        LIMIT 30
    """).fetchall()

    return [{"date": r[0], "revenue": r[1]} for r in result]
