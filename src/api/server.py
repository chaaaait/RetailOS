from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from src.analytics.kpi import get_daily_revenue

app = FastAPI()

con = duckdb.connect("data/warehouse/retail.duckdb")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/kpi/daily-revenue")
def daily_revenue_kpi():
    df = con.execute("""
        SELECT dd.date, SUM(fs.revenue) as revenue
        FROM fact_sales fs
        JOIN dim_date dd ON fs.date_key = dd.date_key
        GROUP BY dd.date
        ORDER BY dd.date
        LIMIT 30
    """).fetchdf()
    return df.to_dict(orient="records")

@app.get("/api/kpi/daily-revenue")
def daily_revenue_api():
    return get_daily_revenue()
