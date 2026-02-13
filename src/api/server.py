from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb

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
def daily_revenue():
    df = con.execute("""
        SELECT date, SUM(revenue) as revenue
        FROM fact_sales fs
        JOIN dim_date dd ON fs.date_key = dd.date_key
        GROUP BY date
        ORDER BY date
    """).fetchdf()
    return df.to_dict(orient="records")

@app.get("/kpi/stockout-risks")
def stockout():
    df = con.execute("""
        SELECT * FROM ml_reasoning_log
        ORDER BY timestamp DESC
        LIMIT 10
    """).fetchdf()
    return df.to_dict(orient="records")


    from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from src.analytics.kpi import get_daily_revenue

@app.get("/api/kpi/daily-revenue")
def daily_revenue():
    return get_daily_revenue()
