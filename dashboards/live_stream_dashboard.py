import streamlit as st
import duckdb
import pandas as pd
import time
from pathlib import Path

DB_PATH = "data/warehouse/retail.duckdb"

st.set_page_config(page_title="RetailOS Live Command Center", layout="wide")

st.title("🚀 RetailOS Live Command Center")

# ===============================
# DB CONNECTION
# ===============================
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH)

con = get_connection()

# ===============================
# LIVE METRICS
# ===============================
def get_live_metrics():
    total_orders = con.execute("""
        SELECT COUNT(*) FROM streaming_orders
    """).fetchone()[0]

    revenue = con.execute("""
        SELECT COALESCE(SUM(price * quantity),0)
        FROM streaming_orders
        WHERE DATE(timestamp) = CURRENT_DATE
    """).fetchone()[0]

    return total_orders, revenue

def get_recent_orders():
    return con.execute("""
        SELECT order_id, product_id, store_id, quantity, price, timestamp
        FROM streaming_orders
        ORDER BY timestamp DESC
        LIMIT 10
    """).fetchdf()

def get_ml_alerts():
    try:
        return con.execute("""
            SELECT product_id, store_id, risk_level, ml_confidence, optimal_reorder_qty
            FROM ml_reasoning_log
            WHERE risk_level >= 2
            ORDER BY timestamp DESC
            LIMIT 5
        """).fetchdf()
    except:
        return pd.DataFrame()

# ===============================
# DISPLAY SECTION
# ===============================

total_orders, revenue = get_live_metrics()

col1, col2 = st.columns(2)

col1.metric("Total Orders (All Time)", total_orders)
col2.metric("Revenue Today (₹)", f"{revenue:,.2f}")

st.divider()

st.subheader("📦 Recent Orders")

recent_orders = get_recent_orders()

if recent_orders.empty:
    st.warning("No streaming orders found.")
else:
    st.dataframe(recent_orders, use_container_width=True)

st.divider()

st.subheader("🚨 ML Stockout Alerts")

alerts = get_ml_alerts()

if alerts.empty:
    st.success("No High Risk Stockouts Detected")
else:
    for _, row in alerts.iterrows():
        st.error(
            f"""
            Product: {row['product_id']}  
            Store: {row['store_id']}  
            Risk Level: {row['risk_level']}  
            Confidence: {round(row['ml_confidence'] * 100,1)}%  
            Recommended Reorder: {row['optimal_reorder_qty']} units
            """
        )

# ===============================
# AUTO REFRESH
# ===============================
time.sleep(2)
st.rerun()
