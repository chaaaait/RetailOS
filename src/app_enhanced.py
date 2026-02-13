import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import asyncio
import json
import pandas as pd
import time
import sys
import os
from pathlib import Path

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config import DB_PATH
except ImportError:
    DB_PATH = 'data/warehouse/retail.duckdb'

st.set_page_config(page_title="RetailOS Intelligence Platform", layout="wide", page_icon="🏪")

@st.cache_resource
def get_db_connection():
    try:
        return duckdb.connect(str(DB_PATH), read_only=True)
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

con = get_db_connection()

# Initialize session state for auto-refresh
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

st.title("🏪 RetailOS Intelligence Platform")

# Sidebar
with st.sidebar:
    st.header("System Status")
    
    if con:
        # Pipeline status
        try:
            last_run = con.execute("""
            SELECT start_time, status, rows_processed, duration_seconds
            FROM pipeline_runs
            ORDER BY run_id DESC
            LIMIT 1
            """).fetchdf()
            
            if not last_run.empty:
                status_emoji = "✅" if last_run.iloc[0]['status'] == 'success' else "❌"
                st.metric(
                    "Last Pipeline Run",
                    last_run.iloc[0]['start_time'].strftime('%H:%M'),
                    f"{status_emoji} {last_run.iloc[0]['rows_processed']:,} rows"
                )
            else:
                st.info("No pipeline runs recorded yet.")
        except Exception:
            st.warning("Pipeline run table not found.")
    else:
        st.error("Database disconnected")
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("🔄 Auto-Refresh Data (5s)", value=False)
    
    # Model status
    st.subheader("ML Models")
    models = ['Stockout Classifier', 'Reorder Regressor', 'Prophet (50 combos)']
    for model in models:
        st.write(f"✅ {model}")

# Main tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Live Intelligence", 
    "🤖 ML Reasoning", 
    "✅ Schema Evolution", 
    "📈 Pipeline Monitoring",
    "⚠️ Approval Queue"
])

if not con:
    st.stop()

## TAB 1: LIVE INTELLIGENCE
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Check if streaming_orders exists
        con.execute("SELECT 1 FROM streaming_orders LIMIT 1")
        
        # Real-time metrics
        today_stats = con.execute("""
        SELECT 
            COUNT(*) as orders_today,
            COALESCE(SUM(price * quantity), 0) as revenue_today,
            COALESCE(AVG(price * quantity), 0) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM streaming_orders
        WHERE DATE(timestamp) = CURRENT_DATE
        """).fetchdf().iloc[0]
        
        col1.metric("Orders Today", f"{today_stats['orders_today']:,}")
        col2.metric("Revenue Today", f"₹{today_stats['revenue_today']:,.0f}")
        col3.metric("Avg Order Value", f"₹{today_stats['avg_order_value']:,.0f}")
        col4.metric("Unique Customers", f"{today_stats['unique_customers']:,}")
        
    except Exception:
        st.warning("Streaming orders table not found. Start the WebSocket generator first.")
        col1.metric("Orders Today", "0")

    # Stockout risks with ML predictions
    st.subheader("🚨 ML-Predicted Stockout Risks")
    
    try:
        risks = con.execute("""
        SELECT 
            ml.store_id,
            ml.product_id,
            dp.name as product_name,
            ds.store_name,
            ml.current_stock,
            ml.prophet_7d_forecast,
            ml.days_remaining_forecast,
            ml.risk_level,
            ml.ml_confidence,
            ml.optimal_reorder_qty
        FROM ml_reasoning_log ml
        JOIN dim_product dp ON ml.product_id = dp.product_id
        JOIN dim_store ds ON ml.store_id = ds.store_id
        WHERE ml.risk_level >= 2  -- High or Critical
        AND ml.timestamp = (
            SELECT MAX(timestamp) FROM ml_reasoning_log ml2
            WHERE ml2.store_id = ml.store_id AND ml2.product_id = ml.product_id
        )
        ORDER BY ml.days_remaining_forecast ASC
        LIMIT 20
        """).fetchdf()
        
        if not risks.empty:
            for _, row in risks.iterrows():
                with st.container(border=True):
                    c1, c2, c3, c4, c5 = st.columns([2, 2, 1, 1, 1])
                    c1.write(f"**{row['store_name']}**")
                    c2.write(row['product_name'])
                    c3.metric("Stock", f"{row['current_stock']:.0f}")
                    c4.metric("Days", f"{row['days_remaining_forecast']:.1f}", 
                               delta=f"{row['ml_confidence']:.0%} conf", delta_color="inverse")
                    c5.button("Reorder", key=f"ro_{row['store_id']}_{row['product_id']}")
        else:
            st.success("✅ No high-risk stockouts predicted")
    except Exception as e:
        st.info(f"ML predictions not available: {e}")
    
    # Live order feed
    st.subheader("🔴 Recent Orders")
    try:
        recent = con.execute("""
        SELECT order_id, timestamp, product_id, quantity, price, payment_method, order_source 
        FROM streaming_orders 
        ORDER BY timestamp DESC 
        LIMIT 10
        """).fetchdf()
        st.dataframe(recent, use_container_width=True, hide_index=True)
    except:
        pass

## TAB 2: ML REASONING EXPLORER
with tab2:
    st.header("🤖 ML Model Reasoning Explorer")
    
    try:
        # Select a recent prediction
        recent_predictions = con.execute("""
        SELECT 
            timestamp,
            store_id,
            product_id,
            risk_level,
            ml_confidence
        FROM ml_reasoning_log
        ORDER BY timestamp DESC
        LIMIT 50
        """).fetchdf()
        
        if not recent_predictions.empty:
            selected = st.selectbox(
                "Select a prediction to examine:",
                recent_predictions.apply(
                    lambda x: f"{x['timestamp']} | Store {x['store_id']} | Product {x['product_id']} | Risk: {x['risk_level']}", 
                    axis=1
                )
            )
            
            if selected:
                # Parse selection to find row
                selected_ts = selected.split('|')[0].strip()
                
                # Get full explanation
                full = con.execute(f"""
                SELECT * FROM ml_reasoning_log
                WHERE CAST(timestamp AS VARCHAR) = '{selected_ts}'
                LIMIT 1
                """).fetchdf()
                
                if not full.empty:
                    row = full.iloc[0]
                    
                    # Display reasoning
                    c1, c2, c3 = st.columns(3)
                    c1.metric("ML Confidence", f"{row['ml_confidence']:.1%}")
                    risk_labels = ['Safe', 'Moderate', 'High', 'Critical']
                    risk_idx = min(int(row['risk_level']), 3)
                    c2.metric("Risk Level", risk_labels[risk_idx])
                    c3.metric("Recommended Reorder", f"{row['optimal_reorder_qty']:.0f} units")
                    
                    st.subheader("📊 Prediction Factors")
                    
                    # Create factor visualization
                    factors = {
                        'Current Stock': row['current_stock'],
                        'Avg Daily Demand': row['avg_sales_7d'],
                        'Demand Volatility': row['demand_volatility_cv'],
                        'Prophet 7d Forecast': row['prophet_7d_forecast']
                    }
                    
                    fig = go.Figure(data=[
                        go.Bar(x=list(factors.keys()), y=list(factors.values()))
                    ])
                    fig.update_layout(title="Key Factors in Prediction", height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Prophet forecast details
                    st.subheader("📈 Prophet 7-Day Forecast")
                    st.info(f"""
                    **Forecast**: {row['prophet_7d_forecast']:.1f} units over next 7 days
                    
                    **Upper Bound**: {row['prophet_upper_bound']:.1f} units (worst case)
                    
                    **Lower Bound**: {row['prophet_lower_bound']:.1f} units (best case)
                    """)
        else:
            st.info("No ML predictions found in log.")
    except Exception as e:
        st.error(f"Error loading ML reasoning: {e}")

## TAB 3: SCHEMA EVOLUTION DASHBOARD
with tab3:
    st.header("🔄 Adaptive Schema Evolution")
    
    try:
        # Summary stats
        c1, c2, c3 = st.columns(3)
        
        total_changes = con.execute("SELECT COUNT(*) FROM schema_change_log").fetchone()[0]
        pending = con.execute("SELECT COUNT(*) FROM schema_change_log WHERE status = 'pending'").fetchone()[0]
        auto_approved = con.execute("SELECT COUNT(*) FROM schema_change_log WHERE status = 'auto_approved'").fetchone()[0]
        
        c1.metric("Total Schema Changes", total_changes)
        c2.metric("Pending Approval", pending)
        c3.metric("Auto-Approved", auto_approved)
        
        # Recent changes
        st.subheader("Recent Schema Changes")
        
        changes = con.execute("""
        SELECT 
            detected_at,
            table_name,
            change_type,
            column_name,
            confidence_score,
            status
        FROM schema_change_log
        ORDER BY detected_at DESC
        LIMIT 20
        """).fetchdf()
        
        st.dataframe(changes, use_container_width=True)
        
    except Exception as e:
        st.error(f"Schema logs not available or empty: {e}")

## TAB 4: PIPELINE MONITORING
with tab4:
    st.header("📈 Pipeline Performance Monitoring")
    
    try:
        # Run history
        runs = con.execute("""
        SELECT 
            start_time,
            status,
            rows_processed,
            rows_quarantined,
            duration_seconds
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT 50
        """).fetchdf()
        
        if not runs.empty:
            # Success rate
            success_rate = (runs['status'] == 'success').sum() / len(runs) * 100
            st.metric("Pipeline Success Rate", f"{success_rate:.1f}%")
            
            # Performance over time
            fig = px.line(runs, x='start_time', y='duration_seconds', 
                         title="Pipeline Duration Over Time")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pipeline runs to display.")
            
    except Exception as e:
        st.error(f"Monitoring data not available: {e}")

## TAB 5: APPROVAL QUEUE
with tab5:
    st.header("⚠️ Order/Schema Approval Queue")
    # Placeholder for approval queue logic
    st.info("Approval queue is currently empty.")

# Auto-refresh handling
if auto_refresh:
    time.sleep(5)
    st.rerun()