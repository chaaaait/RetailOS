import streamlit as st
import asyncio
import websockets
import json
from datetime import datetime

# WebSocket connection state
if 'ws_orders' not in st.session_state:
    st.session_state.ws_orders = []
if 'ws_stats' not in st.session_state:
    st.session_state.ws_stats = {}

async def connect_websocket():
    """Connect to WebSocket server and receive updates"""
    uri = "ws://localhost:8765"
    
    async with websockets.connect(uri) as websocket:
        # Send initial stats request
        await websocket.send(json.dumps({'action': 'get_stats'}))
        
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                data = json.loads(message)
                
                if data['type'] == 'initial_state':
                    st.session_state.ws_orders = data['orders']
                    st.session_state.ws_stats['total_today'] = data['total_today']
                
                elif 'order_id' in data:  # New order
                    st.session_state.ws_orders.insert(0, data)
                    st.session_state.ws_orders = st.session_state.ws_orders[:50]  # Keep last 50
                
                elif data['type'] == 'stats_update':
                    st.session_state.ws_stats = data['stats']
                
                # Trigger Streamlit rerun
                st.rerun()
                
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                st.error("WebSocket connection lost. Reconnecting...")
                await asyncio.sleep(5)
                break

# Streamlit UI
st.set_page_config(page_title="RetailOS Live", layout="wide")

st.title("🔴 LIVE Order Stream")

# Connection status
if st.button("Connect to Live Stream"):
    asyncio.run(connect_websocket())

# Display stats
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Orders Today", st.session_state.ws_stats.get('total_orders_today', 0))
with col2:
    st.metric("Revenue Today", f"₹{st.session_state.ws_stats.get('revenue_today', 0):,.0f}")
with col3:
    st.metric("Avg Order Value", f"₹{st.session_state.ws_stats.get('avg_order_value', 0):,.0f}")
with col4:
    st.metric("Unique Customers", st.session_state.ws_stats.get('unique_customers', 0))

# Live order feed
st.subheader("Latest Orders")
for order in st.session_state.ws_orders[:10]:
    with st.container():
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        col1.write(f"**Order #{order['order_id']}**")
        col2.write(f"Product {order['product_id']}")
        col3.write(f"₹{order['price']:.2f} × {order['quantity']}")
        col4.write(order['order_source'])