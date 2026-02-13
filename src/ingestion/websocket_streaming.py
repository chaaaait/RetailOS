import asyncio
import websockets
import json
import duckdb
from datetime import datetime
import random
import os
import sys
from pathlib import Path

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config import DB_PATH
except ImportError:
    DB_PATH = 'data/warehouse/retail.duckdb'

class WebSocketOrderStream:
    def __init__(self, host='localhost', port=8765):
        self.host = host
        self.port = port
        self.connected_clients = set()
        # Initialize DB connection for the main thread/loop
        try:
            self.con = duckdb.connect(str(DB_PATH))
            # Create streaming table if not exists
            self.con.execute("""
            CREATE TABLE IF NOT EXISTS streaming_orders (
                order_id INTEGER PRIMARY KEY,
                timestamp TIMESTAMP,
                product_id INTEGER,
                customer_id INTEGER,
                store_id INTEGER,
                quantity INTEGER,
                price DECIMAL,
                payment_method VARCHAR,
                order_source VARCHAR
            )
            """)
        except Exception as e:
            print(f"Database connection error: {e}")
            raise
        
    async def register_client(self, websocket):
        """Register new client connection"""
        self.connected_clients.add(websocket)
        print(f"Client connected. Total clients: {len(self.connected_clients)}")
        
    async def unregister_client(self, websocket):
        """Remove disconnected client"""
        self.connected_clients.remove(websocket)
        print(f"Client disconnected. Total clients: {len(self.connected_clients)}")
    
    async def broadcast_order(self, order_data):
        """Send order to all connected clients"""
        if self.connected_clients:
            message = json.dumps(order_data)
            await asyncio.gather(
                *[client.send(message) for client in self.connected_clients],
                return_exceptions=True
            )
    
    async def order_generator(self):
        """Generate realistic orders continuously"""
        try:
            order_id = self.con.execute("SELECT COALESCE(MAX(order_id), 0) FROM streaming_orders").fetchone()[0] + 1
        except:
            order_id = 1
        
        while True:
            try:
                # Generate order
                order = {
                    'order_id': order_id,
                    'timestamp': datetime.now().isoformat(),
                    'product_id': random.randint(1, 500),
                    'customer_id': random.randint(1, 10000),
                    'store_id': random.randint(1, 50),
                    'quantity': random.randint(1, 5),
                    'price': round(random.uniform(100, 5000), 2),
                    'payment_method': random.choice(['UPI', 'Card', 'Cash', 'Wallet']),
                    'order_source': random.choice(['Web', 'App', 'POS'])
                }
                
                # Save to database
                self.con.execute(f"""
                INSERT INTO streaming_orders VALUES (
                    {order['order_id']},
                    '{order['timestamp']}',
                    {order['product_id']},
                    {order['customer_id']},
                    {order['store_id']},
                    {order['quantity']},
                    {order['price']},
                    '{order['payment_method']}',
                    '{order['order_source']}'
                )
                """)
                
                # Broadcast to all connected clients
                await self.broadcast_order(order)
                
                print(f"✓ Order #{order_id} — ₹{order['price']:.2f} — {len(self.connected_clients)} clients notified")
                
                order_id += 1
                
                # Variable delay (1-5 seconds for realistic flow)
                await asyncio.sleep(random.uniform(1, 5))
            except Exception as e:
                print(f"Error generating order: {e}")
                await asyncio.sleep(5) # Wait before retrying
    
    def get_realtime_stats(self):
        """Calculate real-time statistics"""
        try:
            stats = self.con.execute("""
            SELECT 
                COUNT(*) as total_orders_today,
                COALESCE(SUM(price * quantity), 0) as revenue_today,
                COALESCE(AVG(price * quantity), 0) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM streaming_orders
            WHERE DATE(timestamp) = CURRENT_DATE
            """).fetchdf().to_dict('records')[0]
            
            return {
                'type': 'stats_update',
                'timestamp': datetime.now().isoformat(),
                'stats': stats
            }
        except Exception as e:
            print(f"Error calculating stats: {e}")
            return {'type': 'error', 'message': str(e)}

    async def handle_client(self, websocket, path):
        """Handle individual client connections"""
        await self.register_client(websocket)
        
        try:
            # Send current state on connection
            try:
                recent_orders = self.con.execute("""
                SELECT * FROM streaming_orders 
                ORDER BY timestamp DESC 
                LIMIT 10
                """).fetchdf().to_dict('records')
                
                # Convert timestamps for JSON
                for order in recent_orders:
                    if isinstance(order.get('timestamp'), (datetime, pd.Timestamp)):
                        order['timestamp'] = order['timestamp'].isoformat()

            except Exception as e:
                print(f"Error fetching recent orders: {e}")
                recent_orders = []
            
            try:
                total_today = self.con.execute("""
                    SELECT COUNT(*) FROM streaming_orders 
                    WHERE DATE(timestamp) = CURRENT_DATE
                """).fetchone()[0]
            except:
                total_today = 0

            await websocket.send(json.dumps({
                'type': 'initial_state',
                'orders': recent_orders,
                'total_today': total_today
            }))
            
            # Keep connection alive and listen for client messages
            async for message in websocket:
                # Client can send commands (e.g., {"action": "pause"})
                try:
                    data = json.loads(message)
                    if data.get('action') == 'get_stats':
                        stats = self.get_realtime_stats()
                        await websocket.send(json.dumps(stats))
                except json.JSONDecodeError:
                    pass
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            await self.unregister_client(websocket)
    
    async def start_server(self):
        """Start WebSocket server"""
        # Start order generator in background
        asyncio.create_task(self.order_generator())
        
        # Start WebSocket server
        try:
            async with websockets.serve(self.handle_client, self.host, self.port):
                print(f"🚀 WebSocket server running on ws://{self.host}:{self.port}")
                print("Generating orders every 1-5 seconds...")
                print("Press Ctrl+C to stop...")
                await asyncio.Future()  # Run forever
        except OSError as e:
            print(f"Error starting server on port {self.port}: {e}")
        except KeyboardInterrupt:
            print("\nShutting down WebSocket server...")
            self.con.close()

# Run server
if __name__ == "__main__":
    stream = WebSocketOrderStream()
    try:
        asyncio.run(stream.start_server())
    except KeyboardInterrupt:
        pass