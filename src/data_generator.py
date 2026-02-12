import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Initialize Faker with Indian locale
fake = Faker('en_IN')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Constants
NUM_TRANSACTIONS = 100000
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 500
NUM_STORES = 50
NUM_SHIPMENTS = 5000
NUM_CLICKSTREAM = 50000

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", 
    "Pune", "Hyderabad", "Ahmedabad", "Jaipur", "Surat"
]

CATEGORIES = [
    "Electronics", "Apparel", "Grocery", "Home & Kitchen", "Personal Care"
]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 6, 30)
DAYS_RANGE = (END_DATE - START_DATE).days + 1

DATA_DIR = "data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

def generate_stores():
    print("Generating Stores...")
    stores = []
    for i in range(NUM_STORES):
        city = random.choice(CITIES)
        stores.append({
            "store_id": f"ST{i:03d}",
            "store_name": f"RetailOS {city} {random.randint(1, 10)}",
            "city": city,
            "opened_date": fake.date_between(start_date='-5y', end_date='-1y')
        })
    df = pd.DataFrame(stores)
    df.to_csv(os.path.join(DATA_DIR, "stores.csv"), index=False)
    return df

def generate_customers():
    print("Generating Customers...")
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            "customer_id": f"CUST{i:05d}",
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": random.choice(CITIES),
            "age": random.randint(18, 70),
            "registration_date": fake.date_between(start_date='-3y', end_date=END_DATE)
        })
    df = pd.DataFrame(customers)
    df.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)
    return df

def generate_products():
    print("Generating Products...")
    products = []
    for i in range(NUM_PRODUCTS):
        category = random.choice(CATEGORIES)
        price_range = {
            "Electronics": (5000, 50000),
            "Apparel": (500, 5000),
            "Grocery": (50, 2000),
            "Home & Kitchen": (1000, 10000),
            "Personal Care": (100, 1500)
        }
        base_price = random.randint(*price_range[category])
        products.append({
            "product_id": f"P{i:04d}",
            "product_name": f"{category} Item {i}",
            "category": category,
            "base_price": base_price,
            "cost_price": int(base_price * 0.7)
        })
    df = pd.DataFrame(products)
    df.to_csv(os.path.join(DATA_DIR, "products.csv"), index=False)
    return df

def get_weighted_date():
    # Helper to generate dates with festive spikes
    # Holi 2024: March 25
    # Eid 2024: April 11
    
    dates = [START_DATE + timedelta(days=x) for x in range(DAYS_RANGE)]
    weights = []
    
    for d in dates:
        w = 1.0
        # Holi Spike
        if d.month == 3 and 20 <= d.day <= 30:
            w = 2.5
        # Eid Spike
        if d.month == 4 and 5 <= d.day <= 15:
            w = 2.0
        # Weekends
        if d.weekday() >= 5:
            w *= 1.3
        weights.append(w)
    
    weights = np.array(weights)
    weights /= weights.sum()
    
    return np.random.choice(dates, p=weights)

def generate_transactions(stores_df, customers_df, products_df):
    print("Generating Transactions...")
    transactions = []
    
    store_ids = stores_df['store_id'].tolist()
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    product_prices = dict(zip(products_df['product_id'], products_df['base_price']))

    for i in range(NUM_TRANSACTIONS):
        date = get_weighted_date()
        pid = random.choice(product_ids)
        price = product_prices[pid]
        
        # Intentional Data Quality: Negative Price (0.5%)
        if random.random() < 0.005:
            price = -1 * price
            
        # Intentional Data Quality: Future Timestamp (0.3%)
        if random.random() < 0.003:
            date = date + timedelta(days=365)

        # Intentional Data Quality: Missing Customer ID (1%)
        cid = random.choice(customer_ids)
        if random.random() < 0.01:
            cid = None

        transactions.append({
            "transaction_id": f"TXN{i:08d}",
            "date": date,
            "store_id": random.choice(store_ids),
            "customer_id": cid,
            "product_id": pid,
            "quantity": random.randint(1, 5),
            "price": price,
            "payment_method": random.choice(["Cash", "UPI", "Credit Card", "Debit Card"])
        })

    # Intentional Data Quality: Duplicates (2%)
    num_duplicates = int(NUM_TRANSACTIONS * 0.02)
    duplicates = random.choices(transactions, k=num_duplicates)
    transactions.extend(duplicates)
    
    # Shuffle
    random.shuffle(transactions)
    
    df = pd.DataFrame(transactions)
    df.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)
    return df

def generate_inventory(stores_df, products_df):
    print("Generating Inventory (Snapshot)...")
    # Generating full daily snapshots for 6 months is heavy (4.5M rows). 
    # Let's generate a simplified version: Monthly snapshots or just current state?
    # User asked for "daily snapshots for 6 months". 
    # 50 stores * 500 products * 180 days = 4.5M rows. 
    # We will generate it efficiently.
    
    store_ids = stores_df['store_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    
    # Create a base frame for one day
    base_data = []
    for s in store_ids:
        for p in product_ids:
            base_data.append({
                "store_id": s,
                "product_id": p
            })
    base_df = pd.DataFrame(base_data)
    
    # We will just generate for the 1st of each month to save time/space for this demo, 
    # BUT user asked for daily. Let's try to generate daily but maybe write in chunks if needed.
    # Actually, 4.5M rows is fine for modern pandas/csv.
    
    dates = [START_DATE + timedelta(days=x) for x in range(DAYS_RANGE)]
    
    # To reduce file size and time, let's just do weekly snapshots?
    # User requirement: "daily snapshots". I'll stick to it but maybe reduce scope if it fails?
    # Let's try weekly to start, or maybe just 1 month?
    # "daily snapshots for 6 months" -> OK, I will generate a smaller subset for demonstration
    # or I'll implement a generator that writes to CSV without holding all in RAM.
    
    # Strategy: Write headers, then append daily chunks.
    inventory_file = os.path.join(DATA_DIR, "inventory.csv")
    
    # Check if file exists to avoid re-running heavy op
    if os.path.exists(inventory_file):
         print("Inventory file exists, skipping (delete to regenerate).")
         return
         
    # Optimization: Generate random inventory levels
    # We can just generate `store_id`, `product_id`, `date`, `stock_level`
    
    # Write header
    with open(inventory_file, 'w') as f:
        f.write("date,store_id,product_id,stock_level\n")
        
    for d in dates:
        # Vectorized generation for one day
        daily_df = base_df.copy()
        daily_df['date'] = d
        # Random stock levels
        daily_df['stock_level'] = np.random.randint(0, 100, size=len(daily_df))
        
        daily_df.to_csv(inventory_file, mode='a', header=False, index=False)
        
    print(f"Inventory generated at {inventory_file}")

def generate_shipments(transactions_df):
    print("Generating Shipments...")
    # Sample from transactions to create shipments
    # Shipments usually track orders. Let's assume 1 transaction = 1 potential shipment if applicable
    # We need 5000 shipments.
    
    subset = transactions_df.sample(n=NUM_SHIPMENTS)
    shipments = []
    
    for _, row in subset.iterrows():
        delivery_days = random.randint(1, 10)
        ship_date = pd.to_datetime(row['date']) + timedelta(days=1)
        delivery_date = ship_date + timedelta(days=delivery_days)
        
        status = "Delivered"
        if delivery_date > datetime.now(): # If delivery is in future relative to "now" (simulated)
             status = "In Transit"
             
        shipments.append({
            "shipment_id": f"SHP{random.randint(10000, 99999)}",
            "transaction_id": row['transaction_id'],
            "ship_date": ship_date,
            "delivery_date": delivery_date,
            "status": status,
            "courier": random.choice(["BlueDart", "Delhivery", "EcomExpress", "Shadowfax"])
        })
        
    df = pd.DataFrame(shipments)
    df.to_csv(os.path.join(DATA_DIR, "shipments.csv"), index=False)
    return df

def generate_web_clickstream(products_df):
    print("Generating Web Clickstream...")
    events = []
    product_ids = products_df['product_id'].tolist()
    event_types = ["view_item", "add_to_cart", "remove_from_cart", "purchase", "search"]
    
    for i in range(NUM_CLICKSTREAM):
        date = get_weighted_date()
        events.append({
            "session_id": f"SES{random.randint(100000, 999999)}",
            "timestamp": date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59)),
            "event_type": np.random.choice(event_types, p=[0.5, 0.2, 0.1, 0.05, 0.15]), # Weighted
            "product_id": random.choice(product_ids) if random.random() > 0.2 else None,
            "user_id": f"U{random.randint(1, NUM_CUSTOMERS)}" if random.random() > 0.6 else None,
            "device": random.choice(["Mobile", "Desktop", "Tablet"])
        })
        
    df = pd.DataFrame(events)
    df.to_csv(os.path.join(DATA_DIR, "web_clickstream.csv"), index=False)
    return df

if __name__ == "__main__":
    stores_df = generate_stores()
    customers_df = generate_customers()
    products_df = generate_products()
    transactions_df = generate_transactions(stores_df, customers_df, products_df)
    # Passed DF linking might be needed for consistency, but for raw generation this is fine
    generate_inventory(stores_df, products_df)
    generate_shipments(transactions_df)
    generate_web_clickstream(products_df)
    print("Data generation complete!")
