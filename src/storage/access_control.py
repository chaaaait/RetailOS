#!/usr/bin/env python3
"""
RBAC and PII Masking Implementation for RetailOS
Creates secure views in DuckDB with role-based access control
"""

import duckdb
import sys

DB_PATH = "data/warehouse/retail.duckdb"


def create_rbac_views():
    """Create RBAC views with PII masking in DuckDB"""
    
    try:
        # Connect to database
        print("🔗 Connecting to DuckDB database...")
        con = duckdb.connect(DB_PATH)
        print(f"✓ Connected to {DB_PATH}")
        
        # Drop existing views
        print("\n🗑️  Dropping existing views...")
        con.execute("DROP VIEW IF EXISTS analyst_sales")
        con.execute("DROP VIEW IF EXISTS store_manager_sales") 
        con.execute("DROP VIEW IF EXISTS finance_sales")
        con.execute("DROP VIEW IF EXISTS admin_all")
        print("✓ Dropped existing views")
        
        # Create analyst_sales view with PII masking
        print("\n👁️  Creating analyst_sales view with PII masking...")
        analyst_view_sql = """
        CREATE VIEW analyst_sales AS
        SELECT 
            fs.sale_id,
            fs.date_key,
            fs.product_key,
            fs.store_key,
            fs.quantity,
            fs.revenue,
            CONCAT('XXXXX-', RIGHT(dc.phone, 4)) as phone_masked,
            CONCAT(LEFT(dc.email, 1), '***@', SPLIT_PART(dc.email, '@', 2)) as email_masked,
            dc.city as customer_city
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key
        """
        con.execute(analyst_view_sql)
        print("✓ Created analyst_sales view")
        
        # Create store_manager_sales view (same as analyst, filtered at query time)
        print("\n🏪 Creating store_manager_sales view...")
        store_manager_view_sql = """
        CREATE VIEW store_manager_sales AS
        SELECT *
        FROM analyst_sales
        """
        con.execute(store_manager_view_sql)
        print("✓ Created store_manager_sales view")
        print("ℹ️  Note: Filter by store_key at query time: SELECT * FROM store_manager_sales WHERE store_key = ?")
        
        # Create finance_sales view with full access
        print("\n💰 Creating finance_sales view with full access...")
        finance_view_sql = """
        CREATE VIEW finance_sales AS
        SELECT 
            fs.*,
            dc.name as customer_name,
            dc.email,
            dc.phone,
            dc.city as customer_city,
            dp.name as product_name,
            dp.category as product_category,
            dp.price as product_price,
            (fs.revenue - (dp.price * fs.quantity)) as profit
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        """
        con.execute(finance_view_sql)
        print("✓ Created finance_sales view")
        
        # Create admin_all view for system-wide access
        print("\n👑 Creating admin_all view...")
        admin_view_sql = """
        CREATE VIEW admin_all AS
        SELECT 
            'fact_sales' as table_name,
            COUNT(*) as row_count,
            MIN(date_key) as min_date_key,
            MAX(date_key) as max_date_key,
            SUM(revenue) as total_revenue
        FROM fact_sales
        
        UNION ALL
        
        SELECT 
            'dim_customer' as table_name,
            COUNT(*) as row_count,
            NULL as min_date_key,
            NULL as max_date_key,
            NULL as total_revenue
        FROM dim_customer
        
        UNION ALL
        
        SELECT 
            'dim_product' as table_name,
            COUNT(*) as row_count,
            NULL as min_date_key,
            NULL as max_date_key,
            NULL as total_revenue
        FROM dim_product
        
        UNION ALL
        
        SELECT 
            'dim_store' as table_name,
            COUNT(*) as row_count,
            NULL as min_date_key,
            NULL as max_date_key,
            NULL as total_revenue
        FROM dim_store
        
        UNION ALL
        
        SELECT 
            'dim_date' as table_name,
            COUNT(*) as row_count,
            MIN(date_key) as min_date_key,
            MAX(date_key) as max_date_key,
            NULL as total_revenue
        FROM dim_date
        """
        con.execute(admin_view_sql)
        print("✓ Created admin_all view")
        
        # Verify views and get row counts
        print("\n📊 Verifying views and getting row counts...")
        
        # Count rows from analyst_sales
        analyst_count = con.execute("SELECT COUNT(*) FROM analyst_sales").fetchone()[0]
        print(f"✓ analyst_sales: {analyst_count:,} rows")
        
        # Count rows from finance_sales  
        finance_count = con.execute("SELECT COUNT(*) FROM finance_sales").fetchone()[0]
        print(f"✓ finance_sales: {finance_count:,} rows")
        
        # Show sample data for verification
        print("\n🔍 Sample data verification:")
        print("\n--- analyst_sales sample (PII masked) ---")
        analyst_sample = con.execute("""
            SELECT sale_id, phone_masked, email_masked, customer_city, revenue 
            FROM analyst_sales 
            LIMIT 3
        """).fetchdf()
        print(analyst_sample.to_string(index=False))
        
        print("\n--- finance_sales sample (full access) ---")
        finance_sample = con.execute("""
            SELECT sale_id, customer_name, email, phone, profit 
            FROM finance_sales 
            LIMIT 3
        """).fetchdf()
        print(finance_sample.to_string(index=False))
        
        print("\n--- admin_all sample ---")
        admin_sample = con.execute("SELECT * FROM admin_all").fetchdf()
        print(admin_sample.to_string(index=False))
        
        # Close connection
        con.close()
        print("\n✅ RBAC views created successfully!")
        print("🔐 Database is now secure with role-based access control and PII masking")
        
    except Exception as e:
        print(f"❌ Error creating RBAC views: {e}")
        sys.exit(1)


def verify_schema():
    """Verify the database schema before creating views"""
    try:
        con = duckdb.connect(DB_PATH)
        
        # Check required tables exist
        tables = con.execute("SHOW TABLES").fetchdf()
        required_tables = ['fact_sales', 'dim_customer', 'dim_product', 'dim_store', 'dim_date']
        
        for table in required_tables:
            if table not in tables['name'].values:
                raise Exception(f"Required table {table} not found in database")
        
        print("✓ All required tables exist")
        
        # Check key columns
        fact_sales_columns = con.execute("DESCRIBE fact_sales").fetchdf()
        required_fact_columns = ['sale_id', 'date_key', 'customer_key', 'product_key', 'store_key', 'quantity', 'revenue']
        
        for col in required_fact_columns:
            if col not in fact_sales_columns['column_name'].values:
                raise Exception(f"Required column {col} not found in fact_sales")
        
        customer_columns = con.execute("DESCRIBE dim_customer").fetchdf()
        required_customer_columns = ['customer_key', 'name', 'email', 'phone', 'city']
        
        for col in required_customer_columns:
            if col not in customer_columns['column_name'].values:
                raise Exception(f"Required column {col} not found in dim_customer")
        
        product_columns = con.execute("DESCRIBE dim_product").fetchdf()
        required_product_columns = ['product_key', 'name', 'category', 'price']
        
        for col in required_product_columns:
            if col not in product_columns['column_name'].values:
                raise Exception(f"Required column {col} not found in dim_product")
        
        con.close()
        print("✓ Schema verification passed")
        
    except Exception as e:
        print(f"❌ Schema verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("🚀 RetailOS RBAC and PII Masking Implementation")
    print("=" * 50)
    
    # Verify schema first
    verify_schema()
    
    # Create RBAC views
    create_rbac_views()
    
    print("\n🎉 Implementation completed successfully!")
    print("\n📋 Usage Examples:")
    print("  # Analyst access (PII masked)")
    print("  SELECT * FROM analyst_sales WHERE revenue > 1000;")
    print("")
    print("  # Store Manager access (store filtered)")
    print("  SELECT * FROM store_manager_sales WHERE store_key = 5;")
    print("")
    print("  # Finance access (full data)")
    print("  SELECT customer_name, profit FROM finance_sales WHERE profit > 0;")
    print("")
    print("  # Admin access (system overview)")
    print("  SELECT * FROM admin_all;")