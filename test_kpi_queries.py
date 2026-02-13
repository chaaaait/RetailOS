#!/usr/bin/env python3
"""
Test script for KPI queries
Validates each KPI query and provides sample output
"""

import duckdb
import os
import sys
from pathlib import Path

DB_PATH = "data/warehouse/retail.duckdb"
KPI_QUERIES_FILE = "src/analytics/kpi_queries.sql"

def test_database_connection():
    """Test if database is accessible"""
    try:
        con = duckdb.connect(DB_PATH)
        tables = con.execute("SHOW TABLES").fetchdf()
        print(f"✓ Database connected successfully")
        print(f"✓ Available tables: {', '.join(tables['name'].tolist())}")
        
        # Check fact_sales data
        count = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        print(f"✓ fact_sales has {count:,} rows")
        
        con.close()
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

def test_kpi_queries():
    """Test each KPI query"""
    if not os.path.exists(KPI_QUERIES_FILE):
        print(f"✗ KPI queries file not found: {KPI_QUERIES_FILE}")
        return False
    
    try:
        con = duckdb.connect(DB_PATH)
        
        with open(KPI_QUERIES_FILE, 'r') as f:
            content = f.read()
        
        # Split by KPI sections
        kpi_sections = content.split('-- =====================================================')[1:]
        
        print(f"\n📊 Testing {len(kpi_sections)} KPI queries...\n")
        
        for i, section in enumerate(kpi_sections, 1):
            lines = section.strip().split('\n')
            title_line = next((line for line in lines if line.strip().startswith('--')), '')
            title = title_line.replace('--', '').strip() if title_line else f"KPI {i}"
            
            # Extract the query (between comments)
            query_lines = []
            in_query = False
            
            for line in lines:
                if line.strip().startswith('-- Description:') or line.strip().startswith('-- Business Use:') or line.strip().startswith('-- Dependencies:'):
                    continue
                if not line.strip().startswith('--') and line.strip():
                    in_query = True
                if in_query and line.strip():
                    query_lines.append(line)
                elif in_query and line.strip().startswith('--') and 'Additional Utility' not in line:
                    break
            
            if query_lines:
                query = '\n'.join(query_lines)
                
                try:
                    result = con.execute(query).fetchdf()
                    print(f"✓ {title}")
                    print(f"  Rows returned: {len(result)}")
                    if len(result) > 0:
                        print(f"  Sample columns: {', '.join(result.columns[:3])}{'...' if len(result.columns) > 3 else ''}")
                        if len(result) > 0:
                            sample_data = result.head(2).to_string(index=False, max_cols=3)
                            print(f"  Sample data:\n    {sample_data.replace(chr(10), chr(10) + '    ')}")
                    print()
                    
                except Exception as e:
                    print(f"✗ {title} - Error: {e}")
                    print(f"  Query: {query[:100]}...\n")
        
        con.close()
        return True
        
    except Exception as e:
        print(f"✗ Error testing KPI queries: {e}")
        return False

def test_rbac_views():
    """Test RBAC views creation"""
    try:
        from src.storage.access_control import create_rbac_views
        
        print("🔐 Testing RBAC views creation...")
        create_rbac_views()
        
        con = duckdb.connect(DB_PATH)
        
        # Test each view
        views = ['analyst_sales', 'finance_sales', 'admin_all']
        
        for view in views:
            try:
                result = con.execute(f"SELECT COUNT(*) FROM {view} LIMIT 1").fetchone()
                print(f"✓ View {view} accessible")
            except Exception as e:
                print(f"✗ View {view} failed: {e}")
        
        con.close()
        return True
        
    except Exception as e:
        print(f"✗ RBAC views test failed: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 RetailOS KPI and RBAC Testing\n")
    
    # Test database connection
    if not test_database_connection():
        sys.exit(1)
    
    # Test RBAC views
    test_rbac_views()
    
    # Test KPI queries
    test_kpi_queries()
    
    print("✅ Testing completed!")

if __name__ == "__main__":
    main()
