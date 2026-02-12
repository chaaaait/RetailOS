"""
Role-Based Access Control (RBAC) and PII Masking for RetailOS
Implements data access policies and privacy controls for the data warehouse.
"""

import duckdb
from enum import Enum
from typing import Dict, List, Optional, Any
import os

DB_PATH = "data/warehouse/retail.duckdb"


class UserRole(Enum):
    """User roles for RBAC system"""
    ANALYST = "analyst"
    STORE_MANAGER = "store_manager"
    FINANCE = "finance"
    ADMIN = "admin"


class AccessLevel(Enum):
    """Data access levels"""
    MASKED_PII = "masked_pii"  # Phone/email masked, no cost/profit
    STORE_FILTERED = "store_filtered"  # Same as analyst + store filter
    FULL_ACCESS = "full_access"  # Complete data access
    ADMIN_ALL = "admin_all"  # All tables and schemas


class AccessController:
    """Manages role-based access control and data masking"""
    
    def __init__(self):
        self.con = duckdb.connect(DB_PATH)
        self._setup_views()
    
    def _setup_views(self):
        """Create RBAC views in DuckDB"""
        views = [
            # analyst_sales: No cost/profit, masked phone/email
            """
            CREATE OR REPLACE VIEW analyst_sales AS
            SELECT 
                fs.sale_id, 
                fs.date_key, 
                fs.product_key, 
                fs.store_key, 
                fs.quantity, 
                fs.revenue,
                CONCAT('XXXXX-', RIGHT(dc.phone, 4)) as phone_masked,
                CONCAT(LEFT(dc.email, 1), '***@', SPLIT_PART(dc.email, '@', 2)) as email_masked,
                dc.city as customer_city,
                dp.category as product_category
            FROM fact_sales fs
            JOIN dim_customer dc ON fs.customer_key = dc.customer_key
            JOIN dim_product dp ON fs.product_key = dp.product_key
            """,
            
            # store_manager_sales: Same as analyst + filtered by their store
            """
            CREATE OR REPLACE VIEW store_manager_sales AS
            SELECT * FROM analyst_sales
            WHERE store_key = $1
            """,
            
            # finance_sales: Full access to sales data
            """
            CREATE OR REPLACE VIEW finance_sales AS 
            SELECT 
                fs.*,
                dc.name as customer_name,
                dc.email,
                dc.phone,
                dc.city as customer_city,
                dp.product_name,
                dp.category as product_category,
                dp.cost_price,
                (fs.revenue - (dp.cost_price * fs.quantity)) as profit
            FROM fact_sales fs
            JOIN dim_customer dc ON fs.customer_key = dc.customer_key
            JOIN dim_product dp ON fs.product_key = dp.product_key
            """,
            
            # admin_all: Full access to all tables
            """
            CREATE OR REPLACE VIEW admin_all AS
            SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
            UNION ALL
            SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer
            UNION ALL
            SELECT 'dim_product' as table_name, COUNT(*) as row_count FROM dim_product
            UNION ALL
            SELECT 'dim_store' as table_name, COUNT(*) as row_count FROM dim_store
            UNION ALL
            SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
            """
        ]
        
        for view_sql in views:
            try:
                self.con.execute(view_sql)
                print(f"✓ Created view: {view_sql.split('VIEW')[1].split('AS')[0].strip()}")
            except Exception as e:
                print(f"✗ Error creating view: {e}")
    
    def get_accessible_view(self, role: UserRole, store_key: Optional[int] = None) -> str:
        """Get the appropriate view for a user role"""
        view_mapping = {
            UserRole.ANALYST: "analyst_sales",
            UserRole.FINANCE: "finance_sales",
            UserRole.ADMIN: "admin_all"
        }
        
        if role == UserRole.STORE_MANAGER:
            if store_key is None:
                raise ValueError("Store manager role requires store_key parameter")
            return f"store_manager_sales WHERE store_key = {store_key}"
        
        return view_mapping.get(role, "analyst_sales")
    
    def execute_query(self, role: UserRole, query: str, store_key: Optional[int] = None) -> List[Dict]:
        """Execute query with role-based access control"""
        try:
            # For store managers, we need to inject the store filter
            if role == UserRole.STORE_MANAGER:
                if store_key is None:
                    raise ValueError("Store manager role requires store_key parameter")
                
                # Replace view references with store-filtered version
                if "store_manager_sales" in query:
                    query = query.replace("store_manager_sales", f"(SELECT * FROM store_manager_sales WHERE store_key = {store_key})")
            
            result = self.con.execute(query).fetchdf()
            return result.to_dict('records')
        except Exception as e:
            raise Exception(f"Query execution failed: {e}")
    
    def mask_pii(self, data: List[Dict], pii_fields: List[str]) -> List[Dict]:
        """Apply PII masking to sensitive fields"""
        masked_data = []
        
        for record in data:
            masked_record = record.copy()
            for field in pii_fields:
                if field in masked_record and masked_record[field]:
                    if field == 'phone':
                        masked_record[field] = f"XXXXX-{str(masked_record[field])[-4:]}"
                    elif field == 'email':
                        email = str(masked_record[field])
                        parts = email.split('@')
                        if len(parts) == 2:
                            masked_record[field] = f"{parts[0][0]}***@{parts[1]}"
                    elif field == 'name':
                        name = str(masked_record[field])
                        if len(name) > 2:
                            masked_record[field] = f"{name[0]}{'*' * (len(name) - 2)}{name[-1]}"
            masked_data.append(masked_record)
        
        return masked_data
    
    def get_role_permissions(self, role: UserRole) -> Dict[str, Any]:
        """Get permissions for a specific role"""
        permissions = {
            UserRole.ANALYST: {
                "access_level": AccessLevel.MASKED_PII,
                "tables": ["analyst_sales"],
                "pii_fields": ["phone", "email"],
                "restricted_fields": ["cost_price", "profit"],
                "can_aggregate": True,
                "can_export": True
            },
            UserRole.STORE_MANAGER: {
                "access_level": AccessLevel.STORE_FILTERED,
                "tables": ["store_manager_sales"],
                "pii_fields": ["phone", "email"],
                "restricted_fields": ["cost_price", "profit"],
                "can_aggregate": True,
                "can_export": True,
                "store_filter_required": True
            },
            UserRole.FINANCE: {
                "access_level": AccessLevel.FULL_ACCESS,
                "tables": ["finance_sales"],
                "pii_fields": [],
                "restricted_fields": [],
                "can_aggregate": True,
                "can_export": True,
                "can_access_financials": True
            },
            UserRole.ADMIN: {
                "access_level": AccessLevel.ADMIN_ALL,
                "tables": ["admin_all", "fact_sales", "dim_customer", "dim_product", "dim_store", "dim_date"],
                "pii_fields": [],
                "restricted_fields": [],
                "can_aggregate": True,
                "can_export": True,
                "can_access_all": True
            }
        }
        
        return permissions.get(role, permissions[UserRole.ANALYST])
    
    def validate_query(self, role: UserRole, query: str) -> bool:
        """Validate if query is allowed for the given role"""
        permissions = self.get_role_permissions(role)
        
        # Basic SQL injection protection
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        query_upper = query.upper()
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                return False
        
        # Check table access
        for table in permissions["tables"]:
            if table in query_upper:
                return True
        
        return False
    
    def close(self):
        """Close database connection"""
        if self.con:
            self.con.close()


def create_rbac_views():
    """Initialize RBAC views in the database"""
    controller = AccessController()
    controller.close()
    print("✓ RBAC views created successfully")


if __name__ == "__main__":
    create_rbac_views()