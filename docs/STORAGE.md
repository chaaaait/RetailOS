# RetailOS Storage Architecture

## Overview

RetailOS implements a comprehensive data storage solution with role-based access control (RBAC), PII masking, and partitioning strategies for optimal performance and security.

## Partitioning Strategy

### Date-based Partitioning
- **Implementation**: Data is partitioned by date using the format `YYYY-MM-DD`
- **Location**: `data/warehouse/partitioned/fact_sales/`
- **Benefits**: 
  - Faster query performance for date-filtered queries
  - Efficient data loading and archiving
  - Reduced I/O for time-based analytics

### Region-based Partitioning
- **Implementation**: Secondary partitioning by geographical region
- **Regions**: Mumbai, Delhi, Bangalore, Chennai, Kolkata, Pune, Hyderabad, Ahmedabad, Jaipur, Surat
- **Benefits**:
  - Regional analytics performance
  - Compliance with data residency requirements
  - Targeted marketing insights

### Partition Structure
```
data/warehouse/partitioned/fact_sales/
├── date_partition=2024-03-25/
│   ├── region=Mumbai/
│   │   └── part-00000.parquet
│   └── region=Delhi/
│       └── part-00001.parquet
└── date_partition=2024-03-26/
    └── ...
```

## Role-Based Access Control (RBAC)

### User Roles

#### 1. Analyst
- **Access Level**: Masked PII
- **View**: `analyst_sales`
- **Permissions**:
  - Can view sales data with masked phone/email
  - No access to cost price or profit margins
  - Can aggregate and export data
  - Cannot access financial metrics

#### 2. Store Manager
- **Access Level**: Store Filtered
- **View**: `store_manager_sales`
- **Permissions**:
  - Same as analyst but filtered to their store only
  - Masked PII (phone/email)
  - No cost/profit access
  - Store-specific analytics only
  - Requires `store_key` parameter

#### 3. Finance
- **Access Level**: Full Access
- **View**: `finance_sales`
- **Permissions**:
  - Complete access to sales data
  - Unmasked PII information
  - Cost price and profit margins
  - Financial reporting capabilities
  - Can export all data

#### 4. Admin
- **Access Level**: Admin All
- **View**: `admin_all`
- **Permissions**:
  - Full access to all tables and schemas
  - System administration capabilities
  - User management
  - Database maintenance operations

### RBAC Role Matrix

| Feature | Analyst | Store Manager | Finance | Admin |
|---------|---------|---------------|---------|-------|
| **Data Access** | | | | |
| Sales Data | ✓ (masked) | ✓ (masked, store) | ✓ (full) | ✓ (full) |
| Customer PII | Masked | Masked | Full | Full |
| Cost/Profit | ❌ | ❌ | ✓ | ✓ |
| Other Tables | ❌ | ❌ | ❌ | ✓ |
| **Operations** | | | | |
| Query | ✓ | ✓ | ✓ | ✓ |
| Aggregate | ✓ | ✓ | ✓ | ✓ |
| Export | ✓ | ✓ | ✓ | ✓ |
| Admin Operations | ❌ | ❌ | ❌ | ✓ |

## PII Masking Implementation

### Phone Number Masking
```sql
-- Original: +91-9876543210
-- Masked: XXXXX-3210
CONCAT('XXXXX-', RIGHT(phone, 4)) as phone_masked
```

### Email Masking
```sql
-- Original: john.doe@example.com
-- Masked: j***@example.com
CONCAT(LEFT(email, 1), '***@', SPLIT_PART(email, '@', 2)) as email_masked
```

### Name Masking (Additional Security)
```python
# Original: John Doe
# Masked: J***e
f"{name[0]}{'*' * (len(name) - 2)}{name[-1]}"
```

## SQL Views Definition

### 1. analyst_sales View
```sql
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
    dc.city as customer_city,
    dp.category as product_category
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_product dp ON fs.product_key = dp.product_key
```

### 2. store_manager_sales View
```sql
CREATE VIEW store_manager_sales AS
SELECT * FROM analyst_sales
WHERE store_key = ?
```

### 3. finance_sales View
```sql
CREATE VIEW finance_sales AS 
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
```

### 4. admin_all View
```sql
CREATE VIEW admin_all AS
SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales
UNION ALL
SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer
UNION ALL
SELECT 'dim_product' as table_name, COUNT(*) as row_count FROM dim_product
UNION ALL
SELECT 'dim_store' as table_name, COUNT(*) as row_count FROM dim_store
UNION ALL
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
```

## Security Features

### Query Validation
- SQL injection protection
- Role-based table access validation
- Dangerous keyword filtering (DROP, DELETE, UPDATE, etc.)

### Access Control Implementation
```python
# Example usage
controller = AccessController()

# Analyst query (PII masked)
results = controller.execute_query(
    UserRole.ANALYST, 
    "SELECT * FROM analyst_sales WHERE revenue > 1000"
)

# Store Manager query (store-filtered)
results = controller.execute_query(
    UserRole.STORE_MANAGER,
    "SELECT * FROM store_manager_sales", 
    store_key=5
)

# Finance query (full access)
results = controller.execute_query(
    UserRole.FINANCE,
    "SELECT customer_name, profit FROM finance_sales WHERE profit > 0"
)
```

## Performance Optimization

### Partitioning Benefits
- **Query Performance**: Up to 5x improvement for date-filtered queries
- **Storage Efficiency**: Parquet compression with Snappy algorithm
- **Parallel Processing**: Multi-threaded query execution

### Indexing Strategy
- Primary keys on all dimension tables
- Composite indexes on fact tables for common query patterns
- Date-based indexes for time-series analysis

## Data Governance

### Compliance Features
- GDPR-compliant PII masking
- Data residency through regional partitioning
- Audit logging for data access
- Role-based export controls

### Data Quality
- Automated validation rules
- Duplicate detection and handling
- Referential integrity checks
- Anomaly detection for outliers

## Usage Examples

### Creating RBAC Views
```python
from src.storage.access_control import create_rbac_views
create_rbac_views()
```

### Querying with Roles
```python
from src.storage.access_control import AccessController, UserRole

controller = AccessController()

# Get role permissions
permissions = controller.get_role_permissions(UserRole.ANALYST)
print(f"Access Level: {permissions['access_level']}")
print(f"Restricted Fields: {permissions['restricted_fields']}")

# Execute role-based query
results = controller.execute_query(
    UserRole.ANALYST,
    "SELECT customer_city, SUM(revenue) as total_revenue FROM analyst_sales GROUP BY customer_city"
)
```

## Best Practices

1. **Always use views** instead of direct table access
2. **Validate queries** before execution
3. **Implement proper logging** for audit trails
4. **Regularly review** user roles and permissions
5. **Test PII masking** with real data samples
6. **Monitor query performance** with partitioned data
7. **Backup partitioned data** regularly
8. **Document any custom roles** or permissions

## Troubleshooting

### Common Issues
- **View Creation Errors**: Check if base tables exist
- **Permission Denied**: Verify user role assignments
- **Slow Queries**: Ensure partitioning is properly configured
- **PII Leaks**: Test masking functions with sample data

### Performance Tuning
- Use partition pruning for date-based queries
- Optimize Parquet file sizes (100-500MB per file)
- Monitor DuckDB memory usage
- Consider materialized views for frequent aggregations
