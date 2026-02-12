-- RetailOS KPI Queries
-- Key Performance Indicators for Business Intelligence
-- Compatible with DuckDB and the RetailOS data warehouse schema

-- =====================================================
-- 1. Daily Revenue
-- =====================================================
-- Description: Total revenue generated each day
-- Business Use: Track daily sales performance, identify trends
-- Dependencies: fact_sales, dim_date

SELECT 
    dd.date,
    dd.day_name,
    dd.month_name,
    dd.year,
    SUM(fs.revenue) as daily_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.date, dd.day_name, dd.month_name, dd.year
ORDER BY dd.date DESC
LIMIT 30;

-- =====================================================
-- 2. Monthly Revenue
-- =====================================================
-- Description: Total revenue generated each month
-- Business Use: Monthly performance tracking, budget vs actual
-- Dependencies: fact_sales, dim_date

SELECT 
    dd.year,
    dd.month_name,
    dd.month,
    SUM(fs.revenue) as monthly_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold,
    LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month) as prev_month_revenue,
    ROUND((SUM(fs.revenue) - LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month)) / 
          LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month) * 100, 2) as revenue_growth_pct
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month_name, dd.month
ORDER BY dd.year, dd.month;

-- =====================================================
-- 3. City-wise Sales
-- =====================================================
-- Description: Sales performance by city
-- Business Use: Regional performance analysis, market penetration
-- Dependencies: fact_sales, dim_store

SELECT 
    ds.city,
    ds.region,
    COUNT(DISTINCT fs.store_key) as active_stores,
    SUM(fs.revenue) as total_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold,
    ROUND(SUM(fs.revenue) * 100.0 / SUM(SUM(fs.revenue)) OVER (), 2) as revenue_share_pct
FROM fact_sales fs
JOIN dim_store ds ON fs.store_key = ds.store_key
GROUP BY ds.city, ds.region
ORDER BY total_revenue DESC;

-- =====================================================
-- 4. Top Selling Products
-- =====================================================
-- Description: Best performing products by revenue and quantity
-- Business Use: Inventory planning, marketing focus, product mix optimization
-- Dependencies: fact_sales, dim_product

SELECT 
    dp.product_id,
    dp.product_name,
    dp.category,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.revenue) as total_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_revenue_per_transaction,
    AVG(dp.base_price) as avg_product_price,
    ROUND(SUM(fs.revenue) * 100.0 / SUM(SUM(fs.revenue)) OVER (), 2) as revenue_share_pct
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_id, dp.product_name, dp.category, dp.base_price
ORDER BY total_revenue DESC
LIMIT 20;

-- =====================================================
-- 5. Inventory Turnover Ratio
-- =====================================================
-- Description: How quickly inventory is sold and replaced
-- Business Use: Inventory efficiency, working capital optimization
-- Dependencies: fact_sales, dim_product, (assumes inventory snapshot data)

-- Note: This query assumes we have inventory data. If not available, 
-- we'll calculate based on sales velocity
WITH product_sales AS (
    SELECT 
        dp.product_key,
        dp.product_name,
        dp.category,
        dp.base_price,
        SUM(fs.quantity) as total_sold,
        SUM(fs.revenue) as total_revenue,
        COUNT(DISTINCT fs.date_key) as days_sold
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY dp.product_key, dp.product_name, dp.category, dp.base_price
),
daily_avg_sales AS (
    SELECT 
        *,
        total_sold * 1.0 / days_sold as avg_daily_sales
    FROM product_sales
)
SELECT 
    product_id,
    product_name,
    category,
    total_sold,
    total_revenue,
    avg_daily_sales,
    CASE 
        WHEN avg_daily_sales > 10 THEN 'Fast Moving'
        WHEN avg_daily_sales > 2 THEN 'Medium Moving'
        ELSE 'Slow Moving'
    END as movement_category,
    ROUND(avg_daily_sales * 30, 2) as projected_monthly_sales
FROM daily_avg_sales
ORDER BY avg_daily_sales DESC;

-- =====================================================
-- 6. Average Delivery Times
-- =====================================================
-- Description: Average time from order to delivery
-- Business Use: Supply chain performance, customer satisfaction
-- Dependencies: fact_sales, dim_shipments (assumes shipment data)

-- Note: This is a template assuming shipment data exists
-- Modify based on actual shipment schema
SELECT 
    DATE_TRUNC('month', fs.date) as delivery_month,
    COUNT(fs.sale_id) as total_deliveries,
    AVG(DATEDIFF('day', fs.date, COALESCE(sh.delivery_date, fs.date))) as avg_delivery_days,
    MIN(DATEDIFF('day', fs.date, COALESCE(sh.delivery_date, fs.date))) as min_delivery_days,
    MAX(DATEDIFF('day', fs.date, COALESCE(sh.delivery_date, fs.date))) as max_delivery_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', fs.date, COALESCE(sh.delivery_date, fs.date))) as median_delivery_days
FROM fact_sales fs
-- LEFT JOIN dim_shipments sh ON fs.sale_id = sh.sale_id -- Uncomment when shipment data exists
GROUP BY DATE_TRUNC('month', fs.date)
ORDER BY delivery_month DESC;

-- =====================================================
-- 7. Seasonal Demand Trends
-- =====================================================
-- Description: Sales patterns by season and festive periods
-- Business Use: Seasonal planning, promotional campaigns, inventory management
-- Dependencies: fact_sales, dim_date

WITH seasonal_data AS (
    SELECT 
        dd.year,
        dd.month,
        dd.month_name,
        dd.quarter,
        dd.season,
        CASE 
            WHEN dd.month = 3 AND dd.day BETWEEN 20 AND 30 THEN 'Holi'
            WHEN dd.month = 4 AND dd.day BETWEEN 5 AND 15 THEN 'Eid'
            WHEN dd.month IN (10, 11) THEN 'Diwali'
            WHEN dd.month = 12 THEN 'Christmas'
            ELSE 'Regular'
        END as festive_period,
        SUM(fs.revenue) as daily_revenue,
        SUM(fs.quantity) as daily_quantity,
        COUNT(fs.sale_id) as daily_transactions
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.year, dd.month, dd.month_name, dd.quarter, dd.season, dd.day
)
SELECT 
    season,
    festive_period,
    AVG(daily_revenue) as avg_daily_revenue,
    AVG(daily_quantity) as avg_daily_quantity,
    AVG(daily_transactions) as avg_daily_transactions,
    COUNT(*) as days_in_period,
    ROUND(AVG(daily_revenue) * 100.0 / (SELECT AVG(daily_revenue) FROM seasonal_data WHERE festive_period = 'Regular'), 2) as seasonal_index
FROM seasonal_data
GROUP BY season, festive_period
ORDER BY seasonal_index DESC;

-- =====================================================
-- 8. New vs Returning Customers
-- =====================================================
-- Description: Customer acquisition and retention analysis
-- Business Use: Customer loyalty programs, marketing effectiveness
-- Dependencies: fact_sales, dim_customer

WITH customer_first_purchase AS (
    SELECT 
        dc.customer_key,
        dc.customer_id,
        MIN(fs.date_key) as first_purchase_date
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    GROUP BY dc.customer_key, dc.customer_id
),
daily_customer_analysis AS (
    SELECT 
        fs.date_key,
        dd.date,
        COUNT(DISTINCT fs.customer_key) as total_customers,
        COUNT(DISTINCT CASE WHEN fs.date_key = cfp.first_purchase_date THEN fs.customer_key END) as new_customers,
        COUNT(DISTINCT CASE WHEN fs.date_key > cfp.first_purchase_date THEN fs.customer_key END) as returning_customers,
        SUM(fs.revenue) as total_revenue,
        SUM(CASE WHEN fs.date_key = cfp.first_purchase_date THEN fs.revenue END) as new_customer_revenue,
        SUM(CASE WHEN fs.date_key > cfp.first_purchase_date THEN fs.revenue END) as returning_customer_revenue
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    JOIN customer_first_purchase cfp ON fs.customer_key = cfp.customer_key
    GROUP BY fs.date_key, dd.date
)
SELECT 
    date,
    total_customers,
    new_customers,
    returning_customers,
    ROUND(new_customers * 100.0 / total_customers, 2) as new_customer_pct,
    ROUND(returning_customers * 100.0 / total_customers, 2) as returning_customer_pct,
    total_revenue,
    new_customer_revenue,
    returning_customer_revenue,
    ROUND(new_customer_revenue * 100.0 / total_revenue, 2) as new_customer_revenue_pct,
    ROUND(returning_customer_revenue * 100.0 / total_revenue, 2) as returning_customer_revenue_pct
FROM daily_customer_analysis
ORDER BY date DESC
LIMIT 30;

-- =====================================================
-- 9. Customer Lifetime Value (CLV)
-- =====================================================
-- Description: Predicted revenue value of customers over their lifetime
-- Business Use: Customer segmentation, retention investment decisions
-- Dependencies: fact_sales, dim_customer

WITH customer_metrics AS (
    SELECT 
        dc.customer_key,
        dc.customer_id,
        dc.city,
        dc.age_group,
        COUNT(DISTINCT fs.date_key) as purchase_days,
        COUNT(fs.sale_id) as total_transactions,
        MIN(fs.date_key) as first_purchase,
        MAX(fs.date_key) as last_purchase,
        SUM(fs.revenue) as total_revenue,
        AVG(fs.revenue) as avg_transaction_value,
        SUM(fs.quantity) as total_quantity,
        DATEDIFF('day', MIN(fs.date_key), MAX(fs.date_key)) as customer_lifespan_days
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    GROUP BY dc.customer_key, dc.customer_id, dc.city, dc.age_group
),
customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_transactions = 1 THEN 'One-time'
            WHEN total_transactions <= 5 THEN 'Occasional'
            WHEN total_transactions <= 15 THEN 'Regular'
            ELSE 'Loyal'
        END as purchase_frequency_segment,
        CASE 
            WHEN total_revenue < 1000 THEN 'Low Value'
            WHEN total_revenue < 5000 THEN 'Medium Value'
            WHEN total_revenue < 20000 THEN 'High Value'
            ELSE 'Premium'
        END as value_segment
    FROM customer_metrics
)
SELECT 
    value_segment,
    purchase_frequency_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_segments), 2) as customer_percentage,
    SUM(total_revenue) as segment_revenue,
    ROUND(AVG(total_revenue), 2) as avg_clv,
    ROUND(AVG(avg_transaction_value), 2) as avg_transaction_value,
    ROUND(AVG(customer_lifespan_days), 0) as avg_lifespan_days,
    ROUND(AVG(total_transactions), 1) as avg_transactions
FROM customer_segments
GROUP BY value_segment, purchase_frequency_segment
ORDER BY segment_revenue DESC;

-- =====================================================
-- Additional Utility Queries
-- =====================================================

-- Quick Summary Dashboard
SELECT 
    'Total Revenue' as metric,
    SUM(revenue) as value,
    '₹' || FORMAT(SUM(revenue), '#,##0.00') as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Transactions' as metric,
    COUNT(sale_id) as value,
    FORMAT(COUNT(sale_id), '#,##0') as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Customers' as metric,
    COUNT(DISTINCT customer_key) as value,
    FORMAT(COUNT(DISTINCT customer_key), '#,##0') as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Products' as metric,
    COUNT(DISTINCT product_key) as value,
    FORMAT(COUNT(DISTINCT product_key), '#,##0') as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Stores' as metric,
    COUNT(DISTINCT store_key) as value,
    FORMAT(COUNT(DISTINCT store_key), '#,##0') as formatted_value
FROM fact_sales;

-- Performance Optimization Tips:
-- 1. Use date partitions for time-based queries
-- 2. Add indexes on frequently joined columns
-- 3. Consider materialized views for recurring KPIs
-- 4. Use appropriate data types for aggregations
-- 5. Monitor query execution plans regularly
