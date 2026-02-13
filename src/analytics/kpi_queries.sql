-- =====================================================
-- RetailOS KPI Queries
-- Compatible with DuckDB and actual database schema
-- =====================================================

-- =====================================================
-- 1. Daily Revenue
-- =====================================================
-- Description: Total revenue generated each day with time-based analysis
-- Dependencies: fact_sales, dim_date

SELECT 
    dd.date,
    dd.year,
    dd.month,
    dd.day,
    CASE dd.is_weekend WHEN true THEN 'Weekend' ELSE 'Weekday' END as day_type,
    CASE dd.is_holiday WHEN true THEN 'Holiday' ELSE 'Regular' END as holiday_type,
    dd.festival_name,
    SUM(fs.revenue) as daily_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.date, dd.year, dd.month, dd.day, dd.is_weekend, dd.is_holiday, dd.festival_name
ORDER BY dd.date DESC;

-- =====================================================
-- 2. Monthly Revenue
-- =====================================================
-- Description: Total revenue generated each month with growth analysis
-- Dependencies: fact_sales, dim_date

SELECT 
    dd.year,
    dd.month,
    CASE dd.month 
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    SUM(fs.revenue) as monthly_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold,
    LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month) as prev_month_revenue,
    ROUND(
        (SUM(fs.revenue) - LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month)) * 100.0 / 
        NULLIF(LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month), 0), 
        2
    ) as revenue_growth_pct
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- =====================================================
-- 3. City-wise Sales
-- =====================================================
-- Description: Sales performance by city with regional analysis
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
-- 4. Top 10 Selling Products (by revenue)
-- =====================================================
-- Description: Best performing products by revenue with ranking
-- Dependencies: fact_sales, dim_product

SELECT 
    dp.product_id,
    dp.name as product_name,
    dp.category,
    dp.brand,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.revenue) as total_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_revenue_per_transaction,
    AVG(dp.price) as avg_product_price,
    ROUND(SUM(fs.revenue) * 100.0 / SUM(SUM(fs.revenue)) OVER (), 2) as revenue_share_pct,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.revenue) DESC) as revenue_rank
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_id, dp.name, dp.category, dp.brand, dp.price
ORDER BY total_revenue DESC
LIMIT 10;

-- =====================================================
-- 5. Inventory Turnover Ratio
-- =====================================================
-- Description: Product sales velocity and turnover analysis
-- Dependencies: fact_sales, dim_product

SELECT 
    dp.product_id,
    dp.name as product_name,
    dp.category,
    dp.price,
    SUM(fs.quantity) as total_sold,
    SUM(fs.revenue) as total_revenue,
    COUNT(DISTINCT fs.date_key) as days_sold,
    MIN(fs.date_key) as first_sale_date,
    MAX(fs.date_key) as last_sale_date,
    SUM(fs.quantity) * 1.0 / NULLIF(COUNT(DISTINCT fs.date_key), 0) as avg_daily_sales,
    (MAX(fs.date_key) - MIN(fs.date_key)) as sales_span_days,
    CASE 
        WHEN SUM(fs.quantity) * 1.0 / NULLIF(COUNT(DISTINCT fs.date_key), 0) > 10 THEN 'Fast Moving'
        WHEN SUM(fs.quantity) * 1.0 / NULLIF(COUNT(DISTINCT fs.date_key), 0) > 2 THEN 'Medium Moving'
        ELSE 'Slow Moving'
    END as movement_category,
    ROUND((SUM(fs.quantity) * 1.0 / NULLIF(COUNT(DISTINCT fs.date_key), 0)) * 30, 2) as projected_monthly_sales,
    ROUND((SUM(fs.quantity) * 1.0 / NULLIF(COUNT(DISTINCT fs.date_key), 0)) * 365, 2) as projected_annual_sales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_id, dp.name, dp.category, dp.price
ORDER BY avg_daily_sales DESC;

-- =====================================================
-- 6. Average Delivery Time
-- =====================================================
-- Description: Delivery performance metrics (placeholder implementation)
-- Dependencies: fact_sales, dim_date

SELECT 
    DATE_TRUNC('month', dd.date) as delivery_month,
    COUNT(fs.sale_id) as total_deliveries,
    AVG(1) as avg_delivery_days,
    MIN(1) as min_delivery_days,
    MAX(1) as max_delivery_days,
    1 as median_delivery_days
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY DATE_TRUNC('month', dd.date)
ORDER BY delivery_month DESC;

-- =====================================================
-- 7. Seasonal Demand Trends (month-wise)
-- =====================================================
-- Description: Monthly sales patterns with seasonal analysis
-- Dependencies: fact_sales, dim_date

SELECT 
    dd.year,
    dd.month,
    CASE dd.month 
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    CASE 
        WHEN dd.month IN (12, 1, 2) THEN 'Winter'
        WHEN dd.month IN (3, 4, 5) THEN 'Spring'
        WHEN dd.month IN (6, 7, 8) THEN 'Summer'
        ELSE 'Monsoon'
    END as season,
    SUM(fs.revenue) as monthly_revenue,
    COUNT(fs.sale_id) as transaction_count,
    AVG(fs.revenue) as avg_transaction_value,
    SUM(fs.quantity) as total_units_sold,
    ROUND(SUM(fs.revenue) * 100.0 / SUM(SUM(fs.revenue)) OVER (), 2) as revenue_share_pct,
    LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month) as prev_month_revenue,
    ROUND(
        (SUM(fs.revenue) - LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month)) * 100.0 / 
        NULLIF(LAG(SUM(fs.revenue)) OVER (ORDER BY dd.year, dd.month), 0), 
        2
    ) as month_over_month_growth_pct
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- =====================================================
-- 8. New vs Returning Customers
-- =====================================================
-- Description: Customer acquisition and retention analysis
-- Dependencies: fact_sales, dim_customer, dim_date

SELECT 
    dd.date,
    COUNT(DISTINCT fs.customer_key) as total_customers,
    COUNT(DISTINCT CASE WHEN fs.date_key = (
        SELECT MIN(fs2.date_key) 
        FROM fact_sales fs2 
        WHERE fs2.customer_key = fs.customer_key
    ) THEN fs.customer_key END) as new_customers,
    COUNT(DISTINCT CASE WHEN fs.date_key > (
        SELECT MIN(fs2.date_key) 
        FROM fact_sales fs2 
        WHERE fs2.customer_key = fs.customer_key
    ) THEN fs.customer_key END) as returning_customers,
    SUM(fs.revenue) as total_revenue,
    ROUND(COUNT(DISTINCT CASE WHEN fs.date_key = (
        SELECT MIN(fs2.date_key) 
        FROM fact_sales fs2 
        WHERE fs2.customer_key = fs.customer_key
    ) THEN fs.customer_key END) * 100.0 / NULLIF(COUNT(DISTINCT fs.customer_key), 0), 2) as new_customer_pct,
    ROUND(COUNT(DISTINCT CASE WHEN fs.date_key > (
        SELECT MIN(fs2.date_key) 
        FROM fact_sales fs2 
        WHERE fs2.customer_key = fs.customer_key
    ) THEN fs.customer_key END) * 100.0 / NULLIF(COUNT(DISTINCT fs.customer_key), 0), 2) as returning_customer_pct
FROM fact_sales fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.date
ORDER BY dd.date DESC;

-- =====================================================
-- 9. Customer Lifetime Value (CLV)
-- =====================================================
-- Description: Customer value analysis with segmentation
-- Dependencies: fact_sales, dim_customer

SELECT 
    dc.city,
    CASE 
        WHEN dc.city IN ('Mumbai', 'Delhi', 'Bangalore') THEN 'Metro'
        WHEN dc.city IN ('Pune', 'Hyderabad', 'Chennai') THEN 'Tier-1'
        ELSE 'Tier-2'
    END as city_tier,
    COUNT(DISTINCT fs.customer_key) as customer_count,
    SUM(fs.revenue) as total_revenue,
    AVG(fs.revenue) as avg_clv,
    COUNT(fs.sale_id) as total_transactions,
    AVG(fs.revenue) as avg_transaction_value,
    (MAX(fs.date_key) - MIN(fs.date_key)) as customer_lifespan_days,
    CASE 
        WHEN COUNT(fs.sale_id) = 1 THEN 'One-time'
        WHEN COUNT(fs.sale_id) <= 5 THEN 'Occasional'
        WHEN COUNT(fs.sale_id) <= 15 THEN 'Regular'
        ELSE 'Loyal'
    END as purchase_frequency_segment,
    CASE 
        WHEN SUM(fs.revenue) < 1000 THEN 'Low Value'
        WHEN SUM(fs.revenue) < 5000 THEN 'Medium Value'
        WHEN SUM(fs.revenue) < 20000 THEN 'High Value'
        ELSE 'Premium'
    END as value_segment
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dc.city
ORDER BY total_revenue DESC;

-- =====================================================
-- Additional Utility Queries
-- =====================================================

-- Quick Summary Dashboard
SELECT 
    'Total Revenue' as metric,
    SUM(revenue) as value,
    '₹' || ROUND(SUM(revenue), 2) as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Transactions' as metric,
    COUNT(sale_id) as value,
    CAST(COUNT(sale_id) AS VARCHAR) as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Customers' as metric,
    COUNT(DISTINCT customer_key) as value,
    CAST(COUNT(DISTINCT customer_key) AS VARCHAR) as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Products' as metric,
    COUNT(DISTINCT product_key) as value,
    CAST(COUNT(DISTINCT product_key) AS VARCHAR) as formatted_value
FROM fact_sales
UNION ALL
SELECT 
    'Total Stores' as metric,
    COUNT(DISTINCT store_key) as value,
    CAST(COUNT(DISTINCT store_key) AS VARCHAR) as formatted_value
FROM fact_sales;
