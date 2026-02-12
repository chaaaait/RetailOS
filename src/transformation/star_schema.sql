-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    festival_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT,
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    city VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    version INTEGER,
    PRIMARY KEY (customer_id, version)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER,
    product_id VARCHAR,
    name VARCHAR,
    category VARCHAR,
    brand VARCHAR,
    price DOUBLE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key BIGINT,
    store_id VARCHAR,
    store_name VARCHAR,
    city VARCHAR,
    region VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_external_events (
    event_id BIGINT,
    event_name VARCHAR,
    event_date DATE,
    region VARCHAR,
    demand_impact DOUBLE
);

-- =========================
-- FACT TABLES
-- =========================

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id VARCHAR,
    date_key INTEGER,
    customer_key BIGINT,
    product_key BIGINT,
    store_key BIGINT,
    quantity INTEGER,
    revenue DOUBLE,
    discount DOUBLE
);

CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_id BIGINT,
    date_key INTEGER,
    product_key BIGINT,
    store_key BIGINT,
    stock_level INTEGER,
    reorder_point INTEGER
);

CREATE TABLE IF NOT EXISTS fact_shipments (
    shipment_id VARCHAR,
    date_key INTEGER,
    product_key BIGINT,
    store_key BIGINT,
    delivery_time DOUBLE,
    on_time_flag BOOLEAN
);


-- Enforce one row per (customer_id, version)
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_unique
ON dim_customer(customer_id, version);


