// TypeScript types for KPI API responses

export interface DailyRevenue {
    date: string;
    revenue: number;
}

export interface CitySales {
    city: string;
    region: string;
    active_stores: number;
    total_revenue: number;
    transaction_count: number;
    avg_transaction_value: number;
    total_units_sold: number;
    revenue_share_pct: number;
}

export interface StockoutRisk {
    product_id: string;
    product_name: string;
    category: string;
    price: number;
    total_sold: number;
    total_revenue: number;
    days_sold: number;
    first_sale_date: number;
    last_sale_date: number;
    avg_daily_sales: number;
    sales_span_days: number;
    movement_category: 'Fast Moving' | 'Medium Moving' | 'Slow Moving';
    projected_monthly_sales: number;
    projected_annual_sales: number;
}

export interface CustomerDistribution {
    city: string;
    city_tier: 'Metro' | 'Tier-1' | 'Tier-2';
    customer_count: number;
    total_revenue: number;
    avg_clv: number;
    total_transactions: number;
    avg_transaction_value: number;
    customer_lifespan_days: number;
    purchase_frequency_segment: 'One-time' | 'Occasional' | 'Regular' | 'Loyal';
    value_segment: 'Low Value' | 'Medium Value' | 'High Value' | 'Premium';
}

export interface ProductPair {
    product_a: string;
    product_b: string;
    co_occurrence_count: number;
    confidence: number;
    lift: number;
}

export interface AIDecision {
    decision_id: string;
    timestamp: string;
    decision_type: string;
    entity: string;
    action: string;
    confidence: number;
    impact: string;
    status: 'pending' | 'approved' | 'rejected' | 'executed';
}
