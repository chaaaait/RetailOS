# Data Ingestion Architecture in RetailOS

## 🔄 Overview

RetailOS has a **sophisticated multi-layered data ingestion system** that handles both batch and real-time data with adaptive schema management. Here's how data continuously flows into your system:

---

## 1. Batch Ingestion Pipeline

**File:** [src/ingestion/batch_pipeline.py](file:///c:/Users/ramki/retail-os/src/ingestion/batch_pipeline.py)

### What It Does
Processes CSV files from `data/raw/` directory in batches with intelligent validation.

### Key Features
- ✅ **Schema validation** against a registry
- ✅ **Auto-retry** on read failures (exponential backoff)
- ✅ **Quarantine system** for invalid records
- ✅ **Schema drift detection** (new columns logged, not rejected)
- ✅ **Parquet output** with timestamps

### Data Flow
```
CSV Files (data/raw/)
    ↓
Read with retries
    ↓
Validate against schema
    ↓
Split: Valid vs Invalid
    ↓
Valid → Parquet (data/raw/*.parquet)
Invalid → Quarantine (data/quarantine/*.csv)
```

### Tables Processed
- `customers.csv`
- `products.csv`
- `stores.csv`
- `inventory.csv`
- `transactions.csv`
- `shipments.csv`
- `web_clickstream.csv`

### Run Manually
```bash
cd src/ingestion
python batch_pipeline.py
```

---

## 2. Automated Batch Scheduler

**File:** [src/ingestion/batch_scheduler.py](file:///c:/Users/ramki/retail-os/src/ingestion/batch_scheduler.py)

### What It Does
**Automated scheduling** using APScheduler to run pipelines continuously without manual intervention.

### Scheduled Jobs

| Job | Frequency | Description |
|-----|-----------|-------------|
| **Batch Ingestion** | Every 6 hours | Runs full batch pipeline (0:00, 6:00, 12:00, 18:00) |
| **ML Retraining** | Daily at 2 AM | Retrains all ML models with latest data |
| **Data Quality Checks** | Every 30 minutes | Monitors quarantine rates and anomalies |
| **Log Cleanup** | Weekly (Sunday 3 AM) | Removes old pipeline logs (>90 days) |

### Pipeline Stages
1. **Stage 1**: Ingest data with adaptive schema
2. **Stage 2**: Clean data (remove duplicates, fix nulls)
3. **Stage 3**: Partition storage for optimization

### Monitoring Tables
- `pipeline_runs` - Tracks every pipeline execution
- `pipeline_metrics` - Stores quality metrics per run

### Run Scheduler
```bash
cd src/ingestion
python batch_scheduler.py
```

For testing (single run):
```python
scheduler = BatchPipelineScheduler()
scheduler.start(test_mode=True)
```

---

## 3. Real-Time WebSocket Streaming

**File:** [src/ingestion/websocket_streaming.py](file:///c:/Users/ramki/retail-os/src/ingestion/websocket_streaming.py)

### What It Does
**Generates and streams live orders** in real-time via WebSocket connections.

### Features
- 🔴 **Live order generation** (1-5 second intervals)
- 🔴 **WebSocket broadcasting** to all connected clients
- 🔴 **Instant database writes** to `streaming_orders` table
- 🔴 **Real-time stats** (revenue, orders, customers)

### Order Data Structure
```python
{
    'order_id': 12345,
    'timestamp': '2026-02-13T17:28:07',
    'product_id': 234,
    'customer_id': 5678,
    'store_id': 12,
    'quantity': 3,
    'price': 2499.99,
    'payment_method': 'UPI',
    'order_source': 'App'
}
```

### WebSocket Server
- **Host**: `localhost`
- **Port**: `8765`
- **Protocol**: `ws://localhost:8765`

### Run Streaming Server
```bash
cd src/ingestion
python websocket_streaming.py
```

### Client Connection
```javascript
const ws = new WebSocket('ws://localhost:8765');
ws.onmessage = (event) => {
    const order = JSON.parse(event.data);
    console.log('New order:', order);
};
```

---

## 4. Adaptive Schema Management

**File:** [src/ingestion/adaptive_schema_manager.py](file:///c:/Users/ramki/retail-os/src/ingestion/adaptive_schema_manager.py)

### What It Does
**Intelligently handles schema evolution** without breaking the pipeline.

### Multi-Stage Detection

#### Stage 1: Column Detection
- Detects new columns
- Detects missing required columns
- Detects type changes

#### Stage 2: Confidence Scoring
Calculates confidence (0.0-1.0) based on:
- **Naming convention** (contains `id`, `name`, `price`, etc.)
- **Completeness** (low null percentage)
- **Type consistency** (can be cast to expected type)
- **Value distribution** (reasonable uniqueness)

#### Stage 3: Noise Reduction Strategy

| Scenario | Action | Threshold |
|----------|--------|-----------|
| Single high-confidence change | Auto-approve | ≥75% confidence |
| Multiple high-confidence changes (≤3) | Auto-approve | All ≥75% |
| Mixed confidence batch | Batch approval required | Some <75% |
| Mass low-confidence changes (>5) | Quarantine all | Likely corrupt |

### Schema Change Tracking
- `schema_change_log` - All detected changes with confidence scores
- `schema_approval_queue` - Changes awaiting admin review

### Example Workflow
```
New column "customer_email" detected
    ↓
Confidence calculated: 0.85 (high)
    ↓
Auto-approved and added to schema
    ↓
Logged in schema_change_log
    ↓
Data processed normally
```

---

## 5. How Data Keeps Flowing

### Continuous Ingestion Methods

#### Method 1: Automated Scheduler (Recommended)
```bash
# Start the scheduler - runs forever
python src/ingestion/batch_scheduler.py
```
- Runs batch ingestion every 6 hours
- Retrains ML models daily
- Monitors data quality every 30 minutes

#### Method 2: Real-Time Streaming
```bash
# Start WebSocket server - generates orders continuously
python src/ingestion/websocket_streaming.py
```
- Generates orders every 1-5 seconds
- Writes directly to `streaming_orders` table
- Broadcasts to connected dashboards

#### Method 3: Manual Batch Runs
```bash
# Run once manually
python src/ingestion/batch_pipeline.py
```

---

## 6. Data Quality & Monitoring

### Quarantine System
Invalid records are **not discarded** but quarantined with reasons:
- `data/quarantine/{table}_quarantine_{timestamp}.csv`
- Includes `quarantine_reason` column explaining the issue

### Common Quarantine Reasons
- `missing_required_column:customer_id`
- `missing_required_value:price`
- `type_mismatch:expected_int_got_string`

### Pipeline Monitoring
View in Streamlit dashboard:
```bash
streamlit run src/app_enhanced.py
```

Navigate to **Tab 4: Pipeline Monitoring** to see:
- Success rate over time
- Rows processed per run
- Duration trends
- Quarantine rates

---

## 7. Integration with Transformation Layer

After ingestion, data flows to transformation:

```
Batch Ingestion
    ↓
Data Cleaning (src/transformation/data_cleaning.py)
    ↓
Partitioning (src/storage/partitioning.py)
    ↓
DuckDB Warehouse (data/warehouse/retail.duckdb)
    ↓
Analytics & ML
```

---

## 8. Production Deployment

### Recommended Setup

1. **Start Scheduler** (background process)
   ```bash
   nohup python src/ingestion/batch_scheduler.py &
   ```

2. **Start WebSocket Server** (optional, for real-time)
   ```bash
   nohup python src/ingestion/websocket_streaming.py &
   ```

3. **Monitor Logs**
   ```bash
   tail -f logs/batch_pipeline.log
   ```

### Environment Variables
Set in `.env` or `config.py`:
- `DB_PATH` - Path to DuckDB warehouse
- `MODELS_DIR` - Path to ML models directory

---

## Summary

**Your data ingestion is highly automated:**

✅ **Batch ingestion** runs every 6 hours automatically  
✅ **Real-time streaming** generates orders continuously  
✅ **Adaptive schema** handles new columns intelligently  
✅ **Quality monitoring** quarantines bad data with reasons  
✅ **ML retraining** happens daily with fresh data  
✅ **Full observability** via Streamlit dashboard  

The system is **production-ready** and designed to run 24/7 with minimal manual intervention!
