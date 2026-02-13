# AI/ML Usage in RetailOS

## 🤖 Current AI/ML Implementation

Your RetailOS platform has **extensive AI/ML capabilities** already implemented in the backend. Here's where AI is being used:

---

## 1. ML Predictive Engine
**Location:** [src/intelligence/ml_predictive_engine.py](file:///c:/Users/ramki/retail-os/src/intelligence/ml_predictive_engine.py)

This is your **main AI engine** with three ML models:

### A. Prophet Time Series Forecasting
- **Purpose**: Predict future demand for each product-store combination
- **Technology**: Facebook Prophet (handles seasonality, trends, holidays)
- **Features**:
  - Automatic Indian festival/holiday detection
  - Weekly and yearly seasonality
  - Custom external events integration
  - 7-day demand forecasting with confidence intervals
- **Training**: One model per product-store combo (top 50 combinations)
- **Output**: Forecasted demand with upper/lower bounds

### B. Random Forest Stockout Classifier
- **Purpose**: Classify stockout risk level (Safe/Moderate/High/Critical)
- **Technology**: scikit-learn RandomForestClassifier
- **Features Used**:
  - Current stock level
  - 7-day average sales
  - Demand volatility (coefficient of variation)
  - Days to next festival
  - Product category
- **Output**: Risk level (0-3) with confidence score

### C. Gradient Boosting Reorder Regressor
- **Purpose**: Predict optimal reorder quantity
- **Technology**: scikit-learn GradientBoostingRegressor
- **Features Used**:
  - 14-day average sales
  - Sales standard deviation
  - Current stock level
- **Output**: Recommended reorder quantity

---

## 2. ML Reasoning & Explainability

The system provides **full transparency** into AI decisions:

### ML Reasoning Log Table
Stores every prediction with complete explanation:
- Current stock vs predicted demand
- Demand volatility metrics
- Prophet forecast details (7-day, upper/lower bounds)
- Days remaining until stockout
- Risk level and ML confidence score
- Recommended reorder quantity
- Full JSON explanation

### Streamlit Dashboard Integration
**Location:** [src/app_enhanced.py](file:///c:/Users/ramki/retail-os/src/app_enhanced.py)

Shows ML predictions in real-time:
- **Tab 1**: Live stockout risk alerts with ML confidence
- **Tab 2**: ML Reasoning Explorer - drill into any prediction to see:
  - Feature importance visualization
  - Prophet forecast details
  - Confidence scores
  - Recommended actions

---

## 3. Frontend Integration (Placeholder)

### Current Status
The Next.js frontend has **placeholder endpoints** for AI features:

**File:** [frontend/src/services/api.ts](file:///c:/Users/ramki/retail-os/frontend/src/services/api.ts)
```typescript
getAIDecisions()  // Returns empty array currently
```

**File:** [frontend/src/components/dashboard/AIDecisionFeed.tsx](file:///c:/Users/ramki/retail-os/frontend/src/components/dashboard/AIDecisionFeed.tsx)
- Beautiful UI component ready to display AI decisions
- Shows decision type, confidence, status, impact
- Currently displays "No AI decisions available"

### Backend Endpoint
**File:** [src/api/server.py](file:///c:/Users/ramki/retail-os/src/api/server.py)
```python
@app.get("/api/kpi/ai-decisions")
def ai_decisions():
    return get_ai_decisions()  # Currently returns []
```

---

## 4. What AI Features Are Active

✅ **Fully Implemented & Working:**
1. Prophet demand forecasting (per product-store)
2. Random Forest stockout risk classification
3. Gradient Boosting reorder quantity prediction
4. ML reasoning logging with full explainability
5. Streamlit dashboard showing ML predictions
6. Real-time stockout alerts with ML confidence

❌ **Placeholder (Not Yet Connected to Frontend):**
1. AI Decision Feed in Next.js dashboard
2. Product pair recommendations (market basket analysis)

---

## 5. How to Use the AI Features

### Train the Models
```bash
cd src/intelligence
python ml_predictive_engine.py
```

This will:
- Train the stockout classifier
- Train the reorder regressor
- Train Prophet models for top product-store combinations
- Save models to `models/` directory

### Generate Predictions
```python
from src.intelligence.ml_predictive_engine import MLPredictiveEngine

engine = MLPredictiveEngine()
result = engine.predict_stockout_with_explanation(
    product_id=123,
    store_id=456
)

# Returns:
# {
#     'risk_level': 2,  # High risk
#     'confidence': 0.87,
#     'recommended_reorder': 150,
#     'explanation': {...}  # Full details
# }
```

### View in Streamlit Dashboard
```bash
cd src
streamlit run app_enhanced.py
```

Navigate to:
- **Tab 1**: See ML-predicted stockout risks
- **Tab 2**: Explore ML reasoning for any prediction

---

## 6. To Connect AI to Next.js Frontend

You would need to update `get_ai_decisions()` in [src/analytics/kpi.py](file:///c:/Users/ramki/retail-os/src/analytics/kpi.py):

```python
def get_ai_decisions():
    """Get recent AI decisions from ML reasoning log"""
    df = con.execute("""
        SELECT 
            timestamp,
            'Stockout Risk' as decision_type,
            CONCAT('Product ', product_id, ' at Store ', store_id) as entity,
            CONCAT('Reorder ', optimal_reorder_qty, ' units') as action,
            ml_confidence as confidence,
            CONCAT('Risk Level: ', risk_level) as impact,
            CASE 
                WHEN risk_level >= 2 THEN 'pending'
                ELSE 'executed'
            END as status
        FROM ml_reasoning_log
        ORDER BY timestamp DESC
        LIMIT 20
    """).fetchdf()
    
    return df.to_dict(orient="records")
```

Then the AI Decision Feed component would automatically display the predictions!

---

## Summary

**You have a sophisticated ML system already built!** It includes:
- 3 trained ML models (Prophet, Random Forest, Gradient Boosting)
- Full explainability and reasoning logs
- Real-time predictions in Streamlit
- Frontend components ready (just need data connection)

The AI is primarily focused on **inventory optimization** and **stockout prevention** using advanced time series forecasting and classification techniques.
