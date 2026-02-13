import pandas as pd
import numpy as np
import duckdb
import pickle
import json
import os
import sys
from pathlib import Path
from prophet import Prophet
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config import DB_PATH, MODELS_DIR
except ImportError:
    # Fallback if config not found (e.g. running standalone)
    DB_PATH = 'data/warehouse/retail.duckdb'
    MODELS_DIR = Path('models')
    MODELS_DIR.mkdir(exist_ok=True)

class MLPredictiveEngine:
    def __init__(self):
        self.prophet_models = {}  # One per product-store combo
        self.stockout_classifier = None
        self.demand_regressor = None
        
        # Ensure models directory exists
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Initialize database connection
        try:
            self.con = duckdb.connect(str(DB_PATH))
            print(f"Connected to database at {DB_PATH}")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

        # Verify required tables exist
        required_tables = ['fact_sales', 'fact_inventory', 'dim_product', 
                           'dim_store', 'dim_date', 'dim_external_events']
        
        for table in required_tables:
            try:
                self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            except Exception:
                print(f"Warning: Required table {table} not found in database or is empty.")

        # Create ML reasoning log table if not exists
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS ml_reasoning_log (
            timestamp TIMESTAMP,
            store_id INTEGER,
            product_id INTEGER,
            current_stock FLOAT,
            avg_sales_7d FLOAT,
            demand_volatility_cv FLOAT,
            prophet_7d_forecast FLOAT,
            days_remaining_forecast FLOAT,
            risk_level INTEGER,
            ml_confidence FLOAT,
            optimal_reorder_qty INTEGER,
            prophet_upper_bound FLOAT,
            prophet_lower_bound FLOAT,
            explanation TEXT
        )
        """)
        
    def train_demand_forecaster(self, product_id, store_id):
        """
        Train Prophet model for specific product-store combination
        Prophet handles seasonality, holidays, trends automatically
        """
        # Get historical sales data
        query = f"""
        SELECT 
            date as ds,
            SUM(quantity) as y
        FROM fact_sales
        WHERE product_key = (SELECT product_key FROM dim_product WHERE product_id = {product_id})
        AND store_key = (SELECT store_key FROM dim_store WHERE store_id = {store_id})
        GROUP BY date
        ORDER BY date
        """
        try:
            df = self.con.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching data for P{product_id} S{store_id}: {e}")
            return None
        
        # Check minimum data requirement
        if len(df) < 30:
            print(f"Warning: Only {len(df)} days of data for product {product_id}, store {store_id}. Need at least 30.")
            return None

        # Add external events as holidays to Prophet
        try:
            holidays = self.con.execute("""
            SELECT 
                event_date as ds,
                event_name as holiday,
                CAST(demand_impact * 100 AS INTEGER) as prior_scale
            FROM dim_external_events
            """).fetchdf()
        except Exception:
            holidays = pd.DataFrame()
        
        # Initialize Prophet with custom seasonality
        if holidays.empty:
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,  # Flexibility for trend changes
                seasonality_prior_scale=10.0   # Strong seasonal patterns
            )
        else:
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                holidays=holidays,
                changepoint_prior_scale=0.05,
                seasonality_prior_scale=10.0
            )
        
        # Add Indian festival seasonality
        model.add_country_holidays(country_name='IN')
        
        # Fit model
        try:
            model.fit(df)
        except Exception as e:
            print(f"Error fitting Prophet model: {e}")
            return None
        
        # Save model
        model_key = f"{product_id}_{store_id}"
        self.prophet_models[model_key] = model
        
        # Persist to disk
        try:
            with open(MODELS_DIR / f'prophet_{model_key}.pkl', 'wb') as f:
                pickle.dump(model, f)
        except Exception as e:
            print(f"Error saving Prophet model: {e}")
            
        return model
    
    def predict_demand(self, product_id, store_id, days_ahead=7):
        """
        Predict demand for next N days using Prophet
        Returns: forecasted quantities with confidence intervals
        """
        model_key = f"{product_id}_{store_id}"
        
        # Load or train model
        if model_key not in self.prophet_models:
            try:
                with open(MODELS_DIR / f'prophet_{model_key}.pkl', 'rb') as f:
                    self.prophet_models[model_key] = pickle.load(f)
            except FileNotFoundError:
                print(f"Model not found for {model_key}, training new one...")
                model = self.train_demand_forecaster(product_id, store_id)
                if model is None:
                    return None
        
        model = self.prophet_models.get(model_key)
        if not model:
            return None
        
        # Create future dataframe
        future = model.make_future_dataframe(periods=days_ahead)
        forecast = model.predict(future)
        
        # Get only future predictions
        forecast_future = forecast.tail(days_ahead)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        
        return forecast_future
    
    def train_stockout_classifier(self):
        """
        Train Random Forest to classify stockout risk level
        Features: current_stock, avg_sales, variance, days_to_festival, category
        Target: risk_level (0=safe, 1=moderate, 2=high, 3=critical)
        """
        # Build training dataset from historical data
        query = """
        WITH sales_stats AS (
            SELECT 
                fs.store_key,
                fs.product_key,
                fs.date_key,
                AVG(fs.quantity) OVER (
                    PARTITION BY fs.store_key, fs.product_key 
                    ORDER BY fs.date_key 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as avg_sales_7d,
                STDDEV(fs.quantity) OVER (
                    PARTITION BY fs.store_key, fs.product_key 
                    ORDER BY fs.date_key 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as stddev_sales_7d,
                fi.stock_level as current_stock,
                dp.category,
                dd.date,
                COALESCE(MIN(DATEDIFF('day', dd.date, de.event_date)), 999) as days_to_next_festival
            FROM fact_sales fs
            JOIN fact_inventory fi ON fs.product_key = fi.product_key 
                AND fs.store_key = fi.store_key 
                AND fs.date_key = fi.date_key
            JOIN dim_product dp ON fs.product_key = dp.product_key
            JOIN dim_date dd ON fs.date_key = dd.date_key
            LEFT JOIN dim_external_events de ON de.event_date > dd.date
            GROUP BY fs.store_key, fs.product_key, fs.date_key, 
                     fi.stock_level, dp.category, dd.date
        ),
        labeled_data AS (
            SELECT 
                *,
                CASE 
                    WHEN avg_sales_7d > 0 THEN current_stock / avg_sales_7d
                    ELSE 999
                END as days_remaining,
                CASE 
                    WHEN avg_sales_7d > 0 AND (current_stock / avg_sales_7d) < 1 THEN 3  -- Critical
                    WHEN avg_sales_7d > 0 AND (current_stock / avg_sales_7d) < 2 THEN 2  -- High
                    WHEN avg_sales_7d > 0 AND (current_stock / avg_sales_7d) < 5 THEN 1  -- Moderate
                    ELSE 0  -- Safe
                END as risk_level
            FROM sales_stats
        )
        SELECT * FROM labeled_data WHERE avg_sales_7d > 0
        """
        
        try:
            df = self.con.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching training data for classifier: {e}")
            return 0, pd.DataFrame()
            
        if df.empty:
            print("No training data available for Stockout Classifier.")
            return 0, pd.DataFrame()
        
        # Feature engineering
        df['cv'] = df['stddev_sales_7d'] / df['avg_sales_7d']  # Coefficient of variation
        df['festival_boost'] = np.where(df['days_to_next_festival'] < 7, 1, 0)
        df['category_encoded'] = pd.Categorical(df['category']).codes
        
        features = ['current_stock', 'avg_sales_7d', 'stddev_sales_7d', 'cv', 
                   'days_to_next_festival', 'festival_boost', 'category_encoded']
        X = df[features].fillna(0)
        y = df['risk_level']
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train Random Forest
        self.stockout_classifier = RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            min_samples_split=50,
            random_state=42,
            class_weight='balanced'  # Handle imbalanced classes
        )
        self.stockout_classifier.fit(X_train, y_train)
        
        # Evaluate
        accuracy = self.stockout_classifier.score(X_test, y_test)
        print(f"Stockout Classifier Accuracy: {accuracy:.2%}")
        
        # Feature importance
        importance = pd.DataFrame({
            'feature': features,
            'importance': self.stockout_classifier.feature_importances_
        }).sort_values('importance', ascending=False)
        print("\nFeature Importance:")
        print(importance)
        
        # Save model
        with open(MODELS_DIR / 'stockout_classifier.pkl', 'wb') as f:
            pickle.dump(self.stockout_classifier, f)
            
        return accuracy, importance
    
    def train_reorder_amount_regressor(self):
        """
        Train Gradient Boosting to predict optimal reorder quantity
        Considers: historical demand, upcoming events, inter-branch transfers
        """
        query = """
        WITH reorder_history AS (
            SELECT 
                product_key,
                store_key,
                date_key,
                AVG(quantity) OVER (
                    PARTITION BY product_key, store_key 
                    ORDER BY date_key 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as avg_sales_14d,
                STDDEV(quantity) OVER (
                    PARTITION BY product_key, store_key 
                    ORDER BY date_key 
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) as stddev_sales_14d,
                stock_level,
                -- Target: how much was actually reordered (inferred from stock jumps)
                GREATEST(0, stock_level - LAG(stock_level) OVER (
                    PARTITION BY product_key, store_key ORDER BY date_key
                )) as reorder_quantity
            FROM fact_inventory
        )
        SELECT * FROM reorder_history WHERE reorder_quantity > 0
        """
        
        try:
            df = self.con.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching data for regressor: {e}")
            return 0
            
        if df.empty:
            print("No training data available for Reorder Regressor.")
            return 0
        
        features = ['avg_sales_14d', 'stddev_sales_14d', 'stock_level']
        X = df[features].fillna(0)
        y = df['reorder_quantity']
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        self.demand_regressor = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
        self.demand_regressor.fit(X_train, y_train)
        
        # Evaluate
        score = self.demand_regressor.score(X_test, y_test)
        print(f"Reorder Regressor R² Score: {score:.2f}")
        
        # Save model
        with open(MODELS_DIR / 'reorder_regressor.pkl', 'wb') as f:
            pickle.dump(self.demand_regressor, f)
            
        return score
    
    def predict_stockout_with_explanation(self, product_id, store_id):
        """
        Main prediction function with full ML reasoning
        Returns: risk level, predicted demand, recommended action, confidence, explanation
        """
        # Get current inventory
        try:
            current_stock = self.con.execute(f"""
            SELECT stock_level 
            FROM fact_inventory 
            WHERE product_key = (SELECT product_key FROM dim_product WHERE product_id = {product_id})
            AND store_key = (SELECT store_key FROM dim_store WHERE store_id = {store_id})
            AND date_key = (SELECT MAX(date_key) FROM fact_inventory)
            """).fetchone()
            
            if current_stock is None:
                print(f"No inventory data for Product {product_id} at Store {store_id}")
                return None
            current_stock = current_stock[0]
            
            # Get sales statistics
            stats_df = self.con.execute(f"""
            SELECT 
                AVG(quantity) as avg_sales_7d,
                STDDEV(quantity) as stddev_sales_7d,
                category,
                COALESCE(MIN(DATEDIFF('day', CURRENT_DATE, event_date)), 999) as days_to_festival
            FROM fact_sales fs
            JOIN dim_product dp ON fs.product_key = dp.product_key
            LEFT JOIN dim_external_events de ON de.event_date > CURRENT_DATE
            WHERE dp.product_id = {product_id}
            AND fs.store_key = (SELECT store_key FROM dim_store WHERE store_id = {store_id})
            AND fs.date_key >= (SELECT MAX(date_key) - 6 FROM fact_sales)
            GROUP BY category
            """).fetchdf()
            
            if stats_df.empty:
                print(f"No recent stats for Product {product_id} at Store {store_id}")
                return None
            
            stats = stats_df.iloc[0]
            
        except Exception as e:
            print(f"Error fetching real-time data: {e}")
            return None
        
        # Prophet forecast (ML-based demand prediction)
        forecast = self.predict_demand(product_id, store_id, days_ahead=7)
        if forecast is None:
            predicted_demand_7d = stats['avg_sales_7d'] * 7  # Fallback
            forecast_upper = predicted_demand_7d * 1.2
            forecast_lower = predicted_demand_7d * 0.8
        else:
            predicted_demand_7d = forecast['yhat'].sum()
            forecast_upper = forecast['yhat_upper'].sum()
            forecast_lower = forecast['yhat_lower'].sum()
        
        # Prepare features for classifier
        cv = stats['stddev_sales_7d'] / stats['avg_sales_7d'] if stats['avg_sales_7d'] > 0 else 0
        festival_boost = 1 if stats['days_to_festival'] < 7 else 0
        category_encoded = pd.Categorical([stats['category']]).codes[0]
        
        features = np.array([[
            current_stock,
            stats['avg_sales_7d'],
            stats['stddev_sales_7d'],
            cv,
            stats['days_to_festival'],
            festival_boost,
            category_encoded
        ]])
        
        # Load models if needed
        if self.stockout_classifier is None:
            try:
                with open(MODELS_DIR / 'stockout_classifier.pkl', 'rb') as f:
                    self.stockout_classifier = pickle.load(f)
            except:
                print("Stockout classifier not found. Please train models first.")
                return None
                
        if self.demand_regressor is None:
            try:
                with open(MODELS_DIR / 'reorder_regressor.pkl', 'rb') as f:
                    self.demand_regressor = pickle.load(f)
            except:
                print("Reorder regressor not found.")
                return None

        # Predict risk level with ML model
        risk_level = self.stockout_classifier.predict(features)[0]
        risk_proba = self.stockout_classifier.predict_proba(features)[0]
        
        # Check if risk_level is within bounds of risk_proba
        if risk_level < len(risk_proba):
            confidence = risk_proba[risk_level]
        else:
            confidence = 0.0

        # Predict optimal reorder quantity
        reorder_features = np.array([[
            stats['avg_sales_7d'],
            stats['stddev_sales_7d'],
            current_stock
        ]])
        optimal_reorder = int(self.demand_regressor.predict(reorder_features)[0])
        
        # Calculate days remaining using Prophet forecast
        days_remaining = current_stock / (predicted_demand_7d / 7) if predicted_demand_7d > 0 else 999
        
        # Build explanation
        explanation = {
            'current_stock': int(current_stock),
            'avg_daily_demand': round(stats['avg_sales_7d'], 2),
            'demand_volatility_cv': round(cv, 2),
            'prophet_7d_forecast': round(predicted_demand_7d, 2),
            'days_remaining_forecast': round(days_remaining, 2),
            'upcoming_festival': bool(stats['days_to_festival'] < 7),
            'days_to_festival': int(stats['days_to_festival']),
            'risk_level': ['Safe', 'Moderate', 'High', 'Critical'][int(risk_level)],
            'ml_confidence': round(confidence * 100, 1),
            'optimal_reorder_qty': optimal_reorder,
            'prophet_upper_bound': round(forecast_upper, 2),
            'prophet_lower_bound': round(forecast_lower, 2)
        }
        
        # Log reasoning to database
        try:
            insert_query = """
            INSERT INTO ml_reasoning_log VALUES (
                CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """
            self.con.execute(insert_query, [
                store_id, product_id, current_stock, stats['avg_sales_7d'], cv,
                predicted_demand_7d, days_remaining, int(risk_level), confidence,
                optimal_reorder, forecast_upper, forecast_lower, json.dumps(explanation)
            ])
        except Exception as e:
            print(f"Error logging reasoning: {e}")
        
        return {
            'risk_level': int(risk_level),
            'confidence': confidence,
            'recommended_reorder': optimal_reorder,
            'explanation': explanation
        }

if __name__ == "__main__":
    # Initialize and train all models
    engine = MLPredictiveEngine()
    print("Training stockout classifier...")
    engine.train_stockout_classifier()
    print("\nTraining reorder regressor...")
    engine.train_reorder_amount_regressor()
    print("\nTraining Prophet models for top 50 product-store combinations...")
    # Train Prophet for most important combos
    try:
        top_combos = engine.con.execute("""
        SELECT product_id, store_id 
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_key = dp.product_key
        JOIN dim_store ds ON fs.store_key = ds.store_key
        GROUP BY product_id, store_id
        ORDER BY SUM(revenue) DESC
        LIMIT 5
        """).fetchdf() # Limit to 5 for speed in test
        
        for _, row in top_combos.iterrows():
            engine.train_demand_forecaster(row['product_id'], row['store_id'])
            print(f"✓ Trained Prophet for Product {row['product_id']}, Store {row['store_id']}")
    except Exception as e:
        print(f"Error training top combos: {e}")