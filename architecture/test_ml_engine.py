# tests/test_ml_engine.py
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.intelligence.ml_predictive_engine import MLPredictiveEngine
import duckdb
import traceback

def main():
    print("=== TESTING ML PREDICTIVE ENGINE ===\n")

    try:
        # Connect to DuckDB
        con = duckdb.connect("data/warehouse/retail.duckdb")

        # Initialize engine
        engine = MLPredictiveEngine()

        # -------------------------------
        # Test 1: Train Stockout Classifier
        # -------------------------------
        print("1️⃣ Training Stockout Classifier...")
        accuracy, importance = engine.train_stockout_classifier()
        print(f"✅ Accuracy: {accuracy:.2%}\n")

        print("Feature Importance:")
        print(importance)
        print()

        # -------------------------------
        # Test 2: Train Reorder Regressor
        # -------------------------------
        print("2️⃣ Training Reorder Regressor...")
        score = engine.train_reorder_amount_regressor()
        print(f"✅ R² Score: {score:.2f}\n")

        # -------------------------------
        # Test 3: Make Sample Prediction
        # -------------------------------
        print("3️⃣ Making a Sample Prediction...")
        result = engine.predict_stockout_with_explanation(
            product_id='P0000',
            store_id='ST000'
        )

        print(f"✅ Risk Level: {result['explanation']['risk_level']}")
        print(f"✅ Confidence: {result['confidence']:.1%}")
        print(f"✅ Recommended Reorder: {result['recommended_reorder']} units\n")

        # -------------------------------
        # Test 4: Verify Logging
        # -------------------------------
        print("4️⃣ Checking ML Reasoning Log...")
        logged = con.execute(
            "SELECT COUNT(*) FROM ml_reasoning_log"
        ).fetchone()[0]

        print(f"✅ {logged} predictions logged in database\n")

        print("=== ALL ML TESTS COMPLETED ===")

    except Exception as e:
        print("❌ TEST FAILED")
        print("Error details:")
        traceback.print_exc()

    finally:
        try:
            con.close()
        except:
            pass


if __name__ == "__main__":
    main()
