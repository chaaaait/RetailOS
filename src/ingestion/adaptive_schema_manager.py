import pandas as pd
import duckdb
import json
import os
import sys
from collections import defaultdict

# Add parent directory for config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "data/warehouse/retail.duckdb"


class AdaptiveSchemaManager:

    def __init__(self):
        self.schema_registry = {}
        self.confidence_threshold = 0.75
        self.con = duckdb.connect(str(DB_PATH))

    # =====================================================
    # INITIALIZATION
    # =====================================================
    def initialize_registry(self):

        self.schema_registry = {
            "transactions": {
                "version": 1,
                "required": [
                    "transaction_id",
                    "product_id",
                    "store_id",
                    "timestamp",
                    "quantity",
                    "price",
                ],
                "optional": ["discount", "payment_method", "customer_id"],
                "types": {
                    "transaction_id": "int64",
                    "product_id": "int64",
                    "store_id": "int64",
                    "quantity": "int64",
                    "price": "float64",
                    "timestamp": "datetime64",
                },
            }
        }

        # Sequences
        self.con.execute("CREATE SEQUENCE IF NOT EXISTS schema_change_seq START 1")
        self.con.execute("CREATE SEQUENCE IF NOT EXISTS schema_queue_seq START 1")

        # Tables
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS schema_change_log (
            change_id INTEGER DEFAULT nextval('schema_change_seq') PRIMARY KEY,
            table_name VARCHAR,
            change_type VARCHAR,
            column_name VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            confidence_score FLOAT,
            status VARCHAR,
            detected_at TIMESTAMP,
            approved_at TIMESTAMP,
            approved_by VARCHAR,
            affected_rows INTEGER,
            sample_data VARCHAR
        )
        """)

        self.con.execute("""
        CREATE TABLE IF NOT EXISTS schema_approval_queue (
            queue_id INTEGER DEFAULT nextval('schema_queue_seq') PRIMARY KEY,
            table_name VARCHAR,
            action VARCHAR,
            reason VARCHAR,
            decision_json TEXT,
            status VARCHAR,
            created_at TIMESTAMP
        )
        """)

    # =====================================================
    # CHANGE DETECTION
    # =====================================================
    def detect_schema_changes(self, table_name, df):

        schema = self.schema_registry.get(table_name)
        if not schema:
            return [], []

        incoming_cols = set(df.columns)
        expected_cols = set(schema["required"] + schema["optional"])

        new_cols = incoming_cols - expected_cols
        missing_required = set(schema["required"]) - incoming_cols

        changes = []

        for col in new_cols:
            confidence = self._calculate_confidence(df[col])
            changes.append({
                "type": "new_column",
                "column": col,
                "confidence": confidence,
                "data_type": str(df[col].dtype),
                "sample_values": df[col].dropna().astype(str).head(5).tolist()
            })

        return changes, missing_required

    # =====================================================
    # CONFIDENCE
    # =====================================================
    def _calculate_confidence(self, series):

        if len(series) == 0:
            return 0.0

        confidence = 0.0

        null_ratio = series.isnull().mean()
        confidence += (1 - null_ratio) * 0.3

        unique_ratio = series.nunique() / len(series)
        if 0.01 < unique_ratio < 0.99:
            confidence += 0.3
        else:
            confidence += 0.1

        if series.dtype != "object":
            confidence += 0.4
        else:
            confidence += 0.2

        return min(confidence, 1.0)

    # =====================================================
    # DECISION LOGIC
    # =====================================================
    def apply_noise_reduction_strategy(self, changes):

        if not changes:
            return {"action": "none", "changes": []}

        high = [c for c in changes if c["confidence"] >= self.confidence_threshold]
        low = [c for c in changes if c["confidence"] < self.confidence_threshold]

        if len(low) > 5:
            return {
                "action": "quarantine_all",
                "reason": "Mass low-confidence changes",
                "changes": changes,
            }

        if len(high) == len(changes) and len(changes) <= 3:
            return {
                "action": "auto_approve",
                "changes": changes,
            }

        return {
            "action": "manual_review",
            "reason": "Requires review",
            "changes": changes,
        }

    # =====================================================
    # LOGGING
    # =====================================================
    def log_pending_change(self, table_name, decision):

        if "changes" not in decision:
            return

        for change in decision["changes"]:
            self.con.execute("""
            INSERT INTO schema_change_log (
                table_name,
                change_type,
                column_name,
                old_value,
                new_value,
                confidence_score,
                status,
                detected_at,
                approved_at,
                approved_by,
                affected_rows,
                sample_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, NULL, 0, ?)
            """, [
                table_name,
                change["type"],
                change["column"],
                "",
                change["data_type"],
                change["confidence"],
                decision["action"],
                json.dumps(change["sample_values"]),
            ])

    def _create_approval_request(self, table_name, decision):

        self.con.execute("""
        INSERT INTO schema_approval_queue (
            table_name,
            action,
            reason,
            decision_json,
            status,
            created_at
        )
        VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
        """, [
            table_name,
            decision["action"],
            decision.get("reason", ""),
            json.dumps(decision, default=str),
        ])

    # =====================================================
    # MAIN ENTRY
    # =====================================================
    def process_ingestion_with_adaptive_schema(self, table_name, df):

        changes, missing = self.detect_schema_changes(table_name, df)

        if missing:
            return {
                "status": "rejected",
                "action": "quarantine",
                "reason": f"Missing required columns: {missing}",
            }

        if not changes:
            return {"status": "approved", "action": "process"}

        decision = self.apply_noise_reduction_strategy(changes)

        self.log_pending_change(table_name, decision)

        if decision["action"] == "auto_approve":
            return {"status": "approved", "action": "process"}

        self._create_approval_request(table_name, decision)

        return {
            "status": "pending_approval",
            "action": "hold",
            "decision": decision,
        }
