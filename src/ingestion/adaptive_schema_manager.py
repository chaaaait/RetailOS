import pandas as pd
import duckdb
from collections import defaultdict
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config import DB_PATH
except ImportError:
    DB_PATH = 'data/warehouse/retail.duckdb'

class AdaptiveSchemaManager:
    def __init__(self):
        self.schema_registry = {}
        self.schema_history = defaultdict(list)
        self.pending_changes = {}
        self.confidence_threshold = 0.75
        
        try:
            self.con = duckdb.connect(str(DB_PATH))
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
        
    def initialize_registry(self):
        """
        Initialize schema registry with versioning
        """
        self.schema_registry = {
            'transactions': {
                'version': 1,
                'required': ['transaction_id', 'product_id', 'store_id', 'timestamp', 'quantity', 'price'],
                'optional': ['discount', 'payment_method', 'customer_id'],
                'types': {
                    'transaction_id': 'int64',
                    'product_id': 'int64',
                    'store_id': 'int64',
                    'quantity': 'int64',
                    'price': 'float64',
                    'timestamp': 'datetime64',
                    'discount': 'float64',
                    'customer_id': 'int64'
                },
                'constraints': {
                    'price': {'min': 0, 'max': 1000000},
                    'quantity': {'min': 1, 'max': 1000}
                }
            },
            # Similar for other tables...
        }
        
        # Create schema tracking table
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS schema_change_log (
            change_id INTEGER PRIMARY KEY,
            table_name VARCHAR,
            change_type VARCHAR,  -- 'new_column', 'type_change', 'constraint_change'
            column_name VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            confidence_score FLOAT,
            status VARCHAR,  -- 'pending', 'approved', 'rejected', 'auto_approved'
            detected_at TIMESTAMP,
            approved_at TIMESTAMP,
            approved_by VARCHAR,
            affected_rows INTEGER,
            sample_data TEXT
        )
        """)

        self.con.execute("""
        CREATE TABLE IF NOT EXISTS schema_approval_queue (
            queue_id INTEGER PRIMARY KEY,
            table_name VARCHAR,
            action VARCHAR,
            reason VARCHAR,
            decision_json TEXT,
            status VARCHAR,
            created_at TIMESTAMP
        )
        """)
        
    def detect_schema_changes(self, table_name, df):
        """
        Multi-stage schema change detection with confidence scoring
        """
        if table_name not in self.schema_registry:
             # Basic default schema if unknown
             self.schema_registry[table_name] = {'required': [], 'optional': [], 'types': {}, 'version': 1}

        current_schema = self.schema_registry[table_name]
        detected_changes = []
        
        # Stage 1: Column-level detection
        incoming_columns = set(df.columns)
        expected_columns = set(current_schema.get('required', []) + current_schema.get('optional', []))
        
        new_columns = incoming_columns - expected_columns
        missing_columns = set(current_schema.get('required', [])) - incoming_columns
        
        # Stage 2: Calculate confidence for each change
        for col in new_columns:
            confidence = self._calculate_column_confidence(df, col, table_name)
            
            try:
                sample_vals = df[col].dropna().head(10).tolist()
                # Convert timestamps to string for JSON serialization
                sample_vals = [str(x) if isinstance(x, (pd.Timestamp, datetime)) else x for x in sample_vals]
            except:
                sample_vals = []

            change = {
                'type': 'new_column',
                'column': col,
                'confidence': confidence,
                'data_type': str(df[col].dtype),
                'null_percentage': df[col].isnull().sum() / max(len(df), 1),
                'unique_percentage': df[col].nunique() / max(len(df), 1),
                'sample_values': sample_vals
            }
            detected_changes.append(change)
        
        # Stage 3: Type change detection
        for col in incoming_columns & expected_columns:
            expected_type = current_schema.get('types', {}).get(col)
            actual_type = str(df[col].dtype)
            
            if expected_type and expected_type != actual_type:
                confidence = self._calculate_type_change_confidence(df[col], expected_type, actual_type)
                
                try:
                    sample_vals = df[col].head(10).tolist()
                    sample_vals = [str(x) if isinstance(x, (pd.Timestamp, datetime)) else x for x in sample_vals]
                except:
                    sample_vals = []

                change = {
                    'type': 'type_change',
                    'column': col,
                    'confidence': confidence,
                    'old_type': expected_type,
                    'new_type': actual_type,
                    'sample_values': sample_vals
                }
                detected_changes.append(change)
        
        return detected_changes, missing_columns
    
    def _calculate_column_confidence(self, df, col, table_name):
        """
        Calculate confidence that a new column is legitimate (not noise)
        """
        confidence = 0.0
        
        if len(df) == 0:
            return 0.0

        # Factor 1: Naming convention
        col_lower = col.lower()
        common_patterns = ['id', 'name', 'date', 'time', 'amount', 'price', 'quantity', 
                          'status', 'type', 'code', 'description', 'flag']
        if any(pattern in col_lower for pattern in common_patterns):
            confidence += 0.50
        elif col.replace('_', '').isalnum():  # Valid identifier
            confidence += 0.30
        
        # Factor 2: Completeness (less null = higher confidence)
        null_ratio = df[col].isnull().sum() / len(df)
        confidence += (1 - null_ratio) * 0.20
        
        # Factor 3: Type consistency
        try:
            # Check if column can be cast to common types
            if df[col].dtype == 'object':
                # Try numeric conversion
                pd.to_numeric(df[col], errors='raise')
                confidence += 0.15
            else:
                confidence += 0.15
        except:
            # Mixed types reduce confidence
            confidence += 0.05
        
        # Factor 4: Value distribution
        unique_ratio = df[col].nunique() / len(df)
        if 0.01 < unique_ratio < 0.99:  # Not all same, not all unique
            confidence += 0.15
        elif unique_ratio <= 0.01 or unique_ratio >= 0.99:
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def _calculate_type_change_confidence(self, series, old_type, new_type):
        """
        Calculate confidence for type changes
        """
        confidence = 0.0
        
        # Acceptable type transitions
        safe_transitions = {
            ('int64', 'float64'): 0.9,  # Widening is safe
            ('float64', 'int64'): 0.5,  # Narrowing needs review
            ('object', 'datetime64'): 0.8,  # String to date is common
            ('int64', 'object'): 0.3,   # Numeric to string is suspicious
        }
        
        # Normalize types for comparison (e.g. integer vs int64)
        old_simple = 'int64' if 'int' in str(old_type) else old_type
        new_simple = 'int64' if 'int' in str(new_type) else new_type
        
        transition = (old_simple, new_simple)
        confidence = safe_transitions.get(transition, 0.2)
        
        return confidence
    
    def apply_noise_reduction_strategy(self, changes):
        """
        Multi-column change noise reduction
        """
        if not changes:
            return {'action': 'none', 'changes': []}

        high_confidence = [c for c in changes if c['confidence'] >= self.confidence_threshold]
        low_confidence = [c for c in changes if c['confidence'] < self.confidence_threshold]
        
        # Scenario 1: Mass low-confidence changes (likely corrupt batch)
        if len(low_confidence) > 5:
            return {
                'action': 'quarantine_all',
                'reason': f'{len(low_confidence)} low-confidence changes detected simultaneously',
                'changes': changes,
                'recommendation': 'Review source data quality'
            }
        
        # Scenario 2: Mixed confidence batch (requires approval)
        if len(changes) > 1 and len(low_confidence) > 0:
            return {
                'action': 'batch_approval_required',
                'reason': f'{len(changes)} columns changed, {len(low_confidence)} with low confidence',
                'high_confidence': high_confidence,
                'low_confidence': low_confidence,
                'recommendation': 'Review changes as a group before accepting'
            }
        
        # Scenario 3: Single high-confidence change
        if len(changes) == 1 and changes[0]['confidence'] >= self.confidence_threshold:
            return {
                'action': 'auto_approve',
                'reason': 'Single high-confidence change',
                'changes': changes
            }
        
        # Scenario 4: Multiple high-confidence changes (feature expansion)
        if len(high_confidence) == len(changes) and len(changes) <= 3:
            return {
                'action': 'auto_approve',
                'reason': 'All changes have high confidence',
                'changes': changes
            }
        
        # Default: require manual review
        return {
            'action': 'manual_review',
            'reason': 'Changes do not match auto-approval criteria',
            'changes': changes
        }
    
    def log_pending_change(self, table_name, decision):
        """
        Log changes to database for admin dashboard review
        """
        if 'changes' not in decision:
            return

        insert_query = """
        INSERT INTO schema_change_log VALUES (
            NULL, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, NULL, 0, ?
        )
        """
        
        for change in decision['changes']:
            try:
                self.con.execute(insert_query, [
                    table_name,
                    change['type'],
                    change['column'],
                    change.get('old_type', ''),
                    change.get('new_type', change.get('data_type', '')),
                    change['confidence'],
                    decision['action'],
                    json.dumps(change.get('sample_values', []))
                ])
            except Exception as e:
                print(f"Error logging schema change: {e}")
    
    def process_ingestion_with_adaptive_schema(self, table_name, df):
        """
        Main entry point for adaptive schema handling
        """
        # Detect changes
        changes, missing_required = self.detect_schema_changes(table_name, df)
        
        # Critical: missing required columns
        if missing_required:
            return {
                'status': 'rejected',
                'reason': f'Missing required columns: {missing_required}',
                'action': 'quarantine'
            }
        
        # No changes detected
        if not changes:
            return {
                'status': 'approved',
                'reason': 'Schema matches registry',
                'action': 'process'
            }
        
        # Apply noise reduction
        decision = self.apply_noise_reduction_strategy(changes)
        
        # Log for tracking
        self.log_pending_change(table_name, decision)
        
        # Handle based on decision
        if decision['action'] == 'auto_approve':
            self._update_registry(table_name, decision['changes'])
            return {'status': 'approved', 'action': 'process', 'changes': changes}
        
        elif decision['action'] in ['batch_approval_required', 'manual_review', 'quarantine_all']:
            # Send to admin dashboard for approval
            self._create_approval_request(table_name, decision)
            return {'status': 'pending_approval', 'action': 'hold', 'decision': decision}
        
        return {'status': 'approved', 'action': 'process', 'changes': []}
    
    def _update_registry(self, table_name, changes):
        """
        Update schema registry with approved changes
        """
        for change in changes:
            if change['type'] == 'new_column':
                if 'optional' not in self.schema_registry[table_name]:
                     self.schema_registry[table_name]['optional'] = []
                self.schema_registry[table_name]['optional'].append(change['column'])
                
                if 'types' not in self.schema_registry[table_name]:
                     self.schema_registry[table_name]['types'] = {}
                self.schema_registry[table_name]['types'][change['column']] = change['data_type']
            elif change['type'] == 'type_change':
                self.schema_registry[table_name]['types'][change['column']] = change['new_type']
        
        # Increment version
        self.schema_registry[table_name]['version'] += 1
        
    def _create_approval_request(self, table_name, decision):
        """
        Create approval request in admin dashboard
        """
        def json_serial(obj):
            if isinstance(obj, (datetime, pd.Timestamp)):
                return obj.isoformat()
            if isinstance(obj, set):
                return list(obj)
            raise TypeError (f"Type {type(obj)} not serializable")

        try:
            self.con.execute("""
            INSERT INTO schema_approval_queue VALUES (
                NULL, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP
            )
            """, [
                table_name,
                decision['action'],
                decision['reason'],
                json.dumps(decision, default=json_serial)
            ])
        except Exception as e:
            print(f"Error creating approval request: {e}")

# Initialize registry on import
manager = AdaptiveSchemaManager()
manager.initialize_registry()