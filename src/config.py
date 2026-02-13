import os
from pathlib import Path

# Base Paths
# Assuming src/config.py, so parent is src, grandparent is project root
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
WAREHOUSE_DIR = DATA_DIR / 'warehouse'
MODELS_DIR = BASE_DIR / 'models'
LOGS_DIR = BASE_DIR / 'logs'

# Database
DB_PATH = WAREHOUSE_DIR / 'retail.duckdb'

# WebSocket
WS_HOST = os.getenv('WS_HOST', 'localhost')
WS_PORT = int(os.getenv('WS_PORT', 8765))

# Scheduler
BATCH_INTERVAL_HOURS = 6
ML_RETRAIN_HOUR = 2

# ML Models
MIN_TRAINING_DAYS = 30
CONFIDENCE_THRESHOLD = 0.75

# Ensure directories exist
for directory in [DATA_DIR, WAREHOUSE_DIR, MODELS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
