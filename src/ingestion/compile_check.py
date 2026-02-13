import sys
import os
import py_compile

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

files_to_check = [
    'src/config.py',
    'src/intelligence/ml_predictive_engine.py',
    'src/ingestion/adaptive_schema_manager.py',
    'src/ingestion/websocket_streaming.py',
    'src/ingestion/batch_scheduler.py',
    'src/app_enhanced.py'
]

print("Verifying Python syntax for modified files...")
failed = False
for file_path in files_to_check:
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        failed = True
        continue
        
    try:
        py_compile.compile(file_path, doraise=True)
        print(f"✅ Syntax OK: {file_path}")
    except py_compile.PyCompileError as e:
        print(f"❌ Syntax Error in {file_path}:")
        print(e)
        failed = True
    except Exception as e:
        print(f"❌ Error checking {file_path}: {e}")
        failed = True

if failed:
    sys.exit(1)
else:
    print("\nAll files passed syntax check.")
    sys.exit(0)
