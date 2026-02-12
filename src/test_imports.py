import sys
print(f"Python Executable: {sys.executable}")
try:
    import numpy
    print(f"NumPy version: {numpy.__version__}")
except ImportError as e:
    print(f"NumPy import failed: {e}")

try:
    import pandas
    print(f"Pandas version: {pandas.__version__}")
except ImportError as e:
    print(f"Pandas import failed: {e}")

try:
    import faker
    print(f"Faker version: {faker.__version__}")
except ImportError as e:
    print(f"Faker import failed: {e}")
