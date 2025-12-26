import pandas as pd
from sqlalchemy import create_engine, types
import numpy as np
import re
from dotenv import load_dotenv
import os


def clean_column_name(col_name):
    """Clean column name for SQL compatibility"""
    col_name = str(col_name).strip().lower()
    col_name = re.sub(r'[^\w\s]', '', col_name)  # Remove special chars
    col_name = re.sub(r'\s+', '_', col_name)  # Replace spaces with underscore
    col_name = re.sub(r'_+', '_', col_name)  # Remove duplicate underscores
    return col_name


def infer_sql_type(series, col_name):
    """Intelligently infer SQL type from pandas series and column name"""
    col_lower = col_name.lower()

    # Check column name patterns first
    if 'state' in col_lower and series.astype(str).str.len().max() <= 2:
        return types.CHAR(2)
    elif 'zip' in col_lower or 'postal' in col_lower:
        return types.VARCHAR(10)
    elif 'phone' in col_lower or 'fax' in col_lower or 'fein' in col_lower:
        return types.VARCHAR(20)
    elif 'email' in col_lower:
        return types.VARCHAR(255)
    elif 'url' in col_lower or 'website' in col_lower:
        return types.VARCHAR(500)
    elif 'description' in col_lower or 'notes' in col_lower:
        return types.TEXT(None)  # MAX

    # Check data type
    if pd.api.types.is_integer_dtype(series):
        return types.INTEGER
    elif pd.api.types.is_float_dtype(series):
        return types.DECIMAL(15, 2)
    elif pd.api.types.is_datetime64_any_dtype(series):
        return types.DATETIME
    elif pd.api.types.is_bool_dtype(series):
        return types.BOOLEAN
    else:
        # For strings, determine appropriate length
        max_len = series.astype(str).str.len().max()
        if max_len <= 50:
            return types.VARCHAR(50)
        elif max_len <= 255:
            return types.NVARCHAR(255)
        else:
            return types.NVARCHAR(500)


# Read CSV with header in 3rd row (row index 2, since indexing starts at 0)
# This skips rows 0 and 1, and uses row 2 as column names
print(f"Reading XLSX File")
df = pd.read_excel('county_3digitnaics_2022.xlsx',
                sheet_name=0,
                header=2,  # Use 3rd row (index 2) as column names
                engine='openpyxl')

print(f"Original columns from CSV (row 3): {df.columns.tolist()}")
print(f"Total records: {len(df)}")

# Clean column names for SQL compatibility
original_columns = df.columns.tolist()
df.columns = [clean_column_name(col) for col in df.columns]

print(f"\nCleaned columns: {df.columns.tolist()}")

# Create mapping of old to new column names for reference
column_mapping = dict(zip(original_columns, df. columns))
print("\nColumn name mapping:")
for old, new in column_mapping.items():
    if old != new:
        print(f"  '{old}' -> '{new}'")

# Handle nulls only
df = df.replace({np.nan: None})

print(f"\nTotal columns to import: {len(df.columns)}")
print(f"Total records to import: {len(df)}")

# Build dynamic dtype mapping for SQL
dtype_mapping = {}
for col in df.columns:
    dtype_mapping[col] = infer_sql_type(df[col], col)

print("\n" + "=" * 60)
print("SQL Column Type Mapping:")
print("=" * 60)
for col, sql_type in dtype_mapping.items():
    print(f"  {col:30s} -> {sql_type}")
print("=" * 60)
# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD')

if not DB_NAME or not DB_PASSWORD:
    raise ValueError("DB_NAME and DB_PASSWORD must be set in .env file")
# Database connection (LocalHost)
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"\nConnecting to PostgreSQL database...")
try:
    engine = create_engine(connection_string)

    # Test connection
    with engine.connect() as conn:
        print("✓ Successfully connected to PostgreSQL!")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    exit(1)

# Load in chunks (better for large files)
chunk_size = 10000
total_loaded = 0

print(f"\nStarting data load to database...")
print(f"Table name: business_data")
print(f"Chunk size: {chunk_size}")

try:
    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]

        # Use 'replace' for first chunk to create table, 'append' for rest
        if_exists_mode = 'replace' if i == 0 else 'append'

        chunk.to_sql('business_data',
                     engine,
                     if_exists=if_exists_mode,
                     index=False,
                     dtype=dtype_mapping if i == 0 else None,
                     method='multi')

        total_loaded += len(chunk)
        print(f"Progress: {total_loaded: ,}/{len(df):,} records ({total_loaded / len(df) * 100:.1f}%)")

    print(f"\n{'=' * 60}")
    print(f"Import complete!")
    print(f"{'=' * 60}")
    print(f"Total records loaded: {total_loaded: ,}")
    print(f"Table name: business_data")
    print(f"Columns:  {len(df.columns)}")
    print(f"Column names: {', '.join(df.columns.tolist())}")
    print(f"{'=' * 60}")

except Exception as e:
    print(f"\n✗ Error during data load:  {e}")
    raise