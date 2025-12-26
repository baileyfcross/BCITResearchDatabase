import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import re


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
        return types.NVARCHAR(None)  # MAX

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
df = pd.read_csv('us_state_businesses. csv',
                 header=2,  # Use 3rd row (index 2) as column names
                 low_memory=False)

print(f"Original columns from CSV (row 3): {df.columns.tolist()}")
print(f"Total records: {len(df)}")

# Clean column names for SQL compatibility
original_columns = df.columns.tolist()
df.columns = [clean_column_name(col) for col in df.columns]

print(f"\nCleaned columns: {df.columns.tolist()}")

# Create mapping of old to new column names for reference
column_mapping = dict(zip(original_columns, df.columns))
print("\nColumn name mapping:")
for old, new in column_mapping.items():
    if old != new:
        print(f"  '{old}' -> '{new}'")

# Data Cleaning (dynamically handle columns that exist)
# State column
state_col = next((col for col in df.columns if 'state' in col), None)
if state_col:
    df[state_col] = df[state_col].astype(str).str.upper().str.strip()
    print(f"\nCleaning '{state_col}' column")

# Zip code column
zip_col = next((col for col in df.columns if 'zip' in col or 'postal' in col), None)
if zip_col:
    df[zip_col] = df[zip_col].astype(str).str[:5].str.zfill(5)
    print(f"Cleaning '{zip_col}' column")

# Business name column
name_col = next((col for col in df.columns if 'business' in col and 'name' in col), None)
if not name_col:
    name_col = next((col for col in df.columns if 'name' in col or 'company' in col), None)
if name_col:
    df[name_col] = df[name_col].astype(str).str.strip().str.title()
    print(f"Cleaning '{name_col}' column")

# Phone column
phone_col = next((col for col in df.columns if 'phone' in col), None)
if phone_col:
    df[phone_col] = df[phone_col].astype(str).str.replace(r'\D', '', regex=True)
    print(f"Cleaning '{phone_col}' column")

# Handle nulls
df = df.replace({np.nan: None})

# Validate states (if state column exists)
if state_col:
    valid_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC']

    invalid_states = df[~df[state_col].isin(valid_states)]
    if len(invalid_states) > 0:
        print(f"\nWarning: {len(invalid_states)} records with invalid states")
        print(f"Invalid state values: {invalid_states[state_col].unique()}")
        # Optionally filter them out
        df = df[df[state_col].isin(valid_states)]
        print(f"Filtered dataset: {len(df)} valid records remaining")

# Build dynamic dtype mapping for SQL
dtype_mapping = {}
for col in df.columns:
    dtype_mapping[col] = infer_sql_type(df[col], col)

print("\n" + "=" * 60)
print("SQL Column Type Mapping:")
print("=" * 60)
for col, sql_type in dtype_mapping.items():
    print(f"  {col: 30s} -> {sql_type}")
print("=" * 60)

# Database connection (Azure SQL)
connection_string = (
    "mssql+pyodbc://your-server.database.windows.net:1433/"
    "your-database?driver=ODBC+Driver+17+for+SQL+Server"
    "&uid=username&pwd=password"
)

engine = create_engine(connection_string)

# Load in chunks (better for large files)
chunk_size = 10000
total_loaded = 0

print(f"\nStarting data load to database...")
print(f"Table name: business_data")
print(f"Chunk size: {chunk_size}")

for i in range(0, len(df), chunk_size):
    chunk = df[i:i + chunk_size]

    # Use 'replace' for first chunk to create table, 'append' for rest
    if_exists_mode = 'replace' if i == 0 else 'append'

    chunk.to_sql('business_data',
                 engine,
                 if_exists=if_exists_mode,
                 index=False,
                 dtype=dtype_mapping if i == 0 else None,  # Only specify dtypes on first chunk
                 method='multi')  # Faster batch insert

    total_loaded += len(chunk)
    print(f"Progress: {total_loaded: ,}/{len(df):,} records ({total_loaded / len(df) * 100:.1f}%)")

print(f"\n{'=' * 60}")
print(f"Import complete!")
print(f"{'=' * 60}")
print(f"Total records loaded: {total_loaded:,}")
print(f"Table name: business_data")
print(f"Columns: {len(df.columns)}")
print(f"Column names: {', '.join(df.columns.tolist())}")
print(f"{'=' * 60}")
print(f"\nImport Complete!")