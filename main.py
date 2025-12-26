"""

This file takes an Excel file (xlsx) and automatically turns the 3rd row into the column headers for a
postgresSQL Database.

"""

import pandas as pd
from sqlalchemy import create_engine, types, text
import numpy as np
import re
from dotenv import load_dotenv
import os
import tkinter as tk
from tkinter import simpledialog, filedialog
import argparse
import sys
import logging
import time
import io


logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Startup marker to verify updated script is executed
logger.info("IMPORTER_SAFE_SQL_v3 — safe_to_sql and COPY/execute_values fallbacks enabled")


def clean_column_name(col_name):
    """Clean column name for SQL compatibility"""
    col_name = str(col_name).strip().lower()
    col_name = re.sub(r'[^\w\s]', '', col_name)  # Remove special chars
    col_name = re.sub(r'\s+', '_', col_name)  # Replace spaces with underscore
    return col_name


def infer_sql_type(series, col_name=None):
    """Infer SQLAlchemy type from pandas Series. More robust than simple dtype checks.

    Strategy:
    - Drop NA and inspect a sample.
    - Try datetime inference (explicit dtype or >90% coercible to datetime).
    - Detect integers, floats, booleans using pandas type checks and value checks.
    - For text, choose String(length) or Text for long strings.
    - Default to String(255) for empty columns.
    """
    s = series.dropna()

    # If empty column, return a safe VARCHAR
    if s.empty:
        return types.String(length=255)

    # If already datetime dtype, accept it
    if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_datetime64_any_dtype(s):
        return types.DateTime()

    # Boolean detection (prioritize)
    if pd.api.types.is_bool_dtype(series) or s.apply(lambda x: isinstance(x, bool)).all():
        return types.BOOLEAN()

    # Numeric checks (prioritize numbers so they aren't mistaken for date times)
    # If pandas recognizes integer dtype
    if pd.api.types.is_integer_dtype(series) or (not s.empty and s.apply(lambda x: isinstance(x, (int, np.integer)) and not isinstance(x, bool)).all()):
        # Safely compute min/max and choose INTEGER or BIGINT depending on range.
        try:
            nums = pd.to_numeric(s, errors='coerce').dropna()
            # If after coercion we have numeric values, inspect range
            if not nums.empty:
                # Convert to integer-type for range checking
                ints = nums.astype('int64', copy=False)
                minv = ints.min()
                maxv = ints.max()
                # 32-bit signed int bounds
                if minv >= -2147483648 and maxv <= 2147483647:
                    return types.INTEGER()
                else:
                    return types.BigInteger()
        except Exception:
            # If anything goes wrong while checking, prefer BigInteger to avoid overflow errors
            return types.BigInteger()

    # Floats
    if pd.api.types.is_float_dtype(series) or s.apply(lambda x: isinstance(x, (float, np.floating))).all():
        return types.FLOAT()

    # Only attempt to parse date times for object/string-like columns to avoid
    # interpreting numeric IDs as timestamps.
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        coerced = pd.to_datetime(s, errors='coerce')
        if coerced.notna().sum() / len(s) > 0.9:
            return types.DateTime()

    # Fallback to string/text
    # compute max string length (safely)
    try:
        max_len = int(min(max(s.astype(str).map(len).max(), 1), 65535))
    except Exception:
        max_len = 255

    if max_len > 255:
        return types.Text()

    return types.String(length=max_len)


def get_excel_file_popup():
    """Open file browser to select Excel file"""
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)

    # Open file dialog
    file_path = filedialog.askopenfilename(
        title="Select Excel File",
        filetypes=[
            ("Excel files", "*.xlsx *.xls"),
            ("Excel 2007+ files", "*.xlsx"),
            ("Excel 97-2003 files", "*. xls"),
            ("All files", "*.*")
        ],
        parent=root
    )

    root.destroy()

    # Check if user canceled
    if not file_path:
        logger.error("No file selected.  Exiting")
        sys.exit(0)

    return file_path


def get_table_name_popup_simple():
    """Simple popup for table name"""
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)

    table_name = simpledialog.askstring(
        "Database Table Name",
        "Enter table name:",
        parent=root
    )

    root.destroy()

    if not table_name:
        logger.error("No table name provided.  Exiting")
        sys.exit(1)

    # Clean table name
    table_name = re.sub(r'\W', '_', table_name).lower()
    table_name = re.sub(r'_+', '_', table_name).strip('_')

    return table_name


def safe_to_sql(df_sub, engine, table_name, if_exists, dtype, param_threshold=20000):
    """Write df_sub to SQL with retries: if a bulk write fails, split and retry recursively.

    - Chooses method='multi' when estimated params <= param_threshold.
    - On exception, if df_sub has >1 row, split in half and retry each half.
    - If single row fails, dump to CSV for inspection and re-raise.
    """
    num_cols = len(df_sub.columns)
    est = num_cols * len(df_sub)
    method = 'multi' if est <= param_threshold else None
    # Diagnostic: report engine dialect and driver
    try:
        dialect_name = getattr(engine.dialect, 'name', None)
        driver_name = getattr(engine.dialect, 'driver', None)
    except Exception:
        dialect_name = None
        driver_name = None
    logger.info(f"safe_to_sql: engine dialect={dialect_name}, driver={driver_name}, rows={len(df_sub)}, cols={num_cols}, est_params={est}")

    # For PostgresSQL, prefer COPY FROM STDIN (CSV) to avoid building massive parameterized statements
    if hasattr(engine, 'dialect') and getattr(engine.dialect, 'name', None) == 'postgresql' and len(df_sub) > 0:
        try:
            # If table needs to be (re)created, create an empty table with correct schema first
            if if_exists == 'replace':
                df_sub.iloc[:0].to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype)

            # Prepare qualified table name for COPY (handle optional schema.table)
            if '.' in table_name:
                schema, tbl = table_name.split('.', 1)
                qualified = f'"{schema}"."{tbl}"'
            else:
                qualified = f'"{table_name}"'

            cols = ','.join([f'"{c}"' for c in df_sub.columns])

            buf = io.StringIO()
            # Use standard CSV (no index, no header) - COPY will map columns in same order
            df_sub.to_csv(buf, header=False, index=False, na_rep='\\N')
            buf.seek(0)

            raw = engine.raw_connection()
            cur = raw.cursor()
            sql = f"COPY {qualified} ({cols}) FROM STDIN WITH CSV"
            cur.copy_expert(sql, buf)
            raw.commit()
            cur.close()
            logger.info("safe_to_sql: wrote block using Postgres COPY")
            return
        except Exception as copy_e:
            logger.warning(f"Postgres COPY failed: {copy_e} — falling back to execute_values/to_sql")

        # Next try psycopg2.extras.execute_values which avoids massive named-parameter dicts
        try:
            try:
                import psycopg2.extras as extras
            except Exception as imp_e:
                logger.warning(f"psycopg2 extras not available ({imp_e}) — execute_values not available, pandas.to_sql will be used as fallback")
                raise

            raw = engine.raw_connection()
            cur = raw.cursor()

            # Recreate table schema if requested
            if if_exists == 'replace':
                df_sub.iloc[:0].to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype)

            cols = ','.join([f'"{c}"' for c in df_sub.columns])
            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES %s"

            # Convert DataFrame to list of tuples, replace NaN with None
            data = [tuple(row) for row in df_sub.where(pd.notnull(df_sub), None).to_numpy()]

            # Write in pages to keep memory and param counts low
            page_size = 1000
            for start in range(0, len(data), page_size):
                page = data[start:start + page_size]
                try:
                    extras.execute_values(cur, insert_sql, page, template=None, page_size=page_size)
                    raw.commit()
                except Exception as page_e:
                    logger.warning(f"execute_values failed on page {start}..{start+len(page)-1}: {page_e} — trying per-row fallback")
                    # Try per-row to isolate bad rows
                    for idx, row in enumerate(page, start=start):
                        try:
                            extras.execute_values(cur, insert_sql, [row], template=None, page_size=1)
                            raw.commit()
                        except Exception as row_e:
                            # Dump the problematic row
                            bad_df = pd.DataFrame([row], columns=df_sub.columns)
                            fname = os.path.abspath(f"failed_row_{table_name}_{int(time.time())}_{idx}.csv")
                            try:
                                bad_df.to_csv(fname, index=False)
                                logger.error(f"Row {idx} failed to insert; dumped to {fname}")
                            except Exception as dump_e:
                                logger.error(f"Also failed to dump row {idx}: {dump_e}")
                            raw.rollback()
                            raise row_e

            cur.close()
            logger.info("safe_to_sql: wrote block using psycopg2.extras.execute_values in paged batches")
            return
        except Exception as exec_e:
            logger.warning(f"execute_values failed or unavailable: {exec_e} — falling back to pandas to_sql")

    try:
        logger.info(f"safe_to_sql: writing block using pandas.to_sql (method={method})")
        df_sub.to_sql(table_name,
                      engine,
                      if_exists=if_exists,
                      index=False,
                      dtype=dtype,
                      method=method)
        logger.info("safe_to_sql: wrote block using pandas.to_sql")
    except Exception as e:
        logger.error(f"Write failed for block rows ~{len(df_sub)} (est {est} params): {e}")
        if len(df_sub) <= 1:
            # Dump failing single row to CSV for debugging
            fname = os.path.abspath(f"failed_row_{table_name}_{int(time.time())}.csv")
            try:
                df_sub.to_csv(fname, index=False)
                logger.error(f"Single-row write failed. Dumped row to {fname} for inspection.")
            except Exception as dump_e:
                logger.error(f"Also failed to dump row to CSV: {dump_e}")
            raise
        # Split and retry
        mid = len(df_sub) // 2
        first = df_sub.iloc[:mid]
        second = df_sub.iloc[mid:]
        # First keeps the same if_exists flag, subsequent must append
        safe_to_sql(first, engine, table_name, if_exists, dtype, param_threshold)
        safe_to_sql(second, engine, table_name, 'append', None, param_threshold)


# ---------------------------
# CLI argument handling
# ---------------------------
parser = argparse.ArgumentParser(description='Import 3rd-row header Excel into Postgres')
parser.add_argument('--file', '-f', help='Path to Excel file')
parser.add_argument('--table', '-t', help='Table name to write to (optional)')
parser.add_argument('--no-gui', action='store_true', help='Disable GUI dialogs; require --file and --table')
parser.add_argument('--dry-run', action='store_true', help='Do not write to DB; just show what would be done')
parser.add_argument('--chunk-size', type=int, default=1000, help='Row chunk size to process at a time (default: 1000)')
parser.add_argument('--force-replace', action='store_true', help='If set, drop the target table before importing to avoid schema conflicts')
args = parser.parse_args()

# Respect the --dry-run flag. When set, the script will skip database writes and only show actions.
DRY_RUN = bool(getattr(args, 'dry_run', False))
if DRY_RUN:
    logger.info("--dry-run enabled: the script will skip database writes and any destructive database operations. No connection to the database will be attempted.")
else:
    logger.info("Running in normal mode: the script will write to the configured database unless you pass --dry-run.")

# Determine excel file (CLI -> GUI)
if args.file:
    excel_file = args.file
else:
    if args.no_gui:
        logger.error('No Excel file provided and GUI disabled. Exiting.')
        sys.exit(1)
    excel_file = get_excel_file_popup()

logger.info(f"Selected file: {excel_file}")
logger.info("Reading XLSX File")

# ---------------------------
# Read Excel
# ---------------------------
try:
    df = pd.read_excel(excel_file,
                    sheet_name=0,
                    header=2,  # Use 3rd row (index 2) as column names
                    engine='openpyxl')

    logger.info("✓ File loaded successfully")
    logger.info(f"Original columns from Excel (row 3): {df.columns.tolist()}")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Data types:\n{df.dtypes}")

except Exception as e:
    logger.error(f"✗ Error reading Excel file: {e}")
    raise

# Clean column names for SQL compatibility
original_columns = df.columns.tolist()
df.columns = [clean_column_name(col) for col in df.columns]

logger.info(f"\nCleaned columns: {df.columns.tolist()}")

# Create mapping of old to new column names for reference
column_mapping = dict(zip(original_columns, df.columns))
logger.info("\nColumn name mapping:")
for old, new in column_mapping.items():
    if old != new:
        logger.info(f"  '{old}' -> '{new}'")

# Show sample data
logger.info("\n" + "="*60)
logger.info("SAMPLE DATA (first 3 rows):")
logger.info("="*60)
logger.info(str(df.head(3)))
logger.info("\n" + "="*60)

# Determine table name early so force-replace and dry-run logic can access it
if args.table:
    # clean provided table name
    table_name = re.sub(r'\W', '_', args.table).lower()
    table_name = re.sub(r'_+', '_', table_name).strip('_')
else:
    if args.no_gui:
        logger.error('No table name provided and GUI disabled. Exiting.')
        sys.exit(1)
    table_name = get_table_name_popup_simple()

logger.info(f"Table name: {table_name}")

# Check for problematic values
logger.info("\nData Quality Check:")
for col in df.columns:
    null_count = df[col].isna().sum()
    inf_count = np.isinf(df[col].replace([np.inf, -np.inf], np.nan)).sum() if pd.api.types.is_numeric_dtype(df[col]) else 0
    logger.info(f"  {col}: {null_count} nulls, {inf_count} infinities")

# Handle nulls only
logger.info("\nReplacing NaN with None for SQL compatibility...")
# Keep as pandas NaN for to_sql but ensure simple types; some DB drivers accept numpy.nan -> NULL
# We'll keep as is but ensure no python objects in columns

# Convert problematic python objects (like dict/list) to JSON/string
for col in df.columns:
    if df[col].apply(lambda x: x is not None and not isinstance(x, (str, int, float, bool, np.integer, np.floating, np.bool_))).any():
        logger.info(f"  Notice: Column '{col}' contains non-primitive types - converting values to strings for safe import.")
        df[col] = df[col].astype(str)

logger.info(f"\nTotal columns to import: {len(df.columns)}")
logger.info(f"Total records to import: {len(df)}")

# Build dynamic dtype mapping for SQL
logger.info("\nInferring SQL column types...")
dtype_mapping = {}
for col in df.columns:
    try:
        dtype_mapping[col] = infer_sql_type(df[col], col)
        logger.info(f"  ✓ {col:30s} -> {dtype_mapping[col]}")
    except Exception as e:
        logger.error(f"  ✗ Error inferring type for '{col}': {e}")
        dtype_mapping[col] = types.String(length=255)

logger.info("=" * 60)
# Load environment variables from .env file
# Only load and connect to the database if we are not in dry run mode
if not DRY_RUN:
    load_dotenv()

    # Get database credentials from environment
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    if not DB_NAME or not DB_PASSWORD:
        raise ValueError("DB_NAME and DB_PASSWORD must be set in .env file")

    # Database connection (LocalHost:postgresql)
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    logger.info(f"\nConnecting to PostgresSQL database...")
    try:
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as conn:
            logger.info("✓ Successfully connected to PostgresSQL!")
    except Exception as e:
        logger.error(f"✗ Connection failed: {e}")
        raise
else:
    engine = None

# If user requested force replace, drop the existing table first to avoid datatype mismatch
if args.force_replace:
    if DRY_RUN:
        logger.info(f"--force-replace requested but running in dry-run mode. The existing table will not be dropped. To actually drop the table and import, run without --dry-run.")
    else:
        try:
            logger.info(f"--force-replace enabled: dropping table {table_name} if it exists...")
            # Handle optional schema.table
            if '.' in table_name:
                schema, tbl = table_name.split('.', 1)
                drop_sql = text(f'DROP TABLE IF EXISTS "{schema}"."{tbl}" CASCADE')
            else:
                drop_sql = text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            with engine.begin() as conn:
                conn.execute(drop_sql)
            logger.info(f"Dropped table {table_name} (if it existed)")
        except Exception as drop_e:
            logger.error(f"Failed to drop table {table_name}: {drop_e}")
            raise

try:
    total_loaded = 0
    chunk_size = args.chunk_size

    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]

        # Use 'replace' for first chunk to create table, 'append' for rest
        if_exists_mode = 'replace' if i == 0 else 'append'

        logger.info(f"\nProcessing chunk {i//chunk_size + 1} (rows {i} to {i+len(chunk)-1})...")

        if DRY_RUN:
            logger.info("Dry-run enabled - skipping database write. Showing chunk sample:")
            logger.info(str(chunk.head(3)))
            total_loaded += len(chunk)
            logger.info(f"✓ Progress: {total_loaded:,}/{len(df):,} records ({total_loaded / len(df) * 100:.1f}%)")
            continue

        try:
            # Estimate number of SQL parameters if using multi-row INSERT
            num_columns = len(chunk.columns)
            num_rows = len(chunk)
            estimated_params = num_columns * num_rows

            # PostgresSQL has a hard limit on the number of parameters in a prepared statement (~65535)
            # Use a conservative threshold to stay well below the DB/driver limits.
            PARAM_THRESHOLD = 20000

            # Compute safe rows per subchunk so that num_columns * safe_rows <= PARAM_THRESHOLD
            safe_rows = max(1, PARAM_THRESHOLD // max(1, num_columns))

            if estimated_params > PARAM_THRESHOLD:
                logger.info(f"Chunk would create ~{estimated_params:,} SQL params (> {PARAM_THRESHOLD}) — splitting into sub-chunks of up to {safe_rows} rows to avoid DB parameter limits.")

                # Write sub-chunks sequentially
                for start in range(0, num_rows, safe_rows):
                    sub = chunk.iloc[start:start + safe_rows]

                    # first write of whole import should use replace, subsequent are append
                    sub_if_exists = if_exists_mode if (i == 0 and start == 0) else 'append'
                    sub_dtype = dtype_mapping if (i == 0 and start == 0) else None

                    # Choose writer method for the smaller subchunk
                    sub_estimated = len(sub.columns) * len(sub)
                    sub_method = 'multi' if sub_estimated <= PARAM_THRESHOLD else None

                    logger.info(f"  Writing sub-chunk rows {i + start}..{i + start + len(sub) - 1} (~{sub_estimated:,} params) using method={sub_method}")

                    # Use safe_to_sql which will choose method and split on failure
                    safe_to_sql(sub, engine, table_name, sub_if_exists, sub_dtype, PARAM_THRESHOLD)

                    total_loaded += len(sub)
                    logger.info(f"✓ Progress: {total_loaded:,}/{len(df):,} records ({total_loaded / len(df) * 100:.1f}%)")
            else:
                writer_method = 'multi'
                logger.info(f"Chunk would create ~{estimated_params:,} SQL params — using fast multi-row insert (method='multi').")

                # Use safe_to_sql for top-level chunk as well
                safe_to_sql(chunk, engine, table_name, if_exists_mode, dtype_mapping if i == 0 else None, PARAM_THRESHOLD)

                total_loaded += len(chunk)
                logger.info(f"✓ Progress: {total_loaded:,}/{len(df):,} records ({total_loaded / len(df) * 100:.1f}%)")
        except Exception as inner_e:
            logger.error("\n" + "="*60)
            logger.error("✗ ERROR DURING DATA LOAD (chunk-level)")
            logger.error("="*60)
            logger.error(f"Error message: {inner_e}")
            logger.error(f"Error type: {type(inner_e).__name__}")
            logger.error(f"Problematic chunk range: rows {i} to {i + chunk_size}")
            logger.error("\nDataframe info:")
            logger.error(f"  Columns: {df.columns.tolist()}")
            logger.error(f"  Data types: {df.dtypes.to_dict()}")
            logger.error("\nProblematic chunk sample (first 3 rows):")
            logger.error(chunk.head(3))
            logger.error("\nProblematic chunk data types:")
            logger.error(chunk.dtypes)
            logger.error("\nChecking for problematic values in chunk:")
            for col in chunk.columns:
                problematic = chunk[col].apply(lambda x: x is not None and not isinstance(x, (int, float, str, bool, type(None))))
                if problematic.any():
                    logger.error(f"  ✗ Column '{col}' has problematic values:")
                    logger.error(f"    {chunk[col][problematic].head()}")
            logger.error("="*60)
            raise

    logger.info(f"\n{'=' * 60}")
    logger.info(f"✓ Import complete!")
    logger.info(f"{'=' * 60}")
    logger.info(f"Total records loaded: {total_loaded:,}")
    logger.info(f"Table name: {table_name}")
    logger.info(f"Columns: {len(df.columns)}")
    logger.info(f"Column names: {', '.join(df.columns.tolist())}")
    logger.info(f"{'=' * 60}")

except Exception as e:
    logger.error("\n" + "="*60)
    logger.error("✗ ERROR DURING DATA LOAD")
    logger.error("="*60)
    logger.error(f"Error message: {e}")
    logger.error(f"\nError type: {type(e).__name__}")
    raise


