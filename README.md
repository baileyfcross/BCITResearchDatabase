# BCIT Research Database Importer

A small Python tool to convert Excel files into PostgreSQL tables and make it easier to create data visualizations for BCIT, a small information technology consulting company.

This project reads an Excel workbook where the third row contains column headers, cleans and normalizes column names, infers SQL types, and uploads the data into a PostgreSQL database in chunks. The goal is to make repeated imports reliable and to reduce manual pre-processing before visualization and analysis.

Key features

- Read Excel files using the third row (row index 2) as column headers. Can be changed in the code at pd.read_excel function.
- Clean and normalize column names for SQL compatibility
- Heuristic based inference of SQL column types
- Chunked uploads to avoid large memory and query parameter issues
- Robust write routine with preference for PostgreSQL COPY, fallback to fast batch inserts, and recursive retries on failures
- Optional GUI prompts for file and table name when running without CLI arguments

Good use cases

- Quick ingestion of Excel exports for dashboards and charts
- Reproducible imports for recurring reports
- Preparing datasets for downstream visualization tools

Prerequisites

* Python 3.8 or newer
* PostgreSQL database accessible from your environment
    * Setting up and getting familiar with PgAdmin is recommended

Required Python packages

To install required packages, run:

```bash
python -m pip install -r requirements.txt
```

If you do not have a requirements file, install the main dependencies directly:

```bash
python -m pip install pandas sqlalchemy psycopg2-binary openpyxl python-dotenv
```

Environment variables

Create a `.env` file in the project root with the following variables:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=postgres
DB_PASSWORD=your_password
```

Make sure `DB_NAME` and `DB_PASSWORD` are set. The script will raise an error if these values are missing.

Quick start

1. Place your Excel file in a known location. The script expects the third row of the sheet to contain column names.
2. Either run the script with CLI arguments or let the GUI prompt you for file and table name.

Example CLI usage

```bash
# Open a file dialog and a table name prompt
python main.py

# Run headless: specify file and table
python main.py --no-gui --file "C:\path\to\file.xlsx" --table my_table

# Control chunk size and force replace the destination table
python main.py --file "C:\path\to\file.xlsx" --table my_table --chunk-size 500 --force-replace
```

Dry run mode

The script supports a safe dry run mode. When you run with `--dry-run` the tool will not connect to your database and will not perform any destructive operations. Instead it will read the Excel file, show cleaned column names, infer SQL types, and print samples and progress for each chunk. This mode is useful to validate parsing and type inference without changing anything in your database.

How to perform a real import

To allow the script to write to your database, run it without `--dry-run` and make sure you have a `.env` file with valid database credentials. For example:

```bash
python main.py --file "C:\path\to\file.xlsx" --table my_table
```

If you use the `--force-replace` option while not in dry run mode the script will drop the target table if it exists before creating the new table.

Notes on dry run and writing behavior

- The `--dry-run` flag prevents any connection to the database and prevents table drops. Use it to check the import plan and data types.
- To perform a real import, omit `--dry-run` and ensure the `.env` file is present with `DB_NAME` and `DB_PASSWORD` set.

How the import works

- The script reads the Excel using pandas with `header=2` so the third row becomes column names.
- Column names are sanitized to be SQL friendly.
- The script infers SQL column types using a set of heuristics that consider date/time, integer ranges, floats, booleans, and text.
- Data is uploaded in chunks. For PostgreSQL the script prefers `COPY FROM STDIN` for speed, then falls back to `psycopg2.extras.execute_values`, and finally to `pandas.DataFrame.to_sql` as a last resort.
- If a write fails for a chunk, the routine splits the chunk and retries until the problem row is isolated and logged.

Troubleshooting

- If you see `integer out of range` errors, that means a numeric column contains values outside of a 32-bit integer range. The script attempts to pick BigInteger where appropriate, but inspect the column values and consider cleaning or truncation.
- If you see warnings about date parsing, the tool uses a permissive date inference. For consistent behavior, normalize date strings in the source Excel or convert columns to ISO date format before import.
- Check the generated CSVs named like `failed_row_<table>_<timestamp>_<idx>.csv` when a single row fails to insert. These files will be in the project working directory.

Development and contributions

- This project is intentionally small and focused. If you want to contribute, open an issue or submit a pull request with tests and a short description of changes.

License

This project uses the MIT License. See the LICENSE file for details.

Contact

For questions about this tool or to request features related to visualization workflows, contact the BCIT team managing this repository.
