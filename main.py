import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Read CSV with proper data types
df = pd.read_csv('us_state_businesses.csv', 
                 dtype={
                     'zip_code': str,
                     'phone': str,
                     'fein': str  # Federal Employer ID
                 },
                 parse_dates=['registration_date'],
                 low_memory=False)  # For large files

# Data Cleaning (important for business data!)
df['state'] = df['state'].str. upper().str.strip()
df['zip_code'] = df['zip_code'].str[: 5]. str.zfill(5)  # Keep first 5 digits
df['business_name'] = df['business_name'].str.strip().str.title()
df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)  # Remove non-digits

# Handle nulls
df = df.replace({np.nan: None})

# Validate states (optional but recommended)
valid_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC']

invalid_states = df[~df['state'].isin(valid_states)]
if len(invalid_states) > 0:
    print(f"Warning: {len(invalid_states)} records with invalid states")
    df = df[df['state'].isin(valid_states)]

# Database connection (Azure SQL)
connection_string = (
    "mssql+pyodbc://your-server.database.windows.net:1433/"
    "your-database?driver=ODBC+Driver+17+for+SQL+Server"
    "&uid=username&pwd=password"
)
engine = create_engine(connection_string)

# Load in chunks (better for large files)
chunk_size = 10000
for i in range(0, len(df), chunk_size):
    chunk = df[i:i+chunk_size]
    chunk.to_sql('business_data', 
                 engine, 
                 if_exists='append',  # Use 'replace' for first run
                 index=False,
                 method='multi')  # Faster batch insert
    print(f"Loaded {i+chunk_size}/{len(df)} records")

print("Import complete!")