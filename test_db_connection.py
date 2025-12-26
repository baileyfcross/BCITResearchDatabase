import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Get database credentials
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD')

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        print("Successfully connected to PostgresSQL Database")
        print()

except Exception as e:
    print(f"Connection failed!")
    print()
    print(f"Error details: {e}")
    print()