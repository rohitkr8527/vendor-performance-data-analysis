"""
Ingest all CSV files from 'data/raw' into a MySQL database.
Each CSV becomes a table named after the file (without '.csv').
"""

import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DB = os.getenv("MYSQL_DB")

if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DB]):
    logging.error("One or more MySQL environment variables are missing.")
    raise ValueError("Check your .env file for MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DB.")

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}")

def ingest_to_db(df: pd.DataFrame, table_name: str, engine) -> None:
    """Ingest a pandas DataFrame into a MySQL table."""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logging.info(f"Table '{table_name}' successfully ingested into MySQL.")

def load_raw_data(data_dir: str = "data/raw") -> None:
    """Load all CSV files from a directory and ingest them into MySQL."""
    start_time = time.time()

    for file in os.listdir(data_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(data_dir, file)
            logging.info(f"Ingesting '{file}'...")
            df = pd.read_csv(file_path)
            table_name = file[:-4]  # remove ".csv"
            ingest_to_db(df, table_name, engine)

    elapsed = (time.time() - start_time) / 60
    logging.info(f"--- Ingestion Complete in {elapsed:.2f} minutes ---")

if __name__ == "__main__":
    load_raw_data()
