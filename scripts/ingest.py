"""
Ingest all CSV files into MySQL in high-performance chunks with real-time progress.
Each CSV becomes a table named after the file (without '.csv').
Skips already-completed tables unless --force is provided.
"""

import os
import sys
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, URL, inspect, text
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

connection_url = URL.create(
    drivername="mysql+pymysql",
    username=MYSQL_USER,
    password=MYSQL_PASSWORD,
    host=MYSQL_HOST,
    database=MYSQL_DB
)
engine = create_engine(connection_url)


def get_table_row_count(table_name: str, engine) -> int:
    """Return number of rows in table, or 0 if table does not exist."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
            return int(result) if result is not None else 0
    except Exception:
        return 0


def ingest_file_in_chunks(
    file_path: str,
    table_name: str,
    engine,
    read_chunksize: int = 100000,
    sql_batch_size: int = 5000
) -> None:
    """Ingest a CSV into MySQL using chunked streaming and multi-value insert."""
    start_time = time.time()
    total_rows = 0
    first_chunk = True

    logging.info(f"Starting ingestion of '{file_path}' into table '{table_name}'...")

    for chunk in pd.read_csv(file_path, chunksize=read_chunksize, low_memory=False):
        mode = "replace" if first_chunk else "append"
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists=mode,
            index=False,
            method="multi",
            chunksize=sql_batch_size
        )
        total_rows += len(chunk)
        first_chunk = False

        elapsed = time.time() - start_time
        rate = total_rows / elapsed if elapsed > 0 else 0
        logging.info(f"[{table_name}] Ingested {total_rows:,} rows ({rate:,.0f} rows/s)...")

    total_time = (time.time() - start_time) / 60
    logging.info(f"Table '{table_name}' complete! Total: {total_rows:,} rows in {total_time:.2f} mins.")


def load_raw_data(data_dir: str = "data/raw" if os.path.exists("data/raw") else "data", force: bool = False) -> None:
    """Load CSV files and ingest them into MySQL, skipping already-completed tables."""
    start_time = time.time()
    files = [f for f in sorted(os.listdir(data_dir)) if f.endswith(".csv")]

    logging.info(f"Found {len(files)} CSV files in '{data_dir}'. Force overwrite = {force}")

    for file in files:
        file_path = os.path.join(data_dir, file)
        table_name = file[:-4]

        # Check existing row count if not forcing overwrite
        existing_rows = get_table_row_count(table_name, engine)
        if existing_rows > 0 and not force:
            # Special check for sales: if it only has test rows (< 1,000,000), re-ingest
            if table_name == "sales" and existing_rows < 1000000:
                logging.info(f"Table 'sales' has only {existing_rows:,} rows (incomplete). Re-ingesting...")
            else:
                logging.info(f"Table '{table_name}' already populated ({existing_rows:,} rows). Skipping. (Use --force to re-ingest)")
                continue

        ingest_file_in_chunks(file_path, table_name, engine)

    total_elapsed = (time.time() - start_time) / 60
    logging.info(f"--- All Ingestions Complete in {total_elapsed:.2f} minutes ---")


if __name__ == "__main__":
    force_run = "--force" in sys.argv or "-f" in sys.argv
    load_raw_data(force=force_run)
