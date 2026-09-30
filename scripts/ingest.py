"""
Ingest all CSV files into the SQLite database with real-time progress.
Each CSV becomes a table named after the file (without '.csv').
Skips already-completed tables unless --force is provided.
"""

import sys
import time
import pandas as pd
from sqlalchemy import text

from scripts.config import get_engine, setup_logging, DATA_DIR

log = setup_logging(__name__)
engine = get_engine()


def get_table_row_count(table_name: str, engine) -> int:
    """Return number of rows in table, or 0 if table does not exist."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f'SELECT COUNT(*) FROM "{table_name}"')
            ).scalar()
            return int(result) if result is not None else 0
    except Exception:
        return 0


def ingest_file_in_chunks(
    file_path: str,
    table_name: str,
    engine,
    read_chunksize: int = 100_000,
    sql_batch_size: int = 5_000,
) -> None:
    """Ingest a CSV into SQLite using chunked streaming."""
    start_time = time.time()
    total_rows = 0
    first_chunk = True

    log.info(f"Starting ingestion of '{file_path}' into table '{table_name}'...")

    for chunk in pd.read_csv(file_path, chunksize=read_chunksize, low_memory=False):
        mode = "replace" if first_chunk else "append"
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists=mode,
            index=False,
            chunksize=sql_batch_size,
        )
        total_rows += len(chunk)
        first_chunk = False

        elapsed = time.time() - start_time
        rate = total_rows / elapsed if elapsed > 0 else 0
        log.info(f"[{table_name}] Ingested {total_rows:,} rows ({rate:,.0f} rows/s)...")

    total_time = (time.time() - start_time) / 60
    log.info(f"Table '{table_name}' complete! Total: {total_rows:,} rows in {total_time:.2f} mins.")


def load_raw_data(force: bool = False) -> None:
    """Load CSV files and ingest them into SQLite, skipping already-completed tables."""
    data_dir = DATA_DIR / "raw" if (DATA_DIR / "raw").exists() else DATA_DIR
    start_time = time.time()
    files = sorted(f for f in data_dir.iterdir() if f.suffix == ".csv")

    log.info(f"Found {len(files)} CSV files in '{data_dir}'. Force overwrite = {force}")

    for file_path in files:
        table_name = file_path.stem

        # Check existing row count if not forcing overwrite
        existing_rows = get_table_row_count(table_name, engine)
        if existing_rows > 0 and not force:
            # Special check for sales: if it only has test rows (< 1,000,000), re-ingest
            if table_name == "sales" and existing_rows < 1_000_000:
                log.info(f"Table 'sales' has only {existing_rows:,} rows (incomplete). Re-ingesting...")
            else:
                log.info(
                    f"Table '{table_name}' already populated ({existing_rows:,} rows). "
                    "Skipping. (Use --force to re-ingest)"
                )
                continue

        ingest_file_in_chunks(str(file_path), table_name, engine)

    total_elapsed = (time.time() - start_time) / 60
    log.info(f"--- All Ingestions Complete in {total_elapsed:.2f} minutes ---")


if __name__ == "__main__":
    force_run = "--force" in sys.argv or "-f" in sys.argv
    load_raw_data(force=force_run)
