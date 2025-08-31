import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

USER = os.getenv("MYSQL_USER")
PASS = os.getenv("MYSQL_PASSWORD")
HOST = os.getenv("MYSQL_HOST")
DB   = os.getenv("MYSQL_DB")

# Create engine
engine = create_engine(f"mysql+pymysql://{USER}:{PASS}@{HOST}/{DB}")

def ingest_db(df, table_name, engine):
    """Ingest a dataframe into MySQL"""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logging.info(f"Table {table_name} inserted into MySQL")

def load_raw_data():
    """Load CSVs and ingest into MySQL"""
    start = time.time()
    
    for file in os.listdir("data/raw"):   # <-- assuming CSVs are in data/raw/
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join("data/raw", file))
            table_name = file[:-4]  # remove ".csv" from name
            logging.info(f"Ingesting {file} into table {table_name}...")
            ingest_db(df, table_name, engine)
    
    end = time.time()
    logging.info(f"--- Ingestion Complete in {(end-start)/60:.2f} minutes ---")

if __name__ == "__main__":
    load_raw_data()
