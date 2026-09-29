"""
Generate and ingest vendor sales summary into MySQL.

This script merges purchase, sales, and freight data from MySQL to create an overall
vendor summary. It cleans the data, handles infinite values, computes additional metrics,
and ingests the result into the 'vendor_sales_summary' MySQL table.
"""

import os
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, URL
from dotenv import load_dotenv

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Also log to console so the user sees progress in terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console_handler)

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


def ingest_db(df: pd.DataFrame, table_name: str, engine) -> None:
    """Ingest a DataFrame into a MySQL table using multi-row batch insert."""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False, method="multi", chunksize=5000)
    logging.info(f"Table '{table_name}' ({len(df):,} rows) successfully ingested into MySQL.")


def create_vendor_summary(engine) -> pd.DataFrame:
    """Merge tables in MySQL to generate vendor summary."""
    query = """
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost 
        FROM vendor_invoice 
        GROUP BY VendorNumber
    ), 

    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ), 

    SalesSummary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    ) 

    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss 
        ON ps.VendorNumber = ss.VendorNo 
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs 
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    logging.info("Executing vendor summary SQL query on MySQL...")
    return pd.read_sql_query(query, engine)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the vendor summary and create additional analysis columns."""
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    # Handle infinite values from division by zero for MySQL compatibility
    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    return df


if __name__ == '__main__':
    logging.info('Creating Vendor Summary Table from MySQL...')
    summary_df = create_vendor_summary(engine)
    logging.info(f"Query returned {len(summary_df):,} records.")

    logging.info('Cleaning Data and computing KPI metrics...')
    clean_df = clean_data(summary_df)

    logging.info('Ingesting vendor_sales_summary into MySQL...')
    ingest_db(clean_df, 'vendor_sales_summary', engine)
    logging.info('All completed successfully.')