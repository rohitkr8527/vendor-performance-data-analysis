"""
Generate and ingest vendor sales summary into SQLite.

This script merges purchase, sales, and freight data to create an overall
vendor summary. It cleans the data, handles infinite values, computes additional metrics,
and writes the result into the 'vendor_sales_summary' table.
"""

import numpy as np
import pandas as pd

from scripts.config import get_engine, setup_logging

log = setup_logging(__name__, log_file="get_vendor_summary.log")
engine = get_engine()


def ingest_db(df: pd.DataFrame, table_name: str, engine) -> None:
    """Write a DataFrame into a SQLite table, replacing it if it already exists."""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    log.info(f"Table '{table_name}' ({len(df):,} rows) successfully written to SQLite.")


def create_vendor_summary(engine) -> pd.DataFrame:
    """Merge tables in SQLite to generate vendor summary."""
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
    log.info("Executing vendor summary SQL query on SQLite...")
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

    # Handle infinite values from division by zero
    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    return df


if __name__ == '__main__':
    log.info('Creating Vendor Summary Table from SQLite...')
    summary_df = create_vendor_summary(engine)
    log.info(f"Query returned {len(summary_df):,} records.")

    log.info('Cleaning Data and computing KPI metrics...')
    clean_df = clean_data(summary_df)

    log.info('Ingesting vendor_sales_summary into SQLite...')
    ingest_db(clean_df, 'vendor_sales_summary', engine)
    log.info('All completed successfully.')