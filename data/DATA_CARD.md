# 📋 Vendor Performance Data Card

> **Domain:** Retail purchasing, sales, and inventory management  
> **Time Period:** 2024-01-01 to 2024-12-31 (full calendar year)  
> **Scale:** ~15.6 million rows across 6 CSV files  
> **Granularity:** Store–Brand–Date level transactions and inventory snapshots

---

## Dataset Overview

| File | Records | Description |
|------|--------:|-------------|
| `sales.csv` | 12,825,364 | Daily sales transactions by product, store, and brand |
| `purchases.csv` | 2,372,475 | Purchase order receipts and vendor procurement records |
| `end_inventory.csv` | 224,490 | Inventory on-hand snapshot at 2024-12-31 |
| `begin_inventory.csv` | 206,530 | Inventory on-hand snapshot at 2024-01-01 |
| `purchase_prices.csv` | 12,262 | Product catalog with pricing and vendor mapping |
| `vendor_invoice.csv` | 5,544 | Vendor invoice and freight summaries |

---

## Table Schemas

### `sales.csv` — Transaction-level Sales Data

| Column | Type | Description |
|--------|------|-------------|
| InventoryId | VARCHAR | Composite key (`Store_City_Brand`) |
| Store | INT | Store number identifier |
| Brand | VARCHAR | Brand code/identifier |
| Description | VARCHAR | Product description |
| Size | VARCHAR | Product size (e.g., 750mL, 1.75L) |
| SalesQuantity | INT | Units sold in the transaction |
| SalesDollars | DECIMAL | Total transaction revenue |
| SalesPrice | DECIMAL | Unit price at sale |
| SalesDate | DATE | Transaction date |
| Volume | DECIMAL | Volume in mL |
| Classification | INT | Product classification code |
| ExciseTax | DECIMAL | Excise tax amount |
| VendorNo | INT | Vendor number |
| VendorName | VARCHAR | Vendor name (right-padded) |

### `purchases.csv` — Product Procurement Records

| Column | Type | Description |
|--------|------|-------------|
| InventoryId | VARCHAR | Composite key (`Store_City_Brand`) |
| Store | INT | Store number |
| Brand | VARCHAR | Brand code |
| Description | VARCHAR | Product description |
| Size | VARCHAR | Product size |
| VendorNumber | INT | Vendor identifier |
| VendorName | VARCHAR | Vendor name (right-padded) |
| PONumber | INT | Purchase order number |
| PODate | DATE | Purchase order date |
| ReceivingDate | DATE | Date goods received |
| InvoiceDate | DATE | Invoice date |
| PayDate | DATE | Payment date |
| PurchasePrice | DECIMAL | Unit purchase cost |
| Quantity | INT | Units purchased |
| Dollars | DECIMAL | Total purchase cost |
| Classification | INT | Product classification |

### `begin_inventory.csv` / `end_inventory.csv` — Inventory Snapshots

| Column | Type | Description |
|--------|------|-------------|
| InventoryId | VARCHAR | Composite key (`Store_City_Brand`) |
| Store | INT | Store number |
| City | VARCHAR | Store city location |
| Brand | VARCHAR | Brand code |
| Description | VARCHAR | Product description |
| Size | VARCHAR | Product size |
| onHand | INT | Units in stock |
| Price | DECIMAL | Retail price |
| startDate / endDate | DATE | Snapshot date |

### `vendor_invoice.csv` — Vendor Invoice Summary

| Column | Type | Description |
|--------|------|-------------|
| VendorNumber | INT | Vendor identifier |
| VendorName | VARCHAR | Vendor name (right-padded) |
| InvoiceDate | DATE | Invoice date |
| PONumber | INT | Purchase order number |
| PODate | DATE | Purchase order date |
| PayDate | DATE | Payment date |
| Quantity | INT | Total units on invoice |
| Dollars | DECIMAL | Invoice amount |
| Freight | DECIMAL | Freight/shipping costs |
| Approval | VARCHAR | Approval status |

### `purchase_prices.csv` — Product Master Reference

| Column | Type | Description |
|--------|------|-------------|
| Brand | VARCHAR | Brand code (primary key) |
| Description | VARCHAR | Product description |
| Price | DECIMAL | Retail price |
| Size | VARCHAR | Product size |
| Volume | INT | Volume in mL |
| Classification | INT | Product classification |
| PurchasePrice | DECIMAL | Purchase cost per unit |
| VendorNumber | INT | Primary vendor |
| VendorName | VARCHAR | Vendor name |

---

## Data Relationships

**Primary join paths:**

```
sales.VendorNo ──────→ purchases.VendorNumber ──────→ vendor_invoice.VendorNumber
sales.Brand ──────────→ purchase_prices.Brand
sales.InventoryId ────→ begin_inventory.InventoryId ──→ end_inventory.InventoryId
```

**Composite key pattern:**
```
InventoryId = {Store}_{City}_{Brand}
Example:     1_HARDERSFIELD_1004
```

**Inventory equation (reconciled at portfolio level):**
```
EndingUnits = BeginningUnits + PurchaseUnits − SalesUnits
```

---

## Business Definitions

| Metric | Definition |
|--------|-----------|
| **Estimated COGS** | Sales Quantity × Weighted Purchase Cost (same vendor–brand) |
| **Weighted Gross Margin** | (Total Sales − Total Estimated COGS) / Total Sales × 100% |
| **Sell-through Rate** | Sales Units / (Beginning Inventory + Purchased Units) |
| **Inventory Turnover** | Sales Units / Average(Beginning Units, Ending Units) |
| **Cost Matching** | 99.90% of sales have matched purchase costs; the unmatched 0.10% is excluded |

---

## Data Quality Notes

- **Vendor names** are right-padded with spaces — use `TRIM()` when querying
- **Division by zero** — use `NULLIF(denominator, 0)` to prevent errors
- **Sales prices** can vary by date due to promotions and markdowns
- **Purchase prices** are aggregated using weighted average by vendor–brand
- **Inventory snapshots** are point-in-time (start and end of year only)

---

## Executive KPIs (from Analysis)

| KPI | Value |
|-----|------:|
| Total Sales | $452.06M |
| Procurement Spend | $321.90M |
| Estimated Gross Profit | $138.47M |
| Weighted Gross Margin | 30.66% |
| Top-10 Vendor Concentration | 65.33% |
| Ending Inventory (Retail) | $79.70M |
