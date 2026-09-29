# Power BI Measures and Page Specification

Use explicit measures; do not place raw margin percentage columns on cards.

```DAX
Total Sales = SUM(VendorBrand[SalesDollars])
Procurement Spend = SUM(VendorSpend[PurchaseDollars])
Matched Sales = SUMX(FILTER(VendorBrand, VendorBrand[CostMatched]), VendorBrand[SalesDollars])
Estimated COGS = SUMX(FILTER(VendorBrand, VendorBrand[CostMatched]), VendorBrand[EstimatedCOGS])
Estimated Gross Profit = [Matched Sales] - [Estimated COGS]
Weighted Gross Margin % = DIVIDE([Estimated Gross Profit], [Matched Sales])
Purchase Cost Coverage % = DIVIDE([Matched Sales], [Total Sales])

Top 10 Spend =
SUMX(
    TOPN(10, VALUES(VendorSpend[VendorName]), [Procurement Spend], DESC),
    [Procurement Spend]
)

Top 10 Spend Concentration % = DIVIDE([Top 10 Spend], [Procurement Spend])
Ending Inventory Retail Value = SUM(InventoryRisk[EndingRetailValue])
Sell Through % = DIVIDE(SUM(InventoryRisk[SalesUnits]), SUM(InventoryRisk[BeginningUnits]) + SUM(InventoryRisk[PurchaseUnits]))
Inventory Turnover = DIVIDE(SUM(InventoryRisk[SalesUnits]), DIVIDE(SUM(InventoryRisk[BeginningUnits]) + SUM(InventoryRisk[EndingUnits]), 2))
```

`VendorSpend` must be a separate vendor-level aggregation from `purchases`; summing spend from the sales-grain `VendorBrand` table would omit purchase-only combinations. `CostMatched` is true only when a vendor-brand has a weighted purchase cost.

## Page 1 - Executive Overview

- KPI cards: Total Sales, Procurement Spend, Estimated Gross Profit, Weighted Gross Margin %, Top 10 Spend Concentration %.
- Pareto chart: vendor procurement spend with cumulative share.
- Vendor quadrant: sales vs weighted margin; bubble size is estimated gross profit.
- Brand opportunity chart: sales vs weighted margin, highlighting bottom-sales/top-margin quartile.
- Vendor and brand slicers; footer notes the 2024 period and estimated-COGS definition.

## Page 2 - Vendor & Inventory Diagnostics

- Vendor scorecard matrix with sales, spend, profit, margin, share, and rank.
- Ending inventory retail value by brand.
- Sell-through vs ending value risk scatter.
- Inventory turnover table with conditional formatting.
- Brand and city filters; do not imply that ending store inventory is attributable to a vendor.
