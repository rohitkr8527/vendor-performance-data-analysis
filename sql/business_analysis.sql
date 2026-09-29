-- ==============================================================================
-- Vendor Performance Analysis - MySQL 8+
-- Read-only portfolio queries. No database creation or ingestion is performed.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1) Canonical vendor-brand analytical dataset
-- ------------------------------------------------------------------------------
-- This avoids repeating sales when purchase prices change during the year.

WITH SalesSummary AS (
    SELECT
        VendorNo AS VendorNumber,
        MAX(TRIM(VendorName)) AS VendorName,
        Brand,
        MAX(TRIM(Description)) AS Description,
        SUM(SalesQuantity) AS SalesQuantity,
        SUM(SalesDollars) AS SalesDollars
    FROM sales
    GROUP BY VendorNo, Brand
),

PurchaseSummary AS (
    SELECT
        VendorNumber,
        Brand,
        SUM(Quantity) AS PurchaseQuantity,
        SUM(Dollars) AS PurchaseDollars,
        SUM(Dollars) / NULLIF(SUM(Quantity), 0) AS WeightedUnitCost
    FROM purchases
    GROUP BY VendorNumber, Brand
),

FreightSummary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS VendorFreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
)

SELECT
    s.VendorNumber,
    s.VendorName,
    s.Brand,
    s.Description,
    s.SalesQuantity,
    s.SalesDollars,
    COALESCE(p.PurchaseQuantity, 0) AS PurchaseQuantity,
    COALESCE(p.PurchaseDollars, 0) AS PurchaseDollars,
    p.WeightedUnitCost,
    p.WeightedUnitCost IS NOT NULL AS CostMatched,
    s.SalesQuantity * p.WeightedUnitCost AS EstimatedCOGS,
    s.SalesDollars - s.SalesQuantity * p.WeightedUnitCost AS EstimatedGrossProfit,
    100 * (s.SalesDollars - s.SalesQuantity * p.WeightedUnitCost) / NULLIF(s.SalesDollars, 0) AS WeightedMarginPct,
    COALESCE(f.VendorFreightCost, 0) AS VendorFreightCost
FROM SalesSummary s
LEFT JOIN PurchaseSummary p
    ON s.VendorNumber = p.VendorNumber
    AND s.Brand = p.Brand
LEFT JOIN FreightSummary f
    ON s.VendorNumber = f.VendorNumber;

-- ------------------------------------------------------------------------------
-- 2) Vendor scorecard and spend concentration
-- ------------------------------------------------------------------------------

WITH VendorSales AS (
    SELECT
        VendorNo AS VendorNumber,
        MAX(TRIM(VendorName)) AS VendorName,
        SUM(SalesDollars) AS SalesDollars
    FROM sales
    GROUP BY VendorNo
),

VendorPurchases AS (
    SELECT
        VendorNumber,
        MAX(TRIM(VendorName)) AS VendorName,
        SUM(Dollars) AS PurchaseDollars
    FROM purchases
    GROUP BY VendorNumber
),

SalesByBrand AS (
    SELECT
        VendorNo AS VendorNumber,
        Brand,
        SUM(SalesQuantity) AS SalesQuantity,
        SUM(SalesDollars) AS MatchedSalesDollars
    FROM sales
    GROUP BY VendorNo, Brand
),

CostByBrand AS (
    SELECT
        VendorNumber,
        Brand,
        SUM(Dollars) / NULLIF(SUM(Quantity), 0) AS WeightedUnitCost
    FROM purchases
    GROUP BY VendorNumber, Brand
),

VendorCost AS (
    SELECT
        s.VendorNumber,
        SUM(s.MatchedSalesDollars) AS MatchedSalesDollars,
        SUM(s.SalesQuantity * c.WeightedUnitCost) AS EstimatedCOGS
    FROM SalesByBrand s
    JOIN CostByBrand c
        ON s.VendorNumber = c.VendorNumber
        AND s.Brand = c.Brand
    GROUP BY s.VendorNumber
),

VendorMetrics AS (
    SELECT
        p.VendorNumber,
        COALESCE(s.VendorName, p.VendorName) AS VendorName,
        COALESCE(s.SalesDollars, 0) AS SalesDollars,
        p.PurchaseDollars,
        c.MatchedSalesDollars,
        c.EstimatedCOGS
    FROM VendorPurchases p
    LEFT JOIN VendorSales s
        ON p.VendorNumber = s.VendorNumber
    LEFT JOIN VendorCost c
        ON p.VendorNumber = c.VendorNumber
),

Ranked AS (
    SELECT
        *,
        PurchaseDollars / SUM(PurchaseDollars) OVER () AS SpendShare,
        SUM(PurchaseDollars) OVER (ORDER BY PurchaseDollars DESC) / SUM(PurchaseDollars) OVER () AS CumulativeSpendShare,
        ROW_NUMBER() OVER (ORDER BY PurchaseDollars DESC) AS SpendRank
    FROM VendorMetrics
)

SELECT
    VendorNumber,
    VendorName,
    SalesDollars,
    PurchaseDollars,
    MatchedSalesDollars - EstimatedCOGS AS EstimatedGrossProfit,
    100 * (MatchedSalesDollars - EstimatedCOGS) / NULLIF(MatchedSalesDollars, 0) AS WeightedMarginPct,
    100 * SpendShare AS SpendSharePct,
    100 * CumulativeSpendShare AS CumulativeSpendSharePct,
    SpendRank
FROM Ranked
ORDER BY SpendRank;

-- ------------------------------------------------------------------------------
-- 3) Ending inventory risk by brand
-- ------------------------------------------------------------------------------

WITH BeginningInventory AS (
    SELECT
        Brand,
        MAX(TRIM(Description)) AS Description,
        SUM(onHand) AS BeginningUnits
    FROM begin_inventory
    GROUP BY Brand
),

EndingInventory AS (
    SELECT
        Brand,
        MAX(TRIM(Description)) AS Description,
        SUM(onHand) AS EndingUnits,
        SUM(onHand * Price) AS EndingRetailValue
    FROM end_inventory
    GROUP BY Brand
),

SalesMovement AS (
    SELECT
        Brand,
        SUM(SalesQuantity) AS SalesUnits
    FROM sales
    GROUP BY Brand
),

PurchaseMovement AS (
    SELECT
        Brand,
        SUM(Quantity) AS PurchaseUnits
    FROM purchases
    GROUP BY Brand
)

SELECT
    e.Brand,
    e.Description,
    COALESCE(b.BeginningUnits, 0) AS BeginningUnits,
    e.EndingUnits,
    e.EndingRetailValue,
    COALESCE(s.SalesUnits, 0) AS SalesUnits,
    COALESCE(p.PurchaseUnits, 0) AS PurchaseUnits,
    COALESCE(s.SalesUnits, 0) / NULLIF(COALESCE(b.BeginningUnits, 0) + COALESCE(p.PurchaseUnits, 0), 0) AS SellThroughRate,
    COALESCE(s.SalesUnits, 0) / NULLIF((COALESCE(b.BeginningUnits, 0) + e.EndingUnits) / 2, 0) AS InventoryTurnover
FROM EndingInventory e
LEFT JOIN BeginningInventory b
    ON e.Brand = b.Brand
LEFT JOIN SalesMovement s
    ON e.Brand = s.Brand
LEFT JOIN PurchaseMovement p
    ON e.Brand = p.Brand
ORDER BY e.EndingRetailValue DESC;

-- ------------------------------------------------------------------------------
-- 4) Brand opportunity quadrants: bottom-sales quartile and top-margin quartile
-- ------------------------------------------------------------------------------

WITH SalesByBrand AS (
    SELECT
        VendorNo AS VendorNumber,
        Brand,
        MAX(TRIM(Description)) AS Description,
        SUM(SalesQuantity) AS SalesQuantity,
        SUM(SalesDollars) AS SalesDollars
    FROM sales
    GROUP BY VendorNo, Brand
),

CostByBrand AS (
    SELECT
        VendorNumber,
        Brand,
        SUM(Dollars) / NULLIF(SUM(Quantity), 0) AS WeightedUnitCost
    FROM purchases
    GROUP BY VendorNumber, Brand
),

BrandMetrics AS (
    SELECT
        s.Brand,
        MAX(s.Description) AS Description,
        SUM(s.SalesDollars) AS SalesDollars,
        SUM(s.SalesQuantity * c.WeightedUnitCost) AS EstimatedCOGS
    FROM SalesByBrand s
    JOIN CostByBrand c
        ON s.VendorNumber = c.VendorNumber
        AND s.Brand = c.Brand
    GROUP BY s.Brand
),

Scored AS (
    SELECT
        *,
        100 * (SalesDollars - EstimatedCOGS) / NULLIF(SalesDollars, 0) AS WeightedMarginPct,
        NTILE(4) OVER (ORDER BY SalesDollars) AS SalesQuartile,
        NTILE(4) OVER (ORDER BY (SalesDollars - EstimatedCOGS) / NULLIF(SalesDollars, 0)) AS MarginQuartile
    FROM BrandMetrics
)

SELECT *
FROM Scored
WHERE SalesQuartile = 1
    AND MarginQuartile = 4
ORDER BY SalesDollars DESC;

-- ------------------------------------------------------------------------------
-- 5) Portfolio reconciliation and inventory KPIs
-- ------------------------------------------------------------------------------

SELECT
    (SELECT SUM(onHand) FROM begin_inventory) AS BeginningUnits,
    (SELECT SUM(Quantity) FROM purchases) AS PurchasedUnits,
    (SELECT SUM(SalesQuantity) FROM sales) AS SoldUnits,
    (SELECT SUM(onHand) FROM end_inventory) AS EndingUnits,
    (SELECT SUM(onHand * Price) FROM end_inventory) AS EndingInventoryRetailValue;

-- ==============================================================================
-- INTERPRETATION NOTE
-- ==============================================================================
-- Purchase volume and unit cost can be associated because of product mix.
-- Do not call the relationship a bulk discount without matched-product price
-- variation or an experiment.
-- ==============================================================================
