# Vendor Performance — Executive Summary

> **Audience:** Procurement and merchandising leadership
> **Period:** Fiscal 2024 operating year
> **Data:** MySQL sales, purchases, invoices, prices, and inventory snapshots
> **Prepared by:** Rohit (rohitkr8527) · September 2026
> **Tools:** MySQL 8 · Python · Power BI · LaTeX

---

## Decision Statement

The portfolio generates **$452.06M in sales** and **$138.47M in estimated gross profit** at a **30.66% weighted margin**. Purchasing is heavily concentrated among a small vendor group, and year-end inventory represents **$79.70M at retail**. The near-term priorities are managing concentration risk, protecting weighted margin, and focusing inventory action on the brands with the most value and weakest sell-through movement.

---

## KPI Scorecard

| KPI | Result | Status | Definition |
|---|---:|:---:|---|
| Total sales | $452.06M | ✅ | Sales revenue from MySQL transaction data |
| Procurement spend | $321.90M | ℹ️ | Received purchase dollars |
| Estimated gross profit | $138.47M | ✅ | Cost-matched sales less estimated COGS |
| Weighted gross margin | 30.66% | ✅ | Estimated gross profit ÷ cost-matched sales |
| Purchase-cost coverage | 99.90% | ✅ | Share of sales with a vendor-brand purchase cost |
| Top-10 vendor concentration | 65.33% | ⚠️ | Share of total procurement spend |
| Ending inventory at retail | $79.70M | ⚠️ | Ending units × retail price |

> **Note:** ✅ favourable / ℹ️ informational / ⚠️ requires active management attention

---

## Analytical Sequence

The work follows a deliberate **Data Analyst → Business Analyst** progression:

1. **Data Understanding** — Confirm source roles; establish a safe vendor-brand analytical grain
2. **Exploratory Analysis** — Profile distributions, monthly trends, leading vendors, outliers, and correlations
3. **Business Analysis** — Evaluate supplier concentration, promotion candidates, inventory exposure, and statistical claims
4. **Decision Design** — Assign owners, timing, and success measures through a 30/60/90-day action plan

---

## What Management Should Do

| Timing | Owner | Action | Success Measure |
|---|---|---|---|
| **0–30 days** | Procurement | Review the top ten vendors; document alternate sources, lead times, and contract coverage | Continuity records complete; concentration threshold added to scorecard |
| **0–30 days** | Merchandising | Freeze or reduce replenishment for highest-value brands with weak sell-through | Ending retail value declines without avoidable stockouts |
| **31–60 days** | Category Management | Run controlled promotion pilots on 10–15 bottom-sales / top-margin brands with margin guardrails | Incremental units and margin both improve vs. baseline |
| **31–60 days** | Finance + Procurement | Review vendor freight rate once per vendor; do not allocate the full freight total across every brand | Freight-to-spend variance explained with no double-counting |
| **61–90 days** | Analytics | Re-score vendors and brands after interventions using the same methodology | KPIs measured against the current baseline with variance explained |

---

## Exploratory Findings

- **Right-skewed distributions:** Vendor-brand sales and purchase values are strongly skewed; medians and percentile bands are reported alongside means to avoid misleading averages.
- **Seasonal peaks:** Monthly sales peak at ~$49.70M in July and $52.31M in December; procurement peaks earlier in several periods, consistent with pre-season inventory build.
- **Outlier treatment:** IQR flags are retained for review — large commercial observations are often decision-relevant rather than erroneous; removing them would erase key portfolio drivers.
- **Correlations are descriptive:** Spearman correlations between commercial metrics are treated as descriptive associations only, not causal evidence.
- **Concentration risk:** DIAGEO NORTH AMERICA INC alone accounts for ~$50.96M (15.83%) of total procurement spend; disruption to any top-ten vendor carries material financial and service consequences.
- **Promotion pool:** 485 brands sit in the bottom sales quartile and top weighted-margin quartile — a prioritised starting list for controlled pilots, not a blanket discount directive.

---

## Monthly Decision Scorecard Dimensions

| Domain | Metrics to Track |
|---|---|
| **Supplier** | Procurement spend, top-10 concentration %, continuity coverage |
| **Commercial** | Sales, estimated gross profit, weighted margin %, cost coverage % |
| **Inventory** | Ending retail value, sell-through rate, inventory turnover, stockout exceptions |
| **Experiments** | Promotion lift, incremental margin dollars, post-test sell-through |

---

## Analytical Corrections Made

| Item | Correction |
|---|---|
| Margin methodology | Revenue-weighted; not the mean or sum of row-level percentages |
| Sales aggregation | Aggregated once per vendor-brand before joining to purchases |
| COGS estimation | Units sold × weighted purchase cost (not retail-based approximation) |
| Inventory exposure | From the actual ending inventory snapshot (not estimated from sales) |
| 72% bulk-savings claim | Retired — cross-product averages conflate quantity discount with product mix, package size, and supplier terms |
| Statistical tests | Vendor-level Welch's t-test ($t = 0.89$, $p = 0.3779$, Cohen's $d = 0.23$) — no significant margin difference between high- and low-sales vendor groups |

---

## Limitations

- **Estimated COGS** is a management-analysis proxy, not an audited accounting measure; it is units sold multiplied by weighted purchase cost at the vendor-brand level.
- **Inventory value** is stated at retail, not cost — the $79.70M figure overstates cash-at-risk relative to procurement spend.
- **Reconciliation** is exact at the portfolio level but does not prove record-level transaction lineage.
- **Causality** — all observed associations are descriptive; none should be presented as proven causal savings or effects without a designed experiment.
- **Recommendations are pilots** — all proposed actions should be monitored against the baseline rather than treated as guaranteed financial outcomes.

---

*Notebook sequence: `01_data_quality_and_exploratory_analysis.ipynb` (Data Analyst evidence) → `02_vendor_performance_business_analysis.ipynb` (Business Analyst decisions)*
