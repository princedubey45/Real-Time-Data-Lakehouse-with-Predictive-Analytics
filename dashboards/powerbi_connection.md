# Power BI Dashboard — Connection & Page Guide

## Connection Setup

1. Open **Power BI Desktop**
2. **Get Data → PostgreSQL database**
3. Server: `localhost` | Database: `data_warehouse`
4. Credentials: `warehouse` / `warehouse`

## Tables / Views to Load

| Object               | Dashboard Page       |
|----------------------|----------------------|
| v_daily_revenue      | Revenue Trends       |
| v_monthly_revenue    | Monthly Summary      |
| v_category_revenue   | Category Breakdown   |
| v_top_products       | Product Leaderboard  |
| v_customer_overview  | Customer Segments    |
| v_sales_detail       | Drill-Through        |
| agg_customer_ltv     | Customer LTV         |
| anomaly_log          | Anomaly Monitor      |
| forecast_results     | 30-Day Forecast      |

## Suggested Visuals

### Executive Summary
- KPI Cards: Total Revenue · Total Orders · Avg Order Value
- Line sparkline: 30-day revenue trend

### Revenue Trends
- Line Chart: daily gross vs net revenue (v_daily_revenue)
- Bar Chart: monthly rollup (v_monthly_revenue)

### Product Performance
- Bar: Top 10 products by revenue (v_top_products)
- Treemap: Revenue share by category (v_category_revenue)

### Customer LTV
- Donut: LTV tier split (low / medium / high / vip)
- Scatter: orders vs spend coloured by tier

### Anomaly Monitor
- Table: open anomalies (anomaly_log WHERE resolved=false)
- Gauge: Critical count

### 30-Day Forecast
- Line: actual + predicted + confidence band (forecast_results)

## Refresh Schedule
Daily at **07:00 UTC** (30 min after quality_check DAG).
