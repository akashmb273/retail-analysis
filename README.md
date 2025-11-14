# Retail Sales Analysis Pipeline

This project contains a simple, end-to-end data cleaning and analysis pipeline for retail transaction data.

The main script is `retail_pipeline.py`, which reads a raw CSV file, cleans and enriches it, generates summary tables and basic visualizations, and writes all results to an outputs folder.

## Dataset

- Expected input file: `raw_retail.csv` (in the project root).
- Typical columns (case / spacing does not matter; the script standardizes these):
  - InvoiceNo
  - InvoiceDate
  - StockCode
  - Description
  - Quantity
  - UnitPrice
  - CustomerID
  - Country

## Pipeline Steps

`retail_pipeline.py` performs the following steps:

1. **Load raw CSV**
   - Reads `raw_retail.csv` (or a custom path) using pandas.

2. **Standardize column names**
   - Lowercases, replaces spaces/hyphens with `_`, and harmonizes common column variants to:
     - `invoice_no`, `invoice_date`, `stock_code`, `description`, `quantity`, `unit_price`, `customer_id`, `country`.

3. **Convert data types**
   - `invoice_date` → datetime.
   - `quantity`, `unit_price` → numeric.

4. **Remove invalid records**
   - Drops rows with missing `invoice_no` or `invoice_date`.

5. **Handle duplicates and missing values**
   - Logs all duplicate rows to `outputs/duplicates_logged.csv` and removes them from the cleaned data.
   - Fills missing `description` with `"Unknown"`.

6. **Normalize categorical values**
   - Trims and standardizes country names (e.g., `Uk` → `United Kingdom`, `Eire` → `Ireland`).

7. **Create derived columns**
   - `total_value` = `quantity * unit_price`.
   - `year`, `month` from `invoice_date`.

8. **Outlier detection (quantity & price)**
   - Uses IQR (interquartile range) to flag outliers in `quantity` and `unit_price`.
   - Adds boolean columns:
     - `is_outlier_quantity`
     - `is_outlier_unit_price`
   - Logs all outlier rows to `outputs/outliers_logged.csv`.
   - If the `--remove-outliers` flag is used, outlier rows are removed from the final cleaned dataset.

9. **Summary tables**
   - `outputs/revenue_by_month.csv`
     - Total revenue (`total_value`) grouped by `year` and `month`.
   - `outputs/top10_products_by_sales.csv`
     - Top 10 products by total sales value.
   - `outputs/overall_metrics.csv`
     - Overall metrics (e.g., unique customers, average order value).

10. **Visualizations**
    - Saved under `outputs/figures/`:
      - `monthly_revenue.png` – line chart of monthly revenue.
      - `top10_products_by_sales.png` – bar chart of the top 10 products by sales.
      - `quantity_distribution.png` – histogram of quantity distribution.

## How to Run

From the project root (`d:\Python projects\DA_project\Retail`), run:

```bash
python retail_pipeline.py --input raw_retail.csv --output outputs --remove-outliers
```

Arguments:

- `--input` / `-i` (optional)
  - Path to the raw CSV file.
  - Default: `raw_retail.csv`.
- `--output` / `-o` (optional)
  - Directory where cleaned data, logs, summaries, and figures are saved.
  - Default: `outputs`.
- `--remove-outliers` (optional flag)
  - If provided, rows flagged as quantity or unit price outliers are removed from the final cleaned dataset (but still logged).

Examples:

```bash
# Use defaults (input: raw_retail.csv, output: outputs)
python retail_pipeline.py

# Custom input and output directory
python retail_pipeline.py --input data/my_raw_retail.csv --output results

# Run without removing outliers (only flag & log them)
python retail_pipeline.py --input raw_retail.csv --output outputs
```

## Outputs

After running the pipeline, you should see:

- `outputs/retail_cleaned.csv` – cleaned and enriched dataset.
- `outputs/duplicates_logged.csv` – duplicate rows detected (if any).
- `outputs/outliers_logged.csv` – outlier rows detected (if any).
- `outputs/revenue_by_month.csv` – monthly revenue summary.
- `outputs/top10_products_by_sales.csv` – top 10 products by total sales.
- `outputs/overall_metrics.csv` – overall metrics.
- `outputs/figures/` – PNG charts.

## Git & GitHub

Key files tracked in this repository:

- `app.py` – original analysis / exploration script.
- `retail_pipeline.py` – reusable cleaning and analysis pipeline.
- `.gitignore` – ignores large/raw data and generated outputs.
- `README.md` – this documentation.

Raw data (`raw_retail.csv`) and generated outputs (`outputs/`) are intentionally ignored by Git to keep the repository light.
