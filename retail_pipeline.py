import os
import argparse
from typing import Tuple, Dict

import pandas as pd
import matplotlib.pyplot as plt


RAW_DEFAULT = "raw_retail.csv"
OUTPUT_DIR_DEFAULT = "outputs"


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {}
    for c in df.columns:
        normalized = c.strip().lower().replace(" ", "_")
        normalized = normalized.replace("-", "_")
        col_map[c] = normalized
    df = df.rename(columns=col_map)

    # Additional harmonization for common retail dataset variants
    rename_map = {}
    cols_lower = {c.lower(): c for c in df.columns}

    # Invoice number
    for candidate in ["invoiceno", "invoice_no", "invoice_number"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "invoice_no"
            break

    # Invoice date
    for candidate in ["invoicedate", "invoice_date", "date"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "invoice_date"
            break

    # Stock code / product code
    for candidate in ["stockcode", "stock_code", "product_code"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "stock_code"
            break

    # Description
    for candidate in ["description", "product_description", "item_description"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "description"
            break

    # Quantity
    for candidate in ["quantity", "qty"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "quantity"
            break

    # Unit price
    for candidate in ["unitprice", "unit_price", "price"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "unit_price"
            break

    # Customer ID
    for candidate in ["customerid", "customer_id", "cust_id"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "customer_id"
            break

    # Country
    for candidate in ["country", "market"]:
        if candidate in cols_lower:
            rename_map[cols_lower[candidate]] = "country"
            break

    df = df.rename(columns=rename_map)
    return df


def convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if "invoice_date" in df.columns:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    if "unit_price" in df.columns:
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    return df


def drop_invalid_invoices(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_check = []
    if "invoice_no" in df.columns:
        cols_to_check.append("invoice_no")
    if "invoice_date" in df.columns:
        cols_to_check.append("invoice_date")

    if cols_to_check:
        df = df.dropna(subset=cols_to_check)

    return df


def deduplicate_and_log(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    duplicates = df[df.duplicated(keep=False)]
    if not duplicates.empty:
        dup_path = os.path.join(output_dir, "duplicates_logged.csv")
        duplicates.to_csv(dup_path, index=False)

    df = df.drop_duplicates()
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    if "description" in df.columns:
        df["description"] = df["description"].fillna("Unknown")
        df["description"] = df["description"].astype(str).str.strip()

    if "customer_id" in df.columns:
        df["customer_id"] = df["customer_id"].astype("float64")

    return df


def normalize_categorical(df: pd.DataFrame) -> pd.DataFrame:
    if "description" in df.columns:
        df["description"] = df["description"].astype(str).str.strip()

    if "country" in df.columns:
        df["country"] = df["country"].astype(str).str.strip().str.title()
        country_map: Dict[str, str] = {
            "Uk": "United Kingdom",
            "United Kingdom": "United Kingdom",
            "Eire": "Ireland",
        }
        df["country"] = df["country"].replace(country_map)

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "quantity" in df.columns and "unit_price" in df.columns:
        df["total_value"] = df["quantity"] * df["unit_price"]

    if "invoice_date" in df.columns:
        df["year"] = df["invoice_date"].dt.year
        df["month"] = df["invoice_date"].dt.month

    return df


def detect_outliers_iqr(series: pd.Series) -> Tuple[pd.Series, float, float]:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (series < lower) | (series > upper)
    return mask, lower, upper


def flag_and_optionally_remove_outliers(df: pd.DataFrame, remove: bool, output_dir: str) -> pd.DataFrame:
    outlier_cols = []

    if "quantity" in df.columns:
        mask_q, lower_q, upper_q = detect_outliers_iqr(df["quantity"].dropna())
        full_mask_q = pd.Series(False, index=df.index)
        full_mask_q[mask_q.index] = mask_q
        df["is_outlier_quantity"] = full_mask_q
        outlier_cols.append("is_outlier_quantity")

    if "unit_price" in df.columns:
        mask_p, lower_p, upper_p = detect_outliers_iqr(df["unit_price"].dropna())
        full_mask_p = pd.Series(False, index=df.index)
        full_mask_p[mask_p.index] = mask_p
        df["is_outlier_unit_price"] = full_mask_p
        outlier_cols.append("is_outlier_unit_price")

    if outlier_cols:
        outliers_df = df[df[outlier_cols].any(axis=1)]
        if not outliers_df.empty:
            out_path = os.path.join(output_dir, "outliers_logged.csv")
            outliers_df.to_csv(out_path, index=False)

        if remove:
            df = df[~df[outlier_cols].any(axis=1)]

    return df


def create_summary_tables(df: pd.DataFrame, output_dir: str) -> None:
    if "total_value" in df.columns and "invoice_date" in df.columns:
        monthly_revenue = (
            df.groupby(["year", "month"], dropna=False)["total_value"]
            .sum()
            .reset_index()
            .sort_values(["year", "month"])
        )
        monthly_revenue.to_csv(os.path.join(output_dir, "revenue_by_month.csv"), index=False)
    else:
        monthly_revenue = pd.DataFrame()

    if "total_value" in df.columns and "description" in df.columns:
        top_products = (
            df.groupby("description")["total_value"]
            .sum()
            .reset_index()
            .sort_values("total_value", ascending=False)
            .head(10)
        )
        top_products.to_csv(os.path.join(output_dir, "top10_products_by_sales.csv"), index=False)
    else:
        top_products = pd.DataFrame()

    if "customer_id" in df.columns:
        unique_customers = df["customer_id"].nunique(dropna=True)
    else:
        unique_customers = None

    if "invoice_no" in df.columns and "total_value" in df.columns:
        order_values = df.groupby("invoice_no")["total_value"].sum()
        avg_order_value = float(order_values.mean()) if not order_values.empty else None
    else:
        avg_order_value = None

    metrics = {
        "metric": ["unique_customers", "average_order_value"],
        "value": [unique_customers, avg_order_value],
    }
    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(os.path.join(output_dir, "overall_metrics.csv"), index=False)

    # Basic visualizations
    figs_dir = os.path.join(output_dir, "figures")
    os.makedirs(figs_dir, exist_ok=True)

    if not monthly_revenue.empty:
        plt.figure(figsize=(10, 5))
        monthly_revenue["year_month"] = monthly_revenue["year"].astype(str) + "-" + monthly_revenue["month"].astype(str).str.zfill(2)
        plt.plot(monthly_revenue["year_month"], monthly_revenue["total_value"], marker="o")
        plt.xticks(rotation=45, ha="right")
        plt.title("Monthly Revenue")
        plt.xlabel("Year-Month")
        plt.ylabel("Revenue")
        plt.tight_layout()
        plt.savefig(os.path.join(figs_dir, "monthly_revenue.png"))
        plt.close()

    if not top_products.empty:
        plt.figure(figsize=(10, 5))
        plt.bar(top_products["description"], top_products["total_value"])
        plt.xticks(rotation=45, ha="right")
        plt.title("Top 10 Products by Sales")
        plt.xlabel("Product")
        plt.ylabel("Total Sales Value")
        plt.tight_layout()
        plt.savefig(os.path.join(figs_dir, "top10_products_by_sales.png"))
        plt.close()

    if "quantity" in df.columns:
        plt.figure(figsize=(8, 4))
        df["quantity"].hist(bins=50)
        plt.title("Quantity Distribution")
        plt.xlabel("Quantity")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(os.path.join(figs_dir, "quantity_distribution.png"))
        plt.close()


def clean_and_analyze(input_path: str, output_dir: str, remove_outliers: bool = False) -> None:
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_path, encoding="ISO-8859-1", low_memory=False)

    df = standardize_column_names(df)
    df = convert_dtypes(df)
    df = drop_invalid_invoices(df)
    df = handle_missing_values(df)
    df = normalize_categorical(df)
    df = add_derived_columns(df)
    df = deduplicate_and_log(df, output_dir)
    df = flag_and_optionally_remove_outliers(df, remove_outliers, output_dir)

    cleaned_path = os.path.join(output_dir, "retail_cleaned.csv")
    df.to_csv(cleaned_path, index=False)

    create_summary_tables(df, output_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and analyze retail sales data.")
    parser.add_argument(
        "--input",
        "-i",
        default=RAW_DEFAULT,
        help="Path to raw retail CSV (default: raw_retail.csv)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=OUTPUT_DIR_DEFAULT,
        help="Directory to save cleaned data, summaries, and figures (default: outputs)",
    )
    parser.add_argument(
        "--remove-outliers",
        action="store_true",
        help="If set, removes quantity and price outliers after flagging/logging them.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input
    output_dir = args.output
    remove_outliers = args.remove_outliers

    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    clean_and_analyze(input_path=input_path, output_dir=output_dir, remove_outliers=remove_outliers)


if __name__ == "__main__":
    main()
