import pandas as pd
from pathlib import Path


# Loads input sales data from a CSV file
# Handles read and format errors
def load_data(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        df = pd.read_csv(input_path)
        return df
    except pd.errors.EmptyDataError:
        raise ValueError(f"Input file is empty: {input_path}")
    except pd.errors.ParserError:
        raise ValueError(f"Failed to parse input file: {input_path}")
    

# Validates the input DataFrame for required columns
def validate_format(df: pd.DataFrame, required_cols: set) -> None:
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    

# LEFT JOIN orders and customers DataFrames on customer_id
# Fails with MergeError if there are duplicate customer_id in customers_df (one-to-many or many-to-many join)
def join_orders_customers(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    joined_df = pd.merge(
        orders_df,
        customers_df,
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    return joined_df


# Cleans up joined data for some edge cases:
# - if segment is missing (NaN) after the join, fill it with "UNKNOWN"
# - quantity is negative or zero (such rows are removed)
# - unit_price is negative (such rows are removed)
# - numeric columns have non-numeric values
# - data is invalid (doesn't meet the YYYY-MM-DD format) or missing in order_date column (such rows are removed)
# - removes rows with the same order_id provided other data are the same (duplicate orders)
# Fails if:
# - there are rows with the same order_id but different data (potential data quality issue)
# - data format is incorrect (required columns are missing or have wrong types)
def cleanup_data(df: pd.DataFrame, required_cols: set) -> pd.DataFrame:
    # Validate required columns are present
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    cleaned_df = df.copy()

    # Fill missing segment values with "UNKNOWN"
    cleaned_df["segment"] = cleaned_df["segment"].fillna("UNKNOWN")

    # Convert numeric columns to appropriate types, coercing errors to NaN
    for col in ["quantity", "unit_price"]:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")

    # After coercion, remove rows with NaN in critical numeric columns
    cleaned_df = cleaned_df.dropna(subset=["quantity", "unit_price"])

    # Remove rows with non-positive quantity or negative unit price
    cleaned_df = cleaned_df[
        (cleaned_df["quantity"] > 0) & (cleaned_df["unit_price"] >= 0)
    ]

    # Validate and clean order_date column (strict YYYY-MM-DD format only)
    cleaned_df["order_date"] = pd.to_datetime(cleaned_df["order_date"], format="%Y-%m-%d", errors="coerce")
    cleaned_df = cleaned_df.dropna(subset=["order_date"])

    # Check for duplicate order_id with different data (potential data quality issue)
    duplicates = cleaned_df.duplicated(subset=["order_id"], keep=False)
    if duplicates.any():
        duplicate_orders = cleaned_df[duplicates].sort_values("order_id")
        # Check if all rows with the same order_id are identical
        for order_id, group in duplicate_orders.groupby("order_id"):
            if not group.drop(columns=["order_id"]).nunique().eq(1).all():
                raise ValueError(f"Data quality issue: duplicate order_id with different data: {order_id}")
        # Remove duplicate rows (they passed validation, so they are identical)
        cleaned_df = cleaned_df.drop_duplicates(subset=["order_id"], keep="first")

    return cleaned_df


# Adds some calculated metrics to the joined DataFrame:
# - revenue = quantity * unit_price
# - order-month = order_date truncated to month (YYYY-MM)
def add_calculated_metrics(joined_df: pd.DataFrame) -> pd.DataFrame:
    df = joined_df.copy()
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["order_month"] = df["order_date"].dt.to_period("M")
    return df


# Groups the enriched DataFrame by 
# - order_month and
# - segment
# sorted by order_month ascending and segment alphabetically
# and returns aggregated metrics:
# - orders_count = count of orders in each group
# - customers_count = count of unique customers in each group
# - total_revenue = sum of revenue in each group
def group_by_month_and_segment(enriched_df: pd.DataFrame) -> pd.DataFrame:
    grouped_df = enriched_df.groupby(["order_month", "segment"], sort=False).agg(
        orders_count=("order_id", "count"),
        customers_count=("customer_id", lambda x: x.nunique()),
        total_revenue=("revenue", "sum"),
    ).reset_index()
        
    # Sort by order_month ascending and segment alphabetically
    grouped_df = grouped_df.sort_values(by=["order_month", "segment"])
    return grouped_df


# Adds some advanced metrics to the grouped DataFrame:
# - cumulative_revenue = cumulative sum of revenue across months for each segment
# - rolling_avg_revenue = rolling average of total_revenue over the last 3 months for each segment
def add_advanced_metrics(grouped_df: pd.DataFrame) -> pd.DataFrame:
    df = grouped_df.copy()
    df["cumulative_revenue"] = df.groupby("segment")["total_revenue"].cumsum()
    df["rolling_avg_revenue"] = df.groupby("segment")["total_revenue"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())
    return df


# Insert monthly totals across all segments to the grouped DataFrame
# - for each month, adds a row with segment = "ALL" and sums of:
# - orders_count
# - customers_count
# - total_revenue
# - cumulative_revenue
# - rolling_avg_revenue
# across all segments for that month
# Monthly totals are inserted after the last segment of each month (not at the end of the table)
# An empty line is added after each monthly total for better readability
def add_monthly_totals(advanced_df: pd.DataFrame) -> pd.DataFrame:
    df = advanced_df.copy()
    monthly_totals = df.groupby("order_month").agg(
        orders_count=("orders_count", "sum"),
        customers_count=("customers_count", "sum"),
        total_revenue=("total_revenue", "sum"),
    ).reset_index()
    monthly_totals["cumulative_revenue"] = monthly_totals["total_revenue"].cumsum()  # Recalculate cumulative_revenue for totals
    monthly_totals["rolling_avg_revenue"] = (
        monthly_totals["total_revenue"]
            .rolling(window=3, min_periods=1)
            .mean()
    )  # Recalculate rolling_avg_revenue for totals
    monthly_totals["segment"] = "ALL"

    # Build a list of DataFrames and concatenate once (more efficient)
    dfs_to_concat = []
    for month, group in df.groupby("order_month"):
        dfs_to_concat.append(group)
        total_row = monthly_totals[monthly_totals["order_month"] == month]
        dfs_to_concat.append(total_row)
        # Empty row for readability
        empty_row = pd.DataFrame([{col: None for col in df.columns}])
        dfs_to_concat.append(empty_row)

    # Concatenate all at once (O(n) instead of O(n²))
    return pd.concat(dfs_to_concat, ignore_index=True)


# Saves the analysis result to a CSV file
# Handles write errors (folder not found, permission issues, file in use, etc.)
def save_summary(df: pd.DataFrame, output_path: Path) -> None:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
    except PermissionError as e:
        raise RuntimeError(
            f"Cannot write to output file (permission denied or file is open): {output_path}: {e}"
        )
    except OSError as e:
        # On Windows an open file may surface as an OSError with errno 13 (permission denied)
        if getattr(e, "errno", None) == 13:
            raise RuntimeError(
                f"Cannot write to output file (permission denied or file is open): {output_path}: {e}"
            )
        raise RuntimeError(f"Failed to save summary to {output_path}: {e}")


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    input_dir = data_dir / "input"
    result_dir = data_dir / "result"

    input_cusomers = input_dir / "customers.csv"
    input_orders = input_dir / "orders.csv"
    output_file = result_dir / "revenue_summary.csv"

    customers_cols = {"customer_id", "segment"}
    orders_cols = {"order_id", "customer_id", "order_date", "quantity", "unit_price"}
    joined_cols = {"order_id", "customer_id", "order_date", "quantity", "unit_price", "segment"}

    customers_df = load_data(input_cusomers)
    validate_format(customers_df, customers_cols)

    orders_df = load_data(input_orders)
    validate_format(orders_df, orders_cols)

    joined_df = join_orders_customers(orders_df, customers_df)
    cleaned_df = cleanup_data(joined_df, joined_cols)
    enriched_df = add_calculated_metrics(cleaned_df)
    grouped_df = group_by_month_and_segment(enriched_df)
    advanced_df = add_advanced_metrics(grouped_df)
    final_result_df = add_monthly_totals(advanced_df)

    save_summary(final_result_df, output_file)

    print("Analysis completed.")
    print(f"Result saved to: {output_file.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as e:
        # expected errors: show short message and exit 1
        print(f"Error: {e}")
        import sys

        sys.exit(1)
    except Exception as e:
        # unexpected errors: write full traceback to a log next to the script
        import traceback
        import sys

        log_path = Path(__file__).resolve().parent / "error.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(traceback.format_exc())
                f.write("\n")
        except Exception:
            # if logging fails, still print a short message
            print("Unexpected error and failed to write traceback log.")
            sys.exit(2)

        print(f"Unexpected error. Full traceback written to: {log_path}")
        sys.exit(2)
