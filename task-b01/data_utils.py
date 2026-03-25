import pandas as pd
from pathlib import Path
import logging
import time


# Loads input sales data from a CSV file
# Handles read and format errors
def load_data(input_path: Path) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting load_data for {input_path}")
    
    if not input_path.exists():
        end_time = time.time()
        logger.error(f"load_data failed in {end_time - start_time:.2f}s: Input file not found: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        df = pd.read_csv(input_path)
        end_time = time.time()
        logger.info(f"load_data completed successfully in {end_time - start_time:.2f}s: Loaded {len(df)} rows from {input_path}")
        return df
    except pd.errors.EmptyDataError:
        end_time = time.time()
        logger.error(f"load_data failed in {end_time - start_time:.2f}s: Input file is empty: {input_path}")
        raise ValueError(f"Input file is empty: {input_path}")
    except pd.errors.ParserError:
        end_time = time.time()
        logger.error(f"load_data failed in {end_time - start_time:.2f}s: Failed to parse input file: {input_path}")
        raise ValueError(f"Failed to parse input file: {input_path}")
    

# Validates the input DataFrame for required columns
def validate_format(df: pd.DataFrame, required_cols: set) -> None:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting validate_format with {len(df)} rows")
    
    missing = required_cols - set(df.columns)
    if missing:
        end_time = time.time()
        logger.error(f"validate_format failed in {end_time - start_time:.2f}s: Missing columns: {missing}")
        raise ValueError(f"Missing columns: {missing}")
    
    end_time = time.time()
    logger.info(f"validate_format completed successfully in {end_time - start_time:.2f}s: All required columns present")
    

# LEFT JOIN orders and customers DataFrames on customer_id
# Fails with MergeError if there are duplicate customer_id in customers_df (one-to-many or many-to-many join)
def join_orders_customers(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting join_orders_customers with {len(orders_df)} orders and {len(customers_df)} customers")
    
    joined_df = pd.merge(
        orders_df,
        customers_df,
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    
    end_time = time.time()
    logger.info(f"join_orders_customers completed successfully in {end_time - start_time:.2f}s: Joined to {len(joined_df)} rows")
    return joined_df


# Cleans up joined data for some edge cases:
# - if segment is missing (NaN) after the join, fill it with "UNKNOWN"
# - quantity is negative or zero (such rows are removed)
# - unit_price is negative (such rows are removed)
# - numeric columns have non-numeric values
# - data is invalid (doesn't meet the YYYY-MM-DD format) or missing in order_date column (such rows are removed)
# - removes duplicate order_id, keeping the most recent (last) occurrence
# - logs every cleanup action as WARNING:
#   - logs a string with missing segment filled with "UNKNOWN"
#   - logs a string removed due to negative or zero quantity
#   - logs a string removed due to negative unit price
#   - logs a string removed due to invalid or missing order_date
#   - logs a string removed due to non-numeric quantity or unit_price
#   - logs a string removed due to duplicate order_id, keeping the most recent (last) occurrence
# Fails if:
# - data format is incorrect (required columns are missing or have wrong types)
def cleanup_data(df: pd.DataFrame, required_cols: set) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting cleanup_data with {len(df)} rows")
    
    # Validate required columns are present
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        end_time = time.time()
        logger.error(f"cleanup_data failed in {end_time - start_time:.2f}s: Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    cleaned_df = df.copy()

    # Fill missing segment values with "UNKNOWN"
    # Log the number of rows with missing segment that are being filled
    missing_segment_count = cleaned_df["segment"].isna().sum()
    if missing_segment_count > 0:
        logger.warning(f"Found {missing_segment_count} rows with missing segment. Filling with 'UNKNOWN'.")
        cleaned_df["segment"] = cleaned_df["segment"].fillna("UNKNOWN")

    # Convert numeric columns to appropriate types, coercing errors to NaN
    for col in ["quantity", "unit_price"]:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")

    # After coercion, remove rows with NaN in critical numeric columns
    # Log the number of rows removed due to non-numeric values in quantity or unit_price
    non_numeric_count = cleaned_df[["quantity", "unit_price"]].isna().any(axis=1).sum()
    if non_numeric_count > 0:
        logger.warning(f"Found {non_numeric_count} rows with non-numeric quantity or unit_price. Removing these rows.")
        cleaned_df = cleaned_df.dropna(subset=["quantity", "unit_price"])

    # Remove rows with non-positive quantity or negative unit price
    # Log the number of rows removed due to invalid quantity or unit_price
    invalid_quantity_count = (cleaned_df["quantity"] <= 0).sum()
    if invalid_quantity_count > 0:
        logger.warning(f"Found {invalid_quantity_count} rows with non-positive quantity. Removing these rows.")
    invalid_unit_price_count = (cleaned_df["unit_price"] < 0).sum()
    if invalid_unit_price_count > 0:
        logger.warning(f"Found {invalid_unit_price_count} rows with negative unit price. Removing these rows.")
    cleaned_df = cleaned_df[
        (cleaned_df["quantity"] > 0) & (cleaned_df["unit_price"] >= 0)
    ]

    # Validate and clean order_date column (strict YYYY-MM-DD format only)
    # Log the number of rows removed due to invalid or missing order_date
    cleaned_df["order_date"] = pd.to_datetime(cleaned_df["order_date"], format="%Y-%m-%d", errors="coerce")
    invalid_order_date_count = cleaned_df["order_date"].isna().sum()
    if invalid_order_date_count > 0:
        logger.warning(f"Found {invalid_order_date_count} rows with invalid or missing order_date. Removing these rows.")
        cleaned_df = cleaned_df.dropna(subset=["order_date"])

    # Check for duplicate order_id and keep the most recent (last) occurrence
    # Log the number of duplicate order_id found and removed
    duplicate_order_id_count = cleaned_df.duplicated(subset=["order_id"], keep="last").sum()
    cleaned_df = cleaned_df.sort_values(by=["ingestion_date", "index"], ascending=[True, True])  # Ensure the most recent occurrence is last
    if duplicate_order_id_count > 0:
        logger.warning(f"Found {duplicate_order_id_count} duplicate order_id. Keeping the most recent (last) occurrence and removing duplicates.")
        cleaned_df = cleaned_df.drop_duplicates(subset=["order_id"], keep="last")
    
    end_time = time.time()
    logger.info(f"cleanup_data completed successfully in {end_time - start_time:.2f}s: Cleaned to {len(cleaned_df)} rows")
    return cleaned_df


# Adds some calculated metrics to the joined DataFrame:
# - revenue = quantity * unit_price
# - order-month = order_date truncated to month (YYYY-MM)
def add_calculated_metrics(joined_df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting add_calculated_metrics with {len(joined_df)} rows")
    
    df = joined_df.copy()
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["order_month"] = df["order_date"].dt.to_period("M")
    
    end_time = time.time()
    logger.info(f"add_calculated_metrics completed successfully in {end_time - start_time:.2f}s: Added metrics to {len(df)} rows")
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
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting group_by_month_and_segment with {len(enriched_df)} rows")
    
    grouped_df = enriched_df.groupby(["order_month", "segment"], sort=True).agg(
        orders_count=("order_id", "count"),
        customers_count=("customer_id", "nunique"),
        total_revenue=("revenue", "sum"),
    ).reset_index()
        
    end_time = time.time()
    logger.info(f"group_by_month_and_segment completed successfully in {end_time - start_time:.2f}s: Grouped to {len(grouped_df)} rows")
    return grouped_df


# Adds some advanced metrics to the grouped DataFrame:
# - cumulative_revenue = cumulative sum of revenue across months for each segment
# - rolling_avg_revenue = rolling average of total_revenue over the last 3 months for each segment
def add_advanced_metrics(grouped_df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting add_advanced_metrics with {len(grouped_df)} rows")
    
    df = grouped_df.copy()
    df=df.sort_values(by=["segment", "order_month"])  # Ensure correct order for cumulative and rolling calculations
    df["cumulative_revenue"] = df.groupby("segment")["total_revenue"].cumsum()
    df["rolling_avg_revenue"] = df.groupby("segment")["total_revenue"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())
    
    end_time = time.time()
    logger.info(f"add_advanced_metrics completed successfully in {end_time - start_time:.2f}s: Added advanced metrics to {len(df)} rows")
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
# NB: Monthly customers are taken from the data before grouping (enriched_df)
#     The reason is that if a customer belongs to different segments then it will be counted multiple times
#     in the grouped data, but in enriched_df it will be counted only once,
#     which gives a more accurate count of unique customers per month
def add_monthly_totals(advanced_df: pd.DataFrame, enriched_df: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting add_monthly_totals with {len(advanced_df)} rows")
    
    df = advanced_df.copy()
    monthly_customers = (enriched_df.groupby("order_month")["customer_id"].nunique().reset_index(name="customers_count")) # Get unique customers count per month from enriched_df
    monthly_totals = df.groupby("order_month").agg(
        orders_count=("orders_count", "sum"),
        total_revenue=("total_revenue", "sum"),
    ).reset_index()
    monthly_totals = monthly_totals.merge(monthly_customers, on="order_month", how="left")  # Merge unique customers count into totals
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
    result_df = pd.concat(dfs_to_concat, ignore_index=True)
    
    end_time = time.time()
    logger.info(f"add_monthly_totals completed successfully in {end_time - start_time:.2f}s: Added totals to {len(result_df)} rows")
    return result_df


# Saves the analysis result to a CSV file
# Handles write errors (folder not found, permission issues, file in use, etc.)
def save_summary(df: pd.DataFrame, output_path: Path) -> None:
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info(f"Starting save_summary to {output_path} with {len(df)} rows")
    
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        end_time = time.time()
        logger.info(f"save_summary completed successfully in {end_time - start_time:.2f}s: Saved {len(df)} rows to {output_path}")
    except PermissionError as e:
        end_time = time.time()
        logger.error(f"save_summary failed in {end_time - start_time:.2f}s: Cannot write to output file (permission denied or file is open): {output_path}: {e}")
        raise RuntimeError(
            f"Cannot write to output file (permission denied or file is open): {output_path}: {e}"
        )
    except OSError as e:
        # On Windows an open file may surface as an OSError with errno 13 (permission denied)
        if getattr(e, "errno", None) == 13:
            end_time = time.time()
            logger.error(f"save_summary failed in {end_time - start_time:.2f}s: Cannot write to output file (permission denied or file is open): {output_path}: {e}")
            raise RuntimeError(
                f"Cannot write to output file (permission denied or file is open): {output_path}: {e}"
            )
        end_time = time.time()
        logger.error(f"save_summary failed in {end_time - start_time:.2f}s: Failed to save summary to {output_path}: {e}")
        raise RuntimeError(f"Failed to save summary to {output_path}: {e}")
