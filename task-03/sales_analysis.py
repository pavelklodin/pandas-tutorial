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


# Joins sales data with product information to enrich sales records
# Handles missing products by filling in default values for product_name, category, and unit_cost:
# - product_name: "UNKNOWN"
# - category: "UNKNOWN"
# - unit_cost: 0
def join_data(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    joined_df = sales_df.merge(
        products_df,
        on="product_id",
        how="left",
    )

    # Fill missing product information with default values
    joined_df.fillna({"product_name": "UNKNOWN", "category": "UNKNOWN", "unit_cost": 0}, inplace=True)
    return joined_df


# Cleans up joined data for some edge cases:
# - quantity is negative or zero (such rows are removed)
# - unit_price is negative (such rows are removed)
# - numeric columns have non-numeric values
def cleanup_joined_data(joined_df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = joined_df.copy()

    # Convert numeric columns to appropriate types, coercing errors to NaN
    for col in ["quantity", "unit_price", "unit_cost"]:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")

    # After coercion, remove rows with NaN in critical numeric columns
    cleaned_df = cleaned_df.dropna(subset=["quantity", "unit_price", "unit_cost"])

    # Remove rows with non-positive quantity or negative unit price
    cleaned_df = cleaned_df[
        (cleaned_df["quantity"] > 0) & (cleaned_df["unit_price"] >= 0)
    ]

    return cleaned_df


# Adds some calculated metrics to the joined DataFrame:
# - revenue = quantity * unit_price
# - cost = quantity * unit_cost
# - profit = revenue - cost
def add_calculated_metrics(joined_df: pd.DataFrame) -> pd.DataFrame:
    df = joined_df.copy()
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["cost"] = df["quantity"] * df["unit_cost"]
    df["profit"] = df["revenue"] - df["cost"]
    return df


# Groups sales by
# - region and
# - category
# Calculate within each group:
# - orders_count: number of orders (rows) in the group
# - total_revenue: sum of revenue in the group
# - total_cost: sum of cost in the group
# - total_profit: sum of profit in the group
# - profit_margin: total_profit / total_revenue (rounded to 2 decimal places, handle division by zero)
def aggregate_by_group(joined_df: pd.DataFrame) -> pd.DataFrame:
    result = joined_df.groupby(["region", "category"], as_index=False).agg(
        orders_count=("order_id", "count"),
        total_revenue=("revenue", "sum"),
        total_cost=("cost", "sum"),
        total_profit=("profit", "sum")
    ).round(2).sort_values(["region", "category"], ascending=[True, True])
    # Calculate profit margin, handling division by zero
    result["profit_margin"] = (
        (result["total_profit"] / result["total_revenue"])
        .round(2)
        .fillna(0)
    )
    return result


# Aggregates totals across all sales:
# - orders_count: number of orders (rows) in the group
# - total_revenue: sum of revenue in the group
# - total_cost: sum of cost in the group
# - total_profit: sum of profit in the group
# - profit_margin: total_profit / total_revenue (rounded to 2 decimal places, handle division by zero)
# - set region and category to "ALL"
def aggregate_totals(joined_df: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(
        {
            "region": ["ALL"],
            "category": ["ALL"],
            "orders_count": [len(joined_df)],
            "total_revenue": [joined_df["revenue"].sum()],
            "total_cost": [joined_df["cost"].sum()],
            "total_profit": [joined_df["profit"].sum()],
        }
    ).round(2)
    # Calculate profit margin, handling division by zero
    result["profit_margin"] = (
        (result["total_profit"] / result["total_revenue"])
        .round(2)
        .fillna(0)
    )
    return result


# Combines group-level and total-level results into a single DataFrame
# and sorts by region and category in ascending order (with "ALL" at the end)
def combine_results(group_df: pd.DataFrame, totals_df: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([group_df, totals_df], ignore_index=True).sort_values(["region", "category"], ascending=[True, True])
    return combined_df


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

    input_sales = input_dir / "sales.csv"
    products_sales = input_dir / "products.csv"
    output_file = result_dir / "sales_summary.csv"

    sales_cols = {"order_id", "region", "product_id", "quantity", "unit_price"}
    products_cols = {"product_id", "product_name", "category", "unit_cost"}

    sales_df = load_data(input_sales)
    validate_format(sales_df, sales_cols)
    products_df = load_data(products_sales)
    validate_format(products_df, products_cols)

    joined_df = join_data(sales_df, products_df)

    #DEBUG
    print("Joined data:")
    print(joined_df)

    cleaned_joined_df = cleanup_joined_data(joined_df)

    #DEBUG
    print("Cleaned joined data:")
    print(cleaned_joined_df)

    enriched_joined_df = add_calculated_metrics(cleaned_joined_df)

    #DEBUG
    print("Enriched joined data:")
    print(enriched_joined_df)

    aggregated_by_group_df = aggregate_by_group(enriched_joined_df)

    #DEBUG
    print("Aggregated by group data:")
    print(aggregated_by_group_df)

    aggregated_totals_df = aggregate_totals(enriched_joined_df)

    #DEBUG
    print("Aggregated totals data:")
    print(aggregated_totals_df)

    final_result_df = combine_results(aggregated_by_group_df, aggregated_totals_df)

    #DEBUG
    print("Final result data:")
    print(final_result_df)



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
