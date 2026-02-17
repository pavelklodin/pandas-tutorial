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


# Cleans up joined data for some edge cases:
# - quantity is negative or zero (such rows are removed)
# - unit_price is negative (such rows are removed)
# - numeric columns have non-numeric values
def cleanup_data(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()

    # Convert numeric columns to appropriate types, coercing errors to NaN
    for col in ["quantity", "unit_price"]:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")

    # After coercion, remove rows with NaN in critical numeric columns
    cleaned_df = cleaned_df.dropna(subset=["quantity", "unit_price"])

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
    return df


# Makes a pivot table out of the enriched DataFrame with:
# - index: region
# - columns: category
# - values: sum of revenue
# - column TOTAL with sum of revenue across all categories for each region
# missing values are filled with 0
def pivot_sales(enriched_df: pd.DataFrame) -> pd.DataFrame:
    pivot_df = enriched_df.pivot_table(
        index="region",
        columns="category",
        values="revenue",
        aggfunc="sum",
        fill_value=0
    )

    # Ensure the order of category columns is consistent and add missing ones with 0 if needed
    expected_cols = ["Hardware", "Services", "Software"]
    pivot_df = pivot_df.reindex(columns=expected_cols, fill_value=0)

    # Add a TOTAL column (sum across all category columns)
    pivot_df["TOTAL"] = pivot_df.sum(axis=1)
    # Convert region from index to regular column
    return pivot_df.reset_index()


# Aggregates totals across all sales in the pivoted DataFrame
def compute_grand_totals(pivot_df: pd.DataFrame) -> pd.DataFrame:
    totals = pivot_df.select_dtypes("number").sum().to_frame().T  # Convert Series to single-row DataFrame
    totals.insert(0, "region", "ALL")  # Add region column with value
    return totals


# Combines group-level and total-level results into a single DataFrame
def combine_results(pivot_df: pd.DataFrame, totals_df: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.concat([pivot_df, totals_df], ignore_index=True)
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
    output_file = result_dir / "sales_summary.csv"

    sales_cols = {"order_id", "region", "category", "quantity", "unit_price"}

    sales_df = load_data(input_sales)
    validate_format(sales_df, sales_cols)

    cleaned_sales_df = cleanup_data(sales_df)

    enriched_sales_df = add_calculated_metrics(cleaned_sales_df)

    pivoted_sales_df = pivot_sales(enriched_sales_df)

    grand_totals_df = compute_grand_totals(pivoted_sales_df)

    final_result_df = combine_results(pivoted_sales_df, grand_totals_df)

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
