import pandas as pd
from pathlib import Path


# Loads input sales data from a CSV file
# Handles read and format errors
def load_sales_data(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    try:
        df = pd.read_csv(input_path)
        required_cols = {"region", "product", "revenue"}
        missing = required_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns: {missing}")
        return df
    except pd.errors.EmptyDataError:
        raise ValueError(f"Input file is empty: {input_path}")
    except pd.errors.ParserError:
        raise ValueError(f"Failed to parse input file: {input_path}")
    

# Groups sales by
# - region and
# - product
# from the input DataFrame and
# returns aggregated metrics
def summarize_sales(
    df: pd.DataFrame
) -> pd.DataFrame:
    result = df.groupby(["region", "product"], as_index=False).agg(
        sales_count=("revenue", "size"),
        total_revenue=("revenue", "sum"),
        average_revenue=("revenue", "mean"),
    ).round(2)

    return result


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

    input_file = input_dir / "sales_input.csv"
    output_file = result_dir / "sales_summary.csv"

    sales_df = load_sales_data(input_file)

    summary_df = summarize_sales(
        sales_df,
    )

    save_summary(summary_df, output_file)

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
