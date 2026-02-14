import pandas as pd
from pathlib import Path


def load_sales_data(input_path: Path) -> pd.DataFrame:
    """
    Загружает данные о продажах из CSV-файла.
    """
    return pd.read_csv(input_path)


def analyze_sales_by_region(
    df: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
    """
    Фильтрует продажи по региону и
    возвращает агрегированные метрики.
    """
    filtered_df = df.loc[df["region"] == region]

    result = pd.DataFrame(
        {
            "region": [region],
            "sales_count": [len(filtered_df)],
            "total_revenue": [filtered_df["revenue"].sum()],
            "average_revenue": [filtered_df["revenue"].mean()],
        }
    )

    return result


def save_summary(df: pd.DataFrame, output_path: Path) -> None:
    """
    Сохраняет результат анализа в CSV-файл.
    """
    df.to_csv(output_path, index=False)


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    
    input_file = base_dir / "sales_input.csv"
    output_file = base_dir / "sales_summary.csv"

    sales_df = load_sales_data(input_file)

    summary_df = analyze_sales_by_region(
        sales_df,
        region="EU",
    )

    save_summary(summary_df, output_file)

    print("Analysis completed.")
    print(f"Result saved to: {output_file.resolve()}")


if __name__ == "__main__":
    main()
