import pandas as pd
from pathlib import Path
import os
import logging
import time
from data_utils import (
    load_data,
    load_and_enrich_metadata,
    validate_format,
    cleanup_data,
    add_calculated_metrics,
    group_by_month_and_segment,
    add_advanced_metrics,
    add_monthly_totals,
    save_summary
)

# Define global variables for input and output paths and expected columns
base_dir = Path(__file__).resolve().parent
data_dir = base_dir / "data"
input_dir = data_dir / "input"
to_process_dir = input_dir / "to_process"
current_intermediate_dir = input_dir / "current_intermediate"
result_dir = data_dir / "result"

input_cusomers = input_dir / "customers.csv"
customers_to_process = to_process_dir / "customers.csv"
current_orders = current_intermediate_dir / "enriched_raw_data.csv"
output_file = result_dir / "revenue_summary.csv"

customers_cols = {"customer_id", "segment"}
orders_cols = {"order_id", "customer_id", "order_date", "quantity", "unit_price"}
joined_cols = {"order_id", "customer_id", "order_date", "quantity", "unit_price", "segment", "ingestion_date"}

# Configure logging
log_file = base_dir / "pipeline.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Below function checks the pre-conditions before running the main data analytic pipeline:
# - General goal for the checks is to provide the normal start of the script (necessary files
#   must exist). This function doesn't check if the result meets the data.
#   If the pre-conditions are not met, the function should raise an exception
#   with a clear message about what is missing and recommended follow-up actions.
#   If the failure is not critical (missing raw data and existing intermediate result)
#   then the pre-condition check pass and the warning is logged.
# - Check if the customers metadata exist.
#   Either data/input/customers.csv or data/input/to_process/customers.csv should exist.
#   If neither of these files exist, raise an exception with a message that customers metadata
#   is missing and should be provided in one of these two locations.
# - Check if there are the order data to process.
#   If there are no new orders to process (no files in data/input/to_process with the name pattern "orders-<YYYY>-<MM>-<DD>-<NNNN>.csv"),
#   then exit with a message that no data to process (not an error!).
# - Check immutable raw data in folders data/input/<year>/<month>/<day> for consistency.
#   1. If there are no files with the name pattern "orders-<YYYY>-<MM>-<DD>-<NNNN>.csv" in these folders,
#      then the first run is considered. Intermediate result data/input/current_intermediate/enriched_raw_data.csv
#      should not exist.
#      If it exists - log a warning that the immutable raw data is missing. It is
#      recommended to restore the data from the backup.
#   2. If there are files with the name pattern "orders-<YYYY>-<MM>-<DD>-<NNNN>.csv" in these folders,
#      then the normal run is considered. Intermediate result data/input/current_intermediate/enriched_raw_data.csv
#      should exist.
#      If it doesn't exist - raise an exception with a message that the intermediate result is missing.
#      It is recommended to move the immutable raw data to folder data/input/to_process and run the script again.
#      to restore the intermediate result.
def check_preconditions(pipeline_start: float) -> None:
    start_time = time.time()
    logger.info(f"Starting check_preconditions")
    
    # Check if customers metadata exist
    if not input_cusomers.exists() and not customers_to_process.exists():
        end_time = time.time()
        logger.error(f"check_preconditions failed in {end_time - start_time:.2f}s: Customers metadata is missing")
        pipeline_end = time.time()
        logger.info(f"Pipeline completed in {pipeline_end - pipeline_start:.2f}s")
        raise FileNotFoundError(
            f"Customers metadata is missing. Please provide the customers.csv file in either {input_cusomers} or {customers_to_process}."
        )

    # Check if there are new orders to process
    if not any(to_process_dir.glob("orders-????-??-??-????.csv")):
        end_time = time.time()
        logger.info(f"check_preconditions completed in {end_time - start_time:.2f}s: No new orders to process")
        print("No new orders to process. Exiting.")
        exit(0)

    # Check immutable raw data consistency
    raw_data_files = [f for f in input_dir.glob("**/orders-????-??-??-????.csv") if len(f.relative_to(input_dir).parts) == 4]
    intermediate_result_exists = current_orders.exists()

    if not raw_data_files and intermediate_result_exists:
        logger.warning(
            "Immutable raw data is missing, but intermediate result exists. It is recommended to restore the raw data from backup."
        )
    elif raw_data_files and not intermediate_result_exists:
        end_time = time.time()
        logger.error(f"check_preconditions failed in {end_time - start_time:.2f}s: Intermediate result is missing while immutable raw data exists. It is recommended to move the immutable raw data to the data/input/to_process folder and run the script again to restore the intermediate result.")
        pipeline_end = time.time()
        logger.info(f"Pipeline completed in {pipeline_end - pipeline_start:.2f}s")
        raise FileNotFoundError(
            "Intermediate result is missing while immutable raw data exists. It is recommended to move the immutable raw data to the data/input/to_process folder and run the script again to restore the intermediate result."
        )
    
    end_time = time.time()
    logger.info(f"check_preconditions completed successfully in {end_time - start_time:.2f}s")


def main() -> None:
    pipeline_start = time.time()
    logger.info("=" * 60)
    logger.info(f"Starting pipeline run at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Step 1: Check pre-conditions before running the main data analytic pipeline
    logger.info("Step 1: Check pre-conditions before running the main data analytic pipeline")
    check_preconditions()

    # Step 2: Load customers data and validate format
    logger.info("Step 2: Load customers data and validate format")
    customers_df = pd.DataFrame(columns=list(customers_cols))  # default empty DataFrame with expected columns
    if input_cusomers.exists():
        customers_df = load_data(input_cusomers)
        validate_format(customers_df, customers_cols)
    if customers_df["customer_id"].duplicated().any(): # duplication may only happen if immutable customers metadata was updated manually.
        logger.warning("Duplicate customer_id found in customers data. Keeping the last occurrence and removing duplicates.")
        customers_df = customers_df.drop_duplicates(subset=["customer_id"], keep="last")

    # Step 3: If there are updates to customers data in customers_to_process, load and validate it,
    # then overwrite the original customers.csv
    # - load the updates
    # - concatenate the customers data with the updates
    # - if there are duplicates with the same customer_id
    #   (no matter if they have the same or different data), keep the last occurrence
    #   (assuming it's the most recent update)
    logger.info("Step 3: Update customers data if there are updates in the to_process directory")
    if customers_to_process.exists():
        customers_updates_df = load_data(customers_to_process)
        validate_format(customers_updates_df, customers_cols)
        customers_df = pd.concat([customers_df, customers_updates_df], ignore_index=True)
        customers_df = customers_df.drop_duplicates(subset=["customer_id"], keep="last")
    
        # Save the updated customers data to a temporary file and then move it to the original location
        # (to avoid issues if the file is open in Excel or similar)
        temp_customers_path = input_dir / "customers_temp.csv"
        save_summary(customers_df, temp_customers_path)
        os.replace(temp_customers_path, input_cusomers)
        os.remove(customers_to_process)  # remove the updates file after processing

    # Step 4: Load the current orders data and validate format
    # If the intermediate result doesn't exist (this is the first run), create an empty DataFrame
    # with the expected columns (joined_cols) to be used in the next steps
    logger.info("Step 4: Load the current orders data and validate format")
    if current_orders.exists():
        current_orders_df = load_data(current_orders)
        validate_format(current_orders_df, joined_cols)
    else:
        current_orders_df = pd.DataFrame(columns=list(joined_cols))

    # Step 5: Update customers information in the current orders if needed
    logger.info("Step 5: Update customers information in the current orders")
    current_orders_df.update(customers_df.set_index("customer_id")["segment"])

    # Step 6: Load the new orders:
    # - the new orders are located in the to_process_dir folder
    # - these are files with the following name pattern: "orders-<YYYY>-<MM>-<DD>-<NNNN>.csv", where
    #   - <YYYY> is the 4-digit year
    #   - <MM> is the 2-digit month
    #   - <DD> is the 2-digit day
    #   - <NNNN> is a 4-digit sequence number (starting from 0000 each day)
    # - there can be multiple files per day, they should be processed in the order of their sequence number
    # - every order should be enriched with the ingestion date, parsed from the filename; this is needed to
    #   select and keep the most recent update for the same order_id in the next steps (if there are duplicates,
    #   we keep the last occurrence assuming it's the most recent update)
    # - all the files should be concatenated into a single DataFrame orders_to_process_df
    # - if there are no new orders files, skip to the next step
    logger.info("Step 6: Load the new orders from to_process directory")
    orders_to_process_df = pd.DataFrame()
    # only files with zero-padded year/month/day/sequence match; lexical sort orders them correctly
    for file in sorted(to_process_dir.glob("orders-????-??-??-????.csv")):
        new_orders_df = load_and_enrich_metadata(file)
        validate_format(new_orders_df, orders_cols)
        orders_to_process_df = pd.concat([orders_to_process_df, new_orders_df], ignore_index=True)
    if orders_to_process_df.empty:
        pipeline_end = time.time()
        logger.info(f"pipeline completed in {pipeline_end - pipeline_start:.2f}s: No new orders to process. Exiting.")
        exit(0)

    # Step 7: Process and enrich the new orders:
    # - left join the new orders with customers_df on customer_id
    # - cleanup the joined DataFrame by running the cleanup_data function
    # - add calculated metrics to the cleaned DataFrame by running the add_calculated_metrics function
    # - if there are no new orders, skip to the next step
    logger.info("Step 7: Process and enrich the new orders")
    orders_to_process_df = orders_to_process_df.merge(customers_df, on="customer_id", how="left", validate="many_to_one")
    orders_to_process_df = cleanup_data(orders_to_process_df, orders_cols)
    orders_to_process_df = add_calculated_metrics(orders_to_process_df)
    
    # Step 8: Update the current orders data:
    # - concatenate the current orders with the processed new orders
    # - if there are duplicate order_id (no matter if they have the same or different data), keep the last occurrence (assuming it's the most recent update)
    # - save the updated current orders data to a temporary file and then move it to the original location (to avoid issues if the file is open in Excel or similar)
    logger.info("Step 8: Update the current orders data")
    if set(orders_df.columns) != set(orders_to_process_df.columns): # can only concatenate if columns match, otherwise log an error and raise an exception
        logger.error(f"Column mismatch between current orders and new orders. Current orders columns: {orders_df.columns}, New orders columns: {orders_to_process_df.columns}")
        pipeline_end = time.time()
        logger.info(f"Pipeline completed in {pipeline_end - pipeline_start:.2f}s")
        raise ValueError("Column mismatch between current orders and new orders. Please check the logs for details.")
    orders_df = pd.concat([current_orders_df, orders_to_process_df], ignore_index=True)
    orders_df = orders_df.sort_values(by=["order_id", "ingestion_date", "ingestion_seq", "index"], ascending=[True, True, True, True])  # sort by order_id and ingestion_date to keep the last occurrence in case of duplicates
    orders_df = orders_df.drop_duplicates(subset=["order_id"], keep="last")
    temp_orders_path = current_intermediate_dir / "enriched_raw_data_temp.csv"
    save_summary(orders_df, temp_orders_path)
    os.replace(temp_orders_path, current_orders)
    
    # Step 9: Move the processed new orders CSV files to their permanent location:
    # - input_dir/YYYY/MM/DD/orders-<NNNN>.csv
    # - if the destination folder doesn't exist, create it
    logger.info("Step 9: Move the processed new orders CSV files to their permanent location")
    for file in sorted(to_process_dir.glob("orders-????-??-??-????.csv")):
        # Extract date and sequence number from the filename
        filename = file.name
        date_part = filename.split("-")[1:4]  # YYYY-MM-DD
        dest_dir = input_dir / date_part[0] / date_part[1] / date_part[2]
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / filename
        os.replace(file, dest_file)
    
    # Step 10: Recalculate the resulting data out of orders_df:
    # - group the data by month and segment by running the group_by_month_and_segment function
    # - add advanced metrics by running the add_advanced_metrics function
    # - add monthly totals after each month by running the add_monthly_totals function
    logger.info("Step 10: Recalculate the resulting data out of orders_df")
    grouped_df = group_by_month_and_segment(orders_df)
    final_result_df = add_advanced_metrics(grouped_df)
    final_result_df = add_monthly_totals(final_result_df, orders_df)

    # Step 11: Save the final result to a temporary CSV file by running the save_summary function
    # and then move it to the final output_file (to avoid issues if the file is open in Excel or similar)
    logger.info("Step 11: Save the final result to output file")
    temp_summary_path = result_dir / "summary_temp.csv"
    save_summary(final_result_df, temp_summary_path)
    os.replace(temp_summary_path, output_file)
    
    pipeline_end = time.time()
    logger.info(f"Pipeline run completed successfully in {pipeline_end - pipeline_start:.2f}s")
    logger.info("=" * 60)

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
