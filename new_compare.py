import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, monotonically_increasing_id
from datetime import datetime

# Generate unique filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f'comparison_log_{timestamp}.log'

# Set up logging (without INFO - prefix)
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(message)s')

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compare two CSV files.")
parser.add_argument("--file1", required=True, help="Path to the first CSV file (BDH)")
parser.add_argument("--file2", required=True, help="Path to the second CSV file (CDH)")
parser.add_argument("--primary-keys", required=False, help="Comma-separated list of primary key columns. If not provided, row-by-row comparison will be used.")
parser.add_argument("--drop-columns", required=False, help="Comma-separated list of columns to drop from both files.")
args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

# Load data
BDH = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file1)
CDH = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file2)

# Drop columns if specified
if args.drop_columns:
    drop_columns = [col.strip() for col in args.drop_columns.split(",")]
    BDH = BDH.drop(*drop_columns)
    CDH = CDH.drop(*drop_columns)

# Count rows in both DataFrames
count_BDH = BDH.count()
count_CDH = CDH.count()

# Log the counts
logging.info(f"Row count of BDH: {count_BDH}")
logging.info(f"Row count of CDH: {count_CDH}")

# Check if row counts match
if count_BDH != count_CDH:
    logging.info(f"Row count mismatch: BDH has {count_BDH} rows, CDH has {count_CDH} rows.")
    logging.info("Comparing only up to the number of rows in the smaller file.")

    # Get the smaller count to truncate both DataFrames
    min_count = min(count_BDH, count_CDH)

    # Log and show the extra rows
    if count_BDH > min_count:
        logging.info(f"BDH has {count_BDH - min_count} extra rows.")
        extra_BDH = BDH.subtract(BDH.limit(min_count))
        logging.info("Extra rows from BDH:")
        logging.info(extra_BDH._jdf.showString(10, 0, False))

    if count_CDH > min_count:
        logging.info(f"CDH has {count_CDH - min_count} extra rows.")
        extra_CDH = CDH.subtract(CDH.limit(min_count))
        logging.info("Extra rows from CDH:")
        logging.info(extra_CDH._jdf.showString(10, 0, False))

    # Truncate the DataFrames for comparison
    BDH = BDH.limit(min_count)
    CDH = CDH.limit(min_count)

# If no primary keys are provided, add a unique row identifier for row-by-row comparison
if not args.primary_keys:
    logging.info("No primary keys provided. Falling back to row-by-row comparison.")
    BDH = BDH.withColumn("row_id", monotonically_increasing_id())
    CDH = CDH.withColumn("row_id", monotonically_increasing_id())
    primary_keys = ["row_id"]
else:
    primary_keys = args.primary_keys.split(",")

# Register DataFrames as SQL tables
BDH.createOrReplaceTempView("BDH")
CDH.createOrReplaceTempView("CDH")

# Build join condition for multiple primary keys
join_condition = " AND ".join([f"BDH.`{key}` = CDH.`{key}`" for key in primary_keys])

# Create SQL query to compare data
join_query = f"""
SELECT 
    {", ".join([f"BDH.`{key}` as BDH_{key}, CDH.`{key}` as CDH_{key}" for key in primary_keys])},
    BDH.*, 
    CDH.*
FROM BDH
FULL OUTER JOIN CDH
ON {join_condition}
"""

# Execute the SQL query
try:
    joined_df = spark.sql(join_query)
except Exception as e:
    logging.error(f"Error executing query: {e}")
    spark.stop()
    exit(1)

# Compare columns and log details
total_mismatches_exclude_null = 0
total_mismatches_include_null = 0

for col_name in BDH.columns:
    if col_name in primary_keys:
        continue  # Skip primary key columns for mismatch counting

    # Exclude cases where one column is NULL and the other is not
    exclude_null_query = f"""
    SELECT 
        {", ".join([f"BDH.`{key}` as BDH_{key}, CDH.`{key}` as CDH_{key}" for key in primary_keys])},
        BDH.`{col_name}` as BDH_value, 
        CDH.`{col_name}` as CDH_value
    FROM BDH
    FULL OUTER JOIN CDH
    ON {join_condition}
    WHERE 
        coalesce(BDH.`{col_name}`, '') != coalesce(CDH.`{col_name}`, '') 
        AND NOT ((BDH.`{col_name}` IS NULL AND CDH.`{col_name}` IS NOT NULL) 
                 OR (BDH.`{col_name}` IS NOT NULL AND CDH.`{col_name}` IS NULL))
    """

    # Include cases where one column is NULL and the other is not
    include_null_query = f"""
    SELECT 
        {", ".join([f"BDH.`{key}` as BDH_{key}, CDH.`{key}` as CDH_{key}" for key in primary_keys])},
        BDH.`{col_name}` as BDH_value, 
        CDH.`{col_name}` as CDH_value
    FROM BDH
    FULL OUTER JOIN CDH
    ON {join_condition}
    WHERE 
        coalesce(BDH.`{col_name}`, '') != coalesce(CDH.`{col_name}`, '')
    """

    # Execute the SQL queries
    try:
        exclude_null_df = spark.sql(exclude_null_query)
        include_null_df = spark.sql(include_null_query)
    except Exception as e:
        logging.error(f"Error executing column comparison queries: {e}")
        spark.stop()
        exit(1)

    # Convert to Pandas DataFrame for logging
    exclude_null_pandas_df = exclude_null_df.limit(10).toPandas()
    include_null_pandas_df = include_null_df.limit(10).toPandas()

    # Log results
    if not exclude_null_pandas_df.empty:
        mismatch_count_exclude_null = exclude_null_df.count()
        total_mismatches_exclude_null += mismatch_count_exclude_null
        logging.info("\n" + "-"*80)
        logging.info(f"** Excluding NULL vs Non-NULL Mismatches for Column: {col_name} **")
        logging.info(f"Mismatch count for column '{col_name}': {mismatch_count_exclude_null}")

        # Manually log rows in a table-like format
        headers = list(exclude_null_pandas_df.columns)
        header_row = " | ".join(f"{header:<15}" for header in headers)
        separator = "-" * len(header_row)
        logging.info("\n" + separator)
        logging.info(header_row)
        logging.info(separator)
        for index, row in exclude_null_pandas_df.iterrows():
            row_data = " | ".join(f"{str(value):<15}" for value in row)
            logging.info(row_data)
        logging.info(separator)

    if not include_null_pandas_df.empty:
        mismatch_count_include_null = include_null_df.count()
        total_mismatches_include_null += mismatch_count_include_null
        logging.info("\n" + "-"*80)
        logging.info(f"** Including NULL vs Non-NULL Mismatches for Column: {col_name} **")
        logging.info(f"Mismatch count for column '{col_name}': {mismatch_count_include_null}")

        # Manually log rows in a table-like format
        headers = list(include_null_pandas_df.columns)
        header_row = " | ".join(f"{header:<15}" for header in headers)
        separator = "-" * len(header_row)
        logging.info("\n" + separator)
        logging.info(header_row)
        logging.info(separator)
        for index, row in include_null_pandas_df.iterrows():
            row_data = " | ".join(f"{str(value):<15}" for value in row)
            logging.info(row_data)
        logging.info(separator)

# Final section for total mismatch counts
logging.info("\n" + "="*80)
logging.info(f"Total mismatches (excluding NULL vs Non-NULL): {total_mismatches_exclude_null}")
logging.info(f"Total mismatches (including NULL vs Non-NULL): {total_mismatches_include_null}")

# Stop the Spark session
spark.stop()
