import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from datetime import datetime

# Generate unique filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f'comparison_log_{timestamp}.log'

# Set up logging (without INFO - prefix)
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(message)s')

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compare two CSV files.")
parser.add_argument("--file1", required=True, help="Path to the first CSV file")
parser.add_argument("--file2", required=True, help="Path to the second CSV file")
parser.add_argument("--primary-keys", required=True, help="Comma-separated list of primary key columns")
args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

# Load data
df1 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file1)
df2 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file2)

# Register DataFrames as SQL tables
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Define primary keys
primary_keys = args.primary_keys.split(",")
# Build join condition for multiple primary keys
join_condition = " AND ".join([f"df1.`{key}` = df2.`{key}`" for key in primary_keys])

# Create SQL query to compare data
join_query = f"""
SELECT 
    {", ".join([f"df1.`{key}` as df1_{key}, df2.`{key}` as df2_{key}" for key in primary_keys])},
    df1.*, 
    df2.*
FROM df1
FULL OUTER JOIN df2
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

for col_name in df1.columns:
    if col_name in primary_keys:
        continue  # Skip primary key columns for mismatch counting

    # Exclude cases where one column is NULL and the other is not
    exclude_null_query = f"""
    SELECT 
        {", ".join([f"df1.`{key}` as df1_{key}, df2.`{key}` as df2_{key}" for key in primary_keys])},
        df1.`{col_name}` as df1_value, 
        df2.`{col_name}` as df2_value
    FROM df1
    FULL OUTER JOIN df2
    ON {join_condition}
    WHERE 
        coalesce(df1.`{col_name}`, '') != coalesce(df2.`{col_name}`, '') 
        AND NOT ((df1.`{col_name}` IS NULL AND df2.`{col_name}` IS NOT NULL) 
                 OR (df1.`{col_name}` IS NOT NULL AND df2.`{col_name}` IS NULL))
    """

    # Include cases where one column is NULL and the other is not
    include_null_query = f"""
    SELECT 
        {", ".join([f"df1.`{key}` as df1_{key}, df2.`{key}` as df2_{key}" for key in primary_keys])},
        df1.`{col_name}` as df1_value, 
        df2.`{col_name}` as df2_value
    FROM df1
    FULL OUTER JOIN df2
    ON {join_condition}
    WHERE 
        coalesce(df1.`{col_name}`, '') != coalesce(df2.`{col_name}`, '')
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
    exclude_null_pandas_df = exclude_null_df.toPandas()
    include_null_pandas_df = include_null_df.toPandas()

    # Log results
    if not exclude_null_pandas_df.empty:
        mismatch_count_exclude_null = len(exclude_null_pandas_df)
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
        mismatch_count_include_null = len(include_null_pandas_df)
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

