import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, broadcast, coalesce, lit

# Function to compare two CSV files
def compare_csv(file1, file2, primary_keys):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

    # Load data from CSV files
    df1 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(file1)
    df2 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(file2)

    # Display schema
    print(f"Schema for {file1}:")
    df1.printSchema()
    print(f"Schema for {file2}:")
    df2.printSchema()

    # Ensure both DataFrames have the primary keys
    for key in primary_keys:
        if key not in df1.columns or key not in df2.columns:
            raise ValueError(f"Primary key '{key}' is missing from one of the DataFrames")

    print(f"Using {primary_keys} as the composite primary key for comparison.")

    # Alias columns to avoid conflicts
    df1_alias = df1.select([col(c).alias(f"{c}_left") for c in df1.columns])
    df2_alias = df2.select([col(c).alias(f"{c}_right") for c in df2.columns])

    # Create join keys
    df1_join_key = concat_ws('-', *[col(f"{k}_left") for k in primary_keys])
    df2_join_key = concat_ws('-', *[col(f"{k}_right") for k in primary_keys])

    # Join dataframes with broadcasting if one is small
    joined_df = df1_alias.withColumn("join_key", df1_join_key) \
                       .join(broadcast(df2_alias.withColumn("join_key", df2_join_key)), "join_key", how="outer") \
                       .drop("join_key")

    # Compare columns and count mismatches, treating NULLs as equal
    total_mismatches = 0
    for col_name in df1.columns:
        if col_name in primary_keys:
            continue  # Skip primary key columns for mismatch counting

        # Handle NULLs properly
        mismatches_df = joined_df.filter(
            (coalesce(col(f"{col_name}_left"), lit("")) != coalesce(col(f"{col_name}_right"), lit(""))) |
            (col(f"{col_name}_left").isNull() & ~col(f"{col_name}_right").isNull()) |
            (~col(f"{col_name}_left").isNull() & col(f"{col_name}_right").isNull())
        )

        mismatch_count = mismatches_df.count()
        if mismatch_count > 0:
            total_mismatches += mismatch_count
            print(f"Mismatch count for column '{col_name}': {mismatch_count}")

            # Select the primary key values from both files and the mismatched column values
            mismatches_df.select([col(f"{key}_left").alias(f"{key}_df1") for key in primary_keys] + 
                                 [col(f"{key}_right").alias(f"{key}_df2") for key in primary_keys] + 
                                 [col(f"{col_name}_left").alias("df1_value"), col(f"{col_name}_right").alias("df2_value")]).show()

    print(f"Total mismatches across all columns: {total_mismatches}")

    # Stop the Spark session
    spark.stop()

# Main function to handle command-line arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two CSV files based on specified primary keys.")
    
    # Define arguments for the file paths and primary keys
    parser.add_argument("--file1", required=True, help="Path to the first CSV file")
    parser.add_argument("--file2", required=True, help="Path to the second CSV file")
    parser.add_argument("--primary-keys", required=True, nargs="+", help="List of primary keys for comparison")

    # Parse the arguments
    args = parser.parse_args()

    # Call the compare function with the provided arguments
    compare_csv(args.file1, args.file2, args.primary_keys)


# to call python compare_csv.py --file1 /path/to/file1.csv --file2 /path/to/file2.csv --primary-keys id num