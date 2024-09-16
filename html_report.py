import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, broadcast, coalesce, lit
import os

# Function to compare two CSV files and write mismatch details to an HTML file
def compare_csv(file1, file2, primary_keys, output_html):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

    # Load data from CSV files
    df1 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(file1)
    df2 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(file2)

    # Open the HTML file for writing
    with open(output_html, 'w') as html_file:
        # Write the initial part of the HTML file
        html_file.write("<html><head><title>CSV Comparison Mismatches</title></head><body>")
        html_file.write(f"<h1>Mismatch Report for {os.path.basename(file1)} and {os.path.basename(file2)}</h1>")
        
        # Display schema
        html_file.write("<h2>Schemas</h2>")
        html_file.write("<h3>Schema for file 1:</h3><pre>")
        df1.printSchema()
        html_file.write("</pre><h3>Schema for file 2:</h3><pre>")
        df2.printSchema()
        html_file.write("</pre>")

        # Ensure both DataFrames have the primary keys
        for key in primary_keys:
            if key not in df1.columns or key not in df2.columns:
                raise ValueError(f"Primary key '{key}' is missing from one of the DataFrames")

        html_file.write(f"<p>Using {primary_keys} as the composite primary key for comparison.</p>")

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
        html_file.write("<h2>Mismatch Details</h2>")
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
                html_file.write(f"<h3>Mismatch count for column '{col_name}': {mismatch_count}</h3>")

                # Select the primary key values from both files and the mismatched column values
                mismatch_data = mismatches_df.select(
                    [col(f"{key}_left").alias(f"{key}_df1") for key in primary_keys] + 
                    [col(f"{key}_right").alias(f"{key}_df2") for key in primary_keys] + 
                    [col(f"{col_name}_left").alias("df1_value"), col(f"{col_name}_right").alias("df2_value")]
                ).collect()

                # Write mismatch details into the HTML file as a table
                html_file.write("<table border='1'><tr>")
                for key in primary_keys:
                    html_file.write(f"<th>{key}_df1</th><th>{key}_df2</th>")
                html_file.write(f"<th>df1_value</th><th>df2_value</th></tr>")

                for row in mismatch_data:
                    html_file.write("<tr>")
                    for key in primary_keys:
                        html_file.write(f"<td>{row[f'{key}_df1']}</td><td>{row[f'{key}_df2']}</td>")
                    html_file.write(f"<td>{row['df1_value']}</td><td>{row['df2_value']}</td></tr>")
                html_file.write("</table>")

        html_file.write(f"<p><strong>Total mismatches across all columns:</strong> {total_mismatches}</p>")
        html_file.write("</body></html>")

    # Stop the Spark session
    spark.stop()

# Main function to handle command-line arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two CSV files based on specified primary keys.")
    
    # Define arguments for the file paths and primary keys
    parser.add_argument("--file1", required=True, help="Path to the first CSV file")
    parser.add_argument("--file2", required=True, help="Path to the second CSV file")
    parser.add_argument("--primary-keys", required=True, nargs="+", help="List of primary keys for comparison")
    parser.add_argument("--output-html", required=True, help="Path to the output HTML file for mismatch details")

    # Parse the arguments
    args = parser.parse_args()

    # Call the compare function with the provided arguments
    compare_csv(args.file1, args.file2, args.primary_keys, args.output_html)


# to run python compare_csv.py --file1 /path/to/file1.csv --file2 /path/to/file2.csv --primary-keys id num --output-html /path/to/output/mismatch_report.html
