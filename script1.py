from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, broadcast, coalesce, lit
import argparse
import pandas as pd

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Compare two CSV files.")
parser.add_argument("--file1", required=True, help="Path to the first CSV file")
parser.add_argument("--file2", required=True, help="Path to the second CSV file")
parser.add_argument("--primary-keys", required=True, help="Comma-separated list of primary key columns")
parser.add_argument("--report-path", required=True, help="Path to save the HTML report")
args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

# Load data
df1 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file1)
df2 = spark.read.option("header", True).option("delimiter", ",").option("quote", '"').option("escape", '"').csv(args.file2)

# Convert schema to HTML-friendly format
def schema_to_html(schema):
    fields = schema.fields
    schema_html = "<table><thead><tr><th>Field Name</th><th>Type</th><th>Nullable</th></tr></thead><tbody>"
    for field in fields:
        schema_html += f"<tr><td>{field.name}</td><td>{field.dataType.simpleString()}</td><td>{'Yes' if field.nullable else 'No'}</td></tr>"
    schema_html += "</tbody></table>"
    return schema_html

schema_html_df1 = schema_to_html(df1.schema)
schema_html_df2 = schema_to_html(df2.schema)

# Define primary keys
primary_keys = args.primary_keys.split(",")

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

# Prepare HTML report content
html_report = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CSV Comparison Report</title>
    <style>
        table {{ width: 100%; border-collapse: collapse; }}
        table, th, td {{ border: 1px solid black; }}
        th, td {{ padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        h2 {{ color: #333; }}
    </style>
</head>
<body>
    <h1>CSV Comparison Report</h1>

    <h2>Schema for df1:</h2>
    {schema_html_df1}

    <h2>Schema for df2:</h2>
    {schema_html_df2}

    <h2>Using {primary_keys} as the composite primary key for comparison.</h2>
"""

# Compare columns and add details to the report
total_mismatches_exclude_null = 0
total_mismatches_include_null = 0

for col_name in df1.columns:
    if col_name in primary_keys:
        continue  # Skip primary key columns for mismatch counting

    # Exclude cases where one column is NULL and the other is not
    mismatches_exclude_null_df = joined_df.filter(
        (coalesce(col(f"{col_name}_left"), lit("")) != coalesce(col(f"{col_name}_right"), lit(""))) &
        ~((col(f"{col_name}_left").isNull() & ~col(f"{col_name}_right").isNull()) |
          (~col(f"{col_name}_left").isNull() & col(f"{col_name}_right").isNull()))
    )
    
    # Include cases where one column is NULL and the other is not
    mismatches_include_null_df = joined_df.filter(
        (coalesce(col(f"{col_name}_left"), lit("")) != coalesce(col(f"{col_name}_right"), lit("")))
    )
    
    # Convert to Pandas DataFrame for HTML generation
    exclude_null_pandas_df = mismatches_exclude_null_df.toPandas()
    include_null_pandas_df = mismatches_include_null_df.toPandas()

    # Append to HTML report
    if not exclude_null_pandas_df.empty:
        mismatch_count_exclude_null = len(exclude_null_pandas_df)
        total_mismatches_exclude_null += mismatch_count_exclude_null
        html_report += f"""
        <h2>Exclude NULL vs Non-NULL Mismatches</h2>
        <h3>Column: {col_name}</h3>
        <p><strong>Mismatch count for column '{col_name}': {mismatch_count_exclude_null}</strong></p>
        <table>
            <thead>
                <tr>
                    <th>id_df1</th>
                    <th>id_df2</th>
                    <th>df1_value</th>
                    <th>df2_value</th>
                </tr>
            </thead>
            <tbody>
        """
        for _, row in exclude_null_pandas_df.iterrows():
            html_report += f"""
            <tr>
                <td>{row[f"{primary_keys[0]}_left"]}</td>
                <td>{row[f"{primary_keys[0]}_right"]}</td>
                <td>{row[f"{col_name}_left"]}</td>
                <td>{row[f"{col_name}_right"]}</td>
            </tr>
            """
        html_report += "</tbody></table>"

    if not include_null_pandas_df.empty:
        mismatch_count_include_null = len(include_null_pandas_df)
        total_mismatches_include_null += mismatch_count_include_null
        html_report += f"""
        <h2>Include NULL vs Non-NULL Mismatches</h2>
        <h3>Column: {col_name}</h3>
        <p><strong>Mismatch count for column '{col_name}': {mismatch_count_include_null}</strong></p>
        <table>
            <thead>
                <tr>
                    <th>id_df1</th>
                    <th>id_df2</th>
                    <th>df1_value</th>
                    <th>df2_value</th>
                </tr>
            </thead>
            <tbody>
        """
        for _, row in include_null_pandas_df.iterrows():
            html_report += f"""
            <tr>
                <td>{row[f"{primary_keys[0]}_left"]}</td>
                <td>{row[f"{primary_keys[0]}_right"]}</td>
                <td>{row[f"{col_name}_left"]}</td>
                <td>{row[f"{col_name}_right"]}</td>
            </tr>
            """
        html_report += "</tbody></table>"

html_report += f"""
    <h2>Total Mismatches</h2>
    <p>Total mismatches (excluding NULL vs Non-NULL): {total_mismatches_exclude_null}</p>
    <p>Total mismatches (including NULL vs Non-NULL): {total_mismatches_include_null}</p>
</body>
</html>
"""

# Save the HTML report to the specified path
report_path = f"{args.report_path}/csv_comparison_report.html"
with open(report_path, "w") as file:
    file.write(html_report)

print(f"HTML report saved to {report_path}")

# Stop the Spark session
spark.stop()

