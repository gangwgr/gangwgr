from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, concat_ws
import argparse

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

# Register DataFrames as SQL tables
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Define primary keys
primary_keys = args.primary_keys.split(",")
#primary_keys = ["iw", "num", "DOL_Vehicle_ID"]
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
    print(f"Error executing query: {e}")
    spark.stop()
    exit(1)

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
"""

# Compare columns and add details to the report
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
        print(f"Error executing column comparison queries: {e}")
        spark.stop()
        exit(1)

    # Convert to Pandas DataFrame for HTML generation
    exclude_null_pandas_df = exclude_null_df.toPandas()
    include_null_pandas_df = include_null_df.toPandas()

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
                    {"".join([f"<th>df1_{key}</th><th>df2_{key}</th>" for key in primary_keys])}
                    <th>df1_value</th>
                    <th>df2_value</th>
                </tr>
            </thead>
            <tbody>
        """
        for _, row in exclude_null_pandas_df.iterrows():
            html_report += "<tr>"
            for key in primary_keys:
                html_report += f"<td>{row[f'df1_{key}']}</td><td>{row[f'df2_{key}']}</td>"
            html_report += f"<td>{row['df1_value']}</td>"
            html_report += f"<td>{row['df2_value']}</td>"
            html_report += "</tr>"
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
                    {"".join([f"<th>df1_{key}</th><th>df2_{key}</th>" for key in primary_keys])}
                    <th>df1_value</th>
                    <th>df2_value</th>
                </tr>
            </thead>
            <tbody>
        """
        for _, row in include_null_pandas_df.iterrows():
            html_report += "<tr>"
            for key in primary_keys:
                html_report += f"<td>{row[f'df1_{key}']}</td><td>{row[f'df2_{key}']}</td>"
            html_report += f"<td>{row['df1_value']}</td>"
            html_report += f"<td>{row['df2_value']}</td>"
            html_report += "</tr>"
        html_report += "</tbody></table>"

# Final section for total mismatch counts
html_report += f"""
    <h2>Total Mismatches</h2>
    <p>Total mismatches (excluding NULL vs Non-NULL): {total_mismatches_exclude_null}</p>
    <p>Total mismatches (including NULL vs Non-NULL): {total_mismatches_include_null}</p>
</body>
</html>
"""

# Save the HTML report
with open(args.report_path, "w") as f:
    f.write(html_report)

# Stop the Spark session
spark.stop()

