from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, concat_ws
from itertools import combinations

# Initialize Spark session
spark = SparkSession.builder.appName("CompareCSV").getOrCreate()

# Load the data from CSV files (auto-infer schema and header)
df1 = spark.read.option("header", True).csv("/files1.csv")
df2 = spark.read.option("header", True).csv("/files2.csv")

# Display the schema
print("Schema for df1:")
df1.printSchema()

print("Schema for df2:")
df2.printSchema()

# Step 1: Identify potential primary keys by finding columns with unique values
def find_potential_primary_keys(df):
    potential_keys = []
    columns = df.columns
    for r in range(1, len(columns) + 1):
        for cols in combinations(columns, r):
            distinct_count = df.select([col(c) for c in cols]).distinct().count()
            total_count = df.count()
            if distinct_count == total_count:
                potential_keys.append(cols)
    return potential_keys

# Step 2: Find primary keys for both dataframes
potential_primary_keys_df1 = find_potential_primary_keys(df1)
potential_primary_keys_df2 = find_potential_primary_keys(df2)

print(f"Potential primary keys in df1: {potential_primary_keys_df1}")
print(f"Potential primary keys in df2: {potential_primary_keys_df2}")

# Step 3: Choose the common primary key set (for simplicity, using the first one that matches)
common_primary_key = None
for key_set1 in potential_primary_keys_df1:
    for key_set2 in potential_primary_keys_df2:
        if set(key_set1) == set(key_set2):
            common_primary_key = key_set1
            break
    if common_primary_key:
        break

if not common_primary_key:
    print("No common primary keys found!")
else:
    print(f"Using {common_primary_key} as the composite primary key for comparison.")

    # Step 4: Alias columns before the join to avoid column name conflicts
    df1_alias = df1.select([col(c).alias(f"{c}_left") for c in df1.columns])
    df2_alias = df2.select([col(c).alias(f"{c}_right") for c in df2.columns])

    # Create join keys based on the identified primary keys
    df1_join_key = concat_ws('-', *[col(f"{k}_left") for k in common_primary_key])
    df2_join_key = concat_ws('-', *[col(f"{k}_right") for k in common_primary_key])

    # Join the dataframes using the identified primary keys
    joined_df = df1_alias.withColumn("join_key", df1_join_key) \
                   .join(df2_alias.withColumn("join_key", df2_join_key), "join_key", how="outer") \
                   .drop("join_key")

    # Step 5: Compare column by column and count mismatches
    total_mismatches = 0
    for col_name in df1.columns:
        mismatches_df = joined_df.filter((col(f"{col_name}_left") != col(f"{col_name}_right")) | col(f"{col_name}_left").isNull() | col(f"{col_name}_right").isNull())
        mismatch_count = mismatches_df.count()
        if mismatch_count > 0:
            total_mismatches += mismatch_count
            print(f"Mismatch count for column '{col_name}': {mismatch_count}")
            mismatches_df.select(col(f"{col_name}_left").alias("df1"), col(f"{col_name}_right").alias("df2")).show()

    print(f"Total mismatches across all columns: {total_mismatches}")
