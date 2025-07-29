#!/usr/bin/env python3
"""
Mercury OB Data Extract
Chinese data filtering and processing script
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_filter_values(filter_df, column_name):
    """Extract comma-separated values for a specific column"""
    row = filter_df.filter(col("column_name") == column_name).collect()
    if row:
        values_str = row[0]["col_values"]
        return [val.strip() for val in values_str.split(',')]
    return []

def build_chinese_filter(data_df, chinese_columns, filter_values_dict):
    """Build dynamic filter conditions based on chinese columns"""
    filter_conditions = []
    
    for column_name in chinese_columns:
        if column_name in filter_values_dict:
            filter_values = filter_values_dict[column_name]
            column_condition = col(column_name).isin(filter_values)
            filter_conditions.append(column_condition)
            logger.info(f"Added Chinese filter for {column_name}: {len(filter_values)} values")
    
    # Combine all conditions with OR
    if filter_conditions:
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition = combined_condition | condition
        return data_df.filter(combined_condition)
    else:
        return data_df.filter(lit(False))  # No matching records

def build_non_chinese_filter(data_df, chinese_columns, filter_values_dict):
    """Build dynamic filter conditions for non-chinese data"""
    filter_conditions = []
    
    for column_name in chinese_columns:
        if column_name in filter_values_dict:
            filter_values = filter_values_dict[column_name]
            column_condition = ~col(column_name).isin(filter_values)
            filter_conditions.append(column_condition)
            logger.info(f"Added Non-Chinese filter for {column_name}: {len(filter_values)} values")
    
    # Combine all conditions with AND
    if filter_conditions:
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition = combined_condition & condition
        return data_df.filter(combined_condition)
    else:
        return data_df  # Return all records if no filters

def process_chinese_data(spark, data_df, chinese_flag, chinese_data_attributes, business_date):
    """Main function to process Chinese and non-Chinese data"""
    logs = []
    
    try:
        # Parse the chinese_data_attributes from your config
        chinese_columns = [col.strip() for col in chinese_data_attributes.split(',')]
        logs.append(f"Chinese filter columns: {chinese_columns}")
        
        # Read the Chinese filter values table
        chinese_filter_df = spark.read.option("header", "true").option("delimiter", "|").csv("/tenant/ibpt/prod/chinese_filter_values.txt")
        logs.append(chinese_filter_df._jdf.showString(100, 100, False))
        
        filter_values_dict = {}
        
        # Iterate through chinese_columns to get filter values dynamically
        for column_name in chinese_columns:
            filter_values = get_filter_values(chinese_filter_df, column_name)
            if filter_values:  # Only add if values exist
                filter_values_dict[column_name] = filter_values
                logs.append(f"{column_name} values count: {len(filter_values)}")
            else:
                logs.append(f"No filter values found for column: {column_name}")
        
        # Process data based on chinese_flag
        if chinese_flag == 'CN':
            chinese_data_df = build_chinese_filter(data_df, chinese_columns, filter_values_dict)
            non_chinese_data_df = build_non_chinese_filter(data_df, chinese_columns, filter_values_dict)
            
            logs.append(chinese_data_df._jdf.showString(100, 100, False))
            logs.append(non_chinese_data_df._jdf.showString(100, 100, False))
            
            # Get counts
            non_chinese_data_count = non_chinese_data_df.count()
            chinese_data_count = chinese_data_df.count()
            
            logs.append("chinese_data_count input row count: " + str(chinese_data_count))
            logs.append("Non-chinese_data_count input row count: " + str(non_chinese_data_count))
            
            return chinese_data_df, non_chinese_data_df, logs
        else:
            logger.warning(f"Unsupported chinese_flag: {chinese_flag}")
            return data_df, data_df, logs
            
    except Exception as e:
        logger.error(f"Error processing Chinese data: {e}")
        logs.append(f"Error: {str(e)}")
        return data_df, data_df, logs

def main():
    """Main function"""
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("MercuryOBDataExtract") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Example usage
        logger.info("Starting Mercury OB Data Extract")
        
        # Your data processing logic here
        # data_df = spark.read.parquet("your_data_path")
        # chinese_flag = "CN"
        # chinese_data_attributes = "column1,column2,column3"
        # business_date = "2025-01-01"
        
        # chinese_df, non_chinese_df, logs = process_chinese_data(
        #     spark, data_df, chinese_flag, chinese_data_attributes, business_date
        # )
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 
