#!/usr/bin/env python3
"""
Mercury OB Data Extract - Simplified Version
Chinese data filtering and processing script using pandas
"""

import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_filter_values(filter_df, column_name):
    """Extract comma-separated values for a specific column"""
    row = filter_df[filter_df['column_name'] == column_name]
    if not row.empty:
        values_str = row.iloc[0]['col_values']
        return [val.strip() for val in values_str.split(',')]
    return []

def build_chinese_filter(data_df, chinese_columns, filter_values_dict):
    """Build dynamic filter conditions based on chinese columns"""
    filter_conditions = []
    
    for column_name in chinese_columns:
        if column_name in filter_values_dict:
            filter_values = filter_values_dict[column_name]
            # Create pandas filter condition
            condition = data_df[column_name].isin(filter_values)
            filter_conditions.append(condition)
            logger.info(f"Added Chinese filter for {column_name}: {len(filter_values)} values")
    
    # Combine all conditions with OR
    if filter_conditions:
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition = combined_condition | condition
        return data_df[combined_condition]
    else:
        return pd.DataFrame()  # No matching records

def build_non_chinese_filter(data_df, chinese_columns, filter_values_dict):
    """Build dynamic filter conditions for non-chinese data"""
    filter_conditions = []
    
    for column_name in chinese_columns:
        if column_name in filter_values_dict:
            filter_values = filter_values_dict[column_name]
            # Create pandas filter condition (NOT IN)
            condition = ~data_df[column_name].isin(filter_values)
            filter_conditions.append(condition)
            logger.info(f"Added Non-Chinese filter for {column_name}: {len(filter_values)} values")
    
    # Combine all conditions with AND
    if filter_conditions:
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition = combined_condition & condition
        return data_df[combined_condition]
    else:
        return data_df  # Return all records if no filters

def process_chinese_data(data_df, chinese_flag, chinese_data_attributes, business_date):
    """Main function to process Chinese and non-Chinese data"""
    logs = []
    
    try:
        # Parse the chinese_data_attributes from your config
        chinese_columns = [col.strip() for col in chinese_data_attributes.split(',')]
        logs.append(f"Chinese filter columns: {chinese_columns}")
        
        # Read the Chinese filter values table
        chinese_filter_df = pd.read_csv("chinese_filter_values.txt", delimiter="|")
        logs.append(f"Filter values loaded: {len(chinese_filter_df)} rows")
        
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
            
            logs.append(f"Chinese data shape: {chinese_data_df.shape}")
            logs.append(f"Non-Chinese data shape: {non_chinese_data_df.shape}")
            
            # Get counts
            non_chinese_data_count = len(non_chinese_data_df)
            chinese_data_count = len(chinese_data_df)
            
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

def create_sample_data():
    """Create sample data for testing"""
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8],
        'country': ['China', 'USA', 'Chinese', 'Japan', 'CN', 'Germany', 'China', 'USA'],
        'language': ['Chinese', 'English', 'Mandarin', 'Japanese', 'Cantonese', 'German', 'English', 'Chinese'],
        'region': ['Asia', 'North America', 'East Asia', 'Asia', 'Southeast Asia', 'Europe', 'Asia', 'North America'],
        'amount': [1000, 2000, 1500, 3000, 800, 2500, 1200, 1800]
    }
    return pd.DataFrame(data)

def main():
    """Main function"""
    try:
        logger.info("Starting Mercury OB Data Extract (Simplified)")
        
        # Create sample data
        data_df = create_sample_data()
        logger.info(f"Sample data created: {data_df.shape}")
        
        # Test parameters
        chinese_flag = "CN"
        chinese_data_attributes = "country,language,region"
        business_date = "2025-01-01"
        
        logger.info(f"Processing with parameters:")
        logger.info(f"  Chinese Flag: {chinese_flag}")
        logger.info(f"  Chinese Attributes: {chinese_data_attributes}")
        logger.info(f"  Business Date: {business_date}")
        
        # Process the data
        chinese_df, non_chinese_df, logs = process_chinese_data(
            data_df, chinese_flag, chinese_data_attributes, business_date
        )
        
        # Display results
        logger.info(f"Results:")
        logger.info(f"  Chinese data count: {len(chinese_df)}")
        logger.info(f"  Non-Chinese data count: {len(non_chinese_df)}")
        
        print("\n🇨🇳 Chinese Data:")
        print(chinese_df)
        
        print("\n🌍 Non-Chinese Data:")
        print(non_chinese_df)
        
        # Display logs
        print(f"\n📝 Processing Logs:")
        for log in logs:
            print(f"  {log}")
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
