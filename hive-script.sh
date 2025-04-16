# create hive table script
#!/bin/bash

SOURCE_TABLE="your_db.source_table"
TARGET_TABLE="your_db.duplicate_table"

# Function to check if table exists
check_table_exists() {
    local table=$1
    local exists=$(hive -S -e "SHOW TABLES IN ${table%.*} LIKE '${table#*.}';" 2>/dev/null | grep -w "${table#*.}")

    if [[ -z "$exists" ]]; then
        echo "❌ Source table ${table} does not exist."
        return 1
    else
        echo "✅ Source table ${table} found."
        return 0
    fi
}

# Main logic
if check_table_exists "$SOURCE_TABLE"; then
    echo "👉 Proceeding to create duplicate table: $TARGET_TABLE"
    hive -e "
    CREATE TABLE ${TARGET_TABLE} AS 
    SELECT * FROM ${SOURCE_TABLE};
    "

    if [[ $? -eq 0 ]]; then
        echo "✅ Duplicate table ${TARGET_TABLE} created successfully."
    else
        echo "❌ Failed to create duplicate table ${TARGET_TABLE}."
        exit 1
    fi
else
    echo "⚠️ Skipping table creation as source does not exist."
    exit 1
fi
