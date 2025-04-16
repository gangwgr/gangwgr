#!/bin/bash

# Usage: ./archive_partitioned_table.sh mydb.mytable 2024-01-01 2024-01-31

set -euo pipefail

LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/archive_$(date +%Y%m%d_%H%M%S).log"

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

error_exit() {
  log "❌ ERROR: $1"
  exit 1
}

# Input args
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <database.table> <start_date> <end_date>" | tee -a "$LOG_FILE"
  exit 1
fi

SOURCE_TABLE="$1"
START_DATE="$2"
END_DATE="$3"

DB_NAME="${SOURCE_TABLE%%.*}"
TABLE_NAME="${SOURCE_TABLE#*.}"
ARCHIVE_TABLE="${TABLE_NAME}_archive"

log "▶ Starting archival for: $SOURCE_TABLE (From $START_DATE to $END_DATE)"

# Step 1: Validate table exists
TABLE_EXISTS=$(hive -S -e "USE ${DB_NAME}; SHOW TABLES LIKE '${TABLE_NAME}';")
if [ -z "$TABLE_EXISTS" ]; then
  error_exit "Source table ${DB_NAME}.${TABLE_NAME} does not exist."
fi

# Step 2: Detect partition column(s)
PART_COL=$(hive -e "USE ${DB_NAME}; SHOW CREATE TABLE ${TABLE_NAME};" \
  | grep -i "PARTITIONED BY" \
  | sed -E 's/.*PARTITIONED BY\s+\(([^)]+)\).*/\1/' \
  | awk -F' ' '{print $1}')

if [ -z "$PART_COL" ]; then
  error_exit "No partition column found in ${DB_NAME}.${TABLE_NAME}"
fi

log "✔ Detected partition column: $PART_COL"

# Step 3: Create archival table
log "📦 Creating archival table ${DB_NAME}.${ARCHIVE_TABLE} (if not exists)..."
hive -e "USE ${DB_NAME}; CREATE TABLE IF NOT EXISTS ${ARCHIVE_TABLE} LIKE ${TABLE_NAME};" \
  >> "$LOG_FILE" 2>&1 || error_exit "Failed to create archive table."

# Step 4: Insert data with partitions
log "📝 Inserting data into ${ARCHIVE_TABLE} for date range ${START_DATE} to ${END_DATE}..."

hive -e "
USE ${DB_NAME};
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE ${ARCHIVE_TABLE} PARTITION (${PART_COL})
SELECT * FROM ${TABLE_NAME}
WHERE ${PART_COL} BETWEEN '${START_DATE}' AND '${END_DATE}';
" >> "$LOG_FILE" 2>&1 || error_exit "Insert into archive table failed."

log "✅ Archival completed successfully. Log file: $LOG_FILE"
