#!/bin/bash

# Simple Parquet to CSV Converter
# One-liner style script for quick conversions

set -euo pipefail

# Check if file is provided
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <parquet_file> [output_csv_file]"
    echo ""
    echo "Examples:"
    echo "  $0 data.parquet"
    echo "  $0 data.parquet output.csv"
    echo ""
    echo "Available methods:"
    echo "  1. Python pandas (default)"
    echo "  2. parquet-tools"
    echo "  3. DuckDB"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="${2:-$(basename "$INPUT_FILE" .parquet).csv}"

# Check if input file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: File '$INPUT_FILE' not found"
    exit 1
fi

echo "Converting: $INPUT_FILE -> $OUTPUT_FILE"

# Method 1: Python pandas (most common)
if command -v python3 &> /dev/null && python3 -c "import pandas" &> /dev/null 2>&1; then
    echo "Using Python pandas..."
    python3 -c "import pandas as pd; df = pd.read_parquet('$INPUT_FILE'); df.to_csv('$OUTPUT_FILE', index=False); print(f'Success: {len(df)} rows, {len(df.columns)} columns')"
    exit 0
fi

# Method 2: parquet-tools
if command -v parquet-tools &> /dev/null; then
    echo "Using parquet-tools..."
    parquet-tools csv "$INPUT_FILE" > "$OUTPUT_FILE"
    echo "Success: Converted with parquet-tools"
    exit 0
fi

# Method 3: DuckDB
if command -v duckdb &> /dev/null; then
    echo "Using DuckDB..."
    duckdb -c "COPY (SELECT * FROM read_parquet('$INPUT_FILE')) TO '$OUTPUT_FILE' WITH (FORMAT CSV, HEADER TRUE);"
    echo "Success: Converted with DuckDB"
    exit 0
fi

# Method 4: Python pyarrow
if command -v python3 &> /dev/null && python3 -c "import pyarrow" &> /dev/null 2>&1; then
    echo "Using Python pyarrow..."
    python3 -c "import pyarrow.parquet as pq; import pyarrow.csv as csv; table = pq.read_table('$INPUT_FILE'); csv.write_csv(table, '$OUTPUT_FILE'); print(f'Success: {len(table)} rows, {len(table.column_names)} columns')"
    exit 0
fi

echo "Error: No conversion method available. Please install one of:"
echo "  - Python with pandas: pip install pandas pyarrow"
echo "  - parquet-tools: pip install parquet-tools"
echo "  - DuckDB: https://duckdb.org/docs/installation/"
exit 1
