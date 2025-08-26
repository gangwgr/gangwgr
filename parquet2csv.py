#!/bin/bash

# Quick Parquet to CSV Converter
# Simple script for converting parquet files to CSV format

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date +'%H:%M:%S')] $*${NC}"; }
success() { echo -e "${GREEN}✅ $*${NC}"; }
error() { echo -e "${RED}❌ $*${NC}"; }

# Configuration
INPUT_FILE="${1:-}"
OUTPUT_FILE="${2:-}"

# Show usage
usage() {
    echo "Usage: $0 <input_parquet_file> [output_csv_file]"
    echo ""
    echo "Examples:"
    echo "  $0 data.parquet                    # Convert to data.csv"
    echo "  $0 data.parquet output.csv         # Convert to specified output file"
    echo "  $0 *.parquet                       # Convert all parquet files in current directory"
    echo ""
    echo "Dependencies:"
    echo "  - Python 3 with pandas (recommended)"
    echo "  - or parquet-tools"
    echo "  - or DuckDB"
}

# Check if input file is provided
if [[ -z "$INPUT_FILE" ]]; then
    error "No input file specified"
    usage
    exit 1
fi

# Check for help flag
if [[ "$INPUT_FILE" == "-h" || "$INPUT_FILE" == "--help" ]]; then
    usage
    exit 0
fi

# Function to convert single file
convert_single_file() {
    local input="$1"
    local output="$2"
    
    log "Converting: $input -> $output"
    
    # Method 1: Python pandas (most common)
    if command -v python3 &> /dev/null && python3 -c "import pandas" &> /dev/null; then
        log "Using Python pandas..."
        python3 -c "
import pandas as pd
try:
    df = pd.read_parquet('$input')
    df.to_csv('$output', index=False)
    print(f'Success: {len(df)} rows, {len(df.columns)} columns')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    exit(1)
"
        if [[ $? -eq 0 ]]; then
            success "Conversion completed with pandas"
            return 0
        fi
    fi
    
    # Method 2: parquet-tools
    if command -v parquet-tools &> /dev/null; then
        log "Using parquet-tools..."
        parquet-tools csv "$input" > "$output"
        if [[ $? -eq 0 ]]; then
            success "Conversion completed with parquet-tools"
            return 0
        fi
    fi
    
    # Method 3: DuckDB
    if command -v duckdb &> /dev/null; then
        log "Using DuckDB..."
        duckdb -c "COPY (SELECT * FROM read_parquet('$input')) TO '$output' WITH (FORMAT CSV, HEADER TRUE);"
        if [[ $? -eq 0 ]]; then
            success "Conversion completed with DuckDB"
            return 0
        fi
    fi
    
    # Method 4: Python pyarrow
    if command -v python3 &> /dev/null && python3 -c "import pyarrow" &> /dev/null; then
        log "Using Python pyarrow..."
        python3 -c "
import pyarrow.parquet as pq
import pyarrow.csv as csv
try:
    table = pq.read_table('$input')
    csv.write_csv(table, '$output')
    print(f'Success: {len(table)} rows, {len(table.column_names)} columns')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    exit(1)
"
        if [[ $? -eq 0 ]]; then
            success "Conversion completed with pyarrow"
            return 0
        fi
    fi
    
    error "No conversion method available. Please install pandas, parquet-tools, or DuckDB."
    return 1
}

# Main conversion logic
main() {
    # Handle wildcard input
    if [[ "$INPUT_FILE" == *"*"* ]]; then
        log "Processing multiple files matching pattern: $INPUT_FILE"
        
        # Find matching files
        files=()
        while IFS= read -r -d '' file; do
            files+=("$file")
        done < <(find . -name "$INPUT_FILE" -type f -print0 2>/dev/null)
        
        if [[ ${#files[@]} -eq 0 ]]; then
            error "No files found matching pattern: $INPUT_FILE"
            exit 1
        fi
        
        success "Found ${#files[@]} file(s) to convert"
        
        # Convert each file
        for file in "${files[@]}"; do
            base_name=$(basename "$file" .parquet)
            output_file="${base_name}.csv"
            
            echo ""
            if convert_single_file "$file" "$output_file"; then
                success "Converted: $file -> $output_file"
            else
                error "Failed to convert: $file"
            fi
        done
        
    else
        # Single file conversion
        if [[ ! -f "$INPUT_FILE" ]]; then
            error "Input file not found: $INPUT_FILE"
            exit 1
        fi
        
        # Determine output filename
        if [[ -z "$OUTPUT_FILE" ]]; then
            base_name=$(basename "$INPUT_FILE" .parquet)
            OUTPUT_FILE="${base_name}.csv"
        fi
        
        # Convert the file
        if convert_single_file "$INPUT_FILE" "$OUTPUT_FILE"; then
            success "Conversion completed: $INPUT_FILE -> $OUTPUT_FILE"
        else
            error "Conversion failed"
            exit 1
        fi
    fi
}

# Run main function
main "$@"
