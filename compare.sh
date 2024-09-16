#!/bin/bash

# Check if the correct number of arguments is passed
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <file1> <file2> <primary_keys>"
    echo "Example: $0 /path/to/file1.csv /path/to/file2.csv 'id num'"
    exit 1
fi

# Assign arguments to variables
FILE1=$1
FILE2=$2
PRIMARY_KEYS=$3

# Execute the Python script with the provided arguments
python compare_csv.py --file1 "$FILE1" --file2 "$FILE2" --primary-keys "$PRIMARY_KEYS"

How to Use the Script:
Create the Shell Script:

Save the above content in a file named run_compare.sh.
Make the Script Executable: Run the following command to make the script executable:
chmod +x run_compare.sh

Run the Script with Arguments: You can now run the script by passing the file paths and primary keys as arguments:
./run_compare.sh /path/to/file1.csv /path/to/file2.csv "id num"
Example:
./run_compare.sh /data/final_data3.csv /data/final_data1.csv "id num"
Explanation:
$1, $2, and $3 refer to the positional arguments that are passed when you run the script.
The script ensures that at least 3 arguments are passed (2 file paths and primary keys).
