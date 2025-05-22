#!/bin/bash

# handle_error_and_exit: A reusable function to handle errors and exit.
# Arguments:
#   $1: A descriptive error message.
#   $2: (Optional) The context or function name where the error occurred.
function handle_error_and_exit() {
    local error_message="$1"
    local context="$2"

    if [[ -n "$context" ]]; then
        echo "ERROR in $context: $error_message" >&2 # Output to stderr
    else
        echo "ERROR: $error_message" >&2 # Output to stderr
    fi
    exit 1 # Exit with a non-zero status code to indicate failure
}

# --- Example Functions ---

# check_file_exists: Checks if a file exists.
# Calls handle_error_and_exit if the file is not found.
function check_file_exists() {
    local filename="$1"
    local func_name="${FUNCNAME[0]}" # Get current function name

    if [[ -z "$filename" ]]; then
        handle_error_and_exit "Filename not provided." "$func_name"
    fi

    if [[ ! -f "$filename" ]]; then
        handle_error_and_exit "File '$filename' not found." "$func_name"
    fi

    echo "File '$filename' found successfully."
    return 0 # Indicate success
}

# process_data: Simulates a data processing step.
# Calls handle_error_and_exit if processing fails.
function process_data() {
    local input_value="$1"
    local func_name="${FUNCNAME[0]}" # Get current function name

    if [[ -z "$input_value" ]]; then
        handle_error_and_exit "Input value not provided for processing." "$func_name"
    fi

    if (( input_value < 0 )); then
        handle_error_and_exit "Input value cannot be negative: $input_value" "$func_name"
    fi

    echo "Processing data: $input_value"
    # Simulate some processing that might fail
    if (( input_value % 2 != 0 )); then
        handle_error_and_exit "Data processing failed for odd number: $input_value" "$func_name"
    fi

    echo "Data processed successfully."
    return 0 # Indicate success
}

# --- Main Script Execution ---

echo "--- Starting Script ---"

# Scenario 1: File exists (success)
check_file_exists "/etc/passwd"
echo "---"

# Scenario 2: File does not exist (will call handle_error_and_exit and exit)
# Uncomment the line below to test this scenario
 check_file_exists 
# echo "This line will not be reached if the above command exits." # This won't be printed

# Scenario 3: Process data (success)
process_data 10
echo "---"

# Scenario 4: Process data (will call handle_error_and_exit and exit)
# Uncomment the line below to test this scenario
# process_data -5
# echo "This line will not be reached if the above command exits."

# Scenario 5: Process data (will call handle_error_and_exit and exit due to odd number)
# Uncomment the line below to test this scenario
# process_data 7
# echo "This line will not be reached if the above command exits."

echo "--- Script Finished Successfully ---"
