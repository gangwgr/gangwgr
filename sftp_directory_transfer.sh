#!/bin/bash

# SFTP Directory Transfer Script with Backup
# Usage: ./sftp_directory_transfer.sh [OPTIONS]
# This script transfers a directory from source server to target server using SFTP
# If target directory exists, it creates a timestamped backup before transfer

set -e  # Exit on any error

# Default values
SOURCE_PATH=""
TARGET_PATH=""
TARGET_HOST=""
BACKUP_SUFFIX="latest"
CURRENT_USER=$(whoami)

# Logging without colors

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Required Options:"
    echo "  --source-path PATH      Local source directory path"
    echo "  --target-path PATH      Target location on remote server (directory will be placed inside this)"
    echo "  --target-host HOST      Target server hostname/IP"
    echo ""
    echo "Note: Script uses current user ($CURRENT_USER) for SFTP connection"
    echo "Authentication: Uses default SFTP authentication (keys or password prompt)"
    echo ""
    echo "Examples:"
    echo "  $0 --source-path /home/user/myapp --target-path /opt --target-host 192.168.1.20"
    echo "  # → uploads /home/user/myapp to /opt/myapp on server"
    echo ""
    echo "  $0 --source-path /var/www/site --target-path /var/www --target-host server2.com"
    echo "  # → uploads /var/www/site to /var/www/site on server"
}

# Function to log messages
log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        "INFO")
            echo "[INFO] ${timestamp} - $message"
            ;;
        "WARN")
            echo "[WARN] ${timestamp} - $message"
            ;;
        "ERROR")
            echo "[ERROR] ${timestamp} - $message" >&2
            ;;

    esac
}

# Function to validate required parameters
validate_params() {
    local errors=0
    
    if [[ -z "$SOURCE_PATH" ]]; then
        log "ERROR" "Source path is required"
        ((errors++))
    fi
    
    if [[ ! -d "$SOURCE_PATH" ]]; then
        log "ERROR" "Source directory does not exist: $SOURCE_PATH"
        ((errors++))
    fi
    
    if [[ -z "$TARGET_PATH" ]]; then
        log "ERROR" "Target path is required"
        ((errors++))
    fi
    
    if [[ -z "$TARGET_HOST" ]]; then
        log "ERROR" "Target host is required"
        ((errors++))
    fi
    
    if [[ $errors -gt 0 ]]; then
        log "ERROR" "Validation failed with $errors error(s)"
        exit 1
    fi
}

# Cleanup function to remove temporary files
cleanup_temp_files() {
    rm -f /tmp/check_$$.out /tmp/backup_$$.out /tmp/verify_$$.out
}

# Set trap to cleanup on script exit
trap cleanup_temp_files EXIT

# No SSH options needed - using SFTP only

# Function to check if directory exists on remote server
check_remote_directory() {
    local host=$1
    local path=$2
    
    # Use SFTP to check if directory exists
    sftp $CURRENT_USER@$host << EOF > /tmp/check_$$.out 2>&1
ls $path
bye
EOF
    
    if grep -q "No such file" /tmp/check_$$.out; then
        rm -f /tmp/check_$$.out
        return 1  # Directory doesn't exist
    else
        rm -f /tmp/check_$$.out
        return 0  # Directory exists
    fi
}

# Function to create backup on target server using SFTP
create_backup() {
    local host=$1
    local path=$2
    local backup_path="${path}_backup_${BACKUP_SUFFIX}"
    
    log "INFO" "Managing backups (ensuring only latest backup exists): $path -> $backup_path"
    
    # Clean up existing backup and create new one in single operation
    sftp $CURRENT_USER@$host << EOF > /tmp/backup_$$.out 2>&1
# Remove any existing backup directory (ignore error if doesn't exist)
-rm $backup_path
# Rename existing directory to backup if it exists (will fail harmlessly if directory doesn't exist)
-rename $path $backup_path
bye
EOF
    
    # Clean up temporary files
    rm -f /tmp/backup_$$.out
    
    log "INFO" "Backup management completed (latest backup maintained)"
    return 0
}

# Function to transfer directory using SFTP
transfer_directory() {
    local source_path=$1
    local target_host=$2
    local target_path=$3
    
    log "INFO" "Starting directory transfer..."
    log "INFO" "Source: $(hostname):$source_path"
    log "INFO" "Target: $CURRENT_USER@$target_host:$target_path"
    
    # Upload to target using SFTP
    log "INFO" "Uploading directory to target server..."
    
    # Remove trailing slash to avoid double slashes
    local clean_target_path="${target_path%/}"
    
    if sftp $CURRENT_USER@$target_host << EOF
-mkdir $clean_target_path
put -r $source_path $clean_target_path/
bye
EOF
    then
        log "INFO" "Upload completed successfully"
        return 0
    else
        log "ERROR" "Failed to upload to target server"
        return 1
    fi
}

# Function to verify transfer using SFTP
verify_transfer() {
    local target_host=$1
    local target_path=$2
    
    log "INFO" "Verifying transfer..."
    
    # Use SFTP to verify the transfer
    sftp $CURRENT_USER@$target_host << EOF > /tmp/verify_$$.out 2>&1
ls $target_path
bye
EOF
    
    if grep -q "No such file" /tmp/verify_$$.out; then
        log "ERROR" "Transfer verification failed"
        rm -f /tmp/verify_$$.out
        return 1
    else
        log "INFO" "Transfer verification successful"
        rm -f /tmp/verify_$$.out
        return 0
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --source-path)
            SOURCE_PATH="$2"
            shift 2
            ;;
        --target-path)
            TARGET_PATH="$2"
            shift 2
            ;;
        --target-host)
            TARGET_HOST="$2"
            shift 2
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    log "INFO" "Starting SFTP Directory Transfer Script"
    
    # Validate parameters
    validate_params
    
    # Calculate actual target path (target_location/source_directory_name)
    local source_dirname=$(basename "$SOURCE_PATH")
    # Remove trailing slash from TARGET_PATH to avoid double slashes
    local clean_target_path="${TARGET_PATH%/}"
    local actual_target_path="$clean_target_path/$source_dirname"
    
    log "INFO" "Source: $(hostname):$SOURCE_PATH"
    log "INFO" "Target location: $TARGET_PATH"
    log "INFO" "Will be uploaded as: $actual_target_path"
    
    # Check if target directory exists
    if check_remote_directory "$TARGET_HOST" "$actual_target_path"; then
        log "WARN" "Target directory already exists: $actual_target_path"
        
        # Create backup
        if ! create_backup "$TARGET_HOST" "$actual_target_path"; then
            log "ERROR" "Failed to create backup, aborting transfer"
            exit 1
        fi
    else
        log "INFO" "Target directory does not exist, proceeding with transfer"
    fi
    
    # Transfer directory
    if transfer_directory "$SOURCE_PATH" "$TARGET_HOST" "$TARGET_PATH"; then
        log "INFO" "Directory transfer completed successfully"
        
        # Verify transfer
        if ! verify_transfer "$TARGET_HOST" "$actual_target_path"; then
            log "WARN" "Transfer verification failed, but files may still be transferred"
        fi
    else
        log "ERROR" "Directory transfer failed"
        exit 1
    fi
    
    log "INFO" "Script execution completed"
}

# Run main function
main "$@" 
