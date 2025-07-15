#!/bin/bash

# SFTP Directory Transfer Script with Backup
# Usage: ./sftp_directory_transfer.sh [OPTIONS]
# This script transfers a directory from source server to target server using SFTP
# If target directory exists, it creates a timestamped backup before transfer

set -e  # Exit on any error

# Default values
SOURCE_PATH=""
TARGET_HOST=""
TARGET_PATH=""
SSH_KEY=""
BACKUP_SUFFIX=$(date +"%Y%m%d_%H%M%S")
DRY_RUN=false
VERBOSE=false
CURRENT_USER=$(whoami)

# Logging without colors

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Required Options:"
    echo "  --source-path PATH      Local source directory path"
    echo "  --target-host HOST      Target server hostname/IP"
    echo "  --target-path PATH      Target directory path on remote server"
    echo ""
    echo "Optional Options:"
    echo "  --ssh-key PATH          Path to SSH private key file"
    echo "  --backup-suffix SUFFIX  Custom backup suffix (default: YYYYMMDD_HHMMSS)"
    echo "  --dry-run               Show what would be done without executing"
    echo "  --verbose               Enable verbose output"
    echo "  --help                  Show this help message"
    echo ""
    echo "Note: Script uses current user ($CURRENT_USER) for remote connection"
    echo ""
    echo "Examples:"
    echo "  $0 --source-path /opt/app --target-host 192.168.1.20 --target-path /opt/app"
    echo ""
    echo "  $0 --source-path /var/www/html --target-host server2.com \\"
    echo "     --target-path /var/www/html --ssh-key ~/.ssh/deploy_key --verbose"
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
        "DEBUG")
            if [[ "$VERBOSE" == "true" ]]; then
                echo "[DEBUG] ${timestamp} - $message"
            fi
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
    
    if [[ -z "$TARGET_HOST" ]]; then
        log "ERROR" "Target host is required"
        ((errors++))
    fi
    
    if [[ -z "$TARGET_PATH" ]]; then
        log "ERROR" "Target path is required"
        ((errors++))
    fi
    
    if [[ -n "$SSH_KEY" && ! -f "$SSH_KEY" ]]; then
        log "ERROR" "SSH key file not found: $SSH_KEY"
        ((errors++))
    fi
    
    if [[ $errors -gt 0 ]]; then
        log "ERROR" "Validation failed with $errors error(s)"
        exit 1
    fi
}

# Function to build SSH connection string
build_ssh_opts() {
    local ssh_opts=""
    
    if [[ -n "$SSH_KEY" ]]; then
        ssh_opts="$ssh_opts -i $SSH_KEY"
        # Prefer key auth but allow password fallback
        ssh_opts="$ssh_opts -o PreferredAuthentications=publickey,password"
    else
        # Try keys first (if available), then password
        ssh_opts="$ssh_opts -o PreferredAuthentications=publickey,password"
    fi
    
    # Allow both key and password authentication
    ssh_opts="$ssh_opts -o StrictHostKeyChecking=ask -o PasswordAuthentication=yes -o PubkeyAuthentication=yes"
    
    if [[ "$VERBOSE" != "true" ]]; then
        ssh_opts="$ssh_opts -q"
    fi
    
    echo "$ssh_opts"
}

# Function to check if directory exists on remote server
check_remote_directory() {
    local host=$1
    local path=$2
    local ssh_opts=$(build_ssh_opts)
    
    log "DEBUG" "Checking if directory exists: $CURRENT_USER@$host:$path"
    
    if ssh $ssh_opts $CURRENT_USER@$host "test -d '$path'" 2>/dev/null; then
        return 0  # Directory exists
    else
        return 1  # Directory doesn't exist
    fi
}

# Function to create backup on target server
create_backup() {
    local host=$1
    local path=$2
    local ssh_opts=$(build_ssh_opts)
    local backup_path="${path}_backup_${BACKUP_SUFFIX}"
    
    log "INFO" "Creating backup: $path -> $backup_path"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log "INFO" "[DRY RUN] Would create backup: $CURRENT_USER@$host:$backup_path"
        return 0
    fi
    
    if ssh $ssh_opts $CURRENT_USER@$host "mv '$path' '$backup_path'" 2>/dev/null; then
        log "INFO" "Backup created successfully: $backup_path"
        return 0
    else
        log "ERROR" "Failed to create backup"
        return 1
    fi
}

# Function to transfer directory using SFTP
transfer_directory() {
    local source_path=$1
    local target_host=$2
    local target_path=$3
    
    log "INFO" "Starting directory transfer..."
    log "INFO" "Source: $(hostname):$source_path"
    log "INFO" "Target: $CURRENT_USER@$target_host:$target_path"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log "INFO" "[DRY RUN] Would transfer directory from source to target"
        return 0
    fi
    
    local ssh_opts=$(build_ssh_opts)
    local target_parent=$(dirname "$target_path")
    
    # Upload to target using SFTP
    log "INFO" "Uploading directory to target server..."
    if sftp $ssh_opts $CURRENT_USER@$target_host << EOF
mkdir -p $target_parent
put -r $source_path $target_parent/
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

# Function to verify transfer
verify_transfer() {
    local target_host=$1
    local target_path=$2
    
    log "INFO" "Verifying transfer..."
    
    if check_remote_directory "$target_host" "$target_path"; then
        log "INFO" "Transfer verification successful"
        return 0
    else
        log "ERROR" "Transfer verification failed"
        return 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --source-path)
            SOURCE_PATH="$2"
            shift 2
            ;;
        --target-host)
            TARGET_HOST="$2"
            shift 2
            ;;
        --target-path)
            TARGET_PATH="$2"
            shift 2
            ;;
        --ssh-key)
            SSH_KEY="$2"
            shift 2
            ;;
        --backup-suffix)
            BACKUP_SUFFIX="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log "ERROR" "Unknown option: $1"
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
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log "WARN" "DRY RUN MODE - No actual changes will be made"
    fi
    
    # Check if target directory exists
    if check_remote_directory "$TARGET_HOST" "$TARGET_PATH"; then
        log "WARN" "Target directory already exists: $TARGET_PATH"
        
        # Create backup
        if ! create_backup "$TARGET_HOST" "$TARGET_PATH"; then
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
        if ! verify_transfer "$TARGET_HOST" "$TARGET_PATH"; then
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