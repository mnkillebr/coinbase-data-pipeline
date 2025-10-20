#!/bin/bash

# S3 Directory Upload Script for Crypto Data Pipeline
# Usage: upload_directory_to_s3.sh <local_directory_path> <s3_path> <aws_profile>
# Example: upload_directory_to_s3.sh /path/to/directory s3://bucket/path/directory my-profile

set -e  # Exit on any error

# Check if correct number of arguments provided
if [ $# -ne 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <local_directory_path> <s3_path> <aws_profile>"
    echo "Example: $0 /path/to/directory s3://bucket/path/directory my-profile"
    exit 1
fi

LOCAL_DIR="$1"
S3_PATH="$2"
AWS_PROFILE="$3"

# Validate inputs
if [ -z "$LOCAL_DIR" ] || [ -z "$S3_PATH" ] || [ -z "$AWS_PROFILE" ]; then
    echo "Error: All arguments must be non-empty"
    exit 1
fi

# Check if local directory exists
if [ ! -d "$LOCAL_DIR" ]; then
    echo "Error: Local directory does not exist: $LOCAL_DIR"
    exit 1
fi

# Check if AWS CLI is available
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed or not in PATH"
    exit 1
fi

# Check if AWS profile exists
if ! aws configure list-profiles | grep -q "^$AWS_PROFILE$"; then
    echo "Error: AWS profile '$AWS_PROFILE' not found"
    echo "Available profiles:"
    aws configure list-profiles
    exit 1
fi

# Get directory size for logging
DIR_SIZE=$(du -sh "$LOCAL_DIR" | cut -f1)
echo "Uploading directory: $LOCAL_DIR (size: $DIR_SIZE)"
echo "Destination: $S3_PATH"
echo "AWS Profile: $AWS_PROFILE"

# List files in directory for logging
echo "Files to upload:"
find "$LOCAL_DIR" -type f | head -10
if [ $(find "$LOCAL_DIR" -type f | wc -l) -gt 10 ]; then
    echo "... and $(($(find "$LOCAL_DIR" -type f | wc -l) - 10)) more files"
fi

# Upload directory to S3 with error handling
echo "Starting directory upload..."
if aws s3 sync "$LOCAL_DIR" "$S3_PATH" --profile "$AWS_PROFILE"; then
    echo "✅ Directory upload successful: $LOCAL_DIR -> $S3_PATH"
    
    # Verify upload by checking if directory exists in S3
    if aws s3 ls "$S3_PATH" --profile "$AWS_PROFILE" &> /dev/null; then
        echo "✅ Upload verification successful"
        echo "Uploaded files:"
        aws s3 ls "$S3_PATH" --recursive --profile "$AWS_PROFILE" | head -10
        if [ $(aws s3 ls "$S3_PATH" --recursive --profile "$AWS_PROFILE" | wc -l) -gt 10 ]; then
            echo "... and $(($(aws s3 ls "$S3_PATH" --recursive --profile "$AWS_PROFILE" | wc -l) - 10)) more files"
        fi
    else
        echo "⚠️  Warning: Upload completed but directory not found in S3"
    fi
else
    echo "❌ Directory upload failed: $LOCAL_DIR -> $S3_PATH"
    exit 1
fi

echo "Directory upload process completed successfully"
