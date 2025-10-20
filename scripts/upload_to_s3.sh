#!/bin/bash

# S3 Upload Script for Crypto Data Pipeline
# Usage: upload_to_s3.sh <local_file_path> <s3_path> <aws_profile>
# Example: upload_to_s3.sh /path/to/file.csv s3://bucket/path/file.csv my-profile

set -e  # Exit on any error

# Check if correct number of arguments provided
if [ $# -ne 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <local_file_path> <s3_path> <aws_profile>"
    echo "Example: $0 /path/to/file.csv s3://bucket/path/file.csv my-profile"
    exit 1
fi

LOCAL_FILE="$1"
S3_PATH="$2"
AWS_PROFILE="$3"

# Validate inputs
if [ -z "$LOCAL_FILE" ] || [ -z "$S3_PATH" ] || [ -z "$AWS_PROFILE" ]; then
    echo "Error: All arguments must be non-empty"
    exit 1
fi

# Check if local file exists
if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: Local file does not exist: $LOCAL_FILE"
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

# Get file size for logging
FILE_SIZE=$(du -h "$LOCAL_FILE" | cut -f1)
echo "Uploading file: $LOCAL_FILE (size: $FILE_SIZE)"
echo "Destination: $S3_PATH"
echo "AWS Profile: $AWS_PROFILE"

# Upload file to S3 with error handling
echo "Starting upload..."
if aws s3 cp "$LOCAL_FILE" "$S3_PATH" --profile "$AWS_PROFILE"; then
    echo "✅ Upload successful: $LOCAL_FILE -> $S3_PATH"
    
    # Verify upload by checking if file exists in S3
    if aws s3 ls "$S3_PATH" --profile "$AWS_PROFILE" &> /dev/null; then
        echo "✅ Upload verification successful"
    else
        echo "⚠️  Warning: Upload completed but file not found in S3"
    fi
else
    echo "❌ Upload failed: $LOCAL_FILE -> $S3_PATH"
    exit 1
fi

echo "Upload process completed successfully"
