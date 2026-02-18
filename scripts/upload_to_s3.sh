#!/bin/bash

# S3 Upload Script for Crypto Data Pipeline
# Usage: upload_to_s3.sh <local_file_path> <s3_path> [aws_profile]
# Example: upload_to_s3.sh /path/to/file.csv s3://bucket/path/file.csv [my-profile]
# 
# Note: In Docker, AWS credentials can be provided via:
#   - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#   - AWS profile: --profile option (requires mounted ~/.aws/credentials)
#   - IAM role: When running on AWS infrastructure

set -e  # Exit on any error

# Check if correct number of arguments provided
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <local_file_path> <s3_path> [aws_profile]"
    echo "Example: $0 /path/to/file.csv s3://bucket/path/file.csv [my-profile]"
    exit 1
fi

LOCAL_FILE="$1"
S3_PATH="$2"
AWS_PROFILE="${3:-}"

# Validate inputs
if [ -z "$LOCAL_FILE" ] || [ -z "$S3_PATH" ]; then
    echo "Error: Local file path and S3 path must be provided"
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

# Determine AWS authentication method
AWS_CMD_BASE="aws s3 cp"
AWS_CMD_PROFILE=""

# Check if AWS credentials are available via environment variables
if [ -n "$AWS_ACCESS_KEY_ID" ] && [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Using AWS credentials from environment variables"
    export AWS_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY
    if [ -n "$AWS_DEFAULT_REGION" ]; then
        export AWS_DEFAULT_REGION
    fi
elif [ -n "$AWS_PROFILE" ]; then
    # Use AWS profile if provided
    echo "Using AWS profile: $AWS_PROFILE"
    # Check if profile exists (only if ~/.aws/credentials exists)
    if [ -f ~/.aws/credentials ] || [ -f ~/.aws/config ]; then
        if ! aws configure list-profiles 2>/dev/null | grep -q "^$AWS_PROFILE$"; then
            echo "Warning: AWS profile '$AWS_PROFILE' not found in credentials"
            echo "Attempting to use profile anyway..."
        fi
    fi
    AWS_CMD_PROFILE="--profile $AWS_PROFILE"
else
    echo "Warning: No AWS credentials found in environment variables or profile"
    echo "Attempting to use default AWS credentials (IAM role, instance profile, etc.)"
fi

# Get file size for logging
FILE_SIZE=$(du -h "$LOCAL_FILE" | cut -f1)
echo "Uploading file: $LOCAL_FILE (size: $FILE_SIZE)"
echo "Destination: $S3_PATH"
if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
fi

# Upload file to S3 with error handling
echo "Starting upload..."
if aws s3 cp "$LOCAL_FILE" "$S3_PATH" $AWS_CMD_PROFILE; then
    echo "✅ Upload successful: $LOCAL_FILE -> $S3_PATH"
    
    # Verify upload by checking if file exists in S3
    if aws s3 ls "$S3_PATH" $AWS_CMD_PROFILE &> /dev/null; then
        echo "✅ Upload verification successful"
    else
        echo "⚠️  Warning: Upload completed but file not found in S3"
    fi
else
    echo "❌ Upload failed: $LOCAL_FILE -> $S3_PATH"
    exit 1
fi

echo "Upload process completed successfully"
