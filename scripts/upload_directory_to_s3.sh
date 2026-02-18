#!/bin/bash

# S3 Directory Upload Script for Crypto Data Pipeline
# Usage: upload_directory_to_s3.sh <local_directory_path> <s3_path> [aws_profile]
# Example: upload_directory_to_s3.sh /path/to/directory s3://bucket/path/directory [my-profile]
#
# Note: In Docker, AWS credentials can be provided via:
#   - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#   - AWS profile: --profile option (requires mounted ~/.aws/credentials)
#   - IAM role: When running on AWS infrastructure

set -e  # Exit on any error

# Check if correct number of arguments provided
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <local_directory_path> <s3_path> [aws_profile]"
    echo "Example: $0 /path/to/directory s3://bucket/path/directory [my-profile]"
    exit 1
fi

LOCAL_DIR="$1"
S3_PATH="$2"
AWS_PROFILE="${3:-}"

# Validate inputs
if [ -z "$LOCAL_DIR" ] || [ -z "$S3_PATH" ]; then
    echo "Error: Local directory path and S3 path must be provided"
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

# Determine AWS authentication method
AWS_CMD_BASE="aws s3 sync"
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

# Get directory size for logging
DIR_SIZE=$(du -sh "$LOCAL_DIR" | cut -f1)
echo "Uploading directory: $LOCAL_DIR (size: $DIR_SIZE)"
echo "Destination: $S3_PATH"
if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
fi

# List files in directory for logging
echo "Files to upload:"
find "$LOCAL_DIR" -type f | head -10
if [ $(find "$LOCAL_DIR" -type f | wc -l) -gt 10 ]; then
    echo "... and $(($(find "$LOCAL_DIR" -type f | wc -l) - 10)) more files"
fi

# Upload directory to S3 with error handling
echo "Starting directory upload..."
if aws s3 sync "$LOCAL_DIR" "$S3_PATH" $AWS_CMD_PROFILE; then
    echo "✅ Directory upload successful: $LOCAL_DIR -> $S3_PATH"
    
    # Verify upload by checking if directory exists in S3
    if aws s3 ls "$S3_PATH" $AWS_CMD_PROFILE &> /dev/null; then
        echo "✅ Upload verification successful"
        echo "Uploaded files:"
        aws s3 ls "$S3_PATH" --recursive $AWS_CMD_PROFILE | head -10
        if [ $(aws s3 ls "$S3_PATH" --recursive $AWS_CMD_PROFILE | wc -l) -gt 10 ]; then
            echo "... and $(($(aws s3 ls "$S3_PATH" --recursive $AWS_CMD_PROFILE | wc -l) - 10)) more files"
        fi
    else
        echo "⚠️  Warning: Upload completed but directory not found in S3"
    fi
else
    echo "❌ Directory upload failed: $LOCAL_DIR -> $S3_PATH"
    exit 1
fi

echo "Directory upload process completed successfully"
