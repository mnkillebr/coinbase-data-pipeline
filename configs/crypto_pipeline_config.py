"""
Configuration file for the crypto data pipeline.
Contains all products, granularities, schedules, and AWS settings.
"""

import pendulum

# Product IDs to collect data for
PRODUCTS = ["BTC-USD", "ETH-USD"]

# Timezone configuration
TIMEZONE = "America/New_York"  # EST/EDT timezone

# Granularities with their corresponding cron schedules
GRANULARITIES = {
    "FIFTEEN_MINUTE": {
        "schedule": "2,17,32,47 * * * *",  # Every 15 minutes
        "description": "15-minute candles"
    },
    "ONE_HOUR": {
        "schedule": "0 * * * *",  # Every hour at minute 0
        "description": "Hourly candles"
    },
    "FOUR_HOUR": {
        "schedule": "0 */4 * * *",  # Every 4 hours
        "description": "4-hour candles"
    },
    "ONE_DAY": {
        "schedule": "0 20 * * *",  # Daily at 8 PM EST (after market close)
        "description": "Daily candles"
    }
}

# AWS Configuration
AWS_CONFIG = {
    "s3_bucket": "<s3_bucket_name>",  # Update with your S3 bucket name
    "aws_profile": "<aws_profile_name>",  # Update with your AWS profile name
    "s3_prefix": "<s3_prefix>",  # S3 path prefix for uploaded files
    "s3_processed_prefix": "<s3_processed_prefix>"  # S3 path prefix for processed data
}

# File paths and directories
PATHS = {
    "data_dir": "<local_data_dir>",
    "logs_dir": "<local_logs_dir>",
    "scripts_dir": "<local_scripts_dir>",
    "spark_job_path": "<local_spark_job_path>",
    "collect_script": "<local_collect_script>"
}

# DAG Configuration
DAG_CONFIG = {
    "default_args": {
        "owner": "crypto-pipeline",
        "depends_on_past": False,
        "start_date": pendulum.datetime(2025, 1, 1, tz=TIMEZONE),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": 300,  # 5 minutes
    },
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["crypto", "data-pipeline"]
}

# Spark Configuration
SPARK_CONFIG = {
    "app_name": "crypto-candles-processor",
    "master": "local[*]",  # For local execution, use all available cores
    "output_format": "parquet",  # Output format for processed data
    "output_dir": "<local_output_dir>",
    "conn_id": "<spark_conn_id>"
}

