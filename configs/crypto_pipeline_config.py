"""
Configuration file for the crypto data pipeline.
Contains all products, granularities, schedules, and AWS settings.
Supports both local development and Docker containerized environments.
"""

import os
import pendulum

# Product IDs to collect data for
PRODUCTS = ["BTC-USD", "ETH-USD"]

# Timezone configuration
TIMEZONE = "America/New_York"  # EST/EDT timezone

# Granularities with their corresponding cron schedules
GRANULARITIES = {
    "FIFTEEN_MINUTE": {
        "schedule": "1,16,31,46 * * * *",  # Every 15 minutes
        "description": "15-minute candles"
    },
    "ONE_HOUR": {
        "schedule": "1 * * * *",  # Every hour at minute 1
        "description": "Hourly candles"
    },
    "FOUR_HOUR": {
        "schedule": "1 */4 * * *",  # Every 4 hours at minute 1
        "description": "4-hour candles"
    },
    "ONE_DAY": {
        "schedule": "1 20 * * *",  # Daily at 8 PM EST (after market close) at minute 1
        "description": "Daily candles"
    }
}

# AWS Configuration
# Uses environment variables with fallback to defaults for Docker/local compatibility
AWS_CONFIG = {
    "s3_bucket": os.getenv("S3_BUCKET", ""),
    "aws_profile": os.getenv("AWS_PROFILE", ""),
    "s3_prefix": os.getenv("S3_PREFIX", "crypto-data"),
    "s3_processed_prefix": os.getenv("S3_PROCESSED_PREFIX", "crypto-data-processed")
}

# File paths and directories
# Uses environment variables for Docker compatibility, with fallback to local paths
# In Docker: /opt/airflow/{data,logs,scripts,output}
# Locally: can be overridden via environment variables
def get_base_path():
    """Get base path for project files - works in both local and Docker environments"""
    # Check if running in Docker (common indicator)
    if os.path.exists("/opt/airflow"):
        return "/opt/airflow"
    # Otherwise use current project directory
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASE_PATH = get_base_path()

PATHS = {
    "data_dir": os.getenv("DATA_DIR", os.path.join(BASE_PATH, "data")),
    "logs_dir": os.getenv("LOGS_DIR", os.path.join(BASE_PATH, "logs")),
    "scripts_dir": os.getenv("SCRIPTS_DIR", os.path.join(BASE_PATH, "scripts")),
    "spark_job_path": os.path.join(BASE_PATH, "spark_jobs", "process_crypto_data_spark.py"),
    "collect_script": os.path.join(BASE_PATH, "utils", "collect_coinbase_data.py")
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
# Uses environment variables with sensible defaults
SPARK_CONFIG = {
    "app_name": os.getenv("SPARK_APP_NAME", "crypto-candles-processor"),
    "master": os.getenv("SPARK_MASTER", "local[*]"),  # For local execution, use all available cores
    "output_format": "parquet",  # Output format for processed data
    "output_dir": os.getenv("OUTPUT_DIR", os.path.join(BASE_PATH, "output")),
    "conn_id": os.getenv("SPARK_CONN_ID", "spark_default")
}

