# Environment Variables Setup

This document lists all environment variables needed for the Docker deployment.

**Note:** Create a `.env` file in the project root with these variables. You can copy from this template.

## Required Environment Variables

### Airflow Core
```bash
AIRFLOW_EXECUTOR=LocalExecutor
AIRFLOW_UID=50000  # Set to your user ID on Linux: $(id -u)
AIRFLOW_API_SERVER_PORT=8090
AIRFLOW_FERNET_KEY=  # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### PostgreSQL Database
```bash
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow  # Use strong password in production!
POSTGRES_DB=airflow
```

### Redis
```bash
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
```

### AWS Configuration
```bash
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
AWS_PROFILE=  # Optional: alternative to access keys
S3_BUCKET=your-bucket-name
S3_PREFIX=crypto-data
S3_PROCESSED_PREFIX=crypto-data-processed
```

### Coinbase API
```bash
COINBASE_API_KEY_NAME=your-api-key-name
COINBASE_API_PRIVATE_KEY=your-api-private-key
```

### Project Paths (Optional - defaults provided)
```bash
DATA_DIR=/opt/airflow/data
LOGS_DIR=/opt/airflow/logs
SCRIPTS_DIR=/opt/airflow/scripts
OUTPUT_DIR=/opt/airflow/output
LOG_DIR=/opt/airflow/logs
```

### Spark Configuration (Optional)
```bash
SPARK_MASTER=local[*]
SPARK_APP_NAME=crypto-candles-processor
SPARK_CONN_ID=spark_default
```

### Airflow Admin User (First Run)
```bash
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow  # Change in production!
_AIRFLOW_WWW_USER_EMAIL=airflow@example.com
```

## Quick Start

1. Copy this template to `.env`:
   ```bash
   cp ENV_SETUP.md .env
   # Then edit .env and fill in your actual values
   ```

2. Generate Fernet key:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

3. Set your user ID (Linux):
   ```bash
   echo "AIRFLOW_UID=$(id -u)" >> .env
   ```

## Production Notes

- Use Kubernetes secrets or AWS Secrets Manager instead of `.env` files
- Set strong passwords for `POSTGRES_PASSWORD` and `_AIRFLOW_WWW_USER_PASSWORD`
- Use IAM roles instead of access keys when possible
- Never commit `.env` files to version control
