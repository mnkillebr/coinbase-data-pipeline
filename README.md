# Coinbase Data Pipeline

A comprehensive Apache Airflow-managed data pipeline that collects, processes, and stores cryptocurrency market data from Coinbase's REST API. This pipeline supports multiple time granularities and automatically processes data using Apache Spark for efficient storage and analysis.

## Features

- **Multi-granularity Data Collection**: Supports 15-minute, 1-hour, 4-hour, and daily candlestick data
- **Automated Scheduling**: Configurable cron-based scheduling for each granularity
- **Data Persistence**: Local CSV storage with S3 backup capabilities
- **Spark Processing**: Automated data processing and partitioning using Apache Spark
- **Incremental Updates**: Smart data collection that only fetches new data since the last update
- **Error Handling**: Robust error handling and retry mechanisms
- **Monitoring**: Comprehensive logging and task monitoring

## Pipeline Workflow

The pipeline performs the following operations for each configured product and granularity:

1. **Data Collection**: Collects initial dataset or updates existing CSV files with new data
2. **S3 Upload**: Copies updated datasets to Amazon S3 for backup and distributed access
3. **Spark Processing**: Processes data using Apache Spark and saves in partitioned Parquet format

## Supported Products

Currently configured to collect data for:
- BTC-USD (Bitcoin)
- ETH-USD (Ethereum)

## Time Granularities

| Granularity | Schedule | Description |
|-------------|----------|-------------|
| FIFTEEN_MINUTE | Every 15 minutes | 15-minute candlesticks |
| ONE_HOUR | Every hour | Hourly candlesticks |
| FOUR_HOUR | Every 4 hours | 4-hour candlesticks |
| ONE_DAY | Daily at 8 PM EST | Daily candlesticks |

## Prerequisites

- Python 3.12+
- Apache Airflow 3.1.0+
- Apache Spark (for data processing)
- AWS CLI configured (for S3 uploads)
- Coinbase Advanced Trading API credentials

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd coinbase-data-pipeline
   ```

2. **Install dependencies**:
   ```bash
   # Using uv (recommended)
   uv sync
   
   # Or using pip
   pip install -e .
   ```

3. **Set up Airflow**:
   ```bash
   # Initialize Airflow database
   airflow db init
   
   # Create an admin user
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

## Configuration

### 1. Update Configuration File

Edit `configs/crypto_pipeline_config.py` to set up your local paths and AWS settings:

```python
# File paths and directories
PATHS = {
    "data_dir": "/path/to/your/data/directory",
    "logs_dir": "/path/to/your/logs/directory", 
    "scripts_dir": "/path/to/your/scripts/directory",
    "spark_job_path": "/path/to/your/spark/job.py",
    "collect_script": "/path/to/your/collect/script.py"
}

# AWS Configuration
AWS_CONFIG = {
    "s3_bucket": "your-s3-bucket-name",
    "aws_profile": "your-aws-profile-name",
    "s3_prefix": "crypto-data"
}

# Spark Configuration
SPARK_CONFIG = {
    "output_dir": "/path/to/your/spark/output",
    "conn_id": "spark_default"
}
```

### 2. Set Up Airflow Variables

Configure your Coinbase API credentials as Airflow Variables:

```bash
# Set Coinbase API credentials
airflow variables set COINBASE_API_KEY_NAME "your-api-key"
airflow variables set COINBASE_API_PRIVATE_KEY "your-api-secret"
```

### 3. Configure Airflow Connections

Set up the required Airflow connections:

```bash
# Local filesystem connection
airflow connections add local_filesystem \
    --conn-type File \
    --conn-extra '{"path": "/path/to/your/data/directory"}'

# Spark connection
airflow connections add spark_default \
    --conn-type Spark \
    --conn-extra '{"master": "local[*]", "spark_home": "/path/to/spark"}'
```

### 4. Environment Variables (Alternative)

Instead of Airflow Variables, you can set environment variables:

```bash
export COINBASE_API_KEY_NAME="your-api-key"
export COINBASE_API_PRIVATE_KEY="your-api-secret"
```

## Usage

### 1. Start Airflow

```bash
# Start the Airflow webserver
airflow api-server --port 8080

# In another terminal, start the scheduler
airflow scheduler
```

### 2. Access Airflow UI

Open your browser and navigate to `http://localhost:8080`

### 3. Enable DAGs

The pipeline creates separate DAGs for each granularity:
- `crypto_pipeline_fifteen_minute`
- `crypto_pipeline_one_hour` 
- `crypto_pipeline_four_hour`
- `crypto_pipeline_one_day`

Enable the DAGs you want to run from the Airflow UI.

### 4. Monitor Execution

- View DAG runs and task status in the Airflow UI
- Check logs in your configured logs directory
- Monitor data files in your configured data directory

## Project Structure

```
coinbase-data-pipeline/
├── configs/
│   └── crypto_pipeline_config.py    # Configuration settings
├── dags/
│   └── crypto_pipeline_dag.py        # Airflow DAG definitions
├── utils/
│   └── collect_coinbase_data.py      # Data collection utilities
├── pyproject.toml                     # Project dependencies
└── README.md                         # This file
```

## Data Output

- **CSV Files**: Raw data stored locally in CSV format
- **S3 Backup**: CSV files uploaded to S3 for backup
- **Parquet Files**: Processed data stored in partitioned Parquet format for efficient querying

## Troubleshooting

### Common Issues

1. **API Rate Limits**: The pipeline includes rate limiting to respect Coinbase's API limits
2. **Missing Credentials**: Ensure Airflow Variables or environment variables are properly set
3. **Path Configuration**: Verify all paths in the configuration file exist and are accessible
4. **Spark Connection**: Ensure Spark is properly installed and the connection is configured

### Logs

Check the following locations for troubleshooting:
- Airflow task logs in the Airflow UI
- Application logs in your configured logs directory
- Spark job logs (if using Spark cluster)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review Airflow and Spark documentation
3. Open an issue in the repository
