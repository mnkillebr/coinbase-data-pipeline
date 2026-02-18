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

### For Local Development
- Python 3.12+
- Apache Airflow 3.1.0+
- Apache Spark (for data processing)
- AWS CLI configured (for S3 uploads)
- Coinbase Advanced Trading API credentials
- TA-Lib C library installed (see [TA-Lib installation](https://ta-lib.github.io/ta-lib-python/install.html))
  - macOS: `brew install ta-lib`
  - Linux: Compile from source

### For Docker Deployment
- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ RAM available for Docker
- 2+ CPUs recommended
- 10GB+ disk space

## Installation

### Option 1: Docker Deployment (Recommended for Production)

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd coinbase-data-pipeline
   ```

2. **Set up environment variables**:
   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit .env and configure:
   # - AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
   # - Coinbase API credentials (COINBASE_API_KEY_NAME, COINBASE_API_PRIVATE_KEY)
   # - S3 bucket configuration (S3_BUCKET, S3_PREFIX, S3_PROCESSED_PREFIX)
   # - Database passwords (POSTGRES_PASSWORD, AIRFLOW_FERNET_KEY)
   ```

3. **Set Airflow UID (Linux users)**:
   ```bash
   # Get your user ID
   echo $(id -u)
   
   # Set it in .env file
   AIRFLOW_UID=<your-user-id>
   ```

4. **Build and start services**:
   ```bash
   # Build the Docker image
   docker-compose build
   
   # Initialize Airflow (first time only)
   docker-compose up airflow-init
   
   # Start all services
   docker-compose up -d
   ```

5. **Access Airflow UI**:
   - Open http://localhost:8095
   - Default credentials: `airflow` / `airflow` (change in production!)

6. **View logs**:
   ```bash
   # All services
   docker-compose logs -f
   
   # Specific service
   docker-compose logs -f airflow-scheduler
   docker-compose logs -f airflow-webserver
   ```

7. **Stop services**:
   ```bash
   docker-compose down
   ```

### Option 2: Local Development

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

### Docker Environment Variables

When using Docker, configuration is done via environment variables in `.env` file. Key variables:

**Required:**
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`: AWS credentials for S3 access
- `COINBASE_API_KEY_NAME` / `COINBASE_API_PRIVATE_KEY`: Coinbase API credentials
- `S3_BUCKET`: Your S3 bucket name
- `POSTGRES_PASSWORD`: Database password (use strong password in production)
- `AIRFLOW_FERNET_KEY`: Encryption key (generate with: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)

**Optional:**
- `S3_PREFIX`: S3 path prefix (default: `crypto-data`)
- `S3_PROCESSED_PREFIX`: Processed data prefix (default: `crypto-data-processed`)
- `DATA_DIR`: Data directory path (default: `/opt/airflow/data`)
- `LOGS_DIR`: Logs directory (default: `/opt/airflow/logs`)
- `OUTPUT_DIR`: Output directory (default: `/opt/airflow/output`)
- `SPARK_MASTER`: Spark master URL (default: `local[*]`)

See `.env.example` for complete list of available environment variables.

### Local Development Configuration

For local development, edit `configs/crypto_pipeline_config.py`:

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

**Note:** The configuration file now supports environment variables, so it works in both Docker and local environments.

### Docker: Airflow Variables and Connections

In Docker, Airflow variables and connections can be set via:

1. **Environment Variables** (automatically set as Airflow Variables):
   - `COINBASE_API_KEY_NAME` → Airflow Variable `COINBASE_API_KEY_NAME`
   - `COINBASE_API_PRIVATE_KEY` → Airflow Variable `COINBASE_API_PRIVATE_KEY`
   - `LOG_DIR` → Airflow Variable `LOG_DIR`

2. **Airflow UI**:
   - Access http://localhost:8095
   - Navigate to Admin → Variables
   - Add/edit variables as needed

3. **Airflow CLI** (inside container):
   ```bash
   docker-compose exec airflow-webserver airflow variables set COINBASE_API_KEY_NAME "your-key"
   ```

### Local Development: Airflow Variables

For local development, configure Airflow Variables:

```bash
# Set Coinbase API credentials
airflow variables set COINBASE_API_KEY_NAME "your-api-key"
airflow variables set COINBASE_API_PRIVATE_KEY "your-api-secret"
```

### Spark Connection

The Spark connection is configured via environment variables in Docker:
- `SPARK_MASTER`: Spark master URL (default: `local[*]`)
- `SPARK_CONN_ID`: Connection ID (default: `spark_default`)

For local development, set up the connection manually:

```bash
airflow connections add spark_default \
    --conn-type Spark \
    --conn-extra '{"master": "local[*]", "spark_home": "/path/to/spark"}'
```

## Usage

### Docker Deployment

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Access Airflow UI**:
   - Open http://localhost:8095
   - Login with credentials from `.env` (default: `airflow` / `airflow`)

3. **Enable DAGs**:
   The pipeline creates separate DAGs for each granularity:
   - `crypto_pipeline_fifteen_minute`
   - `crypto_pipeline_one_hour` 
   - `crypto_pipeline_four_hour`
   - `crypto_pipeline_one_day`
   
   Enable the DAGs you want to run from the Airflow UI.

4. **Monitor Execution**:
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service logs
   docker-compose logs -f airflow-scheduler
   docker-compose logs -f airflow-webserver
   ```

5. **Access data and logs**:
   - Data files: `./data/` (mounted from container)
   - Logs: `./logs/` (mounted from container)
   - Output: `./output/` (mounted from container)

### Local Development

1. **Start Airflow**:
   ```bash
   # Start the Airflow webserver
   airflow api-server --port 8095
   
   # In another terminal, start the scheduler
   airflow scheduler
   ```

2. **Access Airflow UI**:
   Open your browser and navigate to `http://localhost:8095`

3. **Enable DAGs**:
   Enable the DAGs you want to run from the Airflow UI.

4. **Monitor Execution**:
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
│   ├── collect_coinbase_data.py      # Data collection utilities
│   ├── calculate_technical_indicators.py      # Adding technical indicators
│   └── calculate_risk_target.py      # Adding strategy
├── scripts/
│   ├── upload_to_s3.sh              # S3 upload scripts
│   ├── upload_directory_to_s3.sh
│   ├── entrypoint.sh                 # Docker entrypoint
│   └── init-airflow.sh               # Airflow initialization
├── spark_jobs/
│   └── process_crypto_data_spark.py # Spark processing job
├── Dockerfile                         # Docker image definition
├── docker-compose.yml                 # Docker Compose configuration
├── .dockerignore                      # Docker ignore patterns
├── .env.example                       # Example environment variables
├── pyproject.toml                     # Project dependencies
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## Data Output

- **CSV Files**: Raw data stored locally in CSV format
- **S3 Backup**: CSV files uploaded to S3 for backup
- **Parquet Files**: Processed data stored in partitioned Parquet format for efficient querying

## Troubleshooting

### Docker Issues

1. **Permission Errors**:
   ```bash
   # Set correct Airflow UID (Linux)
   export AIRFLOW_UID=$(id -u)
   echo $AIRFLOW_UID >> .env
   ```

2. **Database Connection Errors**:
   ```bash
   # Check if postgres is healthy
   docker-compose ps postgres
   
   # View postgres logs
   docker-compose logs postgres
   ```

3. **TA-Lib Import Errors**:
   - Ensure TA-Lib C library was compiled during Docker build
   - Check Dockerfile build logs for TA-Lib compilation errors
   - Rebuild image: `docker-compose build --no-cache`

4. **AWS Credentials**:
   - Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set in `.env`
   - Or configure AWS profile and mount `~/.aws` directory

5. **Port Already in Use**:
   ```bash
   # Change port in docker-compose.yml or .env
   AIRFLOW_API_SERVER_PORT=8091
   ```

### Common Issues

1. **API Rate Limits**: The pipeline includes rate limiting to respect Coinbase's API limits
2. **Missing Credentials**: Ensure Airflow Variables or environment variables are properly set
3. **Path Configuration**: Verify all paths in the configuration file exist and are accessible
4. **Spark Connection**: Ensure Spark is properly installed and the connection is configured

### Logs

**Docker:**
- View logs: `docker-compose logs -f [service-name]`
- Task logs: Available in Airflow UI or `./logs/` directory

**Local Development:**
- Airflow task logs in the Airflow UI
- Application logs in your configured logs directory
- Spark job logs (if using Spark cluster)

### Production Deployment Notes

For production Kubernetes deployment:

1. **Use Kubernetes Secrets** instead of `.env` files
2. **External PostgreSQL**: Use managed database service (RDS, Cloud SQL, etc.)
3. **Persistent Volumes**: Configure persistent volumes for data/logs
4. **Resource Limits**: Set appropriate CPU/memory limits
5. **Health Checks**: Configure liveness and readiness probes
6. **Spark**: Consider EMR Serverless or Spark on Kubernetes instead of local Spark
7. **Monitoring**: Set up Prometheus/Grafana for metrics
8. **Logging**: Configure centralized logging (CloudWatch, ELK, etc.)

See the [Airflow Helm Chart documentation](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html) for production best practices.

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
