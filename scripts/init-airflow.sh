#!/bin/bash
# Airflow initialization script
# This script can be used to initialize Airflow database and create admin user
# Note: docker-compose.yml already handles this via airflow-init service

set -e

echo "Initializing Airflow..."

# Wait for database to be ready
echo "Waiting for database to be ready..."
until airflow db check; do
  echo "Database is unavailable - sleeping"
  sleep 1
done
echo "Database is ready!"

# Initialize database (if not already initialized)
if ! airflow db check > /dev/null 2>&1; then
    echo "Initializing Airflow database..."
    airflow db init
else
    echo "Airflow database already initialized, running migrations..."
    airflow db upgrade
fi

# Create admin user (if not exists)
# This is typically handled by docker-compose with _AIRFLOW_WWW_USER_CREATE
# But can be run manually if needed
if [ -n "$AIRFLOW_ADMIN_USERNAME" ] && [ -n "$AIRFLOW_ADMIN_PASSWORD" ] && [ -n "$AIRFLOW_ADMIN_EMAIL" ]; then
    echo "Creating admin user..."
    airflow users create \
        --username "$AIRFLOW_ADMIN_USERNAME" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email "$AIRFLOW_ADMIN_EMAIL" \
        --password "$AIRFLOW_ADMIN_PASSWORD" || echo "User may already exist"
fi

# Set up Airflow connections from environment variables (if any)
if [ -n "$SPARK_CONN_ID" ]; then
    echo "Setting up Spark connection..."
    airflow connections delete "$SPARK_CONN_ID" 2>/dev/null || true
    airflow connections add "$SPARK_CONN_ID" \
        --conn-type spark \
        --conn-host "$SPARK_MASTER" \
        --conn-extra '{"queue": "default"}' || echo "Spark connection setup skipped"
fi

# Set up Airflow variables from environment (if any)
if [ -n "$LOG_DIR" ]; then
    echo "Setting LOG_DIR variable..."
    airflow variables set LOG_DIR "$LOG_DIR" || true
fi

if [ -n "$COINBASE_API_KEY_NAME" ]; then
    echo "Setting Coinbase API credentials..."
    airflow variables set COINBASE_API_KEY_NAME "$COINBASE_API_KEY_NAME" || true
fi

if [ -n "$COINBASE_API_PRIVATE_KEY" ]; then
    airflow variables set COINBASE_API_PRIVATE_KEY "$COINBASE_API_PRIVATE_KEY" || true
fi

echo "Airflow initialization complete!"
