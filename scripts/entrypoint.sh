#!/bin/bash
# Custom entrypoint script for Airflow containers
# This can be used to add custom initialization logic before starting Airflow services
# The standard Airflow entrypoint is used by default in docker-compose

set -e

# Create necessary directories if they don't exist
mkdir -p /opt/airflow/data
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/output

# Set proper permissions
chown -R airflow:root /opt/airflow/data /opt/airflow/logs /opt/airflow/output || true

# Wait for dependencies if needed
if [ -n "$WAIT_FOR_DB" ] && [ "$WAIT_FOR_DB" = "true" ]; then
    echo "Waiting for database to be ready..."
    until airflow db check; do
        echo "Database is unavailable - sleeping"
        sleep 1
    done
    echo "Database is ready!"
fi

if [ -n "$WAIT_FOR_REDIS" ] && [ "$WAIT_FOR_REDIS" = "true" ]; then
    echo "Waiting for Redis to be ready..."
    until redis-cli -h "${REDIS_HOST:-redis}" -p "${REDIS_PORT:-6379}" ping; do
        echo "Redis is unavailable - sleeping"
        sleep 1
    done
    echo "Redis is ready!"
fi

# Run custom initialization script if provided
if [ -f /opt/airflow/scripts/init-airflow.sh ]; then
    echo "Running custom initialization script..."
    bash /opt/airflow/scripts/init-airflow.sh || echo "Initialization script completed with warnings"
fi

# Execute the original Airflow entrypoint or provided command
exec "$@"
