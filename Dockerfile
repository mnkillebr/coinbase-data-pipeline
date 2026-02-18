# Use official Apache Airflow image as base
FROM apache/airflow:3.1.0-python3.12

# Switch to root to install system packages
USER root

# Install system dependencies for TA-Lib compilation and AWS CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    make \
    wget \
    tar \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib C library from source
# This must be done BEFORE installing the Python TA-Lib package
# Reference: https://ta-lib.github.io/ta-lib-python/install.html
RUN cd /tmp && \
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/ta-lib* && \
    ldconfig

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file first for better caching
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
# TA-Lib Python package will now compile successfully since C library is installed
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root utils/ /opt/airflow/utils/
COPY --chown=airflow:root configs/ /opt/airflow/configs/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root spark_jobs/ /opt/airflow/spark_jobs/

# Create necessary directories
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/output && \
    chown -R airflow:root /opt/airflow/data /opt/airflow/logs /opt/airflow/output

# Set Python path to include project root
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Default command (can be overridden in docker-compose)
CMD ["bash"]
