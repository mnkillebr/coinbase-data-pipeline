# Use official Apache Airflow image as base
FROM apache/airflow:3.1.0-python3.12

# Switch to root to install system packages
USER root

# Set working directory
WORKDIR /opt/airflow

# Prevent interactive prompts during build (important for CI/VPS)
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies and download TA-Lib C source
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    curl \
    make \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download TA-Lib (retries and timeout for flaky networks / VPS)
RUN wget --tries=3 --timeout=60 -q http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz -O ta-lib-0.4.0-src.tar.gz && \
    tar -xf ta-lib-0.4.0-src.tar.gz

# Compile and install the TA-Lib C library
# Update config.guess and config.sub to support ARM64 (Apple Silicon)
# The original scripts from 2006 don't recognize modern architectures
WORKDIR /opt/airflow/ta-lib
RUN curl -L 'https://raw.githubusercontent.com/gcc-mirror/gcc/master/config.guess' -o config.guess && \
    curl -L 'https://raw.githubusercontent.com/gcc-mirror/gcc/master/config.sub' -o config.sub && \
    chmod +x config.guess config.sub && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    ldconfig

# Move back to the main directory
WORKDIR /opt/airflow

# Install AWS CLI v2 (supports both x86_64 and aarch64)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
        curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"; \
    else \
        curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; \
    fi && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Switch back to airflow user
USER airflow

# Install the Python wrapper for TA-Lib
RUN pip install TA-Lib

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

# Switch back to root user
USER root

# Clean up the TA-Lib source files
RUN rm -R ta-lib ta-lib-0.4.0-src.tar.gz

# Create necessary directories
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/output && \
    chown -R airflow:root /opt/airflow/data /opt/airflow/logs /opt/airflow/output

# Switch back to airflow user for security (docker-compose can override if needed)
USER airflow

# Set Python path to include project root
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Default command (can be overridden in docker-compose)
CMD ["bash"]
