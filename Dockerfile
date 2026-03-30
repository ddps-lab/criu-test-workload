FROM python:3.13-slim

# Install system dependencies for all workloads
RUN apt-get update && apt-get install -y --no-install-recommends \
    redis-server \
    memcached \
    ffmpeg \
    p7zip-full \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir numpy redis xgboost
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Install YCSB
RUN curl -sSL -O "https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz" \
    && tar xf ycsb-0.17.0.tar.gz \
    && mv ycsb-0.17.0 /opt/ycsb \
    && rm -f ycsb-0.17.0.tar.gz

# Copy all standalone workload scripts
COPY workloads/*_standalone.py /workloads/

WORKDIR /workloads

# Default: just sleep (override command in pod spec)
CMD ["sleep", "infinity"]
