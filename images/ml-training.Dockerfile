# ml-training-workload — 5-layer DNN, hidden 2048-128 (PyTorch CPU).
# Deps: numpy + torch (CPU wheel).
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir numpy
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
COPY workloads/ml_training_standalone.py /workloads/
WORKDIR /workloads
CMD ["sleep", "infinity"]
