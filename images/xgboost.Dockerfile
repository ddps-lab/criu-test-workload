# xgboost-workload — gradient boosted tree, 7 M × 100, 1000 rounds.
# Deps: numpy + xgboost.
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir numpy xgboost
COPY workloads/xgboost_standalone.py /workloads/
WORKDIR /workloads
CMD ["sleep", "infinity"]
