# matmul-workload — power-iteration eigenvalue solver.
# Deps: numpy only.
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir numpy
COPY workloads/matmul_standalone.py /workloads/
WORKDIR /workloads
CMD ["sleep", "infinity"]
