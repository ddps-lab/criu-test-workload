# redis-workload — paper benchmark wrapper around redis-server.
#
# Base is ubuntu:24.04 to match the VM AMI's redis-server version
# (Ubuntu 7.0.15) — keeps the paper's container-side and VM-side
# behaving identically under the same YCSB redis-binding.
#
# Same rationale as memcached.Dockerfile for avoiding alpine: paper
# smoke run with upstream redis:7-alpine occasionally produced
# ptrace-seize EPERM under YCSB load. glibc + apt-managed binary
# behaves consistently across all paper workloads.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates redis-server python3 \
    && rm -rf /var/lib/apt/lists/*
CMD ["sleep", "infinity"]
