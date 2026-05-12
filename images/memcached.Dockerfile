# memcached-workload — paper benchmark wrapper around memcached.
#
# Base is ubuntu:24.04 (not python:3.13-slim / debian-trixie) because
# the VM-AMI side of the paper experiment also runs Ubuntu 24.04, and
# Ubuntu's memcached 1.6.24 is the version the YCSB ycsb-loadgen image
# (spymemcached 2.11.4) is known to interoperate with. debian-trixie's
# memcached 1.6.38 introduced ASCII-protocol changes that drop
# spymemcached connections mid-INSERT ("Disconnected unexpected" loop).
#
# We don't use upstream memcached:1.6-alpine: alpine's musl libc +
# memcached's setuid handling caused CRIU to fail ptrace-seize the
# memcached PID with EPERM even when the container ran with
# runAsUser=0 + SYS_PTRACE. Switching to a glibc-based image with the
# system-packaged memcached resolves it.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates memcached python3 \
    && rm -rf /var/lib/apt/lists/*
CMD ["sleep", "infinity"]
