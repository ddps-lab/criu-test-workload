#!/bin/bash
# AMI Setup Script for CRIU Workload Experiments
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/ddps-lab/criu-test-workload/main/scripts/ami_setup.sh | sudo bash
#
#   Or manually:
#   chmod +x ami_setup.sh
#   sudo ./ami_setup.sh
#
# This script installs all dependencies required for CRIU checkpoint/migration experiments.
# Uses custom CRIU from ddps-lab/criu-s3 with S3 streaming support.

set -e

# Configuration
REPO_URL="https://github.com/ddps-lab/criu-test-workload.git"
WORKLOAD_DIR="/opt/criu_workload"

echo "=== Starting CRIU Workload AMI Setup ==="
echo "Date: $(date)"
echo "Workload directory: $WORKLOAD_DIR"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root (sudo ./ami_setup.sh)"
    exit 1
fi

# 1. System update
echo "[1/7] Updating system..."
apt-get update
apt-get upgrade -y

# 2. Install dependencies
echo "[2/7] Installing system dependencies..."
apt-get install -y \
    build-essential \
    git \
    curl \
    wget \
    htop \
    iotop \
    sysstat \
    net-tools \
    iproute2 \
    libprotobuf-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    protobuf-compiler \
    python3-protobuf \
    pkg-config \
    uuid-dev \
    libbsd-dev \
    libnftables-dev \
    libcap-dev \
    libnl-3-dev \
    libnl-genl-3-dev \
    libnet-dev \
    libaio-dev \
    libgnutls28-dev \
    libdrm-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    python3 \
    python3-pip \
    redis-server \
    ffmpeg \
    memcached \
    p7zip-full \
    default-jre-headless

# Install documentation tools (optional, for building docs)
apt-get install -y --no-install-recommends asciidoc xmlto
snap install --classic aws-cli

# 3. Build CRIU from ddps-lab/criu-s3
echo "[3/7] Building CRIU (ddps-lab/criu-s3)..."
cd /tmp
if [ -d "criu-s3" ]; then
    rm -rf criu-s3
fi
git clone -b ddps-dev https://github.com/ddps-lab/criu-s3.git
cd criu-s3
make clean || true
make -j$(nproc)

# Install CRIU binary to both /usr/local/bin and /usr/local/sbin
install -m 755 ./criu/criu /usr/local/bin/criu
install -m 755 ./criu/criu /usr/local/sbin/criu
setcap cap_checkpoint_restore+eip /usr/local/bin/criu
setcap cap_checkpoint_restore+eip /usr/local/sbin/criu

# Verify CRIU installation
echo "CRIU installed: $(which criu)"
criu --version
cd /tmp
rm -rf criu-s3

# 4. Install Python packages
echo "[4/7] Installing Python packages..."
pip3 install --break-system-packages numpy redis paramiko pyyaml boto3 scp xgboost
pip3 install --break-system-packages torch --index-url https://download.pytorch.org/whl/cpu

# 5. Clone workload repository
echo "[5/7] Cloning workload repository..."
if [ -d "$WORKLOAD_DIR" ]; then
    rm -rf "$WORKLOAD_DIR"
fi
git clone "$REPO_URL" "$WORKLOAD_DIR"
chown -R ubuntu:ubuntu "$WORKLOAD_DIR"

# 5.5 Build dirty page trackers
echo "[5.5/7] Building dirty page trackers..."

# Install Go
GO_VERSION="1.21.0"
if ! command -v go &> /dev/null; then
    echo "Installing Go ${GO_VERSION}..."
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
    tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz
    export PATH=$PATH:/usr/local/go/bin
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile.d/go.sh
fi

# Build Go dirty tracker
echo "Building Go dirty tracker..."
cd "$WORKLOAD_DIR/tools/dirty_tracker_go"
/usr/local/go/bin/go build -o dirty_tracker .

# Build C dirty tracker (PAGEMAP_SCAN for kernel 6.7+)
echo "Building C dirty tracker (PAGEMAP_SCAN)..."
cd "$WORKLOAD_DIR/tools/dirty_tracker_c"
make

# Verify
echo "Dirty trackers built:"
ls -la "$WORKLOAD_DIR/tools/dirty_tracker_go/dirty_tracker"
ls -la "$WORKLOAD_DIR/tools/dirty_tracker_c/dirty_tracker"

# 5.6 Install YCSB benchmark
echo "[5.6/7] Installing YCSB..."
YCSB_VERSION="0.17.0"
if [ ! -d "/opt/ycsb" ]; then
    cd /tmp
    curl -sSL -O "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz"
    tar xf "ycsb-${YCSB_VERSION}.tar.gz"
    mv "ycsb-${YCSB_VERSION}" /opt/ycsb
    rm -f "ycsb-${YCSB_VERSION}.tar.gz"
    echo "YCSB installed at /opt/ycsb"
else
    echo "YCSB already installed at /opt/ycsb"
fi

# 5.7 Install Python 2 for YCSB (bin/ycsb is Python 2 script)
echo "[5.7/7] Installing Python 2 for YCSB..."
PYTHON2_VERSION="2.7.18"
if ! command -v python2 &> /dev/null; then
    # Build Python 2 from source (not in Ubuntu 24.04 repos)
    apt-get install -y libffi-dev libsqlite3-dev zlib1g-dev
    cd /tmp
    wget -q "https://www.python.org/ftp/python/${PYTHON2_VERSION}/Python-${PYTHON2_VERSION}.tgz"
    tar xf "Python-${PYTHON2_VERSION}.tgz"
    cd "Python-${PYTHON2_VERSION}"
    ./configure --prefix=/usr/local --enable-optimizations 2>&1 | tail -1
    make -j$(nproc) 2>&1 | tail -1
    make altinstall 2>&1 | tail -1
    ln -sf /usr/local/bin/python2.7 /usr/local/bin/python2
    cd /tmp
    rm -rf "Python-${PYTHON2_VERSION}" "Python-${PYTHON2_VERSION}.tgz"
    echo "Python 2.7 installed: $(python2 --version 2>&1)"
fi

# Patch YCSB shebang to use python2 explicitly
sed -i '1s|#!/usr/bin/env python$|#!/usr/bin/env python2|' /opt/ycsb/bin/ycsb
echo "YCSB patched to use python2"

# 6. Configure services (disable system services, workloads manage their own)
echo "[6/7] Configuring services..."
systemctl stop redis-server || true
systemctl disable redis-server || true
systemctl stop memcached || true
systemctl disable memcached || true

# 6.5 Pin kernel to 6.8.x (CRIU requires 6.8 for socket option compatibility)
echo "[6.5/7] Pinning kernel to 6.8..."
KERNEL_VER=$(apt-cache search linux-image-6.8 | grep aws | grep -v unsigned | sort -V | tail -1 | awk '{print $1}' | sed 's/linux-image-//')
if [ -n "$KERNEL_VER" ]; then
    apt-get install -y "linux-image-${KERNEL_VER}" "linux-modules-${KERNEL_VER}" "linux-modules-extra-${KERNEL_VER}" || true
    sed -i "s/GRUB_DEFAULT=.*/GRUB_DEFAULT=\"Advanced options for Ubuntu>Ubuntu, with Linux ${KERNEL_VER}\"/" /etc/default/grub
    update-grub
    apt-mark hold "linux-image-${KERNEL_VER}"
    echo "Kernel pinned to ${KERNEL_VER}"
else
    echo "WARNING: Could not find 6.8 AWS kernel package"
fi

# 7. Configure kernel for CRIU
echo "[7/7] Configuring kernel parameters..."
tee /etc/sysctl.d/99-criu.conf << EOF
# CRIU compatibility settings
kernel.ns_last_pid = 0
kernel.unprivileged_userns_clone = 1
kernel.yama.ptrace_scope = 0
# Core dump settings for debugging
kernel.core_pattern = /tmp/core.%e.%p.%t
fs.suid_dumpable = 2
EOF
sysctl --system

# Set ptrace scope immediately
echo 0 > /proc/sys/kernel/yama/ptrace_scope

# Disable apport (Ubuntu crash reporter) for clean core dumps
systemctl stop apport 2>/dev/null || true
systemctl disable apport 2>/dev/null || true

# Set unlimited core dump size for ubuntu user
echo '* soft core unlimited' >> /etc/security/limits.conf
echo '* hard core unlimited' >> /etc/security/limits.conf

# Verify installation
echo ""
echo "=== Installation Summary ==="
echo "CRIU: $(criu --version 2>&1 | head -1)"
echo "Python: $(python3 --version)"
echo "Redis: $(redis-server --version)"
echo "FFmpeg: $(ffmpeg -version 2>&1 | head -1)"
echo "Memcached: $(memcached -h 2>&1 | head -1)"
echo "7zip: $(7z --help 2>&1 | head -1)"
echo "Java: $(java -version 2>&1 | head -1)"
echo "YCSB: $(ls /opt/ycsb/bin/ycsb 2>/dev/null && echo 'installed' || echo 'NOT FOUND')"
python3 -c "import numpy; print(f'NumPy: {numpy.__version__}')"
python3 -c "import torch; print(f'PyTorch: {torch.__version__}')"
python3 -c "import redis; print(f'redis-py: {redis.__version__}')"
python3 -c "import xgboost; print(f'XGBoost: {xgboost.__version__}')"

echo ""
echo "=== CRIU Check ==="
criu check || echo "Warning: Some CRIU checks failed (may be OK in VM)"

echo ""
echo "=== AMI Setup Complete ==="
echo "CRIU source: https://github.com/ddps-lab/criu-s3"
echo "Workload repo: $REPO_URL"
echo "Workload dir: $WORKLOAD_DIR"
echo "Workloads ready: memory, matmul, redis, ml_training, video, dataproc, xgboost, memcached, 7zip"
echo ""
echo "Next steps:"
echo "1. Stop this instance"
echo "2. Create AMI from this instance"
echo "3. Use the AMI for workload nodes"
echo ""
echo "To run experiments:"
echo "  cd $WORKLOAD_DIR"
echo "  python3 run_experiment.py --workload memory --config config/default.yaml"
