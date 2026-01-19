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
    ffmpeg

# Install documentation tools (optional, for building docs)
apt-get install -y --no-install-recommends asciidoc xmlto
snap install --classic aws-cli

# 3. Build CRIU from ddps-lab/criu-s3
echo "[3/7] Building CRIU (ddps-lab/criu-s3)..."
cd /tmp
if [ -d "criu-s3" ]; then
    rm -rf criu-s3
fi
git clone -b experiment-v2 https://github.com/ddps-lab/criu-s3.git
cd criu-s3
make clean || true
make -j$(nproc)

# Install CRIU binary with proper permissions
install -m 755 ./criu/criu /usr/local/bin/criu
setcap cap_checkpoint_restore+eip /usr/local/bin/criu

# Verify CRIU installation
echo "CRIU installed: $(which criu)"
criu --version
cd /tmp
rm -rf criu-s3

# 4. Install Python packages
echo "[4/7] Installing Python packages..."
pip3 install --break-system-packages numpy redis paramiko pyyaml boto3 scp
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

# 6. Configure Redis (disable system service)
echo "[6/7] Configuring services..."
systemctl stop redis-server || true
systemctl disable redis-server || true

# 7. Configure kernel for CRIU
echo "[7/7] Configuring kernel parameters..."
tee /etc/sysctl.d/99-criu.conf << EOF
# CRIU compatibility settings
kernel.ns_last_pid = 0
kernel.unprivileged_userns_clone = 1
kernel.yama.ptrace_scope = 0
EOF
sysctl --system

# Set ptrace scope immediately
echo 0 > /proc/sys/kernel/yama/ptrace_scope

# Verify installation
echo ""
echo "=== Installation Summary ==="
echo "CRIU: $(criu --version 2>&1 | head -1)"
echo "Python: $(python3 --version)"
echo "Redis: $(redis-server --version)"
echo "FFmpeg: $(ffmpeg -version 2>&1 | head -1)"
python3 -c "import numpy; print(f'NumPy: {numpy.__version__}')"
python3 -c "import torch; print(f'PyTorch: {torch.__version__}')"
python3 -c "import redis; print(f'redis-py: {redis.__version__}')"

echo ""
echo "=== CRIU Check ==="
criu check || echo "Warning: Some CRIU checks failed (may be OK in VM)"

echo ""
echo "=== AMI Setup Complete ==="
echo "CRIU source: https://github.com/ddps-lab/criu-s3"
echo "Workload repo: $REPO_URL"
echo "Workload dir: $WORKLOAD_DIR"
echo "Workloads ready: memory, matmul, redis, ml_training, jupyter, video, dataproc"
echo ""
echo "Next steps:"
echo "1. Stop this instance"
echo "2. Create AMI from this instance"
echo "3. Use the AMI for workload nodes"
echo ""
echo "To run experiments:"
echo "  cd $WORKLOAD_DIR"
echo "  python3 run_experiment.py --workload memory --config config/default.yaml"
