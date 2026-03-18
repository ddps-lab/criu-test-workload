#!/bin/bash
# Local Development Setup Script for CRIU Workload Experiments
#
# This script installs dependencies for running workloads locally
# (dirty tracking, checkpoint protocol testing). Unlike ami_setup.sh,
# this does NOT build CRIU or configure kernel parameters.
#
# Usage:
#   chmod +x scripts/setup_local.sh
#   ./scripts/setup_local.sh           # Install all workload dependencies
#   ./scripts/setup_local.sh --minimal # Skip YCSB, PyTorch (lighter install)
#   ./scripts/setup_local.sh --check   # Check what's already installed
#
# Requires: Ubuntu 22.04+ or Debian 12+, sudo access

set -e

YCSB_VERSION="0.17.0"
YCSB_INSTALL_DIR="/opt/ycsb"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}[OK]${NC} $1"; }
fail() { echo -e "  ${RED}[MISSING]${NC} $1"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
info() { echo -e "  $1"; }

# ──────────────────────────────────────────────────
# Check mode: report what's installed
# ──────────────────────────────────────────────────
check_status() {
    echo "=== Dependency Status ==="
    echo ""

    echo "System packages:"
    for pkg in redis-server ffmpeg memcached 7z java; do
        if command -v "$pkg" &>/dev/null; then
            ok "$pkg: $(command -v $pkg)"
        else
            fail "$pkg"
        fi
    done

    echo ""
    echo "Python packages:"
    for pkg in numpy torch redis xgboost paramiko yaml boto3; do
        pymod="$pkg"
        [ "$pkg" = "yaml" ] && pymod="yaml"
        [ "$pkg" = "torch" ] && pymod="torch"
        if python3 -c "import $pymod" 2>/dev/null; then
            ver=$(python3 -c "import $pymod; print(getattr($pymod, '__version__', 'ok'))" 2>/dev/null)
            ok "$pkg: $ver"
        else
            fail "$pkg"
        fi
    done

    echo ""
    echo "YCSB:"
    if [ -f "$YCSB_INSTALL_DIR/bin/ycsb" ]; then
        ok "YCSB installed at $YCSB_INSTALL_DIR"
    else
        fail "YCSB not found at $YCSB_INSTALL_DIR"
    fi

    echo ""
    echo "Python 2 (for YCSB):"
    if command -v python2 &>/dev/null; then
        ok "python2: $(python2 --version 2>&1)"
    else
        fail "python2 (needed for YCSB bin/ycsb script)"
    fi

    echo ""
    echo "Dirty trackers:"
    if [ -f "$PROJECT_DIR/tools/dirty_tracker_c/dirty_tracker" ]; then
        ok "C tracker: $PROJECT_DIR/tools/dirty_tracker_c/dirty_tracker"
    else
        fail "C tracker (run: cd tools/dirty_tracker_c && make)"
    fi
    if [ -f "$PROJECT_DIR/tools/dirty_tracker_go/dirty_tracker" ]; then
        ok "Go tracker: $PROJECT_DIR/tools/dirty_tracker_go/dirty_tracker"
    else
        fail "Go tracker (run: cd tools/dirty_tracker_go && go build -o dirty_tracker .)"
    fi
    ok "Python tracker: tools/dirty_tracker.py (always available)"

    echo ""
    echo "Datasets:"
    if [ -f "/data/HIGGS.csv" ] || [ -f "$HOME/data/HIGGS.csv" ]; then
        ok "Higgs dataset found"
    else
        warn "Higgs dataset not found (optional, run: scripts/download_datasets.sh)"
    fi
}

# ──────────────────────────────────────────────────
# Parse arguments
# ──────────────────────────────────────────────────
MINIMAL=false
CHECK_ONLY=false

for arg in "$@"; do
    case "$arg" in
        --minimal) MINIMAL=true ;;
        --check)   CHECK_ONLY=true ;;
        --help|-h)
            echo "Usage: $0 [--minimal] [--check]"
            echo ""
            echo "Options:"
            echo "  --minimal  Skip YCSB, PyTorch, Python2 (lighter install)"
            echo "  --check    Only check what's installed, don't install anything"
            exit 0
            ;;
    esac
done

if [ "$CHECK_ONLY" = true ]; then
    check_status
    exit 0
fi

echo "=== Local Development Setup ==="
echo "Date: $(date)"
echo "Project: $PROJECT_DIR"
echo "Minimal mode: $MINIMAL"
echo ""

# ──────────────────────────────────────────────────
# 1. System packages
# ──────────────────────────────────────────────────
echo "[1/6] Installing system packages..."
sudo apt-get update -qq

PKGS=(
    build-essential
    git
    curl
    wget
    python3
    python3-pip
    redis-server
    ffmpeg
    memcached
    p7zip-full
)

if [ "$MINIMAL" = false ]; then
    PKGS+=(default-jre-headless)
fi

sudo apt-get install -y "${PKGS[@]}"

# Disable system services (workloads manage their own)
sudo systemctl stop redis-server 2>/dev/null || true
sudo systemctl disable redis-server 2>/dev/null || true
sudo systemctl stop memcached 2>/dev/null || true
sudo systemctl disable memcached 2>/dev/null || true

ok "System packages installed"

# ──────────────────────────────────────────────────
# 2. Python packages
# ──────────────────────────────────────────────────
echo "[2/6] Installing Python packages..."
pip3 install --break-system-packages numpy redis paramiko pyyaml boto3 scp xgboost 2>/dev/null \
    || pip3 install numpy redis paramiko pyyaml boto3 scp xgboost

if [ "$MINIMAL" = false ]; then
    echo "  Installing PyTorch (CPU)..."
    pip3 install --break-system-packages torch --index-url https://download.pytorch.org/whl/cpu 2>/dev/null \
        || pip3 install torch --index-url https://download.pytorch.org/whl/cpu
fi

ok "Python packages installed"

# ──────────────────────────────────────────────────
# 3. YCSB
# ──────────────────────────────────────────────────
if [ "$MINIMAL" = false ]; then
    echo "[3/6] Installing YCSB ${YCSB_VERSION}..."
    if [ ! -d "$YCSB_INSTALL_DIR" ]; then
        cd /tmp
        curl -sSL -O "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz"
        tar xf "ycsb-${YCSB_VERSION}.tar.gz"
        sudo mv "ycsb-${YCSB_VERSION}" "$YCSB_INSTALL_DIR"
        rm -f "ycsb-${YCSB_VERSION}.tar.gz"
        ok "YCSB installed at $YCSB_INSTALL_DIR"
    else
        ok "YCSB already installed at $YCSB_INSTALL_DIR"
    fi

    # Python 2 for YCSB bin/ycsb script
    echo "[3.5/6] Checking Python 2 for YCSB..."
    if ! command -v python2 &>/dev/null; then
        echo "  Building Python 2.7.18 from source (YCSB requires it)..."
        sudo apt-get install -y libffi-dev libsqlite3-dev zlib1g-dev
        cd /tmp
        wget -q "https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz"
        tar xf "Python-2.7.18.tgz"
        cd "Python-2.7.18"
        ./configure --prefix=/usr/local --enable-optimizations 2>&1 | tail -1
        make -j$(nproc) 2>&1 | tail -1
        sudo make altinstall 2>&1 | tail -1
        sudo ln -sf /usr/local/bin/python2.7 /usr/local/bin/python2
        cd /tmp
        rm -rf "Python-2.7.18" "Python-2.7.18.tgz"
        ok "Python 2.7 built and installed"
    else
        ok "Python 2 already available: $(python2 --version 2>&1)"
    fi

    # Patch YCSB shebang
    sudo sed -i '1s|#!/usr/bin/env python$|#!/usr/bin/env python2|' "$YCSB_INSTALL_DIR/bin/ycsb" 2>/dev/null || true
    ok "YCSB shebang patched"
else
    echo "[3/6] Skipping YCSB (--minimal mode)"
fi

# ──────────────────────────────────────────────────
# 4. Build dirty trackers
# ──────────────────────────────────────────────────
echo "[4/6] Building dirty page trackers..."

# C tracker
if [ -f "$PROJECT_DIR/tools/dirty_tracker_c/Makefile" ]; then
    echo "  Building C tracker..."
    cd "$PROJECT_DIR/tools/dirty_tracker_c"
    make -j$(nproc) 2>/dev/null && ok "C dirty tracker built" || warn "C tracker build failed (kernel 6.7+ required)"
    cd "$PROJECT_DIR"
fi

# Go tracker
if command -v go &>/dev/null && [ -f "$PROJECT_DIR/tools/dirty_tracker_go/main.go" ]; then
    echo "  Building Go tracker..."
    cd "$PROJECT_DIR/tools/dirty_tracker_go"
    go build -o dirty_tracker . 2>/dev/null && ok "Go dirty tracker built" || warn "Go tracker build failed"
    cd "$PROJECT_DIR"
else
    warn "Go not installed, skipping Go tracker (Python tracker is fallback)"
fi

ok "Dirty trackers done"

# ──────────────────────────────────────────────────
# 5. Kernel parameters (optional, for CRIU)
# ──────────────────────────────────────────────────
echo "[5/6] Kernel parameters (for CRIU + dirty tracking)..."
echo "  Setting ptrace_scope=0 (required for dirty tracking)..."
echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope > /dev/null 2>/dev/null || warn "Cannot set ptrace_scope"
ok "Kernel parameters set"

# ──────────────────────────────────────────────────
# 6. Verify
# ──────────────────────────────────────────────────
echo "[6/6] Verification..."
echo ""
check_status

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Quick test commands:"
echo "  # Dirty tracking test (matmul, 30s)"
echo "  cd $PROJECT_DIR"
echo "  sudo python3 experiments/dirty_track_only.py --workload matmul --duration 30"
echo ""
echo "  # All workloads checkpoint protocol test"
echo "  bash test_workloads.sh"
echo ""
if [ "$MINIMAL" = true ]; then
    echo "Note: --minimal mode skipped YCSB, PyTorch, Python2."
    echo "  Redis/Memcached YCSB mode and ml_training won't work."
    echo "  Run without --minimal for full setup."
fi
