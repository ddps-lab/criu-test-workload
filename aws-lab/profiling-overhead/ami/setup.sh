#!/bin/bash
# Profiling overhead AMI setup
# This is a thin wrapper around the main AMI setup script.
# See scripts/ami_setup.sh for the full setup.

set -ex

REPO_URL="https://github.com/ddps-lab/criu-test-workload.git"
WORKLOAD_DIR="/opt/criu_workload"

# Clone and run main setup
git clone "$REPO_URL" "$WORKLOAD_DIR"
cd "$WORKLOAD_DIR"
sudo bash scripts/ami_setup.sh

echo "Profiling overhead AMI ready"
