#!/bin/bash
# Run all 6 workloads in parallel
set -e
SCRIPT="/spot_kubernetes/criu_workload/restore_experiment_final_v2.sh"
RESULTS="/spot_kubernetes/criu_workload/results/restore_faults_v4"
LOG_DIR="/tmp/restore_v5"

mkdir -p "$LOG_DIR" "$RESULTS"

for wl in matmul redis memcached ml_training xgboost dataproc; do
    echo "Starting $wl..."
    sudo bash "$SCRIPT" "$wl" > "${LOG_DIR}/${wl}.log" 2>&1 &
done

echo "All 6 workloads started in parallel. PIDs:"
jobs -l
echo "Logs: ${LOG_DIR}/*.log"
echo "Waiting..."
wait
echo "ALL DONE at $(date)"
