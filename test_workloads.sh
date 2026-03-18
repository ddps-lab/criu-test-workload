#!/bin/bash
# Test all workloads: launch, wait for checkpoint_ready, verify, then remove checkpoint_flag to trigger exit
# Each workload runs for max 30s

set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0
SKIP=0
RESULTS=""

test_workload() {
    local name="$1"
    local script="$2"
    local extra_args="$3"
    local working_dir="/tmp/test_workload_${name}"
    local timeout=60

    echo ""
    echo "=========================================="
    echo "Testing: $name"
    echo "=========================================="

    # Clean up
    rm -rf "$working_dir"
    mkdir -p "$working_dir"

    # Create checkpoint_flag (workload waits for its removal)
    touch "$working_dir/checkpoint_flag"

    # Start workload in background
    local cmd="python3 $BASE_DIR/$script --working_dir $working_dir --duration 30 $extra_args"
    echo "CMD: $cmd"
    eval "$cmd" &
    local pid=$!

    # Wait for checkpoint_ready (max timeout seconds)
    local elapsed=0
    while [ ! -f "$working_dir/checkpoint_ready" ] && [ $elapsed -lt $timeout ]; do
        sleep 1
        elapsed=$((elapsed + 1))
        # Check if process died
        if ! kill -0 $pid 2>/dev/null; then
            echo "FAIL: $name - process died before checkpoint_ready (after ${elapsed}s)"
            wait $pid 2>/dev/null || true
            FAIL=$((FAIL + 1))
            RESULTS="$RESULTS\nFAIL: $name (died before ready)"
            return 1
        fi
    done

    if [ ! -f "$working_dir/checkpoint_ready" ]; then
        echo "FAIL: $name - checkpoint_ready not created within ${timeout}s"
        kill $pid 2>/dev/null; wait $pid 2>/dev/null || true
        FAIL=$((FAIL + 1))
        RESULTS="$RESULTS\nFAIL: $name (no checkpoint_ready)"
        return 1
    fi

    # Read checkpoint_ready content
    local ready_content=$(cat "$working_dir/checkpoint_ready")
    echo "checkpoint_ready content: $ready_content"

    # Verify format: ready:<pid>
    if ! echo "$ready_content" | grep -qE '^ready:[0-9]+$'; then
        echo "FAIL: $name - checkpoint_ready format invalid: $ready_content"
        kill $pid 2>/dev/null; wait $pid 2>/dev/null || true
        FAIL=$((FAIL + 1))
        RESULTS="$RESULTS\nFAIL: $name (bad ready format)"
        return 1
    fi

    local ready_pid=$(echo "$ready_content" | cut -d: -f2)
    echo "Ready PID: $ready_pid (wrapper), elapsed: ${elapsed}s"

    # Let it run for a few seconds
    sleep 3

    # Check process is still alive
    if ! kill -0 $pid 2>/dev/null; then
        echo "FAIL: $name - process died after checkpoint_ready"
        wait $pid 2>/dev/null || true
        FAIL=$((FAIL + 1))
        RESULTS="$RESULTS\nFAIL: $name (died after ready)"
        return 1
    fi

    # Remove checkpoint_flag to signal restore complete
    echo "Removing checkpoint_flag to simulate restore..."
    rm -f "$working_dir/checkpoint_flag"

    # Wait for process to exit (max 30s)
    local wait_elapsed=0
    while kill -0 $pid 2>/dev/null && [ $wait_elapsed -lt 30 ]; do
        sleep 1
        wait_elapsed=$((wait_elapsed + 1))
    done

    if kill -0 $pid 2>/dev/null; then
        echo "WARN: $name - process didn't exit after flag removal, killing..."
        kill $pid 2>/dev/null
        wait $pid 2>/dev/null || true
        echo "FAIL: $name - didn't exit on checkpoint_flag removal"
        FAIL=$((FAIL + 1))
        RESULTS="$RESULTS\nFAIL: $name (didn't exit)"
        return 1
    fi

    wait $pid 2>/dev/null || true
    echo "PASS: $name (ready in ${elapsed}s, exited in ${wait_elapsed}s)"
    PASS=$((PASS + 1))
    RESULTS="$RESULTS\nPASS: $name (ready=${elapsed}s, exit=${wait_elapsed}s)"

    # Clean up
    rm -rf "$working_dir"
    return 0
}

echo "============================================"
echo "  Workload Checkpoint Protocol Test Suite"
echo "============================================"

# 1. Memory
test_workload "memory" "workloads/memory_standalone.py" "--mb_size 32 --interval 1.0" || true

# 2. MatMul
test_workload "matmul" "workloads/matmul_standalone.py" "--matrix-size 512" || true

# 3. Redis (built-in mode)
# Stop any existing redis first
redis-cli -p 16379 shutdown nosave 2>/dev/null || true
sleep 1
test_workload "redis_builtin" "workloads/redis_standalone.py" "--redis-port 16379 --num-keys 1000 --value-size 128" || true

# 4. Redis (YCSB mode) - skip if no YCSB
if [ -d "/opt/ycsb" ]; then
    redis-cli -p 16380 shutdown nosave 2>/dev/null || true
    sleep 1
    test_workload "redis_ycsb" "workloads/redis_standalone.py" "--redis-port 16380 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 1000" || true
else
    echo ""
    echo "SKIP: redis_ycsb (no /opt/ycsb)"
    SKIP=$((SKIP + 1))
    RESULTS="$RESULTS\nSKIP: redis_ycsb (no YCSB)"
fi

# 5. ML Training - skip if no torch
if python3 -c "import torch" 2>/dev/null; then
    test_workload "ml_training" "workloads/ml_training_standalone.py" "--model-size small --batch-size 32" || true
else
    echo ""
    echo "SKIP: ml_training (no PyTorch)"
    SKIP=$((SKIP + 1))
    RESULTS="$RESULTS\nSKIP: ml_training (no torch)"
fi

# 6. Video (ffmpeg)
test_workload "video" "workloads/video_standalone.py" "--resolution 320x240 --fps 10" || true

# 7. DataProc
test_workload "dataproc" "workloads/dataproc_standalone.py" "--num-rows 10000 --num-cols 10" || true

# 8. XGBoost
test_workload "xgboost" "workloads/xgboost_standalone.py" "--dataset synthetic --num-samples 5000 --num-features 10 --num-rounds 100" || true

# 9. Memcached (YCSB mode) - skip if no YCSB
if [ -d "/opt/ycsb" ]; then
    test_workload "memcached" "workloads/memcached_standalone.py" "--port 11212 --memory-mb 64 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 1000" || true
else
    echo ""
    echo "SKIP: memcached (no /opt/ycsb)"
    SKIP=$((SKIP + 1))
    RESULTS="$RESULTS\nSKIP: memcached (no YCSB)"
fi

# 10. 7zip
test_workload "7zip" "workloads/sevenzip_standalone.py" "--compression-level 5 --input-size-mb 16 --threads 1" || true

echo ""
echo ""
echo "============================================"
echo "  RESULTS SUMMARY"
echo "============================================"
echo -e "$RESULTS"
echo ""
echo "PASS: $PASS, FAIL: $FAIL, SKIP: $SKIP"
echo "============================================"
