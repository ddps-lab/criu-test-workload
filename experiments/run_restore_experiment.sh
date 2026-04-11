#!/bin/bash
# Restore-only experiment: run on dest instance directly
# S3에서 checkpoint을 가져와서 restore. Source instance 불필요.
#
# Usage:
#   bash run_restore_experiment.sh --workload ml_training --s3-prefix ml-training --repeat 5
#   bash run_restore_experiment.sh --workload memcached --s3-prefix memcached --repeat 5 \
#       --extra-args "--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
#   bash run_restore_experiment.sh --workload memcached --s3-prefix memcached-4gb --repeat 5 \
#       --extra-args "--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"

set +e  # Don't exit on errors — cleanup commands may return non-zero

# ============================================================
# Parse arguments
# ============================================================
WORKLOAD=""
S3_PREFIX=""
REPEAT=5
MODE=""           # empty = all 5 modes
EXTRA_ARGS=""
S3_BUCKET="mhsong-criu-checkpoints"
S3_RESULTS_BUCKET="mhsong-criu-results"
S3_REGION="us-west-2"
S3_ENDPOINT="https://s3.us-west-2.amazonaws.com"

while [[ $# -gt 0 ]]; do
    case $1 in
        --workload)       WORKLOAD="$2"; shift 2 ;;
        --s3-prefix)      S3_PREFIX="$2"; shift 2 ;;
        --repeat)         REPEAT="$2"; shift 2 ;;
        --mode)           MODE="$2"; shift 2 ;;
        --extra-args)     EXTRA_ARGS="$2"; shift 2 ;;
        --s3-bucket)      S3_BUCKET="$2"; shift 2 ;;
        --s3-results)     S3_RESULTS_BUCKET="$2"; shift 2 ;;
        *)                echo "Unknown: $1"; exit 1 ;;
    esac
done

if [ -z "$WORKLOAD" ] || [ -z "$S3_PREFIX" ]; then
    echo "Usage: $0 --workload WORKLOAD --s3-prefix PREFIX [--repeat N] [--extra-args '...']"
    exit 1
fi

# AWS credentials from environment
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"
    exit 1
fi

OUTDIR="/tmp/results/${S3_PREFIX}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

DEST_IP=$(hostname -I | awk '{print $1}')

# Ensure SSH to self works without host key issues
ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -H $DEST_IP >> ~/.ssh/known_hosts 2>/dev/null || true

S3_COMMON="--s3-type standard --s3-upload-bucket $S3_BUCKET --s3-region $S3_REGION \
  --s3-download-endpoint $S3_ENDPOINT \
  --s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY \
  --s3-prefix $S3_PREFIX"

# source-ip = dest-ip (restore on self, S3 provides checkpoint)
COMMON="--config config/experiments/memcached_lazy_prefetch.yaml \
  --source-ip 127.0.0.1 --dest-ip $DEST_IP \
  --workload $WORKLOAD --duration 0 $EXTRA_ARGS"

echo "=========================================="
echo " Restore Experiment"
echo " Workload: $WORKLOAD"
echo " S3 prefix: $S3_PREFIX"
echo " Repeat: $REPEAT"
echo " Mode: ${MODE:-all 5}"
echo " Output: $OUTDIR"
echo " Instance: $(hostname) ($DEST_IP)"
echo "=========================================="

# ============================================================
# Mode definitions
# ============================================================
declare -A MODE_ARGS
# Baseline: handled separately (S3 download + non-lazy restore)
MODE_ARGS[1_baseline]="BASELINE_SPECIAL"
MODE_ARGS[2_s3_lazy_only]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-semi-sync-iov --no-async-prefetch --no-hot-vma-seed"
MODE_ARGS[3_semi_sync]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-async-prefetch --no-hot-vma-seed"
MODE_ARGS[4_async]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-hot-vma-seed"
MODE_ARGS[5_full]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON"

MODE_ORDER=(1_baseline 2_s3_lazy_only 3_semi_sync 4_async 5_full)

# ============================================================
# Cleanup function
# ============================================================
cleanup() {
    # Note: avoid pkill -f patterns that could match this script itself
    sudo pkill -9 memcached || true
    sudo pkill -9 redis-server || true
    sudo pkill -9 -x java || true
    sudo pkill -9 -x criu || true
    # Kill standalone workload scripts by exact process name match
    sudo pgrep -f "python3.*_standalone.py" | xargs -r sudo kill -9 || true
    sudo rm -rf /tmp/criu_checkpoint || true
    sleep 2
} 2>/dev/null

# ============================================================
# Run restore
# ============================================================
run_baseline() {
    local run_num=$1
    local outfile="$OUTDIR/1_baseline_run${run_num}.json"
    local logfile="$OUTDIR/1_baseline_run${run_num}.log"

    echo ""
    echo "--- 1_baseline run $run_num/$REPEAT ($(date +%H:%M:%S)) ---"

    cleanup
    mkdir -p /tmp/criu_checkpoint/1

    # Step 1: Download checkpoint from S3 to local
    local start_dl=$(date +%s%3N)
    aws s3 sync "s3://$S3_BUCKET/$S3_PREFIX/" /tmp/criu_checkpoint/1/ \
        --region $S3_REGION --quiet
    local end_dl=$(date +%s%3N)
    local dl_ms=$((end_dl - start_dl))
    echo "  S3 download: ${dl_ms}ms"

    # Step 2: Non-lazy restore
    local start_r=$(date +%s%3N)
    sudo criu restore \
        -D /tmp/criu_checkpoint/1 \
        --shell-job \
        --tcp-close \
        -v4 \
        --log-file /tmp/criu_checkpoint/1/criu-restore.log \
        2>/dev/null
    local rc=$?
    local end_r=$(date +%s%3N)
    local r_ms=$((end_r - start_r))
    echo "  Restore: ${r_ms}ms (exit=$rc)"

    # Save results as JSON
    python3 -c "
import json
json.dump({
    'transfer': {'duration': $dl_ms/1000, 'method': 's3_download'},
    'restore': {'duration': $r_ms/1000},
    'mode': '1_baseline',
    'run': $run_num
}, open('$outfile', 'w'), indent=2)
" 2>/dev/null

    # Save logs
    cp /tmp/criu_checkpoint/1/criu-restore.log "$OUTDIR/1_baseline_run${run_num}_restore.log" 2>/dev/null || true
    echo "  Total: $((dl_ms + r_ms))ms"
}

run_restore() {
    local mode=$1
    local run_num=$2
    local args="${MODE_ARGS[$mode]}"
    local outfile="$OUTDIR/${mode}_run${run_num}.json"
    local logfile="$OUTDIR/${mode}_run${run_num}.log"

    # Baseline is handled separately
    if [ "$args" = "BASELINE_SPECIAL" ]; then
        run_baseline "$run_num"
        return
    fi

    echo ""
    echo "--- $mode run $run_num/$REPEAT ($(date +%H:%M:%S)) ---"

    cleanup

    python3 experiments/baseline_experiment.py \
        $COMMON \
        --restore-only \
        --name "${mode}_run${run_num}" \
        $args \
        --no-cleanup \
        -o "$outfile" 2>&1 | tee "$logfile" | grep -E 'COMPLETED|FAILED|Faults|Cache|Daemon|Restore:|ERROR|WARNING'

    # Save CRIU logs
    if [ -d /tmp/criu_checkpoint/1 ]; then
        cp /tmp/criu_checkpoint/1/criu-lazy-pages.log "$OUTDIR/${mode}_run${run_num}_lazy.log" 2>/dev/null || true
        cp /tmp/criu_checkpoint/1/criu-restore.log "$OUTDIR/${mode}_run${run_num}_restore.log" 2>/dev/null || true
    fi

    # Quick health check
    sudo dmesg | grep segfault | tail -1 2>/dev/null || true
}

# ============================================================
# Main execution
# ============================================================
cd /opt/criu_workload

if [ -n "$MODE" ]; then
    MODES_TO_RUN=("$MODE")
else
    MODES_TO_RUN=("${MODE_ORDER[@]}")
fi

for mode in "${MODES_TO_RUN[@]}"; do
    echo ""
    echo "=========================================="
    echo " MODE: $mode"
    echo "=========================================="
    for run in $(seq 1 $REPEAT); do
        run_restore "$mode" "$run"
    done
done

cleanup

# ============================================================
# Upload results to S3
# ============================================================
echo ""
echo "=========================================="
echo " Uploading results to S3"
echo "=========================================="

RESULTS_PREFIX="${S3_PREFIX}/$(date +%Y%m%d_%H%M%S)"
aws s3 sync "$OUTDIR" "s3://$S3_RESULTS_BUCKET/$RESULTS_PREFIX/" \
    --region $S3_REGION --quiet
echo "Results uploaded to s3://$S3_RESULTS_BUCKET/$RESULTS_PREFIX/"

# ============================================================
# Summary
# ============================================================
echo ""
echo "=========================================="
echo " Summary: $WORKLOAD ($S3_PREFIX)"
echo "=========================================="

for f in "$OUTDIR"/*.json; do
    [ -f "$f" ] || continue
    fname=$(basename "$f" .json)
    python3 -c "
import json
d = json.load(open('$f'))
rest = d.get('restore',{}).get('duration', 0)
cm = d.get('criu_metrics',{}).get('lazy_pages',{})
faults = cm.get('uffd_faults', '-')
fs = cm.get('fault_stats',{})
if not isinstance(fs, dict): fs = {}
s3_f = fs.get('s3_served','-')
cache_f = fs.get('cache_served','-')
stall = fs.get('stall_ms_avg','-')
cache_d = cm.get('cache',{})
hr = cache_d.get('hit_rate','-') if isinstance(cache_d, dict) else '-'
daemon = cm.get('daemon_duration_s','-')
ds = f'{daemon:.1f}' if isinstance(daemon,(int,float)) else '-'
ss = f'{stall:.1f}' if isinstance(stall,(int,float)) else '-'
hrs = f'{hr}%' if hr != '-' else '-'
print(f'  {\"$fname\":<30s} rest={rest:>6.2f}s faults={str(faults):>5s} S3={str(s3_f):>5s} cache={str(cache_f):>5s} hit={hrs:>6s} stall={ss:>6s}ms daemon={ds:>6s}s')
" 2>/dev/null || echo "  $fname: PARSE_ERR"
done

echo ""
echo "Done. Results in $OUTDIR and s3://$S3_RESULTS_BUCKET/$RESULTS_PREFIX/"
