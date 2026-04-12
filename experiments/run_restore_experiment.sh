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
MODE=""           # empty = all 4 modes
EXTRA_ARGS=""
S3_BUCKET="mhsong-criu-checkpoints"
S3_RESULTS_BUCKET="mhsong-criu-results"
S3_REGION="us-west-2"
S3_ENDPOINT="https://s3.us-west-2.amazonaws.com"
S3_TYPE="standard"           # standard | express-one-zone | cloudfront
RESULTS_SUFFIX=""            # tag appended to results timestamp dir
AUTO_TERMINATE=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --workload)             WORKLOAD="$2"; shift 2 ;;
        --s3-prefix)            S3_PREFIX="$2"; shift 2 ;;
        --repeat)               REPEAT="$2"; shift 2 ;;
        --mode)                 MODE="$2"; shift 2 ;;
        --extra-args)           EXTRA_ARGS="$2"; shift 2 ;;
        --s3-bucket)            S3_BUCKET="$2"; shift 2 ;;
        --s3-region)            S3_REGION="$2"; shift 2 ;;
        --s3-endpoint)          S3_ENDPOINT="$2"; shift 2 ;;
        --s3-type)              S3_TYPE="$2"; shift 2 ;;
        --s3-results)           S3_RESULTS_BUCKET="$2"; shift 2 ;;
        --s3-results-suffix)    RESULTS_SUFFIX="$2"; shift 2 ;;
        --auto-terminate)       AUTO_TERMINATE=1; shift ;;
        *)                      echo "Unknown: $1"; exit 1 ;;
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

_TS=$(date +%Y%m%d_%H%M%S)
if [ -n "$RESULTS_SUFFIX" ]; then
    OUTDIR="/tmp/results/${S3_PREFIX}_${RESULTS_SUFFIX}_${_TS}"
else
    OUTDIR="/tmp/results/${S3_PREFIX}_${_TS}"
fi
mkdir -p "$OUTDIR"

DEST_IP=$(hostname -I | awk '{print $1}')

# Ensure SSH to self works without host key issues
ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -H $DEST_IP >> ~/.ssh/known_hosts 2>/dev/null || true

S3_COMMON="--s3-type $S3_TYPE --s3-upload-bucket $S3_BUCKET --s3-region $S3_REGION \
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

# Default: skip 2_s3_lazy_only (much slower than baseline on real S3, run separately if needed)
MODE_ORDER=(1_baseline 3_semi_sync 4_async 5_full)

# ============================================================
# Cleanup function
# ============================================================
cleanup() {
    # Kill standalone wrappers FIRST so they don't respawn children.
    # Use ps+awk and exclude sudo/pgrep/grep/bash to avoid self-killing the
    # cleanup shell (sudo's own cmdline contains the search pattern).
    local pids
    pids=$(ps -eo pid,cmd --no-headers 2>/dev/null \
        | awk '/python3 .*_standalone\.py/ && !/sudo|pgrep|grep|awk/ {print $1}')
    if [ -n "$pids" ]; then
        sudo kill -9 $pids 2>/dev/null || true
    fi
    sudo pkill -9 memcached 2>/dev/null || true
    sudo pkill -9 redis-server 2>/dev/null || true
    sudo pkill -9 -x java 2>/dev/null || true
    sudo pkill -9 -x criu 2>/dev/null || true
    sudo rm -rf /tmp/criu_checkpoint /tmp/hsperfdata_ubuntu /tmp/hsperfdata_root 2>/dev/null || true
    sleep 3
}

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

    # Step 1b: Extract aux_files.tar.gz (hsperfdata) so Java can find its perf files
    if [ -f /tmp/criu_checkpoint/1/aux_files.tar.gz ]; then
        sudo tar xzf /tmp/criu_checkpoint/1/aux_files.tar.gz -C / 2>/dev/null || true
    fi

    # Step 2: Non-lazy restore.
    # --restore-detached: criu forks the root and exits as soon as
    # "Restore finished successfully. Tasks resumed." is reached. Without
    # this, criu blocks waiting for the restored daemon to exit, which
    # never happens for long-running workloads. The criu binary's elapsed
    # time therefore equals the actual restore-completion time.
    # TCP flags only for workloads that have TCP state (redis/memcached);
    # other workloads have no TCP sockets in their dump.
    local TCP_FLAGS=""
    case "$WORKLOAD" in
        redis|memcached) TCP_FLAGS="--tcp-established" ;;
    esac

    local start_r=$(date +%s%3N)
    sudo criu restore \
        -D /tmp/criu_checkpoint/1 \
        --shell-job \
        --restore-detached \
        $TCP_FLAGS \
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
        -o "$outfile" 2>"$logfile.err" | tee "$logfile" | grep -E 'COMPLETED|FAILED|Faults|Cache|Daemon|Restore:|ERROR|WARNING' || true

    # Save CRIU logs
    if [ -d /tmp/criu_checkpoint/1 ]; then
        cp /tmp/criu_checkpoint/1/criu-lazy-pages.log "$OUTDIR/${mode}_run${run_num}_lazy.log" 2>/dev/null || true
        cp /tmp/criu_checkpoint/1/criu-restore.log "$OUTDIR/${mode}_run${run_num}_restore.log" 2>/dev/null || true
    fi

    # Quick health check
    sudo dmesg | grep segfault | tail -1 2>/dev/null || true
}

# ============================================================
# Backend-aware cache warmup
# ============================================================
echo ""
echo "=========================================="
echo " Cache Warmup (5 rounds, backend=$S3_TYPE)"
echo "=========================================="
mkdir -p /tmp/s3_warmup
case "$S3_TYPE" in
    standard)
        for i in $(seq 1 5); do
            aws s3 sync "s3://$S3_BUCKET/$S3_PREFIX/" /tmp/s3_warmup/ \
                --region "$S3_REGION" --quiet
            rm -rf /tmp/s3_warmup/* 2>/dev/null
            echo "  warmup $i/5 done"
        done
        ;;
    express-one-zone)
        # Express directory buckets do NOT support `s3 sync`. Use cp --recursive
        # which goes through the same backend cache path as the experiment will.
        for i in $(seq 1 5); do
            aws s3 cp "s3://$S3_BUCKET/$S3_PREFIX/" /tmp/s3_warmup/ \
                --recursive --region "$S3_REGION" --endpoint-url "$S3_ENDPOINT" --quiet
            rm -rf /tmp/s3_warmup/* 2>/dev/null
            echo "  warmup $i/5 done"
        done
        ;;
    cloudfront)
        # CloudFront sits in front of the origin S3 bucket. We list keys via
        # the AWS SDK (cheap, server-side) and then GET each object through
        # the distribution to populate the edge cache. The CloudFront origin
        # is configured to point at $CF_ORIGIN_BUCKET in $CF_ORIGIN_REGION.
        : "${CF_ORIGIN_BUCKET:=mhsong-criu-checkpoints}"
        : "${CF_ORIGIN_REGION:=us-west-2}"
        KEYS=$(aws s3 ls "s3://$CF_ORIGIN_BUCKET/$S3_PREFIX/" \
                  --region "$CF_ORIGIN_REGION" --recursive | awk '{print $NF}')
        for i in $(seq 1 5); do
            for key in $KEYS; do
                curl -fsS -o /dev/null "$S3_ENDPOINT/$key" || true
            done
            echo "  warmup $i/5 done"
        done
        ;;
    *)
        echo "Unknown S3_TYPE: $S3_TYPE"; exit 1 ;;
esac
rm -rf /tmp/s3_warmup
echo "Warmup complete."

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

if [ -n "$RESULTS_SUFFIX" ]; then
    RESULTS_PREFIX="${S3_PREFIX}_${RESULTS_SUFFIX}/$(date +%Y%m%d_%H%M%S)"
else
    RESULTS_PREFIX="${S3_PREFIX}/$(date +%Y%m%d_%H%M%S)"
fi
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

# ============================================================
# Auto-terminate instance if --auto-terminate was set
# ============================================================
if [ "$AUTO_TERMINATE" -eq 1 ]; then
    INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')" \
        http://169.254.169.254/latest/meta-data/instance-id)
    echo "Auto-terminating instance $INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $S3_REGION
fi
