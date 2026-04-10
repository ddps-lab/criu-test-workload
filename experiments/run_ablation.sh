#!/bin/bash
# Ablation study: dump once, restore with 5 strategies
# Usage: bash experiments/run_ablation.sh [wait_seconds] [memcached_mb] [record_count]

set -e

WAIT=${1:-600}        # 10 minutes default
MEM_MB=${2:-2800}
RECORDS=${3:-500000}
OUTDIR="results/ablation_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

SOURCE=172.16.0.1
DEST=172.16.0.2
MINIO=http://172.16.0.254:9000
S3_PREFIX="ablation"

S3_COMMON="--s3-type standard --s3-upload-bucket test-bucket --s3-region us-east-1 \
  --s3-download-endpoint $MINIO --s3-access-key minioadmin --s3-secret-key minioadmin \
  --s3-path-style --s3-prefix $S3_PREFIX"

COMMON="--config config/experiments/memcached_lazy_prefetch.yaml \
  --source-ip $SOURCE --dest-ip $DEST \
  --workload memcached --memcached-memory $MEM_MB --record-count $RECORDS \
  --ycsb-threads 4"

cleanup_dest() {
    ssh -o StrictHostKeyChecking=no ubuntu@$DEST \
        "sudo pkill -9 memcached; sudo pkill -9 java; sudo pkill -9 criu; sudo rm -rf /tmp/criu_checkpoint" 2>/dev/null || true
    sleep 2
}

cleanup_all() {
    cleanup_dest
    ssh -o StrictHostKeyChecking=no ubuntu@$SOURCE \
        "sudo pkill -9 memcached; sudo pkill -9 java; sudo pkill -9 criu; sudo rm -rf /tmp/criu_checkpoint" 2>/dev/null || true
    sleep 2
}

echo "=========================================="
echo " Ablation Study (5 modes)"
echo " Wait: ${WAIT}s, Memory: ${MEM_MB}MB"
echo " Records: $RECORDS"
echo " Output: $OUTDIR"
echo "=========================================="

# ============================================================
# Phase 1: Dump (with dirty tracker for hot VMA)
# ============================================================
echo ""
echo "================================================================"
echo "  PHASE 1: DUMP (S3 direct upload + dirty tracker)"
echo "================================================================"

# Clean S3 prefix before dump
echo "[cleanup] Cleaning S3 prefix: s3://test-bucket/$S3_PREFIX/"
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    aws --endpoint-url $MINIO s3 rm "s3://test-bucket/$S3_PREFIX/" --recursive --quiet 2>/dev/null || true

cleanup_all

python3 experiments/baseline_experiment.py \
    $COMMON \
    --wait-before-dump $WAIT \
    --lazy-mode lazy-prefetch \
    --s3-direct-upload $S3_COMMON \
    --track-dirty-pages \
    --no-cleanup \
    -o "$OUTDIR/dump_phase.json" 2>&1 | tee "$OUTDIR/dump_phase.log"

cleanup_dest

echo ""
echo "[DUMP] Complete. Starting restore-only experiments..."

# ============================================================
# Phase 2: Restore-only experiments
# ============================================================

run_restore() {
    local mode=$1
    local extra_args=$2
    local outfile="$OUTDIR/${mode}.json"

    echo ""
    echo "================================================================"
    echo "  RESTORE: $mode"
    echo "================================================================"

    cleanup_dest

    python3 experiments/baseline_experiment.py \
        $COMMON \
        --restore-only \
        --name "$mode" \
        $extra_args \
        --no-cleanup \
        -o "$outfile" 2>&1 | tee "$OUTDIR/${mode}.log"

    # Save CRIU logs before cleanup overwrites them
    ssh -o StrictHostKeyChecking=no ubuntu@$DEST \
        "cp /tmp/criu_checkpoint/1/criu-lazy-pages.log /tmp/criu_lazy_${mode}.log 2>/dev/null; \
         cp /tmp/criu_checkpoint/1/criu-restore.log /tmp/criu_restore_${mode}.log 2>/dev/null" || true
    scp -o StrictHostKeyChecking=no "ubuntu@$DEST:/tmp/criu_lazy_${mode}.log" "$OUTDIR/" 2>/dev/null || true
    scp -o StrictHostKeyChecking=no "ubuntu@$DEST:/tmp/criu_restore_${mode}.log" "$OUTDIR/" 2>/dev/null || true

    echo "[${mode}] Done."
    cleanup_dest
}

# 1. baseline: rsync + non-lazy (download from S3 to local, rsync to dest)
run_restore "1_baseline" \
    "--lazy-mode none --transfer-method rsync $S3_COMMON"

# 2. s3-lazy-only: S3 lazy, no semi-sync, no async, no hot VMA
run_restore "2_s3_lazy_only" \
    "--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON \
     --no-semi-sync-iov --no-async-prefetch --no-hot-vma-seed"

# 3. +semi-sync: S3 lazy + semi-sync IOV
run_restore "3_semi_sync" \
    "--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON \
     --no-async-prefetch --no-hot-vma-seed"

# 4. +async: S3 lazy + semi-sync + async prefetch
run_restore "4_async" \
    "--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON \
     --no-hot-vma-seed"

# 5. full: S3 lazy + semi-sync + async + hot VMA
run_restore "5_full" \
    "--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON"

# ============================================================
# Summary
# ============================================================
echo ""
echo "=========================================="
echo " Results"
echo "=========================================="
echo ""
echo "  Dump duration: ${DUMP_DUR}s (shared across all modes)"
echo ""
printf "  %-18s %9s %9s %10s %9s %7s %6s %9s %7s\n" \
    "Mode" "Transfer" "Restore" "Downtime" "Cache" "Faults" "HotF" "Pages" "Daemon"
echo "  $(printf '%0.s-' {1..80})"

DUMP_DUR=$(python3 -c "
import json
d = json.load(open('$OUTDIR/dump_phase.json'))
print(f\"{d.get('final_dump',{}).get('duration',0):.2f}\")
" 2>/dev/null || echo "0")

for f in "$OUTDIR"/1_*.json "$OUTDIR"/2_*.json "$OUTDIR"/3_*.json "$OUTDIR"/4_*.json "$OUTDIR"/5_*.json; do
    [ -f "$f" ] || continue
    mode=$(basename "$f" .json)
    python3 -c "
import json
d = json.load(open('$f'))
xfer = d.get('transfer',{}).get('duration', 0)
rest = d.get('restore',{}).get('duration', 0)
lazy_mode = d.get('config',{}).get('checkpoint',{}).get('strategy',{}).get('lazy_mode','none')
dump_d = float('$DUMP_DUR')
# Downtime calculation:
# - baseline (non-lazy): dump + transfer + restore (all blocking)
# - S3 lazy: dump + restore_setup (process resumes after sigreturn,
#   but CRIU restore cmd duration includes setup before process runs)
if lazy_mode == 'none':
    downtime = dump_d + xfer + rest
else:
    downtime = dump_d + rest  # transfer is S3-concurrent, not blocking
cm = d.get('criu_metrics',{}).get('lazy_pages',{})
cache = cm.get('cache',{}).get('hit_rate', '-')
ctrl = cm.get('controller',{})
# Use controller faults if available, fallback to uffd_faults
faults = ctrl.get('faults_processed') or cm.get('uffd_faults', '-')
hot_f = ctrl.get('hot_vma_faults', '-')
uffd = cm.get('uffd_summary',{})
pages = uffd.get('total_pages_transferred', '-')
daemon = cm.get('daemon_duration_s', '-')
daemon_s = f'{daemon:.1f}' if isinstance(daemon, float) else str(daemon)
print(f'  {\"$mode\":<18s} {xfer:>8.2f}s {rest:>8.2f}s {downtime:>9.2f}s {str(cache)+\"%\":>8s} {str(faults):>6s} {str(hot_f):>5s} {str(pages):>8s} {daemon_s:>6s}s')
" 2>/dev/null || printf "  %-18s %10s\n" "$mode" "FAILED"
done
echo "  $(printf '%0.s-' {1..80})"
