#!/bin/bash
# Ablation study: dump once, restore with 7 strategies
#
# Usage:
#   bash run_ablation.sh [options]
#
# Modes:
#   --dump-only         Only run dump phase (upload to S3)
#   --restore-only      Only run restore phases (reuse existing S3 dump)
#   --mode MODE         Run only a specific restore mode (e.g., 5_full)
#   --repeat N          Repeat each restore N times (default: 1)
#
# Examples:
#   bash run_ablation.sh --dump-only --wait 600 --memory 11264 --records 8500000
#   bash run_ablation.sh --restore-only --repeat 5
#   bash run_ablation.sh --restore-only --mode 5_full --repeat 3
#   bash run_ablation.sh                                  # dump + all restores

set -e

# ============================================================
# Parse arguments
# ============================================================
WAIT=600
DURATION=0          # YCSB duration (0 = auto: WAIT + 120s)
MEM_MB=11264
RECORDS=8500000
THREADS=4
DUMP_ONLY=0
RESTORE_ONLY=0
SINGLE_MODE=""
REPEAT=1

while [[ $# -gt 0 ]]; do
    case $1 in
        --dump-only)     DUMP_ONLY=1; shift ;;
        --restore-only)  RESTORE_ONLY=1; shift ;;
        --mode)          SINGLE_MODE="$2"; shift 2 ;;
        --repeat)        REPEAT="$2"; shift 2 ;;
        --wait)          WAIT="$2"; shift 2 ;;
        --duration)      DURATION="$2"; shift 2 ;;
        --memory)        MEM_MB="$2"; shift 2 ;;
        --records)       RECORDS="$2"; shift 2 ;;
        --threads)       THREADS="$2"; shift 2 ;;
        *)               echo "Unknown option: $1"; exit 1 ;;
    esac
done

OUTDIR="results/ablation_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

SOURCE=172.16.0.1
DEST=172.16.0.2
MINIO=http://172.16.0.254:9000
S3_PREFIX="ablation"

S3_COMMON="--s3-type standard --s3-upload-bucket test-bucket --s3-region us-east-1 \
  --s3-download-endpoint $MINIO --s3-access-key minioadmin --s3-secret-key minioadmin \
  --s3-path-style --s3-prefix $S3_PREFIX"

# YCSB must run longer than dump wait so TCP connections are alive at dump time
if [ "$DURATION" -eq 0 ]; then
    DURATION=$((WAIT + 120))
fi

COMMON="--config config/experiments/memcached_lazy_prefetch.yaml \
  --source-ip $SOURCE --dest-ip $DEST \
  --workload memcached --memcached-memory $MEM_MB --record-count $RECORDS \
  --ycsb-threads $THREADS --duration $DURATION"

echo "=========================================="
echo " Ablation Study (7 modes)"
echo " Wait: ${WAIT}s, Memory: ${MEM_MB}MB"
echo " Records: $RECORDS, Threads: $THREADS, YCSB duration: ${DURATION}s"
echo " Dump-only: $DUMP_ONLY, Restore-only: $RESTORE_ONLY"
echo " Single mode: ${SINGLE_MODE:-all}, Repeat: $REPEAT"
echo " Output: $OUTDIR"
echo "=========================================="

# ============================================================
# Helper functions
# ============================================================
cleanup_dest() {
    ssh -o StrictHostKeyChecking=no ubuntu@$DEST \
        "sudo pkill -9 memcached; sudo pkill -9 java; sudo pkill -9 criu; sudo pkill -9 python; sudo rm -rf /tmp/criu_checkpoint" 2>/dev/null || true
    sleep 2
}

cleanup_all() {
    cleanup_dest
    ssh -o StrictHostKeyChecking=no ubuntu@$SOURCE \
        "sudo pkill -9 memcached; sudo pkill -9 java; sudo pkill -9 criu; sudo pkill -9 python; sudo rm -rf /tmp/criu_checkpoint" 2>/dev/null || true
    sleep 2
}

run_restore() {
    local mode=$1
    local run_num=$2
    local extra_args=$3
    local suffix=""
    [ "$REPEAT" -gt 1 ] && suffix="_run${run_num}"
    local outfile="$OUTDIR/${mode}${suffix}.json"
    local logfile="$OUTDIR/${mode}${suffix}.log"

    echo ""
    echo "================================================================"
    echo "  RESTORE: $mode (run $run_num/$REPEAT) — $(date +%H:%M:%S)"
    echo "================================================================"

    cleanup_dest

    python3 experiments/baseline_experiment.py \
        $COMMON \
        --restore-only \
        --name "${mode}${suffix}" \
        $extra_args \
        --no-cleanup \
        -o "$outfile" 2>&1 | tee "$logfile"

    # Save CRIU logs
    ssh -o StrictHostKeyChecking=no ubuntu@$DEST \
        "cp /tmp/criu_checkpoint/1/criu-lazy-pages.log /tmp/criu_lazy_${mode}${suffix}.log 2>/dev/null; \
         cp /tmp/criu_checkpoint/1/criu-restore.log /tmp/criu_restore_${mode}${suffix}.log 2>/dev/null" || true
    scp -o StrictHostKeyChecking=no "ubuntu@$DEST:/tmp/criu_lazy_${mode}${suffix}.log" "$OUTDIR/" 2>/dev/null || true
    scp -o StrictHostKeyChecking=no "ubuntu@$DEST:/tmp/criu_restore_${mode}${suffix}.log" "$OUTDIR/" 2>/dev/null || true

    # Quick health check
    ssh ubuntu@$DEST "sudo dmesg | grep segfault | tail -2" 2>/dev/null || true

    echo "[$mode run $run_num] Done."
}

# ============================================================
# Mode definitions
# ============================================================
declare -A MODE_ARGS
MODE_ARGS[1_baseline]="--lazy-mode none --transfer-method rsync $S3_COMMON"
MODE_ARGS[2_s3_lazy_only]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-semi-sync-iov --no-async-prefetch --no-hot-vma-seed"
MODE_ARGS[3_semi_sync]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-async-prefetch --no-hot-vma-seed"
MODE_ARGS[4_async]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-hot-vma-seed"
MODE_ARGS[5_full]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON"
MODE_ARGS[6_async_nosemi]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-semi-sync-iov --no-hot-vma-seed"
MODE_ARGS[7_async_nosemi_hot]="--lazy-mode lazy-prefetch --s3-direct-upload $S3_COMMON --no-semi-sync-iov"

MODE_ORDER=(1_baseline 2_s3_lazy_only 3_semi_sync 4_async 5_full 6_async_nosemi 7_async_nosemi_hot)

# ============================================================
# Phase 1: Dump
# ============================================================
if [ "$RESTORE_ONLY" -eq 0 ]; then
    echo ""
    echo "================================================================"
    echo "  PHASE 1: DUMP (S3 direct upload + dirty tracker)"
    echo "================================================================"

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
    echo "[DUMP] Complete."

    # Verify S3 upload
    echo "[verify] Checking S3 upload..."
    S3_COUNT=$(AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
        aws --endpoint-url $MINIO s3 ls "s3://test-bucket/$S3_PREFIX/" --recursive 2>/dev/null | wc -l)
    echo "[verify] S3 objects: $S3_COUNT"

    if [ "$DUMP_ONLY" -eq 1 ]; then
        echo ""
        echo "=========================================="
        echo " DUMP ONLY — done. S3 prefix: $S3_PREFIX"
        echo "=========================================="
        exit 0
    fi
fi

# ============================================================
# Phase 2: Restore
# ============================================================
echo ""
echo "================================================================"
echo "  PHASE 2: RESTORE (${REPEAT}x each mode)"
echo "================================================================"

# Determine which modes to run
if [ -n "$SINGLE_MODE" ]; then
    MODES_TO_RUN=("$SINGLE_MODE")
else
    MODES_TO_RUN=("${MODE_ORDER[@]}")
fi

for mode in "${MODES_TO_RUN[@]}"; do
    args="${MODE_ARGS[$mode]}"
    if [ -z "$args" ]; then
        echo "ERROR: Unknown mode '$mode'"
        echo "Available modes: ${MODE_ORDER[*]}"
        exit 1
    fi
    for run in $(seq 1 $REPEAT); do
        run_restore "$mode" "$run" "$args"
    done
done

# ============================================================
# Summary
# ============================================================
echo ""
echo "=========================================="
echo " Results: $OUTDIR"
echo "=========================================="
echo ""

DUMP_DUR=$(python3 -c "
import json, os
p = '$OUTDIR/dump_phase.json'
if os.path.exists(p):
    d = json.load(open(p))
    print(f\"{d.get('final_dump',{}).get('duration',0):.2f}\")
else:
    print('N/A')
" 2>/dev/null || echo "N/A")

echo "  Dump duration: ${DUMP_DUR}s"
echo ""

printf "  %-25s %9s %9s %7s %6s %6s %7s %7s %7s\n" \
    "Mode" "Transfer" "Restore" "Faults" "S3" "Cache" "HitRate" "Stall" "Daemon"
echo "  $(printf '%0.s-' {1..90})"

for f in "$OUTDIR"/*.json; do
    [ -f "$f" ] || continue
    fname=$(basename "$f" .json)
    [[ "$fname" == "dump_phase" ]] && continue
    python3 -c "
import json
d = json.load(open('$f'))
xfer = d.get('transfer',{}).get('duration', 0)
rest = d.get('restore',{}).get('duration', 0)
cm = d.get('criu_metrics',{}).get('lazy_pages',{})
faults = cm.get('uffd_faults', '-')
fs = cm.get('fault_stats',{})
s3_f = fs.get('s3_served','-') if isinstance(fs, dict) else '-'
cache_f = fs.get('cache_served','-') if isinstance(fs, dict) else '-'
cache_d = cm.get('cache',{})
hr = cache_d.get('hit_rate','-') if isinstance(cache_d, dict) else '-'
stall = fs.get('stall_ms_avg','-') if isinstance(fs, dict) else '-'
stall_s = f'{stall:.1f}ms' if isinstance(stall, (int,float)) else '-'
daemon = cm.get('daemon_duration_s', '-')
daemon_s = f'{daemon:.1f}s' if isinstance(daemon, (int,float)) else '-'
hr_s = f'{hr}%' if hr != '-' else '-'
print(f'  {\"$fname\":<25s} {xfer:>8.1f}s {rest:>8.2f}s {str(faults):>6s} {str(s3_f):>5s} {str(cache_f):>5s} {hr_s:>6s} {stall_s:>6s} {daemon_s:>6s}')
" 2>/dev/null || printf "  %-25s %10s\n" "$fname" "PARSE_ERR"
done
echo "  $(printf '%0.s-' {1..90})"
echo ""
echo "Done. $(ls "$OUTDIR"/*.json 2>/dev/null | wc -l) result files in $OUTDIR"
