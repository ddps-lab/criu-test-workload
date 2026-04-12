#!/bin/bash
# Restore Experiment v3: v2 + lazy-pages race condition fix + restore-side metadata capture
# Changes from v2:
#   1. Wait for ALL PIDs to finish lazy restore (not just the first)
#   2. Capture restore-side /proc/PID/maps after restore
#   3. Parse faults.csv AFTER lazy-pages process fully exits
#   4. Record per-PID transfer stats from lazy_pages.log
#
# Usage: ./restore_experiment_final_v3.sh <workload> [extra_args]
# All 6 workloads: ./restore_experiment_final_v3.sh ALL

set -e

C_TRACKER="/spot_kubernetes/criu_workload/tools/dirty_tracker_c/dirty_tracker"
WORKLOADS_DIR="/spot_kubernetes/criu_workload/workloads"
RESULTS_BASE="/spot_kubernetes/criu_workload/results/restore_faults_v6"

WARMUP_SEC=600    # 10분 warm-up
PROFILE_SEC=300   # 5분 dirty profiling
PROFILE_INTERVAL=5000  # 5초 interval

run_one() {
    local WL=$1
    shift
    local EXTRA_ARGS="$@"

    local CKPT_DIR="/tmp/ckpt_${WL}"
    local RESULTS_DIR="${RESULTS_BASE}/${WL}"

    # Table 3 defaults
    if [ -z "$EXTRA_ARGS" ]; then
        case $WL in
            matmul)       EXTRA_ARGS="--matrix-size 2048" ;;
            ml_training)  EXTRA_ARGS="--model-size large --dataset-size 50000 --epochs 0" ;;
            redis)        EXTRA_ARGS="--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4" ;;
            memcached)    EXTRA_ARGS="--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4" ;;
            xgboost)      EXTRA_ARGS="--dataset synthetic --num-samples 7000000 --num-features 100 --num-rounds 0 --num-threads 3" ;;
            dataproc)     EXTRA_ARGS="--num-rows 1500000 --num-cols 60 --batch-size 1000" ;;
            *) echo "Unknown: $WL"; return 1 ;;
        esac
    fi

    # CRIU flags
    local CRIU_EXTRA=""
    if [ "$WL" = "redis" ] || [ "$WL" = "memcached" ]; then
        CRIU_EXTRA="--tcp-established"
    fi

    sudo rm -rf "${RESULTS_DIR}" "${CKPT_DIR}"
    mkdir -p "${RESULTS_DIR}" "${CKPT_DIR}"

    echo ""
    echo "========================================="
    echo " ${WL} | Config: ${EXTRA_ARGS}"
    echo "========================================="

    # ── 1. Start workload ──
    echo "[1] Starting workload..."
    local SCRIPT="${WORKLOADS_DIR}/${WL}_standalone.py"
    cd "${CKPT_DIR}"
    CRIU_NO_SETSID=1 setsid python3 "${SCRIPT}" --working_dir "${CKPT_DIR}" --duration 1800 --keep-running ${EXTRA_ARGS} \
        > "${RESULTS_DIR}/workload.log" 2>&1 &

    # ── 2. Wait for ready ──
    echo "[2] Waiting for ready..."
    local ELAPSED=0
    while [ ! -f "${CKPT_DIR}/checkpoint_ready" ]; do
        sleep 1
        ELAPSED=$((ELAPSED + 1))
        if [ $ELAPSED -ge 600 ]; then
            echo "ERROR: Timeout (600s)"
            tail -10 "${RESULTS_DIR}/workload.log"
            sudo kill -9 ${WL_PID} 2>/dev/null || true
            return 1
        fi
    done

    local WL_PID=$(cat "${CKPT_DIR}/checkpoint_ready" 2>/dev/null | grep -oP '\d+' | head -1)
    [ -z "$WL_PID" ] && WL_PID=$(pgrep -f "${WL}_standalone" | head -1)
    echo "    Ready after ${ELAPSED}s, PID=${WL_PID}"

    # ── 3. Warm-up ──
    echo "[3] Warming up ${WARMUP_SEC}s..."
    sleep ${WARMUP_SEC}

    local RSS_KB=$(grep VmRSS /proc/${WL_PID}/status 2>/dev/null | awk '{print $2}')
    RSS_KB=${RSS_KB:-0}
    echo "    VmRSS: ${RSS_KB} KB ($((RSS_KB / 1024)) MB)"

    # ── 4. Dirty profiling (C tracker) ──
    echo "[4] Dirty profiling (C tracker, ${PROFILE_SEC}s, ${PROFILE_INTERVAL}ms)..."

    # Capture pre-dump maps for all tracked PIDs
    cat /proc/${WL_PID}/maps > "${RESULTS_DIR}/maps_before_dump.txt" 2>/dev/null || true
    cat /proc/${WL_PID}/smaps > "${RESULTS_DIR}/smaps_before_dump.txt" 2>/dev/null || true
    # Also capture child process maps
    local CHILD_IDX=0
    for CHILD_PID in $(pgrep -P ${WL_PID} 2>/dev/null); do
        cat /proc/${CHILD_PID}/maps > "${RESULTS_DIR}/maps_before_dump_child${CHILD_IDX}_pid${CHILD_PID}.txt" 2>/dev/null || true
        CHILD_IDX=$((CHILD_IDX + 1))
    done
    echo "    Captured maps for root + ${CHILD_IDX} children"

    sudo "${C_TRACKER}" -p ${WL_PID} -d ${PROFILE_SEC} -i ${PROFILE_INTERVAL} \
        -w ${WL} -o "${RESULTS_DIR}/dirty_profile.json" \
        > "${RESULTS_DIR}/dirty_tracker.log" 2>&1
    local TRACK_EXIT=$?

    if [ $TRACK_EXIT -ne 0 ] || [ ! -f "${RESULTS_DIR}/dirty_profile.json" ]; then
        echo "    WARNING: C tracker failed (exit=$TRACK_EXIT)"
        tail -5 "${RESULTS_DIR}/dirty_tracker.log"
    else
        local SAMPLES=$(python3 -c "import json; d=json.load(open('${RESULTS_DIR}/dirty_profile.json')); print(len(d.get('samples',[])))" 2>/dev/null)
        echo "    Done: ${SAMPLES} samples"
    fi

    # ── 5. CRIU dump ──
    echo "[5] CRIU dump..."
    local DUMP_START=$(date +%s%N)
    sudo /usr/local/sbin/criu dump -t ${WL_PID} -D "${CKPT_DIR}" --shell-job ${CRIU_EXTRA} -v1 \
        > "${RESULTS_DIR}/dump.log" 2>&1
    local DUMP_END=$(date +%s%N)
    local DUMP_MS=$(( (DUMP_END - DUMP_START) / 1000000 ))

    local CKPT_SIZE=$(du -sb "${CKPT_DIR}/pages-"*.img 2>/dev/null | awk '{s+=$1}END{print s+0}')
    echo "    Dump: ${DUMP_MS}ms, Checkpoint: $((CKPT_SIZE / 1024 / 1024)) MB"

    # Save CRIU image metadata (VMA layout from checkpoint)
    echo "[5b] Extracting CRIU image metadata..."
    if command -v crit &>/dev/null; then
        for MM_IMG in "${CKPT_DIR}"/mm-*.img; do
            [ -f "$MM_IMG" ] || continue
            local BASENAME=$(basename "$MM_IMG" .img)
            crit decode -i "$MM_IMG" -o "${RESULTS_DIR}/${BASENAME}.json" 2>/dev/null || true
        done
        for PAGEMAP_IMG in "${CKPT_DIR}"/pagemap-*.img; do
            [ -f "$PAGEMAP_IMG" ] || continue
            local BASENAME=$(basename "$PAGEMAP_IMG" .img)
            crit decode -i "$PAGEMAP_IMG" -o "${RESULTS_DIR}/${BASENAME}.json" 2>/dev/null || true
        done
        echo "    CRIU metadata extracted"
    else
        echo "    WARNING: crit not found, skipping CRIU metadata extraction"
    fi

    # ── 6. Lazy restore ──
    echo "[6] Lazy restore..."
    sudo /usr/local/sbin/criu lazy-pages -D "${CKPT_DIR}" -v4 \
        > "${RESULTS_DIR}/lazy_pages.log" 2>&1 &
    local LP_PID=$!
    sleep 1

    local RESTORE_START=$(date +%s%N)
    sudo /usr/local/sbin/criu restore -D "${CKPT_DIR}" --shell-job --lazy-pages ${CRIU_EXTRA} -v1 \
        > "${RESULTS_DIR}/restore.log" 2>&1 &
    local RESTORE_PID=$!

    # ── 6b. Wait for lazy-pages to FULLY complete (all PIDs) ──
    # Count expected "UFFD transferred pages" messages = number of tracked PIDs
    # Wait for lazy-pages process to exit naturally (all pages transferred)
    echo "    Waiting for lazy-pages to complete (all PIDs)..."
    local WAIT=0
    while kill -0 $LP_PID 2>/dev/null; do
        sleep 1
        WAIT=$((WAIT + 1))
        if [ $WAIT -ge 300 ]; then
            echo "    Timeout (300s), lazy-pages still running"
            # Count how many PIDs completed so far
            local COMPLETED=$(grep -c "UFFD transferred pages" "${RESULTS_DIR}/lazy_pages.log" 2>/dev/null || echo 0)
            echo "    PIDs completed so far: ${COMPLETED}"
            sudo kill $LP_PID 2>/dev/null
            break
        fi

        # Progress reporting every 10s
        if [ $((WAIT % 10)) -eq 0 ]; then
            local COMPLETED=$(grep -c "UFFD transferred pages" "${RESULTS_DIR}/lazy_pages.log" 2>/dev/null || echo 0)
            echo "    ... ${WAIT}s elapsed, ${COMPLETED} PIDs completed"
        fi
    done
    local RESTORE_END=$(date +%s%N)
    local RESTORE_MS=$(( (RESTORE_END - RESTORE_START) / 1000000 ))

    # Wait a moment for log flush
    sleep 1

    # ── 6c. Capture restore-side metadata ──
    echo "[6c] Capturing restore-side metadata..."
    # Find restored process PID (CRIU restores with original PIDs, but they may differ)
    # Try the original PID first
    if [ -d "/proc/${WL_PID}" ]; then
        cat /proc/${WL_PID}/maps > "${RESULTS_DIR}/maps_after_restore.txt" 2>/dev/null || true
        cat /proc/${WL_PID}/smaps > "${RESULTS_DIR}/smaps_after_restore.txt" 2>/dev/null || true
        cat /proc/${WL_PID}/status > "${RESULTS_DIR}/status_after_restore.txt" 2>/dev/null || true
        echo "    Captured restore-side maps for PID ${WL_PID}"

        # Child processes
        CHILD_IDX=0
        for CHILD_PID in $(pgrep -P ${WL_PID} 2>/dev/null); do
            cat /proc/${CHILD_PID}/maps > "${RESULTS_DIR}/maps_after_restore_child${CHILD_IDX}_pid${CHILD_PID}.txt" 2>/dev/null || true
            CHILD_IDX=$((CHILD_IDX + 1))
        done
        echo "    Captured maps for ${CHILD_IDX} child processes"
    else
        echo "    WARNING: PID ${WL_PID} not found after restore"
    fi

    # ── 7. Parse results ──
    # CRITICAL: Parse AFTER lazy-pages has fully exited, so lazy_pages.log is complete
    echo "[7] Parsing (from complete lazy_pages.log)..."

    # Verify log completeness
    local N_TRANSFERRED=$(grep -c "UFFD transferred pages" "${RESULTS_DIR}/lazy_pages.log" 2>/dev/null || echo 0)
    echo "    UFFD transferred messages: ${N_TRANSFERRED}"
    grep "UFFD transferred pages" "${RESULTS_DIR}/lazy_pages.log" 2>/dev/null || true

    python3 -c "
import re, csv, json, numpy as np
from collections import defaultdict

# Parse fault log - AFTER lazy-pages fully completed
with open('${RESULTS_DIR}/lazy_pages.log') as f:
    lines = [l for l in f if 'uffd_copy' in l]

faults = []
with open('${RESULTS_DIR}/faults.csv', 'w', newline='') as out:
    w = csv.writer(out)
    w.writerow(['timestamp_s', 'address', 'size_bytes', 'n_pages'])
    for l in lines:
        m = re.search(r'\((\d+\.\d+)\).*uffd_copy: (0x[0-9a-f]+)/(\d+)', l)
        if m:
            ts, addr_s, sz = float(m.group(1)), m.group(2), int(m.group(3))
            w.writerow([f'{ts:.6f}', addr_s, sz, sz // 4096])
            faults.append((ts, int(addr_s, 16), sz // 4096))

if not faults:
    print('No faults!')
    exit(0)

ts = np.array([f[0] for f in faults])
ts_rel = ts - ts[0]
addrs = [f[1] for f in faults]
total_pages = sum(f[2] for f in faults)
diffs = [abs(addrs[i+1] - addrs[i]) for i in range(len(addrs)-1)]

print(f'Total uffd_copy entries: {len(faults)}')
print(f'Total pages: {total_pages} ({total_pages * 4096 / 1024 / 1024:.0f} MB)')

# Per-PID stats from log
pid_pattern = r'uffd: (\d+)-\d+: UFFD transferred pages: \((\d+)/(\d+)\)'
with open('${RESULTS_DIR}/lazy_pages.log') as f:
    pid_stats = {}
    for line in f:
        m = re.search(pid_pattern, line)
        if m:
            pid, transferred, total = m.group(1), int(m.group(2)), int(m.group(3))
            pid_stats[pid] = {'transferred': transferred, 'total': total}

# M9: match faults to hot VMAs from dirty profile
hot_vmas = []
try:
    with open('${RESULTS_DIR}/dirty_profile.json') as df:
        dirty = json.load(df)

    vma_dirty = defaultdict(set)
    vma_total_pages = {}
    for sample in dirty.get('samples', []):
        for dp in sample.get('dirty_pages', []):
            vs = int(dp.get('vma_start', '0x0'), 16)
            ve = int(dp.get('vma_end', '0x0'), 16)
            addr = int(dp['addr'], 16)
            if vs > 0 and ve > 0:
                vma_dirty[(vs, ve)].add(addr)
                vma_total_pages[(vs, ve)] = (ve - vs) // 4096

    THETA = 0.3
    for (vs, ve), dirty_addrs in vma_dirty.items():
        total_p = vma_total_pages.get((vs, ve), 1)
        if total_p > 0 and len(dirty_addrs) / total_p > THETA:
            hot_vmas.append((vs, ve))
except Exception as e:
    print(f'M9 skipped: {e}')

# Count faults in hot VMAs + recovery tracking
faults_in_hot = 0
hot_restored_pages = 0
hot_total_pages = sum(vma_total_pages.get(v, 0) for v in hot_vmas)

if hot_vmas:
    for _, addr, n_pg in faults:
        for vs, ve in hot_vmas:
            if vs <= addr < ve:
                faults_in_hot += 1
                hot_restored_pages += n_pg
                break

recovery_pct = round(min(hot_restored_pages / max(1, hot_total_pages) * 100, 100), 1)

result = {
    'workload': '${WL}',
    'rss_kb': ${RSS_KB}, 'dump_ms': ${DUMP_MS}, 'restore_ms': ${RESTORE_MS},
    'checkpoint_bytes': ${CKPT_SIZE},
    'total_faults': len(faults), 'total_pages': total_pages,
    'pages_per_fault': round(total_pages / max(1, len(faults)), 1),
    'time_span_ms': float(ts_rel[-1] * 1000),
    'temporal_cdf': {
        'p50_ms': float(np.percentile(ts_rel, 50) * 1000),
        'p90_ms': float(np.percentile(ts_rel, 90) * 1000),
        'p99_ms': float(np.percentile(ts_rel, 99) * 1000),
        'p100_ms': float(ts_rel[-1] * 1000),
    },
    'spatial_locality': {
        'within_4mb_pct': round(sum(1 for d in diffs if d <= 4*1024*1024) / max(1, len(diffs)) * 100, 1),
        'median_page_dist': int(np.median([d // 4096 for d in diffs])) if diffs else 0,
    },
    'm9_hot_vma': {
        'n_hot_vmas': len(hot_vmas),
        'n_total_vmas': len(vma_dirty),
        'faults_in_hot': faults_in_hot,
        'faults_in_hot_pct': round(faults_in_hot / max(1, len(faults)) * 100, 1),
        'hot_total_pages': hot_total_pages,
        'hot_restored_pages': hot_restored_pages,
        'recovery_at_100_pct': recovery_pct,
    },
    'per_pid_transfer': pid_stats,
    'n_pids_completed': len(pid_stats),
}
with open('${RESULTS_DIR}/summary.json', 'w') as f:
    json.dump(result, f, indent=2)
print(json.dumps(result, indent=2))
"

    # ── 8. Cleanup ──
    local WL_PGID=$(ps -o pgid= -p ${WL_PID} 2>/dev/null | tr -d " ")
    if [ -n "$WL_PGID" ] && [ "$WL_PGID" != "0" ]; then
        sudo kill -9 -${WL_PGID} 2>/dev/null || true
    else
        sudo kill -9 ${WL_PID} 2>/dev/null || true
    fi
    [ -n "$LP_PID" ] && sudo kill -9 $LP_PID 2>/dev/null || true
    sleep 2

    echo "[${WL}] DONE"
}

# ── Main ──
if [ "$1" = "ALL" ]; then
    echo "=== All 6 workloads (v3) ==="
    echo "=== $(date) ==="
    for WL in matmul dataproc redis ml_training memcached xgboost; do
        echo ""
        echo ">>>>>>>>> ${WL} starting at $(date) <<<<<<<<<<<"
        run_one ${WL}
        echo ">>>>>>>>> ${WL} finished at $(date) <<<<<<<<<<<"
    done
    echo ""
    echo "=== ALL DONE at $(date) ==="
else
    run_one "$@"
fi
