#!/bin/bash
# Debug: ML Training (PyTorch) uffd-wp dirty page miss
#
# PyTorch ML Training shows WP:SD ratio=0.069 (93% miss).
# This script diagnoses why uffd-wp misses pages that soft-dirty catches.
#
# Usage: sudo bash tools/debug_ml_wp_miss.sh [work_dir]
#
# Steps:
#   0. Launch ML Training workload
#   1. Dual-channel delta comparison (-D -S) — confirm the miss
#   2. Per-VMA WP vs SD comparison — locate which pages are missed
#   3. VMA dynamics — check for munmap/mmap churn
#   4. Thread info
#   5. strace syscall summary (madvise, mprotect, mremap, munmap)
#   6. madvise detail (MADV_DONTNEED / MADV_FREE)

set -e

WORK_DIR="${1:-/tmp/debug_ml_wp}"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TRACKER="$SCRIPT_DIR/tools/dirty_tracker_c/dirty_tracker"
DURATION=120  # workload duration

echo "======================================"
echo "ML Training uffd-wp miss debugger"
echo "======================================"
echo "Work dir: $WORK_DIR"
echo "Script dir: $SCRIPT_DIR"
echo ""

# Check prerequisites
if [ ! -x "$TRACKER" ]; then
    echo "ERROR: dirty_tracker not found at $TRACKER"
    echo "Run: cd $SCRIPT_DIR/tools/dirty_tracker_c && make"
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "ERROR: Must run as root (sudo)"
    exit 1
fi

# ============================================================
# Step 0: Launch ML Training workload
# ============================================================
echo "=== Step 0: Launching ML Training workload ==="
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
echo "flag" > "$WORK_DIR/checkpoint_flag"

cd "$SCRIPT_DIR"
python3 workloads/ml_training_standalone.py \
    --model-size small --batch-size 32 --duration $DURATION \
    --working_dir "$WORK_DIR" > "$WORK_DIR/workload_stdout.txt" 2>&1 &
WL_PID=$!

echo "Workload launched (shell PID=$WL_PID), waiting for checkpoint_ready..."
for i in $(seq 1 30); do
    if [ -f "$WORK_DIR/checkpoint_ready" ]; then
        break
    fi
    sleep 1
done

if [ ! -f "$WORK_DIR/checkpoint_ready" ]; then
    echo "ERROR: checkpoint_ready not found after 30s"
    kill $WL_PID 2>/dev/null
    exit 1
fi

PID=$(awk -F: '/ready/{print $2}' "$WORK_DIR/checkpoint_ready" | tr -d ' ')
echo "Target PID: $PID"

# Verify process is running
if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: Process $PID not running"
    exit 1
fi

# ============================================================
# Step 1: Dual-channel delta comparison (-D -S)
# ============================================================
echo ""
echo "=== Step 1: Dual-channel delta comparison (10s, 1s interval) ==="
"$TRACKER" \
    -p "$PID" -i 1000 -d 10 -D -S \
    -o "$WORK_DIR/dual_delta.json" -w ml_training 2>"$WORK_DIR/tracker_stderr.txt"

echo "Tracker stderr:"
cat "$WORK_DIR/tracker_stderr.txt"
echo ""

python3 -c "
import json, sys

d = json.load(open('$WORK_DIR/dual_delta.json'))
samples = d.get('samples', [])
if not samples:
    print('No samples found!')
    sys.exit(1)

print(f'Total samples: {len(samples)}')
print(f'{\"Sample\":>8} {\"WP\":>8} {\"SD\":>8} {\"Ratio\":>8}')
print('-' * 36)

wp_total, sd_total = 0, 0
for i, s in enumerate(samples):
    wp = s.get('wp_channel', {}).get('dirty_count', s.get('delta_dirty_count', 0))
    sd = s.get('sd_channel', {}).get('dirty_count', 0)
    ratio = wp / sd if sd > 0 else 0
    wp_total += wp
    sd_total += sd
    print(f'{i:>8} {wp:>8} {sd:>8} {ratio:>8.3f}')

overall = wp_total / sd_total if sd_total > 0 else 0
print('-' * 36)
print(f'{\"TOTAL\":>8} {wp_total:>8} {sd_total:>8} {overall:>8.3f}')

if overall < 0.5:
    print(f'\n*** WP misses {(1-overall)*100:.1f}% of dirty pages! ***')
elif overall > 0.9:
    print(f'\n*** WP/SD ratio looks normal ({overall:.3f}) ***')
    print('If normal, the original issue may have been SD cumulative vs WP delta comparison.')
"
echo ""

# ============================================================
# Step 2: Per-VMA WP vs SD comparison
# ============================================================
echo "=== Step 2: Per-VMA WP vs SD analysis ==="
python3 -c "
import json
from collections import Counter

d = json.load(open('$WORK_DIR/dual_delta.json'))
samples = d.get('samples', [])
if len(samples) < 3:
    print('Not enough samples for analysis')
    exit()

# Use 3rd sample (after warmup)
s = samples[2]
wp_pages = s.get('wp_channel', {}).get('dirty_pages', [])
sd_pages = s.get('sd_channel', {}).get('dirty_pages', [])

wp_addrs = set(p['addr'] for p in wp_pages)
sd_addrs = set(p['addr'] for p in sd_pages)

only_wp = wp_addrs - sd_addrs
only_sd = sd_addrs - wp_addrs
both = wp_addrs & sd_addrs

print(f'Sample 2 analysis:')
print(f'  WP pages: {len(wp_addrs)}')
print(f'  SD pages: {len(sd_addrs)}')
print(f'  Both: {len(both)}')
print(f'  WP-only: {len(only_wp)}')
print(f'  SD-only (WP miss): {len(only_sd)}')
print()

if only_sd:
    sd_only_list = [p for p in sd_pages if p['addr'] in only_sd]
    types = Counter(p.get('vma_type', 'unknown') for p in sd_only_list)
    print('SD-only page VMA types:')
    for t, cnt in types.most_common():
        print(f'  {t}: {cnt}')

    pathnames = Counter(p.get('pathname', '') for p in sd_only_list)
    print()
    print('SD-only page pathnames (top 10):')
    for path, cnt in pathnames.most_common(10):
        print(f'  {path or \"[anon]\"}: {cnt}')
"
echo ""

# ============================================================
# Step 3: VMA dynamics
# ============================================================
echo "=== Step 3: VMA dynamics (15s) ==="
echo "Tracking VMA count changes..."
PREV_COUNT=0
for i in $(seq 1 15); do
    COUNT=$(wc -l < "/proc/$PID/maps" 2>/dev/null || echo 0)
    if [ "$PREV_COUNT" -ne 0 ]; then
        DELTA=$((COUNT - PREV_COUNT))
        echo "  t=${i}s: $COUNT VMAs (delta=$DELTA)"
    else
        echo "  t=${i}s: $COUNT VMAs"
    fi
    PREV_COUNT=$COUNT
    sleep 1
done
echo ""

# ============================================================
# Step 4: Thread info
# ============================================================
echo "=== Step 4: Thread info ==="
THREADS=$(ls "/proc/$PID/task/" 2>/dev/null | wc -l)
echo "Kernel threads: $THREADS"
grep Threads "/proc/$PID/status" 2>/dev/null || true
python3 -c "import torch; print(f'PyTorch threads: {torch.get_num_threads()}')" 2>/dev/null || true
echo ""

# ============================================================
# Step 5: Syscall frequency summary
# ============================================================
echo "=== Step 5: Syscall frequency (15s strace) ==="
strace -e trace=madvise,mprotect,mremap,munmap,mmap \
    -p "$PID" -f -c -S calls 2>"$WORK_DIR/strace_summary.txt" &
STRACE_PID=$!
sleep 15
kill $STRACE_PID 2>/dev/null; wait $STRACE_PID 2>/dev/null || true
echo ""
cat "$WORK_DIR/strace_summary.txt"
echo ""

# ============================================================
# Step 6: madvise detail
# ============================================================
echo "=== Step 6: madvise detail (5s) ==="
strace -e trace=madvise -p "$PID" -f \
    2>"$WORK_DIR/strace_madvise.txt" &
STRACE_PID=$!
sleep 5
kill $STRACE_PID 2>/dev/null; wait $STRACE_PID 2>/dev/null || true

DONTNEED=$(grep -c "MADV_DONTNEED" "$WORK_DIR/strace_madvise.txt" 2>/dev/null || echo 0)
MADVFREE=$(grep -c "MADV_FREE" "$WORK_DIR/strace_madvise.txt" 2>/dev/null || echo 0)
echo "MADV_DONTNEED calls: $DONTNEED"
echo "MADV_FREE calls: $MADVFREE"
echo "First 20 madvise calls:"
head -20 "$WORK_DIR/strace_madvise.txt"
echo ""

# ============================================================
# Cleanup
# ============================================================
echo "=== Cleanup ==="
rm -f "$WORK_DIR/checkpoint_flag"
echo "Removed checkpoint_flag, waiting for workload to exit..."
sleep 3
kill "$WL_PID" 2>/dev/null || true
wait "$WL_PID" 2>/dev/null || true
echo ""

# ============================================================
# Summary
# ============================================================
echo "======================================"
echo "Results saved to: $WORK_DIR/"
echo "======================================"
echo "  strace_summary.txt   - syscall frequency"
echo "  strace_madvise.txt   - madvise detail"
echo "  dual_delta.json      - WP vs SD per-sample"
echo "  tracker_stderr.txt   - tracker output"
echo "  workload_stdout.txt  - workload output"
echo ""
echo "Key finding:"
if [ "$DONTNEED" -gt 10 ]; then
    echo "  *** MADV_DONTNEED detected ($DONTNEED calls in 5s) ***"
    echo "  This likely causes WP bit loss on freed pages."
elif [ "$MADVFREE" -gt 10 ]; then
    echo "  *** MADV_FREE detected ($MADVFREE calls in 5s) ***"
    echo "  This may cause WP bit loss on lazily freed pages."
else
    echo "  madvise is not the primary cause."
    echo "  Check VMA dynamics and dual_delta.json for further clues."
fi
