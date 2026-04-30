#!/bin/bash
#
# Restore-fault paired-data experiment.
#
# Per workload:
#   1. start the standalone workload (writes its PID to checkpoint_ready)
#   2. 60-min warm-up
#   3. capture /proc/<pid,children>/maps + smaps
#   4. attach the C uffd-wp tracker for 60 min × 5 s = 720 samples →
#      dirty_output.json
#   5. CRIU dump (--track-mem -v4)
#   6. CRIU lazy-pages daemon -v4 + criu restore --lazy-pages
#   7. wait for the restore tree to settle, capture /proc/<pid>/maps for
#      VA-preservation check
#   8. parse uffd_copy lines from the lazy log → faults.csv
#   9. tar everything; optionally upload to S3
#
# Designed for the criu-workload-v6 AMI with the experiment code rsynced in.

set -euo pipefail

WORKLOAD=""
WARMUP_MIN=60
PROFILE_MIN=60
RESTORE_GRACE_MIN=5
OUTPUT_BASE="/home/ubuntu/restore_fault_runs"
S3_PREFIX=""
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload)        WORKLOAD="$2";        shift 2;;
    --warmup-min)      WARMUP_MIN="$2";      shift 2;;
    --profile-min)     PROFILE_MIN="$2";     shift 2;;
    --restore-grace-min) RESTORE_GRACE_MIN="$2"; shift 2;;
    --output-base)     OUTPUT_BASE="$2";     shift 2;;
    --s3-prefix)       S3_PREFIX="$2";       shift 2;;
    --)                shift; EXTRA_ARGS="$*"; break;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

[[ -z "$WORKLOAD" ]] && { echo "--workload required" >&2; exit 1; }
[[ $EUID -ne 0 ]]   && { echo "must run as root (CRIU dump/restore)" >&2; exit 1; }

WORKLOAD_DIR="${WORKLOAD_DIR:-/opt/criu_workload}"
CRIU_BIN=/usr/local/sbin/criu
TRACKER_BIN="$WORKLOAD_DIR/tools/dirty_tracker_c/dirty_tracker"

declare -A STANDALONE
STANDALONE[matmul]="$WORKLOAD_DIR/workloads/matmul_standalone.py"
STANDALONE[redis]="$WORKLOAD_DIR/workloads/redis_standalone.py"
STANDALONE[memcached]="$WORKLOAD_DIR/workloads/memcached_standalone.py"
STANDALONE[ml_training]="$WORKLOAD_DIR/workloads/ml_training_standalone.py"
STANDALONE[xgboost]="$WORKLOAD_DIR/workloads/xgboost_standalone.py"
STANDALONE[dataproc]="$WORKLOAD_DIR/workloads/dataproc_standalone.py"
STANDALONE_SCRIPT="${STANDALONE[$WORKLOAD]:-}"
[[ -z "$STANDALONE_SCRIPT" || ! -f "$STANDALONE_SCRIPT" ]] && \
  { echo "no standalone script for $WORKLOAD ($STANDALONE_SCRIPT)" >&2; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
RUN_DIR="$OUTPUT_BASE/${WORKLOAD}_${TS}"
WORK_DIR="$RUN_DIR/work"
DUMP_DIR="$RUN_DIR/dump"
LOG_DIR="$RUN_DIR/logs"
mkdir -p "$WORK_DIR" "$DUMP_DIR" "$LOG_DIR"
# Standalone workload runs as `ubuntu`; let it write checkpoint_ready,
# checkpoint_flag, dirty_output.json into the run dir.
chown -R ubuntu:ubuntu "$RUN_DIR" 2>/dev/null || true

log() { echo "[$(date +'%H:%M:%S')] $*" | tee -a "$LOG_DIR/orchestrator.log"; }

cleanup() {
  log "cleanup"
  [[ -n "${TRACKER_PID:-}"  ]] && kill -TERM "$TRACKER_PID"  2>/dev/null || true
  [[ -n "${LAZY_PID:-}"     ]] && kill -TERM "$LAZY_PID"     2>/dev/null || true
  [[ -n "${WORKLOAD_PID:-}" ]] && kill -TERM "$WORKLOAD_PID" 2>/dev/null || true
  rm -f "$WORK_DIR/checkpoint_flag" 2>/dev/null || true
}
trap cleanup EXIT

log "WORKLOAD=$WORKLOAD WARMUP=${WARMUP_MIN}min PROFILE=${PROFILE_MIN}min RUN_DIR=$RUN_DIR"

# 1. Start workload (standalone.py blocks on $WORK_DIR/checkpoint_flag)
touch "$WORK_DIR/checkpoint_flag"
log "starting $WORKLOAD ..."
sudo -u ubuntu setsid python3 "$STANDALONE_SCRIPT" \
    --working_dir "$WORK_DIR" $EXTRA_ARGS \
    > "$LOG_DIR/workload.log" 2>&1 &
disown $!
log "waiting for checkpoint_ready ..."
for _ in $(seq 1 600); do
  [[ -f "$WORK_DIR/checkpoint_ready" ]] && break
  sleep 1
done
[[ -f "$WORK_DIR/checkpoint_ready" ]] || { log "checkpoint_ready timeout"; exit 2; }
WORKLOAD_PID=$(cat "$WORK_DIR/checkpoint_ready")
log "workload PID=$WORKLOAD_PID"

# 2. Warm-up
log "warm-up ${WARMUP_MIN} min ..."
sleep $((WARMUP_MIN * 60))

# 3. Capture maps + smaps (root + children)
capture_maps() {
  local tag="$1" pid="$2"
  log "capture maps tag=$tag pid=$pid"
  cp "/proc/$pid/maps"  "$RUN_DIR/maps_${tag}.txt"  2>/dev/null || true
  cp "/proc/$pid/smaps" "$RUN_DIR/smaps_${tag}.txt" 2>/dev/null || true
  for kid in $(pgrep -P "$pid" 2>/dev/null || true); do
    cp "/proc/$kid/maps"  "$RUN_DIR/maps_${tag}_child${kid}.txt"  2>/dev/null || true
    cp "/proc/$kid/smaps" "$RUN_DIR/smaps_${tag}_child${kid}.txt" 2>/dev/null || true
  done
}
capture_maps before_dump "$WORKLOAD_PID"

# 4. Attach C uffd-wp tracker for PROFILE_MIN
log "attaching C tracker for ${PROFILE_MIN} min ..."
"$TRACKER_BIN" -p "$WORKLOAD_PID" -i 5000 -d $((PROFILE_MIN * 60)) \
    -o "$RUN_DIR/dirty_output.json" \
    > "$LOG_DIR/tracker.log" 2>&1 &
TRACKER_PID=$!
wait "$TRACKER_PID" || true
log "tracker done."

# 5. CRIU dump
log "criu dump ..."
DUMP_FLAGS=("--shell-job" "--ext-unix-sk" "--tcp-established" "--file-locks"
            "--track-mem" "-v4" "-o" "$LOG_DIR/criu_dump.log")
$CRIU_BIN dump --tree "$WORKLOAD_PID" --images-dir "$DUMP_DIR" "${DUMP_FLAGS[@]}"
log "dump done."

# 6. lazy-pages daemon + restore
log "lazy-pages daemon -v4 ..."
$CRIU_BIN lazy-pages \
    --images-dir "$DUMP_DIR" \
    --address "$WORK_DIR/lazy.sock" \
    -v4 -o "$LOG_DIR/criu_lazy.log" &
LAZY_PID=$!
sleep 1

log "criu restore --lazy-pages ..."
$CRIU_BIN restore \
    --images-dir "$DUMP_DIR" \
    --lazy-pages --address "$WORK_DIR/lazy.sock" \
    --shell-job --tcp-established --ext-unix-sk --file-locks \
    -v4 -o "$LOG_DIR/criu_restore.log" &
RESTORE_PID=$!

log "waiting for lazy-pages daemon to drain ..."
wait "$LAZY_PID" 2>/dev/null || true

log "restore-grace ${RESTORE_GRACE_MIN} min ..."
sleep $((RESTORE_GRACE_MIN * 60))

# 7. After-restore maps capture
RESTORED_PID=$(pgrep -P "$RESTORE_PID" 2>/dev/null | head -1 || true)
[[ -n "$RESTORED_PID" ]] && capture_maps after_restore "$RESTORED_PID"

# 8. Parse fault log
log "parsing uffd_copy log → faults.csv"
python3 - "$LOG_DIR/criu_lazy.log" "$RUN_DIR/faults.csv" <<'PY'
import re, sys
LINE_RE = re.compile(
    r'^\(\s*(\d+\.\d+)\)\s+uffd:\s+\d+-\d+:\s+uffd_copy:\s+0x([0-9a-fA-F]+)/(\d+)')
src, dst = sys.argv[1], sys.argv[2]
n = 0
with open(src) as fin, open(dst, 'w') as fout:
    fout.write('timestamp_s,address,size_bytes,n_pages\n')
    for line in fin:
        m = LINE_RE.match(line)
        if not m:
            continue
        ts = m.group(1); addr = '0x' + m.group(2); sz = int(m.group(3))
        fout.write(f'{ts},{addr},{sz},{sz // 4096}\n'); n += 1
print(f'wrote {n} faults to {dst}')
PY

# 9. Tar + (optionally) upload to S3
TAR_PATH="$OUTPUT_BASE/${WORKLOAD}_${TS}.tar.gz"
log "creating archive $TAR_PATH"
tar -czf "$TAR_PATH" -C "$OUTPUT_BASE" "${WORKLOAD}_${TS}"

if [[ -n "$S3_PREFIX" ]]; then
  log "uploading to ${S3_PREFIX}${WORKLOAD}_${TS}.tar.gz"
  aws s3 cp "$TAR_PATH" "${S3_PREFIX}${WORKLOAD}_${TS}.tar.gz"
fi

log "done."
