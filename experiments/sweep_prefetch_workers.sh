#!/bin/bash
# Phase 6 — Prefetch worker count sweep (Step 1 pre-patch validation).
#
# Drives baseline_experiment.py via run_restore_experiment.sh for a fixed
# workload across a list of --prefetch-workers values, leveraging the
# existing daemon-wait + criu_metrics collection pipeline so that every cell
# yields a JSON containing criu_metrics.lazy_pages.daemon_duration_s.
#
# This replaces the ad-hoc bash sweeps that were timing out / mis-killing
# the lazy-pages daemon. All experiments must live in this script (no inline
# bash on the EC2).
#
# Prereqs on the EC2 host:
#   - AMI v5+ with criu-s3 X-Cache patch
#   - /opt/criu_workload checked out (this script lives in experiments/)
#   - aws CLI v2 + IAM Role granting S3 GetObject
#   - Long-lived AWS keys exported as AWS_ACCESS_KEY_ID/SECRET (criu binary
#     does not understand x-amz-security-token)
#
# Usage:
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#       bash experiments/sweep_prefetch_workers.sh \
#           --workload memcached --s3-prefix memcached-16gb \
#           --workers "4 8 16 32" --reps 2 \
#           --extra-base "--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4"
set -uo pipefail

cd /opt/criu_workload

WORKLOAD=""
S3_PREFIX=""
WORKERS_LIST="4 8 16 32"
REPS=2
EXTRA_BASE=""
MODE="5_full"
SKIP_WARMUP=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)    WORKLOAD="$2"; shift 2 ;;
        --s3-prefix)   S3_PREFIX="$2"; shift 2 ;;
        --workers)     WORKERS_LIST="$2"; shift 2 ;;
        --reps)        REPS="$2"; shift 2 ;;
        --extra-base)  EXTRA_BASE="$2"; shift 2 ;;
        --mode)        MODE="$2"; shift 2 ;;
        --skip-warmup) SKIP_WARMUP=1; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[ -z "$WORKLOAD" ]  && { echo "ERROR: --workload required"; exit 1; }
[ -z "$S3_PREFIX" ] && { echo "ERROR: --s3-prefix required"; exit 1; }
: "${AWS_ACCESS_KEY_ID:?need AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?need AWS_SECRET_ACCESS_KEY}"
unset AWS_SESSION_TOKEN
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="/tmp/prefetch_sweep_${WORKLOAD}_${S3_PREFIX//\//_}_${TS}"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.csv"
META="$OUT/run_metadata.json"

cat > "$META" <<JSON
{
  "timestamp_utc": "$TS",
  "workload": "$WORKLOAD",
  "s3_prefix": "$S3_PREFIX",
  "workers_list": "$WORKERS_LIST",
  "reps": $REPS,
  "mode": "$MODE",
  "extra_base": "$EXTRA_BASE",
  "instance_type": "$(curl -sS -H "X-aws-ec2-metadata-token: $(curl -sS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 600')" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo unknown)"
}
JSON

echo "workers,rep,restore_s,daemon_s,total_s,uffd_faults,s3_served,cache_served,stall_ms_avg,s3_stall_ms_avg,cache_stall_ms_avg" > "$SUMMARY"

echo "[sweep] ts=$TS out=$OUT"
echo "[sweep] workload=$WORKLOAD prefix=$S3_PREFIX workers=($WORKERS_LIST) reps=$REPS"

# === S3 warmup: aws s3 cp the prefix three times so DNS/TCP/ENA caches
# === and any S3 backend object metadata are hot before the first criu
# === run. Single-pass warmup proved insufficient — first cell of a
# === sweep observed ~2x latency vs subsequent cells, biasing the
# === comparison.
if [ "$SKIP_WARMUP" -eq 0 ]; then
    for pass in 1 2 3; do
        echo "[sweep] === warmup pass $pass: aws s3 cp s3://$S3_PREFIX === "
        mkdir -p /tmp/_warmup
        rm -rf /tmp/_warmup/*
        time aws s3 cp "s3://mhsong-criu-checkpoints/$S3_PREFIX/" /tmp/_warmup/ \
            --recursive --region us-west-2 --quiet
        sync
        rm -rf /tmp/_warmup
    done
    echo "[sweep] warmup complete (3 passes)"
fi

extract_metrics() {
    local json="$1" w="$2" rep="$3"
    python3 - "$json" "$w" "$rep" "$SUMMARY" <<'PY'
import json, sys
path, w, rep, csv = sys.argv[1:]
try:
    d = json.load(open(path))
except Exception as e:
    print(f"PARSE_ERR: {e}"); sys.exit(0)
restore_s = d.get('restore', {}).get('duration', '')
total_s = d.get('total_duration', '')
cm = d.get('criu_metrics', {}).get('lazy_pages', {}) or {}
daemon = cm.get('daemon_duration_s', '')
faults = cm.get('uffd_faults', '')
fs = cm.get('fault_stats', {}) or {}
s3_served = fs.get('s3_served', '')
cache_served = fs.get('cache_served', '')
stall_avg = fs.get('stall_ms_avg', '')
s3_stall = fs.get('s3_stall_ms_avg', '')
cache_stall = fs.get('cache_stall_ms_avg', '')
def f(x):
    if isinstance(x, (int, float)):
        return f"{x:.2f}"
    return str(x)
row = [w, rep, f(restore_s), f(daemon), f(total_s), str(faults),
       str(s3_served), str(cache_served), f(stall_avg), f(s3_stall), f(cache_stall)]
with open(csv, 'a') as fp:
    fp.write(','.join(row) + '\n')
print(f"  metrics: daemon={f(daemon)}s faults={faults} s3={s3_served} stall_avg={f(stall_avg)}ms")
PY
}

for W in $WORKERS_LIST; do
    for REP in $(seq 1 "$REPS"); do
        TAG="w${W}_rep${REP}"
        LOG="$OUT/${TAG}.log"
        echo ""
        echo "[sweep] === $TAG ==="
        T0=$(date +%s)
        bash experiments/run_restore_experiment.sh \
            --workload "$WORKLOAD" \
            --s3-prefix "$S3_PREFIX" \
            --repeat 1 \
            --mode "$MODE" \
            --extra-args "$EXTRA_BASE --prefetch-workers $W" \
            > "$LOG" 2>&1
        RC=$?
        T1=$(date +%s)
        echo "[sweep] $TAG exit=$RC wall=$((T1-T0))s"

        RDIR=$(ls -dt /tmp/results/${S3_PREFIX//\//_}_* 2>/dev/null | head -1)
        if [ -z "$RDIR" ]; then
            RDIR=$(ls -dt /tmp/results/* 2>/dev/null | head -1)
        fi
        if [ -n "$RDIR" ] && [ -d "$RDIR" ]; then
            cp -r "$RDIR" "$OUT/${TAG}_results" 2>/dev/null || true
            METRIC="$RDIR/${MODE}_run1.json"
            if [ -f "$METRIC" ]; then
                extract_metrics "$METRIC" "$W" "$REP"
            else
                echo "[sweep] WARN: no metric json at $METRIC"
                echo "$W,$REP,,,,,,,,," >> "$SUMMARY"
            fi
        else
            echo "[sweep] WARN: no results dir for $TAG"
            echo "$W,$REP,,,,,,,,," >> "$SUMMARY"
        fi
    done
done

echo ""
echo "[sweep] DONE -> $OUT"
echo "[sweep] === summary.csv ==="
cat "$SUMMARY"
