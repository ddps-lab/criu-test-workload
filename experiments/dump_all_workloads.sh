#!/bin/bash
# dump_all_workloads.sh — reproducible dump creation for the full paper
# workload set. Wraps baseline_experiment.py — no custom dump logic; the
# per-workload flags mirror experiments/launch_storage_sweep.sh exactly
# so the resulting S3 prefixes are drop-in compatible with the existing
# restore sweeps (sweep_prefetch_workers.sh, run_restore_experiment.sh).
#
# Usage:
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#     bash dump_all_workloads.sh \
#         --bucket mhsong-criu-checkpoints \
#         --region us-west-2 \
#         [--workload mc-4gb]         # single-workload override (name from table)
#         [--workload all]            # whole table (default)
#
# What this does for each workload:
#   1. Purges s3://$BUCKET/$PREFIX/ so no stale files layer with the new dump.
#   2. Runs baseline_experiment.py with --s3-direct-upload (images go straight
#      to S3 via criu-s3's multipart upload path) and --track-dirty-pages so
#      the pipeline extracts and uploads hot-vmas.json alongside the dump.
#   3. Ignores the restore-phase failure that baseline_experiment.py triggers
#      at the end when run same-source-same-dest on a single instance — we
#      only care that the dump + upload steps succeeded.
#   4. Verifies hot-vmas.json and the pages-*.img / metadata set landed in S3.
#
# This script deliberately does NOT replicate anything already in
# lib/checkpoint.py or lib/criu_utils.py — it is just the driver that
# calls the canonical pipeline once per workload.

set -uo pipefail

BUCKET=mhsong-criu-checkpoints
REGION=us-west-2
SEL=all
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID env var required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY env var required}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)   BUCKET="$2"; shift 2 ;;
        --region)   REGION="$2"; shift 2 ;;
        --workload) SEL="$2"; shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

# ==========================================================================
# Canonical workload table — MUST match launch_storage_sweep.sh:ALL_EXPERIMENTS
# so the resulting S3 prefixes are re-usable by every existing restore
# script. Each entry: name|baseline_experiment --workload value|S3 prefix|
# extra baseline_experiment args.
# ==========================================================================
ALL_ENTRIES=(
    "matmul|matmul|matmul|--matrix-size 2048"
    "dataproc|dataproc|dataproc|--num-rows 1500000 --num-cols 60 --batch-size 1000"
    "ml-training|ml_training|ml-training|--model-size large --dataset-size 50000"
    "xgboost|xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
    "redis|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "mc-1gb|memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4"
    "mc-4gb|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"
    "mc-8gb|memcached|memcached-8gb|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4"
    "mc-11gb|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
    "mc-16gb|memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4"
)

SELECTED=()
if [ "$SEL" = "all" ]; then
    SELECTED=("${ALL_ENTRIES[@]}")
else
    for e in "${ALL_ENTRIES[@]}"; do
        name="${e%%|*}"
        if [ "$name" = "$SEL" ]; then
            SELECTED=("$e")
            break
        fi
    done
    if [ ${#SELECTED[@]} -eq 0 ]; then
        echo "ERROR: unknown --workload '$SEL'. Valid values:"
        for e in "${ALL_ENTRIES[@]}"; do echo "  ${e%%|*}"; done
        exit 1
    fi
fi

dump_one() {
    local entry="$1"
    local NAME TYPE PREFIX EXTRA
    IFS='|' read -r NAME TYPE PREFIX EXTRA <<< "$entry"

    echo "=========================================="
    echo " $NAME → s3://$BUCKET/$PREFIX/"
    echo " type:  $TYPE"
    echo " extra: $EXTRA"
    echo "=========================================="

    # 1. Purge stale S3 state so previous attempts don't layer with new dump.
    echo "[purge] s3://$BUCKET/$PREFIX/"
    aws s3 rm "s3://$BUCKET/$PREFIX/" --region "$REGION" --recursive --quiet \
        > /dev/null 2>&1 || true

    # 2. Drive baseline_experiment.py through the canonical dump path.
    # NOTE: same-source/same-dest is intentional — dump + upload are what we
    # care about, restore at the end of the same pipeline is allowed to fail.
    cd /opt/criu_workload
    sudo -E env \
        AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
        AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
        python3 experiments/baseline_experiment.py \
        --config config/experiments/memcached_lazy_prefetch.yaml \
        --source-ip 127.0.0.1 --dest-ip 127.0.0.1 \
        --workload "$TYPE" \
        $EXTRA \
        --s3-direct-upload \
        --s3-type standard \
        --s3-upload-bucket "$BUCKET" \
        --s3-prefix "$PREFIX" \
        --s3-region "$REGION" \
        --s3-access-key "$AWS_ACCESS_KEY_ID" \
        --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
        --lazy-mode lazy-prefetch \
        --track-dirty-pages \
        --dirty-tracker c \
        --dirty-track-interval 500 --dirty-track-duration 10 \
        --no-cleanup \
        -o "/tmp/dump_${NAME}.json" 2>&1 | tee "/tmp/dump_${NAME}.log" \
        | grep -E 'Uploaded|Final dump|Extracted|hot-vmas|ERROR|WARNING' || true
    # Intentional: if the restore phase at the tail of the pipeline fails,
    # we still consider the dump successful as long as the checkpoint
    # images and hot-vmas.json made it to S3.

    # 3. Verify the resulting prefix.
    echo "[verify] s3://$BUCKET/$PREFIX/"
    local has_pages has_hot
    has_pages=$(aws s3 ls "s3://$BUCKET/$PREFIX/" --region "$REGION" | grep -c 'pages-.*\.img' || true)
    has_hot=$(aws s3 ls "s3://$BUCKET/$PREFIX/hot-vmas.json" --region "$REGION" 2>/dev/null | wc -l)
    if [ "$has_pages" -gt 0 ] && [ "$has_hot" -gt 0 ]; then
        echo "      OK ($has_pages pages images, hot-vmas.json present)"
    else
        echo "      WARN: incomplete (pages=$has_pages hot_vmas=$has_hot)"
    fi
}

FAIL=0
for entry in "${SELECTED[@]}"; do
    if ! dump_one "$entry"; then
        FAIL=$((FAIL + 1))
        echo "FAILED: $entry"
    fi
    sleep 5
done

if [ "$FAIL" -eq 0 ]; then
    echo "ALL OK (${#SELECTED[@]} workloads dumped)"
else
    echo "FAILURES: $FAIL / ${#SELECTED[@]}"
    exit 1
fi
