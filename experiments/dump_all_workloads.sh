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
#      the pipeline extracts and uploads hot-iovs.json alongside the dump.
#   3. Ignores the restore-phase failure that baseline_experiment.py triggers
#      at the end when run same-source-same-dest on a single instance — we
#      only care that the dump + upload steps succeeded.
#   4. Verifies hot-iovs.json and the pages-*.img / metadata set landed in S3.
#
# This script deliberately does NOT replicate anything already in
# lib/checkpoint.py or lib/criu_utils.py — it is just the driver that
# calls the canonical pipeline once per workload.

set -uo pipefail

BUCKET=mhsong-criu-checkpoints
REGION=us-west-2
SEL=all
COMPRESS=0
REPEAT=1
PREFIX_BASE=""   # e.g. "instance-scaling/m5.xlarge/" — prepended to per-workload
                 # PREFIX so existing dumps at s3://$BUCKET/<wl>/ stay untouched.
BASELINE_CLI=0   # --baseline-cli: after all reps, measure aws-cli upload (default + crt) on raw image
BASELINE_CLI_REPS=5
SELF_TERMINATE=0 # --self-terminate: aws ec2 terminate-instances on completion (auto-detects own instance id)
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID env var required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY env var required}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket)             BUCKET="$2"; shift 2 ;;
        --region)             REGION="$2"; shift 2 ;;
        --workload)           SEL="$2"; shift 2 ;;
        --compress)           COMPRESS=1; shift ;;
        --repeat)             REPEAT="$2"; shift 2 ;;
        --prefix-base)        PREFIX_BASE="$2"; shift 2 ;;
        --baseline-cli)       BASELINE_CLI=1; shift ;;
        --baseline-cli-reps)  BASELINE_CLI_REPS="$2"; shift 2 ;;
        --self-terminate)     SELF_TERMINATE=1; shift ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

# Ensure trailing slash for clean concat: $BUCKET/$PREFIX_BASE$PREFIX/
if [ -n "$PREFIX_BASE" ] && [[ "$PREFIX_BASE" != */ ]]; then
    PREFIX_BASE="$PREFIX_BASE/"
fi

# --compress: produce zstd-seekable page images. S3 prefix gets a
# "-compressed" suffix so the raw and compressed dumps can coexist and
# the ablation launcher can pick either with --s3-prefix.
COMPRESS_FLAGS=""
PREFIX_SUFFIX=""
if [ "$COMPRESS" = "1" ]; then
    COMPRESS_FLAGS="--compress-pages --compress-workers 8"
    PREFIX_SUFFIX="-compressed"
fi

# ==========================================================================
# Canonical workload table — MUST match launch_storage_sweep.sh:ALL_EXPERIMENTS
# so the resulting S3 prefixes are re-usable by every existing restore
# script. Each entry: name|baseline_experiment --workload value|S3 prefix|
# extra baseline_experiment args.
# ==========================================================================
ALL_ENTRIES=(
    "matmul|matmul|matmul|--matrix-size 25000"
    "dataproc|dataproc|dataproc|--num-rows 17000000 --num-cols 60 --batch-size 1000"
    "ml-training|ml_training|ml-training|--model-size large --dataset-size 2000000"
    "xgboost|xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
    "redis|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "mc-1gb|memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4"
    "mc-4gb|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"
    "mc-8gb|memcached|memcached|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4"
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
    PREFIX="${PREFIX_BASE}${PREFIX}${PREFIX_SUFFIX}"

    echo "=========================================="
    echo " $NAME → s3://$BUCKET/$PREFIX/ (repeat x$REPEAT)"
    echo " type:     $TYPE"
    echo " extra:    $EXTRA"
    echo " compress: $COMPRESS"
    echo "=========================================="

    local r
    for r in $(seq 1 $REPEAT); do
        echo ""
        echo "--- [$(date +%H:%M:%S)] $NAME repeat $r/$REPEAT ---"

        # Clean any leftover workload processes / criu state from prior
        # repeat on the same instance. Baseline_experiment.py spawns fresh
        # workloads but stale redis-server / memcached can collide on ports
        # and /tmp/criu_checkpoint/<iter>/ accumulates stale criu-*.log that
        # would otherwise pollute the next repeat's dump-metrics/ upload.
        if [ "$r" -gt 1 ]; then
            sudo pkill -9 -f "_standalone\.py|redis-server|memcached -m" 2>/dev/null || true
            sleep 3
            sudo find /tmp/criu_checkpoint -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} \; 2>/dev/null || true
        fi

        # 1. Purge stale checkpoint files. The checkpoint/ subfolder is
        # purged wholesale — pages-*.img / pagemap-*.img filenames depend
        # on PID, which changes between reps, so leftover files from a
        # prior rep would mix with the current one. dump-metrics/ sits
        # outside checkpoint/ and is never touched here.
        echo "[purge] s3://$BUCKET/$PREFIX/checkpoint/"
        aws s3 rm "s3://$BUCKET/$PREFIX/checkpoint/" --region "$REGION" --recursive --quiet \
            > /dev/null 2>&1 || true

        # 2. Drive baseline_experiment.py through the canonical dump path.
        # NOTE: same-source/same-dest is intentional — dump + upload are
        # what we care about, restore at the end is allowed to fail.
        cd /opt/criu_workload
        sudo -E env \
            AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
            AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
            python3 experiments/baseline_experiment.py \
            --source-ip 127.0.0.1 --dest-ip 127.0.0.1 \
            --workload "$TYPE" \
            $EXTRA \
            --s3-direct-upload \
            --s3-type standard \
            --s3-upload-bucket "$BUCKET" \
            --s3-prefix "$PREFIX/checkpoint" \
            --s3-region "$REGION" \
            --s3-access-key "$AWS_ACCESS_KEY_ID" \
            --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
            --lazy-mode lazy-prefetch \
            --track-dirty-pages \
            --dirty-tracker c \
            --dirty-track-interval 5000 \
            $COMPRESS_FLAGS \
            --no-cleanup \
            -o "/tmp/dump_${NAME}${PREFIX_SUFFIX}.json" 2>&1 | tee "/tmp/dump_${NAME}${PREFIX_SUFFIX}.log" \
            | grep -E 'Uploaded|Final dump|Extracted|hot-vmas|ERROR|WARNING' || true

        # 3. Verify the resulting prefix.
        echo "[verify] s3://$BUCKET/$PREFIX/checkpoint/"
        local has_pages has_hot
        has_pages=$(aws s3 ls "s3://$BUCKET/$PREFIX/checkpoint/" --region "$REGION" | grep -c 'pages-.*\.img' || true)
        has_hot=$(aws s3 ls "s3://$BUCKET/$PREFIX/checkpoint/hot-iovs.json" --region "$REGION" 2>/dev/null | wc -l)
        if [ "$has_pages" -gt 0 ] && [ "$has_hot" -gt 0 ]; then
            echo "      OK ($has_pages pages images, hot-iovs.json present)"
        else
            echo "      WARN: incomplete (pages=$has_pages hot_iovs=$has_hot)"
        fi

        # 4. Persist per-repeat dump metrics under dump-metrics/repeat-$r/.
        # JSON has final_dump.duration; .log has CRIU stderr incl.
        # compress_stats; criu-dump.log has per-phase timings + upload_pool.
        local metrics_dir="dump-metrics/repeat-$r"
        local json_src log_src driver_src dirty_src
        json_src="/tmp/dump_${NAME}${PREFIX_SUFFIX}.json"
        log_src="/tmp/dump_${NAME}${PREFIX_SUFFIX}.log"
        driver_src="/tmp/driver.log"
        dirty_src="/tmp/dirty_pattern.json"
        for src in "$json_src" "$log_src" "$driver_src" "$dirty_src"; do
            if [ -f "$src" ]; then
                aws s3 cp "$src" "s3://$BUCKET/$PREFIX/$metrics_dir/$(basename "$src")" \
                    --region "$REGION" --only-show-errors || \
                    echo "      WARN: failed to upload $(basename "$src")"
            fi
        done

        # CRIU internal logs (criu-dump.log, criu-pre-dump.log, …).
        local iter_dir criu_log rel
        for iter_dir in /tmp/criu_checkpoint/*/; do
            [ -d "$iter_dir" ] || continue
            for criu_log in "$iter_dir"criu-*.log; do
                [ -f "$criu_log" ] || continue
                rel="$(basename "$iter_dir")_$(basename "$criu_log")"
                aws s3 cp "$criu_log" "s3://$BUCKET/$PREFIX/$metrics_dir/$rel" \
                    --region "$REGION" --only-show-errors || \
                    echo "      WARN: failed to upload $(basename "$criu_log")"
            done
        done
    done
}

baseline_cli_one() {
    # Measure aws-cli upload throughput on the last rep's checkpoint files,
    # comparing default transfer client vs CRT-enabled client. Only meaningful
    # for raw mode (compressed checkpoints already include their own pipeline
    # behaviour and aren't the baseline target).
    local entry="$1"
    local NAME TYPE PREFIX EXTRA
    IFS='|' read -r NAME TYPE PREFIX EXTRA <<< "$entry"
    PREFIX="${PREFIX_BASE}${PREFIX}${PREFIX_SUFFIX}"

    if [ "$COMPRESS" = "1" ]; then
        echo "[baseline-cli] skipping (compressed dump)"; return 0
    fi

    echo "=========================================="
    echo " baseline-cli for $NAME (raw)"
    echo " src: s3://$BUCKET/$PREFIX/checkpoint/"
    echo "=========================================="

    local local_in=/tmp/baseline_input
    sudo rm -rf "$local_in"; mkdir -p "$local_in"
    aws s3 sync "s3://$BUCKET/$PREFIX/checkpoint/" "$local_in/" --region "$REGION" --only-show-errors

    local local_bytes
    local_bytes=$(du -sb "$local_in" | awk '{print $1}')
    echo "[baseline-cli] downloaded $((local_bytes / 1024 / 1024)) MB to $local_in"

    local metrics_file=/tmp/baseline-cli-metrics.json
    echo "{\"workload\": \"$NAME\", \"local_bytes\": $local_bytes, \"reps\": [" > "$metrics_file"

    local mode rep tmp_prefix t_start t_end wall_ms
    local first=1
    for mode in default crt; do
        # configure aws-cli transfer client
        if [ "$mode" = "crt" ]; then
            aws configure set s3.preferred_transfer_client crt
        else
            aws configure set s3.preferred_transfer_client classic
        fi
        for rep in $(seq 1 "$BASELINE_CLI_REPS"); do
            tmp_prefix="$PREFIX/baseline-cli-tmp/${mode}-rep${rep}"
            echo "[baseline-cli] $mode rep $rep -> s3://$BUCKET/$tmp_prefix/"
            t_start=$(date +%s%3N)
            aws s3 sync "$local_in/" "s3://$BUCKET/$tmp_prefix/" --region "$REGION" --only-show-errors
            t_end=$(date +%s%3N)
            wall_ms=$((t_end - t_start))
            local throughput_mbps=$(( local_bytes * 8 / 1000 / wall_ms ))
            [ "$first" = "0" ] && echo "," >> "$metrics_file"
            cat <<EOF >> "$metrics_file"
  {"mode": "$mode", "rep": $rep, "wall_ms": $wall_ms, "throughput_mbps": $throughput_mbps}
EOF
            first=0
            # delete the just-uploaded objects to keep storage cost minimal
            aws s3 rm "s3://$BUCKET/$tmp_prefix/" --region "$REGION" --recursive --quiet > /dev/null 2>&1 || true
        done
    done

    aws configure set s3.preferred_transfer_client classic  # restore default

    echo "]}" >> "$metrics_file"

    aws s3 cp "$metrics_file" \
        "s3://$BUCKET/$PREFIX/dump-metrics/baseline-cli/metrics.json" \
        --region "$REGION" --only-show-errors

    sudo rm -rf "$local_in"
    echo "[baseline-cli] done. Metrics at s3://$BUCKET/$PREFIX/dump-metrics/baseline-cli/metrics.json"
}

self_terminate() {
    # IMDSv2 — get token then instance ID, then aws ec2 terminate-instances.
    local TOKEN ID
    TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60") || true
    ID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
        http://169.254.169.254/latest/meta-data/instance-id) || true
    if [ -z "$ID" ]; then
        echo "[self-terminate] FAILED to get instance-id from IMDSv2"
        return 1
    fi
    echo "[self-terminate] terminating instance $ID in region $REGION"
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$ID" \
        --query 'TerminatingInstances[0].CurrentState.Name' --output text
}

FAIL=0
for entry in "${SELECTED[@]}"; do
    if ! dump_one "$entry"; then
        FAIL=$((FAIL + 1))
        echo "FAILED: $entry"
    fi
    sleep 5
    if [ "$BASELINE_CLI" = "1" ]; then
        baseline_cli_one "$entry" || echo "WARN: baseline-cli failed for $entry"
    fi
done

if [ "$FAIL" -eq 0 ]; then
    echo "ALL OK (${#SELECTED[@]} workloads dumped)"
    [ "$SELF_TERMINATE" = "1" ] && self_terminate
else
    echo "FAILURES: $FAIL / ${#SELECTED[@]}"
    [ "$SELF_TERMINATE" = "1" ] && self_terminate
    exit 1
fi
