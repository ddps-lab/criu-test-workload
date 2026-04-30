#!/bin/bash
# E5 — End-to-end CRIU lazy restore in eu-west-3 with X-Cache logging.
#
# Spawns one m5.8xlarge in eu-west-3 from AMI v5 (the patched criu binary
# with FETCH_DONE x_cache=/x_pop=/src= logging) and runs a real
# run_restore_experiment.sh against the CloudFront distribution. The
# warming step uses cf_warmer Lambda (eu-west-3) with the optimum config
# discovered in E1/E2. After restore, this script greps the lazy-pages.log
# for FETCH_DONE lines and breaks down hits/misses by src=fault and
# src=prefetch.
#
# This is the paper's ground-truth measurement: "after warming, what
# fraction of the lazy restore's actual fetches hit CloudFront edge?"
#
# Usage:
#   WORKLOADS="matmul redis memcached" MODES="3_semi_sync 5_full" REPS=3 \
#       bash e5_eu_restore.sh
#
# Required env defaults match prior experiments. AMI must already be
# replicated into eu-west-3.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WARMER_DIR="$(cd "$HERE/.." && pwd)"
: "${REGION:=eu-west-3}"
: "${AMI_ID:=ami-035969277600ff57a}"
: "${INSTANCE_TYPE:=m5.8xlarge}"
: "${KEY_NAME:=mhsong-paris}"
: "${SG_ID:=sg-0e15048137db7780d}"
: "${SUBNET_ID:=subnet-094303a0aaa3c1b4e}"
: "${IAM_PROFILE:=mhsong-ec2-admin}"
: "${SSH_KEY:=$HOME/.ssh/mhsong-ddps-oregon.pem}"
: "${BUCKET:=mhsong-criu-checkpoints}"
: "${ORIGIN_REGION:=us-west-2}"
: "${DISTRIBUTION_ID:=E1J7V6EO31JOOO}"
: "${DISTRIBUTION_DOMAIN:=deytbsxznbpj1.cloudfront.net}"
: "${WARMER_FN:=cf_warmer}"
: "${WARMER_REGION:=eu-west-3}"
: "${WARMER_SHARDS:=16}"
: "${WARMER_CHILD_CONCURRENCY:=64}"
: "${WORKLOADS:=matmul}"
: "${MODES:=3_semi_sync 5_full}"
: "${REPS:=3}"
: "${KEEP_INSTANCE:=0}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/e5_$TS"
mkdir -p "$OUTDIR"
META="$OUTDIR/run_metadata.json"
SUMMARY="$OUTDIR/summary.csv"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "kind": "e5_eu_restore",
  "region": "$REGION",
  "ami": "$AMI_ID",
  "distribution": "$DISTRIBUTION_DOMAIN",
  "warmer_fn": "$WARMER_FN",
  "warmer_shards": $WARMER_SHARDS,
  "warmer_child_concurrency": $WARMER_CHILD_CONCURRENCY,
  "workloads": "$WORKLOADS",
  "modes": "$MODES",
  "reps": $REPS
}
META_JSON

echo "workload,mode,rep,src,hit,miss,total,hit_pct,fetch_count,unique_pops" > "$SUMMARY"

echo "[e5] ts=$TS outdir=$OUTDIR"
echo "[e5] workloads=($WORKLOADS) modes=($MODES) reps=$REPS"

# --- 1. Spawn the destination EC2 in eu-west-3 -------------------------------
echo "[e5] spawning $INSTANCE_TYPE in $REGION from $AMI_ID"
IID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --count 1 \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --iam-instance-profile Name="$IAM_PROFILE" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-e5-$TS}]" \
    --query 'Instances[0].InstanceId' --output text)
echo "[e5] iid=$IID"
aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "[e5] ip=$IP"

cleanup_instance() {
    if [ "$KEEP_INSTANCE" = "1" ]; then
        echo "[e5] KEEP_INSTANCE=1; not terminating $IID"
        return
    fi
    echo "[e5] terminating $IID"
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$IID" \
        --query 'TerminatingInstances[0].CurrentState.Name' --output text || true
}
trap cleanup_instance EXIT

# Wait for SSH ready.
for i in $(seq 1 30); do
    ssh -i "$SSH_KEY" -o ConnectTimeout=3 -o StrictHostKeyChecking=no \
        ubuntu@"$IP" "echo ok" 2>/dev/null && break
    sleep 4
done
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no ubuntu@"$IP" \
    "criu --version 2>&1 | head -2" || {
    echo "[e5] criu not present on instance!"; exit 1;
}

# Push AWS credentials so run_restore_experiment.sh can talk to S3 listing,
# CF distribution, etc. (the patched criu uses the env vars too).
AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no ubuntu@"$IP" \
    "cd /opt/criu_workload && git pull origin main -q 2>&1 | tail -2 || true"

invoke_warmer() {
    local workload="$1" out="$2"
    # Build payload: bucket+prefix mode (cf_warmer parses pagemap-*.img)
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    cat > "$payload" <<JSON
{
  "mode": "dispatch",
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "bucket": "$BUCKET",
  "prefix": "$workload",
  "concurrency": $WARMER_CHILD_CONCURRENCY,
  "shard_count": $WARMER_SHARDS
}
JSON
    AWS_MAX_ATTEMPTS=1 aws lambda invoke \
        --region "$WARMER_REGION" \
        --function-name "$WARMER_FN" \
        --cli-binary-format raw-in-base64-out \
        --cli-read-timeout 0 \
        --cli-connect-timeout 30 \
        --payload "file://$payload" \
        "$resp" \
        --query 'StatusCode' --output text >/dev/null
    cp "$resp" "$out"
    rm -f "$payload" "$resp"
}

invalidate_prefix() {
    local prefix="$1"
    local inv_id
    inv_id=$(aws cloudfront create-invalidation \
        --distribution-id "$DISTRIBUTION_ID" \
        --paths "/${prefix}/*" \
        --query 'Invalidation.Id' --output text)
    aws cloudfront wait invalidation-completed \
        --distribution-id "$DISTRIBUTION_ID" --id "$inv_id" >/dev/null 2>&1
    echo "$inv_id"
}

WORKLOAD_PREFIX_AND_ARGS() {
    case "$1" in
        matmul)     echo "matmul --matrix-size 2048" ;;
        redis)      echo "redis --record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
        memcached)  echo "memcached --memcached-memory 11264 --record-count 8500000 --ycsb-threads 4" ;;
        *) echo ""; return 1 ;;
    esac
}

for workload in $WORKLOADS; do
    spec=$(WORKLOAD_PREFIX_AND_ARGS "$workload")
    prefix=$(echo "$spec" | awk '{print $1}')
    extra_args=$(echo "$spec" | cut -d' ' -f2-)

    echo "[e5] === workload=$workload prefix=$prefix ==="

    # Invalidate CF prefix first so the warmer is the source of truth.
    inv=$(invalidate_prefix "$prefix")
    echo "[e5] invalidation=$inv"

    # Warm the prefix via Lambda dispatcher.
    warm_out="$OUTDIR/${workload}_warm.json"
    echo "[e5] warming $prefix via cf_warmer (shards=$WARMER_SHARDS)"
    invoke_warmer "$workload" "$warm_out"
    python3 - "$warm_out" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"  warmer: hit={d.get('hit_count')} miss={d.get('miss_count')} "
      f"wall_ms={d.get('wall_ms')} pops={d.get('pop_counts')}")
PY

    for mode in $MODES; do
        for rep in $(seq 1 "$REPS"); do
            tag="${workload}_${mode}_rep${rep}"
            echo "[e5] --- run $tag ---"
            ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no ubuntu@"$IP" \
                "cd /opt/criu_workload && \
                 export AWS_ACCESS_KEY_ID='$AWS_KEY' AWS_SECRET_ACCESS_KEY='$AWS_SECRET' && \
                 bash experiments/run_restore_experiment.sh \
                     --workload $workload \
                     --s3-prefix $prefix \
                     --repeat 1 \
                     --mode $mode \
                     --extra-args '$extra_args' \
                     --s3-bucket $BUCKET \
                     --s3-region $ORIGIN_REGION \
                     --s3-endpoint https://$DISTRIBUTION_DOMAIN \
                     --s3-type cloudfront 2>&1 | tail -15" \
                > "$OUTDIR/${tag}.console.log"

            # Pull the lazy-pages log back. run_restore_experiment.sh writes
            # results to /tmp/results/<prefix>_<TS>/. Find the most recent
            # one and grab the *_lazy.log file for this mode.
            remote_log=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no \
                ubuntu@"$IP" \
                "ls -t /tmp/results/${prefix}_*/${mode}_run1_lazy.log 2>/dev/null | head -1")
            if [ -z "$remote_log" ]; then
                echo "[e5] WARN: no lazy.log for $tag"
                continue
            fi
            scp -i "$SSH_KEY" -o StrictHostKeyChecking=no \
                "ubuntu@$IP:$remote_log" "$OUTDIR/${tag}_lazy.log" 2>/dev/null

            python3 - "$OUTDIR/${tag}_lazy.log" "$workload" "$mode" "$rep" \
                "$SUMMARY" <<'PY'
import csv, re, sys
from collections import Counter
path, workload, mode, rep, csv_path = sys.argv[1:]
hit_re = re.compile(rb'src=(\w+).*x_cache="([^"]*)".*x_pop="([^"]*)"')
done_re = re.compile(rb'FETCH_DONE.*x_cache="([^"]*)".*x_pop="([^"]*)".*src=(\w+)')
by_src = {"fault": [0, 0, Counter()], "prefetch": [0, 0, Counter()]}
total = 0
with open(path, "rb") as f:
    for line in f:
        m = done_re.search(line)
        if not m:
            continue
        cache, pop, src = m.group(1).decode(), m.group(2).decode(), m.group(3).decode()
        if src not in by_src:
            by_src[src] = [0, 0, Counter()]
        if cache.startswith("Hit"):
            by_src[src][0] += 1
        elif cache.startswith("Miss"):
            by_src[src][1] += 1
        if pop:
            by_src[src][2][pop] += 1
        total += 1
with open(csv_path, "a") as f:
    w = csv.writer(f)
    for src, (hit, miss, pops) in by_src.items():
        n = hit + miss
        if n == 0:
            continue
        pct = hit / n * 100
        w.writerow([
            workload, mode, rep, src, hit, miss, n, f"{pct:.1f}",
            n, ";".join(f"{k}:{v}" for k, v in pops.most_common()),
        ])
        print(f"  {src:<8}: hit={hit}/{n} ({pct:.1f}%) pops={dict(pops)}")
print(f"  total FETCH_DONE: {total}")
PY
        done
    done
done

echo "[e5] done. results in $OUTDIR"
