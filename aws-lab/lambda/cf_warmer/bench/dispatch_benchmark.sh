#!/bin/bash
# Multi-Lambda fan-out benchmark (dispatch mode).
#
# Unlike synth_benchmark.sh (single Lambda instance sweeping
# memory/concurrency), this script measures how fan-out across N
# sibling Lambdas scales aggregate throughput. The dispatcher Lambda
# splits the IOV list into N shards, invokes the same function N times
# in parallel, and collects per-child summaries.
#
# For each (size_gb, shard_count):
#   1. Invalidate the target key at CloudFront.
#   2. Invoke handler.py in `dispatch` mode with shard_count=N.
#      The dispatcher fan-outs N child Lambda invocations, each warming
#      roughly size_gb/N worth of chunks at `child_concurrency`.
#   3. Record (wall_ms, total_hit, total_miss, pop_counts, cost).
#
# Cost model matches synth_benchmark.sh. Child Lambda GB-s is counted
# per-invocation so total Lambda cost = sum(child durations * memory).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${DISTRIBUTION_ID:=E1J7V6EO31JOOO}"
: "${DISTRIBUTION_DOMAIN:=deytbsxznbpj1.cloudfront.net}"
: "${BUCKET:=mhsong-criu-checkpoints}"
: "${TARGET_KEY:=memcached-16gb/pages-2.img}"
: "${REGION:=eu-west-3}"
: "${FN_NAME:=cf_warmer}"
: "${LAMBDA_MEMORY_MB:=3008}"
: "${CHILD_CONCURRENCY:=64}"
: "${CF_PRICE_PER_GB:=0.085}"
: "${LAMBDA_PRICE_PER_GBS:=0.0000166667}"

SIZES_GB="${1:-${SIZES_GB:-1 2 4 8 16}}"
SHARDS="${2:-${SHARDS:-2 4 8}}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/dispatch_$TS"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/summary.csv"
META="$OUTDIR/run_metadata.json"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "kind": "dispatch",
  "distribution_id": "$DISTRIBUTION_ID",
  "target_key": "$TARGET_KEY",
  "region": "$REGION",
  "function_name": "$FN_NAME",
  "lambda_memory_mb": $LAMBDA_MEMORY_MB,
  "child_concurrency": $CHILD_CONCURRENCY,
  "sizes_gb": "$SIZES_GB",
  "shards": "$SHARDS"
}
META_JSON

echo "size_gb,shards,phase,wall_ms,thru_mb_s,hit_count,miss_count,pop_counts,cf_gb,cf_cost_usd,lambda_s_total,lambda_cost_usd,total_cost_usd" > "$CSV"

echo "[dispatch] ts=$TS outdir=$OUTDIR target=$TARGET_KEY"
echo "[dispatch] sizes_gb=($SIZES_GB) shards=($SHARDS) child_conc=$CHILD_CONCURRENCY"

invalidate_key() {
    local inv_id
    inv_id=$(aws cloudfront create-invalidation \
        --distribution-id "$DISTRIBUTION_ID" \
        --paths "/${TARGET_KEY}" \
        --query 'Invalidation.Id' --output text)
    aws cloudfront wait invalidation-completed \
        --distribution-id "$DISTRIBUTION_ID" --id "$inv_id" >/dev/null 2>&1
    echo "$inv_id"
}

invoke_dispatch() {
    local size_gb="$1" shard_count="$2" out="$3"
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    python3 - "$size_gb" "$shard_count" "$payload" <<'PY'
import json, os, sys
size_gb, shard_count, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
size_bytes = int(float(size_gb) * (1024**3))
chunk = 4 * 1024 * 1024
key = os.environ.get("TARGET_KEY", "memcached-16gb/pages-2.img")
distribution = os.environ["DISTRIBUTION_DOMAIN"]
child_conc = int(os.environ.get("CHILD_CONCURRENCY", "64"))
chunks = []
off = 0
while off < size_bytes:
    length = min(chunk, size_bytes - off)
    chunks.append([key, off, length])
    off += length
payload = {
    "mode": "dispatch",
    "distribution_domain": distribution,
    "concurrency": child_conc,
    "shard_count": shard_count,
    "iov_override": chunks,
    "meta_override": [],
}
with open(out, "w") as f:
    json.dump(payload, f)
PY
    AWS_MAX_ATTEMPTS=1 aws lambda invoke \
        --region "$REGION" \
        --function-name "$FN_NAME" \
        --cli-binary-format raw-in-base64-out \
        --cli-read-timeout 0 \
        --cli-connect-timeout 30 \
        --payload "file://$payload" \
        "$resp" \
        --query 'StatusCode' --output text >/dev/null
    cp "$resp" "$out"
    rm -f "$payload" "$resp"
}

export DISTRIBUTION_DOMAIN BUCKET TARGET_KEY CHILD_CONCURRENCY

record_phase() {
    # phase=cold|verify
    local phase="$1" out="$2" sg="$3" n="$4"
    python3 - "$out" "$sg" "$n" "$LAMBDA_MEMORY_MB" \
        "$CF_PRICE_PER_GB" "$LAMBDA_PRICE_PER_GBS" "$CSV" "$phase" <<'PY'
import csv, json, sys
path, sg, n, mem_mb, cf_price, lam_price, csv_path, phase = sys.argv[1:]
with open(path) as f:
    d = json.load(f)
total_bytes = d.get("total_bytes", 0)
hit = d.get("hit_count", 0)
miss = d.get("miss_count", 0)
wall_ms = d.get("wall_ms", 0.0)
pops = d.get("pop_counts") or {}
children = d.get("child_summaries") or []
# Sum child Lambda durations for GB-s cost (dispatcher itself is ~free).
child_ms = sum(c.get("wall_ms", 0.0) for c in children)
child_ms += wall_ms  # include dispatcher wall
lam_s = child_ms / 1000.0
cf_gb = total_bytes / (1024**3)
cf_cost = cf_gb * float(cf_price)
lam_cost = (int(mem_mb) / 1024.0) * lam_s * float(lam_price)
total = cf_cost + lam_cost
thru = (total_bytes / (1024 * 1024)) / (wall_ms / 1000.0) if wall_ms > 0 else 0
with open(csv_path, "a") as f:
    w = csv.writer(f)
    w.writerow([
        sg, n, phase, wall_ms, f"{thru:.1f}",
        hit, miss, json.dumps(pops, separators=(",", ":")),
        f"{cf_gb:.4f}", f"{cf_cost:.6f}",
        f"{lam_s:.2f}", f"{lam_cost:.6f}", f"{total:.6f}",
    ])
print(f"  size={sg}GB n={n} {phase:<6}: bytes={total_bytes/1e6:.0f}MB "
      f"wall={wall_ms/1000:.1f}s thru={thru:.1f}MB/s "
      f"hit={hit} miss={miss} pops={pops} cost=${total:.4f}")
PY
}

for sg in $SIZES_GB; do
    for n in $SHARDS; do
        tag="size${sg}gb_n${n}"

        # --- cold pass: invalidate then warm ---
        echo "[dispatch] invalidate for $tag (cold)"
        inv=$(invalidate_key)
        echo "[dispatch] invalidation=$inv"
        cold_out="$OUTDIR/${tag}_cold.json"
        echo "[dispatch] cold invoke $tag"
        invoke_dispatch "$sg" "$n" "$cold_out"
        record_phase cold "$cold_out" "$sg" "$n"

        # --- verify pass: same payload, no invalidation, expect ~100% hit ---
        verify_out="$OUTDIR/${tag}_verify.json"
        echo "[dispatch] verify invoke $tag"
        invoke_dispatch "$sg" "$n" "$verify_out"
        record_phase verify "$verify_out" "$sg" "$n"
    done
done

echo "[dispatch] done. results in $OUTDIR"
