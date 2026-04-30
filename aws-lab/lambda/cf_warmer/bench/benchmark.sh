#!/bin/bash
# Reproducible cf_warmer benchmark harness.
#
# For each (workload, concurrency) point, this script:
#   1. Creates a CloudFront cache invalidation for the workload's prefix
#      (guarantees cold edge state) and waits for it to propagate.
#   2. Invokes the cf_warmer Lambda with the given concurrency.
#   3. Persists the raw handler JSON response as
#      results/<ts>/<workload>_<C>_rep<N>.json
#   4. Appends a summary row to results/<ts>/summary.csv with throughput,
#      hit/miss count, CloudFront egress GB, estimated Lambda + CF cost.
#
# Usage:
#   bash benchmark.sh matmul,ml-training "16 64 128" 3
#   WORKLOADS="matmul ml-training" CONCURRENCIES="16 64 128" REPEATS=3 bash benchmark.sh
#
# Cost model (us-west-2 origin → eu-west-3 Paris edge, 2026-04):
#   - CloudFront data-out (Europe, tier 1):  $0.085 / GB
#   - Lambda (arm64 is cheaper but we use x86_64): $0.0000166667 / (GB-s)
#   - CloudFront invalidation: first 1000 paths/month free, then $0.005/path
#
# We treat each workload prefix as 1 invalidation path (wildcard /<prefix>/*).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
: "${DISTRIBUTION_ID:=E1J7V6EO31JOOO}"
: "${DISTRIBUTION_DOMAIN:=deytbsxznbpj1.cloudfront.net}"
: "${BUCKET:=mhsong-criu-checkpoints}"
: "${REGION:=eu-west-3}"
: "${FN_NAME:=cf_warmer}"
: "${LAMBDA_MEMORY_MB:=3008}"      # must match deployed memory
: "${CF_PRICE_PER_GB:=0.085}"
: "${LAMBDA_PRICE_PER_GBS:=0.0000166667}"
: "${INVALIDATION_PRICE_PER_PATH:=0}"  # first 1000 free

WORKLOADS_RAW="${1:-${WORKLOADS:-matmul ml-training dataproc}}"
CONCURRENCIES_RAW="${2:-${CONCURRENCIES:-16 64 128}}"
REPEATS="${3:-${REPEATS:-3}}"

# Accept comma or space separation
WORKLOADS=$(echo "$WORKLOADS_RAW" | tr ',' ' ')
CONCURRENCIES=$(echo "$CONCURRENCIES_RAW" | tr ',' ' ')

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/$TS"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/summary.csv"
META="$OUTDIR/run_metadata.json"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "distribution_id": "$DISTRIBUTION_ID",
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "bucket": "$BUCKET",
  "region": "$REGION",
  "function_name": "$FN_NAME",
  "lambda_memory_mb": $LAMBDA_MEMORY_MB,
  "workloads": "$WORKLOADS",
  "concurrencies": "$CONCURRENCIES",
  "repeats": $REPEATS,
  "price_cf_out_eu_per_gb": $CF_PRICE_PER_GB,
  "price_lambda_per_gb_s": $LAMBDA_PRICE_PER_GBS
}
META_JSON

echo "workload,concurrency,repeat,iov_count,meta_count,total_bytes,hit_count,miss_count,wall_ms,pop_counts,cf_gb,cf_cost_usd,lambda_s,lambda_cost_usd,total_cost_usd" > "$CSV"

echo "[bench] ts=$TS outdir=$OUTDIR"
echo "[bench] workloads=($WORKLOADS) concurrencies=($CONCURRENCIES) repeats=$REPEATS lambda=${LAMBDA_MEMORY_MB}MB"

invalidate() {
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

invoke_one() {
    local workload="$1" concurrency="$2" out="$3"
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    cat > "$payload" <<JSON
{
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "bucket": "$BUCKET",
  "prefix": "$workload",
  "concurrency": $concurrency
}
JSON
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

for wl in $WORKLOADS; do
    for c in $CONCURRENCIES; do
        for rep in $(seq 1 "$REPEATS"); do
            tag="${wl}_c${c}_rep${rep}"
            echo "[bench] invalidate $wl for $tag"
            inv=$(invalidate "$wl")
            echo "[bench] invalidation=$inv"
            outfile="$OUTDIR/${tag}.json"
            echo "[bench] invoke $tag"
            invoke_one "$wl" "$c" "$outfile"
            python3 - "$outfile" "$wl" "$c" "$rep" "$LAMBDA_MEMORY_MB" \
                "$CF_PRICE_PER_GB" "$LAMBDA_PRICE_PER_GBS" "$CSV" <<'PY'
import csv, json, sys
path, wl, c, rep, mem_mb, cf_price, lam_price, csv_path = sys.argv[1:]
with open(path) as f:
    d = json.load(f)
iov = d.get("iov_count", 0)
meta = d.get("meta_count", 0)
total_bytes = d.get("total_bytes", 0)
hit = d.get("hit_count", 0)
miss = d.get("miss_count", 0)
wall_ms = d.get("wall_ms", 0.0)
pops = d.get("pop_counts") or {}
cf_gb = total_bytes / (1024**3)
cf_cost = cf_gb * float(cf_price)
lam_s = wall_ms / 1000.0
lam_cost = (int(mem_mb) / 1024.0) * lam_s * float(lam_price)
total = cf_cost + lam_cost
with open(csv_path, "a") as f:
    w = csv.writer(f)
    w.writerow([
        wl, c, rep, iov, meta, total_bytes, hit, miss, wall_ms,
        json.dumps(pops, separators=(",", ":")),
        f"{cf_gb:.4f}", f"{cf_cost:.6f}",
        f"{lam_s:.2f}", f"{lam_cost:.6f}", f"{total:.6f}",
    ])
print(f"  {wl} c={c} rep={rep}: bytes={total_bytes/1e6:.1f}MB "
      f"wall={lam_s:.1f}s thru={(total_bytes/1e6)/lam_s:.1f}MB/s "
      f"hit={hit} miss={miss} cost=${total:.4f}")
PY
        done
    done
done

# Aggregate + print
python3 - "$CSV" "$OUTDIR/aggregate.csv" <<'PY'
import csv, statistics
rows = list(csv.DictReader(open(__import__('sys').argv[1])))
groups = {}
for r in rows:
    k = (r["workload"], r["concurrency"])
    groups.setdefault(k, []).append(r)
with open(__import__('sys').argv[2], "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["workload","concurrency","n","bytes_mb",
                "wall_s_mean","wall_s_std",
                "thru_mb_s_mean","thru_mb_s_std",
                "hit_mean","miss_mean","cost_mean_usd"])
    for (wl, c), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], int(kv[0][1]))):
        walls = [float(r["wall_ms"])/1000 for r in rs]
        bytes_mb = float(rs[0]["total_bytes"])/1e6
        thru = [bytes_mb/w for w in walls if w > 0]
        hits = [int(r["hit_count"]) for r in rs]
        miss = [int(r["miss_count"]) for r in rs]
        costs = [float(r["total_cost_usd"]) for r in rs]
        def _std(xs):
            return statistics.stdev(xs) if len(xs) > 1 else 0.0
        w.writerow([
            wl, c, len(rs), f"{bytes_mb:.1f}",
            f"{statistics.mean(walls):.2f}", f"{_std(walls):.2f}",
            f"{statistics.mean(thru):.1f}", f"{_std(thru):.1f}",
            f"{statistics.mean(hits):.0f}", f"{statistics.mean(miss):.0f}",
            f"{statistics.mean(costs):.5f}",
        ])
print(f"aggregate -> {__import__('sys').argv[2]}")
PY

echo "[bench] done. results in $OUTDIR"
