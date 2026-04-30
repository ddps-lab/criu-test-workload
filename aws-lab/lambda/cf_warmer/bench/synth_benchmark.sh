#!/bin/bash
# Synthetic warming benchmark.
#
# Drives cf_warmer with a *constructed* IOV list of N chunks × 4 MiB each,
# all targeting the same physical S3 object (mc-16gb/pages-2.img, 17 GB).
# This isolates pagemap parsing from the warming throughput measurement: we
# can sweep total warm size (1 / 2 / 4 / 8 / 16 GB) and concurrency without
# needing a different real dump per size point.
#
# For each (size_gb, concurrency, repeat):
#   1. Invalidate the target S3 key at the CloudFront edge.
#   2. Wait for invalidation to complete.
#   3. Build an iov_override = N×4MB Range list and invoke cf_warmer.
#   4. Persist raw response + summary CSV with cost.
#
# Cost model (us-west-2 origin → eu-west-3 Paris edge, 2026-04):
#   CloudFront data-out (Europe, tier 1):  $0.085 / GB
#   Lambda x86_64:                         $0.0000166667 / (GB-s)
#   CloudFront invalidation:               first 1000 paths/month free
#
# Each run transfers `size_gb` from POP to Lambda. POP refills from origin
# only once per cold run (subsequent reps would hit cache; we invalidate
# between every rep for an apples-to-apples cold measurement).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${DISTRIBUTION_ID:=E1J7V6EO31JOOO}"
: "${DISTRIBUTION_DOMAIN:=deytbsxznbpj1.cloudfront.net}"
: "${BUCKET:=mhsong-criu-checkpoints}"
: "${TARGET_KEY:=memcached-16gb/pages-2.img}"
: "${REGION:=eu-west-3}"
: "${FN_NAME:=cf_warmer}"
: "${LAMBDA_MEMORY_MB:=3008}"
: "${CHUNK_BYTES:=4194304}"          # 4 MiB == criu xfer_len cap
: "${CF_PRICE_PER_GB:=0.085}"
: "${LAMBDA_PRICE_PER_GBS:=0.0000166667}"

SIZES_GB="${1:-${SIZES_GB:-1 2 4 8 16}}"
CONCURRENCIES="${2:-${CONCURRENCIES:-128}}"
REPEATS="${3:-${REPEATS:-1}}"
LAMBDA_MEMORIES="${LAMBDA_MEMORIES:-${LAMBDA_MEMORY_MB}}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/synth_$TS"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/summary.csv"
META="$OUTDIR/run_metadata.json"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "kind": "synthetic",
  "distribution_id": "$DISTRIBUTION_ID",
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "bucket": "$BUCKET",
  "target_key": "$TARGET_KEY",
  "region": "$REGION",
  "function_name": "$FN_NAME",
  "lambda_memory_mb": $LAMBDA_MEMORY_MB,
  "chunk_bytes": $CHUNK_BYTES,
  "sizes_gb": "$SIZES_GB",
  "concurrencies": "$CONCURRENCIES",
  "repeats": $REPEATS,
  "price_cf_out_eu_per_gb": $CF_PRICE_PER_GB,
  "price_lambda_per_gb_s": $LAMBDA_PRICE_PER_GBS
}
META_JSON

echo "size_gb,concurrency,repeat,phase,chunk_count,total_bytes,wall_ms,thru_mb_s,hit_count,miss_count,pop_counts,cf_gb,cf_cost_usd,lambda_s,lambda_cost_usd,total_cost_usd" > "$CSV"

echo "[synth] ts=$TS outdir=$OUTDIR target=$TARGET_KEY"
echo "[synth] sizes_gb=($SIZES_GB) concurrencies=($CONCURRENCIES) reps=$REPEATS"

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

invoke_one() {
    local size_gb="$1" concurrency="$2" out="$3"
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    python3 - "$size_gb" "$concurrency" "$payload" <<'PY'
import json, sys
size_gb, concurrency, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
size_bytes = int(float(size_gb) * (1024**3))
chunk = 4 * 1024 * 1024
import os
key = os.environ.get("TARGET_KEY", "memcached-16gb/pages-2.img")
distribution = os.environ["DISTRIBUTION_DOMAIN"]
chunks = []
off = 0
while off < size_bytes:
    length = min(chunk, size_bytes - off)
    chunks.append([key, off, length])
    off += length
payload = {
    "distribution_domain": distribution,
    "concurrency": concurrency,
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

export DISTRIBUTION_DOMAIN BUCKET TARGET_KEY

record_run() {
    # phase=cold|verify
    local phase="$1" json="$2" sg="$3" c="$4" rep="$5"
    python3 - "$json" "$sg" "$c" "$rep" "$LAMBDA_MEMORY_MB" \
        "$CF_PRICE_PER_GB" "$LAMBDA_PRICE_PER_GBS" "$CSV" "$phase" <<'PY'
import csv, json, sys
path, sg, c, rep, mem_mb, cf_price, lam_price, csv_path, phase = sys.argv[1:]
with open(path) as f:
    d = json.load(f)
iov = d.get("iov_count", 0)
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
thru = (total_bytes / (1024 * 1024)) / lam_s if lam_s > 0 else 0
with open(csv_path, "a") as f:
    w = csv.writer(f)
    w.writerow([
        sg, c, rep, phase, iov, total_bytes, wall_ms, f"{thru:.1f}",
        hit, miss, json.dumps(pops, separators=(",", ":")),
        f"{cf_gb:.4f}", f"{cf_cost:.6f}",
        f"{lam_s:.2f}", f"{lam_cost:.6f}", f"{total:.6f}",
    ])
print(f"  size={sg}GB c={c} rep={rep} {phase:<6}: chunks={iov} bytes={total_bytes/1e6:.0f}MB "
      f"wall={lam_s:.1f}s thru={thru:.1f}MB/s hit={hit} miss={miss} cost=${total:.4f}")
PY
}

for sg in $SIZES_GB; do
    for c in $CONCURRENCIES; do
        for rep in $(seq 1 "$REPEATS"); do
            tag="size${sg}gb_c${c}_rep${rep}"

            # --- cold pass: invalidate then warm ---
            echo "[synth] invalidate $TARGET_KEY for $tag (cold)"
            inv=$(invalidate_key)
            cold_out="$OUTDIR/${tag}_cold.json"
            invoke_one "$sg" "$c" "$cold_out"
            record_run cold "$cold_out" "$sg" "$c" "$rep"

            # --- verify pass: same payload, expect ~100% hit ---
            verify_out="$OUTDIR/${tag}_verify.json"
            invoke_one "$sg" "$c" "$verify_out"
            record_run verify "$verify_out" "$sg" "$c" "$rep"
        done
    done
done

# Per-(size,concurrency,phase) aggregate
python3 - "$CSV" "$OUTDIR/aggregate.csv" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
groups = {}
for r in rows:
    k = (float(r["size_gb"]), int(r["concurrency"]), r["phase"])
    groups.setdefault(k, []).append(r)
with open(sys.argv[2], "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["size_gb","concurrency","phase","n",
                "wall_s_mean","wall_s_std",
                "thru_mb_s_mean","thru_mb_s_std",
                "hit_mean","miss_mean",
                "cost_mean_usd"])
    for (sg, c, phase), rs in sorted(groups.items()):
        walls = [float(r["wall_ms"])/1000 for r in rs]
        thru = [float(r["thru_mb_s"]) for r in rs]
        costs = [float(r["total_cost_usd"]) for r in rs]
        hits = [int(r["hit_count"]) for r in rs]
        miss = [int(r["miss_count"]) for r in rs]
        def _std(xs):
            return statistics.stdev(xs) if len(xs) > 1 else 0.0
        w.writerow([
            sg, c, phase, len(rs),
            f"{statistics.mean(walls):.2f}", f"{_std(walls):.2f}",
            f"{statistics.mean(thru):.1f}", f"{_std(thru):.1f}",
            f"{statistics.mean(hits):.0f}", f"{statistics.mean(miss):.0f}",
            f"{statistics.mean(costs):.5f}",
        ])
print(f"aggregate -> {sys.argv[2]}")
PY

echo "[synth] done. results in $OUTDIR"
