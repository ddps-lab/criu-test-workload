#!/bin/bash
# E1 — Single-Lambda memory × concurrency grid benchmark.
#
# Sweeps the (memory, concurrency) grid for a single Lambda instance on a
# fixed synthetic workload (1 GiB × 4 MiB chunks targeting the same object
# so POP routing stays consistent). Redeploys the function when memory
# changes (amortises deploy overhead across all concurrency points at that
# memory level).
#
# For each (mem, conc, rep) cell: invalidate → cold → verify → verify2.
# verify2 runs immediately after verify with no sleep to probe POP commit
# lag (does the hit ratio go up when we ask a third time?).
#
# Usage:
#   REGION=us-west-2 bash e1_memconc_grid.sh "1024 1769 3008 5120 7168 10240" \
#                                            "16 24 32 48 64 96" \
#                                            3
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
: "${DISTRIBUTION_ID:=E1J7V6EO31JOOO}"
: "${DISTRIBUTION_DOMAIN:=deytbsxznbpj1.cloudfront.net}"
: "${BUCKET:=mhsong-criu-checkpoints}"
: "${TARGET_KEY:=memcached-16gb/pages-2.img}"
: "${REGION:=us-west-2}"
: "${FN_NAME:=cf_warmer}"
: "${SIZE_GB:=1}"
: "${CF_PRICE_PER_GB:=0.085}"
: "${LAMBDA_PRICE_PER_GBS:=0.0000166667}"

MEMORIES="${1:-${MEMORIES:-1024 1769 3008 5120 7168 10240}}"
CONCURRENCIES="${2:-${CONCURRENCIES:-16 24 32 48 64 96}}"
REPEATS="${3:-${REPEATS:-3}}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/e1_$TS"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/summary.csv"
META="$OUTDIR/run_metadata.json"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "kind": "e1_memconc_grid",
  "region": "$REGION",
  "function_name": "$FN_NAME",
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "target_key": "$TARGET_KEY",
  "size_gb": $SIZE_GB,
  "memories": "$MEMORIES",
  "concurrencies": "$CONCURRENCIES",
  "repeats": $REPEATS,
  "phases": "cold verify verify2"
}
META_JSON

echo "memory_mb,concurrency,repeat,phase,chunk_count,total_bytes,wall_ms,thru_mb_s,hit_count,miss_count,pop_counts,cf_gb,cf_cost_usd,lambda_s,lambda_cost_usd,total_cost_usd" > "$CSV"

echo "[e1] ts=$TS outdir=$OUTDIR target=$TARGET_KEY size=${SIZE_GB}GB"
echo "[e1] memories=($MEMORIES) concurrencies=($CONCURRENCIES) reps=$REPEATS"

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

redeploy() {
    local mem="$1"
    echo "[e1] redeploy mem=${mem}MB in $REGION"
    REGION="$REGION" FN_NAME="$FN_NAME" MEMORY_MB="$mem" \
        bash "$HERE/../deploy.sh" 2>&1 | tail -2
}

invoke_one() {
    local conc="$1" out="$2"
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    python3 - "$conc" "$payload" <<PY
import json, os, sys
conc = int(sys.argv[1])
out = sys.argv[2]
size_bytes = int(float("$SIZE_GB") * (1024**3))
chunk = 4 * 1024 * 1024
key = "$TARGET_KEY"
distribution = "$DISTRIBUTION_DOMAIN"
chunks = []
off = 0
while off < size_bytes:
    length = min(chunk, size_bytes - off)
    chunks.append([key, off, length])
    off += length
payload = {
    "distribution_domain": distribution,
    "concurrency": conc,
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

record() {
    local phase="$1" json="$2" mem="$3" conc="$4" rep="$5"
    python3 - "$json" "$mem" "$conc" "$rep" "$phase" \
        "$CF_PRICE_PER_GB" "$LAMBDA_PRICE_PER_GBS" "$CSV" <<'PY'
import csv, json, sys
path, mem, c, rep, phase, cf_price, lam_price, csv_path = sys.argv[1:]
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
lam_cost = (int(mem) / 1024.0) * lam_s * float(lam_price)
total = cf_cost + lam_cost
thru = (total_bytes / (1024 * 1024)) / lam_s if lam_s > 0 else 0
with open(csv_path, "a") as f:
    w = csv.writer(f)
    w.writerow([
        mem, c, rep, phase, iov, total_bytes, wall_ms, f"{thru:.1f}",
        hit, miss, json.dumps(pops, separators=(",", ":")),
        f"{cf_gb:.4f}", f"{cf_cost:.6f}",
        f"{lam_s:.2f}", f"{lam_cost:.6f}", f"{total:.6f}",
    ])
print(f"  mem={mem}MB c={c} rep={rep} {phase:<7}: bytes={total_bytes/1e6:.0f}MB "
      f"wall={lam_s:.1f}s thru={thru:.1f}MB/s hit={hit} miss={miss}")
PY
}

for mem in $MEMORIES; do
    redeploy "$mem"
    for c in $CONCURRENCIES; do
        for rep in $(seq 1 "$REPEATS"); do
            tag="mem${mem}_c${c}_rep${rep}"
            inv=$(invalidate_key)
            echo "[e1] $tag inv=$inv"
            cold_out="$OUTDIR/${tag}_cold.json"
            invoke_one "$c" "$cold_out"
            record cold "$cold_out" "$mem" "$c" "$rep"
            verify_out="$OUTDIR/${tag}_verify.json"
            invoke_one "$c" "$verify_out"
            record verify "$verify_out" "$mem" "$c" "$rep"
            verify2_out="$OUTDIR/${tag}_verify2.json"
            invoke_one "$c" "$verify2_out"
            record verify2 "$verify2_out" "$mem" "$c" "$rep"
        done
    done
done

# Per-(mem, conc, phase) aggregate.
python3 - "$CSV" "$OUTDIR/aggregate.csv" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
groups = {}
for r in rows:
    k = (int(r["memory_mb"]), int(r["concurrency"]), r["phase"])
    groups.setdefault(k, []).append(r)
with open(sys.argv[2], "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["memory_mb","concurrency","phase","n",
                "wall_s_mean","wall_s_std",
                "thru_mb_s_mean","thru_mb_s_std",
                "hit_mean","miss_mean","cost_mean_usd"])
    for (mem, c, phase), rs in sorted(groups.items()):
        walls = [float(r["wall_ms"]) / 1000 for r in rs]
        thru = [float(r["thru_mb_s"]) for r in rs]
        hits = [int(r["hit_count"]) for r in rs]
        miss = [int(r["miss_count"]) for r in rs]
        costs = [float(r["total_cost_usd"]) for r in rs]
        def _std(xs):
            return statistics.stdev(xs) if len(xs) > 1 else 0.0
        w.writerow([
            mem, c, phase, len(rs),
            f"{statistics.mean(walls):.2f}", f"{_std(walls):.2f}",
            f"{statistics.mean(thru):.1f}", f"{_std(thru):.1f}",
            f"{statistics.mean(hits):.0f}", f"{statistics.mean(miss):.0f}",
            f"{statistics.mean(costs):.5f}",
        ])
print(f"aggregate -> {sys.argv[2]}")
PY

echo "[e1] done. results in $OUTDIR"
