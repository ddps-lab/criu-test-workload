#!/bin/bash
# E3 — Pre-warming iteration → hit ratio convergence.
#
# Runs the same cold warming payload N_WARMS times back-to-back (no
# invalidation between), then a final verify pass. Each call targets the
# same ranges so later iterations observe whichever chunks the POP has
# committed so far. This probes whether the POP's cache write lag or LRU
# churn can be brushed through by repeated pre-warming.
#
# Sequence per rep:
#   invalidate  → warm1 → warm2 → ... → warmN → verify
#
# warm1 is effectively cold. warm2..N measure "how much of the prior pass
# has settled into hit state". verify is a final check with no new writes.
#
# Usage:
#   REGION=eu-west-3 SIZE_GB=4 SHARDS=16 WARMS=4 REPEATS=3 \
#       bash e3_precache_iter.sh
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
: "${SIZE_GB:=4}"
: "${SHARDS:=16}"
: "${WARMS:=4}"
: "${REPEATS:=3}"
: "${CF_PRICE_PER_GB:=0.085}"
: "${LAMBDA_PRICE_PER_GBS:=0.0000166667}"

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/results/e3_$TS"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/summary.csv"
META="$OUTDIR/run_metadata.json"

cat > "$META" <<META_JSON
{
  "timestamp_utc": "$TS",
  "kind": "e3_precache_iter",
  "region": "$REGION",
  "function_name": "$FN_NAME",
  "distribution_domain": "$DISTRIBUTION_DOMAIN",
  "target_key": "$TARGET_KEY",
  "size_gb": $SIZE_GB,
  "shards": $SHARDS,
  "child_concurrency": $CHILD_CONCURRENCY,
  "warms": $WARMS,
  "repeats": $REPEATS,
  "lambda_memory_mb": $LAMBDA_MEMORY_MB
}
META_JSON

echo "rep,phase_idx,phase,wall_ms,thru_mb_s,hit_count,miss_count,pop_counts,cf_gb,cf_cost_usd,lambda_s,lambda_cost_usd,total_cost_usd" > "$CSV"

echo "[e3] ts=$TS outdir=$OUTDIR size=${SIZE_GB}GB shards=$SHARDS warms=$WARMS reps=$REPEATS"

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
    local out="$1"
    local payload resp
    payload=$(mktemp); resp=$(mktemp)
    python3 - "$payload" <<PY
import json, sys
out = sys.argv[1]
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
    "mode": "dispatch",
    "distribution_domain": distribution,
    "concurrency": $CHILD_CONCURRENCY,
    "shard_count": $SHARDS,
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
    local rep="$1" idx="$2" phase="$3" json="$4"
    python3 - "$json" "$rep" "$idx" "$phase" "$LAMBDA_MEMORY_MB" \
        "$CF_PRICE_PER_GB" "$LAMBDA_PRICE_PER_GBS" "$CSV" <<'PY'
import csv, json, sys
path, rep, idx, phase, mem_mb, cf_price, lam_price, csv_path = sys.argv[1:]
with open(path) as f:
    d = json.load(f)
total_bytes = d.get("total_bytes", 0)
hit = d.get("hit_count", 0)
miss = d.get("miss_count", 0)
wall_ms = d.get("wall_ms", 0.0)
pops = d.get("pop_counts") or {}
children = d.get("child_summaries") or []
child_ms = sum(c.get("wall_ms", 0.0) for c in children) + wall_ms
lam_s = child_ms / 1000.0
cf_gb = total_bytes / (1024**3)
cf_cost = cf_gb * float(cf_price)
lam_cost = (int(mem_mb) / 1024.0) * lam_s * float(lam_price)
total = cf_cost + lam_cost
thru = (total_bytes / (1024 * 1024)) / (wall_ms / 1000.0) if wall_ms > 0 else 0
with open(csv_path, "a") as f:
    w = csv.writer(f)
    w.writerow([
        rep, idx, phase, wall_ms, f"{thru:.1f}",
        hit, miss, json.dumps(pops, separators=(",", ":")),
        f"{cf_gb:.4f}", f"{cf_cost:.6f}",
        f"{lam_s:.2f}", f"{lam_cost:.6f}", f"{total:.6f}",
    ])
total_n = hit + miss
hr = (hit / total_n * 100) if total_n > 0 else 0
print(f"  rep={rep} {phase:<8}: wall={wall_ms/1000:.1f}s thru={thru:.1f}MB/s "
      f"hit={hit}/{total_n} ({hr:.1f}%) pops={list(pops.keys())}")
PY
}

for rep in $(seq 1 "$REPEATS"); do
    echo "[e3] === rep $rep/$REPEATS ==="
    inv=$(invalidate_key)
    echo "[e3] invalidation=$inv"
    for i in $(seq 1 "$WARMS"); do
        phase="warm${i}"
        out="$OUTDIR/rep${rep}_${phase}.json"
        invoke_dispatch "$out"
        record "$rep" "$i" "$phase" "$out"
    done
    verify_idx=$((WARMS + 1))
    out="$OUTDIR/rep${rep}_verify.json"
    invoke_dispatch "$out"
    record "$rep" "$verify_idx" "verify" "$out"
done

# Aggregate by phase across reps.
python3 - "$CSV" "$OUTDIR/aggregate.csv" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
groups = {}
for r in rows:
    groups.setdefault(r["phase"], []).append(r)
with open(sys.argv[2], "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["phase","n","wall_s_mean","wall_s_std",
                "thru_mb_s_mean","hit_mean","miss_mean","hit_pct_mean"])
    order = ["warm1","warm2","warm3","warm4","warm5","warm6","warm7","warm8","verify"]
    for phase in order:
        rs = groups.get(phase)
        if not rs:
            continue
        walls = [float(r["wall_ms"]) / 1000 for r in rs]
        thru = [float(r["thru_mb_s"]) for r in rs]
        hits = [int(r["hit_count"]) for r in rs]
        miss = [int(r["miss_count"]) for r in rs]
        pct = [h / (h + m) * 100 for h, m in zip(hits, miss) if (h + m) > 0]
        def _std(xs):
            return statistics.stdev(xs) if len(xs) > 1 else 0.0
        w.writerow([
            phase, len(rs),
            f"{statistics.mean(walls):.2f}", f"{_std(walls):.2f}",
            f"{statistics.mean(thru):.1f}",
            f"{statistics.mean(hits):.0f}", f"{statistics.mean(miss):.0f}",
            f"{statistics.mean(pct):.1f}",
        ])
print(f"aggregate -> {sys.argv[2]}")
PY

echo "[e3] done. results in $OUTDIR"
