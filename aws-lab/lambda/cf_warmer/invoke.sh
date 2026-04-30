#!/bin/bash
# Invoke cf_warmer Lambda for a given S3 prefix.
#
# Usage:
#   bash invoke.sh matmul
#   bash invoke.sh memcached-16gb
#   REGION=eu-west-1 DISTRIBUTION=other.cloudfront.net bash invoke.sh matmul
#
# Prints the full handler response JSON to stdout. Summary (hit/miss/POP)
# is surfaced to stderr for quick eyeballing.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REGION="${REGION:-eu-west-3}"
FN_NAME="${FN_NAME:-cf_warmer}"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
DISTRIBUTION="${DISTRIBUTION:-deytbsxznbpj1.cloudfront.net}"
CONCURRENCY="${CONCURRENCY:-16}"

PREFIX="${1:?usage: invoke.sh <s3-prefix>}"
PAYLOAD=$(mktemp)
RESP=$(mktemp)
trap 'rm -f "$PAYLOAD" "$RESP"' EXIT

cat > "$PAYLOAD" <<JSON
{
  "distribution_domain": "$DISTRIBUTION",
  "bucket": "$BUCKET",
  "prefix": "$PREFIX",
  "concurrency": $CONCURRENCY
}
JSON

# IMPORTANT: --cli-read-timeout 0 disables client-side read timeout so a
# cold warm that takes 4+ min doesn't trigger a retry. --cli-connect-timeout
# stays short. AWS_MAX_ATTEMPTS=1 disables the SDK retry loop so we don't
# silently double-bill the Lambda on a single slow invocation.
AWS_MAX_ATTEMPTS=1 aws lambda invoke \
    --region "$REGION" \
    --function-name "$FN_NAME" \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 0 \
    --cli-connect-timeout 30 \
    --payload "file://$PAYLOAD" \
    --log-type Tail \
    "$RESP" \
    --query 'StatusCode' --output text >&2

# Emit the full handler JSON on stdout so callers can parse.
cat "$RESP"

# Short summary on stderr for humans.
python3 - "$RESP" >&2 <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
iov = d.get("iov_count", 0)
meta = d.get("meta_count", 0)
print(f"[cf_warmer] iov={iov} meta={meta} "
      f"hit={d['hit_count']} miss={d['miss_count']} "
      f"bytes={d['total_bytes']} "
      f"wall_ms={d['wall_ms']} pops={d.get('pop_counts',{})}",
      file=sys.stderr)
PY
