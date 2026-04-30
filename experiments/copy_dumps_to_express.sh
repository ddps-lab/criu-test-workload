#!/bin/bash
# One-time copy of existing dumps from S3 standard bucket to S3 Express
# One Zone bucket. Run this BEFORE launch_storage_sweep.sh --backend s3-express.
#
# Usage:
#   EXPRESS_BUCKET=mhsong-criu-express--usw2-az1--x-s3 \
#     bash copy_dumps_to_express.sh
#
# Express buckets must be created in advance via AWS console / CLI in the
# same Availability Zone as the experiment instances. Bucket name must
# follow the directory-bucket convention `name--zone-id--x-s3`.

set -e

SRC_BUCKET="${SRC_BUCKET:-mhsong-criu-checkpoints}"
DST_BUCKET="${EXPRESS_BUCKET:?EXPRESS_BUCKET env required (e.g., mhsong-criu-express--usw2-az1--x-s3)}"
REGION="${EXPRESS_REGION:-us-west-2}"
ENDPOINT="${EXPRESS_ENDPOINT:-https://s3express-usw2-az1.us-west-2.amazonaws.com}"

# Storage sweep workloads (matches launch_storage_sweep.sh ALL_EXPERIMENTS).
# Override via PREFIXES env var (space-separated) to copy a subset.
if [ -n "$PREFIXES_OVERRIDE" ]; then
    read -r -a PREFIXES <<< "$PREFIXES_OVERRIDE"
else
    PREFIXES=(
        matmul
        dataproc
        ml-training
        xgboost
        redis
        memcached     # mc-11gb
    )
fi

echo "Copying dumps from s3://$SRC_BUCKET/ → s3://$DST_BUCKET/"
echo "  endpoint: $ENDPOINT"
echo "  region:   $REGION"
echo

STAGE_DIR="${STAGE_DIR:-/tmp/express_stage}"
mkdir -p "$STAGE_DIR"

for p in "${PREFIXES[@]}"; do
    echo "=== $p ==="
    rm -rf "$STAGE_DIR/$p"
    mkdir -p "$STAGE_DIR/$p"

    # Step 1: download standard bucket → local stage (uses standard endpoint).
    aws s3 sync "s3://$SRC_BUCKET/$p/" "$STAGE_DIR/$p/" \
        --region "$REGION" --quiet

    # Step 2: upload local stage → express bucket (uses express endpoint).
    aws s3 cp "$STAGE_DIR/$p/" "s3://$DST_BUCKET/$p/" \
        --recursive --region "$REGION" --endpoint-url "$ENDPOINT" --quiet

    rm -rf "$STAGE_DIR/$p"

    count=$(aws s3api list-objects-v2 --bucket "$DST_BUCKET" --prefix "$p/" \
                --region "$REGION" --endpoint-url "$ENDPOINT" \
                --query 'KeyCount' --output text 2>/dev/null)
    echo "  $p: $count objects in Express bucket"
done

rmdir "$STAGE_DIR" 2>/dev/null || true

echo
echo "Done. Express bucket ready for storage sweep."
