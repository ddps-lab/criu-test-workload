#!/bin/bash
# Deploy cf_warmer Lambda.
#
# Idempotent: creates function + role on first run, updates code on
# subsequent runs. The function is deployed to the destination region (the
# whole point is that the warmer's outbound GETs originate from that region
# so CloudFront routes them to the nearby edge POP).
#
# Usage:
#   bash deploy.sh                       # defaults to eu-west-3
#   REGION=eu-west-1 bash deploy.sh
#
# Environment overrides:
#   REGION        default eu-west-3
#   FN_NAME       default cf_warmer
#   ROLE_NAME     default cf_warmer-lambda-role
#   MEMORY_MB     default 1024
#   TIMEOUT_S     default 900
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REGION="${REGION:-eu-west-3}"
FN_NAME="${FN_NAME:-cf_warmer}"
ROLE_NAME="${ROLE_NAME:-cf_warmer-lambda-role}"
# 3008 MB = ~1.75 vCPU + proportionally larger NIC burst budget.
# At 1024 MB we observed ~40 MB/s aggregate with concurrency=16 (limited by
# Lambda's per-function bandwidth allocation, not by CF or origin). 3008 MB
# should roughly triple throughput. Bump further to 10240 MB if still short.
MEMORY_MB="${MEMORY_MB:-3008}"
TIMEOUT_S="${TIMEOUT_S:-900}"
RUNTIME="python3.12"

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE_NAME}"

echo "[deploy] region=$REGION fn=$FN_NAME role=$ROLE_NAME account=$ACCOUNT"

# --- 1. Ensure IAM role exists ---------------------------------------------
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    echo "[deploy] creating IAM role $ROLE_NAME"
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "file://$HERE/iam/trust-policy.json" \
        >/dev/null
    aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "cf_warmer-inline" \
        --policy-document "file://$HERE/iam/permissions.json"
    # IAM is globally eventually consistent — wait for Lambda to see the role.
    echo "[deploy] waiting 10s for IAM propagation"
    sleep 10
else
    echo "[deploy] IAM role exists, re-syncing inline policy"
    aws iam put-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "cf_warmer-inline" \
        --policy-document "file://$HERE/iam/permissions.json"
fi

# --- 2. Build deployment zip ------------------------------------------------
BUILD_DIR="$HERE/.build"
ZIP_FILE="$HERE/.build/cf_warmer.zip"
rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR/pkg"
cp "$HERE/handler.py" "$BUILD_DIR/pkg/"
pip install --quiet --target "$BUILD_DIR/pkg" -r "$HERE/requirements.txt"
(cd "$BUILD_DIR/pkg" && zip -qr "$ZIP_FILE" .)
echo "[deploy] built $(du -h "$ZIP_FILE" | cut -f1) zip"

# --- 3. Create or update Lambda function ------------------------------------
if aws lambda get-function --function-name "$FN_NAME" --region "$REGION" \
     >/dev/null 2>&1; then
    echo "[deploy] updating function code + config"
    aws lambda update-function-code \
        --region "$REGION" \
        --function-name "$FN_NAME" \
        --zip-file "fileb://$ZIP_FILE" \
        --publish >/dev/null
    aws lambda wait function-updated \
        --region "$REGION" --function-name "$FN_NAME"
    aws lambda update-function-configuration \
        --region "$REGION" \
        --function-name "$FN_NAME" \
        --memory-size "$MEMORY_MB" \
        --timeout "$TIMEOUT_S" \
        --runtime "$RUNTIME" >/dev/null
else
    echo "[deploy] creating new function"
    aws lambda create-function \
        --region "$REGION" \
        --function-name "$FN_NAME" \
        --runtime "$RUNTIME" \
        --role "$ROLE_ARN" \
        --handler "handler.lambda_handler" \
        --memory-size "$MEMORY_MB" \
        --timeout "$TIMEOUT_S" \
        --zip-file "fileb://$ZIP_FILE" >/dev/null
fi

aws lambda wait function-updated --region "$REGION" --function-name "$FN_NAME"
echo "[deploy] done: $FN_NAME in $REGION"
