#!/bin/bash
# launch_s3_bench.sh — spawn EC2 instances to measure per-connection S3
# Range-GET throughput. Runs bench_s3_range.py against an existing dump
# object (pages-*.img) to isolate S3 throughput R(N) from the CRIU
# prefetch pipeline.
#
# Each instance:
#   1. Installs python3-requests (idempotent on the v4 AMI)
#   2. Runs bench_s3_range.py across worker counts {1,2,4,8,12,16,20,24,32}
#   3. Uploads the resulting JSON to
#      s3://$BUCKET/bench-s3-range/<instance-type>/<timestamp>.json
#   4. Self-terminates via IMDSv2 + aws ec2 terminate-instances
#
# Usage:
#   bash launch_s3_bench.sh                         # m5.8xlarge + m5.large
#   bash launch_s3_bench.sh --only m5.large         # single instance type
#
# Env: BUCKET (default mhsong-criu-checkpoints), REGION (default us-west-2),
#      KEY (default memcached-16gb-compressed/pages-1.img),
#      RANGE_MB (default 4), DURATION (default 30)
set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="${REGION:-us-west-2}"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
KEY_OBJ="${KEY:-memcached-16gb-compressed/pages-2.img}"
RANGE_MB="${RANGE_MB:-4}"
DURATION="${DURATION:-30}"
WORKERS_CSV="${WORKERS_CSV:-1,2,4,8,12,16,20,24,32}"

INSTANCE_TYPES=("m5.8xlarge" "m5.large")

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only) INSTANCE_TYPES=("$2"); shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

SCRIPT_SRC="/spot_kubernetes/criu_workload/tools/bench_s3_range.py"
LIBCURL_SRC="/spot_kubernetes/criu_workload/tools/bench_s3_libcurl.c"
[ -f "$SCRIPT_SRC" ] || { echo "ERROR: $SCRIPT_SRC not found"; exit 1; }
[ -f "$LIBCURL_SRC" ] || { echo "ERROR: $LIBCURL_SRC not found"; exit 1; }

echo "=========================================="
echo " Bench: s3://$BUCKET/$KEY_OBJ ($REGION)"
echo " range=${RANGE_MB}MB duration=${DURATION}s workers=$WORKERS_CSV"
echo " Instance types: ${INSTANCE_TYPES[*]}"
echo "=========================================="

launch_one() {
    local INSTANCE_TYPE="$1"
    echo ""
    echo "--- spawning $INSTANCE_TYPE ---"
    local IID IP
    IID=$(aws ec2 run-instances \
        --region $REGION --image-id $AMI_ID --instance-type "$INSTANCE_TYPE" \
        --count 1 --key-name $KEY_NAME \
        --security-group-ids $SG --subnet-id $SUBNET \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bench-s3-${INSTANCE_TYPE}}]" \
        --query 'Instances[0].InstanceId' --output text)
    aws ec2 wait instance-running --region $REGION --instance-ids "$IID"
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids "$IID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    echo "  $INSTANCE_TYPE → $IID @ $IP"

    # SSH readiness
    for i in $(seq 1 40); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no \
            ubuntu@$IP "echo ok" 2>/dev/null && break
        sleep 5
    done

    scp -i $SSH_KEY -o StrictHostKeyChecking=no "$SCRIPT_SRC" \
        ubuntu@$IP:/tmp/bench_s3_range.py >/dev/null
    scp -i $SSH_KEY -o StrictHostKeyChecking=no "$LIBCURL_SRC" \
        ubuntu@$IP:/tmp/bench_s3_libcurl.c >/dev/null

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'

echo "=== [\$(date +%H:%M:%S)] install deps ==="
sudo apt-get update -qq
sudo apt-get install -y -qq python3-requests libcurl4-openssl-dev build-essential

echo "=== [\$(date +%H:%M:%S)] build libcurl bench ==="
gcc -O2 -o /tmp/bench_s3_libcurl /tmp/bench_s3_libcurl.c -lcurl -lpthread
echo "Build exit: \$?"
ls -la /tmp/bench_s3_libcurl

TS=\$(date -u +%Y%m%dT%H%M%SZ)

# --- Python bench (comparison) ---
OUT_PY=/tmp/bench_s3_py_${INSTANCE_TYPE}_\$TS.json
LOG_PY=/tmp/bench_s3_py_${INSTANCE_TYPE}_\$TS.log
echo "=== [\$(date +%H:%M:%S)] Python bench ==="
python3 /tmp/bench_s3_range.py \\
    --bucket '${BUCKET}' --key '${KEY_OBJ}' --region '${REGION}' \\
    --range-mb ${RANGE_MB} --duration ${DURATION} \\
    --workers '${WORKERS_CSV}' \\
    --label '${INSTANCE_TYPE}' \\
    --output \$OUT_PY 2>&1 | tee \$LOG_PY

# --- libcurl bench (apples-to-apples with CRIU) ---
OUT_LC=/tmp/bench_s3_lc_${INSTANCE_TYPE}_\$TS.json
LOG_LC=/tmp/bench_s3_lc_${INSTANCE_TYPE}_\$TS.log

echo "=== [\$(date +%H:%M:%S)] libcurl bench ==="
URL=\$(aws s3 presign s3://'${BUCKET}'/'${KEY_OBJ}' --expires-in 7200 --region '${REGION}')
OBJSIZE=\$(aws s3api head-object --bucket '${BUCKET}' --key '${KEY_OBJ}' --region '${REGION}' --query ContentLength --output text)
RANGE_BYTES=\$(( ${RANGE_MB} * 1024 * 1024 ))
WORKERS="${WORKERS_CSV//,/ }"

echo "libcurl bench: obj=\$OBJSIZE B, range=\$RANGE_BYTES B, workers=[\$WORKERS]" | tee \$LOG_LC
echo "[" > \$OUT_LC
first=1
for N in \$WORKERS; do
    if [ \$first -eq 1 ]; then first=0; else echo "," >> \$OUT_LC; fi
    # warmup=5, duration=${DURATION}
    /tmp/bench_s3_libcurl "\$URL" "\$OBJSIZE" "\$RANGE_BYTES" ${DURATION} 5 \$N 2>> \$LOG_LC >> \$OUT_LC
done
echo "]" >> \$OUT_LC
# Wrap as outer object with metadata
python3 -c "
import json
rows = json.load(open('\$OUT_LC'))
meta = {
    'label': '${INSTANCE_TYPE}',
    'bucket': '${BUCKET}', 'key': '${KEY_OBJ}', 'region': '${REGION}',
    'object_size': \$OBJSIZE, 'range_mb': ${RANGE_MB}, 'duration_s': ${DURATION},
    'rows': rows,
}
json.dump(meta, open('\$OUT_LC','w'), indent=2)
"
cat \$OUT_LC | tail -60

echo "=== [\$(date +%H:%M:%S)] upload results ==="
for f in \$OUT_PY \$LOG_PY \$OUT_LC \$LOG_LC; do
    [ -f "\$f" ] && aws s3 cp "\$f" "s3://${BUCKET}/bench-s3-range/${INSTANCE_TYPE}/\$(basename \$f)" \\
        --region ${REGION} --only-show-errors
done

echo "=== [\$(date +%H:%M:%S)] terminating ==="
TOKEN=\$(curl -sX PUT http://169.254.169.254/latest/api/token \\
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
INSTANCE_ID=\$(curl -s -H "X-aws-ec2-metadata-token: \$TOKEN" \\
    http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids \$INSTANCE_ID --region ${REGION}
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "  launched $INSTANCE_TYPE: ssh -i $SSH_KEY ubuntu@$IP 'tail -f /tmp/driver.log'  # $IID"
}

for t in "${INSTANCE_TYPES[@]}"; do
    launch_one "$t"
done

echo ""
echo "=========================================="
echo " All instances launched. Results will appear at:"
echo "   s3://$BUCKET/bench-s3-range/<instance-type>/<timestamp>.{json,log}"
echo "=========================================="
