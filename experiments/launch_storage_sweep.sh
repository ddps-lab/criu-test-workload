#!/bin/bash
# Storage backend sweep launcher.
#
# Spawns one EC2 instance per workload, runs ablation against a chosen
# storage backend (S3 standard / S3 Express One Zone / CloudFront) using
# the dump that already exists in s3://mhsong-criu-checkpoints/, then
# auto-terminates.
#
# Usage:
#   bash launch_storage_sweep.sh --backend s3-express
#   bash launch_storage_sweep.sh --backend cloudfront
#   bash launch_storage_sweep.sh --backend s3-standard      # control rerun
#
# Optional:
#   --workloads "redis xgboost"   # subset; default = all 6
#   REPEAT=3 bash launch_storage_sweep.sh --backend s3-express
#
# Required env / config:
#   For s3-express: EXPRESS_BUCKET (e.g., mhsong-criu-express--usw2-az1--x-s3)
#                  EXPRESS_REGION  (default: us-west-2)
#                  EXPRESS_ENDPOINT (e.g., s3express-usw2-az1.us-west-2.amazonaws.com)
#   For cloudfront: CF_DISTRIBUTION (e.g., d1abcd1234.cloudfront.net)
#
# This script does NOT re-dump. The checkpoint at
# s3://mhsong-criu-checkpoints/<prefix>/ is reused. For S3 Express we
# expect a one-time copy to have already populated EXPRESS_BUCKET with the
# same prefix layout (use copy_to_express.sh).

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
REPEAT="${REPEAT:-5}"
BACKEND=""
WORKLOAD_FILTER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend)   BACKEND="$2"; shift 2 ;;
        --workloads) WORKLOAD_FILTER="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [ -z "$BACKEND" ]; then
    echo "ERROR: --backend {s3-standard|s3-express|cloudfront} required"
    exit 1
fi

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -z "$AWS_KEY" ] && { echo "AWS credentials not configured"; exit 1; }

# Backend-specific endpoint, bucket, s3-type
case "$BACKEND" in
    s3-standard)
        S3_TYPE="standard"
        S3_BUCKET="mhsong-criu-checkpoints"
        S3_ENDPOINT="https://s3.us-west-2.amazonaws.com"
        S3_REGION="us-west-2"
        ;;
    s3-express)
        S3_TYPE="express-one-zone"
        S3_BUCKET="${EXPRESS_BUCKET:-mhsong-criu-express--usw2-az1--x-s3}"
        S3_REGION="${EXPRESS_REGION:-us-west-2}"
        S3_ENDPOINT="${EXPRESS_ENDPOINT:-https://s3express-usw2-az1.us-west-2.amazonaws.com}"
        ;;
    cloudfront)
        S3_TYPE="cloudfront"
        S3_BUCKET="${CF_BUCKET:-mhsong-criu-checkpoints}"
        S3_REGION="us-west-2"
        S3_ENDPOINT="${CF_DISTRIBUTION:?CF_DISTRIBUTION env var required for cloudfront backend}"
        case "$S3_ENDPOINT" in https://*) ;; *) S3_ENDPOINT="https://$S3_ENDPOINT" ;; esac
        ;;
    *) echo "ERROR: backend '$BACKEND' not recognized"; exit 1 ;;
esac

# 6 workload subset for storage sweep (excludes mc-1/4/8/16gb).
ALL_EXPERIMENTS=(
    "matmul|matmul|matmul|--matrix-size 2048"
    "dataproc|dataproc|dataproc|--num-rows 1500000 --num-cols 60 --batch-size 1000"
    "ml-training|ml_training|ml-training|--model-size large --dataset-size 50000"
    "xgboost|xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
    "redis|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "mc-11gb|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
)

if [ -n "$WORKLOAD_FILTER" ]; then
    EXPERIMENTS=()
    for want in $WORKLOAD_FILTER; do
        for entry in "${ALL_EXPERIMENTS[@]}"; do
            n="${entry%%|*}"
            [ "$n" = "$want" ] && EXPERIMENTS+=("$entry")
        done
    done
else
    EXPERIMENTS=("${ALL_EXPERIMENTS[@]}")
fi
[ ${#EXPERIMENTS[@]} -eq 0 ] && { echo "no matching workloads"; exit 1; }

echo "=========================================="
echo " Storage Backend Sweep"
echo " Backend:  $BACKEND"
echo " Bucket:   $S3_BUCKET"
echo " Endpoint: $S3_ENDPOINT"
echo " AMI:      $AMI_ID"
echo " Repeat:   $REPEAT"
echo " Workloads:${#EXPERIMENTS[@]}"
echo "=========================================="

INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count ${#EXPERIMENTS[@]} \
    --key-name $KEY_NAME --security-group-ids $SG \
    --subnet-id $SUBNET --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-storage-${BACKEND}}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS
echo "All running."

declare -a IPS IIDS
idx=0
for iid in $INSTANCE_IDS; do
    IIDS[$idx]=$iid
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids $iid \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    IPS[$idx]=$IP
    idx=$((idx + 1))
done

wait_ssh() {
    local ip=$1
    for i in $(seq 1 30); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    IFS='|' read -r NAME WORKLOAD S3_PREFIX EXTRA_ARGS <<< "${EXPERIMENTS[$i]}"
    IP=${IPS[$i]}
    IID=${IIDS[$i]}

    echo ""
    echo "--- $NAME ($BACKEND) on $IP ($IID) ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=${NAME}-${BACKEND}" 2>/dev/null || true

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        DEST_IP=\$(hostname -I | awk '{print \$1}')
        ssh-keyscan -H \$DEST_IP >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    # Per-instance driver. NO dump phase: dump is reused from S3 standard
    # bucket (or already mirrored to express bucket / fronted by CloudFront).
    # The driver does:
    #   1) backend-specific cache warmup (5 rounds of GET on the prefix)
    #   2) run_restore_experiment.sh against the chosen backend endpoint
    #   3) auto-terminate
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
cd /opt/criu_workload

BACKEND="${BACKEND}"
S3_TYPE="${S3_TYPE}"
S3_BUCKET="${S3_BUCKET}"
S3_REGION="${S3_REGION}"
S3_ENDPOINT="${S3_ENDPOINT}"
S3_PREFIX="${S3_PREFIX}"

echo "=== [\$(date +%H:%M:%S)] $NAME (\$BACKEND): ablation (4 modes x ${REPEAT}) ==="
# run_restore_experiment.sh handles backend-specific cache warmup itself
# based on --s3-type (standard / express-one-zone / cloudfront).
bash experiments/run_restore_experiment.sh \\
    --workload $WORKLOAD \\
    --s3-prefix \$S3_PREFIX \\
    --repeat $REPEAT \\
    --extra-args '$EXTRA_ARGS' \\
    --s3-bucket \$S3_BUCKET \\
    --s3-region \$S3_REGION \\
    --s3-endpoint \$S3_ENDPOINT \\
    --s3-type \$S3_TYPE \\
    --s3-results-suffix \$BACKEND \\
    --auto-terminate \\
    > /tmp/ablation.log 2>&1

echo "=== [\$(date +%H:%M:%S)] $NAME (\$BACKEND): done ==="
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "  $NAME launched"
done

echo ""
echo "=========================================="
echo " ${#EXPERIMENTS[@]} storage-sweep instances launched (${BACKEND})"
echo "=========================================="
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  $NAME : ssh ubuntu@${IPS[$i]} 'tail -f /tmp/driver.log'   # ${IIDS[$i]}"
done
