#!/bin/bash
# launch_dump.sh — EC2 driver for dump creation only. Wraps
# dump_all_workloads.sh (which runs on the source EC2 instance).
#
# For each (workload, mode) pair this spins up one m5.8xlarge, ships the
# dev-VM criu binary and the patched criu_workload files, runs
# dump_all_workloads.sh with the right flags, then auto-terminates.
# Dumps land at s3://$BUCKET/<workload-prefix>[-compressed]/ and can be
# reused by any later ablation launcher — no need to re-dump per run.
#
# Usage:
#   bash launch_dump.sh mc-4gb mc-16gb              # raw dumps only
#   bash launch_dump.sh --compress mc-4gb mc-16gb   # compressed dumps only
#   bash launch_dump.sh --both mc-4gb               # raw + compressed (two instances)
#
# Env:
#   AMI_ID (default v4 kernel-6.8 AMI), CRIU_SRC (default dev path), SSH_KEY
set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"

MODE_RAW=0
MODE_COMP=0
WORKLOADS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --compress) MODE_COMP=1; shift ;;
        --raw)      MODE_RAW=1; shift ;;
        --both)     MODE_RAW=1; MODE_COMP=1; shift ;;
        *)          WORKLOADS+=("$1"); shift ;;
    esac
done

# Default to raw if neither specified
if [ "$MODE_RAW" = "0" ] && [ "$MODE_COMP" = "0" ]; then
    MODE_RAW=1
fi

[ ${#WORKLOADS[@]} -gt 0 ] || { echo "ERROR: provide at least one workload (e.g. mc-4gb)"; exit 1; }
[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu not built"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

# Build the (workload, mode) launch matrix
MATRIX=()
for w in "${WORKLOADS[@]}"; do
    [ "$MODE_RAW" = "1" ]  && MATRIX+=("$w|raw")
    [ "$MODE_COMP" = "1" ] && MATRIX+=("$w|compress")
done

N=${#MATRIX[@]}
echo "=========================================="
echo " criu: ${CRIU_SRC}/criu/criu ($(${CRIU_SRC}/criu/criu --version 2>&1 | grep -i gitid))"
echo " Dumps to make: $N"
for e in "${MATRIX[@]}"; do echo "   $e"; done
echo "=========================================="

INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count $N --key-name $KEY_NAME \
    --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-dump}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS

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
    for i in $(seq 1 40); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

for i in $(seq 0 $((N - 1))); do
    IFS='|' read -r WL MODE <<< "${MATRIX[$i]}"
    IP=${IPS[$i]}
    IID=${IIDS[$i]}
    COMPRESS_FLAG=""
    [ "$MODE" = "compress" ] && COMPRESS_FLAG="--compress"

    echo ""
    echo "--- $WL ($MODE) on $IP ($IID) ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=dump-${WL}-${MODE}" 2>/dev/null || true

    # Bootstrap SSH-to-self (dirty tracker needs sudo -u ubuntu ssh 127.0.0.1)
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    echo "   uploading criu binary..."
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        "${CRIU_SRC}/criu/criu" ubuntu@$IP:/tmp/criu.phase6-compression >/dev/null

    echo "   uploading criu_workload patches..."
    for f in experiments/baseline_experiment.py lib/checkpoint.py lib/criu_utils.py \
             experiments/dump_all_workloads.sh; do
        scp -i $SSH_KEY -o StrictHostKeyChecking=no \
            "/spot_kubernetes/criu_workload/$f" \
            ubuntu@$IP:/tmp/$(basename $f) >/dev/null
    done

    # Driver: install criu + patches, then run dump_all_workloads.sh
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'

echo "=== install criu ==="
sudo install -m 0755 /tmp/criu.phase6-compression /usr/local/sbin/criu
/usr/local/sbin/criu --version

echo "=== overlay criu_workload patches ==="
cd /opt/criu_workload
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/baseline_experiment.py  experiments/baseline_experiment.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/checkpoint.py           lib/checkpoint.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/criu_utils.py           lib/criu_utils.py
sudo install -m 0755 -o ubuntu -g ubuntu /tmp/dump_all_workloads.sh   experiments/dump_all_workloads.sh

echo "=== [\$(date +%H:%M:%S)] dump: ${WL} ${MODE} ==="
bash experiments/dump_all_workloads.sh \\
    --bucket '${BUCKET}' --region '${REGION}' \\
    --workload '${WL}' \\
    ${COMPRESS_FLAG} \\
    > /tmp/dump.log 2>&1
echo "Dump exit: \$?"

aws s3 ls s3://${BUCKET}/${WL}${COMPRESS_FLAG:+-compressed}/ --region ${REGION} --human-readable --summarize | tail -5 || true

echo "=== [\$(date +%H:%M:%S)] DONE — self-terminating ==="
INSTANCE_ID=\$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids \$INSTANCE_ID --region ${REGION}
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "   $WL $MODE launched on $IP"
done

echo ""
echo "=========================================="
echo " $N dumps launched (auto-terminate)"
for i in $(seq 0 $((N - 1))); do
    echo "  ${MATRIX[$i]} : ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/driver.log'   # ${IIDS[$i]}"
done
