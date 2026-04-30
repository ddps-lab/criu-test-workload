#!/bin/bash
# launch_instance_scaling.sh — measure CRIU dump throughput on N instance
# types for the instance-scaling calibration campaign (issues/
# instance-scaling-experiment-proposal.md). Critically, dumps land at a
# scaling-specific S3 path so the existing baseline dumps at
# s3://$BUCKET/<workload>/ are NOT overwritten.
#
# Each instance:
#   1. Spawn one spot/on-demand instance of the given type from AMI v6
#   2. Bootstrap (install criu, sync workload tree)
#   3. Run dump_all_workloads.sh --both --repeat $REPEAT --workload mc-1gb
#      (and --workload mc-8gb, mc-16gb) with --prefix-base
#      "instance-scaling/<inst>/" so dumps land at
#      s3://$BUCKET/instance-scaling/<inst>/<workload>{,-compressed}/
#   4. Self-terminate
#
# Usage:
#   bash launch_instance_scaling.sh m5.xlarge                          # smoke
#   bash launch_instance_scaling.sh m5.xlarge mc-1gb                   # one workload
#   REPEAT=5 bash launch_instance_scaling.sh m5.xlarge m5.4xlarge \
#       m5.16xlarge r5.4xlarge c5.9xlarge m6i.8xlarge m6gd.8xlarge
#
# Default workloads if not specified: mc-1gb mc-8gb mc-16gb
#
# Env: REPEAT (default 5), AMI_ID, BUCKET, REGION

set -e

AMI_ID="${AMI_ID:-ami-0697e5cc271d1da64}"  # criu-workload-v6-20260425 (x86)
AMI_ID_ARM="${AMI_ID_ARM:-}"               # set when launching arm64 instance
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"
REPEAT="${REPEAT:-5}"

# Default workloads (small / medium / large for bandwidth scaling visibility)
DEFAULT_WORKLOADS="mc-1gb mc-8gb mc-16gb"

# Parse: instance types come first, then optional workload list after a `--`
INSTANCES=()
WORKLOADS_LIST=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workloads) WORKLOADS_LIST="$2"; shift 2 ;;
        --) shift; WORKLOADS_LIST="$*"; break ;;
        m[0-9]*|r[0-9]*|c[0-9]*|t[0-9]*|*xlarge|*metal)
            INSTANCES+=("$1"); shift ;;
        mc-*|matmul|dataproc|ml-training|xgboost|redis)
            # Workload tokens (allow mixed)
            WORKLOADS_LIST="${WORKLOADS_LIST:+$WORKLOADS_LIST }$1"; shift ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

WORKLOADS_LIST="${WORKLOADS_LIST:-$DEFAULT_WORKLOADS}"

[ ${#INSTANCES[@]} -gt 0 ] || { echo "ERROR: provide at least one instance type"; exit 1; }
[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu missing"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

echo "=========================================="
echo " Instance-scaling campaign"
echo " Instance types : ${INSTANCES[*]}"
echo " Workloads      : $WORKLOADS_LIST"
echo " Repeats        : $REPEAT"
echo " S3 path        : s3://$BUCKET/instance-scaling/<inst>/<wl>{,-compressed}/"
echo "  (existing dumps at s3://$BUCKET/<wl>/ will NOT be touched)"
echo "=========================================="
echo

# --- Launch ---
LAUNCH_LOG=/tmp/instance-scaling-launch.log
: > $LAUNCH_LOG

for INST in "${INSTANCES[@]}"; do
    # Pick AMI by arch — Graviton/ARM instances need arm64 AMI
    case "$INST" in
        *gd.*|*g.*|c8gd.*|t4g.*|m6gd.*|c7gd.*) ARCH=arm64 ;;
        *) ARCH=x86 ;;
    esac
    if [ "$ARCH" = "arm64" ]; then
        if [ -z "$AMI_ID_ARM" ]; then
            echo "[$INST] ARM64 — no AMI_ID_ARM set; using Ubuntu 24.04 arm64 default"
            AMI_TO_USE="ami-08f7157c7b8abcc0a"  # ubuntu noble arm64 (us-west-2)
        else
            AMI_TO_USE="$AMI_ID_ARM"
        fi
    else
        AMI_TO_USE="$AMI_ID"
    fi

    echo "[$INST] launching ($ARCH, AMI=$AMI_TO_USE)..."
    INSTANCE_ID=$(aws ec2 run-instances \
        --region $REGION --image-id $AMI_TO_USE --instance-type $INST \
        --count 1 --key-name $KEY_NAME \
        --security-group-ids $SG --subnet-id $SUBNET \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-scaling-${INST}}]" \
        --query 'Instances[0].InstanceId' --output text)
    echo "  $INST  $INSTANCE_ID" | tee -a $LAUNCH_LOG
done

# Wait running
ALL_IDS=$(awk '{print $2}' $LAUNCH_LOG | tr '\n' ' ')
echo "Waiting for instance-running..."
aws ec2 wait instance-running --region $REGION --instance-ids $ALL_IDS

declare -A IIDS IPS ARCHS
idx=0
while read -r INST IID; do
    IIDS[$INST]=$IID
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids $IID \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    IPS[$INST]=$IP
    case "$INST" in
        *gd.*|*g.*|c8gd.*|t4g.*|m6gd.*|c7gd.*) ARCHS[$INST]=arm64 ;;
        *) ARCHS[$INST]=x86 ;;
    esac
done < $LAUNCH_LOG

wait_ssh() {
    local ip=$1
    for i in $(seq 1 40); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

# --- Bootstrap each + run ---
for INST in "${INSTANCES[@]}"; do
    IP=${IPS[$INST]}
    IID=${IIDS[$INST]}
    ARCH=${ARCHS[$INST]}

    echo
    echo "=== $INST ($ARCH) on $IP ($IID) ==="
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=instance-scaling-${INST}" 2>/dev/null || true

    # Bootstrap SSH-to-self for paramiko
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q 2>/dev/null || true
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    # ARM instance: build criu binary on-the-fly is heavy. For now skip ARM
    # (will need separate ARM AMI build). Use the x86 AMI's pre-installed
    # criu if it exists, otherwise fail with a clear message.
    if [ "$ARCH" = "arm64" ]; then
        ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "which criu || sudo apt-get install -y criu" >/dev/null 2>&1
    else
        # x86: ship the same criu binary we built (matches AMI v6)
        echo "  uploading x86 criu binary..."
        scp -i $SSH_KEY -o StrictHostKeyChecking=no \
            "${CRIU_SRC}/criu/criu" ubuntu@$IP:/tmp/criu.scaling >/dev/null
    fi

    # Sync workload tree (rsync — same pattern as launch_dump.sh)
    echo "  syncing criu_workload tree..."
    rsync -a -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
        /spot_kubernetes/criu_workload/lib \
        /spot_kubernetes/criu_workload/workloads \
        /spot_kubernetes/criu_workload/experiments \
        /spot_kubernetes/criu_workload/config \
        /spot_kubernetes/criu_workload/tools \
        ubuntu@$IP:/tmp/criu_workload_sync/ >/dev/null

    # Driver
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'

echo "=== install criu ==="
if [ -f /tmp/criu.scaling ]; then
    sudo install -m 0755 /tmp/criu.scaling /usr/local/sbin/criu
fi
sudo /usr/local/sbin/criu --version || criu --version || true

echo "=== overlay criu_workload tree ==="
for d in lib workloads experiments config tools; do
    if [ -d /tmp/criu_workload_sync/\$d ]; then
        sudo rsync -a --chown=ubuntu:ubuntu /tmp/criu_workload_sync/\$d/ /opt/criu_workload/\$d/
    fi
done
sudo chmod +x /opt/criu_workload/experiments/*.sh 2>/dev/null || true
cd /opt/criu_workload

echo "=== [\$(date +%H:%M:%S)] DUMP CAMPAIGN: ${INST} ==="
echo "S3 base: s3://${BUCKET}/instance-scaling/${INST}/"
for wl in $WORKLOADS_LIST; do
    echo
    echo "--- raw: \$wl ($REPEAT reps) ---"
    bash experiments/dump_all_workloads.sh \\
        --bucket '${BUCKET}' --region '${REGION}' \\
        --workload "\$wl" \\
        --repeat ${REPEAT} \\
        --prefix-base "instance-scaling/${INST}" \\
        > /tmp/dump_\${wl}_raw.log 2>&1
    echo "raw exit: \$?"
    tail -5 /tmp/dump_\${wl}_raw.log

    echo
    echo "--- compressed: \$wl ($REPEAT reps) ---"
    bash experiments/dump_all_workloads.sh \\
        --bucket '${BUCKET}' --region '${REGION}' \\
        --workload "\$wl" \\
        --compress --repeat ${REPEAT} \\
        --prefix-base "instance-scaling/${INST}" \\
        > /tmp/dump_\${wl}_comp.log 2>&1
    echo "compressed exit: \$?"
    tail -5 /tmp/dump_\${wl}_comp.log
done

echo
echo "=== summary ==="
aws s3 ls s3://${BUCKET}/instance-scaling/${INST}/ --recursive --human-readable --summarize 2>&1 | tail -5

# Auto-terminate
echo "Auto-terminating ${IID}..."
aws ec2 terminate-instances --region ${REGION} --instance-ids ${IID} 2>&1 | tail -3
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "  $INST launched on $IP"
done

echo
echo "=========================================="
echo " ${#INSTANCES[@]} instance-scaling jobs launched (each auto-terminates)"
for INST in "${INSTANCES[@]}"; do
    echo "  $INST: ssh -i $SSH_KEY ubuntu@${IPS[$INST]} 'tail -f /tmp/driver.log'   # ${IIDS[$INST]}"
done
