#!/bin/bash
# Re-run failed experiments:
# - redis, memcached-1gb/4gb/8gb/16gb: baseline only (lazy results exist)
# - memcached-11gb: all 4 modes
#
# Usage: bash launch_rerun.sh

AMI_ID="ami-0b05e0817465be719"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

# NAME|WORKLOAD|S3_PREFIX|EXTRA_ARGS|MODE (empty=all, "1_baseline"=baseline only)
EXPERIMENTS=(
    "redis-bl|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a|1_baseline"
    "mc1gb-bl|memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4|1_baseline"
    "mc4gb-bl|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4|1_baseline"
    "mc8gb-bl|memcached|memcached-8gb|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4|1_baseline"
    "mc16gb-bl|memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4|1_baseline"
    "mc11gb-all|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4|"
)

REPEAT=5

echo "=========================================="
echo " Re-run: ${#EXPERIMENTS[@]} experiments"
echo "=========================================="

# Launch instances
INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION \
    --image-id $AMI_ID \
    --instance-type $INSTANCE_TYPE \
    --count ${#EXPERIMENTS[@]} \
    --key-name $KEY_NAME \
    --security-group-ids $SG \
    --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-rerun}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS
echo "All running."

# Get IPs
declare -a IPS
idx=0
for iid in $INSTANCE_IDS; do
    IP=$(aws ec2 describe-instances --region $REGION --instance-ids $iid \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    IPS[$idx]=$IP
    idx=$((idx + 1))
done

# Setup and launch
wait_ssh() {
    local ip=$1
    for i in $(seq 1 30); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$ip "echo ok" 2>/dev/null && return 0
        sleep 5
    done
    return 1
}

for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    IFS='|' read -r NAME WORKLOAD S3_PREFIX EXTRA_ARGS MODE <<< "${EXPERIMENTS[$i]}"
    IP=${IPS[$i]}

    echo ""
    echo "--- $NAME on $IP ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    # Setup
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q 2>/dev/null
    " 2>/dev/null
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        DEST_IP=\$(hostname -I | awk '{print \$1}')
        ssh-keyscan -H \$DEST_IP >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    # Build command
    MODE_ARG=""
    if [ -n "$MODE" ]; then
        MODE_ARG="--mode $MODE"
    fi

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        export AWS_ACCESS_KEY_ID='${AWS_KEY}'
        export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
        cd /opt/criu_workload
        nohup bash experiments/run_restore_experiment.sh \
            --workload $WORKLOAD \
            --s3-prefix $S3_PREFIX \
            --repeat $REPEAT \
            --extra-args '$EXTRA_ARGS' \
            $MODE_ARG \
            --auto-terminate \
            > /tmp/experiment.log 2>&1 &
        disown
        echo 'PID:' \$!
    " 2>&1

    echo "  $NAME started"
done

echo ""
echo "=========================================="
echo " All ${#EXPERIMENTS[@]} re-runs launched!"
echo "=========================================="
echo ""
echo "Monitor:"
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/experiment.log'  # $NAME"
done
echo ""
echo "Instances auto-terminate when done."
