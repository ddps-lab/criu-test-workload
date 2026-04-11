#!/bin/bash
# Launch parallel restore experiments on multiple On-Demand instances
#
# Usage: bash launch_parallel_experiments.sh
#
# Launches 11 m5.8xlarge instances, assigns workloads, runs experiments,
# auto-terminates each instance when done. Results go to S3.

set -e

AMI_ID="ami-0b05e0817465be719"
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"

# AWS credentials (read from local config, never hardcoded)
AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)

if [ -z "$AWS_KEY" ] || [ -z "$AWS_SECRET" ]; then
    echo "ERROR: AWS credentials not configured"
    exit 1
fi

# ============================================================
# Experiment definitions: NAME|WORKLOAD|S3_PREFIX|EXTRA_ARGS
# ============================================================
EXPERIMENTS=(
    "matmul|matmul|matmul|--matrix-size 2048"
    "dataproc|dataproc|dataproc|--num-rows 1500000 --num-cols 60 --batch-size 1000"
    "ml-training|ml_training|ml-training|--model-size large --dataset-size 50000"
    "xgboost|xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
    "redis|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "memcached-11gb|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
    "sweep-1gb|memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4"
    "sweep-4gb|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"
    "sweep-8gb|memcached|memcached-8gb|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4"
    "sweep-11gb|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
    "sweep-16gb|memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4"
)

REPEAT=5

echo "=========================================="
echo " Parallel Experiment Launcher"
echo " Instances: ${#EXPERIMENTS[@]}"
echo " Type: $INSTANCE_TYPE (On-Demand)"
echo " AMI: $AMI_ID"
echo " Repeat: $REPEAT per mode"
echo "=========================================="

# ============================================================
# Launch all instances
# ============================================================
echo ""
echo "Launching ${#EXPERIMENTS[@]} instances..."

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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-parallel-exp}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"

# Wait for all running
echo "Waiting for instances to start..."
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

echo ""
echo "Instance IPs:"
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  $NAME: ${IPS[$i]}"
done

# ============================================================
# Setup SSH and launch experiments
# ============================================================
echo ""
echo "Setting up and launching experiments..."

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

    echo ""
    echo "--- Setting up $NAME on $IP ---"

    # Wait for SSH
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    # Pull latest code and setup SSH keys
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

    # Tag instance with experiment name
    IID=$(echo $INSTANCE_IDS | awk "{print \$((i+1))}")
    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=$NAME" 2>/dev/null || true

    # Launch experiment via nohup
    echo "  Launching: $WORKLOAD ($S3_PREFIX) repeat=$REPEAT"
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        export AWS_ACCESS_KEY_ID='${AWS_KEY}'
        export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
        cd /opt/criu_workload
        nohup bash experiments/run_restore_experiment.sh \
            --workload $WORKLOAD \
            --s3-prefix $S3_PREFIX \
            --repeat $REPEAT \
            --extra-args '$EXTRA_ARGS' \
            --auto-terminate \
            > /tmp/experiment.log 2>&1 &
        disown
        echo 'PID:' \$!
    " 2>&1

    echo "  $NAME started on $IP"
done

# ============================================================
# Summary
# ============================================================
echo ""
echo "=========================================="
echo " All ${#EXPERIMENTS[@]} experiments launched!"
echo "=========================================="
echo ""
echo "Monitor progress:"
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/experiment.log'  # $NAME"
done
echo ""
echo "Results will be in: s3://mhsong-criu-results/"
echo "Instances auto-terminate when done."
echo ""
echo "Check running instances:"
echo "  aws ec2 describe-instances --region $REGION --filters 'Name=tag:Name,Values=criu-parallel-exp' --query 'Reservations[*].Instances[*].{ID:InstanceId,State:State.Name,Exp:Tags[?Key==\`Experiment\`].Value|[0]}' --output table"
