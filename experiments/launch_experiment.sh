#!/bin/bash
# Unified experiment launcher.
#
# Spawns one EC2 instance per workload, runs full dump + 4-mode ablation
# (1_baseline / 3_semi_sync / 4_async / 5_full), repeats `--repeat` times,
# uploads results to S3, then auto-terminates the instance.
#
# Usage:
#   bash launch_experiment.sh                           # all 10 workloads
#   bash launch_experiment.sh redis matmul               # only listed
#   REPEAT=3 bash launch_experiment.sh                  # override repeat count
#
# Required:
#   - aws cli configured with mhsong-criu-* permissions
#   - SSH key at $HOME/.ssh/mhsong-ddps-oregon.pem
#   - latest criu_workload pushed to ddps-lab/criu-test-workload main

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"   # v4: kernel 6.8 + iptables-legacy + latest fixes
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
REPEAT="${REPEAT:-5}"

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
if [ -z "$AWS_KEY" ] || [ -z "$AWS_SECRET" ]; then
    echo "ERROR: AWS credentials not configured"
    exit 1
fi

# NAME|WORKLOAD|S3_PREFIX|EXTRA_ARGS
# Memory sweep keeps mc-11gb on its own instance (paper baseline).
ALL_EXPERIMENTS=(
    "matmul|matmul|matmul|--matrix-size 2048"
    "dataproc|dataproc|dataproc|--num-rows 1500000 --num-cols 60 --batch-size 1000"
    "ml-training|ml_training|ml-training|--model-size large --dataset-size 50000"
    "xgboost|xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3"
    "redis|redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a"
    "mc1gb|memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4"
    "mc4gb|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"
    "mc8gb|memcached|memcached-8gb|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4"
    "mc11gb|memcached|memcached|--memcached-memory 11264 --record-count 8500000 --ycsb-threads 4"
    "mc16gb|memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4"
)

# Filter by user-supplied names if any.
if [ $# -gt 0 ]; then
    EXPERIMENTS=()
    for want in "$@"; do
        for entry in "${ALL_EXPERIMENTS[@]}"; do
            name="${entry%%|*}"
            if [ "$name" = "$want" ]; then
                EXPERIMENTS+=("$entry")
                break
            fi
        done
    done
    if [ ${#EXPERIMENTS[@]} -eq 0 ]; then
        echo "ERROR: no matching workloads from '$@'"
        exit 1
    fi
else
    EXPERIMENTS=("${ALL_EXPERIMENTS[@]}")
fi

echo "=========================================="
echo " AMI: $AMI_ID"
echo " Type: $INSTANCE_TYPE"
echo " Repeat: $REPEAT"
echo " Workloads: ${#EXPERIMENTS[@]}"
echo "=========================================="

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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-experiment-v4}]" \
    --query 'Instances[*].InstanceId' --output text)

echo "Instances: $INSTANCE_IDS"
aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_IDS
echo "All running."

declare -a IPS
declare -a IIDS
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
    echo "--- $NAME on $IP ($IID) ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=$NAME" 2>/dev/null || true

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        DEST_IP=\$(hostname -I | awk '{print \$1}')
        ssh-keyscan -H \$DEST_IP >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    # Build the per-instance driver script. The driver does:
    #   1) wipe stale S3 dump for this prefix
    #   2) baseline_experiment.py for the dump phase (uploads to S3)
    #   3) run_restore_experiment.sh for the 4-mode ablation
    #   4) auto-terminate instance
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
cd /opt/criu_workload
DEST_PRIV=\$(hostname -I | awk '{print \$1}')

echo "=== [\$(date +%H:%M:%S)] $NAME: wipe old S3 dump ==="
aws s3 rm s3://mhsong-criu-checkpoints/${S3_PREFIX}/ --recursive --region us-west-2 --quiet

echo "=== [\$(date +%H:%M:%S)] $NAME: dump (--wait-before-dump 120 --duration 86400) ==="
sudo -E python3 -u experiments/baseline_experiment.py \\
    --config config/experiments/memcached_lazy_prefetch.yaml \\
    --source-ip 127.0.0.1 --dest-ip \$DEST_PRIV \\
    --ssh-user ubuntu --workload $WORKLOAD \\
    --lazy-mode lazy-prefetch \\
    --wait-before-dump 120 --duration 86400 \\
    $EXTRA_ARGS \\
    --s3-type standard --s3-upload-bucket mhsong-criu-checkpoints \\
    --s3-region us-west-2 \\
    --s3-download-endpoint https://s3.us-west-2.amazonaws.com \\
    --s3-access-key '${AWS_KEY}' --s3-secret-key '${AWS_SECRET}' \\
    --s3-prefix ${S3_PREFIX} \\
    --s3-direct-upload \\
    > /tmp/dump.log 2>&1
DUMP_RC=\$?
echo "Dump exit: \$DUMP_RC"
echo "tcp-stream count: \$(aws s3 ls s3://mhsong-criu-checkpoints/${S3_PREFIX}/ --region us-west-2 | grep -c tcp-stream)"

echo "=== [\$(date +%H:%M:%S)] $NAME: ablation (4 modes x ${REPEAT}) ==="
bash experiments/run_restore_experiment.sh \\
    --workload $WORKLOAD \\
    --s3-prefix ${S3_PREFIX} \\
    --repeat $REPEAT \\
    --extra-args '$EXTRA_ARGS' \\
    --auto-terminate \\
    > /tmp/ablation.log 2>&1
echo "=== [\$(date +%H:%M:%S)] $NAME: done ==="
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "  $NAME launched on $IP"
done

echo ""
echo "=========================================="
echo " ${#EXPERIMENTS[@]} experiments launched"
echo "=========================================="
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  $NAME : ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/driver.log'   # ${IIDS[$i]}"
done
echo ""
echo "Instances auto-terminate at end of ablation."
echo "Results: s3://mhsong-criu-results/<prefix>/<timestamp>/"
