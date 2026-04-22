#!/bin/bash
# launch_compress_ablation.sh — EC2 driver for the compress ablation.
#
# Per workload (mc4gb / mc16gb):
#   1) spin up an m5.8xlarge
#   2) scp over the phase6-compression criu binary
#   3) scp a driver that:
#        a) installs the new criu
#        b) runs baseline_experiment.py --compress-pages to create the
#           compressed S3 dump at <S3_PREFIX>-compressed/
#        c) runs run_restore_experiment.sh on both the existing raw prefix
#           and the new compressed prefix
#        d) uploads logs + auto-terminates the instance
#
# Usage:
#   bash launch_compress_ablation.sh                  # mc4gb + mc16gb
#   bash launch_compress_ablation.sh mc4gb            # one workload
#   REPEAT=3 bash launch_compress_ablation.sh mc4gb   # override repeat
#
# Requirements:
#   - Local criu-s3 (phase6-compression) built: criu/criu exists
#   - aws cli with mhsong-criu-* permissions, us-west-2
#   - SSH key at $HOME/.ssh/mhsong-ddps-oregon.pem

set -e

AMI_ID="${AMI_ID:-ami-0fd8cddbe746f93aa}"    # v4 AMI (kernel 6.8)
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
REPEAT="${REPEAT:-3}"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"

[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu not built"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

# NAME | WORKLOAD | S3_PREFIX | EXTRA_ARGS
ALL=(
    "mc4gb|memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4"
    "mc16gb|memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4"
)

if [ $# -gt 0 ]; then
    EXPERIMENTS=()
    for want in "$@"; do
        for entry in "${ALL[@]}"; do
            name="${entry%%|*}"
            if [ "$name" = "$want" ]; then
                EXPERIMENTS+=("$entry")
                break
            fi
        done
    done
    [ ${#EXPERIMENTS[@]} -gt 0 ] || { echo "ERROR: no matching workloads"; exit 1; }
else
    EXPERIMENTS=("${ALL[@]}")
fi

echo "=========================================="
echo " AMI: $AMI_ID"
echo " Type: $INSTANCE_TYPE"
echo " Repeat: $REPEAT"
echo " Workloads: ${#EXPERIMENTS[@]}"
for e in "${EXPERIMENTS[@]}"; do echo "   $e"; done
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
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-compress-ablation}]" \
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

for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    IFS='|' read -r NAME WORKLOAD S3_PREFIX EXTRA_ARGS <<< "${EXPERIMENTS[$i]}"
    IP=${IPS[$i]}
    IID=${IIDS[$i]}
    COMP_PREFIX="${S3_PREFIX}-compressed"

    echo ""
    echo "--- $NAME on $IP ($IID) ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=${NAME}-compress" 2>/dev/null || true

    # Bootstrap SSH + ssh-to-self.
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q
        ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa -q <<< y >/dev/null 2>&1 || true
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts 2>/dev/null
        DEST_IP=\$(hostname -I | awk '{print \$1}')
        ssh-keyscan -H \$DEST_IP >> ~/.ssh/known_hosts 2>/dev/null
        cp ~/.ssh/id_rsa ~/.ssh/id_ed25519 2>/dev/null || true
    " 2>/dev/null

    # scp the freshly-built criu binary from the local dev VM.
    echo "   uploading criu binary..."
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        "${CRIU_SRC}/criu/criu" ubuntu@$IP:/tmp/criu.phase6-compression >/dev/null
    # Also ship libcriu (dynamic dep of crit / tools) just in case.
    if [ -e "${CRIU_SRC}/lib/c/libcriu.so" ]; then
        scp -i $SSH_KEY -o StrictHostKeyChecking=no \
            "${CRIU_SRC}/lib/c/libcriu.so" ubuntu@$IP:/tmp/libcriu.so >/dev/null 2>&1 || true
    fi

    # scp patched criu_workload files (compress-pages flag is not in origin/main yet)
    echo "   uploading criu_workload patches..."
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        /spot_kubernetes/criu_workload/experiments/baseline_experiment.py \
        ubuntu@$IP:/tmp/baseline_experiment.py >/dev/null
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        /spot_kubernetes/criu_workload/lib/checkpoint.py \
        ubuntu@$IP:/tmp/checkpoint.py >/dev/null
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        /spot_kubernetes/criu_workload/lib/criu_utils.py \
        ubuntu@$IP:/tmp/criu_utils.py >/dev/null

    # Driver script body.
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
cd /opt/criu_workload
DEST_PRIV=\$(hostname -I | awk '{print \$1}')

echo "=== [\$(date +%H:%M:%S)] ${NAME}: installing phase6-compression criu ==="
sudo install -m 0755 /tmp/criu.phase6-compression /usr/local/sbin/criu
/usr/local/sbin/criu --version

# Overlay the criu_workload patches that add --compress-pages.
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/baseline_experiment.py experiments/baseline_experiment.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/checkpoint.py                lib/checkpoint.py
sudo install -m 0644 -o ubuntu -g ubuntu /tmp/criu_utils.py                lib/criu_utils.py

# Sanity: --compress flag must be recognized.
if ! /usr/local/sbin/criu dump --help 2>&1 | grep -q -- '--compress'; then
    echo "ERROR: installed criu does not support --compress"
    exit 1
fi
if ! python3 -u experiments/baseline_experiment.py --help 2>&1 | grep -q -- '--compress-pages'; then
    echo "ERROR: baseline_experiment.py missing --compress-pages"
    exit 1
fi

echo "=== [\$(date +%H:%M:%S)] ${NAME}: compressed dump -> s3://mhsong-criu-checkpoints/${COMP_PREFIX}/ ==="
aws s3 rm s3://mhsong-criu-checkpoints/${COMP_PREFIX}/ --recursive --region us-west-2 --quiet

sudo -E python3 -u experiments/baseline_experiment.py \\
    --config config/experiments/memcached_lazy_prefetch.yaml \\
    --source-ip 127.0.0.1 --dest-ip \$DEST_PRIV \\
    --ssh-user ubuntu --workload ${WORKLOAD} \\
    --lazy-mode lazy-prefetch \\
    --wait-before-dump 120 --duration 86400 \\
    --readiness-timeout 900 \\
    ${EXTRA_ARGS} \\
    --s3-type standard --s3-upload-bucket mhsong-criu-checkpoints \\
    --s3-region us-west-2 \\
    --s3-download-endpoint https://s3.us-west-2.amazonaws.com \\
    --s3-access-key '${AWS_KEY}' --s3-secret-key '${AWS_SECRET}' \\
    --s3-prefix ${COMP_PREFIX} \\
    --s3-direct-upload \\
    --track-dirty-pages \\
    --dirty-tracker c \\
    --dirty-track-interval 5000 \\
    --compress-pages --compress-workers 8 \\
    > /tmp/dump_compressed.log 2>&1
echo "Compressed dump exit: \$?"
aws s3 ls s3://mhsong-criu-checkpoints/${COMP_PREFIX}/ --region us-west-2 --human-readable --summarize | tail -5

echo "=== [\$(date +%H:%M:%S)] ${NAME}: ablation on RAW prefix (${S3_PREFIX}) ==="
bash experiments/run_restore_experiment.sh \\
    --workload ${WORKLOAD} \\
    --s3-prefix ${S3_PREFIX} \\
    --repeat ${REPEAT} \\
    --extra-args '${EXTRA_ARGS}' \\
    --s3-results-suffix raw \\
    > /tmp/ablation_raw.log 2>&1

echo "=== [\$(date +%H:%M:%S)] ${NAME}: ablation on COMPRESSED prefix (${COMP_PREFIX}) ==="
bash experiments/run_restore_experiment.sh \\
    --workload ${WORKLOAD} \\
    --s3-prefix ${COMP_PREFIX} \\
    --repeat ${REPEAT} \\
    --extra-args '${EXTRA_ARGS}' \\
    --s3-results-suffix compressed \\
    --auto-terminate \\
    > /tmp/ablation_compressed.log 2>&1

echo "=== [\$(date +%H:%M:%S)] ${NAME}: done ==="
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "   $NAME launched on $IP"
done

echo ""
echo "=========================================="
echo " ${#EXPERIMENTS[@]} experiments launched"
echo "=========================================="
for i in $(seq 0 $((${#EXPERIMENTS[@]} - 1))); do
    NAME=$(echo "${EXPERIMENTS[$i]}" | cut -d'|' -f1)
    echo "  $NAME : ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/driver.log'   # ${IIDS[$i]}"
done
