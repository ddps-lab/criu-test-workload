#!/bin/bash
# launch_restore_fault.sh — orchestrator for the paired-protocol
# restore-fault campaign. Spawns one m5.8xlarge per workload, syncs the
# dev-VM criu_workload + criu binary, runs restore_fault_experiment.sh
# with the chunk-level tracker (AMI v8+), uploads paired data
# (faults.csv + dirty_profile.json with chunk_dirty[] + summary.json) to
# S3, self-terminates.
#
# Usage:
#   bash launch_restore_fault.sh matmul redis mc-8gb
#   bash launch_restore_fault.sh --all                   # all 6 paper wls
#
# Env:
#   AMI_ID (default v8), REGION (us-west-2), BUCKET, REPEAT, WARMUP_MIN,
#   PROFILE_MIN, CRIU_SRC.
set -e

AMI_ID="${AMI_ID:-ami-0f689eeba0a840177}"  # criu-workload-v9 (chunk tracker)
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
BUCKET="${BUCKET:-mhsong-criu-data-artifact}"
S3_PREFIX="${S3_PREFIX:-restore-fault-chunk}"
WARMUP_MIN="${WARMUP_MIN:-60}"
PROFILE_MIN="${PROFILE_MIN:-60}"

WORKLOADS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)  WORKLOADS=(matmul dataproc ml-training xgboost redis mc-8gb); shift ;;
        *)      WORKLOADS+=("$1"); shift ;;
    esac
done
[ ${#WORKLOADS[@]} -gt 0 ] || { echo "ERROR: provide workload(s) or --all"; exit 1; }

# Map surface name → (standalone-key, extra-args) for restore_fault_experiment.sh.
# Mirrors dump_all_workloads.sh so tracker workloads use identical params.
wl_resolve() {
    case "$1" in
        matmul)      RF_KEY=matmul;       RF_EXTRA="--matrix-size 25000" ;;
        dataproc)    RF_KEY=dataproc;     RF_EXTRA="--num-rows 17000000 --num-cols 60 --batch-size 1000" ;;
        ml-training) RF_KEY=ml_training;  RF_EXTRA="--model-size large --dataset-size 2000000" ;;
        xgboost)     RF_KEY=xgboost;      RF_EXTRA="--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3" ;;
        redis)       RF_KEY=redis;        RF_EXTRA="--record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-1gb)      RF_KEY=memcached;    RF_EXTRA="--memory-mb 1024 --record-count 773000 --ycsb-threads 4" ;;
        mc-4gb)      RF_KEY=memcached;    RF_EXTRA="--memory-mb 4096 --record-count 3100000 --ycsb-threads 4" ;;
        mc-8gb)      RF_KEY=memcached;    RF_EXTRA="--memory-mb 8192 --record-count 6200000 --ycsb-threads 4" ;;
        mc-16gb)     RF_KEY=memcached;    RF_EXTRA="--memory-mb 16384 --record-count 12400000 --ycsb-threads 4" ;;
        *) echo "ERROR: unknown workload '$1'"; return 1 ;;
    esac
}
[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu missing"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

echo "=========================================="
echo " restore-fault paired campaign"
echo " AMI: $AMI_ID  workloads: ${WORKLOADS[*]}"
echo " warmup: ${WARMUP_MIN}min  profile: ${PROFILE_MIN}min"
echo " S3: s3://$BUCKET/$S3_PREFIX/"
echo "=========================================="

for WL in "${WORKLOADS[@]}"; do
    wl_resolve "$WL" || continue
    INSTANCE_NAME="criu-restorefault-${WL}-$(date +%H%M%S)"
    echo ""
    echo "--- launching $WL  (key=$RF_KEY  args='$RF_EXTRA') ---"

    INSTANCE_ID=$(aws ec2 run-instances \
        --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
        --count 1 --key-name $KEY_NAME \
        --security-group-ids $SG --subnet-id $SUBNET \
        --iam-instance-profile Name=$IAM_PROFILE \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":200,"VolumeType":"gp3"}}]' \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME},{Key=Owner,Value=mhsong},{Key=Project,Value=criu-restore-fault-chunk},{Key=Workload,Value=$WL}]" \
        --instance-initiated-shutdown-behavior terminate \
        --query 'Instances[0].InstanceId' --output text)
    echo "  instance: $INSTANCE_ID"

    aws ec2 wait instance-running --region $REGION --instance-ids $INSTANCE_ID
    PUBIP=$(aws ec2 describe-instances --region $REGION --instance-ids $INSTANCE_ID \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

    # Wait for SSH
    for i in $(seq 1 30); do
        ssh -i $SSH_KEY -o ConnectTimeout=3 -o StrictHostKeyChecking=no ubuntu@$PUBIP echo ok 2>/dev/null && break
        sleep 5
    done

    # Sync code + criu binary
    rsync -azq -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        --exclude='__pycache__' --exclude='.git' --exclude='results' --exclude='hs_err_*' \
        /spot_kubernetes/criu_workload/ ubuntu@$PUBIP:/opt/criu_workload/ &
    rsync -azq -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        $CRIU_SRC/criu/criu ubuntu@$PUBIP:/tmp/criu &
    wait

    # Driver script (runs experiment as root, self-terminates on completion)
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$PUBIP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
export AWS_DEFAULT_REGION='${REGION}'

echo "=== install criu ==="
sudo install -m 0755 /tmp/criu /usr/local/sbin/criu
/usr/local/sbin/criu --version

sudo chown -R ubuntu:ubuntu /opt/criu_workload
sudo chmod +x /opt/criu_workload/experiments/*.sh 2>/dev/null || true

# Rebuild dirty_tracker — dev rsync may overwrite AMI prebuilt binary
# with a stale build (pre-04-30 had no chunk_dirty emit).
echo "=== rebuild dirty_tracker (chunk_dirty emit) ==="
(cd /opt/criu_workload/tools/dirty_tracker_c && make clean && make -j) 2>&1 | tail -3
strings /opt/criu_workload/tools/dirty_tracker_c/dirty_tracker | grep -q chunk_dirty \\
    && echo "  chunk_dirty emit: ON" \\
    || { echo "  ERROR: chunk_dirty NOT in tracker — abort"; sudo shutdown -h +1; exit 1; }

echo "=== [\$(date +%H:%M:%S)] restore-fault: ${WL} key=${RF_KEY} args='${RF_EXTRA}' (warmup=${WARMUP_MIN}min profile=${PROFILE_MIN}min) ==="
cd /opt/criu_workload/experiments
sudo -E bash restore_fault_experiment.sh \\
    --workload ${RF_KEY} \\
    --warmup-min ${WARMUP_MIN} --profile-min ${PROFILE_MIN} \\
    --output-base /home/ubuntu/restore_fault_runs \\
    --s3-prefix s3://${BUCKET}/${S3_PREFIX}/${WL}/ \\
    -- ${RF_EXTRA} \\
    > /tmp/restore_fault.log 2>&1
echo "=== [\$(date +%H:%M:%S)] experiment exit=\$? ==="

# Best-effort upload of orchestrator log (for failure diagnosis)
aws s3 cp /tmp/restore_fault.log s3://${BUCKET}/${S3_PREFIX}/${WL}/orchestrator.log 2>/dev/null || true

sudo shutdown -h +1
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$PUBIP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    "
    echo "  $WL launched on $PUBIP (will self-terminate)"
done

echo ""
echo "=========================================="
echo "All ${#WORKLOADS[@]} workload(s) launched."
echo "Track progress: tail -f /tmp/restore_fault.log on each instance."
echo "Results: s3://$BUCKET/$S3_PREFIX/<workload>/"
echo "=========================================="
