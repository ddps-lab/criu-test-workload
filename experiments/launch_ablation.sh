#!/bin/bash
# launch_ablation.sh — run restore ablation against pre-existing S3 dumps.
# Assumes the raw and/or compressed dumps already exist in S3 (produced
# earlier by launch_dump.sh). Spins up one m5.8xlarge per (workload, mode)
# and runs run_restore_experiment.sh for the full 5-mode ablation.
#
# Usage:
#   bash launch_ablation.sh mc-4gb mc-16gb                          # raw prefix only
#   bash launch_ablation.sh --compress mc-4gb mc-16gb               # compressed prefix only
#   bash launch_ablation.sh --both mc-4gb mc-16gb                   # raw + compressed
#   bash launch_ablation.sh --both --workers 16,24 mc-4gb mc-16gb   # sweep 16 and 24 prefetch workers
#
# --workers accepts comma-separated list. One EC2 instance spawned per
# (workload, mode, worker-count) triple. Results uploaded with suffix
# "${mode}-w${N}" so sweeps coexist.
#
# Env: REPEAT (default 3), BUCKET, REGION, CRIU_SRC, MODES (e.g. "4_async 5_full")
set -e

AMI_ID="${AMI_ID:-ami-0697e5cc271d1da64}"  # criu-workload-v6-20260425
INSTANCE_TYPE="m5.8xlarge"
KEY_NAME="mhsong-ddps-oregon"
SG="sg-0eb08e8fa10cb3031"
SUBNET="subnet-09c8aacd484cac3e2"
IAM_PROFILE="mhsong-ec2-admin"
REGION="us-west-2"
SSH_KEY="$HOME/.ssh/mhsong-ddps-oregon.pem"
REPEAT="${REPEAT:-3}"
CRIU_SRC="${CRIU_SRC:-/spot_kubernetes/criu_build/criu-s3}"
BUCKET="${BUCKET:-mhsong-criu-checkpoints}"

MODE_RAW=0
MODE_COMP=0
WORKLOADS=()
WORKERS_CSV=""  # empty = default (12), matches run_restore_experiment.sh default

while [[ $# -gt 0 ]]; do
    case "$1" in
        --compress)  MODE_COMP=1; shift ;;
        --raw)       MODE_RAW=1; shift ;;
        --both)      MODE_RAW=1; MODE_COMP=1; shift ;;
        --workers)   WORKERS_CSV="$2"; shift 2 ;;
        --all)       WORKLOADS=(matmul dataproc ml-training xgboost redis mc-1gb mc-4gb mc-8gb mc-16gb); shift ;;
        *)           WORKLOADS+=("$1"); shift ;;
    esac
done

if [ "$MODE_RAW" = "0" ] && [ "$MODE_COMP" = "0" ]; then
    MODE_RAW=1
fi

# --workers accepts comma-separated list: "16,24" spawns one instance per
# (workload, mode, worker-count). Empty string = single worker count from
# run_restore_experiment.sh default.
if [ -z "$WORKERS_CSV" ]; then
    WORKERS_LIST=("")  # single empty element → skip PREFETCH_WORKERS override
else
    IFS=',' read -ra WORKERS_LIST <<< "$WORKERS_CSV"
fi

[ ${#WORKLOADS[@]} -gt 0 ] || { echo "ERROR: provide at least one workload"; exit 1; }
[ -x "${CRIU_SRC}/criu/criu" ] || { echo "ERROR: ${CRIU_SRC}/criu/criu not built"; exit 1; }

AWS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET=$(aws configure get aws_secret_access_key)
[ -n "$AWS_KEY" ] || { echo "ERROR: aws creds missing"; exit 1; }

# workload → (criu workload type, raw prefix, extra args) table.
# Mirrors the table in dump_all_workloads.sh — kept in sync so ablation
# always uses the same dump artifact prefix the dump pipeline produced.
workload_config() {
    case "$1" in
        matmul)      echo "matmul|matmul|--matrix-size 25000" ;;
        dataproc)    echo "dataproc|dataproc|--num-rows 17000000 --num-cols 60 --batch-size 1000" ;;
        ml-training) echo "ml_training|ml-training|--model-size large --dataset-size 2000000" ;;
        xgboost)     echo "xgboost|xgboost|--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3" ;;
        redis)       echo "redis|redis|--record-count 5000000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-1gb)      echo "memcached|memcached-1gb|--memcached-memory 1024 --record-count 773000 --ycsb-threads 4" ;;
        mc-4gb)      echo "memcached|memcached-4gb|--memcached-memory 4096 --record-count 3100000 --ycsb-threads 4" ;;
        mc-8gb)      echo "memcached|memcached|--memcached-memory 8192 --record-count 6200000 --ycsb-threads 4 --ycsb-workload a" ;;
        mc-16gb)     echo "memcached|memcached-16gb|--memcached-memory 16384 --record-count 12400000 --ycsb-threads 4" ;;
        *) return 1 ;;
    esac
}

MATRIX=()
for w in "${WORKLOADS[@]}"; do
    cfg=$(workload_config "$w") || { echo "ERROR: unknown workload $w"; exit 1; }
    for W in "${WORKERS_LIST[@]}"; do
        [ "$MODE_RAW" = "1" ]  && MATRIX+=("$w|raw|$W|$cfg")
        [ "$MODE_COMP" = "1" ] && MATRIX+=("$w|compress|$W|$cfg")
    done
done

N=${#MATRIX[@]}
echo "=========================================="
echo " criu: ${CRIU_SRC}/criu/criu ($(${CRIU_SRC}/criu/criu --version 2>&1 | grep -i gitid))"
echo " Repeat: $REPEAT"
echo " Ablations to run: $N"
for e in "${MATRIX[@]}"; do echo "   $e"; done
echo "=========================================="

INSTANCE_IDS=$(aws ec2 run-instances \
    --region $REGION --image-id $AMI_ID --instance-type $INSTANCE_TYPE \
    --count $N --key-name $KEY_NAME \
    --security-group-ids $SG --subnet-id $SUBNET \
    --iam-instance-profile Name=$IAM_PROFILE \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=criu-ablation}]" \
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
    IFS='|' read -r WL MODE W WTYPE RAW_PREFIX EXTRA_ARGS <<< "${MATRIX[$i]}"
    IP=${IPS[$i]}
    IID=${IIDS[$i]}

    if [ "$MODE" = "compress" ]; then
        PREFIX="${RAW_PREFIX}-compressed"
        SUFFIX="compressed"
    else
        PREFIX="${RAW_PREFIX}"
        SUFFIX="raw"
    fi
    # Worker count embedded in results suffix (e.g. compressed-w16) so different
    # PREFETCH_WORKERS sweeps coexist in the same results bucket.
    if [ -n "$W" ]; then
        SUFFIX="${SUFFIX}-w${W}"
    fi

    echo ""
    echo "--- $WL ($MODE${W:+ w=$W}) on $IP ($IID) — prefix=$PREFIX → suffix=$SUFFIX ---"
    wait_ssh $IP || { echo "ERROR: SSH timeout for $IP"; continue; }

    aws ec2 create-tags --region $REGION --resources $IID \
        --tags "Key=Experiment,Value=ablation-${WL}-${MODE}${W:+-w${W}}" 2>/dev/null || true

    # Bootstrap
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        cd /opt/criu_workload && git pull origin main -q
    " 2>/dev/null

    echo "   uploading criu binary..."
    scp -i $SSH_KEY -o StrictHostKeyChecking=no \
        "${CRIU_SRC}/criu/criu" ubuntu@$IP:/tmp/criu.phase6-compression >/dev/null

    echo "   syncing criu_workload tree (rsync)..."
    # Sync full source dirs so any local edit propagates (committed or not).
    rsync -a -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        --exclude '__pycache__' --exclude '*.pyc' --exclude '.git' \
        /spot_kubernetes/criu_workload/lib \
        /spot_kubernetes/criu_workload/workloads \
        /spot_kubernetes/criu_workload/experiments \
        /spot_kubernetes/criu_workload/config \
        /spot_kubernetes/criu_workload/tools \
        ubuntu@$IP:/tmp/criu_workload_sync/ >/dev/null

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "cat > /tmp/driver.sh" <<DRIVER
#!/bin/bash
set +e
export AWS_ACCESS_KEY_ID='${AWS_KEY}'
export AWS_SECRET_ACCESS_KEY='${AWS_SECRET}'
cd /opt/criu_workload

echo "=== install criu ==="
sudo install -m 0755 /tmp/criu.phase6-compression /usr/local/sbin/criu
/usr/local/sbin/criu --version

echo "=== overlay criu_workload tree (lib workloads experiments config tools) ==="
for d in lib workloads experiments config tools; do
    if [ -d /tmp/criu_workload_sync/\$d ]; then
        sudo rsync -a --chown=ubuntu:ubuntu \\
            /tmp/criu_workload_sync/\$d/ /opt/criu_workload/\$d/
    fi
done
sudo chmod +x /opt/criu_workload/experiments/*.sh 2>/dev/null || true

echo "=== [\$(date +%H:%M:%S)] ablation: ${WL} (${MODE}${W:+ w=${W}}) against s3://${BUCKET}/${PREFIX}/ ==="
MODES='${MODES:-}' PREFETCH_WORKERS='${W}' bash experiments/run_restore_experiment.sh \\
    --workload ${WTYPE} \\
    --s3-prefix ${PREFIX} \\
    --repeat ${REPEAT} \\
    --extra-args '${EXTRA_ARGS}' \\
    --s3-results-suffix ${SUFFIX} \\
    --auto-terminate \\
    > /tmp/ablation.log 2>&1
echo "Ablation exit: \$?"

echo "=== [\$(date +%H:%M:%S)] ${WL} (${MODE}${W:+ w=${W}}): DONE ==="
DRIVER

    ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$IP "
        chmod +x /tmp/driver.sh
        nohup bash /tmp/driver.sh > /tmp/driver.log 2>&1 &
        disown
        echo PID \$!
    " 2>&1
    echo "   $WL $MODE${W:+ w=$W} launched on $IP"
done

echo ""
echo "=========================================="
echo " $N ablations launched (each auto-terminates)"
for i in $(seq 0 $((N - 1))); do
    echo "  ${MATRIX[$i]%%|*|*} : ssh -i $SSH_KEY ubuntu@${IPS[$i]} 'tail -f /tmp/driver.log'   # ${IIDS[$i]}"
done
